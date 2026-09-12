/** @file server_connection.cpp @brief Implements one server-side RPC connection. */

#include "server/server_connection.h"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include <sys/socket.h>

#include "common/log.h"
#include "server/connection_io_loop.h"
#include "server/service_registry.h"

namespace xrpc {
namespace {

auto MakeMaxWriteBatchBytes() -> std::size_t { return 64U * 1024U; }

void ExecuteDispatchBatchOnWorker(ConnectionId connection_id, ConnectionIoLoop &owner_loop, ServiceRegistry &registry,
                                  ProtocolLimits protocol_limits, std::vector<RequestEnvelope> &requests) {
  const std::size_t request_count = requests.size();
  std::string batch_response_bytes;
  std::size_t successful_jobs = 0;

  for (RequestEnvelope &request : requests) {
    ResponseEnvelope response = registry.Dispatch(std::move(request));
    FrameCodec codec(protocol_limits);
    StatusOr<std::string> encoded = codec.Encode(response);
    if (!encoded.ok()) {
      break;
    }
    batch_response_bytes.append(std::move(encoded).value());
    ++successful_jobs;
  }

  if (successful_jobs > 0) {
    DispatchCompletion completion{.connection_id_ = connection_id,
                                  .response_bytes_ = std::move(batch_response_bytes),
                                  .completed_jobs_ = successful_jobs,
                                  .encode_failed_ = false};
    owner_loop.PostDispatchCompletion(std::move(completion));
  }

  if (successful_jobs < request_count) {
    DispatchCompletion completion{
        .connection_id_ = connection_id, .completed_jobs_ = request_count - successful_jobs, .encode_failed_ = true};
    owner_loop.PostDispatchCompletion(std::move(completion));
  }
}

}  // namespace

ServerConnection::ServerConnection(ConnectionId connection_id, ConnectionIoLoop &owner_loop, io::UringContext &context,
                                   ServiceRegistry &registry, WorkerPool &worker_pool, io::Socket socket,
                                   ServerConnectionConfig config, std::function<void()> on_closed)
    : connection_id_(connection_id),
      owner_loop_(owner_loop),
      context_(context),
      worker_pool_(worker_pool),
      registry_(registry),
      frame_stream_(config.protocol_limits_),
      protocol_limits_(config.protocol_limits_),
      socket_(std::move(socket)),
      limits_(config.limits_),
      on_closed_(std::move(on_closed)),
      read_loop_task_(ReadLoop()),
      write_loop_task_(WriteLoop()) {}

ServerConnection::~ServerConnection() = default;

void ServerConnection::Start() {
  write_loop_task_.Start();
  read_loop_task_.Start();
}

auto ServerConnection::WriteQueueAwaiter::await_ready() const noexcept -> bool {
  return connection_.state_ == State::Closed || !connection_.write_queue_.empty();
}

void ServerConnection::WriteQueueAwaiter::await_suspend(std::coroutine_handle<> continuation) const noexcept {
  assert(!connection_.write_queue_waiter_);
  connection_.write_queue_waiter_ = continuation;
}

auto ServerConnection::ReadLoop() -> runtime::Task<void> {
  auto receive = context_.RecvProvidedMultishot(socket_.fd());
  bool receive_pending = false;
  while (state_ == State::Active) {
    io::IoResult recv_result = co_await receive;
    receive_pending = recv_result.has_more_;
    if (state_ != State::Active) {
      break;
    }

    if (recv_result.result_ == 0) {
      state_ = State::Draining;
      break;
    }

    if (recv_result.error_code_ == ENOBUFS && !receive_pending) {
      recv_result.buffer_.Reset();
      const auto outcome =
          co_await context_.WaitForBufferReturnSince(socket_.fd(), recv_result.buffer_returns_at_start_);
      if (outcome == io::BufferReturnWaitOutcome::Cancelled || state_ != State::Active) {
        break;
      }
      receive = context_.RecvProvidedMultishot(socket_.fd());
      continue;
    }

    if (recv_result.result_ < 0) {
      if (recv_result.error_code_ != ECANCELED) {
        const char *reason = recv_result.error_code_ == ENOBUFS ? "recv buffer pool exhausted" : "recv failed";
        LOG(ERROR) << reason << " connection_id=" << connection_id_ << " fd=" << recv_result.fd_
                   << " buffer_group=" << recv_result.buffer_group_ << " errno=" << recv_result.error_code_
                   << " action=close_connection";
      }
      Close();
      break;
    }

    const auto bytes = recv_result.buffer_.Bytes();
    const std::string_view received_bytes(reinterpret_cast<const char *>(bytes.data()), bytes.size());
    FrameStreamFeedResult feed_result = frame_stream_.FeedBytes(received_bytes);
    // FeedBytes owns its copied input. Release before dispatch and the next receive.
    recv_result.buffer_.Reset();
    if (!HandleFeedResult(std::move(feed_result))) {
      break;
    }
    if (!receive_pending && state_ == State::Active) {
      // A successful final CQE still carries data, but needs a new receive.
      receive = context_.RecvProvidedMultishot(socket_.fd());
    }
  }

  // Close() cancels I/O; BeginDrain() shuts down reads without cancelling sends.
  // Both can leave queued data CQEs before the final completion. Keep the
  // awaitable alive and return each discarded buffer before waiting again.
  while (receive_pending) {
    io::IoResult discarded = co_await receive;
    receive_pending = discarded.has_more_;
  }

  TryFinishDrain();
}

auto ServerConnection::HandleFeedResult(FrameStreamFeedResult &&feed) -> bool {
  if (feed.closed_) {
    Close();
    return false;
  }

  const std::size_t request_count = feed.requests_.size();
  if (request_count == 0) {
    return true;
  }

  assert(inflight_requests_ <= limits_.max_inflight_);
  if (request_count > limits_.max_inflight_ - inflight_requests_) {
    return feed.requests_.ConsumeEach([this](RequestEnvelope request) -> bool {
      return RejectForBackpressure(std::move(request), "server per-connection in-flight limit exceeded");
    });
  }

  std::vector<RequestEnvelope> requests;
  requests.reserve(request_count);
  feed.requests_.ConsumeEach([&requests](RequestEnvelope request) -> bool {
    requests.push_back(std::move(request));
    return true;
  });
  return SubmitDispatchBatch(std::move(requests));
}

void ServerConnection::Close() {
  if (state_ == State::Closed) {
    return;
  }

  state_ = State::Closed;
  write_queue_.clear();
  owner_loop_.RecordWriteBytesChange(pending_write_bytes_, 0);
  pending_write_bytes_ = 0;
  WakeWriteLoop();
  context_.CancelFd(socket_.fd());
  socket_.Close();
  if (on_closed_) {
    on_closed_();
  }
}

void ServerConnection::BeginDrain() {
  if (state_ != State::Active) {
    return;
  }

  state_ = State::Draining;
  context_.CancelBufferReturnWaitsForFd(socket_.fd());
  if (socket_.valid()) {
    (void)::shutdown(socket_.fd(), SHUT_RD);
  }
  TryFinishDrain();
}

void ServerConnection::OnEncodedDispatchComplete(std::string &&response_bytes, std::size_t completed_jobs) {
  ReleaseDispatchJobs(completed_jobs);
  if (state_ == State::Closed) {
    return;
  }
  try {
    (void)EnqueueWrite(std::move(response_bytes));
  } catch (const std::bad_alloc &) {  // XRPC_EXCEPTION_GUARD: isolate allocation failure to this connection
    Close();
  }
  TryFinishDrain();
}

void ServerConnection::OnDispatchEncodeFailure(std::size_t completed_jobs) {
  ReleaseDispatchJobs(completed_jobs);
  Close();
}

void ServerConnection::ReleaseDispatchJobs(std::size_t completed_jobs) {
  assert(completed_jobs <= inflight_requests_);
  inflight_requests_ -= completed_jobs;
}

auto ServerConnection::EnqueueWrite(std::string bytes) -> bool {
  if (state_ == State::Closed) {
    return false;
  }

  if (!TryReserveWriteBytes(bytes.size())) {
    return false;
  }

  write_queue_.push_back(PendingWrite{.bytes_ = std::move(bytes)});
  WakeWriteLoop();
  return true;
}

auto ServerConnection::TryReserveWriteBytes(std::size_t bytes) -> bool {
  assert(pending_write_bytes_ <= limits_.max_write_queue_bytes_);
  if (bytes > limits_.max_write_queue_bytes_ - pending_write_bytes_) {
    Close();
    return false;
  }

  const std::size_t before = pending_write_bytes_;
  pending_write_bytes_ += bytes;
  owner_loop_.RecordWriteBytesChange(before, pending_write_bytes_);
  return true;
}

void ServerConnection::ReleaseWriteBytes(std::size_t bytes) {
  assert(bytes <= pending_write_bytes_);
  const std::size_t before = pending_write_bytes_;
  pending_write_bytes_ -= bytes;
  owner_loop_.RecordWriteBytesChange(before, pending_write_bytes_);
}

auto ServerConnection::WriteLoop() -> runtime::Task<void> {
  while (state_ != State::Closed) {
    co_await WriteQueueAwaiter(*this);

    while (state_ != State::Closed && !write_queue_.empty()) {
      PendingWrite pending_write = std::move(write_queue_.front());
      write_queue_.pop_front();
      std::string frame = std::move(pending_write.bytes_);
      std::size_t frame_size = frame.size();
      if (!write_queue_.empty()) {
        const std::size_t max_batch_bytes = MakeMaxWriteBatchBytes();
        frame.reserve(std::min(pending_write_bytes_, max_batch_bytes));
        while (!write_queue_.empty() && frame.size() + write_queue_.front().bytes_.size() <= max_batch_bytes) {
          frame_size += write_queue_.front().bytes_.size();
          frame.append(write_queue_.front().bytes_);
          write_queue_.pop_front();
        }
      }

      std::size_t offset = 0;
      while (state_ != State::Closed && offset < frame.size()) {
        const std::string_view remaining(frame.data() + offset, frame.size() - offset);
        const io::IoResult send_result = co_await context_.Send(socket_.fd(), remaining.data(), remaining.size());
        if (state_ == State::Closed) {
          co_return;
        }
        if (send_result.result_ <= 0) {
          ReleaseWriteBytes(frame_size);
          Close();
          co_return;
        }

        offset += send_result.bytes_transferred_;
      }
      ReleaseWriteBytes(frame_size);
    }

    TryFinishDrain();
  }
}

void ServerConnection::WakeWriteLoop() {
  if (!write_queue_waiter_) {
    return;
  }

  const std::coroutine_handle<> continuation = std::exchange(write_queue_waiter_, nullptr);
  continuation.resume();
}

auto ServerConnection::CanBeCollected() const -> bool {
  return state_ == State::Closed && read_loop_task_.Done() && write_loop_task_.Done();
}

auto ServerConnection::SubmitDispatchBatch(std::vector<RequestEnvelope> requests) -> bool {
  const std::size_t request_count = requests.size();
  assert(request_count > 0);

  auto request_batch = std::make_shared<std::vector<RequestEnvelope>>(std::move(requests));
  const ConnectionId connection_id = connection_id_;
  ConnectionIoLoop *owner_loop = &owner_loop_;
  ServiceRegistry *registry = &registry_;
  const ProtocolLimits protocol_limits = protocol_limits_;
  const bool accepted = worker_pool_.TrySubmitBatch(
      [connection_id, owner_loop, registry, protocol_limits, request_batch]() -> void {
        ExecuteDispatchBatchOnWorker(connection_id, *owner_loop, *registry, protocol_limits, *request_batch);
      },
      request_count);

  if (!accepted) {
    if (!worker_pool_.accepting_submissions()) {
      BeginDrain();
      return false;
    }
    for (RequestEnvelope &request : *request_batch) {
      if (!RejectForBackpressure(std::move(request), "server global pending job limit exceeded")) {
        return false;
      }
    }
    return true;
  }

  inflight_requests_ += request_count;
  return true;
}

auto ServerConnection::RejectForBackpressure(RequestEnvelope &&request, std::string message) -> bool {
  ResponseEnvelope response;
  response.request_id_ = request.request_id_;
  response.status_ = {StatusCode::ResourceExhausted, std::move(message)};

  StatusOr<std::string> encoded = frame_stream_.EncodeResponse(std::move(response));
  if (!encoded.ok()) {
    Close();
    return false;
  }
  return EnqueueWrite(std::move(encoded).value());
}

void ServerConnection::TryFinishDrain() {
  if (state_ != State::Draining) {
    return;
  }

  if (inflight_requests_ == 0 && pending_write_bytes_ == 0) {
    assert(write_queue_.empty());
    Close();
  }
}

}  // namespace xrpc
