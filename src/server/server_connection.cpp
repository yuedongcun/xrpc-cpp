/** @file server_connection.cpp @brief Implements one server-side RPC connection. */

#include "server/server_connection.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <sys/socket.h>

#include "common/latency_trace.h"
#include "server/connection_io_loop.h"
#include "server/service_registry.h"

namespace xrpc {
namespace {

auto MakeReadBufferSize() -> std::size_t { return 16U * 1024U; }

auto MakeMaxWriteBatchBytes() -> std::size_t { return 64U * 1024U; }

#ifdef XRPC_ENABLE_LATENCY_TRACE
void RecordBatchStage(const std::vector<RequestEnvelope> &requests, diagnostics::LatencyStage stage,
                      std::uint32_t value = 0, std::uint64_t timestamp_ns = 0) {
  for (const RequestEnvelope &request : requests) {
    diagnostics::RecordLatencyTrace(stage, request.request_id_, value, timestamp_ns);
  }
}
#endif

void ExecuteDispatchBatchOnWorker(ConnectionId connection_id, ConnectionIoLoop &owner_loop, ServiceRegistry &registry,
                                  ProtocolLimits protocol_limits, std::vector<RequestEnvelope> &requests) {
  const std::size_t request_count = requests.size();
  std::string batch_response_bytes;
  std::size_t successful_jobs = 0;
#ifdef XRPC_ENABLE_LATENCY_TRACE
  std::vector<std::uint64_t> trace_request_ids;
  trace_request_ids.reserve(request_count);
#endif

  for (std::size_t index = 0; index < request_count; ++index) {
    RequestEnvelope &request = requests[index];
    const std::uint64_t request_id = request.request_id_;
#ifdef XRPC_ENABLE_LATENCY_TRACE
    const std::uint32_t batch_position =
        (static_cast<std::uint32_t>(std::min<std::size_t>(request_count, UINT16_MAX)) << 16U) |
        static_cast<std::uint32_t>(std::min<std::size_t>(index, UINT16_MAX));
    diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::RequestStart, request_id, batch_position);
#endif
    ResponseEnvelope response = registry.Dispatch(std::move(request));
#ifdef XRPC_ENABLE_LATENCY_TRACE
    diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::DispatchEnd, request_id);
#endif
    try {
      FrameCodec codec(protocol_limits);
      batch_response_bytes.append(codec.Encode(response));
#ifdef XRPC_ENABLE_LATENCY_TRACE
      diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::EncodeEnd, request_id);
      if (diagnostics::LatencyTraceSampled(request_id)) {
        trace_request_ids.push_back(request_id);
      }
#endif
      ++successful_jobs;
    } catch (...) {
      break;
    }
  }

  if (successful_jobs > 0) {
    DispatchCompletion completion{.connection_id_ = connection_id,
                                  .response_bytes_ = std::move(batch_response_bytes),
                                  .completed_jobs_ = successful_jobs,
                                  .encode_failed_ = false};
#ifdef XRPC_ENABLE_LATENCY_TRACE
    completion.trace_request_ids_ = std::move(trace_request_ids);
    for (const std::uint64_t request_id : completion.trace_request_ids_) {
      diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::CompletionPost, request_id,
                                      static_cast<std::uint32_t>(completion.completed_jobs_));
    }
#endif
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
      read_buffer_(MakeReadBufferSize(), '\0'),
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
  while (state_ == State::Active) {
    const io::IoResult recv_result = co_await context_.Recv(socket_.fd(), read_buffer_.data(), read_buffer_.size());
#ifdef XRPC_ENABLE_LATENCY_TRACE
    const std::uint64_t received_at_ns = diagnostics::LatencyTraceEnabled() ? diagnostics::LatencyNowNs() : 0;
#endif
    if (state_ == State::Closed) {
      co_return;
    }

    if (state_ == State::Draining) {
      TryFinishDrain();
      co_return;
    }

    if (recv_result.result_ == 0) {
      state_ = State::Draining;
      break;
    }

    if (recv_result.result_ < 0) {
      Close();
      co_return;
    }

    const std::string_view received_bytes(read_buffer_.data(), recv_result.bytes_transferred_);
    FrameStreamFeedResult feed_result = frame_stream_.FeedBytes(received_bytes);
#ifdef XRPC_ENABLE_LATENCY_TRACE
    const std::uint64_t decoded_at_ns = diagnostics::LatencyTraceEnabled() ? diagnostics::LatencyNowNs() : 0;
    if (!HandleFeedResult(std::move(feed_result), received_at_ns, decoded_at_ns)) {
#else
    if (!HandleFeedResult(std::move(feed_result))) {
#endif
      co_return;
    }
  }

  TryFinishDrain();
}

auto ServerConnection::HandleFeedResult(FrameStreamFeedResult &&feed
#ifdef XRPC_ENABLE_LATENCY_TRACE
                                        ,
                                        std::uint64_t received_at_ns, std::uint64_t decoded_at_ns
#endif
                                        ) -> bool {
  if (feed.closed_) {
    Close();
    return false;
  }

  const std::size_t request_count = feed.requests_.size();
  if (request_count == 0) {
    return true;
  }

#ifdef XRPC_ENABLE_LATENCY_TRACE
  const std::uint32_t batch_size = static_cast<std::uint32_t>(std::min<std::size_t>(request_count, UINT32_MAX));
  for (std::size_t i = 0; i < request_count; ++i) {
    const std::uint64_t request_id = feed.requests_[i].request_id_;
    diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::ServerRecv, request_id, batch_size, received_at_ns);
    diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::ServerDecoded, request_id, batch_size, decoded_at_ns);
  }
#endif

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
  if (socket_.valid()) {
    (void)::shutdown(socket_.fd(), SHUT_RD);
  }
  TryFinishDrain();
}

void ServerConnection::OnEncodedDispatchComplete(std::string &&response_bytes, std::size_t completed_jobs
#ifdef XRPC_ENABLE_LATENCY_TRACE
                                                 ,
                                                 std::vector<std::uint64_t> trace_request_ids
#endif
) {
  ReleaseDispatchJobs(completed_jobs);
  if (state_ == State::Closed) {
    return;
  }
  try {
    (void)EnqueueWrite(std::move(response_bytes)
#ifdef XRPC_ENABLE_LATENCY_TRACE
                           ,
                       std::move(trace_request_ids)
#endif
    );
  } catch (...) {
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

auto ServerConnection::EnqueueWrite(std::string bytes
#ifdef XRPC_ENABLE_LATENCY_TRACE
                                    ,
                                    std::vector<std::uint64_t> trace_request_ids
#endif
                                    ) -> bool {
  if (state_ == State::Closed) {
    return false;
  }

  if (!TryReserveWriteBytes(bytes.size())) {
    return false;
  }

  write_queue_.push_back(PendingWrite{.bytes_ = std::move(bytes)
#ifdef XRPC_ENABLE_LATENCY_TRACE
                                          ,
                                      .trace_request_ids_ = std::move(trace_request_ids)
#endif
  });
  WakeWriteLoop();
  return true;
}

auto ServerConnection::TryReserveWriteBytes(std::size_t bytes) -> bool {
  assert(pending_write_bytes_ <= limits_.max_write_queue_bytes_);
  if (bytes > limits_.max_write_queue_bytes_ - pending_write_bytes_) {
    Close();
    return false;
  }

  pending_write_bytes_ += bytes;
  return true;
}

void ServerConnection::ReleaseWriteBytes(std::size_t bytes) {
  assert(bytes <= pending_write_bytes_);
  pending_write_bytes_ -= bytes;
}

auto ServerConnection::WriteLoop() -> runtime::Task<void> {
  while (state_ != State::Closed) {
    co_await WriteQueueAwaiter(*this);

    while (state_ != State::Closed && !write_queue_.empty()) {
      PendingWrite pending_write = std::move(write_queue_.front());
      write_queue_.pop_front();
      std::string frame = std::move(pending_write.bytes_);
#ifdef XRPC_ENABLE_LATENCY_TRACE
      std::vector<std::uint64_t> trace_request_ids = std::move(pending_write.trace_request_ids_);
#endif
      std::size_t frame_size = frame.size();
      if (!write_queue_.empty()) {
        const std::size_t max_batch_bytes = MakeMaxWriteBatchBytes();
        frame.reserve(std::min(pending_write_bytes_, max_batch_bytes));
        while (!write_queue_.empty() && frame.size() + write_queue_.front().bytes_.size() <= max_batch_bytes) {
          frame_size += write_queue_.front().bytes_.size();
          frame.append(write_queue_.front().bytes_);
#ifdef XRPC_ENABLE_LATENCY_TRACE
          trace_request_ids.insert(trace_request_ids.end(), write_queue_.front().trace_request_ids_.begin(),
                                   write_queue_.front().trace_request_ids_.end());
#endif
          write_queue_.pop_front();
        }
      }

      std::size_t offset = 0;
#ifdef XRPC_ENABLE_LATENCY_TRACE
      const std::uint64_t send_started_at_ns = diagnostics::LatencyTraceEnabled() ? diagnostics::LatencyNowNs() : 0;
      for (const std::uint64_t request_id : trace_request_ids) {
        diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::ServerSend, request_id,
                                        static_cast<std::uint32_t>(std::min<std::size_t>(frame.size(), UINT32_MAX)),
                                        send_started_at_ns);
      }
#endif
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
#ifdef XRPC_ENABLE_LATENCY_TRACE
      for (const std::uint64_t request_id : trace_request_ids) {
        diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::ServerSendComplete, request_id);
      }
#endif
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
#ifdef XRPC_ENABLE_LATENCY_TRACE
  const std::size_t pending_after_submission = worker_pool_.pending_jobs() + request_count;
  RecordBatchStage(*request_batch, diagnostics::LatencyStage::WorkerEnqueue,
                   static_cast<std::uint32_t>(std::min<std::size_t>(pending_after_submission, UINT32_MAX)));
#endif
  const ConnectionId connection_id = connection_id_;
  ConnectionIoLoop *owner_loop = &owner_loop_;
  ServiceRegistry *registry = &registry_;
  WorkerPool *worker_pool = &worker_pool_;
  const ProtocolLimits protocol_limits = protocol_limits_;
  const bool accepted = worker_pool_.TrySubmitBatch(
      [connection_id, owner_loop, registry, worker_pool, protocol_limits, request_batch]() -> void {
#ifdef XRPC_ENABLE_LATENCY_TRACE
        RecordBatchStage(*request_batch, diagnostics::LatencyStage::WorkerStart,
                         static_cast<std::uint32_t>(std::min<std::size_t>(worker_pool->pending_jobs(), UINT32_MAX)));
#endif
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

  try {
    return EnqueueWrite(frame_stream_.EncodeResponse(std::move(response)));
  } catch (...) {
    Close();
    return false;
  }
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
