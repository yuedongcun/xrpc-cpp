/** @file server_connection.cpp @brief Implements one server-side RPC connection. */

#include "server/server_connection.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <sys/socket.h>

#include "server/dispatch_mailbox.h"
#include "server/service_registry.h"

namespace xrpc {
namespace {

auto MakeReadBufferSize() -> std::size_t { return 16U * 1024U; }

auto MakeMaxWriteBatchBytes() -> std::size_t { return 64U * 1024U; }

}  // namespace

ServerConnection::ServerConnection(io::UringContext &context, ServiceRegistry &registry, ThreadPoolExecutor &executor,
                                   DispatchMailbox &mailbox, io::Socket socket, ServerConnectionConfig config,
                                   std::function<void()> on_closed)
    : context_(&context),
      mailbox_(&mailbox),
      executor_(&executor),
      registry_(&registry),
      frame_stream_(config.protocol_limits_),
      protocol_limits_(config.protocol_limits_),
      socket_(std::move(socket)),
      read_buffer_(MakeReadBufferSize(), '\0'),
      limits_(config.limits_),
      on_closed_(std::move(on_closed)) {}

ServerConnection::~ServerConnection() = default;

auto ServerConnection::Run() -> runtime::Task<void> {
  while (state_ == State::Active) {
    const io::IoResult recv_result = co_await context_->Recv(socket_.fd(), read_buffer_.data(), read_buffer_.size());
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
    if (!HandleFeedResult(std::move(feed_result))) {
      co_return;
    }
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
    return feed.requests_.ConsumeEach([this](RawRequest request) -> bool {
      return RejectRequestDueToBackpressure(std::move(request), "server per-connection in-flight limit exceeded");
    });
  }

  std::vector<RawRequest> requests;
  requests.reserve(request_count);
  feed.requests_.ConsumeEach([&requests](RawRequest request) -> bool {
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
  write_in_progress_ = false;
  context_->CancelFd(socket_.fd());
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

void ServerConnection::OnEncodedDispatchComplete(std::string &&response_bytes, std::size_t completed_jobs) {
  ReleaseDispatchJobs(completed_jobs);
  if (state_ == State::Closed) {
    return;
  }
  try {
    (void)EnqueueWrite(std::move(response_bytes));
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

auto ServerConnection::EnqueueWrite(std::string bytes) -> bool {
  if (state_ == State::Closed) {
    return false;
  }

  if (!TryReserveWriteBytes(bytes.size())) {
    return false;
  }

  write_queue_.push_back(std::move(bytes));
  if (write_in_progress_) {
    return true;
  }

  write_in_progress_ = true;
  write_task_.emplace(DrainWriteQueue());
  write_task_->Start();
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

auto ServerConnection::DrainWriteQueue() -> runtime::Task<void> {
  while (state_ != State::Closed && !write_queue_.empty()) {
    std::string frame = std::move(write_queue_.front());
    write_queue_.pop_front();
    std::size_t frame_size = frame.size();
    if (!write_queue_.empty()) {
      const std::size_t max_batch_bytes = MakeMaxWriteBatchBytes();
      frame.reserve(std::min(pending_write_bytes_, max_batch_bytes));
      while (!write_queue_.empty() && frame.size() + write_queue_.front().size() <= max_batch_bytes) {
        frame_size += write_queue_.front().size();
        frame.append(write_queue_.front());
        write_queue_.pop_front();
      }
    }
    std::size_t offset = 0;
    while (state_ != State::Closed && offset < frame.size()) {
      const std::string_view remaining(frame.data() + offset, frame.size() - offset);
      const io::IoResult send_result = co_await context_->Send(socket_.fd(), remaining.data(), remaining.size());
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
  write_in_progress_ = false;
  TryFinishDrain();
}

auto ServerConnection::SubmitDispatchBatch(std::vector<RawRequest> requests) -> bool {
  const std::size_t request_count = requests.size();
  assert(request_count > 0);

  std::weak_ptr<ServerConnection> weak_self = weak_from_this();
  auto request_batch = std::make_shared<std::vector<RawRequest>>(std::move(requests));
  const bool accepted = executor_->TrySubmitBatch(
      [weak_self, request_batch]() -> void {
        std::shared_ptr<ServerConnection> self = weak_self.lock();
        if (!self) {
          return;
        }
        self->ExecuteDispatchBatchOnWorker(weak_self, *request_batch);
      },
      request_count);

  if (!accepted) {
    if (!executor_->accepting_submissions()) {
      BeginDrain();
      return false;
    }
    for (RawRequest &request : *request_batch) {
      if (!RejectRequestDueToBackpressure(std::move(request), "server global pending job limit exceeded")) {
        return false;
      }
    }
    return true;
  }

  inflight_requests_ += request_count;
  return true;
}

void ServerConnection::ExecuteDispatchBatchOnWorker(const std::weak_ptr<ServerConnection> &target,
                                                    std::vector<RawRequest> &requests) {
  const std::size_t request_count = requests.size();
  std::string batch_response_bytes;
  std::size_t successful_jobs = 0;

  for (RawRequest &request : requests) {
    RawResponse response = registry_->Dispatch(std::move(request));
    try {
      batch_response_bytes.append(EncodeResponseOnWorker(std::move(response)));
      ++successful_jobs;
    } catch (...) {
      break;
    }
  }

  if (successful_jobs > 0) {
    mailbox_->Submit(DispatchCompletion{.target_connection_ = target,
                                        .response_bytes_ = std::move(batch_response_bytes),
                                        .completed_jobs_ = successful_jobs,
                                        .encode_failed_ = false});
  }

  if (successful_jobs < request_count) {
    mailbox_->Submit(DispatchCompletion{
        .target_connection_ = target, .completed_jobs_ = request_count - successful_jobs, .encode_failed_ = true});
  }
}

auto ServerConnection::RejectRequestDueToBackpressure(RawRequest &&request, std::string message) -> bool {
  RawResponse response;
  response.request_id_ = request.request_id_;
  response.status_ = {StatusCode::ResourceExhausted, std::move(message)};

  try {
    return EnqueueWrite(frame_stream_.EncodeResponse(std::move(response)));
  } catch (...) {
    Close();
    return false;
  }
}

auto ServerConnection::EncodeResponseOnWorker(RawResponse &&response) const -> std::string {
  FrameCodec codec(protocol_limits_);
  return codec.EncodeResponse(response);
}

void ServerConnection::TryFinishDrain() {
  if (state_ != State::Draining) {
    return;
  }

  if (inflight_requests_ == 0 && !write_in_progress_ && write_queue_.empty()) {
    assert(pending_write_bytes_ == 0);
    Close();
  }
}

}  // namespace xrpc
