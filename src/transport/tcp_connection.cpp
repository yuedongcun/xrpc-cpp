#include "transport/tcp_connection.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rpc/protocol_adapter.h"
#include "transport/dispatch_completion_queue.h"

namespace xrpc {
namespace {

/** @return Default receive buffer size for each asynchronous socket read. */
auto MakeReadBufferSize() -> std::size_t { return 16U * 1024U; }

/** @return Maximum bytes coalesced into one asynchronous send operation. */
auto MakeMaxWriteBatchBytes() -> std::size_t { return 64U * 1024U; }

}  // namespace

/**
 * @brief Creates a connection actor for one accepted TCP socket.
 *
 * All socket I/O runs on `context`, while request handlers run on `executor` and report completions
 * back through `completion_queue_`.
 *
 * @param context Event loop owning this connection.
 * @param handler Raw request dispatcher.
 * @param executor Worker pool for handler execution.
 * @param socket Accepted nonblocking client socket.
 * @param options Protocol limits, backpressure limits, and idle timeout.
 */
TcpConnection::TcpConnection(io::UringContext &context, RawHandler handler, ThreadPoolExecutor &executor,
                             io::Socket socket, TcpConnectionOptions options)
    : context_(&context),
      completion_queue_(std::move(options.completion_queue_)),
      executor_(&executor),
      handler_(std::move(handler)),
      frame_stream_(options.protocol_limits_),
      protocol_limits_(options.protocol_limits_),
      socket_(std::move(socket)),
      read_buffer_(MakeReadBufferSize(), '\0'),
      idle_timeout_(options.idle_timeout_),
      limits_(options.limits_),
      backpressure_stats_(options.backpressure_stats_) {
  assert(completion_queue_ != nullptr);
  assert(backpressure_stats_ != nullptr);
}

/** @brief Connection state is closed by the run loop or owner before destruction. */
TcpConnection::~TcpConnection() = default;

/**
 * @brief Runs the asynchronous read loop until peer close, socket error, or protocol failure.
 *
 * @return Coroutine task completed after the connection finishes or closes.
 */
auto TcpConnection::Run() -> runtime::Task<void> {
  StartIdleTimerIfNeeded();

  while (!closed_ && !peer_read_closed_) {
    const io::IoResult recv_result = co_await context_->Recv(socket_.fd(), read_buffer_.data(), read_buffer_.size());
    if (closed_) {
      co_return;
    }

    if (recv_result.result_ == 0) {
      peer_read_closed_ = true;
      break;
    }

    if (recv_result.result_ < 0) {
      Close(ConnectionCloseReason::SocketError);
      co_return;
    }

    TouchActivity();
    if (!HandleFeedResult(
            frame_stream_.FeedBytes(std::string_view(read_buffer_.data(), recv_result.bytes_transferred_)))) {
      co_return;
    }
  }

  TryFinishAfterPeerClosed();
}

/**
 * @brief Handles decoded requests produced by feeding bytes into the RPC frame stream.
 *
 * Requests decoded from one read are batched into one worker submission to reduce context handoffs.
 * Per-connection backpressure is enforced before the batch reaches the executor.
 *
 * @param feed Result from `RpcFrameStream::FeedBytes()`.
 * @return true when the connection can continue reading.
 */
auto TcpConnection::HandleFeedResult(FrameStreamFeedResult &&feed) -> bool {
  if (feed.closed_) {
    Close(ConnectionCloseReason::ProtocolError);
    return false;
  }

  const std::size_t request_count = feed.requests_.size();
  if (request_count == 0) {
    return true;
  }

  // Batch all requests decoded from this read into one worker task. This is a
  // throughput tradeoff: fewer executor handoffs and wakeups, at the cost of
  // coarser scheduling granularity because one worker owns the whole batch.
  std::vector<RawRequest> requests;
  requests.reserve(request_count);
  const bool accepted_all = feed.requests_.ConsumeEach([this, &requests](RawRequest request) {
    if (pending_dispatch_jobs_ + requests.size() >= limits_.max_inflight_) {
      backpressure_stats_->RecordInflightRejection();
      return RejectRequestDueToBackpressure(std::move(request), "server per-connection in-flight limit exceeded");
    }
    requests.push_back(std::move(request));
    return true;
  });
  if (!accepted_all) {
    return false;
  }
  if (requests.empty()) {
    return true;
  }
  return SubmitDispatchBatch(std::move(requests));
}

/**
 * @brief Closes the connection and cancels all pending socket operations.
 *
 * @param reason First close reason to record for diagnostics.
 */
void TcpConnection::Close(ConnectionCloseReason reason) {
  if (closed_) {
    return;
  }

  closed_ = true;
  SetClosedReason(reason);
  write_queue_.clear();
  pending_write_bytes_ = 0;
  write_in_progress_ = false;
  context_->CancelFd(socket_.fd());
  socket_.Close();
}

/**
 * @brief Enqueues encoded worker responses back on the event-loop thread.
 *
 * @param response_bytes One or more encoded response frames.
 * @param completed_jobs Number of logical requests completed by the worker batch.
 */
void TcpConnection::OnEncodedDispatchComplete(std::string &&response_bytes, std::size_t completed_jobs) {
  ReleaseDispatchJobs(completed_jobs);
  TouchActivity();
  if (closed_) {
    TryFinishAfterPeerClosed();
    return;
  }
  try {
    (void)EnqueueWrite(std::move(response_bytes));
  } catch (...) {
    Close(ConnectionCloseReason::ProtocolError);
  }
  TryFinishAfterPeerClosed();
}

/**
 * @brief Accounts for a worker-side response encoding failure.
 *
 * @param completed_jobs Number of logical requests removed from in-flight accounting.
 */
void TcpConnection::OnDispatchEncodeFailure(std::size_t completed_jobs) {
  ReleaseDispatchJobs(completed_jobs);
  Close(ConnectionCloseReason::ProtocolError);
  TryFinishAfterPeerClosed();
}

/**
 * @brief Releases per-connection in-flight worker accounting.
 *
 * @param completed_jobs Number of logical dispatch jobs completed or failed.
 */
void TcpConnection::ReleaseDispatchJobs(std::size_t completed_jobs) {
  if (pending_dispatch_jobs_ >= completed_jobs) {
    pending_dispatch_jobs_ -= completed_jobs;
    return;
  }
  pending_dispatch_jobs_ = 0;
}

/**
 * @brief Adds encoded response bytes to the write queue and starts a drain task if needed.
 *
 * @param bytes Encoded response frame bytes.
 * @return true when bytes were queued, false when the connection is already closed or backpressured.
 */
auto TcpConnection::EnqueueWrite(std::string bytes) -> bool {
  if (closed_) {
    return false;
  }

  if (!TryReserveWriteBytes(bytes.size())) {
    return false;
  }

  write_queue_.push_back(std::move(bytes));
  TouchActivity();
  if (write_in_progress_) {
    return true;
  }

  write_in_progress_ = true;
  write_task_.emplace(DrainWriteQueue());
  write_task_->Start();
  return true;
}

/**
 * @brief Reserves write-queue bytes against the per-connection backpressure limit.
 *
 * @param bytes Number of bytes about to be queued.
 * @return true when the reservation succeeds, false after closing for backpressure.
 */
auto TcpConnection::TryReserveWriteBytes(std::size_t bytes) -> bool {
  if (pending_write_bytes_ > limits_.max_write_queue_bytes_ ||
      bytes > limits_.max_write_queue_bytes_ - pending_write_bytes_) {
    backpressure_stats_->RecordWriteQueueClosure();
    Close(ConnectionCloseReason::Backpressure);
    return false;
  }

  pending_write_bytes_ += bytes;
  backpressure_stats_->ObserveWriteQueueBytes(pending_write_bytes_);
  return true;
}

/**
 * @brief Releases bytes after they leave the write queue.
 *
 * @param bytes Number of queued bytes represented by the drained frame or batch.
 */
void TcpConnection::ReleaseWriteBytes(std::size_t bytes) {
  if (pending_write_bytes_ >= bytes) {
    pending_write_bytes_ -= bytes;
    return;
  }
  pending_write_bytes_ = 0;
}

/**
 * @brief Sends queued response frames until the queue is empty or the socket fails.
 *
 * Adjacent frames may be coalesced into one send buffer because the wire protocol is already
 * self-delimiting. This improves throughput without changing response framing.
 *
 * @return Coroutine task completed after the current write drain finishes.
 */
auto TcpConnection::DrainWriteQueue() -> runtime::Task<void> {
  while (!closed_ && !write_queue_.empty()) {
    std::string frame = std::move(write_queue_.front());
    write_queue_.pop_front();
    std::size_t frame_size = frame.size();
    if (!write_queue_.empty()) {
      // TCP is a byte stream, so adjacent complete frames can be sent in one
      // write without changing protocol semantics. Batching cuts per-frame
      // send overhead on high-QPS workloads.
      const std::size_t max_batch_bytes = MakeMaxWriteBatchBytes();
      frame.reserve(std::min(pending_write_bytes_, max_batch_bytes));
      while (!write_queue_.empty() && frame.size() + write_queue_.front().size() <= max_batch_bytes) {
        frame_size += write_queue_.front().size();
        frame.append(write_queue_.front());
        write_queue_.pop_front();
      }
    }
    bool sent = true;
    std::size_t offset = 0;
    while (!closed_ && offset < frame.size()) {
      const std::string_view remaining(frame.data() + offset, frame.size() - offset);
      const io::IoResult send_result = co_await context_->Send(socket_.fd(), remaining.data(), remaining.size());
      if (send_result.result_ <= 0) {
        Close(ConnectionCloseReason::SocketError);
        sent = false;
        break;
      }

      TouchActivity();
      offset += send_result.bytes_transferred_;
    }
    if (closed_) {
      sent = false;
    }
    ReleaseWriteBytes(frame_size);
    if (!sent) {
      write_in_progress_ = false;
      co_return;
    }
  }
  write_in_progress_ = false;
  TryFinishAfterPeerClosed();
}

/**
 * @brief Submits decoded requests to the worker pool as one logical batch.
 *
 * The executor capacity is charged by logical request count. On rejection, each request gets an
 * immediate `ResourceExhausted` response when the connection can still write.
 *
 * @param requests Requests decoded from one read.
 * @return true when the connection can continue reading.
 */
auto TcpConnection::SubmitDispatchBatch(std::vector<RawRequest> requests) -> bool {
  const std::size_t request_count = requests.size();
  if (request_count == 0) {
    return true;
  }

  std::weak_ptr<TcpConnection> weak_self = weak_from_this();
  auto request_batch = std::make_shared<std::vector<RawRequest>>(std::move(requests));
  const bool accepted = executor_->TrySubmitBatch(
      [weak_self, request_batch, request_count]() mutable {
        std::shared_ptr<TcpConnection> self = weak_self.lock();
        if (!self) {
          return;
        }

        // A worker batch emits one combined write buffer on the success path.
        // This removes per-response completion and write-queue nodes, but it
        // makes the first response in the batch wait until the whole batch has
        // been dispatched and encoded.
        // This is intentionally not a cross-read coalescing buffer: the batch
        // is bounded by requests already decoded from one read. If this ever
        // waits for future reads to fill a batch, it must add a flush deadline
        // so a partial batch cannot hold the first response indefinitely.
        std::string batch_response_bytes;
        std::size_t successful_jobs = 0;
        bool encode_failed = false;
        for (RawRequest &request : *request_batch) {
          RawResponse response = self->handler_(std::move(request));
          try {
            batch_response_bytes.append(self->EncodeResponseOnWorker(std::move(response)));
            ++successful_jobs;
          } catch (...) {
            encode_failed = true;
            break;
          }
        }
        if (successful_jobs > 0) {
          self->completion_queue_->Submit(DispatchCompletion{.connection_ = weak_self,
                                                             .response_bytes_ = std::move(batch_response_bytes),
                                                             .completed_jobs_ = successful_jobs,
                                                             .encode_failed_ = false});
        }
        if (encode_failed) {
          self->completion_queue_->Submit(DispatchCompletion{
              .connection_ = weak_self, .completed_jobs_ = request_count - successful_jobs, .encode_failed_ = true});
        }
      },
      request_count);
  if (!accepted) {
    for (RawRequest &request : *request_batch) {
      if (!RejectRequestDueToBackpressure(std::move(request), "server global pending job limit exceeded")) {
        return false;
      }
    }
    return true;
  }
  pending_dispatch_jobs_ += request_count;
  backpressure_stats_->ObserveInflight(pending_dispatch_jobs_);
  return true;
}

/**
 * @brief Encodes and queues a backpressure rejection response for one request.
 *
 * @param request Request rejected before worker execution.
 * @param message Public failure message.
 * @return true when the rejection response was queued successfully.
 */
auto TcpConnection::RejectRequestDueToBackpressure(RawRequest &&request, std::string message) -> bool {
  RawResponse response;
  response.request_id_ = request.request_id_;
  response.status_ = {StatusCode::ResourceExhausted, std::move(message)};

  try {
    return EnqueueWrite(frame_stream_.EncodeResponse(std::move(response)));
  } catch (...) {
    Close(ConnectionCloseReason::ProtocolError);
    return false;
  }
}

/**
 * @brief Encodes a raw response on the worker thread before event-loop handoff.
 *
 * @param response Raw response returned by the handler.
 * @return Encoded response frame bytes.
 */
auto TcpConnection::EncodeResponseOnWorker(RawResponse &&response) const -> std::string {
  FrameCodec codec(protocol_limits_);
  return codec.EncodeResponse(ToProtocolResponse(std::move(response)));
}

/**
 * @brief Stores the first close reason for this connection.
 *
 * @param reason Candidate close reason.
 */
void TcpConnection::SetClosedReason(ConnectionCloseReason reason) {
  if (close_reason_ == ConnectionCloseReason::None) {
    close_reason_ = reason;
  }
}

/**
 * @brief Closes after peer read shutdown once all pending responses have drained.
 *
 * This allows a client half-close to receive responses for already submitted requests before the
 * connection records a normal peer-close reason.
 */
void TcpConnection::TryFinishAfterPeerClosed() {
  if (!peer_read_closed_ || closed_) {
    return;
  }

  if (pending_dispatch_jobs_ == 0 && !write_in_progress_ && write_queue_.empty()) {
    Close(ConnectionCloseReason::PeerClosed);
  }
}

/**
 * @brief Starts the idle timer coroutine when idle timeouts are enabled.
 */
void TcpConnection::StartIdleTimerIfNeeded() {
  if (idle_timeout_ <= std::chrono::milliseconds::zero() || idle_timer_task_.has_value()) {
    return;
  }

  idle_timer_task_.emplace(RunIdleTimer());
  idle_timer_task_->Start();
}

/**
 * @brief Closes the connection after an idle period with no pending work.
 *
 * The generation counter avoids resetting kernel timers on every activity; the timer only checks
 * whether any read, write, or dispatch completion occurred during the sleep interval.
 *
 * @return Coroutine task completed when the timer exits or closes the connection.
 */
auto TcpConnection::RunIdleTimer() -> runtime::Task<void> {
  while (!closed_) {
    // The generation avoids resetting or canceling the kernel timeout on every
    // activity; any read/write/dispatch completion changes the observed value.
    const std::uint64_t observed_generation = activity_generation_;
    const io::IoResult result = co_await context_->SleepFor(idle_timeout_);
    if (closed_ || result.result_ < 0) {
      co_return;
    }

    if (activity_generation_ == observed_generation && !HasPendingWork()) {
      Close(ConnectionCloseReason::IdleTimeout);
      co_return;
    }
  }
}

/** @brief Marks that connection activity occurred for idle-timeout accounting. */
void TcpConnection::TouchActivity() noexcept { ++activity_generation_; }

/** @return true when dispatch or write state should keep the connection alive. */
auto TcpConnection::HasPendingWork() const noexcept -> bool {
  return pending_dispatch_jobs_ > 0 || write_in_progress_ || !write_queue_.empty() || pending_write_bytes_ > 0;
}

}  // namespace xrpc
