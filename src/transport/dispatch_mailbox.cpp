#include "transport/dispatch_mailbox.h"

#include <utility>

#include "transport/tcp_connection.h"

namespace xrpc {

/**
 * @brief Creates a dispatch mailbox bound to one io_uring context.
 *
 * @param context Event-loop context on which connection callbacks must execute.
 */
DispatchMailbox::DispatchMailbox(io::UringContext &context) : context_(&context) {}

/**
 * @brief Enqueues one worker completion and schedules a drain callback on the context thread.
 *
 * Worker threads may call this concurrently. At most one posted drain is active at a time; that
 * drain swaps and processes all accumulated completions on the owning event-loop thread.
 *
 * @param completion Encoded response or encode-failure accounting result.
 */
void DispatchMailbox::Submit(DispatchCompletion completion) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (context_ == nullptr) {
    return;
  }

  pending_completions_.push_back(std::move(completion));
  if (drain_posted_) {
    return;
  }

  // Only one drain callback is needed; it will swap and process all completions
  // currently queued, then loop once more for completions submitted meanwhile.
  drain_posted_ = true;
  std::weak_ptr<DispatchMailbox> weak_mailbox = weak_from_this();
  context_->Post([weak_mailbox]() {
    std::shared_ptr<DispatchMailbox> mailbox = weak_mailbox.lock();
    if (!mailbox) {
      return;
    }
    mailbox->DrainOnContext();
  });
}

/**
 * @brief Disables future handoff callbacks during connection-loop shutdown.
 *
 * Any completions not yet delivered are dropped because their event loop is no longer allowed to
 * touch connections.
 */
void DispatchMailbox::Disable() {
  std::lock_guard<std::mutex> lock(mutex_);
  context_ = nullptr;
  pending_completions_.clear();
  drain_completions_.clear();
  drain_posted_ = false;
}

/**
 * @brief Drains queued worker completions on the owning io_uring context thread.
 *
 * This method invokes `TcpConnection` callbacks without holding the mailbox mutex so workers can keep
 * submitting completions while connection code enqueues response bytes.
 */
void DispatchMailbox::DrainOnContext() {
  while (true) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (pending_completions_.empty()) {
        drain_posted_ = false;
        return;
      }
      // Swap under the mutex, then invoke connection callbacks outside it so
      // worker threads can keep submitting completions.
      drain_completions_.clear();
      drain_completions_.swap(pending_completions_);
    }

    for (DispatchCompletion &completion : drain_completions_) {
      std::shared_ptr<TcpConnection> connection = completion.target_connection_.lock();
      if (!connection) {
        continue;
      }
      if (completion.encode_failed_) {
        connection->OnDispatchEncodeFailure(completion.completed_jobs_);
      } else {
        connection->OnEncodedDispatchComplete(std::move(completion.response_bytes_), completion.completed_jobs_);
      }
    }
    drain_completions_.clear();
  }
}

}  // namespace xrpc
