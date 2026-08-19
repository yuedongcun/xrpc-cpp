/**
 * @file dispatch_mailbox.cpp
 * @brief Implements batched worker-to-I/O-thread completion delivery.
 */

#include "server/dispatch_mailbox.h"

#include <utility>

#include "server/server_connection.h"

namespace xrpc {

DispatchMailbox::DispatchMailbox(io::UringContext &context) : context_(&context) {}

void DispatchMailbox::Submit(DispatchCompletion completion) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (context_ == nullptr) {
    return;
  }

  pending_completions_.push_back(std::move(completion));
  // One outstanding processing callback owns responsibility for both the
  // current batch and completions that arrive before it finishes.
  if (completion_processing_pending_) {
    return;
  }

  completion_processing_pending_ = true;
  std::weak_ptr<DispatchMailbox> weak_mailbox = weak_from_this();
  // The posted callback must not extend mailbox lifetime across shutdown.
  context_->Post([weak_mailbox]() -> void {
    std::shared_ptr<DispatchMailbox> mailbox = weak_mailbox.lock();
    if (!mailbox) {
      return;
    }
    mailbox->ProcessCompletionsOnContext();
  });
}

void DispatchMailbox::Disable() {
  std::lock_guard<std::mutex> lock(mutex_);

  // Serialize shutdown with concurrent Submit() calls. Once context_ becomes
  // null, future submissions are ignored.
  context_ = nullptr;

  pending_completions_.clear();
  drain_completions_.clear();
  completion_processing_pending_ = false;
}

void DispatchMailbox::ProcessCompletionsOnContext() {
  // Keep processing until no completion arrived while the previous batch was
  // being processed. Submit() does not post another callback while
  // completion_processing_pending_ remains true.
  while (true) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (pending_completions_.empty()) {
        // Reset under the same mutex used by Submit(), so a concurrent submission
        // either joins this drain cycle or observes false and posts a new one.
        completion_processing_pending_ = false;
        return;
      }

      // Move the current batch out so workers can continue submitting while the
      // I/O thread invokes connection callbacks without holding the mailbox lock.
      drain_completions_.clear();
      drain_completions_.swap(pending_completions_);
    }

    for (DispatchCompletion &completion : drain_completions_) {
      std::shared_ptr<ServerConnection> connection = completion.target_connection_.lock();
      // The connection may have closed while the worker was processing the RPC.
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
