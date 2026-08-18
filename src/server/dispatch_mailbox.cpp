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
  if (drain_posted_) {
    return;
  }

  drain_posted_ = true;
  std::weak_ptr<DispatchMailbox> weak_mailbox = weak_from_this();
  context_->Post([weak_mailbox]() -> void {
    std::shared_ptr<DispatchMailbox> mailbox = weak_mailbox.lock();
    if (!mailbox) {
      return;
    }
    mailbox->DrainOnContext();
  });
}

void DispatchMailbox::Disable() {
  std::lock_guard<std::mutex> lock(mutex_);
  context_ = nullptr;
  pending_completions_.clear();
  drain_completions_.clear();
  drain_posted_ = false;
}

void DispatchMailbox::DrainOnContext() {
  while (true) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (pending_completions_.empty()) {
        drain_posted_ = false;
        return;
      }

      drain_completions_.clear();
      drain_completions_.swap(pending_completions_);
    }

    for (DispatchCompletion &completion : drain_completions_) {
      std::shared_ptr<ServerConnection> connection = completion.target_connection_.lock();
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
