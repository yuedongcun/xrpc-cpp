/**
 * @file dispatch_mailbox.cpp
 * @brief Implements batched worker-to-I/O-thread completion delivery.
 */

#include "server/dispatch_mailbox.h"

#include <algorithm>
#include <utility>

#include "common/latency_trace.h"
#include "server/server_connection.h"

namespace xrpc {

DispatchMailbox::DispatchMailbox(io::UringContext &context) : context_(&context) {}

void DispatchMailbox::Submit(DispatchCompletion completion) {
#ifdef XRPC_ENABLE_LATENCY_TRACE
  const std::vector<std::uint64_t> trace_request_ids = completion.trace_request_ids_;
  for (const std::uint64_t request_id : completion.trace_request_ids_) {
    diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::MailboxSubmit, request_id,
                                    static_cast<std::uint32_t>(completion.completed_jobs_));
  }
#endif
  std::lock_guard<std::mutex> lock(mutex_);
#ifdef XRPC_ENABLE_LATENCY_TRACE
  for (const std::uint64_t request_id : trace_request_ids) {
    diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::MailboxLockAcquired, request_id);
  }
#endif
  if (context_ == nullptr) {
    return;
  }

  pending_completions_.push_back(std::move(completion));
#ifdef XRPC_ENABLE_LATENCY_TRACE
  for (const std::uint64_t request_id : trace_request_ids) {
    diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::MailboxQueued, request_id);
  }
#endif
  // One outstanding processing callback owns responsibility for both the
  // current batch and completions that arrive before it finishes.
  if (completion_processing_pending_) {
    return;
  }

  completion_processing_pending_ = true;
  // The owning ConnectionIoLoop joins its run thread before destroying this
  // mailbox, so queued callbacks cannot outlive the mailbox.
  context_->Post([this]() -> void { ProcessCompletionsOnContext(); });
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

#ifdef XRPC_ENABLE_LATENCY_TRACE
    const std::uint64_t callback_begin_at_ns = diagnostics::LatencyTraceEnabled() ? diagnostics::LatencyNowNs() : 0;
    for (const DispatchCompletion &completion : drain_completions_) {
      for (const std::uint64_t request_id : completion.trace_request_ids_) {
        diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::MailboxCallbackBegin, request_id, 0,
                                        callback_begin_at_ns);
      }
    }
#endif
    for (DispatchCompletion &completion : drain_completions_) {
#ifdef XRPC_ENABLE_LATENCY_TRACE
      const std::uint32_t drain_size =
          static_cast<std::uint32_t>(std::min<std::size_t>(drain_completions_.size(), UINT32_MAX));
      for (const std::uint64_t request_id : completion.trace_request_ids_) {
        diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::MailboxDrain, request_id, drain_size);
      }
#endif
      std::shared_ptr<ServerConnection> connection = completion.target_connection_.lock();
      // The connection may have closed while the worker was processing the RPC.
      if (!connection) {
        continue;
      }
      if (completion.encode_failed_) {
        connection->OnDispatchEncodeFailure(completion.completed_jobs_);
      } else {
#ifdef XRPC_ENABLE_LATENCY_TRACE
        for (const std::uint64_t request_id : completion.trace_request_ids_) {
          diagnostics::RecordLatencyTrace(diagnostics::LatencyStage::WriteEnqueue, request_id);
        }
#endif
        connection->OnEncodedDispatchComplete(std::move(completion.response_bytes_), completion.completed_jobs_
#ifdef XRPC_ENABLE_LATENCY_TRACE
                                              ,
                                              std::move(completion.trace_request_ids_)
#endif
        );
      }
    }
    drain_completions_.clear();
  }
}

}  // namespace xrpc
