/**
 * @file dispatch_mailbox.h
 * @brief Defines the worker-to-connection-I/O-thread completion handoff.
 *
 * DispatchMailbox collects RPC dispatch completions produced concurrently by
 * worker threads and delivers them back to ServerConnection objects on the
 * owning UringContext thread.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "io/uring_context.h"

namespace xrpc {

class ServerConnection;

/**
 * @brief Result of worker-side dispatch processing returned to the connection
 * I/O thread.
 */
struct DispatchCompletion {
  // Weak ownership allows the connection to disappear before worker completion
  // is drained without extending its lifetime.
  std::weak_ptr<ServerConnection> target_connection_;

  std::string response_bytes_;

  // Number of logical RPC jobs completed by this dispatch result.
  std::size_t completed_jobs_ = 1;

  // Indicates that response encoding failed and no response bytes are usable.
  bool encode_failed_ = false;
};

/**
 * @brief Thread-safe mailbox for returning worker completions to the owning
 * connection I/O thread.
 *
 * Worker threads only enqueue completion data. `ProcessCompletionsOnContext()` runs on the
 * owning `UringContext` thread and is the only path that invokes
 * `ServerConnection` completion methods.
 *
 * Multiple submissions are coalesced into one posted processing callback. While completion processing is
 * already pending or running, new completions are queued and consumed by that
 * processing cycle instead of posting another callback.
 */
class DispatchMailbox final : public std::enable_shared_from_this<DispatchMailbox> {
 public:
  explicit DispatchMailbox(io::UringContext &context);

  DispatchMailbox(const DispatchMailbox &) = delete;
  auto operator=(const DispatchMailbox &) -> DispatchMailbox & = delete;

  DispatchMailbox(DispatchMailbox &&) = delete;
  auto operator=(DispatchMailbox &&) -> DispatchMailbox & = delete;

  /**
   * @brief Queues one worker completion for delivery on the owning I/O thread.
   *
   * Thread-safe and callable concurrently by worker threads. Posting is
   * coalesced so multiple submissions may share one context callback.
   */
  void Submit(DispatchCompletion completion);

  /**
   * @brief Disables further completion delivery during owner-thread shutdown.
   *
   * Pending completions are discarded and later `Submit()` calls become no-ops.
   * This must run only after the server no longer requires those completions.
   */
  void Disable();

 private:
  // UringContext-thread-only.
  void ProcessCompletionsOnContext();

  io::UringContext *context_;

  std::mutex mutex_;

  // True while one posted or currently running callback is responsible for
  // pending completions. Protected by mutex_.
  bool completion_processing_pending_ = false;

  // Completions concurrently appended by worker threads.
  std::vector<DispatchCompletion> pending_completions_;

  // Batch currently being processed by the owning I/O thread.
  std::vector<DispatchCompletion> drain_completions_;
};

}  // namespace xrpc
