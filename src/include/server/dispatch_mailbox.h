#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "io/uring_context.h"

namespace xrpc {

class TcpConnection;

/**
 * @brief Method-dispatch completion produced by a worker thread.
 *
 * Design note:
 * - Ownership: `TcpConnection` holds a shared mailbox; worker completions hold weak connection references so closed
 *   connections can disappear safely.
 * - Threading: workers call `Submit()`, which posts one drain callback onto the `UringContext` thread; only that drain
 *   touches `TcpConnection` state.
 * - Batching: multiple worker completions are drained together to avoid posting one event-loop callback per RPC.
 */
struct DispatchCompletion {
  /** @brief Weak connection reference; expired means the connection closed before completion. */
  std::weak_ptr<TcpConnection> target_connection_;

  /** @brief Encoded response bytes ready to enqueue on the connection. */
  std::string response_bytes_;

  /** @brief Number of logical dispatch jobs completed by this worker task. */
  std::size_t completed_jobs_ = 1;

  /** @brief True when response encoding failed and the connection should account failure without writing bytes. */
  bool encode_failed_ = false;
};

/**
 * @brief Cross-thread mailbox from worker threads back to one io_uring context.
 *
 * The mailbox coalesces many worker completions into one posted drain callback. All connection state changes happen in
 * `DrainOnContext()` on the event-loop thread.
 */
class DispatchMailbox final : public std::enable_shared_from_this<DispatchMailbox> {
 public:
  /** @brief Creates a completion mailbox bound to one `UringContext`. */
  explicit DispatchMailbox(io::UringContext &context);

  DispatchMailbox(const DispatchMailbox &) = delete;
  auto operator=(const DispatchMailbox &) -> DispatchMailbox & = delete;

  DispatchMailbox(DispatchMailbox &&) = delete;
  auto operator=(DispatchMailbox &&) -> DispatchMailbox & = delete;

  /** @brief Submits a worker completion and posts a drain callback if needed. */
  void Submit(DispatchCompletion completion);

  /** @brief Prevents future drain callbacks from touching the context during shutdown. */
  void Disable();

 private:
  /** @brief Drains pending completions on the bound `UringContext` thread. */
  void DrainOnContext();

  io::UringContext *context_;
  std::mutex mutex_;
  std::vector<DispatchCompletion> pending_completions_;
  std::vector<DispatchCompletion> drain_completions_;
  bool drain_posted_ = false;
};

}  // namespace xrpc
