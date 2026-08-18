/** @file dispatch_mailbox.h @brief Declares worker-to-I/O-loop completion delivery. */

#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "io/uring_context.h"

namespace xrpc {

class ServerConnection;

struct DispatchCompletion {
  std::weak_ptr<ServerConnection> target_connection_;

  std::string response_bytes_;

  std::size_t completed_jobs_ = 1;

  bool encode_failed_ = false;
};

/**
 * @brief Thread-safe mailbox for worker-to-I/O-loop dispatch completions.
 *
 * Workers may call `Submit()` concurrently. The mailbox batches those
 * completions and drains them on the owning `UringContext` thread; it is the
 * only worker path that touches a `ServerConnection` after handler execution.
 */
class DispatchMailbox final : public std::enable_shared_from_this<DispatchMailbox> {
 public:
  explicit DispatchMailbox(io::UringContext &context);

  DispatchMailbox(const DispatchMailbox &) = delete;
  auto operator=(const DispatchMailbox &) -> DispatchMailbox & = delete;

  DispatchMailbox(DispatchMailbox &&) = delete;
  auto operator=(DispatchMailbox &&) -> DispatchMailbox & = delete;

  void Submit(DispatchCompletion completion);

  // Owner-thread shutdown operation, called after completion draining.
  void Disable();

 private:
  // UringContext-thread-only.
  void DrainOnContext();

  io::UringContext *context_;
  std::mutex mutex_;
  std::vector<DispatchCompletion> pending_completions_;
  std::vector<DispatchCompletion> drain_completions_;
  bool drain_posted_ = false;
};

}  // namespace xrpc
