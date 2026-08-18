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

class DispatchMailbox final : public std::enable_shared_from_this<DispatchMailbox> {
 public:
  explicit DispatchMailbox(io::UringContext &context);

  DispatchMailbox(const DispatchMailbox &) = delete;
  auto operator=(const DispatchMailbox &) -> DispatchMailbox & = delete;

  DispatchMailbox(DispatchMailbox &&) = delete;
  auto operator=(DispatchMailbox &&) -> DispatchMailbox & = delete;

  void Submit(DispatchCompletion completion);

  void Disable();

 private:
  void DrainOnContext();

  io::UringContext *context_;
  std::mutex mutex_;
  std::vector<DispatchCompletion> pending_completions_;
  std::vector<DispatchCompletion> drain_completions_;
  bool drain_posted_ = false;
};

}  // namespace xrpc
