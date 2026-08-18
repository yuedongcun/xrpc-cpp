/**
 * @file context_runtime.h
 * @brief Defines private state shared by UringContext implementation files.
 *
 * This header is intentionally private to `src/io/`. It contains the
 * `UringContext::Runtime` state and internal `Operation` representation needed
 * by the split run-loop, operation, and control implementations. It is not an
 * io module interface.
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <liburing.h>

#include "io/uring_context.h"

namespace xrpc::io {

struct Operation {
  OperationType type_ = OperationType::Nop;
  int fd_ = -1;
  void *buffer_ = nullptr;
  std::size_t length_ = 0;
  __kernel_timespec timeout_{};
  detail::AwaitableState *awaitable_state_ = nullptr;
};

struct UringContext::Runtime final {
  explicit Runtime(std::uint32_t entries);

  ~Runtime();

  void BeginRun();

  void EndRun();

  void AssertRunThread(std::string_view action) const;

  [[nodiscard]] auto IsRunning() const -> bool;

  template <typename Prep>
  void SubmitOperation(std::unique_ptr<Operation> operation, Prep &&prep);

  [[nodiscard]] auto AcquireOperation() -> std::unique_ptr<Operation>;

  void RecycleOperation(std::unique_ptr<Operation> operation);

  void ProcessCqe(io_uring_cqe *cqe);

  static auto MakeCancelledResult(const Operation &operation) -> IoResult;

  static void CompleteAwaitableState(Operation &operation, const IoResult &result);

  static void DetachAwaitableState(Operation &operation) noexcept;

  void SubmitCancelFd(int fd);

  void SubmitCancelOperation(Operation *operation_to_cancel);

  auto TrackTimeoutOperation(Operation &operation) -> bool;

  void UntrackTimeoutOperation(Operation &operation);

  void SubmitCancelPendingTimeouts();

  void EnqueuePosted(std::function<void()> fn);

  void RequestStop();

  void DrainPosted();

  void SubmitWakeupPoll();

  void ProcessWakeupCqe(const Operation &operation, io_uring_cqe *cqe);

  void SignalWakeup() const;

  void DrainWakeupCounter() const;

  [[nodiscard]] static auto MakeErrorMessage(std::string_view action, int error_code) -> std::string;

  [[nodiscard]] static auto CurrentThreadToken() -> const void *;

  io_uring ring_{};

  int wakeup_fd_ = -1;

  std::atomic<const void *> run_thread_token_{nullptr};

  std::atomic<bool> stop_requested_{false};

  std::size_t pending_io_operations_ = 0;

  bool wakeup_poll_pending_ = false;

  bool timeout_cancellations_submitted_ = false;

  std::mutex post_mutex_;

  bool accepting_posts_ = true;

  std::queue<std::function<void()>> posted_callbacks_;

  std::unordered_set<Operation *> pending_timeout_operations_;

  std::vector<std::unique_ptr<Operation>> operation_pool_;
};

}  // namespace xrpc::io
