/**
 * @file context_runtime.h
 * @brief Defines the private runtime state of `UringContext`.
 *
 * This header contains the internal `Runtime` and `Operation` definitions
 * shared by the run-loop, operation, and control implementation files.
 * It is used only inside the io_uring implementation.
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

#include <liburing.h>

#include "io/uring_context.h"

namespace xrpc::io {

struct Operation {
  enum class CompletionCategory : std::uint8_t {
    Awaitable,
    Cancel,
    Wakeup,
  };

  OperationType type_ = OperationType::Unknown;
  CompletionCategory completion_category_ = CompletionCategory::Awaitable;
  int fd_ = -1;
  void *buffer_ = nullptr;
  std::size_t length_ = 0;
  __kernel_timespec timeout_{};
  std::weak_ptr<detail::AwaitableState> awaitable_state_;
};

struct UringContext::Runtime final {
  explicit Runtime(std::uint32_t entries);

  ~Runtime();

  void BeginRun();

  void EndRun();

  void AssertRunThread(std::string_view action) const;

  [[nodiscard]] auto IsRunning() const -> bool;

  template <typename Prep>
  void SubmitAwaitableOperation(std::unique_ptr<Operation> operation, Prep &&prep);

  void ProcessCqe(io_uring_cqe *cqe);

  void ProcessAwaitableCqe(Operation &operation, io_uring_cqe *cqe);

  void ProcessCancelCqe(io_uring_cqe *cqe);

  static auto MakeCancelledResult(const Operation &operation) -> IoResult;

  static void CompleteAwaitableState(Operation &operation, const IoResult &result);

  void SubmitCancelFd(int fd);

  void SubmitCancelOperation(Operation *operation_to_cancel);

  auto TrackTimeoutOperation(Operation &operation) -> bool;

  void UntrackTimeoutOperation(Operation &operation);

  void SubmitCancelPendingTimeouts();

  void EnqueuePosted(std::function<void()> fn);

  void RequestStop();

  void DrainPosted();

  void SubmitWakeupPoll();

  void ProcessWakeupCqe(io_uring_cqe *cqe);

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
};

}  // namespace xrpc::io
