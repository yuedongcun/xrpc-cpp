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

#include "io/operation.h"
#include "io/uring_context.h"

namespace xrpc::io {

/**
 * @brief Private io_uring runtime owned by `UringContext`.
 *
 * The runtime has a strict threading contract:
 * - `Run()` owns submission queue and completion queue processing.
 * - Other threads communicate through `Post()` and `Stop()`.
 * - Cross-thread callbacks wake the run thread through an `eventfd`.
 * - Direct SQE submission outside the run thread is rejected by `AssertRunThread()`.
 */
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

  /** @brief Kernel submission/completion queue. */
  io_uring ring_{};

  /** @brief Eventfd used to wake the run thread for posted callbacks and shutdown. */
  int wakeup_fd_ = -1;

  /** @brief Token identifying the thread currently executing `Run()`. */
  std::atomic<const void *> run_thread_token_{nullptr};

  /** @brief Set once shutdown has been requested. */
  std::atomic<bool> stop_requested_{false};

  /** @brief Number of user-visible and cancellation operations waiting for CQEs. */
  std::size_t pending_io_operations_ = 0;

  /** @brief true while the internal eventfd poll SQE is in flight. */
  bool wakeup_poll_pending_ = false;

  /** @brief Guards against submitting duplicate shutdown cancellations for timeout SQEs. */
  bool timeout_cancellations_submitted_ = false;

  /** @brief Protects cross-thread callback queue and accepting-posts state. */
  std::mutex post_mutex_;

  /** @brief false after stop so later `Post()` calls become no-ops. */
  bool accepting_posts_ = true;

  /** @brief Callbacks waiting to run on the event-loop thread. */
  std::queue<std::function<void()>> posted_callbacks_;

  /** @brief Timeout operations that should be canceled when shutdown starts. */
  std::unordered_set<Operation *> pending_timeout_operations_;

  /** @brief Reusable operation objects recovered after CQE processing. */
  std::vector<std::unique_ptr<Operation>> operation_pool_;
};

}  // namespace xrpc::io
