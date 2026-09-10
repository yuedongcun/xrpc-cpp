/**
 * @file uring_context_runtime.h
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
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <vector>

#include <sys/types.h>

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
  IoResult result_;
  std::coroutine_handle<> continuation_;
};

struct UringContext::Runtime final {
  explicit Runtime(std::uint32_t entries, std::optional<UringBufferPoolConfig> buffer_pool_config = std::nullopt);

  ~Runtime();

  void BeginRun();

  void EndRun();

  void AssertRunThread(std::string_view action) const;

  [[nodiscard]] auto IsRunning() const -> bool;

  [[nodiscard]] auto TryStartAwaitableOperation(std::unique_ptr<Operation> &operation,
                                                std::coroutine_handle<> continuation) -> bool;

  [[nodiscard]] auto AcquireSqe() -> io_uring_sqe *;

  void SubmitPreparedOperation(std::unique_ptr<Operation> operation, bool counts_as_pending_io) noexcept;

  void FlushSubmissionBatch();

  void ProcessCqe(io_uring_cqe *cqe);

  void ProcessAwaitableCqe(Operation &operation, io_uring_cqe *cqe);

  void ProcessCancelCqe(io_uring_cqe *cqe);

  static auto MakeCancelledResult(const Operation &operation) -> IoResult;

  void SubmitCancelFd(int fd);

  void EnqueuePosted(std::function<void()> fn);

  void RequestStop();

  void DrainPosted();

  void SubmitWakeupPoll();

  void ProcessWakeupCqe(io_uring_cqe *cqe);

  void SignalWakeup() const;

  void DrainWakeupCounter() const;

  [[nodiscard]] static auto MakeErrorMessage(std::string_view action, int error_code) -> std::string;

  [[nodiscard]] static auto CurrentThreadId() -> pid_t;

  io_uring ring_{};

  std::unique_ptr<UringProvidedBufferPool> provided_buffer_pool_;

  int wakeup_fd_ = -1;

  std::atomic<pid_t> run_thread_id_{0};

  std::atomic<bool> stop_requested_{false};

  std::size_t pending_io_operations_ = 0;

  std::vector<std::unique_ptr<Operation>> staged_operations_;

  bool wakeup_poll_pending_ = false;

  std::mutex post_mutex_;

  bool accepting_posts_ = true;

  std::queue<std::function<void()>> posted_callbacks_;
};

}  // namespace xrpc::io
