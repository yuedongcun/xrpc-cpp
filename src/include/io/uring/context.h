/**
 * @file context.h
 * @brief Declares xRPC's single-threaded io_uring event loop.
 *
 * A `UringContext` owns one io_uring ring and drives asynchronous operations
 * on the thread running `Run()`. `Accept`, `Recv`, `RecvProvided`,
 * `RecvProvidedMultishot`, and `Send` create deferred, move-only awaitables.
 * The operation starts when the coroutine suspends and resumes that coroutine
 * with an `IoResult`.
 *
 * `Post()` and `RequestStop()` form the cross-thread control boundary. They wake the
 * event loop safely, but callbacks themselves always execute on the run thread.
 * `CancelFd()` and all awaitable I/O starts must run on that same thread.
 */

#pragma once

#include <atomic>
#include <coroutine>
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

#include <liburing.h>
#include <sys/types.h>
#include <utility>

#include "io/uring/buffer_pool.h"

namespace xrpc::io {

enum class OperationType : std::uint8_t {
  Unknown = 0,
  Accept,
  Recv,
  RecvProvided,
  Send,
};

struct IoResult {
  IoResult() = default;

  IoResult(const IoResult &) = delete;
  auto operator=(const IoResult &) -> IoResult & = delete;

  IoResult(IoResult &&) noexcept = default;
  auto operator=(IoResult &&) noexcept -> IoResult & = default;

  OperationType type_ = OperationType::Unknown;
  int fd_ = -1;
  int result_ = 0;
  int error_code_ = 0;
  std::size_t bytes_transferred_ = 0;
  // True while this multishot operation remains active. Check even on success;
  // false means a final result (also for one-shot and pre-submission cancellation).
  bool has_more_ = false;
  // Identifies the provided-buffer group even when receive fails without a buffer.
  std::uint16_t buffer_group_ = 0;
  UringBuffer buffer_;
};

struct Operation;
class UringAwaitable;

/**
 * @brief Single-threaded io_uring execution context with cross-thread control.
 *
 * `Run()` has one owner. `Post()` and `RequestStop()` may be called concurrently
 * from other threads. Awaitable construction is deferred; `Accept()`, `Recv()`,
 * `RecvProvided()`, and `Send()` start on the run thread when awaited.
 * `CancelFd()` is also a run-thread-only operation.
 */
class UringContext final {
 public:
  explicit UringContext(std::uint32_t entries = 256);

  UringContext(std::uint32_t entries, UringBufferPoolConfig buffer_pool_config);

  ~UringContext();

  UringContext(const UringContext &) = delete;
  auto operator=(const UringContext &) -> UringContext & = delete;

  UringContext(UringContext &&) = delete;
  auto operator=(UringContext &&) -> UringContext & = delete;

  /**
   * @brief Runs the event loop on the calling thread.
   *
   * Returns after stop has been requested and all submitted operations and the
   * wakeup poll have produced their completion events.
   */
  void Run();

  /**
   * @brief Thread-safely requests event-loop shutdown without waiting.
   *
   * New posted callbacks are rejected, callbacks already queued are drained,
   * and the run thread is awakened. Return from `Run()` confirms shutdown.
   */
  void RequestStop();

  [[nodiscard]] auto Accept(int listen_fd) -> UringAwaitable;

  [[nodiscard]] auto Recv(int fd, void *buffer, std::size_t len) -> UringAwaitable;

  /**
   * @brief Receives into a buffer selected from this context's registered pool.
   *
   * The context must have been constructed with a UringBufferPoolConfig. A
   * successful result owns the selected buffer through `IoResult::buffer_`.
   */
  [[nodiscard]] auto RecvProvided(int fd) -> UringAwaitable;

  /**
   * @brief Receives repeatedly into this context's provided-buffer pool.
   *
   * The returned awaitable is reused with `co_await` for each completion. It
   * must remain alive until the final completion and is only suitable for a
   * synchronous consumer on the context's run thread.
   */
  [[nodiscard]] auto RecvProvidedMultishot(int fd) -> UringAwaitable;

  [[nodiscard]] auto Send(int fd, const void *buffer, std::size_t len) -> UringAwaitable;

  void CancelFd(int fd);

  void Post(std::function<void()> fn);

 private:
  friend class UringAwaitable;

  // Resource lifetime and ownership of the Run thread.
  UringContext(std::optional<UringBufferPoolConfig> buffer_pool_config, std::uint32_t entries);
  void BeginRun();
  void EndRun();
  void AssertRunThread(std::string_view action) const;
  [[nodiscard]] auto IsRunning() const -> bool;
  [[nodiscard]] static auto CurrentThreadId() -> pid_t;
  [[nodiscard]] static auto MakeErrorMessage(std::string_view action, int error_code) -> std::string;

  // Submission and completion of coroutine I/O.
  [[nodiscard]] auto TryStartOperation(std::unique_ptr<Operation> &operation, std::coroutine_handle<> continuation)
      -> bool;
  [[nodiscard]] auto AcquireSqe() -> io_uring_sqe *;
  void SubmitPreparedOperation(std::unique_ptr<Operation> operation, bool counts_as_pending_io) noexcept;
  void FlushSubmissionBatch();
  void ProcessCqe(io_uring_cqe *cqe);
  void ProcessAwaitableCqe(Operation &operation, io_uring_cqe *cqe);
  void ProcessCancelCqe(io_uring_cqe *cqe);
  static auto MakeCancelledResult(const Operation &operation) -> IoResult;
  void SubmitCancelFd(int fd);

  // Cross-thread control enters through Post() and RequestStop().
  void DrainPosted();
  void SubmitWakeupPoll();
  void ProcessWakeupCqe(io_uring_cqe *cqe);
  void SignalWakeup() const;
  void DrainWakeupCounter() const;

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

/**
 * @brief Move-only result of an I/O submission for one coroutine awaiter.
 *
 * An awaitable owns one unstarted operation. `await_suspend()` transfers that
 * operation to the `UringContext`; `await_resume()` moves the completed result
 * out of the operation. A multishot awaitable keeps its operation alive while
 * CQEs carry `IORING_CQE_F_MORE`; its final CQE ends the operation.
 */
class UringAwaitable final {
 public:
  ~UringAwaitable();

  UringAwaitable(const UringAwaitable &) = delete;
  auto operator=(const UringAwaitable &) -> UringAwaitable & = delete;

  UringAwaitable(UringAwaitable &&other) noexcept;
  auto operator=(UringAwaitable &&other) noexcept -> UringAwaitable &;

  auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> continuation) -> bool;

  auto await_resume() -> IoResult;

 private:
  explicit UringAwaitable(UringContext &context, std::unique_ptr<Operation> operation, bool multishot = false) noexcept;

  friend class UringContext;

  UringContext *context_ = nullptr;
  std::unique_ptr<Operation> unstarted_operation_;
  Operation *active_operation_ = nullptr;
  IoResult result_;
  bool multishot_ = false;
  bool result_ready_ = false;
};

}  // namespace xrpc::io
