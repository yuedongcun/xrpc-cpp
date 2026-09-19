/**
 * @file context.h
 * @brief Declares xRPC's single-threaded io_uring event loop.
 *
 * A `UringContext` owns one io_uring ring and drives asynchronous operations
 * on the thread running `Run()`. `Accept`, `AcceptMultishot`, `Recv`,
 * `RecvProvided` and `Send` create deferred, address-stable awaitables.
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
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <liburing.h>
#include <sys/types.h>
#include <utility>

#include "io/uring/buffer_pool.h"
#include "io/uring/stats.h"

namespace xrpc::io {

/** Owns one initialized io_uring instance; its address stays stable for buffer pools. */
class UringInstance final {
 public:
  explicit UringInstance(std::uint32_t entries);
  ~UringInstance();

  UringInstance(const UringInstance &) = delete;
  auto operator=(const UringInstance &) -> UringInstance & = delete;
  UringInstance(UringInstance &&) = delete;
  auto operator=(UringInstance &&) -> UringInstance & = delete;

  [[nodiscard]] auto Get() noexcept -> io_uring & { return ring_; }

 private:
  io_uring ring_{};
};

/** Owns the eventfd used to wake a UringContext from another thread. */
class WakeupEventFd final {
 public:
  WakeupEventFd();
  ~WakeupEventFd();

  WakeupEventFd(const WakeupEventFd &) = delete;
  auto operator=(const WakeupEventFd &) -> WakeupEventFd & = delete;
  WakeupEventFd(WakeupEventFd &&) = delete;
  auto operator=(WakeupEventFd &&) -> WakeupEventFd & = delete;

  [[nodiscard]] auto Get() const noexcept -> int { return fd_; }

 private:
  int fd_ = -1;
};

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
  // False only for an intermediate multishot result that must be awaited again.
  bool is_final_ = true;
  // Identifies the provided-buffer group for submitted RecvProvided completions,
  // including failures where the kernel selected no buffer.
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
  explicit UringContext(std::uint32_t entries = 256,
                        std::optional<UringBufferPoolConfig> buffer_pool_config = std::nullopt);

  ~UringContext();

  UringContext(const UringContext &) = delete;
  auto operator=(const UringContext &) -> UringContext & = delete;

  UringContext(UringContext &&) = delete;
  auto operator=(UringContext &&) -> UringContext & = delete;

  /**
   * @brief Runs the event loop on the calling thread.
   *
   * Can be called at most once. After RequestStop(), continues until every
   * submitted operation reaches its final CQE. An unhandled runtime failure
   * aborts the process.
   */
  void Run() noexcept;

  /**
   * @brief Thread-safely requests event-loop shutdown without waiting.
   *
   * New posted callbacks are rejected, callbacks already queued are drained,
   * and the run thread is awakened. Return from `Run()` confirms shutdown.
   */
  void RequestStop();

  [[nodiscard]] auto Accept(int listen_fd) -> UringAwaitable;

  /**
   * @brief Accepts repeatedly from one listening socket.
   *
   * Reuse the returned awaitable for each accepted socket. It remains alive
   * until a final CQE, including cancellation during listener shutdown.
   */
  [[nodiscard]] auto AcceptMultishot(int listen_fd) -> UringAwaitable;

  [[nodiscard]] auto Recv(int fd, void *buffer, std::size_t len) -> UringAwaitable;

  /**
   * @brief Receives into a buffer selected from this context's registered pool.
   *
   * The context must have been constructed with a UringBufferPoolConfig. A
   * successful result owns the selected buffer through `IoResult::buffer_`.
   */
  [[nodiscard]] auto RecvProvided(int fd) -> UringAwaitable;

  [[nodiscard]] auto Send(int fd, const void *buffer, std::size_t len) -> UringAwaitable;

  void CancelFd(int fd);

  void Post(std::function<void()> fn);

  // Run-thread-only; Post() from other threads. Starting a window resets peaks
  // to current values before copying and increments its ID. Counters never reset.
  [[nodiscard]] auto SnapshotStats(bool start_window = false) -> UringStatsSnapshot;

 private:
  class RunOwnershipGuard;

  friend class UringAwaitable;

  // Resource lifetime and ownership of the Run thread.
  void AcquireRunOwnership();
  void ReleaseRunOwnership();
  void AssertRunThread(std::string_view action) const;
  [[nodiscard]] auto IsRunning() const -> bool;
  [[nodiscard]] static auto CurrentThreadId() -> pid_t;

  // Submission and completion of coroutine I/O.
  [[nodiscard]] auto TryStartOperation(std::unique_ptr<Operation> &operation) -> bool;
  [[nodiscard]] auto AcquireSqe() -> io_uring_sqe *;
  void StageOperation(std::unique_ptr<Operation> operation) noexcept;
  [[nodiscard]] auto TakeOperation(Operation &operation) -> std::unique_ptr<Operation>;
  void SubmitStagedSqes();
  void ProcessCqe(io_uring_cqe *cqe);
  void ProcessAwaitableCqe(Operation &operation, io_uring_cqe *cqe, bool is_final);
  void ProcessCancelCqe(io_uring_cqe *cqe);

  // Cross-thread control enters through Post() and RequestStop().
  void RunPostedCallbacks();
  void StageWakeupPoll();
  void ProcessWakeupCqe(io_uring_cqe *cqe, bool has_more);
  void SignalWakeup() const;
  void ConsumeWakeupSignal() const;

  // --- Context resources: declaration order preserves pool-before-ring destruction. ---
  UringInstance uring_;
  std::unique_ptr<UringProvidedBufferPool> provided_buffer_pool_;
  WakeupEventFd wakeup_;

  // --- Cross-thread control: atomic access, independent of post_mutex_. ---
  // Linux thread ID while running; zero means never run and -1 means finished.
  std::atomic<pid_t> run_thread_id_{0};
  // RequestStop() sets this; Run() observes it and drains outstanding work.
  std::atomic<bool> stop_requested_{false};

  // --- Submission and completion tracking: Run thread only. ---
  // Owns staged and submitted operations until their final CQE is taken for processing.
  std::vector<std::unique_ptr<Operation>> operations_;
  // Prepared SQEs not yet submitted.
  std::size_t staged_sqe_count_ = 0;

  // --- eventfd multishot poll lifecycle: Run thread only. ---
  // True after submitting cancellation until the poll's final CQE.
  bool wakeup_poll_cancel_submitted_ = false;

  // --- Statistics: Run thread only. ---
  UringCounters counters_;
  // Incremented when SnapshotStats(true) starts a new window.
  std::uint64_t stats_window_id_ = 0;

  // --- Cross-thread callback queue: both fields below require post_mutex_. ---
  std::mutex post_mutex_;
  // RequestStop() closes admission under the same lock used by Post().
  bool accepting_posts_ = true;
  // Producers enqueue under the lock; Run() takes a batch and executes unlocked.
  std::deque<std::function<void()>> posted_callbacks_;
};

/**
 * @brief Address-stable result of an I/O submission for one coroutine awaiter.
 *
 * An awaitable owns one unstarted operation. `await_suspend()` transfers that
 * operation to the `UringContext`; each completion delivers its result through
 * the awaitable. A multishot awaitable keeps its operation alive while CQEs
 * carry `IORING_CQE_F_MORE`; its final CQE ends the operation. It supports the
 * synchronous consumer protocol used by multishot accept. Awaitables are
 * immovable so an active operation can safely retain its awaitable's address.
 */
class UringAwaitable final {
 public:
  ~UringAwaitable();

  UringAwaitable(const UringAwaitable &) = delete;
  auto operator=(const UringAwaitable &) -> UringAwaitable & = delete;
  UringAwaitable(UringAwaitable &&) = delete;
  auto operator=(UringAwaitable &&) -> UringAwaitable & = delete;

  auto await_ready() const noexcept -> bool;

  auto await_suspend(std::coroutine_handle<> continuation) -> bool;

  auto await_resume() -> IoResult;

 private:
  friend class UringContext;

  UringAwaitable(UringContext &context, std::unique_ptr<Operation> operation, bool multishot) noexcept;

  UringContext &context_;
  // owned_operation_ owns the Operation before handoff. handed_off_operation_
  // is non-owning and remains valid through the final CQE.
  std::unique_ptr<Operation> owned_operation_;
  Operation *handed_off_operation_ = nullptr;
  std::coroutine_handle<> continuation_;
  IoResult result_;
  bool result_ready_ = false;
};

}  // namespace xrpc::io
