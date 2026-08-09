#include "io/uring_context.h"

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include <liburing.h>

#include "rpc/xrpc_exception.h"

#include "io/operation.h"

namespace xrpc::io {
namespace {

/**
 * @brief Builds a diagnostic message from an operation name and errno value.
 *
 * @param action io_uring or eventfd operation name.
 * @param error_code Positive errno value, or zero when no system error applies.
 * @return Human-readable diagnostic message.
 */
auto MakeErrorMessage(std::string_view action, int error_code) -> std::string {
  std::string message(action);
  message.append(" failed");
  if (error_code != 0) {
    message.append(": ");
    message.append(std::error_code(error_code, std::generic_category()).message());
  }
  return message;
}

/**
 * @brief Returns a stable token for the current thread.
 *
 * @return Address of a thread-local object unique to the current thread.
 */
auto CurrentThreadToken() -> const void * {
  static thread_local const char token = 0;
  return &token;
}

/**
 * @brief Atomically records the maximum observed value.
 *
 * @param maximum Relaxed diagnostic counter to update.
 * @param value Candidate maximum.
 */
void ObserveMaximum(std::atomic<std::uint64_t> &maximum, std::size_t value) {
  std::uint64_t observed = maximum.load(std::memory_order_relaxed);
  const auto candidate = static_cast<std::uint64_t>(value);
  while (observed < candidate &&
         !maximum.compare_exchange_weak(observed, candidate, std::memory_order_relaxed, std::memory_order_relaxed)) {
  }
}

/**
 * @brief Converts a C++ timeout duration to the kernel timeout structure used by io_uring.
 *
 * @param timeout Requested timeout. Negative durations are clamped to zero.
 * @return Kernel timespec value owned by an `Operation`.
 */
auto MakeKernelTimespec(std::chrono::nanoseconds timeout) -> __kernel_timespec {
  if (timeout < std::chrono::nanoseconds::zero()) {
    timeout = std::chrono::nanoseconds::zero();
  }

  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
  const auto nanoseconds = timeout - seconds;

  __kernel_timespec timespec{};
  timespec.tv_sec = static_cast<decltype(timespec.tv_sec)>(seconds.count());
  timespec.tv_nsec = static_cast<decltype(timespec.tv_nsec)>(nanoseconds.count());
  return timespec;
}

}  // namespace

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
  /**
   * @brief Initializes the kernel ring and eventfd wakeup channel.
   *
   * @param entries Submission/completion queue depth.
   * @throws InternalException when io_uring or eventfd initialization fails.
   */
  explicit Runtime(std::uint32_t entries) {
    const int ret = io_uring_queue_init(entries, &ring_, 0);
    if (ret < 0) {
      throw InternalException(MakeErrorMessage("io_uring_queue_init", -ret));
    }

    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ < 0) {
      const int error_code = errno;
      io_uring_queue_exit(&ring_);
      throw InternalException(MakeErrorMessage("eventfd", error_code));
    }
  }

  /** @brief Releases eventfd and io_uring kernel resources. */
  ~Runtime() {
    if (wakeup_fd_ >= 0) {
      (void)::close(wakeup_fd_);
    }
    io_uring_queue_exit(&ring_);
  }

  /**
   * @brief Marks the current thread as the unique run thread.
   *
   * @throws LifecycleException when `Run()` is already active.
   */
  void BeginRun() {
    const void *expected = nullptr;
    if (!run_thread_token_.compare_exchange_strong(expected, CurrentThreadToken(), std::memory_order_acq_rel)) {
      throw LifecycleException("UringContext::Run is not reentrant");
    }
  }

  /** @brief Clears the run-thread token after `Run()` exits. */
  void EndRun() { run_thread_token_.store(nullptr, std::memory_order_release); }

  /**
   * @brief Verifies that an action is executing on the run thread.
   *
   * @param action Action name used in the lifecycle error message.
   * @throws LifecycleException when called from any other thread.
   */
  void AssertRunThread(std::string_view action) const {
    if (run_thread_token_.load(std::memory_order_acquire) != CurrentThreadToken()) {
      throw LifecycleException(std::string(action) + " must run on the UringContext thread");
    }
  }

  /** @return true while a thread is inside `UringContext::Run()`. */
  [[nodiscard]] auto IsRunning() const -> bool { return run_thread_token_.load(std::memory_order_acquire) != nullptr; }

  /**
   * @brief Submits one user-visible operation to io_uring.
   *
   * Ownership of `operation` transfers to SQE user data after successful submit and is recovered in
   * `ProcessCqe()`. If shutdown has started, the awaitable completes synchronously as canceled.
   *
   * @tparam Prep Callable that prepares the SQE.
   * @param operation Operation state bound to the caller's awaitable.
   * @param prep SQE preparation callback.
   */
  template <typename Prep>
  void SubmitOperation(std::unique_ptr<Operation> operation, Prep &&prep) {
    bool tracked_timeout = false;
    try {
      AssertRunThread("io_uring submission");
      if (stop_requested_.load(std::memory_order_acquire)) {
        // After Stop(), new awaitables complete synchronously as canceled
        // instead of leaking an operation that will never be submitted.
        CompleteAwaitableState(*operation, MakeCancelledResult(*operation));
        RecycleOperation(std::move(operation));
        return;
      }

      tracked_timeout = TrackTimeoutOperation(*operation);
      io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
      if (sqe == nullptr) {
        throw InternalException("io_uring_get_sqe failed");
      }

      prep(*sqe);

      Operation *raw_operation = operation.get();
      io_uring_sqe_set_data(sqe, raw_operation);

      const int ret = io_uring_submit(&ring_);
      if (ret < 0) {
        throw InternalException(MakeErrorMessage("io_uring_submit", -ret));
      }

      ++pending_io_operations_;
      // Ownership transfers to the kernel-visible CQE user data. ProcessCqe()
      // rebuilds the unique_ptr and returns the Operation to the pool.
      [[maybe_unused]] Operation *released = operation.release();
    } catch (...) {
      if (operation) {
        if (tracked_timeout) {
          pending_timeout_operations_.erase(operation.get());
        }
        DetachAwaitableState(*operation);
      }
      throw;
    }
  }

  /**
   * @brief Gets an operation object from the pool or allocates a new one.
   *
   * @return Empty operation ready to be populated by a submission API.
   */
  [[nodiscard]] auto AcquireOperation() -> std::unique_ptr<Operation> {
    if (operation_pool_.empty()) {
      return std::make_unique<Operation>();
    }

    std::unique_ptr<Operation> operation = std::move(operation_pool_.back());
    operation_pool_.pop_back();
    return operation;
  }

  /**
   * @brief Resets and returns an operation object to the pool.
   *
   * @param operation Operation whose awaitable link has already been completed or detached.
   */
  void RecycleOperation(std::unique_ptr<Operation> operation) {
    *operation = Operation{};
    operation_pool_.push_back(std::move(operation));
  }

  /**
   * @brief Processes one completion queue entry.
   *
   * @param cqe Completion queue entry returned by the kernel.
   * @throws InternalException when completion accounting or cancellation status is inconsistent.
   */
  void ProcessCqe(io_uring_cqe *cqe) {
    auto *raw_operation = static_cast<Operation *>(io_uring_cqe_get_data(cqe));
    if (raw_operation == nullptr) {
      io_uring_cqe_seen(&ring_, cqe);
      return;
    }

    std::unique_ptr<Operation> operation(raw_operation);
    if (operation->type_ == OperationType::Wakeup) {
      // Wakeups are event-loop control messages, not user-visible I/O, so they
      // are excluded from pending_io_operations_ accounting.
      ProcessWakeupCqe(*operation, cqe);
      RecycleOperation(std::move(operation));
      return;
    }

    if (pending_io_operations_ == 0) {
      io_uring_cqe_seen(&ring_, cqe);
      throw InternalException("io_uring completion without a pending operation");
    }
    --pending_io_operations_;
    UntrackTimeoutOperation(*operation);

    IoResult result;
    result.type_ = operation->type_;
    result.fd_ = operation->fd_;
    result.result_ = cqe->res;
    result.error_code_ = cqe->res < 0 ? -cqe->res : 0;
    if (operation->type_ == OperationType::Timeout && result.error_code_ == ETIME) {
      result.result_ = 0;
      result.error_code_ = 0;
    }
    if (operation->type_ == OperationType::Recv || operation->type_ == OperationType::Send) {
      result.bytes_transferred_ = cqe->res > 0 ? static_cast<std::size_t>(cqe->res) : 0;
    }

    io_uring_cqe_seen(&ring_, cqe);

    if (operation->type_ == OperationType::Cancel) {
      // Cancel completions are bookkeeping. Linux may report that the target
      // already completed, was already canceled, or no longer exists.
      if (result.result_ < 0 && result.error_code_ != ENOENT && result.error_code_ != EALREADY &&
          result.error_code_ != ECANCELED) {
        throw InternalException(MakeErrorMessage("io_uring cancel", result.error_code_));
      }
      RecycleOperation(std::move(operation));
      return;
    }

    if (operation->awaitable_state_) {
      CompleteAwaitableState(*operation, result);
    }
    RecycleOperation(std::move(operation));
  }

  /**
   * @brief Builds the completion result returned for submissions after stop.
   *
   * @param operation Operation being canceled before kernel submission.
   * @return ECANCELED completion result.
   */
  static auto MakeCancelledResult(const Operation &operation) -> IoResult {
    IoResult result;
    result.type_ = operation.type_;
    result.fd_ = operation.fd_;
    result.result_ = -ECANCELED;
    result.error_code_ = ECANCELED;
    result.bytes_transferred_ = 0;
    return result;
  }

  /**
   * @brief Stores a completion result and resumes the awaiting coroutine.
   *
   * @param operation Operation whose awaitable state should be completed.
   * @param result Completion result to expose through `await_resume()`.
   */
  static void CompleteAwaitableState(Operation &operation, const IoResult &result) {
    detail::AwaitableState *state = operation.awaitable_state_;
    if (state == nullptr) {
      return;
    }

    operation.awaitable_state_ = nullptr;
    state->operation_ = nullptr;
    state->result_ = result;
    state->ready_ = true;
    if (state->continuation_) {
      state->continuation_.resume();
    }
  }

  /**
   * @brief Detaches an awaitable from an operation after a submission failure.
   *
   * @param operation Operation whose awaitable link should be cleared.
   */
  static void DetachAwaitableState(Operation &operation) noexcept {
    detail::AwaitableState *state = operation.awaitable_state_;
    if (state == nullptr) {
      return;
    }
    operation.awaitable_state_ = nullptr;
    state->operation_ = nullptr;
  }

  /**
   * @brief Submits cancellation requests for all operations associated with a file descriptor.
   *
   * @param fd File descriptor whose pending operations should be canceled.
   */
  void SubmitCancelFd(int fd) {
    AssertRunThread("UringContext::CancelFd");

    auto operation = AcquireOperation();
    operation->type_ = OperationType::Cancel;
    operation->fd_ = fd;

    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      throw InternalException("io_uring_get_sqe failed");
    }

    io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
    Operation *raw_operation = operation.get();
    io_uring_sqe_set_data(sqe, raw_operation);

    const int ret = io_uring_submit(&ring_);
    if (ret < 0) {
      throw InternalException(MakeErrorMessage("io_uring_submit", -ret));
    }

    ++pending_io_operations_;
    [[maybe_unused]] Operation *released = operation.release();
  }

  /**
   * @brief Submits a cancellation request for a specific timeout operation.
   *
   * @param operation_to_cancel Operation pointer stored in the target timeout SQE user data.
   */
  void SubmitCancelOperation(Operation *operation_to_cancel) {
    AssertRunThread("UringContext timeout cancellation");

    auto operation = AcquireOperation();
    operation->type_ = OperationType::Cancel;

    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      throw InternalException("io_uring_get_sqe failed");
    }

    io_uring_prep_cancel(sqe, operation_to_cancel, 0);
    Operation *raw_operation = operation.get();
    io_uring_sqe_set_data(sqe, raw_operation);

    const int ret = io_uring_submit(&ring_);
    if (ret < 0) {
      throw InternalException(MakeErrorMessage("io_uring_submit", -ret));
    }

    ++pending_io_operations_;
    [[maybe_unused]] Operation *released = operation.release();
  }

  /**
   * @brief Adds a timeout operation to shutdown cancellation tracking.
   *
   * @param operation Operation about to be submitted.
   * @return true when the operation was tracked as a timeout.
   */
  auto TrackTimeoutOperation(Operation &operation) -> bool {
    if (operation.type_ != OperationType::Timeout) {
      return false;
    }
    pending_timeout_operations_.insert(&operation);
    return true;
  }

  /**
   * @brief Removes a timeout operation from shutdown cancellation tracking.
   *
   * @param operation Operation that has completed or failed before completion.
   */
  void UntrackTimeoutOperation(Operation &operation) {
    if (operation.type_ == OperationType::Timeout) {
      pending_timeout_operations_.erase(&operation);
    }
  }

  /**
   * @brief Cancels all pending timeout operations once shutdown starts.
   *
   * Timeout SQEs can otherwise keep `Run()` alive until their deadlines expire.
   */
  void SubmitCancelPendingTimeouts() {
    if (timeout_cancellations_submitted_) {
      return;
    }

    timeout_cancellations_submitted_ = true;
    for (Operation *operation : pending_timeout_operations_) {
      SubmitCancelOperation(operation);
    }
  }

  /**
   * @brief Enqueues a cross-thread callback for execution on the run thread.
   *
   * @param fn Callback to execute from `DrainPosted()`.
   */
  void EnqueuePosted(std::function<void()> fn) {
    bool should_wake = false;
    {
      std::lock_guard<std::mutex> lock(post_mutex_);
      if (!accepting_posts_) {
        return;
      }
      should_wake = posted_callbacks_.empty();
      posted_callbacks_.emplace(std::move(fn));
      posted_callbacks_count_.fetch_add(1, std::memory_order_relaxed);
      ObserveMaximum(max_observed_post_queue_depth_, posted_callbacks_.size());
    }

    if (should_wake) {
      // One eventfd write is enough to make the Run thread drain the whole
      // posted queue; additional callbacks can piggyback on that wakeup.
      SignalWakeup();
    }
  }

  /**
   * @brief Requests event-loop shutdown and wakes the run thread.
   */
  void RequestStop() {
    {
      std::lock_guard<std::mutex> lock(post_mutex_);
      if (!accepting_posts_) {
        return;
      }
      accepting_posts_ = false;
      stop_requested_.store(true, std::memory_order_release);
    }

    SignalWakeup();
  }

  /**
   * @brief Executes all callbacks currently posted to the run thread.
   *
   * Callbacks are swapped out under the mutex and run without holding it so callbacks may safely
   * post follow-up work or request shutdown.
   */
  void DrainPosted() {
    std::queue<std::function<void()>> callbacks;
    std::size_t batch_size = 0;
    {
      std::lock_guard<std::mutex> lock(post_mutex_);
      // Swap under the mutex, then execute callbacks without holding it so
      // callbacks can safely post more work or call Stop().
      batch_size = posted_callbacks_.size();
      std::swap(callbacks, posted_callbacks_);
    }
    if (batch_size > 0) {
      drained_callbacks_count_.fetch_add(batch_size, std::memory_order_relaxed);
      drain_batches_.fetch_add(1, std::memory_order_relaxed);
    }

    while (!callbacks.empty()) {
      std::function<void()> &callback = callbacks.front();
      callback();
      callbacks.pop();
    }
  }

  /**
   * @brief Submits the internal eventfd poll used to wake the run thread.
   *
   * There is exactly one wakeup poll in flight while the loop is running.
   */
  void SubmitWakeupPoll() {
    AssertRunThread("wakeup poll submission");
    if (wakeup_poll_pending_) {
      throw InternalException("eventfd wakeup poll already pending");
    }

    auto operation = AcquireOperation();
    operation->type_ = OperationType::Wakeup;
    operation->fd_ = wakeup_fd_;

    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      throw InternalException("io_uring_get_sqe failed");
    }

    io_uring_prep_poll_add(sqe, wakeup_fd_, POLLIN);
    Operation *raw_operation = operation.get();
    io_uring_sqe_set_data(sqe, raw_operation);

    const int ret = io_uring_submit(&ring_);
    if (ret < 0) {
      throw InternalException(MakeErrorMessage("io_uring_submit", -ret));
    }

    wakeup_poll_pending_ = true;
    [[maybe_unused]] Operation *released = operation.release();
  }

  /**
   * @brief Handles completion of the internal eventfd wakeup poll.
   *
   * @param operation Wakeup operation associated with the CQE.
   * @param cqe Completion entry from the kernel.
   */
  void ProcessWakeupCqe(const Operation &operation, io_uring_cqe *cqe) {
    wakeup_poll_pending_ = false;

    IoResult result;
    result.type_ = operation.type_;
    result.fd_ = operation.fd_;
    result.result_ = cqe->res;
    result.error_code_ = cqe->res < 0 ? -cqe->res : 0;
    io_uring_cqe_seen(&ring_, cqe);

    if (result.result_ < 0) {
      if (stop_requested_.load(std::memory_order_acquire) && result.error_code_ == ECANCELED) {
        return;
      }
      throw InternalException(MakeErrorMessage("eventfd poll", result.error_code_));
    }
    if ((result.result_ & POLLIN) == 0) {
      throw InternalException("eventfd poll completed without POLLIN");
    }

    DrainWakeupCounter();
    DrainPosted();
    if (stop_requested_.load(std::memory_order_acquire)) {
      // Timeout SQEs can otherwise keep the loop alive until their deadlines.
      // Cancel them once shutdown has started.
      SubmitCancelPendingTimeouts();
      return;
    }
    SubmitWakeupPoll();
  }

  /**
   * @brief Writes to the eventfd so the run thread wakes and drains posted callbacks.
   */
  void SignalWakeup() const {
    constexpr std::uint64_t value = 1;
    while (true) {
      const ssize_t written = ::write(wakeup_fd_, &value, sizeof(value));
      if (std::cmp_equal(written, sizeof(value))) {
        return;
      }
      if (written < 0 && errno == EINTR) {
        continue;
      }
      if (written < 0 && errno == EAGAIN) {
        return;
      }
      throw InternalException(MakeErrorMessage("eventfd write", errno));
    }
  }

  /**
   * @brief Drains the eventfd counter after a wakeup poll completion.
   */
  void DrainWakeupCounter() const {
    std::uint64_t value = 0;
    while (true) {
      const ssize_t read_size = ::read(wakeup_fd_, &value, sizeof(value));
      if (std::cmp_equal(read_size, sizeof(value))) {
        return;
      }
      if (read_size < 0 && errno == EINTR) {
        continue;
      }
      if (read_size < 0 && errno == EAGAIN) {
        return;
      }
      throw InternalException(MakeErrorMessage("eventfd read", errno));
    }
  }

  /** @return Snapshot of cross-thread post diagnostics. */
  [[nodiscard]] auto PostStats() const -> UringPostStatsSnapshot {
    return UringPostStatsSnapshot{
        .posted_callbacks_ = posted_callbacks_count_.load(std::memory_order_relaxed),
        .drained_callbacks_ = drained_callbacks_count_.load(std::memory_order_relaxed),
        .drain_batches_ = drain_batches_.load(std::memory_order_relaxed),
        .max_observed_post_queue_depth_ = max_observed_post_queue_depth_.load(std::memory_order_relaxed),
    };
  }

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

  /** @brief Total callbacks accepted by `Post()`. */
  std::atomic<std::uint64_t> posted_callbacks_count_{0};

  /** @brief Total callbacks executed by `DrainPosted()`. */
  std::atomic<std::uint64_t> drained_callbacks_count_{0};

  /** @brief Number of batches drained from the posted callback queue. */
  std::atomic<std::uint64_t> drain_batches_{0};

  /** @brief Highest posted callback queue depth observed. */
  std::atomic<std::uint64_t> max_observed_post_queue_depth_{0};

  /** @brief Timeout operations that should be canceled when shutdown starts. */
  std::unordered_set<Operation *> pending_timeout_operations_;

  /** @brief Reusable operation objects recovered after CQE processing. */
  std::vector<std::unique_ptr<Operation>> operation_pool_;
};

/**
 * @brief Creates an io_uring context with the requested queue depth.
 *
 * @param entries Submission/completion queue depth.
 */
UringContext::UringContext(std::uint32_t entries) : runtime_(std::make_unique<Runtime>(entries)) {}

/** @brief Releases the private runtime. */
UringContext::~UringContext() = default;

/**
 * @brief Runs the event loop on the current thread until shutdown completes.
 *
 * @throws LifecycleException when called reentrantly or when submissions occur from the wrong thread.
 * @throws InternalException when io_uring or eventfd completion processing fails.
 */
void UringContext::Run() {
  runtime_->BeginRun();
  try {
    runtime_->SubmitWakeupPoll();

    // The loop exits only after Stop() has been requested and every submitted
    // user operation plus the internal wakeup poll has produced a CQE.
    while (!runtime_->stop_requested_.load(std::memory_order_acquire) || runtime_->pending_io_operations_ > 0 ||
           runtime_->wakeup_poll_pending_) {
      io_uring_cqe *cqe = nullptr;
      const int ret = io_uring_wait_cqe(&runtime_->ring_, &cqe);
      if (ret < 0) {
        if (ret == -EINTR) {
          continue;
        }
        throw InternalException(MakeErrorMessage("io_uring_wait_cqe", -ret));
      }

      runtime_->ProcessCqe(cqe);
      while (io_uring_peek_cqe(&runtime_->ring_, &cqe) == 0) {
        runtime_->ProcessCqe(cqe);
      }
    }
  } catch (...) {
    runtime_->EndRun();
    throw;
  }
  runtime_->EndRun();
}

/** @brief Requests event-loop shutdown from any thread. */
void UringContext::Stop() { runtime_->RequestStop(); }

/**
 * @brief Submits an asynchronous accept operation.
 *
 * @param listen_fd Listening socket file descriptor.
 * @return Awaitable that completes with accepted fd or kernel error.
 */
auto UringContext::Accept(int listen_fd) -> UringAwaitable {
  UringAwaitable awaitable;
  auto operation = runtime_->AcquireOperation();
  operation->type_ = OperationType::Accept;
  operation->fd_ = listen_fd;
  awaitable.Bind(*operation);

  runtime_->SubmitOperation(std::move(operation), [listen_fd](io_uring_sqe &sqe) {
    io_uring_prep_accept(&sqe, listen_fd, nullptr, nullptr, 0);
  });

  return awaitable;
}

/**
 * @brief Submits an asynchronous receive operation.
 *
 * @param fd Connected socket file descriptor.
 * @param buffer Caller-owned destination buffer that must outlive the await.
 * @param len Destination buffer length.
 * @return Awaitable that completes with bytes read, EOF, or kernel error.
 */
auto UringContext::Recv(int fd, void *buffer, std::size_t len) -> UringAwaitable {
  UringAwaitable awaitable;
  auto operation = runtime_->AcquireOperation();
  operation->type_ = OperationType::Recv;
  operation->fd_ = fd;
  operation->buffer_ = buffer;
  operation->length_ = len;
  awaitable.Bind(*operation);

  runtime_->SubmitOperation(std::move(operation),
                            [fd, buffer, len](io_uring_sqe &sqe) { io_uring_prep_recv(&sqe, fd, buffer, len, 0); });

  return awaitable;
}

/**
 * @brief Submits an asynchronous send operation.
 *
 * @param fd Connected socket file descriptor.
 * @param buffer Caller-owned source buffer that must outlive the await.
 * @param len Source buffer length.
 * @return Awaitable that completes with bytes written or kernel error.
 */
auto UringContext::Send(int fd, const void *buffer, std::size_t len) -> UringAwaitable {
  UringAwaitable awaitable;
  auto operation = runtime_->AcquireOperation();
  operation->type_ = OperationType::Send;
  operation->fd_ = fd;
  operation->buffer_ = const_cast<void *>(buffer);
  operation->length_ = len;
  awaitable.Bind(*operation);

  runtime_->SubmitOperation(std::move(operation),
                            [fd, buffer, len](io_uring_sqe &sqe) { io_uring_prep_send(&sqe, fd, buffer, len, 0); });

  return awaitable;
}

/**
 * @brief Submits an asynchronous timeout operation.
 *
 * @param timeout Sleep duration.
 * @return Awaitable that completes after timeout or cancellation.
 */
auto UringContext::SleepFor(std::chrono::nanoseconds timeout) -> UringAwaitable {
  UringAwaitable awaitable;
  auto operation = runtime_->AcquireOperation();
  operation->type_ = OperationType::Timeout;
  operation->timeout_ = MakeKernelTimespec(timeout);
  Operation *raw_operation = operation.get();
  awaitable.Bind(*operation);

  runtime_->SubmitOperation(std::move(operation), [raw_operation](io_uring_sqe &sqe) {
    io_uring_prep_timeout(&sqe, &raw_operation->timeout_, 0, 0);
  });

  return awaitable;
}

/**
 * @brief Submits an asynchronous no-op operation.
 *
 * @return Awaitable used by tests and wakeup plumbing.
 */
auto UringContext::Nop() -> UringAwaitable {
  UringAwaitable awaitable;
  auto operation = runtime_->AcquireOperation();
  operation->type_ = OperationType::Nop;
  awaitable.Bind(*operation);

  runtime_->SubmitOperation(std::move(operation), [](io_uring_sqe &sqe) { io_uring_prep_nop(&sqe); });

  return awaitable;
}

/** @return Snapshot of cross-thread post diagnostics. */
auto UringContext::post_stats() const -> UringPostStatsSnapshot { return runtime_->PostStats(); }

/**
 * @brief Cancels pending operations associated with one file descriptor.
 *
 * @param fd File descriptor to cancel. Negative descriptors are ignored.
 */
void UringContext::CancelFd(int fd) {
  if (fd < 0 || !runtime_->IsRunning()) {
    return;
  }
  runtime_->SubmitCancelFd(fd);
}

/**
 * @brief Schedules a callback to run on the event-loop thread.
 *
 * @param fn Callback to execute from the run thread.
 */
void UringContext::Post(std::function<void()> fn) { runtime_->EnqueuePosted(std::move(fn)); }

}  // namespace xrpc::io
