#include "io/uring_context.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <liburing.h>

#include "io/uring_context_runtime.h"
#include "rpc/xrpc_exception.h"

namespace xrpc::io {

/**
 * @brief Builds a diagnostic message from an operation name and errno value.
 *
 * @param action io_uring or eventfd operation name.
 * @param error_code Positive errno value, or zero when no system error applies.
 * @return Human-readable diagnostic message.
 */
auto UringContext::Runtime::MakeErrorMessage(std::string_view action, int error_code) -> std::string {
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
auto UringContext::Runtime::CurrentThreadToken() -> const void * {
  static thread_local const char token = 0;
  return &token;
}

/**
 * @brief Initializes the kernel ring and eventfd wakeup channel.
 *
 * @param entries Submission/completion queue depth.
 * @throws InternalException when io_uring or eventfd initialization fails.
 */
UringContext::Runtime::Runtime(std::uint32_t entries) {
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
UringContext::Runtime::~Runtime() {
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
void UringContext::Runtime::BeginRun() {
  const void *expected = nullptr;
  if (!run_thread_token_.compare_exchange_strong(expected, CurrentThreadToken(), std::memory_order_acq_rel)) {
    throw LifecycleException("UringContext::Run is not reentrant");
  }
}

/** @brief Clears the run-thread token after `Run()` exits. */
void UringContext::Runtime::EndRun() { run_thread_token_.store(nullptr, std::memory_order_release); }

/**
 * @brief Verifies that an action is executing on the run thread.
 *
 * @param action Action name used in the lifecycle error message.
 * @throws LifecycleException when called from any other thread.
 */
void UringContext::Runtime::AssertRunThread(std::string_view action) const {
  if (run_thread_token_.load(std::memory_order_acquire) != CurrentThreadToken()) {
    throw LifecycleException(std::string(action) + " must run on the UringContext thread");
  }
}

/** @return true while a thread is inside `UringContext::Run()`. */
auto UringContext::Runtime::IsRunning() const -> bool {
  return run_thread_token_.load(std::memory_order_acquire) != nullptr;
}

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
        throw InternalException(Runtime::MakeErrorMessage("io_uring_wait_cqe", -ret));
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
