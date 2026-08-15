#include "io/uring_context.h"

#include <poll.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <utility>

#include <liburing.h>

#include "io/operation.h"
#include "io/uring_context_runtime.h"
#include "common/xrpc_exception.h"

namespace xrpc::io {

/**
 * @brief Enqueues a cross-thread callback for execution on the run thread.
 *
 * @param fn Callback to execute from `DrainPosted()`.
 */
void UringContext::Runtime::EnqueuePosted(std::function<void()> fn) {
  bool should_wake = false;
  {
    std::lock_guard<std::mutex> lock(post_mutex_);
    if (!accepting_posts_) {
      return;
    }
    should_wake = posted_callbacks_.empty();
    posted_callbacks_.emplace(std::move(fn));
  }

  if (should_wake) {
    // One eventfd write is enough to make the Run thread drain the whole
    // posted queue; additional callbacks can piggyback on that wakeup.
    SignalWakeup();
  }
}

/** @brief Requests event-loop shutdown and wakes the run thread. */
void UringContext::Runtime::RequestStop() {
  {
    std::lock_guard<std::mutex> lock(post_mutex_);
    if (!accepting_posts_) {
      return;
    }
    accepting_posts_ = false;
    stop_requested_.store(true);
  }

  SignalWakeup();
}

/**
 * @brief Executes all callbacks currently posted to the run thread.
 *
 * Callbacks are swapped out under the mutex and run without holding it so callbacks may safely
 * post follow-up work or request shutdown.
 */
void UringContext::Runtime::DrainPosted() {
  std::queue<std::function<void()>> callbacks;
  {
    std::lock_guard<std::mutex> lock(post_mutex_);
    // Swap under the mutex, then execute callbacks without holding it so
    // callbacks can safely post more work or call Stop().
    std::swap(callbacks, posted_callbacks_);
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
void UringContext::Runtime::SubmitWakeupPoll() {
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
void UringContext::Runtime::ProcessWakeupCqe(const Operation &operation, io_uring_cqe *cqe) {
  wakeup_poll_pending_ = false;

  IoResult result;
  result.type_ = operation.type_;
  result.fd_ = operation.fd_;
  result.result_ = cqe->res;
  result.error_code_ = cqe->res < 0 ? -cqe->res : 0;
  io_uring_cqe_seen(&ring_, cqe);

  if (result.result_ < 0) {
    if (stop_requested_.load() && result.error_code_ == ECANCELED) {
      return;
    }
    throw InternalException(MakeErrorMessage("eventfd poll", result.error_code_));
  }
  if ((result.result_ & POLLIN) == 0) {
    throw InternalException("eventfd poll completed without POLLIN");
  }

  DrainWakeupCounter();
  DrainPosted();
  if (stop_requested_.load()) {
    // Timeout SQEs can otherwise keep the loop alive until their deadlines.
    // Cancel them once shutdown has started.
    SubmitCancelPendingTimeouts();
    return;
  }
  SubmitWakeupPoll();
}

/** @brief Writes to the eventfd so the run thread wakes and drains posted callbacks. */
void UringContext::Runtime::SignalWakeup() const {
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

/** @brief Drains the eventfd counter after a wakeup poll completion. */
void UringContext::Runtime::DrainWakeupCounter() const {
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

}  // namespace xrpc::io
