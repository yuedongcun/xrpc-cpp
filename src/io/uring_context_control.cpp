/**
 * @file uring_context_control.cpp
 * @brief Implements UringContext's cross-thread control path.
 *
 * `Post()` queues callbacks for execution on the run thread, while `Stop()`
 * closes that queue and requests event-loop shutdown. An eventfd wakes a
 * blocked io_uring wait so queued callbacks and shutdown requests are observed
 * promptly.
 *
 * The callback queue is mutex-protected; execution remains confined to the
 * UringContext run thread.
 */

/**
 * @file uring_context_control.cpp
 * @brief Implements `UringContext` cross-thread control and wakeup handling.
 *
 * `Post()` queues callbacks for execution on the run thread. `Stop()` stops
 * accepting new callbacks and requests event-loop shutdown; callbacks already
 * queued are still drained.
 *
 * Cross-thread wakeup path:
 *
 *   Post() / Stop()
 *         |
 *         v
 *   signal eventfd
 *         |
 *         v
 *   io_uring poll CQE
 *         |
 *         v
 *   ProcessWakeupCqe()
 *         |
 *         +-- drain eventfd counter
 *         +-- drain posted callbacks
 *         `-- rearm wakeup poll or continue shutdown
 *
 * Multiple threads may call `Post()` concurrently, so the callback queue is
 * protected by a mutex. The thread executing `UringContext::Run()` handles
 * wakeup CQEs and executes all posted callbacks.
 */

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

#include "common/xrpc_exception.h"
#include "detail/context_runtime.h"

namespace xrpc::io {

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
    SignalWakeup();
  }
}

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

void UringContext::Runtime::DrainPosted() {
  std::queue<std::function<void()>> callbacks;
  {
    std::lock_guard<std::mutex> lock(post_mutex_);

    std::swap(callbacks, posted_callbacks_);
  }

  while (!callbacks.empty()) {
    std::function<void()> &callback = callbacks.front();
    callback();
    callbacks.pop();
  }
}

/**
 * @brief Arms the eventfd poll used to wake the io_uring event loop.
 *
 * At most one wakeup poll may be pending at a time. The submitted operation is
 * released to the completion path and remains alive until its CQE is processed.
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
 * @brief Handles completion of the eventfd wakeup poll.
 *
 * The wakeup counter and posted callbacks are drained first. During normal
 * operation, a new wakeup poll is submitted. During shutdown, the poll is not
 * rearmed and pending timeout operations are cancelled instead.
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
    SubmitCancelPendingTimeouts();
    return;
  }
  SubmitWakeupPoll();
}

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
