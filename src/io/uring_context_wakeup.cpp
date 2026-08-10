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
#include "rpc/xrpc_exception.h"

namespace xrpc::io {

/**
 * @brief Atomically records the maximum observed value.
 *
 * @param maximum Relaxed diagnostic counter to update.
 * @param value Candidate maximum.
 */
void UringContext::Runtime::ObserveMaximum(std::atomic<std::uint64_t> &maximum, std::size_t value) {
  std::uint64_t observed = maximum.load(std::memory_order_relaxed);
  const auto candidate = static_cast<std::uint64_t>(value);
  while (observed < candidate &&
         !maximum.compare_exchange_weak(observed, candidate, std::memory_order_relaxed, std::memory_order_relaxed)) {
  }
}

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
    posted_callbacks_count_.fetch_add(1, std::memory_order_relaxed);
    ObserveMaximum(max_observed_post_queue_depth_, posted_callbacks_.size());
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
void UringContext::Runtime::DrainPosted() {
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

/** @return Snapshot of cross-thread post diagnostics. */
auto UringContext::Runtime::PostStats() const -> UringPostStatsSnapshot {
  return UringPostStatsSnapshot{
      .posted_callbacks_ = posted_callbacks_count_.load(std::memory_order_relaxed),
      .drained_callbacks_ = drained_callbacks_count_.load(std::memory_order_relaxed),
      .drain_batches_ = drain_batches_.load(std::memory_order_relaxed),
      .max_observed_post_queue_depth_ = max_observed_post_queue_depth_.load(std::memory_order_relaxed),
  };
}

}  // namespace xrpc::io
