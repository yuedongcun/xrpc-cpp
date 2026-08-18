#include "io/uring_context.h"

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <memory>
#include <utility>

#include <liburing.h>
#include <linux/time_types.h>

#include "common/xrpc_exception.h"
#include "io/operation.h"
#include "io/uring_context_runtime.h"

namespace xrpc::io {
namespace {

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
void UringContext::Runtime::SubmitOperation(std::unique_ptr<Operation> operation, Prep &&prep) {
  bool tracked_timeout = false;
  try {
    AssertRunThread("io_uring submission");
    if (stop_requested_.load()) {
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
auto UringContext::Runtime::AcquireOperation() -> std::unique_ptr<Operation> {
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
void UringContext::Runtime::RecycleOperation(std::unique_ptr<Operation> operation) {
  *operation = Operation{};
  operation_pool_.push_back(std::move(operation));
}

/**
 * @brief Processes one completion queue entry.
 *
 * @param cqe Completion queue entry returned by the kernel.
 * @throws InternalException when completion accounting or cancellation status is inconsistent.
 */
void UringContext::Runtime::ProcessCqe(io_uring_cqe *cqe) {
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

  if (operation->awaitable_state_ != nullptr) {
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
auto UringContext::Runtime::MakeCancelledResult(const Operation &operation) -> IoResult {
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
void UringContext::Runtime::CompleteAwaitableState(Operation &operation, const IoResult &result) {
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
void UringContext::Runtime::DetachAwaitableState(Operation &operation) noexcept {
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
void UringContext::Runtime::SubmitCancelFd(int fd) {
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
void UringContext::Runtime::SubmitCancelOperation(Operation *operation_to_cancel) {
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
auto UringContext::Runtime::TrackTimeoutOperation(Operation &operation) -> bool {
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
void UringContext::Runtime::UntrackTimeoutOperation(Operation &operation) {
  if (operation.type_ == OperationType::Timeout) {
    pending_timeout_operations_.erase(&operation);
  }
}

/**
 * @brief Cancels all pending timeout operations once shutdown starts.
 *
 * Timeout SQEs can otherwise keep `Run()` alive until their deadlines expire.
 */
void UringContext::Runtime::SubmitCancelPendingTimeouts() {
  if (timeout_cancellations_submitted_) {
    return;
  }

  timeout_cancellations_submitted_ = true;
  for (Operation *operation : pending_timeout_operations_) {
    SubmitCancelOperation(operation);
  }
}

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

  runtime_->SubmitOperation(std::move(operation), [listen_fd](io_uring_sqe &sqe) -> void {
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

  runtime_->SubmitOperation(std::move(operation), [fd, buffer, len](io_uring_sqe &sqe) -> void {
    io_uring_prep_recv(&sqe, fd, buffer, len, 0);
  });

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

  runtime_->SubmitOperation(std::move(operation), [fd, buffer, len](io_uring_sqe &sqe) -> void {
    io_uring_prep_send(&sqe, fd, buffer, len, 0);
  });

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

  runtime_->SubmitOperation(std::move(operation), [raw_operation](io_uring_sqe &sqe) -> void {
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

  runtime_->SubmitOperation(std::move(operation), [](io_uring_sqe &sqe) -> void { io_uring_prep_nop(&sqe); });

  return awaitable;
}

}  // namespace xrpc::io
