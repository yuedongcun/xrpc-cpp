/**
 * @file uring_context_operations.cpp
 * @brief Implements io_uring operation submission and completion handling.
 *
 * Each asynchronous request uses an `Operation` for the kernel-side state and
 * an `AwaitableState` for the coroutine-side state. `UringAwaitable` owns the
 * awaitable state, while `Operation` keeps only a weak reference to it.
 *
 * Operation lifecycle:
 *
 *   create Operation + AwaitableState
 *              |
 *              v
 *       prepare + submit SQE
 *              |
 *              v
 *           kernel
 *              |
 *              v
 *             CQE
 *              |
 *              v
 *         recover Operation
 *              |
 *              v
 *           IoResult
 *              |
 *              +-- AwaitableState alive --> store result --> resume coroutine
 *              |
 *              `-- AwaitableState gone  --> skip coroutine completion
 *              |
 *              v
 *        destroy Operation
 *
 * Pending timeout operations are tracked separately so they can be cancelled
 * during shutdown instead of delaying event-loop termination.
 */

#include "io/uring_context.h"

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <memory>
#include <utility>

#include <liburing.h>
#include <linux/time_types.h>

#include "common/xrpc_exception.h"
#include "detail/context_runtime.h"

namespace xrpc::io {
namespace {

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
 * @brief Submits an operation to io_uring and transfers it to the CQE path.
 *
 * After successful submission, the `Operation` remains alive through the
 * pointer stored in SQE user data. `ProcessCqe()` restores unique ownership
 * when the matching CQE arrives.
 */
template <typename Prep>
void UringContext::Runtime::SubmitOperation(std::unique_ptr<Operation> operation, Prep &&prep) {
  bool tracked_timeout = false;
  try {
    AssertRunThread("io_uring submission");
    if (stop_requested_.load()) {
      CompleteAwaitableState(*operation, MakeCancelledResult(*operation));
      return;
    }

    tracked_timeout = TrackTimeoutOperation(*operation);
    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      throw InternalException("io_uring_get_sqe failed");
    }

    prep(sqe);

    Operation *raw_operation = operation.get();
    io_uring_sqe_set_data(sqe, raw_operation);

    const int ret = io_uring_submit(&ring_);
    if (ret < 0) {
      throw InternalException(MakeErrorMessage("io_uring_submit", -ret));
    }

    ++pending_io_operations_;

    [[maybe_unused]] Operation *released = operation.release();
  } catch (...) {
    if (operation) {
      if (tracked_timeout) {
        pending_timeout_operations_.erase(operation.get());
      }
    }
    throw;
  }
}

void UringContext::Runtime::ProcessCqe(io_uring_cqe *cqe) {
  auto *raw_operation = static_cast<Operation *>(io_uring_cqe_get_data(cqe));
  if (raw_operation == nullptr) {
    io_uring_cqe_seen(&ring_, cqe);
    return;
  }

  std::unique_ptr<Operation> operation(raw_operation);
  if (operation->type_ == OperationType::Wakeup) {
    ProcessWakeupCqe(*operation, cqe);
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
    if (result.result_ < 0 && result.error_code_ != ENOENT && result.error_code_ != EALREADY &&
        result.error_code_ != ECANCELED) {
      throw InternalException(MakeErrorMessage("io_uring cancel", result.error_code_));
    }
    return;
  }

  CompleteAwaitableState(*operation, result);
}

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
 * @brief Delivers an I/O result to the coroutine if its awaitable state still exists.
 *
 * A missing state means the owning awaitable or coroutine frame was already
 * destroyed, so the completion is discarded without resuming a coroutine.
 */
void UringContext::Runtime::CompleteAwaitableState(Operation &operation, const IoResult &result) {
  std::shared_ptr<detail::AwaitableState> state = operation.awaitable_state_.lock();
  if (!state) {
    return;
  }

  state->result_ = result;
  state->ready_ = true;
  std::coroutine_handle<> continuation = std::exchange(state->continuation_, {});
  if (continuation) {
    continuation.resume();
  }
}

void UringContext::Runtime::SubmitCancelFd(int fd) {
  AssertRunThread("UringContext::CancelFd");

  auto operation = std::make_unique<Operation>();
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

void UringContext::Runtime::SubmitCancelOperation(Operation *operation_to_cancel) {
  AssertRunThread("UringContext timeout cancellation");

  auto operation = std::make_unique<Operation>();
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

auto UringContext::Runtime::TrackTimeoutOperation(Operation &operation) -> bool {
  if (operation.type_ != OperationType::Timeout) {
    return false;
  }
  pending_timeout_operations_.insert(&operation);
  return true;
}

void UringContext::Runtime::UntrackTimeoutOperation(Operation &operation) {
  if (operation.type_ == OperationType::Timeout) {
    pending_timeout_operations_.erase(&operation);
  }
}

void UringContext::Runtime::SubmitCancelPendingTimeouts() {
  if (timeout_cancellations_submitted_) {
    return;
  }

  timeout_cancellations_submitted_ = true;
  for (Operation *operation : pending_timeout_operations_) {
    SubmitCancelOperation(operation);
  }
}

auto UringContext::Accept(int listen_fd) -> UringAwaitable {
  auto state = std::make_shared<detail::AwaitableState>();
  UringAwaitable awaitable(state);
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Accept;
  operation->fd_ = listen_fd;
  operation->awaitable_state_ = state;

  runtime_->SubmitOperation(std::move(operation), [listen_fd](io_uring_sqe *sqe) -> void {
    io_uring_prep_accept(sqe, listen_fd, nullptr, nullptr, 0);
  });

  return awaitable;
}

auto UringContext::Recv(int fd, void *buffer, std::size_t len) -> UringAwaitable {
  auto state = std::make_shared<detail::AwaitableState>();
  UringAwaitable awaitable(state);
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Recv;
  operation->fd_ = fd;
  operation->buffer_ = buffer;
  operation->length_ = len;
  operation->awaitable_state_ = state;

  runtime_->SubmitOperation(std::move(operation), [fd, buffer, len](io_uring_sqe *sqe) -> void {
    io_uring_prep_recv(sqe, fd, buffer, len, 0);
  });

  return awaitable;
}

auto UringContext::Send(int fd, const void *buffer, std::size_t len) -> UringAwaitable {
  auto state = std::make_shared<detail::AwaitableState>();
  UringAwaitable awaitable(state);
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Send;
  operation->fd_ = fd;
  operation->buffer_ = const_cast<void *>(buffer);
  operation->length_ = len;
  operation->awaitable_state_ = state;

  runtime_->SubmitOperation(std::move(operation), [fd, buffer, len](io_uring_sqe *sqe) -> void {
    io_uring_prep_send(sqe, fd, buffer, len, 0);
  });

  return awaitable;
}

auto UringContext::SleepFor(std::chrono::nanoseconds timeout) -> UringAwaitable {
  auto state = std::make_shared<detail::AwaitableState>();
  UringAwaitable awaitable(state);
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Timeout;
  operation->timeout_ = MakeKernelTimespec(timeout);
  Operation *raw_operation = operation.get();
  operation->awaitable_state_ = state;

  runtime_->SubmitOperation(std::move(operation), [raw_operation](io_uring_sqe *sqe) -> void {
    io_uring_prep_timeout(sqe, &raw_operation->timeout_, 0, 0);
  });

  return awaitable;
}

auto UringContext::Nop() -> UringAwaitable {
  auto state = std::make_shared<detail::AwaitableState>();
  UringAwaitable awaitable(state);
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Nop;
  operation->awaitable_state_ = state;

  runtime_->SubmitOperation(std::move(operation), [](io_uring_sqe *sqe) -> void { io_uring_prep_nop(sqe); });

  return awaitable;
}

}  // namespace xrpc::io
