/**
 * @file uring_context_operations.cpp
 * @brief Implements io_uring operation submission and completion handling.
 *
 * Each asynchronous request uses an `Operation` for the kernel-side state and
 * an `AwaitableState` for the coroutine-side state. `UringAwaitable` owns the
 * awaitable state, while `Operation` keeps only a weak reference to it.
 * CQE dispatch is selected by completion category: awaitable completions
 * resume coroutine state, while cancel and wakeup completions follow separate
 * control paths.
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
 */

#include "io/uring_context.h"

#include <cerrno>
#include <cstddef>
#include <memory>
#include <utility>

#include <liburing.h>
#include <sys/socket.h>

#include "common/xrpc_exception.h"
#include "detail/context_runtime.h"

namespace xrpc::io {
/**
 * @brief Submits an awaitable operation and transfers ownership to the CQE path.
 *
 * After successful submission, ownership is released from the local
 * `unique_ptr`. The SQE stores the raw operation pointer, and `ProcessCqe()`
 * restores unique ownership when the matching CQE arrives.
 */
template <typename Prep>
void UringContext::Runtime::SubmitAwaitableOperation(std::unique_ptr<Operation> operation, Prep &&prep) {
  AssertRunThread("io_uring submission");
  if (stop_requested_.load()) {
    CompleteAwaitableState(*operation, MakeCancelledResult(*operation));
    return;
  }

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
}

void UringContext::Runtime::ProcessCqe(io_uring_cqe *cqe) {
  auto *raw_operation = static_cast<Operation *>(io_uring_cqe_get_data(cqe));
  if (raw_operation == nullptr) {
    io_uring_cqe_seen(&ring_, cqe);
    return;
  }

  std::unique_ptr<Operation> operation(raw_operation);
  switch (operation->completion_category_) {
    case Operation::CompletionCategory::Awaitable:
      ProcessAwaitableCqe(*operation, cqe);
      return;
    case Operation::CompletionCategory::Cancel:
      ProcessCancelCqe(cqe);
      return;
    case Operation::CompletionCategory::Wakeup:
      ProcessWakeupCqe(cqe);
      return;
  }
}

void UringContext::Runtime::ProcessAwaitableCqe(Operation &operation, io_uring_cqe *cqe) {
  if (pending_io_operations_ == 0) {
    io_uring_cqe_seen(&ring_, cqe);
    throw InternalException("io_uring completion without a pending operation");
  }
  --pending_io_operations_;

  IoResult result;
  result.type_ = operation.type_;
  result.fd_ = operation.fd_;
  result.result_ = cqe->res;
  result.error_code_ = cqe->res < 0 ? -cqe->res : 0;
  if (operation.type_ == OperationType::Recv || operation.type_ == OperationType::Send) {
    result.bytes_transferred_ = cqe->res > 0 ? static_cast<std::size_t>(cqe->res) : 0;
  }

  io_uring_cqe_seen(&ring_, cqe);
  CompleteAwaitableState(operation, result);
}

void UringContext::Runtime::ProcessCancelCqe(io_uring_cqe *cqe) {
  if (pending_io_operations_ == 0) {
    io_uring_cqe_seen(&ring_, cqe);
    throw InternalException("io_uring completion without a pending operation");
  }
  --pending_io_operations_;

  IoResult result;
  result.result_ = cqe->res;
  result.error_code_ = cqe->res < 0 ? -cqe->res : 0;
  io_uring_cqe_seen(&ring_, cqe);
  if (result.result_ < 0 && result.error_code_ != ENOENT && result.error_code_ != EALREADY &&
      result.error_code_ != ECANCELED) {
    throw InternalException(MakeErrorMessage("io_uring cancel", result.error_code_));
  }
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
 * @brief Delivers an I/O result if the coroutine-side state still exists.
 *
 * If the awaitable state has already been destroyed, the completion is
 * discarded and no coroutine is resumed.
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
  operation->completion_category_ = Operation::CompletionCategory::Cancel;
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

auto UringContext::Accept(int listen_fd) -> UringAwaitable {
  auto state = std::make_shared<detail::AwaitableState>();
  UringAwaitable awaitable(state);
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Accept;
  operation->fd_ = listen_fd;
  operation->awaitable_state_ = state;

  runtime_->SubmitAwaitableOperation(std::move(operation), [listen_fd](io_uring_sqe *sqe) -> void {
    io_uring_prep_accept(sqe, listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
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

  runtime_->SubmitAwaitableOperation(std::move(operation), [fd, buffer, len](io_uring_sqe *sqe) -> void {
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

  runtime_->SubmitAwaitableOperation(std::move(operation), [fd, buffer, len](io_uring_sqe *sqe) -> void {
    io_uring_prep_send(sqe, fd, buffer, len, MSG_NOSIGNAL);
  });

  return awaitable;
}

}  // namespace xrpc::io
