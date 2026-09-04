/**
 * @file uring_context_operations.cpp
 * @brief Implements io_uring operation submission and completion handling.
 *
 * A one-shot asynchronous request has one `Operation` containing both its I/O
 * parameters and coroutine completion state. `UringAwaitable` owns the
 * operation until `await_suspend()` transfers it to the runtime. The CQE path
 * takes final ownership, stores the result, resumes the coroutine synchronously,
 * and destroys the operation after `await_resume()` has consumed that result.
 *
 * Operation lifecycle:
 *
 *      create deferred Operation
 *              |
 *              v
 *       await_suspend(waiter)
 *              |
 *              v
 *        prepare + stage SQE
 *              |
 *              v
 *       event-turn batch submit
 *              |
 *              v
 *           kernel -> CQE
 *              |
 *              v
 *         recover Operation
 *              |
 *              v
 *       store result + resume
 *              |
 *              v
 *     await_resume() reads result
 *              |
 *              v
 *       destroy Operation
 *
 */

#include "io/uring_context.h"

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <exception>
#include <memory>
#include <utility>

#include <liburing.h>
#include <sys/socket.h>

#include "common/xrpc_exception.h"
#include "detail/context_runtime.h"

namespace xrpc::io {

auto UringContext::Runtime::AcquireSqe() -> io_uring_sqe * {
  io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr && !staged_operations_.empty()) {
    FlushSubmissionBatch();
    sqe = io_uring_get_sqe(&ring_);
  }
  if (sqe == nullptr) {
    throw InternalException("io_uring_get_sqe failed");
  }
  return sqe;
}

/** @brief Stages a prepared operation for the next submission flush. */
void UringContext::Runtime::SubmitPreparedOperation(std::unique_ptr<Operation> operation,
                                                    bool counts_as_pending_io) noexcept {
  assert(staged_operations_.size() < staged_operations_.capacity());
  if (counts_as_pending_io) {
    ++pending_io_operations_;
  }
  staged_operations_.push_back(std::move(operation));
}

void UringContext::Runtime::FlushSubmissionBatch() {
  while (!staged_operations_.empty()) {
    int ret = 0;
    do {
      ret = io_uring_submit(&ring_);
    } while (ret == -EINTR);

    if (ret < 0) {
      throw InternalException(MakeErrorMessage("io_uring_submit", -ret));
    }
    if (ret == 0) {
      throw InternalException("io_uring_submit made no progress");
    }

    const auto submitted = static_cast<std::size_t>(ret);
    if (submitted > staged_operations_.size()) {
      throw InternalException("io_uring_submit returned an invalid submission count");
    }
    for (std::size_t index = 0; index < submitted; ++index) {
      [[maybe_unused]] Operation *released = staged_operations_[index].release();
    }
    staged_operations_.erase(staged_operations_.begin(), staged_operations_.begin() + ret);
  }
}

/**
 * @brief Starts a deferred awaitable operation on the run thread.
 *
 * A stop request produces a synchronous cancellation result and leaves
 * ownership with the awaitable. Otherwise, all potentially failing work is
 * completed before the SQE receives the operation pointer. From that commit
 * point onward, preparing the SQE and moving the operation into the reserved
 * staging vector do not throw.
 */
auto UringContext::Runtime::TryStartAwaitableOperation(std::unique_ptr<Operation> &operation,
                                                       std::coroutine_handle<> continuation) -> bool {
  AssertRunThread("io_uring submission");
  if (!operation) {
    throw LifecycleException("io_uring awaitable operation was already started");
  }
  if (stop_requested_.load()) {
    operation->result_ = MakeCancelledResult(*operation);
    return false;
  }

  switch (operation->type_) {
    case OperationType::Accept:
    case OperationType::Recv:
    case OperationType::Send:
      break;
    case OperationType::Unknown:
      throw InternalException("cannot start an unknown io_uring operation");
  }

  operation->continuation_ = continuation;
  io_uring_sqe *sqe = AcquireSqe();
  assert(staged_operations_.size() < staged_operations_.capacity());

  switch (operation->type_) {
    case OperationType::Accept:
      io_uring_prep_accept(sqe, operation->fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      break;
    case OperationType::Recv:
      io_uring_prep_recv(sqe, operation->fd_, operation->buffer_, operation->length_, 0);
      break;
    case OperationType::Send:
      io_uring_prep_send(sqe, operation->fd_, operation->buffer_, operation->length_, MSG_NOSIGNAL);
      break;
    case OperationType::Unknown:
      std::terminate();
  }

  Operation *raw_operation = operation.get();
  io_uring_sqe_set_data(sqe, raw_operation);

  SubmitPreparedOperation(std::move(operation), true);
  return true;
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

  operation.result_.type_ = operation.type_;
  operation.result_.fd_ = operation.fd_;
  operation.result_.result_ = cqe->res;
  operation.result_.error_code_ = cqe->res < 0 ? -cqe->res : 0;
  if (operation.type_ == OperationType::Recv || operation.type_ == OperationType::Send) {
    operation.result_.bytes_transferred_ = cqe->res > 0 ? static_cast<std::size_t>(cqe->res) : 0;
  }

  io_uring_cqe_seen(&ring_, cqe);
  std::coroutine_handle<> continuation = std::exchange(operation.continuation_, {});
  if (!continuation) {
    throw InternalException("io_uring completion has no coroutine waiter");
  }
  continuation.resume();
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

void UringContext::Runtime::SubmitCancelFd(int fd) {
  AssertRunThread("UringContext::CancelFd");

  auto operation = std::make_unique<Operation>();
  operation->completion_category_ = Operation::CompletionCategory::Cancel;
  operation->fd_ = fd;

  io_uring_sqe *sqe = AcquireSqe();

  io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
  Operation *raw_operation = operation.get();
  io_uring_sqe_set_data(sqe, raw_operation);

  SubmitPreparedOperation(std::move(operation), true);

  // Callers close the descriptor immediately after CancelFd() returns. Publish
  // the cancellation before that close instead of waiting for the turn boundary.
  FlushSubmissionBatch();
}

auto UringContext::Accept(int listen_fd) -> UringAwaitable {
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Accept;
  operation->fd_ = listen_fd;
  return UringAwaitable(*this, std::move(operation));
}

auto UringContext::Recv(int fd, void *buffer, std::size_t len) -> UringAwaitable {
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Recv;
  operation->fd_ = fd;
  operation->buffer_ = buffer;
  operation->length_ = len;
  return UringAwaitable(*this, std::move(operation));
}

auto UringContext::Send(int fd, const void *buffer, std::size_t len) -> UringAwaitable {
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Send;
  operation->fd_ = fd;
  operation->buffer_ = const_cast<void *>(buffer);
  operation->length_ = len;
  return UringAwaitable(*this, std::move(operation));
}

UringAwaitable::UringAwaitable(UringContext &context, std::unique_ptr<Operation> operation) noexcept
    : context_(&context), unstarted_operation_(std::move(operation)) {}

UringAwaitable::~UringAwaitable() {
  if (active_operation_ != nullptr) {
    std::terminate();
  }
}

UringAwaitable::UringAwaitable(UringAwaitable &&other) noexcept
    : context_(std::exchange(other.context_, nullptr)),
      unstarted_operation_(std::move(other.unstarted_operation_)),
      active_operation_(std::exchange(other.active_operation_, nullptr)) {
  if (active_operation_ != nullptr) {
    std::terminate();
  }
}

auto UringAwaitable::operator=(UringAwaitable &&other) noexcept -> UringAwaitable & {
  if (this == &other) {
    return *this;
  }
  if (active_operation_ != nullptr || other.active_operation_ != nullptr) {
    std::terminate();
  }
  context_ = std::exchange(other.context_, nullptr);
  unstarted_operation_ = std::move(other.unstarted_operation_);
  return *this;
}

auto UringAwaitable::await_suspend(std::coroutine_handle<> continuation) -> bool {
  if (context_ == nullptr || !unstarted_operation_ || active_operation_ != nullptr) {
    throw LifecycleException("io_uring awaitable may only be awaited once");
  }

  Operation *operation = unstarted_operation_.get();
  if (!context_->TryStartOperation(unstarted_operation_, continuation)) {
    return false;
  }
  active_operation_ = operation;
  return true;
}

auto UringAwaitable::await_resume() -> IoResult {
  if (active_operation_ != nullptr) {
    IoResult result = active_operation_->result_;
    active_operation_ = nullptr;
    return result;
  }
  if (unstarted_operation_) {
    IoResult result = unstarted_operation_->result_;
    unstarted_operation_.reset();
    return result;
  }
  throw LifecycleException("io_uring awaitable result was already consumed");
}

auto UringContext::TryStartOperation(std::unique_ptr<Operation> &operation, std::coroutine_handle<> continuation)
    -> bool {
  return runtime_->TryStartAwaitableOperation(operation, continuation);
}

}  // namespace xrpc::io
