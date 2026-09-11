/**
 * @file uring_context_operations.cpp
 * @brief Implements io_uring operation submission and completion handling.
 *
 * A one-shot asynchronous request has one `Operation` containing both its I/O
 * parameters and coroutine completion state. `UringAwaitable` owns the
 * operation until `await_suspend()` transfers it to the runtime. A multishot
 * awaitable keeps that operation in the runtime while CQEs carry MORE and
 * releases it on the final CQE.
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

#include "common/abort.h"
#include "common/xrpc_exception.h"
#include "io/uring_context_runtime.h"

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
      Abort("io_uring_submit returned zero while operations remain staged");
    }

    const auto submitted = static_cast<std::size_t>(ret);
    if (submitted > staged_operations_.size()) {
      Abort("io_uring_submit reported more operations than were staged");
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
  AssertRunThread("io_uring submission attempted outside the owning Run thread");
  if (!operation) {
    Abort("UringContext attempted to start an empty operation");
  }
  if (stop_requested_.load()) {
    operation->result_ = MakeCancelledResult(*operation);
    return false;
  }

  switch (operation->type_) {
    case OperationType::Accept:
    case OperationType::Recv:
    case OperationType::RecvProvided:
    case OperationType::Send:
      break;
    case OperationType::Unknown:
      Abort("UringContext attempted to start an operation with unknown type");
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
    case OperationType::RecvProvided:
      if (!provided_buffer_pool_) {
        Abort("UringContext attempted a provided-buffer receive without a registered pool");
      }
      if (operation->awaitable_ != nullptr) {
        io_uring_prep_recv_multishot(sqe, operation->fd_, nullptr, 0, 0);
      } else {
        io_uring_prep_recv(sqe, operation->fd_, nullptr, provided_buffer_pool_->BufferSize(), 0);
      }
      sqe->flags |= IOSQE_BUFFER_SELECT;
      sqe->buf_group = provided_buffer_pool_->GroupId();
      break;
    case OperationType::Send:
      io_uring_prep_send(sqe, operation->fd_, operation->buffer_, operation->length_, MSG_NOSIGNAL);
      break;
    case OperationType::Unknown:
      Abort("UringContext prepared an operation with unknown type");
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
  const bool keep_multishot_operation = operation->awaitable_ != nullptr && (cqe->flags & IORING_CQE_F_MORE) != 0;
  switch (operation->completion_category_) {
    case Operation::CompletionCategory::Awaitable:
      ProcessAwaitableCqe(*operation, cqe);
      if (keep_multishot_operation) {
        [[maybe_unused]] Operation *released = operation.release();
      }
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
    Abort("UringContext received an awaitable CQE with no pending I/O");
  }
  const bool is_multishot = operation.awaitable_ != nullptr;
  const bool has_more = (cqe->flags & IORING_CQE_F_MORE) != 0;
  if (!is_multishot || !has_more) {
    --pending_io_operations_;
  }

  operation.result_.type_ = operation.type_;
  operation.result_.fd_ = operation.fd_;
  operation.result_.result_ = cqe->res;
  operation.result_.error_code_ = cqe->res < 0 ? -cqe->res : 0;
  if (operation.type_ == OperationType::Recv || operation.type_ == OperationType::RecvProvided ||
      operation.type_ == OperationType::Send) {
    operation.result_.bytes_transferred_ = cqe->res > 0 ? static_cast<std::size_t>(cqe->res) : 0;
  }
  if (operation.type_ == OperationType::RecvProvided) {
    operation.result_.buffer_group_ = provided_buffer_pool_->GroupId();
    const bool has_selected_buffer = (cqe->flags & IORING_CQE_F_BUFFER) != 0;
    if (cqe->res > 0 && !has_selected_buffer) {
      io_uring_cqe_seen(&ring_, cqe);
      Abort("io_uring completed a provided-buffer receive without selecting a buffer");
    }
    if (has_selected_buffer) {
      if (!provided_buffer_pool_) {
        io_uring_cqe_seen(&ring_, cqe);
        Abort("io_uring selected a buffer after its provided-buffer pool was destroyed");
      }
      const auto buffer_id = static_cast<std::uint16_t>(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
      operation.result_.buffer_ = provided_buffer_pool_->Acquire(buffer_id, operation.result_.bytes_transferred_);
    }
  }

  io_uring_cqe_seen(&ring_, cqe);
  if (is_multishot) {
    UringAwaitable *awaitable = operation.awaitable_;
    awaitable->result_ = std::move(operation.result_);
    awaitable->result_ready_ = true;
    if (!has_more) {
      awaitable->active_operation_ = nullptr;
    }
    std::coroutine_handle<> continuation = std::exchange(operation.continuation_, {});
    if (!continuation) {
      Abort("multishot receive completed without a waiting coroutine");
    }
    continuation.resume();
    if (has_more && !operation.continuation_) {
      Abort("multishot consumer must await the receive again before yielding");
    }
    return;
  }

  std::coroutine_handle<> continuation = std::exchange(operation.continuation_, {});
  if (!continuation) {
    Abort("UringContext completed an awaitable operation without a continuation");
  }
  continuation.resume();
}

void UringContext::Runtime::ProcessCancelCqe(io_uring_cqe *cqe) {
  if (pending_io_operations_ == 0) {
    io_uring_cqe_seen(&ring_, cqe);
    Abort("UringContext received a cancel CQE with no pending I/O");
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
  AssertRunThread("UringContext::CancelFd called outside the owning Run thread");

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

void UringContext::CancelFd(int fd) {
  if (fd < 0 || !runtime_->IsRunning()) {
    return;
  }
  runtime_->SubmitCancelFd(fd);
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

auto UringContext::RecvProvided(int fd) -> UringAwaitable {
  if (!runtime_->provided_buffer_pool_) {
    throw LifecycleException("UringContext::RecvProvided requires a registered provided-buffer pool");
  }
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::RecvProvided;
  operation->fd_ = fd;
  return UringAwaitable(*this, std::move(operation));
}

auto UringContext::RecvProvidedMultishot(int fd) -> UringAwaitable {
  if (!runtime_->provided_buffer_pool_) {
    throw LifecycleException("UringContext::RecvProvidedMultishot requires a registered provided-buffer pool");
  }
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::RecvProvided;
  operation->fd_ = fd;
  return UringAwaitable(*this, std::move(operation), true);
}

auto UringContext::Send(int fd, const void *buffer, std::size_t len) -> UringAwaitable {
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Send;
  operation->fd_ = fd;
  operation->buffer_ = const_cast<void *>(buffer);
  operation->length_ = len;
  return UringAwaitable(*this, std::move(operation));
}

UringAwaitable::UringAwaitable(UringContext &context, std::unique_ptr<Operation> operation, bool multishot) noexcept
    : context_(&context), unstarted_operation_(std::move(operation)), multishot_(multishot) {}

UringAwaitable::~UringAwaitable() {
  if (active_operation_ != nullptr) {
    Abort("UringAwaitable destroyed while an I/O operation is pending");
  }
}

UringAwaitable::UringAwaitable(UringAwaitable &&other) noexcept
    : context_(std::exchange(other.context_, nullptr)),
      unstarted_operation_(std::move(other.unstarted_operation_)),
      active_operation_(std::exchange(other.active_operation_, nullptr)),
      result_(std::move(other.result_)),
      multishot_(std::exchange(other.multishot_, false)),
      result_ready_(std::exchange(other.result_ready_, false)) {
  if (active_operation_ != nullptr) {
    Abort("UringAwaitable moved while an I/O operation is pending");
  }
}

auto UringAwaitable::operator=(UringAwaitable &&other) noexcept -> UringAwaitable & {
  if (this == &other) {
    return *this;
  }
  if (active_operation_ != nullptr || other.active_operation_ != nullptr) {
    Abort("UringAwaitable move-assigned while an I/O operation is pending");
  }
  context_ = std::exchange(other.context_, nullptr);
  unstarted_operation_ = std::move(other.unstarted_operation_);
  result_ = std::move(other.result_);
  multishot_ = std::exchange(other.multishot_, false);
  result_ready_ = std::exchange(other.result_ready_, false);
  return *this;
}

auto UringAwaitable::await_suspend(std::coroutine_handle<> continuation) -> bool {
  if (context_ == nullptr || (!unstarted_operation_ && active_operation_ == nullptr)) {
    Abort("UringAwaitable suspended in an invalid or already-consumed state");
  }

  if (multishot_) {
    if (active_operation_ == nullptr) {
      Operation *operation = unstarted_operation_.get();
      operation->awaitable_ = this;
      if (!context_->TryStartOperation(unstarted_operation_, continuation)) {
        return false;
      }
      active_operation_ = operation;
    } else {
      if (active_operation_->continuation_) {
        Abort("multishot receive already has a waiting coroutine");
      }
      active_operation_->continuation_ = continuation;
    }
    return true;
  }

  if (active_operation_ != nullptr || !unstarted_operation_) {
    Abort("UringAwaitable suspended in an invalid or already-consumed state");
  }
  Operation *operation = unstarted_operation_.get();
  if (!context_->TryStartOperation(unstarted_operation_, continuation)) {
    return false;
  }
  active_operation_ = operation;
  return true;
}

auto UringAwaitable::await_resume() -> IoResult {
  if (multishot_) {
    if (result_ready_) {
      result_ready_ = false;
      return std::exchange(result_, {});
    }
    if (unstarted_operation_) {
      IoResult result = std::move(unstarted_operation_->result_);
      unstarted_operation_.reset();
      return result;
    }
    Abort("multishot receive resumed without an operation result");
  }
  if (active_operation_ != nullptr) {
    IoResult result = std::move(active_operation_->result_);
    active_operation_ = nullptr;
    return result;
  }
  if (unstarted_operation_) {
    IoResult result = std::move(unstarted_operation_->result_);
    unstarted_operation_.reset();
    return result;
  }
  Abort("UringAwaitable resumed without an operation result");
}

auto UringContext::TryStartOperation(std::unique_ptr<Operation> &operation, std::coroutine_handle<> continuation)
    -> bool {
  return runtime_->TryStartAwaitableOperation(operation, continuation);
}

}  // namespace xrpc::io
