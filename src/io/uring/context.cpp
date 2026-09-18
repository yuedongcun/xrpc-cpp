/**
 * @file context.cpp
 * @brief Implements UringContext and its coroutine I/O lifecycle.
 *
 * Awaitables own deferred operations until suspension transfers them to the
 * context's submission batch. Submitted operations live until their final CQE
 * is processed. Run() drives submission and completion on its owning thread;
 * Post() and RequestStop() provide cross-thread control through eventfd.
 *
 * One-shot I/O ends with one CQE. Multishot I/O retains its Operation while
 * MORE is set and resumes a synchronous consumer once per CQE. Shutdown returns
 * only after pending I/O and the wakeup poll have drained.
 */

#include "io/uring/context.h"

#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <exception>
#include <utility>

#include "common/abort.h"
#include "common/xrpc_exception.h"
#include "io/system_error.h"

namespace xrpc::io {

// -----------------------------------------------------------------------------
// Operation state
//
// Ownership model:
//
// Before submission, an awaitable Operation is first owned by its
// UringAwaitable, then moved into staged_operations_ by await_suspend().
// Internal operations are staged directly. In both cases, a unique_ptr owns
// the Operation until io_uring_submit() succeeds.
//
// After a successful submission, no long-lived C++ owner remains. The pending
// io_uring request carries a raw pointer in user_data, and the Operation must
// remain alive until that request produces its final CQE.
//
// ProcessCqe() temporarily adopts the Operation into a unique_ptr. For a
// non-final multishot CQE (IORING_CQE_F_MORE), it releases the unique_ptr
// because the pending request will produce another CQE. The final CQE leaves
// the unique_ptr owning the Operation, which is then destroyed.
//
// RequestStop() only initiates shutdown. Run() must drain every submitted
// Operation through its final CQE before returning and allowing ring teardown.
// -----------------------------------------------------------------------------

struct Operation {
  enum class CompletionCategory : std::uint8_t {
    Awaitable,
    Cancel,
    Wakeup,
  };

  OperationType type_ = OperationType::Unknown;
  CompletionCategory completion_category_ = CompletionCategory::Awaitable;
  int fd_ = -1;
  void *buffer_ = nullptr;
  std::size_t length_ = 0;
  IoResult result_;
  std::coroutine_handle<> continuation_;
  UringAwaitable *awaitable_ = nullptr;
  bool multishot_ = false;
};

// -----------------------------------------------------------------------------
// Resource lifetime and Run-thread ownership
// Initialize ring/pool/eventfd; only the Run thread submits and completes I/O.
// -----------------------------------------------------------------------------

UringInstance::UringInstance(std::uint32_t entries) {
  const int result = io_uring_queue_init(entries, &ring_, 0);  // Use the default setup flags.
  if (result < 0) {
    throw InternalException(MakeSystemErrorMessage("io_uring_queue_init", -result));
  }
}

UringInstance::~UringInstance() { io_uring_queue_exit(&ring_); }

WakeupEventFd::WakeupEventFd() {
  fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd_ < 0) {
    throw InternalException(MakeSystemErrorMessage("eventfd", errno));
  }
}

WakeupEventFd::~WakeupEventFd() { (void)::close(fd_); }

UringContext::UringContext(std::uint32_t entries, std::optional<UringBufferPoolConfig> buffer_pool_config)
    : uring_(entries) {
  staged_operations_.reserve(entries);

  if (buffer_pool_config.has_value()) {
    StatusOr<std::unique_ptr<UringProvidedBufferPool>> registered =
        UringProvidedBufferPool::Register(uring_.Get(), *buffer_pool_config);
    if (!registered.ok()) {
      throw InternalException(registered.status().message());
    }
    provided_buffer_pool_ = std::move(registered).value();
  }
}

UringContext::~UringContext() = default;

auto UringContext::CurrentThreadId() -> pid_t {
  thread_local const auto thread_id = static_cast<pid_t>(::syscall(SYS_gettid));
  return thread_id;
}

void UringContext::AcquireRunOwnership() {
  pid_t expected = 0;
  if (!run_thread_id_.compare_exchange_strong(expected, CurrentThreadId())) {
    Abort("UringContext::Run called while another Run thread owns the context");
  }
}

void UringContext::ReleaseRunOwnership() { run_thread_id_.store(0); }

void UringContext::AssertRunThread(std::string_view action) const {
  if (run_thread_id_.load() != CurrentThreadId()) {
    Abort(action);
  }
}

auto UringContext::IsRunning() const -> bool { return run_thread_id_.load() != 0; }

// -----------------------------------------------------------------------------
// Deferred I/O APIs
// Construction records I/O parameters. Nothing reaches the kernel until awaited.
// -----------------------------------------------------------------------------

auto UringContext::Accept(int listen_fd) -> UringAwaitable {
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Accept;
  operation->fd_ = listen_fd;
  return {*this, std::move(operation), /*multishot=*/false};
}

auto UringContext::AcceptMultishot(int listen_fd) -> UringAwaitable {
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Accept;
  operation->fd_ = listen_fd;
  return {*this, std::move(operation), /*multishot=*/true};
}

auto UringContext::Recv(int fd, void *buffer, std::size_t len) -> UringAwaitable {
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Recv;
  operation->fd_ = fd;
  operation->buffer_ = buffer;
  operation->length_ = len;
  return {*this, std::move(operation), /*multishot=*/false};
}

auto UringContext::RecvProvided(int fd) -> UringAwaitable {
  if (!provided_buffer_pool_) {
    throw LifecycleException("UringContext::RecvProvided requires a registered provided-buffer pool");
  }
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::RecvProvided;
  operation->fd_ = fd;
  return {*this, std::move(operation), /*multishot=*/false};
}

auto UringContext::Send(int fd, const void *buffer, std::size_t len) -> UringAwaitable {
  auto operation = std::make_unique<Operation>();
  operation->type_ = OperationType::Send;
  operation->fd_ = fd;
  operation->buffer_ = const_cast<void *>(buffer);
  operation->length_ = len;
  return {*this, std::move(operation), /*multishot=*/false};
}

// -----------------------------------------------------------------------------
// Coroutine handoff: await_suspend -> completion -> await_resume
// An Operation survives until its final CQE; every result returns through its awaitable.
// -----------------------------------------------------------------------------

UringAwaitable::UringAwaitable(UringContext &context, std::unique_ptr<Operation> operation, bool multishot) noexcept
    : context_(context), owned_operation_(std::move(operation)) {
  owned_operation_->multishot_ = multishot;
}

UringAwaitable::~UringAwaitable() {
  if (handed_off_operation_ != nullptr) {
    Abort("UringAwaitable destroyed while an I/O operation is pending");
  }
}

auto UringAwaitable::await_ready() const noexcept -> bool { return false; }

auto UringAwaitable::await_suspend(std::coroutine_handle<> continuation) -> bool {
  if (handed_off_operation_ != nullptr) {
    if (!handed_off_operation_->multishot_) {
      Abort("one-shot UringAwaitable awaited again while its operation is pending");
    }
    if (handed_off_operation_->continuation_) {
      Abort("multishot UringAwaitable already has a waiting coroutine");
    }
    handed_off_operation_->continuation_ = continuation;
    return true;
  }

  if (!owned_operation_) {
    Abort("UringAwaitable awaited after its operation was consumed");
  }
  Operation *operation = owned_operation_.get();
  operation->awaitable_ = this;
  if (!context_.TryStartOperation(owned_operation_, continuation)) {
    // A rejected start resumes synchronously through the common result slot.
    auto rejected_operation = std::move(owned_operation_);
    result_ = std::move(rejected_operation->result_);
    result_ready_ = true;
    return false;
  }
  handed_off_operation_ = operation;
  return true;
}

auto UringAwaitable::await_resume() -> IoResult {
  if (!result_ready_) {
    Abort("UringAwaitable resumed without an operation result");
  }
  result_ready_ = false;
  return std::exchange(result_, {});
}

// -----------------------------------------------------------------------------
// SQE preparation and batched submission
// Staging owns each Operation until io_uring_submit publishes its SQE.
// -----------------------------------------------------------------------------

auto UringContext::AcquireSqe() -> io_uring_sqe * {
  io_uring_sqe *sqe = io_uring_get_sqe(&uring_.Get());
  if (sqe == nullptr && !staged_operations_.empty()) {
    FlushSubmissionBatch();
    sqe = io_uring_get_sqe(&uring_.Get());
  }
  if (sqe == nullptr) {
    throw InternalException("io_uring_get_sqe failed");
  }
  return sqe;
}

/** @brief Stages a prepared operation for the next submission flush. */
void UringContext::SubmitPreparedOperation(std::unique_ptr<Operation> operation) noexcept {
  assert(staged_operations_.size() < staged_operations_.capacity());
  ++pending_operations_;
  staged_operations_.push_back(std::move(operation));
}

void UringContext::FlushSubmissionBatch() {
  while (!staged_operations_.empty()) {
    int ret = 0;
    do {
      ret = io_uring_submit(&uring_.Get());
    } while (ret == -EINTR);

    if (ret < 0) {
      throw InternalException(MakeSystemErrorMessage("io_uring_submit", -ret));
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
auto UringContext::TryStartOperation(std::unique_ptr<Operation> &operation, std::coroutine_handle<> continuation)
    -> bool {
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

  switch (operation->type_) {
    case OperationType::Accept:
      if (operation->multishot_) {
        io_uring_prep_multishot_accept(sqe, operation->fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      } else {
        io_uring_prep_accept(sqe, operation->fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      }
      break;
    case OperationType::Recv:
      io_uring_prep_recv(sqe, operation->fd_, operation->buffer_, operation->length_, 0);
      break;
    case OperationType::RecvProvided:
      if (!provided_buffer_pool_) {
        Abort("UringContext attempted a provided-buffer receive without a registered pool");
      }
      io_uring_prep_recv(sqe, operation->fd_, nullptr, provided_buffer_pool_->BufferSize(), 0);
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

  SubmitPreparedOperation(std::move(operation));
  return true;
}

// -----------------------------------------------------------------------------
// CQE consumption and coroutine resumption
// Each CQE temporarily takes ownership; only a non-final multishot CQE releases it.
// -----------------------------------------------------------------------------

void UringContext::ProcessCqe(io_uring_cqe *cqe) {
  auto *raw_operation = static_cast<Operation *>(io_uring_cqe_get_data(cqe));
  if (raw_operation == nullptr) {
    io_uring_cqe_seen(&uring_.Get(), cqe);
    return;
  }

  std::unique_ptr<Operation> operation(raw_operation);
  const bool has_more = (cqe->flags & IORING_CQE_F_MORE) != 0;
  const bool is_final = !operation->multishot_ || !has_more;
  if (pending_operations_ == 0) {
    io_uring_cqe_seen(&uring_.Get(), cqe);
    Abort("UringContext received an operation CQE with no pending operation");
  }
  if (is_final) {
    --pending_operations_;
  }
  switch (operation->completion_category_) {
    case Operation::CompletionCategory::Awaitable:
      ProcessAwaitableCqe(*operation, cqe);
      if (!is_final) {
        [[maybe_unused]] Operation *released = operation.release();
      }
      return;
    case Operation::CompletionCategory::Cancel:
      ProcessCancelCqe(cqe);
      return;
    case Operation::CompletionCategory::Wakeup:
      ProcessWakeupCqe(cqe, has_more);
      if (!is_final) {
        [[maybe_unused]] Operation *released = operation.release();
      }
      return;
  }
}

void UringContext::ProcessAwaitableCqe(Operation &operation, io_uring_cqe *cqe) {
  const bool is_multishot = operation.multishot_;
  const bool has_more = (cqe->flags & IORING_CQE_F_MORE) != 0;
  const bool is_final = !is_multishot || !has_more;
  if (operation.type_ == OperationType::RecvProvided && cqe->res == -ENOBUFS) {
    ++counters_.provided_buffer_enobufs_;
  }
  operation.result_.type_ = operation.type_;
  operation.result_.fd_ = operation.fd_;
  operation.result_.result_ = cqe->res;
  operation.result_.error_code_ = cqe->res < 0 ? -cqe->res : 0;
  operation.result_.has_more_ = is_multishot && has_more;
  if (operation.type_ == OperationType::Recv || operation.type_ == OperationType::RecvProvided ||
      operation.type_ == OperationType::Send) {
    operation.result_.bytes_transferred_ = cqe->res > 0 ? static_cast<std::size_t>(cqe->res) : 0;
  }
  if (operation.type_ == OperationType::RecvProvided) {
    operation.result_.buffer_group_ = provided_buffer_pool_->GroupId();
    const bool has_selected_buffer = (cqe->flags & IORING_CQE_F_BUFFER) != 0;
    if (cqe->res > 0 && !has_selected_buffer) {
      io_uring_cqe_seen(&uring_.Get(), cqe);
      Abort("io_uring completed a provided-buffer receive without selecting a buffer");
    }
    if (has_selected_buffer) {
      if (!provided_buffer_pool_) {
        io_uring_cqe_seen(&uring_.Get(), cqe);
        Abort("io_uring selected a buffer after its provided-buffer pool was destroyed");
      }
      const auto buffer_id = static_cast<std::uint16_t>(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
      operation.result_.buffer_ = provided_buffer_pool_->Acquire(buffer_id, operation.result_.bytes_transferred_);
    }
  }

  io_uring_cqe_seen(&uring_.Get(), cqe);
  UringAwaitable *awaitable = operation.awaitable_;
  if (awaitable == nullptr) {
    Abort("UringContext completed an operation without an awaitable");
  }
  awaitable->result_ = std::move(operation.result_);
  awaitable->result_ready_ = true;
  if (is_final) {
    awaitable->handed_off_operation_ = nullptr;
  }

  std::coroutine_handle<> continuation = std::exchange(operation.continuation_, {});
  if (!continuation) {
    Abort("UringContext completed an awaitable operation without a continuation");
  }
  continuation.resume();
  if (!is_final && !operation.continuation_) {
    Abort("multishot consumer must await the operation again before yielding");
  }
}

void UringContext::ProcessCancelCqe(io_uring_cqe *cqe) {
  IoResult result;
  result.result_ = cqe->res;
  result.error_code_ = cqe->res < 0 ? -cqe->res : 0;
  io_uring_cqe_seen(&uring_.Get(), cqe);
  if (result.result_ < 0 && result.error_code_ != ENOENT && result.error_code_ != EALREADY &&
      result.error_code_ != ECANCELED) {
    throw InternalException(MakeSystemErrorMessage("io_uring cancel", result.error_code_));
  }
}

auto UringContext::MakeCancelledResult(const Operation &operation) -> IoResult {
  IoResult result;
  result.type_ = operation.type_;
  result.fd_ = operation.fd_;
  result.result_ = -ECANCELED;
  result.error_code_ = ECANCELED;
  result.bytes_transferred_ = 0;
  return result;
}

// -----------------------------------------------------------------------------
// Cancellation submission
// ProcessCancelCqe() acknowledges requests; ProcessAwaitableCqe() completes the original I/O.
// -----------------------------------------------------------------------------

void UringContext::SubmitCancelFd(int fd) {
  AssertRunThread("UringContext::CancelFd called outside the owning Run thread");

  auto operation = std::make_unique<Operation>();
  operation->completion_category_ = Operation::CompletionCategory::Cancel;
  operation->fd_ = fd;

  io_uring_sqe *sqe = AcquireSqe();

  io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
  Operation *raw_operation = operation.get();
  io_uring_sqe_set_data(sqe, raw_operation);

  SubmitPreparedOperation(std::move(operation));

  // Callers close the descriptor immediately after CancelFd() returns. Publish
  // the cancellation before that close instead of waiting for the turn boundary.
  FlushSubmissionBatch();
}

void UringContext::CancelFd(int fd) {
  if (fd < 0 || !IsRunning()) {
    return;
  }
  SubmitCancelFd(fd);
}

// -----------------------------------------------------------------------------
// Cross-thread callbacks and eventfd wakeup
// Post/RequestStop enqueue or signal; callbacks and poll CQEs run on the owner thread.
// -----------------------------------------------------------------------------

void UringContext::Post(std::function<void()> fn) {
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

void UringContext::RequestStop() {
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

void UringContext::DrainPosted() {
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
 * One multishot wakeup poll remains pending while the context runs. Its
 * operation is released to the completion path and survives each CQE carrying
 * MORE; shutdown explicitly cancels it to obtain the final CQE.
 */
void UringContext::SubmitWakeupPoll() {
  AssertRunThread("wakeup poll submission attempted outside the owning Run thread");
  if (wakeup_poll_pending_) {
    Abort("UringContext attempted to arm a second wakeup poll");
  }

  auto operation = std::make_unique<Operation>();
  operation->completion_category_ = Operation::CompletionCategory::Wakeup;
  operation->fd_ = wakeup_.Get();
  operation->multishot_ = true;

  io_uring_sqe *sqe = AcquireSqe();

  io_uring_prep_poll_multishot(sqe, wakeup_.Get(), POLLIN);
  Operation *raw_operation = operation.get();
  io_uring_sqe_set_data(sqe, raw_operation);

  SubmitPreparedOperation(std::move(operation));
  wakeup_poll_pending_ = true;
}

/**
 * @brief Handles completion of the eventfd wakeup poll.
 *
 * The wakeup counter and posted callbacks are drained first. The multishot
 * request stays armed during normal operation. During shutdown it is cancelled
 * and its final CQE releases the operation.
 */
void UringContext::ProcessWakeupCqe(io_uring_cqe *cqe, bool has_more) {
  wakeup_poll_pending_ = has_more;

  const int result = cqe->res;
  const int error_code = result < 0 ? -result : 0;
  io_uring_cqe_seen(&uring_.Get(), cqe);

  if (result < 0) {
    if (stop_requested_.load() && error_code == ECANCELED && !has_more) {
      wakeup_poll_cancel_requested_ = false;
      return;
    }
    throw InternalException(MakeSystemErrorMessage("eventfd poll", error_code));
  }
  if ((result & POLLIN) == 0) {
    throw InternalException("eventfd poll completed without POLLIN");
  }

  DrainWakeupCounter();
  DrainPosted();
  if (stop_requested_.load()) {
    if (has_more && !wakeup_poll_cancel_requested_) {
      wakeup_poll_cancel_requested_ = true;
      SubmitCancelFd(wakeup_.Get());
    }
  } else if (!has_more) {
    SubmitWakeupPoll();
  }
}

void UringContext::SignalWakeup() const {
  constexpr std::uint64_t value = 1;
  while (true) {
    const ssize_t written = ::write(wakeup_.Get(), &value, sizeof(value));
    if (std::cmp_equal(written, sizeof(value))) {
      return;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    if (written < 0 && errno == EAGAIN) {
      return;
    }
    throw InternalException(MakeSystemErrorMessage("eventfd write", errno));
  }
}

void UringContext::DrainWakeupCounter() const {
  std::uint64_t value = 0;
  while (true) {
    const ssize_t read_size = ::read(wakeup_.Get(), &value, sizeof(value));
    if (std::cmp_equal(read_size, sizeof(value))) {
      return;
    }
    if (read_size < 0 && errno == EINTR) {
      continue;
    }
    if (read_size < 0 && errno == EAGAIN) {
      return;
    }
    throw InternalException(MakeSystemErrorMessage("eventfd read", errno));
  }
}

// -----------------------------------------------------------------------------
// Statistics snapshots
// Copy on the owning thread; formatting and aggregation belong to the caller.
// -----------------------------------------------------------------------------

auto UringContext::SnapshotStats(bool start_window) -> UringStatsSnapshot {
  AssertRunThread("UringContext::SnapshotStats called outside the owning Run thread");
  if (start_window) {
    ++stats_window_id_;
  }
  return {.counters_ = counters_,
          .window_id_ = stats_window_id_,
          .buffer_pool_ =
              provided_buffer_pool_ ? std::optional{provided_buffer_pool_->SnapshotStats(start_window)} : std::nullopt};
}

// -----------------------------------------------------------------------------
// Event loop
// Process a bounded CQE batch, then flush staged SQEs until shutdown has drained.
// -----------------------------------------------------------------------------

void UringContext::Run() {
  AcquireRunOwnership();
  struct RunOwnership final {
    UringContext &context_;
    ~RunOwnership() { context_.ReleaseRunOwnership(); }
  } run_ownership{*this};

  SubmitWakeupPoll();
  FlushSubmissionBatch();

  while (!stop_requested_.load() || pending_operations_ > 0) {
    // No operation may remain staged while the event loop blocks.
    assert(staged_operations_.empty());
    io_uring_cqe *cqe = nullptr;
    const int ret = io_uring_wait_cqe(&uring_.Get(), &cqe);
    if (ret < 0) {
      if (ret == -EINTR) {
        continue;
      }
      throw InternalException(MakeSystemErrorMessage("io_uring_wait_cqe", -ret));
    }

    // Bound one event-loop turn so newly staged Recv/Send/Accept operations
    // cannot be starved by a continuously replenished completion queue.
    const std::size_t completion_budget = staged_operations_.capacity();
    std::size_t processed_cqes = 0;
    try {
      ProcessCqe(cqe);
      ++processed_cqes;
      while (processed_cqes < completion_budget && io_uring_peek_cqe(&uring_.Get(), &cqe) == 0) {
        ProcessCqe(cqe);
        ++processed_cqes;
      }
    } catch (const std::exception &) {  // XRPC_EXCEPTION_GUARD: flush staged SQEs before propagation
      FlushSubmissionBatch();
      throw;
    }
    FlushSubmissionBatch();
  }
}

}  // namespace xrpc::io
