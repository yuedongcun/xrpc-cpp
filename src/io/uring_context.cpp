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

#include "common/xrpc_exception.h"
#include "io/uring_context_runtime.h"

namespace xrpc::io {

auto UringContext::Runtime::MakeErrorMessage(std::string_view action, int error_code) -> std::string {
  std::string message(action);
  message.append(" failed");
  if (error_code != 0) {
    message.append(": ");
    message.append(std::error_code(error_code, std::generic_category()).message());
  }
  return message;
}

auto UringContext::Runtime::CurrentThreadToken() -> const void * {
  static thread_local const char token = 0;
  return &token;
}

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

UringContext::Runtime::~Runtime() {
  if (wakeup_fd_ >= 0) {
    (void)::close(wakeup_fd_);
  }
  io_uring_queue_exit(&ring_);
}

void UringContext::Runtime::BeginRun() {
  const void *expected = nullptr;
  if (!run_thread_token_.compare_exchange_strong(expected, CurrentThreadToken())) {
    throw LifecycleException("UringContext::Run is not reentrant");
  }
}

void UringContext::Runtime::EndRun() { run_thread_token_.store(nullptr); }

void UringContext::Runtime::AssertRunThread(std::string_view action) const {
  if (run_thread_token_.load() != CurrentThreadToken()) {
    throw LifecycleException(std::string(action) + " must run on the UringContext thread");
  }
}

auto UringContext::Runtime::IsRunning() const -> bool { return run_thread_token_.load() != nullptr; }

UringContext::UringContext(std::uint32_t entries) : runtime_(std::make_unique<Runtime>(entries)) {}

UringContext::~UringContext() = default;

void UringContext::Run() {
  runtime_->BeginRun();
  try {
    runtime_->SubmitWakeupPoll();

    while (!runtime_->stop_requested_.load() || runtime_->pending_io_operations_ > 0 ||
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

void UringContext::Stop() { runtime_->RequestStop(); }

void UringContext::CancelFd(int fd) {
  if (fd < 0 || !runtime_->IsRunning()) {
    return;
  }
  runtime_->SubmitCancelFd(fd);
}

void UringContext::Post(std::function<void()> fn) { runtime_->EnqueuePosted(std::move(fn)); }

}  // namespace xrpc::io
