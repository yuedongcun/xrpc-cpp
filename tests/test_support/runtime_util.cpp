#include "test_support/runtime_util.h"

#include <xrpc/xrpc_exception.h>

namespace xrpc::testsupport {

UringContextRunner::~UringContextRunner() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

void UringContextRunner::Start(io::UringContext &context) {
  if (thread_.joinable()) {
    throw LifecycleException("UringContextRunner already started");
  }

  error_ = nullptr;
  thread_ = std::jthread([&context, this]() {
    try {
      context.Run();
    } catch (...) {
      error_ = std::current_exception();
    }
  });
}

void UringContextRunner::StopAndJoin(io::UringContext &context) {
  context.Stop();
  if (thread_.joinable()) {
    thread_.join();
  }
  if (error_) {
    std::rethrow_exception(error_);
  }
}

auto WaitTaskDone(runtime::Task<void> &task, std::chrono::milliseconds timeout, std::chrono::milliseconds poll_interval)
    -> bool {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!task.Done() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(poll_interval);
  }
  return task.Done();
}

}  // namespace xrpc::testsupport
