#pragma once

#include <chrono>
#include <exception>
#include <thread>

#include "common/task.h"
#include "io/uring_context.h"

namespace xrpc::testsupport {

class UringContextRunner final {
 public:
  UringContextRunner() = default;
  ~UringContextRunner();

  UringContextRunner(const UringContextRunner &) = delete;
  auto operator=(const UringContextRunner &) -> UringContextRunner & = delete;

  UringContextRunner(UringContextRunner &&) = delete;
  auto operator=(UringContextRunner &&) -> UringContextRunner & = delete;

  void Start(io::UringContext &context);
  void StopAndJoin(io::UringContext &context);

 private:
  std::jthread thread_;
  std::exception_ptr error_;
};

template <typename T>
void StartTaskOnContext(io::UringContext &context, runtime::Task<T> &task) {
  context.Post([&task]() { task.Start(); });
}

auto WaitTaskDone(runtime::Task<void> &task, std::chrono::milliseconds timeout, std::chrono::milliseconds poll_interval)
    -> bool;

}  // namespace xrpc::testsupport
