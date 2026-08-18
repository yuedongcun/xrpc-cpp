#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "io/uring_awaitable.h"

namespace xrpc::io {

class UringContext final {
 public:
  explicit UringContext(std::uint32_t entries = 256);

  ~UringContext();

  UringContext(const UringContext &) = delete;
  auto operator=(const UringContext &) -> UringContext & = delete;

  UringContext(UringContext &&) = delete;
  auto operator=(UringContext &&) -> UringContext & = delete;

  void Run();

  void Stop();

  [[nodiscard]] auto Accept(int listen_fd) -> UringAwaitable;

  [[nodiscard]] auto Recv(int fd, void *buffer, std::size_t len) -> UringAwaitable;

  [[nodiscard]] auto Send(int fd, const void *buffer, std::size_t len) -> UringAwaitable;

  [[nodiscard]] auto SleepFor(std::chrono::nanoseconds timeout) -> UringAwaitable;

  [[nodiscard]] auto Nop() -> UringAwaitable;

  void CancelFd(int fd);

  void Post(std::function<void()> fn);

 private:
  struct Runtime;

  std::unique_ptr<Runtime> runtime_;
};

}  // namespace xrpc::io
