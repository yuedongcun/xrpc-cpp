#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "io/uring_awaitable.h"

namespace xrpc::io {

/**
 * @brief Single-threaded io_uring event-loop runtime.
 *
 * Design note:
 * - Ownership: `Runtime` owns the io_uring ring, eventfd, submitted operations, and posted callback queue.
 * - Threading: I/O submissions happen only on `Run()`'s thread; other threads enter through `Post()` or `Stop()`, which
 *   wake the loop via eventfd.
 * - Completion: CQEs resume the coroutine bound to each `Operation`; cancellation completes through the same path.
 * - Shutdown: `Stop()` prevents new posts, wakes `Run()`, and lets the loop drain or cancel outstanding operations
 *   before returning.
 */
class UringContext final {
 public:
  /** @brief Creates an io_uring runtime with the requested queue depth. */
  explicit UringContext(std::uint32_t entries = 256);

  /** @brief Stops and releases runtime resources. */
  ~UringContext();

  UringContext(const UringContext &) = delete;
  auto operator=(const UringContext &) -> UringContext & = delete;

  UringContext(UringContext &&) = delete;
  auto operator=(UringContext &&) -> UringContext & = delete;

  /**
   * @brief Runs the event loop on the current thread.
   *
   * `Run()` owns the ring until `Stop()` is requested and all submitted operations have completed or been canceled. It
   * is not reentrant.
   */
  void Run();

  /**
   * @brief Requests event-loop shutdown.
   *
   * `Stop()` is safe to call from another thread. It prevents new posts from being accepted and wakes `Run()` so
   * shutdown can make progress.
   */
  void Stop();

  /** @brief Submits an async accept operation. Must be called on the `Run()` thread. */
  [[nodiscard]] auto Accept(int listen_fd) -> UringAwaitable;

  /** @brief Submits an async receive operation. Must be called on the `Run()` thread. */
  [[nodiscard]] auto Recv(int fd, void *buffer, std::size_t len) -> UringAwaitable;

  /** @brief Submits an async send operation. Must be called on the `Run()` thread. */
  [[nodiscard]] auto Send(int fd, const void *buffer, std::size_t len) -> UringAwaitable;

  /** @brief Submits an async timeout operation. Must be called on the `Run()` thread. */
  [[nodiscard]] auto SleepFor(std::chrono::nanoseconds timeout) -> UringAwaitable;

  /** @brief Submits an async no-op operation. Must be called on the `Run()` thread. */
  [[nodiscard]] auto Nop() -> UringAwaitable;

  /**
   * @brief Cancels outstanding operations for `fd`.
   *
   * This is a run-thread operation. Cross-thread callers should `Post()` a callback that performs cancellation on the
   * event loop.
   */
  void CancelFd(int fd);

  /**
   * @brief Posts a callback to run on the event-loop thread.
   *
   * Callbacks run on the `Run()` thread and may submit I/O, post more work, or call `Stop()`.
   */
  void Post(std::function<void()> fn);

 private:
  struct Runtime;

  std::unique_ptr<Runtime> runtime_;
};

}  // namespace xrpc::io
