/**
 * @file uring_context.h
 * @brief Declares xRPC's single-threaded io_uring event loop.
 *
 * A `UringContext` owns one io_uring ring and drives asynchronous operations
 * on the thread running `Run()`. `Accept`, `Recv`, `Send`, and `SleepFor`
 * submit work on that thread and return move-only awaitables that resume
 * the awaiting coroutine with an `IoResult`.
 *
 * `Post()` and `Stop()` form the cross-thread control boundary. They wake the
 * event loop safely, but callbacks themselves always execute on the run thread.
 * `CancelFd()` and all awaitable I/O submission must run on that same thread.
 */

#pragma once

#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

namespace xrpc::io {

enum class OperationType : std::uint8_t {
  Unknown = 0,
  Accept,
  Recv,
  Send,
  Timeout,
};

struct IoResult {
  OperationType type_ = OperationType::Unknown;
  int fd_ = -1;
  int result_ = 0;
  int error_code_ = 0;
  std::size_t bytes_transferred_ = 0;
};

struct Operation;

namespace detail {

struct AwaitableState {
  IoResult result_{};
  bool ready_ = false;
  std::coroutine_handle<> continuation_;
};

}  // namespace detail

/**
 * @brief Move-only result of an I/O submission for one coroutine awaiter.
 *
 * An awaitable has one awaiter and is used on the `UringContext` run thread.
 * Its completion state is updated by that thread before the coroutine resumes.
 */
class UringAwaitable final {
 public:
  ~UringAwaitable() = default;

  UringAwaitable(const UringAwaitable &) = delete;
  auto operator=(const UringAwaitable &) -> UringAwaitable & = delete;

  UringAwaitable(UringAwaitable &&other) noexcept = default;
  auto operator=(UringAwaitable &&other) noexcept -> UringAwaitable & = default;

  auto await_ready() const noexcept -> bool { return state_->ready_; }

  auto await_suspend(std::coroutine_handle<> continuation) noexcept -> bool {
    state_->continuation_ = continuation;
    return !state_->ready_;
  }

  auto await_resume() -> IoResult { return state_->result_; }

 private:
  explicit UringAwaitable(std::shared_ptr<detail::AwaitableState> state) noexcept : state_(std::move(state)) {}

  friend class UringContext;

  std::shared_ptr<detail::AwaitableState> state_;
};

/**
 * @brief Single-threaded io_uring execution context with cross-thread control.
 *
 * `Run()` has one owner. `Post()` and `Stop()` may be called concurrently from
 * other threads; `Accept()`, `Recv()`, `Send()`, `SleepFor()`, and `CancelFd()`
 * are run-thread-only operations.
 */
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

  void CancelFd(int fd);

  void Post(std::function<void()> fn);

 private:
  struct Runtime;

  std::unique_ptr<Runtime> runtime_;
};

}  // namespace xrpc::io
