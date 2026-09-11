/**
 * @file uring_context.h
 * @brief Declares xRPC's single-threaded io_uring event loop.
 *
 * A `UringContext` owns one io_uring ring and drives asynchronous operations
 * on the thread running `Run()`. `Accept`, `Recv`, `RecvProvided`, and `Send`
 * create deferred, move-only awaitables. The operation starts when the
 * coroutine suspends and resumes that coroutine with an `IoResult`.
 *
 * `Post()` and `RequestStop()` form the cross-thread control boundary. They wake the
 * event loop safely, but callbacks themselves always execute on the run thread.
 * `CancelFd()` and all awaitable I/O starts must run on that same thread.
 */

#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

#include "io/uring_buffer_pool.h"

namespace xrpc::io {

enum class OperationType : std::uint8_t {
  Unknown = 0,
  Accept,
  Recv,
  RecvProvided,
  Send,
};

struct IoResult {
  IoResult() = default;

  IoResult(const IoResult &) = delete;
  auto operator=(const IoResult &) -> IoResult & = delete;

  IoResult(IoResult &&) noexcept = default;
  auto operator=(IoResult &&) noexcept -> IoResult & = default;

  OperationType type_ = OperationType::Unknown;
  int fd_ = -1;
  int result_ = 0;
  int error_code_ = 0;
  std::size_t bytes_transferred_ = 0;
  // Identifies the provided-buffer group even when receive fails without a buffer.
  std::uint16_t buffer_group_ = 0;
  UringBuffer buffer_;
};

struct Operation;
class UringContext;

/**
 * @brief Move-only result of an I/O submission for one coroutine awaiter.
 *
 * An awaitable owns one unstarted operation. `await_suspend()` transfers that
 * operation to the `UringContext`; `await_resume()` moves the completed result
 * out of the operation. Destroying an awaitable while its operation is pending
 * is a programming error.
 */
class UringAwaitable final {
 public:
  ~UringAwaitable();

  UringAwaitable(const UringAwaitable &) = delete;
  auto operator=(const UringAwaitable &) -> UringAwaitable & = delete;

  UringAwaitable(UringAwaitable &&other) noexcept;
  auto operator=(UringAwaitable &&other) noexcept -> UringAwaitable &;

  auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> continuation) -> bool;

  auto await_resume() -> IoResult;

 private:
  explicit UringAwaitable(UringContext &context, std::unique_ptr<Operation> operation) noexcept;

  friend class UringContext;

  UringContext *context_ = nullptr;
  std::unique_ptr<Operation> unstarted_operation_;
  Operation *active_operation_ = nullptr;
};

/**
 * @brief Single-threaded io_uring execution context with cross-thread control.
 *
 * `Run()` has one owner. `Post()` and `RequestStop()` may be called concurrently
 * from other threads. Awaitable construction is deferred; `Accept()`, `Recv()`,
 * `RecvProvided()`, and `Send()` start on the run thread when awaited.
 * `CancelFd()` is also a run-thread-only operation.
 */
class UringContext final {
 public:
  explicit UringContext(std::uint32_t entries = 256);

  UringContext(std::uint32_t entries, UringBufferPoolConfig buffer_pool_config);

  ~UringContext();

  UringContext(const UringContext &) = delete;
  auto operator=(const UringContext &) -> UringContext & = delete;

  UringContext(UringContext &&) = delete;
  auto operator=(UringContext &&) -> UringContext & = delete;

  /**
   * @brief Runs the event loop on the calling thread.
   *
   * Returns after stop has been requested and all submitted operations and the
   * wakeup poll have produced their completion events.
   */
  void Run();

  /**
   * @brief Thread-safely requests event-loop shutdown without waiting.
   *
   * New posted callbacks are rejected, callbacks already queued are drained,
   * and the run thread is awakened. Return from `Run()` confirms shutdown.
   */
  void RequestStop();

  [[nodiscard]] auto Accept(int listen_fd) -> UringAwaitable;

  [[nodiscard]] auto Recv(int fd, void *buffer, std::size_t len) -> UringAwaitable;

  /**
   * @brief Receives into a buffer selected from this context's registered pool.
   *
   * The context must have been constructed with a UringBufferPoolConfig. A
   * successful result owns the selected buffer through `IoResult::buffer_`.
   */
  [[nodiscard]] auto RecvProvided(int fd) -> UringAwaitable;

  [[nodiscard]] auto Send(int fd, const void *buffer, std::size_t len) -> UringAwaitable;

  void CancelFd(int fd);

  void Post(std::function<void()> fn);

 private:
  friend class UringAwaitable;

  struct Runtime;

  [[nodiscard]] auto TryStartOperation(std::unique_ptr<Operation> &operation, std::coroutine_handle<> continuation)
      -> bool;

  std::unique_ptr<Runtime> runtime_;
};

}  // namespace xrpc::io
