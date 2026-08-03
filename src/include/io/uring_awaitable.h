#pragma once

#include <coroutine>

#include "io/io_result.h"

namespace xrpc::io {

struct Operation;

namespace detail {

/**
 * @brief Shared link between a coroutine awaitable and an in-flight `Operation`.
 *
 * The `Operation` lives in `UringContext::Runtime`; the awaitable may move or be destroyed before completion, so both
 * sides must detach defensively.
 */
struct AwaitableState {
  /** @brief Completion result stored before the coroutine resumes. */
  IoResult result_{};

  /** @brief True when the kernel completion has already been delivered. */
  bool ready_ = false;

  /** @brief Coroutine continuation waiting on the completion. */
  std::coroutine_handle<> continuation_;

  /** @brief Back pointer to the runtime-owned operation, cleared during detach. */
  Operation *operation_ = nullptr;
};

}  // namespace detail

/**
 * @brief Coroutine awaitable returned by `UringContext` submission APIs.
 *
 * Design note:
 * - Ownership: `UringContext::Runtime` owns `Operation`; `UringAwaitable` owns only the coroutine-facing
 *   `AwaitableState`.
 * - Move: moving an awaitable rewires the `Operation` to the new state so callers can return awaitables by value.
 * - Lifetime: destruction detaches from `Operation`, preventing a late CQE from resuming a dead coroutine frame.
 */
class UringAwaitable final {
 public:
  UringAwaitable() = default;

  /** @brief Detaches from any in-flight operation. */
  ~UringAwaitable();

  UringAwaitable(const UringAwaitable &) = delete;
  auto operator=(const UringAwaitable &) -> UringAwaitable & = delete;

  /** @brief Moves awaitable state and rewires any bound operation. */
  UringAwaitable(UringAwaitable &&other) noexcept;

  /** @brief Replaces this awaitable state and rewires any bound operation. */
  auto operator=(UringAwaitable &&other) noexcept -> UringAwaitable &;

  /** @return true when the operation completed before suspension. */
  auto await_ready() const noexcept -> bool { return state_.ready_; }

  /** @brief Stores the continuation that should resume when the CQE arrives. */
  auto await_suspend(std::coroutine_handle<> continuation) noexcept -> bool {
    state_.continuation_ = continuation;
    return !state_.ready_;
  }

  /** @return Completion payload produced by the operation. */
  auto await_resume() -> IoResult { return state_.result_; }

  /** @brief Binds this awaitable to a runtime-owned operation. */
  void Bind(Operation &operation) noexcept;

  /** @return Mutable awaitable state used by the runtime completion path. */
  [[nodiscard]] auto state() noexcept -> detail::AwaitableState & { return state_; }

 private:
  /** @brief Detaches this awaitable from a runtime-owned operation. */
  void Detach() noexcept;

  detail::AwaitableState state_;
};

}  // namespace xrpc::io
