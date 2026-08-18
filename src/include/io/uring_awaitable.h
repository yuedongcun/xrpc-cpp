#pragma once

#include <coroutine>

#include "io/io_result.h"

namespace xrpc::io {

struct Operation;

namespace detail {

struct AwaitableState {
  IoResult result_{};

  bool ready_ = false;

  std::coroutine_handle<> continuation_;

  Operation *operation_ = nullptr;
};

}  // namespace detail

class UringAwaitable final {
 public:
  UringAwaitable() = default;

  ~UringAwaitable();

  UringAwaitable(const UringAwaitable &) = delete;
  auto operator=(const UringAwaitable &) -> UringAwaitable & = delete;

  UringAwaitable(UringAwaitable &&other) noexcept;

  auto operator=(UringAwaitable &&other) noexcept -> UringAwaitable &;

  auto await_ready() const noexcept -> bool { return state_.ready_; }

  auto await_suspend(std::coroutine_handle<> continuation) noexcept -> bool {
    state_.continuation_ = continuation;
    return !state_.ready_;
  }

  auto await_resume() -> IoResult { return state_.result_; }

  void Bind(Operation &operation) noexcept;

  [[nodiscard]] auto state() noexcept -> detail::AwaitableState & { return state_; }

 private:
  void Detach() noexcept;

  detail::AwaitableState state_;
};

}  // namespace xrpc::io
