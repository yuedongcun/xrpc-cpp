#include "io/uring_awaitable.h"

#include <utility>

#include "io/operation.h"

namespace xrpc::io {

UringAwaitable::~UringAwaitable() { Detach(); }

UringAwaitable::UringAwaitable(UringAwaitable &&other) noexcept
    : state_(std::exchange(other.state_, detail::AwaitableState{})) {
  if (state_.operation_ != nullptr) {
    state_.operation_->awaitable_state_ = &state_;
  }
}

auto UringAwaitable::operator=(UringAwaitable &&other) noexcept -> UringAwaitable & {
  if (this != &other) {
    Detach();
    state_ = std::exchange(other.state_, detail::AwaitableState{});
    if (state_.operation_ != nullptr) {
      state_.operation_->awaitable_state_ = &state_;
    }
  }

  return *this;
}

void UringAwaitable::Bind(Operation &operation) noexcept {
  Detach();
  state_.operation_ = &operation;
  operation.awaitable_state_ = &state_;
}

void UringAwaitable::Detach() noexcept {
  if (state_.operation_ != nullptr && state_.operation_->awaitable_state_ == &state_) {
    state_.operation_->awaitable_state_ = nullptr;
  }
  state_.operation_ = nullptr;
}

}  // namespace xrpc::io
