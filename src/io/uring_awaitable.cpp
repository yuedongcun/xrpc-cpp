#include "io/uring_awaitable.h"

#include <utility>

#include "io/operation.h"

namespace xrpc::io {

/**
 * @brief Detaches the awaitable from any in-flight operation before destruction.
 */
UringAwaitable::~UringAwaitable() { Detach(); }

/**
 * @brief Moves awaitable state and rewires the linked operation back pointer.
 *
 * @param other Awaitable whose state should move into this object.
 */
UringAwaitable::UringAwaitable(UringAwaitable &&other) noexcept
    : state_(std::exchange(other.state_, detail::AwaitableState{})) {
  if (state_.operation_ != nullptr) {
    // The kernel operation keeps a raw pointer to the awaitable state, so moves
    // must retarget that pointer before the old state goes out of scope.
    state_.operation_->awaitable_state_ = &state_;
  }
}

/**
 * @brief Replaces this awaitable state with another awaitable's state.
 *
 * @param other Awaitable whose state should move into this object.
 * @return This awaitable.
 */
auto UringAwaitable::operator=(UringAwaitable &&other) noexcept -> UringAwaitable & {
  if (this != &other) {
    Detach();
    state_ = std::exchange(other.state_, detail::AwaitableState{});
    if (state_.operation_ != nullptr) {
      // See the move constructor: the in-flight operation follows the new
      // awaitable state, not the moved-from object.
      state_.operation_->awaitable_state_ = &state_;
    }
  }

  return *this;
}

/**
 * @brief Binds this awaitable to a runtime-owned operation.
 *
 * @param operation Operation that will complete or detach this awaitable state.
 */
void UringAwaitable::Bind(Operation &operation) noexcept {
  Detach();
  state_.operation_ = &operation;
  operation.awaitable_state_ = &state_;
}

/**
 * @brief Clears the operation back pointer without completing the awaitable.
 *
 * Detach is used when an awaitable is destroyed or replaced before the kernel completion arrives.
 * The later CQE still recycles the operation but will not resume a missing coroutine.
 */
void UringAwaitable::Detach() noexcept {
  if (state_.operation_ != nullptr && state_.operation_->awaitable_state_ == &state_) {
    // Completion may still arrive after the awaitable is gone. Clearing the
    // back pointer turns that CQE into a no-resume completion.
    state_.operation_->awaitable_state_ = nullptr;
  }
  state_.operation_ = nullptr;
}

}  // namespace xrpc::io
