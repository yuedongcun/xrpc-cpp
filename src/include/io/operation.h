#pragma once

#include <cstddef>
#include <cstdint>

#include <linux/time_types.h>

#include "io/io_result.h"
#include "io/uring_awaitable.h"

namespace xrpc::io {

/**
 * @brief Per-submission state owned by `UringContext::Runtime`.
 *
 * The runtime owns an `Operation` from SQE submission until the matching CQE is processed. `awaitable_state_` links the
 * kernel operation to a movable `UringAwaitable` and must be cleared before the operation is recycled.
 */
struct Operation {
  /** @brief Operation kind used to interpret completion data. */
  OperationType type_ = OperationType::Nop;

  /** @brief File descriptor associated with the operation, when applicable. */
  int fd_ = -1;

  /** @brief Caller-owned byte buffer for recv/send operations. */
  void *buffer_ = nullptr;

  /** @brief Buffer length for byte I/O operations. */
  std::size_t length_ = 0;

  /** @brief Kernel timeout payload for sleep/timeout operations. */
  __kernel_timespec timeout_{};

  /** @brief Awaitable state to resume or detach when the completion arrives. */
  detail::AwaitableState *awaitable_state_ = nullptr;
};

}  // namespace xrpc::io
