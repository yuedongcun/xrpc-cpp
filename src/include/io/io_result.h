#pragma once

#include <cstddef>
#include <cstdint>

namespace xrpc::io {

/**
 * @brief Kernel-facing operation kinds used by the io_uring runtime.
 *
 * These values are internal classification tags; they are not part of the XRPC wire protocol.
 */
enum class OperationType : std::uint8_t {
  /** @brief Accept operation on the listening socket. */
  Accept = 0,

  /** @brief Receive operation on a connected socket. */
  Recv,

  /** @brief Send operation on a connected socket. */
  Send,

  /** @brief Timeout operation used for sleeps and idle timers. */
  Timeout,

  /** @brief No-op operation used by tests and wakeup plumbing. */
  Nop,

  /** @brief Cancellation request submitted to io_uring. */
  Cancel,

  /** @brief Internal eventfd wakeup for posted callbacks. */
  Wakeup,
};

/**
 * @brief Completion payload returned by `UringAwaitable`.
 *
 * `result_` preserves the raw kernel result value. `error_code_` is the positive errno value when the operation failed.
 * `bytes_transferred_` is meaningful only for byte I/O.
 */
struct IoResult {
  /** @brief Operation kind that produced this completion. */
  OperationType type_ = OperationType::Nop;

  /** @brief File descriptor associated with the operation, when applicable. */
  int fd_ = -1;

  /** @brief Raw kernel result. Negative values are converted into `error_code_`. */
  int result_ = 0;

  /** @brief Positive errno value when the operation failed, otherwise zero. */
  int error_code_ = 0;

  /** @brief Bytes received or sent for byte I/O operations. */
  std::size_t bytes_transferred_ = 0;
};

}  // namespace xrpc::io
