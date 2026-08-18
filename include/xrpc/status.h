#pragma once

/**
 * @file status.h
 * @brief Public status and status-or-value error types.
 */

#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <utility>

namespace xrpc {

/**
 * @brief Failures that can be returned by public xRPC APIs or remote services.
 *
 * Numeric values are serialized on the xRPC wire and intentionally remain
 * stable; gaps correspond to status codes outside the current project scope.
 */
enum class StatusCode : std::uint8_t {
  /** The operation completed successfully. */
  Ok = 0,
  /** A caller supplied an invalid option, address, or request payload. */
  InvalidArgument = 2,
  /** The client-side deadline expired before a response was received. */
  DeadlineExceeded = 3,
  /** The requested service does not exist on the server. */
  NotFound = 4,
  /** A configured concurrency, queue, or protocol-size limit was reached. */
  ResourceExhausted = 7,
  /** The API was called in an invalid lifecycle state. */
  FailedPrecondition = 8,
  /** The requested method does not exist on the selected service. */
  Unimplemented = 9,
  /** An unexpected local or handler failure occurred. */
  Internal = 10,
  /** A network, endpoint discovery, or remote availability failure occurred. */
  Unavailable = 11,
  /** A received frame or serialized response was malformed. */
  DataLoss = 12,
};

/**
 * @brief A success code or a structured RPC failure.
 *
 * The default constructor and `Ok()` both create a successful status. Error
 * statuses carry a human-readable diagnostic message.
 */
class [[nodiscard]] Status final {
 public:
  /** Constructs a successful status. */
  Status() = default;

  /** Constructs a status with the supplied code and diagnostic message. */
  Status(StatusCode code, std::string message) : code_(code), message_(std::move(message)) {}

  /** Returns a successful status. */
  [[nodiscard]] static auto Ok() -> Status { return {}; }

  [[nodiscard]] auto ok() const -> bool { return code_ == StatusCode::Ok; }

  [[nodiscard]] auto code() const -> StatusCode { return code_; }

  [[nodiscard]] auto message() const -> const std::string & { return message_; }

 private:
  StatusCode code_ = StatusCode::Ok;
  std::string message_;
};

/**
 * @brief A value on success or a non-OK `Status` on failure.
 *
 * Construct from `T` for success and from a non-OK `Status` for failure.
 * Calling `value()` on an error result terminates because it violates the
 * caller contract; check `ok()` first or inspect `status()`.
 */
template <typename T>
class [[nodiscard]] StatusOr final {
 public:
  /** Constructs a successful result. */
  explicit StatusOr(T value) : value_(std::move(value)) {}

  /** Constructs a failed result. `status` must not be `StatusCode::Ok`. */
  explicit StatusOr(Status status) : status_(RequireErrorStatus(std::move(status))) {}

  [[nodiscard]] auto ok() const -> bool { return status_.ok(); }

  [[nodiscard]] auto status() const -> const Status & { return status_; }

  [[nodiscard]] auto value() const & -> const T & {
    RequireValue();
    return *value_;
  }

  [[nodiscard]] auto value() & -> T & {
    RequireValue();
    return *value_;
  }

  [[nodiscard]] auto value() && -> T && {
    RequireValue();
    return std::move(*value_);
  }

 private:
  [[nodiscard]] static auto RequireErrorStatus(Status status) -> Status {
    if (status.ok()) {
      std::terminate();
    }
    return status;
  }

  void RequireValue() const {
    if (!value_.has_value()) {
      std::terminate();
    }
  }

  Status status_;
  std::optional<T> value_;
};

}  // namespace xrpc
