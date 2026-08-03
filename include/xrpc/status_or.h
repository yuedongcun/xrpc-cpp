#pragma once

#include <exception>
#include <optional>
#include <utility>

#include <xrpc/status.h>

namespace xrpc {

/**
 * @brief Small value-or-error carrier for public RPC APIs.
 *
 * `StatusOr<T>` contains either a `T` value or a non-OK `Status`. Constructing an error `StatusOr` from `Status::Ok()`
 * terminates, and calling `value()` when `!ok()` terminates, because both cases indicate a caller bug rather than a
 * recoverable RPC failure.
 */
template <typename T>
class [[nodiscard]] StatusOr final {
 public:
  /**
   * @brief Constructs a successful result.
   *
   * @param value Value to store in the result.
   */
  explicit StatusOr(T value) : value_(std::move(value)) {}

  /**
   * @brief Constructs an error result.
   *
   * @param status Non-OK error status.
   */
  explicit StatusOr(Status status) : status_(RequireErrorStatus(std::move(status))) {}

  /** @return true when this result contains a value. */
  [[nodiscard]] auto ok() const -> bool { return status_.ok(); }

  /** @return `Status::Ok()` for successful results, otherwise the stored error. */
  [[nodiscard]] auto status() const -> const Status & { return status_; }

  /** @return Const reference to the stored value. Terminates when this result is an error. */
  [[nodiscard]] auto value() const & -> const T & {
    RequireValue();
    return *value_;
  }

  /** @return Mutable reference to the stored value. Terminates when this result is an error. */
  [[nodiscard]] auto value() & -> T & {
    RequireValue();
    return *value_;
  }

  /** @return Rvalue reference to the stored value. Terminates when this result is an error. */
  [[nodiscard]] auto value() && -> T && {
    RequireValue();
    return std::move(*value_);
  }

 private:
  /** @brief Rejects accidental construction of an error result from `Status::Ok()`. */
  [[nodiscard]] static auto RequireErrorStatus(Status status) -> Status {
    if (status.ok()) {
      std::terminate();
    }
    return status;
  }

  /** @brief Terminates when callers access a value on an error result. */
  void RequireValue() const {
    if (!value_.has_value()) {
      std::terminate();
    }
  }

  Status status_;
  std::optional<T> value_;
};

}  // namespace xrpc
