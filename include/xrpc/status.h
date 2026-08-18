#pragma once

#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <utility>

namespace xrpc {

enum class StatusCode : std::uint8_t {
  Ok = 0,
  Cancelled = 1,
  InvalidArgument = 2,
  DeadlineExceeded = 3,
  NotFound = 4,
  AlreadyExists = 5,
  PermissionDenied = 6,
  ResourceExhausted = 7,
  FailedPrecondition = 8,
  Unimplemented = 9,
  Internal = 10,
  Unavailable = 11,
  DataLoss = 12,
  Unauthenticated = 13,
};

class [[nodiscard]] Status final {
 public:
  Status() = default;

  Status(StatusCode code, std::string message) : code_(code), message_(std::move(message)) {}

  [[nodiscard]] static auto Ok() -> Status { return {}; }

  [[nodiscard]] auto ok() const -> bool { return code_ == StatusCode::Ok; }

  [[nodiscard]] auto code() const -> StatusCode { return code_; }

  [[nodiscard]] auto message() const -> const std::string & { return message_; }

 private:
  StatusCode code_ = StatusCode::Ok;
  std::string message_;
};

template <typename T>
class [[nodiscard]] StatusOr final {
 public:
  explicit StatusOr(T value) : value_(std::move(value)) {}

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
