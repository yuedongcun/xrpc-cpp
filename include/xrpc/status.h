#pragma once

#include <cstdint>
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

}  // namespace xrpc
