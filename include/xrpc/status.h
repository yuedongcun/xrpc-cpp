#pragma once

#include <cstdint>
#include <string>
#include <utility>

namespace xrpc {

/**
 * @brief Portable RPC failure categories returned by public XRPC APIs.
 *
 * The codes intentionally mirror common RPC failure categories while staying independent from any one transport,
 * protobuf status type, or service-discovery system. `Ok` is the only success code.
 */
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

/**
 * @brief Small immutable success-or-error status value.
 *
 * `Status` is used for API calls whose successful result carries no payload and as the error half of `StatusOr<T>`.
 * A default-constructed status is OK. Error statuses should carry a human-readable message suitable for logs and tests,
 * but callers should branch on `code()` rather than parsing message text.
 */
class [[nodiscard]] Status final {
 public:
  /** @brief Constructs `StatusCode::Ok` with an empty message. */
  Status() = default;

  /**
   * @brief Constructs a status with an explicit code and message.
   *
   * @param code Status category.
   * @param message Human-readable diagnostic text.
   */
  Status(StatusCode code, std::string message) : code_(code), message_(std::move(message)) {}

  /** @return A success status. */
  [[nodiscard]] static auto Ok() -> Status { return {}; }

  /** @return true when the status code is `StatusCode::Ok`. */
  [[nodiscard]] auto ok() const -> bool { return code_ == StatusCode::Ok; }

  /** @return The status category. */
  [[nodiscard]] auto code() const -> StatusCode { return code_; }

  /** @return Human-readable diagnostic text. Empty for default OK statuses. */
  [[nodiscard]] auto message() const -> const std::string & { return message_; }

 private:
  StatusCode code_ = StatusCode::Ok;
  std::string message_;
};

}  // namespace xrpc
