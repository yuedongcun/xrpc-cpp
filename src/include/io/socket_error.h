#pragma once

#include <cstdint>
#include <string>
#include <system_error>
#include <utility>

#include "common/xrpc_exception.h"

namespace xrpc::io {

/**
 * @brief Socket failure categories used before mapping to public RPC statuses.
 *
 * Transport-level socket failures keep both the portable XRPC status mapping and the original errno so callers can
 * choose between RPC semantics and system diagnostics.
 */
enum class SocketErrorCode : std::uint8_t {
  /** @brief Address string or port could not be resolved or represented. */
  InvalidAddress,

  /** @brief Socket creation failed. */
  CreateFailed,

  /** @brief Bind failed. */
  BindFailed,

  /** @brief Listen failed. */
  ListenFailed,

  /** @brief Accept failed. */
  AcceptFailed,

  /** @brief Connect failed before a timeout was reached. */
  ConnectFailed,

  /** @brief Connect timed out. */
  ConnectTimeout,

  /** @brief Read failed before a timeout was reached. */
  ReadFailed,

  /** @brief Read timed out. */
  ReadTimeout,

  /** @brief Write failed before a timeout was reached. */
  WriteFailed,

  /** @brief Write timed out. */
  WriteTimeout,

  /** @brief Socket option or fd flag configuration failed. */
  ConfigureFailed,

  /** @brief Socket shutdown failed. */
  ShutdownFailed,

  /** @brief Peer closed the stream. */
  PeerClosed,
};

/** @return Public status code corresponding to a socket error category. */
[[nodiscard]] inline auto ToStatusCode(SocketErrorCode code) -> StatusCode {
  switch (code) {
    case SocketErrorCode::ConnectTimeout:
    case SocketErrorCode::ReadTimeout:
    case SocketErrorCode::WriteTimeout:
      return StatusCode::DeadlineExceeded;
    case SocketErrorCode::InvalidAddress:
      return StatusCode::InvalidArgument;
    case SocketErrorCode::CreateFailed:
    case SocketErrorCode::BindFailed:
    case SocketErrorCode::ListenFailed:
    case SocketErrorCode::AcceptFailed:
    case SocketErrorCode::ConnectFailed:
    case SocketErrorCode::ReadFailed:
    case SocketErrorCode::WriteFailed:
    case SocketErrorCode::ConfigureFailed:
    case SocketErrorCode::ShutdownFailed:
    case SocketErrorCode::PeerClosed:
      return StatusCode::Unavailable;
  }
  return StatusCode::Internal;
}

/** @return `std::error_code` wrapping a positive errno value, or empty code for zero. */
[[nodiscard]] inline auto MakeSystemErrorCode(int system_error) -> std::error_code {
  if (system_error == 0) {
    return {};
  }
  return {system_error, std::generic_category()};
}

/**
 * @brief Transport exception that preserves both XRPC and system socket error details.
 */
class SocketError final : public TransportException {
 public:
  /** @brief Constructs a socket error from a portable code and system error code. */
  SocketError(SocketErrorCode code, std::error_code system_error, const std::string &message)
      : TransportException(ToStatusCode(code), message), code_(code), system_error_(system_error) {}

  /** @brief Constructs a socket error from a portable code and raw errno value. */
  SocketError(SocketErrorCode code, int system_error, const std::string &message)
      : SocketError(code, MakeSystemErrorCode(system_error), message) {}

  /** @return Portable socket error category. */
  [[nodiscard]] auto code() const noexcept -> SocketErrorCode { return code_; }

  /** @return Raw errno value, or zero when no system error is stored. */
  [[nodiscard]] auto system_error() const noexcept -> int { return system_error_.value(); }

  /** @return Full `std::error_code` for system diagnostics. */
  [[nodiscard]] auto system_error_code() const noexcept -> const std::error_code & { return system_error_; }

 private:
  SocketErrorCode code_;
  std::error_code system_error_;
};

}  // namespace xrpc::io
