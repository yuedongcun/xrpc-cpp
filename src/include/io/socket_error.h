#pragma once

#include <cstdint>
#include <string>
#include <system_error>
#include <utility>

#include "common/xrpc_exception.h"

namespace xrpc::io {

enum class SocketErrorCode : std::uint8_t {

  InvalidAddress,

  CreateFailed,

  BindFailed,

  ListenFailed,

  AcceptFailed,

  ConnectFailed,

  ConnectTimeout,

  ReadFailed,

  ReadTimeout,

  WriteFailed,

  WriteTimeout,

  ConfigureFailed,

  ShutdownFailed,

  PeerClosed,
};

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

[[nodiscard]] inline auto MakeSystemErrorCode(int system_error) -> std::error_code {
  if (system_error == 0) {
    return {};
  }
  return {system_error, std::generic_category()};
}

class SocketError final : public TransportException {
 public:
  SocketError(SocketErrorCode code, std::error_code system_error, const std::string &message)
      : TransportException(ToStatusCode(code), message), code_(code), system_error_(system_error) {}

  SocketError(SocketErrorCode code, int system_error, const std::string &message)
      : SocketError(code, MakeSystemErrorCode(system_error), message) {}

  [[nodiscard]] auto code() const noexcept -> SocketErrorCode { return code_; }

  [[nodiscard]] auto system_error() const noexcept -> int { return system_error_.value(); }

  [[nodiscard]] auto system_error_code() const noexcept -> const std::error_code & { return system_error_; }

 private:
  SocketErrorCode code_;
  std::error_code system_error_;
};

}  // namespace xrpc::io
