/**
 * @file socket.h
 * @brief Declares the RAII TCP socket wrapper used by xRPC.
 *
 * `Socket` owns a single socket file descriptor and provides the basic TCP
 * lifecycle and blocking I/O operations used by the runtime.
 *
 * Socket ownership is move-only, and the descriptor is closed on destruction.
 * A `Socket` is a single-owner object; callers must not operate on the same
 * instance concurrently from multiple threads.
 */

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>

#include <sys/types.h>

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

class Socket final {
 public:
  Socket() = default;

  explicit Socket(int fd);

  ~Socket();

  Socket(const Socket &) = delete;
  auto operator=(const Socket &) -> Socket & = delete;

  Socket(Socket &&other) noexcept;

  auto operator=(Socket &&other) noexcept -> Socket &;

  [[nodiscard]] auto fd() const noexcept -> int { return fd_; }

  [[nodiscard]] auto valid() const noexcept -> bool { return fd_ >= 0; }

  [[nodiscard]] auto LocalPort() const -> std::uint16_t;

  void Bind(std::string_view host, std::uint16_t port);

  void Listen(int backlog);

  [[nodiscard]] auto Accept() -> Socket;

  void Connect(std::string_view host, std::uint16_t port);

  void Connect(std::string_view host, std::uint16_t port, std::chrono::milliseconds timeout);

  [[nodiscard]] auto Read(char *buf, std::size_t len) -> ssize_t;

  [[nodiscard]] auto Write(std::string_view bytes) -> ssize_t;

  void WriteAll(std::string_view bytes);

  void SetReadWriteTimeout(std::chrono::milliseconds timeout);

  void ShutdownWrite();

  void ShutdownReadWrite();

  void Close();

 private:
  int fd_ = -1;
};

}  // namespace xrpc::io
