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

#include <xrpc/status.h>

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

  [[nodiscard]] auto LocalPort() const -> StatusOr<std::uint16_t>;

  [[nodiscard]] auto Bind(std::string_view host, std::uint16_t port) -> Status;

  [[nodiscard]] auto Listen(int backlog) -> Status;

  [[nodiscard]] auto Accept() -> StatusOr<Socket>;

  [[nodiscard]] auto Connect(std::string_view host, std::uint16_t port) -> Status;

  [[nodiscard]] auto Connect(std::string_view host, std::uint16_t port, std::chrono::milliseconds timeout) -> Status;

  [[nodiscard]] auto Read(char *buf, std::size_t len) -> StatusOr<ssize_t>;

  [[nodiscard]] auto Write(std::string_view bytes) -> StatusOr<ssize_t>;

  [[nodiscard]] auto WriteAll(std::string_view bytes) -> Status;

  [[nodiscard]] auto SetReadWriteTimeout(std::chrono::milliseconds timeout) -> Status;

  void ShutdownWrite() noexcept;

  void ShutdownReadWrite() noexcept;

  void Close() noexcept;

 private:
  int fd_ = -1;
};

}  // namespace xrpc::io
