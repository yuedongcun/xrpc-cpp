/**
 * @file socket.h
 * @brief Declares the RAII TCP socket wrapper used by xRPC.
 *
 * `Socket` owns a single socket file descriptor and provides the basic TCP
 * lifecycle and blocking I/O operations used by the runtime.
 *
 * Socket ownership is move-only, and the descriptor is closed on destruction.
 */

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include <sys/types.h>

namespace xrpc::io {

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
