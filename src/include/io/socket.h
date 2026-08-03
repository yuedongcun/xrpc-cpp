#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <sys/types.h>

namespace xrpc::io {

/**
 * @brief RAII wrapper for blocking TCP sockets.
 *
 * This type is used by tests, benchmark plumbing, setup paths, and the blocking client transport. Asynchronous
 * server-side data paths should use `UringContext` instead so they share the event-loop cancellation and completion
 * model.
 */
class Socket final {
 public:
  Socket() = default;

  /** @brief Takes ownership of an existing file descriptor. */
  explicit Socket(int fd);

  /** @brief Closes the owned file descriptor if it is still valid. */
  ~Socket();

  Socket(const Socket &) = delete;
  auto operator=(const Socket &) -> Socket & = delete;

  /** @brief Moves file-descriptor ownership from another socket. */
  Socket(Socket &&other) noexcept;

  /** @brief Replaces this socket with another socket's file descriptor. */
  auto operator=(Socket &&other) noexcept -> Socket &;

  /** @return Owned file descriptor, or -1 when invalid. */
  [[nodiscard]] auto fd() const noexcept -> int { return fd_; }

  /** @return true when this object owns a valid file descriptor. */
  [[nodiscard]] auto valid() const noexcept -> bool { return fd_ >= 0; }

  /** @return Local bound TCP port. */
  [[nodiscard]] auto LocalPort() const -> std::uint16_t;

  /** @brief Binds the socket to `host:port`, creating the socket when needed. */
  void Bind(std::string_view host, std::uint16_t port);

  /** @brief Marks the socket as a listening socket. */
  void Listen(int backlog);

  /** @return Accepted client socket. */
  [[nodiscard]] auto Accept() -> Socket;

  /** @brief Connects to a remote endpoint without a client-side timeout. */
  void Connect(std::string_view host, std::uint16_t port);

  /** @brief Connects to a remote endpoint with a client-side timeout. */
  void Connect(std::string_view host, std::uint16_t port, std::chrono::milliseconds timeout);

  /** @return Bytes read, zero on EOF, or throws on socket error. */
  [[nodiscard]] auto Read(char *buf, std::size_t len) -> ssize_t;

  /** @return Bytes written by one send call, or throws on socket error. */
  [[nodiscard]] auto Write(std::string_view bytes) -> ssize_t;

  /** @brief Writes all bytes or throws if the socket cannot make progress. */
  void WriteAll(std::string_view bytes);

  /** @brief Applies read and write timeouts to the socket. */
  void SetReadWriteTimeout(std::chrono::milliseconds timeout);

  /** @brief Shuts down the write half of the socket. */
  void ShutdownWrite();

  /** @brief Shuts down both read and write halves of the socket. */
  void ShutdownReadWrite();

  /** @brief Closes the owned file descriptor and marks this socket invalid. */
  void Close();

 private:
  int fd_ = -1;
};

}  // namespace xrpc::io
