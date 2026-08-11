#include "io/socket.h"

#include <cerrno>
#include <chrono>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "io/socket_error.h"

#include "common/xrpc_exception.h"

namespace xrpc::io {
namespace {

/**
 * @brief Builds a socket diagnostic message from an action and system error.
 *
 * @param action Socket operation name.
 * @param error System error code, or empty code when no errno applies.
 * @return Human-readable diagnostic message.
 */
auto MakeErrorMessage(std::string_view action, std::error_code error) -> std::string {
  std::string message(action);
  message.append(" failed");
  if (error) {
    message.append(": ");
    message.append(error.message());
  }
  return message;
}

/**
 * @brief Throws a `SocketError` using the current `errno` value.
 *
 * @param code Portable socket error category.
 * @param action Socket operation name.
 */
[[noreturn]] void ThrowSocketError(SocketErrorCode code, std::string_view action) {
  const int error = errno;
  const std::error_code system_error = MakeSystemErrorCode(error);
  throw SocketError(code, system_error, MakeErrorMessage(action, system_error));
}

/**
 * @brief Throws a `SocketError` using an explicit errno value.
 *
 * @param code Portable socket error category.
 * @param action Socket operation name.
 * @param error Positive errno value.
 */
[[noreturn]] void ThrowSocketError(SocketErrorCode code, std::string_view action, int error) {
  const std::error_code system_error = MakeSystemErrorCode(error);
  throw SocketError(code, system_error, MakeErrorMessage(action, system_error));
}

/**
 * @brief Creates an IPv4 TCP socket.
 *
 * @return New file descriptor owned by the caller.
 * @throws SocketError when `socket()` fails.
 */
auto CreateSocket() -> int {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    ThrowSocketError(SocketErrorCode::CreateFailed, "socket");
  }

  return fd;
}

/**
 * @brief Converts a millisecond timeout to a POSIX socket `timeval`.
 *
 * @param timeout Timeout duration. Non-positive values disable the socket timeout.
 * @return `timeval` suitable for `SO_RCVTIMEO` and `SO_SNDTIMEO`.
 */
auto ToTimeval(std::chrono::milliseconds timeout) -> timeval {
  timeval tv{};
  if (timeout <= std::chrono::milliseconds::zero()) {
    return tv;
  }

  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(timeout - seconds);
  tv.tv_sec = static_cast<time_t>(seconds.count());
  tv.tv_usec = static_cast<suseconds_t>(micros.count());
  if (tv.tv_sec == 0 && tv.tv_usec == 0) {
    tv.tv_usec = 1;
  }
  return tv;
}

/**
 * @brief Converts an IPv4 host/port pair into a `sockaddr_in`.
 *
 * @param host Numeric IPv4 address.
 * @param port TCP port in host byte order.
 * @return IPv4 socket address.
 * @throws SocketError when `host` is not a numeric IPv4 address.
 */
auto ToAddress(std::string_view host, std::uint16_t port) -> sockaddr_in {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);

  const std::string host_name(host);
  if (::inet_pton(AF_INET, host_name.c_str(), &addr.sin_addr) != 1) {
    throw SocketError(SocketErrorCode::InvalidAddress, 0, "invalid host address");
  }

  return addr;
}

/**
 * @brief Restores socket status flags after a temporary nonblocking connect.
 *
 * @param fd Socket file descriptor.
 * @param flags Status flags previously returned by `F_GETFL`.
 * @throws SocketError when `fcntl(F_SETFL)` fails.
 */
void RestoreSocketFlags(int fd, int flags) {
  if (::fcntl(fd, F_SETFL, flags) < 0) {
    ThrowSocketError(SocketErrorCode::ConfigureFailed, "fcntl(F_SETFL)");
  }
}

}  // namespace

/**
 * @brief Takes ownership of an existing socket file descriptor.
 *
 * @param fd Socket file descriptor, or -1 for an invalid socket.
 */
Socket::Socket(int fd) : fd_(fd) {}

/** @brief Closes the owned file descriptor, if any. */
Socket::~Socket() { Close(); }

/**
 * @brief Moves file-descriptor ownership from another socket wrapper.
 *
 * @param other Source socket wrapper.
 */
Socket::Socket(Socket &&other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

/**
 * @brief Replaces this socket with another wrapper's file descriptor.
 *
 * @param other Source socket wrapper.
 * @return This socket wrapper.
 */
auto Socket::operator=(Socket &&other) noexcept -> Socket & {
  if (this != &other) {
    Close();
    fd_ = std::exchange(other.fd_, -1);
  }
  return *this;
}

/**
 * @brief Returns the TCP port currently bound to the socket.
 *
 * @return Local port in host byte order.
 * @throws LifecycleException when the socket is invalid.
 * @throws SocketError when `getsockname()` fails.
 */
auto Socket::LocalPort() const -> std::uint16_t {
  if (!valid()) {
    throw LifecycleException("Socket::LocalPort called on invalid socket");
  }

  sockaddr_in addr{};
  socklen_t len = sizeof(addr);
  if (::getsockname(fd_, reinterpret_cast<sockaddr *>(&addr), &len) < 0) {
    ThrowSocketError(SocketErrorCode::ConfigureFailed, "getsockname");
  }

  return ntohs(addr.sin_port);
}

/**
 * @brief Binds this socket to a numeric IPv4 address and port.
 *
 * The socket is created lazily if this wrapper is invalid. Bind failures close the descriptor so
 * callers do not accidentally reuse a partially configured socket.
 *
 * @param host Numeric IPv4 bind address.
 * @param port TCP port, or zero for OS assignment.
 * @throws SocketError on address conversion, socket creation, or bind failure.
 */
void Socket::Bind(std::string_view host, std::uint16_t port) {
  if (!valid()) {
    fd_ = CreateSocket();
  }

  const int yes = 1;
  (void)::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  const sockaddr_in addr = ToAddress(host, port);
  if (::bind(fd_, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) < 0) {
    const int error = errno;
    Close();
    ThrowSocketError(SocketErrorCode::BindFailed, "bind", error);
  }
}

/**
 * @brief Marks a bound socket as a TCP listener.
 *
 * @param backlog Listen backlog.
 * @throws LifecycleException when the socket is invalid.
 * @throws SocketError when `listen()` fails.
 */
void Socket::Listen(int backlog) {
  if (!valid()) {
    throw LifecycleException("Socket::Listen called on invalid socket");
  }

  if (::listen(fd_, backlog) < 0) {
    const int error = errno;
    Close();
    ThrowSocketError(SocketErrorCode::ListenFailed, "listen", error);
  }
}

/**
 * @brief Accepts one incoming TCP client connection.
 *
 * @return Socket wrapper owning the accepted client descriptor.
 * @throws LifecycleException when the listener is invalid.
 * @throws SocketError when `accept()` fails for a non-interrupt reason.
 */
auto Socket::Accept() -> Socket {
  if (!valid()) {
    throw LifecycleException("Socket::Accept called on invalid socket");
  }

  while (true) {
    const int client_fd = ::accept(fd_, nullptr, nullptr);
    if (client_fd >= 0) {
      return Socket(client_fd);
    }
    if (errno == EINTR) {
      continue;
    }
    ThrowSocketError(SocketErrorCode::AcceptFailed, "accept");
  }
}

/**
 * @brief Connects to a numeric IPv4 endpoint without an explicit timeout.
 *
 * @param host Numeric IPv4 remote address.
 * @param port Remote TCP port.
 */
void Socket::Connect(std::string_view host, std::uint16_t port) {
  Connect(host, port, std::chrono::milliseconds::zero());
}

/**
 * @brief Connects to a numeric IPv4 endpoint, optionally with a timeout.
 *
 * Timed connect temporarily switches the socket to nonblocking mode, waits for writability with
 * `poll()`, then checks `SO_ERROR` to distinguish success from asynchronous connect failure.
 *
 * @param host Numeric IPv4 remote address.
 * @param port Remote TCP port.
 * @param timeout Connect timeout. Non-positive values use a blocking connect.
 * @throws SocketError on timeout, connect failure, address conversion, or socket configuration failure.
 */
void Socket::Connect(std::string_view host, std::uint16_t port, std::chrono::milliseconds timeout) {
  if (!valid()) {
    fd_ = CreateSocket();
  }

  const sockaddr_in addr = ToAddress(host, port);
  if (timeout <= std::chrono::milliseconds::zero()) {
    while (true) {
      if (::connect(fd_, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) == 0) {
        return;
      }
      if (errno == EINTR) {
        continue;
      }
      const int error = errno;
      Close();
      ThrowSocketError(SocketErrorCode::ConnectFailed, "connect", error);
    }
  }

  const int flags = ::fcntl(fd_, F_GETFL, 0);
  if (flags < 0) {
    const int error = errno;
    Close();
    ThrowSocketError(SocketErrorCode::ConfigureFailed, "fcntl(F_GETFL)", error);
  }
  if (::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
    const int error = errno;
    Close();
    ThrowSocketError(SocketErrorCode::ConfigureFailed, "fcntl(F_SETFL)", error);
  }

  // Timed connect uses the standard non-blocking connect handshake: wait for
  // writability, then read SO_ERROR to distinguish success from async failure.
  while (true) {
    if (::connect(fd_, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) == 0) {
      RestoreSocketFlags(fd_, flags);
      return;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EINPROGRESS) {
      pollfd pfd{};
      pfd.fd = fd_;
      pfd.events = POLLOUT;

      const int poll_timeout = static_cast<int>(timeout.count());
      const int poll_result = ::poll(&pfd, 1, poll_timeout);
      if (poll_result == 0) {
        Close();
        throw SocketError(SocketErrorCode::ConnectTimeout, 0, "connect timed out");
      }
      if (poll_result < 0) {
        if (errno == EINTR) {
          continue;
        }
        const int error = errno;
        Close();
        ThrowSocketError(SocketErrorCode::ConnectFailed, "poll", error);
      }

      int socket_error = 0;
      socklen_t socket_error_len = sizeof(socket_error);
      if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len) < 0) {
        const int error = errno;
        Close();
        ThrowSocketError(SocketErrorCode::ConnectFailed, "getsockopt(SO_ERROR)", error);
      }
      if (socket_error == 0) {
        RestoreSocketFlags(fd_, flags);
        return;
      }

      const int error = socket_error;
      Close();
      ThrowSocketError(SocketErrorCode::ConnectFailed, "connect", error);
    }
    const int error = errno;
    Close();
    ThrowSocketError(SocketErrorCode::ConnectFailed, "connect", error);
  }
}

/**
 * @brief Reads bytes from the connected socket.
 *
 * @param buf Destination buffer.
 * @param len Destination capacity.
 * @return Bytes read, or zero on orderly peer shutdown.
 * @throws LifecycleException when the socket is invalid.
 * @throws SocketError on read timeout or read failure.
 */
auto Socket::Read(char *buf, std::size_t len) -> ssize_t {
  if (!valid()) {
    throw LifecycleException("Socket::Read called on invalid socket");
  }

  while (true) {
    const ssize_t received = ::recv(fd_, buf, len, 0);
    if (received >= 0) {
      return received;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      throw SocketError(SocketErrorCode::ReadTimeout, errno, "recv timed out");
    }
    ThrowSocketError(SocketErrorCode::ReadFailed, "recv");
  }
}

/**
 * @brief Writes some bytes to the connected socket.
 *
 * @param bytes Bytes to send.
 * @return Bytes accepted by one `send()` call.
 * @throws LifecycleException when the socket is invalid.
 * @throws SocketError on write timeout or write failure.
 */
auto Socket::Write(std::string_view bytes) -> ssize_t {
  if (!valid()) {
    throw LifecycleException("Socket::Write called on invalid socket");
  }

  while (true) {
    const ssize_t sent = ::send(fd_, bytes.data(), bytes.size(), 0);
    if (sent >= 0) {
      return sent;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      throw SocketError(SocketErrorCode::WriteTimeout, errno, "send timed out");
    }
    ThrowSocketError(SocketErrorCode::WriteFailed, "send");
  }
}

/**
 * @brief Writes all bytes or throws if the socket cannot make progress.
 *
 * @param bytes Bytes to send.
 * @throws SocketError when a write fails or `send()` returns zero.
 */
void Socket::WriteAll(std::string_view bytes) {
  std::size_t written = 0;
  while (written < bytes.size()) {
    const ssize_t sent = Write(bytes.substr(written));
    if (sent == 0) {
      throw SocketError(SocketErrorCode::PeerClosed, 0, "send returned 0");
    }
    written += static_cast<std::size_t>(sent);
  }
}

/**
 * @brief Applies the same receive and send timeout to the socket.
 *
 * @param timeout Timeout duration. Non-positive values disable the kernel timeout.
 * @throws LifecycleException when the socket is invalid.
 * @throws SocketError when either socket option fails.
 */
void Socket::SetReadWriteTimeout(std::chrono::milliseconds timeout) {
  if (!valid()) {
    throw LifecycleException("Socket::SetReadWriteTimeout called on invalid socket");
  }

  const timeval tv = ToTimeval(timeout);
  if (::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
    ThrowSocketError(SocketErrorCode::ConfigureFailed, "setsockopt(SO_RCVTIMEO)");
  }
  if (::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
    ThrowSocketError(SocketErrorCode::ConfigureFailed, "setsockopt(SO_SNDTIMEO)");
  }
}

/**
 * @brief Shuts down the write half of the connection.
 *
 * @throws LifecycleException when the socket is invalid.
 * @throws SocketError when `shutdown(SHUT_WR)` fails.
 */
void Socket::ShutdownWrite() {
  if (!valid()) {
    throw LifecycleException("Socket::ShutdownWrite called on invalid socket");
  }

  if (::shutdown(fd_, SHUT_WR) < 0) {
    ThrowSocketError(SocketErrorCode::ShutdownFailed, "shutdown");
  }
}

/**
 * @brief Shuts down both read and write halves of the connection.
 *
 * @throws LifecycleException when the socket is invalid.
 * @throws SocketError when `shutdown(SHUT_RDWR)` fails.
 */
void Socket::ShutdownReadWrite() {
  if (!valid()) {
    throw LifecycleException("Socket::ShutdownReadWrite called on invalid socket");
  }

  if (::shutdown(fd_, SHUT_RDWR) < 0) {
    ThrowSocketError(SocketErrorCode::ShutdownFailed, "shutdown");
  }
}

/** @brief Closes the owned file descriptor and marks this wrapper invalid. */
void Socket::Close() {
  if (fd_ >= 0) {
    (void)::close(fd_);
    fd_ = -1;
  }
}

}  // namespace xrpc::io
