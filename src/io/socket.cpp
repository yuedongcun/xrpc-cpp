/**
 * @file socket.cpp
 * @brief Implements `Socket` with Linux socket system calls.
 *
 * Timed connection setup temporarily switches the socket to non-blocking mode:
 *
 *   connect
 *      |
 *      v
 *   EINPROGRESS
 *      |
 *      v
 *   poll(POLLOUT)
 *      |
 *      v
 *   getsockopt(SO_ERROR)
 *      |
 *      v
 *   restore file flags
 *
 * Read and write timeouts are configured with SO_RCVTIMEO and SO_SNDTIMEO.
 * Socket system-call failures are translated into `SocketError`.
 */

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

#include "common/xrpc_exception.h"

namespace xrpc::io {
namespace {

auto MakeErrorMessage(std::string_view action, std::error_code error) -> std::string {
  std::string message(action);
  message.append(" failed");
  if (error) {
    message.append(": ");
    message.append(error.message());
  }
  return message;
}

[[noreturn]] void ThrowSocketError(SocketErrorCode code, std::string_view action) {
  const int error = errno;
  const std::error_code system_error = MakeSystemErrorCode(error);
  throw SocketError(code, system_error, MakeErrorMessage(action, system_error));
}

[[noreturn]] void ThrowSocketError(SocketErrorCode code, std::string_view action, int error) {
  const std::error_code system_error = MakeSystemErrorCode(error);
  throw SocketError(code, system_error, MakeErrorMessage(action, system_error));
}

auto CreateSocket() -> int {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    ThrowSocketError(SocketErrorCode::CreateFailed, "socket");
  }

  return fd;
}

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

void RestoreSocketFlags(int fd, int flags) {
  if (::fcntl(fd, F_SETFL, flags) < 0) {
    ThrowSocketError(SocketErrorCode::ConfigureFailed, "fcntl(F_SETFL)");
  }
}

}  // namespace

Socket::Socket(int fd) : fd_(fd) {}

Socket::~Socket() { Close(); }

Socket::Socket(Socket &&other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

auto Socket::operator=(Socket &&other) noexcept -> Socket & {
  if (this != &other) {
    Close();
    fd_ = std::exchange(other.fd_, -1);
  }
  return *this;
}

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

void Socket::Connect(std::string_view host, std::uint16_t port) {
  Connect(host, port, std::chrono::milliseconds::zero());
}

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

auto Socket::Write(std::string_view bytes) -> ssize_t {
  if (!valid()) {
    throw LifecycleException("Socket::Write called on invalid socket");
  }

  while (true) {
    const ssize_t sent = ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
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

void Socket::ShutdownWrite() {
  if (!valid()) {
    throw LifecycleException("Socket::ShutdownWrite called on invalid socket");
  }

  if (::shutdown(fd_, SHUT_WR) < 0) {
    ThrowSocketError(SocketErrorCode::ShutdownFailed, "shutdown");
  }
}

void Socket::ShutdownReadWrite() {
  if (!valid()) {
    throw LifecycleException("Socket::ShutdownReadWrite called on invalid socket");
  }

  if (::shutdown(fd_, SHUT_RDWR) < 0) {
    ThrowSocketError(SocketErrorCode::ShutdownFailed, "shutdown");
  }
}

void Socket::Close() {
  if (fd_ >= 0) {
    (void)::close(fd_);
    fd_ = -1;
  }
}

}  // namespace xrpc::io
