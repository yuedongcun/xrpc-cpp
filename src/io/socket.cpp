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
 * Socket system-call failures are translated into `Status` values.
 */

#include "io/socket.h"

#include <cerrno>
#include <chrono>
#include <exception>
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

auto MakeSocketStatus(SocketErrorCode code, std::string_view action, int error) -> Status {
  const std::error_code system_error = MakeSystemErrorCode(error);
  return {ToStatusCode(code), MakeErrorMessage(action, system_error)};
}

auto MakeSocketStatus(SocketErrorCode code, std::string_view action) -> Status {
  return MakeSocketStatus(code, action, errno);
}

auto CreateSocket() -> StatusOr<int> {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return StatusOr<int>(MakeSocketStatus(SocketErrorCode::CreateFailed, "socket"));
  }

  return StatusOr<int>(fd);
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

auto ToAddress(std::string_view host, std::uint16_t port) -> StatusOr<sockaddr_in> {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);

  const std::string host_name(host);
  if (::inet_pton(AF_INET, host_name.c_str(), &addr.sin_addr) != 1) {
    return StatusOr<sockaddr_in>(Status{StatusCode::InvalidArgument, "invalid host address"});
  }

  return StatusOr<sockaddr_in>(addr);
}

auto RestoreSocketFlags(int fd, int flags) -> Status {
  if (::fcntl(fd, F_SETFL, flags) < 0) {
    return MakeSocketStatus(SocketErrorCode::ConfigureFailed, "fcntl(F_SETFL)");
  }
  return Status::Ok();
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

auto Socket::LocalPort() const -> StatusOr<std::uint16_t> {
  if (!valid()) {
    return StatusOr<std::uint16_t>(Status{StatusCode::FailedPrecondition, "Socket::LocalPort requires a valid socket"});
  }

  sockaddr_in addr{};
  socklen_t len = sizeof(addr);
  if (::getsockname(fd_, reinterpret_cast<sockaddr *>(&addr), &len) < 0) {
    return StatusOr<std::uint16_t>(MakeSocketStatus(SocketErrorCode::ConfigureFailed, "getsockname"));
  }

  return StatusOr<std::uint16_t>(ntohs(addr.sin_port));
}

auto Socket::Bind(std::string_view host, std::uint16_t port) -> Status {
  if (!valid()) {
    StatusOr<int> socket = CreateSocket();
    if (!socket.ok()) {
      return socket.status();
    }
    fd_ = std::move(socket).value();
  }

  const int yes = 1;
  (void)::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  StatusOr<sockaddr_in> address = ToAddress(host, port);
  if (!address.ok()) {
    Close();
    return address.status();
  }
  const sockaddr_in addr = std::move(address).value();
  if (::bind(fd_, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) < 0) {
    const int error = errno;
    Close();
    return MakeSocketStatus(SocketErrorCode::BindFailed, "bind", error);
  }
  return Status::Ok();
}

auto Socket::Listen(int backlog) -> Status {
  if (!valid()) {
    return {StatusCode::FailedPrecondition, "Socket::Listen requires a valid socket"};
  }

  if (::listen(fd_, backlog) < 0) {
    const int error = errno;
    Close();
    return MakeSocketStatus(SocketErrorCode::ListenFailed, "listen", error);
  }
  return Status::Ok();
}

auto Socket::Accept() -> StatusOr<Socket> {
  if (!valid()) {
    return StatusOr<Socket>(Status{StatusCode::FailedPrecondition, "Socket::Accept requires a valid socket"});
  }

  while (true) {
    const int client_fd = ::accept(fd_, nullptr, nullptr);
    if (client_fd >= 0) {
      return StatusOr<Socket>(Socket(client_fd));
    }
    if (errno == EINTR) {
      continue;
    }
    return StatusOr<Socket>(MakeSocketStatus(SocketErrorCode::AcceptFailed, "accept"));
  }
}

auto Socket::Connect(std::string_view host, std::uint16_t port) -> Status {
  return Connect(host, port, std::chrono::milliseconds::zero());
}

auto Socket::Connect(std::string_view host, std::uint16_t port, std::chrono::milliseconds timeout) -> Status {
  if (!valid()) {
    StatusOr<int> socket = CreateSocket();
    if (!socket.ok()) {
      return socket.status();
    }
    fd_ = std::move(socket).value();
  }

  StatusOr<sockaddr_in> address = ToAddress(host, port);
  if (!address.ok()) {
    Close();
    return address.status();
  }
  const sockaddr_in addr = std::move(address).value();
  if (timeout <= std::chrono::milliseconds::zero()) {
    while (true) {
      if (::connect(fd_, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) == 0) {
        return Status::Ok();
      }
      if (errno == EINTR) {
        continue;
      }
      const int error = errno;
      Close();
      return MakeSocketStatus(SocketErrorCode::ConnectFailed, "connect", error);
    }
  }

  const int flags = ::fcntl(fd_, F_GETFL, 0);
  if (flags < 0) {
    const int error = errno;
    Close();
    return MakeSocketStatus(SocketErrorCode::ConfigureFailed, "fcntl(F_GETFL)", error);
  }
  if (::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
    const int error = errno;
    Close();
    return MakeSocketStatus(SocketErrorCode::ConfigureFailed, "fcntl(F_SETFL)", error);
  }

  while (true) {
    if (::connect(fd_, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) == 0) {
      return RestoreSocketFlags(fd_, flags);
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
        return {StatusCode::DeadlineExceeded, "connect timed out"};
      }
      if (poll_result < 0) {
        if (errno == EINTR) {
          continue;
        }
        const int error = errno;
        Close();
        return MakeSocketStatus(SocketErrorCode::ConnectFailed, "poll", error);
      }

      int socket_error = 0;
      socklen_t socket_error_len = sizeof(socket_error);
      if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len) < 0) {
        const int error = errno;
        Close();
        return MakeSocketStatus(SocketErrorCode::ConnectFailed, "getsockopt(SO_ERROR)", error);
      }
      if (socket_error == 0) {
        return RestoreSocketFlags(fd_, flags);
      }

      const int error = socket_error;
      Close();
      return MakeSocketStatus(SocketErrorCode::ConnectFailed, "connect", error);
    }
    const int error = errno;
    Close();
    return MakeSocketStatus(SocketErrorCode::ConnectFailed, "connect", error);
  }
}

auto Socket::Read(char *buf, std::size_t len) -> StatusOr<ssize_t> {
  if (!valid()) {
    return StatusOr<ssize_t>(Status{StatusCode::FailedPrecondition, "Socket::Read requires a valid socket"});
  }

  while (true) {
    const ssize_t received = ::recv(fd_, buf, len, 0);
    if (received >= 0) {
      return StatusOr<ssize_t>(received);
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return StatusOr<ssize_t>(Status{StatusCode::DeadlineExceeded, "recv timed out"});
    }
    return StatusOr<ssize_t>(MakeSocketStatus(SocketErrorCode::ReadFailed, "recv"));
  }
}

auto Socket::Write(std::string_view bytes) -> StatusOr<ssize_t> {
  if (!valid()) {
    return StatusOr<ssize_t>(Status{StatusCode::FailedPrecondition, "Socket::Write requires a valid socket"});
  }

  while (true) {
    const ssize_t sent = ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (sent >= 0) {
      return StatusOr<ssize_t>(sent);
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return StatusOr<ssize_t>(Status{StatusCode::DeadlineExceeded, "send timed out"});
    }
    return StatusOr<ssize_t>(MakeSocketStatus(SocketErrorCode::WriteFailed, "send"));
  }
}

auto Socket::WriteAll(std::string_view bytes) -> Status {
  std::size_t written = 0;
  while (written < bytes.size()) {
    StatusOr<ssize_t> write_result = Write(bytes.substr(written));
    if (!write_result.ok()) {
      return write_result.status();
    }
    const ssize_t sent = std::move(write_result).value();
    if (sent == 0) {
      return {StatusCode::Unavailable, "send returned 0"};
    }
    written += static_cast<std::size_t>(sent);
  }
  return Status::Ok();
}

auto Socket::SetReadWriteTimeout(std::chrono::milliseconds timeout) -> Status {
  if (!valid()) {
    return {StatusCode::FailedPrecondition, "Socket::SetReadWriteTimeout requires a valid socket"};
  }

  const timeval tv = ToTimeval(timeout);
  if (::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
    return MakeSocketStatus(SocketErrorCode::ConfigureFailed, "setsockopt(SO_RCVTIMEO)");
  }
  if (::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
    return MakeSocketStatus(SocketErrorCode::ConfigureFailed, "setsockopt(SO_SNDTIMEO)");
  }
  return Status::Ok();
}

void Socket::ShutdownWrite() noexcept {
  if (valid()) {
    (void)::shutdown(fd_, SHUT_WR);
  }
}

void Socket::ShutdownReadWrite() noexcept {
  if (valid()) {
    (void)::shutdown(fd_, SHUT_RDWR);
  }
}

void Socket::Close() noexcept {
  if (fd_ >= 0) {
    (void)::close(fd_);
    fd_ = -1;
  }
}

}  // namespace xrpc::io
