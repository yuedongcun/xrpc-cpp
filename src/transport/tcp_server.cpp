#include "transport/tcp_server.h"

#include <fcntl.h>
#include <cassert>
#include <memory>
#include <utility>

#include <unistd.h>

#include "rpc/xrpc_exception.h"

#include "transport/connection_io_loop.h"

namespace xrpc {

/**
 * @brief Creates a TCP server from normalized transport settings.
 *
 * The accept loop runs on `accept_context`; accepted sockets are handed to a connection I/O loop group so
 * request parsing, writes, and idle cleanup can be spread across event-loop threads.
 *
 * @param accept_context Accept-loop io_uring context.
 * @param handler Raw RPC handler invoked by connections.
 * @param executor Worker pool used for request dispatch.
 * @param config Listener and connection transport settings.
 */
TcpServer::TcpServer(io::UringContext &accept_context, RawHandler handler, ThreadPoolExecutor &executor,
                     TcpServerConfig config)
    : accept_context_(&accept_context), config_(config) {
  assert(config_.listen_backlog_ > 0);
  assert(config_.connection_io_threads_ > 0);
  connection_io_loop_group_ = std::make_unique<ConnectionIoLoopGroup>(
      config_.connection_io_threads_, handler, executor, config_.backpressure_limits_, backpressure_stats_,
      config_.protocol_limits_, config_.connection_idle_timeout_);
}

/** @brief Releases the connection-loop group after shutdown. */
TcpServer::~TcpServer() = default;

/**
 * @brief Binds the listening socket and records the actual bound port.
 *
 * @param host Local address to bind.
 * @param port Local TCP port, or zero to let the OS choose.
 */
void TcpServer::Listen(std::string_view host, std::uint16_t port) {
  if (listen_socket_.valid()) {
    return;
  }

  listen_socket_.Bind(host, port);
  listen_socket_.Listen(config_.listen_backlog_);
  port_ = listen_socket_.LocalPort();
}

/**
 * @brief Accepts clients until `Stop()` cancels the listening socket.
 *
 * @return Coroutine task completed after the connection-loop group stops.
 * @throws LifecycleException when called before `Listen()`.
 */
auto TcpServer::Run() -> runtime::Task<void> {
  if (!listen_socket_.valid()) {
    throw LifecycleException("TcpServer::Run called before Listen");
  }

  accept_stopped_ = false;
  connection_io_loop_group_->Start();

  while (!accept_stopped_) {
    const io::IoResult accept_result = co_await accept_context_->Accept(listen_socket_.fd());

    if (accept_result.result_ < 0) {
      if (!accept_stopped_) {
        StopAccepting();
      }
      break;
    }

    io::Socket client_socket(accept_result.result_);
    SetSocketFlags(client_socket.fd());
    connection_io_loop_group_->Dispatch(std::move(client_socket));
  }

  try {
    BeginDrain();
  } catch (...) {
    accept_context_->Stop();
    throw;
  }
  accept_context_->Stop();
}

/**
 * @brief Stops accepting while leaving connection loops alive for graceful drain.
 *
 * The listening file descriptor is canceled through the accept-loop context before being closed so
 * a suspended `Accept()` awaiter wakes promptly.
 */
void TcpServer::StopAccepting() {
  if (accept_stopped_) {
    return;
  }

  accept_stopped_ = true;
  accept_context_->CancelFd(listen_socket_.fd());
  listen_socket_.Close();
}

/** @brief Stops reads on all accepted connections. */
void TcpServer::BeginDrain() { connection_io_loop_group_->BeginDrain(); }

/** @brief Waits for accepted responses to drain before stopping connection loops. */
void TcpServer::FinishDrain() {
  connection_io_loop_group_->FinishDrain();
  connection_io_loop_group_->RethrowIfFailed();
}

/**
 * @brief Applies close-on-exec and nonblocking flags to an accepted socket.
 *
 * @param fd Accepted socket file descriptor.
 */
void TcpServer::SetSocketFlags(int fd) const {
  const int fd_flags = ::fcntl(fd, F_GETFD);
  if (fd_flags >= 0) {
    (void)::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC);
  }

  const int status_flags = ::fcntl(fd, F_GETFL);
  if (status_flags >= 0) {
    (void)::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK);
  }
}

}  // namespace xrpc
