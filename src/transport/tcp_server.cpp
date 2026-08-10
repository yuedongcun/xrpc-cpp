#include "transport/tcp_server.h"

#include <cassert>
#include <fcntl.h>
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
    : accept_context_(&accept_context), handler_(std::move(handler)), executor_(&executor), config_(std::move(config)) {
  assert(config_.listen_backlog_ > 0);
  assert(config_.connection_io_threads_ > 0);
  connection_io_loop_group_ = std::make_unique<ConnectionIoLoopGroup>(
      config_.connection_io_threads_, handler_, *executor_, config_.backpressure_limits_, backpressure_stats_,
      io_stats_, config_.protocol_limits_, config_.connection_idle_timeout_);
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

  stopped_ = false;
  connection_io_loop_group_->Start();

  while (!stopped_) {
    const io::IoResult accept_result = co_await accept_context_->Accept(listen_socket_.fd());

    if (accept_result.result_ < 0) {
      if (!stopped_) {
        Stop();
      }
      break;
    }

    io::Socket client_socket(accept_result.result_);
    SetSocketFlags(client_socket.fd());
    StartConnection(std::move(client_socket));
  }

  connection_io_loop_group_->Stop();
  connection_io_loop_group_->RethrowIfFailed();
}

/**
 * @brief Stops accepting and requests all connection event loops to stop.
 *
 * The listening file descriptor is canceled through the accept-loop context before being closed so
 * a suspended `Accept()` awaiter wakes promptly.
 */
void TcpServer::Stop() {
  if (stopped_) {
    return;
  }

  stopped_ = true;
  accept_context_->CancelFd(listen_socket_.fd());
  listen_socket_.Close();

  connection_io_loop_group_->Stop();
}

/** @return Number of connections currently owned by connection event loops. */
auto TcpServer::ConnectionCount() const -> std::size_t { return connection_io_loop_group_->ConnectionCount(); }

/** @return Snapshot of cross-thread post counters from connection event loops. */
auto TcpServer::io_loop_post_stats() const -> io::UringPostStatsSnapshot {
  return connection_io_loop_group_->post_stats();
}

/**
 * @brief Hands an accepted client socket to the connection-loop group.
 *
 * @param client_socket Accepted socket with nonblocking flags applied by the accept loop.
 */
void TcpServer::StartConnection(io::Socket client_socket) {
  connection_io_loop_group_->Dispatch(std::move(client_socket));
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
