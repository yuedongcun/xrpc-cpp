#include "transport/tcp_server.h"

#include <fcntl.h>
#include <memory>
#include <utility>

#include <unistd.h>

#include <xrpc/xrpc_exception.h>

#include "transport/connection_io_loop.h"

namespace xrpc {
namespace {

/**
 * @brief Validates the number of connection event loops owned by a server.
 *
 * @param connection_io_threads Requested number of connection-loop threads.
 * @throws ConfigException when no connection loop would be created.
 */
void ValidateConnectionIoThreads(std::size_t connection_io_threads) {
  if (connection_io_threads == 0) {
    throw ConfigException("TcpServer connection_io_threads must be greater than 0");
  }
}

}  // namespace

/**
 * @brief Creates a TCP server with default backpressure and one connection event loop.
 *
 * @param context Accept-loop io_uring context.
 * @param handler Raw RPC handler invoked by accepted connections.
 * @param executor Worker pool used for request dispatch.
 */
TcpServer::TcpServer(io::UringContext &context, RawHandler handler, ThreadPoolExecutor &executor)
    : TcpServer(context, std::move(handler), executor, ServerBackpressureLimits{}, 1) {}

/**
 * @brief Creates a TCP server with explicit connection-loop and backpressure settings.
 *
 * The accept loop runs on `context`; accepted sockets are handed to a connection I/O loop group so
 * request parsing, writes, and idle cleanup can be spread across event-loop threads.
 *
 * @param context Accept-loop io_uring context.
 * @param handler Raw RPC handler invoked by connections.
 * @param executor Worker pool used for request dispatch.
 * @param limits Per-connection and global resource limits.
 * @param connection_io_threads Number of connection-loop threads to start.
 * @param protocol_limits Frame and payload size limits.
 * @param connection_idle_timeout Idle timeout applied to accepted connections.
 */
TcpServer::TcpServer(io::UringContext &context, RawHandler handler, ThreadPoolExecutor &executor,
                     ServerBackpressureLimits limits, std::size_t connection_io_threads, ProtocolLimits protocol_limits,
                     std::chrono::milliseconds connection_idle_timeout)
    : context_(&context),
      handler_(std::move(handler)),
      executor_(&executor),
      backpressure_limits_(limits),
      protocol_limits_(protocol_limits),
      connection_idle_timeout_(connection_idle_timeout) {
  ValidateConnectionIoThreads(connection_io_threads);
  connection_io_loop_group_ = std::make_unique<ConnectionIoLoopGroup>(
      connection_io_threads, handler_, *executor_, backpressure_limits_, backpressure_stats_, io_stats_,
      protocol_limits_, connection_idle_timeout_);
}

/** @brief Releases the connection-loop group after shutdown. */
TcpServer::~TcpServer() = default;

/**
 * @brief Binds the listening socket and records the actual bound port.
 *
 * @param host Local address to bind.
 * @param port Local TCP port, or zero to let the OS choose.
 * @param backlog Listen backlog passed to the socket.
 */
void TcpServer::Listen(std::string_view host, std::uint16_t port, std::size_t backlog) {
  if (listen_socket_.valid()) {
    return;
  }

  listen_socket_.Bind(host, port);
  listen_socket_.Listen(static_cast<int>(backlog));
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
    const io::IoResult accept_result = co_await context_->Accept(listen_socket_.fd());

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
  context_->CancelFd(listen_socket_.fd());
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
