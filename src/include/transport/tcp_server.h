#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include "common/task.h"
#include "io/socket.h"
#include "io/uring_context.h"
#include "protocol/frame_codec.h"
#include "rpc/handler.h"
#include "transport/server_backpressure.h"
#include "transport/server_io_stats.h"
#include "transport/tcp_connection.h"
#include "transport/thread_pool_executor.h"

namespace xrpc {

class ConnectionIoLoopGroup;

/** @brief Fully normalized transport settings consumed directly by `TcpServer`. */
struct TcpServerConfig {
  int listen_backlog_;
  ConnectionBackpressureLimits backpressure_limits_;
  std::size_t connection_io_threads_;
  ProtocolLimits protocol_limits_;
  std::chrono::milliseconds connection_idle_timeout_;
};

/**
 * @brief Accept loop for the listening socket.
 *
 * Accepted sockets are handed to a `ConnectionIoLoopGroup` so connection I/O can scale independently of accept. The
 * server itself owns the listening socket, shared stats, and connection-loop group; each accepted connection owns its
 * socket after dispatch.
 */
class TcpServer final {
 public:
  /** @brief Creates a server from its accept context, request handler, executor, and transport config. */
  TcpServer(io::UringContext &accept_context, RawHandler handler, ThreadPoolExecutor &executor, TcpServerConfig config);

  /** @brief Stops accepting and closes connection loops before destroying state. */
  ~TcpServer();

  TcpServer(const TcpServer &) = delete;
  auto operator=(const TcpServer &) -> TcpServer & = delete;

  TcpServer(TcpServer &&) noexcept = delete;
  auto operator=(TcpServer &&) noexcept -> TcpServer & = delete;

  /** @brief Binds and starts listening on the server socket. */
  void Listen(std::string_view host, std::uint16_t port);

  /** @brief Runs the accept loop until `Stop()` is requested or accept fails. */
  [[nodiscard]] auto Run() -> runtime::Task<void>;

  /** @brief Requests shutdown of accept and connection I/O loops. */
  void Stop();

  /** @return Bound listen port after `Listen()`. */
  [[nodiscard]] auto port() const -> std::uint16_t { return port_; }

  /** @return Total number of tracked accepted connections. */
  [[nodiscard]] auto ConnectionCount() const -> std::size_t;

  /** @return Backpressure diagnostic snapshot. */
  [[nodiscard]] auto stats() const -> ServerBackpressureSnapshot { return backpressure_stats_.Snapshot(); }

  /** @return Server I/O diagnostic snapshot. */
  [[nodiscard]] auto io_stats() const -> ServerIoStatsSnapshot { return io_stats_.Snapshot(); }

  /** @return Aggregated post statistics from connection I/O loops. */
  [[nodiscard]] auto io_loop_post_stats() const -> io::UringPostStatsSnapshot;

 private:
  /** @brief Dispatches one accepted socket to the connection I/O loop group. */
  void StartConnection(io::Socket client_socket);

  /** @brief Applies non-blocking and close-on-exec flags to an accepted socket. */
  void SetSocketFlags(int fd) const;

  /** @brief Context that owns the accept coroutine. */
  io::UringContext *accept_context_;

  /** @brief Raw handler passed to accepted connections. */
  RawHandler handler_;

  /** @brief Worker pool used by accepted connections. */
  ThreadPoolExecutor *executor_ = nullptr;

  /** @brief Transport settings used by the listener and accepted connections. */
  TcpServerConfig config_;

  /** @brief Shared backpressure stats for accepted connections. */
  ServerBackpressureStats backpressure_stats_;

  /** @brief Shared write-path stats for accepted connections. */
  ServerIoStats io_stats_;

  /** @brief Connection loops that own accepted sockets after dispatch. */
  std::unique_ptr<ConnectionIoLoopGroup> connection_io_loop_group_;

  /** @brief Listening socket owned by the accept loop. */
  io::Socket listen_socket_;

  /** @brief Bound listen port. */
  std::uint16_t port_ = 0;

  /** @brief True after shutdown has been requested. */
  bool stopped_ = false;
};

}  // namespace xrpc
