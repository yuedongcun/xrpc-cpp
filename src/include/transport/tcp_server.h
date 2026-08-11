#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>

#include "common/task.h"
#include "io/socket.h"
#include "io/uring_context.h"
#include "protocol/frame_codec.h"
#include "rpc/raw_message.h"
#include "transport/server_backpressure.h"
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
 * The server owns the accept context and coroutine, listening socket, shared stats, and connection-loop group.
 * Accepted sockets are handed to a connection I/O loop so connection processing can scale independently of accept.
 */
class TcpServer final {
 public:
  /** @brief Creates a TCP runtime from its request handler, executor, and transport config. */
  TcpServer(const RawHandler &handler, ThreadPoolExecutor &executor, TcpServerConfig config);

  /** @brief Stops accepting and closes connection loops before destroying state. */
  ~TcpServer();

  TcpServer(const TcpServer &) = delete;
  auto operator=(const TcpServer &) -> TcpServer & = delete;

  TcpServer(TcpServer &&) noexcept = delete;
  auto operator=(TcpServer &&) noexcept -> TcpServer & = delete;

  /** @brief Binds and starts listening on the server socket. */
  void Listen(std::string_view host, std::uint16_t port);

  /** @brief Publishes the accept coroutine before the runtime enters its blocking loop. */
  void Start();

  /** @brief Drives the owned accept context until accepting stops or fails. */
  void Run();

  /** @brief Posts an accept-stop request from another thread. */
  void RequestStopAccepting();

  /** @brief Stops accepting new connections without stopping connection I/O loops. */
  void StopAccepting();

  /** @brief Starts graceful drain on all accepted connections. */
  void BeginDrain();

  /** @brief Waits for accepted connections to drain and stops their I/O loops. */
  void FinishDrain();

  /** @return Bound listen port after `Listen()`. */
  [[nodiscard]] auto port() const -> std::uint16_t { return port_; }

  /** @return Backpressure diagnostic snapshot. */
  [[nodiscard]] auto stats() const -> ServerBackpressureSnapshot { return backpressure_stats_.Snapshot(); }

 private:
  /** @brief Accepts sockets and dispatches them to connection I/O loops. */
  [[nodiscard]] auto AcceptLoop() -> runtime::Task<void>;

  /** @brief Applies non-blocking and close-on-exec flags to an accepted socket. */
  void SetSocketFlags(int fd) const;

  /** @brief Event-loop runtime dedicated to accepting TCP connections. */
  io::UringContext accept_context_;

  /** @brief Accept coroutine driven by `accept_context_`. */
  std::optional<runtime::Task<void>> accept_task_;

  /** @brief Transport settings used by the listener and accepted connections. */
  TcpServerConfig config_;

  /** @brief Shared backpressure stats for accepted connections. */
  ServerBackpressureStats backpressure_stats_;

  /** @brief Connection loops that own accepted sockets after dispatch. */
  std::unique_ptr<ConnectionIoLoopGroup> connection_io_loop_group_;

  /** @brief Listening socket owned by the accept loop. */
  io::Socket listen_socket_;

  /** @brief Bound listen port. */
  std::uint16_t port_ = 0;

  /** @brief True after the listener has stopped accepting new connections. */
  bool accept_stopped_ = false;
};

}  // namespace xrpc
