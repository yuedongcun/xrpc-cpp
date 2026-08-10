#pragma once

#include <chrono>
#include <cstddef>
#include <exception>
#include <memory>
#include <thread>
#include <vector>

#include "common/task.h"
#include "io/socket.h"
#include "io/uring_context.h"
#include "protocol/frame_codec.h"
#include "rpc/raw_message.h"
#include "transport/dispatch_completion_queue.h"
#include "transport/server_backpressure.h"
#include "transport/tcp_connection.h"
#include "transport/thread_pool_executor.h"

namespace xrpc {

/**
 * @brief Owns one connection event-loop thread and its assigned TCP connections.
 *
 * Design note:
 * - Ownership: one loop owns one `UringContext` thread and all `TcpConnection` objects assigned to that thread.
 * - Threading: accepted sockets are posted into the loop; connection creation, cleanup, and close all run on the
 *   `UringContext` thread.
 * - Failure: event-loop exceptions are captured and rethrown through `RethrowIfFailed()` after `Stop()`.
 */
class ConnectionIoLoop final {
 public:
  /** @brief Creates an unstarted connection I/O loop. */
  ConnectionIoLoop(RawHandler handler, ThreadPoolExecutor &executor, ConnectionBackpressureLimits limits,
                   ServerBackpressureStats &backpressure_stats, ProtocolLimits protocol_limits,
                   std::chrono::milliseconds connection_idle_timeout);

  /** @brief Stops the loop and closes owned connections. */
  ~ConnectionIoLoop();

  ConnectionIoLoop(const ConnectionIoLoop &) = delete;
  auto operator=(const ConnectionIoLoop &) -> ConnectionIoLoop & = delete;

  ConnectionIoLoop(ConnectionIoLoop &&) = delete;
  auto operator=(ConnectionIoLoop &&) -> ConnectionIoLoop & = delete;

  /** @brief Starts the loop thread and begins running the owned `UringContext`. */
  void Start();

  /** @brief Requests loop shutdown. Safe to call from shutdown paths. */
  void Stop() noexcept;

  /** @brief Posts an accepted socket into this loop for connection creation. */
  void PostStartConnection(io::Socket client_socket);

  /** @brief Rethrows an exception captured by the event-loop thread, if any. */
  void RethrowIfFailed() const;

 private:
  /** @brief Live connection and its coroutine task. */
  struct ConnectionEntry {
    std::shared_ptr<TcpConnection> connection_;
    runtime::Task<void> task_;
  };

  /** @brief Creates and starts one `TcpConnection` on this event-loop thread. */
  void StartConnection(io::Socket client_socket);

  /** @brief Removes closed connections whose tasks have completed. */
  void CleanupClosedConnections();

  /** @brief Closes all tracked connections during loop shutdown. */
  void CloseConnections();

  io::UringContext context_;
  std::shared_ptr<DispatchCompletionQueue> completion_queue_;
  RawHandler handler_;
  ThreadPoolExecutor *executor_;
  ConnectionBackpressureLimits limits_;
  ProtocolLimits protocol_limits_;
  std::chrono::milliseconds connection_idle_timeout_{0};
  ServerBackpressureStats *backpressure_stats_;
  std::vector<ConnectionEntry> connections_;
  std::jthread thread_;
  std::exception_ptr error_;
  bool started_ = false;
};

/**
 * @brief Round-robin dispatcher across connection I/O loops.
 *
 * The group owns loop lifetimes and keeps accepted sockets out of stopped loops by stopping all loops together.
 */
class ConnectionIoLoopGroup final {
 public:
  /** @brief Creates `loop_count` unstarted I/O loops. */
  ConnectionIoLoopGroup(std::size_t loop_count, const RawHandler &handler, ThreadPoolExecutor &executor,
                        ConnectionBackpressureLimits limits, ServerBackpressureStats &backpressure_stats,
                        ProtocolLimits protocol_limits, std::chrono::milliseconds connection_idle_timeout);

  /** @brief Stops all loops before destroying them. */
  ~ConnectionIoLoopGroup();

  ConnectionIoLoopGroup(const ConnectionIoLoopGroup &) = delete;
  auto operator=(const ConnectionIoLoopGroup &) -> ConnectionIoLoopGroup & = delete;

  ConnectionIoLoopGroup(ConnectionIoLoopGroup &&) = delete;
  auto operator=(ConnectionIoLoopGroup &&) -> ConnectionIoLoopGroup & = delete;

  /** @brief Starts all owned loops. */
  void Start();

  /** @brief Stops all owned loops. */
  void Stop() noexcept;

  /** @brief Dispatches an accepted socket to the next loop. */
  void Dispatch(io::Socket client_socket);

  /** @brief Rethrows the first loop failure observed after shutdown. */
  void RethrowIfFailed() const;

 private:
  std::vector<std::unique_ptr<ConnectionIoLoop>> loops_;
  std::size_t next_loop_index_ = 0;
};

}  // namespace xrpc
