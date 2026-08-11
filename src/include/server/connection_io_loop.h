#pragma once

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "common/task.h"
#include "io/socket.h"
#include "io/uring_context.h"
#include "protocol/frame_codec.h"
#include "rpc/raw_message.h"
#include "server/dispatch_mailbox.h"
#include "server/server_backpressure.h"
#include "server/server_connection.h"
#include "server/thread_pool_executor.h"

namespace xrpc {

class ServiceRegistry;

/**
 * @brief Owns one connection event-loop thread and its assigned TCP connections.
 *
 * Design note:
 * - Ownership: one loop owns one `UringContext` thread and all `ServerConnection` objects assigned to that thread.
 * - Threading: accepted sockets are posted into the loop; connection creation, cleanup, and close all run on the
 *   `UringContext` thread.
 * - Failure: event-loop exceptions are captured and rethrown through `RethrowIfFailed()` after `Stop()`.
 */
class ConnectionIoLoop final {
 public:
  /** @brief Creates an unstarted connection I/O loop. */
  ConnectionIoLoop(ServiceRegistry &registry, ThreadPoolExecutor &executor, ConnectionBackpressureLimits limits,
                   ProtocolLimits protocol_limits);

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

  /** @brief Stops connection reads while keeping the loop alive for response drain. */
  void BeginDrain();

  /** @brief Waits for all connections to drain, then stops the loop. */
  void FinishDrain();

  /** @brief Posts an accepted socket into this loop for connection creation. */
  void PostStartConnection(io::Socket client_socket);

  /** @brief Rethrows an exception captured by the event-loop thread, if any. */
  void RethrowIfFailed() const;

 private:
  /** @brief Live connection and its coroutine task. */
  struct ConnectionEntry {
    std::shared_ptr<ServerConnection> connection_;
    runtime::Task<void> task_;
  };

  /** @brief Creates and starts one `ServerConnection` on this event-loop thread. */
  void StartConnection(io::Socket client_socket);

  /** @brief Removes closed connections whose tasks have completed. */
  void CleanupClosedConnections();

  /** @brief Closes all tracked connections during loop shutdown. */
  void CloseConnections();

  /** @brief Begins graceful drain on the event-loop thread. */
  void BeginDrainOnContext();

  /** @brief Accounts for one connection reaching its terminal state. */
  void OnConnectionClosed();

  io::UringContext context_;
  std::shared_ptr<DispatchMailbox> dispatch_mailbox_;
  ServiceRegistry *registry_;
  ThreadPoolExecutor *executor_;
  ConnectionBackpressureLimits limits_;
  ProtocolLimits protocol_limits_;
  std::vector<ConnectionEntry> connections_;
  std::jthread thread_;
  std::exception_ptr error_;
  std::mutex drain_mutex_;
  std::condition_variable drain_cv_;
  std::size_t live_connections_ = 0;
  bool drain_requested_ = false;
  bool drain_ready_ = false;
  bool started_ = false;
};

}  // namespace xrpc
