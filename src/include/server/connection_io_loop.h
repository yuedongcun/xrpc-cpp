/**
 * @file connection_io_loop.h
 * @brief Declares the server connection I/O loop.
 *
 * `ConnectionIoLoop` owns one `UringContext`, one I/O thread, and the server
 * connections assigned to that context.
 */

#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "common/task.h"
#include "io/socket.h"
#include "io/uring_context.h"
#include "protocol/frame_codec.h"
#include "server/connection_backpressure.h"
#include "server/dispatch_mailbox.h"
#include "server/server_connection.h"
#include "server/worker_pool.h"

namespace xrpc {

class ServiceRegistry;

/**
 * @brief Owns one server connection I/O execution domain.
 *
 * `RpcServer::Impl` calls the owner-side methods serially; they are not an
 * arbitrary concurrent API. `PostStartConnection()` and `BeginDrain()` post
 * their actual work to the `UringContext` thread. `connections_` is confined
 * to that thread, while lifecycle state and the live-connection count are
 * synchronized between the owner and context threads.
 */
class ConnectionIoLoop final {
 public:
  ConnectionIoLoop(ServiceRegistry &registry, WorkerPool &worker_pool, ConnectionBackpressureLimits limits,
                   ProtocolLimits protocol_limits);

  ~ConnectionIoLoop();

  ConnectionIoLoop(const ConnectionIoLoop &) = delete;
  auto operator=(const ConnectionIoLoop &) -> ConnectionIoLoop & = delete;

  ConnectionIoLoop(ConnectionIoLoop &&) = delete;
  auto operator=(ConnectionIoLoop &&) -> ConnectionIoLoop & = delete;

  // Owner-thread lifecycle API. The owning runtime calls these serially.
  void Start();

  // Owner-issued command. Publishes Draining before posting work to the I/O thread.
  void BeginDrain();

  // Owner-thread graceful shutdown operation. Not concurrent with Start().
  void FinishDrain();

  // Accept-thread command. Posts connection creation to the I/O thread.
  void PostStartConnection(io::Socket client_socket);

 private:
  enum class State : std::uint8_t {
    Created,
    Running,
    Draining,
    Stopped,
  };

  struct ConnectionEntry {
    std::shared_ptr<ServerConnection> connection_;
    runtime::Task<void> task_;
  };

  // Immediate fallback used only by destruction or failed shutdown cleanup.
  void StopImmediately() noexcept;

  // I/O-context-thread-only operations. They do not take a state lock.
  void StartConnectionOnContext(io::Socket client_socket);

  void CollectClosedConnections();

  void CloseConnectionsOnContext();

  // I/O-context-thread-only drain operation.
  void BeginDrainOnContext();

  // Invoked on the I/O context thread when a connection reaches Closed.
  void OnConnectionClosed();

  io::UringContext context_;
  std::shared_ptr<DispatchMailbox> dispatch_mailbox_;
  ServiceRegistry *registry_;
  WorkerPool *worker_pool_;
  ConnectionBackpressureLimits limits_;
  ProtocolLimits protocol_limits_;
  std::vector<ConnectionEntry> connections_;
  std::jthread thread_;
  std::exception_ptr error_;
  std::mutex drain_mutex_;
  std::condition_variable drain_cv_;
  std::size_t live_connections_ = 0;
  State state_ = State::Created;
};

}  // namespace xrpc
