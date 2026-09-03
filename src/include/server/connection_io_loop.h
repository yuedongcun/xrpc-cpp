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
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "io/socket.h"
#include "io/uring_context.h"
#include "protocol/frame_codec.h"
#include "server/connection_backpressure.h"
#include "server/server_connection.h"
#include "server/worker_pool.h"

namespace xrpc {

class ServiceRegistry;

/** Encoded worker result returned to the connection's owning I/O loop. */
struct DispatchCompletion final {
  ConnectionId connection_id_ = 0;

  std::string response_bytes_;

  std::size_t completed_jobs_ = 1;

  bool encode_failed_ = false;

#ifdef XRPC_ENABLE_LATENCY_TRACE
  std::vector<std::uint64_t> trace_request_ids_;
#endif
};

/**
 * @brief Owns one server connection I/O execution domain.
 *
 * `RpcServer::Impl` calls the owner-side methods serially; they are not an
 * arbitrary concurrent API. `PostStartConnection()` and `BeginDrain()` post
 * their actual work to the `UringContext` thread. `connections_` is confined
 * to that thread and uniquely owns every connection. Worker completions carry
 * only a `ConnectionId`; lifecycle state and the live-connection count are
 * synchronized between the owner and context threads. The owning runtime
 * drains `WorkerPool` before destroying this loop, so posted completions and
 * their callbacks cannot outlive it.
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

  // Worker-thread command. Posts an encoded result to this loop's I/O thread.
  void PostDispatchCompletion(DispatchCompletion completion);

 private:
  enum class State : std::uint8_t {
    Created,
    Running,
    Draining,
    Stopped,
  };

  // Immediate fallback used only by destruction or failed shutdown cleanup.
  void StopImmediately() noexcept;

  // I/O-context-thread-only operations. They do not take a state lock.
  void StartConnectionOnContext(io::Socket client_socket);

  void CollectClosedConnections();

  void CloseConnectionsOnContext();

  void HandleDispatchCompletion(DispatchCompletion completion);

  // I/O-context-thread-only drain operation.
  void BeginDrainOnContext();

  // Invoked on the I/O context thread when a connection reaches Closed.
  void OnConnectionClosed();

  io::UringContext context_;
  ServiceRegistry &registry_;
  WorkerPool &worker_pool_;
  ConnectionBackpressureLimits limits_;
  ProtocolLimits protocol_limits_;
  std::unordered_map<ConnectionId, std::unique_ptr<ServerConnection>> connections_;
  ConnectionId next_connection_id_ = 1;
  std::jthread thread_;
  std::exception_ptr error_;
  std::mutex drain_mutex_;
  std::condition_variable drain_cv_;
  std::size_t live_connections_ = 0;
  State state_ = State::Created;
};

}  // namespace xrpc
