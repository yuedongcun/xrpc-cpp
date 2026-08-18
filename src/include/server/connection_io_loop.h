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
#include "protocol/protocol_message.h"
#include "server/dispatch_mailbox.h"
#include "server/server_backpressure.h"
#include "server/server_connection.h"
#include "server/thread_pool_executor.h"

namespace xrpc {

class ServiceRegistry;

class ConnectionIoLoop final {
 public:
  ConnectionIoLoop(ServiceRegistry &registry, ThreadPoolExecutor &executor, ConnectionBackpressureLimits limits,
                   ProtocolLimits protocol_limits);

  ~ConnectionIoLoop();

  ConnectionIoLoop(const ConnectionIoLoop &) = delete;
  auto operator=(const ConnectionIoLoop &) -> ConnectionIoLoop & = delete;

  ConnectionIoLoop(ConnectionIoLoop &&) = delete;
  auto operator=(ConnectionIoLoop &&) -> ConnectionIoLoop & = delete;

  void Start();

  void Stop() noexcept;

  void BeginDrain();

  void FinishDrain();

  void PostStartConnection(io::Socket client_socket);

  void RethrowIfFailed() const;

 private:
  struct ConnectionEntry {
    std::shared_ptr<ServerConnection> connection_;
    runtime::Task<void> task_;
  };

  void StartConnection(io::Socket client_socket);

  void CleanupClosedConnections();

  void CloseConnections();

  void BeginDrainOnContext();

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
