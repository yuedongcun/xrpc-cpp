/** @file rpc_server_impl.h @brief Declares the private RpcServer runtime implementation. */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <xrpc/rpc_server.h>

#include "common/task.h"
#include "io/socket.h"
#include "io/uring_context.h"
#include "server/connection_io_loop.h"
#include "server/server_config.h"
#include "server/service_registry.h"
#include "server/worker_pool.h"

namespace xrpc {

class ConsulRegistrar;

class RpcServer::Impl final {
 public:
  explicit Impl(const RpcServerOptions &options);
  ~Impl();

  void RegisterMethod(MethodRegistration registration);
  void Listen(std::string_view host, std::uint16_t port);
  void Run();
  void Stop();

  [[nodiscard]] auto port() const -> std::uint16_t;

 private:
  enum class State : std::uint8_t {
    Created,
    Listening,
    Running,
    Stopping,
    Stopped,
  };

  void StartAcceptLoop();
  [[nodiscard]] auto AcceptLoop() -> runtime::Task<void>;

  // Thread-safe request that forwards accept shutdown to the accept context.
  void RequestStopAccepting();

  // Accept-context-thread-only. Closes the listener and cancels pending accept.
  void StopAcceptingOnContext();

  void DispatchAcceptedConnection(io::Socket socket);

  void StartConnectionLoops();
  void BeginConnectionDrain();
  void FinishConnectionDrain();

  [[nodiscard]] auto TryDeregisterService() noexcept -> Status;

  void CompleteShutdown();
  void ShutdownComponents();
  void ShutdownComponentsBestEffort() noexcept;

  ServerConfig config_;
  ServiceRegistry registry_;

  WorkerPool worker_pool_;
  std::unique_ptr<ConsulRegistrar> registrar_;

  io::UringContext accept_context_;
  std::vector<std::unique_ptr<ConnectionIoLoop>> connection_io_loops_;
  std::optional<runtime::Task<void>> accept_task_;
  io::Socket listen_socket_;

  std::mutex lifecycle_mutex_;
  State state_ = State::Created;

  std::size_t next_connection_io_loop_ = 0;
  std::string listen_host_;
  std::uint16_t port_ = 0;
  bool accept_stopped_ = false;
};

}  // namespace xrpc
