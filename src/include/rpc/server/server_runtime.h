#pragma once

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include <xrpc/rpc_server.h>

#include "common/task.h"
#include "io/uring_context.h"
#include "rpc/naming/consul_registrar.h"
#include "rpc/raw_message.h"
#include "rpc/server/server_runtime_config.h"
#include "rpc/server/service_registry.h"
#include "transport/tcp_server.h"
#include "transport/thread_pool_executor.h"

namespace xrpc {

/**
 * @brief Private server runtime owned by the public `RpcServer` facade.
 *
 * Design note:
 * - Ownership: `RpcServer` owns this runtime; the runtime owns the registry, accept loop, worker pool, TCP server,
 *   and optional Consul registrar.
 * - State: public lifecycle methods are serialized and checked against `State`.
 * - Threading: accept and connection I/O stay on their io_uring loops, while handlers run on `ThreadPoolExecutor`.
 * - Shutdown: `Stop()` closes admission; `Run()` keeps workers and connection loops alive until admitted responses
 *   drain, then publishes the terminal state.
 */
class RpcServer::ServerRuntime final {
 public:
  /** @brief Creates runtime state from public server options. */
  explicit ServerRuntime(const RpcServerOptions &options);

  /** @brief Stops runtime work and deregisters Consul service if needed. */
  ~ServerRuntime();

  /** @brief Registers one type-erased method before the server starts listening. */
  void RegisterMethod(MethodRegistration registration);

  /** @brief Binds the TCP listener and performs optional Consul registration. */
  void Listen(std::string_view host, std::uint16_t port);

  /** @brief Runs the accept loop until shutdown. */
  void Run();

  /** @brief Requests graceful shutdown from any thread. */
  void Stop();

  /** @return Bound listener port after `Listen()`. */
  [[nodiscard]] auto port() const -> std::uint16_t;

  /** @return Snapshot of server runtime diagnostics. */
  [[nodiscard]] auto stats() const -> RpcServerStats;

 private:
  /** @brief Lifecycle state machine for public server operations. */
  enum class State : std::uint8_t {
    Created,
    Starting,
    Listening,
    Running,
    Stopping,
    Stopped,
  };

  /** @brief Registers this server in Consul when registration options are enabled. */
  void RegisterServiceIfEnabled(std::string_view host);

  /** @brief Completes graceful drain after shutdown has been requested. */
  void CompleteShutdown();

  /** @brief Completes graceful drain while suppressing cleanup failures. */
  void CompleteShutdownBestEffort() noexcept;

  /** @brief Deregisters the Consul service if one was registered. */
  [[nodiscard]] auto TryDeregisterService() noexcept -> Status;

  /** @brief Starts the TCP accept coroutine on the accept context. */
  void StartServerTaskOnAcceptContext();

  /** @brief Posts TCP server stop onto the accept context. */
  void RequestServerStopOnAcceptContext();

  /** @brief Blocks until the accept coroutine reaches completion. */
  void WaitForServerTaskCompletion() const;

  ServerRuntimeConfig config_;
  ServiceRegistry registry_;
  io::UringContext accept_context_;
  ThreadPoolExecutor executor_;
  TcpServer server_;
  std::optional<runtime::Task<void>> server_task_;
  std::unique_ptr<ConsulRegistrar> registrar_;
  std::mutex lifecycle_operation_mutex_;
  std::mutex lifecycle_mutex_;
  std::condition_variable lifecycle_cv_;
  State state_ = State::Created;
  bool listen_completed_ = false;
  bool run_active_ = false;
};

}  // namespace xrpc
