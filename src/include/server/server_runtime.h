#pragma once

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include <xrpc/rpc_server.h>

#include "naming/consul_registrar.h"
#include "rpc/raw_message.h"
#include "server/server_runtime_config.h"
#include "server/service_registry.h"
#include "server/tcp_server.h"
#include "server/thread_pool_executor.h"

namespace xrpc {

/**
 * @brief Private server runtime owned by the public `RpcServer` facade.
 *
 * Design note:
 * - Ownership: `RpcServer` owns this runtime; the runtime owns the registry, worker pool, TCP transport runtime, and
 *   optional Consul registrar.
 * - State: public lifecycle methods are serialized and checked against `State`.
 * - Threading: accept and connection I/O stay on their io_uring loops, while handlers run on `ThreadPoolExecutor`.
 * - Shutdown: before `Run()`, `Stop()` closes the runtime directly; while running, `Run()` owns graceful drain and
 *   publishes the terminal state.
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
    Listening,
    Running,
    Stopping,
    Stopped,
  };

  /** @brief Registers this server in Consul when registration options are enabled. */
  void RegisterServiceIfEnabled(std::string_view host);

  /** @brief Completes graceful drain after shutdown has been requested. */
  void CompleteShutdown();

  /** @brief Stops all owned runtime components in dependency order. */
  void ShutdownComponents();

  /** @brief Stops all owned runtime components while suppressing cleanup failures. */
  void ShutdownComponentsBestEffort() noexcept;

  /** @brief Deregisters the Consul service if one was registered. */
  [[nodiscard]] auto TryDeregisterService() noexcept -> Status;

  ServerRuntimeConfig config_;
  ServiceRegistry registry_;
  ThreadPoolExecutor executor_;
  TcpServer server_;
  std::unique_ptr<ConsulRegistrar> registrar_;
  std::mutex lifecycle_mutex_;
  std::condition_variable lifecycle_cv_;
  State state_ = State::Created;
};

}  // namespace xrpc
