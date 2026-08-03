#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include <xrpc/rpc_server.h>

#include "common/task.h"
#include "io/uring_context.h"
#include "protocol/protocol_message.h"
#include "rpc/handler.h"
#include "rpc/naming/consul_registrar.h"
#include "rpc/raw_message.h"
#include "rpc/server/server_config.h"
#include "rpc/server/service_registry.h"
#include "transport/tcp_server.h"
#include "transport/thread_pool_executor.h"

namespace xrpc {

/**
 * @brief Private lifecycle controller owned by the public `RpcServer` facade.
 *
 * Design note:
 * - Ownership: `RpcServer` owns this controller; the controller owns the registry, event loop, worker pool, TCP server,
 *   and optional Consul registrar.
 * - State: public lifecycle methods are serialized and checked against `State`.
 * - Threading: io_uring work stays on `UringContext`, while handlers run on the `ThreadPoolExecutor`.
 * - Failure: shutdown is best-effort after construction, including destructor cleanup and Consul deregistration.
 */
class RpcServer::ServerController final {
 public:
  /** @brief Creates controller runtime state from public server options. */
  explicit ServerController(const RpcServerOptions &options);

  /** @brief Stops runtime work and deregisters Consul service if needed. */
  ~ServerController();

  /** @brief Registers one type-erased method before the server starts listening. */
  void RegisterMethodRegistration(MethodRegistration registration);

  /** @brief Binds the TCP listener and performs optional Consul registration. */
  void Listen(std::string_view host, std::uint16_t port);

  /** @brief Runs the accept loop until shutdown. */
  void Run();

  /** @brief Requests best-effort shutdown from any thread. */
  void Stop();

  /** @return Bound listener port after `Listen()`. */
  [[nodiscard]] auto port() const -> std::uint16_t;

  /** @return Snapshot of server runtime diagnostics. */
  [[nodiscard]] auto stats() const -> RpcServerStats;

  /** @return Raw response produced by the registered service registry. */
  [[nodiscard]] auto Dispatch(const RawRequest &request) const -> RawResponse;

  /** @return Protocol response produced from a decoded protocol request. */
  [[nodiscard]] auto Dispatch(const ProtocolRequest &request) const -> ProtocolResponse;

  /** @return Encoded response frame for one encoded request frame. */
  [[nodiscard]] auto DispatchFrame(std::string_view frame_bytes) const -> std::string;

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

  /** @brief Resolves zero worker count to a hardware-concurrency based value. */
  [[nodiscard]] static auto ResolveWorkerCount(std::size_t worker_threads) -> std::size_t;

  /** @brief Registers this server in Consul when registration options are enabled. */
  void RegisterServiceIfEnabled(std::string_view host);

  /**
   * @brief Performs ordered shutdown and records a status for public lifecycle calls.
   *
   * `Shutdown()` can be entered from `Run()`, `Stop()`, or the destructor. `run_loop_active` tells `StopRuntime()`
   * whether it is already on the `UringContext` thread path and must post server shutdown before stopping.
   */
  [[nodiscard]] auto Shutdown(bool run_loop_active) noexcept -> Status;

  /** @brief Runs shutdown and suppresses failures for destructors and cleanup paths. */
  void ShutdownBestEffort(bool run_loop_active = true) noexcept;

  /** @brief Deregisters the Consul service if one was registered. */
  [[nodiscard]] auto TryDeregisterService() noexcept -> Status;

  /** @brief Stops TCP server, event loop, and worker pool in lifecycle order. */
  void StopRuntime(bool run_loop_active);

  /** @brief Starts the TCP accept coroutine on the event-loop context. */
  void StartServerTaskOnContext();

  /** @brief Posts TCP server stop onto the event-loop context. */
  void RequestServerStopOnContext();

  /** @brief Blocks until the accept coroutine reaches completion. */
  void WaitForServerTaskCompletion() const;

  ServerConfig config_;
  ServiceRegistry registry_;
  io::UringContext context_;
  ThreadPoolExecutor executor_;
  TcpServer server_;
  std::optional<runtime::Task<void>> server_task_;
  std::size_t listen_backlog_ = 0;
  std::unique_ptr<ConsulRegistrar> registrar_;
  std::mutex lifecycle_operation_mutex_;
  std::mutex lifecycle_mutex_;
  Status shutdown_status_;
  State state_ = State::Created;
};

}  // namespace xrpc
