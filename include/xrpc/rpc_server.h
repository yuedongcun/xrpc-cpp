#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <xrpc/method_registration.h>
#include <xrpc/protocol_options.h>

namespace xrpc {

/**
 * @brief Server configuration copied into the runtime before the listening socket is opened.
 *
 * Zero `worker_threads_` means "use hardware concurrency". Zero `connection_idle_timeout_` disables idle connection
 * cleanup. Consul registration fields are optional; when `service_name_` is empty, the server runs without Consul
 * registration.
 */
struct RpcServerOptions {
  /** @brief Number of handler worker threads, or zero to use hardware concurrency. */
  std::size_t worker_threads_ = 0;

  /** @brief Number of event-loop threads that own accepted connections and io_uring operations. */
  std::size_t connection_io_threads_ = 1;

  /** @brief Listen backlog passed to the TCP listening socket. */
  std::size_t listen_backlog_ = 128;

  /** @brief Maximum handler jobs that one connection may have in flight. */
  std::size_t max_inflight_per_connection_ = 128;

  /** @brief Maximum queued response bytes allowed for one connection before it is closed. */
  std::size_t max_write_queue_bytes_per_connection_ = 8U * 1024U * 1024U;

  /** @brief Global cap for handler jobs queued or running in the worker pool. */
  std::size_t max_pending_jobs_global_ = 10000;

  /** @brief Idle timeout for connections. A zero timeout disables idle cleanup. */
  std::chrono::milliseconds connection_idle_timeout_{0};

  /** @brief Maximum accepted request or response payload size. */
  std::size_t max_payload_size_ = DefaultMaxPayloadSize;

  /** @brief Optional service name to register in Consul. Empty means no Consul registration. */
  std::string service_name_;

  /** @brief Optional Consul service id. If empty, the runtime derives a stable id from bind information. */
  std::string service_id_;

  /** @brief Address advertised to Consul clients. Empty means use the listen host when possible. */
  std::string service_address_;

  /** @brief Port advertised to Consul clients. Zero means use the bound listen port. */
  std::uint16_t service_port_ = 0;

  /** @brief Consul agent address used for service registration and deregistration. */
  std::string consul_address_{"127.0.0.1:8500"};

  /** @brief Timeout for individual Consul HTTP registration calls. */
  std::chrono::milliseconds consul_timeout_{1000};
};

/**
 * @brief Snapshot of server-side resource guard and worker-pool diagnostics.
 *
 * Counters are monotonic diagnostics, except `max_*` fields which record high-water marks observed since server
 * construction. The values are intended for tests, metrics, and operational debugging; they are not synchronization
 * primitives.
 */
struct RpcServerStats {
  /** @brief Requests rejected because one connection exceeded its in-flight job limit. */
  std::uint64_t rejected_by_inflight_limit_ = 0;

  /** @brief Requests rejected because the worker pool exceeded its global pending-job limit. */
  std::uint64_t rejected_by_global_pending_limit_ = 0;

  /** @brief Connections closed after their queued response bytes exceeded the configured watermark. */
  std::uint64_t closed_by_write_queue_high_watermark_ = 0;

  /** @brief Highest per-connection in-flight job count observed. */
  std::uint64_t max_observed_inflight_ = 0;

  /** @brief Highest queued response byte count observed on one connection. */
  std::uint64_t max_observed_write_queue_bytes_ = 0;

  /** @brief Worker jobs rejected by the global worker-pool queue limit. */
  std::uint64_t worker_jobs_rejected_ = 0;

  /** @brief Highest global worker queue depth observed. */
  std::uint64_t max_observed_worker_queue_depth_ = 0;
};

/**
 * @brief Synchronous RPC server facade that owns the listening runtime and method registry.
 *
 * `RpcServer` is move-only because it owns event loops, worker threads, accepted sockets, and optional Consul
 * registration. Methods must be registered before `Listen()`. After `Run()` starts, handlers may execute concurrently
 * on worker threads, so any state captured by handlers must be safe for concurrent access.
 */
class RpcServer final {
 public:
  /** @brief Creates a server with default runtime and protocol limits. */
  RpcServer();

  /**
   * @brief Creates a server with explicit runtime, resource, and registration options.
   *
   * @param options Server options copied into the controller at construction time.
   */
  explicit RpcServer(const RpcServerOptions &options);

  /** @brief Stops the server runtime if it is still active. */
  ~RpcServer();

  RpcServer(const RpcServer &) = delete;
  auto operator=(const RpcServer &) -> RpcServer & = delete;

  /** @brief Moves the server controller from another server facade. */
  RpcServer(RpcServer &&) noexcept;

  /** @brief Replaces this server controller with another server facade's controller. */
  auto operator=(RpcServer &&) noexcept -> RpcServer &;

  /**
   * @brief Registers a typed protobuf method.
   *
   * Registration is only valid before `Listen()`. `Request` must parse from the request payload and `Response` must
   * serialize to the response payload. The handler may run concurrently with other calls to the same method.
   *
   * @param service_name Service namespace used by clients.
   * @param method_name Method name within the service.
   * @param func Callable that accepts a `Request` and returns a `Response`.
   */
  template <typename Request, typename Response, typename Func>
  void RegisterMethod(std::string service_name, std::string method_name, Func func) {
    RegisterMethodRegistration(
        MakeMethodRegistration<Request, Response>(std::move(service_name), std::move(method_name), std::move(func)));
  }

  /**
   * @brief Binds the listening socket and optionally registers the service in Consul.
   *
   * @param host Local address to bind.
   * @param port Local TCP port. A zero port lets the OS choose an available port.
   */
  void Listen(std::string_view host, std::uint16_t port);

  /**
   * @brief Runs the server event loop until `Stop()` or an unrecoverable runtime error.
   *
   * `Run()` is a blocking call and is not reentrant. Handler execution happens on the worker pool while connection I/O
   * remains on the connection event-loop threads.
   */
  void Run();

  /**
   * @brief Requests server shutdown.
   *
   * Stop is best-effort and idempotent. It may be called from another thread while `Run()` is blocked in the server
   * event loop. Shutdown closes the listener, accepted connections, worker pool, and Consul registration in order.
   */
  void Stop();

  /** @return The bound TCP port after `Listen()` succeeds. */
  [[nodiscard]] auto port() const -> std::uint16_t;

  /** @return A point-in-time snapshot of server resource guard diagnostics. */
  [[nodiscard]] auto stats() const -> RpcServerStats;

 private:
  class ServerController;

  /** @brief Registers one type-erased method descriptor with the private controller. */
  void RegisterMethodRegistration(MethodRegistration registration);

  std::unique_ptr<ServerController> controller_;
};

}  // namespace xrpc
