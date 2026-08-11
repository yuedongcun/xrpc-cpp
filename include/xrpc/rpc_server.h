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
#include <xrpc/status_or.h>

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
  std::size_t max_payload_size_ = DEFAULT_MAX_PAYLOAD_SIZE;

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
 * construction. The values are intended for tests and operational debugging; they are not synchronization
 * primitives.
 */
struct RpcServerStats {
  /** @brief Requests rejected because one connection exceeded its in-flight job limit. */
  std::uint64_t rejected_by_inflight_limit_ = 0;

  /** @brief Logical RPC jobs rejected because the worker pool exceeded its global pending-job limit. */
  std::uint64_t rejected_by_global_pending_limit_ = 0;

  /** @brief Connections closed after their queued response bytes exceeded the configured watermark. */
  std::uint64_t closed_by_write_queue_high_watermark_ = 0;

  /** @brief Highest per-connection in-flight job count observed. */
  std::uint64_t max_observed_inflight_ = 0;

  /** @brief Highest queued response byte count observed on one connection. */
  std::uint64_t max_observed_write_queue_bytes_ = 0;

  /** @brief Highest physical task depth observed on one worker queue. */
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
  /**
   * @brief Creates a server with validated runtime, resource, and registration options.
   *
   * @param options Server options copied into the runtime at construction time. Defaults are suitable for a basic
   * server.
   * @return A server, or a status describing configuration or runtime initialization failure.
   */
  [[nodiscard]] static auto Create(const RpcServerOptions &options = {}) -> StatusOr<RpcServer>;

  /** @brief Stops the server runtime if it is still active. */
  ~RpcServer();

  RpcServer(const RpcServer &) = delete;
  auto operator=(const RpcServer &) -> RpcServer & = delete;

  /** @brief Moves the server runtime from another server facade. */
  RpcServer(RpcServer &&) noexcept;

  /** @brief Replaces this server runtime with another server facade's runtime. */
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
  [[nodiscard]] auto RegisterMethod(std::string service_name, std::string method_name, Func func) -> Status {
    StatusOr<MethodRegistration> registration =
        MakeMethodRegistration<Request, Response>(std::move(service_name), std::move(method_name), std::move(func));
    if (!registration.ok()) {
      return registration.status();
    }
    return RegisterMethod(std::move(registration).value());
  }

  /**
   * @brief Binds the listening socket and optionally registers the service in Consul.
   *
   * @param host Local address to bind.
   * @param port Local TCP port. A zero port lets the OS choose an available port.
   */
  [[nodiscard]] auto Listen(std::string_view host, std::uint16_t port) -> Status;

  /**
   * @brief Runs the server event loop until `Stop()` or an unrecoverable runtime error.
   *
   * `Run()` is a blocking call and is not reentrant. Handler execution happens on the worker pool while connection I/O
   * remains on the connection event-loop threads.
   */
  [[nodiscard]] auto Run() -> Status;

  /**
   * @brief Requests server shutdown.
   *
   * Stop is thread-safe and idempotent. It closes admission and returns after requesting shutdown. `Run()` returns
   * only after already admitted handlers finish and their responses have been given a chance to drain.
   */
  void Stop();

  /** @return The bound TCP port after `Listen()` succeeds. */
  [[nodiscard]] auto port() const -> StatusOr<std::uint16_t>;

  /** @return A point-in-time snapshot of server resource guard diagnostics. */
  [[nodiscard]] auto stats() const -> RpcServerStats;

 private:
  class ServerRuntime;

  /** @brief Creates a facade from a successfully constructed private runtime. */
  explicit RpcServer(std::unique_ptr<ServerRuntime> runtime);

  /** @brief Registers one type-erased method descriptor with the private runtime. */
  [[nodiscard]] auto RegisterMethod(MethodRegistration registration) -> Status;

  std::unique_ptr<ServerRuntime> runtime_;
};

}  // namespace xrpc
