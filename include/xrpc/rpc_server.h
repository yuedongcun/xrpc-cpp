#pragma once

/**
 * @file rpc_server.h
 * @brief Public RPC server API and service registration helpers.
 */

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <xrpc/rpc_options.h>
#include <xrpc/status.h>

namespace xrpc {

/**
 * @brief A serialized service method ready for registration.
 *
 * Applications normally create this through `MakeMethodRegistration()` rather
 * than filling `invoke_` directly.
 */
struct MethodRegistration final {
  /** Service name carried by incoming requests. */
  std::string service_name_;

  /** Method name within `service_name_`. */
  std::string method_name_;

  /** Converts serialized request bytes into serialized response bytes. */
  std::function<StatusOr<std::string>(std::string_view)> invoke_;
};

/**
 * @brief Adapts a typed unary Protobuf handler for `RpcServer` registration.
 *
 * `Request` must support `ParseFromArray`; `Response` must support
 * `SerializeToString`; and `func` must be callable as `Response(Request)`.
 * Handler exceptions become `Internal` RPC responses.
 */
template <typename Request, typename Response, typename Func>
auto MakeMethodRegistration(std::string service_name, std::string method_name, Func func)
    -> StatusOr<MethodRegistration> {
  try {
    MethodRegistration registration;
    registration.service_name_ = std::move(service_name);
    registration.method_name_ = std::move(method_name);
    registration.invoke_ = [func = std::move(func)](std::string_view payload) mutable -> StatusOr<std::string> {
      try {
        Request request;
        if (!request.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
          return StatusOr<std::string>(Status{StatusCode::InvalidArgument, "failed to parse protobuf request"});
        }

        Response response = func(request);
        std::string encoded;
        if (!response.SerializeToString(&encoded)) {
          return StatusOr<std::string>(Status{StatusCode::Internal, "failed to serialize protobuf response"});
        }
        return StatusOr<std::string>(std::move(encoded));
      } catch (const std::exception &exception) {
        return StatusOr<std::string>(Status{StatusCode::Internal, exception.what()});
      } catch (...) {
        return StatusOr<std::string>(Status{StatusCode::Internal, "handler threw unknown exception"});
      }
    };
    return StatusOr<MethodRegistration>(std::move(registration));
  } catch (const std::exception &exception) {
    return StatusOr<MethodRegistration>(Status{StatusCode::Internal, exception.what()});
  } catch (...) {
    return StatusOr<MethodRegistration>(Status{StatusCode::Internal, "failed to create method registration"});
  }
}

/**
 * @brief Configuration used when creating an `RpcServer`.
 *
 * Options are validated by `Create()`. Leaving `service_name_` empty disables
 * Consul registration; the remaining Consul registration fields are ignored.
 */
struct RpcServerOptions {
  /** Handler worker count. Zero selects `std::thread::hardware_concurrency()`. */
  std::size_t worker_threads_ = 0;

  /** Number of server connection I/O execution domains. Must be positive. */
  std::size_t connection_io_threads_ = 1;

  /** TCP listen backlog. Must be positive and fit the platform socket API. */
  std::size_t listen_backlog_ = 128;

  /** Maximum admitted RPCs on a single client connection. Must be positive. */
  std::size_t max_inflight_per_connection_ = 128;

  /** Maximum queued response bytes on one client connection. Must be positive. */
  std::size_t max_write_queue_bytes_per_connection_ = 8U * 1024U * 1024U;

  /** Maximum admitted worker jobs across all connections. Must be positive. */
  std::size_t max_pending_jobs_global_ = 10000;

  /** Maximum serialized request or response payload. Must be positive. */
  std::size_t max_payload_size_ = DEFAULT_MAX_PAYLOAD_SIZE;

  /** Consul service name. An empty value disables service registration. */
  std::string service_name_;

  /** Optional Consul service ID. An ID is generated when this is empty. */
  std::string service_id_;

  /** Optional advertised service address. Required for wildcard listen hosts. */
  std::string service_address_;

  /** Advertised service port. Zero uses the listen port. */
  std::uint16_t service_port_ = 0;

  /** Consul agent address used when service registration is enabled. */
  std::string consul_address_{"127.0.0.1:8500"};

  /** Timeout for Consul registration and deregistration requests. */
  std::chrono::milliseconds consul_timeout_{1000};
};

/**
 * @brief A Linux TCP server for unary Protobuf RPC methods.
 *
 * Register all methods before `Listen()`, then call `Run()` once. Registered
 * handlers execute on shared worker threads and may run concurrently.
 *
 * `Run()` has one owner and must not run concurrently with another `Run()`.
 * `Stop()` is the only cross-thread lifecycle operation: it is thread-safe and
 * idempotent. The application must not move or destroy a server while another
 * thread is using it.
 */
class RpcServer final {
 public:
  /**
   * @brief Validates options and creates a server in the registration state.
   *
   * @return A server, or `InvalidArgument` when options are invalid.
   */
  [[nodiscard]] static auto Create(const RpcServerOptions &options = {}) -> StatusOr<RpcServer>;

  ~RpcServer();

  RpcServer(const RpcServer &) = delete;
  auto operator=(const RpcServer &) -> RpcServer & = delete;

  RpcServer(RpcServer &&) noexcept;

  auto operator=(RpcServer &&) noexcept -> RpcServer &;

  /**
   * @brief Registers a typed unary Protobuf method before `Listen()`.
   *
   * The handler may be called concurrently after `Run()` begins. Registration
   * after listening returns `FailedPrecondition`. Complete server setup before
   * sharing the instance with the thread that calls `Run()`.
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
   * @brief Binds and starts listening on a TCP address.
   *
   * Must be called once after registration and before `Run()`. This is a
   * setup-phase operation, not a concurrent runtime operation.
   */
  [[nodiscard]] auto Listen(std::string_view host, std::uint16_t port) -> Status;

  /**
   * @brief Runs the accept loop until graceful shutdown completes.
   *
   * `Run()` must be called once after `Listen()`. Its return means admitted
   * handlers have completed and their queued responses have been flushed or
   * their connections closed. It is a single-owner blocking operation; another
   * thread may call only `Stop()` while it runs.
   */
  [[nodiscard]] auto Run() -> Status;

  /**
   * @brief Thread-safely requests graceful shutdown.
   *
   * Stops accepting new RPCs but lets already admitted handlers complete.
   * `Stop()` initiates shutdown; `Run()` returning confirms completion.
   */
  void Stop();

  /** Returns the bound TCP port after `Listen()` has completed. */
  [[nodiscard]] auto port() const -> StatusOr<std::uint16_t>;

 private:
  class Impl;

  explicit RpcServer(std::unique_ptr<Impl> impl);

  [[nodiscard]] auto RegisterMethod(MethodRegistration registration) -> Status;

  std::unique_ptr<Impl> impl_;
};

}  // namespace xrpc
