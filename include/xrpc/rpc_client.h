#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include <xrpc/call_options.h>
#include <xrpc/protocol_options.h>
#include <xrpc/status.h>
#include <xrpc/status_or.h>
#include <xrpc/xrpc_exception.h>

namespace xrpc {

/**
 * @brief A concrete RPC endpoint address.
 *
 * The client treats equal host and port pairs as the same endpoint for connection reuse, endpoint-state lookup, and
 * sticky routing. The type is intentionally a small value object so resolver snapshots can copy endpoint lists without
 * sharing mutable state with the caller.
 */
struct Endpoint {
  /** @brief DNS name or numeric address used when opening the TCP connection. */
  std::string host_;

  /** @brief TCP port in host byte order. */
  std::uint16_t port_ = 0;

  /** @return true when both endpoints identify the same host and TCP port. */
  auto operator==(const Endpoint &other) const -> bool = default;
};

/**
 * @brief Client-wide configuration resolved once when `RpcClient` creates its runtime.
 *
 * `target_` selects either a static endpoint list or a resolver-backed target. Per-call options may override the
 * timeout and sticky routing key, but they do not change discovery, protocol limits, or the maximum in-flight request
 * count for an endpoint.
 */
struct RpcClientOptions {
  /** @brief Endpoint source such as a static list target or a resolver URI. */
  std::string target_;

  /** @brief Consul agent address used only for Consul resolver targets. */
  std::string consul_address_{"127.0.0.1:8500"};

  /** @brief Interval used by polling resolvers after the first endpoint snapshot. */
  std::chrono::milliseconds discovery_refresh_interval_{5000};

  /** @brief Default call timeout. A zero timeout leaves calls without a client-side deadline. */
  std::chrono::milliseconds timeout_{0};

  /** @brief Maximum accepted request or response payload size for this client. */
  std::size_t max_payload_size_ = DefaultMaxPayloadSize;

  /** @brief Per-endpoint limit for outstanding calls waiting on a transport response. */
  std::size_t max_inflight_per_endpoint_ = 1024;
};

/**
 * @brief Synchronous RPC client facade for raw payload calls and protobuf-typed calls.
 *
 * `RpcClient` owns a private runtime that hides endpoint discovery, connection reuse, request id allocation, retry
 * decisions, and wire-protocol encoding. Instances are move-only because their runtime owns threads, sockets, and
 * shared endpoint state. Public calls return `StatusOr` instead of throwing for normal RPC and transport failures.
 */
class RpcClient final {
 public:
  /**
   * @brief Creates a client for one static TCP endpoint.
   *
   * @param host DNS name or numeric address of the server.
   * @param port TCP port in host byte order.
   */
  RpcClient(std::string host, std::uint16_t port);

  /**
   * @brief Creates a client from explicit options.
   *
   * Construction validates configuration that can be checked locally. Resolver startup and the first endpoint snapshot
   * are performed by `Init()` or lazily by the first call path that needs routing.
   *
   * @param options Client runtime, discovery, and protocol configuration.
   */
  explicit RpcClient(const RpcClientOptions &options);

  /** @brief Stops owned transports and resolver work before destroying the runtime. */
  ~RpcClient();

  RpcClient(const RpcClient &) = delete;
  auto operator=(const RpcClient &) -> RpcClient & = delete;

  /** @brief Moves the client runtime from another client. */
  RpcClient(RpcClient &&) noexcept;

  /** @brief Replaces this client's runtime with another client's runtime. */
  auto operator=(RpcClient &&) noexcept -> RpcClient &;

  /**
   * @brief Initializes discovery and applies the first endpoint snapshot when a resolver is configured.
   *
   * Static endpoint clients may call this eagerly, but payload calls can still connect lazily. Calling `Init()` more
   * than once is safe; subsequent calls observe the already initialized runtime.
   *
   * @return `Status::Ok()` on success, or a resolver/configuration status when no usable endpoints can be loaded.
   */
  [[nodiscard]] auto Init() -> Status;

  /**
   * @brief Sends a raw payload request using default call options.
   *
   * Raw calls preserve payload bytes exactly. The client does not interpret the request or response body, so
   * application-level serialization errors are only reported by the typed `Call()` wrapper.
   *
   * @param service_name Service name registered on the server.
   * @param method_name Method name registered under the service.
   * @param payload Opaque request body bytes.
   * @return Response body bytes, or the RPC/transport failure status.
   */
  [[nodiscard]] auto CallPayload(std::string service_name, std::string method_name, std::string payload)
      -> StatusOr<std::string> {
    return CallPayload(std::move(service_name), std::move(method_name), std::move(payload), CallOptions{});
  }

  /**
   * @brief Sends a raw payload request with per-call options.
   *
   * The timeout in `options` is converted to one deadline shared by connection setup, retries, the write, and waiting
   * for the response. Once a request may have been sent to an endpoint, the client will not retry it on another
   * endpoint because the server may already be executing it.
   *
   * @param service_name Service name registered on the server.
   * @param method_name Method name registered under the service.
   * @param payload Opaque request body bytes.
   * @param options Per-call timeout and sticky routing overrides.
   * @return Response body bytes, or the RPC/transport failure status.
   */
  [[nodiscard]] auto CallPayload(std::string service_name, std::string method_name, std::string payload,
                                 const CallOptions &options) -> StatusOr<std::string>;

  /**
   * @brief Sends a protobuf-typed request using default call options.
   *
   * `Req` must provide `SerializeAsString()`, and `Resp` must provide `ParseFromString()`. Local serialization failure,
   * transport failure, server-side status, and response parse failure are all returned as `StatusOr<Resp>` errors.
   *
   * @param service_name Service name registered on the server.
   * @param method_name Method name registered under the service.
   * @param request Protobuf-compatible request object.
   * @return Decoded response object, or the failure status.
   */
  template <typename Resp, typename Req>
  [[nodiscard]] auto Call(std::string service_name, std::string method_name, const Req &request) -> StatusOr<Resp> {
    return Call<Resp>(std::move(service_name), std::move(method_name), request, CallOptions{});
  }

  /**
   * @brief Sends a protobuf-typed request with per-call options.
   *
   * The typed wrapper is a thin adapter over `CallPayload()`: it serializes the request before routing and parses the
   * response after the raw payload call succeeds. It does not hide transport or server status errors.
   *
   * @param service_name Service name registered on the server.
   * @param method_name Method name registered under the service.
   * @param request Protobuf-compatible request object.
   * @param options Per-call timeout and sticky routing overrides.
   * @return Decoded response object, or the failure status.
   */
  template <typename Resp, typename Req>
  [[nodiscard]] auto Call(std::string service_name, std::string method_name, const Req &request,
                          const CallOptions &options) -> StatusOr<Resp> {
    std::string payload;
    try {
      payload = request.SerializeAsString();
    } catch (...) {
      return StatusOr<Resp>(CaughtExceptionToStatus("failed to serialize typed request"));
    }

    StatusOr<std::string> payload_result =
        CallPayload(std::move(service_name), std::move(method_name), std::move(payload), options);
    if (!payload_result.ok()) {
      return StatusOr<Resp>(payload_result.status());
    }

    Resp response;
    if (!response.ParseFromString(payload_result.value())) {
      return StatusOr<Resp>(Status(StatusCode::DataLoss, "failed to decode typed response"));
    }
    return StatusOr<Resp>(std::move(response));
  }

 private:
  struct PayloadRequest {
    std::uint64_t request_id_ = 0;
    std::string service_name_;
    std::string method_name_;
    std::string payload_;
  };

  struct PayloadResponse {
    std::uint64_t request_id_ = 0;
    std::string payload_;
  };

  class ClientRuntime;

  /** @brief Sends a fully prepared payload request through the private runtime. */
  [[nodiscard]] auto CallPayloadRequest(const PayloadRequest &request, const CallOptions &options)
      -> StatusOr<PayloadResponse>;

  /** @brief Allocates the next request id from the private runtime. */
  [[nodiscard]] auto NextRequestId() -> std::uint64_t;

  std::unique_ptr<ClientRuntime> runtime_;
};

}  // namespace xrpc
