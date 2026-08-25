#pragma once

/**
 * @file rpc_client.h
 * @brief Public synchronous RPC client API and client options.
 */

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <utility>

#include <xrpc/rpc_options.h>
#include <xrpc/status.h>

namespace xrpc {

/** A concrete TCP endpoint returned by service discovery. */
struct Endpoint {
  /** Numeric address or host name used for the TCP connection. */
  std::string host_;

  /** TCP port in host byte order. */
  std::uint16_t port_ = 0;

  auto operator==(const Endpoint &other) const -> bool = default;
};

/**
 * @brief Configuration used when creating an `RpcClient`.
 *
 * `target_` selects service discovery: `list://host:port,...` uses a fixed
 * endpoint list, while `consul://service-name` discovers healthy instances
 * through the configured Consul agent.
 */
struct RpcClientOptions {
  /** Discovery target. Must use the `list://` or `consul://` scheme. */
  std::string target_;

  /** Consul agent address used only by `consul://` targets. */
  std::string consul_address_{"127.0.0.1:8500"};

  /** Default call timeout. Zero disables the default deadline. */
  std::chrono::milliseconds timeout_{0};

  /** Maximum serialized request or response payload. */
  std::size_t max_payload_size_ = DEFAULT_MAX_PAYLOAD_SIZE;

  /** Maximum simultaneous RPCs using one discovered endpoint connection. */
  std::size_t max_inflight_per_endpoint_ = 1024;
};

/**
 * @brief A synchronous unary RPC client with endpoint discovery and failover.
 *
 * `Call()` and `CallPayload()` are safe to invoke concurrently. A client must
 * not be destroyed or moved while another thread is calling it.
 */
class RpcClient final {
 public:
  /**
   * @brief Creates a client and starts its configured service discovery.
   *
   * @return A client, or `InvalidArgument` when options are invalid.
   */
  [[nodiscard]] static auto Create(const RpcClientOptions &options) -> StatusOr<RpcClient>;

  ~RpcClient();

  RpcClient(const RpcClient &) = delete;
  auto operator=(const RpcClient &) -> RpcClient & = delete;

  RpcClient(RpcClient &&) noexcept;

  auto operator=(RpcClient &&) noexcept -> RpcClient &;

  /**
   * @brief Performs one synchronous unary RPC using serialized request bytes.
   *
   * Uses the client's default `CallOptions`. The result contains serialized
   * response bytes or a local/remote RPC failure status.
   */
  [[nodiscard]] auto CallPayload(std::string service_name, std::string method_name, std::string payload)
      -> StatusOr<std::string> {
    return CallPayload(std::move(service_name), std::move(method_name), std::move(payload), CallOptions{});
  }

  /**
   * @brief Performs one synchronous unary RPC using serialized request bytes.
   *
   * The call blocks until it receives a response, observes a failure, or its
   * effective timeout expires. Per-call options override the relevant client
   * defaults.
   */
  [[nodiscard]] auto CallPayload(std::string service_name, std::string method_name, std::string payload,
                                 const CallOptions &options) -> StatusOr<std::string>;

  /**
   * @brief Performs one synchronous unary Protobuf RPC with default options.
   *
   * `Req` and `Resp` must provide the usual Protobuf serialization API.
   */
  template <typename Resp, typename Req>
  [[nodiscard]] auto Call(std::string service_name, std::string method_name, const Req &request) -> StatusOr<Resp> {
    return Call<Resp>(std::move(service_name), std::move(method_name), request, CallOptions{});
  }

  /**
   * @brief Performs one synchronous unary Protobuf RPC with per-call options.
   *
   * Serialization or deserialization failures are returned as `Internal` or
   * `DataLoss`; a non-OK status returned by the server is forwarded unchanged.
   */
  template <typename Resp, typename Req>
  [[nodiscard]] auto Call(std::string service_name, std::string method_name, const Req &request,
                          const CallOptions &options) -> StatusOr<Resp> {
    std::string payload;
    try {
      payload = request.SerializeAsString();
    } catch (const std::exception &exception) {
      return StatusOr<Resp>(Status{StatusCode::Internal, exception.what()});
    } catch (...) {
      return StatusOr<Resp>(Status{StatusCode::Internal, "failed to serialize Protobuf request"});
    }

    StatusOr<std::string> payload_result =
        CallPayload(std::move(service_name), std::move(method_name), std::move(payload), options);
    if (!payload_result.ok()) {
      return StatusOr<Resp>(payload_result.status());
    }

    try {
      Resp response;
      if (!response.ParseFromString(payload_result.value())) {
        return StatusOr<Resp>(Status(StatusCode::DataLoss, "failed to decode Protobuf response"));
      }
      return StatusOr<Resp>(std::move(response));
    } catch (const std::exception &exception) {
      return StatusOr<Resp>(Status{StatusCode::Internal, exception.what()});
    } catch (...) {
      return StatusOr<Resp>(Status{StatusCode::Internal, "failed to decode Protobuf response"});
    }
  }

 private:
  class Impl;

  explicit RpcClient(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace xrpc
