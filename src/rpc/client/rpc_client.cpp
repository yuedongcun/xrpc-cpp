#include <xrpc/rpc_client.h>

#include <memory>
#include <utility>

#include "rpc/client/rpc_client_runtime.h"

namespace xrpc {

/**
 * @brief Creates a client that targets one static TCP endpoint.
 *
 * The public convenience constructor is expressed as a `list://` target so the rest of the runtime
 * can use the same resolver/channel path as multi-endpoint clients.
 *
 * @param host Server host name or numeric address.
 * @param port Server TCP port.
 */
RpcClient::RpcClient(std::string host, std::uint16_t port)
    : RpcClient(RpcClientOptions{.target_ = "list://" + std::move(host) + ":" + std::to_string(port)}) {}

/**
 * @brief Creates the private client runtime from explicit public options.
 *
 * @param options Target, resolver, protocol, timeout, and concurrency settings.
 */
RpcClient::RpcClient(const RpcClientOptions &options) : runtime_(std::make_unique<ClientRuntime>(options)) {}

/** @brief Stops resolver and transport state through `ClientRuntime` destruction. */
RpcClient::~RpcClient() = default;

/** @brief Moves ownership of the private runtime from another facade. */
RpcClient::RpcClient(RpcClient &&) noexcept = default;

/** @brief Replaces this facade's runtime with another facade's runtime. */
auto RpcClient::operator=(RpcClient &&) noexcept -> RpcClient & = default;

/**
 * @brief Initializes resolver state and the first channel endpoint snapshot.
 *
 * @return `Status::Ok()` when the client can issue calls, otherwise a resolver/configuration status.
 */
auto RpcClient::Init() -> Status { return runtime_->Init(); }

/**
 * @brief Sends a raw payload request through the private runtime and returns only the response body.
 *
 * @param service_name Service namespace registered on the server.
 * @param method_name Method name registered under the service.
 * @param payload Request body bytes.
 * @param options Per-call timeout and sticky-key overrides.
 * @return Response payload bytes, or the failure status returned by runtime/channel/transport code.
 */
auto RpcClient::CallPayload(std::string service_name, std::string method_name, std::string payload,
                            const CallOptions &options) -> StatusOr<std::string> {
  PayloadRequest request;
  request.request_id_ = NextRequestId();
  request.service_name_ = std::move(service_name);
  request.method_name_ = std::move(method_name);
  request.payload_ = std::move(payload);

  StatusOr<PayloadResponse> result = CallPayloadRequest(request, options);
  if (!result.ok()) {
    return StatusOr<std::string>(result.status());
  }
  return StatusOr<std::string>(std::move(result.value().payload_));
}

/**
 * @brief Sends a fully prepared payload request through the private runtime.
 *
 * @param request Request metadata and payload. The request id must already be assigned.
 * @param options Per-call timeout and sticky-key overrides.
 * @return Full payload response metadata and body, or a public failure status.
 */
auto RpcClient::CallPayloadRequest(const PayloadRequest &request, const CallOptions &options)
    -> StatusOr<PayloadResponse> {
  return runtime_->Call(request, options);
}

/**
 * @brief Allocates a monotonically increasing client request id.
 *
 * @return Request id unique within this client runtime.
 */
auto RpcClient::NextRequestId() -> std::uint64_t { return runtime_->NextRequestId(); }

}  // namespace xrpc
