#include <xrpc/rpc_client.h>

#include <memory>
#include <string>
#include <utility>

#include "rpc/client/rpc_client_runtime.h"
#include "rpc/xrpc_exception.h"

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
auto RpcClient::Create(std::string host, std::uint16_t port) -> StatusOr<RpcClient> {
  try {
    return Create(RpcClientOptions{.target_ = "list://" + std::move(host) + ":" + std::to_string(port)});
  } catch (...) {
    return StatusOr<RpcClient>(CaughtExceptionToStatus("failed to create RPC client"));
  }
}

/**
 * @brief Creates the private client runtime from explicit public options.
 *
 * @param options Target, resolver, protocol, timeout, and concurrency settings.
 */
auto RpcClient::Create(const RpcClientOptions &options) -> StatusOr<RpcClient> {
  try {
    return StatusOr<RpcClient>(RpcClient(std::make_unique<ClientRuntime>(options)));
  } catch (...) {
    return StatusOr<RpcClient>(CaughtExceptionToStatus("failed to create RPC client"));
  }
}

RpcClient::RpcClient(std::unique_ptr<ClientRuntime> runtime) : runtime_(std::move(runtime)) {}

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
auto RpcClient::Init() -> Status {
  try {
    return runtime_->Init();
  } catch (...) {
    return CaughtExceptionToStatus("failed to initialize RPC client");
  }
}

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
  try {
    return runtime_->Call(std::move(service_name), std::move(method_name), std::move(payload), options);
  } catch (...) {
    return StatusOr<std::string>(CaughtExceptionToStatus("RPC call failed"));
  }
}

}  // namespace xrpc
