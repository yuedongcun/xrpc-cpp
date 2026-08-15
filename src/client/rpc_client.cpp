#include <xrpc/rpc_client.h>

#include <memory>
#include <string>
#include <utility>

#include "client/rpc_client_impl.h"
#include "common/xrpc_exception.h"

namespace xrpc {

/**
 * @brief Creates the private client runtime from explicit public options.
 *
 * @param options Target, resolver, protocol, timeout, and concurrency settings.
 */
auto RpcClient::Create(const RpcClientOptions &options) -> StatusOr<RpcClient> {
  try {
    return StatusOr<RpcClient>(RpcClient(std::make_unique<Impl>(options)));
  } catch (...) {
    return StatusOr<RpcClient>(CaughtExceptionToStatus("failed to create RPC client"));
  }
}

RpcClient::RpcClient(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

/** @brief Stops discovery and transport state through `Impl` destruction. */
RpcClient::~RpcClient() = default;

/** @brief Moves ownership of the private runtime from another facade. */
RpcClient::RpcClient(RpcClient &&) noexcept = default;

/** @brief Replaces this facade's runtime with another facade's runtime. */
auto RpcClient::operator=(RpcClient &&) noexcept -> RpcClient & = default;

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
    return impl_->Call(std::move(service_name), std::move(method_name), std::move(payload), options);
  } catch (...) {
    return StatusOr<std::string>(CaughtExceptionToStatus("RPC call failed"));
  }
}

}  // namespace xrpc
