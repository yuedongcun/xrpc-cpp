/**
 * @file rpc_client.cpp
 * @brief Implements the synchronous RpcClient facade and its exception boundary.
 *
 * Public operations translate internal exceptions into StatusOr results and
 * delegate discovery, routing, and transport work to RpcClient::Impl.
 */

#include <xrpc/rpc_client.h>

#include <memory>
#include <string>
#include <utility>

#include "client/rpc_client_impl.h"
#include "common/xrpc_exception.h"

namespace xrpc {

auto RpcClient::Create(const RpcClientOptions &options) -> StatusOr<RpcClient> {
  try {
    return StatusOr<RpcClient>(RpcClient(std::make_unique<Impl>(options)));
  } catch (...) {
    return StatusOr<RpcClient>(CaughtExceptionToStatus("failed to create RPC client"));
  }
}

RpcClient::RpcClient(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

RpcClient::~RpcClient() = default;

RpcClient::RpcClient(RpcClient &&) noexcept = default;

auto RpcClient::operator=(RpcClient &&) noexcept -> RpcClient & = default;

auto RpcClient::CallPayload(std::string service_name, std::string method_name, std::string payload,
                            const CallOptions &options) -> StatusOr<std::string> {
  try {
    return impl_->Call(std::move(service_name), std::move(method_name), std::move(payload), options);
  } catch (...) {
    return StatusOr<std::string>(CaughtExceptionToStatus("RPC call failed"));
  }
}

}  // namespace xrpc
