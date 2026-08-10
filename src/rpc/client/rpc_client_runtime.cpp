#include "rpc/client/rpc_client_runtime.h"

#include <string>
#include <utility>

#include "rpc/xrpc_exception.h"

#include "rpc/client/client_config.h"
#include "rpc/raw_message.h"

namespace xrpc {

/**
 * @brief Constructs resolver and channel state for the public client facade.
 */
RpcClient::ClientRuntime::ClientRuntime(const RpcClientOptions &options) {
  const ProtocolLimits protocol_limits = ValidateClientOptions(options);
  resolver_ = MakeEndpointResolver(options.target_, options.consul_address_, options.discovery_refresh_interval_);
  channel_ = std::make_unique<ClientChannel>(options.timeout_, protocol_limits, options.max_inflight_per_endpoint_);
  (void)resolver_->Start();
}

/**
 * @brief Stops resolver background work before destroying channel state.
 */
RpcClient::ClientRuntime::~ClientRuntime() {
  if (resolver_) {
    resolver_->Stop();
  }
}

/**
 * @brief Applies the first resolver snapshot and warms at least one transport.
 */
auto RpcClient::ClientRuntime::Init() -> Status {
  Status snapshot_status = ApplyResolverSnapshot();
  if (!snapshot_status.ok()) {
    return std::move(snapshot_status);
  }

  return channel_->EnsureConnected();
}

/**
 * @brief Allocates a unique request id for the next payload call.
 */
auto RpcClient::ClientRuntime::NextRequestId() -> std::uint64_t {
  return next_request_id_.fetch_add(1, std::memory_order_relaxed);
}

/**
 * @brief Executes one public payload request through discovery, routing, and transport.
 *
 */
auto RpcClient::ClientRuntime::Call(std::string service_name, std::string method_name, std::string payload,
                                    const CallOptions &options) -> StatusOr<std::string> {
  const Status call_options_status = ValidateCallOptions(options);
  if (!call_options_status.ok()) {
    return StatusOr<std::string>(call_options_status);
  }

  RawRequest raw_request;
  raw_request.request_id_ = NextRequestId();
  raw_request.service_name_ = std::move(service_name);
  raw_request.method_name_ = std::move(method_name);
  raw_request.payload_ = std::move(payload);

  const Status snapshot_status = ApplyResolverSnapshot();
  if (!snapshot_status.ok()) {
    return StatusOr<std::string>(snapshot_status);
  }

  RawCallResult call_result = channel_->Call(raw_request, options);
  if (call_result.HasResponse()) {
    const RawResponse &raw_response = call_result.response();
    if (!raw_response.status_.ok()) {
      return StatusOr<std::string>(raw_response.status_);
    }
    return StatusOr<std::string>(raw_response.payload_);
  }

  return StatusOr<std::string>(call_result.failure().status_);
}

/**
 * @brief Copies the latest resolver snapshot into the channel when it changes.
 *
 * Empty snapshots are treated as endpoint unavailability and include the resolver's last error when one is available.
 */
auto RpcClient::ClientRuntime::ApplyResolverSnapshot() -> Status {
  std::lock_guard lock(resolver_snapshot_mu_);
  std::vector<Endpoint> snapshot = resolver_->Snapshot();
  if (snapshot.empty()) {
    std::string message = "resolver has no endpoints";
    const std::string last_error = resolver_->last_error();
    if (!last_error.empty()) {
      message += ": " + last_error;
    }
    return {StatusCode::Unavailable, std::move(message)};
  }
  if (snapshot != last_applied_endpoints_) {
    channel_->UpdateEndpoints(snapshot);
    last_applied_endpoints_ = std::move(snapshot);
  }
  return Status::Ok();
}

}  // namespace xrpc
