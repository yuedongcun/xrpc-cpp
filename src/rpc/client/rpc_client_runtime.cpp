#include "rpc/client/rpc_client_runtime.h"

#include <chrono>
#include <string>
#include <string_view>
#include <utility>

#include <xrpc/xrpc_exception.h>

#include "observability/rpc_metrics.h"
#include "rpc/client/client_config.h"

namespace xrpc {
namespace {

using SteadyClock = std::chrono::steady_clock;

/**
 * @brief Maps resolver implementation kind to a stable metrics label.
 */
[[nodiscard]] auto ResolverTypeLabel(ResolverKind resolver_kind) -> std::string_view {
  if (resolver_kind == ResolverKind::Consul) {
    return "consul";
  }
  return "static";
}

/**
 * @brief Records completion counters and latency for one client call.
 */
void RecordClientCallResult(std::string_view service_name, std::string_view method_name, StatusCode status_code,
                            SteadyClock::time_point started_at) {
  if (status_code == StatusCode::Ok) {
    RecordClientRpcCompleted(service_name, method_name, status_code);
  } else {
    RecordClientRpcFailed(service_name, method_name, status_code);
  }
  RecordClientRpcLatency(service_name, method_name, status_code,
                         std::chrono::duration_cast<std::chrono::nanoseconds>(SteadyClock::now() - started_at));
}

}  // namespace

/**
 * @brief Constructs resolver and channel state for the public client facade.
 */
RpcClient::ClientRuntime::ClientRuntime(const RpcClientOptions &options)
    : config_(NormalizeClientOptions(options)),
      resolver_(MakeEndpointResolver(ResolverOptions{
          .target_ = config_.target_,
          .consul_address_ = config_.consul_address_,
          .discovery_refresh_interval_ = config_.discovery_refresh_interval_,
      })),
      channel_(std::make_unique<ClientChannel>(config_)) {
  StartResolverAndDeferInitialFailure();
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
 * @brief Returns discovery snapshot-application counters.
 */
auto RpcClient::ClientRuntime::discovery_stats() const -> DiscoveryStats {
  return DiscoveryStats{
      .snapshot_apply_attempt_count_ = snapshot_apply_attempt_count_.load(),
      .snapshot_update_count_ = snapshot_update_count_.load(),
      .empty_snapshot_count_ = empty_snapshot_count_.load(),
  };
}

/**
 * @brief Returns resolver-specific counters and last error text.
 */
auto RpcClient::ClientRuntime::resolver_stats() const -> ResolverStats {
  ResolverStats result;
  result.last_error_ = resolver_->last_error();
  result.is_consul_resolver_ = resolver_->kind() == ResolverKind::Consul;
  const ResolverStatsSnapshot stats = resolver_->stats();
  result.refresh_success_count_ = stats.refresh_success_count_;
  result.refresh_failure_count_ = stats.refresh_failure_count_;
  result.empty_snapshot_count_ = stats.empty_snapshot_count_;
  return result;
}

/**
 * @brief Starts the resolver and records any initial failure for later calls.
 *
 * Some resolvers can start with an empty or failed first refresh. Calls surface that state through
 * `ApplyResolverSnapshot()` instead of throwing during client construction.
 */
void RpcClient::ClientRuntime::StartResolverAndDeferInitialFailure() {
  const Status status = resolver_->Start();
  if (status.ok()) {
    return;
  }

  // Resolver startup may perform an initial discovery request. A failed initial
  // refresh is reported through last_error() and ApplyResolverSnapshot().
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
 * The runtime records metrics at the facade boundary so failures from option validation, discovery, transport, and
 * server status all contribute to one consistent client-side view.
 */
auto RpcClient::ClientRuntime::Call(const RpcClient::PayloadRequest &request, const CallOptions &options)
    -> StatusOr<RpcClient::PayloadResponse> {
  const SteadyClock::time_point started_at = SteadyClock::now();
  const Status call_options_status = ValidateCallOptions(options);
  if (!call_options_status.ok()) {
    RecordClientCallResult(request.service_name_, request.method_name_, call_options_status.code(), started_at);
    return StatusOr<RpcClient::PayloadResponse>(call_options_status);
  }

  RawRequest raw_request;
  raw_request.request_id_ = request.request_id_;
  raw_request.service_name_ = request.service_name_;
  raw_request.method_name_ = request.method_name_;
  raw_request.payload_ = request.payload_;

  const Status snapshot_status = ApplyResolverSnapshot();
  if (!snapshot_status.ok()) {
    RecordClientCallResult(request.service_name_, request.method_name_, snapshot_status.code(), started_at);
    return StatusOr<RpcClient::PayloadResponse>(snapshot_status);
  }

  RawCallResult call_result = channel_->Call(raw_request, options);
  if (call_result.HasResponse()) {
    const RawResponse &raw_response = call_result.response();
    if (!raw_response.status_.ok()) {
      RecordClientCallResult(request.service_name_, request.method_name_, raw_response.status_.code(), started_at);
      return StatusOr<RpcClient::PayloadResponse>(raw_response.status_);
    }

    RpcClient::PayloadResponse response;
    response.request_id_ = call_result.request_id_;
    response.payload_ = raw_response.payload_;
    RecordClientCallResult(request.service_name_, request.method_name_, StatusCode::Ok, started_at);
    return StatusOr<RpcClient::PayloadResponse>(std::move(response));
  }

  RecordClientCallResult(request.service_name_, request.method_name_, call_result.failure().status_.code(), started_at);
  return StatusOr<RpcClient::PayloadResponse>(call_result.failure().status_);
}

/**
 * @brief Copies the latest resolver snapshot into the channel when it changes.
 *
 * Empty snapshots are treated as endpoint unavailability and include the resolver's last error when one is available.
 */
auto RpcClient::ClientRuntime::ApplyResolverSnapshot() -> Status {
  std::lock_guard lock(resolver_snapshot_mu_);
  snapshot_apply_attempt_count_.fetch_add(1);
  std::vector<Endpoint> snapshot = resolver_->Snapshot();
  if (snapshot.empty()) {
    RecordClientResolverEndpoints(config_.target_, ResolverTypeLabel(resolver_->kind()), snapshot.size());
    empty_snapshot_count_.fetch_add(1);
    std::string message = "resolver has no endpoints";
    const std::string last_error = resolver_->last_error();
    if (!last_error.empty()) {
      message += ": " + last_error;
    }
    return {StatusCode::Unavailable, std::move(message)};
  }
  if (snapshot != last_applied_endpoints_) {
    channel_->UpdateEndpoints(snapshot);
    RecordClientResolverEndpoints(config_.target_, ResolverTypeLabel(resolver_->kind()), snapshot.size());
    last_applied_endpoints_ = std::move(snapshot);
    snapshot_update_count_.fetch_add(1);
  }
  return Status::Ok();
}

}  // namespace xrpc
