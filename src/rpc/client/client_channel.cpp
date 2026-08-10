#include "rpc/client/client_channel.h"

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>

#include "rpc/xrpc_exception.h"

#include "io/socket_error.h"
#include "rpc/client/client_config.h"

namespace xrpc {

void ClientChannel::EndpointStateTable::UpdateEndpoints(const std::vector<Endpoint> &endpoints) {
  std::unordered_set<std::string> next_endpoint_ids;
  next_endpoint_ids.reserve(endpoints.size());

  active_endpoint_ids_.clear();
  active_endpoint_ids_.reserve(endpoints.size());

  for (const Endpoint &endpoint : endpoints) {
    const std::string endpoint_id = MakeEndpointId(endpoint);
    next_endpoint_ids.insert(endpoint_id);
    active_endpoint_ids_.push_back(endpoint_id);

    auto &entry = endpoint_entries_[endpoint_id];
    entry.endpoint_ = endpoint;
    entry.draining_ = false;
  }

  for (auto &entry : endpoint_entries_) {
    if (next_endpoint_ids.contains(entry.first)) {
      continue;
    }
    if (!entry.second.draining_) {
      drained_endpoint_ids_.push_back(entry.first);
    }
    entry.second.draining_ = true;
  }
}

auto ClientChannel::EndpointStateTable::ActiveEndpointIds() const -> const std::vector<std::string> & {
  return active_endpoint_ids_;
}

auto ClientChannel::EndpointStateTable::FindEndpoint(const std::string &endpoint_id) const -> const Endpoint * {
  const auto it = endpoint_entries_.find(endpoint_id);
  if (it == endpoint_entries_.end()) {
    return nullptr;
  }
  return &it->second.endpoint_;
}

auto ClientChannel::EndpointStateTable::TakeDrainedEndpointIds() -> std::vector<std::string> {
  std::vector<std::string> drained_endpoint_ids = std::move(drained_endpoint_ids_);
  drained_endpoint_ids_.clear();
  return drained_endpoint_ids;
}

void ClientChannel::EndpointStateTable::CleanupDrainedEndpoints() {
  for (auto it = endpoint_entries_.begin(); it != endpoint_entries_.end();) {
    if (it->second.draining_) {
      it = endpoint_entries_.erase(it);
      continue;
    }
    ++it;
  }
}

auto ClientChannel::EndpointStateTable::MakeEndpointId(const Endpoint &endpoint) -> std::string {
  return endpoint.host_ + ":" + std::to_string(endpoint.port_);
}

/**
 * @brief Creates a channel with validated transport settings.
 *
 * @param default_timeout Client-wide timeout inherited by calls without an override.
 * @param protocol_limits Validated wire-protocol limits.
 * @param max_inflight_per_endpoint Per-endpoint pending call limit.
 */
ClientChannel::ClientChannel(std::chrono::milliseconds default_timeout, ProtocolLimits protocol_limits,
                             std::size_t max_inflight_per_endpoint)
    : default_timeout_(default_timeout),
      protocol_limits_(protocol_limits),
      max_inflight_per_endpoint_(max_inflight_per_endpoint) {}

/** @brief Releases endpoint transports through owned runtime state. */
ClientChannel::~ClientChannel() = default;

/**
 * @brief Publishes a new resolver endpoint snapshot.
 *
 * Snapshot publication is guarded by `state_mutex_`, but callers copy the immutable snapshot and release the mutex
 * before opening transports or sending requests.
 *
 * @param endpoints Resolver-provided endpoints to make active for future calls.
 */
void ClientChannel::UpdateEndpoints(const std::vector<Endpoint> &endpoints) {
  std::lock_guard lock(state_mutex_);
  endpoint_state_table_.UpdateEndpoints(endpoints);

  auto snapshot = std::make_shared<RoutingSnapshot>();
  snapshot->active_endpoint_ids_ = endpoint_state_table_.ActiveEndpointIds();
  snapshot->hash_ring_ = EndpointSelector::BuildHashRing(snapshot->active_endpoint_ids_);
  snapshot->active_endpoints_.reserve(snapshot->active_endpoint_ids_.size());
  for (const std::string &endpoint_id : snapshot->active_endpoint_ids_) {
    const Endpoint *endpoint = endpoint_state_table_.FindEndpoint(endpoint_id);
    if (endpoint == nullptr) {
      continue;
    }

    std::shared_ptr<EndpointRuntimeState> runtime_state = EnsureRuntimeEndpointState(endpoint_id);
    snapshot->active_endpoints_.push_back(ActiveEndpointSnapshot{
        .endpoint_id_ = endpoint_id,
        .endpoint_ = *endpoint,
        .runtime_state_ = std::move(runtime_state),
    });
  }

  routing_snapshot_ = std::move(snapshot);

  const std::vector<std::string> drained_endpoint_ids = endpoint_state_table_.TakeDrainedEndpointIds();
  for (const std::string &endpoint_id : drained_endpoint_ids) {
    endpoint_runtime_states_.erase(endpoint_id);
  }
  endpoint_state_table_.CleanupDrainedEndpoints();
}

/**
 * @brief Finds or creates shared runtime state for one endpoint id.
 *
 * Runtime state is intentionally separate from immutable routing snapshots so existing in-flight
 * calls can continue using an endpoint transport while discovery publishes the next snapshot.
 *
 * @param endpoint_id Stable endpoint id from `EndpointStateTable::MakeEndpointId()`.
 * @return Shared per-endpoint transport state.
 */
auto ClientChannel::EnsureRuntimeEndpointState(const std::string &endpoint_id)
    -> std::shared_ptr<EndpointRuntimeState> {
  auto it = endpoint_runtime_states_.find(endpoint_id);
  if (it != endpoint_runtime_states_.end()) {
    return it->second;
  }

  auto state = std::make_shared<EndpointRuntimeState>();
  auto [inserted_it, inserted] = endpoint_runtime_states_.emplace(endpoint_id, std::move(state));
  (void)inserted;
  return inserted_it->second;
}

/**
 * @brief Copies the currently published routing snapshot.
 *
 * @return Shared immutable snapshot, or null before the first endpoint update.
 */
auto ClientChannel::LoadRoutingSnapshot() const -> std::shared_ptr<const RoutingSnapshot> {
  std::lock_guard lock(state_mutex_);
  return routing_snapshot_;
}

/**
 * @brief Attempts to connect to at least one active endpoint.
 *
 * This is an eager warmup helper; normal calls still connect lazily if the transport for a selected endpoint has not
 * been created yet.
 *
 * @return `Status::Ok()` after one endpoint connects, otherwise the last connection failure.
 */
auto ClientChannel::EnsureConnected() -> Status {
  const EffectiveCallOptions effective_options = ResolveCallOptions(default_timeout_, CallOptions{});
  const std::shared_ptr<const RoutingSnapshot> snapshot = LoadRoutingSnapshot();
  if (!snapshot || snapshot->active_endpoints_.empty()) {
    return {StatusCode::Unavailable, "no endpoints available"};
  }

  const std::size_t endpoint_count = snapshot->active_endpoints_.size();
  const std::size_t start = endpoint_selector_.SelectRoundRobinStartIndex(endpoint_count);
  Status last_status{StatusCode::Unavailable, "transport connect failed"};

  for (std::size_t i = 0; i < endpoint_count; ++i) {
    const std::size_t idx = (start + i) % endpoint_count;
    last_status = EnsureConnectedAtEndpoint(snapshot->active_endpoints_[idx], effective_options);
    if (last_status.ok()) {
      return last_status;
    }
  }
  return last_status;
}

/**
 * @brief Sends one raw request through sticky or round-robin endpoint routing.
 *
 * Retry is allowed only while the transport can prove no request bytes reached the selected endpoint.
 *
 * @param request Raw request metadata and payload.
 * @param options User-facing per-call options.
 * @return Successful response, or the final failure result with commit state.
 */
auto ClientChannel::Call(const RawRequest &request, const CallOptions &options) -> RawCallResult {
  const EffectiveCallOptions effective_options = ResolveCallOptions(default_timeout_, options);
  RawCallResult last_result =
      MakeCallFailure({StatusCode::Unavailable, "no endpoints available"}, RequestCommitState::NotSent);

  const std::shared_ptr<const RoutingSnapshot> snapshot = LoadRoutingSnapshot();
  if (!snapshot || snapshot->active_endpoints_.empty()) {
    return last_result;
  }

  const std::size_t endpoint_count = snapshot->active_endpoints_.size();
  const std::optional<std::size_t> sticky_start =
      effective_options.sticky_key_.empty()
          ? std::nullopt
          : EndpointSelector::SelectStickyStartIndex(effective_options.sticky_key_, snapshot->hash_ring_,
                                                     snapshot->active_endpoint_ids_);
  const std::size_t start =
      sticky_start.has_value() ? *sticky_start : endpoint_selector_.SelectRoundRobinStartIndex(endpoint_count);

  for (std::size_t i = 0; i < endpoint_count; ++i) {
    const std::size_t idx = (start + i) % endpoint_count;
    last_result = CallAtEndpoint(snapshot->active_endpoints_[idx], request, effective_options);
    if (last_result.HasResponse()) {
      return last_result;
    }
    if (last_result.MustStopRetryToAvoidDuplicateRequest()) {
      // Once a request may have been sent, failover is unsafe: another endpoint
      // could execute the same non-idempotent RPC again.
      return last_result;
    }
  }

  return last_result;
}

/**
 * @brief Ensures the transport for one endpoint is connected.
 *
 * Transport construction and replacement are protected by the endpoint-local mutex, not by the channel snapshot mutex.
 *
 * @param endpoint Endpoint snapshot containing address and runtime state.
 * @param options Effective connect deadline.
 * @return `Status::Ok()` when connected, otherwise mapped socket or transport status.
 */
auto ClientChannel::EnsureConnectedAtEndpoint(const ActiveEndpointSnapshot &endpoint,
                                              const EffectiveCallOptions &options) -> Status {
  auto &state = *endpoint.runtime_state_;
  std::lock_guard lock(state.mutex_);
  try {
    if (!state.transport_) {
      state.transport_ = std::make_unique<TcpTransport>(endpoint.endpoint_.host_, endpoint.endpoint_.port_,
                                                        protocol_limits_, max_inflight_per_endpoint_);
    }
    state.transport_->EnsureConnected(options);
  } catch (const io::SocketError &error) {
    state.transport_.reset();
    return error.status();
  } catch (...) {
    state.transport_.reset();
    return CaughtExceptionToStatus("transport connect failed");
  }
  return Status::Ok();
}

/**
 * @brief Executes one call attempt against a specific endpoint.
 *
 * The endpoint mutex is held only while creating or retrieving the transport. The blocking call itself runs without the
 * mutex so other threads can use the same established transport concurrently.
 *
 * @param endpoint Endpoint selected for this attempt.
 * @param request Raw request metadata and payload.
 * @param options Effective deadline and sticky routing options.
 * @return Successful response or failure result from the endpoint transport.
 */
auto ClientChannel::CallAtEndpoint(const ActiveEndpointSnapshot &endpoint, const RawRequest &request,
                                   const EffectiveCallOptions &options) -> RawCallResult {
  auto &state = *endpoint.runtime_state_;

  TcpTransport *transport = nullptr;
  {
    std::lock_guard lock(state.mutex_);
    try {
      if (!state.transport_) {
        state.transport_ = std::make_unique<TcpTransport>(endpoint.endpoint_.host_, endpoint.endpoint_.port_,
                                                          protocol_limits_, max_inflight_per_endpoint_);
      }
      transport = state.transport_.get();
    } catch (...) {
      return MakeCallFailure(CaughtExceptionToStatus("failed to create transport"), RequestCommitState::NotSent);
    }
  }

  return transport->Call(request, options);
}

}  // namespace xrpc
