/**
 * @file rpc_client_impl.cpp
 * @brief Implements discovery snapshots, endpoint selection, and safe failover.
 *
 * Call path: discovery snapshot -> endpoint selection -> TcpTransport attempt
 * -> commit-aware failover -> public result.
 */

#include "client/rpc_client_impl.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "common/xrpc_exception.h"

namespace xrpc {

namespace {

constexpr std::size_t VIRTUAL_NODE_COUNT = 128;
constexpr std::uint64_t FNV1A_OFFSET_BASIS = 14695981039346656037ULL;
constexpr std::uint64_t FNV1A_PRIME = 1099511628211ULL;

auto ValidateClientOptions(const RpcClientOptions &options) -> ProtocolLimits {
  if (options.target_.empty()) {
    throw ConfigException("RpcClient target must not be empty");
  }
  if (options.consul_address_.empty()) {
    throw ConfigException("RpcClient consul_address must not be empty");
  }
  if (options.timeout_ < std::chrono::milliseconds::zero()) {
    throw ConfigException("RpcClient timeout must not be negative");
  }
  if (options.max_inflight_per_endpoint_ == 0) {
    throw ConfigException("RpcClient max_inflight_per_endpoint must be greater than 0");
  }
  return MakeProtocolLimits(options.max_payload_size_);
}

auto ValidateCallOptions(const CallOptions &options) -> Status {
  if (options.timeout_ < std::chrono::milliseconds::zero()) {
    return {StatusCode::InvalidArgument, "CallOptions timeout must not be negative"};
  }
  return Status::Ok();
}

auto ResolveCallOptions(std::chrono::milliseconds default_timeout, const CallOptions &options) -> EffectiveCallOptions {
  EffectiveCallOptions effective_options;
  const std::chrono::milliseconds timeout =
      options.timeout_ > std::chrono::milliseconds::zero() ? options.timeout_ : default_timeout;
  if (timeout > std::chrono::milliseconds::zero()) {
    effective_options.deadline_ = std::chrono::steady_clock::now() + timeout;
  }
  effective_options.sticky_key_ = options.sticky_key_;
  return effective_options;
}

}  // namespace

RpcClient::Impl::Impl(const RpcClientOptions &options)
    : protocol_limits_(ValidateClientOptions(options)),
      default_timeout_(options.timeout_),
      max_inflight_per_endpoint_(options.max_inflight_per_endpoint_),
      discovery_(MakeServiceDiscovery(options.target_, options.consul_address_)) {
  (void)discovery_->Start();
}

RpcClient::Impl::~Impl() { discovery_->Stop(); }

auto RpcClient::Impl::NextRequestId() -> std::uint64_t { return next_request_id_.fetch_add(1); }

auto RpcClient::Impl::Call(std::string service_name, std::string method_name, std::string payload,
                           const CallOptions &options) -> StatusOr<std::string> {
  const Status call_options_status = ValidateCallOptions(options);
  if (!call_options_status.ok()) {
    return StatusOr<std::string>(call_options_status);
  }
  const EffectiveCallOptions effective_options = ResolveCallOptions(default_timeout_, options);

  RequestEnvelope request;
  request.request_id_ = NextRequestId();
  request.service_name_ = std::move(service_name);
  request.method_name_ = std::move(method_name);
  request.payload_ = std::move(payload);

  CallAttemptResult result = CallWithFailover(request, effective_options);
  if (!result.HasResponse()) {
    return StatusOr<std::string>(result.failure().status_);
  }

  const ResponseEnvelope &response = result.response();
  if (!response.status_.ok()) {
    return StatusOr<std::string>(response.status_);
  }
  return StatusOr<std::string>(response.payload_);
}

auto RpcClient::Impl::ResolveRoutingSnapshot() -> StatusOr<std::shared_ptr<const RoutingSnapshot>> {
  // Discovery publishes immutable snapshots. Pointer identity therefore means
  // that endpoint membership has not changed since the routing state was built.
  const std::shared_ptr<const DiscoverySnapshot> discovered = discovery_->Snapshot();
  if (!discovered || discovered->empty()) {
    std::string message = "service discovery has no endpoints";
    const std::string last_error = discovery_->last_error();
    if (!last_error.empty()) {
      message += ": " + last_error;
    }
    return StatusOr<std::shared_ptr<const RoutingSnapshot>>(Status{StatusCode::Unavailable, std::move(message)});
  }

  // The stable path is lock-free: concurrent calls share the already-published
  // routing snapshot while discovery membership remains unchanged.
  std::shared_ptr<const RoutingSnapshot> current = routing_snapshot_.load();
  if (current && current->endpoints_ == discovered) {
    return StatusOr<std::shared_ptr<const RoutingSnapshot>>(std::move(current));
  }

  // Membership changed. Serialize rebuilding, then check again because another
  // caller may have published the routing state while this caller waited.
  std::lock_guard lock(routing_update_mutex_);
  current = routing_snapshot_.load();
  if (current && current->endpoints_ == discovered) {
    return StatusOr<std::shared_ptr<const RoutingSnapshot>>(std::move(current));
  }

  auto next = std::make_shared<RoutingSnapshot>();
  next->endpoints_ = discovered;
  next->transports_.reserve(discovered->size());

  for (const Endpoint &endpoint : *discovered) {
    // An unchanged endpoint keeps its transport and established connection.
    std::shared_ptr<TcpTransport> transport = FindReusableTransport(current, endpoint);
    if (!transport) {
      try {
        transport = std::make_shared<TcpTransport>(endpoint.host_, endpoint.port_, protocol_limits_,
                                                   max_inflight_per_endpoint_);
      } catch (...) {
        return StatusOr<std::shared_ptr<const RoutingSnapshot>>(CaughtExceptionToStatus("failed to create transport"));
      }
    }

    next->transports_.push_back(std::move(transport));
  }

  // Publish only after transports and the consistent-hash ring form one
  // complete immutable view; in-flight calls may keep using the previous view.
  next->hash_ring_ = BuildHashRing(*discovered);
  std::shared_ptr<const RoutingSnapshot> published = std::move(next);
  routing_snapshot_.store(published);
  return StatusOr<std::shared_ptr<const RoutingSnapshot>>(std::move(published));
}

auto RpcClient::Impl::CallWithFailover(const RequestEnvelope &request, const EffectiveCallOptions &options)
    -> CallAttemptResult {
  StatusOr<std::shared_ptr<const RoutingSnapshot>> snapshot_result = ResolveRoutingSnapshot();
  if (!snapshot_result.ok()) {
    return MakeCallFailure(snapshot_result.status(), RequestCommitState::NotSent);
  }

  CallAttemptResult last_result =
      MakeCallFailure({StatusCode::Unavailable, "no endpoints available"}, RequestCommitState::NotSent);
  const std::shared_ptr<const RoutingSnapshot> snapshot = std::move(snapshot_result).value();

  const std::size_t endpoint_count = snapshot->transports_.size();
  const std::optional<std::size_t> sticky_start =
      options.sticky_key_.empty() ? std::nullopt : SelectStickyStart(options.sticky_key_, snapshot->hash_ring_);
  const std::size_t start =
      sticky_start.has_value() ? *sticky_start : next_endpoint_index_.fetch_add(1) % endpoint_count;

  for (std::size_t offset = 0; offset < endpoint_count; ++offset) {
    const std::size_t index = (start + offset) % endpoint_count;
    last_result = snapshot->transports_[index]->Call(request, options);
    if (last_result.HasResponse() || last_result.MustStopRetryToAvoidDuplicateRequest()) {
      return last_result;
    }
  }
  return last_result;
}

auto RpcClient::Impl::FindReusableTransport(const std::shared_ptr<const RoutingSnapshot> &snapshot,
                                            const Endpoint &endpoint) -> std::shared_ptr<TcpTransport> {
  if (!snapshot) {
    return nullptr;
  }

  for (std::size_t index = 0; index < snapshot->endpoints_->size(); ++index) {
    if ((*snapshot->endpoints_)[index] == endpoint) {
      return snapshot->transports_[index];
    }
  }
  return nullptr;
}

auto RpcClient::Impl::MakeEndpointId(const Endpoint &endpoint) -> std::string {
  return endpoint.host_ + ":" + std::to_string(endpoint.port_);
}

auto RpcClient::Impl::BuildHashRing(const std::vector<Endpoint> &endpoints) -> std::vector<HashRingEntry> {
  std::vector<HashRingEntry> hash_ring;
  hash_ring.reserve(endpoints.size() * VIRTUAL_NODE_COUNT);
  for (std::size_t endpoint_index = 0; endpoint_index < endpoints.size(); ++endpoint_index) {
    const std::string endpoint_id = MakeEndpointId(endpoints[endpoint_index]);
    for (std::size_t vnode = 0; vnode < VIRTUAL_NODE_COUNT; ++vnode) {
      hash_ring.push_back(HashRingEntry{
          .hash_ = Fnv1a64(endpoint_id + "#" + std::to_string(vnode)),
          .endpoint_index_ = endpoint_index,
      });
    }
  }
  std::ranges::sort(hash_ring, [](const HashRingEntry &lhs, const HashRingEntry &rhs) -> bool {
    if (lhs.hash_ != rhs.hash_) {
      return lhs.hash_ < rhs.hash_;
    }
    return lhs.endpoint_index_ < rhs.endpoint_index_;
  });
  return hash_ring;
}

auto RpcClient::Impl::SelectStickyStart(std::string_view sticky_key, const std::vector<HashRingEntry> &hash_ring)
    -> std::optional<std::size_t> {
  if (sticky_key.empty() || hash_ring.empty()) {
    return std::nullopt;
  }

  const std::uint64_t key_hash = Fnv1a64(sticky_key);
  const auto entry = std::ranges::lower_bound(hash_ring, key_hash, {},
                                              [](const HashRingEntry &item) -> std::uint64_t { return item.hash_; });
  return entry == hash_ring.end() ? hash_ring.front().endpoint_index_ : entry->endpoint_index_;
}

auto RpcClient::Impl::Fnv1a64(std::string_view value) -> std::uint64_t {
  std::uint64_t hash = FNV1A_OFFSET_BASIS;
  for (const char ch : value) {
    hash ^= static_cast<unsigned char>(ch);
    hash *= FNV1A_PRIME;
  }
  return hash;
}

}  // namespace xrpc
