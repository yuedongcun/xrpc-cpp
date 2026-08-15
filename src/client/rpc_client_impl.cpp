#include "client/rpc_client_impl.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include "client/client_config.h"
#include "common/xrpc_exception.h"

namespace xrpc {

namespace {

constexpr std::size_t VIRTUAL_NODE_COUNT = 128;
constexpr std::uint64_t FNV1A_OFFSET_BASIS = 14695981039346656037ULL;
constexpr std::uint64_t FNV1A_PRIME = 1099511628211ULL;

}  // namespace

RpcClient::Impl::Impl(const RpcClientOptions &options)
    : protocol_limits_(ValidateClientOptions(options)),
      default_timeout_(options.timeout_),
      max_inflight_per_endpoint_(options.max_inflight_per_endpoint_),
      discovery_(MakeServiceDiscovery(options.target_, options.consul_address_, options.discovery_refresh_interval_)) {
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

  RawRequest request;
  request.request_id_ = NextRequestId();
  request.service_name_ = std::move(service_name);
  request.method_name_ = std::move(method_name);
  request.payload_ = std::move(payload);

  const Status refresh_status = RefreshEndpoints();
  if (!refresh_status.ok()) {
    return StatusOr<std::string>(refresh_status);
  }

  RawCallResult result = CallEndpoints(request, options);
  if (!result.HasResponse()) {
    return StatusOr<std::string>(result.failure().status_);
  }

  const RawResponse &response = result.response();
  if (!response.status_.ok()) {
    return StatusOr<std::string>(response.status_);
  }
  return StatusOr<std::string>(response.payload_);
}

auto RpcClient::Impl::RefreshEndpoints() -> Status {
  std::lock_guard lock(endpoints_mutex_);

  const std::vector<Endpoint> discovered = discovery_->Snapshot();
  if (discovered.empty()) {
    std::string message = "service discovery has no endpoints";
    const std::string last_error = discovery_->last_error();
    if (!last_error.empty()) {
      message += ": " + last_error;
    }
    return {StatusCode::Unavailable, std::move(message)};
  }

  if (active_endpoints_ && Matches(*active_endpoints_, discovered)) {
    return Status::Ok();
  }

  std::unordered_map<std::string, std::shared_ptr<EndpointSlot>> reusable_slots;
  if (active_endpoints_) {
    reusable_slots.reserve(active_endpoints_->endpoints_.size());
    for (const std::shared_ptr<EndpointSlot> &slot : active_endpoints_->endpoints_) {
      reusable_slots.emplace(MakeEndpointId(slot->endpoint_), slot);
    }
  }

  auto next = std::make_shared<EndpointSet>();
  next->endpoints_.reserve(discovered.size());

  for (const Endpoint &endpoint : discovered) {
    std::string endpoint_id = MakeEndpointId(endpoint);
    auto existing = reusable_slots.find(endpoint_id);

    std::shared_ptr<EndpointSlot> slot;
    if (existing != reusable_slots.end()) {
      slot = existing->second;
    } else {
      slot = std::make_shared<EndpointSlot>(endpoint);
    }

    next->endpoints_.push_back(std::move(slot));
  }

  next->hash_ring_ = BuildHashRing(next->endpoints_);
  active_endpoints_ = std::move(next);
  return Status::Ok();
}

auto RpcClient::Impl::LoadEndpointSet() const -> std::shared_ptr<const EndpointSet> {
  std::lock_guard lock(endpoints_mutex_);
  return active_endpoints_;
}

auto RpcClient::Impl::CallEndpoints(const RawRequest &request, const CallOptions &options) -> RawCallResult {
  const EffectiveCallOptions effective_options = ResolveCallOptions(default_timeout_, options);
  RawCallResult last_result =
      MakeCallFailure({StatusCode::Unavailable, "no endpoints available"}, RequestCommitState::NotSent);

  const std::shared_ptr<const EndpointSet> endpoints = LoadEndpointSet();
  if (!endpoints || endpoints->endpoints_.empty()) {
    return last_result;
  }

  const std::size_t endpoint_count = endpoints->endpoints_.size();
  const std::optional<std::size_t> sticky_start =
      effective_options.sticky_key_.empty() ? std::nullopt
                                            : SelectStickyStart(effective_options.sticky_key_, endpoints->hash_ring_);
  const std::size_t start =
      sticky_start.has_value() ? *sticky_start : next_endpoint_index_.fetch_add(1) % endpoint_count;

  for (std::size_t offset = 0; offset < endpoint_count; ++offset) {
    const std::size_t index = (start + offset) % endpoint_count;
    last_result = CallAtEndpoint(endpoints->endpoints_[index], request, effective_options);
    if (last_result.HasResponse() || last_result.MustStopRetryToAvoidDuplicateRequest()) {
      return last_result;
    }
  }
  return last_result;
}

auto RpcClient::Impl::CallAtEndpoint(const std::shared_ptr<EndpointSlot> &endpoint, const RawRequest &request,
                                     const EffectiveCallOptions &options) -> RawCallResult {
  TcpTransport *transport = nullptr;
  {
    std::lock_guard lock(endpoint->mutex_);
    try {
      if (!endpoint->transport_) {
        endpoint->transport_ = std::make_unique<TcpTransport>(endpoint->endpoint_.host_, endpoint->endpoint_.port_,
                                                              protocol_limits_, max_inflight_per_endpoint_);
      }
      transport = endpoint->transport_.get();
    } catch (...) {
      return MakeCallFailure(CaughtExceptionToStatus("failed to create transport"), RequestCommitState::NotSent);
    }
  }

  return transport->Call(request, options);
}

auto RpcClient::Impl::MakeEndpointId(const Endpoint &endpoint) -> std::string {
  return endpoint.host_ + ":" + std::to_string(endpoint.port_);
}

auto RpcClient::Impl::BuildHashRing(const std::vector<std::shared_ptr<EndpointSlot>> &endpoints)
    -> std::vector<HashRingEntry> {
  std::vector<HashRingEntry> hash_ring;
  hash_ring.reserve(endpoints.size() * VIRTUAL_NODE_COUNT);
  for (std::size_t endpoint_index = 0; endpoint_index < endpoints.size(); ++endpoint_index) {
    const std::string endpoint_id = MakeEndpointId(endpoints[endpoint_index]->endpoint_);
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
  const auto entry =
      std::ranges::lower_bound(hash_ring, key_hash, {}, [](const HashRingEntry &item) -> std::uint64_t {
        return item.hash_;
      });
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

auto RpcClient::Impl::Matches(const EndpointSet &current, const std::vector<Endpoint> &discovered) -> bool {
  if (current.endpoints_.size() != discovered.size()) {
    return false;
  }
  for (std::size_t index = 0; index < discovered.size(); ++index) {
    if (current.endpoints_[index]->endpoint_ != discovered[index]) {
      return false;
    }
  }
  return true;
}

}  // namespace xrpc
