#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <xrpc/rpc_client.h>

#include "client/effective_call_options.h"
#include "client/raw_call_result.h"
#include "client/tcp_transport.h"
#include "naming/service_discovery.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_message.h"

namespace xrpc {

class RpcClient::Impl final {
 public:
  explicit Impl(const RpcClientOptions &options);
  ~Impl();

  [[nodiscard]] auto Call(std::string service_name, std::string method_name, std::string payload,
                          const CallOptions &options) -> StatusOr<std::string>;

 private:
  struct EndpointSlot final {
    explicit EndpointSlot(Endpoint endpoint) : endpoint_(std::move(endpoint)) {}

    Endpoint endpoint_;
    std::mutex mutex_;
    std::unique_ptr<TcpTransport> transport_;
  };

  struct HashRingEntry final {
    std::uint64_t hash_ = 0;
    std::size_t endpoint_index_ = 0;
  };

  struct EndpointSet final {
    std::vector<std::shared_ptr<EndpointSlot>> endpoints_;
    std::vector<HashRingEntry> hash_ring_;
  };

  [[nodiscard]] auto NextRequestId() -> std::uint64_t;

  [[nodiscard]] auto RefreshEndpoints() -> Status;

  [[nodiscard]] auto LoadEndpointSet() const -> std::shared_ptr<const EndpointSet>;

  [[nodiscard]] auto CallEndpoints(const RawRequest &request, const CallOptions &options) -> RawCallResult;

  [[nodiscard]] auto CallAtEndpoint(const std::shared_ptr<EndpointSlot> &endpoint, const RawRequest &request,
                                    const EffectiveCallOptions &options) -> RawCallResult;

  [[nodiscard]] static auto MakeEndpointId(const Endpoint &endpoint) -> std::string;

  [[nodiscard]] static auto BuildHashRing(const std::vector<std::shared_ptr<EndpointSlot>> &endpoints)
      -> std::vector<HashRingEntry>;

  [[nodiscard]] static auto SelectStickyStart(std::string_view sticky_key, const std::vector<HashRingEntry> &hash_ring)
      -> std::optional<std::size_t>;

  [[nodiscard]] static auto Fnv1a64(std::string_view value) -> std::uint64_t;

  [[nodiscard]] static auto Matches(const EndpointSet &current, const std::vector<Endpoint> &discovered) -> bool;

  ProtocolLimits protocol_limits_;
  std::chrono::milliseconds default_timeout_;
  std::size_t max_inflight_per_endpoint_;
  std::unique_ptr<ServiceDiscovery> discovery_;

  mutable std::mutex endpoints_mutex_;
  std::shared_ptr<const EndpointSet> active_endpoints_;

  std::atomic<std::size_t> next_endpoint_index_{0};
  std::atomic<std::uint64_t> next_request_id_{1};
};

}  // namespace xrpc
