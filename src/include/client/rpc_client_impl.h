/**
 * @file rpc_client_impl.h
 * @brief Declares client-side discovery, routing, and failover orchestration.
 *
 * RpcClient::Impl maintains an immutable endpoint snapshot, selects the first
 * endpoint for each call, and performs only retries known to be safe.
 * Endpoint-local network state remains inside TcpTransport.
 */

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

#include "client/tcp_transport.h"
#include "naming/service_discovery.h"
#include "protocol/frame_codec.h"
#include "protocol/rpc_envelope.h"

namespace xrpc {

/**
 * @brief Coordinates service discovery, endpoint routing, and safe failover.
 *
 * Concurrent calls share immutable RoutingSnapshot objects. Transports are
 * reused across discovery updates so an unchanged endpoint keeps its existing
 * connection. A failed attempt is retried only when the transport reports that
 * no request bytes were committed.
 */
class RpcClient::Impl final {
 public:
  explicit Impl(const RpcClientOptions &options);
  ~Impl();

  [[nodiscard]] auto Call(std::string service_name, std::string method_name, std::string payload,
                          const CallOptions &options) -> StatusOr<std::string>;

 private:
  /** One virtual-node entry used for sticky endpoint selection. */
  struct HashRingEntry final {
    std::uint64_t hash_ = 0;
    std::size_t endpoint_index_ = 0;
  };

  /**
   * Immutable routing state for one discovery snapshot.
   *
   * `transports_[i]` belongs to `(*endpoints_)[i]`; hash-ring entries refer to
   * the same index space.
   */
  struct RoutingSnapshot final {
    std::shared_ptr<const DiscoverySnapshot> endpoints_;
    std::vector<std::shared_ptr<TcpTransport>> transports_;
    std::vector<HashRingEntry> hash_ring_;
  };

  [[nodiscard]] auto NextRequestId() -> std::uint64_t;

  /** Reconciles discovery membership and returns one immutable routing snapshot. */
  [[nodiscard]] auto ResolveRoutingSnapshot() -> StatusOr<std::shared_ptr<const RoutingSnapshot>>;

  /** Selects a starting endpoint and performs only commit-safe failover. */
  [[nodiscard]] auto CallWithFailover(const RequestEnvelope &request, const EffectiveCallOptions &options)
      -> CallAttemptResult;

  /** Returns the existing transport for an unchanged endpoint, if any. */
  [[nodiscard]] static auto FindReusableTransport(const std::shared_ptr<const RoutingSnapshot> &snapshot,
                                                  const Endpoint &endpoint) -> std::shared_ptr<TcpTransport>;

  [[nodiscard]] static auto MakeEndpointId(const Endpoint &endpoint) -> std::string;

  /** Builds the immutable virtual-node ring used by sticky routing. */
  [[nodiscard]] static auto BuildHashRing(const std::vector<Endpoint> &endpoints) -> std::vector<HashRingEntry>;

  [[nodiscard]] static auto SelectStickyStart(std::string_view sticky_key, const std::vector<HashRingEntry> &hash_ring)
      -> std::optional<std::size_t>;

  [[nodiscard]] static auto Fnv1a64(std::string_view value) -> std::uint64_t;

  ProtocolLimits protocol_limits_;
  std::chrono::milliseconds default_timeout_;
  std::size_t max_inflight_per_endpoint_;
  std::unique_ptr<ServiceDiscovery> discovery_;

  /** Serializes routing snapshot rebuilds after discovery membership changes. */
  std::mutex routing_update_mutex_;

  /** Immutable routing state atomically published to concurrent calls. */
  std::atomic<std::shared_ptr<const RoutingSnapshot>> routing_snapshot_;

  std::atomic<std::size_t> next_endpoint_index_{0};
  std::atomic<std::uint64_t> next_request_id_{1};
};

}  // namespace xrpc
