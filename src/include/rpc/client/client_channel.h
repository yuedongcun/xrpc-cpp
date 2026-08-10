#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <xrpc/rpc_client.h>

#include "protocol/frame_codec.h"
#include "rpc/client/endpoint_selector.h"
#include "rpc/client/tcp_transport.h"
#include "rpc/raw_call_result.h"
#include "rpc/raw_message.h"

namespace xrpc {

/** @brief Mutable transport state shared by routing snapshots for one endpoint. */
struct EndpointRuntimeState final {
  std::mutex mutex_;
  std::unique_ptr<TcpTransport> transport_;
};

/** @brief Immutable endpoint entry published as part of a routing snapshot. */
struct ActiveEndpointSnapshot final {
  std::string endpoint_id_;
  Endpoint endpoint_;
  std::shared_ptr<EndpointRuntimeState> runtime_state_;
};

/**
 * @brief Owns client-side routing state and per-endpoint transports.
 *
 * Design note:
 * - Ownership: the channel owns endpoint runtime state and per-endpoint transports.
 * - State: endpoint refresh rebuilds an immutable `RoutingSnapshot`, then publishes it under `state_mutex_`.
 * - Calls: callers copy the current snapshot under the mutex, release the mutex, and then use per-endpoint locks while
 *   connecting or sending requests.
 * - Failure: retry stops once a request might have reached an endpoint.
 */
class ClientChannel final {
 public:
  /** @brief Creates a channel from validated client transport settings. */
  ClientChannel(std::chrono::milliseconds default_timeout, ProtocolLimits protocol_limits,
                std::size_t max_inflight_per_endpoint);

  /** @brief Closes owned transports before destroying endpoint runtime state. */
  ~ClientChannel();

  ClientChannel(const ClientChannel &) = delete;
  auto operator=(const ClientChannel &) -> ClientChannel & = delete;

  ClientChannel(ClientChannel &&) = delete;
  auto operator=(ClientChannel &&) -> ClientChannel & = delete;

  /**
   * @brief Ensures at least one active endpoint transport can connect.
   *
   * @return `Status::Ok()` on success, or endpoint-unavailable status when no active endpoint can connect.
   */
  [[nodiscard]] auto EnsureConnected() -> Status;

  /**
   * @brief Sends one raw request through the active routing snapshot.
   *
   * The channel tries endpoints in sticky or round-robin order. It retries only while failures are known to have
   * occurred before request bytes reached an endpoint.
   */
  [[nodiscard]] auto Call(const RawRequest &request, const CallOptions &options) -> RawCallResult;

  /**
   * @brief Publishes a new resolver endpoint snapshot.
   *
   * Existing endpoint runtime state is reused for unchanged endpoint ids. Removed endpoints are drained and closed
   * after the new snapshot is visible to callers.
   */
  void UpdateEndpoints(const std::vector<Endpoint> &endpoints);

 private:
  /** @brief Tracks active and draining endpoint identities across resolver snapshots. */
  class EndpointStateTable final {
   public:
    void UpdateEndpoints(const std::vector<Endpoint> &endpoints);

    [[nodiscard]] auto ActiveEndpointIds() const -> const std::vector<std::string> &;

    [[nodiscard]] auto FindEndpoint(const std::string &endpoint_id) const -> const Endpoint *;

    [[nodiscard]] auto TakeDrainedEndpointIds() -> std::vector<std::string>;

    void CleanupDrainedEndpoints();

    /** @brief Builds the stable endpoint identity used by routing and transport state. */
    [[nodiscard]] static auto MakeEndpointId(const Endpoint &endpoint) -> std::string;

   private:
    struct EndpointEntry {
      Endpoint endpoint_;
      bool draining_ = false;
    };

    std::unordered_map<std::string, EndpointEntry> endpoint_entries_;
    std::vector<std::string> active_endpoint_ids_;
    std::vector<std::string> drained_endpoint_ids_;
  };

  /** @brief Immutable routing view copied by callers before they leave `state_mutex_`. */
  struct RoutingSnapshot final {
    std::vector<std::string> active_endpoint_ids_;
    std::vector<ActiveEndpointSnapshot> active_endpoints_;
    std::vector<EndpointSelector::HashRingEntry> hash_ring_;
  };

  /** @brief Ensures the transport for one endpoint is connected. */
  [[nodiscard]] auto EnsureConnectedAtEndpoint(const ActiveEndpointSnapshot &endpoint,
                                               const EffectiveCallOptions &options) -> Status;

  /** @brief Attempts one raw call against a specific endpoint snapshot entry. */
  [[nodiscard]] auto CallAtEndpoint(const ActiveEndpointSnapshot &endpoint, const RawRequest &request,
                                    const EffectiveCallOptions &options) -> RawCallResult;

  /** @brief Copies the currently published routing snapshot under `state_mutex_`. */
  [[nodiscard]] auto LoadRoutingSnapshot() const -> std::shared_ptr<const RoutingSnapshot>;

  /** @brief Finds or creates mutable runtime state for an endpoint id. */
  [[nodiscard]] auto EnsureRuntimeEndpointState(const std::string &endpoint_id)
      -> std::shared_ptr<EndpointRuntimeState>;

  std::chrono::milliseconds default_timeout_;
  ProtocolLimits protocol_limits_;
  std::size_t max_inflight_per_endpoint_;

  /** @brief Protects endpoint table, runtime-state map, and routing snapshot publication. */
  mutable std::mutex state_mutex_;

  /** @brief Tracks active and drained endpoint ids across resolver updates. */
  EndpointStateTable endpoint_state_table_;

  /** @brief Sticky and round-robin endpoint selection helper. */
  EndpointSelector endpoint_selector_;

  /** @brief Mutable runtime state keyed by stable endpoint id. */
  std::unordered_map<std::string, std::shared_ptr<EndpointRuntimeState>> endpoint_runtime_states_;

  /** @brief Immutable routing snapshot currently visible to callers. */
  std::shared_ptr<const RoutingSnapshot> routing_snapshot_;
};

}  // namespace xrpc
