#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include <xrpc/rpc_client.h>

namespace xrpc {

/**
 * @brief Tracks active and draining endpoint identities across resolver snapshots.
 *
 * Drained endpoint ids are returned once so `ClientChannel` can close obsolete transports after publishing the new
 * snapshot. The table stores value copies of endpoints; transport lifetime is managed separately by
 * `EndpointRuntimeState`.
 */
class EndpointStateTable final {
 public:
  /**
   * @brief Applies a new endpoint snapshot.
   *
   * Endpoints missing from the new snapshot are marked as draining and will be reported by
   * `TakeDrainedEndpointIds()`. Existing endpoint ids keep their identity so sticky routing and transport reuse remain
   * stable across identical resolver snapshots.
   */
  void UpdateEndpoints(const std::vector<Endpoint> &endpoints);

  /** @return Active endpoint ids in routing order. */
  [[nodiscard]] auto ActiveEndpointIds() const -> const std::vector<std::string> &;

  /** @return Borrowed endpoint for `endpoint_id`, or null when it is not active. */
  [[nodiscard]] auto FindEndpoint(const std::string &endpoint_id) const -> const Endpoint *;

  /** @return Drained endpoint ids since the last call, clearing the drained-id queue. */
  [[nodiscard]] auto TakeDrainedEndpointIds() -> std::vector<std::string>;

  /** @brief Removes endpoint entries that have already been reported as drained. */
  void CleanupDrainedEndpoints();

  /**
   * @brief Builds the stable endpoint identity used by routing and transport state.
   *
   * V1 endpoint identity is exactly `host:port`. This string feeds endpoint state lookup and the sticky hash ring, so
   * changing it changes routing behavior.
   */
  [[nodiscard]] static auto MakeEndpointId(const Endpoint &endpoint) -> std::string;

 private:
  /** @brief Endpoint plus whether it is waiting for cleanup after removal. */
  struct EndpointEntry {
    Endpoint endpoint_;
    bool draining_ = false;
  };

  std::unordered_map<std::string, EndpointEntry> endpoint_entries_;
  std::vector<std::string> active_endpoint_ids_;
  std::vector<std::string> drained_endpoint_ids_;
};

}  // namespace xrpc
