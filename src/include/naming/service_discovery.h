/**
 * @file service_discovery.h
 * @brief Defines the client-side endpoint discovery boundary.
 *
 * Discovery implementations publish complete endpoint snapshots. `RpcClient`
 * consumes those snapshots for routing and owns discovery lifecycle.
 */

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <xrpc/rpc_client.h>
#include <xrpc/status.h>

namespace xrpc {

/** Complete endpoint membership published as one immutable snapshot. */
using DiscoverySnapshot = std::vector<Endpoint>;

/**
 * @brief Supplies complete snapshots of currently available service endpoints.
 *
 * Implementations may refresh endpoints in a background thread. `Snapshot()`
 * and `last_error()` must therefore be safe while refresh is active. `Start()`
 * and `Stop()` are serialized by the owning client implementation.
 */
class ServiceDiscovery {
 public:
  virtual ~ServiceDiscovery() = default;

  /** Starts discovery and publishes the first available snapshot. */
  [[nodiscard]] virtual auto Start() -> Status = 0;

  /** Stops background refresh and waits for its worker thread to exit. */
  virtual void Stop() = 0;

  /** Returns the latest immutable endpoint snapshot without copying its entries. */
  [[nodiscard]] virtual auto Snapshot() const -> std::shared_ptr<const DiscoverySnapshot> = 0;

  [[nodiscard]] virtual auto last_error() const -> std::string = 0;
};

/** Sorts endpoints by address and removes duplicates. */
[[nodiscard]] auto CanonicalizeEndpoints(std::vector<Endpoint> endpoints) -> std::vector<Endpoint>;

/** Creates discovery for a `list://host:port,...` or `consul://service` target. */
[[nodiscard]] auto MakeServiceDiscovery(std::string_view target, const std::string &consul_address)
    -> std::unique_ptr<ServiceDiscovery>;

}  // namespace xrpc
