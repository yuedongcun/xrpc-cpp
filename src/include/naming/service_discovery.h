#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <xrpc/rpc_client.h>
#include <xrpc/status.h>

namespace xrpc {

/**
 * @brief Publishes the current endpoint set for one client target.
 *
 * Design note:
 * - Ownership: `RpcClient::Impl` owns one discovery source chosen from the target string.
 * - Snapshot: `Snapshot()` returns a copy so client routing does not borrow discovery state.
 * - Failure: `Start()` may fail, but the client can defer the first discovery failure and let calls report endpoint
 *   unavailability.
 */
class ServiceDiscovery {
 public:
  virtual ~ServiceDiscovery() = default;

  /** @brief Starts resolver work and performs any required initial refresh. */
  [[nodiscard]] virtual auto Start() -> Status = 0;

  /** @brief Stops background resolver work. */
  virtual void Stop() = 0;

  /** @return Copy of the most recently known endpoint list. */
  [[nodiscard]] virtual auto Snapshot() const -> std::vector<Endpoint> = 0;

  /** @return Last refresh error text, or empty string when the last refresh succeeded. */
  [[nodiscard]] virtual auto last_error() const -> std::string = 0;
};

/** @return Canonicalized endpoints with invalid and duplicate entries removed. */
[[nodiscard]] auto CanonicalizeEndpoints(std::vector<Endpoint> endpoints) -> std::vector<Endpoint>;

/** @return Concrete discovery implementation selected by the target URI. */
[[nodiscard]] auto MakeServiceDiscovery(std::string_view target, const std::string &consul_address,
                                        std::chrono::milliseconds refresh_interval)
    -> std::unique_ptr<ServiceDiscovery>;

}  // namespace xrpc
