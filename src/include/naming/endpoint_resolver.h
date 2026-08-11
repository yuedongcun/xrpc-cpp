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
 * @brief Endpoint discovery interface used by `RpcClient::ClientRuntime`.
 *
 * Design note:
 * - Ownership: `RpcClientRuntime` owns one resolver chosen from the target string.
 * - Snapshot: `Snapshot()` returns a copy so channel routing can update without holding resolver locks.
 * - Failure: `Start()` may fail, but runtime can still defer the first resolver failure and let calls report endpoint
 *   unavailability.
 */
class EndpointResolver {
 public:
  virtual ~EndpointResolver() = default;

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

/** @return Concrete resolver implementation selected by the target URI. */
[[nodiscard]] auto MakeEndpointResolver(std::string_view target, const std::string &consul_address,
                                        std::chrono::milliseconds refresh_interval)
    -> std::unique_ptr<EndpointResolver>;

}  // namespace xrpc
