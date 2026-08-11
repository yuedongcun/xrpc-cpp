#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <xrpc/rpc_client.h>

#include "client/client_channel.h"
#include "naming/endpoint_resolver.h"

namespace xrpc {

/**
 * @brief Private runtime owned by the public `RpcClient` facade.
 *
 * Design note:
 * - Ownership: `RpcClient` owns `ClientRuntime`; the runtime owns resolver and channel.
 * - Discovery: `Init()` starts discovery and applies the first endpoint snapshot; calls may apply later snapshots
 *   before routing.
 * - Threading: request ids are atomic, while resolver snapshot application is serialized by `resolver_snapshot_mu_`.
 * - Facade: the public `RpcClient` remains move-only and small.
 */
class RpcClient::ClientRuntime final {
 public:
  /** @brief Builds resolver and channel objects from public client options. */
  explicit ClientRuntime(const RpcClientOptions &options);

  /** @brief Stops resolver work and closes channel transports. */
  ~ClientRuntime();

  /** @return Resolver startup and first-snapshot status. */
  [[nodiscard]] auto Init() -> Status;

  /** @return Response payload from one discovered and routed RPC call. */
  [[nodiscard]] auto Call(std::string service_name, std::string method_name, std::string payload,
                          const CallOptions &options) -> StatusOr<std::string>;

 private:
  friend class RpcClient;

  /** @brief Copies the latest resolver snapshot into the channel when it changes. */
  [[nodiscard]] auto ApplyResolverSnapshot() -> Status;

  /** @brief Allocates the next monotonically increasing request id. */
  [[nodiscard]] auto NextRequestId() -> std::uint64_t;

  /** @brief Endpoint resolver chosen from the target string. */
  std::unique_ptr<EndpointResolver> resolver_;

  /** @brief Routing and transport manager. */
  std::unique_ptr<ClientChannel> channel_;

  /** @brief Last resolver snapshot applied to the channel, used to skip no-op updates. */
  std::vector<Endpoint> last_applied_endpoints_;

  /** @brief Next request id assigned to a payload request. */
  std::atomic<std::uint64_t> next_request_id_{1};

  /** @brief Serializes resolver snapshot reads and channel updates. */
  mutable std::mutex resolver_snapshot_mu_;
};

}  // namespace xrpc
