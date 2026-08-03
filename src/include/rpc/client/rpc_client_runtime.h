#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <xrpc/rpc_client.h>

#include "rpc/client/client_channel.h"
#include "rpc/client/client_config.h"
#include "rpc/naming/endpoint_resolver.h"
#include "rpc/raw_message.h"

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
  /** @brief Diagnostics for resolver snapshot application into the channel. */
  struct DiscoveryStats {
    std::uint64_t snapshot_apply_attempt_count_ = 0;
    std::uint64_t snapshot_update_count_ = 0;
    std::uint64_t empty_snapshot_count_ = 0;
  };

  /** @brief Diagnostics copied from the active resolver implementation. */
  struct ResolverStats {
    bool is_consul_resolver_ = false;
    std::uint64_t refresh_success_count_ = 0;
    std::uint64_t refresh_failure_count_ = 0;
    std::uint64_t empty_snapshot_count_ = 0;
    std::string last_error_;
  };

  /** @brief Builds resolver and channel objects from public client options. */
  explicit ClientRuntime(const RpcClientOptions &options);

  /** @brief Stops resolver work and closes channel transports. */
  ~ClientRuntime();

  /** @return Resolver startup and first-snapshot status. */
  [[nodiscard]] auto Init() -> Status;

  /** @return Raw payload response converted to the public client response shape. */
  [[nodiscard]] auto Call(const RpcClient::PayloadRequest &request, const CallOptions &options)
      -> StatusOr<RpcClient::PayloadResponse>;

  /** @return Discovery snapshot application counters. */
  [[nodiscard]] auto discovery_stats() const -> DiscoveryStats;

  /** @return Resolver counters and last error text. */
  [[nodiscard]] auto resolver_stats() const -> ResolverStats;

 private:
  friend class RpcClient;

  /** @brief Starts discovery and stores any initial failure for later call paths. */
  void StartResolverAndDeferInitialFailure();

  /** @brief Copies the latest resolver snapshot into the channel when it changes. */
  [[nodiscard]] auto ApplyResolverSnapshot() -> Status;

  /** @brief Allocates the next monotonically increasing request id. */
  [[nodiscard]] auto NextRequestId() -> std::uint64_t;

  /** @brief Normalized configuration shared by resolver and channel. */
  ClientConfig config_;

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

  /** @brief Number of attempts to apply a resolver snapshot. */
  std::atomic<std::uint64_t> snapshot_apply_attempt_count_{0};

  /** @brief Number of times a changed snapshot was published to the channel. */
  std::atomic<std::uint64_t> snapshot_update_count_{0};

  /** @brief Number of empty resolver snapshots observed. */
  std::atomic<std::uint64_t> empty_snapshot_count_{0};
};

}  // namespace xrpc
