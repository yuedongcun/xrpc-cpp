/**
 * @file consul_discovery.h
 * @brief Defines endpoint discovery through Consul health queries.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include "naming/consul/consul_http_client.h"
#include "naming/service_discovery.h"

namespace xrpc {

/**
 * @brief Publishes healthy service endpoints from one Consul Agent.
 *
 * `Start()` performs an initial query and then starts one refresh thread. The
 * refresh thread uses Consul blocking queries after obtaining an index.
 * Successful queries atomically replace the complete endpoint snapshot;
 * failures preserve the previous snapshot and update `last_error()`.
 *
 * `Snapshot()` and `last_error()` may run concurrently with refresh. `Start()`
 * and `Stop()` are serialized by the owning client implementation.
 */
class ConsulDiscovery final : public ServiceDiscovery {
 public:
  ConsulDiscovery(std::string service_name, const std::string &consul_address);

  ~ConsulDiscovery() override;

  ConsulDiscovery(const ConsulDiscovery &) = delete;
  auto operator=(const ConsulDiscovery &) -> ConsulDiscovery & = delete;

  [[nodiscard]] auto Start() -> Status override;

  void Stop() override;

  [[nodiscard]] auto Snapshot() const -> std::shared_ptr<const DiscoverySnapshot> override;

  [[nodiscard]] auto last_error() const -> std::string override;

 private:
  /** Performs one immediate or Consul blocking health query. */
  [[nodiscard]] auto Fetch(bool use_blocking_query) -> Status;

  /** Runs blocking refresh queries until stop is requested. */
  void RefreshLoop(const std::stop_token &stop_token);

  /** Replaces the published snapshot and advances the Consul query index. */
  void SetSnapshot(std::vector<Endpoint> endpoints, std::uint64_t index);

  void SetLastError(std::string error);

  /** Builds the passing-only health query, optionally using the last index. */
  [[nodiscard]] auto QueryPath(bool use_blocking_query) const -> std::string;

  std::string service_name_;

  ConsulHttpClient http_client_;

  /** Immutable membership atomically published to concurrent callers. */
  std::atomic<std::shared_ptr<const DiscoverySnapshot>> snapshot_{std::make_shared<const DiscoverySnapshot>()};

  /** Protects the blocking-query index and last refresh error. */
  mutable std::mutex refresh_state_mutex_;

  std::string last_error_;

  std::uint64_t last_index_ = 0;

  std::jthread refresh_thread_;
};

}  // namespace xrpc
