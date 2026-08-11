#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include "naming/consul_http_client.h"
#include "naming/endpoint_resolver.h"

namespace xrpc {

/**
 * @brief Consul-backed endpoint resolver for one service name.
 *
 * Design note:
 * - Ownership: the resolver owns one Consul HTTP client and one refresh thread.
 * - State: `snapshot_`, `last_error_`, and `last_index_` are protected by `mutex_`.
 * - Refresh: `Start()` performs one immediate fetch, then the refresh thread uses Consul blocking queries once an index
 *   is available.
 * - Failure: failed refreshes keep the previous snapshot and update `last_error_`.
 */
class ConsulResolver final : public EndpointResolver {
 public:
  /**
   * @brief Creates a resolver using the default Consul HTTP client.
   *
   * @param service_name Consul service name to watch.
   * @param consul_address Consul agent host:port.
   * @param refresh_interval Maximum wait/backoff interval between refreshes.
   */
  ConsulResolver(std::string service_name, const std::string &consul_address,
                 std::chrono::milliseconds refresh_interval);

  /** @brief Stops the refresh thread before destroying resolver state. */
  ~ConsulResolver() override;

  ConsulResolver(const ConsulResolver &) = delete;
  auto operator=(const ConsulResolver &) -> ConsulResolver & = delete;

  /** @brief Performs the first fetch and starts the background refresh loop. */
  [[nodiscard]] auto Start() -> Status override;

  /** @brief Requests refresh loop shutdown and joins the thread. */
  void Stop() override;

  /** @return Copy of the most recent healthy endpoint snapshot. */
  [[nodiscard]] auto Snapshot() const -> std::vector<Endpoint> override;

  /** @return Last refresh error text, or empty string after a successful refresh. */
  [[nodiscard]] auto last_error() const -> std::string override;

 private:
  /** @brief Fetches one Consul service snapshot, optionally using a blocking query. */
  [[nodiscard]] auto Fetch(bool blocking) -> Status;

  /** @brief Background loop that refreshes service endpoints until stopped. */
  void RefreshLoop(std::stop_token stop_token);

  /** @brief Publishes a new endpoint snapshot and Consul index under the mutex. */
  void SetSnapshot(std::vector<Endpoint> endpoints, std::uint64_t index);

  /** @brief Records the last refresh error under the mutex. */
  void SetLastError(std::string error);

  /** @brief Builds the Consul catalog query path for immediate or blocking fetches. */
  [[nodiscard]] auto QueryPath(bool blocking) const -> std::string;

  /** @brief Consul service name being watched. */
  std::string service_name_;

  /** @brief HTTP client used for Consul catalog requests. */
  ConsulHttpClient http_client_;

  /** @brief Refresh wait/backoff interval. */
  std::chrono::milliseconds refresh_interval_;

  /** @brief Protects snapshot, last error, and blocking-query index. */
  mutable std::mutex mutex_;

  /** @brief Last known endpoint snapshot. */
  std::vector<Endpoint> snapshot_;

  /** @brief Last refresh error text. Empty after success. */
  std::string last_error_;

  /** @brief Last Consul index used for blocking queries. */
  std::uint64_t last_index_ = 0;

  /** @brief Background thread performing blocking Consul refreshes. */
  std::jthread refresh_thread_;
};

}  // namespace xrpc
