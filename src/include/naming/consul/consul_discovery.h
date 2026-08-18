/** @file consul_discovery.h @brief Declares Consul-backed service discovery. */

#pragma once

#include <chrono>
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
 * @brief Service discovery implementation with one Consul refresh thread.
 *
 * `Snapshot()` and `last_error()` synchronize with the refresh thread.
 * `Start()` and `Stop()` are called serially by the owning client runtime.
 */
class ConsulDiscovery final : public ServiceDiscovery {
 public:
  ConsulDiscovery(std::string service_name, const std::string &consul_address,
                  std::chrono::milliseconds refresh_interval);

  ~ConsulDiscovery() override;

  ConsulDiscovery(const ConsulDiscovery &) = delete;
  auto operator=(const ConsulDiscovery &) -> ConsulDiscovery & = delete;

  [[nodiscard]] auto Start() -> Status override;

  void Stop() override;

  [[nodiscard]] auto Snapshot() const -> std::vector<Endpoint> override;

  [[nodiscard]] auto last_error() const -> std::string override;

 private:
  [[nodiscard]] auto Fetch(bool blocking) -> Status;

  void RefreshLoop(const std::stop_token &stop_token);

  void SetSnapshot(std::vector<Endpoint> endpoints, std::uint64_t index);

  void SetLastError(std::string error);

  [[nodiscard]] auto QueryPath(bool blocking) const -> std::string;

  std::string service_name_;

  ConsulHttpClient http_client_;

  std::chrono::milliseconds refresh_interval_;

  mutable std::mutex mutex_;

  std::vector<Endpoint> snapshot_;

  std::string last_error_;

  std::uint64_t last_index_ = 0;

  std::jthread refresh_thread_;
};

}  // namespace xrpc
