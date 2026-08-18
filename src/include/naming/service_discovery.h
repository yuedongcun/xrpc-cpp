#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <xrpc/rpc_client.h>
#include <xrpc/status.h>

namespace xrpc {

class ServiceDiscovery {
 public:
  virtual ~ServiceDiscovery() = default;

  [[nodiscard]] virtual auto Start() -> Status = 0;

  virtual void Stop() = 0;

  [[nodiscard]] virtual auto Snapshot() const -> std::vector<Endpoint> = 0;

  [[nodiscard]] virtual auto last_error() const -> std::string = 0;
};

[[nodiscard]] auto CanonicalizeEndpoints(std::vector<Endpoint> endpoints) -> std::vector<Endpoint>;

[[nodiscard]] auto MakeServiceDiscovery(std::string_view target, const std::string &consul_address,
                                        std::chrono::milliseconds refresh_interval)
    -> std::unique_ptr<ServiceDiscovery>;

}  // namespace xrpc
