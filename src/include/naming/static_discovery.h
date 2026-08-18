#pragma once

#include <string_view>
#include <vector>

#include "naming/service_discovery.h"

namespace xrpc {

class StaticDiscovery final : public ServiceDiscovery {
 public:
  explicit StaticDiscovery(std::string_view target);

  [[nodiscard]] auto Start() -> Status override;
  void Stop() override;
  [[nodiscard]] auto Snapshot() const -> std::vector<Endpoint> override;
  [[nodiscard]] auto last_error() const -> std::string override;

 private:
  std::vector<Endpoint> endpoints_;
};

}  // namespace xrpc
