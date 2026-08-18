#include "naming/service_discovery.h"

#include <algorithm>
#include <cctype>
#include <ranges>
#include <string_view>
#include <utility>

#include "common/xrpc_exception.h"
#include "naming/consul/consul_discovery.h"
#include "naming/static_discovery.h"

namespace xrpc {
namespace {

constexpr std::string_view LIST_SCHEME = "list://";
constexpr std::string_view CONSUL_SCHEME = "consul://";

auto Trim(std::string_view value) -> std::string_view {
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())) != 0) {
    value.remove_prefix(1);
  }
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())) != 0) {
    value.remove_suffix(1);
  }
  return value;
}

}  // namespace

auto CanonicalizeEndpoints(std::vector<Endpoint> endpoints) -> std::vector<Endpoint> {
  std::ranges::sort(endpoints, [](const Endpoint &lhs, const Endpoint &rhs) -> bool {
    if (lhs.host_ != rhs.host_) {
      return lhs.host_ < rhs.host_;
    }
    return lhs.port_ < rhs.port_;
  });
  const auto end = std::ranges::unique(endpoints, [](const Endpoint &lhs, const Endpoint &rhs) -> bool {
    return lhs.host_ == rhs.host_ && lhs.port_ == rhs.port_;
  });
  endpoints.erase(end.begin(), end.end());
  return endpoints;
}

auto MakeServiceDiscovery(std::string_view target, const std::string &consul_address,
                          std::chrono::milliseconds refresh_interval) -> std::unique_ptr<ServiceDiscovery> {
  target = Trim(target);

  if (target.starts_with(LIST_SCHEME)) {
    return std::make_unique<StaticDiscovery>(target);
  }

  if (target.starts_with(CONSUL_SCHEME)) {
    std::string service_name(Trim(target.substr(CONSUL_SCHEME.size())));
    if (service_name.empty()) {
      throw ConfigException("consul target requires a service name");
    }
    return std::make_unique<ConsulDiscovery>(std::move(service_name), consul_address, refresh_interval);
  }
  throw ConfigException("unsupported RpcClient target scheme");
}

}  // namespace xrpc
