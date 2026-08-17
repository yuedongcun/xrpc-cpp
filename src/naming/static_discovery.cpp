#include "naming/static_discovery.h"

#include <cctype>
#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "common/xrpc_exception.h"

namespace xrpc {
namespace {

constexpr std::string_view LIST_SCHEME = "list://";

auto Trim(std::string_view value) -> std::string_view {
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())) != 0) {
    value.remove_prefix(1);
  }
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())) != 0) {
    value.remove_suffix(1);
  }
  return value;
}

auto ParsePort(std::string_view port_text) -> std::uint16_t {
  int parsed_port = 0;
  const auto result = std::from_chars(port_text.data(), port_text.data() + port_text.size(), parsed_port);
  if (result.ec != std::errc{} || result.ptr != port_text.data() + port_text.size() || parsed_port <= 0 ||
      parsed_port > 65535) {
    throw ConfigException("invalid endpoint port");
  }
  return static_cast<std::uint16_t>(parsed_port);
}

auto ParseEndpoint(std::string_view endpoint_text) -> Endpoint {
  endpoint_text = Trim(endpoint_text);
  const std::size_t colon = endpoint_text.rfind(':');
  if (colon == std::string_view::npos || colon == 0 || colon + 1 >= endpoint_text.size()) {
    throw ConfigException("invalid list target endpoint");
  }

  std::string_view host = Trim(endpoint_text.substr(0, colon));
  std::string_view port = Trim(endpoint_text.substr(colon + 1));
  if (host.empty() || port.empty()) {
    throw ConfigException("invalid list target endpoint");
  }
  return Endpoint{.host_ = std::string(host), .port_ = ParsePort(port)};
}

auto ParseListTarget(std::string_view target) -> std::vector<Endpoint> {
  std::vector<Endpoint> endpoints;
  std::string_view rest = target.substr(LIST_SCHEME.size());
  while (true) {
    const std::size_t comma = rest.find(',');
    endpoints.push_back(ParseEndpoint(comma == std::string_view::npos ? rest : rest.substr(0, comma)));
    if (comma == std::string_view::npos) {
      break;
    }
    rest.remove_prefix(comma + 1);
  }

  if (endpoints.empty()) {
    throw ConfigException("list target requires at least one endpoint");
  }
  return CanonicalizeEndpoints(std::move(endpoints));
}

}  // namespace

StaticDiscovery::StaticDiscovery(std::string_view target) : endpoints_(ParseListTarget(target)) {}

auto StaticDiscovery::Start() -> Status { return Status::Ok(); }

void StaticDiscovery::Stop() {}

auto StaticDiscovery::Snapshot() const -> std::vector<Endpoint> { return endpoints_; }

auto StaticDiscovery::last_error() const -> std::string { return {}; }

}  // namespace xrpc
