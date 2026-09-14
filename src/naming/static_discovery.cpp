/**
 * @file static_discovery.cpp
 * @brief Parses `list://host:port,...` targets into immutable endpoint snapshots.
 */

#include "naming/static_discovery.h"

#include <cctype>
#include <charconv>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

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

auto ParsePort(std::string_view port_text) -> StatusOr<std::uint16_t> {
  int parsed_port = 0;
  const auto result = std::from_chars(port_text.data(), port_text.data() + port_text.size(), parsed_port);
  if (result.ec != std::errc{} || result.ptr != port_text.data() + port_text.size() || parsed_port <= 0 ||
      parsed_port > 65535) {
    return StatusOr<std::uint16_t>(Status{StatusCode::InvalidArgument, "invalid endpoint port"});
  }
  return StatusOr<std::uint16_t>(static_cast<std::uint16_t>(parsed_port));
}

auto ParseEndpoint(std::string_view endpoint_text) -> StatusOr<Endpoint> {
  endpoint_text = Trim(endpoint_text);
  const std::size_t colon = endpoint_text.rfind(':');
  if (colon == std::string_view::npos || colon == 0 || colon + 1 >= endpoint_text.size()) {
    return StatusOr<Endpoint>(Status{StatusCode::InvalidArgument, "invalid list target endpoint"});
  }

  std::string_view host = Trim(endpoint_text.substr(0, colon));
  std::string_view port = Trim(endpoint_text.substr(colon + 1));
  if (host.empty() || port.empty()) {
    return StatusOr<Endpoint>(Status{StatusCode::InvalidArgument, "invalid list target endpoint"});
  }
  StatusOr<std::uint16_t> parsed_port = ParsePort(port);
  if (!parsed_port.ok()) {
    return StatusOr<Endpoint>(parsed_port.status());
  }
  return StatusOr<Endpoint>(Endpoint{.host_ = std::string(host), .port_ = std::move(parsed_port).value()});
}

auto ParseListTarget(std::string_view target) -> StatusOr<std::vector<Endpoint>> {
  std::vector<Endpoint> endpoints;
  std::string_view rest = target.substr(LIST_SCHEME.size());
  while (true) {
    const std::size_t comma = rest.find(',');
    StatusOr<Endpoint> endpoint = ParseEndpoint(comma == std::string_view::npos ? rest : rest.substr(0, comma));
    if (!endpoint.ok()) {
      return StatusOr<std::vector<Endpoint>>(endpoint.status());
    }
    endpoints.push_back(std::move(endpoint).value());
    if (comma == std::string_view::npos) {
      break;
    }
    rest.remove_prefix(comma + 1);
  }

  if (endpoints.empty()) {
    return StatusOr<std::vector<Endpoint>>(
        Status{StatusCode::InvalidArgument, "list target requires at least one endpoint"});
  }
  return StatusOr<std::vector<Endpoint>>(CanonicalizeEndpoints(std::move(endpoints)));
}

}  // namespace

StaticDiscovery::StaticDiscovery(DiscoverySnapshot endpoints)
    : snapshot_(std::make_shared<const DiscoverySnapshot>(std::move(endpoints))) {}

auto StaticDiscovery::ParseTarget(std::string_view target) -> StatusOr<DiscoverySnapshot> {
  return ParseListTarget(target);
}

auto StaticDiscovery::Start() -> Status { return Status::Ok(); }

void StaticDiscovery::Stop() {}

auto StaticDiscovery::Snapshot() const -> std::shared_ptr<const DiscoverySnapshot> { return snapshot_; }

auto StaticDiscovery::last_error() const -> std::string { return {}; }

}  // namespace xrpc
