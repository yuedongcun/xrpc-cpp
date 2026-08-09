#include "rpc/naming/endpoint_resolver.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <ranges>
#include <string_view>
#include <utility>

#include "rpc/xrpc_exception.h"

#include "rpc/naming/consul_resolver.h"

namespace xrpc {

namespace {

/** @brief URI scheme for an inline, comma-separated endpoint list. */
constexpr std::string_view LIST_SCHEME = "list://";

/** @brief URI scheme for a Consul service-name target. */
constexpr std::string_view CONSUL_SCHEME = "consul://";

/**
 * @brief Removes leading and trailing ASCII whitespace from a borrowed string view.
 *
 * @param value Input view to trim.
 * @return View into `value` with surrounding whitespace removed.
 */
auto Trim(std::string_view value) -> std::string_view {
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())) != 0) {
    value.remove_prefix(1);
  }
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())) != 0) {
    value.remove_suffix(1);
  }
  return value;
}

/**
 * @brief Tests whether a target uses a resolver scheme.
 *
 * @param value Full client target string.
 * @param prefix Scheme prefix to match.
 * @return true when `value` begins with `prefix`.
 */
auto StartsWith(std::string_view value, std::string_view prefix) -> bool { return value.starts_with(prefix); }

/**
 * @brief Parses a TCP port from a target endpoint.
 *
 * @param port_text Decimal port text without surrounding host data.
 * @return Port in host byte order.
 * @throws ConfigException when the text is not a valid non-zero TCP port.
 */
auto ParsePort(std::string_view port_text) -> std::uint16_t {
  int parsed_port = 0;
  const char *begin = port_text.data();
  const char *end = port_text.data() + port_text.size();
  const auto result = std::from_chars(begin, end, parsed_port);
  if (result.ec != std::errc{} || result.ptr != end || parsed_port <= 0 || parsed_port > 65535) {
    throw ConfigException("invalid endpoint port");
  }
  return static_cast<std::uint16_t>(parsed_port);
}

/**
 * @brief Parses one `host:port` entry from a `list://` target.
 *
 * @param endpoint_text Endpoint text, optionally surrounded by whitespace.
 * @return Normalized endpoint address.
 * @throws ConfigException when the endpoint is malformed.
 */
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

/**
 * @brief Parses and canonicalizes a static endpoint list target.
 *
 * @param target Target string using the `list://host:port[,host:port...]` format.
 * @return Sorted unique endpoint snapshot.
 * @throws ConfigException when the target has no usable endpoints.
 */
auto ParseListTarget(std::string_view target) -> std::vector<Endpoint> {
  std::vector<Endpoint> endpoints;
  std::string_view rest = target.substr(LIST_SCHEME.size());
  while (true) {
    const std::size_t comma = rest.find(',');
    const std::string_view endpoint_text = comma == std::string_view::npos ? rest : rest.substr(0, comma);
    endpoints.push_back(ParseEndpoint(endpoint_text));
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

class StaticResolver final : public EndpointResolver {
 public:
  /**
   * @brief Creates a resolver that always returns one fixed endpoint snapshot.
   *
   * @param endpoints Sorted unique endpoints parsed from a `list://` target.
   */
  explicit StaticResolver(std::vector<Endpoint> endpoints) : endpoints_(std::move(endpoints)) {}

  /** @brief Static endpoints need no background startup work. */
  [[nodiscard]] auto Start() -> Status override { return Status::Ok(); }

  /** @brief Static endpoints own no background work to stop. */
  void Stop() override {}

  /** @return Copy of the fixed endpoint snapshot. */
  [[nodiscard]] auto Snapshot() const -> std::vector<Endpoint> override { return endpoints_; }

  /** @return Empty error text because static resolution cannot refresh-fail after construction. */
  [[nodiscard]] auto last_error() const -> std::string override { return {}; }

  /** @return Resolver kind used for client diagnostics. */
  [[nodiscard]] auto kind() const -> ResolverKind override { return ResolverKind::StaticList; }

 private:
  std::vector<Endpoint> endpoints_;
};

}  // namespace

/**
 * @brief Sorts endpoints and removes duplicate `host:port` entries.
 *
 * Canonical snapshots make resolver changes deterministic and allow channel updates to skip
 * equivalent endpoint lists even when discovery returns entries in a different order.
 *
 * @param endpoints Endpoint list to normalize.
 * @return Sorted unique endpoints.
 */
auto CanonicalizeEndpoints(std::vector<Endpoint> endpoints) -> std::vector<Endpoint> {
  std::ranges::sort(endpoints, [](const Endpoint &lhs, const Endpoint &rhs) {
    if (lhs.host_ != rhs.host_) {
      return lhs.host_ < rhs.host_;
    }
    return lhs.port_ < rhs.port_;
  });
  const auto end = std::ranges::unique(endpoints, [](const Endpoint &lhs, const Endpoint &rhs) {
    return lhs.host_ == rhs.host_ && lhs.port_ == rhs.port_;
  });
  endpoints.erase(end.begin(), end.end());
  return endpoints;
}

/**
 * @brief Validates resolver options and converts a target URI into resolver configuration.
 *
 * Supported targets are `list://host:port[,host:port...]` and `consul://service-name`.
 *
 * @param options Public resolver-related client options.
 * @return Normalized resolver configuration.
 * @throws ConfigException when the target scheme or scheme payload is invalid.
 */
auto NormalizeResolverOptions(const ResolverOptions &options) -> ResolverConfig {
  const std::string_view target = Trim(options.target_);
  if (target.empty()) {
    throw ConfigException("RpcClient target must not be empty");
  }
  if (options.discovery_refresh_interval_ < std::chrono::milliseconds::zero()) {
    throw ConfigException("RpcClient discovery_refresh_interval must not be negative");
  }
  if (StartsWith(target, LIST_SCHEME)) {
    return ResolverConfig{
        .kind_ = ResolverKind::StaticList,
        .static_endpoints_ = ParseListTarget(target),
        .consul_service_name_ = {},
        .consul_address_ = {},
        .discovery_refresh_interval_ = options.discovery_refresh_interval_,
    };
  }
  if (StartsWith(target, CONSUL_SCHEME)) {
    std::string service_name(Trim(target.substr(CONSUL_SCHEME.size())));
    if (service_name.empty()) {
      throw ConfigException("consul target requires a service name");
    }
    if (options.consul_address_.empty()) {
      throw ConfigException("consul target requires consul_address");
    }
    return ResolverConfig{
        .kind_ = ResolverKind::Consul,
        .static_endpoints_ = {},
        .consul_service_name_ = std::move(service_name),
        .consul_address_ = options.consul_address_,
        .discovery_refresh_interval_ = options.discovery_refresh_interval_,
    };
  }
  throw ConfigException("unsupported RpcClient target scheme");
}

/**
 * @brief Constructs the resolver implementation selected by the normalized target URI.
 *
 * @param options Public resolver-related client options.
 * @return Resolver instance ready for `Start()`.
 */
auto MakeEndpointResolver(const ResolverOptions &options) -> std::unique_ptr<EndpointResolver> {
  const ResolverConfig config = NormalizeResolverOptions(options);
  if (config.kind_ == ResolverKind::StaticList) {
    return std::make_unique<StaticResolver>(config.static_endpoints_);
  }
  return std::make_unique<ConsulResolver>(config.consul_service_name_, config.consul_address_,
                                          config.discovery_refresh_interval_);
}

}  // namespace xrpc
