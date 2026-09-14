/**
 * @file consul_discovery.cpp
 * @brief Implements snapshot refresh with Consul blocking health queries.
 *
 * Only passing service instances are published. A service address takes
 * precedence over its node address when both are present.
 */

#include "naming/consul/consul_discovery.h"

#include <charconv>
#include <chrono>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include <nlohmann/json.hpp>

namespace xrpc {

namespace {

constexpr std::chrono::seconds CONSUL_RETRY_DELAY{1};
constexpr std::chrono::seconds CONSUL_QUERY_WAIT{5};

auto HeaderValue(const ConsulHttpResponse &response, const std::string &name) -> std::string {
  const auto it = response.headers_.find(name);
  if (it == response.headers_.end()) {
    return {};
  }
  return it->second;
}

auto ParseConsulIndex(const ConsulHttpResponse &response) -> std::uint64_t {
  const std::string value = HeaderValue(response, "x-consul-index");
  if (value.empty()) {
    return 0;
  }
  std::uint64_t parsed = 0;
  const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
    return 0;
  }
  return parsed;
}

auto ParseEndpoints(const std::string &body) -> StatusOr<std::vector<Endpoint>> {
  const nlohmann::json root = nlohmann::json::parse(body, nullptr, false);
  if (root.is_discarded() || !root.is_array()) {
    return StatusOr<std::vector<Endpoint>>(Status{StatusCode::DataLoss, "Consul health response is not an array"});
  }

  std::vector<Endpoint> endpoints;
  for (const nlohmann::json &entry : root) {
    if (!entry.is_object() || !entry.contains("Service") || !entry["Service"].is_object() || !entry.contains("Node") ||
        !entry["Node"].is_object()) {
      return StatusOr<std::vector<Endpoint>>(
          Status{StatusCode::DataLoss, "Consul health response contains an invalid service entry"});
    }
    const nlohmann::json &service = entry["Service"];
    const nlohmann::json &node = entry["Node"];

    std::string host;
    if (const auto address = service.find("Address"); address != service.end()) {
      if (!address->is_string()) {
        return StatusOr<std::vector<Endpoint>>(
            Status{StatusCode::DataLoss, "Consul health response contains an invalid service address"});
      }
      host = address->get<std::string>();
    }
    if (host.empty()) {
      if (const auto address = node.find("Address"); address != node.end()) {
        if (!address->is_string()) {
          return StatusOr<std::vector<Endpoint>>(
              Status{StatusCode::DataLoss, "Consul health response contains an invalid node address"});
        }
        host = address->get<std::string>();
      }
    }
    if (!service.contains("Port") || (!service["Port"].is_number_integer() && !service["Port"].is_number_unsigned())) {
      return StatusOr<std::vector<Endpoint>>(
          Status{StatusCode::DataLoss, "Consul health response contains an invalid service port"});
    }
    const std::int64_t port = service["Port"].is_number_unsigned()
                                  ? (service["Port"].get<std::uint64_t>() <= 65535
                                         ? static_cast<std::int64_t>(service["Port"].get<std::uint64_t>())
                                         : 0)
                                  : service["Port"].get<std::int64_t>();
    if (host.empty() || port <= 0 || port > 65535) {
      continue;
    }

    endpoints.push_back(Endpoint{.host_ = std::move(host), .port_ = static_cast<std::uint16_t>(port)});
  }
  return StatusOr<std::vector<Endpoint>>(CanonicalizeEndpoints(std::move(endpoints)));
}

}  // namespace

ConsulDiscovery::ConsulDiscovery(std::string service_name, ConsulHttpClient http_client)
    : service_name_(std::move(service_name)), http_client_(std::move(http_client)) {}

ConsulDiscovery::~ConsulDiscovery() { Stop(); }

auto ConsulDiscovery::Start() -> Status {
  const Status status = Fetch(false);
  refresh_thread_ = std::jthread([this](const std::stop_token &stop_token) -> void { RefreshLoop(stop_token); });
  return status;
}

void ConsulDiscovery::Stop() {
  refresh_thread_.request_stop();
  if (refresh_thread_.joinable()) {
    refresh_thread_.join();
  }
}

auto ConsulDiscovery::Snapshot() const -> std::shared_ptr<const DiscoverySnapshot> { return snapshot_.load(); }

auto ConsulDiscovery::last_error() const -> std::string {
  std::lock_guard lock(refresh_state_mutex_);
  return last_error_;
}

auto ConsulDiscovery::Fetch(bool use_blocking_query) -> Status {
  const std::chrono::milliseconds request_timeout =
      use_blocking_query ? CONSUL_QUERY_WAIT + CONSUL_HTTP_TIMEOUT : CONSUL_HTTP_TIMEOUT;
  const StatusOr<ConsulHttpResponse> response_result = http_client_.Get(QueryPath(use_blocking_query), request_timeout);
  if (!response_result.ok()) {
    SetLastError(response_result.status().message());
    return response_result.status();
  }

  const ConsulHttpResponse &response = response_result.value();
  if (response.status_code_ < 200 || response.status_code_ >= 300) {
    std::string error = "Consul request failed with HTTP status " + std::to_string(response.status_code_);
    SetLastError(error);
    return {StatusCode::Unavailable, std::move(error)};
  }

  StatusOr<std::vector<Endpoint>> endpoints = ParseEndpoints(response.body_);
  if (!endpoints.ok()) {
    SetLastError(endpoints.status().message());
    return endpoints.status();
  }
  SetSnapshot(std::move(endpoints).value(), ParseConsulIndex(response));
  SetLastError({});
  return Status::Ok();
}

void ConsulDiscovery::RefreshLoop(const std::stop_token &stop_token) {
  while (!stop_token.stop_requested()) {
    const Status status = Fetch(true);
    // Keep the previous snapshot and retry after a short delay when Consul is unavailable.
    if (!status.ok() && !stop_token.stop_requested()) {
      std::this_thread::sleep_for(CONSUL_RETRY_DELAY);
    }
  }
}

void ConsulDiscovery::SetSnapshot(std::vector<Endpoint> endpoints, std::uint64_t index) {
  const std::shared_ptr<const DiscoverySnapshot> current = snapshot_.load();
  if (*current != endpoints) {
    snapshot_.store(std::make_shared<const DiscoverySnapshot>(std::move(endpoints)));
  }

  if (index != 0) {
    std::lock_guard lock(refresh_state_mutex_);
    last_index_ = index;
  }
}

void ConsulDiscovery::SetLastError(std::string error) {
  std::lock_guard lock(refresh_state_mutex_);
  last_error_ = std::move(error);
}

auto ConsulDiscovery::QueryPath(bool use_blocking_query) const -> std::string {
  std::uint64_t index = 0;
  {
    std::lock_guard lock(refresh_state_mutex_);
    index = last_index_;
  }

  std::ostringstream path;
  path << "/v1/health/service/" << service_name_ << "?passing=true";
  if (use_blocking_query && index != 0) {
    path << "&index=" << index << "&wait=" << CONSUL_QUERY_WAIT.count() << "s";
  }
  return path.str();
}

}  // namespace xrpc
