#include "naming/consul_resolver.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include <nlohmann/json.hpp>

#include "common/xrpc_exception.h"

namespace xrpc {

namespace {

/** @brief Backoff used after a failed Consul refresh attempt. */
constexpr std::chrono::seconds CONSUL_ERROR_SLEEP{1};

/**
 * @brief Reads a normalized HTTP header value from a Consul response.
 *
 * @param response Parsed HTTP response.
 * @param name Lowercase header name.
 * @return Header value, or empty string when absent.
 */
auto HeaderValue(const ConsulHttpResponse &response, const std::string &name) -> std::string {
  const auto it = response.headers_.find(name);
  if (it == response.headers_.end()) {
    return {};
  }
  return it->second;
}

/**
 * @brief Parses Consul's blocking-query index header.
 *
 * @param response Parsed HTTP response.
 * @return Consul index, or zero when the header is absent or malformed.
 */
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

/**
 * @brief Parses healthy service entries returned by Consul.
 *
 * Service address is preferred over node address because Consul permits service-specific advertised
 * addresses. Invalid or incomplete entries are skipped instead of failing the whole snapshot.
 *
 * @param body JSON body from `/v1/health/service/<name>?passing=true`.
 * @return Canonicalized endpoint snapshot.
 * @throws ProtocolException when the top-level response shape is invalid.
 */
auto ParseEndpoints(const std::string &body) -> std::vector<Endpoint> {
  const nlohmann::json root = nlohmann::json::parse(body);
  if (!root.is_array()) {
    throw ProtocolException(StatusCode::DataLoss, "Consul health response is not an array");
  }

  std::vector<Endpoint> endpoints;
  for (const nlohmann::json &entry : root) {
    const nlohmann::json &service = entry.at("Service");
    const nlohmann::json &node = entry.at("Node");

    std::string host = service.value("Address", "");
    if (host.empty()) {
      host = node.value("Address", "");
    }
    const int port = service.value("Port", 0);
    if (host.empty() || port <= 0 || port > 65535) {
      continue;
    }

    endpoints.push_back(Endpoint{.host_ = std::move(host), .port_ = static_cast<std::uint16_t>(port)});
  }
  return CanonicalizeEndpoints(std::move(endpoints));
}

}  // namespace

/**
 * @brief Creates a Consul resolver using the default HTTP client.
 *
 * @param service_name Consul service name to watch.
 * @param consul_address Consul agent address in `host:port` form.
 * @param refresh_interval Maximum blocking-query wait interval.
 */
ConsulResolver::ConsulResolver(std::string service_name, const std::string &consul_address,
                               std::chrono::milliseconds refresh_interval)
    : service_name_(std::move(service_name)), http_client_(consul_address), refresh_interval_(refresh_interval) {}

/** @brief Stops the refresh thread before destroying resolver state. */
ConsulResolver::~ConsulResolver() { Stop(); }

/**
 * @brief Performs the initial fetch and starts the background refresh loop.
 *
 * @return Status from the initial non-blocking fetch.
 */
auto ConsulResolver::Start() -> Status {
  const Status status = Fetch(false);
  refresh_thread_ = std::jthread([this](std::stop_token stop_token) { RefreshLoop(stop_token); });
  return status;
}

/** @brief Requests refresh-loop shutdown and joins the thread. */
void ConsulResolver::Stop() {
  refresh_thread_.request_stop();
  if (refresh_thread_.joinable()) {
    refresh_thread_.join();
  }
}

/** @return Copy of the latest healthy endpoint snapshot. */
auto ConsulResolver::Snapshot() const -> std::vector<Endpoint> {
  std::lock_guard lock(mutex_);
  return snapshot_;
}

/** @return Last refresh error text, or empty string after a successful refresh. */
auto ConsulResolver::last_error() const -> std::string {
  std::lock_guard lock(mutex_);
  return last_error_;
}

/**
 * @brief Fetches one service snapshot from Consul.
 *
 * Blocking fetches use Consul's index/wait parameters so the refresh thread sleeps inside Consul
 * until the catalog changes or the wait period expires.
 *
 * @param blocking true to issue a blocking query based on the last Consul index.
 * @return OK after a successfully parsed response, otherwise server/protocol status.
 */
auto ConsulResolver::Fetch(bool blocking) -> Status {
  // Blocking queries use the last Consul index to wait for changes instead of
  // polling at a fixed rate. The timeout includes a small error backoff margin.
  const std::chrono::milliseconds request_timeout =
      blocking ? refresh_interval_ + std::chrono::duration_cast<std::chrono::milliseconds>(CONSUL_ERROR_SLEEP)
               : refresh_interval_;
  const StatusOr<ConsulHttpResponse> response_result = http_client_.Get(QueryPath(blocking), request_timeout);
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

  try {
    std::vector<Endpoint> endpoints = ParseEndpoints(response.body_);
    SetSnapshot(std::move(endpoints), ParseConsulIndex(response));
    SetLastError({});
    return Status::Ok();
  } catch (...) {
    Status status = CaughtExceptionToStatus(StatusCode::DataLoss, "failed to parse Consul response");
    SetLastError(status.message());
    return status;
  }
}

/**
 * @brief Repeatedly refreshes the Consul snapshot until stop is requested.
 *
 * @param stop_token Cooperative stop token owned by the refresh jthread.
 */
void ConsulResolver::RefreshLoop(std::stop_token stop_token) {
  while (!stop_token.stop_requested()) {
    const Status status = Fetch(true);
    if (!status.ok() && !stop_token.stop_requested()) {
      std::this_thread::sleep_for(CONSUL_ERROR_SLEEP);
    }
  }
}

/**
 * @brief Publishes a new endpoint snapshot and remembers the latest Consul index.
 *
 * @param endpoints Canonicalized endpoint snapshot.
 * @param index Consul blocking-query index, or zero when unavailable.
 */
void ConsulResolver::SetSnapshot(std::vector<Endpoint> endpoints, std::uint64_t index) {
  std::lock_guard lock(mutex_);
  snapshot_ = std::move(endpoints);
  if (index != 0) {
    last_index_ = index;
  }
}

/**
 * @brief Stores the latest refresh error text.
 *
 * @param error Error text to expose through `last_error()`, or empty after success.
 */
void ConsulResolver::SetLastError(std::string error) {
  std::lock_guard lock(mutex_);
  last_error_ = std::move(error);
}

/**
 * @brief Builds the Consul health API path for immediate or blocking fetches.
 *
 * @param blocking true to include `index` and `wait` query parameters when an index is known.
 * @return Absolute Consul HTTP API path.
 */
auto ConsulResolver::QueryPath(bool blocking) const -> std::string {
  std::uint64_t index = 0;
  {
    std::lock_guard lock(mutex_);
    index = last_index_;
  }

  std::ostringstream path;
  path << "/v1/health/service/" << service_name_ << "?passing=true";
  if (blocking && index != 0) {
    // Consul requires wait to be at least one second; shorter refresh intervals
    // still become a valid blocking query.
    const auto wait_seconds = std::chrono::duration_cast<std::chrono::seconds>(refresh_interval_);
    path << "&index=" << index << "&wait=" << std::max<std::int64_t>(wait_seconds.count(), 1) << "s";
  }
  return path.str();
}

}  // namespace xrpc
