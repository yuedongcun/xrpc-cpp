#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include <xrpc/status.h>
#include <xrpc/status_or.h>

namespace xrpc {

/**
 * @brief Minimal HTTP response representation for Consul v1 API calls.
 *
 * Only the status code, headers, and body are needed by discovery and registration logic. Header names are kept in a
 * map so Consul index and wait-related fields can be inspected deterministically in tests.
 */
struct ConsulHttpResponse {
  /** @brief Numeric HTTP response status code. */
  int status_code_ = 0;

  /** @brief Response headers keyed by header name. */
  std::map<std::string, std::string> headers_;

  /** @brief Raw response body. */
  std::string body_;
};

/** @brief Small blocking HTTP/1.1 client for Consul agent API calls. */
class ConsulHttpClient final {
 public:
  /** @brief Creates a client for `host:port` Consul agent address. */
  explicit ConsulHttpClient(const std::string &address);

  /** @brief Sends a blocking HTTP GET request. */
  [[nodiscard]] auto Get(std::string_view path, std::chrono::milliseconds timeout) const
      -> StatusOr<ConsulHttpResponse>;

  /** @brief Sends a blocking HTTP PUT request. */
  [[nodiscard]] auto Put(std::string_view path, std::string_view body, std::chrono::milliseconds timeout) const
      -> StatusOr<ConsulHttpResponse>;

 private:
  /** @brief Sends one blocking HTTP request to the configured Consul agent. */
  [[nodiscard]] auto SendRequest(std::string_view method, std::string_view path, std::string_view body,
                                 std::chrono::milliseconds timeout) const -> StatusOr<ConsulHttpResponse>;
  std::string host_;
  std::uint16_t port_ = 0;
};

}  // namespace xrpc
