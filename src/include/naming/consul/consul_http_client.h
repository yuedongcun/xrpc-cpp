/**
 * @file consul_http_client.h
 * @brief Defines the small blocking HTTP client used by Consul integration.
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include <xrpc/status.h>

namespace xrpc {

constexpr std::chrono::seconds CONSUL_HTTP_TIMEOUT{2};

/** @brief Parsed status line, headers, and body of one HTTP response. */
struct ConsulHttpResponse {
  int status_code_ = 0;

  std::map<std::string, std::string> headers_;

  std::string body_;
};

/**
 * @brief Performs the limited HTTP/1.1 requests required by Consul.
 *
 * Each request opens one blocking TCP connection and sends
 * `Connection: close`. Responses may use Content-Length, chunked transfer
 * encoding, or connection-close framing. This is not a general HTTP client.
 */
class ConsulHttpClient final {
 public:
  explicit ConsulHttpClient(const std::string &address);

  [[nodiscard]] auto Get(std::string_view path, std::chrono::milliseconds timeout) const
      -> StatusOr<ConsulHttpResponse>;

  [[nodiscard]] auto Put(std::string_view path, std::string_view body, std::chrono::milliseconds timeout) const
      -> StatusOr<ConsulHttpResponse>;

 private:
  /** Sends one request and maps transport or HTTP parsing failures to `Status`. */
  [[nodiscard]] auto SendRequest(std::string_view method, std::string_view path, std::string_view body,
                                 std::chrono::milliseconds timeout) const -> StatusOr<ConsulHttpResponse>;
  std::string host_;
  std::uint16_t port_ = 0;
};

}  // namespace xrpc
