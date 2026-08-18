#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include <xrpc/status.h>

namespace xrpc {

struct ConsulHttpResponse {
  int status_code_ = 0;

  std::map<std::string, std::string> headers_;

  std::string body_;
};

class ConsulHttpClient final {
 public:
  explicit ConsulHttpClient(const std::string &address);

  [[nodiscard]] auto Get(std::string_view path, std::chrono::milliseconds timeout) const
      -> StatusOr<ConsulHttpResponse>;

  [[nodiscard]] auto Put(std::string_view path, std::string_view body, std::chrono::milliseconds timeout) const
      -> StatusOr<ConsulHttpResponse>;

 private:
  [[nodiscard]] auto SendRequest(std::string_view method, std::string_view path, std::string_view body,
                                 std::chrono::milliseconds timeout) const -> StatusOr<ConsulHttpResponse>;
  std::string host_;
  std::uint16_t port_ = 0;
};

}  // namespace xrpc
