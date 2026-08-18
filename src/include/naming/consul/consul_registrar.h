#pragma once

#include <chrono>
#include <string>

#include <xrpc/status.h>

#include "naming/consul/consul_http_client.h"

namespace xrpc {

class ConsulRegistrar final {
 public:
  struct Options {
    std::string service_name_;

    std::string service_id_;

    std::string service_address_;

    std::uint16_t service_port_ = 0;

    std::chrono::milliseconds timeout_{1000};
  };

  explicit ConsulRegistrar(const std::string &consul_address);

  [[nodiscard]] auto Register(const Options &options) -> Status;

  [[nodiscard]] auto Deregister() -> Status;

  [[nodiscard]] auto registered() const -> bool;

 private:
  [[nodiscard]] static auto ValidateOptions(const Options &options) -> Status;

  [[nodiscard]] auto BuildRegisterPayload(const Options &options) const -> std::string;

  [[nodiscard]] static auto HandleAgentWriteResponse(const StatusOr<ConsulHttpResponse> &response) -> Status;

  ConsulHttpClient http_client_;
  bool registered_ = false;
  std::string registered_service_id_;
  std::chrono::milliseconds timeout_{1000};
};

}  // namespace xrpc
