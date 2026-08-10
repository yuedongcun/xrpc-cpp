#pragma once

#include <chrono>
#include <string>

#include <xrpc/status.h>

#include "rpc/naming/consul_http_client.h"

namespace xrpc {

/** @brief Consul agent client backed by the shared blocking HTTP client. */
class ConsulAgentClient final {
 public:
  /** @brief Creates a client for `host:port` Consul agent address. */
  explicit ConsulAgentClient(const std::string &address);

  /** @brief Registers a service through `/v1/agent/service/register`. */
  [[nodiscard]] auto RegisterService(const std::string &payload, std::chrono::milliseconds timeout) const -> Status;

  /** @brief Deregisters a service through `/v1/agent/service/deregister/<id>`. */
  [[nodiscard]] auto DeregisterService(const std::string &service_id, std::chrono::milliseconds timeout) const
      -> Status;

 private:
  /** @brief Converts a Consul agent write response into an XRPC status. */
  [[nodiscard]] auto HandleAgentWriteResponse(const StatusOr<ConsulHttpResponse> &response) const -> Status;

  ConsulHttpClient http_client_;
};

}  // namespace xrpc
