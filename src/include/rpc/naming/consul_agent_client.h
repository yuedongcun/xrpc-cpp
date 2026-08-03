#pragma once

#include <chrono>
#include <string>

#include <xrpc/status.h>

#include "rpc/naming/consul_http_client.h"

namespace xrpc {

/**
 * @brief Narrow Consul agent API used by `ConsulRegistrar`.
 *
 * Tests can replace this client without depending on wire payload construction details or a real Consul agent.
 */
class ConsulAgentClientInterface {
 public:
  virtual ~ConsulAgentClientInterface() = default;

  /** @brief Registers a service using a prebuilt Consul JSON payload. */
  [[nodiscard]] virtual auto RegisterService(const std::string &payload, std::chrono::milliseconds timeout) const
      -> Status = 0;

  /** @brief Deregisters a service by Consul service id. */
  [[nodiscard]] virtual auto DeregisterService(const std::string &service_id, std::chrono::milliseconds timeout) const
      -> Status = 0;

  /** @brief Marks a TTL check as passing. */
  [[nodiscard]] virtual auto PassTTL(const std::string &check_id, std::chrono::milliseconds timeout) const
      -> Status = 0;
};

/** @brief Consul agent client backed by the shared blocking HTTP client. */
class ConsulAgentClient final : public ConsulAgentClientInterface {
 public:
  /** @brief Creates a client for `host:port` Consul agent address. */
  explicit ConsulAgentClient(const std::string &address);

  /** @brief Registers a service through `/v1/agent/service/register`. */
  [[nodiscard]] auto RegisterService(const std::string &payload, std::chrono::milliseconds timeout) const
      -> Status override;

  /** @brief Deregisters a service through `/v1/agent/service/deregister/<id>`. */
  [[nodiscard]] auto DeregisterService(const std::string &service_id, std::chrono::milliseconds timeout) const
      -> Status override;

  /** @brief Marks a TTL check as passing through `/v1/agent/check/pass/<id>`. */
  [[nodiscard]] auto PassTTL(const std::string &check_id, std::chrono::milliseconds timeout) const -> Status override;

 private:
  /** @brief Converts a Consul agent write response into an XRPC status. */
  [[nodiscard]] auto HandleAgentWriteResponse(const StatusOr<ConsulHttpResponse> &response) const -> Status;

  ConsulHttpClient http_client_;
};

}  // namespace xrpc
