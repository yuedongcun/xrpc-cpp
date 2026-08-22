/**
 * @file consul_registrar.h
 * @brief Defines server registration through the Consul Agent API.
 */

#pragma once

#include <cstdint>
#include <string>

#include <xrpc/status.h>

#include "naming/consul/consul_http_client.h"

namespace xrpc {

/**
 * @brief Registers one server instance with a Consul agent.
 *
 * Registration is owned and serialized by the server lifecycle. A successful
 * `Register()` publishes the advertised endpoint with a TCP health check and
 * records the service id required by `Deregister()`. Failed writes do not
 * change the current registration state.
 */
class ConsulRegistrar final {
 public:
  struct Options {
    std::string service_name_;

    std::string service_id_;

    std::string service_address_;

    std::uint16_t service_port_ = 0;
  };

  explicit ConsulRegistrar(const std::string &consul_address);

  /** Registers or replaces this instance and its TCP health check. */
  [[nodiscard]] auto Register(const Options &options) -> Status;

  /** Deregisters the recorded service id; succeeds when not registered. */
  [[nodiscard]] auto Deregister() -> Status;

  [[nodiscard]] auto registered() const -> bool;

 private:
  [[nodiscard]] static auto ValidateOptions(const Options &options) -> Status;

  [[nodiscard]] auto BuildRegisterPayload(const Options &options) const -> std::string;

  /** Converts transport failures and non-2xx Agent responses into `Status`. */
  [[nodiscard]] static auto HandleAgentWriteResponse(const StatusOr<ConsulHttpResponse> &response) -> Status;

  ConsulHttpClient http_client_;
  bool registered_ = false;
  std::string registered_service_id_;
};

}  // namespace xrpc
