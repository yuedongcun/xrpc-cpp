#pragma once

#include <chrono>
#include <string>

#include <xrpc/status.h>

#include "naming/consul_http_client.h"

namespace xrpc {

/**
 * @brief Best-effort Consul service registration helper.
 *
 * The registrar remembers the registered service id so deregistration can run during server shutdown. Registration
 * failures are returned as `Status`.
 */
class ConsulRegistrar final {
 public:
  /** @brief User-facing registration options after server bind information is known. */
  struct Options {
    /** @brief Consul service name. */
    std::string service_name_;

    /** @brief Consul service id. */
    std::string service_id_;

    /** @brief Address advertised to clients. */
    std::string service_address_;

    /** @brief Port advertised to clients. */
    std::uint16_t service_port_ = 0;

    /** @brief Timeout for each Consul agent call. */
    std::chrono::milliseconds timeout_{1000};
  };

  /** @brief Creates a registrar for one Consul agent address. */
  explicit ConsulRegistrar(const std::string &consul_address);

  /** @brief Registers the service and records the registered service id on success. */
  [[nodiscard]] auto Register(const Options &options) -> Status;

  /** @brief Deregisters the currently registered service id, if any. */
  [[nodiscard]] auto Deregister() -> Status;

  /** @return true after successful registration and before successful deregistration. */
  [[nodiscard]] auto registered() const -> bool;

 private:
  /** @brief Validates service registration options. */
  [[nodiscard]] static auto ValidateOptions(const Options &options) -> Status;

  /** @brief Builds the JSON payload accepted by Consul's service registration API. */
  [[nodiscard]] auto BuildRegisterPayload(const Options &options) const -> std::string;

  /** @brief Converts a Consul agent write response into an XRPC status. */
  [[nodiscard]] static auto HandleAgentWriteResponse(const StatusOr<ConsulHttpResponse> &response) -> Status;

  ConsulHttpClient http_client_;
  bool registered_ = false;
  std::string registered_service_id_;
  std::chrono::milliseconds timeout_{1000};
};

}  // namespace xrpc
