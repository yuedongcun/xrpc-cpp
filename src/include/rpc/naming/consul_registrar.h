#pragma once

#include <chrono>
#include <memory>
#include <string>

#include <xrpc/status.h>

#include "rpc/naming/consul_agent_client.h"

namespace xrpc {

/**
 * @brief Best-effort Consul service registration helper.
 *
 * The registrar remembers the registered service id so deregistration can run during server shutdown. Registration
 * failures are returned as `Status` and also stored as `last_error_` for diagnostics.
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

  /** @brief Validated registration configuration used to build the Consul payload. */
  struct Config {
    std::string service_name_;
    std::string service_id_;
    std::string service_address_;
    std::uint16_t service_port_ = 0;
    std::chrono::milliseconds timeout_{0};
  };

  /** @brief Creates a registrar with an injected Consul agent client. */
  explicit ConsulRegistrar(std::unique_ptr<ConsulAgentClientInterface> agent_client);

  /** @brief Registers the service and records the registered service id on success. */
  [[nodiscard]] auto Register(const Options &options) -> Status;

  /** @brief Deregisters the currently registered service id, if any. */
  [[nodiscard]] auto Deregister() -> Status;

  /** @return true after successful registration and before successful deregistration. */
  [[nodiscard]] auto registered() const -> bool;

  /** @return Last registration or deregistration error text. */
  [[nodiscard]] auto last_error() const -> std::string;

 private:
  /** @brief Validates user-facing options and fills the internal registration config. */
  [[nodiscard]] static auto NormalizeOptions(const Options &options) -> StatusOr<Config>;

  /** @brief Builds the JSON payload accepted by Consul's service registration API. */
  [[nodiscard]] auto BuildRegisterPayload(const Config &options) const -> std::string;

  std::unique_ptr<ConsulAgentClientInterface> agent_client_;
  bool registered_ = false;
  std::string registered_service_id_;
  std::chrono::milliseconds timeout_{1000};
  std::string last_error_;
};

}  // namespace xrpc
