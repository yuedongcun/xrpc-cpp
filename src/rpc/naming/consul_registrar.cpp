#include "rpc/naming/consul_registrar.h"

#include <nlohmann/json.hpp>

namespace xrpc {

/**
 * @brief Creates a registrar that delegates Consul agent writes to an injected client.
 *
 * @param agent_client Consul agent client used for register and deregister requests.
 */
ConsulRegistrar::ConsulRegistrar(std::unique_ptr<ConsulAgentClientInterface> agent_client)
    : agent_client_(std::move(agent_client)) {}

/**
 * @brief Registers this process as a Consul service instance.
 *
 * On success, the registrar records enough state to deregister the same service id later. On
 * failure, the last error is retained for server diagnostics.
 *
 * @param options User-facing service identity and advertised address.
 * @return `Status::Ok()` on successful Consul write, otherwise validation or agent failure status.
 */
auto ConsulRegistrar::Register(const Options &options) -> Status {
  const StatusOr<Config> config_result = NormalizeOptions(options);
  if (!config_result.ok()) {
    last_error_ = config_result.status().message();
    return config_result.status();
  }

  const Config &config = config_result.value();
  const std::string payload = BuildRegisterPayload(config);
  Status status = agent_client_->RegisterService(payload, config.timeout_);
  if (!status.ok()) {
    last_error_ = status.message();
    return {status.code(), status.message()};
  }
  registered_ = true;
  timeout_ = config.timeout_;
  registered_service_id_ = config.service_id_;
  last_error_.clear();
  return Status::Ok();
}

/**
 * @brief Deregisters the previously registered service instance if one exists.
 *
 * @return `Status::Ok()` when nothing is registered or Consul accepts the deregistration request.
 */
auto ConsulRegistrar::Deregister() -> Status {
  if (!registered_) {
    return Status::Ok();
  }
  const Status status = agent_client_->DeregisterService(registered_service_id_, timeout_);
  if (!status.ok()) {
    last_error_ = status.message();
    return {status.code(), status.message()};
  }

  last_error_.clear();
  registered_ = false;
  registered_service_id_.clear();
  return Status::Ok();
}

/** @return true after successful registration and before successful deregistration. */
auto ConsulRegistrar::registered() const -> bool { return registered_; }

/** @return Last registration or deregistration error text. */
auto ConsulRegistrar::last_error() const -> std::string { return last_error_; }

/**
 * @brief Validates public registration options and builds the internal payload model.
 *
 * @param options User-facing service registration options.
 * @return Normalized config, or an invalid-argument status describing the first invalid field.
 */
auto ConsulRegistrar::NormalizeOptions(const Options &options) -> StatusOr<Config> {
  if (options.service_name_.empty()) {
    return StatusOr<Config>(Status{StatusCode::InvalidArgument, "ConsulRegistrar service_name must not be empty"});
  }
  if (options.service_id_.empty()) {
    return StatusOr<Config>(Status{StatusCode::InvalidArgument, "ConsulRegistrar service_id must not be empty"});
  }
  if (options.service_address_.empty()) {
    return StatusOr<Config>(Status{StatusCode::InvalidArgument, "ConsulRegistrar service_address must not be empty"});
  }
  if (options.service_port_ == 0) {
    return StatusOr<Config>(Status{StatusCode::InvalidArgument, "ConsulRegistrar service_port must be non-zero"});
  }
  if (options.timeout_ < std::chrono::milliseconds::zero()) {
    return StatusOr<Config>(Status{StatusCode::InvalidArgument, "ConsulRegistrar timeout must not be negative"});
  }

  return StatusOr<Config>(Config{
      .service_name_ = options.service_name_,
      .service_id_ = options.service_id_,
      .service_address_ = options.service_address_,
      .service_port_ = options.service_port_,
      .timeout_ = options.timeout_,
  });
}

/**
 * @brief Builds the JSON body accepted by Consul's `/v1/agent/service/register` API.
 *
 * @param options Validated service registration config.
 * @return Compact JSON registration payload.
 */
auto ConsulRegistrar::BuildRegisterPayload(const Config &options) const -> std::string {
  nlohmann::json payload;
  payload["Name"] = options.service_name_;
  payload["ID"] = options.service_id_;
  payload["Address"] = options.service_address_;
  payload["Port"] = options.service_port_;
  return payload.dump();
}

}  // namespace xrpc
