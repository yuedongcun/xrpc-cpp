#include "discovery/consul_registrar.h"

#include <nlohmann/json.hpp>

namespace xrpc {

/**
 * @brief Creates a registrar for one Consul agent.
 *
 * @param consul_address Consul agent address in `host:port` form.
 */
ConsulRegistrar::ConsulRegistrar(const std::string &consul_address) : http_client_(consul_address) {}

/**
 * @brief Registers this process as a Consul service instance.
 *
 * On success, the registrar records enough state to deregister the same service id later. On
 * @param options User-facing service identity and advertised address.
 * @return `Status::Ok()` on successful Consul write, otherwise validation or agent failure status.
 */
auto ConsulRegistrar::Register(const Options &options) -> Status {
  const Status validation_status = ValidateOptions(options);
  if (!validation_status.ok()) {
    return validation_status;
  }

  const std::string payload = BuildRegisterPayload(options);
  Status status = HandleAgentWriteResponse(http_client_.Put("/v1/agent/service/register", payload, options.timeout_));
  if (!status.ok()) {
    return {status.code(), status.message()};
  }
  registered_ = true;
  timeout_ = options.timeout_;
  registered_service_id_ = options.service_id_;
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
  const Status status = HandleAgentWriteResponse(
      http_client_.Put("/v1/agent/service/deregister/" + registered_service_id_, "", timeout_));
  if (!status.ok()) {
    return {status.code(), status.message()};
  }

  registered_ = false;
  registered_service_id_.clear();
  return Status::Ok();
}

/** @return true after successful registration and before successful deregistration. */
auto ConsulRegistrar::registered() const -> bool { return registered_; }

/**
 * @brief Validates service registration options.
 *
 * @param options User-facing service registration options.
 * @return OK, or an invalid-argument status describing the first invalid field.
 */
auto ConsulRegistrar::ValidateOptions(const Options &options) -> Status {
  if (options.service_name_.empty()) {
    return {StatusCode::InvalidArgument, "ConsulRegistrar service_name must not be empty"};
  }
  if (options.service_id_.empty()) {
    return {StatusCode::InvalidArgument, "ConsulRegistrar service_id must not be empty"};
  }
  if (options.service_address_.empty()) {
    return {StatusCode::InvalidArgument, "ConsulRegistrar service_address must not be empty"};
  }
  if (options.service_port_ == 0) {
    return {StatusCode::InvalidArgument, "ConsulRegistrar service_port must be non-zero"};
  }
  if (options.timeout_ < std::chrono::milliseconds::zero()) {
    return {StatusCode::InvalidArgument, "ConsulRegistrar timeout must not be negative"};
  }
  return Status::Ok();
}

/**
 * @brief Builds the JSON body accepted by Consul's `/v1/agent/service/register` API.
 *
 * @param options Validated service registration config.
 * @return Compact JSON registration payload.
 */
auto ConsulRegistrar::BuildRegisterPayload(const Options &options) const -> std::string {
  nlohmann::json payload;
  payload["Name"] = options.service_name_;
  payload["ID"] = options.service_id_;
  payload["Address"] = options.service_address_;
  payload["Port"] = options.service_port_;
  return payload.dump();
}

auto ConsulRegistrar::HandleAgentWriteResponse(const StatusOr<ConsulHttpResponse> &response) -> Status {
  if (!response.ok()) {
    return response.status();
  }
  const int status_code = response.value().status_code_;
  if (status_code < 200 || status_code >= 300) {
    return {StatusCode::Unavailable, "Consul agent request failed with HTTP status " + std::to_string(status_code)};
  }
  return Status::Ok();
}

}  // namespace xrpc
