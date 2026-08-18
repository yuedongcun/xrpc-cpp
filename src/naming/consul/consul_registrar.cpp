#include "naming/consul/consul_registrar.h"

#include <nlohmann/json.hpp>

namespace xrpc {

ConsulRegistrar::ConsulRegistrar(const std::string &consul_address) : http_client_(consul_address) {}

auto ConsulRegistrar::Register(const Options &options) -> Status {
  Status validation_status = ValidateOptions(options);
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

auto ConsulRegistrar::registered() const -> bool { return registered_; }

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
