/**
 * @file consul_registrar.cpp
 * @brief Implements `RpcServer` registration through the Consul Agent API.
 */

#include "naming/consul/consul_registrar.h"

#include <string_view>

#include <nlohmann/json.hpp>

namespace xrpc {
namespace {

// The local Consul Agent probes often enough to remove an unreachable instance
// within a few seconds. One second leaves ample margin for a local/LAN connect
// while keeping the failure bound shorter than the check interval.
constexpr std::string_view TCP_CHECK_INTERVAL = "5s";
constexpr std::string_view TCP_CHECK_TIMEOUT = "1s";

}  // namespace

ConsulRegistrar::ConsulRegistrar(const std::string &consul_address) : http_client_(consul_address) {}

auto ConsulRegistrar::Register(const Options &options) -> Status {
  Status validation_status = ValidateOptions(options);
  if (!validation_status.ok()) {
    return validation_status;
  }

  const std::string payload = BuildRegisterPayload(options);
  Status status = HandleAgentWriteResponse(
      http_client_.Put("/v1/agent/service/register?replace-existing-checks=true", payload, CONSUL_HTTP_TIMEOUT));
  if (!status.ok()) {
    return {status.code(), status.message()};
  }
  registered_ = true;
  registered_service_id_ = options.service_id_;
  return Status::Ok();
}

auto ConsulRegistrar::Deregister() -> Status {
  if (!registered_) {
    return Status::Ok();
  }
  const Status status = HandleAgentWriteResponse(
      http_client_.Put("/v1/agent/service/deregister/" + registered_service_id_, "", CONSUL_HTTP_TIMEOUT));
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
  return Status::Ok();
}

auto ConsulRegistrar::BuildRegisterPayload(const Options &options) const -> std::string {
  nlohmann::json payload;
  payload["Name"] = options.service_name_;
  payload["ID"] = options.service_id_;
  payload["Address"] = options.service_address_;
  payload["Port"] = options.service_port_;
  payload["Check"] = {
      {"Name", "xRPC TCP health check"},
      {"TCP", options.service_address_ + ":" + std::to_string(options.service_port_)},
      {"Interval", TCP_CHECK_INTERVAL},
      {"Timeout", TCP_CHECK_TIMEOUT},
  };
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
