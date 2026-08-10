#include "rpc/naming/consul_agent_client.h"

namespace xrpc {

/**
 * @brief Creates an agent write client for one Consul `host:port` address.
 *
 * @param address Consul agent HTTP address.
 */
ConsulAgentClient::ConsulAgentClient(const std::string &address) : http_client_(address) {}

/**
 * @brief Registers a service by sending a JSON payload to the local Consul agent.
 *
 * @param payload JSON body accepted by Consul's service registration endpoint.
 * @param timeout Connect and socket I/O timeout.
 * @return `Status::Ok()` for any 2xx response, otherwise transport or HTTP failure status.
 */
auto ConsulAgentClient::RegisterService(const std::string &payload, std::chrono::milliseconds timeout) const -> Status {
  return HandleAgentWriteResponse(http_client_.Put("/v1/agent/service/register", payload, timeout));
}

/**
 * @brief Deregisters a service id from the local Consul agent.
 *
 * @param service_id Consul service instance id.
 * @param timeout Connect and socket I/O timeout.
 * @return `Status::Ok()` for any 2xx response, otherwise transport or HTTP failure status.
 */
auto ConsulAgentClient::DeregisterService(const std::string &service_id, std::chrono::milliseconds timeout) const
    -> Status {
  return HandleAgentWriteResponse(http_client_.Put("/v1/agent/service/deregister/" + service_id, "", timeout));
}

/**
 * @brief Converts a Consul agent write response into the public status contract.
 *
 * @param response HTTP response or transport/protocol failure from `ConsulHttpClient`.
 * @return OK for 2xx responses, otherwise the propagated or synthesized failure status.
 */
auto ConsulAgentClient::HandleAgentWriteResponse(const StatusOr<ConsulHttpResponse> &response) const -> Status {
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
