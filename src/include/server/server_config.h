/**
 * @file server_config.h
 * @brief Defines normalized server configuration derived from public options.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <xrpc/rpc_server.h>

#include "naming/consul/consul_registrar.h"
#include "protocol/frame_codec.h"
#include "server/connection_backpressure.h"

namespace xrpc {

/** @brief Normalized service-registration settings for Consul. */
struct ConsulRegistrationConfig {
  std::string service_name_;
  std::string service_id_;
  std::string service_address_;
  std::string agent_address_;
};

/**
 * @brief Validated internal configuration consumed by the server runtime.
 *
 * Values are normalized from `RpcServerOptions`; runtime components should not
 * reinterpret public defaults or repeat configuration validation.
 */
struct ServerConfig {
  std::size_t worker_threads_;
  std::size_t max_pending_jobs_;
  int backlog_;
  std::size_t io_threads_;
  ConnectionBackpressureLimits connection_limits_;
  ProtocolLimits protocol_limits_;
  ConsulRegistrationConfig consul_;
};

/**
 * @brief Validates public server options and produces the internal runtime
 * configuration.
 */
[[nodiscard]] auto NormalizeServerOptions(const RpcServerOptions &options) -> ServerConfig;

[[nodiscard]] auto ServiceRegistrationEnabled(const ServerConfig &config) -> bool;

/**
 * @brief Resolves concrete Consul registration options for the listening
 * endpoint.
 *
 * Missing service address and ID values are derived where permitted. The
 * concrete registration always uses the actual listening port.
 */
[[nodiscard]] auto ResolveRegistrarOptions(const ServerConfig &config, std::string_view host, std::uint16_t listen_port)
    -> ConsulRegistrar::Options;

}  // namespace xrpc
