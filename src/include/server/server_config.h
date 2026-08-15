#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <xrpc/rpc_server.h>

#include "discovery/consul_registrar.h"
#include "protocol/frame_codec.h"
#include "server/server_backpressure.h"

namespace xrpc {

/** @brief Normalized settings used when registering this server instance in Consul. */
struct ConsulRegistrationConfig {
  std::string service_name_;
  std::string service_id_;
  std::string service_address_;
  std::uint16_t service_port_;
  std::string agent_address_;
  std::chrono::milliseconds timeout_;
};

/**
 * @brief Fully normalized options consumed by `RpcServer::Impl`.
 *
 * Execution, listener, connection, protocol, and registration values are resolved before this configuration reaches
 * the runtime.
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
 * @brief Validates public server options and converts them to internal configuration.
 *
 * @param options Public server options.
 * @return Normalized server configuration.
 */
[[nodiscard]] auto NormalizeServerOptions(const RpcServerOptions &options) -> ServerConfig;

/** @return true when this server configuration should register itself in Consul. */
[[nodiscard]] auto ServiceRegistrationEnabled(const ServerConfig &config) -> bool;

/**
 * @brief Resolves the final Consul registration payload.
 *
 * The listen host and bound port are used as fallbacks for missing advertised address and port fields.
 */
[[nodiscard]] auto ResolveRegistrarOptions(const ServerConfig &config, std::string_view host, std::uint16_t listen_port)
    -> ConsulRegistrar::Options;

}  // namespace xrpc
