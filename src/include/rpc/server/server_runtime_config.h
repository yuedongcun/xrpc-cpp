#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <xrpc/rpc_server.h>

#include "rpc/naming/consul_registrar.h"
#include "transport/tcp_server.h"

namespace xrpc {

/**
 * @brief Normalized server options used internally by `RpcServer::ServerRuntime`.
 *
 * All defaults, resource limits, and transport values are resolved before this configuration reaches the runtime.
 */
struct ServerRuntimeConfig {
  std::size_t worker_threads_;
  std::size_t max_pending_jobs_global_;
  TcpServerConfig transport_;
  std::string service_name_;
  std::string service_id_;
  std::string service_address_;
  std::uint16_t service_port_;
  std::string consul_address_;
  std::chrono::milliseconds consul_timeout_;
};

/**
 * @brief Validates public server options and converts them to internal configuration.
 *
 * @param options Public server options.
 * @return Normalized server configuration.
 */
[[nodiscard]] auto NormalizeServerOptions(const RpcServerOptions &options) -> ServerRuntimeConfig;

/** @return true when this server configuration should register itself in Consul. */
[[nodiscard]] auto ServiceRegistrationEnabled(const ServerRuntimeConfig &config) -> bool;

/**
 * @brief Resolves the final Consul registration payload.
 *
 * The listen host and bound port are used as fallbacks for missing advertised address and port fields.
 */
[[nodiscard]] auto ResolveRegistrarOptions(const ServerRuntimeConfig &config, std::string_view host,
                                           std::uint16_t listen_port) -> ConsulRegistrar::Options;

}  // namespace xrpc
