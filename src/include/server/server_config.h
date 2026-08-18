/** @file server_config.h @brief Defines normalized internal server configuration. */

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <xrpc/rpc_server.h>

#include "naming/consul/consul_registrar.h"
#include "protocol/frame_codec.h"
#include "server/connection_backpressure.h"

namespace xrpc {

struct ConsulRegistrationConfig {
  std::string service_name_;
  std::string service_id_;
  std::string service_address_;
  std::uint16_t service_port_;
  std::string agent_address_;
  std::chrono::milliseconds timeout_;
};

struct ServerConfig {
  std::size_t worker_threads_;
  std::size_t max_pending_jobs_;
  int backlog_;
  std::size_t io_threads_;
  ConnectionBackpressureLimits connection_limits_;
  ProtocolLimits protocol_limits_;
  ConsulRegistrationConfig consul_;
};

[[nodiscard]] auto NormalizeServerOptions(const RpcServerOptions &options) -> ServerConfig;

[[nodiscard]] auto ServiceRegistrationEnabled(const ServerConfig &config) -> bool;

[[nodiscard]] auto ResolveRegistrarOptions(const ServerConfig &config, std::string_view host, std::uint16_t listen_port)
    -> ConsulRegistrar::Options;

}  // namespace xrpc
