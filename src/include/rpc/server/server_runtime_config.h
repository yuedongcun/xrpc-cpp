#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <xrpc/rpc_server.h>

#include "protocol/frame_codec.h"
#include "rpc/naming/consul_registrar.h"

namespace xrpc {

/**
 * @brief Normalized server options used internally by `RpcServer::ServerRuntime`.
 *
 * Values here are validated and have protocol limits resolved. `worker_threads_` may still be zero until runtime
 * startup resolves it to hardware concurrency.
 */
struct ServerRuntimeConfig {
  std::size_t worker_threads_ = 0;
  std::size_t connection_io_threads_ = 0;
  std::size_t listen_backlog_ = 0;
  std::size_t max_inflight_per_connection_ = 0;
  std::size_t max_write_queue_bytes_per_connection_ = 0;
  std::size_t max_pending_jobs_global_ = 0;
  std::chrono::milliseconds connection_idle_timeout_{0};
  std::string service_name_;
  std::string service_id_;
  std::string service_address_;
  std::uint16_t service_port_ = 0;
  std::string consul_address_;
  std::chrono::milliseconds consul_timeout_{0};
  ProtocolLimits protocol_limits_;
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
