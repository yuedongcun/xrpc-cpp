#include "rpc/server/server_runtime_config.h"

#include <unistd.h>

#include <cstdint>
#include <limits>
#include <string>
#include <thread>

#include "rpc/xrpc_exception.h"

namespace xrpc {

namespace {

/**
 * @brief Returns true when a listen address binds all local interfaces.
 *
 * @param host Listen address configured by the caller.
 * @return true when `host` is an IPv4 or IPv6 wildcard bind address.
 */
auto IsWildcardAddress(std::string_view host) -> bool { return host == "0.0.0.0" || host == "::"; }

/** @brief Resolves the public zero value to a concrete worker count. */
auto ResolveWorkerThreads(std::size_t worker_threads) -> std::size_t {
  if (worker_threads > 0) {
    return worker_threads;
  }

  const auto hardware_threads = std::thread::hardware_concurrency();
  return hardware_threads == 0 ? 1U : static_cast<std::size_t>(hardware_threads);
}

}  // namespace

/**
 * @brief Validates public server options and resolves internal runtime configuration.
 *
 * The returned configuration is the shape consumed by `ServerRuntime`, `TcpServer`, worker
 * pool, protocol codec, and optional Consul registration. Later runtime code can rely on all
 * numeric resource limits being non-zero and all optional Consul fields being internally coherent.
 *
 * @param options User-facing server options.
 * @return Normalized server configuration.
 * @throws ConfigException when any option combination is invalid.
 */
auto NormalizeServerOptions(const RpcServerOptions &options) -> ServerRuntimeConfig {
  if (options.listen_backlog_ == 0) {
    throw ConfigException("RpcServer listen_backlog must be greater than 0");
  }
  if (options.listen_backlog_ > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    throw ConfigException("RpcServer listen_backlog exceeds the socket API range");
  }
  if (options.connection_io_threads_ == 0) {
    throw ConfigException("RpcServer connection_io_threads must be greater than 0");
  }
  if (options.max_inflight_per_connection_ == 0) {
    throw ConfigException("RpcServer max_inflight_per_connection must be greater than 0");
  }
  if (options.max_write_queue_bytes_per_connection_ == 0) {
    throw ConfigException("RpcServer max_write_queue_bytes_per_connection must be greater than 0");
  }
  if (options.max_pending_jobs_global_ == 0) {
    throw ConfigException("RpcServer max_pending_jobs_global must be greater than 0");
  }
  if (options.consul_timeout_ < std::chrono::milliseconds::zero()) {
    throw ConfigException("RpcServer consul_timeout must not be negative");
  }
  if (options.connection_idle_timeout_ < std::chrono::milliseconds::zero()) {
    throw ConfigException("RpcServer connection_idle_timeout must not be negative");
  }
  if (options.service_name_.empty()) {
    if (!options.service_id_.empty()) {
      throw ConfigException("RpcServer service_id requires service_name");
    }
    if (!options.service_address_.empty()) {
      throw ConfigException("RpcServer service_address requires service_name");
    }
    if (options.service_port_ != 0) {
      throw ConfigException("RpcServer service_port requires service_name");
    }
  } else if (options.consul_address_.empty()) {
    throw ConfigException("RpcServer consul_address must not be empty when service registration is enabled");
  }
  return ServerRuntimeConfig{
      .worker_threads_ = ResolveWorkerThreads(options.worker_threads_),
      .max_pending_jobs_global_ = options.max_pending_jobs_global_,
      .transport_ =
          TcpServerConfig{
              .listen_backlog_ = static_cast<int>(options.listen_backlog_),
              .backpressure_limits_ =
                  ConnectionBackpressureLimits{
                      .max_inflight_ = options.max_inflight_per_connection_,
                      .max_write_queue_bytes_ = options.max_write_queue_bytes_per_connection_,
                  },
              .connection_io_threads_ = options.connection_io_threads_,
              .protocol_limits_ = MakeProtocolLimits(options.max_payload_size_),
              .connection_idle_timeout_ = options.connection_idle_timeout_,
          },
      .service_name_ = options.service_name_,
      .service_id_ = options.service_id_,
      .service_address_ = options.service_address_,
      .service_port_ = options.service_port_,
      .consul_address_ = options.consul_address_,
      .consul_timeout_ = options.consul_timeout_,
  };
}

/**
 * @brief Returns whether this server should register itself in Consul.
 *
 * @param config Normalized server configuration.
 * @return true when a non-empty service name enables registration.
 */
auto ServiceRegistrationEnabled(const ServerRuntimeConfig &config) -> bool { return !config.service_name_.empty(); }

/**
 * @brief Builds the final Consul registrar options after the listen socket is bound.
 *
 * Wildcard listen addresses are local bind choices, not useful advertised addresses, so the default advertised address
 * falls back to loopback unless the user configured one explicitly.
 *
 * @param config Normalized server configuration.
 * @param host Listen host passed to `RpcServer::Listen()`.
 * @param listen_port Actual bound listen port.
 * @return Fully resolved Consul registrar options.
 * @throws LifecycleException when service registration is disabled.
 * @throws ConfigException when the advertised address or port cannot be derived safely.
 */
auto ResolveRegistrarOptions(const ServerRuntimeConfig &config, std::string_view host, std::uint16_t listen_port)
    -> ConsulRegistrar::Options {
  if (!ServiceRegistrationEnabled(config)) {
    throw LifecycleException("service registration is not enabled");
  }

  const std::uint16_t service_port = config.service_port_ == 0 ? listen_port : config.service_port_;
  if (service_port != listen_port) {
    throw ConfigException("service_port must equal listening port in phase one");
  }

  std::string service_address = config.service_address_;
  if (service_address.empty()) {
    if (host.empty() || IsWildcardAddress(host)) {
      throw ConfigException("service_address is required when listen host is wildcard");
    }
    service_address = std::string(host);
  }

  std::string service_id = config.service_id_;
  if (service_id.empty()) {
    service_id = config.service_name_ + "_" + service_address + "_" + std::to_string(service_port) + "_" +
                 std::to_string(static_cast<std::int64_t>(::getpid()));
  }

  return ConsulRegistrar::Options{
      .service_name_ = config.service_name_,
      .service_id_ = std::move(service_id),
      .service_address_ = std::move(service_address),
      .service_port_ = service_port,
      .timeout_ = config.consul_timeout_,
  };
}

}  // namespace xrpc
