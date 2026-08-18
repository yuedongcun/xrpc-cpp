/**
 * @file server_config.cpp
 * @brief Implements server configuration validation and normalization.
 *
 * Public `RpcServerOptions` are validated and converted into the internal
 * `ServerConfig` used by the server runtime. Service-registration settings are
 * also resolved here into concrete Consul registration options.
 */

#include "server/server_config.h"

#include <unistd.h>

#include <cstdint>
#include <limits>
#include <string>
#include <thread>

#include "common/xrpc_exception.h"

namespace xrpc {

namespace {

auto IsWildcardAddress(std::string_view host) -> bool { return host == "0.0.0.0" || host == "::"; }

auto ResolveWorkerThreads(std::size_t worker_threads) -> std::size_t {
  if (worker_threads > 0) {
    return worker_threads;
  }

  const auto hardware_threads = std::thread::hardware_concurrency();
  return hardware_threads == 0 ? 1U : static_cast<std::size_t>(hardware_threads);
}

}  // namespace

auto NormalizeServerOptions(const RpcServerOptions &options) -> ServerConfig {
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
  return ServerConfig{
      .worker_threads_ = ResolveWorkerThreads(options.worker_threads_),
      .max_pending_jobs_ = options.max_pending_jobs_global_,
      .backlog_ = static_cast<int>(options.listen_backlog_),
      .io_threads_ = options.connection_io_threads_,
      .connection_limits_ =
          ConnectionBackpressureLimits{
              .max_inflight_ = options.max_inflight_per_connection_,
              .max_write_queue_bytes_ = options.max_write_queue_bytes_per_connection_,
          },
      .protocol_limits_ = MakeProtocolLimits(options.max_payload_size_),
      .consul_ =
          ConsulRegistrationConfig{
              .service_name_ = options.service_name_,
              .service_id_ = options.service_id_,
              .service_address_ = options.service_address_,
              .service_port_ = options.service_port_,
              .agent_address_ = options.consul_address_,
              .timeout_ = options.consul_timeout_,
          },
  };
}

auto ServiceRegistrationEnabled(const ServerConfig &config) -> bool { return !config.consul_.service_name_.empty(); }

auto ResolveRegistrarOptions(const ServerConfig &config, std::string_view host, std::uint16_t listen_port)
    -> ConsulRegistrar::Options {
  if (!ServiceRegistrationEnabled(config)) {
    throw LifecycleException("service registration is not enabled");
  }

  const std::uint16_t service_port = config.consul_.service_port_ == 0 ? listen_port : config.consul_.service_port_;
  if (service_port != listen_port) {
    throw ConfigException("service_port must equal listening port in phase one");
  }

  std::string service_address = config.consul_.service_address_;
  if (service_address.empty()) {
    if (host.empty() || IsWildcardAddress(host)) {
      throw ConfigException("service_address is required when listen host is wildcard");
    }
    service_address = std::string(host);
  }

  std::string service_id = config.consul_.service_id_;
  if (service_id.empty()) {
    service_id = config.consul_.service_name_ + "_" + service_address + "_" + std::to_string(service_port) + "_" +
                 std::to_string(static_cast<std::int64_t>(::getpid()));
  }

  return ConsulRegistrar::Options{
      .service_name_ = config.consul_.service_name_,
      .service_id_ = std::move(service_id),
      .service_address_ = std::move(service_address),
      .service_port_ = service_port,
      .timeout_ = config.consul_.timeout_,
  };
}

}  // namespace xrpc
