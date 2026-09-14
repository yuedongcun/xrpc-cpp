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

namespace xrpc {

namespace {

auto IsWildcardAddress(std::string_view host) -> bool { return host == "0.0.0.0" || host == "::"; }

auto MakeListenConfig(std::size_t backlog) -> StatusOr<ListenConfig> {
  if (backlog == 0) {
    return StatusOr<ListenConfig>(
        Status{StatusCode::InvalidArgument, "RpcServer listen_backlog must be greater than 0"});
  }
  if (backlog > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
    return StatusOr<ListenConfig>(
        Status{StatusCode::InvalidArgument, "RpcServer listen_backlog exceeds the socket API range"});
  }
  return StatusOr<ListenConfig>(ListenConfig{.backlog_ = static_cast<int>(backlog)});
}

auto MakeConsulRegistrationConfig(const RpcServerOptions &options) -> StatusOr<ConsulRegistrationConfig> {
  if (options.service_name_.empty()) {
    if (!options.service_id_.empty()) {
      return StatusOr<ConsulRegistrationConfig>(
          Status{StatusCode::InvalidArgument, "RpcServer service_id requires service_name"});
    }
    if (!options.service_address_.empty()) {
      return StatusOr<ConsulRegistrationConfig>(
          Status{StatusCode::InvalidArgument, "RpcServer service_address requires service_name"});
    }
  } else if (options.consul_address_.empty()) {
    return StatusOr<ConsulRegistrationConfig>(
        Status{StatusCode::InvalidArgument,
               "RpcServer consul_address must not be empty when service registration is enabled"});
  }
  return StatusOr<ConsulRegistrationConfig>(ConsulRegistrationConfig{
      .service_name_ = options.service_name_,
      .service_id_ = options.service_id_,
      .service_address_ = options.service_address_,
      .agent_address_ = options.consul_address_,
  });
}

}  // namespace

auto ServerConfig::Create(const RpcServerOptions &options, io::UringBufferPoolConfig buffer_pool)
    -> StatusOr<ServerConfig> {
  const auto pool_status = buffer_pool.Validate();
  if (!pool_status.ok()) {
    return StatusOr<ServerConfig>(pool_status);
  }
  StatusOr<WorkerPoolConfig> worker_pool =
      MakeWorkerPoolConfig(options.worker_threads_, options.max_pending_jobs_global_);
  if (!worker_pool.ok()) {
    return StatusOr<ServerConfig>(worker_pool.status());
  }
  StatusOr<ListenConfig> listen = MakeListenConfig(options.listen_backlog_);
  if (!listen.ok()) {
    return StatusOr<ServerConfig>(listen.status());
  }
  StatusOr<ConnectionIoConfig> connection_io =
      MakeConnectionIoConfig(options.connection_io_threads_, options.max_inflight_per_connection_,
                             options.max_write_queue_bytes_per_connection_, options.max_payload_size_);
  if (!connection_io.ok()) {
    return StatusOr<ServerConfig>(connection_io.status());
  }
  connection_io.value().buffer_pool_ = buffer_pool;
  StatusOr<ConsulRegistrationConfig> consul = MakeConsulRegistrationConfig(options);
  if (!consul.ok()) {
    return StatusOr<ServerConfig>(consul.status());
  }
  return StatusOr<ServerConfig>(ServerConfig{
      .worker_pool_ = std::move(worker_pool).value(),
      .listen_ = std::move(listen).value(),
      .connection_io_ = std::move(connection_io).value(),
      .consul_ = std::move(consul).value(),
  });
}

auto ServiceRegistrationEnabled(const ServerConfig &config) -> bool { return !config.consul_.service_name_.empty(); }

/**
 * @brief Resolves concrete Consul registration options from the normalized server configuration.
 *
 * Missing service address and ID values are derived from the listening
 * endpoint when possible. A wildcard listen address requires an explicit
 * service address. The registered port is always the actual listening port.
 */
auto ResolveRegistrarOptions(const ServerConfig &config, std::string_view host, std::uint16_t listen_port)
    -> StatusOr<ConsulRegistrar::Options> {
  if (!ServiceRegistrationEnabled(config)) {
    return StatusOr<ConsulRegistrar::Options>(
        Status{StatusCode::FailedPrecondition, "service registration is not enabled"});
  }

  std::string service_address = config.consul_.service_address_;
  if (service_address.empty()) {
    if (host.empty() || IsWildcardAddress(host)) {
      return StatusOr<ConsulRegistrar::Options>(
          Status{StatusCode::InvalidArgument, "service_address is required when listen host is wildcard"});
    }
    service_address = std::string(host);
  }

  std::string service_id = config.consul_.service_id_;
  if (service_id.empty()) {
    // Generate a process-unique default ID from the resolved service endpoint.
    service_id = config.consul_.service_name_ + "_" + service_address + "_" + std::to_string(listen_port) + "_" +
                 std::to_string(static_cast<std::int64_t>(::getpid()));
  }

  return StatusOr<ConsulRegistrar::Options>(ConsulRegistrar::Options{
      .service_name_ = config.consul_.service_name_,
      .service_id_ = std::move(service_id),
      .service_address_ = std::move(service_address),
      .service_port_ = listen_port,
  });
}

}  // namespace xrpc
