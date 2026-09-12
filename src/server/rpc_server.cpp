/**
 * @file rpc_server.cpp
 * @brief Implements the public `RpcServer` facade.
 *
 * Public server operations delegate to `RpcServer::Impl`, while exceptions
 * raised by the internal runtime are converted into `Status` or `StatusOr`
 * values at the API boundary.
 */

#include <xrpc/rpc_server.h>

#include <memory>
#include <utility>

#include "common/xrpc_exception.h"
#include "server/rpc_server_impl.h"
#include "server/runtime_access.h"
#include "server/runtime_stats.h"

namespace xrpc {

RpcServer::RpcServer(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

auto RpcServer::Create(const RpcServerOptions &options) -> StatusOr<RpcServer> {
  return ServerRuntimeAccess::CreateWithBufferPool(options, {});
}

auto ServerRuntimeAccess::CreateWithBufferPool(const RpcServerOptions &options, io::UringBufferPoolConfig buffer_pool)
    -> StatusOr<RpcServer> {
  try {
    StatusOr<ServerConfig> config = ServerConfig::Create(options, buffer_pool);
    if (!config.ok()) {
      return StatusOr<RpcServer>(config.status());
    }
    return StatusOr<RpcServer>(RpcServer(std::make_unique<RpcServer::Impl>(std::move(config).value())));
  } catch (...) {  // XRPC_EXTERNAL_EXCEPTION_BOUNDARY: public API
    return StatusOr<RpcServer>(CaughtExceptionToStatus("failed to create RPC server"));
  }
}

RpcServer::~RpcServer() = default;
RpcServer::RpcServer(RpcServer &&) noexcept = default;
auto RpcServer::operator=(RpcServer &&) noexcept -> RpcServer & = default;

auto RpcServer::RegisterMethod(MethodRegistration registration) -> Status {
  try {
    return impl_->RegisterMethod(std::move(registration));
  } catch (...) {  // XRPC_EXTERNAL_EXCEPTION_BOUNDARY: public API
    return CaughtExceptionToStatus("failed to register RPC method");
  }
}

auto RpcServer::Listen(std::string_view host, std::uint16_t port) -> Status {
  try {
    return impl_->Listen(host, port);
  } catch (...) {  // XRPC_EXTERNAL_EXCEPTION_BOUNDARY: public API
    return CaughtExceptionToStatus("failed to listen");
  }
}

auto RpcServer::Run() -> Status {
  try {
    return impl_->Run();
  } catch (...) {  // XRPC_EXTERNAL_EXCEPTION_BOUNDARY: public API
    return CaughtExceptionToStatus("server runtime failed");
  }
}

void RpcServer::Stop() { impl_->Stop(); }

auto RpcServer::port() const -> StatusOr<std::uint16_t> { return StatusOr<std::uint16_t>(impl_->port()); }

auto ServerStatsAccess::Snapshot(RpcServer &server, bool start_window) -> StatusOr<ServerStatsSnapshot> {
  if (!server.impl_) {
    return StatusOr<ServerStatsSnapshot>(
        Status{StatusCode::FailedPrecondition, "statistics requested from a moved-from server"});
  }
  return server.impl_->SnapshotStats(start_window);
}

}  // namespace xrpc
