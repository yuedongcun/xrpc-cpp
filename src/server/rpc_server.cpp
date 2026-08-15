#include <xrpc/rpc_server.h>

#include <memory>
#include <utility>

#include "common/xrpc_exception.h"
#include "server/rpc_server_impl.h"

namespace xrpc {

RpcServer::RpcServer(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

auto RpcServer::Create(const RpcServerOptions &options) -> StatusOr<RpcServer> {
  try {
    return StatusOr<RpcServer>(RpcServer(std::make_unique<Impl>(options)));
  } catch (...) {
    return StatusOr<RpcServer>(CaughtExceptionToStatus("failed to create RPC server"));
  }
}

RpcServer::~RpcServer() = default;
RpcServer::RpcServer(RpcServer &&) noexcept = default;
auto RpcServer::operator=(RpcServer &&) noexcept -> RpcServer & = default;

auto RpcServer::RegisterMethod(MethodRegistration registration) -> Status {
  try {
    impl_->RegisterMethod(std::move(registration));
    return Status::Ok();
  } catch (...) {
    return CaughtExceptionToStatus("failed to register RPC method");
  }
}

auto RpcServer::Listen(std::string_view host, std::uint16_t port) -> Status {
  try {
    impl_->Listen(host, port);
    return Status::Ok();
  } catch (...) {
    return CaughtExceptionToStatus("failed to listen");
  }
}

auto RpcServer::Run() -> Status {
  try {
    impl_->Run();
    return Status::Ok();
  } catch (...) {
    return CaughtExceptionToStatus("server runtime failed");
  }
}

void RpcServer::Stop() { impl_->Stop(); }

auto RpcServer::port() const -> StatusOr<std::uint16_t> {
  try {
    return StatusOr<std::uint16_t>(impl_->port());
  } catch (...) {
    return StatusOr<std::uint16_t>(CaughtExceptionToStatus("server port is unavailable"));
  }
}

}  // namespace xrpc
