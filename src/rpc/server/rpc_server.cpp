#include <xrpc/rpc_server.h>

#include <utility>

#include "rpc/server/server_runtime.h"
#include "rpc/xrpc_exception.h"

namespace xrpc {

/**
 * @brief Creates a server facade from an initialized private runtime.
 */
RpcServer::RpcServer(std::unique_ptr<ServerRuntime> runtime) : runtime_(std::move(runtime)) {}

/**
 * @brief Creates a configured server without exposing internal exceptions.
 */
auto RpcServer::Create(const RpcServerOptions &options) -> StatusOr<RpcServer> {
  try {
    return StatusOr<RpcServer>(RpcServer(std::make_unique<ServerRuntime>(options)));
  } catch (...) {
    return StatusOr<RpcServer>(CaughtExceptionToStatus("failed to create RPC server"));
  }
}

RpcServer::~RpcServer() = default;

/**
 * @brief Moves the private server runtime from another facade.
 */
RpcServer::RpcServer(RpcServer &&) noexcept = default;

/**
 * @brief Replaces this facade's runtime with another facade's runtime.
 */
auto RpcServer::operator=(RpcServer &&) noexcept -> RpcServer & = default;

/**
 * @brief Registers one type-erased method with the private runtime.
 */
auto RpcServer::RegisterMethod(MethodRegistration registration) -> Status {
  try {
    runtime_->RegisterMethod(std::move(registration));
    return Status::Ok();
  } catch (...) {
    return CaughtExceptionToStatus("failed to register RPC method");
  }
}

/**
 * @brief Binds the listening socket through the private runtime.
 */
auto RpcServer::Listen(std::string_view host, std::uint16_t port) -> Status {
  try {
    runtime_->Listen(host, port);
    return Status::Ok();
  } catch (...) {
    return CaughtExceptionToStatus("failed to listen");
  }
}

/**
 * @brief Runs the blocking server lifecycle through the private runtime.
 */
auto RpcServer::Run() -> Status {
  try {
    runtime_->Run();
    return Status::Ok();
  } catch (...) {
    return CaughtExceptionToStatus("server runtime failed");
  }
}

/**
 * @brief Requests server shutdown through the private runtime.
 */
void RpcServer::Stop() { runtime_->Stop(); }

/**
 * @brief Returns the bound listen port from the private runtime.
 */
auto RpcServer::port() const -> StatusOr<std::uint16_t> {
  try {
    return StatusOr<std::uint16_t>(runtime_->port());
  } catch (...) {
    return StatusOr<std::uint16_t>(CaughtExceptionToStatus("server port is unavailable"));
  }
}

/**
 * @brief Returns server diagnostic counters from the private runtime.
 */
auto RpcServer::stats() const -> RpcServerStats { return runtime_->stats(); }

}  // namespace xrpc
