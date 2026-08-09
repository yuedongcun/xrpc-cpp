#include <xrpc/rpc_server.h>

#include <utility>

#include "rpc/server/rpc_server_controller.h"
#include "rpc/xrpc_exception.h"

namespace xrpc {

/**
 * @brief Creates a server facade from an initialized private controller.
 */
RpcServer::RpcServer(std::unique_ptr<ServerController> controller) : controller_(std::move(controller)) {}

/**
 * @brief Creates a configured server without exposing internal exceptions.
 */
auto RpcServer::Create(const RpcServerOptions &options) -> StatusOr<RpcServer> {
  try {
    return StatusOr<RpcServer>(RpcServer(std::make_unique<ServerController>(options)));
  } catch (...) {
    return StatusOr<RpcServer>(CaughtExceptionToStatus("failed to create RPC server"));
  }
}

RpcServer::~RpcServer() = default;

/**
 * @brief Moves the private lifecycle controller from another facade.
 */
RpcServer::RpcServer(RpcServer &&) noexcept = default;

/**
 * @brief Replaces this facade's controller with another facade's controller.
 */
auto RpcServer::operator=(RpcServer &&) noexcept -> RpcServer & = default;

/**
 * @brief Registers one type-erased method with the private controller.
 */
auto RpcServer::RegisterMethodRegistration(MethodRegistration registration) -> Status {
  try {
    controller_->RegisterMethodRegistration(std::move(registration));
    return Status::Ok();
  } catch (...) {
    return CaughtExceptionToStatus("failed to register RPC method");
  }
}

/**
 * @brief Binds the listening socket through the private controller.
 */
auto RpcServer::Listen(std::string_view host, std::uint16_t port) -> Status {
  try {
    controller_->Listen(host, port);
    return Status::Ok();
  } catch (...) {
    return CaughtExceptionToStatus("failed to listen");
  }
}

/**
 * @brief Runs the blocking server lifecycle through the private controller.
 */
auto RpcServer::Run() -> Status {
  try {
    controller_->Run();
    return Status::Ok();
  } catch (...) {
    return CaughtExceptionToStatus("server runtime failed");
  }
}

/**
 * @brief Requests server shutdown through the private controller.
 */
void RpcServer::Stop() { controller_->Stop(); }

/**
 * @brief Returns the bound listen port from the private controller.
 */
auto RpcServer::port() const -> StatusOr<std::uint16_t> {
  try {
    return StatusOr<std::uint16_t>(controller_->port());
  } catch (...) {
    return StatusOr<std::uint16_t>(CaughtExceptionToStatus("server port is unavailable"));
  }
}

/**
 * @brief Returns server diagnostic counters from the private controller.
 */
auto RpcServer::stats() const -> RpcServerStats { return controller_->stats(); }

}  // namespace xrpc
