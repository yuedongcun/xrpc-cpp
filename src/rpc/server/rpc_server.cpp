#include <xrpc/rpc_server.h>

#include "rpc/server/rpc_server_controller.h"

namespace xrpc {

/**
 * @brief Creates a server facade with default options.
 */
RpcServer::RpcServer() : RpcServer(RpcServerOptions{}) {}

/**
 * @brief Creates a server facade that owns a private lifecycle controller.
 */
RpcServer::RpcServer(const RpcServerOptions &options) : controller_(std::make_unique<ServerController>(options)) {}

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
void RpcServer::RegisterMethodRegistration(MethodRegistration registration) {
  controller_->RegisterMethodRegistration(std::move(registration));
}

/**
 * @brief Binds the listening socket through the private controller.
 */
void RpcServer::Listen(std::string_view host, std::uint16_t port) { controller_->Listen(host, port); }

/**
 * @brief Runs the blocking server lifecycle through the private controller.
 */
void RpcServer::Run() { controller_->Run(); }

/**
 * @brief Requests server shutdown through the private controller.
 */
void RpcServer::Stop() { controller_->Stop(); }

/**
 * @brief Returns the bound listen port from the private controller.
 */
auto RpcServer::port() const -> std::uint16_t { return controller_->port(); }

/**
 * @brief Returns server diagnostic counters from the private controller.
 */
auto RpcServer::stats() const -> RpcServerStats { return controller_->stats(); }

}  // namespace xrpc
