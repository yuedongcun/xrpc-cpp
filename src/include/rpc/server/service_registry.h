#pragma once

#include <string>
#include <unordered_map>

#include "rpc/raw_message.h"

namespace xrpc {

/**
 * @brief Owns all registered services and performs raw request dispatch.
 *
 * The registry is single-writer during server setup and read-only after `RpcServer::Listen()`. Lookup failures produce
 * normal RPC status responses so unknown services and methods do not crash the connection.
 */
class ServiceRegistry final {
 public:
  /**
   * @brief Registers a raw method handler.
   *
   * @param service Service namespace.
   * @param method Method name inside the service.
   * @param handler Raw request handler.
   */
  void RegisterRaw(const std::string &service, const std::string &method, RawHandler handler);

  /**
   * @brief Dispatches one raw request through the registered method table.
   *
   * @param request Decoded request with service, method, payload, and request id.
   * @return Raw response containing either method output or a status failure.
   */
  [[nodiscard]] auto Dispatch(RawRequest request) const -> RawResponse;

 private:
  using MethodMap = std::unordered_map<std::string, RawHandler>;

  /** @brief Registered handlers keyed first by service name, then by method name. */
  std::unordered_map<std::string, MethodMap> services_;
};

}  // namespace xrpc
