/**
 * @file service_registry.h
 * @brief Defines RPC service registration and request dispatch.
 */

#pragma once

#include <functional>
#include <string>
#include <unordered_map>

#include "protocol/rpc_envelope.h"

namespace xrpc {

using RequestHandler = std::function<ResponseEnvelope(RequestEnvelope)>;

/**
 * @brief Runtime read-only mapping from RPC service and method names to handlers.
 *
 * Registration remains available through the listening setup phase. `Run()`
 * freezes the registry before worker threads begin calling `Dispatch()`
 * concurrently, so runtime lookup needs no lock.
 */
class ServiceRegistry final {
 public:
  /**
   * @brief Registers one request handler.
   *
   * Throws `ConfigException` if the service-method pair is already registered.
   */
  void Register(const std::string &service, const std::string &method, RequestHandler handler);

  /**
   * @brief Dispatches one request envelope to its registered handler.
   *
   * Unknown services return `NotFound`, unknown methods return `Unimplemented`,
   * and handler exceptions are converted into an RPC error status.
   */
  [[nodiscard]] auto Dispatch(RequestEnvelope request) const -> ResponseEnvelope;

 private:
  using MethodMap = std::unordered_map<std::string, RequestHandler>;

  std::unordered_map<std::string, MethodMap> services_;
};

}  // namespace xrpc
