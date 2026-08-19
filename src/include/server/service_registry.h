/**
 * @file service_registry.h
 * @brief Defines RPC service registration and request dispatch.
 */

#pragma once

#include <functional>
#include <string>
#include <unordered_map>

#include "protocol/protocol_message.h"

namespace xrpc {

using RawHandler = std::function<RawResponse(RawRequest)>;

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
   * @brief Registers one raw RPC handler.
   *
   * Throws `ConfigException` if the service-method pair is already registered.
   */
  void RegisterRaw(const std::string &service, const std::string &method, RawHandler handler);

  /**
   * @brief Dispatches one raw RPC request to its registered handler.
   *
   * Unknown services return `NotFound`, unknown methods return `Unimplemented`,
   * and handler exceptions are converted into an RPC error status.
   */
  [[nodiscard]] auto Dispatch(RawRequest request) const -> RawResponse;

 private:
  using MethodMap = std::unordered_map<std::string, RawHandler>;

  std::unordered_map<std::string, MethodMap> services_;
};

}  // namespace xrpc
