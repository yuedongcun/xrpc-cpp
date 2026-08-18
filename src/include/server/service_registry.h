/** @file service_registry.h @brief Declares RPC service and method dispatch registration. */

#pragma once

#include <functional>
#include <string>
#include <unordered_map>

#include "protocol/protocol_message.h"

namespace xrpc {

using RawHandler = std::function<RawResponse(RawRequest)>;

/**
 * @brief Immutable-at-runtime mapping from service and method names to handlers.
 *
 * Registration happens during server setup. After `Run()` begins the registry
 * is read-only, so worker threads may call `Dispatch()` concurrently without a
 * lookup lock.
 */
class ServiceRegistry final {
 public:
  void RegisterRaw(const std::string &service, const std::string &method, RawHandler handler);

  [[nodiscard]] auto Dispatch(RawRequest request) const -> RawResponse;

 private:
  using MethodMap = std::unordered_map<std::string, RawHandler>;

  std::unordered_map<std::string, MethodMap> services_;
};

}  // namespace xrpc
