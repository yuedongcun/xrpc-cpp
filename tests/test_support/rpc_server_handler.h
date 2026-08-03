#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <xrpc/method_registration.h>

#include "rpc/handler.h"
#include "rpc/server/service_registry.h"

namespace xrpc::testsupport {

inline auto MakeRegisteredHandler(std::vector<MethodRegistration> registrations) -> RawHandler {
  auto registry = std::make_shared<ServiceRegistry>();
  for (MethodRegistration &registration : registrations) {
    auto invoke = std::move(registration.invoke_);
    registry->RegisterRaw(registration.service_name_, registration.method_name_,
                          [invoke = std::move(invoke)](RawRequest request) -> RawResponse {
                            RawResponse response;
                            response.request_id_ = request.request_id_;
                            response.payload_ = invoke(request.payload_);
                            return response;
                          });
  }
  return [registry = std::move(registry)](RawRequest request) { return registry->Dispatch(std::move(request)); };
}

inline auto MakeRegisteredHandler(MethodRegistration registration) -> RawHandler {
  std::vector<MethodRegistration> registrations;
  registrations.push_back(std::move(registration));
  return MakeRegisteredHandler(std::move(registrations));
}

template <typename Request, typename Response, typename Func>
auto MakeRegisteredHandler(std::string service_name, std::string method_name, Func func) -> RawHandler {
  return MakeRegisteredHandler(
      MakeMethodRegistration<Request, Response>(std::move(service_name), std::move(method_name), std::move(func)));
}

}  // namespace xrpc::testsupport
