#include "rpc/server/service_registry.h"

#include <sstream>
#include <utility>

#include "rpc/xrpc_exception.h"

namespace xrpc {

/**
 * @brief Invokes the type-erased raw handler owned by this method descriptor.
 */
auto MethodDescriptor::Invoke(RawRequest request) const -> RawResponse { return handler_(std::move(request)); }

/**
 * @brief Registers or replaces one method under this service.
 */
void ServiceDescriptor::RegisterMethod(std::string method_name, RawHandler handler) {
  if (methods_.contains(method_name)) {
    std::ostringstream oss;
    oss << "method already registered: service=" << service_name_ << ", method=" << method_name;
    throw ConfigException(oss.str());
  }

  std::string method_key = method_name;
  methods_.emplace(method_key, MethodDescriptor(service_name_, std::move(method_name), std::move(handler)));
}

/**
 * @brief Finds a registered method by name.
 */
auto ServiceDescriptor::FindMethod(std::string_view method_name) const -> const MethodDescriptor * {
  auto it = methods_.find(std::string(method_name));
  if (it == methods_.end()) {
    return nullptr;
  }
  return &it->second;
}

/**
 * @brief Registers a raw service method in the registry.
 *
 * Registration owns the handler. Dispatch later borrows immutable descriptors from the built registry.
 */
void ServiceRegistry::RegisterRaw(const std::string &service, const std::string &method, RawHandler handler) {
  auto result = services_.try_emplace(service, ServiceDescriptor(service));
  auto &service_descriptor = result.first->second;
  service_descriptor.RegisterMethod(method, std::move(handler));
}

/**
 * @brief Finds a registered service by name.
 */
auto ServiceRegistry::FindService(std::string_view service_name) const -> const ServiceDescriptor * {
  auto service_it = services_.find(std::string(service_name));
  if (service_it == services_.end()) {
    return nullptr;
  }
  return &service_it->second;
}

/**
 * @brief Finds a registered method by service and method name.
 */
auto ServiceRegistry::FindMethod(std::string_view service, std::string_view method) const -> const MethodDescriptor * {
  const ServiceDescriptor *service_descriptor = FindService(service);
  if (service_descriptor == nullptr) {
    return nullptr;
  }
  return service_descriptor->FindMethod(method);
}

/**
 * @brief Dispatches one decoded request through the registry.
 *
 * Lookup misses and thrown handler failures are converted to raw responses so the transport can still return a normal
 * RPC status frame.
 */
auto ServiceRegistry::Dispatch(RawRequest request) const -> RawResponse {
  const ServiceDescriptor *service = FindService(request.service_name_);
  if (service == nullptr) {
    RawResponse resp;
    resp.request_id_ = request.request_id_;
    resp.status_ = {StatusCode::NotFound, "unknown service"};
    return resp;
  }

  const MethodDescriptor *method = service->FindMethod(request.method_name_);
  if (method == nullptr) {
    RawResponse resp;
    resp.request_id_ = request.request_id_;
    resp.status_ = {StatusCode::Unimplemented, "unknown method"};
    return resp;
  }

  try {
    return method->Invoke(std::move(request));
  } catch (...) {
    RawResponse resp;
    resp.request_id_ = request.request_id_;
    resp.status_ = CaughtExceptionToStatus("handler threw unknown exception");
    return resp;
  }
}

}  // namespace xrpc
