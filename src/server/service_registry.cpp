/** @file service_registry.cpp @brief Implements service and method dispatch lookup. */

#include "server/service_registry.h"

#include <sstream>
#include <utility>

#include "common/xrpc_exception.h"

namespace xrpc {

void ServiceRegistry::RegisterRaw(const std::string &service, const std::string &method, RawHandler handler) {
  MethodMap &methods = services_[service];
  if (methods.contains(method)) {
    std::ostringstream oss;
    oss << "method already registered: service=" << service << ", method=" << method;
    throw ConfigException(oss.str());
  }

  methods.emplace(method, std::move(handler));
}

auto ServiceRegistry::Dispatch(RawRequest request) const -> RawResponse {
  const auto service = services_.find(request.service_name_);
  if (service == services_.end()) {
    RawResponse resp;
    resp.request_id_ = request.request_id_;
    resp.status_ = {StatusCode::NotFound, "unknown service"};
    return resp;
  }

  const auto method = service->second.find(request.method_name_);
  if (method == service->second.end()) {
    RawResponse resp;
    resp.request_id_ = request.request_id_;
    resp.status_ = {StatusCode::Unimplemented, "unknown method"};
    return resp;
  }

  try {
    return method->second(std::move(request));
  } catch (...) {
    RawResponse resp;
    resp.request_id_ = request.request_id_;
    resp.status_ = CaughtExceptionToStatus("handler threw unknown exception");
    return resp;
  }
}

}  // namespace xrpc
