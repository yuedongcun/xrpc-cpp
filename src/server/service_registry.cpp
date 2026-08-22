/**
 * @file service_registry.cpp
 * @brief Implements RPC handler registration and dispatch.
 */

#include "server/service_registry.h"

#include <sstream>
#include <utility>

#include "common/xrpc_exception.h"

namespace xrpc {

void ServiceRegistry::Register(const std::string &service, const std::string &method, RequestHandler handler) {
  MethodMap &methods = services_[service];
  if (methods.contains(method)) {
    std::ostringstream oss;
    oss << "method already registered: service=" << service << ", method=" << method;
    throw ConfigException(oss.str());
  }

  methods.emplace(method, std::move(handler));
}

auto ServiceRegistry::Dispatch(RequestEnvelope request) const -> ResponseEnvelope {
  const auto service = services_.find(request.service_name_);
  if (service == services_.end()) {
    ResponseEnvelope resp;
    resp.request_id_ = request.request_id_;
    resp.status_ = {StatusCode::NotFound, "unknown service"};
    return resp;
  }

  const auto method = service->second.find(request.method_name_);
  if (method == service->second.end()) {
    ResponseEnvelope resp;
    resp.request_id_ = request.request_id_;
    resp.status_ = {StatusCode::Unimplemented, "unknown method"};
    return resp;
  }

  const std::uint64_t request_id = request.request_id_;
  try {
    return method->second(std::move(request));
  } catch (...) {
    ResponseEnvelope resp;
    resp.request_id_ = request_id;
    resp.status_ = CaughtExceptionToStatus("handler threw unknown exception");
    return resp;
  }
}

}  // namespace xrpc
