#include "server/service_registry.h"

#include <sstream>
#include <utility>

#include "common/xrpc_exception.h"

namespace xrpc {

/**
 * @brief Registers a raw service method in the registry.
 *
 * Registration owns the handler. Dispatch reads the built registry after the server starts listening.
 */
void ServiceRegistry::RegisterRaw(const std::string &service, const std::string &method, RawHandler handler) {
  MethodMap &methods = services_[service];
  if (methods.contains(method)) {
    std::ostringstream oss;
    oss << "method already registered: service=" << service << ", method=" << method;
    throw ConfigException(oss.str());
  }

  methods.emplace(method, std::move(handler));
}

/**
 * @brief Dispatches one decoded request through the registry.
 *
 * Lookup misses and thrown handler failures are converted to raw responses so the transport can still return a normal
 * RPC status frame.
 */
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
