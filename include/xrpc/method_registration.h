#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <utility>

#include <xrpc/xrpc_exception.h>

namespace xrpc {

/**
 * @brief Type-erased server method registration consumed by the internal service registry.
 *
 * `invoke_` receives raw protobuf request bytes and returns raw protobuf response bytes. It may throw `XrpcException`
 * subclasses to report protocol, application, or internal failures; the registry converts those exceptions to RPC
 * status responses before the transport writes a frame back to the client.
 */
struct MethodRegistration final {
  /** @brief Service namespace clients use in `RpcClient::Call*()`. */
  std::string service_name_;

  /** @brief Method name inside `service_name_`. */
  std::string method_name_;

  /** @brief Type-erased method body that maps raw request bytes to raw response bytes. */
  std::function<std::string(std::string_view)> invoke_;
};

/**
 * @brief Adapts a typed protobuf handler into the raw server registry contract.
 *
 * `Request` must support `ParseFromArray()`, and `Response` must support `SerializeToString()`. Parse and serialize
 * failures become `ProtocolException`s so the transport can return a normal RPC status instead of terminating the
 * server.
 *
 * @param service_name Service namespace clients use when issuing calls.
 * @param method_name Method name inside the service.
 * @param func Callable that accepts `Request` and returns `Response`.
 * @return A type-erased registration ready for `RpcServer`.
 */
template <typename Request, typename Response, typename Func>
auto MakeMethodRegistration(std::string service_name, std::string method_name, Func func) -> MethodRegistration {
  MethodRegistration registration;
  registration.service_name_ = std::move(service_name);
  registration.method_name_ = std::move(method_name);
  registration.invoke_ = [func = std::move(func)](std::string_view payload) mutable -> std::string {
    Request request;
    if (!request.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
      throw ProtocolException(StatusCode::InvalidArgument, "failed to parse protobuf request");
    }

    Response response = func(request);
    std::string encoded;
    if (!response.SerializeToString(&encoded)) {
      throw ProtocolException(StatusCode::Internal, "failed to serialize protobuf response");
    }
    return encoded;
  };
  return registration;
}

}  // namespace xrpc
