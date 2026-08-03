#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "rpc/handler.h"
#include "rpc/raw_message.h"

namespace xrpc {

/**
 * @brief Immutable descriptor for one registered RPC method.
 *
 * The descriptor owns the service name, method name, and raw handler. Dispatch code borrows descriptors after lookup
 * and invokes the handler with a fully decoded raw request.
 */
class MethodDescriptor final {
 public:
  /**
   * @brief Creates a descriptor from registration data.
   *
   * @param service_name Owning service namespace.
   * @param method_name Method name inside the service.
   * @param handler Raw request handler.
   */
  MethodDescriptor(std::string service_name, std::string method_name, RawHandler handler)
      : service_name_(std::move(service_name)), method_name_(std::move(method_name)), handler_(std::move(handler)) {}

  /** @return Owning service name. */
  [[nodiscard]] auto service_name() const -> std::string_view { return service_name_; }

  /** @return Method name inside the service. */
  [[nodiscard]] auto method_name() const -> std::string_view { return method_name_; }

  /**
   * @brief Invokes the registered raw handler.
   *
   * @param request Fully decoded raw request.
   * @return Raw response produced by the handler or converted from a thrown exception.
   */
  [[nodiscard]] auto Invoke(RawRequest request) const -> RawResponse;

 private:
  /** @brief Owning service namespace. */
  std::string service_name_;

  /** @brief Method name inside the service. */
  std::string method_name_;

  /** @brief Type-erased raw handler supplied during registration. */
  RawHandler handler_;
};

/**
 * @brief Registry entry for one service and its registered methods.
 *
 * `ServiceDescriptor` and `MethodDescriptor` are intentionally lightweight: registration owns handlers, dispatch only
 * borrows descriptors. The registry is built before the server starts listening and then read concurrently by handler
 * dispatch paths.
 */
class ServiceDescriptor final {
 public:
  /**
   * @brief Creates an empty service descriptor.
   *
   * @param service_name Service namespace.
   */
  explicit ServiceDescriptor(std::string service_name) : service_name_(std::move(service_name)) {}

  /**
   * @brief Adds or replaces a method under this service.
   *
   * @param method_name Method name inside the service.
   * @param handler Raw request handler.
   */
  void RegisterMethod(std::string method_name, RawHandler handler);

  /** @return Borrowed descriptor for `method_name`, or null when it is not registered. */
  [[nodiscard]] auto FindMethod(std::string_view method_name) const -> const MethodDescriptor *;

  /** @return Service namespace owned by this descriptor. */
  [[nodiscard]] auto service_name() const -> std::string_view { return service_name_; }

 private:
  /** @brief Service namespace. */
  std::string service_name_;

  /** @brief Registered methods keyed by method name. */
  std::unordered_map<std::string, MethodDescriptor> methods_;
};

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

  /** @return Borrowed service descriptor, or null when the service is not registered. */
  [[nodiscard]] auto FindService(std::string_view service_name) const -> const ServiceDescriptor *;

  /** @return Borrowed method descriptor, or null when service or method lookup fails. */
  [[nodiscard]] auto FindMethod(std::string_view service, std::string_view method) const -> const MethodDescriptor *;

  /**
   * @brief Dispatches one raw request through the registered method table.
   *
   * @param request Decoded request with service, method, payload, and request id.
   * @return Raw response containing either method output or a status failure.
   */
  [[nodiscard]] auto Dispatch(RawRequest request) const -> RawResponse;

 private:
  /** @brief Registered services keyed by service name. */
  std::unordered_map<std::string, ServiceDescriptor> services_;
};

}  // namespace xrpc
