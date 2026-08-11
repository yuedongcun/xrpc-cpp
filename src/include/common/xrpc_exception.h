#pragma once

#include <exception>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>

#include <xrpc/status.h>

namespace xrpc {

class XrpcException : public std::runtime_error {
 public:
  XrpcException(StatusCode code, const std::string &message);

  [[nodiscard]] auto code() const noexcept -> StatusCode;
  [[nodiscard]] auto status() const -> Status;

 private:
  StatusCode code_;
};

class ConfigException final : public XrpcException {
 public:
  explicit ConfigException(const std::string &message) : XrpcException(StatusCode::InvalidArgument, message) {}
};

class LifecycleException final : public XrpcException {
 public:
  explicit LifecycleException(const std::string &message) : XrpcException(StatusCode::FailedPrecondition, message) {}
};

class InternalException final : public XrpcException {
 public:
  explicit InternalException(const std::string &message) : XrpcException(StatusCode::Internal, message) {}
};

class ProtocolException final : public XrpcException {
 public:
  explicit ProtocolException(const std::string &message) : XrpcException(StatusCode::DataLoss, message) {}
  ProtocolException(StatusCode code, const std::string &message) : XrpcException(code, message) {}
};

class TransportException : public XrpcException {
 public:
  explicit TransportException(const std::string &message) : XrpcException(StatusCode::Unavailable, message) {}
  TransportException(StatusCode code, const std::string &message) : XrpcException(code, message) {}
};

[[nodiscard]] auto ExceptionToStatus(const XrpcException &exception) -> Status;
[[nodiscard]] auto ExceptionToStatus(const std::bad_alloc &exception) -> Status;
[[nodiscard]] auto ExceptionToStatus(const std::invalid_argument &exception) -> Status;
[[nodiscard]] auto ExceptionToStatus(const std::exception &exception) -> Status;
[[nodiscard]] auto CaughtExceptionToStatus(std::string_view non_standard_exception_message) -> Status;
[[nodiscard]] auto CaughtExceptionToStatus(StatusCode non_standard_exception_code,
                                           std::string_view non_standard_exception_message) -> Status;

}  // namespace xrpc
