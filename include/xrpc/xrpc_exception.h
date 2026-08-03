#pragma once

#include <exception>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>

#include <xrpc/status.h>

namespace xrpc {

/**
 * @brief Base exception type used inside XRPC implementation boundaries.
 *
 * Public RPC APIs convert these exceptions to `Status` so callers do not need exception handling for normal RPC
 * failures. Throwing is reserved for configuration, lifecycle, protocol, and transport boundaries where propagating a
 * status through every internal call would obscure the control flow.
 */
class XrpcException : public std::runtime_error {
 public:
  /**
   * @brief Constructs an exception with the status code returned to public callers.
   *
   * @param code Public status code associated with this exception.
   * @param message Human-readable diagnostic text.
   */
  XrpcException(StatusCode code, const std::string &message);

  /** @return Public status code associated with this exception. */
  [[nodiscard]] auto code() const noexcept -> StatusCode;

  /** @return A `Status` carrying this exception's code and message text. */
  [[nodiscard]] auto status() const -> Status;

 private:
  StatusCode code_;
};

/** @brief Invalid user configuration detected before runtime work starts. */
class ConfigException final : public XrpcException {
 public:
  /** @brief Constructs an invalid-argument configuration exception. */
  explicit ConfigException(const std::string &message) : XrpcException(StatusCode::InvalidArgument, message) {}
};

/** @brief Invalid lifecycle transition, such as registering after listening. */
class LifecycleException final : public XrpcException {
 public:
  /** @brief Constructs a failed-precondition lifecycle exception. */
  explicit LifecycleException(const std::string &message) : XrpcException(StatusCode::FailedPrecondition, message) {}
};

/** @brief Internal invariant failure that should be reported as an RPC internal error. */
class InternalException final : public XrpcException {
 public:
  /** @brief Constructs an internal-error exception. */
  explicit InternalException(const std::string &message) : XrpcException(StatusCode::Internal, message) {}
};

/** @brief Malformed protocol input or protobuf adaptation failure. */
class ProtocolException final : public XrpcException {
 public:
  /** @brief Constructs a data-loss protocol exception. */
  explicit ProtocolException(const std::string &message) : XrpcException(StatusCode::DataLoss, message) {}

  /** @brief Constructs a protocol exception with an explicit status code. */
  ProtocolException(StatusCode code, const std::string &message) : XrpcException(code, message) {}
};

/** @brief Socket, connection, or remote endpoint failure. */
class TransportException : public XrpcException {
 public:
  /** @brief Constructs an unavailable transport exception. */
  explicit TransportException(const std::string &message) : XrpcException(StatusCode::Unavailable, message) {}

  /** @brief Constructs a transport exception with an explicit status code. */
  TransportException(StatusCode code, const std::string &message) : XrpcException(code, message) {}
};

/** @return Status equivalent of a known XRPC exception. */
[[nodiscard]] auto ExceptionToStatus(const XrpcException &exception) -> Status;

/** @return Resource-exhausted status for allocation failure. */
[[nodiscard]] auto ExceptionToStatus(const std::bad_alloc &exception) -> Status;

/** @return Invalid-argument status for standard argument errors. */
[[nodiscard]] auto ExceptionToStatus(const std::invalid_argument &exception) -> Status;

/** @return Internal status for other standard exceptions. */
[[nodiscard]] auto ExceptionToStatus(const std::exception &exception) -> Status;

/** @return Status equivalent of the currently handled exception. */
[[nodiscard]] auto CaughtExceptionToStatus() -> Status;

/** @return Status equivalent of the currently handled non-standard exception. */
[[nodiscard]] auto CaughtExceptionToStatus(std::string_view non_standard_exception_message) -> Status;

/** @return Status equivalent of the currently handled non-standard exception using an explicit code. */
[[nodiscard]] auto CaughtExceptionToStatus(StatusCode non_standard_exception_code,
                                           std::string_view non_standard_exception_message) -> Status;

/** @return Status equivalent of `std::current_exception()`, or internal status when no exception is active. */
[[nodiscard]] auto CurrentExceptionToStatus() -> Status;

}  // namespace xrpc
