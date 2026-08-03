#include <xrpc/xrpc_exception.h>

#include <string>

namespace xrpc {

/**
 * @brief Creates an implementation exception with the status code exposed to public callers.
 */
XrpcException::XrpcException(StatusCode code, const std::string &message) : std::runtime_error(message), code_(code) {}

/**
 * @brief Returns the status code associated with this exception.
 */
auto XrpcException::code() const noexcept -> StatusCode { return code_; }

/**
 * @brief Converts this exception into a public status.
 */
auto XrpcException::status() const -> Status { return {code_, what()}; }

/**
 * @brief Converts a known XRPC exception to status without changing its code.
 */
auto ExceptionToStatus(const XrpcException &exception) -> Status { return exception.status(); }

/**
 * @brief Maps allocation failure to resource exhaustion.
 */
auto ExceptionToStatus(const std::bad_alloc &) -> Status {
  return {StatusCode::ResourceExhausted, "memory allocation failed"};
}

/**
 * @brief Maps standard argument errors to invalid argument.
 */
auto ExceptionToStatus(const std::invalid_argument &exception) -> Status {
  return {StatusCode::InvalidArgument, exception.what()};
}

/**
 * @brief Maps other standard exceptions to internal errors.
 */
auto ExceptionToStatus(const std::exception &exception) -> Status { return {StatusCode::Internal, exception.what()}; }

/**
 * @brief Converts the currently handled exception to status.
 */
auto CaughtExceptionToStatus() -> Status {
  return CaughtExceptionToStatus(StatusCode::Internal, "unknown non-standard exception");
}

/**
 * @brief Converts the currently handled exception with a fallback message for non-standard exceptions.
 */
auto CaughtExceptionToStatus(std::string_view non_standard_exception_message) -> Status {
  return CaughtExceptionToStatus(StatusCode::Internal, non_standard_exception_message);
}

/**
 * @brief Converts the currently handled exception with explicit fallback status details.
 */
auto CaughtExceptionToStatus(StatusCode non_standard_exception_code, std::string_view non_standard_exception_message)
    -> Status {
  const std::exception_ptr current_exception = std::current_exception();
  if (current_exception == nullptr) {
    return {StatusCode::Internal, "no active exception"};
  }

  try {
    std::rethrow_exception(current_exception);
  } catch (const XrpcException &exception) {
    return ExceptionToStatus(exception);
  } catch (const std::bad_alloc &exception) {
    return ExceptionToStatus(exception);
  } catch (const std::invalid_argument &exception) {
    return ExceptionToStatus(exception);
  } catch (const std::exception &exception) {
    return ExceptionToStatus(exception);
  } catch (...) {
    return {non_standard_exception_code, std::string(non_standard_exception_message)};
  }
}

/**
 * @brief Converts `std::current_exception()` to status.
 */
auto CurrentExceptionToStatus() -> Status { return CaughtExceptionToStatus(); }

}  // namespace xrpc
