/**
 * @brief Base exception carrying the RPC status code for an xRPC failure.
 */

#include "common/xrpc_exception.h"

#include <string>

namespace xrpc {

XrpcException::XrpcException(StatusCode code, const std::string &message) : std::runtime_error(message), code_(code) {}

auto XrpcException::status() const -> Status { return {code_, what()}; }

/** @brief Converts the active exception using `Internal` as the fallback code. */
auto CaughtExceptionToStatus(std::string_view non_standard_exception_message) -> Status {
  return CaughtExceptionToStatus(StatusCode::Internal, non_standard_exception_message);
}

/**
 * @brief Converts the exception currently being handled into an RPC `Status`.
 *
 * Call this from a `catch` block. Known xRPC and standard exceptions use their
 * built-in status mappings; the supplied code and message are used only as a
 * fallback for non-standard exceptions.
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
    return exception.status();
  } catch (const std::bad_alloc &) {
    return {StatusCode::ResourceExhausted, "memory allocation failed"};
  } catch (const std::invalid_argument &exception) {
    return {StatusCode::InvalidArgument, exception.what()};
  } catch (const std::exception &exception) {
    return {StatusCode::Internal, exception.what()};
  } catch (...) {
    return {non_standard_exception_code, std::string(non_standard_exception_message)};
  }
}

}  // namespace xrpc
