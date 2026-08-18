/** @file xrpc_exception.cpp @brief Implements internal exception-to-status conversion. */

#include "common/xrpc_exception.h"

#include <string>

namespace xrpc {

XrpcException::XrpcException(StatusCode code, const std::string &message) : std::runtime_error(message), code_(code) {}

auto XrpcException::status() const -> Status { return {code_, what()}; }

auto CaughtExceptionToStatus(std::string_view non_standard_exception_message) -> Status {
  return CaughtExceptionToStatus(StatusCode::Internal, non_standard_exception_message);
}

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
