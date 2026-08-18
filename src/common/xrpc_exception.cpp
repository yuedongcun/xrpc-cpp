#include "common/xrpc_exception.h"

#include <string>

namespace xrpc {

XrpcException::XrpcException(StatusCode code, const std::string &message) : std::runtime_error(message), code_(code) {}

auto XrpcException::code() const noexcept -> StatusCode { return code_; }

auto XrpcException::status() const -> Status { return {code_, what()}; }

auto ExceptionToStatus(const XrpcException &exception) -> Status { return exception.status(); }

auto ExceptionToStatus([[maybe_unused]] const std::bad_alloc &exception) -> Status {
  return {StatusCode::ResourceExhausted, "memory allocation failed"};
}

auto ExceptionToStatus(const std::invalid_argument &exception) -> Status {
  return {StatusCode::InvalidArgument, exception.what()};
}

auto ExceptionToStatus(const std::exception &exception) -> Status { return {StatusCode::Internal, exception.what()}; }

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

}  // namespace xrpc
