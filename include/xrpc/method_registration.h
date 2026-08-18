#pragma once

#include <exception>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

#include <xrpc/status_or.h>

namespace xrpc {

struct MethodRegistration final {
  std::string service_name_;

  std::string method_name_;

  std::function<StatusOr<std::string>(std::string_view)> invoke_;
};

template <typename Request, typename Response, typename Func>
auto MakeMethodRegistration(std::string service_name, std::string method_name, Func func)
    -> StatusOr<MethodRegistration> {
  try {
    MethodRegistration registration;
    registration.service_name_ = std::move(service_name);
    registration.method_name_ = std::move(method_name);
    registration.invoke_ = [func = std::move(func)](std::string_view payload) mutable -> StatusOr<std::string> {
      try {
        Request request;
        if (!request.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
          return StatusOr<std::string>(Status{StatusCode::InvalidArgument, "failed to parse protobuf request"});
        }

        Response response = func(request);
        std::string encoded;
        if (!response.SerializeToString(&encoded)) {
          return StatusOr<std::string>(Status{StatusCode::Internal, "failed to serialize protobuf response"});
        }
        return StatusOr<std::string>(std::move(encoded));
      } catch (const std::exception &exception) {
        return StatusOr<std::string>(Status{StatusCode::Internal, exception.what()});
      } catch (...) {
        return StatusOr<std::string>(Status{StatusCode::Internal, "handler threw unknown exception"});
      }
    };
    return StatusOr<MethodRegistration>(std::move(registration));
  } catch (const std::exception &exception) {
    return StatusOr<MethodRegistration>(Status{StatusCode::Internal, exception.what()});
  } catch (...) {
    return StatusOr<MethodRegistration>(Status{StatusCode::Internal, "failed to create method registration"});
  }
}

}  // namespace xrpc
