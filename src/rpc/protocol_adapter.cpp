#include "rpc/protocol_adapter.h"

#include <utility>

namespace xrpc {

namespace {

/**
 * @brief Converts protocol metadata status fields into a public `Status`.
 *
 * Unknown status codes are treated as data loss because the peer sent a response this runtime cannot interpret.
 */
auto DecodeStatus(std::int32_t code, std::string message) -> Status {
  switch (static_cast<StatusCode>(code)) {
    case StatusCode::Ok:
      return Status::Ok();
    case StatusCode::Cancelled:
    case StatusCode::InvalidArgument:
    case StatusCode::DeadlineExceeded:
    case StatusCode::NotFound:
    case StatusCode::AlreadyExists:
    case StatusCode::PermissionDenied:
    case StatusCode::ResourceExhausted:
    case StatusCode::FailedPrecondition:
    case StatusCode::Unimplemented:
    case StatusCode::Internal:
    case StatusCode::Unavailable:
    case StatusCode::DataLoss:
    case StatusCode::Unauthenticated:
      return {static_cast<StatusCode>(code), std::move(message)};
  }
  return {StatusCode::DataLoss, "response contains an invalid RPC status code"};
}

}  // namespace

/**
 * @brief Moves a decoded protocol request into the raw RPC core request model.
 */
auto ToRawRequest(ProtocolRequest &&request) -> RawRequest {
  RawRequest raw;
  raw.request_id_ = request.request_id_;
  raw.service_name_ = std::move(request.service_name_);
  raw.method_name_ = std::move(request.method_name_);
  raw.payload_ = std::move(request.payload_);
  return raw;
}

/**
 * @brief Copies a raw RPC request into protocol fields for frame encoding.
 */
auto ToProtocolRequest(const RawRequest &request) -> ProtocolRequest {
  ProtocolRequest protocol;
  protocol.request_id_ = request.request_id_;
  protocol.service_name_ = request.service_name_;
  protocol.method_name_ = request.method_name_;
  protocol.payload_ = request.payload_;
  return protocol;
}

/**
 * @brief Moves a raw RPC response into protocol fields for frame encoding.
 */
auto ToProtocolResponse(RawResponse &&response) -> ProtocolResponse {
  ProtocolResponse protocol;
  protocol.request_id_ = response.request_id_;
  protocol.error_code_ = static_cast<std::int32_t>(response.status_.code());
  protocol.error_text_ = response.status_.message();
  protocol.payload_ = std::move(response.payload_);
  return protocol;
}

/**
 * @brief Converts a decoded protocol response into a raw RPC response.
 */
auto ToRawResponse(const ProtocolResponse &response) -> RawResponse {
  RawResponse raw;
  raw.request_id_ = response.request_id_;
  raw.status_ = DecodeStatus(response.error_code_, response.error_text_);
  raw.payload_ = response.payload_;
  return raw;
}

}  // namespace xrpc
