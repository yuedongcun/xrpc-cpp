#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include <xrpc/status.h>

namespace xrpc {

struct RawRequest {
  std::uint64_t request_id_ = 0;

  std::string service_name_;
  std::string method_name_;

  std::string payload_;
};

struct RawResponse {
  std::uint64_t request_id_ = 0;

  Status status_;

  std::string payload_;
};

enum class ProtocolError : std::uint8_t {
  Ok = 0,
  NeedMoreData,
  InvalidMagic,
  UnsupportedVersion,
  InvalidMessageType,
  UnsupportedMessageType,
  FrameTooLarge,
  DecodeError,
  EncodeError,
};

struct DecodeResult {
  ProtocolError error_;

  size_t consumed_;

  std::optional<RawRequest> request_;

  std::optional<RawResponse> response_;

  [[nodiscard]] auto HasMessage() const -> bool { return request_.has_value() || response_.has_value(); }
};

struct RequestDecodeResult {
  ProtocolError error_;

  size_t consumed_;

  std::optional<RawRequest> request_;
};

}  // namespace xrpc
