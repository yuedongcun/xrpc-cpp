/** @file frame_codec.h @brief Declares xRPC request and response frame encoding. */

#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include <xrpc/rpc_options.h>

#include "protocol/fixed_header.h"
#include "protocol/protocol_message.h"

namespace xrpc {

struct ProtocolLimits {
  static constexpr std::size_t DEFAULT_MAX_HEADER_SIZE = 64U * 1024U;

  static constexpr std::size_t DEFAULT_MAX_PAYLOAD_SIZE = xrpc::DEFAULT_MAX_PAYLOAD_SIZE;

  static constexpr std::size_t DEFAULT_MAX_FRAME_SIZE =
      FixedHeader::SIZE + DEFAULT_MAX_HEADER_SIZE + DEFAULT_MAX_PAYLOAD_SIZE;

  std::size_t max_header_size_ = DEFAULT_MAX_HEADER_SIZE;

  std::size_t max_payload_size_ = DEFAULT_MAX_PAYLOAD_SIZE;

  std::size_t max_frame_size_ = DEFAULT_MAX_FRAME_SIZE;
};

[[nodiscard]] auto MakeProtocolLimits(std::size_t max_payload_size) -> ProtocolLimits;

struct RequestHeaderDecodeCache {
  std::string header_bytes_;

  std::string service_name_;

  std::string method_name_;

  bool has_value_ = false;
};

class FrameCodec final {
 public:
  FrameCodec() = default;

  explicit FrameCodec(ProtocolLimits limits);

  auto EncodeRequest(const RawRequest &request) -> std::string;

  auto EncodeResponse(const RawResponse &response) -> std::string;

  auto TryDecode(std::string_view buf) -> DecodeResult;

  auto TryDecode(std::string_view buf, RequestHeaderDecodeCache &request_header_cache) -> DecodeResult;

  auto TryDecodeRequest(std::string_view buf, RequestHeaderDecodeCache &request_header_cache) -> RequestDecodeResult;

 private:
  auto TryDecode(std::string_view buf, RequestHeaderDecodeCache *request_header_cache) -> DecodeResult;

  ProtocolLimits limits_;
};

}  // namespace xrpc
