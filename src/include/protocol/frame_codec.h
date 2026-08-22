/**
 * @file frame_codec.h
 * @brief Defines complete-frame encoding and decoding for the xRPC wire protocol.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <xrpc/rpc_options.h>

#include "protocol/frame_header.h"
#include "protocol/rpc_envelope.h"

namespace xrpc {

/** @brief Size limits applied before allocating or decoding frame contents. */
struct ProtocolLimits {
  static constexpr std::size_t DEFAULT_MAX_METADATA_SIZE = 64U * 1024U;

  static constexpr std::size_t DEFAULT_MAX_PAYLOAD_SIZE = xrpc::DEFAULT_MAX_PAYLOAD_SIZE;

  std::size_t max_metadata_size_ = DEFAULT_MAX_METADATA_SIZE;

  std::size_t max_payload_size_ = DEFAULT_MAX_PAYLOAD_SIZE;
};

/** @brief Builds validated protocol limits for one configured payload bound. */
[[nodiscard]] auto MakeProtocolLimits(std::size_t max_payload_size) -> ProtocolLimits;

/**
 * @brief Outcome of decoding an xRPC wire frame.
 *
 * `NeedMoreData` is a normal stream condition: callers retain the buffered
 * bytes and retry after receiving more input. Other non-OK values describe a
 * malformed, unsupported, or oversized frame.
 */
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

/** @brief Result of decoding either a request or a response frame. */
struct FrameDecodeResult {
  ProtocolError error_;

  /** Bytes consumed after establishing a complete frame boundary; otherwise zero. */
  std::size_t consumed_;

  /** Exactly one envelope is present after successful decoding. */
  std::optional<RequestEnvelope> request_;

  std::optional<ResponseEnvelope> response_;

  [[nodiscard]] auto HasEnvelope() const -> bool { return request_.has_value() || response_.has_value(); }
};

/**
 * @brief Encodes and decodes complete xRPC wire frames.
 *
 * FrameCodec validates the fixed prefix and configured size limits, parses the
 * Protobuf metadata section, and leaves the user payload serialized. It does
 * not buffer incomplete TCP input; stream buffering belongs to RpcFrameStream.
 */
class FrameCodec final {
 public:
  explicit FrameCodec(ProtocolLimits limits = {});

  /** @brief Encodes one request into a complete wire frame. */
  [[nodiscard]] auto Encode(const RequestEnvelope &request) const -> std::string;

  /** @brief Encodes one response into a complete wire frame. */
  [[nodiscard]] auto Encode(const ResponseEnvelope &response) const -> std::string;

  /**
   * @brief Attempts to decode one request or response from the start of `buf`.
   *
   * `NeedMoreData` asks the caller to retain the current bytes. On success,
   * exactly one of `request_` and `response_` is populated.
   */
  [[nodiscard]] auto Decode(std::string_view buf) const -> FrameDecodeResult;

 private:
  ProtocolLimits limits_;
};

}  // namespace xrpc
