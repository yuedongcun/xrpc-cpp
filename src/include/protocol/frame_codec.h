#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include <xrpc/protocol_options.h>

#include "protocol/fixed_header.h"
#include "protocol/protocol_message.h"

namespace xrpc {

/**
 * @brief Frame-size limits enforced by `FrameCodec`.
 *
 * Design note:
 * - Wire format: every frame is `FixedHeader` + protobuf header bytes + opaque payload.
 * - Limits: `max_frame_size_` must include the fixed header and both variable sections.
 * - Decode: `TryDecode()` never consumes partial frames; callers keep buffered bytes until `consumed_` is non-zero.
 * - Failure: malformed complete frames return a protocol error instead of throwing.
 */
struct ProtocolLimits {
  /** @brief Default cap for serialized protobuf metadata headers. */
  static constexpr std::size_t DEFAULT_MAX_HEADER_SIZE = 64U * 1024U;

  /** @brief Default cap for opaque request or response payloads. */
  static constexpr std::size_t DEFAULT_MAX_PAYLOAD_SIZE = xrpc::DEFAULT_MAX_PAYLOAD_SIZE;

  /** @brief Default cap for a full wire frame, including fixed prefix, metadata, and payload. */
  static constexpr std::size_t DEFAULT_MAX_FRAME_SIZE =
      FixedHeader::SIZE + DEFAULT_MAX_HEADER_SIZE + DEFAULT_MAX_PAYLOAD_SIZE;

  /** @brief Maximum accepted protobuf metadata header size. */
  std::size_t max_header_size_ = DEFAULT_MAX_HEADER_SIZE;

  /** @brief Maximum accepted opaque payload size. */
  std::size_t max_payload_size_ = DEFAULT_MAX_PAYLOAD_SIZE;

  /** @brief Maximum accepted full wire frame size. */
  std::size_t max_frame_size_ = DEFAULT_MAX_FRAME_SIZE;
};

/**
 * @brief Builds protocol limits from a public payload-size option.
 *
 * @param max_payload_size Maximum opaque payload size.
 * @return Internal header, payload, and full-frame limits.
 */
[[nodiscard]] auto MakeProtocolLimits(std::size_t max_payload_size) -> ProtocolLimits;

/**
 * @brief Server-side hot-path cache for repeated request headers on one connection.
 *
 * Firehose-style workloads often reuse service and method names while changing only the payload. The cache is explicit
 * and owned by `RpcFrameStream`, so it never shares header bytes across connections.
 */
struct RequestHeaderDecodeCache {
  /** @brief Serialized protobuf header bytes from the last cached request. */
  std::string header_bytes_;

  /** @brief Decoded service name associated with `header_bytes_`. */
  std::string service_name_;

  /** @brief Decoded method name associated with `header_bytes_`. */
  std::string method_name_;

  /** @brief True after the cache has been populated with one complete request header. */
  bool has_value_ = false;
};

/**
 * @brief Stateless frame encoder and decoder for one protocol version.
 *
 * Request header caching is explicit so server sessions can reuse it without hidden global state or cross-connection
 * sharing. One `FrameCodec` can be used repeatedly by the same owner, but it does not provide synchronization.
 */
class FrameCodec final {
 public:
  /** @brief Creates a codec with default protocol limits. */
  FrameCodec() = default;

  /**
   * @brief Creates a codec with explicit protocol limits.
   *
   * @param limits Maximum header, payload, and full-frame sizes.
   */
  explicit FrameCodec(ProtocolLimits limits);

  /** @return Encoded request frame bytes ready for a TCP stream. */
  auto EncodeRequest(const ProtocolRequest &req) -> std::string;

  /** @return Encoded response frame bytes ready for a TCP stream. */
  auto EncodeResponse(const ProtocolResponse &resp) -> std::string;

  /**
   * @brief Attempts to decode one request or response frame.
   *
   * `NeedMoreData` means callers should append more bytes and retry without discarding the existing buffer.
   *
   * @param buf Buffered stream bytes.
   * @return Decode outcome and number of bytes consumed.
   */
  auto TryDecode(std::string_view buf) -> DecodeResult;

  /**
   * @brief Attempts to decode one frame while reusing request-header cache entries.
   *
   * @param buf Buffered stream bytes.
   * @param request_header_cache Per-frame-stream request header cache.
   * @return Decode outcome and number of bytes consumed.
   */
  auto TryDecode(std::string_view buf, RequestHeaderDecodeCache &request_header_cache) -> DecodeResult;

  /**
   * @brief Attempts to decode one request frame for the server path.
   *
   * @param buf Buffered stream bytes.
   * @param request_header_cache Per-frame-stream request header cache.
   * @return Request-only decode outcome.
   */
  auto TryDecodeRequest(std::string_view buf, RequestHeaderDecodeCache &request_header_cache) -> RequestDecodeResult;

 private:
  /** @brief Shared decode implementation with an optional request-header cache. */
  auto TryDecode(std::string_view buf, RequestHeaderDecodeCache *request_header_cache) -> DecodeResult;

  ProtocolLimits limits_;
};

}  // namespace xrpc
