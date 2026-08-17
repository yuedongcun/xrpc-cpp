#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include <xrpc/status.h>

namespace xrpc {

/**
 * @brief Decoded XRPC request metadata and opaque payload.
 *
 * This is the message passed from frame decoding to service dispatch.
 */
struct RawRequest {
  /** @brief Request id copied from the fixed header. */
  std::uint64_t request_id_ = 0;

  /** @brief Service and method selected by the request metadata header. */
  std::string service_name_;
  std::string method_name_;

  /** @brief Serialized application request payload. */
  std::string payload_;
};

/**
 * @brief XRPC response metadata and opaque payload.
 *
 * `status_` is encoded in the response metadata header; `payload_` carries a successful method result.
 */
struct RawResponse {
  /** @brief Request id echoed from the original request. */
  std::uint64_t request_id_ = 0;

  /** @brief Application or framework result. */
  Status status_;

  /** @brief Serialized application response payload. */
  std::string payload_;
};

/**
 * @brief Non-throwing protocol encode/decode outcome.
 *
 * `NeedMoreData` is recoverable. Other errors mean the current frame cannot be accepted.
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

/**
 * @brief Result of decoding at most one complete frame from a byte buffer.
 *
 * `NeedMoreData` leaves `consumed_` at zero and does not discard the caller's buffered bytes. Successful decodes fill
 * exactly one raw RPC message and report how many bytes the caller can remove from the buffer.
 */
struct DecodeResult {
  /** @brief Decode status for this attempt. */
  ProtocolError error_;

  /** @brief Number of bytes consumed from the caller's buffer. */
  size_t consumed_;

  /** @brief Decoded request when `error_ == ProtocolError::Ok` and the frame is a request. */
  std::optional<RawRequest> request_;

  /** @brief Decoded response when `error_ == ProtocolError::Ok` and the frame is a response. */
  std::optional<RawResponse> response_;

  /** @return true when this result contains either a request or a response. */
  [[nodiscard]] auto HasMessage() const -> bool { return request_.has_value() || response_.has_value(); }
};

/**
 * @brief Request-only decode result used by server hot paths.
 *
 * The server only expects request frames from clients. A response frame on this path is reported as a protocol error so
 * the frame stream can close the connection instead of trying to dispatch an invalid message.
 */
struct RequestDecodeResult {
  /** @brief Decode status for this attempt. */
  ProtocolError error_;

  /** @brief Number of bytes consumed from the caller's buffer. */
  size_t consumed_;

  /** @brief Decoded request when `error_ == ProtocolError::Ok`. */
  std::optional<RawRequest> request_;
};

}  // namespace xrpc
