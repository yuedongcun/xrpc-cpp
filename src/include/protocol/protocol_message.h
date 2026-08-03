#pragma once

#include <optional>
#include <string>

#include "protocol/protocol_error.h"

namespace xrpc {

/**
 * @brief Protocol-level request after fixed-frame and protobuf-header decoding.
 *
 * `service_name_` and `method_name_` live in the protobuf metadata header. `payload_` is opaque to the protocol layer
 * and is interpreted later by the RPC method adapter.
 */
struct ProtocolRequest {
  /** @brief Request id copied from the fixed header and echoed by the response. */
  uint64_t request_id_;

  /** @brief Service namespace selected by the client. */
  std::string service_name_;

  /** @brief Method name inside `service_name_`. */
  std::string method_name_;

  /** @brief Opaque request body bytes. */
  std::string payload_;
};

/**
 * @brief Protocol-level response after fixed-frame and protobuf-header decoding.
 *
 * `error_code_ == 0` and empty `error_text_` represent success. Non-zero error codes are mapped back to public
 * `StatusCode` values after the transport hands the response to the RPC layer.
 */
struct ProtocolResponse {
  /** @brief Request id of the original call. */
  uint64_t request_id_;

  /** @brief Encoded status code. Zero means success. */
  int32_t error_code_;

  /** @brief Human-readable server-side error message for failed calls. */
  std::string error_text_;

  /** @brief Opaque response body bytes. Empty when the response carries an error status. */
  std::string payload_;
};

/**
 * @brief Result of decoding at most one complete frame from a byte buffer.
 *
 * `NeedMoreData` leaves `consumed_` at zero and does not discard the caller's buffered bytes. Successful decodes fill
 * exactly one of `request_` or `response_` and report how many bytes the caller can remove from the buffer.
 */
struct DecodeResult {
  /** @brief Decode status for this attempt. */
  ProtocolError error_;

  /** @brief Number of bytes consumed from the caller's buffer. */
  size_t consumed_;

  /** @brief Decoded request when `error_ == ProtocolError::Ok` and the frame is a request. */
  std::optional<ProtocolRequest> request_;

  /** @brief Decoded response when `error_ == ProtocolError::Ok` and the frame is a response. */
  std::optional<ProtocolResponse> response_;

  /** @return true when this result contains either a request or a response. */
  [[nodiscard]] auto HasMessage() const -> bool { return request_.has_value() || response_.has_value(); }
};

/**
 * @brief Request-only decode result used by server hot paths.
 *
 * The server only expects request frames from clients. A response frame on this path is reported as a protocol error so
 * the session can close the connection instead of trying to dispatch an invalid message.
 */
struct RequestDecodeResult {
  /** @brief Decode status for this attempt. */
  ProtocolError error_;

  /** @brief Number of bytes consumed from the caller's buffer. */
  size_t consumed_;

  /** @brief Decoded request when `error_ == ProtocolError::Ok`. */
  std::optional<ProtocolRequest> request_;
};

}  // namespace xrpc
