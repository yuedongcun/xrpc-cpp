#pragma once

#include <cstdint>

namespace xrpc {

/**
 * @brief Non-throwing protocol encode/decode outcome.
 *
 * `NeedMoreData` is recoverable and tells stream owners to keep buffering. The other errors indicate that the current
 * frame cannot be accepted and the owning session should close or report failure.
 */
enum class ProtocolError : std::uint8_t {
  /** @brief Operation completed successfully. */
  Ok = 0,

  /** @brief Buffer does not yet contain a full frame. */
  NeedMoreData,

  /** @brief Fixed header magic did not match the XRPC marker. */
  InvalidMagic,

  /** @brief Wire version is not supported by this codec. */
  UnsupportedVersion,

  /** @brief Message type field is not a known enum value. */
  InvalidMessageType,

  /** @brief Message type is known but not supported on this path. */
  UnsupportedMessageType,

  /** @brief Header, payload, or full frame exceeds configured limits. */
  FrameTooLarge,

  /** @brief Protobuf metadata or frame fields could not be decoded. */
  DecodeError,

  /** @brief Response or request metadata could not be encoded. */
  EncodeError,
};

}  // namespace xrpc
