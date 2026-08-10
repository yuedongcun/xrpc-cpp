#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace xrpc {

/**
 * @brief Values stored in `FixedHeader::message_type_`.
 *
 * Heartbeat variants are reserved for protocol evolution. Request and response are the only message types accepted by
 * the current frame codec.
 */
enum class MessageType : std::uint8_t {
  Request = 1,
  Response = 2,
  Heartbeat = 3,
  HeartbeatAck = 4,
};

/**
 * @brief Fixed-size prefix that starts every XRPC wire frame.
 *
 * Numeric fields are encoded in network byte order. The fixed prefix is followed by `header_len_` bytes of protobuf
 * metadata and `payload_len_` bytes of opaque request or response body. Changing the size or field order changes the
 * wire protocol and must be treated as a compatibility decision.
 */
struct FixedHeader {
  /** @brief ASCII "XRPC" marker used to reject obviously invalid streams. */
  static constexpr uint32_t MAGIC = 0x58525043;  // "XRPC" in ASCII

  /** @brief Current wire protocol version understood by this codec. */
  static constexpr uint8_t VERSION = 1;

  /** @brief Serialized byte size of the fixed prefix. */
  static constexpr size_t SIZE = 24;

  /** @brief Magic marker. Must equal `MAGIC` for decoded frames. */
  uint32_t magic_ = MAGIC;

  /** @brief Wire protocol version. Must equal `VERSION` for decoded frames. */
  uint8_t version_ = VERSION;

  /** @brief Identifies whether the frame carries a request or response header. */
  MessageType message_type_ = MessageType::Request;

  /** @brief Reserved flag bits. Unknown non-zero values are currently rejected by the codec. */
  uint16_t flags_ = 0;

  /** @brief Size in bytes of the protobuf metadata header following this prefix. */
  uint32_t header_len_ = 0;

  /** @brief Size in bytes of the opaque payload following the protobuf header. */
  uint32_t payload_len_ = 0;

  /** @brief Request id used to match client responses with pending calls. */
  uint64_t request_id_ = 0;

  /** @return Serialized fixed header bytes. */
  static auto Encode(const FixedHeader &hdr) -> std::string;

  /**
   * @brief Serializes a fixed header into a caller-owned buffer.
   *
   * @param hdr Header to encode.
   * @param buffer Destination buffer with at least `SIZE` bytes.
   */
  static void EncodeTo(const FixedHeader &hdr, char *buffer);

  /**
   * @brief Decodes a fixed header from the first `SIZE` bytes of `buf`.
   *
   * @return The decoded header, or `std::nullopt` when `buf` is shorter than `SIZE` or has an invalid fixed prefix.
   */
  static auto Decode(std::string_view buf) -> std::optional<FixedHeader>;
};

}  // namespace xrpc
