/**
 * @file frame_header.h
 * @brief Defines the fixed 24-byte prefix of every xRPC wire frame.
 *
 * All multi-byte integers use network byte order.
 *
 * Byte layout:
 *
 *   0        4 5 6       8            12           16           24
 *   +---------+-+-+-------+-------------+------------+------------+
 *   |  magic  |v|t| flags |metadata_size|payload_size| request_id |
 *   +---------+-+-+-------+-------------+------------+------------+
 *
 * The frame header is followed by Protobuf metadata and an opaque serialized
 * user payload:
 *
 *   FrameHeader | Protobuf metadata | payload
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace xrpc {

enum class MessageType : std::uint8_t {
  Request = 1,
  Response = 2,
};

/** @brief Host representation of the fixed wire-frame prefix. */
struct FrameHeader {
  /** ASCII "XRPC" encoded as a 32-bit wire marker. */
  static constexpr uint32_t MAGIC = 0x58525043;

  /** Current xRPC wire-format version. */
  static constexpr uint8_t VERSION = 1;

  /** Encoded size of the fixed prefix in bytes. */
  static constexpr size_t SIZE = 24;

  uint32_t magic_ = MAGIC;

  uint8_t version_ = VERSION;

  MessageType message_type_ = MessageType::Request;

  uint16_t flags_ = 0;

  /** Size of the following Protobuf metadata, excluding this fixed prefix. */
  uint32_t metadata_size_ = 0;

  /** Size of the following serialized user payload. */
  uint32_t payload_size_ = 0;

  /** Correlates a response with its multiplexed request. */
  uint64_t request_id_ = 0;

  /** @brief Encodes one frame header into a caller-provided buffer of at least `SIZE` bytes. */
  static void EncodeTo(const FrameHeader &header, char *buffer);

  /**
   * @brief Decodes the fixed prefix without validating version or message type.
   *
   * Returns no value when fewer than `SIZE` bytes are available or the magic
   * marker is invalid. Higher-level protocol validation belongs to FrameCodec.
   */
  static auto Decode(std::string_view buf) -> std::optional<FrameHeader>;
};

}  // namespace xrpc
