#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace xrpc {

enum class MessageType : std::uint8_t {
  Request = 1,
  Response = 2,
  Heartbeat = 3,
  HeartbeatAck = 4,
};

struct FixedHeader {
  static constexpr uint32_t MAGIC = 0x58525043;

  static constexpr uint8_t VERSION = 1;

  static constexpr size_t SIZE = 24;

  uint32_t magic_ = MAGIC;

  uint8_t version_ = VERSION;

  MessageType message_type_ = MessageType::Request;

  uint16_t flags_ = 0;

  uint32_t header_len_ = 0;

  uint32_t payload_len_ = 0;

  uint64_t request_id_ = 0;

  static auto Encode(const FixedHeader &hdr) -> std::string;

  static void EncodeTo(const FixedHeader &hdr, char *buffer);

  static auto Decode(std::string_view buf) -> std::optional<FixedHeader>;
};

}  // namespace xrpc
