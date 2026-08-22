/**
 * @file frame_header.cpp
 * @brief Implements frame-header serialization with explicit offsets and network byte order.
 */

#include "protocol/frame_header.h"

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>

namespace {

// POSIX provides 16- and 32-bit conversions but no universally available
// 64-bit variant, so request ids are converted from two 32-bit halves.
auto HostToNetwork64(uint64_t x) -> uint64_t {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  return x;
#else
  return (static_cast<uint64_t>(htonl(static_cast<uint32_t>(x & 0xFFFFFFFF))) << 32) |
         htonl(static_cast<uint32_t>(x >> 32));
#endif
}

auto NetworkToHost64(uint64_t x) -> uint64_t { return HostToNetwork64(x); }

}  // namespace

namespace xrpc {

void FrameHeader::EncodeTo(const FrameHeader &header, char *buffer) {
  uint32_t network_magic = htonl(header.magic_);
  uint32_t network_metadata_size = htonl(header.metadata_size_);
  uint32_t network_payload_size = htonl(header.payload_size_);
  uint64_t network_request_id = HostToNetwork64(header.request_id_);

  // memcpy keeps the wire access valid even when the destination is unaligned.
  std::memcpy(buffer, &network_magic, 4);
  buffer[4] = header.version_;
  buffer[5] = static_cast<uint8_t>(header.message_type_);
  uint16_t network_flags = htons(header.flags_);
  std::memcpy(&buffer[6], &network_flags, 2);
  std::memcpy(&buffer[8], &network_metadata_size, 4);
  std::memcpy(&buffer[12], &network_payload_size, 4);
  std::memcpy(&buffer[16], &network_request_id, 8);
}

auto FrameHeader::Decode(std::string_view buf) -> std::optional<FrameHeader> {
  if (buf.size() < SIZE) {
    return std::nullopt;
  }

  FrameHeader header;
  uint32_t network_magic;
  std::memcpy(&network_magic, buf.data(), 4);
  header.magic_ = ntohl(network_magic);
  if (header.magic_ != MAGIC) {
    return std::nullopt;
  }

  header.version_ = static_cast<uint8_t>(buf[4]);
  header.message_type_ = static_cast<MessageType>(buf[5]);

  uint16_t network_flags;
  std::memcpy(&network_flags, &buf[6], 2);
  header.flags_ = ntohs(network_flags);

  uint32_t network_metadata_size;
  std::memcpy(&network_metadata_size, &buf[8], 4);
  header.metadata_size_ = ntohl(network_metadata_size);

  uint32_t network_payload_size;
  std::memcpy(&network_payload_size, &buf[12], 4);
  header.payload_size_ = ntohl(network_payload_size);

  uint64_t network_request_id;
  std::memcpy(&network_request_id, &buf[16], 8);
  header.request_id_ = NetworkToHost64(network_request_id);

  return header;
}

}  // namespace xrpc
