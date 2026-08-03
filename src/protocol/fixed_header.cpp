#include "protocol/fixed_header.h"

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>

namespace {

/**
 * @brief Converts a 64-bit integer from host byte order to network byte order.
 *
 * The fixed header stores request ids as 64-bit values, while the portable socket byte-order helpers only cover 16-bit
 * and 32-bit integers. The implementation composes the 64-bit value from two network-order halves.
 */
auto HostToNetwork64(uint64_t x) -> uint64_t {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  return x;
#else
  return (static_cast<uint64_t>(htonl(static_cast<uint32_t>(x & 0xFFFFFFFF))) << 32) |
         htonl(static_cast<uint32_t>(x >> 32));
#endif
}

/**
 * @brief Converts a 64-bit integer from network byte order to host byte order.
 */
auto NetworkToHost64(uint64_t x) -> uint64_t { return HostToNetwork64(x); }

}  // namespace

namespace xrpc {

/**
 * @brief Encodes the fixed header into a new byte string.
 */
auto FixedHeader::Encode(const FixedHeader &hdr) -> std::string {
  std::string buf(SIZE, '\0');
  EncodeTo(hdr, buf.data());
  return buf;
}

/**
 * @brief Encodes the fixed header into caller-owned storage.
 *
 * The caller must provide at least `FixedHeader::SIZE` writable bytes. All multi-byte numeric fields are written in
 * network byte order so the wire format is stable across host architectures.
 */
void FixedHeader::EncodeTo(const FixedHeader &hdr, char *buffer) {
  uint32_t net_magic = htonl(hdr.magic_);
  uint32_t net_header_len = htonl(hdr.header_len_);
  uint32_t net_payload_len = htonl(hdr.payload_len_);
  uint64_t net_request_id = HostToNetwork64(hdr.request_id_);

  std::memcpy(buffer, &net_magic, 4);
  buffer[4] = hdr.version_;
  buffer[5] = static_cast<uint8_t>(hdr.message_type_);
  uint16_t net_flags = htons(hdr.flags_);
  std::memcpy(&buffer[6], &net_flags, 2);
  std::memcpy(&buffer[8], &net_header_len, 4);
  std::memcpy(&buffer[12], &net_payload_len, 4);
  std::memcpy(&buffer[16], &net_request_id, 8);
}

/**
 * @brief Decodes and validates one fixed header from a byte buffer.
 *
 * The fixed-header decoder validates the magic value and leaves higher-level message compatibility checks to
 * `FrameCodec`.
 */
auto FixedHeader::Decode(std::string_view buf) -> std::optional<FixedHeader> {
  if (buf.size() < SIZE) {
    return std::nullopt;
  }

  FixedHeader hdr;
  uint32_t net_magic;
  std::memcpy(&net_magic, buf.data(), 4);
  hdr.magic_ = ntohl(net_magic);
  if (hdr.magic_ != MAGIC) {
    return std::nullopt;
  }

  hdr.version_ = static_cast<uint8_t>(buf[4]);
  hdr.message_type_ = static_cast<MessageType>(buf[5]);

  uint16_t net_flags;
  std::memcpy(&net_flags, &buf[6], 2);
  hdr.flags_ = ntohs(net_flags);

  uint32_t net_header_len;
  std::memcpy(&net_header_len, &buf[8], 4);
  hdr.header_len_ = ntohl(net_header_len);

  uint32_t net_payload_len;
  std::memcpy(&net_payload_len, &buf[12], 4);
  hdr.payload_len_ = ntohl(net_payload_len);

  uint64_t net_request_id;
  std::memcpy(&net_request_id, &buf[16], 8);
  hdr.request_id_ = NetworkToHost64(net_request_id);

  return hdr;
}

}  // namespace xrpc
