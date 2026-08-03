#pragma once

#include <cstdint>

namespace xrpc {

/**
 * @brief Values stored in `FixedHeader::message_type_`.
 *
 * Heartbeat variants are reserved for protocol evolution and currently decode as unsupported. Request and response are
 * the only message types accepted by the v1 frame codec.
 */
enum class MessageType : uint8_t {
  /** @brief Client request frame. */
  Request = 1,

  /** @brief Server response frame. */
  Response = 2,

  /** @brief Reserved heartbeat frame. */
  Heartbeat = 3,

  /** @brief Reserved heartbeat acknowledgement frame. */
  HeartbeatAck = 4,
};

}  // namespace xrpc
