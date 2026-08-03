#pragma once

#include <cstdint>

namespace xrpc {

/**
 * @brief First reason recorded when a server-side connection closes.
 *
 * The value is diagnostic. `TcpConnection` records the first reason and keeps it stable so tests and metrics can
 * distinguish protocol failures from peer EOF, socket errors, backpressure, and idle cleanup.
 */
enum class ConnectionCloseReason : std::uint8_t {
  /** @brief Connection has not been closed yet. */
  None = 0,

  /** @brief Peer performed an orderly read-side close. */
  PeerClosed,

  /** @brief Protocol decoder rejected the byte stream. */
  ProtocolError,

  /** @brief Socket read, write, or cancellation failed. */
  SocketError,

  /** @brief Connection exceeded an in-flight or write-queue resource guard. */
  Backpressure,

  /** @brief Idle timer fired while no work was pending. */
  IdleTimeout,
};

}  // namespace xrpc
