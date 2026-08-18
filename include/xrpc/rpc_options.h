#pragma once

/**
 * @file rpc_options.h
 * @brief Shared call options and protocol limits for public RPC APIs.
 */

#include <chrono>
#include <cstddef>
#include <string>

namespace xrpc {

/** Maximum serialized request or response payload accepted by default. */
inline constexpr std::size_t DEFAULT_MAX_PAYLOAD_SIZE = 4U * 1024U * 1024U;

/**
 * @brief Per-call routing and deadline options.
 *
 * A zero timeout leaves deadline selection to `RpcClientOptions::timeout_`.
 * An empty sticky key uses normal round-robin endpoint selection.
 */
struct CallOptions {
  /** Optional per-call timeout. Zero selects the client's default timeout. */
  std::chrono::milliseconds timeout_{0};

  /** Stable routing key used to choose the initial endpoint for this call. */
  std::string sticky_key_;
};

}  // namespace xrpc
