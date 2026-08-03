#pragma once

#include <chrono>
#include <string>

namespace xrpc {

/**
 * @brief Per-call overrides layered on top of `RpcClientOptions`.
 *
 * A zero timeout inherits the client default. A non-empty sticky key asks the client to start routing from a stable
 * endpoint derived from that key; failover can still move the call before any bytes have been sent.
 */
struct CallOptions {
  /** @brief Per-call timeout. Zero means inherit the client default. */
  std::chrono::milliseconds timeout_{0};

  /** @brief Optional key used for deterministic sticky endpoint selection. */
  std::string sticky_key_;
};

}  // namespace xrpc
