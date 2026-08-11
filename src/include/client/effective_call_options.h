#pragma once

#include <chrono>
#include <optional>
#include <string>

#include <xrpc/call_options.h>

namespace xrpc {

/**
 * @brief Resolved call options used by transports and retry loops.
 *
 * `deadline_` is derived once so retry attempts across endpoints share the original timeout budget. `timeout_` keeps
 * the effective duration for APIs that need a relative timeout, while transports use `deadline_` to avoid extending a
 * call during failover.
 */
struct EffectiveCallOptions {
  /** @brief Effective timeout after applying the client default. Zero means no deadline. */
  std::chrono::milliseconds timeout_{0};

  /** @brief Absolute deadline when the call has a timeout. */
  std::optional<std::chrono::steady_clock::time_point> deadline_;

  /** @brief Sticky routing key copied from public call options. */
  std::string sticky_key_;
};

}  // namespace xrpc
