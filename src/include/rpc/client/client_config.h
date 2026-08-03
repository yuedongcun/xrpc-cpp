#pragma once

#include <chrono>
#include <cstddef>
#include <string>

#include <xrpc/rpc_client.h>
#include <xrpc/status.h>

#include "protocol/frame_codec.h"
#include "rpc/client/effective_call_options.h"

namespace xrpc {

/**
 * @brief Internal client configuration after target parsing and default resolution.
 *
 * Public options are copied into this normalized form before the runtime creates resolver and channel objects. Values
 * stored here are stable for the lifetime of `RpcClient::ClientRuntime`.
 */
struct ClientConfig {
  /** @brief Original resolver target or static endpoint target. */
  std::string target_;

  /** @brief Consul agent address used by Consul resolver targets. */
  std::string consul_address_;

  /** @brief Discovery refresh interval after the first resolver snapshot. */
  std::chrono::milliseconds discovery_refresh_interval_{0};

  /** @brief Default timeout used when `CallOptions::timeout_` is zero. */
  std::chrono::milliseconds default_timeout_{0};

  /** @brief Protocol limits derived from public payload-size options. */
  ProtocolLimits protocol_limits_;

  /** @brief Per-endpoint cap for pending calls in one transport. */
  std::size_t max_inflight_per_endpoint_ = 1024;
};

/**
 * @brief Validates public client options and converts them to internal configuration.
 *
 * @param options Public client options.
 * @return Normalized client configuration.
 */
[[nodiscard]] auto NormalizeClientOptions(const RpcClientOptions &options) -> ClientConfig;

/**
 * @brief Rejects per-call values that cannot be enforced safely.
 *
 * Negative timeouts are invalid because transports derive absolute deadlines from these values.
 *
 * @param options Public per-call options.
 * @return `Status::Ok()` when the options are usable.
 */
[[nodiscard]] auto ValidateCallOptions(const CallOptions &options) -> Status;

/**
 * @brief Resolves per-call options against client defaults.
 *
 * The deadline is derived once so retries across endpoints share the original timeout budget.
 */
[[nodiscard]] auto ResolveCallOptions(const ClientConfig &config, const CallOptions &options) -> EffectiveCallOptions;

}  // namespace xrpc
