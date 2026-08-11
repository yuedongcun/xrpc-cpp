#pragma once

#include <chrono>

#include <xrpc/rpc_client.h>
#include <xrpc/status.h>

#include "protocol/frame_codec.h"
#include "client/effective_call_options.h"

namespace xrpc {

/**
 * @brief Validates client-wide options before runtime construction.
 *
 * @param options Public client options.
 * @return Protocol limits derived from the validated payload-size option.
 */
[[nodiscard]] auto ValidateClientOptions(const RpcClientOptions &options) -> ProtocolLimits;

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
[[nodiscard]] auto ResolveCallOptions(std::chrono::milliseconds default_timeout, const CallOptions &options)
    -> EffectiveCallOptions;

}  // namespace xrpc
