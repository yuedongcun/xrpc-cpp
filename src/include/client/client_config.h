#pragma once

#include <chrono>

#include <xrpc/rpc_client.h>
#include <xrpc/status.h>

#include "client/effective_call_options.h"
#include "protocol/frame_codec.h"

namespace xrpc {

[[nodiscard]] auto ValidateClientOptions(const RpcClientOptions &options) -> ProtocolLimits;

[[nodiscard]] auto ValidateCallOptions(const CallOptions &options) -> Status;

[[nodiscard]] auto ResolveCallOptions(std::chrono::milliseconds default_timeout, const CallOptions &options)
    -> EffectiveCallOptions;

}  // namespace xrpc
