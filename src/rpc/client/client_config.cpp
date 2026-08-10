#include "rpc/client/client_config.h"

#include "rpc/xrpc_exception.h"

namespace xrpc {

/**
 * @brief Validates public client options before constructing runtime components.
 *
 * @param options User-facing client options copied from the public `RpcClient` constructor.
 * @return Protocol limits derived from the validated payload-size option.
 * @throws ConfigException when any required option is missing or invalid.
 */
auto ValidateClientOptions(const RpcClientOptions &options) -> ProtocolLimits {
  if (options.target_.empty()) {
    throw ConfigException("RpcClient target must not be empty");
  }
  if (options.consul_address_.empty()) {
    throw ConfigException("RpcClient consul_address must not be empty");
  }
  if (options.discovery_refresh_interval_ < std::chrono::milliseconds::zero()) {
    throw ConfigException("RpcClient discovery_refresh_interval must not be negative");
  }
  if (options.timeout_ < std::chrono::milliseconds::zero()) {
    throw ConfigException("RpcClient timeout must not be negative");
  }
  if (options.max_inflight_per_endpoint_ == 0) {
    throw ConfigException("RpcClient max_inflight_per_endpoint must be greater than 0");
  }
  return MakeProtocolLimits(options.max_payload_size_);
}

/**
 * @brief Checks per-call overrides before they are merged with client defaults.
 *
 * @param options User-provided timeout and sticky-key overrides.
 * @return `Status::Ok()` when the overrides are usable, otherwise an invalid-argument status.
 */
auto ValidateCallOptions(const CallOptions &options) -> Status {
  if (options.timeout_ < std::chrono::milliseconds::zero()) {
    return {StatusCode::InvalidArgument, "CallOptions timeout must not be negative"};
  }
  return Status::Ok();
}

/**
 * @brief Merges per-call overrides with the normalized client defaults.
 *
 * A zero timeout inherits the client default. A positive effective timeout is converted into an
 * absolute steady-clock deadline so lower layers can test expiry without recomputing durations.
 *
 * @param default_timeout Client-wide timeout used when the call has no override.
 * @param options Per-call overrides validated by `ValidateCallOptions()`.
 * @return Effective options used by channel routing and transport execution.
 */
auto ResolveCallOptions(std::chrono::milliseconds default_timeout, const CallOptions &options) -> EffectiveCallOptions {
  EffectiveCallOptions effective_options;
  effective_options.timeout_ =
      options.timeout_ > std::chrono::milliseconds::zero() ? options.timeout_ : default_timeout;
  if (effective_options.timeout_ > std::chrono::milliseconds::zero()) {
    effective_options.deadline_ = std::chrono::steady_clock::now() + effective_options.timeout_;
  }
  effective_options.sticky_key_ = options.sticky_key_;
  return effective_options;
}

}  // namespace xrpc
