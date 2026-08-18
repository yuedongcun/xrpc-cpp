#include "client/client_config.h"

#include "common/xrpc_exception.h"

namespace xrpc {

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

auto ValidateCallOptions(const CallOptions &options) -> Status {
  if (options.timeout_ < std::chrono::milliseconds::zero()) {
    return {StatusCode::InvalidArgument, "CallOptions timeout must not be negative"};
  }
  return Status::Ok();
}

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
