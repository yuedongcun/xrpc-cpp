#pragma once

#include <chrono>
#include <optional>
#include <string>

#include <xrpc/rpc_options.h>

namespace xrpc {

struct EffectiveCallOptions {
  std::chrono::milliseconds timeout_{0};

  std::optional<std::chrono::steady_clock::time_point> deadline_;

  std::string sticky_key_;
};

}  // namespace xrpc
