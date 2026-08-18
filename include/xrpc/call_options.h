#pragma once

#include <chrono>
#include <string>

namespace xrpc {

struct CallOptions {
  std::chrono::milliseconds timeout_{0};

  std::string sticky_key_;
};

}  // namespace xrpc
