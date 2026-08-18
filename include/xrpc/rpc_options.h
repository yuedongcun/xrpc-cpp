#pragma once

#include <chrono>
#include <cstddef>
#include <string>

namespace xrpc {

inline constexpr std::size_t DEFAULT_MAX_PAYLOAD_SIZE = 4U * 1024U * 1024U;

struct CallOptions {
  std::chrono::milliseconds timeout_{0};
  std::string sticky_key_;
};

}  // namespace xrpc
