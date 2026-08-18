#pragma once

#include <cstddef>

namespace xrpc {

struct ConnectionBackpressureLimits {
  std::size_t max_inflight_ = 128;

  std::size_t max_write_queue_bytes_ = 8U * 1024U * 1024U;
};

}  // namespace xrpc
