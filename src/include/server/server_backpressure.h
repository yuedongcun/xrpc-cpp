#pragma once

#include <cstddef>

namespace xrpc {

/**
 * @brief Per-connection safety limits enforced before dispatch and write queue growth.
 *
 * The global worker queue limit lives in `ThreadPoolExecutor` because it spans all connections.
 */
struct ConnectionBackpressureLimits {
  /** @brief Maximum handler jobs one connection may have in flight. */
  std::size_t max_inflight_ = 128;

  /** @brief Maximum encoded response bytes one connection may queue before closure. */
  std::size_t max_write_queue_bytes_ = 8U * 1024U * 1024U;
};

}  // namespace xrpc
