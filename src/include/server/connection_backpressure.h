/**
 * @file server_backpressure.h
 * @brief Defines per-connection server backpressure limits.
 */

#pragma once

#include <cstddef>

/**
 * @brief Limits resource usage for a single server connection.
 *
 * The limits bound the number of in-flight RPCs and the amount of response
 * data waiting to be written for one connection.
 */
namespace xrpc {

struct ConnectionBackpressureLimits {
  std::size_t max_inflight_;

  std::size_t max_write_queue_bytes_;
};

}  // namespace xrpc
