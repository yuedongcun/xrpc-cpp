#include "server/connection_backpressure.h"

namespace xrpc {

auto MakeConnectionBackpressureLimits(std::size_t max_inflight, std::size_t max_write_queue_bytes)
    -> StatusOr<ConnectionBackpressureLimits> {
  if (max_inflight == 0) {
    return StatusOr<ConnectionBackpressureLimits>(
        Status{StatusCode::InvalidArgument, "RpcServer max_inflight_per_connection must be greater than 0"});
  }
  if (max_write_queue_bytes == 0) {
    return StatusOr<ConnectionBackpressureLimits>(
        Status{StatusCode::InvalidArgument, "RpcServer max_write_queue_bytes_per_connection must be greater than 0"});
  }
  return StatusOr<ConnectionBackpressureLimits>(ConnectionBackpressureLimits{
      .max_inflight_ = max_inflight,
      .max_write_queue_bytes_ = max_write_queue_bytes,
  });
}

}  // namespace xrpc
