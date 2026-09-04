#include "server/connection_config.h"

#include <utility>

namespace xrpc {

auto MakeConnectionIoConfig(std::size_t threads, std::size_t max_inflight, std::size_t max_write_queue_bytes,
                            std::size_t max_payload_size) -> StatusOr<ConnectionIoConfig> {
  if (threads == 0) {
    return StatusOr<ConnectionIoConfig>(
        Status{StatusCode::InvalidArgument, "RpcServer connection_io_threads must be greater than 0"});
  }
  StatusOr<ConnectionBackpressureLimits> backpressure =
      MakeConnectionBackpressureLimits(max_inflight, max_write_queue_bytes);
  if (!backpressure.ok()) {
    return StatusOr<ConnectionIoConfig>(backpressure.status());
  }
  StatusOr<ProtocolLimits> protocol = MakeProtocolLimits(max_payload_size);
  if (!protocol.ok()) {
    return StatusOr<ConnectionIoConfig>(protocol.status());
  }
  return StatusOr<ConnectionIoConfig>(ConnectionIoConfig{
      .threads_ = threads,
      .connection_ =
          ServerConnectionConfig{
              .limits_ = std::move(backpressure).value(),
              .protocol_limits_ = std::move(protocol).value(),
          },
  });
}

}  // namespace xrpc
