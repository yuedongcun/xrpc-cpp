#pragma once

#include <cstddef>

#include "protocol/frame_codec.h"
#include "server/connection_backpressure.h"

namespace xrpc {

struct ServerConnectionConfig final {
  ConnectionBackpressureLimits limits_;
  ProtocolLimits protocol_limits_;
};

struct ConnectionIoConfig final {
  std::size_t threads_;
  ServerConnectionConfig connection_;
};

[[nodiscard]] auto MakeConnectionIoConfig(std::size_t threads, std::size_t max_inflight,
                                          std::size_t max_write_queue_bytes, std::size_t max_payload_size)
    -> StatusOr<ConnectionIoConfig>;

}  // namespace xrpc
