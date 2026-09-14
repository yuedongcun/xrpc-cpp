#pragma once

#include <xrpc/rpc_server.h>

#include "io/uring/buffer_pool.h"

namespace xrpc {

// Internal factory for benchmark pool sizing; public server options keep defaults.
struct ServerRuntimeAccess {
  [[nodiscard]] static auto CreateWithBufferPool(const RpcServerOptions &options, io::UringBufferPoolConfig buffer_pool)
      -> StatusOr<RpcServer>;
};

}  // namespace xrpc
