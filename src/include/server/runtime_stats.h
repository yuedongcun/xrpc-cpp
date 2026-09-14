#pragma once

#include <vector>

#include <xrpc/status.h>

#include "server/stats.h"

namespace xrpc {

class RpcServer;

// Internal control-thread access. Connection loops are sampled independently;
// this is not an atomic snapshot of the whole server. Accept-loop stats excluded.
struct ServerStatsAccess {
  [[nodiscard]] static auto Snapshot(RpcServer &server, bool start_window = false) -> StatusOr<ServerStatsSnapshot>;
};

}  // namespace xrpc
