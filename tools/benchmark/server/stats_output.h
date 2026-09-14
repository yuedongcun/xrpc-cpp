#pragma once

#include <string>

#include <xrpc/rpc_server.h>

namespace xrpc::benchmark {

// Benchmark-only serialization and atomic publication of an internal snapshot.
auto WriteStatsSnapshot(RpcServer &server, const std::string &path, bool start_window = false) -> Status;

}  // namespace xrpc::benchmark
