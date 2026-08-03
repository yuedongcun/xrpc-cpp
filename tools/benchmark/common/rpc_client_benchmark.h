#pragma once

#include "common/benchmark_config.h"
#include "common/benchmark_stats.h"

namespace xrpc::benchmark {

// Exercises the public typed RpcClient path with one shared client and
// synchronous calls issued concurrently by the configured worker threads.
[[nodiscard]] auto RunRpcClientBenchmark(const BenchmarkConfig &config) -> BenchmarkStats;

}  // namespace xrpc::benchmark
