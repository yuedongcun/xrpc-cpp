#pragma once

#include "common/benchmark_config.h"
#include "common/benchmark_stats.h"

namespace xrpc::benchmark {

// High-throughput benchmark client that maintains configured in-flight depth
// with epoll-managed TCP connections.
[[nodiscard]] auto RunFirehoseBenchmark(const BenchmarkConfig &config) -> BenchmarkStats;

}  // namespace xrpc::benchmark
