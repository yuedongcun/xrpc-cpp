#pragma once

#include "common/benchmark_config.h"
#include "common/benchmark_stats.h"

namespace xrpc::benchmark {

[[nodiscard]] auto RunBenchmark(const BenchmarkConfig &config) -> BenchmarkStats;

}  // namespace xrpc::benchmark
