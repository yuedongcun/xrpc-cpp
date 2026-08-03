#include <cstdio>
#include <exception>
#include <string>

#include "common/benchmark_client.h"
#include "common/benchmark_config.h"
#include "common/benchmark_stats.h"

auto main(int argc, char **argv) -> int {
  try {
    xrpc::benchmark::BenchmarkConfig config = xrpc::benchmark::ParseBenchmarkClientConfig(argc, argv);
    const std::string workload(xrpc::benchmark::ToString(config.workload_));
    const std::string client_mode(xrpc::benchmark::ToString(config.client_mode_));

    std::printf(
        "workload=%s client_mode=%s host=%s "
        "port=%u metrics_host=%s metrics_port=%u "
        "duration_s=%llu payload_size=%zu client_threads=%zu "
        "firehose_connections=%zu firehose_inflight=%zu firehose_io_threads=%zu",
        workload.c_str(), client_mode.c_str(), config.host_.c_str(), config.port_, config.metrics_host_.c_str(),
        config.metrics_port_, static_cast<unsigned long long>(config.duration_s_), config.payload_size_,
        config.client_threads_, config.firehose_connections_, config.firehose_inflight_, config.firehose_io_threads_);
    std::printf("\n");

    const xrpc::benchmark::BenchmarkStats stats = xrpc::benchmark::RunBenchmark(config);
    xrpc::benchmark::PrintStats(stats);
    return 0;
  } catch (const std::exception &ex) {
    std::fprintf(stderr, "%s\n%s\n", ex.what(), xrpc::benchmark::ClientUsage(argv[0]).c_str());
    return 1;
  }
}
