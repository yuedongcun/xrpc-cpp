#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <exception>
#include <string>
#include <thread>

#include "common/benchmark_config.h"
#include "common/benchmark_server.h"

namespace {

std::atomic<bool> stop_requested{false};

void HandleSignal(int signal) {
  (void)signal;
  stop_requested.store(true, std::memory_order_relaxed);
}

}  // namespace

auto main(int argc, char **argv) -> int {
  try {
    xrpc::benchmark::BenchmarkServerConfig config = xrpc::benchmark::ParseBenchmarkServerConfig(argc, argv);
    xrpc::benchmark::BenchmarkServer server(config);

    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    server.Start(config.host_, config.port_);
    const std::string workload(xrpc::benchmark::ToString(config.workload_));
    std::printf(
        "ready host=%s port=%u workload=%s worker_threads=%zu connection_io_threads=%zu "
        "metrics_host=%s metrics_port=%u "
        "delay_us=%llu "
        "max_inflight_per_connection=%zu "
        "max_write_queue_bytes_per_connection=%zu max_pending_jobs_global=%zu\n",
        config.host_.c_str(), server.port(), workload.c_str(), config.server_options_.worker_threads_,
        config.server_options_.connection_io_threads_, config.metrics_host_.c_str(), config.metrics_port_,
        static_cast<unsigned long long>(config.server_delay_us_), config.server_options_.max_inflight_per_connection_,
        config.server_options_.max_write_queue_bytes_per_connection_, config.server_options_.max_pending_jobs_global_);
    std::fflush(stdout);

    while (!stop_requested.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    server.Stop();
    const xrpc::RpcServerStats stats = server.stats();
    std::printf(
        "backpressure rejected_inflight=%llu rejected_global_pending=%llu closed_write_queue=%llu "
        "max_inflight=%llu max_write_queue_bytes=%llu\n",
        static_cast<unsigned long long>(stats.rejected_by_inflight_limit_),
        static_cast<unsigned long long>(stats.rejected_by_global_pending_limit_),
        static_cast<unsigned long long>(stats.closed_by_write_queue_high_watermark_),
        static_cast<unsigned long long>(stats.max_observed_inflight_),
        static_cast<unsigned long long>(stats.max_observed_write_queue_bytes_));
    std::printf("worker_queue rejected=%llu max_depth=%llu\n",
                static_cast<unsigned long long>(stats.worker_jobs_rejected_),
                static_cast<unsigned long long>(stats.max_observed_worker_queue_depth_));
    std::fflush(stdout);
    return 0;
  } catch (const std::exception &ex) {
    std::fprintf(stderr, "%s\n%s\n", ex.what(), xrpc::benchmark::ServerUsage(argv[0]).c_str());
    return 1;
  }
}
