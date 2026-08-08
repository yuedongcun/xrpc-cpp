#include "benchmark_stats.h"

#include <algorithm>
#include <cstdio>
#include <iterator>

namespace xrpc::benchmark {

namespace {

auto PercentileIndex(std::size_t size, double fraction) -> std::size_t {
  if (size == 0) {
    return 0;
  }
  const double raw = static_cast<double>(size - 1) * fraction;
  return static_cast<std::size_t>(raw);
}

auto AtPercentile(const std::vector<std::chrono::nanoseconds> &sorted_latencies, double fraction)
    -> std::chrono::nanoseconds {
  if (sorted_latencies.empty()) {
    return std::chrono::nanoseconds(0);
  }
  const std::size_t idx = PercentileIndex(sorted_latencies.size(), fraction);
  return sorted_latencies[idx];
}

}  // namespace

void BenchmarkRecorder::RecordSuccess(std::chrono::nanoseconds latency) {
  ++success_calls_;
  total_latency_ += latency;
  latencies_.push_back(latency);
}

void BenchmarkRecorder::RecordFailure(std::chrono::nanoseconds latency) {
  ++failed_calls_;
  total_latency_ += latency;
  latencies_.push_back(latency);
}

void BenchmarkRecorder::MergeFrom(BenchmarkRecorder &&other) {
  success_calls_ += other.success_calls_;
  failed_calls_ += other.failed_calls_;
  total_latency_ += other.total_latency_;
  if (latencies_.empty()) {
    latencies_ = std::move(other.latencies_);
    return;
  }
  latencies_.insert(latencies_.end(), std::make_move_iterator(other.latencies_.begin()),
                    std::make_move_iterator(other.latencies_.end()));
}

auto BenchmarkRecorder::Finalize(std::chrono::nanoseconds wall_time) -> BenchmarkStats {
  BenchmarkStats stats;
  stats.success_calls_ = success_calls_;
  stats.failed_calls_ = failed_calls_;
  stats.total_calls_ = success_calls_ + failed_calls_;
  stats.total_latency_ = total_latency_;

  std::ranges::sort(latencies_);
  stats.p50_latency_ = AtPercentile(latencies_, 0.50);
  stats.p95_latency_ = AtPercentile(latencies_, 0.95);
  stats.p99_latency_ = AtPercentile(latencies_, 0.99);

  if (wall_time > std::chrono::nanoseconds::zero()) {
    const double seconds = static_cast<double>(wall_time.count()) / 1'000'000'000.0;
    stats.qps_ = static_cast<double>(stats.total_calls_) / seconds;
  }
  return stats;
}

void PrintStats(const BenchmarkStats &stats) {
  const double avg_us = stats.total_calls_ == 0 ? 0.0
                                                : static_cast<double>(stats.total_latency_.count()) /
                                                      static_cast<double>(stats.total_calls_) / 1000.0;
  const double p50_us = static_cast<double>(stats.p50_latency_.count()) / 1000.0;
  const double p95_us = static_cast<double>(stats.p95_latency_.count()) / 1000.0;
  const double p99_us = static_cast<double>(stats.p99_latency_.count()) / 1000.0;

  std::printf("total_calls=%zu success=%zu failed=%zu\n", stats.total_calls_, stats.success_calls_,
              stats.failed_calls_);
  std::printf("qps=%.2f avg_us=%.2f p50_us=%.2f p95_us=%.2f p99_us=%.2f\n", stats.qps_, avg_us, p50_us, p95_us, p99_us);
}

}  // namespace xrpc::benchmark
