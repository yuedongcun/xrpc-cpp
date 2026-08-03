#pragma once

#include <chrono>
#include <cstddef>
#include <utility>
#include <vector>

namespace xrpc::benchmark {

// Aggregated client-side benchmark output. Latency percentiles are computed
// from completed calls recorded by BenchmarkRecorder.
struct BenchmarkStats {
  std::size_t total_calls_ = 0;
  std::size_t success_calls_ = 0;
  std::size_t failed_calls_ = 0;
  std::chrono::nanoseconds total_latency_{0};
  std::chrono::nanoseconds p50_latency_{0};
  std::chrono::nanoseconds p95_latency_{0};
  std::chrono::nanoseconds p99_latency_{0};
  double qps_ = 0.0;
};

// Not thread-safe by itself; benchmark workers should aggregate through their
// own recorder and merge at a higher level.
class BenchmarkRecorder final {
 public:
  void RecordSuccess(std::chrono::nanoseconds latency);
  void RecordFailure(std::chrono::nanoseconds latency);
  void MergeFrom(BenchmarkRecorder &&other);
  [[nodiscard]] auto Finalize(std::chrono::nanoseconds wall_time) -> BenchmarkStats;

 private:
  std::vector<std::chrono::nanoseconds> latencies_;
  std::size_t success_calls_ = 0;
  std::size_t failed_calls_ = 0;
  std::chrono::nanoseconds total_latency_{0};
};

void PrintStats(const BenchmarkStats &stats);

}  // namespace xrpc::benchmark
