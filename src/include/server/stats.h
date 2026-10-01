#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "io/uring/stats.h"

namespace xrpc {

struct WorkerQueueStatsSnapshot {
  std::size_t queued_logical_jobs_ = 0;
  std::size_t pending_logical_jobs_ = 0;  // Queued or executing on this worker.
  std::size_t queued_logical_jobs_peak_ = 0;
};

struct WorkerPoolStatsSnapshot {
  // Existing admission count: includes reservations, queued and executing RPCs.
  std::size_t pending_logical_jobs_ = 0;
  std::uint64_t window_id_ = 0;
  std::vector<WorkerQueueStatsSnapshot> queues_;  // Independently locked samples.
};

struct ConnectionLoopStatsSnapshot {
  io::UringStatsSnapshot uring_;
  std::size_t live_connections_ = 0;
  // Includes responses queued or currently sending; excludes encoded worker output in transit.
  std::size_t pending_write_bytes_ = 0;
  std::size_t pending_write_bytes_peak_ = 0;  // Exact simultaneous sum within this loop.
  std::uint64_t worker_results_enqueued_ = 0;
  std::uint64_t worker_results_drained_ = 0;
  std::uint64_t worker_result_drain_callbacks_ = 0;
  std::uint64_t worker_result_overflow_fallbacks_ = 0;
};

struct ServerStatsSnapshot {
  std::vector<ConnectionLoopStatsSnapshot> loops_;
  WorkerPoolStatsSnapshot worker_pool_;
};

}  // namespace xrpc
