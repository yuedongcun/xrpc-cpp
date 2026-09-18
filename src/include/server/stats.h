#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "io/uring/stats.h"

namespace xrpc {

struct WorkerQueueStatsSnapshot {
  std::size_t queued_batches_ = 0;
  std::size_t queued_logical_jobs_ = 0;
  std::size_t pending_batches_ = 0;       // Queued or executing; sampled from existing atomic.
  std::size_t pending_logical_jobs_ = 0;  // Queued or executing on this worker.
  std::size_t queued_batches_peak_ = 0;
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
};

struct ServerStatsSnapshot {
  std::vector<ConnectionLoopStatsSnapshot> loops_;
  WorkerPoolStatsSnapshot worker_pool_;
};

}  // namespace xrpc
