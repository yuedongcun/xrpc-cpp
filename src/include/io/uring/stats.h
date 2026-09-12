#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

namespace xrpc::io {

// Lifetime counters owned by the Run thread. Snapshot differences describe a
// measurement interval; these counters are never reset.
struct UringCounters {
  // Count only operations staged for submission, not awaitable construction.
  std::uint64_t prepared_accept_sqes_ = 0;
  std::uint64_t prepared_recv_sqes_ = 0;            // Includes ordinary and provided-buffer recv.
  std::uint64_t prepared_multishot_recv_sqes_ = 0;  // Subset of prepared_recv_sqes_.
  std::uint64_t prepared_send_sqes_ = 0;
  std::uint64_t prepared_cancel_sqes_ = 0;
  std::uint64_t prepared_wakeup_sqes_ = 0;
  // liburing calls, including EINTR retries; not a count of system calls.
  std::uint64_t submit_calls_ = 0;
  std::uint64_t submitted_sqes_ = 0;  // Sum of successful submission return values.
  std::uint64_t recv_cqes_ = 0;       // Includes data, EOF, errors and cancellation.
  std::uint64_t received_bytes_ = 0;  // Positive receive results only.
  std::uint64_t provided_buffer_enobufs_ = 0;
};

struct UringWindowPeaks {
  std::size_t staged_operations_ = 0;  // Updated on every staging insertion.
  std::size_t cq_ready_sampled_ = 0;   // Sampled before each CQE batch and at snapshots.
};

struct BufferPoolStatsSnapshot {
  std::uint64_t acquires_ = 0;
  std::uint64_t returns_ = 0;
  std::size_t capacity_ = 0;
  // Only buffers leased to user space; excludes kernel-selected buffers whose
  // CQEs have not yet been consumed. capacity - outstanding is NOT free capacity.
  std::size_t outstanding_leases_ = 0;
  std::size_t outstanding_leases_peak_ = 0;
};

struct UringStatsSnapshot {
  UringCounters counters_;
  std::size_t staged_operations_ = 0;
  // Includes staged receives; ends only when the receive's final CQE is consumed.
  // This is an operation count, not the number of occupied SQ slots.
  std::size_t active_recv_requests_ = 0;
  std::size_t cq_ready_ = 0;
  std::uint64_t window_id_ = 0;
  UringWindowPeaks peaks_;
  std::optional<BufferPoolStatsSnapshot> buffer_pool_;
};

}  // namespace xrpc::io
