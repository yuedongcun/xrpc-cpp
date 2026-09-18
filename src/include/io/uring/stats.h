#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

namespace xrpc::io {

struct UringCounters {
  std::uint64_t provided_buffer_enobufs_ = 0;
};

struct BufferPoolStatsSnapshot {
  std::size_t capacity_ = 0;
  std::size_t buffer_size_ = 0;
  // Only buffers leased to user space; excludes kernel-selected buffers whose
  // CQEs have not yet been consumed. capacity - outstanding is NOT free capacity.
  std::size_t outstanding_leases_ = 0;
  std::size_t outstanding_leases_peak_ = 0;
};

struct UringStatsSnapshot {
  UringCounters counters_;
  std::uint64_t window_id_ = 0;
  std::optional<BufferPoolStatsSnapshot> buffer_pool_;
};

}  // namespace xrpc::io
