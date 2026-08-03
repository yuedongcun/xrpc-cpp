#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace xrpc {

/**
 * @brief Per-connection safety limits enforced before dispatch and write queue growth.
 *
 * The global worker queue limit lives in `ThreadPoolExecutor` because it spans all connections.
 */
struct ServerBackpressureLimits {
  /** @brief Maximum handler jobs one connection may have in flight. */
  std::size_t max_inflight_per_connection_ = 128;

  /** @brief Maximum encoded response bytes one connection may queue before closure. */
  std::size_t max_write_queue_bytes_per_connection_ = 8U * 1024U * 1024U;
};

/**
 * @brief Snapshot of relaxed backpressure diagnostic counters.
 *
 * These values do not participate in synchronization and may be approximate under concurrent reads.
 */
struct ServerBackpressureSnapshot {
  std::uint64_t rejected_by_inflight_limit_ = 0;
  std::uint64_t rejected_by_global_pending_limit_ = 0;
  std::uint64_t closed_by_write_queue_high_watermark_ = 0;
  std::uint64_t max_observed_inflight_ = 0;
  std::uint64_t max_observed_write_queue_bytes_ = 0;
};

/**
 * @brief Atomic diagnostic counters for server-side backpressure decisions.
 *
 * All updates are relaxed because these counters are for diagnostics only. The actual backpressure decisions are made
 * from event-loop-owned connection state and worker-pool reservation results.
 */
class ServerBackpressureStats final {
 public:
  /** @brief Records a request rejected by the per-connection in-flight limit. */
  void RecordInflightRejection() { rejected_by_inflight_limit_.fetch_add(1, std::memory_order_relaxed); }

  /** @brief Records a request rejected by the global worker pending-job limit. */
  void RecordGlobalPendingRejection() { rejected_by_global_pending_limit_.fetch_add(1, std::memory_order_relaxed); }

  /** @brief Records a connection closed by write-queue high watermark. */
  void RecordWriteQueueClosure() { closed_by_write_queue_high_watermark_.fetch_add(1, std::memory_order_relaxed); }

  /** @brief Observes a current in-flight count and updates the high-water mark. */
  void ObserveInflight(std::size_t value) { ObserveMaximum(max_observed_inflight_, value); }

  /** @brief Observes a current queued-byte count and updates the high-water mark. */
  void ObserveWriteQueueBytes(std::size_t value) { ObserveMaximum(max_observed_write_queue_bytes_, value); }

  /** @return Point-in-time snapshot of relaxed diagnostic counters. */
  [[nodiscard]] auto Snapshot() const -> ServerBackpressureSnapshot {
    return ServerBackpressureSnapshot{
        .rejected_by_inflight_limit_ = rejected_by_inflight_limit_.load(std::memory_order_relaxed),
        .rejected_by_global_pending_limit_ = rejected_by_global_pending_limit_.load(std::memory_order_relaxed),
        .closed_by_write_queue_high_watermark_ = closed_by_write_queue_high_watermark_.load(std::memory_order_relaxed),
        .max_observed_inflight_ = max_observed_inflight_.load(std::memory_order_relaxed),
        .max_observed_write_queue_bytes_ = max_observed_write_queue_bytes_.load(std::memory_order_relaxed),
    };
  }

 private:
  /** @brief Atomically raises a relaxed high-water counter when `value` is larger. */
  static void ObserveMaximum(std::atomic<std::uint64_t> &maximum, std::size_t value) {
    std::uint64_t observed = maximum.load(std::memory_order_relaxed);
    const auto candidate = static_cast<std::uint64_t>(value);
    while (observed < candidate &&
           !maximum.compare_exchange_weak(observed, candidate, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
  }

  std::atomic<std::uint64_t> rejected_by_inflight_limit_{0};
  std::atomic<std::uint64_t> rejected_by_global_pending_limit_{0};
  std::atomic<std::uint64_t> closed_by_write_queue_high_watermark_{0};
  std::atomic<std::uint64_t> max_observed_inflight_{0};
  std::atomic<std::uint64_t> max_observed_write_queue_bytes_{0};
};

}  // namespace xrpc
