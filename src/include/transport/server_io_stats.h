#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace xrpc {

/**
 * @brief Server I/O diagnostics used to understand queueing and send batching.
 *
 * These counters are relaxed and approximate under concurrent reads. They are intended for tests, metrics, and
 * performance analysis, not for synchronization.
 */
struct ServerIoStatsSnapshot {
  /** @brief Number of response frames enqueued for writing. */
  std::uint64_t write_frames_enqueued_ = 0;

  /** @brief Number of write-drain coroutines started. */
  std::uint64_t write_drains_started_ = 0;

  /** @brief Number of socket send operations issued. */
  std::uint64_t send_operations_ = 0;

  /** @brief Total response bytes passed to socket sends. */
  std::uint64_t send_bytes_ = 0;

  /** @brief Highest queued frame count observed for one connection. */
  std::uint64_t max_observed_write_queue_frames_ = 0;
};

/**
 * @brief Atomic diagnostic counters for server connection write paths.
 *
 * Updates use relaxed atomics because the counters only describe behavior after the actual event-loop state transition
 * has already happened.
 */
class ServerIoStats final {
 public:
  /** @brief Records one enqueued response frame and the queue depth after enqueue. */
  void RecordWriteFrameEnqueued(std::size_t queue_frames) {
    write_frames_enqueued_.fetch_add(1, std::memory_order_relaxed);
    ObserveMaximum(max_observed_write_queue_frames_, queue_frames);
  }

  /** @brief Records that a connection started draining its write queue. */
  void RecordWriteDrainStarted() { write_drains_started_.fetch_add(1, std::memory_order_relaxed); }

  /** @brief Records one socket send operation. */
  void RecordSendOperation(std::size_t bytes) {
    send_operations_.fetch_add(1, std::memory_order_relaxed);
    send_bytes_.fetch_add(static_cast<std::uint64_t>(bytes), std::memory_order_relaxed);
  }

  /** @return Point-in-time snapshot of relaxed diagnostic counters. */
  [[nodiscard]] auto Snapshot() const -> ServerIoStatsSnapshot {
    return ServerIoStatsSnapshot{
        .write_frames_enqueued_ = write_frames_enqueued_.load(std::memory_order_relaxed),
        .write_drains_started_ = write_drains_started_.load(std::memory_order_relaxed),
        .send_operations_ = send_operations_.load(std::memory_order_relaxed),
        .send_bytes_ = send_bytes_.load(std::memory_order_relaxed),
        .max_observed_write_queue_frames_ = max_observed_write_queue_frames_.load(std::memory_order_relaxed),
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

  std::atomic<std::uint64_t> write_frames_enqueued_{0};
  std::atomic<std::uint64_t> write_drains_started_{0};
  std::atomic<std::uint64_t> send_operations_{0};
  std::atomic<std::uint64_t> send_bytes_{0};
  std::atomic<std::uint64_t> max_observed_write_queue_frames_{0};
};

}  // namespace xrpc
