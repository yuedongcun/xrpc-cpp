#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

namespace xrpc {

/**
 * @brief Worker-pool diagnostics copied from relaxed atomic counters.
 *
 * `rejected_by_pending_limit_` counts logical jobs. A batch of N RPCs submitted as one physical worker task contributes
 * N logical jobs. `max_observed_worker_queue_depth_` tracks queued physical tasks.
 */
struct ThreadPoolExecutorSnapshot {
  /** @brief Logical jobs rejected by the global pending-job limit. */
  std::uint64_t rejected_by_pending_limit_ = 0;

  /** @brief Highest physical queue depth observed on any worker queue. */
  std::uint64_t max_observed_worker_queue_depth_ = 0;
};

/**
 * @brief Fixed-size worker pool used for server method dispatch.
 *
 * Design note:
 * - Ownership: each worker owns one private queue; the executor owns all workers and the global pending-job counter.
 * - Backpressure: submission reserves logical jobs before queueing. Rejection is visible to `TcpConnection` so it can
 * send an RPC error instead of growing memory.
 * - Shutdown: `Stop()` flips `stopped_`, wakes all queues, and lets `jthread`s join.
 * - Batching: `TrySubmitBatch()` counts multiple RPCs as one physical worker task.
 */
class ThreadPoolExecutor final {
 public:
  /**
   * @brief Starts `worker_count` worker threads.
   *
   * @param worker_count Number of worker threads and private queues.
   * @param max_pending_jobs Global logical-job limit across queued and running work.
   */
  explicit ThreadPoolExecutor(std::size_t worker_count,
                              std::size_t max_pending_jobs = std::numeric_limits<std::size_t>::max());

  /** @brief Stops workers before destroying queues and counters. */
  ~ThreadPoolExecutor();

  ThreadPoolExecutor(const ThreadPoolExecutor &) = delete;
  auto operator=(const ThreadPoolExecutor &) -> ThreadPoolExecutor & = delete;

  ThreadPoolExecutor(ThreadPoolExecutor &&) = delete;
  auto operator=(ThreadPoolExecutor &&) -> ThreadPoolExecutor & = delete;

  /**
   * @brief Batches multiple logical jobs into one physical worker task.
   *
   * This improves high-throughput handoff cost, but trades away per-request scheduling fairness because one worker runs
   * the whole batch before taking another task.
   */
  [[nodiscard]] auto TrySubmitBatch(std::function<void()> job, std::size_t logical_jobs) -> bool;

  /** @brief Prevents new jobs from being admitted while allowing accepted jobs to finish. */
  void CloseSubmissions() noexcept;

  /** @return true while new jobs may still be submitted. */
  [[nodiscard]] auto accepting_submissions() const noexcept -> bool;

  /** @return Snapshot of relaxed worker-pool diagnostics. */
  [[nodiscard]] auto stats() const -> ThreadPoolExecutorSnapshot;

  /** @brief Stops the pool, wakes all workers, and prevents new submissions. */
  void Stop();

 private:
  struct WorkerJob {
    std::function<void()> run_;
    std::size_t logical_jobs_ = 1;
  };

  struct WorkerQueue {
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<WorkerJob> jobs_;
    std::atomic<std::size_t> pending_jobs_{0};
  };

  /** @brief Chooses the worker queue for the next accepted task. */
  [[nodiscard]] auto SelectWorkerQueue() -> WorkerQueue &;

  /** @brief Reserves logical pending-job capacity before queueing work. */
  [[nodiscard]] auto TryReservePendingJobs(std::size_t logical_jobs) -> std::optional<std::size_t>;

  /** @brief Releases logical pending-job capacity after work finishes or submission rolls back. */
  void ReleasePendingJobs(std::size_t logical_jobs);

  /** @brief Worker thread loop for one private queue. */
  void WorkerLoop(WorkerQueue &queue);

  std::vector<std::unique_ptr<WorkerQueue>> worker_queues_;
  std::vector<std::jthread> workers_;
  std::atomic<std::size_t> next_worker_index_{0};
  std::size_t max_pending_jobs_ = 0;
  std::atomic<std::size_t> pending_jobs_{0};
  std::atomic_bool accepting_submissions_{true};
  std::atomic_bool stopped_{false};
  std::atomic<std::uint64_t> rejected_by_pending_limit_{0};
  std::atomic<std::uint64_t> max_observed_worker_queue_depth_{0};
};

}  // namespace xrpc
