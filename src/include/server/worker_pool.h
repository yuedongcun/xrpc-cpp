/**
 * @file worker_pool.h
 * @brief Defines the server worker pool for RPC dispatch processing.
 *
 * The pool receives work from connection I/O threads, executes admitted RPC
 * batches on worker threads, and supports bounded admission and graceful drain
 * during server shutdown.
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
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
 * @brief Worker pool for server RPC dispatch processing.
 *
 * Multiple connection I/O threads may submit work concurrently through
 * `TrySubmitBatch()`. Each submitted `WorkerJob` may represent multiple logical
 * RPCs, which are counted against the global pending-job limit for backpressure.
 *
 * `CloseSubmissions()` prevents new work from being admitted while preserving
 * already admitted work. The owning server runtime eventually calls
 * `DrainAndJoin()` to drain queued work and join all worker threads.
 */
class WorkerPool final {
 public:
  explicit WorkerPool(std::size_t worker_count, std::size_t max_pending_jobs = std::numeric_limits<std::size_t>::max());

  ~WorkerPool();

  WorkerPool(const WorkerPool &) = delete;
  auto operator=(const WorkerPool &) -> WorkerPool & = delete;

  WorkerPool(WorkerPool &&) = delete;
  auto operator=(WorkerPool &&) -> WorkerPool & = delete;

  /**
   * @brief Attempts to admit one worker job representing a batch of logical RPCs.
   *
   * The job is accepted only while submissions are open and reserving
   * `logical_jobs` would not exceed the global pending-job limit.
   *
   * @return `true` if the job was admitted and queued; `false` if admission is
   * closed or the pending-job limit would be exceeded.
   */
  [[nodiscard]] auto TrySubmitBatch(std::function<void()> job, std::size_t logical_jobs) -> bool;

  /**
   * @brief Closes worker admission and synchronizes with concurrent submissions.
   *
   * After this function returns, no concurrent `TrySubmitBatch()` can still
   * enqueue new work. Jobs admitted before closure remain queued and will be
   * executed during draining.
   */
  void CloseSubmissions() noexcept;

  [[nodiscard]] auto accepting_submissions() const noexcept -> bool;

  /**
   * @brief Drains admitted work and joins all worker threads.
   *
   * This owner-thread operation first closes submissions, then lets workers
   * finish all queued jobs before exiting. Repeated calls are harmless.
   */
  void DrainAndJoin();

 private:
  struct WorkerJob {
    std::function<void()> run_;

    // Number of logical RPCs represented by this queued worker job.
    std::size_t logical_jobs_ = 1;
  };

  struct WorkerQueue {
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<WorkerJob> jobs_;

    // Number of WorkerJob entries queued or currently executing on this worker.
    std::atomic<std::size_t> pending_entries_{0};
  };

  [[nodiscard]] auto SelectWorkerQueue() -> WorkerQueue &;

  [[nodiscard]] auto TryReservePendingJobs(std::size_t logical_jobs) -> std::optional<std::size_t>;

  void ReleasePendingJobs(std::size_t logical_jobs);

  void WorkerLoop(WorkerQueue &queue);

  std::vector<std::unique_ptr<WorkerQueue>> worker_queues_;
  std::vector<std::jthread> workers_;
  std::atomic<std::size_t> next_worker_index_{0};
  std::size_t max_pending_jobs_ = 0;
  // Total logical RPCs represented by admitted worker jobs.
  std::atomic<std::size_t> pending_jobs_{0};
  // Admission gate shared by connection I/O threads and the shutdown owner.
  std::atomic_bool accepting_submissions_{true};
  // Set when workers should drain queued jobs and then exit.
  std::atomic_bool drain_requested_{false};
};

}  // namespace xrpc
