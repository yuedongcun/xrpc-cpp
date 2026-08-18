/** @file thread_pool_executor.h @brief Declares the server handler worker executor. */

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
 * @brief Shared handler executor with bounded concurrent submission.
 *
 * Multiple connection I/O threads may call `TrySubmitBatch()` concurrently.
 * `CloseSubmissions()` is thread-safe and prevents new work; the owning server
 * runtime then calls `Stop()` once to wait for admitted work and join workers.
 */
class ThreadPoolExecutor final {
 public:
  explicit ThreadPoolExecutor(std::size_t worker_count,
                              std::size_t max_pending_jobs = std::numeric_limits<std::size_t>::max());

  ~ThreadPoolExecutor();

  ThreadPoolExecutor(const ThreadPoolExecutor &) = delete;
  auto operator=(const ThreadPoolExecutor &) -> ThreadPoolExecutor & = delete;

  ThreadPoolExecutor(ThreadPoolExecutor &&) = delete;
  auto operator=(ThreadPoolExecutor &&) -> ThreadPoolExecutor & = delete;

  [[nodiscard]] auto TrySubmitBatch(std::function<void()> job, std::size_t logical_jobs) -> bool;

  void CloseSubmissions() noexcept;

  [[nodiscard]] auto accepting_submissions() const noexcept -> bool;

  // Owner-thread shutdown operation. It is not a general concurrent API.
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

  [[nodiscard]] auto SelectWorkerQueue() -> WorkerQueue &;

  [[nodiscard]] auto TryReservePendingJobs(std::size_t logical_jobs) -> std::optional<std::size_t>;

  void ReleasePendingJobs(std::size_t logical_jobs);

  void WorkerLoop(WorkerQueue &queue);

  std::vector<std::unique_ptr<WorkerQueue>> worker_queues_;
  std::vector<std::jthread> workers_;
  std::atomic<std::size_t> next_worker_index_{0};
  std::size_t max_pending_jobs_ = 0;
  std::atomic<std::size_t> pending_jobs_{0};
  std::atomic_bool accepting_submissions_{true};
  std::atomic_bool stopped_{false};
};

}  // namespace xrpc
