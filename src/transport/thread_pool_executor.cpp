#include "transport/thread_pool_executor.h"

#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <utility>

#include <xrpc/metrics.h>
#include <xrpc/xrpc_exception.h>

#include "observability/rpc_metrics.h"

namespace xrpc {
namespace {

/**
 * @brief Atomically records the maximum observed value.
 *
 * @param maximum Relaxed diagnostic counter to update.
 * @param value Candidate maximum value.
 */
void ObserveMaximum(std::atomic<std::uint64_t> &maximum, std::size_t value) {
  std::uint64_t observed = maximum.load(std::memory_order_relaxed);
  const auto candidate = static_cast<std::uint64_t>(value);
  while (observed < candidate &&
         !maximum.compare_exchange_weak(observed, candidate, std::memory_order_relaxed, std::memory_order_relaxed)) {
  }
}

/**
 * @brief Emits one worker-job metric per logical RPC job when metrics are enabled.
 *
 * @param state Worker state label such as `submitted`, `completed`, or `rejected`.
 * @param count Number of logical RPC jobs represented by the event.
 */
void RecordServerWorkerJobs(std::string_view state, std::size_t count) {
  if (!MetricsEnabled()) {
    return;
  }
  for (std::size_t i = 0; i < count; ++i) {
    RecordServerWorkerJob(state);
  }
}

}  // namespace

/**
 * @brief Starts a fixed-size worker pool with a global pending logical-job limit.
 *
 * Each worker owns a private FIFO. Submission first reserves capacity against the global limit, then
 * chooses a worker queue. This keeps backpressure independent from the number of worker threads.
 *
 * @param worker_count Number of worker threads to start.
 * @param max_pending_jobs Maximum queued-or-running logical jobs accepted by the pool.
 * @throws ConfigException when either limit is zero.
 */
ThreadPoolExecutor::ThreadPoolExecutor(std::size_t worker_count, std::size_t max_pending_jobs)
    : max_pending_jobs_(max_pending_jobs) {
  if (worker_count == 0) {
    throw ConfigException("ThreadPoolExecutor requires at least one worker");
  }
  if (max_pending_jobs_ == 0) {
    throw ConfigException("ThreadPoolExecutor requires a positive pending job limit");
  }

  worker_queues_.reserve(worker_count);
  workers_.reserve(worker_count);
  for (std::size_t i = 0; i < worker_count; ++i) {
    worker_queues_.push_back(std::make_unique<WorkerQueue>());
  }
  for (const auto &queue : worker_queues_) {
    workers_.emplace_back([this, queue = queue.get()] { WorkerLoop(*queue); });
  }
}

/** @brief Stops workers and joins all owned threads. */
ThreadPoolExecutor::~ThreadPoolExecutor() { Stop(); }

/**
 * @brief Attempts to submit one logical job.
 *
 * @param job Callable to run on a worker thread.
 * @return true when accepted, false when the global pending limit is full.
 */
auto ThreadPoolExecutor::TrySubmit(std::function<void()> job) -> bool { return TrySubmitBatch(std::move(job), 1); }

/**
 * @brief Attempts to submit a callable representing multiple logical RPC jobs.
 *
 * Batched submission is used when one worker callable drains several decoded requests from the same
 * connection. Capacity accounting still reflects the logical RPC count.
 *
 * @param job Callable to run on a worker thread.
 * @param logical_jobs Number of RPC jobs represented by `job`.
 * @return true when accepted, false when the global pending limit is full.
 * @throws LifecycleException when the pool is already stopped.
 */
auto ThreadPoolExecutor::TrySubmitBatch(std::function<void()> job, std::size_t logical_jobs) -> bool {
  if (logical_jobs == 0) {
    throw std::invalid_argument("ThreadPoolExecutor::TrySubmitBatch requires at least one logical job");
  }
  if (stopped_.load(std::memory_order_acquire)) {
    throw LifecycleException("ThreadPoolExecutor::TrySubmit called after Stop");
  }

  const std::optional<std::size_t> pending_after_reservation = TryReservePendingJobs(logical_jobs);
  if (!pending_after_reservation.has_value()) {
    return false;
  }

  WorkerQueue &queue = SelectWorkerQueue();
  try {
    std::lock_guard<std::mutex> lock(queue.mutex_);
    if (stopped_.load(std::memory_order_relaxed)) {
      throw LifecycleException("ThreadPoolExecutor::TrySubmit called after Stop");
    }
    queue.jobs_.push(WorkerJob{.run_ = std::move(job), .logical_jobs_ = logical_jobs});
    const std::size_t queue_depth = queue.pending_jobs_.fetch_add(1, std::memory_order_relaxed) + 1;
    ObserveMaximum(max_observed_worker_queue_depth_, queue_depth);
    submitted_jobs_.fetch_add(logical_jobs, std::memory_order_relaxed);
    RecordServerWorkerJobs("submitted", logical_jobs);
    RecordServerWorkerPendingJobs(*pending_after_reservation);
  } catch (...) {
    ReleasePendingJobs(logical_jobs);
    throw;
  }
  queue.cv_.notify_one();
  return true;
}

/** @return Snapshot of worker-pool counters used by server diagnostics. */
auto ThreadPoolExecutor::stats() const -> ThreadPoolExecutorSnapshot {
  return ThreadPoolExecutorSnapshot{
      .submitted_jobs_ = submitted_jobs_.load(std::memory_order_relaxed),
      .completed_jobs_ = completed_jobs_.load(std::memory_order_relaxed),
      .rejected_jobs_ = rejected_jobs_.load(std::memory_order_relaxed),
      .max_observed_worker_queue_depth_ = max_observed_worker_queue_depth_.load(std::memory_order_relaxed),
  };
}

/**
 * @brief Selects the worker queue for a newly accepted job.
 *
 * The algorithm starts with round-robin fairness, then opportunistically prefers a visibly shorter
 * queue using relaxed counters. Exact ordering is unnecessary because the global admission guard has
 * already protected resource bounds.
 *
 * @return Worker queue that should receive the next job.
 */
auto ThreadPoolExecutor::SelectWorkerQueue() -> WorkerQueue & {
  const std::size_t worker_count = worker_queues_.size();
  const std::size_t start = next_worker_index_.fetch_add(1, std::memory_order_relaxed) % worker_count;
  std::size_t selected = start;
  std::size_t selected_pending = worker_queues_[selected]->pending_jobs_.load(std::memory_order_relaxed);

  // Start round-robin for fairness, then prefer a shorter queue when one is
  // visible through relaxed counters.
  for (std::size_t offset = 1; offset < worker_count && selected_pending > 0; ++offset) {
    const std::size_t candidate = (start + offset) % worker_count;
    const std::size_t candidate_pending = worker_queues_[candidate]->pending_jobs_.load(std::memory_order_relaxed);
    if (candidate_pending < selected_pending) {
      selected = candidate;
      selected_pending = candidate_pending;
    }
  }
  return *worker_queues_[selected];
}

/**
 * @brief Reserves global pending capacity for logical jobs.
 *
 * @param logical_jobs Number of logical RPC jobs to account for.
 * @return Pending count after reservation, or empty when the global limit is full.
 */
auto ThreadPoolExecutor::TryReservePendingJobs(std::size_t logical_jobs) -> std::optional<std::size_t> {
  std::size_t pending = pending_jobs_.load(std::memory_order_relaxed);
  while (true) {
    if (pending > max_pending_jobs_ || logical_jobs > max_pending_jobs_ - pending) {
      rejected_jobs_.fetch_add(logical_jobs, std::memory_order_relaxed);
      RecordServerWorkerJobs("rejected", logical_jobs);
      return std::nullopt;
    }
    if (pending_jobs_.compare_exchange_weak(pending, pending + logical_jobs, std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
      return pending + logical_jobs;
    }
  }
}

/**
 * @brief Releases logical-job capacity after execution or failed submission.
 *
 * @param logical_jobs Number of logical RPC jobs to remove from pending accounting.
 */
void ThreadPoolExecutor::ReleasePendingJobs(std::size_t logical_jobs) {
  const std::size_t remaining = pending_jobs_.fetch_sub(logical_jobs, std::memory_order_release) - logical_jobs;
  RecordServerWorkerPendingJobs(remaining);
}

/**
 * @brief Requests all workers to stop after draining their local queues.
 *
 * Stop is idempotent and joins every owned `std::jthread` before returning.
 */
void ThreadPoolExecutor::Stop() {
  if (stopped_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  for (const auto &queue : worker_queues_) {
    // Synchronize with the worker's predicate check before notifying it.
    std::lock_guard<std::mutex> lock(queue->mutex_);
    queue->cv_.notify_all();
  }

  for (auto &worker : workers_) {
    worker.request_stop();
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();
}

/**
 * @brief Runs jobs from one worker queue until stop is requested and the queue is empty.
 *
 * @param queue Worker-local FIFO and condition variable.
 */
void ThreadPoolExecutor::WorkerLoop(WorkerQueue &queue) {
  while (true) {
    WorkerJob job;
    {
      std::unique_lock<std::mutex> lock(queue.mutex_);
      queue.cv_.wait(lock, [this, &queue] { return stopped_.load(std::memory_order_acquire) || !queue.jobs_.empty(); });
      if (stopped_.load(std::memory_order_relaxed) && queue.jobs_.empty()) {
        return;
      }

      // Pending counters are released after job execution so the global limit
      // includes both queued and currently running work.
      job = std::move(queue.jobs_.front());
      queue.jobs_.pop();
    }

    job.run_();
    completed_jobs_.fetch_add(job.logical_jobs_, std::memory_order_relaxed);
    RecordServerWorkerJobs("completed", job.logical_jobs_);
    queue.pending_jobs_.fetch_sub(1, std::memory_order_relaxed);
    ReleasePendingJobs(job.logical_jobs_);
  }
}

}  // namespace xrpc
