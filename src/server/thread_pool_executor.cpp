/** @file thread_pool_executor.cpp @brief Implements the server handler worker executor. */

#include "server/thread_pool_executor.h"

#include <stdexcept>
#include <utility>

#include "common/xrpc_exception.h"

namespace xrpc {

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

ThreadPoolExecutor::~ThreadPoolExecutor() { Stop(); }

auto ThreadPoolExecutor::TrySubmitBatch(std::function<void()> job, std::size_t logical_jobs) -> bool {
  if (logical_jobs == 0) {
    throw std::invalid_argument("ThreadPoolExecutor::TrySubmitBatch requires at least one logical job");
  }
  if (!accepting_submissions_.load()) {
    return false;
  }

  const std::optional<std::size_t> pending_after_reservation = TryReservePendingJobs(logical_jobs);
  if (!pending_after_reservation.has_value()) {
    return false;
  }

  WorkerQueue &queue = SelectWorkerQueue();
  try {
    std::lock_guard<std::mutex> lock(queue.mutex_);
    if (!accepting_submissions_.load()) {
      ReleasePendingJobs(logical_jobs);
      return false;
    }
    queue.jobs_.push(WorkerJob{.run_ = std::move(job), .logical_jobs_ = logical_jobs});
    queue.pending_jobs_.fetch_add(1);
  } catch (...) {
    ReleasePendingJobs(logical_jobs);
    throw;
  }
  queue.cv_.notify_one();
  return true;
}

void ThreadPoolExecutor::CloseSubmissions() noexcept {
  accepting_submissions_.store(false);
  for (const auto &queue : worker_queues_) {
    std::lock_guard lock(queue->mutex_);
  }
}

auto ThreadPoolExecutor::accepting_submissions() const noexcept -> bool { return accepting_submissions_.load(); }

auto ThreadPoolExecutor::SelectWorkerQueue() -> WorkerQueue & {
  const std::size_t worker_count = worker_queues_.size();
  const std::size_t start = next_worker_index_.fetch_add(1) % worker_count;
  std::size_t selected = start;
  std::size_t selected_pending = worker_queues_[selected]->pending_jobs_.load();

  for (std::size_t offset = 1; offset < worker_count && selected_pending > 0; ++offset) {
    const std::size_t candidate = (start + offset) % worker_count;
    const std::size_t candidate_pending = worker_queues_[candidate]->pending_jobs_.load();
    if (candidate_pending < selected_pending) {
      selected = candidate;
      selected_pending = candidate_pending;
    }
  }
  return *worker_queues_[selected];
}

auto ThreadPoolExecutor::TryReservePendingJobs(std::size_t logical_jobs) -> std::optional<std::size_t> {
  std::size_t pending = pending_jobs_.load();
  while (true) {
    if (pending > max_pending_jobs_ || logical_jobs > max_pending_jobs_ - pending) {
      return std::nullopt;
    }
    if (pending_jobs_.compare_exchange_weak(pending, pending + logical_jobs)) {
      return pending + logical_jobs;
    }
  }
}

void ThreadPoolExecutor::ReleasePendingJobs(std::size_t logical_jobs) { pending_jobs_.fetch_sub(logical_jobs); }

void ThreadPoolExecutor::Stop() {
  CloseSubmissions();
  if (stopped_.exchange(true)) {
    return;
  }
  for (const auto &queue : worker_queues_) {
    std::lock_guard<std::mutex> lock(queue->mutex_);
    queue->cv_.notify_one();
  }

  for (auto &worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();
}

void ThreadPoolExecutor::WorkerLoop(WorkerQueue &queue) {
  while (true) {
    WorkerJob job;
    {
      std::unique_lock<std::mutex> lock(queue.mutex_);
      queue.cv_.wait(lock, [this, &queue]() -> bool { return stopped_.load() || !queue.jobs_.empty(); });
      if (stopped_.load() && queue.jobs_.empty()) {
        return;
      }

      job = std::move(queue.jobs_.front());
      queue.jobs_.pop();
    }

    job.run_();
    queue.pending_jobs_.fetch_sub(1);
    ReleasePendingJobs(job.logical_jobs_);
  }
}

}  // namespace xrpc
