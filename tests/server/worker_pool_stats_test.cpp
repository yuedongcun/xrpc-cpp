#include <gtest/gtest.h>

#include <chrono>
#include <future>

#include "server/worker_pool.h"

TEST(WorkerPoolStatsTest, TracksLogicalJobsAndResetsPeaks) {
  std::promise<void> started;
  auto started_future = started.get_future();
  std::promise<void> release;
  auto release_future = release.get_future().share();
  xrpc::WorkerPool pool({.threads_ = 1, .max_pending_jobs_ = 20});
  EXPECT_TRUE(pool.TrySubmitBatch(
      [&]() {
        started.set_value();
        release_future.wait();
      },
      3));
  // Use a gate, not a sleep, to keep exactly one batch executing.
  EXPECT_EQ(started_future.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_TRUE(pool.TrySubmitBatch([]() {}, 2));
  EXPECT_TRUE(pool.TrySubmitBatch([]() {}, 5));
  EXPECT_FALSE(pool.TrySubmitBatch([]() {}, 11));

  const auto held = pool.SnapshotStats(true);
  EXPECT_EQ(held.pending_logical_jobs_, 10U);  // 3 executing + 7 queued.
  EXPECT_EQ(held.queues_[0].pending_logical_jobs_, 10U);
  EXPECT_EQ(held.queues_[0].queued_logical_jobs_, 7U);
  EXPECT_EQ(held.queues_[0].queued_logical_jobs_peak_, 7U);

  // Release before fatal assertions or pool destruction, even if expectations fail.
  release.set_value();
  pool.DrainAndJoin();
  const auto drained = pool.SnapshotStats();
  EXPECT_EQ(drained.pending_logical_jobs_, 0U);
  EXPECT_EQ(drained.queues_[0].queued_logical_jobs_, 0U);
  EXPECT_EQ(drained.queues_[0].pending_logical_jobs_, 0U);
  EXPECT_EQ(drained.queues_[0].queued_logical_jobs_peak_, 7U);
  const auto reset = pool.SnapshotStats(true);
  EXPECT_EQ(reset.window_id_, held.window_id_ + 1);
  EXPECT_EQ(reset.queues_[0].queued_logical_jobs_peak_, 0U);
}

TEST(WorkerPoolStatsTest, SchedulesByPendingLogicalJobsInsteadOfBatchCount) {
  std::promise<void> first_started;
  std::promise<void> second_started;
  auto first_started_future = first_started.get_future();
  auto second_started_future = second_started.get_future();
  std::promise<void> release;
  auto release_future = release.get_future().share();
  xrpc::WorkerPool pool({.threads_ = 2, .max_pending_jobs_ = 20});

  EXPECT_TRUE(pool.TrySubmitBatch(
      [&]() {
        first_started.set_value();
        release_future.wait();
      },
      10));
  EXPECT_TRUE(pool.TrySubmitBatch(
      [&]() {
        second_started.set_value();
        release_future.wait();
      },
      1));
  const auto first_ready = first_started_future.wait_for(std::chrono::seconds(1));
  const auto second_ready = second_started_future.wait_for(std::chrono::seconds(1));
  EXPECT_EQ(first_ready, std::future_status::ready);
  EXPECT_EQ(second_ready, std::future_status::ready);
  if (first_ready != std::future_status::ready || second_ready != std::future_status::ready) {
    release.set_value();
    pool.DrainAndJoin();
    return;
  }

  // Both workers have one running batch, but worker 1 has fewer logical RPCs.
  EXPECT_TRUE(pool.TrySubmitBatch([]() {}, 1));
  const auto snapshot = pool.SnapshotStats();
  EXPECT_EQ(snapshot.queues_[0].queued_logical_jobs_, 0U);
  EXPECT_EQ(snapshot.queues_[1].queued_logical_jobs_, 1U);
  EXPECT_EQ(snapshot.queues_[0].pending_logical_jobs_, 10U);
  EXPECT_EQ(snapshot.queues_[1].pending_logical_jobs_, 2U);

  release.set_value();
  pool.DrainAndJoin();
}
