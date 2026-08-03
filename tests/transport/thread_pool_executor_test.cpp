#include "transport/thread_pool_executor.h"

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include <gtest/gtest.h>

#include <xrpc/xrpc_exception.h>

namespace xrpc {
namespace {

TEST(ThreadPoolExecutorTest, ExecutesSubmittedJobs) {
  ThreadPoolExecutor executor(2);

  std::promise<void> done;
  std::atomic<int> value = 0;
  ASSERT_TRUE(executor.TrySubmit([&done, &value] {
    value.store(42);
    done.set_value();
  }));

  ASSERT_EQ(done.get_future().wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_EQ(value.load(), 42);
}

TEST(ThreadPoolExecutorTest, BatchSubmissionCountsLogicalJobs) {
  ThreadPoolExecutor executor(1, 3);

  std::promise<void> done;
  std::atomic<int> runs = 0;
  ASSERT_TRUE(executor.TrySubmitBatch(
      [&] {
        runs.fetch_add(1, std::memory_order_relaxed);
        done.set_value();
      },
      3));

  ASSERT_EQ(done.get_future().wait_for(std::chrono::seconds(1)), std::future_status::ready);
  executor.Stop();

  EXPECT_EQ(runs.load(std::memory_order_relaxed), 1);
  const ThreadPoolExecutorSnapshot snapshot = executor.stats();
  EXPECT_EQ(snapshot.submitted_jobs_, 3);
  EXPECT_EQ(snapshot.completed_jobs_, 3);
  EXPECT_EQ(snapshot.rejected_jobs_, 0);
}

TEST(ThreadPoolExecutorTest, StopRejectsNewJobs) {
  ThreadPoolExecutor executor(1);
  executor.Stop();

  EXPECT_THROW(static_cast<void>(executor.TrySubmit([] {})), LifecycleException);
}

TEST(ThreadPoolExecutorTest, MultipleWorkersCanRunJobsConcurrently) {
  ThreadPoolExecutor executor(2);
  std::promise<void> first_started;
  std::promise<void> second_started;
  std::future<void> first_started_future = first_started.get_future();
  std::future<void> second_started_future = second_started.get_future();
  std::promise<void> release;
  std::shared_future<void> release_future = release.get_future().share();
  std::promise<void> first_done;
  std::promise<void> second_done;

  ASSERT_TRUE(executor.TrySubmit([&] {
    first_started.set_value();
    release_future.wait();
    first_done.set_value();
  }));
  ASSERT_TRUE(executor.TrySubmit([&] {
    second_started.set_value();
    release_future.wait();
    second_done.set_value();
  }));

  EXPECT_EQ(first_started_future.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_EQ(second_started_future.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  release.set_value();
  EXPECT_EQ(first_done.get_future().wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_EQ(second_done.get_future().wait_for(std::chrono::seconds(1)), std::future_status::ready);
}

TEST(ThreadPoolExecutorTest, AvoidsQueueingBehindBusyWorkerWhenAnotherWorkerIsIdle) {
  ThreadPoolExecutor executor(2);
  std::promise<void> slow_started;
  std::promise<void> release_slow;
  std::shared_future<void> release_slow_future = release_slow.get_future().share();
  std::promise<void> first_fast_done;
  std::promise<void> second_fast_done;

  ASSERT_TRUE(executor.TrySubmit([&] {
    slow_started.set_value();
    release_slow_future.wait();
  }));
  EXPECT_EQ(slow_started.get_future().wait_for(std::chrono::seconds(1)), std::future_status::ready);

  ASSERT_TRUE(executor.TrySubmit([&] { first_fast_done.set_value(); }));
  EXPECT_EQ(first_fast_done.get_future().wait_for(std::chrono::seconds(1)), std::future_status::ready);

  ASSERT_TRUE(executor.TrySubmit([&] { second_fast_done.set_value(); }));
  const std::future_status second_fast_status = second_fast_done.get_future().wait_for(std::chrono::seconds(1));
  release_slow.set_value();
  EXPECT_EQ(second_fast_status, std::future_status::ready);
}

TEST(ThreadPoolExecutorTest, DestructorWaitsForOutstandingJobs) {
  std::atomic<bool> finished = false;
  {
    ThreadPoolExecutor executor(1);
    ASSERT_TRUE(executor.TrySubmit([&finished] {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      finished.store(true);
    }));
  }

  EXPECT_TRUE(finished.load());
}

TEST(ThreadPoolExecutorTest, RejectsJobWhenGlobalPendingLimitIsReached) {
  ThreadPoolExecutor executor(1, 1);
  std::promise<void> started;
  std::promise<void> release;
  std::shared_future<void> release_future = release.get_future().share();

  ASSERT_TRUE(executor.TrySubmit([&] {
    started.set_value();
    release_future.wait();
  }));
  ASSERT_EQ(started.get_future().wait_for(std::chrono::seconds(1)), std::future_status::ready);

  EXPECT_FALSE(executor.TrySubmit([] {}));
  release.set_value();
}

TEST(ThreadPoolExecutorTest, ReportsQueueStats) {
  ThreadPoolExecutor executor(1, 1);
  std::promise<void> started;
  std::promise<void> release;
  std::shared_future<void> release_future = release.get_future().share();

  ASSERT_TRUE(executor.TrySubmit([&] {
    started.set_value();
    release_future.wait();
  }));
  ASSERT_EQ(started.get_future().wait_for(std::chrono::seconds(1)), std::future_status::ready);
  EXPECT_FALSE(executor.TrySubmit([] {}));

  const ThreadPoolExecutorSnapshot blocked_snapshot = executor.stats();
  EXPECT_EQ(blocked_snapshot.submitted_jobs_, 1);
  EXPECT_EQ(blocked_snapshot.completed_jobs_, 0);
  EXPECT_EQ(blocked_snapshot.rejected_jobs_, 1);
  EXPECT_GE(blocked_snapshot.max_observed_worker_queue_depth_, 1);

  release.set_value();
  executor.Stop();

  const ThreadPoolExecutorSnapshot final_snapshot = executor.stats();
  EXPECT_EQ(final_snapshot.submitted_jobs_, 1);
  EXPECT_EQ(final_snapshot.completed_jobs_, 1);
  EXPECT_EQ(final_snapshot.rejected_jobs_, 1);
  EXPECT_GE(final_snapshot.max_observed_worker_queue_depth_, 1);
}

}  // namespace
}  // namespace xrpc
