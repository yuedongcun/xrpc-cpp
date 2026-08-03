#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <thread>

#include <xrpc/xrpc_exception.h>

#include "io/uring_context.h"
#include "test_support/runtime_util.h"

namespace {

constexpr auto PollInterval = std::chrono::milliseconds(1);
constexpr auto WaitTimeout = std::chrono::milliseconds(1000);

}  // namespace

TEST(IoUringContextPostTest, ExecutesPostedCallbackOnRunThread) {
  xrpc::io::UringContext context;
  xrpc::testsupport::UringContextRunner runner;
  runner.Start(context);

  std::atomic<bool> invoked = false;
  std::atomic<bool> done = false;
  std::thread::id run_thread_id;

  context.Post([&]() {
    run_thread_id = std::this_thread::get_id();
    invoked.store(true);
  });
  context.Post([&]() { done.store(true); });

  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (!done.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }

  ASSERT_TRUE(done.load());
  ASSERT_TRUE(invoked.load());
  EXPECT_NE(run_thread_id, std::this_thread::get_id());

  runner.StopAndJoin(context);
}

TEST(IoUringContextPostTest, ExecutesCallbackPostedBeforeRun) {
  xrpc::io::UringContext context;
  std::atomic<bool> invoked = false;
  context.Post([&invoked]() { invoked.store(true); });

  xrpc::testsupport::UringContextRunner runner;
  runner.Start(context);

  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (!invoked.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }

  runner.StopAndJoin(context);
  EXPECT_TRUE(invoked.load());
}

TEST(IoUringContextPostTest, ReportsPostStats) {
  xrpc::io::UringContext context;
  std::atomic<std::size_t> executed = 0;

  context.Post([&executed]() { executed.fetch_add(1); });
  context.Post([&executed]() { executed.fetch_add(1); });

  const xrpc::io::UringPostStatsSnapshot before_run_snapshot = context.post_stats();
  EXPECT_EQ(before_run_snapshot.posted_callbacks_, 2);
  EXPECT_EQ(before_run_snapshot.drained_callbacks_, 0);
  EXPECT_EQ(before_run_snapshot.drain_batches_, 0);
  EXPECT_GE(before_run_snapshot.max_observed_post_queue_depth_, 2);

  xrpc::testsupport::UringContextRunner runner;
  runner.Start(context);

  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (executed.load() != 2 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }

  runner.StopAndJoin(context);

  const xrpc::io::UringPostStatsSnapshot final_snapshot = context.post_stats();
  EXPECT_EQ(executed.load(), 2);
  EXPECT_EQ(final_snapshot.posted_callbacks_, 2);
  EXPECT_EQ(final_snapshot.drained_callbacks_, 2);
  EXPECT_EQ(final_snapshot.drain_batches_, 1);
  EXPECT_GE(final_snapshot.max_observed_post_queue_depth_, 2);
}

TEST(IoUringContextPostTest, RejectsCallbacksPostedAfterStopBoundary) {
  xrpc::io::UringContext context;
  bool accepted_callback_invoked = false;
  bool rejected_callback_invoked = false;

  context.Post([&accepted_callback_invoked]() { accepted_callback_invoked = true; });
  context.Stop();
  context.Post([&rejected_callback_invoked]() { rejected_callback_invoked = true; });
  context.Run();

  EXPECT_TRUE(accepted_callback_invoked);
  EXPECT_FALSE(rejected_callback_invoked);
}

TEST(IoUringContextPostTest, WakesBlockedRunWithoutPollingDelay) {
  constexpr std::size_t callback_count = 10;
  constexpr auto expected_max_elapsed = std::chrono::milliseconds(300);

  xrpc::io::UringContext context;
  xrpc::testsupport::UringContextRunner runner;
  runner.Start(context);

  std::mutex mutex;
  std::condition_variable completed_cv;
  std::size_t completed = 0;
  bool all_completed = true;
  const auto start = std::chrono::steady_clock::now();

  for (std::size_t expected = 1; expected <= callback_count; ++expected) {
    context.Post([&]() {
      {
        std::lock_guard<std::mutex> lock(mutex);
        ++completed;
      }
      completed_cv.notify_one();
    });

    std::unique_lock<std::mutex> lock(mutex);
    if (!completed_cv.wait_for(lock, WaitTimeout, [&]() { return completed >= expected; })) {
      all_completed = false;
      break;
    }
  }

  const auto elapsed = std::chrono::steady_clock::now() - start;
  runner.StopAndJoin(context);

  ASSERT_TRUE(all_completed);
  EXPECT_LT(elapsed, expected_max_elapsed);
}

TEST(IoUringContextPostTest, ExecutesConcurrentPostsExactlyOnce) {
  constexpr std::size_t producer_count = 4;
  constexpr std::size_t callbacks_per_producer = 100;
  constexpr std::size_t expected_callbacks = producer_count * callbacks_per_producer;

  xrpc::io::UringContext context;
  xrpc::testsupport::UringContextRunner runner;
  runner.Start(context);

  std::atomic<std::size_t> executed = 0;
  std::array<std::jthread, producer_count> producers;
  for (auto &producer : producers) {
    producer = std::jthread([&]() {
      for (std::size_t index = 0; index < callbacks_per_producer; ++index) {
        context.Post([&executed]() { executed.fetch_add(1); });
      }
    });
  }
  for (auto &producer : producers) {
    producer.join();
  }

  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (executed.load() != expected_callbacks && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }

  runner.StopAndJoin(context);
  EXPECT_EQ(executed.load(), expected_callbacks);
}

TEST(IoUringContextPostTest, RejectsIoSubmissionOutsideRunThread) {
  xrpc::io::UringContext context;
  xrpc::testsupport::UringContextRunner runner;
  runner.Start(context);

  std::atomic<bool> run_thread_ready = false;
  context.Post([&run_thread_ready]() { run_thread_ready.store(true); });
  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (!run_thread_ready.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }

  EXPECT_TRUE(run_thread_ready.load());
  EXPECT_THROW((void)context.Nop(), xrpc::LifecycleException);
  EXPECT_THROW(context.CancelFd(123), xrpc::LifecycleException);
  runner.StopAndJoin(context);
}
