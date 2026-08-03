#include "common/task.h"

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

namespace xrpc::runtime {
namespace {

auto ReturnAnswer() -> Task<int> { co_return 42; }

auto ReturnNothing(bool &called) -> Task<void> {
  called = true;
  co_return;
}

auto NestedAnswer() -> Task<int> {
  const int answer = co_await ReturnAnswer();
  co_return answer + 1;
}

auto FailWithMessage() -> Task<int> {
  throw std::runtime_error("task failed");
  co_return 0;
}

TEST(RuntimeTaskTest, SyncWaitReturnsValue) {
  Task<int> task = ReturnAnswer();

  EXPECT_FALSE(task.Done());
  EXPECT_EQ(SyncWait(std::move(task)), 42);
}

TEST(RuntimeTaskTest, VoidTaskCompletes) {
  bool called = false;
  Task<void> task = ReturnNothing(called);

  EXPECT_FALSE(task.Done());
  SyncWait(std::move(task));
  EXPECT_TRUE(called);
}

TEST(RuntimeTaskTest, NestedAwaitReturnsValue) { EXPECT_EQ(SyncWait(NestedAnswer()), 43); }

TEST(RuntimeTaskTest, WaitBlocksUntilStartedTaskCompletes) {
  bool called = false;
  Task<void> task = ReturnNothing(called);
  std::atomic<bool> wait_returned = false;

  std::jthread waiter([&task, &wait_returned]() {
    task.Wait();
    wait_returned.store(true, std::memory_order_release);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_FALSE(wait_returned.load(std::memory_order_acquire));

  task.Start();
  waiter.join();

  EXPECT_TRUE(wait_returned.load(std::memory_order_acquire));
  EXPECT_TRUE(called);
}

TEST(RuntimeTaskTest, WaitForReturnsFalseWhenTaskIsNotComplete) {
  Task<int> task = ReturnAnswer();

  EXPECT_FALSE(task.WaitFor(std::chrono::milliseconds(1)));
  task.Start();
  EXPECT_TRUE(task.WaitFor(std::chrono::milliseconds(1)));
  EXPECT_EQ(task.Result(), 42);
}

TEST(RuntimeTaskTest, ExceptionsAreRethrown) {
  Task<int> task = FailWithMessage();

  EXPECT_THROW(
      {
        try {
          static_cast<void>(SyncWait(std::move(task)));
        } catch (const std::runtime_error &error) {
          EXPECT_STREQ(error.what(), "task failed");
          throw;
        }
      },
      std::runtime_error);
}

}  // namespace
}  // namespace xrpc::runtime
