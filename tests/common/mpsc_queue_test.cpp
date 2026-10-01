#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/mpsc_queue.h"

namespace {

using namespace std::chrono_literals;

struct MoveOnlyValue final {
  explicit MoveOnlyValue(int input) : value(input) {}

  MoveOnlyValue(const MoveOnlyValue &) = delete;
  auto operator=(const MoveOnlyValue &) -> MoveOnlyValue & = delete;

  MoveOnlyValue(MoveOnlyValue &&other) noexcept : value(std::exchange(other.value, MovedFrom)) {}

  auto operator=(MoveOnlyValue &&other) noexcept -> MoveOnlyValue & {
    value = std::exchange(other.value, MovedFrom);
    return *this;
  }

  static constexpr int MovedFrom = -1;
  int value;
};

struct Message final {
  Message(std::size_t producer_input, std::size_t sequence_input)
      : producer(producer_input), sequence(sequence_input) {}

  Message(const Message &) = delete;
  auto operator=(const Message &) -> Message & = delete;
  Message(Message &&) noexcept = default;
  auto operator=(Message &&) noexcept -> Message & = default;

  std::size_t producer;
  std::size_t sequence;
};

struct TrackedValue final {
  explicit TrackedValue(std::atomic<int> &live_input) : live(&live_input) { live->fetch_add(1); }

  ~TrackedValue() {
    if (live != nullptr) {
      live->fetch_sub(1);
    }
  }

  TrackedValue(const TrackedValue &) = delete;
  auto operator=(const TrackedValue &) -> TrackedValue & = delete;

  TrackedValue(TrackedValue &&other) noexcept : live(other.live) {
    if (live != nullptr) {
      live->fetch_add(1);
    }
  }

  auto operator=(TrackedValue &&other) noexcept -> TrackedValue & {
    if (live != nullptr) {
      live->fetch_sub(1);
    }
    live = other.live;
    if (live != nullptr) {
      live->fetch_add(1);
    }
    return *this;
  }

  std::atomic<int> *live;
};

static_assert(!std::is_default_constructible_v<MoveOnlyValue>);
static_assert(!std::is_copy_constructible_v<MoveOnlyValue>);
static_assert(std::is_nothrow_move_constructible_v<MoveOnlyValue>);
static_assert(std::is_nothrow_move_assignable_v<MoveOnlyValue>);
static_assert(!std::is_copy_constructible_v<xrpc::MpscQueue<int>>);
static_assert(!std::is_move_constructible_v<xrpc::MpscQueue<int>>);

TEST(MpscQueueTest, RejectsZeroCapacity) { EXPECT_THROW((xrpc::MpscQueue<int>(0)), std::invalid_argument); }

TEST(MpscQueueTest, ReportsConfiguredCapacity) {
  xrpc::MpscQueue<int> queue(3);
  EXPECT_EQ(queue.capacity(), 3U);
}

TEST(MpscQueueTest, EmptyQueueDoesNotModifyOutput) {
  xrpc::MpscQueue<int> queue(2);
  int output = 42;

  EXPECT_FALSE(queue.TryPop(output));
  EXPECT_EQ(output, 42);
}

TEST(MpscQueueTest, StoresExactlyCapacityAndPreservesFifoOrder) {
  xrpc::MpscQueue<MoveOnlyValue> queue(3);  // Deliberately not a power of two.

  MoveOnlyValue first(1);
  MoveOnlyValue second(2);
  MoveOnlyValue third(3);
  ASSERT_TRUE(queue.TryPush(std::move(first)));
  ASSERT_TRUE(queue.TryPush(std::move(second)));
  ASSERT_TRUE(queue.TryPush(std::move(third)));
  EXPECT_EQ(first.value, MoveOnlyValue::MovedFrom);
  EXPECT_EQ(second.value, MoveOnlyValue::MovedFrom);
  EXPECT_EQ(third.value, MoveOnlyValue::MovedFrom);

  MoveOnlyValue output(0);
  ASSERT_TRUE(queue.TryPop(output));
  EXPECT_EQ(output.value, 1);
  ASSERT_TRUE(queue.TryPop(output));
  EXPECT_EQ(output.value, 2);
  ASSERT_TRUE(queue.TryPop(output));
  EXPECT_EQ(output.value, 3);
  EXPECT_FALSE(queue.TryPop(output));
}

TEST(MpscQueueTest, FullQueueDoesNotConsumeRejectedValue) {
  xrpc::MpscQueue<MoveOnlyValue> queue(1);
  MoveOnlyValue accepted(7);
  MoveOnlyValue rejected(8);

  ASSERT_TRUE(queue.TryPush(std::move(accepted)));
  EXPECT_FALSE(queue.TryPush(std::move(rejected)));
  EXPECT_EQ(rejected.value, 8);

  MoveOnlyValue output(0);
  ASSERT_TRUE(queue.TryPop(output));
  EXPECT_EQ(output.value, 7);
  ASSERT_TRUE(queue.TryPush(std::move(rejected)));
  ASSERT_TRUE(queue.TryPop(output));
  EXPECT_EQ(output.value, 8);
}

TEST(MpscQueueTest, ReusesSlotsAcrossManyWraparounds) {
  constexpr std::size_t Capacity = 3;
  constexpr std::size_t Rounds = 10'000;
  xrpc::MpscQueue<MoveOnlyValue> queue(Capacity);
  MoveOnlyValue output(0);

  for (std::size_t round = 0; round < Rounds; ++round) {
    for (std::size_t offset = 0; offset < Capacity; ++offset) {
      MoveOnlyValue input(static_cast<int>(round * Capacity + offset));
      ASSERT_TRUE(queue.TryPush(std::move(input)));
    }
    for (std::size_t offset = 0; offset < Capacity; ++offset) {
      ASSERT_TRUE(queue.TryPop(output));
      EXPECT_EQ(output.value, static_cast<int>(round * Capacity + offset));
    }
  }
}

TEST(MpscQueueTest, DestructionDestroysValuesThatWereNotPopped) {
  std::atomic<int> live{0};
  {
    xrpc::MpscQueue<TrackedValue> queue(2);
    {
      TrackedValue input(live);
      ASSERT_TRUE(queue.TryPush(std::move(input)));
    }
    EXPECT_EQ(live.load(), 1);
  }
  EXPECT_EQ(live.load(), 0);
}

TEST(MpscQueueTest, SupportsConcurrentProducersWithoutLossDuplicationOrProducerReordering) {
  constexpr std::size_t ProducerCount = 4;
  constexpr std::size_t MessagesPerProducer = 20'000;
  constexpr std::size_t TotalMessages = ProducerCount * MessagesPerProducer;
  constexpr auto Deadline = 4s;

  xrpc::MpscQueue<Message> queue(257);  // Exercise non-power-of-two wraparound.
  std::atomic<bool> start{false};
  std::atomic<bool> timed_out{false};
  std::vector<std::jthread> producers;
  producers.reserve(ProducerCount);

  for (std::size_t producer = 0; producer < ProducerCount; ++producer) {
    producers.emplace_back([&, producer]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      const auto deadline = std::chrono::steady_clock::now() + Deadline;
      for (std::size_t sequence = 0; sequence < MessagesPerProducer; ++sequence) {
        Message message(producer, sequence);
        while (!queue.TryPush(std::move(message))) {
          if (std::chrono::steady_clock::now() >= deadline) {
            timed_out.store(true, std::memory_order_relaxed);
            return;
          }
          std::this_thread::yield();
        }
      }
    });
  }

  std::vector<std::vector<bool>> seen(ProducerCount, std::vector<bool>(MessagesPerProducer, false));
  std::vector<std::size_t> next_sequence(ProducerCount, 0);
  std::size_t consumed = 0;
  Message output(0, 0);
  const auto deadline = std::chrono::steady_clock::now() + Deadline;
  start.store(true, std::memory_order_release);

  while (consumed < TotalMessages && !timed_out.load(std::memory_order_relaxed)) {
    if (!queue.TryPop(output)) {
      if (std::chrono::steady_clock::now() >= deadline) {
        timed_out.store(true, std::memory_order_relaxed);
        break;
      }
      std::this_thread::yield();
      continue;
    }

    ASSERT_LT(output.producer, ProducerCount);
    ASSERT_LT(output.sequence, MessagesPerProducer);
    EXPECT_FALSE(seen[output.producer][output.sequence]);
    seen[output.producer][output.sequence] = true;
    EXPECT_EQ(output.sequence, next_sequence[output.producer]);
    ++next_sequence[output.producer];
    ++consumed;
  }

  for (auto &producer : producers) {
    producer.join();
  }

  EXPECT_FALSE(timed_out.load());
  EXPECT_EQ(consumed, TotalMessages);
  for (std::size_t producer = 0; producer < ProducerCount; ++producer) {
    EXPECT_EQ(next_sequence[producer], MessagesPerProducer);
  }
}

}  // namespace
