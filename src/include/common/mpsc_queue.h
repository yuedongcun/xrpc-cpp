/**
 * @file mpsc_queue.h
 * @brief Declares a bounded multi-producer, single-consumer queue.
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace xrpc {

/**
 * @brief Fixed-capacity FIFO queue with multiple producers and one consumer.
 *
 * Producers may call TryPush() concurrently. Exactly one consumer may call
 * TryPop(); concurrent consumers are outside the contract. Destruction requires
 * all producers and the consumer to have stopped using the queue.
 *
 * The queue accepts move-only, non-default-constructible values. A failed push
 * must leave `value` untouched so callers can retry or use an overflow path.
 * Ordering is FIFO for values successfully reserved by the queue, which also
 * preserves each producer's program order.
 */
template <typename T>
class MpscQueue final {
 public:
  /**
   * @brief Creates a queue with room for exactly `capacity` values.
   * @throws std::invalid_argument when capacity is zero.
   */
  explicit MpscQueue(std::size_t capacity) : capacity_(ValidateCapacity(capacity)), slots_(new Slot[capacity_]) {
    static_assert(std::is_nothrow_move_constructible_v<T>, "MpscQueue requires T to be nothrow move constructible");
    static_assert(std::is_nothrow_move_assignable_v<T>, "MpscQueue requires T to be nothrow move assignable");
  }

  /**
   * @brief Destroys the queue and any values that were not popped.
   *
   * No producer or consumer may access the queue during destruction.
   */
  ~MpscQueue() noexcept {
    for (std::size_t index = 0; index < capacity_; ++index) {
      Slot &slot = slots_[index];
      if (slot.ready_.load(std::memory_order_relaxed)) {
        std::destroy_at(slot.Value());
      }
    }
  }

  MpscQueue(const MpscQueue &) = delete;
  auto operator=(const MpscQueue &) -> MpscQueue & = delete;
  MpscQueue(MpscQueue &&) = delete;
  auto operator=(MpscQueue &&) -> MpscQueue & = delete;

  /**
   * @brief Attempts to enqueue one value without blocking.
   *
   * On success, constructs a queue element by moving from `value` and returns
   * true. When the queue is full, returns false without moving from or otherwise
   * modifying `value`.
   *
   * Multiple producers may call this function concurrently. T's move
   * construction used by the successful path must not throw; once a producer
   * reserves a position, an exception cannot leave an unpublished hole.
   */
  [[nodiscard]] auto TryPush(T &&value) -> bool {
    while (true) {
      const std::size_t dequeue_position = dequeue_position_.load(std::memory_order_acquire);
      const std::size_t enqueue_position = enqueue_position_.load(std::memory_order_relaxed);
      if (enqueue_position - dequeue_position >= capacity_) {
        return false;
      }

      std::size_t expected = enqueue_position;
      if (!enqueue_position_.compare_exchange_weak(expected, enqueue_position + 1, std::memory_order_relaxed)) {
        continue;
      }

      Slot &slot = slots_[enqueue_position % capacity_];
      slot.Construct(std::move(value));
      slot.ready_.store(true, std::memory_order_release);
      return true;
    }
  }

  /**
   * @brief Attempts to move the oldest published value into `output`.
   *
   * Returns false when the next FIFO position has not been published. On
   * success, moves the value into `output`, destroys the slot value, and makes
   * the slot reusable by a producer.
   *
   * Exactly one thread may call this function. T's move assignment used by the
   * successful path must not throw, otherwise the queue cannot safely publish
   * the slot as reusable.
   */
  [[nodiscard]] auto TryPop(T &output) -> bool {
    const std::size_t enqueue_position = enqueue_position_.load(std::memory_order_acquire);
    const std::size_t dequeue_position = dequeue_position_.load(std::memory_order_relaxed);
    if (enqueue_position == dequeue_position) {
      return false;
    }

    Slot &slot = slots_[dequeue_position % capacity_];
    if (!slot.ready_.load(std::memory_order_acquire)) {
      return false;
    }

    output = std::move(*slot.Value());
    std::destroy_at(slot.Value());
    slot.ready_.store(false, std::memory_order_relaxed);
    dequeue_position_.store(dequeue_position + 1, std::memory_order_release);
    return true;
  }

  /** @return The immutable maximum number of queued values. */
  [[nodiscard]] auto capacity() const noexcept -> std::size_t { return capacity_; }

 private:
  struct Slot final {
    void Construct(T &&value) noexcept { std::construct_at(reinterpret_cast<T *>(storage_), std::move(value)); }

    [[nodiscard]] auto Value() noexcept -> T * { return std::launder(reinterpret_cast<T *>(storage_)); }

    alignas(T) std::byte storage_[sizeof(T)];
    std::atomic<bool> ready_{false};
  };

  [[nodiscard]] static auto ValidateCapacity(std::size_t capacity) -> std::size_t {
    if (capacity == 0) {
      throw std::invalid_argument("MpscQueue capacity must be greater than zero");
    }
    return capacity;
  }

  const std::size_t capacity_;
  std::unique_ptr<Slot[]> slots_;

  // Producers contend on this cache line. The single consumer advances its
  // position on a separate cache line to avoid false sharing between the two.
  alignas(64) std::atomic<std::size_t> enqueue_position_{0};
  alignas(64) std::atomic<std::size_t> dequeue_position_{0};
};

}  // namespace xrpc
