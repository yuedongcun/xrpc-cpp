/**
 * @file buffer_pool.cpp
 * @brief Implements io_uring provided-buffer registration and buffer leases.
 *
 * UringProvidedBufferPool registers buffers for kernel selection. Acquire()
 * wraps a selected buffer in a move-only UringBuffer lease; destroying or
 * resetting that lease returns the buffer to the pool for reuse.
 */

#include "io/uring/buffer_pool.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <system_error>
#include <utility>

#include <liburing.h>

#include "common/abort.h"

namespace xrpc::io {
namespace {

constexpr std::uint32_t MAX_BUFFER_COUNT = 1U << 15U;

auto ValidateConfig(const UringBufferPoolConfig &config) -> Status {
  if (config.buffer_count_ == 0 || (config.buffer_count_ & (config.buffer_count_ - 1U)) != 0) {
    return {StatusCode::InvalidArgument, "provided buffer count must be a power of two"};
  }
  if (config.buffer_count_ > MAX_BUFFER_COUNT) {
    return {StatusCode::InvalidArgument, "provided buffer count must not exceed 32768"};
  }
  if (config.buffer_size_ == 0) {
    return {StatusCode::InvalidArgument, "provided buffer size must be greater than zero"};
  }
  if (config.buffer_count_ > std::numeric_limits<std::size_t>::max() / config.buffer_size_) {
    return {StatusCode::InvalidArgument, "provided buffer storage size overflows size_t"};
  }
  return Status::Ok();
}

auto RegistrationError(int error_code) -> Status {
  std::string message("io_uring provided buffer registration failed");
  if (error_code != 0) {
    message.append(": ");
    message.append(std::error_code(error_code, std::generic_category()).message());
  }
  return {StatusCode::Unavailable, std::move(message)};
}

}  // namespace

UringBuffer::UringBuffer(UringProvidedBufferPool &pool, std::uint16_t buffer_id,
                         std::span<const std::byte> bytes) noexcept
    : pool_(&pool), buffer_id_(buffer_id), bytes_(bytes) {}

UringBuffer::~UringBuffer() { Reset(); }

UringBuffer::UringBuffer(UringBuffer &&other) noexcept
    : pool_(std::exchange(other.pool_, nullptr)),
      buffer_id_(other.buffer_id_),
      bytes_(std::exchange(other.bytes_, {})) {}

auto UringBuffer::operator=(UringBuffer &&other) noexcept -> UringBuffer & {
  if (this == &other) {
    return *this;
  }
  Reset();
  pool_ = std::exchange(other.pool_, nullptr);
  buffer_id_ = other.buffer_id_;
  bytes_ = std::exchange(other.bytes_, {});
  return *this;
}

auto UringBuffer::Bytes() const noexcept -> std::span<const std::byte> { return bytes_; }

auto UringBuffer::Empty() const noexcept -> bool { return bytes_.empty(); }

void UringBuffer::Reset() noexcept {
  if (pool_ == nullptr) {
    return;
  }
  UringProvidedBufferPool *pool = std::exchange(pool_, nullptr);
  bytes_ = {};
  pool->Release(buffer_id_);
}

UringProvidedBufferPool::UringProvidedBufferPool(io_uring &ring, UringBufferPoolConfig config) noexcept
    : ring_(&ring), config_(config) {}

auto UringProvidedBufferPool::Register(io_uring &ring, UringBufferPoolConfig config)
    -> StatusOr<std::unique_ptr<UringProvidedBufferPool>> {
  const Status validation = ValidateConfig(config);
  if (!validation.ok()) {
    return StatusOr<std::unique_ptr<UringProvidedBufferPool>>(validation);
  }

  auto pool = std::unique_ptr<UringProvidedBufferPool>(new UringProvidedBufferPool(ring, config));
  int result = 0;
  pool->buffer_ring_ = io_uring_setup_buf_ring(&ring, config.buffer_count_, config.group_id_, 0, &result);
  if (pool->buffer_ring_ == nullptr) {
    return StatusOr<std::unique_ptr<UringProvidedBufferPool>>(RegistrationError(result < 0 ? -result : result));
  }

  const std::size_t storage_size =
      static_cast<std::size_t>(config.buffer_count_) * static_cast<std::size_t>(config.buffer_size_);
  pool->storage_ = std::make_unique<std::byte[]>(storage_size);
  pool->leased_.resize(config.buffer_count_);

  for (std::uint32_t index = 0; index < config.buffer_count_; ++index) {
    pool->Provide(static_cast<std::uint16_t>(index), static_cast<int>(index));
  }
  io_uring_buf_ring_advance(pool->buffer_ring_, static_cast<int>(config.buffer_count_));
  return StatusOr<std::unique_ptr<UringProvidedBufferPool>>(std::move(pool));
}

UringProvidedBufferPool::~UringProvidedBufferPool() {
  if (outstanding_leases_ != 0) {
    Abort("UringProvidedBufferPool destroyed with outstanding buffer leases");
  }
  if (buffer_ring_ != nullptr) {
    (void)io_uring_free_buf_ring(ring_, buffer_ring_, config_.buffer_count_, config_.group_id_);
  }
}

auto UringProvidedBufferPool::Acquire(std::uint16_t buffer_id, std::size_t size) -> UringBuffer {
  if (buffer_id >= config_.buffer_count_) {
    Abort("io_uring selected a provided buffer ID outside the registered pool");
  }
  if (size > config_.buffer_size_) {
    Abort("io_uring reported more bytes than the selected provided buffer can hold");
  }
  if (leased_[buffer_id] != 0) {
    Abort("io_uring selected a provided buffer that is already leased");
  }

  leased_[buffer_id] = 1;
  ++outstanding_leases_;
  ++acquires_;
  outstanding_leases_peak_ = std::max(outstanding_leases_peak_, outstanding_leases_);
  std::byte *data = storage_.get() + (static_cast<std::size_t>(buffer_id) * config_.buffer_size_);
  return {*this, buffer_id, std::span<const std::byte>(data, size)};
}

auto UringProvidedBufferPool::BufferSize() const noexcept -> std::uint32_t { return config_.buffer_size_; }

auto UringProvidedBufferPool::GroupId() const noexcept -> std::uint16_t { return config_.group_id_; }

auto UringProvidedBufferPool::SnapshotStats(bool start_window) noexcept -> BufferPoolStatsSnapshot {
  if (start_window) {
    outstanding_leases_peak_ = outstanding_leases_;
  }
  return {.acquires_ = acquires_,
          .returns_ = returns_,
          .capacity_ = config_.buffer_count_,
          .outstanding_leases_ = outstanding_leases_,
          .outstanding_leases_peak_ = outstanding_leases_peak_};
}

void UringProvidedBufferPool::Release(std::uint16_t buffer_id) noexcept {
  if (buffer_id >= config_.buffer_count_ || leased_[buffer_id] == 0 || outstanding_leases_ == 0) {
    Abort("attempted to release a provided buffer that is not leased");
  }

  Provide(buffer_id, 0);
  io_uring_buf_ring_advance(buffer_ring_, 1);
  leased_[buffer_id] = 0;
  --outstanding_leases_;
  ++returns_;
}

void UringProvidedBufferPool::Provide(std::uint16_t buffer_id, int offset) noexcept {
  const int mask = io_uring_buf_ring_mask(config_.buffer_count_);
  std::byte *data = storage_.get() + (static_cast<std::size_t>(buffer_id) * config_.buffer_size_);
  io_uring_buf_ring_add(buffer_ring_, data, config_.buffer_size_, buffer_id, mask, offset);
}

}  // namespace xrpc::io
