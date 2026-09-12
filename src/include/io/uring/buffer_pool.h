/**
 * @file buffer_pool.h
 * @brief Declares a provided-buffer ring and its move-only buffer lease.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include <liburing.h>

#include <xrpc/status.h>

namespace xrpc::io {

struct UringBufferPoolConfig final {
  std::uint32_t buffer_count_ = 512;
  std::uint32_t buffer_size_ = 16U * 1024U;
  std::uint16_t group_id_ = 1;
};

class UringProvidedBufferPool;

/**
 * @brief Move-only ownership of one buffer selected by io_uring.
 *
 * The pool is re-provided with the buffer when this lease is destroyed. The
 * pool and its owning UringContext must outlive every lease.
 */
class UringBuffer final {
 public:
  UringBuffer() = default;

  ~UringBuffer();

  UringBuffer(const UringBuffer &) = delete;
  auto operator=(const UringBuffer &) -> UringBuffer & = delete;

  UringBuffer(UringBuffer &&other) noexcept;
  auto operator=(UringBuffer &&other) noexcept -> UringBuffer &;

  [[nodiscard]] auto Bytes() const noexcept -> std::span<const std::byte>;

  [[nodiscard]] auto Empty() const noexcept -> bool;

 private:
  friend class UringProvidedBufferPool;

  UringBuffer(UringProvidedBufferPool &pool, std::uint16_t buffer_id, std::span<const std::byte> bytes) noexcept;

  void Reset() noexcept;

  UringProvidedBufferPool *pool_ = nullptr;
  std::uint16_t buffer_id_ = 0;
  std::span<const std::byte> bytes_;
};

/**
 * @brief Buffer storage registered with one io_uring as a provided-buffer ring.
 *
 * Registration and all buffer ownership transitions are confined to the
 * owning UringContext thread.
 */
class UringProvidedBufferPool final {
 public:
  [[nodiscard]] static auto Register(io_uring &ring, UringBufferPoolConfig config)
      -> StatusOr<std::unique_ptr<UringProvidedBufferPool>>;

  ~UringProvidedBufferPool();

  UringProvidedBufferPool(const UringProvidedBufferPool &) = delete;
  auto operator=(const UringProvidedBufferPool &) -> UringProvidedBufferPool & = delete;

  UringProvidedBufferPool(UringProvidedBufferPool &&) = delete;
  auto operator=(UringProvidedBufferPool &&) -> UringProvidedBufferPool & = delete;

  [[nodiscard]] auto Acquire(std::uint16_t buffer_id, std::size_t size) -> UringBuffer;

  [[nodiscard]] auto BufferSize() const noexcept -> std::uint32_t;

  [[nodiscard]] auto GroupId() const noexcept -> std::uint16_t;

 private:
  friend class UringBuffer;

  UringProvidedBufferPool(io_uring &ring, UringBufferPoolConfig config) noexcept;

  void Release(std::uint16_t buffer_id) noexcept;

  void Provide(std::uint16_t buffer_id, int offset) noexcept;

  io_uring *ring_ = nullptr;
  UringBufferPoolConfig config_;
  io_uring_buf_ring *buffer_ring_ = nullptr;
  std::unique_ptr<std::byte[]> storage_;
  std::vector<std::uint8_t> leased_;
  std::size_t outstanding_leases_ = 0;
};

}  // namespace xrpc::io
