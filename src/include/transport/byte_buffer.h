#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace xrpc {

/**
 * @brief Append/consume byte buffer for stream decoders.
 *
 * `Consume()` advances a read offset and `Compact()` reclaims storage once consumed bytes dominate the buffer. The
 * buffer keeps stable ownership of unread bytes between partial frame decode attempts.
 */
class ByteBuffer final {
 public:
  /** @brief Appends newly read stream bytes to the unread suffix. */
  void Append(std::string_view bytes);

  /** @return View over unread buffered bytes. */
  [[nodiscard]] auto ReadableBytes() const -> std::string_view;

  /** @brief Marks `n` readable bytes as consumed and compacts when worthwhile. */
  void Consume(std::size_t n);

  /** @return true when no unread bytes are buffered. */
  [[nodiscard]] auto Empty() const -> bool;

 private:
  /** @return Number of unread bytes currently buffered. */
  [[nodiscard]] auto ReadableSize() const -> std::size_t;

  /** @brief Reclaims consumed prefix storage when it dominates the buffer. */
  void Compact();

  std::string buffer_;
  std::size_t read_offset_ = 0;
};

}  // namespace xrpc
