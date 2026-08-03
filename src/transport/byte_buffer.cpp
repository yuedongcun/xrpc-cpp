#include "transport/byte_buffer.h"

#include <xrpc/xrpc_exception.h>

namespace xrpc {

/**
 * @brief Appends bytes after compacting any consumed prefix.
 *
 * @param bytes New readable bytes to append to the tail of the buffer.
 */
void ByteBuffer::Append(std::string_view bytes) {
  if (bytes.empty()) {
    return;
  }
  Compact();
  buffer_.append(bytes.data(), bytes.size());
}

/**
 * @brief Returns a contiguous view of all unread bytes.
 *
 * @return Borrowed view valid until the next mutating operation on this buffer.
 */
auto ByteBuffer::ReadableBytes() const -> std::string_view {
  if (ReadableSize() == 0) {
    return {};
  }
  return {buffer_.data() + read_offset_, ReadableSize()};
}

/**
 * @brief Advances the read cursor by `n` bytes.
 *
 * @param n Number of readable bytes to discard.
 * @throws LifecycleException when `n` exceeds the readable byte count.
 */
void ByteBuffer::Consume(std::size_t n) {
  if (n > ReadableSize()) {
    throw LifecycleException("ByteBuffer::Consume exceeds readable bytes");
  }
  read_offset_ += n;
  if (read_offset_ == buffer_.size()) {
    buffer_.clear();
    read_offset_ = 0;
  }
}

/** @return Number of bytes available through `ReadableBytes()`. */
auto ByteBuffer::ReadableSize() const -> std::size_t { return buffer_.size() - read_offset_; }

/** @return true when no unread bytes remain. */
auto ByteBuffer::Empty() const -> bool { return ReadableSize() == 0; }

/**
 * @brief Removes the consumed prefix so future appends keep storage bounded.
 *
 * The transport frequently consumes partial frames. Compacting before append keeps the remaining
 * readable bytes contiguous without reallocating on every consume.
 */
void ByteBuffer::Compact() {
  if (read_offset_ == 0) {
    return;
  }
  if (read_offset_ >= buffer_.size()) {
    buffer_.clear();
    read_offset_ = 0;
    return;
  }
  buffer_.erase(0, read_offset_);
  read_offset_ = 0;
}

}  // namespace xrpc
