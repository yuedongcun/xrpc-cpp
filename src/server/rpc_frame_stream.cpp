/**
 * @file rpc_frame_stream.cpp
 * @brief Implements buffered incremental RPC request decoding.
 */

#include "server/rpc_frame_stream.h"

#include <cassert>
#include <string>
#include <utility>

#include "common/xrpc_exception.h"
#include "protocol/frame_codec.h"
#include "protocol/rpc_envelope.h"

namespace xrpc {

void RpcFrameStream::ByteBuffer::Append(std::string_view bytes) {
  if (bytes.empty()) {
    return;
  }

  // Keep appending after the readable suffix while the string still has tail
  // capacity. Reclaim the consumed prefix only when that tail space is not
  // large enough for the new input.
  if (bytes.size() > buffer_.capacity() - buffer_.size()) {
    Compact();
  }
  buffer_.append(bytes.data(), bytes.size());
}

auto RpcFrameStream::ByteBuffer::ReadableBytes() const -> std::string_view {
  if (ReadableSize() == 0) {
    return {};
  }
  return {buffer_.data() + read_offset_, ReadableSize()};
}

void RpcFrameStream::ByteBuffer::Consume(std::size_t n) {
  if (n > ReadableSize()) {
    throw LifecycleException("ByteBuffer::Consume exceeds readable bytes");
  }
  read_offset_ += n;
  if (read_offset_ == buffer_.size()) {
    buffer_.clear();
    read_offset_ = 0;
  }
}

auto RpcFrameStream::ByteBuffer::ReadableSize() const -> std::size_t { return buffer_.size() - read_offset_; }

auto RpcFrameStream::ByteBuffer::Empty() const -> bool { return ReadableSize() == 0; }

void RpcFrameStream::ByteBuffer::Compact() {
  if (read_offset_ == 0) {
    return;
  }

  // Consume() clears a fully consumed buffer, so a non-zero offset here must
  // always point inside a non-empty readable suffix.
  assert(read_offset_ < buffer_.size());

  // Delay prefix removal until new bytes are appended so repeated frame
  // consumption does not shift the remaining buffer after every decode.
  buffer_.erase(0, read_offset_);
  read_offset_ = 0;
}

void RequestEnvelopeBatch::Push(RequestEnvelope request) {
  if (!first_request_.has_value()) {
    first_request_.emplace(std::move(request));
    return;
  }
  additional_requests_.push_back(std::move(request));
}

auto RequestEnvelopeBatch::size() const -> std::size_t {
  return (first_request_.has_value() ? 1U : 0U) + additional_requests_.size();
}

auto RequestEnvelopeBatch::operator[](std::size_t index) const -> const RequestEnvelope & {
  if (!first_request_.has_value()) {
    return additional_requests_[index];
  }
  if (index == 0) {
    return *first_request_;
  }
  return additional_requests_[index - 1U];
}

RpcFrameStream::RpcFrameStream(ProtocolLimits protocol_limits) : protocol_limits_(protocol_limits) {}

auto RpcFrameStream::FeedBytes(std::string_view bytes) -> FrameStreamFeedResult {
  if (closed_) {
    return {.requests_ = {}, .closed_ = true};
  }

  buffer_.Append(bytes);
  RequestEnvelopeBatch requests = DecodeAvailableRequests();
  return {.requests_ = std::move(requests), .closed_ = closed_};
}

auto RpcFrameStream::EncodeResponse(ResponseEnvelope &&response) const -> std::string {
  FrameCodec codec(protocol_limits_);
  return codec.Encode(response);
}

auto RpcFrameStream::DecodeAvailableRequests() -> RequestEnvelopeBatch {
  RequestEnvelopeBatch requests;
  FrameCodec codec(protocol_limits_);

  while (!buffer_.Empty() && !closed_) {
    FrameDecodeResult decoded = codec.Decode(buffer_.ReadableBytes());

    // Preserve an incomplete trailing frame for the next FeedBytes() call.
    if (decoded.error_ == ProtocolError::NeedMoreData) {
      break;
    }

    // Any non-recoverable protocol error permanently closes this stream.
    if (decoded.error_ != ProtocolError::Ok || !decoded.request_.has_value()) {
      closed_ = true;
      break;
    }

    buffer_.Consume(decoded.consumed_);
    requests.Push(std::move(*decoded.request_));
  }

  return requests;
}

}  // namespace xrpc
