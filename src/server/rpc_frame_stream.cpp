#include "server/rpc_frame_stream.h"

#include <string>
#include <utility>

#include "common/xrpc_exception.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_message.h"

namespace xrpc {

void RpcFrameStream::ByteBuffer::Append(std::string_view bytes) {
  if (bytes.empty()) {
    return;
  }
  Compact();
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
  if (read_offset_ >= buffer_.size()) {
    buffer_.clear();
    read_offset_ = 0;
    return;
  }
  buffer_.erase(0, read_offset_);
  read_offset_ = 0;
}

void RawRequestBatch::Push(RawRequest request) {
  if (!first_request_.has_value()) {
    first_request_.emplace(std::move(request));
    return;
  }
  if (additional_requests_.empty()) {
    additional_requests_.reserve(16);
  }
  additional_requests_.push_back(std::move(request));
}

auto RawRequestBatch::size() const -> std::size_t {
  return (first_request_.has_value() ? 1U : 0U) + additional_requests_.size();
}

auto RawRequestBatch::operator[](std::size_t index) const -> const RawRequest & {
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
  RawRequestBatch requests = DrainReadableRequests();
  return {.requests_ = std::move(requests), .closed_ = closed_};
}

auto RpcFrameStream::EncodeResponse(RawResponse &&response) const -> std::string {
  FrameCodec codec(protocol_limits_);
  return codec.EncodeResponse(response);
}

auto RpcFrameStream::DrainReadableRequests() -> RawRequestBatch {
  RawRequestBatch requests;
  FrameCodec codec(protocol_limits_);

  while (!buffer_.Empty() && !closed_) {
    RequestDecodeResult decoded = codec.TryDecodeRequest(buffer_.ReadableBytes(), request_header_cache_);
    if (decoded.error_ == ProtocolError::NeedMoreData) {
      break;
    }

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
