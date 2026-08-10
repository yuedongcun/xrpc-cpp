#include "rpc/server/rpc_frame_stream.h"

#include <string>
#include <utility>

#include "protocol/frame_codec.h"
#include "protocol/protocol_error.h"
#include "protocol/protocol_message.h"
#include "rpc/protocol_adapter.h"

namespace xrpc {

/**
 * @brief Adds a decoded request to the batch.
 *
 * The first request stays in inline optional storage so the common one-request read avoids vector allocation.
 */
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

/**
 * @brief Returns the number of decoded requests stored in the batch.
 */
auto RawRequestBatch::size() const -> std::size_t {
  return (first_request_.has_value() ? 1U : 0U) + additional_requests_.size();
}

/**
 * @brief Returns a decoded request by index.
 */
auto RawRequestBatch::operator[](std::size_t index) const -> const RawRequest & {
  if (!first_request_.has_value()) {
    return additional_requests_[index];
  }
  if (index == 0) {
    return *first_request_;
  }
  return additional_requests_[index - 1U];
}

/** @brief Creates framing state for one TCP byte stream. */
RpcFrameStream::RpcFrameStream(ProtocolLimits protocol_limits) : protocol_limits_(protocol_limits) {}

/**
 * @brief Appends stream bytes and drains all complete request frames.
 *
 * Protocol errors mark the frame stream closed so the owning connection can stop reading from the peer.
 */
auto RpcFrameStream::FeedBytes(std::string_view bytes) -> FrameStreamFeedResult {
  if (closed_) {
    return {.requests_ = {}, .closed_ = true};
  }

  buffer_.Append(bytes);
  RawRequestBatch requests = DrainReadableRequests();
  return {.requests_ = std::move(requests), .closed_ = closed_};
}

/**
 * @brief Converts and encodes a raw response with this frame stream's limits.
 */
auto RpcFrameStream::EncodeResponse(RawResponse &&response) const -> std::string {
  FrameCodec codec(protocol_limits_);
  return codec.EncodeResponse(ToProtocolResponse(std::move(response)));
}

/**
 * @brief Drains complete request frames already buffered in the frame stream.
 *
 * Partial frames remain buffered. Malformed complete frames close the frame stream and stop the drain.
 */
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
    requests.Push(ToRawRequest(std::move(*decoded.request_)));
  }

  return requests;
}

}  // namespace xrpc
