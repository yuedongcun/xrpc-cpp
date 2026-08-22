/**
 * @file rpc_frame_stream.h
 * @brief Defines incremental RPC request decoding over a TCP byte stream.
 *
 * RpcFrameStream buffers incomplete input across recv operations, extracts all
 * complete request frames currently available, and marks the stream closed
 * after an unrecoverable protocol error.
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "protocol/frame_codec.h"
#include "protocol/rpc_envelope.h"

namespace xrpc {

/**
 * @brief Move-oriented batch of decoded request envelopes.
 *
 * The first envelope is stored separately so the common single-request case
 * does not require allocating the additional-request vector. Further
 * envelopes are appended to the vector.
 */
class RequestEnvelopeBatch final {
 public:
  void Push(RequestEnvelope request);

  [[nodiscard]] auto empty() const -> bool { return size() == 0; }

  [[nodiscard]] auto size() const -> std::size_t;

  [[nodiscard]] auto operator[](std::size_t index) const -> const RequestEnvelope &;

  /**
   * @brief Moves each envelope into `callback` in decode order.
   *
   * Iteration stops and returns `false` when the callback rejects an envelope;
   * otherwise all envelopes are consumed and `true` is returned.
   */
  template <typename Callback>
  auto ConsumeEach(Callback &&callback) -> bool {
    if (first_request_.has_value() && !callback(std::move(*first_request_))) {
      return false;
    }
    return std::ranges::all_of(additional_requests_,
                               [&callback](RequestEnvelope &request) -> bool { return callback(std::move(request)); });
  }

 private:
  std::optional<RequestEnvelope> first_request_;

  std::vector<RequestEnvelope> additional_requests_;
};

/**
 * @brief Request envelopes decoded from one feed together with the resulting stream state.
 */
struct FrameStreamFeedResult {
  RequestEnvelopeBatch requests_;

  // True when the frame stream can no longer accept or decode input.
  bool closed_ = false;
};

/**
 * @brief Stateful decoder for RPC frames received from one TCP connection.
 *
 * `FeedBytes()` accepts arbitrary TCP byte chunks: a chunk may contain a
 * partial frame, one complete frame, or multiple frames. Incomplete bytes are
 * retained across calls until enough data arrives to decode a request.
 *
 * A protocol error permanently closes the stream. Once closed, later calls to
 * `FeedBytes()` produce no request envelopes.
 */
class RpcFrameStream final {
 public:
  explicit RpcFrameStream(ProtocolLimits protocol_limits = {});

  /**
   * @brief Appends received bytes and decodes all complete request frames available.
   *
   * Incomplete trailing bytes remain buffered for the next feed.
   */
  [[nodiscard]] auto FeedBytes(std::string_view bytes) -> FrameStreamFeedResult;

  [[nodiscard]] auto EncodeResponse(ResponseEnvelope &&response) const -> std::string;

 private:
  /**
   * @brief Append-only byte storage with a movable readable window.
   *
   * Consuming bytes advances `read_offset_` without immediately moving the
   * remaining data. `Compact()` removes the consumed prefix before later
   * appends when necessary.
   */
  class ByteBuffer final {
   public:
    void Append(std::string_view bytes);

    [[nodiscard]] auto ReadableBytes() const -> std::string_view;

    void Consume(std::size_t n);

    [[nodiscard]] auto Empty() const -> bool;

   private:
    [[nodiscard]] auto ReadableSize() const -> std::size_t;

    void Compact();

    std::string buffer_;
    std::size_t read_offset_ = 0;
  };

  [[nodiscard]] auto DecodeAvailableRequests() -> RequestEnvelopeBatch;

  ByteBuffer buffer_;

  ProtocolLimits protocol_limits_;

  bool closed_ = false;
};

}  // namespace xrpc
