#pragma once

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "protocol/frame_codec.h"
#include "rpc/raw_message.h"

namespace xrpc {

/**
 * @brief Small request batch optimized for the common one-request read.
 *
 * The first request is stored without allocating a vector. Additional requests are used only when a read contains
 * pipelined frames.
 */
class RawRequestBatch final {
 public:
  /** @brief Adds a decoded request to the batch. */
  void Push(RawRequest request);

  /** @return true when the batch has no decoded requests. */
  [[nodiscard]] auto empty() const -> bool { return size() == 0; }

  /** @return Number of decoded requests stored in the batch. */
  [[nodiscard]] auto size() const -> std::size_t;

  /** @return Request at `index`. */
  [[nodiscard]] auto operator[](std::size_t index) const -> const RawRequest &;

  /**
   * @brief Moves every request into `callback` until the callback returns false.
   *
   * @return true when every request was consumed, false when the callback stopped early.
   */
  template <typename Callback>
  auto ConsumeEach(Callback &&callback) -> bool {
    if (first_request_.has_value() && !callback(std::move(*first_request_))) {
      return false;
    }
    return std::ranges::all_of(additional_requests_,
                               [&callback](RawRequest &request) { return callback(std::move(request)); });
  }

 private:
  /** @brief Inline storage for the common single-request read. */
  std::optional<RawRequest> first_request_;

  /** @brief Extra decoded requests from the same read buffer. */
  std::vector<RawRequest> additional_requests_;
};

/** @brief Result of feeding bytes into one server-side RPC frame stream. */
struct FrameStreamFeedResult {
  /** @brief Complete requests decoded from the buffered stream bytes. */
  RawRequestBatch requests_;

  /** @brief True when a protocol error closed the frame stream. */
  bool closed_ = false;
};

/**
 * @brief Per-connection framing state for decoding requests and encoding responses over a TCP byte stream.
 *
 * The frame stream owns one byte buffer and one request-header decode cache, so callers must feed bytes from only one
 * TCP stream. It is not synchronized; the owning `TcpConnection` keeps access on the event-loop thread.
 */
class RpcFrameStream final {
 public:
  /** @brief Creates framing state with the frame limits for one TCP byte stream. */
  explicit RpcFrameStream(ProtocolLimits protocol_limits = {});

  /**
   * @brief Appends stream bytes and drains all complete request frames.
   *
   * @param bytes Newly read TCP stream bytes.
   * @return Decoded request batch and closed flag.
   */
  [[nodiscard]] auto FeedBytes(std::string_view bytes) -> FrameStreamFeedResult;

  /** @return Encoded response frame bytes after mapping raw status fields to protocol fields. */
  [[nodiscard]] auto EncodeResponse(RawResponse &&response) const -> std::string;

 private:
  /**
   * @brief Append/consume buffer that preserves unread bytes across partial frame decode attempts.
   *
   * Consumed prefix storage is reclaimed before later appends, while unread bytes remain contiguous.
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

  /** @brief Drains all complete request frames currently buffered in the frame stream. */
  [[nodiscard]] auto DrainReadableRequests() -> RawRequestBatch;

  /** @brief Buffered TCP stream bytes not yet consumed by the frame codec. */
  ByteBuffer buffer_;

  /** @brief Per-connection cache for repeated request protobuf headers. */
  RequestHeaderDecodeCache request_header_cache_;

  /** @brief Protocol limits applied to every decoded and encoded frame. */
  ProtocolLimits protocol_limits_;

  /** @brief True after a protocol error; further feed operations do not reopen the frame stream. */
  bool closed_ = false;
};

}  // namespace xrpc
