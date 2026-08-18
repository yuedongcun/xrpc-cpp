/** @file rpc_frame_stream.h @brief Declares incremental RPC frame stream decoding. */

#pragma once

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "protocol/frame_codec.h"
#include "protocol/protocol_message.h"

namespace xrpc {

class RawRequestBatch final {
 public:
  void Push(RawRequest request);

  [[nodiscard]] auto empty() const -> bool { return size() == 0; }

  [[nodiscard]] auto size() const -> std::size_t;

  [[nodiscard]] auto operator[](std::size_t index) const -> const RawRequest &;

  template <typename Callback>
  auto ConsumeEach(Callback &&callback) -> bool {
    if (first_request_.has_value() && !callback(std::move(*first_request_))) {
      return false;
    }
    return std::ranges::all_of(additional_requests_,
                               [&callback](RawRequest &request) -> bool { return callback(std::move(request)); });
  }

 private:
  std::optional<RawRequest> first_request_;

  std::vector<RawRequest> additional_requests_;
};

struct FrameStreamFeedResult {
  RawRequestBatch requests_;

  bool closed_ = false;
};

class RpcFrameStream final {
 public:
  explicit RpcFrameStream(ProtocolLimits protocol_limits = {});

  [[nodiscard]] auto FeedBytes(std::string_view bytes) -> FrameStreamFeedResult;

  [[nodiscard]] auto EncodeResponse(RawResponse &&response) const -> std::string;

 private:
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

  [[nodiscard]] auto DrainReadableRequests() -> RawRequestBatch;

  ByteBuffer buffer_;

  RequestHeaderDecodeCache request_header_cache_;

  ProtocolLimits protocol_limits_;

  bool closed_ = false;
};

}  // namespace xrpc
