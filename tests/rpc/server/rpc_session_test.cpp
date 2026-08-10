#include "rpc/server/rpc_session.h"

#include <cstdint>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "protocol/fixed_header.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_error.h"
#include "protocol/protocol_message.h"

namespace {

auto MakeRequestFrame(std::string payload, std::uint64_t request_id) -> std::string {
  xrpc::ProtocolRequest protocol_req;
  protocol_req.request_id_ = request_id;
  protocol_req.service_name_ = "EchoService";
  protocol_req.method_name_ = "Echo";
  protocol_req.payload_ = std::move(payload);

  xrpc::FrameCodec codec;
  return codec.EncodeRequest(protocol_req);
}

auto DecodeResponseFrame(const std::string &frame) -> xrpc::ProtocolResponse {
  xrpc::FrameCodec codec;
  xrpc::DecodeResult decoded = codec.TryDecode(frame);
  EXPECT_EQ(decoded.error_, xrpc::ProtocolError::Ok);
  EXPECT_EQ(decoded.consumed_, frame.size());
  EXPECT_TRUE(decoded.response_.has_value());
  return *decoded.response_;
}

}  // namespace

TEST(RpcSessionTest, FeedBytesCollectsRequestsWithoutDispatching) {
  xrpc::RpcSession session;
  const std::string frame_one = MakeRequestFrame("hello-1", 101);
  const std::string frame_two = MakeRequestFrame("hello-2", 102);

  const xrpc::SessionFeedResult fed = session.FeedBytes(frame_one + frame_two);
  EXPECT_FALSE(fed.closed_);
  ASSERT_EQ(fed.requests_.size(), 2U);
  EXPECT_EQ(fed.requests_[0].request_id_, 101U);
  EXPECT_EQ(fed.requests_[1].request_id_, 102U);
  EXPECT_EQ(fed.requests_[0].payload_, "hello-1");
  EXPECT_EQ(fed.requests_[1].payload_, "hello-2");
}

TEST(RpcSessionTest, FeedBytesHandlesRepeatedAndChangedRequestHeaders) {
  xrpc::RpcSession session;
  const std::string repeated_one = MakeRequestFrame("hello-1", 101);
  const std::string repeated_two = MakeRequestFrame("hello-2", 102);

  xrpc::ProtocolRequest different_header_request;
  different_header_request.request_id_ = 103;
  different_header_request.service_name_ = "OtherService";
  different_header_request.method_name_ = "OtherMethod";
  different_header_request.payload_ = "payload";

  xrpc::FrameCodec codec;
  const std::string changed = codec.EncodeRequest(different_header_request);

  const xrpc::SessionFeedResult fed = session.FeedBytes(repeated_one + repeated_two + changed);
  EXPECT_FALSE(fed.closed_);
  ASSERT_EQ(fed.requests_.size(), 3U);
  EXPECT_EQ(fed.requests_[0].service_name_, "EchoService");
  EXPECT_EQ(fed.requests_[0].method_name_, "Echo");
  EXPECT_EQ(fed.requests_[1].service_name_, "EchoService");
  EXPECT_EQ(fed.requests_[1].method_name_, "Echo");
  EXPECT_EQ(fed.requests_[2].service_name_, "OtherService");
  EXPECT_EQ(fed.requests_[2].method_name_, "OtherMethod");
}

TEST(RpcSessionTest, FeedBytesRejectsCorruptedHeaderAfterCacheWarmup) {
  xrpc::RpcSession session;
  const xrpc::SessionFeedResult first = session.FeedBytes(MakeRequestFrame("hello", 101));
  EXPECT_FALSE(first.closed_);
  ASSERT_EQ(first.requests_.size(), 1U);

  std::string corrupted_header;
  corrupted_header.push_back(static_cast<char>(0x0A));

  xrpc::FixedHeader header;
  header.message_type_ = xrpc::MessageType::Request;
  header.request_id_ = 102;
  header.header_len_ = static_cast<std::uint32_t>(corrupted_header.size());
  header.payload_len_ = 0;

  std::string corrupted_frame = xrpc::FixedHeader::Encode(header);
  corrupted_frame.append(corrupted_header);

  const xrpc::SessionFeedResult second = session.FeedBytes(corrupted_frame);
  EXPECT_TRUE(second.closed_);
  EXPECT_TRUE(second.requests_.empty());
}

TEST(RpcSessionTest, FeedBytesKeepsHalfPacketUntilComplete) {
  xrpc::RpcSession session;
  const std::string frame = MakeRequestFrame("half", 201);
  const std::string first_half = frame.substr(0, frame.size() / 2);
  const std::string second_half = frame.substr(frame.size() / 2);

  const xrpc::SessionFeedResult first = session.FeedBytes(first_half);
  EXPECT_FALSE(first.closed_);
  EXPECT_TRUE(first.requests_.empty());

  const xrpc::SessionFeedResult second = session.FeedBytes(second_half);
  EXPECT_FALSE(second.closed_);
  ASSERT_EQ(second.requests_.size(), 1U);
  EXPECT_EQ(second.requests_[0].request_id_, 201U);
  EXPECT_EQ(second.requests_[0].payload_, "half");
}

TEST(RpcSessionTest, FeedBytesClosesSessionWhenDeclaredPayloadExceedsDefaultLimit) {
  xrpc::RpcSession session;
  xrpc::FixedHeader header;
  header.payload_len_ = static_cast<std::uint32_t>(xrpc::ProtocolLimits::DEFAULT_MAX_PAYLOAD_SIZE + 1U);

  const xrpc::SessionFeedResult result = session.FeedBytes(xrpc::FixedHeader::Encode(header));

  EXPECT_TRUE(result.closed_);
  EXPECT_TRUE(result.requests_.empty());
}

TEST(RpcSessionTest, FeedBytesClosesSessionWhenPayloadExceedsConfiguredLimit) {
  xrpc::RpcSession session(xrpc::MakeProtocolLimits(3));

  xrpc::ProtocolRequest request;
  request.request_id_ = 501;
  request.service_name_ = "EchoService";
  request.method_name_ = "Echo";
  request.payload_ = "1234";

  xrpc::FrameCodec default_codec;
  const xrpc::SessionFeedResult result = session.FeedBytes(default_codec.EncodeRequest(request));

  EXPECT_TRUE(result.closed_);
  EXPECT_TRUE(result.requests_.empty());
}

TEST(RpcSessionTest, EncodeResponseBuildsResponseFrame) {
  xrpc::RpcSession session;

  xrpc::RawResponse response;
  response.request_id_ = 301;
  response.status_ = xrpc::Status::Ok();
  response.payload_ = "payload";

  const std::string frame = session.EncodeResponse(std::move(response));
  const xrpc::ProtocolResponse decoded = DecodeResponseFrame(frame);
  EXPECT_EQ(decoded.request_id_, 301U);
  EXPECT_EQ(decoded.error_code_, 0);
  EXPECT_EQ(decoded.payload_, "payload");
}

TEST(RpcSessionTest, EncodeResponseMapsRawErrorStatus) {
  xrpc::RpcSession session;
  xrpc::RawResponse raw_response;
  raw_response.request_id_ = 402;
  raw_response.status_ = {xrpc::StatusCode::Internal, "handler failed"};

  const std::string response_frame = session.EncodeResponse(std::move(raw_response));
  const xrpc::ProtocolResponse response = DecodeResponseFrame(response_frame);

  EXPECT_EQ(response.request_id_, 402U);
  EXPECT_EQ(response.error_code_, static_cast<std::int32_t>(xrpc::StatusCode::Internal));
  EXPECT_EQ(response.error_text_, "handler failed");
}
