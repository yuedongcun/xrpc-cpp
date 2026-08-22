#include "server/rpc_frame_stream.h"

#include <cstdint>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "protocol/frame_codec.h"
#include "protocol/frame_header.h"
#include "protocol/rpc_envelope.h"

namespace {

auto EncodeFrameHeader(const xrpc::FrameHeader &header) -> std::string {
  std::string bytes(xrpc::FrameHeader::SIZE, '\0');
  xrpc::FrameHeader::EncodeTo(header, bytes.data());
  return bytes;
}

auto MakeRequestFrame(std::string payload, std::uint64_t request_id) -> std::string {
  xrpc::RequestEnvelope request;
  request.request_id_ = request_id;
  request.service_name_ = "EchoService";
  request.method_name_ = "Echo";
  request.payload_ = std::move(payload);

  xrpc::FrameCodec codec;
  return codec.Encode(request);
}

auto DecodeResponseFrame(const std::string &frame) -> xrpc::ResponseEnvelope {
  xrpc::FrameCodec codec;
  xrpc::FrameDecodeResult decoded = codec.Decode(frame);
  EXPECT_EQ(decoded.error_, xrpc::ProtocolError::Ok);
  EXPECT_EQ(decoded.consumed_, frame.size());
  EXPECT_TRUE(decoded.response_.has_value());
  return *decoded.response_;
}

}  // namespace

TEST(RpcFrameStreamTest, FeedBytesCollectsRequestsWithoutDispatching) {
  xrpc::RpcFrameStream frame_stream;
  const std::string frame_one = MakeRequestFrame("hello-1", 101);
  const std::string frame_two = MakeRequestFrame("hello-2", 102);

  const xrpc::FrameStreamFeedResult fed = frame_stream.FeedBytes(frame_one + frame_two);
  EXPECT_FALSE(fed.closed_);
  ASSERT_EQ(fed.requests_.size(), 2U);
  EXPECT_EQ(fed.requests_[0].request_id_, 101U);
  EXPECT_EQ(fed.requests_[1].request_id_, 102U);
  EXPECT_EQ(fed.requests_[0].payload_, "hello-1");
  EXPECT_EQ(fed.requests_[1].payload_, "hello-2");
}

TEST(RpcFrameStreamTest, FeedBytesDecodesRepeatedAndChangedRoutingMetadata) {
  xrpc::RpcFrameStream frame_stream;
  const std::string repeated_one = MakeRequestFrame("hello-1", 101);
  const std::string repeated_two = MakeRequestFrame("hello-2", 102);

  xrpc::RequestEnvelope different_route_request;
  different_route_request.request_id_ = 103;
  different_route_request.service_name_ = "OtherService";
  different_route_request.method_name_ = "OtherMethod";
  different_route_request.payload_ = "payload";

  xrpc::FrameCodec codec;
  const std::string changed = codec.Encode(different_route_request);

  const xrpc::FrameStreamFeedResult fed = frame_stream.FeedBytes(repeated_one + repeated_two + changed);
  EXPECT_FALSE(fed.closed_);
  ASSERT_EQ(fed.requests_.size(), 3U);
  EXPECT_EQ(fed.requests_[0].service_name_, "EchoService");
  EXPECT_EQ(fed.requests_[0].method_name_, "Echo");
  EXPECT_EQ(fed.requests_[1].service_name_, "EchoService");
  EXPECT_EQ(fed.requests_[1].method_name_, "Echo");
  EXPECT_EQ(fed.requests_[2].service_name_, "OtherService");
  EXPECT_EQ(fed.requests_[2].method_name_, "OtherMethod");
}

TEST(RpcFrameStreamTest, FeedBytesClosesAfterCorruptedRequestMetadata) {
  xrpc::RpcFrameStream frame_stream;
  std::string corrupted_metadata;
  corrupted_metadata.push_back(static_cast<char>(0x0A));

  xrpc::FrameHeader header;
  header.message_type_ = xrpc::MessageType::Request;
  header.request_id_ = 102;
  header.metadata_size_ = static_cast<std::uint32_t>(corrupted_metadata.size());
  header.payload_size_ = 0;

  std::string corrupted_frame = EncodeFrameHeader(header);
  corrupted_frame.append(corrupted_metadata);

  const xrpc::FrameStreamFeedResult result = frame_stream.FeedBytes(corrupted_frame);
  EXPECT_TRUE(result.closed_);
  EXPECT_TRUE(result.requests_.empty());
}

TEST(RpcFrameStreamTest, FeedBytesKeepsHalfPacketUntilComplete) {
  xrpc::RpcFrameStream frame_stream;
  const std::string frame = MakeRequestFrame("half", 201);
  const std::string first_half = frame.substr(0, frame.size() / 2);
  const std::string second_half = frame.substr(frame.size() / 2);

  const xrpc::FrameStreamFeedResult first = frame_stream.FeedBytes(first_half);
  EXPECT_FALSE(first.closed_);
  EXPECT_TRUE(first.requests_.empty());

  const xrpc::FrameStreamFeedResult second = frame_stream.FeedBytes(second_half);
  EXPECT_FALSE(second.closed_);
  ASSERT_EQ(second.requests_.size(), 1U);
  EXPECT_EQ(second.requests_[0].request_id_, 201U);
  EXPECT_EQ(second.requests_[0].payload_, "half");
}

TEST(RpcFrameStreamTest, FeedBytesClosesFrameStreamWhenDeclaredPayloadExceedsDefaultLimit) {
  xrpc::RpcFrameStream frame_stream;
  xrpc::FrameHeader header;
  header.payload_size_ = static_cast<std::uint32_t>(xrpc::ProtocolLimits::DEFAULT_MAX_PAYLOAD_SIZE + 1U);

  const xrpc::FrameStreamFeedResult result = frame_stream.FeedBytes(EncodeFrameHeader(header));

  EXPECT_TRUE(result.closed_);
  EXPECT_TRUE(result.requests_.empty());
}

TEST(RpcFrameStreamTest, FeedBytesClosesFrameStreamWhenPayloadExceedsConfiguredLimit) {
  xrpc::RpcFrameStream frame_stream(xrpc::MakeProtocolLimits(3));

  xrpc::RequestEnvelope request;
  request.request_id_ = 501;
  request.service_name_ = "EchoService";
  request.method_name_ = "Echo";
  request.payload_ = "1234";

  xrpc::FrameCodec default_codec;
  const xrpc::FrameStreamFeedResult result = frame_stream.FeedBytes(default_codec.Encode(request));

  EXPECT_TRUE(result.closed_);
  EXPECT_TRUE(result.requests_.empty());
}

TEST(RpcFrameStreamTest, EncodeResponseBuildsResponseFrame) {
  xrpc::RpcFrameStream frame_stream;

  xrpc::ResponseEnvelope response;
  response.request_id_ = 301;
  response.status_ = xrpc::Status::Ok();
  response.payload_ = "payload";

  const std::string frame = frame_stream.EncodeResponse(std::move(response));
  const xrpc::ResponseEnvelope decoded = DecodeResponseFrame(frame);
  EXPECT_EQ(decoded.request_id_, 301U);
  EXPECT_TRUE(decoded.status_.ok());
  EXPECT_EQ(decoded.payload_, "payload");
}

TEST(RpcFrameStreamTest, EncodeResponsePreservesErrorStatus) {
  xrpc::RpcFrameStream frame_stream;
  xrpc::ResponseEnvelope response;
  response.request_id_ = 402;
  response.status_ = {xrpc::StatusCode::Internal, "handler failed"};

  const std::string response_frame = frame_stream.EncodeResponse(std::move(response));
  const xrpc::ResponseEnvelope decoded_response = DecodeResponseFrame(response_frame);

  EXPECT_EQ(decoded_response.request_id_, 402U);
  EXPECT_EQ(decoded_response.status_.code(), xrpc::StatusCode::Internal);
  EXPECT_EQ(decoded_response.status_.message(), "handler failed");
}
