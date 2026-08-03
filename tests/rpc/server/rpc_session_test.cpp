#include "rpc/server/rpc_session.h"

#include <cstdint>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include <xrpc/rpc_server.h>

#include "proto/echo.pb.h"
#include "protocol/fixed_header.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_error.h"
#include "protocol/protocol_message.h"
#include "rpc/protobuf_codec.h"
#include "test_support/rpc_server_handler.h"

namespace {

auto Echo(const xrpc::test::EchoRequest &req) -> xrpc::test::EchoResponse {
  xrpc::test::EchoResponse resp;
  resp.set_message("echo: " + req.message());
  return resp;
}

auto MakeRequestFrame(std::string message, std::uint64_t request_id) -> std::string {
  xrpc::test::EchoRequest req;
  req.set_message(std::move(message));

  xrpc::ProtocolRequest protocol_req;
  protocol_req.request_id_ = request_id;
  protocol_req.service_name_ = "EchoService";
  protocol_req.method_name_ = "Echo";
  protocol_req.payload_ = xrpc::ProtobufCodec::Encode(req);

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
  xrpc::RpcServer server;
  server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService", "Echo", Echo);

  xrpc::RpcSession session(xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "Echo", Echo));
  const std::string frame_one = MakeRequestFrame("hello-1", 101);
  const std::string frame_two = MakeRequestFrame("hello-2", 102);

  const xrpc::SessionFeedResult fed = session.FeedBytes(frame_one + frame_two);
  EXPECT_FALSE(fed.closed_);
  ASSERT_EQ(fed.requests_.size(), 2U);
  EXPECT_EQ(fed.requests_[0].request_id_, 101U);
  EXPECT_EQ(fed.requests_[1].request_id_, 102U);
}

TEST(RpcSessionTest, FeedBytesHandlesRepeatedAndChangedRequestHeaders) {
  xrpc::RpcServer server;
  server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService", "Echo", Echo);

  xrpc::RpcSession session(xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "Echo", Echo));
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
  xrpc::RpcServer server;
  server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService", "Echo", Echo);

  xrpc::RpcSession session(xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "Echo", Echo));
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
  xrpc::RpcServer server;
  server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService", "Echo", Echo);

  xrpc::RpcSession session(xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "Echo", Echo));
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
}

TEST(RpcSessionTest, FeedBytesClosesSessionWhenDeclaredPayloadExceedsDefaultLimit) {
  xrpc::RpcServer server;
  xrpc::RpcSession session(xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "Echo", Echo));
  xrpc::FixedHeader header;
  header.payload_len_ = static_cast<std::uint32_t>(xrpc::ProtocolLimits::DEFAULT_MAX_PAYLOAD_SIZE + 1U);

  const xrpc::SessionFeedResult result = session.FeedBytes(xrpc::FixedHeader::Encode(header));

  EXPECT_TRUE(result.closed_);
  EXPECT_TRUE(result.requests_.empty());
}

TEST(RpcSessionTest, FeedBytesClosesSessionWhenPayloadExceedsConfiguredLimit) {
  xrpc::RpcServer server;
  xrpc::RpcSession session(xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
                               "EchoService", "Echo", Echo),
                           xrpc::MakeProtocolLimits(3));

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
  xrpc::RpcServer server;
  xrpc::RpcSession session(xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "Echo", Echo));

  xrpc::ProtocolResponse response;
  response.request_id_ = 301;
  response.error_code_ = 0;
  response.error_text_.clear();
  response.payload_ = "payload";

  const std::string frame = session.EncodeResponse(response);
  const xrpc::ProtocolResponse decoded = DecodeResponseFrame(frame);
  EXPECT_EQ(decoded.request_id_, 301U);
  EXPECT_EQ(decoded.error_code_, 0);
  EXPECT_EQ(decoded.payload_, "payload");
}

TEST(RpcSessionTest, HandleBytesRemainsCompatible) {
  xrpc::RpcServer server;
  server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService", "Echo", Echo);

  xrpc::RpcSession session(xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "Echo", Echo));
  const std::string request_frame = MakeRequestFrame("compat", 401);
  const std::string response_frame = session.HandleBytes(request_frame);
  const xrpc::ProtocolResponse response = DecodeResponseFrame(response_frame);

  EXPECT_EQ(response.request_id_, 401U);
  EXPECT_EQ(response.error_code_, 0);
  const xrpc::test::EchoResponse payload = xrpc::ProtobufCodec::Decode<xrpc::test::EchoResponse>(response.payload_);
  EXPECT_EQ(payload.message(), "echo: compat");
}
