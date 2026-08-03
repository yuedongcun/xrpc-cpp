#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <xrpc/xrpc_exception.h>

#include "protocol/fixed_header.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_error.h"
#include "protocol/protocol_message.h"

namespace xrpc {
namespace {

auto U8(char c) -> uint8_t { return static_cast<uint8_t>(c); }

auto MakeRawFrame(MessageType type, std::string_view header_bytes, std::string_view payload, uint64_t request_id)
    -> std::string {
  FixedHeader hdr;
  hdr.message_type_ = type;
  hdr.request_id_ = request_id;
  hdr.header_len_ = static_cast<uint32_t>(header_bytes.size());
  hdr.payload_len_ = static_cast<uint32_t>(payload.size());

  std::string frame;
  frame.reserve(FixedHeader::SIZE + header_bytes.size() + payload.size());
  frame.append(FixedHeader::Encode(hdr));
  frame.append(header_bytes);
  frame.append(payload);
  return frame;
}

}  // namespace

TEST(FixedHeaderRobustTest, EncodedLayoutUsesNetworkByteOrder) {
  FixedHeader hdr;
  hdr.message_type_ = MessageType::Response;
  hdr.flags_ = 0x1234;
  hdr.header_len_ = 0x01020304;
  hdr.payload_len_ = 0x0A0B0C0D;
  hdr.request_id_ = 0x0102030405060708ULL;

  std::string encoded = FixedHeader::Encode(hdr);
  ASSERT_EQ(encoded.size(), FixedHeader::SIZE);

  // magic: "XRPC"
  EXPECT_EQ(U8(encoded[0]), 0x58);
  EXPECT_EQ(U8(encoded[1]), 0x52);
  EXPECT_EQ(U8(encoded[2]), 0x50);
  EXPECT_EQ(U8(encoded[3]), 0x43);

  EXPECT_EQ(U8(encoded[4]), FixedHeader::VERSION);
  EXPECT_EQ(U8(encoded[5]), static_cast<uint8_t>(MessageType::Response));

  // flags: 0x1234, big endian
  EXPECT_EQ(U8(encoded[6]), 0x12);
  EXPECT_EQ(U8(encoded[7]), 0x34);

  // header_len: 0x01020304, big endian
  EXPECT_EQ(U8(encoded[8]), 0x01);
  EXPECT_EQ(U8(encoded[9]), 0x02);
  EXPECT_EQ(U8(encoded[10]), 0x03);
  EXPECT_EQ(U8(encoded[11]), 0x04);

  // payload_len: 0x0A0B0C0D, big endian
  EXPECT_EQ(U8(encoded[12]), 0x0A);
  EXPECT_EQ(U8(encoded[13]), 0x0B);
  EXPECT_EQ(U8(encoded[14]), 0x0C);
  EXPECT_EQ(U8(encoded[15]), 0x0D);

  // request_id: 0x0102030405060708, big endian
  EXPECT_EQ(U8(encoded[16]), 0x01);
  EXPECT_EQ(U8(encoded[17]), 0x02);
  EXPECT_EQ(U8(encoded[18]), 0x03);
  EXPECT_EQ(U8(encoded[19]), 0x04);
  EXPECT_EQ(U8(encoded[20]), 0x05);
  EXPECT_EQ(U8(encoded[21]), 0x06);
  EXPECT_EQ(U8(encoded[22]), 0x07);
  EXPECT_EQ(U8(encoded[23]), 0x08);
}

TEST(FrameCodecRobustTest, EveryPrefixOfAValidFrameNeedsMoreData) {
  FrameCodec codec;

  ProtocolRequest req;
  req.request_id_ = 100;
  req.service_name_ = "CalculatorService";
  req.method_name_ = "Add";
  req.payload_ = "serialized request payload";

  std::string frame = codec.EncodeRequest(req);
  ASSERT_GT(frame.size(), FixedHeader::SIZE);

  for (size_t n = 0; n < frame.size(); ++n) {
    auto result = codec.TryDecode(std::string_view(frame.data(), n));
    EXPECT_EQ(result.error_, ProtocolError::NeedMoreData) << "prefix size = " << n;
    EXPECT_EQ(result.consumed_, 0U) << "prefix size = " << n;
    EXPECT_FALSE(result.HasMessage()) << "prefix size = " << n;
  }

  auto result = codec.TryDecode(frame);
  EXPECT_EQ(result.error_, ProtocolError::Ok);
  EXPECT_EQ(result.consumed_, frame.size());
  ASSERT_TRUE(result.HasMessage());
}

TEST(FrameCodecRobustTest, DecodeTwoFramesFromOneBufferUsingConsumed) {
  FrameCodec codec;

  ProtocolRequest req1;
  req1.request_id_ = 1;
  req1.service_name_ = "S1";
  req1.method_name_ = "M1";
  req1.payload_ = "payload-1";

  ProtocolResponse resp2;
  resp2.request_id_ = 2;
  resp2.error_code_ = 0;
  resp2.error_text_ = "";
  resp2.payload_ = "payload-2";

  std::string frame1 = codec.EncodeRequest(req1);
  std::string frame2 = codec.EncodeResponse(resp2);

  std::string buffer = frame1 + frame2;

  auto first = codec.TryDecode(buffer);
  ASSERT_EQ(first.error_, ProtocolError::Ok);
  ASSERT_EQ(first.consumed_, frame1.size());
  ASSERT_TRUE(first.request_.has_value());

  const auto &decoded_req = *first.request_;
  EXPECT_EQ(decoded_req.request_id_, 1U);
  EXPECT_EQ(decoded_req.service_name_, "S1");
  EXPECT_EQ(decoded_req.method_name_, "M1");
  EXPECT_EQ(decoded_req.payload_, "payload-1");

  std::string_view remain(buffer.data() + first.consumed_, buffer.size() - first.consumed_);
  auto second = codec.TryDecode(remain);
  ASSERT_EQ(second.error_, ProtocolError::Ok);
  ASSERT_EQ(second.consumed_, frame2.size());
  ASSERT_TRUE(second.response_.has_value());

  const auto &decoded_resp = *second.response_;
  EXPECT_EQ(decoded_resp.request_id_, 2U);
  EXPECT_EQ(decoded_resp.error_code_, 0);
  EXPECT_EQ(decoded_resp.error_text_, "");
  EXPECT_EQ(decoded_resp.payload_, "payload-2");
}

TEST(FrameCodecRobustTest, OkResponseWithEmptyErrorTextUsesEmptyHeader) {
  FrameCodec codec;

  ProtocolResponse response;
  response.request_id_ = 333;
  response.error_code_ = 0;
  response.error_text_ = "";
  response.payload_ = "payload";

  const std::string frame = codec.EncodeResponse(response);
  const std::optional<FixedHeader> header = FixedHeader::Decode(std::string_view(frame.data(), FixedHeader::SIZE));
  ASSERT_TRUE(header.has_value());
  EXPECT_EQ(header->message_type_, MessageType::Response);
  EXPECT_EQ(header->request_id_, response.request_id_);
  EXPECT_EQ(header->header_len_, 0U);
  EXPECT_EQ(header->payload_len_, response.payload_.size());

  auto result = codec.TryDecode(frame);
  ASSERT_EQ(result.error_, ProtocolError::Ok);
  ASSERT_TRUE(result.response_.has_value());

  const auto &decoded = *result.response_;
  EXPECT_EQ(decoded.request_id_, response.request_id_);
  EXPECT_EQ(decoded.error_code_, 0);
  EXPECT_EQ(decoded.error_text_, "");
  EXPECT_EQ(decoded.payload_, response.payload_);
}

TEST(FrameCodecRobustTest, NonOkResponseKeepsEncodedHeader) {
  FrameCodec codec;

  ProtocolResponse response;
  response.request_id_ = 444;
  response.error_code_ = 14;
  response.error_text_ = "unavailable";
  response.payload_ = "";

  const std::string frame = codec.EncodeResponse(response);
  const std::optional<FixedHeader> header = FixedHeader::Decode(std::string_view(frame.data(), FixedHeader::SIZE));
  ASSERT_TRUE(header.has_value());
  EXPECT_EQ(header->message_type_, MessageType::Response);
  EXPECT_GT(header->header_len_, 0U);
  EXPECT_EQ(header->payload_len_, 0U);

  auto result = codec.TryDecode(frame);
  ASSERT_EQ(result.error_, ProtocolError::Ok);
  ASSERT_TRUE(result.response_.has_value());

  const auto &decoded = *result.response_;
  EXPECT_EQ(decoded.request_id_, response.request_id_);
  EXPECT_EQ(decoded.error_code_, response.error_code_);
  EXPECT_EQ(decoded.error_text_, response.error_text_);
  EXPECT_EQ(decoded.payload_, "");
}

TEST(FrameCodecRobustTest, DecodeOneFrameAndLeaveTrailingGarbageUnconsumed) {
  FrameCodec codec;

  ProtocolRequest req;
  req.request_id_ = 7;
  req.service_name_ = "EchoService";
  req.method_name_ = "Echo";
  req.payload_ = "hello";

  std::string frame = codec.EncodeRequest(req);
  std::string garbage = "THIS_IS_NOT_A_FRAME";
  std::string buffer = frame + garbage;

  auto result = codec.TryDecode(buffer);
  ASSERT_EQ(result.error_, ProtocolError::Ok);
  EXPECT_EQ(result.consumed_, frame.size());

  std::string_view remain(buffer.data() + result.consumed_, buffer.size() - result.consumed_);
  auto bad = codec.TryDecode(remain);

  if (remain.size() < FixedHeader::SIZE) {
    EXPECT_EQ(bad.error_, ProtocolError::NeedMoreData);
  } else {
    EXPECT_EQ(bad.error_, ProtocolError::InvalidMagic);
  }
}

TEST(FrameCodecRobustTest, InvalidMessageTypeIsRejected) {
  FrameCodec codec;

  FixedHeader hdr;
  hdr.message_type_ = static_cast<MessageType>(99);
  hdr.request_id_ = 123;
  hdr.header_len_ = 0;
  hdr.payload_len_ = 0;

  std::string frame = FixedHeader::Encode(hdr);

  auto result = codec.TryDecode(frame);
  EXPECT_EQ(result.error_, ProtocolError::InvalidMessageType);
  EXPECT_EQ(result.consumed_, frame.size());
  EXPECT_FALSE(result.HasMessage());
}

TEST(FrameCodecRobustTest, UnsupportedVersionIsRejectedFromFixedHeader) {
  FrameCodec codec;
  FixedHeader header;
  header.version_ = FixedHeader::VERSION + 1;

  const DecodeResult result = codec.TryDecode(FixedHeader::Encode(header));

  EXPECT_EQ(result.error_, ProtocolError::UnsupportedVersion);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasMessage());
}

TEST(FrameCodecRobustTest, OversizedHeaderIsRejectedBeforeBodyArrives) {
  const ProtocolLimits limits{.max_header_size_ = 8, .max_payload_size_ = 32, .max_frame_size_ = 64};
  FrameCodec codec(limits);
  FixedHeader header;
  header.header_len_ = 9;

  const DecodeResult result = codec.TryDecode(FixedHeader::Encode(header));

  EXPECT_EQ(result.error_, ProtocolError::FrameTooLarge);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasMessage());
}

TEST(FrameCodecRobustTest, OversizedPayloadIsRejectedBeforeBodyArrives) {
  const ProtocolLimits limits{.max_header_size_ = 8, .max_payload_size_ = 32, .max_frame_size_ = 64};
  FrameCodec codec(limits);
  FixedHeader header;
  header.payload_len_ = 33;

  const DecodeResult result = codec.TryDecode(FixedHeader::Encode(header));

  EXPECT_EQ(result.error_, ProtocolError::FrameTooLarge);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasMessage());
}

TEST(FrameCodecRobustTest, OversizedCombinedFrameIsRejectedBeforeBodyArrives) {
  const ProtocolLimits limits{.max_header_size_ = 32, .max_payload_size_ = 32, .max_frame_size_ = 40};
  FrameCodec codec(limits);
  FixedHeader header;
  header.header_len_ = 8;
  header.payload_len_ = 9;

  const DecodeResult result = codec.TryDecode(FixedHeader::Encode(header));

  EXPECT_EQ(result.error_, ProtocolError::FrameTooLarge);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasMessage());
}

TEST(FrameCodecRobustTest, EncodingHonorsPayloadLimit) {
  const ProtocolLimits limits{.max_header_size_ = 64, .max_payload_size_ = 3, .max_frame_size_ = 128};
  FrameCodec codec(limits);
  ProtocolRequest request;
  request.request_id_ = 1;
  request.service_name_ = "S";
  request.method_name_ = "M";
  request.payload_ = "four";

  EXPECT_THROW(static_cast<void>(codec.EncodeRequest(request)), ProtocolException);
}

TEST(FrameCodecRobustTest, HeartbeatIsReservedButUnsupported) {
  FrameCodec codec;
  FixedHeader header;
  header.message_type_ = MessageType::Heartbeat;

  const DecodeResult result = codec.TryDecode(FixedHeader::Encode(header));

  EXPECT_EQ(result.error_, ProtocolError::UnsupportedMessageType);
  EXPECT_EQ(result.consumed_, FixedHeader::SIZE);
  EXPECT_FALSE(result.HasMessage());
}

TEST(FrameCodecRobustTest, CorruptedRequestHeaderReturnsDecodeError) {
  FrameCodec codec;

  // 0x0A is the protobuf tag for field 1 as a string. The test omits the
  // length and value bytes to force a truncated protobuf header.
  std::string corrupted_header;
  corrupted_header.push_back(static_cast<char>(0x0A));

  std::string frame = MakeRawFrame(MessageType::Request, corrupted_header, "payload", 111);

  auto result = codec.TryDecode(frame);
  EXPECT_EQ(result.error_, ProtocolError::DecodeError);
  EXPECT_EQ(result.consumed_, frame.size());
  EXPECT_FALSE(result.HasMessage());
}

TEST(FrameCodecRobustTest, CorruptedResponseHeaderReturnsDecodeError) {
  FrameCodec codec;

  // RpcResponseHeader field 1 is an int32 with tag 0x08. The test provides the
  // tag without a varint value, so protobuf parsing must fail.
  std::string corrupted_header;
  corrupted_header.push_back(static_cast<char>(0x08));

  std::string frame = MakeRawFrame(MessageType::Response, corrupted_header, "payload", 222);

  auto result = codec.TryDecode(frame);
  EXPECT_EQ(result.error_, ProtocolError::DecodeError);
  EXPECT_EQ(result.consumed_, frame.size());
  EXPECT_FALSE(result.HasMessage());
}

TEST(FrameCodecRobustTest, PayloadMayContainNullBytesAndNonTextBytes) {
  FrameCodec codec;

  std::string payload;
  payload.push_back('\0');
  payload.push_back('A');
  payload.push_back(static_cast<char>(0xFF));
  payload.push_back('B');
  payload.push_back('\0');
  payload.push_back(static_cast<char>(0x80));

  ProtocolRequest req;
  req.request_id_ = 888;
  req.service_name_ = "BinaryService";
  req.method_name_ = "Upload";
  req.payload_ = payload;

  std::string frame = codec.EncodeRequest(req);

  auto result = codec.TryDecode(frame);
  ASSERT_EQ(result.error_, ProtocolError::Ok);
  ASSERT_TRUE(result.request_.has_value());

  const auto &decoded = *result.request_;
  EXPECT_EQ(decoded.request_id_, 888U);
  EXPECT_EQ(decoded.service_name_, "BinaryService");
  EXPECT_EQ(decoded.method_name_, "Upload");
  EXPECT_EQ(decoded.payload_.size(), payload.size());
  EXPECT_EQ(decoded.payload_, payload);
}

TEST(FrameCodecRobustTest, CompleteFixedHeaderButIncompleteHeaderBytesNeedsMoreData) {
  FrameCodec codec;

  ProtocolRequest req;
  req.request_id_ = 9;
  req.service_name_ = "S";
  req.method_name_ = "M";
  req.payload_ = "P";

  std::string frame = codec.EncodeRequest(req);
  ASSERT_GT(frame.size(), FixedHeader::SIZE);

  // Provide only the complete FixedHeader, without the protobuf header or
  // payload bytes that follow it.
  auto result = codec.TryDecode(std::string_view(frame.data(), FixedHeader::SIZE));
  EXPECT_EQ(result.error_, ProtocolError::NeedMoreData);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasMessage());
}

TEST(FrameCodecRobustTest, IncompletePayloadNeedsMoreData) {
  FrameCodec codec;

  ProtocolRequest req;
  req.request_id_ = 10;
  req.service_name_ = "S";
  req.method_name_ = "M";
  req.payload_ = "long-payload";

  std::string frame = codec.EncodeRequest(req);
  ASSERT_GT(frame.size(), FixedHeader::SIZE);

  auto decoded_header = FixedHeader::Decode(std::string_view(frame.data(), FixedHeader::SIZE));
  ASSERT_TRUE(decoded_header.has_value());

  const size_t full_size = frame.size();
  const size_t missing_one_byte = full_size - 1;

  auto result = codec.TryDecode(std::string_view(frame.data(), missing_one_byte));
  EXPECT_EQ(result.error_, ProtocolError::NeedMoreData);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasMessage());
}

}  // namespace xrpc
