#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <protocol/xrpc/frame_metadata.pb.h>

#include "common/xrpc_exception.h"

#include "protocol/frame_codec.h"
#include "protocol/frame_header.h"
#include "protocol/rpc_envelope.h"

namespace xrpc {
namespace {

auto U8(char c) -> uint8_t { return static_cast<uint8_t>(c); }

auto EncodeFrameHeader(const FrameHeader &header) -> std::string {
  std::string bytes(FrameHeader::SIZE, '\0');
  FrameHeader::EncodeTo(header, bytes.data());
  return bytes;
}

auto MakeFrame(MessageType type, std::string_view metadata_bytes, std::string_view payload, uint64_t request_id)
    -> std::string {
  FrameHeader header;
  header.message_type_ = type;
  header.request_id_ = request_id;
  header.metadata_size_ = static_cast<uint32_t>(metadata_bytes.size());
  header.payload_size_ = static_cast<uint32_t>(payload.size());

  std::string frame;
  frame.reserve(FrameHeader::SIZE + metadata_bytes.size() + payload.size());
  frame.append(EncodeFrameHeader(header));
  frame.append(metadata_bytes);
  frame.append(payload);
  return frame;
}

}  // namespace

TEST(FrameHeaderRobustTest, EncodedLayoutUsesNetworkByteOrder) {
  FrameHeader header;
  header.message_type_ = MessageType::Response;
  header.flags_ = 0x1234;
  header.metadata_size_ = 0x01020304;
  header.payload_size_ = 0x0A0B0C0D;
  header.request_id_ = 0x0102030405060708ULL;

  std::string encoded = EncodeFrameHeader(header);
  ASSERT_EQ(encoded.size(), FrameHeader::SIZE);

  // magic: "XRPC"
  EXPECT_EQ(U8(encoded[0]), 0x58);
  EXPECT_EQ(U8(encoded[1]), 0x52);
  EXPECT_EQ(U8(encoded[2]), 0x50);
  EXPECT_EQ(U8(encoded[3]), 0x43);

  EXPECT_EQ(U8(encoded[4]), FrameHeader::VERSION);
  EXPECT_EQ(U8(encoded[5]), static_cast<uint8_t>(MessageType::Response));

  // flags: 0x1234, big endian
  EXPECT_EQ(U8(encoded[6]), 0x12);
  EXPECT_EQ(U8(encoded[7]), 0x34);

  // metadata_size: 0x01020304, big endian
  EXPECT_EQ(U8(encoded[8]), 0x01);
  EXPECT_EQ(U8(encoded[9]), 0x02);
  EXPECT_EQ(U8(encoded[10]), 0x03);
  EXPECT_EQ(U8(encoded[11]), 0x04);

  // payload_size: 0x0A0B0C0D, big endian
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

  RequestEnvelope req;
  req.request_id_ = 100;
  req.service_name_ = "CalculatorService";
  req.method_name_ = "Add";
  req.payload_ = "serialized request payload";

  std::string frame = codec.Encode(req);
  ASSERT_GT(frame.size(), FrameHeader::SIZE);

  for (size_t n = 0; n < frame.size(); ++n) {
    auto result = codec.Decode(std::string_view(frame.data(), n));
    EXPECT_EQ(result.error_, ProtocolError::NeedMoreData) << "prefix size = " << n;
    EXPECT_EQ(result.consumed_, 0U) << "prefix size = " << n;
    EXPECT_FALSE(result.HasEnvelope()) << "prefix size = " << n;
  }

  auto result = codec.Decode(frame);
  EXPECT_EQ(result.error_, ProtocolError::Ok);
  EXPECT_EQ(result.consumed_, frame.size());
  ASSERT_TRUE(result.HasEnvelope());
}

TEST(FrameCodecRobustTest, DecodeTwoFramesFromOneBufferUsingConsumed) {
  FrameCodec codec;

  RequestEnvelope req1;
  req1.request_id_ = 1;
  req1.service_name_ = "S1";
  req1.method_name_ = "M1";
  req1.payload_ = "payload-1";

  ResponseEnvelope resp2;
  resp2.request_id_ = 2;
  resp2.status_ = Status::Ok();
  resp2.payload_ = "payload-2";

  std::string frame1 = codec.Encode(req1);
  std::string frame2 = codec.Encode(resp2);

  std::string buffer = frame1 + frame2;

  auto first = codec.Decode(buffer);
  ASSERT_EQ(first.error_, ProtocolError::Ok);
  ASSERT_EQ(first.consumed_, frame1.size());
  ASSERT_TRUE(first.request_.has_value());

  const auto &decoded_req = *first.request_;
  EXPECT_EQ(decoded_req.request_id_, 1U);
  EXPECT_EQ(decoded_req.service_name_, "S1");
  EXPECT_EQ(decoded_req.method_name_, "M1");
  EXPECT_EQ(decoded_req.payload_, "payload-1");

  std::string_view remain(buffer.data() + first.consumed_, buffer.size() - first.consumed_);
  auto second = codec.Decode(remain);
  ASSERT_EQ(second.error_, ProtocolError::Ok);
  ASSERT_EQ(second.consumed_, frame2.size());
  ASSERT_TRUE(second.response_.has_value());

  const auto &decoded_resp = *second.response_;
  EXPECT_EQ(decoded_resp.request_id_, 2U);
  EXPECT_TRUE(decoded_resp.status_.ok());
  EXPECT_EQ(decoded_resp.status_.message(), "");
  EXPECT_EQ(decoded_resp.payload_, "payload-2");
}

TEST(FrameCodecRobustTest, OkResponseWithEmptyErrorTextUsesEmptyMetadata) {
  FrameCodec codec;

  ResponseEnvelope response;
  response.request_id_ = 333;
  response.status_ = Status::Ok();
  response.payload_ = "payload";

  const std::string frame = codec.Encode(response);
  const std::optional<FrameHeader> header = FrameHeader::Decode(std::string_view(frame.data(), FrameHeader::SIZE));
  ASSERT_TRUE(header.has_value());
  EXPECT_EQ(header->message_type_, MessageType::Response);
  EXPECT_EQ(header->request_id_, response.request_id_);
  EXPECT_EQ(header->metadata_size_, 0U);
  EXPECT_EQ(header->payload_size_, response.payload_.size());

  auto result = codec.Decode(frame);
  ASSERT_EQ(result.error_, ProtocolError::Ok);
  ASSERT_TRUE(result.response_.has_value());

  const auto &decoded = *result.response_;
  EXPECT_EQ(decoded.request_id_, response.request_id_);
  EXPECT_TRUE(decoded.status_.ok());
  EXPECT_EQ(decoded.status_.message(), "");
  EXPECT_EQ(decoded.payload_, response.payload_);
}

TEST(FrameCodecRobustTest, NonOkResponseKeepsEncodedMetadata) {
  FrameCodec codec;

  ResponseEnvelope response;
  response.request_id_ = 444;
  response.status_ = {StatusCode::Unavailable, "unavailable"};
  response.payload_ = "";

  const std::string frame = codec.Encode(response);
  const std::optional<FrameHeader> header = FrameHeader::Decode(std::string_view(frame.data(), FrameHeader::SIZE));
  ASSERT_TRUE(header.has_value());
  EXPECT_EQ(header->message_type_, MessageType::Response);
  EXPECT_GT(header->metadata_size_, 0U);
  EXPECT_EQ(header->payload_size_, 0U);

  auto result = codec.Decode(frame);
  ASSERT_EQ(result.error_, ProtocolError::Ok);
  ASSERT_TRUE(result.response_.has_value());

  const auto &decoded = *result.response_;
  EXPECT_EQ(decoded.request_id_, response.request_id_);
  EXPECT_EQ(decoded.status_.code(), response.status_.code());
  EXPECT_EQ(decoded.status_.message(), response.status_.message());
  EXPECT_EQ(decoded.payload_, "");
}

TEST(FrameCodecRobustTest, DecodeOneFrameAndLeaveTrailingGarbageUnconsumed) {
  FrameCodec codec;

  RequestEnvelope req;
  req.request_id_ = 7;
  req.service_name_ = "EchoService";
  req.method_name_ = "Echo";
  req.payload_ = "hello";

  std::string frame = codec.Encode(req);
  std::string garbage = "THIS_IS_NOT_A_FRAME";
  std::string buffer = frame + garbage;

  auto result = codec.Decode(buffer);
  ASSERT_EQ(result.error_, ProtocolError::Ok);
  EXPECT_EQ(result.consumed_, frame.size());

  std::string_view remain(buffer.data() + result.consumed_, buffer.size() - result.consumed_);
  auto bad = codec.Decode(remain);

  if (remain.size() < FrameHeader::SIZE) {
    EXPECT_EQ(bad.error_, ProtocolError::NeedMoreData);
  } else {
    EXPECT_EQ(bad.error_, ProtocolError::InvalidMagic);
  }
}

TEST(FrameCodecRobustTest, InvalidMessageTypeIsRejected) {
  FrameCodec codec;

  FrameHeader header;
  header.message_type_ = static_cast<MessageType>(99);
  header.request_id_ = 123;
  header.metadata_size_ = 0;
  header.payload_size_ = 0;

  std::string frame = EncodeFrameHeader(header);

  auto result = codec.Decode(frame);
  EXPECT_EQ(result.error_, ProtocolError::InvalidMessageType);
  EXPECT_EQ(result.consumed_, frame.size());
  EXPECT_FALSE(result.HasEnvelope());
}

TEST(FrameCodecRobustTest, UnsupportedVersionIsRejectedFromFrameHeader) {
  FrameCodec codec;
  FrameHeader header;
  header.version_ = FrameHeader::VERSION + 1;

  const FrameDecodeResult result = codec.Decode(EncodeFrameHeader(header));

  EXPECT_EQ(result.error_, ProtocolError::UnsupportedVersion);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasEnvelope());
}

TEST(FrameCodecRobustTest, OversizedMetadataIsRejectedBeforeBodyArrives) {
  const ProtocolLimits limits{.max_metadata_size_ = 8, .max_payload_size_ = 32};
  FrameCodec codec(limits);
  FrameHeader header;
  header.metadata_size_ = 9;

  const FrameDecodeResult result = codec.Decode(EncodeFrameHeader(header));

  EXPECT_EQ(result.error_, ProtocolError::FrameTooLarge);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasEnvelope());
}

TEST(FrameCodecRobustTest, OversizedPayloadIsRejectedBeforeBodyArrives) {
  const ProtocolLimits limits{.max_metadata_size_ = 8, .max_payload_size_ = 32};
  FrameCodec codec(limits);
  FrameHeader header;
  header.payload_size_ = 33;

  const FrameDecodeResult result = codec.Decode(EncodeFrameHeader(header));

  EXPECT_EQ(result.error_, ProtocolError::FrameTooLarge);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasEnvelope());
}

TEST(FrameCodecRobustTest, EncodingHonorsPayloadLimit) {
  const ProtocolLimits limits{.max_metadata_size_ = 64, .max_payload_size_ = 3};
  FrameCodec codec(limits);
  RequestEnvelope request;
  request.request_id_ = 1;
  request.service_name_ = "S";
  request.method_name_ = "M";
  request.payload_ = "four";

  EXPECT_THROW(static_cast<void>(codec.Encode(request)), ProtocolException);
}

TEST(FrameCodecRobustTest, CorruptedRequestMetadataReturnsDecodeError) {
  FrameCodec codec;

  // 0x0A is the protobuf tag for field 1 as a string. The test omits the
  // length and value bytes to force truncated protobuf metadata.
  std::string corrupted_metadata;
  corrupted_metadata.push_back(static_cast<char>(0x0A));

  std::string frame = MakeFrame(MessageType::Request, corrupted_metadata, "payload", 111);

  auto result = codec.Decode(frame);
  EXPECT_EQ(result.error_, ProtocolError::DecodeError);
  EXPECT_EQ(result.consumed_, frame.size());
  EXPECT_FALSE(result.HasEnvelope());
}

TEST(FrameCodecRobustTest, CorruptedResponseMetadataReturnsDecodeError) {
  FrameCodec codec;

  // ResponseMetadata field 1 is an int32 with tag 0x08. The test provides the
  // tag without a varint value, so protobuf parsing must fail.
  std::string corrupted_metadata;
  corrupted_metadata.push_back(static_cast<char>(0x08));

  std::string frame = MakeFrame(MessageType::Response, corrupted_metadata, "payload", 222);

  auto result = codec.Decode(frame);
  EXPECT_EQ(result.error_, ProtocolError::DecodeError);
  EXPECT_EQ(result.consumed_, frame.size());
  EXPECT_FALSE(result.HasEnvelope());
}

TEST(FrameCodecRobustTest, UnknownResponseStatusBecomesDataLoss) {
  FrameCodec codec;
  ResponseMetadata metadata;
  metadata.set_error_code(99);
  metadata.set_error_text("unknown status");

  std::string metadata_bytes;
  ASSERT_TRUE(metadata.SerializeToString(&metadata_bytes));
  const std::string frame = MakeFrame(MessageType::Response, metadata_bytes, "payload", 223);

  const FrameDecodeResult result = codec.Decode(frame);
  ASSERT_EQ(result.error_, ProtocolError::Ok);
  ASSERT_TRUE(result.response_.has_value());
  EXPECT_EQ(result.response_->status_.code(), StatusCode::DataLoss);
  EXPECT_EQ(result.response_->status_.message(), "response contains an invalid RPC status code");
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

  RequestEnvelope req;
  req.request_id_ = 888;
  req.service_name_ = "BinaryService";
  req.method_name_ = "Upload";
  req.payload_ = payload;

  std::string frame = codec.Encode(req);

  auto result = codec.Decode(frame);
  ASSERT_EQ(result.error_, ProtocolError::Ok);
  ASSERT_TRUE(result.request_.has_value());

  const auto &decoded = *result.request_;
  EXPECT_EQ(decoded.request_id_, 888U);
  EXPECT_EQ(decoded.service_name_, "BinaryService");
  EXPECT_EQ(decoded.method_name_, "Upload");
  EXPECT_EQ(decoded.payload_.size(), payload.size());
  EXPECT_EQ(decoded.payload_, payload);
}

TEST(FrameCodecRobustTest, CompleteFrameHeaderButIncompleteMetadataNeedsMoreData) {
  FrameCodec codec;

  RequestEnvelope req;
  req.request_id_ = 9;
  req.service_name_ = "S";
  req.method_name_ = "M";
  req.payload_ = "P";

  std::string frame = codec.Encode(req);
  ASSERT_GT(frame.size(), FrameHeader::SIZE);

  // Provide only the complete FrameHeader, without the protobuf metadata or
  // payload bytes that follow it.
  auto result = codec.Decode(std::string_view(frame.data(), FrameHeader::SIZE));
  EXPECT_EQ(result.error_, ProtocolError::NeedMoreData);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasEnvelope());
}

TEST(FrameCodecRobustTest, IncompletePayloadNeedsMoreData) {
  FrameCodec codec;

  RequestEnvelope req;
  req.request_id_ = 10;
  req.service_name_ = "S";
  req.method_name_ = "M";
  req.payload_ = "long-payload";

  std::string frame = codec.Encode(req);
  ASSERT_GT(frame.size(), FrameHeader::SIZE);

  auto decoded_header = FrameHeader::Decode(std::string_view(frame.data(), FrameHeader::SIZE));
  ASSERT_TRUE(decoded_header.has_value());

  const size_t full_size = frame.size();
  const size_t missing_one_byte = full_size - 1;

  auto result = codec.Decode(std::string_view(frame.data(), missing_one_byte));
  EXPECT_EQ(result.error_, ProtocolError::NeedMoreData);
  EXPECT_EQ(result.consumed_, 0U);
  EXPECT_FALSE(result.HasEnvelope());
}

}  // namespace xrpc
