#include <gtest/gtest.h>

#include <utility>

#include "protocol/protocol_message.h"
#include "rpc/protocol_adapter.h"
#include "rpc/raw_message.h"

TEST(ProtocolAdapterTest, ConvertsProtocolRequestToRawRequest) {
  xrpc::ProtocolRequest protocol;
  protocol.request_id_ = 11;
  protocol.service_name_ = "EchoService";
  protocol.method_name_ = "Echo";
  protocol.payload_ = "request-payload";

  const xrpc::RawRequest raw = xrpc::ToRawRequest(protocol);
  EXPECT_EQ(raw.request_id_, 11U);
  EXPECT_EQ(raw.service_name_, "EchoService");
  EXPECT_EQ(raw.method_name_, "Echo");
  EXPECT_EQ(raw.payload_, "request-payload");
}

TEST(ProtocolAdapterTest, ConvertsRawRequestToProtocolRequest) {
  xrpc::RawRequest raw;
  raw.request_id_ = 12;
  raw.service_name_ = "CalcService";
  raw.method_name_ = "Add";
  raw.payload_ = "raw-request";

  const xrpc::ProtocolRequest protocol = xrpc::ToProtocolRequest(raw);
  EXPECT_EQ(protocol.request_id_, 12U);
  EXPECT_EQ(protocol.service_name_, "CalcService");
  EXPECT_EQ(protocol.method_name_, "Add");
  EXPECT_EQ(protocol.payload_, "raw-request");
}

TEST(ProtocolAdapterTest, ConvertsRawResponseToProtocolResponse) {
  xrpc::RawResponse raw;
  raw.request_id_ = 13;
  raw.status_ = {xrpc::StatusCode::ResourceExhausted, "raw-error"};
  raw.payload_ = "raw-response";

  const xrpc::ProtocolResponse protocol = xrpc::ToProtocolResponse(raw);
  EXPECT_EQ(protocol.request_id_, 13U);
  EXPECT_EQ(protocol.error_code_, static_cast<std::int32_t>(xrpc::StatusCode::ResourceExhausted));
  EXPECT_EQ(protocol.error_text_, "raw-error");
  EXPECT_EQ(protocol.payload_, "raw-response");
}

TEST(ProtocolAdapterTest, ConvertsMovedRawResponseToProtocolResponse) {
  xrpc::RawResponse raw;
  raw.request_id_ = 15;
  raw.status_ = {xrpc::StatusCode::Ok, ""};
  raw.payload_ = "raw-response";

  const xrpc::ProtocolResponse protocol = xrpc::ToProtocolResponse(std::move(raw));
  EXPECT_EQ(protocol.request_id_, 15U);
  EXPECT_EQ(protocol.error_code_, static_cast<std::int32_t>(xrpc::StatusCode::Ok));
  EXPECT_TRUE(protocol.error_text_.empty());
  EXPECT_EQ(protocol.payload_, "raw-response");
}

TEST(ProtocolAdapterTest, ConvertsProtocolResponseToRawResponse) {
  xrpc::ProtocolResponse protocol;
  protocol.request_id_ = 14;
  protocol.error_code_ = static_cast<std::int32_t>(xrpc::StatusCode::DeadlineExceeded);
  protocol.error_text_ = "protocol-error";
  protocol.payload_ = "protocol-response";

  const xrpc::RawResponse raw = xrpc::ToRawResponse(protocol);
  EXPECT_EQ(raw.request_id_, 14U);
  EXPECT_EQ(raw.status_.code(), xrpc::StatusCode::DeadlineExceeded);
  EXPECT_EQ(raw.status_.message(), "protocol-error");
  EXPECT_EQ(raw.payload_, "protocol-response");
}

TEST(ProtocolAdapterTest, RejectsUnknownWireStatusCode) {
  xrpc::ProtocolResponse protocol;
  protocol.error_code_ = 1000;
  protocol.error_text_ = "unknown";

  const xrpc::RawResponse raw = xrpc::ToRawResponse(protocol);
  EXPECT_EQ(raw.status_.code(), xrpc::StatusCode::DataLoss);
}
