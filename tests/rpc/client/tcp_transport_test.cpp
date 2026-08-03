#include <gtest/gtest.h>

#include "io/socket_error.h"
#include "rpc/client/tcp_transport.h"
#include "rpc/client/transport_error.h"
#include "rpc/protobuf_codec.h"
#include "rpc/raw_message.h"

TEST(TcpTransportTest, MapsStructuredSocketErrorsWithoutInspectingMessages) {
  const xrpc::io::SocketError timeout(xrpc::io::SocketErrorCode::ReadTimeout, 0, "arbitrary timeout text");
  const xrpc::io::SocketError unavailable(xrpc::io::SocketErrorCode::ConnectFailed, 111, "arbitrary connect text");

  EXPECT_EQ(xrpc::ToStatus(timeout).code(), xrpc::StatusCode::DeadlineExceeded);
  EXPECT_EQ(xrpc::ToStatus(unavailable).code(), xrpc::StatusCode::Unavailable);
}

TEST(TcpTransportTest, CallReturnsFailureIfNotConnected) {
  xrpc::TcpTransport transport("127.0.0.1", 1);

  xrpc::RawRequest request;
  request.request_id_ = 1;
  request.service_name_ = "EchoService";
  request.method_name_ = "Echo";

  const xrpc::RawCallResult result = transport.Call(request, xrpc::EffectiveCallOptions{});

  ASSERT_TRUE(result.HasFailure());
  EXPECT_EQ(result.failure().status_.code(), xrpc::StatusCode::Unavailable);
  EXPECT_TRUE(result.CanRetryWithoutDuplicateRequest());
}
