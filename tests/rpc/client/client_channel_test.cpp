#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include "io/socket.h"
#include "proto/echo.pb.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_error.h"
#include "protocol/protocol_message.h"
#include "rpc/client/client_channel.h"
#include "rpc/client/client_config.h"
#include "rpc/protobuf_codec.h"
#include "rpc/raw_message.h"

namespace {

class BlockingEchoServer final {
 public:
  void Listen() {
    listener_.Bind("127.0.0.1", 0);
    listener_.Listen(8);
  }

  [[nodiscard]] auto port() const -> std::uint16_t { return listener_.LocalPort(); }

  void ServeOneRequestUntilReleased() {
    xrpc::io::Socket socket = listener_.Accept();
    std::string buffer;
    char chunk[4096];
    xrpc::FrameCodec codec;

    while (true) {
      const xrpc::DecodeResult decoded = codec.TryDecode(buffer);
      if (decoded.error_ == xrpc::ProtocolError::Ok && decoded.request_.has_value()) {
        request_received_.store(true, std::memory_order_release);
        release_response_.get_future().wait();

        const auto &request = *decoded.request_;
        xrpc::test::EchoRequest echo_request = xrpc::ProtobufCodec::Decode<xrpc::test::EchoRequest>(request.payload_);

        xrpc::test::EchoResponse echo_response;
        echo_response.set_message("echo: " + echo_request.message());

        xrpc::ProtocolResponse response;
        response.request_id_ = request.request_id_;
        response.error_code_ = 0;
        response.payload_ = xrpc::ProtobufCodec::Encode(echo_response);
        socket.WriteAll(codec.EncodeResponse(response));
        socket.Close();
        return;
      }

      ASSERT_EQ(decoded.error_, xrpc::ProtocolError::NeedMoreData);
      const ssize_t received = socket.Read(chunk, sizeof(chunk));
      ASSERT_GT(received, 0);
      buffer.append(chunk, static_cast<std::size_t>(received));
    }
  }

  void WaitUntilRequestReceived() {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!request_received_.load(std::memory_order_acquire)) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  void ReleaseResponse() { release_response_.set_value(); }

 private:
  xrpc::io::Socket listener_;
  std::atomic<bool> request_received_{false};
  std::promise<void> release_response_;
};

auto MakeEndpoint(std::uint16_t port) -> xrpc::Endpoint {
  xrpc::Endpoint endpoint;
  endpoint.host_ = "127.0.0.1";
  endpoint.port_ = port;
  return endpoint;
}

auto MakeRequest() -> xrpc::RawRequest {
  xrpc::test::EchoRequest echo_request;
  echo_request.set_message("hello");

  xrpc::RawRequest request;
  request.request_id_ = 1;
  request.service_name_ = "EchoService";
  request.method_name_ = "Echo";
  request.payload_ = xrpc::ProtobufCodec::Encode(echo_request);
  return request;
}

}  // namespace

TEST(ClientChannelTest, UpdateEndpointsDoesNotWaitForBlockingCall) {
  BlockingEchoServer server;
  server.Listen();

  xrpc::ClientConfig config;
  config.default_timeout_ = std::chrono::milliseconds(1000);

  xrpc::ClientChannel channel(config);
  channel.UpdateEndpoints({MakeEndpoint(server.port())});

  std::jthread server_thread([&]() { server.ServeOneRequestUntilReleased(); });
  std::future<xrpc::RawCallResult> call_result =
      std::async(std::launch::async, [&]() { return channel.Call(MakeRequest(), xrpc::CallOptions{}); });

  server.WaitUntilRequestReceived();

  const auto start = std::chrono::steady_clock::now();
  channel.UpdateEndpoints({MakeEndpoint(server.port()), MakeEndpoint(static_cast<std::uint16_t>(server.port() + 1))});
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(elapsed, std::chrono::milliseconds(100));

  server.ReleaseResponse();
  const xrpc::RawCallResult result = call_result.get();
  ASSERT_TRUE(result.HasResponse());
  EXPECT_TRUE(result.response().status_.ok());

  server_thread.join();
}
