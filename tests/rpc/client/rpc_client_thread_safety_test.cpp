#include <gtest/gtest.h>

#include <poll.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <latch>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <xrpc/rpc_client.h>

#include "io/socket.h"
#include "proto/echo.pb.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_error.h"
#include "protocol/protocol_message.h"
#include "rpc/protobuf_codec.h"

namespace {

class EchoTestServer final {
 public:
  EchoTestServer() = default;

  void Listen() {
    listener_.Bind("127.0.0.1", 0);
    listener_.Listen(8);
  }

  [[nodiscard]] auto port() const -> std::uint16_t { return listener_.LocalPort(); }

  void ServeSingleConnectionOutOfOrderBatch(std::size_t request_count) {
    // Respond in reverse order to prove concurrent callers are matched by
    // request id, not by write order on the shared TCP connection.
    xrpc::io::Socket socket = listener_.Accept();
    std::string buffer;
    xrpc::FrameCodec codec;
    std::vector<xrpc::ProtocolRequest> requests;
    requests.reserve(request_count);
    for (std::size_t handled = 0; handled < request_count; ++handled) {
      requests.push_back(ReadRequest(socket, buffer, codec));
    }

    for (auto it = requests.rbegin(); it != requests.rend(); ++it) {
      socket.WriteAll(codec.EncodeResponse(MakeEchoResponse(*it)));
    }
    socket.Close();
  }

  void ServeOneRequestAfterRelease(std::promise<void> &request_received, std::shared_future<void> release_response,
                                   bool &extra_request_received) {
    xrpc::io::Socket socket = listener_.Accept();
    std::string buffer;
    xrpc::FrameCodec codec;
    const xrpc::ProtocolRequest request = ReadRequest(socket, buffer, codec);
    request_received.set_value();

    release_response.wait();

    pollfd client{};
    client.fd = socket.fd();
    client.events = POLLIN;
    const int poll_result = ::poll(&client, 1, 50);
    extra_request_received = poll_result > 0 && (client.revents & POLLIN) != 0;

    socket.WriteAll(codec.EncodeResponse(MakeEchoResponse(request)));
    socket.Close();
  }

 private:
  static auto ReadRequest(xrpc::io::Socket &socket, std::string &buffer, xrpc::FrameCodec &codec)
      -> xrpc::ProtocolRequest {
    char chunk[4096];
    while (true) {
      const xrpc::DecodeResult decoded = codec.TryDecode(buffer);
      if (decoded.error_ == xrpc::ProtocolError::Ok && decoded.HasMessage()) {
        if (!decoded.request_.has_value()) {
          throw std::runtime_error("expected request frame");
        }

        xrpc::ProtocolRequest request = *decoded.request_;
        buffer.erase(0, decoded.consumed_);
        return request;
      }

      if (decoded.error_ != xrpc::ProtocolError::NeedMoreData) {
        throw std::runtime_error("failed to decode request frame");
      }

      const ssize_t received = socket.Read(chunk, sizeof(chunk));
      if (received <= 0) {
        throw std::runtime_error("connection closed before request frame was complete");
      }
      buffer.append(chunk, static_cast<std::size_t>(received));
    }
  }

  static auto MakeEchoResponse(const xrpc::ProtocolRequest &request) -> xrpc::ProtocolResponse {
    const xrpc::test::EchoRequest echo_request = xrpc::ProtobufCodec::Decode<xrpc::test::EchoRequest>(request.payload_);

    xrpc::test::EchoResponse echo_response;
    echo_response.set_message("echo: " + echo_request.message());

    xrpc::ProtocolResponse response;
    response.request_id_ = request.request_id_;
    response.error_code_ = 0;
    response.error_text_.clear();
    response.payload_ = xrpc::ProtobufCodec::Encode(echo_response);
    return response;
  }

  xrpc::io::Socket listener_;
};

auto MakeTarget(std::uint16_t port) -> std::string { return "list://127.0.0.1:" + std::to_string(port); }

void RunConcurrentCalls(xrpc::RpcClient &client, std::size_t thread_count) {
  std::latch start_latch(1);
  std::vector<std::jthread> threads;
  threads.reserve(thread_count);

  for (std::size_t thread_index = 0; thread_index < thread_count; ++thread_index) {
    threads.emplace_back([&, thread_index]() {
      xrpc::test::EchoRequest request;
      request.set_message("t" + std::to_string(thread_index));
      start_latch.wait();
      const xrpc::StatusOr<xrpc::test::EchoResponse> response =
          client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
      ASSERT_TRUE(response.ok()) << response.status().message();
      EXPECT_EQ(response.value().message(), "echo: " + request.message());
    });
  }

  start_latch.count_down();
  for (auto &thread : threads) {
    thread.join();
  }
}

}  // namespace

TEST(RpcClientThreadSafetyTest, ConnectionSupportsConcurrentCallsOnSharedConnection) {
  EchoTestServer server;
  server.Listen();

  std::exception_ptr server_error;
  std::jthread server_thread([&]() {
    try {
      server.ServeSingleConnectionOutOfOrderBatch(4);
    } catch (...) {
      server_error = std::current_exception();
    }
  });

  xrpc::RpcClientOptions options;
  options.target_ = MakeTarget(server.port());
  options.timeout_ = std::chrono::milliseconds(2000);

  {
    xrpc::StatusOr<xrpc::RpcClient> client_result = xrpc::RpcClient::Create(options);
    ASSERT_TRUE(client_result.ok()) << client_result.status().message();
    xrpc::RpcClient client = std::move(client_result).value();
    RunConcurrentCalls(client, 4);
  }

  server_thread.join();
  if (server_error) {
    std::rethrow_exception(server_error);
  }
}

TEST(RpcClientThreadSafetyTest, MaxInflightPerEndpointFailsFastWithoutSendingSecondRequest) {
  EchoTestServer server;
  server.Listen();

  std::promise<void> request_received;
  std::future<void> request_received_future = request_received.get_future();
  std::promise<void> release_response;
  std::shared_future<void> release_response_future = release_response.get_future().share();
  bool extra_request_received = false;
  std::exception_ptr server_error;
  std::jthread server_thread([&]() {
    try {
      server.ServeOneRequestAfterRelease(request_received, release_response_future, extra_request_received);
    } catch (...) {
      server_error = std::current_exception();
    }
  });

  xrpc::RpcClientOptions options;
  options.target_ = MakeTarget(server.port());
  options.timeout_ = std::chrono::milliseconds(2000);
  options.max_inflight_per_endpoint_ = 1;

  xrpc::StatusOr<xrpc::RpcClient> client_result = xrpc::RpcClient::Create(options);
  ASSERT_TRUE(client_result.ok()) << client_result.status().message();
  xrpc::RpcClient client = std::move(client_result).value();
  xrpc::StatusOr<xrpc::test::EchoResponse> first_response(xrpc::Status{xrpc::StatusCode::Internal, "not set"});
  std::jthread first_call_thread([&]() {
    xrpc::test::EchoRequest request;
    request.set_message("first");
    first_response = client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
  });

  ASSERT_EQ(request_received_future.wait_for(std::chrono::milliseconds(1000)), std::future_status::ready);

  xrpc::test::EchoRequest second_request;
  second_request.set_message("second");
  const xrpc::StatusOr<xrpc::test::EchoResponse> second_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", second_request);

  EXPECT_FALSE(second_response.ok());
  EXPECT_EQ(second_response.status().code(), xrpc::StatusCode::ResourceExhausted);

  release_response.set_value();
  first_call_thread.join();
  server_thread.join();
  if (server_error) {
    std::rethrow_exception(server_error);
  }

  ASSERT_TRUE(first_response.ok()) << first_response.status().message();
  EXPECT_EQ(first_response.value().message(), "echo: first");
  EXPECT_FALSE(extra_request_received);
}
