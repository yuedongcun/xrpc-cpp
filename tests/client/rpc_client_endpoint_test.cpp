#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <xrpc/rpc_client.h>

#include "io/socket.h"
#include "proto/echo.pb.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_message.h"

namespace {

class EchoTestServer final {
 public:
  EchoTestServer() = default;

  void Listen() {
    listener_.Bind("127.0.0.1", 0);
    listener_.Listen(8);
  }

  [[nodiscard]] auto port() const -> std::uint16_t { return listener_.LocalPort(); }

  void ServeConnections(std::size_t connection_count) {
    for (std::size_t i = 0; i < connection_count; ++i) {
      ServeOneConnection(1);
    }
  }

  void ServeOnce() { ServeConnections(1); }
  void ServeConnectionRequestBatches(const std::vector<std::size_t> &requests_per_connection) {
    for (std::size_t request_count : requests_per_connection) {
      ServeOneConnection(request_count);
    }
  }

 private:
  void ServeOneConnection(std::size_t request_count) {
    xrpc::io::Socket socket = listener_.Accept();
    std::string buffer;
    char chunk[4096];
    xrpc::FrameCodec codec;

    for (std::size_t handled = 0; handled < request_count; ++handled) {
      while (true) {
        const xrpc::DecodeResult decoded = codec.TryDecode(buffer);
        if (decoded.error_ == xrpc::ProtocolError::Ok && decoded.request_.has_value()) {
          const auto &request = *decoded.request_;
          xrpc::test::EchoRequest echo_request;
          ASSERT_TRUE(echo_request.ParseFromString(request.payload_));

          xrpc::test::EchoResponse echo_response;
          echo_response.set_message("echo: " + echo_request.message());

          xrpc::ProtocolResponse response;
          response.request_id_ = request.request_id_;
          response.error_code_ = 0;
          response.error_text_.clear();
          response.payload_ = echo_response.SerializeAsString();

          socket.WriteAll(codec.EncodeResponse(response));
          buffer.erase(0, decoded.consumed_);
          break;
        }

        ASSERT_EQ(decoded.error_, xrpc::ProtocolError::NeedMoreData);
        const ssize_t received = socket.Read(chunk, sizeof(chunk));
        ASSERT_GT(received, 0);
        buffer.append(chunk, static_cast<std::size_t>(received));
      }
    }

    socket.Close();
  }

  xrpc::io::Socket listener_;
};

constexpr std::size_t VIRTUAL_NODE_COUNT = 128;
constexpr std::uint64_t FNV1A_OFFSET_BASIS = 14695981039346656037ULL;
constexpr std::uint64_t FNV1A_PRIME = 1099511628211ULL;

auto Fnv1a64(std::string_view value) -> std::uint64_t {
  std::uint64_t hash = FNV1A_OFFSET_BASIS;
  for (const char ch : value) {
    hash ^= static_cast<unsigned char>(ch);
    hash *= FNV1A_PRIME;
  }
  return hash;
}

auto MakeLoopbackEndpointId(std::uint16_t port) -> std::string { return "127.0.0.1:" + std::to_string(port); }

auto RouteStickyKey(const std::vector<std::string> &endpoint_ids, std::string_view sticky_key) -> std::string {
  std::vector<std::pair<std::uint64_t, std::string>> ring;
  for (const std::string &endpoint_id : endpoint_ids) {
    for (std::size_t vnode = 0; vnode < VIRTUAL_NODE_COUNT; ++vnode) {
      ring.emplace_back(Fnv1a64(endpoint_id + "#" + std::to_string(vnode)), endpoint_id);
    }
  }
  std::sort(ring.begin(), ring.end());
  const std::uint64_t key_hash = Fnv1a64(sticky_key);
  const auto it = std::lower_bound(ring.begin(), ring.end(), std::pair<std::uint64_t, std::string>{key_hash, {}});
  return it == ring.end() ? ring.front().second : it->second;
}

auto StickyKeyForEndpoint(std::string_view endpoint_id, const std::vector<std::string> &endpoint_ids) -> std::string {
  for (std::size_t i = 0; i < 10000; ++i) {
    const std::string candidate = "sticky-" + std::to_string(i);
    if (RouteStickyKey(endpoint_ids, candidate) == endpoint_id) {
      return candidate;
    }
  }
  throw std::runtime_error("failed to find sticky key for endpoint index");
}

}  // namespace

TEST(RpcClientEndpointTest, CallFallsBackToNextEndpointOnConnectFailure) {
  EchoTestServer server;
  server.Listen();

  std::exception_ptr server_error;
  std::jthread server_thread([&]() {
    try {
      server.ServeOnce();
    } catch (...) {
      server_error = std::current_exception();
    }
  });

  xrpc::RpcClientOptions options;
  options.target_ = "list://127.0.0.1:1,127.0.0.1:" + std::to_string(server.port());

  xrpc::test::EchoRequest request;
  request.set_message("hello");

  {
    xrpc::StatusOr<xrpc::RpcClient> client_result = xrpc::RpcClient::Create(options);
    ASSERT_TRUE(client_result.ok()) << client_result.status().message();
    xrpc::RpcClient client = std::move(client_result).value();
    const xrpc::StatusOr<xrpc::test::EchoResponse> response =
        client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
    ASSERT_TRUE(response.ok()) << response.status().message();
    EXPECT_EQ(response.value().message(), "echo: hello");
  }

  server_thread.join();
  if (server_error) {
    std::rethrow_exception(server_error);
  }
}

TEST(RpcClientEndpointTest, MultiplexedConnectionRoutesByStickyKeyAndReusesPerEndpointTransport) {
  EchoTestServer endpoint0_server;
  endpoint0_server.Listen();

  EchoTestServer endpoint1_server;
  endpoint1_server.Listen();

  std::exception_ptr endpoint0_error;
  std::jthread endpoint0_thread([&]() {
    try {
      endpoint0_server.ServeConnectionRequestBatches({2});
    } catch (...) {
      endpoint0_error = std::current_exception();
    }
  });

  std::exception_ptr endpoint1_error;
  std::jthread endpoint1_thread([&]() {
    try {
      endpoint1_server.ServeConnectionRequestBatches({1});
    } catch (...) {
      endpoint1_error = std::current_exception();
    }
  });

  xrpc::RpcClientOptions options;
  options.target_ = "list://127.0.0.1:" + std::to_string(endpoint0_server.port()) +
                    ",127.0.0.1:" + std::to_string(endpoint1_server.port());
  options.timeout_ = std::chrono::milliseconds(50);

  xrpc::test::EchoRequest request;
  request.set_message("hello");

  xrpc::CallOptions options_a;
  std::vector<std::string> endpoint_ids = {MakeLoopbackEndpointId(endpoint0_server.port()),
                                           MakeLoopbackEndpointId(endpoint1_server.port())};
  std::sort(endpoint_ids.begin(), endpoint_ids.end());
  options_a.sticky_key_ = StickyKeyForEndpoint(MakeLoopbackEndpointId(endpoint0_server.port()), endpoint_ids);

  xrpc::CallOptions options_b;
  options_b.sticky_key_ = StickyKeyForEndpoint(MakeLoopbackEndpointId(endpoint1_server.port()), endpoint_ids);

  {
    xrpc::StatusOr<xrpc::RpcClient> client_result = xrpc::RpcClient::Create(options);
    ASSERT_TRUE(client_result.ok()) << client_result.status().message();
    xrpc::RpcClient client = std::move(client_result).value();

    const xrpc::StatusOr<xrpc::test::EchoResponse> first_response =
        client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, options_a);
    ASSERT_TRUE(first_response.ok()) << first_response.status().message();
    EXPECT_EQ(first_response.value().message(), "echo: hello");

    const xrpc::StatusOr<xrpc::test::EchoResponse> second_response =
        client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, options_b);
    ASSERT_TRUE(second_response.ok()) << second_response.status().message();
    EXPECT_EQ(second_response.value().message(), "echo: hello");

    const xrpc::StatusOr<xrpc::test::EchoResponse> third_response =
        client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, options_a);
    ASSERT_TRUE(third_response.ok()) << third_response.status().message();
    EXPECT_EQ(third_response.value().message(), "echo: hello");
  }

  endpoint0_thread.join();
  endpoint1_thread.join();
  if (endpoint0_error) {
    std::rethrow_exception(endpoint0_error);
  }
  if (endpoint1_error) {
    std::rethrow_exception(endpoint1_error);
  }
}

TEST(RpcClientEndpointTest, RejectsEmptyTargetAtCreation) {
  xrpc::RpcClientOptions options;

  const xrpc::StatusOr<xrpc::RpcClient> result = xrpc::RpcClient::Create(options);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), xrpc::StatusCode::InvalidArgument);
}

TEST(RpcClientEndpointTest, RejectsZeroMaxPayloadSizeAtCreation) {
  xrpc::RpcClientOptions options;
  options.target_ = "list://127.0.0.1:9000";
  options.max_payload_size_ = 0;

  const xrpc::StatusOr<xrpc::RpcClient> result = xrpc::RpcClient::Create(options);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), xrpc::StatusCode::InvalidArgument);
}

TEST(RpcClientEndpointTest, CallPayloadRejectsPayloadLargerThanConfiguredLimitBeforeConnect) {
  xrpc::RpcClientOptions options;
  options.target_ = "list://127.0.0.1:1";
  options.max_payload_size_ = 3;

  xrpc::StatusOr<xrpc::RpcClient> client_result = xrpc::RpcClient::Create(options);
  ASSERT_TRUE(client_result.ok()) << client_result.status().message();
  xrpc::RpcClient client = std::move(client_result).value();
  const xrpc::StatusOr<std::string> response = client.CallPayload("EchoService", "Echo", "1234");

  ASSERT_FALSE(response.ok());
  EXPECT_EQ(response.status().code(), xrpc::StatusCode::ResourceExhausted);
}

TEST(RpcClientEndpointTest, CallRejectsNegativeTimeout) {
  xrpc::RpcClientOptions options;
  options.target_ = "list://127.0.0.1:9000";

  xrpc::StatusOr<xrpc::RpcClient> client_result = xrpc::RpcClient::Create(options);
  ASSERT_TRUE(client_result.ok()) << client_result.status().message();
  xrpc::RpcClient client = std::move(client_result).value();
  xrpc::CallOptions call_options;
  call_options.timeout_ = std::chrono::milliseconds(-1);

  xrpc::test::EchoRequest request;
  request.set_message("hello");

  const xrpc::StatusOr<xrpc::test::EchoResponse> response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, call_options);
  EXPECT_FALSE(response.ok());
  EXPECT_EQ(response.status().code(), xrpc::StatusCode::InvalidArgument);
}
