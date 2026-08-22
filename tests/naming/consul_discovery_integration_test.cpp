#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <unistd.h>

#include <xrpc/rpc_client.h>

#include "io/socket.h"
#include "proto/echo.pb.h"
#include "protocol/frame_codec.h"
#include "protocol/rpc_envelope.h"

namespace {

class EchoTestServer final {
 public:
  void Listen() {
    listener_.Bind("127.0.0.1", 0);
    listener_.Listen(8);
  }

  [[nodiscard]] auto port() const -> std::uint16_t { return listener_.LocalPort(); }

  void ServeOnce() {
    xrpc::io::Socket socket = listener_.Accept();
    std::string buffer;
    char chunk[4096];
    xrpc::FrameCodec codec;

    while (true) {
      const xrpc::FrameDecodeResult decoded = codec.Decode(buffer);
      if (decoded.error_ == xrpc::ProtocolError::Ok && decoded.request_.has_value()) {
        xrpc::test::EchoRequest request;
        ASSERT_TRUE(request.ParseFromString(decoded.request_->payload_));

        xrpc::test::EchoResponse response;
        response.set_message("echo: " + request.message());

        xrpc::ResponseEnvelope response_envelope;
        response_envelope.request_id_ = decoded.request_->request_id_;
        response_envelope.payload_ = response.SerializeAsString();
        socket.WriteAll(codec.Encode(response_envelope));
        return;
      }

      ASSERT_EQ(decoded.error_, xrpc::ProtocolError::NeedMoreData);
      const ssize_t received = socket.Read(chunk, sizeof(chunk));
      ASSERT_GT(received, 0);
      buffer.append(chunk, static_cast<std::size_t>(received));
    }
  }

 private:
  xrpc::io::Socket listener_;
};

auto ConsulIntegrationEnabled() -> bool {
  const char *enabled = std::getenv("XRPC_ENABLE_CONSUL_TESTS");
  return enabled != nullptr && std::string(enabled) == "1";
}

void ConsulRequest(std::string_view method, std::string_view path, std::string_view body) {
  xrpc::io::Socket socket;
  socket.Connect("127.0.0.1", 8500, std::chrono::milliseconds(500));
  socket.SetReadWriteTimeout(std::chrono::milliseconds(500));

  std::string request;
  request.append(method);
  request.append(" ");
  request.append(path);
  request.append(" HTTP/1.1\r\nHost: 127.0.0.1:8500\r\nConnection: close\r\nContent-Length: ");
  request.append(std::to_string(body.size()));
  request.append("\r\n\r\n");
  request.append(body);
  socket.WriteAll(request);

  std::string response;
  char chunk[1024];
  while (true) {
    const ssize_t received = socket.Read(chunk, sizeof(chunk));
    if (received == 0) {
      break;
    }
    ASSERT_GT(received, 0);
    response.append(chunk, static_cast<std::size_t>(received));
  }

  ASSERT_TRUE(response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.1 204")) << response;
}

auto RegisterServiceBody(std::string_view service_id, std::string_view service_name, std::uint16_t port)
    -> std::string {
  return R"({"ID":")" + std::string(service_id) + R"(","Name":")" + std::string(service_name) +
         R"(","Address":"127.0.0.1","Port":)" + std::to_string(port) + "}";
}

void RegisterConsulService(std::string_view service_id, std::string_view service_name, std::uint16_t port) {
  ConsulRequest("PUT", "/v1/agent/service/register", RegisterServiceBody(service_id, service_name, port));
}

void DeregisterConsulService(std::string_view service_id) {
  ConsulRequest("PUT", "/v1/agent/service/deregister/" + std::string(service_id), "");
}

auto Echo(const xrpc::test::EchoRequest &request) -> xrpc::test::EchoResponse {
  xrpc::test::EchoResponse response;
  response.set_message("echo: " + request.message());
  return response;
}

}  // namespace

TEST(ConsulDiscoveryIntegrationTest, DiscoversRegisteredEndpointAndCallsEcho) {
  if (!ConsulIntegrationEnabled()) {
    GTEST_SKIP() << "set XRPC_ENABLE_CONSUL_TESTS=1 to run live Consul integration tests";
  }

  EchoTestServer server;
  server.Listen();

  const std::string service_name = "XrpcEchoService" + std::to_string(::getpid()) + "_" + std::to_string(server.port());
  const std::string service_id = service_name + "_instance";
  RegisterConsulService(service_id, service_name, server.port());

  std::exception_ptr server_error;
  std::jthread server_thread([&]() {
    try {
      server.ServeOnce();
    } catch (...) {
      server_error = std::current_exception();
    }
  });

  xrpc::RpcClientOptions options;
  options.target_ = "consul://" + service_name;
  options.discovery_refresh_interval_ = std::chrono::milliseconds(200);
  options.timeout_ = std::chrono::milliseconds(1000);

  xrpc::StatusOr<xrpc::RpcClient> client_result = xrpc::RpcClient::Create(options);
  ASSERT_TRUE(client_result.ok()) << client_result.status().message();
  xrpc::RpcClient client = std::move(client_result).value();
  xrpc::test::EchoRequest request;
  request.set_message("hello");

  const xrpc::StatusOr<xrpc::test::EchoResponse> response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
  ASSERT_TRUE(response.ok()) << response.status().message();
  EXPECT_EQ(response.value().message(), "echo: hello");

  server_thread.join();
  DeregisterConsulService(service_id);
  if (server_error) {
    std::rethrow_exception(server_error);
  }
}
