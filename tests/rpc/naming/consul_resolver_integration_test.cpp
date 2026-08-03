#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include <xrpc/rpc_client.h>
#include <xrpc/rpc_server.h>

#include "io/socket.h"
#include "proto/echo.pb.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_error.h"
#include "protocol/protocol_message.h"
#include "rpc/naming/endpoint_resolver.h"
#include "rpc/protobuf_codec.h"

namespace {

class EchoTestServer final {
 public:
  void Listen() {
    listener_.Bind("127.0.0.1", 0);
    listener_.Listen(8);
  }

  [[nodiscard]] auto port() const -> std::uint16_t { return listener_.LocalPort(); }

  void ServeOnce() { ServeRequestsOnSingleConnection(1); }

  void ServeRequestsOnSingleConnection(std::size_t request_count) {
    xrpc::io::Socket socket = listener_.Accept();
    std::string buffer;
    char chunk[4096];
    xrpc::FrameCodec codec;

    for (std::size_t handled = 0; handled < request_count; ++handled) {
      while (true) {
        const xrpc::DecodeResult decoded = codec.TryDecode(buffer);
        if (decoded.error_ == xrpc::ProtocolError::Ok && decoded.request_.has_value()) {
          const auto &request = *decoded.request_;
          const xrpc::test::EchoRequest echo_request =
              xrpc::ProtobufCodec::Decode<xrpc::test::EchoRequest>(request.payload_);

          xrpc::test::EchoResponse echo_response;
          echo_response.set_message("echo: " + echo_request.message());

          xrpc::ProtocolResponse response;
          response.request_id_ = request.request_id_;
          response.error_code_ = 0;
          response.error_text_.clear();
          response.payload_ = xrpc::ProtobufCodec::Encode(echo_response);

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
  }

 private:
  xrpc::io::Socket listener_;
};

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

auto ConsulIntegrationEnabled() -> bool {
  const char *enabled = std::getenv("XRPC_ENABLE_CONSUL_TESTS");
  return enabled != nullptr && std::string(enabled) == "1";
}

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

auto RouteStickyKey(const std::vector<std::string> &endpoint_ids, std::string_view sticky_key) -> std::string {
  // Mirror EndpointSelector's ring so tests can choose a sticky key that is
  // expected to start at a specific live endpoint.
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
  throw std::runtime_error("failed to find sticky key for endpoint");
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

template <typename Predicate>
auto WaitUntil(Predicate predicate, std::chrono::milliseconds timeout) -> bool {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return predicate();
}

}  // namespace

TEST(ConsulResolverIntegrationTest, RpcClientDiscoversEndpointFromConsul) {
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
  options.discovery_refresh_interval_ = std::chrono::milliseconds(1000);
  options.timeout_ = std::chrono::milliseconds(500);

  xrpc::test::EchoRequest request;
  request.set_message("hello");

  {
    xrpc::RpcClient client(std::move(options));
    const xrpc::StatusOr<xrpc::test::EchoResponse> response =
        client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
    ASSERT_TRUE(response.ok()) << response.status().message();
    EXPECT_EQ(response.value().message(), "echo: hello");
  }

  server_thread.join();
  DeregisterConsulService(service_id);
  if (server_error) {
    std::rethrow_exception(server_error);
  }
}

TEST(ConsulResolverIntegrationTest, RpcClientSwitchesToNewEndpointAfterConsulUpdate) {
  if (!ConsulIntegrationEnabled()) {
    GTEST_SKIP() << "set XRPC_ENABLE_CONSUL_TESTS=1 to run live Consul integration tests";
  }

  EchoTestServer server_a;
  server_a.Listen();
  EchoTestServer server_b;
  server_b.Listen();

  const std::string service_name = "XrpcEchoSwitchService" + std::to_string(::getpid());
  const std::string service_id_a = service_name + "_a";
  const std::string service_id_b = service_name + "_b";
  RegisterConsulService(service_id_a, service_name, server_a.port());

  std::exception_ptr server_a_error;
  std::jthread server_a_thread([&]() {
    try {
      server_a.ServeOnce();
    } catch (...) {
      server_a_error = std::current_exception();
    }
  });

  xrpc::RpcClientOptions options;
  options.target_ = "consul://" + service_name;
  options.discovery_refresh_interval_ = std::chrono::milliseconds(500);
  options.timeout_ = std::chrono::milliseconds(500);
  xrpc::RpcClient client(std::move(options));

  xrpc::test::EchoRequest request;
  request.set_message("hello");
  const xrpc::StatusOr<xrpc::test::EchoResponse> first_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
  ASSERT_TRUE(first_response.ok()) << first_response.status().message();
  EXPECT_EQ(first_response.value().message(), "echo: hello");

  server_a_thread.join();
  DeregisterConsulService(service_id_a);
  RegisterConsulService(service_id_b, service_name, server_b.port());

  std::exception_ptr server_b_error;
  std::jthread server_b_thread([&]() {
    try {
      server_b.ServeOnce();
    } catch (...) {
      server_b_error = std::current_exception();
    }
  });

  xrpc::StatusOr<xrpc::test::EchoResponse> second_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
  if (!second_response.ok()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    second_response = client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
  }
  ASSERT_TRUE(second_response.ok()) << second_response.status().message();
  EXPECT_EQ(second_response.value().message(), "echo: hello");

  server_b_thread.join();
  DeregisterConsulService(service_id_b);
  if (server_a_error) {
    std::rethrow_exception(server_a_error);
  }
  if (server_b_error) {
    std::rethrow_exception(server_b_error);
  }
}

TEST(ConsulResolverIntegrationTest, ExistingEndpointTransportIsReusedAfterAddingEndpoint) {
  if (!ConsulIntegrationEnabled()) {
    GTEST_SKIP() << "set XRPC_ENABLE_CONSUL_TESTS=1 to run live Consul integration tests";
  }

  EchoTestServer server_a;
  server_a.Listen();
  EchoTestServer server_b;
  server_b.Listen();

  const std::string endpoint_a = "127.0.0.1:" + std::to_string(server_a.port());
  const std::string endpoint_b = "127.0.0.1:" + std::to_string(server_b.port());

  const std::string service_name = "XrpcReuseService" + std::to_string(::getpid());
  const std::string service_id_a = service_name + "_a";
  const std::string service_id_b = service_name + "_b";
  RegisterConsulService(service_id_a, service_name, server_a.port());

  std::exception_ptr server_a_error;
  std::jthread server_a_thread([&]() {
    try {
      server_a.ServeRequestsOnSingleConnection(2);
    } catch (...) {
      server_a_error = std::current_exception();
    }
  });

  xrpc::RpcClientOptions options;
  options.target_ = "consul://" + service_name;
  options.discovery_refresh_interval_ = std::chrono::milliseconds(500);
  options.timeout_ = std::chrono::milliseconds(500);
  xrpc::RpcClient client(std::move(options));

  xrpc::test::EchoRequest request;
  request.set_message("hello");

  const xrpc::StatusOr<xrpc::test::EchoResponse> first_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
  ASSERT_TRUE(first_response.ok()) << first_response.status().message();

  RegisterConsulService(service_id_b, service_name, server_b.port());
  std::this_thread::sleep_for(std::chrono::milliseconds(700));

  xrpc::CallOptions call_options;
  call_options.sticky_key_ = StickyKeyForEndpoint(endpoint_a, {endpoint_a, endpoint_b});
  const xrpc::StatusOr<xrpc::test::EchoResponse> second_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, call_options);
  ASSERT_TRUE(second_response.ok()) << second_response.status().message();
  EXPECT_EQ(second_response.value().message(), "echo: hello");

  server_a_thread.join();
  DeregisterConsulService(service_id_a);
  DeregisterConsulService(service_id_b);
  if (server_a_error) {
    std::rethrow_exception(server_a_error);
  }
}

TEST(ConsulResolverIntegrationTest, RemovedEndpointIsNotSelectedForNewCalls) {
  if (!ConsulIntegrationEnabled()) {
    GTEST_SKIP() << "set XRPC_ENABLE_CONSUL_TESTS=1 to run live Consul integration tests";
  }

  EchoTestServer server_a;
  server_a.Listen();
  EchoTestServer server_b;
  server_b.Listen();

  const std::string endpoint_a = "127.0.0.1:" + std::to_string(server_a.port());
  const std::string endpoint_b = "127.0.0.1:" + std::to_string(server_b.port());

  const std::string service_name = "XrpcRemoveService" + std::to_string(::getpid());
  const std::string service_id_a = service_name + "_a";
  const std::string service_id_b = service_name + "_b";
  RegisterConsulService(service_id_a, service_name, server_a.port());
  RegisterConsulService(service_id_b, service_name, server_b.port());

  const std::string sticky_for_a = StickyKeyForEndpoint(endpoint_a, {endpoint_a, endpoint_b});

  std::exception_ptr server_a_error;
  std::jthread server_a_thread([&]() {
    try {
      server_a.ServeOnce();
    } catch (...) {
      server_a_error = std::current_exception();
    }
  });

  xrpc::RpcClientOptions options;
  options.target_ = "consul://" + service_name;
  options.discovery_refresh_interval_ = std::chrono::milliseconds(500);
  options.timeout_ = std::chrono::milliseconds(500);
  xrpc::RpcClient client(std::move(options));

  xrpc::test::EchoRequest request;
  request.set_message("hello");

  xrpc::CallOptions call_options;
  call_options.sticky_key_ = sticky_for_a;
  const xrpc::StatusOr<xrpc::test::EchoResponse> first_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, call_options);
  ASSERT_TRUE(first_response.ok()) << first_response.status().message();

  server_a_thread.join();
  DeregisterConsulService(service_id_a);

  std::exception_ptr server_b_error;
  std::jthread server_b_thread([&]() {
    try {
      server_b.ServeOnce();
    } catch (...) {
      server_b_error = std::current_exception();
    }
  });

  xrpc::StatusOr<xrpc::test::EchoResponse> second_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, call_options);
  if (!second_response.ok()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    second_response = client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, call_options);
  }
  ASSERT_TRUE(second_response.ok()) << second_response.status().message();
  EXPECT_EQ(second_response.value().message(), "echo: hello");

  server_b_thread.join();
  DeregisterConsulService(service_id_b);
  if (server_a_error) {
    std::rethrow_exception(server_a_error);
  }
  if (server_b_error) {
    std::rethrow_exception(server_b_error);
  }
}

TEST(ConsulResolverIntegrationTest, EmptySnapshotMakesSubsequentClientCallsFailBeforeUsingOldEndpoint) {
  if (!ConsulIntegrationEnabled()) {
    GTEST_SKIP() << "set XRPC_ENABLE_CONSUL_TESTS=1 to run live Consul integration tests";
  }

  EchoTestServer server;
  server.Listen();

  const std::string service_name = "XrpcEmptySnapshotService" + std::to_string(::getpid());
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
  options.discovery_refresh_interval_ = std::chrono::milliseconds(100);
  options.timeout_ = std::chrono::milliseconds(200);
  xrpc::RpcClient client(std::move(options));

  xrpc::test::EchoRequest request;
  request.set_message("hello");

  const xrpc::StatusOr<xrpc::test::EchoResponse> first_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
  ASSERT_TRUE(first_response.ok()) << first_response.status().message();
  EXPECT_EQ(first_response.value().message(), "echo: hello");
  server_thread.join();
  if (server_error) {
    std::rethrow_exception(server_error);
  }

  DeregisterConsulService(service_id);
  const bool empty_snapshot_observed = WaitUntil(
      [&client, &request]() {
        const xrpc::StatusOr<xrpc::test::EchoResponse> response =
            client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
        return !response.ok() && response.status().code() == xrpc::StatusCode::Unavailable &&
               response.status().message().find("resolver has no endpoints") != std::string::npos;
      },
      std::chrono::milliseconds(1000));
  ASSERT_TRUE(empty_snapshot_observed);

  const xrpc::StatusOr<xrpc::test::EchoResponse> second_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request);
  ASSERT_FALSE(second_response.ok());
  EXPECT_EQ(second_response.status().code(), xrpc::StatusCode::Unavailable);
  EXPECT_NE(second_response.status().message().find("resolver has no endpoints"), std::string::npos);
}

TEST(ConsulResolverIntegrationTest, MixedAddRemoveUpdateKeepsRetainedAndSelectsAddedEndpoint) {
  if (!ConsulIntegrationEnabled()) {
    GTEST_SKIP() << "set XRPC_ENABLE_CONSUL_TESTS=1 to run live Consul integration tests";
  }

  EchoTestServer server_a;
  server_a.Listen();
  EchoTestServer server_b;
  server_b.Listen();
  EchoTestServer server_c;
  server_c.Listen();

  const std::string endpoint_a = "127.0.0.1:" + std::to_string(server_a.port());
  const std::string endpoint_b = "127.0.0.1:" + std::to_string(server_b.port());
  const std::string endpoint_c = "127.0.0.1:" + std::to_string(server_c.port());

  const std::string service_name = "XrpcMixedUpdateService" + std::to_string(::getpid());
  const std::string service_id_a = service_name + "_a";
  const std::string service_id_b = service_name + "_b";
  const std::string service_id_c = service_name + "_c";
  RegisterConsulService(service_id_a, service_name, server_a.port());
  RegisterConsulService(service_id_b, service_name, server_b.port());

  std::exception_ptr server_b_error;
  std::jthread server_b_thread([&]() {
    try {
      server_b.ServeRequestsOnSingleConnection(2);
    } catch (...) {
      server_b_error = std::current_exception();
    }
  });

  std::exception_ptr server_c_error;
  std::jthread server_c_thread([&]() {
    try {
      server_c.ServeOnce();
    } catch (...) {
      server_c_error = std::current_exception();
    }
  });

  xrpc::RpcClientOptions options;
  options.target_ = "consul://" + service_name;
  options.discovery_refresh_interval_ = std::chrono::milliseconds(500);
  options.timeout_ = std::chrono::milliseconds(500);
  xrpc::RpcClient client(std::move(options));

  xrpc::test::EchoRequest request;
  request.set_message("hello");

  xrpc::CallOptions options_for_b_before_update;
  options_for_b_before_update.sticky_key_ = StickyKeyForEndpoint(endpoint_b, {endpoint_a, endpoint_b});
  const xrpc::StatusOr<xrpc::test::EchoResponse> first_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, options_for_b_before_update);
  ASSERT_TRUE(first_response.ok()) << first_response.status().message();

  DeregisterConsulService(service_id_a);
  RegisterConsulService(service_id_c, service_name, server_c.port());
  std::this_thread::sleep_for(std::chrono::milliseconds(700));

  xrpc::CallOptions options_for_b_after_update;
  options_for_b_after_update.sticky_key_ = StickyKeyForEndpoint(endpoint_b, {endpoint_b, endpoint_c});
  const xrpc::StatusOr<xrpc::test::EchoResponse> second_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, options_for_b_after_update);
  ASSERT_TRUE(second_response.ok()) << second_response.status().message();

  xrpc::CallOptions options_for_c_after_update;
  options_for_c_after_update.sticky_key_ = StickyKeyForEndpoint(endpoint_c, {endpoint_b, endpoint_c});
  xrpc::StatusOr<xrpc::test::EchoResponse> third_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, options_for_c_after_update);
  if (!third_response.ok()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    third_response = client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request, options_for_c_after_update);
  }
  ASSERT_TRUE(third_response.ok()) << third_response.status().message();

  server_b_thread.join();
  server_c_thread.join();
  DeregisterConsulService(service_id_b);
  DeregisterConsulService(service_id_c);
  if (server_b_error) {
    std::rethrow_exception(server_b_error);
  }
  if (server_c_error) {
    std::rethrow_exception(server_c_error);
  }
}

TEST(ConsulResolverIntegrationTest, RpcServerRegistersAndDeregistersServiceInConsul) {
  if (!ConsulIntegrationEnabled()) {
    GTEST_SKIP() << "set XRPC_ENABLE_CONSUL_TESTS=1 to run live Consul integration tests";
  }

  const std::string service_name = "XrpcServerAutoRegisterService" + std::to_string(::getpid());

  xrpc::RpcServerOptions server_options;
  server_options.service_name_ = service_name;
  server_options.service_address_ = "127.0.0.1";
  server_options.consul_timeout_ = std::chrono::milliseconds(1000);

  xrpc::RpcServer server(server_options);
  server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService", "Echo", Echo);
  server.Listen("127.0.0.1", 0);

  auto resolver = xrpc::MakeEndpointResolver(xrpc::ResolverOptions{
      .target_ = "consul://" + service_name,
      .consul_address_ = "127.0.0.1:8500",
      .discovery_refresh_interval_ = std::chrono::milliseconds(500),
  });
  const xrpc::Status start_status = resolver->Start();
  ASSERT_TRUE(start_status.ok()) << start_status.message();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  const auto registered_snapshot = resolver->Snapshot();
  EXPECT_FALSE(registered_snapshot.empty());

  server.Stop();

  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  const auto after_stop_snapshot = resolver->Snapshot();
  EXPECT_TRUE(after_stop_snapshot.empty());
  resolver->Stop();
}
