#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include <xrpc/rpc_client.h>
#include <xrpc/rpc_server.h>

#include "io/socket.h"
#include "io/socket_error.h"
#include "proto/echo.pb.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_message.h"

namespace {

constexpr auto WaitTimeout = std::chrono::milliseconds(1000);

auto Echo(const xrpc::test::EchoRequest &request) -> xrpc::test::EchoResponse {
  xrpc::test::EchoResponse response;
  response.set_message(request.message());
  return response;
}

auto MakeServer() -> xrpc::RpcServer {
  xrpc::StatusOr<xrpc::RpcServer> result = xrpc::RpcServer::Create();
  if (!result.ok()) {
    throw std::runtime_error(result.status().message());
  }
  return std::move(result).value();
}

auto MakeRequestFrame(std::string message, std::uint64_t request_id) -> std::string {
  xrpc::test::EchoRequest request;
  request.set_message(std::move(message));

  xrpc::ProtocolRequest protocol_request;
  protocol_request.request_id_ = request_id;
  protocol_request.service_name_ = "EchoService";
  protocol_request.method_name_ = "Echo";
  protocol_request.payload_ = request.SerializeAsString();

  xrpc::FrameCodec codec;
  return codec.EncodeRequest(protocol_request);
}

auto RecvFrame(xrpc::io::Socket &socket) -> std::string {
  std::string buffer;
  char chunk[4096];
  xrpc::FrameCodec codec;

  while (true) {
    const xrpc::DecodeResult decoded = codec.TryDecode(buffer);
    if (decoded.error_ == xrpc::ProtocolError::Ok && decoded.response_.has_value()) {
      std::string frame = buffer.substr(0, decoded.consumed_);
      buffer.erase(0, decoded.consumed_);
      return frame;
    }

    EXPECT_EQ(decoded.error_, xrpc::ProtocolError::NeedMoreData);
    const ssize_t received = socket.Read(chunk, sizeof(chunk));
    if (received <= 0) {
      throw std::runtime_error("failed to receive frame");
    }
    buffer.append(chunk, static_cast<std::size_t>(received));
  }
}

}  // namespace

TEST(RpcServerLifecycleTest, RunBeforeListenReturnsFailedPrecondition) {
  xrpc::RpcServer server = MakeServer();
  const xrpc::Status status = server.Run();
  EXPECT_EQ(status.code(), xrpc::StatusCode::FailedPrecondition);
}

TEST(RpcServerLifecycleTest, StopUnblocksBlockingRun) {
  xrpc::RpcServer server = MakeServer();
  ASSERT_TRUE(server.Listen("127.0.0.1", 0).ok());

  bool run_returned = false;
  std::promise<void> run_started;
  std::future<void> run_started_future = run_started.get_future();
  std::jthread run_thread([&]() {
    run_started.set_value();
    EXPECT_TRUE(server.Run().ok());
    run_returned = true;
  });

  ASSERT_EQ(run_started_future.wait_for(WaitTimeout), std::future_status::ready);
  server.Stop();

  if (run_thread.joinable()) {
    run_thread.join();
  }

  EXPECT_TRUE(run_returned);
}

TEST(RpcServerLifecycleTest, DestructorClosesListeningRuntime) {
  std::uint16_t port = 0;
  {
    xrpc::RpcServer server = MakeServer();
    ASSERT_TRUE(server.Listen("127.0.0.1", 0).ok());
    const xrpc::StatusOr<std::uint16_t> port_result = server.port();
    ASSERT_TRUE(port_result.ok()) << port_result.status().message();
    port = port_result.value();
  }

  xrpc::io::Socket socket;
  EXPECT_THROW(socket.Connect("127.0.0.1", port, std::chrono::milliseconds(100)), xrpc::io::SocketError);
}

TEST(RpcServerLifecycleTest, ListenTwiceReturnsFailedPrecondition) {
  xrpc::RpcServer server = MakeServer();
  ASSERT_TRUE(server.Listen("127.0.0.1", 0).ok());
  EXPECT_EQ(server.Listen("127.0.0.1", 0).code(), xrpc::StatusCode::FailedPrecondition);
}

TEST(RpcServerLifecycleTest, RegisterMethodAfterListenReturnsFailedPrecondition) {
  xrpc::RpcServer server = MakeServer();
  ASSERT_TRUE(server.Listen("127.0.0.1", 0).ok());

  const xrpc::Status status =
      server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService", "Echo", Echo);
  EXPECT_EQ(status.code(), xrpc::StatusCode::FailedPrecondition);
}

TEST(RpcServerLifecycleTest, RegisterMethodAfterStopReturnsFailedPrecondition) {
  xrpc::RpcServer server = MakeServer();
  server.Stop();

  const xrpc::Status status =
      server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService", "Echo", Echo);
  EXPECT_EQ(status.code(), xrpc::StatusCode::FailedPrecondition);
}

TEST(RpcServerLifecycleTest, ConcurrentListenAndRegisterMethodAreSerialized) {
  xrpc::RpcServer server = MakeServer();
  std::atomic<bool> start = false;
  xrpc::Status listen_status;
  xrpc::Status register_status;

  std::jthread listen_thread([&]() {
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    listen_status = server.Listen("127.0.0.1", 0);
  });
  std::jthread register_thread([&]() {
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    register_status =
        server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService", "Echo", Echo);
  });

  start.store(true, std::memory_order_release);
  listen_thread.join();
  register_thread.join();

  EXPECT_TRUE(listen_status.ok()) << listen_status.message();
  EXPECT_TRUE(register_status.ok() || register_status.code() == xrpc::StatusCode::FailedPrecondition)
      << register_status.message();
}

TEST(RpcServerLifecycleTest, WildcardListenRequiresServiceAddressWhenRegistrationEnabled) {
  xrpc::RpcServerOptions options;
  options.service_name_ = "EchoService";
  xrpc::StatusOr<xrpc::RpcServer> server_result = xrpc::RpcServer::Create(options);
  ASSERT_TRUE(server_result.ok()) << server_result.status().message();
  xrpc::RpcServer server = std::move(server_result).value();
  EXPECT_EQ(server.Listen("0.0.0.0", 0).code(), xrpc::StatusCode::InvalidArgument);
}

TEST(RpcServerLifecycleTest, RejectsZeroListenBacklogAtConstruction) {
  xrpc::RpcServerOptions options;
  options.listen_backlog_ = 0;

  const xrpc::StatusOr<xrpc::RpcServer> result = xrpc::RpcServer::Create(options);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), xrpc::StatusCode::InvalidArgument);
}

TEST(RpcServerLifecycleTest, RejectsListenBacklogOutsideSocketApiRange) {
  xrpc::RpcServerOptions options;
  options.listen_backlog_ = static_cast<std::size_t>(std::numeric_limits<int>::max()) + 1U;

  const xrpc::StatusOr<xrpc::RpcServer> result = xrpc::RpcServer::Create(options);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), xrpc::StatusCode::InvalidArgument);
}

TEST(RpcServerLifecycleTest, RejectsZeroConnectionIoThreadsAtConstruction) {
  xrpc::RpcServerOptions options;
  options.connection_io_threads_ = 0;

  const xrpc::StatusOr<xrpc::RpcServer> result = xrpc::RpcServer::Create(options);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), xrpc::StatusCode::InvalidArgument);
}

TEST(RpcServerLifecycleTest, RejectsServiceAddressWithoutServiceNameAtConstruction) {
  xrpc::RpcServerOptions options;
  options.service_address_ = "127.0.0.1";

  const xrpc::StatusOr<xrpc::RpcServer> result = xrpc::RpcServer::Create(options);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), xrpc::StatusCode::InvalidArgument);
}

TEST(RpcServerLifecycleTest, RejectsZeroBackpressureLimitsAtConstruction) {
  xrpc::RpcServerOptions options;
  options.max_pending_jobs_global_ = 0;

  const xrpc::StatusOr<xrpc::RpcServer> result = xrpc::RpcServer::Create(options);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), xrpc::StatusCode::InvalidArgument);
}

TEST(RpcServerLifecycleTest, RejectsZeroMaxPayloadSizeAtConstruction) {
  xrpc::RpcServerOptions options;
  options.max_payload_size_ = 0;

  const xrpc::StatusOr<xrpc::RpcServer> result = xrpc::RpcServer::Create(options);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), xrpc::StatusCode::InvalidArgument);
}

TEST(RpcServerLifecycleTest, RejectsNegativeConnectionIdleTimeoutAtConstruction) {
  xrpc::RpcServerOptions options;
  options.connection_idle_timeout_ = std::chrono::milliseconds(-1);

  const xrpc::StatusOr<xrpc::RpcServer> result = xrpc::RpcServer::Create(options);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), xrpc::StatusCode::InvalidArgument);
}

TEST(RpcServerLifecycleTest, PublicStatsExposePerConnectionInflightBackpressure) {
  xrpc::RpcServerOptions options;
  options.worker_threads_ = 1;
  options.max_inflight_per_connection_ = 1;

  xrpc::StatusOr<xrpc::RpcServer> server_result = xrpc::RpcServer::Create(options);
  ASSERT_TRUE(server_result.ok()) << server_result.status().message();
  xrpc::RpcServer server = std::move(server_result).value();

  std::atomic<bool> first_handler_started = false;
  std::promise<void> handler_started;
  std::future<void> handler_started_future = handler_started.get_future();
  std::promise<void> release_handler;
  std::shared_future<void> release_handler_future = release_handler.get_future().share();

  const xrpc::Status registration_status = server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "Echo", [&](const xrpc::test::EchoRequest &request) {
        if (!first_handler_started.exchange(true, std::memory_order_acq_rel)) {
          handler_started.set_value();
        }
        release_handler_future.wait();
        return Echo(request);
      });
  ASSERT_TRUE(registration_status.ok()) << registration_status.message();

  ASSERT_TRUE(server.Listen("127.0.0.1", 0).ok());
  const xrpc::StatusOr<std::uint16_t> port_result = server.port();
  ASSERT_TRUE(port_result.ok()) << port_result.status().message();
  std::jthread run_thread([&server]() { EXPECT_TRUE(server.Run().ok()); });

  xrpc::io::Socket client_socket;
  client_socket.Connect("127.0.0.1", port_result.value(), WaitTimeout);
  client_socket.WriteAll(MakeRequestFrame("first", 1));

  const bool handler_started_before_timeout = handler_started_future.wait_for(WaitTimeout) == std::future_status::ready;
  EXPECT_TRUE(handler_started_before_timeout);
  if (handler_started_before_timeout) {
    client_socket.WriteAll(MakeRequestFrame("second", 2));
    const std::string rejection_frame = RecvFrame(client_socket);
    xrpc::FrameCodec codec;
    const xrpc::DecodeResult decoded = codec.TryDecode(rejection_frame);
    ASSERT_EQ(decoded.error_, xrpc::ProtocolError::Ok);
    ASSERT_TRUE(decoded.response_.has_value());
    EXPECT_EQ(decoded.response_->request_id_, 2U);
    EXPECT_EQ(decoded.response_->error_code_, static_cast<std::int32_t>(xrpc::StatusCode::ResourceExhausted));
    const xrpc::RpcServerStats stats = server.stats();
    EXPECT_EQ(stats.rejected_by_inflight_limit_, 1U);
    EXPECT_EQ(stats.rejected_by_global_pending_limit_, 0U);
  }

  release_handler.set_value();
  client_socket.Close();
  server.Stop();
  run_thread.join();

  const xrpc::RpcServerStats stats = server.stats();
  EXPECT_EQ(stats.rejected_by_inflight_limit_, 1U);
  EXPECT_EQ(stats.rejected_by_global_pending_limit_, 0U);
  EXPECT_EQ(stats.closed_by_write_queue_high_watermark_, 0U);
  EXPECT_GE(stats.max_observed_inflight_, 1U);
}

TEST(RpcServerLifecycleTest, RpcClientReceivesResourceExhaustedWhenInflightLimitIsReached) {
  xrpc::RpcServerOptions options;
  options.worker_threads_ = 1;
  options.max_inflight_per_connection_ = 1;

  xrpc::StatusOr<xrpc::RpcServer> server_result = xrpc::RpcServer::Create(options);
  ASSERT_TRUE(server_result.ok()) << server_result.status().message();
  xrpc::RpcServer server = std::move(server_result).value();

  std::atomic<bool> first_handler_started = false;
  std::promise<void> handler_started;
  std::future<void> handler_started_future = handler_started.get_future();
  std::promise<void> release_handler;
  std::shared_future<void> release_handler_future = release_handler.get_future().share();

  const xrpc::Status registration_status = server.RegisterMethod<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "Echo", [&](const xrpc::test::EchoRequest &request) {
        if (!first_handler_started.exchange(true, std::memory_order_acq_rel)) {
          handler_started.set_value();
        }
        release_handler_future.wait();
        return Echo(request);
      });
  ASSERT_TRUE(registration_status.ok()) << registration_status.message();

  ASSERT_TRUE(server.Listen("127.0.0.1", 0).ok());
  const xrpc::StatusOr<std::uint16_t> port_result = server.port();
  ASSERT_TRUE(port_result.ok()) << port_result.status().message();
  std::jthread run_thread([&server]() { EXPECT_TRUE(server.Run().ok()); });

  xrpc::RpcClientOptions client_options;
  client_options.target_ = "list://127.0.0.1:" + std::to_string(port_result.value());
  client_options.timeout_ = WaitTimeout;
  xrpc::StatusOr<xrpc::RpcClient> client_result = xrpc::RpcClient::Create(client_options);
  ASSERT_TRUE(client_result.ok()) << client_result.status().message();
  xrpc::RpcClient client = std::move(client_result).value();
  const xrpc::Status init_status = client.Init();
  ASSERT_TRUE(init_status.ok()) << init_status.message();

  std::optional<xrpc::StatusOr<xrpc::test::EchoResponse>> first_response;
  std::jthread first_call_thread([&]() {
    xrpc::test::EchoRequest request;
    request.set_message("first");
    first_response.emplace(client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", request));
  });

  ASSERT_EQ(handler_started_future.wait_for(WaitTimeout), std::future_status::ready);

  xrpc::test::EchoRequest second_request;
  second_request.set_message("second");
  const xrpc::StatusOr<xrpc::test::EchoResponse> second_response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "Echo", second_request);

  EXPECT_FALSE(second_response.ok());
  EXPECT_EQ(second_response.status().code(), xrpc::StatusCode::ResourceExhausted);

  release_handler.set_value();
  first_call_thread.join();

  ASSERT_TRUE(first_response.has_value());
  ASSERT_TRUE(first_response->ok()) << first_response->status().message();
  EXPECT_EQ(first_response->value().message(), "first");

  server.Stop();
  run_thread.join();
}
