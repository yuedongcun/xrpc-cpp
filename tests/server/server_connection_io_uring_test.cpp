#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <limits>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "io/socket.h"
#include "proto/echo.pb.h"
#include "protocol/frame_codec.h"
#include "protocol/rpc_envelope.h"
#include "server/connection_io_loop.h"
#include "server/service_registry.h"
#include "server/worker_pool.h"

namespace {

constexpr auto WaitTimeout = std::chrono::milliseconds(1000);

auto Echo(const xrpc::test::EchoRequest &request) -> xrpc::test::EchoResponse {
  xrpc::test::EchoResponse response;
  response.set_message("echo: " + request.message());
  return response;
}

auto MakeEchoResponseEnvelope(const xrpc::RequestEnvelope &request) -> xrpc::ResponseEnvelope {
  xrpc::ResponseEnvelope response;
  response.request_id_ = request.request_id_;

  xrpc::test::EchoRequest parsed_request;
  if (!parsed_request.ParseFromString(request.payload_)) {
    response.status_ = {xrpc::StatusCode::InvalidArgument, "failed to parse protobuf request"};
    return response;
  }

  response.payload_ = Echo(parsed_request).SerializeAsString();
  return response;
}

auto MakeEchoHandler() -> xrpc::RequestHandler {
  return [](const xrpc::RequestEnvelope &request) { return MakeEchoResponseEnvelope(request); };
}

auto MakeSlowEchoHandler() -> xrpc::RequestHandler {
  return [](const xrpc::RequestEnvelope &request) {
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    return MakeEchoResponseEnvelope(request);
  };
}

auto MakeRegistry(xrpc::RequestHandler handler) -> xrpc::ServiceRegistry {
  xrpc::ServiceRegistry registry;
  EXPECT_TRUE(registry.Register("EchoService", "Echo", handler).ok());
  EXPECT_TRUE(registry.Register("EchoService", "SlowEcho", std::move(handler)).ok());
  return registry;
}

auto MakeConnectionConfig(xrpc::ConnectionBackpressureLimits limits = {.max_inflight_ = 128,
                                                                       .max_write_queue_bytes_ = 8U * 1024U * 1024U})
    -> xrpc::ServerConnectionConfig {
  return {.limits_ = limits, .protocol_limits_ = {}};
}

auto MakeWorkerConfig(std::size_t threads) -> xrpc::WorkerPoolConfig {
  return {.threads_ = threads, .max_pending_jobs_ = std::numeric_limits<std::size_t>::max()};
}

auto MakeRequestFrame(std::string message, std::uint64_t request_id) -> std::string {
  xrpc::test::EchoRequest request;
  request.set_message(std::move(message));

  xrpc::RequestEnvelope request_envelope;
  request_envelope.request_id_ = request_id;
  request_envelope.service_name_ = "EchoService";
  request_envelope.method_name_ = "Echo";
  request_envelope.payload_ = request.SerializeAsString();

  xrpc::FrameCodec codec;
  return codec.Encode(request_envelope).value();
}

auto RecvFrame(xrpc::io::Socket &socket, std::string &buffer) -> std::string {
  char chunk[4096];
  xrpc::FrameCodec codec;

  while (true) {
    xrpc::FrameDecodeResult decoded = codec.Decode(buffer);
    if (decoded.error_ == xrpc::ProtocolError::Ok && decoded.HasEnvelope() && decoded.consumed_ <= buffer.size()) {
      std::string frame = buffer.substr(0, decoded.consumed_);
      buffer.erase(0, decoded.consumed_);
      return frame;
    }

    const ssize_t received = socket.Read(chunk, sizeof(chunk)).value();
    if (received <= 0) {
      break;
    }
    buffer.append(chunk, static_cast<std::size_t>(received));
  }

  return buffer;
}

auto DecodeEchoMessage(std::string_view frame, std::uint64_t expected_request_id) -> std::string {
  xrpc::FrameCodec codec;
  const xrpc::FrameDecodeResult decoded = codec.Decode(frame);
  EXPECT_EQ(decoded.error_, xrpc::ProtocolError::Ok);
  EXPECT_TRUE(decoded.response_.has_value());

  const auto &protocol_response = *decoded.response_;
  EXPECT_EQ(protocol_response.request_id_, expected_request_id);
  EXPECT_TRUE(protocol_response.status_.ok());
  xrpc::test::EchoResponse response;
  EXPECT_TRUE(response.ParseFromString(protocol_response.payload_));
  return response.message();
}

auto DecodeResponseStatus(std::string_view frame, std::uint64_t expected_request_id) -> xrpc::Status {
  xrpc::FrameCodec codec;
  const xrpc::FrameDecodeResult decoded = codec.Decode(frame);
  EXPECT_EQ(decoded.error_, xrpc::ProtocolError::Ok);
  EXPECT_TRUE(decoded.response_.has_value());

  const auto &protocol_response = *decoded.response_;
  EXPECT_EQ(protocol_response.request_id_, expected_request_id);
  return protocol_response.status_;
}

struct ConnectedPair {
  xrpc::io::Socket client_socket_;
  xrpc::io::Socket server_socket_;
};

auto MakeConnectedPair() -> ConnectedPair {
  xrpc::io::Socket listen_socket;
  EXPECT_TRUE(listen_socket.Bind("127.0.0.1", 0).ok());
  EXPECT_TRUE(listen_socket.Listen(1).ok());

  xrpc::io::Socket client_socket;
  EXPECT_TRUE(client_socket.Connect("127.0.0.1", listen_socket.LocalPort().value()).ok());
  EXPECT_TRUE(client_socket.SetReadWriteTimeout(WaitTimeout).ok());

  return ConnectedPair{.client_socket_ = std::move(client_socket), .server_socket_ = listen_socket.Accept().value()};
}

}  // namespace

TEST(ServerConnectionTest, EchoesSingleFrameAndClosesAfterPeerShutdown) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  const auto config = MakeConnectionConfig();
  xrpc::ConnectionIoLoop loop(registry, worker_pool, config);
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  std::string received_buffer;
  EXPECT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame("hello", 7)).ok());
  pair.client_socket_.ShutdownWrite();

  const std::string response = RecvFrame(pair.client_socket_, received_buffer);
  pair.client_socket_.Close();

  EXPECT_TRUE(loop.FinishDrain().ok());
  EXPECT_EQ(DecodeEchoMessage(response, 7), "echo: hello");
}

TEST(ServerConnectionTest, ServerDrainClosesConnection) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  const auto config = MakeConnectionConfig();
  xrpc::ConnectionIoLoop loop(registry, worker_pool, config);
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  std::string received_buffer;
  EXPECT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame("before-drain", 8)).ok());
  const std::string response = RecvFrame(pair.client_socket_, received_buffer);
  EXPECT_EQ(DecodeEchoMessage(response, 8), "echo: before-drain");

  loop.BeginDrain();

  EXPECT_TRUE(loop.FinishDrain().ok());
  char byte = 0;
  EXPECT_EQ(pair.client_socket_.Read(&byte, sizeof(byte)).value(), 0);
  pair.client_socket_.Close();
}

TEST(ServerConnectionTest, HandlesHalfPacketsAndStickyPackets) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ServiceRegistry registry = MakeRegistry(MakeSlowEchoHandler());
  const auto config = MakeConnectionConfig();
  xrpc::ConnectionIoLoop loop(registry, worker_pool, config);
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  const std::string first_request = MakeRequestFrame("first", 11);
  const std::string second_request = MakeRequestFrame("second", 12);
  const std::size_t split = first_request.size() / 2;
  EXPECT_TRUE(pair.client_socket_.WriteAll(std::string_view(first_request.data(), split)).ok());

  std::string remainder_and_second(first_request.data() + split, first_request.size() - split);
  remainder_and_second.append(second_request);
  EXPECT_TRUE(pair.client_socket_.WriteAll(remainder_and_second).ok());
  pair.client_socket_.ShutdownWrite();

  std::string received_buffer;
  const std::string first_response = RecvFrame(pair.client_socket_, received_buffer);
  const std::string second_response = RecvFrame(pair.client_socket_, received_buffer);
  pair.client_socket_.Close();

  EXPECT_TRUE(loop.FinishDrain().ok());
  EXPECT_EQ(DecodeEchoMessage(first_response, 11), "echo: first");
  EXPECT_EQ(DecodeEchoMessage(second_response, 12), "echo: second");
}

TEST(ServerConnectionTest, HandlesPipelinedRequestsOnOneConnection) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  const auto config = MakeConnectionConfig();
  xrpc::ConnectionIoLoop loop(registry, worker_pool, config);
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  const std::string first_request = MakeRequestFrame("first", 21);
  const std::string second_request = MakeRequestFrame("second", 22);
  EXPECT_TRUE(pair.client_socket_.WriteAll(first_request + second_request).ok());

  std::string received_buffer;
  const std::string first_response = RecvFrame(pair.client_socket_, received_buffer);
  const std::string second_response = RecvFrame(pair.client_socket_, received_buffer);

  pair.client_socket_.ShutdownWrite();
  pair.client_socket_.Close();

  EXPECT_TRUE(loop.FinishDrain().ok());
  EXPECT_EQ(DecodeEchoMessage(first_response, 21), "echo: first");
  EXPECT_EQ(DecodeEchoMessage(second_response, 22), "echo: second");
}

TEST(ServerConnectionTest, WakesIdleWriteLoopForLaterResponse) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  const auto config = MakeConnectionConfig();
  xrpc::ConnectionIoLoop loop(registry, worker_pool, config);
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  std::string received_buffer;
  EXPECT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame("first", 23)).ok());
  const std::string first_response = RecvFrame(pair.client_socket_, received_buffer);

  EXPECT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame("second", 24)).ok());
  const std::string second_response = RecvFrame(pair.client_socket_, received_buffer);

  pair.client_socket_.ShutdownWrite();
  pair.client_socket_.Close();

  EXPECT_TRUE(loop.FinishDrain().ok());
  EXPECT_EQ(DecodeEchoMessage(first_response, 23), "echo: first");
  EXPECT_EQ(DecodeEchoMessage(second_response, 24), "echo: second");
}

TEST(ServerConnectionTest, ClosesOnInvalidFrame) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  const auto config = MakeConnectionConfig();
  xrpc::ConnectionIoLoop loop(registry, worker_pool, config);
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  std::string invalid_request = MakeRequestFrame("hello", 42);
  invalid_request[0] = '\0';
  EXPECT_TRUE(pair.client_socket_.WriteAll(invalid_request).ok());
  pair.client_socket_.ShutdownWrite();
  pair.client_socket_.Close();

  EXPECT_TRUE(loop.FinishDrain().ok());
}

TEST(ServerConnectionTest, HandlesConcurrentResponsesWithWorkerPool) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(2));
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  const auto config = MakeConnectionConfig();
  xrpc::ConnectionIoLoop loop(registry, worker_pool, config);
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  xrpc::test::EchoRequest slow_request;
  slow_request.set_message("slow");
  xrpc::RequestEnvelope slow_request_envelope;
  slow_request_envelope.request_id_ = 31;
  slow_request_envelope.service_name_ = "EchoService";
  slow_request_envelope.method_name_ = "SlowEcho";
  slow_request_envelope.payload_ = slow_request.SerializeAsString();

  xrpc::test::EchoRequest fast_request;
  fast_request.set_message("fast");
  xrpc::RequestEnvelope fast_request_envelope;
  fast_request_envelope.request_id_ = 32;
  fast_request_envelope.service_name_ = "EchoService";
  fast_request_envelope.method_name_ = "Echo";
  fast_request_envelope.payload_ = fast_request.SerializeAsString();

  xrpc::FrameCodec codec;
  EXPECT_TRUE(pair.client_socket_
                  .WriteAll(codec.Encode(slow_request_envelope).value() +
                            codec.Encode(fast_request_envelope).value())
                  .ok());

  std::string received_buffer;
  const std::string first_response = RecvFrame(pair.client_socket_, received_buffer);
  const std::string second_response = RecvFrame(pair.client_socket_, received_buffer);
  pair.client_socket_.ShutdownWrite();
  pair.client_socket_.Close();

  EXPECT_TRUE(loop.FinishDrain().ok());

  const xrpc::FrameDecodeResult first_decoded = codec.Decode(first_response);
  const xrpc::FrameDecodeResult second_decoded = codec.Decode(second_response);
  ASSERT_EQ(first_decoded.error_, xrpc::ProtocolError::Ok);
  ASSERT_EQ(second_decoded.error_, xrpc::ProtocolError::Ok);
  ASSERT_TRUE(first_decoded.response_.has_value());
  ASSERT_TRUE(second_decoded.response_.has_value());

  const auto &first_protocol_response = *first_decoded.response_;
  const auto &second_protocol_response = *second_decoded.response_;
  EXPECT_NE(first_protocol_response.request_id_, second_protocol_response.request_id_);
  EXPECT_TRUE((first_protocol_response.request_id_ == 31U && second_protocol_response.request_id_ == 32U) ||
              (first_protocol_response.request_id_ == 32U && second_protocol_response.request_id_ == 31U));
}

TEST(ServerConnectionTest, KeepsReadingWhileWorkerHandlerIsPending) {
  std::promise<void> handler_started;
  std::future<void> handler_started_future = handler_started.get_future();
  std::promise<void> release_handler;
  std::shared_future<void> release_handler_future = release_handler.get_future().share();

  xrpc::RequestHandler blocking_handler = [&](const xrpc::RequestEnvelope &request) {
    handler_started.set_value();
    release_handler_future.wait();

    xrpc::ResponseEnvelope response;
    response.request_id_ = request.request_id_;
    response.status_ = xrpc::Status::Ok();
    response.payload_ = request.payload_;
    return response;
  };

  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ServiceRegistry registry = MakeRegistry(std::move(blocking_handler));
  const auto config =
      MakeConnectionConfig(xrpc::ConnectionBackpressureLimits{.max_inflight_ = 1, .max_write_queue_bytes_ = 1024});
  xrpc::ConnectionIoLoop loop(registry, worker_pool, config);
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  EXPECT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame("first", 81)).ok());
  const std::future_status handler_started_status = handler_started_future.wait_for(WaitTimeout);

  if (handler_started_status == std::future_status::ready) {
    EXPECT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame("second", 82)).ok());
    std::string received_buffer;
    const std::string rejection_response = RecvFrame(pair.client_socket_, received_buffer);
    const xrpc::Status rejection_status = DecodeResponseStatus(rejection_response, 82);
    EXPECT_EQ(rejection_status.code(), xrpc::StatusCode::ResourceExhausted);
  }
  release_handler.set_value();

  pair.client_socket_.ShutdownWrite();
  pair.client_socket_.Close();
  EXPECT_TRUE(loop.FinishDrain().ok());

  EXPECT_EQ(handler_started_status, std::future_status::ready);
}

TEST(ServerConnectionTest, RejectsEntireReadBatchWhenInflightLimitWouldBeExceeded) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  std::atomic<std::size_t> handler_calls = 0;
  xrpc::ServiceRegistry registry = MakeRegistry([&handler_calls](const xrpc::RequestEnvelope &request) {
    ++handler_calls;
    return MakeEchoResponseEnvelope(request);
  });
  const auto config =
      MakeConnectionConfig(xrpc::ConnectionBackpressureLimits{.max_inflight_ = 1, .max_write_queue_bytes_ = 1024});
  xrpc::ConnectionIoLoop loop(registry, worker_pool, config);
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  xrpc::test::EchoRequest request;
  request.set_message("slow");
  xrpc::RequestEnvelope request_envelope;
  request_envelope.request_id_ = 51;
  request_envelope.service_name_ = "EchoService";
  request_envelope.method_name_ = "SlowEcho";
  request_envelope.payload_ = request.SerializeAsString();
  xrpc::RequestEnvelope rejected_request_envelope = request_envelope;
  rejected_request_envelope.request_id_ = 52;
  xrpc::FrameCodec codec;
  EXPECT_TRUE(pair.client_socket_
                  .WriteAll(codec.Encode(request_envelope).value() +
                            codec.Encode(rejected_request_envelope).value())
                  .ok());

  std::string received_buffer;
  const std::string first_rejection = RecvFrame(pair.client_socket_, received_buffer);
  const std::string second_rejection = RecvFrame(pair.client_socket_, received_buffer);
  EXPECT_EQ(DecodeResponseStatus(first_rejection, 51).code(), xrpc::StatusCode::ResourceExhausted);
  EXPECT_EQ(DecodeResponseStatus(second_rejection, 52).code(), xrpc::StatusCode::ResourceExhausted);

  pair.client_socket_.ShutdownWrite();
  pair.client_socket_.Close();
  EXPECT_TRUE(loop.FinishDrain().ok());

  EXPECT_EQ(handler_calls.load(), 0U);
}

TEST(ServerConnectionTest, ClosesWhenWriteQueueByteLimitIsReached) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  const auto config =
      MakeConnectionConfig(xrpc::ConnectionBackpressureLimits{.max_inflight_ = 8, .max_write_queue_bytes_ = 1});
  xrpc::ConnectionIoLoop loop(registry, worker_pool, config);
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  EXPECT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame("response-is-larger-than-one-byte", 71)).ok());

  char byte = 0;
  EXPECT_EQ(pair.client_socket_.Read(&byte, sizeof(byte)).value(), 0);
  pair.client_socket_.Close();
  EXPECT_TRUE(loop.FinishDrain().ok());
}
