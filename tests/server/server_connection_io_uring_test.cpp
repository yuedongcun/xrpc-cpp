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

#include "common/xrpc_exception.h"
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

void ExpectPeerClosed(xrpc::io::Socket &socket) {
  char byte = 0;
  auto received = socket.Read(&byte, sizeof(byte));
  if (received.ok()) {
    EXPECT_EQ(received.value(), 0);
  } else {
    // Closing with unread TCP data may reset the peer; a read timeout is a failure.
    EXPECT_EQ(received.status().code(), xrpc::StatusCode::Unavailable);
  }
}

}  // namespace

TEST(ServerConnectionTest, BufferExhaustionRetriesWithoutClosingTheConnection) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  xrpc::ConnectionIoLoop loop(registry, worker_pool, MakeConnectionConfig(), {.buffer_count_ = 1, .buffer_size_ = 256});
  const std::string payload(32U * 1024U, 'x');
  // Queue more data than the entire pool before the receive starts.
  ASSERT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame(payload, 1)).ok());
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));
  std::string received_buffer;
  EXPECT_EQ(DecodeEchoMessage(RecvFrame(pair.client_socket_, received_buffer), 1), "echo: " + payload);
  const auto recovered = loop.RequestStats().get();
  EXPECT_GT(recovered.uring_.counters_.provided_buffer_enobufs_, 0U);
  EXPECT_GT(recovered.uring_.counters_.prepared_multishot_recv_sqes_, 1U);
  EXPECT_EQ(recovered.live_connections_, 1U);
  EXPECT_EQ(recovered.uring_.buffer_pool_->outstanding_leases_, 0U);
  EXPECT_EQ(recovered.uring_.buffer_pool_->acquires_, recovered.uring_.buffer_pool_->returns_);
  // The same socket must remain usable after recovery.
  ASSERT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame("next", 2)).ok());
  EXPECT_EQ(DecodeEchoMessage(RecvFrame(pair.client_socket_, received_buffer), 2), "echo: next");
  EXPECT_TRUE(loop.FinishDrain().ok());
  ExpectPeerClosed(pair.client_socket_);
}

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

  auto snapshot_future = loop.RequestStats();
  ASSERT_EQ(snapshot_future.wait_for(WaitTimeout), std::future_status::ready);
  const auto snapshot = snapshot_future.get();
  EXPECT_GE(snapshot.uring_.counters_.prepared_multishot_recv_sqes_, 1U);
  EXPECT_GE(snapshot.uring_.counters_.recv_cqes_, 1U);
  EXPECT_GT(snapshot.uring_.counters_.received_bytes_, 0U);
  EXPECT_EQ(snapshot.uring_.active_recv_requests_, 1U);
  EXPECT_EQ(snapshot.live_connections_, 1U);
  EXPECT_GT(snapshot.pending_write_bytes_peak_, 0U);

  auto reset_future = loop.RequestStats(true);
  ASSERT_EQ(reset_future.wait_for(WaitTimeout), std::future_status::ready);
  const auto reset = reset_future.get();
  EXPECT_EQ(reset.pending_write_bytes_peak_, reset.pending_write_bytes_);
  EXPECT_EQ(reset.uring_.window_id_, snapshot.uring_.window_id_ + 1);

  loop.BeginDrain();

  EXPECT_TRUE(loop.FinishDrain().ok());
  EXPECT_THROW((void)loop.RequestStats(), xrpc::LifecycleException);
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
  // Keep the peer open: protocol rejection must cancel and drain the receive.
  char byte = 0;
  EXPECT_EQ(pair.client_socket_.Read(&byte, sizeof(byte)).value(), 0);
  pair.client_socket_.Close();

  EXPECT_TRUE(loop.FinishDrain().ok());
}

TEST(ServerConnectionTest, ReusesProvidedBuffersAcrossLargeRequests) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  xrpc::ConnectionIoLoop loop(registry, worker_pool, MakeConnectionConfig());
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  std::string received_buffer;
  // Each request spans multiple 16 KiB buffers; the total exceeds the 512-buffer pool.
  for (std::uint64_t request_id = 1; request_id <= 40; ++request_id) {
    const std::string message(256U * 1024U, static_cast<char>('a' + request_id % 26));
    ASSERT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame(message, request_id)).ok());
    const std::string response = RecvFrame(pair.client_socket_, received_buffer);
    ASSERT_EQ(DecodeEchoMessage(response, request_id), "echo: " + message);
  }
  EXPECT_TRUE(loop.FinishDrain().ok());
}

TEST(ServerConnectionTest, DrainsQueuedReceiveDataAfterProtocolClose) {
  ConnectedPair pair = MakeConnectedPair();
  std::atomic<std::size_t> handler_calls = 0;
  xrpc::ServiceRegistry registry = MakeRegistry([&](const xrpc::RequestEnvelope &request) {
    ++handler_calls;
    return MakeEchoResponseEnvelope(request);
  });
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ConnectionIoLoop loop(registry, worker_pool, MakeConnectionConfig());

  std::string burst = MakeRequestFrame("invalid", 101);
  burst[0] = '\0';
  burst += MakeRequestFrame(std::string(128U * 1024U, 'x'), 102);
  // Queue more than one provided buffer before the server starts receiving.
  ASSERT_TRUE(pair.client_socket_.WriteAll(burst).ok());
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  ExpectPeerClosed(pair.client_socket_);
  EXPECT_TRUE(loop.FinishDrain().ok());
  EXPECT_EQ(handler_calls.load(), 0U);
  // Loop destruction also checks that no provided-buffer leases remain outstanding.
}

TEST(ServerConnectionTest, ClosingOneConnectionPreservesSharedPoolAndOtherReceiver) {
  ConnectedPair closing = MakeConnectedPair();
  ConnectedPair surviving = MakeConnectedPair();
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ConnectionIoLoop loop(registry, worker_pool, MakeConnectionConfig());
  loop.Start();
  loop.PostStartConnection(std::move(closing.server_socket_));
  loop.PostStartConnection(std::move(surviving.server_socket_));

  std::string closing_buffer;
  std::string surviving_buffer;
  ASSERT_TRUE(closing.client_socket_.WriteAll(MakeRequestFrame("closing", 111)).ok());
  ASSERT_TRUE(surviving.client_socket_.WriteAll(MakeRequestFrame("surviving", 112)).ok());
  EXPECT_EQ(DecodeEchoMessage(RecvFrame(closing.client_socket_, closing_buffer), 111), "echo: closing");
  EXPECT_EQ(DecodeEchoMessage(RecvFrame(surviving.client_socket_, surviving_buffer), 112), "echo: surviving");

  std::string invalid = MakeRequestFrame("invalid", 113);
  invalid[0] = '\0';
  ASSERT_TRUE(closing.client_socket_.WriteAll(invalid).ok());
  // Keep the other connection receiving while the first is being cancelled.
  for (std::uint64_t request_id = 120; request_id < 160; ++request_id) {
    const std::string message(256U * 1024U, static_cast<char>('a' + request_id % 26));
    ASSERT_TRUE(surviving.client_socket_.WriteAll(MakeRequestFrame(message, request_id)).ok());
    ASSERT_EQ(DecodeEchoMessage(RecvFrame(surviving.client_socket_, surviving_buffer), request_id), "echo: " + message);
  }
  ExpectPeerClosed(closing.client_socket_);
  EXPECT_TRUE(loop.FinishDrain().ok());
  ExpectPeerClosed(surviving.client_socket_);
}

TEST(ServerConnectionTest, RepeatedConnectionsDrainBeforeCollection) {
  xrpc::ServiceRegistry registry = MakeRegistry(MakeEchoHandler());
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ConnectionIoLoop loop(registry, worker_pool, MakeConnectionConfig());
  loop.Start();

  for (std::uint64_t request_id = 1; request_id <= 64; ++request_id) {
    SCOPED_TRACE(request_id);
    ConnectedPair pair = MakeConnectedPair();
    loop.PostStartConnection(std::move(pair.server_socket_));
    const std::string message = "connection-" + std::to_string(request_id);
    ASSERT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame(message, request_id)).ok());
    std::string received_buffer;
    ASSERT_EQ(DecodeEchoMessage(RecvFrame(pair.client_socket_, received_buffer), request_id), "echo: " + message);

    if (request_id % 2 == 0) {
      pair.client_socket_.ShutdownWrite();
    } else {
      std::string invalid = MakeRequestFrame("invalid", request_id);
      invalid[0] = '\0';
      ASSERT_TRUE(pair.client_socket_.WriteAll(invalid).ok());
    }
    ExpectPeerClosed(pair.client_socket_);
    // The next admission calls CollectClosedConnections() on the same loop.
  }
  EXPECT_TRUE(loop.FinishDrain().ok());
}

TEST(ServerConnectionTest, DrainPreservesPendingWorkerResponse) {
  std::promise<void> handler_started;
  auto started = handler_started.get_future();
  std::promise<void> release_handler;
  auto release = release_handler.get_future().share();
  xrpc::ServiceRegistry registry = MakeRegistry([&](const xrpc::RequestEnvelope &request) {
    handler_started.set_value();
    release.wait();
    return MakeEchoResponseEnvelope(request);
  });
  ConnectedPair pair = MakeConnectedPair();
  xrpc::WorkerPool worker_pool(MakeWorkerConfig(1));
  xrpc::ConnectionIoLoop loop(registry, worker_pool, MakeConnectionConfig());
  loop.Start();
  loop.PostStartConnection(std::move(pair.server_socket_));

  EXPECT_TRUE(pair.client_socket_.WriteAll(MakeRequestFrame("pending", 91)).ok());
  const auto handler_status = started.wait_for(WaitTimeout);
  loop.BeginDrain();
  release_handler.set_value();
  EXPECT_EQ(handler_status, std::future_status::ready);
  std::string received_buffer;
  const std::string response = RecvFrame(pair.client_socket_, received_buffer);
  EXPECT_EQ(DecodeEchoMessage(response, 91), "echo: pending");
  EXPECT_TRUE(loop.FinishDrain().ok());
  char byte = 0;
  EXPECT_EQ(pair.client_socket_.Read(&byte, sizeof(byte)).value(), 0);
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
                  .WriteAll(codec.Encode(slow_request_envelope).value() + codec.Encode(fast_request_envelope).value())
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
                  .WriteAll(codec.Encode(request_envelope).value() + codec.Encode(rejected_request_envelope).value())
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
