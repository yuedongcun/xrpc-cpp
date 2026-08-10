#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "io/socket.h"
#include "io/uring_context.h"
#include "proto/echo.pb.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_message.h"
#include "transport/dispatch_completion_queue.h"
#include "transport/tcp_connection.h"
#include "transport/thread_pool_executor.h"

namespace {

constexpr auto PollInterval = std::chrono::milliseconds(1);
constexpr auto WaitTimeout = std::chrono::milliseconds(1000);

auto Echo(const xrpc::test::EchoRequest &request) -> xrpc::test::EchoResponse {
  xrpc::test::EchoResponse response;
  response.set_message("echo: " + request.message());
  return response;
}

auto MakeEchoRawResponse(const xrpc::RawRequest &request) -> xrpc::RawResponse {
  xrpc::RawResponse response;
  response.request_id_ = request.request_id_;

  xrpc::test::EchoRequest parsed_request;
  if (!parsed_request.ParseFromString(request.payload_)) {
    response.status_ = {xrpc::StatusCode::InvalidArgument, "failed to parse protobuf request"};
    return response;
  }

  response.payload_ = Echo(parsed_request).SerializeAsString();
  return response;
}

auto MakeEchoHandler() -> xrpc::RawHandler {
  return [](const xrpc::RawRequest &request) { return MakeEchoRawResponse(request); };
}

auto MakeSlowEchoHandler() -> xrpc::RawHandler {
  return [](const xrpc::RawRequest &request) {
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    return MakeEchoRawResponse(request);
  };
}

auto MakeConnectionOptions(xrpc::io::UringContext &context, xrpc::ServerBackpressureStats &stats,
                           xrpc::ConnectionBackpressureLimits limits = {}) -> xrpc::TcpConnectionOptions {
  xrpc::TcpConnectionOptions options;
  options.limits_ = limits;
  options.backpressure_stats_ = &stats;
  options.completion_queue_ = std::make_shared<xrpc::DispatchCompletionQueue>(context);
  return options;
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

auto RecvFrame(xrpc::io::Socket &socket, std::string &buffer) -> std::string {
  char chunk[4096];
  xrpc::FrameCodec codec;

  while (true) {
    xrpc::DecodeResult decoded = codec.TryDecode(buffer);
    if (decoded.error_ == xrpc::ProtocolError::Ok && decoded.HasMessage() && decoded.consumed_ <= buffer.size()) {
      std::string frame = buffer.substr(0, decoded.consumed_);
      buffer.erase(0, decoded.consumed_);
      return frame;
    }

    const ssize_t received = socket.Read(chunk, sizeof(chunk));
    if (received <= 0) {
      break;
    }
    buffer.append(chunk, static_cast<std::size_t>(received));
  }

  return buffer;
}

class UringContextRunner final {
 public:
  UringContextRunner() = default;
  ~UringContextRunner() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  UringContextRunner(const UringContextRunner &) = delete;
  auto operator=(const UringContextRunner &) -> UringContextRunner & = delete;

  UringContextRunner(UringContextRunner &&) = delete;
  auto operator=(UringContextRunner &&) -> UringContextRunner & = delete;

  void Start(xrpc::io::UringContext &context) {
    if (thread_.joinable()) {
      throw xrpc::LifecycleException("UringContextRunner already started");
    }

    error_ = nullptr;
    thread_ = std::jthread([&context, this]() {
      try {
        context.Run();
      } catch (...) {
        error_ = std::current_exception();
      }
    });
  }

  void StopAndJoin(xrpc::io::UringContext &context) {
    context.Stop();
    if (thread_.joinable()) {
      thread_.join();
    }
    if (error_) {
      std::rethrow_exception(error_);
    }
  }

 private:
  std::jthread thread_;
  std::exception_ptr error_;
};

template <typename T>
void StartTaskOnContext(xrpc::io::UringContext &context, xrpc::runtime::Task<T> &task) {
  context.Post([&task]() { task.Start(); });
}

auto WaitTaskDone(xrpc::runtime::Task<void> &task, std::chrono::milliseconds timeout,
                  std::chrono::milliseconds poll_interval) -> bool {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!task.Done() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(poll_interval);
  }
  return task.Done();
}

auto DecodeEchoMessage(std::string_view frame, std::uint64_t expected_request_id) -> std::string {
  xrpc::FrameCodec codec;
  const xrpc::DecodeResult decoded = codec.TryDecode(frame);
  EXPECT_EQ(decoded.error_, xrpc::ProtocolError::Ok);
  EXPECT_TRUE(decoded.response_.has_value());

  const auto &protocol_response = *decoded.response_;
  EXPECT_EQ(protocol_response.request_id_, expected_request_id);
  EXPECT_EQ(protocol_response.error_code_, 0);
  xrpc::test::EchoResponse response;
  EXPECT_TRUE(response.ParseFromString(protocol_response.payload_));
  return response.message();
}

auto DecodeResponseStatus(std::string_view frame, std::uint64_t expected_request_id) -> xrpc::Status {
  xrpc::FrameCodec codec;
  const xrpc::DecodeResult decoded = codec.TryDecode(frame);
  EXPECT_EQ(decoded.error_, xrpc::ProtocolError::Ok);
  EXPECT_TRUE(decoded.response_.has_value());

  const auto &protocol_response = *decoded.response_;
  EXPECT_EQ(protocol_response.request_id_, expected_request_id);
  return {static_cast<xrpc::StatusCode>(protocol_response.error_code_), protocol_response.error_text_};
}

struct ConnectedPair {
  xrpc::io::Socket client_socket_;
  xrpc::io::Socket server_socket_;
};

auto MakeConnectedPair() -> ConnectedPair {
  xrpc::io::Socket listen_socket;
  listen_socket.Bind("127.0.0.1", 0);
  listen_socket.Listen(1);

  xrpc::io::Socket client_socket;
  client_socket.Connect("127.0.0.1", listen_socket.LocalPort());

  return ConnectedPair{.client_socket_ = std::move(client_socket), .server_socket_ = listen_socket.Accept()};
}

auto WaitForConnectionTask(xrpc::runtime::Task<void> &task, xrpc::io::UringContext &context, UringContextRunner &runner)
    -> bool {
  const bool done = WaitTaskDone(task, WaitTimeout, PollInterval);
  runner.StopAndJoin(context);
  return done;
}

}  // namespace

TEST(CoroutineTcpConnectionTest, EchoesSingleFrameAndClosesAfterPeerShutdown) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::io::UringContext context;
  xrpc::ServerBackpressureStats stats;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpConnectionOptions options = MakeConnectionOptions(context, stats);
  auto connection = std::make_shared<xrpc::TcpConnection>(context, MakeEchoHandler(), executor,
                                                          std::move(pair.server_socket_), std::move(options));
  xrpc::runtime::Task<void> task = connection->Run();
  UringContextRunner runner;
  StartTaskOnContext(context, task);
  runner.Start(context);

  std::string received_buffer;
  pair.client_socket_.WriteAll(MakeRequestFrame("hello", 7));
  pair.client_socket_.ShutdownWrite();

  const std::string response = RecvFrame(pair.client_socket_, received_buffer);
  pair.client_socket_.Close();

  ASSERT_TRUE(WaitForConnectionTask(task, context, runner));
  EXPECT_TRUE(connection->IsClosed());
  EXPECT_EQ(connection->close_reason(), xrpc::ConnectionCloseReason::PeerClosed);
  EXPECT_EQ(DecodeEchoMessage(response, 7), "echo: hello");
}

TEST(CoroutineTcpConnectionTest, ClosesIdleConnectionAfterTimeout) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::io::UringContext context;
  xrpc::ServerBackpressureStats stats;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpConnectionOptions options = MakeConnectionOptions(context, stats);
  options.idle_timeout_ = std::chrono::milliseconds(20);
  auto connection = std::make_shared<xrpc::TcpConnection>(context, MakeEchoHandler(), executor,
                                                          std::move(pair.server_socket_), std::move(options));
  xrpc::runtime::Task<void> task = connection->Run();
  UringContextRunner runner;
  StartTaskOnContext(context, task);
  runner.Start(context);

  ASSERT_TRUE(WaitForConnectionTask(task, context, runner));
  pair.client_socket_.Close();

  EXPECT_TRUE(connection->IsClosed());
  EXPECT_EQ(connection->close_reason(), xrpc::ConnectionCloseReason::IdleTimeout);
}

TEST(CoroutineTcpConnectionTest, HandlesHalfPacketsAndStickyPackets) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::io::UringContext context;
  xrpc::ServerBackpressureStats stats;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpConnectionOptions options = MakeConnectionOptions(context, stats);
  auto connection = std::make_shared<xrpc::TcpConnection>(context, MakeSlowEchoHandler(), executor,
                                                          std::move(pair.server_socket_), std::move(options));
  xrpc::runtime::Task<void> task = connection->Run();
  UringContextRunner runner;
  StartTaskOnContext(context, task);
  runner.Start(context);

  const std::string first_request = MakeRequestFrame("first", 11);
  const std::string second_request = MakeRequestFrame("second", 12);
  const std::size_t split = first_request.size() / 2;
  pair.client_socket_.WriteAll(std::string_view(first_request.data(), split));

  std::string remainder_and_second(first_request.data() + split, first_request.size() - split);
  remainder_and_second.append(second_request);
  pair.client_socket_.WriteAll(remainder_and_second);
  pair.client_socket_.ShutdownWrite();

  std::string received_buffer;
  const std::string first_response = RecvFrame(pair.client_socket_, received_buffer);
  const std::string second_response = RecvFrame(pair.client_socket_, received_buffer);
  pair.client_socket_.Close();

  ASSERT_TRUE(WaitForConnectionTask(task, context, runner));
  EXPECT_TRUE(connection->IsClosed());
  EXPECT_EQ(connection->close_reason(), xrpc::ConnectionCloseReason::PeerClosed);
  EXPECT_EQ(DecodeEchoMessage(first_response, 11), "echo: first");
  EXPECT_EQ(DecodeEchoMessage(second_response, 12), "echo: second");
}

TEST(CoroutineTcpConnectionTest, HandlesPipelinedRequestsOnOneConnection) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::io::UringContext context;
  xrpc::ServerBackpressureStats stats;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpConnectionOptions options = MakeConnectionOptions(context, stats);
  auto connection = std::make_shared<xrpc::TcpConnection>(context, MakeEchoHandler(), executor,
                                                          std::move(pair.server_socket_), std::move(options));
  xrpc::runtime::Task<void> task = connection->Run();
  UringContextRunner runner;
  StartTaskOnContext(context, task);
  runner.Start(context);

  const std::string first_request = MakeRequestFrame("first", 21);
  const std::string second_request = MakeRequestFrame("second", 22);
  pair.client_socket_.WriteAll(first_request + second_request);

  std::string received_buffer;
  const std::string first_response = RecvFrame(pair.client_socket_, received_buffer);
  const std::string second_response = RecvFrame(pair.client_socket_, received_buffer);

  pair.client_socket_.ShutdownWrite();
  pair.client_socket_.Close();

  ASSERT_TRUE(WaitForConnectionTask(task, context, runner));
  EXPECT_TRUE(connection->IsClosed());
  EXPECT_EQ(connection->close_reason(), xrpc::ConnectionCloseReason::PeerClosed);
  EXPECT_EQ(DecodeEchoMessage(first_response, 21), "echo: first");
  EXPECT_EQ(DecodeEchoMessage(second_response, 22), "echo: second");
}

TEST(CoroutineTcpConnectionTest, ClosesOnInvalidFrame) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::io::UringContext context;
  xrpc::ServerBackpressureStats stats;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpConnectionOptions options = MakeConnectionOptions(context, stats);
  auto connection = std::make_shared<xrpc::TcpConnection>(context, MakeEchoHandler(), executor,
                                                          std::move(pair.server_socket_), std::move(options));
  xrpc::runtime::Task<void> task = connection->Run();
  UringContextRunner runner;
  StartTaskOnContext(context, task);
  runner.Start(context);

  std::string invalid_request = MakeRequestFrame("hello", 42);
  invalid_request[0] = '\0';
  pair.client_socket_.WriteAll(invalid_request);
  pair.client_socket_.ShutdownWrite();
  pair.client_socket_.Close();

  ASSERT_TRUE(WaitForConnectionTask(task, context, runner));
  EXPECT_TRUE(connection->IsClosed());
  EXPECT_EQ(connection->close_reason(), xrpc::ConnectionCloseReason::ProtocolError);
}

TEST(CoroutineTcpConnectionTest, HandlesConcurrentResponsesWithThreadPoolExecutor) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::io::UringContext context;
  xrpc::ServerBackpressureStats stats;
  xrpc::ThreadPoolExecutor executor(2);
  xrpc::TcpConnectionOptions options = MakeConnectionOptions(context, stats);
  auto connection = std::make_shared<xrpc::TcpConnection>(context, MakeEchoHandler(), executor,
                                                          std::move(pair.server_socket_), std::move(options));
  xrpc::runtime::Task<void> task = connection->Run();
  UringContextRunner runner;
  StartTaskOnContext(context, task);
  runner.Start(context);

  xrpc::test::EchoRequest slow_request;
  slow_request.set_message("slow");
  xrpc::ProtocolRequest slow_protocol_request;
  slow_protocol_request.request_id_ = 31;
  slow_protocol_request.service_name_ = "EchoService";
  slow_protocol_request.method_name_ = "SlowEcho";
  slow_protocol_request.payload_ = slow_request.SerializeAsString();

  xrpc::test::EchoRequest fast_request;
  fast_request.set_message("fast");
  xrpc::ProtocolRequest fast_protocol_request;
  fast_protocol_request.request_id_ = 32;
  fast_protocol_request.service_name_ = "EchoService";
  fast_protocol_request.method_name_ = "Echo";
  fast_protocol_request.payload_ = fast_request.SerializeAsString();

  xrpc::FrameCodec codec;
  pair.client_socket_.WriteAll(codec.EncodeRequest(slow_protocol_request) + codec.EncodeRequest(fast_protocol_request));

  std::string received_buffer;
  const std::string first_response = RecvFrame(pair.client_socket_, received_buffer);
  const std::string second_response = RecvFrame(pair.client_socket_, received_buffer);
  pair.client_socket_.ShutdownWrite();
  pair.client_socket_.Close();

  ASSERT_TRUE(WaitForConnectionTask(task, context, runner));
  EXPECT_TRUE(connection->IsClosed());
  EXPECT_EQ(connection->close_reason(), xrpc::ConnectionCloseReason::PeerClosed);

  const xrpc::DecodeResult first_decoded = codec.TryDecode(first_response);
  const xrpc::DecodeResult second_decoded = codec.TryDecode(second_response);
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

TEST(CoroutineTcpConnectionTest, KeepsReadingWhileWorkerHandlerIsPending) {
  std::promise<void> handler_started;
  std::future<void> handler_started_future = handler_started.get_future();
  std::promise<void> release_handler;
  std::shared_future<void> release_handler_future = release_handler.get_future().share();

  xrpc::RawHandler blocking_handler = [&](const xrpc::RawRequest &request) {
    handler_started.set_value();
    release_handler_future.wait();

    xrpc::RawResponse response;
    response.request_id_ = request.request_id_;
    response.status_ = xrpc::Status::Ok();
    response.payload_ = request.payload_;
    return response;
  };

  ConnectedPair pair = MakeConnectedPair();
  xrpc::io::UringContext context;
  xrpc::ServerBackpressureStats stats;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpConnectionOptions options = MakeConnectionOptions(
      context, stats, xrpc::ConnectionBackpressureLimits{.max_inflight_ = 1, .max_write_queue_bytes_ = 1024});
  auto connection = std::make_shared<xrpc::TcpConnection>(context, std::move(blocking_handler), executor,
                                                          std::move(pair.server_socket_), std::move(options));
  xrpc::runtime::Task<void> task = connection->Run();
  UringContextRunner runner;
  StartTaskOnContext(context, task);
  runner.Start(context);

  pair.client_socket_.WriteAll(MakeRequestFrame("first", 81));
  const std::future_status handler_started_status = handler_started_future.wait_for(WaitTimeout);

  bool task_done_before_handler_release = false;
  if (handler_started_status == std::future_status::ready) {
    pair.client_socket_.WriteAll(MakeRequestFrame("second", 82));
    std::string received_buffer;
    const std::string rejection_response = RecvFrame(pair.client_socket_, received_buffer);
    const xrpc::Status rejection_status = DecodeResponseStatus(rejection_response, 82);
    EXPECT_EQ(rejection_status.code(), xrpc::StatusCode::ResourceExhausted);
    task_done_before_handler_release = WaitTaskDone(task, std::chrono::milliseconds(20), PollInterval);
  }
  release_handler.set_value();

  pair.client_socket_.ShutdownWrite();
  pair.client_socket_.Close();
  ASSERT_TRUE(WaitTaskDone(task, WaitTimeout, PollInterval));
  runner.StopAndJoin(context);

  EXPECT_EQ(handler_started_status, std::future_status::ready);
  EXPECT_FALSE(task_done_before_handler_release);
  EXPECT_TRUE(connection->IsClosed());
  EXPECT_NE(connection->close_reason(), xrpc::ConnectionCloseReason::Backpressure);

  const xrpc::ServerBackpressureSnapshot snapshot = stats.Snapshot();
  EXPECT_EQ(snapshot.rejected_by_inflight_limit_, 1U);
  EXPECT_EQ(snapshot.max_observed_inflight_, 1U);
}

TEST(CoroutineTcpConnectionTest, ReturnsResourceExhaustedWhenPerConnectionInflightLimitIsReached) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::io::UringContext context;
  xrpc::ServerBackpressureStats stats;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpConnectionOptions options = MakeConnectionOptions(
      context, stats, xrpc::ConnectionBackpressureLimits{.max_inflight_ = 1, .max_write_queue_bytes_ = 1024});
  auto connection = std::make_shared<xrpc::TcpConnection>(context, MakeSlowEchoHandler(), executor,
                                                          std::move(pair.server_socket_), std::move(options));
  xrpc::runtime::Task<void> task = connection->Run();
  UringContextRunner runner;
  StartTaskOnContext(context, task);
  runner.Start(context);

  xrpc::test::EchoRequest request;
  request.set_message("slow");
  xrpc::ProtocolRequest protocol_request;
  protocol_request.request_id_ = 51;
  protocol_request.service_name_ = "EchoService";
  protocol_request.method_name_ = "SlowEcho";
  protocol_request.payload_ = request.SerializeAsString();
  xrpc::ProtocolRequest rejected_protocol_request = protocol_request;
  rejected_protocol_request.request_id_ = 52;
  xrpc::FrameCodec codec;
  pair.client_socket_.WriteAll(codec.EncodeRequest(protocol_request) + codec.EncodeRequest(rejected_protocol_request));

  std::string received_buffer;
  const std::string rejection_response = RecvFrame(pair.client_socket_, received_buffer);
  const xrpc::Status rejection_status = DecodeResponseStatus(rejection_response, 52);
  EXPECT_EQ(rejection_status.code(), xrpc::StatusCode::ResourceExhausted);

  pair.client_socket_.ShutdownWrite();
  ASSERT_TRUE(WaitTaskDone(task, WaitTimeout, PollInterval));
  pair.client_socket_.Close();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  runner.StopAndJoin(context);

  EXPECT_NE(connection->close_reason(), xrpc::ConnectionCloseReason::Backpressure);
  const xrpc::ServerBackpressureSnapshot snapshot = stats.Snapshot();
  EXPECT_EQ(snapshot.rejected_by_inflight_limit_, 1U);
  EXPECT_EQ(snapshot.max_observed_inflight_, 1U);
}

TEST(CoroutineTcpConnectionTest, ClosesWhenWriteQueueByteLimitIsReached) {
  ConnectedPair pair = MakeConnectedPair();
  xrpc::io::UringContext context;
  xrpc::ServerBackpressureStats stats;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpConnectionOptions options = MakeConnectionOptions(
      context, stats, xrpc::ConnectionBackpressureLimits{.max_inflight_ = 8, .max_write_queue_bytes_ = 1});
  auto connection = std::make_shared<xrpc::TcpConnection>(context, MakeEchoHandler(), executor,
                                                          std::move(pair.server_socket_), std::move(options));
  xrpc::runtime::Task<void> task = connection->Run();
  UringContextRunner runner;
  StartTaskOnContext(context, task);
  runner.Start(context);

  pair.client_socket_.WriteAll(MakeRequestFrame("response-is-larger-than-one-byte", 71));

  ASSERT_TRUE(WaitTaskDone(task, WaitTimeout, PollInterval));
  pair.client_socket_.Close();
  runner.StopAndJoin(context);

  EXPECT_EQ(connection->close_reason(), xrpc::ConnectionCloseReason::Backpressure);
  const xrpc::ServerBackpressureSnapshot snapshot = stats.Snapshot();
  EXPECT_EQ(snapshot.closed_by_write_queue_high_watermark_, 1U);
  EXPECT_EQ(snapshot.max_observed_write_queue_bytes_, 0U);
}
