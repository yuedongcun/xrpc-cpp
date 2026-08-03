#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <xrpc/rpc_client.h>

#include "io/uring_context.h"
#include "proto/echo.pb.h"
#include "test_support/rpc_server_handler.h"
#include "test_support/runtime_util.h"
#include "transport/tcp_server.h"
#include "transport/thread_pool_executor.h"

namespace {

constexpr auto PollInterval = std::chrono::milliseconds(1);
constexpr auto WaitTimeout = std::chrono::milliseconds(1000);

auto Echo(const xrpc::test::EchoRequest &request) -> xrpc::test::EchoResponse {
  xrpc::test::EchoResponse response;
  response.set_message("echo: " + request.message());
  return response;
}

auto SlowEcho(const xrpc::test::EchoRequest &request) -> xrpc::test::EchoResponse {
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  return Echo(request);
}

auto MakeEchoHandler() -> xrpc::RawHandler {
  return xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService",
                                                                                                     "Echo", Echo);
}

auto MakeSlowEchoHandler() -> xrpc::RawHandler {
  return xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "SlowEcho", SlowEcho);
}

auto MakeEchoAndSlowEchoHandler() -> xrpc::RawHandler {
  std::vector<xrpc::MethodRegistration> registrations;
  registrations.push_back(
      xrpc::MakeMethodRegistration<xrpc::test::EchoRequest, xrpc::test::EchoResponse>("EchoService", "Echo", Echo));
  registrations.push_back(xrpc::MakeMethodRegistration<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "SlowEcho", SlowEcho));
  return xrpc::testsupport::MakeRegisteredHandler(std::move(registrations));
}

auto CallMethod(std::uint16_t port, const std::string &method_name, const std::string &message) -> std::string {
  xrpc::RpcClient client("127.0.0.1", port);
  const xrpc::Status init_status = client.Init();
  if (!init_status.ok()) {
    return init_status.message();
  }

  xrpc::test::EchoRequest request;
  request.set_message(message);

  const xrpc::StatusOr<xrpc::test::EchoResponse> response =
      client.Call<xrpc::test::EchoResponse>("EchoService", method_name, request);
  if (!response.ok()) {
    return response.status().message();
  }
  return response.value().message();
}

auto CallEcho(std::uint16_t port, const std::string &message) -> std::string {
  return CallMethod(port, "Echo", message);
}

}  // namespace

TEST(CoroutineTcpServerTest, AcceptsSequentialTypedRpcClients) {
  xrpc::io::UringContext context;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpServer server(context, MakeEchoHandler(), executor);
  server.Listen("127.0.0.1", 0);

  xrpc::runtime::Task<void> server_task = server.Run();
  xrpc::testsupport::UringContextRunner runner;
  xrpc::testsupport::StartTaskOnContext(context, server_task);
  runner.Start(context);

  EXPECT_EQ(CallEcho(server.port(), "one"), "echo: one");
  EXPECT_EQ(CallEcho(server.port(), "two"), "echo: two");

  context.Post([&server]() { server.Stop(); });
  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (!server_task.Done() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }
  runner.StopAndJoin(context);

  EXPECT_TRUE(server_task.Done());
  EXPECT_EQ(server.ConnectionCount(), 0U);
}

TEST(CoroutineTcpServerTest, FastRequestCanFinishBeforeSlowRequestWithThreadPoolExecutor) {
  xrpc::io::UringContext context;
  xrpc::ThreadPoolExecutor executor(2);
  xrpc::TcpServer server(context, MakeEchoAndSlowEchoHandler(), executor);
  server.Listen("127.0.0.1", 0);

  xrpc::runtime::Task<void> server_task = server.Run();
  xrpc::testsupport::UringContextRunner runner;
  xrpc::testsupport::StartTaskOnContext(context, server_task);
  runner.Start(context);

  auto slow_call = std::async(std::launch::async, [&]() { return CallMethod(server.port(), "SlowEcho", "slow"); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  const auto fast_start = std::chrono::steady_clock::now();
  const std::string fast_result = CallEcho(server.port(), "fast");
  const auto fast_elapsed = std::chrono::steady_clock::now() - fast_start;

  const std::string slow_result = slow_call.get();

  EXPECT_EQ(fast_result, "echo: fast");
  EXPECT_EQ(slow_result, "echo: slow");
  EXPECT_LT(fast_elapsed, std::chrono::milliseconds(180));

  context.Post([&server]() { server.Stop(); });
  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (!server_task.Done() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }
  runner.StopAndJoin(context);

  EXPECT_TRUE(server_task.Done());
}

TEST(CoroutineTcpServerTest, AcceptLoopCanDispatchConnectionsToMultipleIoLoops) {
  xrpc::io::UringContext context;
  xrpc::ThreadPoolExecutor executor(2);
  xrpc::TcpServer server(context, MakeEchoHandler(), executor, xrpc::ServerBackpressureLimits{}, 2);
  server.Listen("127.0.0.1", 0);

  xrpc::runtime::Task<void> server_task = server.Run();
  xrpc::testsupport::UringContextRunner runner;
  xrpc::testsupport::StartTaskOnContext(context, server_task);
  runner.Start(context);

  EXPECT_EQ(CallEcho(server.port(), "one"), "echo: one");
  EXPECT_EQ(CallEcho(server.port(), "two"), "echo: two");
  EXPECT_EQ(CallEcho(server.port(), "three"), "echo: three");
  EXPECT_EQ(CallEcho(server.port(), "four"), "echo: four");

  context.Post([&server]() { server.Stop(); });
  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (!server_task.Done() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }
  runner.StopAndJoin(context);

  EXPECT_TRUE(server_task.Done());
  EXPECT_EQ(server.ConnectionCount(), 0U);
}

TEST(CoroutineTcpServerTest, StopUnblocksRunWhileAcceptIsIdle) {
  xrpc::io::UringContext context;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpServer server(context, MakeEchoHandler(), executor);
  server.Listen("127.0.0.1", 0);

  xrpc::runtime::Task<void> server_task = server.Run();
  xrpc::testsupport::UringContextRunner runner;
  xrpc::testsupport::StartTaskOnContext(context, server_task);
  runner.Start(context);

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  context.Post([&server]() { server.Stop(); });

  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (!server_task.Done() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }
  runner.StopAndJoin(context);

  EXPECT_TRUE(server_task.Done());
}

TEST(CoroutineTcpServerTest, StopReturnsWhileWorkerHandlerIsPendingOnConnectionIoLoop) {
  std::promise<void> handler_started;
  std::future<void> handler_started_future = handler_started.get_future();
  std::promise<void> release_handler;
  std::shared_future<void> release_handler_future = release_handler.get_future().share();
  std::atomic<bool> handler_started_once = false;

  auto blocking_handler = xrpc::testsupport::MakeRegisteredHandler<xrpc::test::EchoRequest, xrpc::test::EchoResponse>(
      "EchoService", "BlockingEcho", [&](const xrpc::test::EchoRequest &request) {
        if (!handler_started_once.exchange(true, std::memory_order_acq_rel)) {
          handler_started.set_value();
        }
        release_handler_future.wait();
        return Echo(request);
      });

  xrpc::io::UringContext context;
  xrpc::ThreadPoolExecutor executor(1);
  xrpc::TcpServer server(context, std::move(blocking_handler), executor, xrpc::ServerBackpressureLimits{}, 2);
  server.Listen("127.0.0.1", 0);

  xrpc::runtime::Task<void> server_task = server.Run();
  xrpc::testsupport::UringContextRunner runner;
  xrpc::testsupport::StartTaskOnContext(context, server_task);
  runner.Start(context);

  std::future<std::string> blocking_call =
      std::async(std::launch::async, [&]() { return CallMethod(server.port(), "BlockingEcho", "blocked"); });
  const std::future_status handler_started_status = handler_started_future.wait_for(WaitTimeout);

  context.Post([&server]() { server.Stop(); });
  const bool stopped_before_handler_release = xrpc::testsupport::WaitTaskDone(server_task, WaitTimeout, PollInterval);
  release_handler.set_value();

  EXPECT_EQ(handler_started_status, std::future_status::ready);
  EXPECT_TRUE(stopped_before_handler_release);
  EXPECT_EQ(blocking_call.wait_for(WaitTimeout), std::future_status::ready);

  runner.StopAndJoin(context);

  EXPECT_TRUE(server_task.Done());
  EXPECT_EQ(server.ConnectionCount(), 0U);
}

TEST(CoroutineTcpServerTest, CallReturnsDeadlineExceededWhenCallOptionsTimeoutExpires) {
  xrpc::io::UringContext context;
  xrpc::ThreadPoolExecutor executor(2);
  xrpc::TcpServer server(context, MakeSlowEchoHandler(), executor);
  server.Listen("127.0.0.1", 0);

  xrpc::runtime::Task<void> server_task = server.Run();
  xrpc::testsupport::UringContextRunner runner;
  xrpc::testsupport::StartTaskOnContext(context, server_task);
  runner.Start(context);

  xrpc::RpcClient client("127.0.0.1", server.port());
  const xrpc::Status init_status = client.Init();
  ASSERT_TRUE(init_status.ok()) << init_status.message();

  xrpc::CallOptions options;
  options.timeout_ = std::chrono::milliseconds(50);

  xrpc::test::EchoRequest request;
  request.set_message("slow");

  const xrpc::StatusOr<xrpc::test::EchoResponse> response =
      client.Call<xrpc::test::EchoResponse>("EchoService", "SlowEcho", request, options);
  EXPECT_FALSE(response.ok());
  EXPECT_EQ(response.status().code(), xrpc::StatusCode::DeadlineExceeded);

  context.Post([&server]() { server.Stop(); });
  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (!server_task.Done() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }
  runner.StopAndJoin(context);

  EXPECT_TRUE(server_task.Done());
}
