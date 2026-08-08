#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "common/task.h"
#include "io/socket.h"
#include "io/uring_context.h"

namespace {

constexpr auto PollInterval = std::chrono::milliseconds(1);
constexpr auto WaitTimeout = std::chrono::milliseconds(1000);

template <typename T>
void StartTaskOnContext(xrpc::io::UringContext &context, xrpc::runtime::Task<T> &task) {
  context.Post([&task]() { task.Start(); });
}

template <typename T>
auto WaitTaskWithContext(xrpc::runtime::Task<T> task, xrpc::io::UringContext &context) -> T {
  StartTaskOnContext(context, task);

  std::exception_ptr context_error;
  std::jthread context_thread([&]() {
    try {
      context.Run();
    } catch (...) {
      context_error = std::current_exception();
    }
  });

  const auto deadline = std::chrono::steady_clock::now() + WaitTimeout;
  while (!task.Done() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(PollInterval);
  }

  context.Stop();
  context_thread.join();
  if (context_error) {
    std::rethrow_exception(context_error);
  }

  if (!task.Done()) {
    throw std::runtime_error("timed out waiting for task completion");
  }

  return task.Result();
}

auto ReadOne(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<xrpc::io::IoResult> {
  auto read_buffer = std::make_shared<std::array<char, 64>>();
  xrpc::io::IoResult result = co_await context.Recv(fd, read_buffer->data(), read_buffer->size());
  co_return result;
}

auto SendOne(xrpc::io::UringContext &context, int fd, const std::shared_ptr<std::string> &payload)
    -> xrpc::runtime::Task<xrpc::io::IoResult> {
  xrpc::io::IoResult result = co_await context.Send(fd, payload->data(), payload->size());
  co_return result;
}

auto AwaitNop(xrpc::io::UringContext &context) -> xrpc::runtime::Task<xrpc::io::IoResult> {
  xrpc::io::IoResult result = co_await context.Nop();
  co_return result;
}

auto SleepFor(xrpc::io::UringContext &context, std::chrono::nanoseconds timeout)
    -> xrpc::runtime::Task<xrpc::io::IoResult> {
  xrpc::io::IoResult result = co_await context.SleepFor(timeout);
  co_return result;
}

auto ReadInvalidFd(xrpc::io::UringContext &context) -> xrpc::runtime::Task<xrpc::io::IoResult> {
  auto read_buffer = std::make_shared<std::array<char, 8>>();
  xrpc::io::IoResult result = co_await context.Recv(-1, read_buffer->data(), read_buffer->size());
  co_return result;
}

}  // namespace

TEST(IoUringAwaitableTest, NopResumesCoroutine) {
  xrpc::io::UringContext context;

  const xrpc::io::IoResult result = WaitTaskWithContext(AwaitNop(context), context);
  EXPECT_EQ(result.type_, xrpc::io::OperationType::Nop);
  EXPECT_EQ(result.error_code_, 0);
  EXPECT_EQ(result.result_, 0);
}

TEST(IoUringAwaitableTest, SleepForResumesCoroutine) {
  xrpc::io::UringContext context;

  const xrpc::io::IoResult result = WaitTaskWithContext(SleepFor(context, std::chrono::milliseconds(5)), context);
  EXPECT_EQ(result.type_, xrpc::io::OperationType::Timeout);
  EXPECT_EQ(result.error_code_, 0);
  EXPECT_EQ(result.result_, 0);
}

TEST(IoUringAwaitableTest, StopCancelsPendingSleepFor) {
  xrpc::io::UringContext context;
  xrpc::runtime::Task<xrpc::io::IoResult> task = SleepFor(context, std::chrono::hours(1));
  StartTaskOnContext(context, task);

  std::exception_ptr context_error;
  std::jthread context_thread([&]() {
    try {
      context.Run();
    } catch (...) {
      context_error = std::current_exception();
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  context.Stop();
  context_thread.join();
  if (context_error) {
    std::rethrow_exception(context_error);
  }

  ASSERT_TRUE(task.Done());
  const xrpc::io::IoResult result = task.Result();
  EXPECT_EQ(result.type_, xrpc::io::OperationType::Timeout);
  EXPECT_EQ(result.error_code_, ECANCELED);
  EXPECT_LT(result.result_, 0);
}

TEST(IoUringAwaitableTest, SendAndRecvReturnExpectedResults) {
  xrpc::io::Socket listen_socket;
  listen_socket.Bind("127.0.0.1", 0);
  listen_socket.Listen(1);

  xrpc::io::Socket client_socket;
  client_socket.Connect("127.0.0.1", listen_socket.LocalPort());
  xrpc::io::Socket server_socket = listen_socket.Accept();

  const auto payload = std::make_shared<std::string>("awaitable-message");

  {
    xrpc::io::UringContext send_context;
    xrpc::io::IoResult send_result =
        WaitTaskWithContext(SendOne(send_context, client_socket.fd(), payload), send_context);
    EXPECT_EQ(send_result.type_, xrpc::io::OperationType::Send);
    EXPECT_EQ(send_result.error_code_, 0);
    EXPECT_EQ(send_result.bytes_transferred_, payload->size());
  }

  {
    xrpc::io::UringContext recv_context;
    xrpc::io::IoResult recv_result = WaitTaskWithContext(ReadOne(recv_context, server_socket.fd()), recv_context);
    EXPECT_EQ(recv_result.type_, xrpc::io::OperationType::Recv);
    EXPECT_EQ(recv_result.error_code_, 0);
    EXPECT_EQ(recv_result.bytes_transferred_, payload->size());
  }
}

TEST(IoUringAwaitableTest, RecvOnInvalidFdReturnsError) {
  xrpc::io::UringContext context;

  const xrpc::io::IoResult result = WaitTaskWithContext(ReadInvalidFd(context), context);
  EXPECT_EQ(result.type_, xrpc::io::OperationType::Recv);
  EXPECT_NE(result.error_code_, 0);
  EXPECT_LT(result.result_, 0);
}
