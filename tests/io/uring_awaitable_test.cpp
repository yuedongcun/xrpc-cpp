#include <gtest/gtest.h>

#include <array>
#include <atomic>
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

  const bool completed = task.WaitFor(WaitTimeout);

  context.RequestStop();
  context_thread.join();
  if (context_error) {
    std::rethrow_exception(context_error);
  }

  if (!completed) {
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

auto ReadInvalidFd(xrpc::io::UringContext &context) -> xrpc::runtime::Task<xrpc::io::IoResult> {
  auto read_buffer = std::make_shared<std::array<char, 8>>();
  xrpc::io::IoResult result = co_await context.Recv(-1, read_buffer->data(), read_buffer->size());
  co_return result;
}

auto MoveAwaitableBeforeSuspend(xrpc::io::UringContext &context) -> xrpc::runtime::Task<xrpc::io::IoResult> {
  auto read_buffer = std::make_shared<std::array<char, 8>>();
  xrpc::io::UringAwaitable awaitable = context.Recv(-1, read_buffer->data(), read_buffer->size());
  xrpc::io::UringAwaitable moved_awaitable = std::move(awaitable);
  co_return co_await std::move(moved_awaitable);
}

auto PendingRead(xrpc::io::UringContext &context, int fd, std::atomic<bool> &submitted)
    -> xrpc::runtime::Task<xrpc::io::IoResult> {
  auto read_buffer = std::make_shared<std::array<char, 8>>();
  xrpc::io::UringAwaitable awaitable = context.Recv(fd, read_buffer->data(), read_buffer->size());
  submitted.store(true);
  co_return co_await std::move(awaitable);
}

auto SubmitAfterStop(xrpc::io::UringContext &context) -> xrpc::runtime::Task<xrpc::io::IoResult> {
  auto read_buffer = std::make_shared<std::array<char, 8>>();
  context.RequestStop();
  co_return co_await context.Recv(-1, read_buffer->data(), read_buffer->size());
}

}  // namespace

TEST(IoUringAwaitableTest, MoveAfterSubmissionPreservesCompletion) {
  xrpc::io::UringContext context;

  const xrpc::io::IoResult result = WaitTaskWithContext(MoveAwaitableBeforeSuspend(context), context);
  EXPECT_EQ(result.type_, xrpc::io::OperationType::Recv);
  EXPECT_NE(result.error_code_, 0);
  EXPECT_LT(result.result_, 0);
}

TEST(IoUringAwaitableTest, CompletionBeforeAwaitSuspendIsObserved) {
  xrpc::io::UringContext context;

  const xrpc::io::IoResult result = WaitTaskWithContext(SubmitAfterStop(context), context);
  EXPECT_EQ(result.type_, xrpc::io::OperationType::Recv);
  EXPECT_EQ(result.error_code_, ECANCELED);
  EXPECT_LT(result.result_, 0);
}

TEST(IoUringAwaitableTest, DestroyingPendingTaskDoesNotLeaveCompletionTarget) {
  xrpc::io::Socket listen_socket;
  listen_socket.Bind("127.0.0.1", 0);
  listen_socket.Listen(1);

  xrpc::io::Socket client_socket;
  client_socket.Connect("127.0.0.1", listen_socket.LocalPort());
  xrpc::io::Socket server_socket = listen_socket.Accept();

  xrpc::io::UringContext context;
  std::exception_ptr context_error;
  std::atomic<bool> submitted = false;

  std::jthread context_thread([&]() {
    try {
      context.Run();
    } catch (...) {
      context_error = std::current_exception();
    }
  });

  {
    xrpc::runtime::Task<xrpc::io::IoResult> task = PendingRead(context, server_socket.fd(), submitted);
    StartTaskOnContext(context, task);

    for (int attempt = 0; attempt < 100 && !submitted.load(); ++attempt) {
      std::this_thread::sleep_for(PollInterval);
    }
    EXPECT_TRUE(submitted.load());
  }

  client_socket.Close();
  context.RequestStop();
  context_thread.join();

  if (context_error) {
    std::rethrow_exception(context_error);
  }
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
