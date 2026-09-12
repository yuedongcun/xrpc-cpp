#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>

#include "common/task.h"
#include "common/xrpc_exception.h"
#include "io/socket.h"
#include "io/uring/context.h"

namespace {

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
    } catch (...) {  // XRPC_EXTERNAL_EXCEPTION_BOUNDARY: test thread entry
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
  EXPECT_FALSE(result.has_more_);
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

auto MoveAwaitableBeforeAwait(xrpc::io::UringContext &context) -> xrpc::runtime::Task<xrpc::io::IoResult> {
  auto read_buffer = std::make_shared<std::array<char, 8>>();
  xrpc::io::UringAwaitable awaitable = context.Recv(-1, read_buffer->data(), read_buffer->size());
  xrpc::io::UringAwaitable moved_awaitable = std::move(awaitable);
  co_return co_await std::move(moved_awaitable);
}

auto PendingRead(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<xrpc::io::IoResult> {
  auto read_buffer = std::make_shared<std::array<char, 8>>();
  co_return co_await context.Recv(fd, read_buffer->data(), read_buffer->size());
}

auto SubmitAfterStop(xrpc::io::UringContext &context) -> xrpc::runtime::Task<xrpc::io::IoResult> {
  auto read_buffer = std::make_shared<std::array<char, 8>>();
  context.RequestStop();
  co_return co_await context.Recv(-1, read_buffer->data(), read_buffer->size());
}

auto ReadProvidedTwice(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<std::string> {
  std::string received;
  for (int read = 0; read < 2; ++read) {
    xrpc::io::IoResult result = co_await context.RecvProvided(fd);
    if (result.result_ <= 0) {
      co_return received;
    }
    const std::span<const std::byte> bytes = result.buffer_.Bytes();
    received.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
  }
  co_return received;
}

auto ReadProvidedInvalidFd(xrpc::io::UringContext &context) -> xrpc::runtime::Task<xrpc::io::IoResult> {
  co_return co_await context.RecvProvided(-1);
}

auto ExhaustAndReusePool(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<void> {
  auto first = co_await context.RecvProvided(fd);
  EXPECT_EQ(first.result_, 4);
  auto held_buffer = std::move(first.buffer_);
  EXPECT_TRUE(first.buffer_.Empty());

  // The only buffer remains leased, so the next receive must fail rather than overwrite it.
  auto exhausted = co_await context.RecvProvided(fd);
  EXPECT_EQ(exhausted.error_code_, ENOBUFS);
  EXPECT_EQ(exhausted.buffer_group_, 7);
  EXPECT_TRUE(exhausted.buffer_.Empty());
  const auto bytes = held_buffer.Bytes();
  EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(bytes.data()), bytes.size()), "abcd");

  held_buffer = {};
  auto next = co_await context.RecvProvided(fd);
  EXPECT_EQ(next.result_, 4);
  const auto next_bytes = next.buffer_.Bytes();
  EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(next_bytes.data()), next_bytes.size()), "efgh");
  next.buffer_ = {};
  auto eof = co_await context.RecvProvided(fd);
  EXPECT_EQ(eof.result_, 0);
  EXPECT_TRUE(eof.buffer_.Empty());
}

auto ReadMultishot(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<std::string> {
  auto receive = context.RecvProvidedMultishot(fd);
  std::string received;
  std::size_t completions = 0;
  while (true) {
    auto result = co_await receive;
    EXPECT_EQ(result.error_code_, 0);
    if (result.result_ <= 0) {
      EXPECT_EQ(result.result_, 0);
      EXPECT_FALSE(result.has_more_);
      break;
    }
    ++completions;
    const auto bytes = result.buffer_.Bytes();
    received.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
    if (!result.has_more_) {
      receive = context.RecvProvidedMultishot(fd);
    }
  }
  EXPECT_GE(completions, 3);
  co_return received;
}

auto CancelMultishot(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<void> {
  auto receive = context.RecvProvidedMultishot(fd);
  context.Post([&context, fd]() {
    context.CancelFd(fd);
    context.CancelFd(fd);
  });
  while (true) {
    auto result = co_await receive;
    if (result.error_code_ == ECANCELED) {
      EXPECT_TRUE(result.buffer_.Empty());
      EXPECT_FALSE(result.has_more_);
      break;
    }
    EXPECT_GT(result.result_, 0);
    if (result.result_ <= 0) {
      co_return;
    }
    result.buffer_ = {};
  }
}

auto DrainCancelledMultishot(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<void> {
  {
    auto receive = context.RecvProvidedMultishot(fd);
    auto result = co_await receive;
    EXPECT_GT(result.result_, 0);
    context.CancelFd(fd);
    while (true) {
      auto drained = co_await receive;
      if (drained.error_code_ == ECANCELED) {
        EXPECT_TRUE(drained.buffer_.Empty());
        EXPECT_FALSE(drained.has_more_);
        break;
      }
      EXPECT_GT(drained.result_, 0);
      if (drained.result_ <= 0) {
        co_return;
      }
      drained.buffer_ = {};
    }
  }
  // Reception has drained; Run also drains the cancellation acknowledgement.
  context.RequestStop();
}

auto DestroyUndrainedMultishot(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<void> {
  auto receive = context.RecvProvidedMultishot(fd);
  auto result = co_await receive;
  EXPECT_GT(result.result_, 0);
  context.CancelFd(fd);
  // A cancellation request alone is not permission to destroy the awaitable.
}

auto ExhaustMultishot(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<void> {
  auto receive = context.RecvProvidedMultishot(fd);
  auto first = co_await receive;
  EXPECT_EQ(first.result_, 4);
  EXPECT_TRUE(first.has_more_);
  auto exhausted = co_await receive;
  EXPECT_EQ(exhausted.error_code_, ENOBUFS);
  EXPECT_FALSE(exhausted.has_more_);
  EXPECT_EQ(exhausted.buffer_group_, 7);
  const auto bytes = first.buffer_.Bytes();
  EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(bytes.data()), bytes.size()), "abcd");
  first.buffer_ = {};
  auto remaining = co_await context.RecvProvided(fd);
  EXPECT_EQ(remaining.result_, 4);
  const auto next_bytes = remaining.buffer_.Bytes();
  EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(next_bytes.data()), next_bytes.size()), "efgh");
}

auto CheckMultishotErrors(xrpc::io::UringContext &context) -> xrpc::runtime::Task<void> {
  auto invalid = context.RecvProvidedMultishot(-1);
  auto error = co_await invalid;
  EXPECT_EQ(error.error_code_, EBADF);
  EXPECT_FALSE(error.has_more_);
  auto stopped = context.RecvProvidedMultishot(-1);
  context.RequestStop();
  auto after_stop = co_await stopped;
  EXPECT_EQ(after_stop.error_code_, ECANCELED);
  EXPECT_FALSE(after_stop.has_more_);
}

}  // namespace

TEST(IoUringAwaitableTest, MoveBeforeAwaitPreservesOperation) {
  xrpc::io::UringContext context;

  const xrpc::io::IoResult result = WaitTaskWithContext(MoveAwaitableBeforeAwait(context), context);
  EXPECT_EQ(result.type_, xrpc::io::OperationType::Recv);
  EXPECT_NE(result.error_code_, 0);
  EXPECT_LT(result.result_, 0);
}

TEST(IoUringAwaitableTest, StartAfterStopReturnsSynchronousCancellation) {
  xrpc::io::UringContext context;

  const xrpc::io::IoResult result = WaitTaskWithContext(SubmitAfterStop(context), context);
  EXPECT_EQ(result.type_, xrpc::io::OperationType::Recv);
  EXPECT_EQ(result.error_code_, ECANCELED);
  EXPECT_LT(result.result_, 0);
}

TEST(IoUringAwaitableTest, UnawaitedOperationIsNeverStarted) {
  xrpc::io::UringContext context;
  std::array<char, 8> read_buffer{};
  [[maybe_unused]] xrpc::io::UringAwaitable awaitable = context.Recv(-1, read_buffer.data(), read_buffer.size());
}

TEST(IoUringAwaitableTest, ProvidedRecvRequiresRegisteredPool) {
  xrpc::io::UringContext context;

  EXPECT_THROW((void)context.RecvProvided(-1), xrpc::LifecycleException);
}

TEST(IoUringAwaitableTest, DestroyingPendingIoTaskTerminates) {
  EXPECT_DEATH(
      {
        xrpc::io::Socket listen_socket;
        EXPECT_TRUE(listen_socket.Bind("127.0.0.1", 0).ok());
        EXPECT_TRUE(listen_socket.Listen(1).ok());

        xrpc::io::Socket client_socket;
        EXPECT_TRUE(client_socket.Connect("127.0.0.1", listen_socket.LocalPort().value()).ok());
        xrpc::io::Socket server_socket = listen_socket.Accept().value();

        xrpc::io::UringContext context;
        std::optional<xrpc::runtime::Task<xrpc::io::IoResult>> task;
        task.emplace(PendingRead(context, server_socket.fd()));
        context.Post([&task]() -> void {
          task->Start();
          task.reset();
        });
        context.Run();
      },
      "UringAwaitable destroyed while an I/O operation is pending");
}

TEST(SocketTest, InvalidSocketOperationsReturnFailedPrecondition) {
  xrpc::io::Socket socket;
  std::array<char, 8> buffer{};

  const auto local_port = socket.LocalPort();
  EXPECT_FALSE(local_port.ok());
  EXPECT_EQ(local_port.status().code(), xrpc::StatusCode::FailedPrecondition);

  const xrpc::Status listen_status = socket.Listen(1);
  EXPECT_EQ(listen_status.code(), xrpc::StatusCode::FailedPrecondition);

  const auto accept_result = socket.Accept();
  EXPECT_FALSE(accept_result.ok());
  EXPECT_EQ(accept_result.status().code(), xrpc::StatusCode::FailedPrecondition);

  const auto read_result = socket.Read(buffer.data(), buffer.size());
  EXPECT_FALSE(read_result.ok());
  EXPECT_EQ(read_result.status().code(), xrpc::StatusCode::FailedPrecondition);

  const auto write_result = socket.Write("payload");
  EXPECT_FALSE(write_result.ok());
  EXPECT_EQ(write_result.status().code(), xrpc::StatusCode::FailedPrecondition);

  const xrpc::Status timeout_status = socket.SetReadWriteTimeout(std::chrono::milliseconds(1));
  EXPECT_EQ(timeout_status.code(), xrpc::StatusCode::FailedPrecondition);
}

TEST(IoUringAwaitableTest, SendAndRecvReturnExpectedResults) {
  xrpc::io::Socket listen_socket;
  ASSERT_TRUE(listen_socket.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listen_socket.Listen(1).ok());

  xrpc::io::Socket client_socket;
  ASSERT_TRUE(client_socket.Connect("127.0.0.1", listen_socket.LocalPort().value()).ok());
  xrpc::io::Socket server_socket = listen_socket.Accept().value();

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

TEST(IoUringAwaitableTest, ProvidedRecvReturnsAndRecyclesSelectedBuffer) {
  xrpc::io::Socket listen_socket;
  ASSERT_TRUE(listen_socket.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listen_socket.Listen(1).ok());

  xrpc::io::Socket client_socket;
  ASSERT_TRUE(client_socket.Connect("127.0.0.1", listen_socket.LocalPort().value()).ok());
  xrpc::io::Socket server_socket = listen_socket.Accept().value();
  ASSERT_TRUE(client_socket.WriteAll("abcdefgh").ok());

  const xrpc::io::UringBufferPoolConfig buffer_config{
      .buffer_count_ = 1,
      .buffer_size_ = 4,
      .group_id_ = 7,
  };
  xrpc::io::UringContext context(8, buffer_config);

  const std::string received = WaitTaskWithContext(ReadProvidedTwice(context, server_socket.fd()), context);
  EXPECT_EQ(received, "abcdefgh");
}

TEST(IoUringAwaitableTest, ProvidedRecvErrorDoesNotReturnBuffer) {
  const xrpc::io::UringBufferPoolConfig buffer_config{
      .buffer_count_ = 1,
      .buffer_size_ = 4,
      .group_id_ = 7,
  };
  xrpc::io::UringContext context(8, buffer_config);

  xrpc::io::IoResult result = WaitTaskWithContext(ReadProvidedInvalidFd(context), context);
  EXPECT_EQ(result.type_, xrpc::io::OperationType::RecvProvided);
  EXPECT_NE(result.error_code_, 0);
  EXPECT_LT(result.result_, 0);
  EXPECT_TRUE(result.buffer_.Empty());
}

TEST(IoUringAwaitableTest, ProvidedPoolExhaustionPreservesLeaseAndRecoversAfterRelease) {
  xrpc::io::Socket listen_socket;
  ASSERT_TRUE(listen_socket.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listen_socket.Listen(1).ok());
  xrpc::io::Socket client_socket;
  ASSERT_TRUE(client_socket.Connect("127.0.0.1", listen_socket.LocalPort().value()).ok());
  auto server_socket = listen_socket.Accept().value();
  ASSERT_TRUE(client_socket.WriteAll("abcdefgh").ok());
  client_socket.ShutdownWrite();
  xrpc::io::UringContext context(8, {.buffer_count_ = 1, .buffer_size_ = 4, .group_id_ = 7});
  WaitTaskWithContext(ExhaustAndReusePool(context, server_socket.fd()), context);
}

TEST(IoUringMultishotTest, ReceivesMultipleBuffersAndEof) {
  xrpc::io::Socket listener;
  ASSERT_TRUE(listener.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listener.Listen(1).ok());
  xrpc::io::Socket client;
  ASSERT_TRUE(client.Connect("127.0.0.1", listener.LocalPort().value()).ok());
  auto server = listener.Accept().value();
  ASSERT_TRUE(client.WriteAll("abcdefghijkl").ok());
  client.ShutdownWrite();
  xrpc::io::UringContext context(16, {.buffer_count_ = 16, .buffer_size_ = 4});
  EXPECT_EQ(WaitTaskWithContext(ReadMultishot(context, server.fd()), context), "abcdefghijkl");
}

TEST(IoUringMultishotTest, CancellationDrainsBeforeDestruction) {
  xrpc::io::Socket listener;
  ASSERT_TRUE(listener.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listener.Listen(1).ok());
  xrpc::io::Socket client;
  ASSERT_TRUE(client.Connect("127.0.0.1", listener.LocalPort().value()).ok());
  auto server = listener.Accept().value();
  {
    xrpc::io::UringContext context(16, {.buffer_count_ = 16, .buffer_size_ = 4});
    WaitTaskWithContext(CancelMultishot(context, server.fd()), context);
  }
  ASSERT_TRUE(client.WriteAll("abcdefghijkl").ok());
  {
    xrpc::io::UringContext context(16, {.buffer_count_ = 16, .buffer_size_ = 4});
    WaitTaskWithContext(DrainCancelledMultishot(context, server.fd()), context);
  }
}

TEST(IoUringMultishotTest, CancellationWithoutDrainDoesNotPermitDestruction) {
  EXPECT_DEATH(
      {
        xrpc::io::Socket listener;
        EXPECT_TRUE(listener.Bind("127.0.0.1", 0).ok());
        EXPECT_TRUE(listener.Listen(1).ok());
        xrpc::io::Socket client;
        EXPECT_TRUE(client.Connect("127.0.0.1", listener.LocalPort().value()).ok());
        auto server = listener.Accept().value();
        EXPECT_TRUE(client.WriteAll("abcd").ok());
        xrpc::io::UringContext context(16, {.buffer_count_ = 16, .buffer_size_ = 4});
        WaitTaskWithContext(DestroyUndrainedMultishot(context, server.fd()), context);
      },
      "UringAwaitable destroyed while an I/O operation is pending");
}

TEST(IoUringMultishotTest, PoolExhaustionIsTerminalAndLeaseCanBeReused) {
  xrpc::io::Socket listener;
  ASSERT_TRUE(listener.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listener.Listen(1).ok());
  xrpc::io::Socket client;
  ASSERT_TRUE(client.Connect("127.0.0.1", listener.LocalPort().value()).ok());
  auto server = listener.Accept().value();
  ASSERT_TRUE(client.WriteAll("abcdefgh").ok());
  xrpc::io::UringContext context(8, {.buffer_count_ = 1, .buffer_size_ = 4, .group_id_ = 7});
  WaitTaskWithContext(ExhaustMultishot(context, server.fd()), context);
}

TEST(IoUringMultishotTest, InvalidFdAndCancellationBeforeAdmission) {
  xrpc::io::UringContext no_pool;
  EXPECT_THROW((void)no_pool.RecvProvidedMultishot(-1), xrpc::LifecycleException);
  xrpc::io::UringContext context(8, {.buffer_count_ = 8, .buffer_size_ = 4});
  WaitTaskWithContext(CheckMultishotErrors(context), context);
}
