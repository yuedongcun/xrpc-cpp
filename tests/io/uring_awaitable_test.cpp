#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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

  const auto held = context.SnapshotStats(true);
  EXPECT_EQ(held.buffer_pool_->capacity_, 1U);
  EXPECT_EQ(held.buffer_pool_->outstanding_leases_, 1U);
  EXPECT_EQ(held.buffer_pool_->outstanding_leases_peak_, 1U);
  EXPECT_EQ(held.buffer_pool_->acquires_, 1U);
  EXPECT_EQ(held.buffer_pool_->returns_, 0U);

  // The only buffer remains leased, so the next receive must fail rather than overwrite it.
  auto exhausted = co_await context.RecvProvided(fd);
  EXPECT_EQ(exhausted.error_code_, ENOBUFS);
  EXPECT_EQ(exhausted.buffer_group_, 7);
  EXPECT_TRUE(exhausted.buffer_.Empty());
  EXPECT_EQ(context.SnapshotStats().counters_.provided_buffer_enobufs_, 1U);
  const auto bytes = held_buffer.Bytes();
  EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(bytes.data()), bytes.size()), "abcd");

  held_buffer.Reset();
  const auto reset = context.SnapshotStats(true);
  EXPECT_EQ(reset.window_id_, held.window_id_ + 1);
  EXPECT_EQ(reset.buffer_pool_->outstanding_leases_, 0U);
  EXPECT_EQ(reset.buffer_pool_->outstanding_leases_peak_, 0U);
  EXPECT_EQ(reset.buffer_pool_->acquires_, 1U);  // Resetting peaks preserves counters.
  EXPECT_EQ(reset.buffer_pool_->returns_, 1U);
  EXPECT_EQ(reset.peaks_.staged_operations_, reset.staged_operations_);
  EXPECT_EQ(reset.peaks_.cq_ready_sampled_, reset.cq_ready_);
  auto next = co_await context.RecvProvided(fd);
  EXPECT_EQ(next.result_, 4);
  EXPECT_EQ(context.SnapshotStats().buffer_pool_->outstanding_leases_peak_, 1U);
  const auto next_bytes = next.buffer_.Bytes();
  EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(next_bytes.data()), next_bytes.size()), "efgh");
  next.buffer_.Reset();
  auto eof = co_await context.RecvProvided(fd);
  EXPECT_EQ(eof.result_, 0);
  EXPECT_TRUE(eof.buffer_.Empty());
}

auto AcceptMultishotTwiceAndCancel(xrpc::io::UringContext &context, int listen_fd) -> xrpc::runtime::Task<void> {
  const auto before = context.SnapshotStats();
  auto accept = context.AcceptMultishot(listen_fd);
  for (int accepted = 0; accepted < 2; ++accepted) {
    const xrpc::io::IoResult result = co_await accept;
    EXPECT_EQ(result.type_, xrpc::io::OperationType::Accept);
    EXPECT_EQ(result.error_code_, 0);
    EXPECT_GE(result.result_, 0);
    EXPECT_TRUE(result.has_more_);
    xrpc::io::Socket client_socket(result.result_);
  }

  context.CancelFd(listen_fd);
  const xrpc::io::IoResult cancelled = co_await accept;
  EXPECT_EQ(cancelled.error_code_, ECANCELED);
  EXPECT_FALSE(cancelled.has_more_);
  EXPECT_EQ(context.SnapshotStats().counters_.prepared_accept_sqes_ - before.counters_.prepared_accept_sqes_, 1U);
}

auto ExhaustProvidedRecv(xrpc::io::UringContext &context, int fd, bool return_after_wait = false)
    -> xrpc::runtime::Task<void> {
  auto first = co_await context.RecvProvided(fd);
  EXPECT_EQ(first.result_, 4);
  EXPECT_FALSE(first.has_more_);
  auto exhausted = co_await context.RecvProvided(fd);
  EXPECT_EQ(exhausted.error_code_, ENOBUFS);
  EXPECT_FALSE(exhausted.has_more_);
  const auto pressure = context.SnapshotStats();
  EXPECT_EQ(pressure.counters_.provided_buffer_enobufs_, 1U);
  EXPECT_EQ(pressure.active_recv_requests_, 0U);
  EXPECT_EQ(pressure.counters_.prepared_recv_sqes_, 2U);
  EXPECT_TRUE(exhausted.buffer_.Empty());
  EXPECT_EQ(pressure.buffer_pool_->outstanding_leases_, 1U);
  EXPECT_EQ(pressure.buffer_pool_->outstanding_leases_peak_, 1U);
  EXPECT_EQ(exhausted.buffer_group_, 7);
  const auto bytes = first.buffer_.Bytes();
  EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(bytes.data()), bytes.size()), "abcd");
  EXPECT_EQ(exhausted.buffer_returns_at_start_, 0U);
  if (return_after_wait) {
    auto held = std::make_shared<xrpc::io::UringBuffer>(std::move(first.buffer_));
    context.Post([held]() -> void { held->Reset(); });
  } else {
    first.buffer_.Reset();
  }
  EXPECT_EQ(co_await context.WaitForBufferReturnSince(fd, exhausted.buffer_returns_at_start_),
            xrpc::io::BufferReturnWaitOutcome::ReturnObserved);
  const auto released = context.SnapshotStats();
  EXPECT_EQ(released.buffer_pool_->outstanding_leases_, 0U);
  EXPECT_EQ(released.buffer_pool_->acquires_, released.buffer_pool_->returns_);
  EXPECT_EQ(released.counters_.buffer_return_waits_, 1U);
  EXPECT_EQ(released.counters_.buffer_return_wait_suspensions_, return_after_wait ? 1U : 0U);
  EXPECT_EQ(released.counters_.buffer_return_waits_completed_, 1U);
  EXPECT_EQ(released.counters_.buffer_return_waits_cancelled_, 0U);
  EXPECT_EQ(released.buffer_return_waiters_, 0U);
  EXPECT_EQ(released.peaks_.buffer_return_waiters_, return_after_wait ? 1U : 0U);

  auto remaining = co_await context.RecvProvided(fd);
  EXPECT_EQ(remaining.result_, 4);
  EXPECT_EQ(remaining.buffer_returns_at_start_, 1U);
  const auto next_bytes = remaining.buffer_.Bytes();
  EXPECT_EQ(std::string_view(reinterpret_cast<const char *>(next_bytes.data()), next_bytes.size()), "efgh");
  remaining.buffer_.Reset();

  const auto recovered = context.SnapshotStats();
  EXPECT_EQ(recovered.counters_.prepared_recv_sqes_, 3U);
  EXPECT_EQ(recovered.counters_.received_bytes_, 8U);
  EXPECT_EQ(recovered.active_recv_requests_, 0U);
  EXPECT_EQ(recovered.buffer_pool_->acquires_, 2U);
  EXPECT_EQ(recovered.buffer_pool_->returns_, 2U);
  EXPECT_EQ(recovered.buffer_pool_->outstanding_leases_, 0U);
}

auto WaitForReturn(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<xrpc::io::BufferReturnWaitOutcome> {
  co_return co_await context.WaitForBufferReturnSince(fd, 0);
}

auto CheckWaitCancellation(xrpc::io::UringContext &context) -> xrpc::runtime::Task<void> {
  auto first = WaitForReturn(context, 100);
  auto second = WaitForReturn(context, 101);
  first.Start();
  second.Start();
  EXPECT_FALSE(first.Done());
  EXPECT_FALSE(second.Done());
  const auto start = context.SnapshotStats(true);
  EXPECT_EQ(start.buffer_return_waiters_, 2U);
  EXPECT_EQ(start.peaks_.buffer_return_waiters_, 2U);
  context.CancelBufferReturnWaitsForFd(100);
  EXPECT_TRUE(first.Done());
  EXPECT_EQ(first.Result(), xrpc::io::BufferReturnWaitOutcome::Cancelled);
  EXPECT_FALSE(second.Done());
  context.Post([&context]() -> void { context.RequestStop(); });
  EXPECT_EQ(co_await context.WaitForBufferReturnSince(102, 0), xrpc::io::BufferReturnWaitOutcome::Cancelled);
  EXPECT_TRUE(second.Done());
  EXPECT_EQ(second.Result(), xrpc::io::BufferReturnWaitOutcome::Cancelled);
  EXPECT_EQ(co_await context.WaitForBufferReturnSince(103, 0), xrpc::io::BufferReturnWaitOutcome::Cancelled);
  const auto stopped = context.SnapshotStats();
  EXPECT_EQ(stopped.counters_.buffer_return_waits_, 4U);
  EXPECT_EQ(stopped.counters_.buffer_return_wait_suspensions_, 3U);
  EXPECT_EQ(stopped.counters_.buffer_return_waits_completed_, 0U);
  EXPECT_EQ(stopped.counters_.buffer_return_waits_cancelled_, 4U);
  EXPECT_EQ(stopped.buffer_return_waiters_, 0U);
  EXPECT_EQ(stopped.peaks_.buffer_return_waiters_, 2U);
}

auto CheckFdWaitCancellation(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<void> {
  context.Post([&context, fd]() -> void { context.CancelFd(fd); });
  EXPECT_EQ(co_await context.WaitForBufferReturnSince(fd, 0), xrpc::io::BufferReturnWaitOutcome::Cancelled);
}

auto WakeManyReturnWaiters(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<void> {
  auto received = co_await context.RecvProvided(fd);
  EXPECT_EQ(received.result_, 4);
  std::vector<xrpc::runtime::Task<xrpc::io::BufferReturnWaitOutcome>> waiters;
  for (int i = 0; i < 32; ++i) {
    waiters.push_back(WaitForReturn(context, i + 100));
    waiters.back().Start();
  }
  auto held = std::make_shared<xrpc::io::UringBuffer>(std::move(received.buffer_));
  context.Post([held]() -> void { held->Reset(); });
  EXPECT_EQ(co_await context.WaitForBufferReturnSince(fd, 0), xrpc::io::BufferReturnWaitOutcome::ReturnObserved);
  for (auto &waiter : waiters) {
    EXPECT_TRUE(waiter.Done());
    EXPECT_EQ(waiter.Result(), xrpc::io::BufferReturnWaitOutcome::ReturnObserved);
  }
  const auto completed = context.SnapshotStats();
  EXPECT_EQ(completed.buffer_pool_->outstanding_leases_, 0U);
  EXPECT_EQ(completed.counters_.buffer_return_waits_, 33U);
  EXPECT_EQ(completed.counters_.buffer_return_wait_suspensions_, 33U);
  EXPECT_EQ(completed.counters_.buffer_return_waits_completed_, 33U);
  EXPECT_EQ(completed.buffer_return_waiters_, 0U);
  EXPECT_EQ(completed.peaks_.buffer_return_waiters_, 33U);
  const auto reset = context.SnapshotStats(true);
  EXPECT_EQ(reset.peaks_.buffer_return_waiters_, 0U);
  EXPECT_EQ(reset.counters_.buffer_return_waits_completed_, 33U);
}

auto CheckProvidedRecvErrors(xrpc::io::UringContext &context) -> xrpc::runtime::Task<void> {
  auto invalid = context.RecvProvided(-1);
  auto error = co_await invalid;
  EXPECT_EQ(error.error_code_, EBADF);
  EXPECT_FALSE(error.has_more_);
  auto stopped = context.RecvProvided(-1);
  context.RequestStop();
  auto after_stop = co_await stopped;
  EXPECT_EQ(after_stop.error_code_, ECANCELED);
  EXPECT_FALSE(after_stop.has_more_);
}

auto CheckUnsubmittedStats(xrpc::io::UringContext &context) -> xrpc::runtime::Task<void> {
  const auto before = context.SnapshotStats();
  auto unawaited = context.RecvProvided(-1);
  EXPECT_EQ(context.SnapshotStats().counters_.prepared_recv_sqes_, before.counters_.prepared_recv_sqes_);
  context.RequestStop();
  auto rejected = co_await context.RecvProvided(-1);
  EXPECT_EQ(rejected.error_code_, ECANCELED);
  const auto after = context.SnapshotStats();
  EXPECT_EQ(after.counters_.prepared_recv_sqes_, before.counters_.prepared_recv_sqes_);
  EXPECT_EQ(after.counters_.recv_cqes_, before.counters_.recv_cqes_);
  EXPECT_EQ(after.active_recv_requests_, 0U);
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

TEST(IoUringMultishotTest, AcceptsMultipleConnectionsAndDrainsCancellation) {
  xrpc::io::Socket listener;
  ASSERT_TRUE(listener.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listener.Listen(2).ok());
  xrpc::io::Socket first_client;
  xrpc::io::Socket second_client;
  ASSERT_TRUE(first_client.Connect("127.0.0.1", listener.LocalPort().value()).ok());
  ASSERT_TRUE(second_client.Connect("127.0.0.1", listener.LocalPort().value()).ok());

  xrpc::io::UringContext context(16);
  WaitTaskWithContext(AcceptMultishotTwiceAndCancel(context, listener.fd()), context);
}

TEST(IoUringAwaitableTest, ProvidedRecvWaitObservesReturnBeforeSuspension) {
  xrpc::io::Socket listener;
  ASSERT_TRUE(listener.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listener.Listen(1).ok());
  xrpc::io::Socket client;
  ASSERT_TRUE(client.Connect("127.0.0.1", listener.LocalPort().value()).ok());
  auto server = listener.Accept().value();
  ASSERT_TRUE(client.WriteAll("abcdefgh").ok());
  xrpc::io::UringContext context(8, {.buffer_count_ = 1, .buffer_size_ = 4, .group_id_ = 7});
  WaitTaskWithContext(ExhaustProvidedRecv(context, server.fd()), context);
}

TEST(IoUringAwaitableTest, ProvidedRecvWaitResumesAfterDeferredBufferReturn) {
  xrpc::io::Socket listener;
  ASSERT_TRUE(listener.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listener.Listen(1).ok());
  xrpc::io::Socket client;
  ASSERT_TRUE(client.Connect("127.0.0.1", listener.LocalPort().value()).ok());
  auto server = listener.Accept().value();
  ASSERT_TRUE(client.WriteAll("abcdefgh").ok());
  xrpc::io::UringContext context(8, {.buffer_count_ = 1, .buffer_size_ = 4, .group_id_ = 7});
  WaitTaskWithContext(ExhaustProvidedRecv(context, server.fd(), true), context);
}

TEST(IoUringBufferReturnWaitTest, FdCancellationIsIsolatedAndStopCancelsRemainingWaiters) {
  xrpc::io::UringContext context(8, {.buffer_count_ = 1, .buffer_size_ = 4});
  WaitTaskWithContext(CheckWaitCancellation(context), context);
}

TEST(IoUringBufferReturnWaitTest, CancelFdAlsoCancelsUserSpaceBufferWait) {
  xrpc::io::Socket listener;
  ASSERT_TRUE(listener.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listener.Listen(1).ok());
  xrpc::io::Socket client;
  ASSERT_TRUE(client.Connect("127.0.0.1", listener.LocalPort().value()).ok());
  auto server = listener.Accept().value();
  xrpc::io::UringContext context(8, {.buffer_count_ = 1, .buffer_size_ = 4});
  WaitTaskWithContext(CheckFdWaitCancellation(context, server.fd()), context);
}

TEST(IoUringBufferReturnWaitTest, OneReturnResumesMoreWaitersThanThePerTurnBudget) {
  xrpc::io::Socket listener;
  ASSERT_TRUE(listener.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listener.Listen(1).ok());
  xrpc::io::Socket client;
  ASSERT_TRUE(client.Connect("127.0.0.1", listener.LocalPort().value()).ok());
  auto server = listener.Accept().value();
  ASSERT_TRUE(client.WriteAll("abcd").ok());
  xrpc::io::UringContext context(8, {.buffer_count_ = 1, .buffer_size_ = 4});
  WaitTaskWithContext(WakeManyReturnWaiters(context, server.fd()), context);
}

TEST(IoUringAwaitableTest, ProvidedRecvInvalidFdAndCancellationBeforeAdmission) {
  xrpc::io::UringContext no_pool;
  EXPECT_THROW((void)no_pool.RecvProvided(-1), xrpc::LifecycleException);
  xrpc::io::UringContext context(8, {.buffer_count_ = 8, .buffer_size_ = 4});
  WaitTaskWithContext(CheckProvidedRecvErrors(context), context);
}

TEST(IoUringStatsTest, UnawaitedAndRejectedReceivesDoNotCountAsPreparedSqes) {
  xrpc::io::UringContext context(8, {.buffer_count_ = 8, .buffer_size_ = 4});
  WaitTaskWithContext(CheckUnsubmittedStats(context), context);
}

TEST(IoUringMultishotTest, WakeupPollSurvivesRepeatedPostsAndStops) {
  xrpc::io::UringContext context(16);
  std::jthread thread([&context]() -> void { context.Run(); });
  // Each acknowledged callback precedes the next Post. This exercises wakeups
  // across turns rather than only draining a single preloaded callback queue.
  for (int round = 0; round < 32; ++round) {
    auto promise = std::make_shared<std::promise<std::uint64_t>>();
    auto future = promise->get_future();
    context.Post(
        [&context, promise]() -> void { promise->set_value(context.SnapshotStats().counters_.prepared_wakeup_sqes_); });
    const auto ready = future.wait_for(WaitTimeout);
    EXPECT_EQ(ready, std::future_status::ready);
    if (ready != std::future_status::ready) {
      break;
    }
    EXPECT_EQ(future.get(), 1U);
  }
  context.RequestStop();
  thread.join();
}

auto CancelAcceptWithQueuedConnections(xrpc::io::UringContext &context, int fd) -> xrpc::runtime::Task<void> {
  auto accept = context.AcceptMultishot(fd);
  auto result = co_await accept;
  EXPECT_GE(result.result_, 0);
  EXPECT_TRUE(result.has_more_);
  xrpc::io::Socket first(result.result_);
  context.CancelFd(fd);
  // Cancellation can race with successful completions already in the CQ.
  // Every returned descriptor is owned even though admission is now closed.
  while (result.has_more_) {
    result = co_await accept;
    if (result.result_ >= 0) {
      xrpc::io::Socket late_connection(result.result_);
    } else {
      EXPECT_EQ(result.error_code_, ECANCELED);
      EXPECT_FALSE(result.has_more_);
    }
  }
  EXPECT_EQ(result.error_code_, ECANCELED);
}

TEST(IoUringMultishotTest, AcceptCancellationDrainsQueuedConnections) {
  xrpc::io::Socket listener;
  ASSERT_TRUE(listener.Bind("127.0.0.1", 0).ok());
  ASSERT_TRUE(listener.Listen(16).ok());
  std::array<xrpc::io::Socket, 16> clients;
  for (auto &client : clients) {
    ASSERT_TRUE(client.Connect("127.0.0.1", listener.LocalPort().value()).ok());
  }
  xrpc::io::UringContext context(32);
  WaitTaskWithContext(CancelAcceptWithQueuedConnections(context, listener.fd()), context);
}
