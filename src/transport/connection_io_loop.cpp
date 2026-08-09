#include "transport/connection_io_loop.h"

#include <algorithm>
#include <utility>

#include "rpc/xrpc_exception.h"

namespace xrpc {
namespace {

/**
 * @brief Accumulates post diagnostics from one connection event loop.
 *
 * @param target Aggregate snapshot to update.
 * @param source Snapshot reported by one `UringContext`.
 */
void AddPostStats(io::UringPostStatsSnapshot &target, const io::UringPostStatsSnapshot &source) {
  target.posted_callbacks_ += source.posted_callbacks_;
  target.drained_callbacks_ += source.drained_callbacks_;
  target.drain_batches_ += source.drain_batches_;
  target.max_observed_post_queue_depth_ =
      std::max(target.max_observed_post_queue_depth_, source.max_observed_post_queue_depth_);
}

}  // namespace

/**
 * @brief Creates one event loop that owns accepted TCP connections.
 *
 * @param handler Raw RPC dispatcher shared by all connections.
 * @param executor Worker pool used for handler execution.
 * @param limits Per-connection and global backpressure limits.
 * @param backpressure_stats Shared backpressure diagnostics.
 * @param io_stats Shared connection I/O diagnostics.
 * @param protocol_limits Frame and payload limits.
 * @param connection_idle_timeout Idle timeout applied to new connections.
 */
ConnectionIoLoop::ConnectionIoLoop(RawHandler handler, ThreadPoolExecutor &executor, ServerBackpressureLimits limits,
                                   ServerBackpressureStats &backpressure_stats, ServerIoStats &io_stats,
                                   ProtocolLimits protocol_limits, std::chrono::milliseconds connection_idle_timeout)
    : completion_queue_(std::make_shared<DispatchCompletionQueue>(context_)),
      handler_(std::move(handler)),
      executor_(&executor),
      limits_(limits),
      protocol_limits_(protocol_limits),
      connection_idle_timeout_(connection_idle_timeout),
      backpressure_stats_(&backpressure_stats),
      io_stats_(&io_stats) {}

/** @brief Stops the loop thread and closes owned connections. */
ConnectionIoLoop::~ConnectionIoLoop() { Stop(); }

/**
 * @brief Starts the io_uring event loop on its dedicated thread.
 *
 * Exceptions escaping `UringContext::Run()` are saved and rethrown by `RethrowIfFailed()`.
 */
void ConnectionIoLoop::Start() {
  if (started_) {
    return;
  }

  started_ = true;
  thread_ = std::jthread([this]() {
    try {
      context_.Run();
    } catch (...) {
      error_ = std::current_exception();
    }
  });
}

/**
 * @brief Stops the event loop after closing live connections on the loop thread.
 *
 * Closing connections through `Post()` lets each connection cancel its own in-flight operations
 * before the underlying context stops processing completions.
 */
void ConnectionIoLoop::Stop() noexcept {
  if (!started_) {
    return;
  }

  thread_.request_stop();
  // Close live connections on the loop thread before stopping the context so
  // each connection can cancel its own in-flight io_uring operations.
  context_.Post([this]() { CloseConnections(); });
  context_.Stop();
  if (thread_.joinable()) {
    thread_.join();
  }
  completion_queue_->Disable();
  started_ = false;
}

/**
 * @brief Schedules a newly accepted socket to start on this loop thread.
 *
 * @param client_socket Accepted socket moved from the accept loop.
 */
void ConnectionIoLoop::PostStartConnection(io::Socket client_socket) {
  // Post requires a copyable callable. The socket is move-only, so wrap it in a
  // shared holder until the loop thread consumes it.
  auto socket_holder = std::make_shared<io::Socket>(std::move(client_socket));
  context_.Post([this, socket_holder]() {
    CleanupClosedConnections();
    StartConnection(std::move(*socket_holder));
  });
}

/** @brief Rethrows any exception captured from the loop thread. */
void ConnectionIoLoop::RethrowIfFailed() const {
  if (error_) {
    std::rethrow_exception(error_);
  }
}

/** @return Number of live connections that have not reached closed state. */
auto ConnectionIoLoop::ConnectionCount() const -> std::size_t {
  return static_cast<std::size_t>(std::count_if(connections_.begin(), connections_.end(),
                                                [](const auto &entry) { return !entry.connection_->IsClosed(); }));
}

/**
 * @brief Creates a `TcpConnection` actor and starts its coroutine task.
 *
 * @param client_socket Accepted socket already moved onto this loop thread.
 */
void ConnectionIoLoop::StartConnection(io::Socket client_socket) {
  TcpConnectionOptions options;
  options.limits_ = limits_;
  options.backpressure_stats_ = backpressure_stats_;
  options.io_stats_ = io_stats_;
  options.completion_queue_ = completion_queue_;
  options.protocol_limits_ = protocol_limits_;
  options.idle_timeout_ = connection_idle_timeout_;
  auto connection =
      std::make_shared<TcpConnection>(context_, handler_, *executor_, std::move(client_socket), std::move(options));
  ConnectionEntry entry{.connection_ = connection, .task_ = connection->Run()};
  entry.task_.Start();
  connections_.push_back(std::move(entry));
}

/** @brief Removes closed connections whose coroutine tasks have completed. */
void ConnectionIoLoop::CleanupClosedConnections() {
  std::erase_if(connections_,
                [](const ConnectionEntry &entry) { return entry.connection_->IsClosed() && entry.task_.Done(); });
}

/**
 * @brief Requests close on every live connection and removes completed entries.
 */
void ConnectionIoLoop::CloseConnections() {
  for (auto &entry : connections_) {
    if (!entry.connection_->IsClosed()) {
      entry.connection_->Close(ConnectionCloseReason::SocketError);
    }
  }
  CleanupClosedConnections();
}

/**
 * @brief Creates a round-robin group of connection event loops.
 *
 * @param loop_count Number of event-loop threads to own.
 * @param handler Raw RPC dispatcher shared by all loops.
 * @param executor Worker pool used by all connections.
 * @param limits Per-connection and global backpressure limits.
 * @param backpressure_stats Shared backpressure diagnostics.
 * @param io_stats Shared connection I/O diagnostics.
 * @param protocol_limits Frame and payload limits.
 * @param connection_idle_timeout Idle timeout applied to new connections.
 * @throws ConfigException when `loop_count` is zero.
 */
ConnectionIoLoopGroup::ConnectionIoLoopGroup(std::size_t loop_count, const RawHandler &handler,
                                             ThreadPoolExecutor &executor, ServerBackpressureLimits limits,
                                             ServerBackpressureStats &backpressure_stats, ServerIoStats &io_stats,
                                             ProtocolLimits protocol_limits,
                                             std::chrono::milliseconds connection_idle_timeout) {
  if (loop_count == 0) {
    throw ConfigException("server io loop count must be greater than 0");
  }

  loops_.reserve(loop_count);
  for (std::size_t index = 0; index < loop_count; ++index) {
    loops_.push_back(std::make_unique<ConnectionIoLoop>(handler, executor, limits, backpressure_stats, io_stats,
                                                        protocol_limits, connection_idle_timeout));
  }
}

/** @brief Stops all connection event loops. */
ConnectionIoLoopGroup::~ConnectionIoLoopGroup() { Stop(); }

/** @brief Starts every owned connection event loop. */
void ConnectionIoLoopGroup::Start() {
  for (auto &loop : loops_) {
    loop->Start();
  }
}

/** @brief Requests every connection event loop to stop. */
void ConnectionIoLoopGroup::Stop() noexcept {
  for (auto &loop : loops_) {
    loop->Stop();
  }
}

/**
 * @brief Dispatches an accepted socket to the next connection loop.
 *
 * @param client_socket Accepted socket from the TCP server accept loop.
 */
void ConnectionIoLoopGroup::Dispatch(io::Socket client_socket) {
  if (loops_.empty()) {
    return;
  }

  loops_[next_loop_index_]->PostStartConnection(std::move(client_socket));
  next_loop_index_ = (next_loop_index_ + 1) % loops_.size();
}

/** @brief Rethrows the first stored exception from any connection loop. */
void ConnectionIoLoopGroup::RethrowIfFailed() const {
  for (const auto &loop : loops_) {
    loop->RethrowIfFailed();
  }
}

/** @return Total number of live connections across all connection loops. */
auto ConnectionIoLoopGroup::ConnectionCount() const -> std::size_t {
  std::size_t count = 0;
  for (const auto &loop : loops_) {
    count += loop->ConnectionCount();
  }
  return count;
}

/** @return Aggregate cross-thread post diagnostics from all connection loops. */
auto ConnectionIoLoopGroup::post_stats() const -> io::UringPostStatsSnapshot {
  io::UringPostStatsSnapshot result;
  for (const auto &loop : loops_) {
    AddPostStats(result, loop->post_stats());
  }
  return result;
}

}  // namespace xrpc
