#include "server/connection_io_loop.h"

#include <utility>

#include "server/service_registry.h"

namespace xrpc {
/**
 * @brief Creates one event loop that owns accepted TCP connections.
 *
 * @param registry Registered RPC methods shared by all connections.
 * @param executor Worker pool used for handler execution.
 * @param limits Per-connection backpressure limits.
 * @param protocol_limits Frame and payload limits.
 * @param idle_timeout Idle timeout applied to new connections.
 */
ConnectionIoLoop::ConnectionIoLoop(ServiceRegistry &registry, ThreadPoolExecutor &executor,
                                   ConnectionBackpressureLimits limits, ProtocolLimits protocol_limits,
                                   std::chrono::milliseconds idle_timeout)
    : dispatch_mailbox_(std::make_shared<DispatchMailbox>(context_)),
      registry_(&registry),
      executor_(&executor),
      limits_(limits),
      protocol_limits_(protocol_limits),
      idle_timeout_(idle_timeout) {}

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
  dispatch_mailbox_->Disable();
  started_ = false;
}

/** @brief Stops connection reads without stopping the event-loop runtime. */
void ConnectionIoLoop::BeginDrain() {
  {
    std::unique_lock lock(drain_mutex_);
    if (!started_) {
      drain_requested_ = true;
      drain_ready_ = true;
      return;
    }
    if (drain_requested_) {
      drain_cv_.wait(lock, [this] { return drain_ready_; });
      return;
    }
    drain_requested_ = true;
  }

  context_.Post([this]() { BeginDrainOnContext(); });
  std::unique_lock lock(drain_mutex_);
  drain_cv_.wait(lock, [this] { return drain_ready_; });
}

/** @brief Waits for admitted work and response writes before stopping the loop. */
void ConnectionIoLoop::FinishDrain() {
  BeginDrain();
  {
    std::unique_lock lock(drain_mutex_);
    drain_cv_.wait(lock, [this] { return live_connections_ == 0; });
  }

  if (!started_) {
    return;
  }
  context_.Stop();
  if (thread_.joinable()) {
    thread_.join();
  }
  dispatch_mailbox_->Disable();
  connections_.clear();
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

/**
 * @brief Creates a `ServerConnection` actor and starts its coroutine task.
 *
 * @param client_socket Accepted socket already moved onto this loop thread.
 */
void ConnectionIoLoop::StartConnection(io::Socket client_socket) {
  {
    std::lock_guard lock(drain_mutex_);
    if (drain_requested_) {
      client_socket.Close();
      return;
    }
    ++live_connections_;
  }

  ServerConnectionOptions options;
  options.limits_ = limits_;
  options.dispatch_mailbox_ = dispatch_mailbox_;
  options.protocol_limits_ = protocol_limits_;
  options.idle_timeout_ = idle_timeout_;
  options.on_closed_ = [this]() { OnConnectionClosed(); };
  std::shared_ptr<ServerConnection> connection;
  try {
    connection = std::make_shared<ServerConnection>(context_, *registry_, *executor_, std::move(client_socket),
                                                    std::move(options));
  } catch (...) {
    OnConnectionClosed();
    throw;
  }
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
      entry.connection_->Close(ServerConnectionCloseReason::SocketError);
    }
  }
  CleanupClosedConnections();
}

/** @brief Marks all live connections as draining on the owning event-loop thread. */
void ConnectionIoLoop::BeginDrainOnContext() {
  for (auto &entry : connections_) {
    entry.connection_->BeginDrain();
  }
  {
    std::lock_guard lock(drain_mutex_);
    drain_ready_ = true;
  }
  drain_cv_.notify_all();
}

/** @brief Releases one live connection from graceful-drain accounting. */
void ConnectionIoLoop::OnConnectionClosed() {
  {
    std::lock_guard lock(drain_mutex_);
    if (live_connections_ > 0) {
      --live_connections_;
    }
  }
  drain_cv_.notify_all();
}

}  // namespace xrpc
