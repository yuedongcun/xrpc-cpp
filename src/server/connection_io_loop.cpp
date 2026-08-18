/**
 * @file connection_io_loop.cpp
 * @brief Implements connection admission, draining, and I/O-thread coordination.
 *
 * Connection creation, connection state changes, and connection cleanup run
 * on the `UringContext` thread. Lifecycle state and the live-connection count
 * are synchronized across threads, while cross-thread control requests are
 * forwarded to the context with `Post()`.
 */

#include "server/connection_io_loop.h"

#include <utility>

#include "server/service_registry.h"

namespace xrpc {

ConnectionIoLoop::ConnectionIoLoop(ServiceRegistry &registry, ThreadPoolExecutor &executor,
                                   ConnectionBackpressureLimits limits, ProtocolLimits protocol_limits)
    : dispatch_mailbox_(std::make_shared<DispatchMailbox>(context_)),
      registry_(&registry),
      executor_(&executor),
      limits_(limits),
      protocol_limits_(protocol_limits) {}

ConnectionIoLoop::~ConnectionIoLoop() { StopImmediately(); }

void ConnectionIoLoop::Start() {
  std::lock_guard lock(drain_mutex_);
  if (state_ != State::Created) {
    return;
  }
  state_ = State::Running;
  try {
    thread_ = std::jthread([this]() -> void {
      try {
        context_.Run();
      } catch (...) {
        error_ = std::current_exception();
      }
    });
  } catch (...) {
    state_ = State::Stopped;
    drain_cv_.notify_all();
    throw;
  }
}

/**
 * @brief Performs best-effort immediate shutdown without graceful draining.
 */
void ConnectionIoLoop::StopImmediately() noexcept {
  {
    std::lock_guard lock(drain_mutex_);
    if (state_ == State::Created || state_ == State::Stopped) {
      state_ = State::Stopped;
      drain_cv_.notify_all();
      return;
    }
  }

  if (!thread_.joinable()) {
    std::lock_guard lock(drain_mutex_);
    state_ = State::Stopped;
    drain_cv_.notify_all();
    return;
  }

  thread_.request_stop();

  context_.Post([this]() -> void { CloseConnectionsOnContext(); });
  context_.Stop();
  if (thread_.joinable()) {
    thread_.join();
  }
  dispatch_mailbox_->Disable();
  {
    std::lock_guard lock(drain_mutex_);
    state_ = State::Stopped;
  }
  drain_cv_.notify_all();
}

void ConnectionIoLoop::BeginDrain() {
  {
    std::lock_guard lock(drain_mutex_);
    if (state_ == State::Created) {
      state_ = State::Stopped;
      drain_cv_.notify_all();
      return;
    }
    if (state_ != State::Running) {
      return;
    }
    state_ = State::Draining;
  }

  context_.Post([this]() -> void { BeginDrainOnContext(); });
}

void ConnectionIoLoop::FinishDrain() {
  BeginDrain();
  {
    std::unique_lock lock(drain_mutex_);
    if (state_ == State::Stopped) {
      return;
    }
    drain_cv_.wait(lock, [this]() -> bool { return live_connections_ == 0; });
  }
  context_.Stop();

  if (thread_.joinable()) {
    thread_.join();
  }

  dispatch_mailbox_->Disable();
  connections_.clear();
  {
    std::lock_guard lock(drain_mutex_);
    state_ = State::Stopped;
  }
  drain_cv_.notify_all();
  if (error_) {
    std::rethrow_exception(error_);
  }
}

void ConnectionIoLoop::PostStartConnection(io::Socket client_socket) {
  {
    std::lock_guard lock(drain_mutex_);
    if (state_ != State::Running) {
      client_socket.Close();
      return;
    }
  }

  auto socket_holder = std::make_shared<io::Socket>(std::move(client_socket));
  context_.Post([this, socket_holder]() -> void {
    CollectClosedConnections();
    StartConnectionOnContext(std::move(*socket_holder));
  });
}

void ConnectionIoLoop::StartConnectionOnContext(io::Socket client_socket) {
  {
    std::lock_guard lock(drain_mutex_);
    if (state_ != State::Running) {
      client_socket.Close();
      return;
    }
    ++live_connections_;
  }

  const ServerConnectionConfig config{.limits_ = limits_, .protocol_limits_ = protocol_limits_};
  std::shared_ptr<ServerConnection> connection;
  try {
    connection = std::make_shared<ServerConnection>(context_, *registry_, *executor_, *dispatch_mailbox_,
                                                    std::move(client_socket), config,
                                                    [this]() -> void { OnConnectionClosed(); });
  } catch (...) {
    OnConnectionClosed();
    throw;
  }
  ConnectionEntry entry{.connection_ = connection, .task_ = connection->Run()};
  entry.task_.Start();
  connections_.push_back(std::move(entry));
}

void ConnectionIoLoop::CollectClosedConnections() {
  std::erase_if(connections_, [](const ConnectionEntry &entry) -> bool {
    return entry.connection_->IsClosed() && entry.task_.Done();
  });
}

void ConnectionIoLoop::CloseConnectionsOnContext() {
  for (auto &entry : connections_) {
    if (!entry.connection_->IsClosed()) {
      entry.connection_->Close();
    }
  }
  CollectClosedConnections();
}

void ConnectionIoLoop::BeginDrainOnContext() {
  for (auto &entry : connections_) {
    entry.connection_->BeginDrain();
  }
}

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
