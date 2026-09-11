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

#include <exception>
#include <utility>

#include "common/abort.h"

#include "common/xrpc_exception.h"
#include "server/service_registry.h"

namespace xrpc {

ConnectionIoLoop::ConnectionIoLoop(ServiceRegistry &registry, WorkerPool &worker_pool, ServerConnectionConfig config)
    : context_(256, io::UringBufferPoolConfig{}), registry_(registry), worker_pool_(worker_pool), config_(config) {}

ConnectionIoLoop::~ConnectionIoLoop() { StopImmediately(); }

void ConnectionIoLoop::Start() {
  std::lock_guard lock(drain_mutex_);
  if (state_ != State::Created) {
    return;
  }
  thread_ = std::jthread([this]() -> void {
    try {
      context_.Run();
    } catch (...) {  // XRPC_EXTERNAL_EXCEPTION_BOUNDARY: thread entry
      error_ = CaughtExceptionToStatus("connection I/O loop failed");
    }
  });
  state_ = State::Running;
}

/**
 * @brief Performs best-effort immediate shutdown without graceful draining.
 */
void ConnectionIoLoop::StopImmediately() noexcept {
  {
    std::lock_guard lock(drain_mutex_);
    if (state_ == State::Created || state_ == State::Stopped) {
      state_ = State::Stopped;
      return;
    }
  }

  if (!thread_.joinable()) {
    std::lock_guard lock(drain_mutex_);
    state_ = State::Stopped;
    return;
  }

  context_.Post([this]() -> void { CloseConnectionsOnContext(); });
  context_.RequestStop();
  if (thread_.joinable()) {
    thread_.join();
  }
  {
    std::lock_guard lock(drain_mutex_);
    state_ = State::Stopped;
  }
}

void ConnectionIoLoop::BeginDrain() {
  {
    std::lock_guard lock(drain_mutex_);
    if (state_ == State::Created) {
      state_ = State::Stopped;
      return;
    }
    if (state_ != State::Running) {
      return;
    }
    state_ = State::Draining;
  }

  context_.Post([this]() -> void { BeginDrainOnContext(); });
}

auto ConnectionIoLoop::FinishDrain() -> Status {
  BeginDrain();
  {
    std::unique_lock lock(drain_mutex_);
    if (state_ == State::Stopped) {
      return error_;
    }
    drain_cv_.wait(lock, [this]() -> bool { return live_connections_ == 0; });
  }
  context_.RequestStop();

  if (thread_.joinable()) {
    thread_.join();
  }

  connections_.clear();
  {
    std::lock_guard lock(drain_mutex_);
    state_ = State::Stopped;
  }
  return error_;
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
  ServerConnection *connection = nullptr;
  {
    std::lock_guard lock(drain_mutex_);
    if (state_ != State::Running) {
      client_socket.Close();
      return;
    }

    const ConnectionId connection_id = AllocateConnectionId();
    // The constructor is private so only the owning loop can create a connection.
    auto owned_connection = std::unique_ptr<ServerConnection>(
        new ServerConnection(connection_id, *this, context_, registry_, worker_pool_, std::move(client_socket), config_,
                             [this]() -> void { OnConnectionClosed(); }));
    auto [position, inserted] = connections_.emplace(connection_id, std::move(owned_connection));
    if (!inserted) {
      Abort("ConnectionIoLoop generated a duplicate connection ID");
    }
    connection = position->second.get();
    ++live_connections_;
  }
  connection->Start();
}

auto ConnectionIoLoop::AllocateConnectionId() -> ConnectionId {
  const ConnectionId connection_id = next_connection_id_;
  if (connection_id == 0) {
    Abort("ConnectionIoLoop exhausted the connection ID space");
  }
  ++next_connection_id_;
  return connection_id;
}

void ConnectionIoLoop::CollectClosedConnections() {
  std::erase_if(connections_, [](const auto &entry) -> bool { return entry.second->CanBeCollected(); });
}

void ConnectionIoLoop::CloseConnectionsOnContext() {
  for (const auto &entry : connections_) {
    const auto &connection = entry.second;
    if (!connection->IsClosed()) {
      connection->Close();
    }
  }
  CollectClosedConnections();
}

void ConnectionIoLoop::BeginDrainOnContext() {
  for (const auto &entry : connections_) {
    const auto &connection = entry.second;
    connection->BeginDrain();
  }
}

void ConnectionIoLoop::PostDispatchCompletion(DispatchCompletion completion) {
  context_.Post([this, completion = std::move(completion)]() mutable -> void {
    HandleDispatchCompletion(std::move(completion));
  });
}

void ConnectionIoLoop::HandleDispatchCompletion(DispatchCompletion completion) {
  auto connection = connections_.find(completion.connection_id_);
  if (connection == connections_.end()) {
    return;
  }

  if (completion.encode_failed_) {
    connection->second->OnDispatchEncodeFailure(completion.completed_jobs_);
  } else {
    connection->second->OnEncodedDispatchComplete(std::move(completion.response_bytes_), completion.completed_jobs_);
  }
  CollectClosedConnections();
}

void ConnectionIoLoop::OnConnectionClosed() {
  {
    std::lock_guard lock(drain_mutex_);
    if (live_connections_ > 0) {
      --live_connections_;
    }
  }
  drain_cv_.notify_one();
}

}  // namespace xrpc
