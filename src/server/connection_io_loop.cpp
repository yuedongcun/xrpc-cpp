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

ConnectionIoLoop::~ConnectionIoLoop() { Stop(); }

void ConnectionIoLoop::Start() {
  if (started_) {
    return;
  }

  started_ = true;
  thread_ = std::jthread([this]() -> void {
    try {
      context_.Run();
    } catch (...) {
      error_ = std::current_exception();
    }
  });
}

void ConnectionIoLoop::Stop() noexcept {
  if (!started_) {
    return;
  }

  thread_.request_stop();

  context_.Post([this]() -> void { CloseConnections(); });
  context_.Stop();
  if (thread_.joinable()) {
    thread_.join();
  }
  dispatch_mailbox_->Disable();
  started_ = false;
}

void ConnectionIoLoop::BeginDrain() {
  {
    std::unique_lock lock(drain_mutex_);
    if (!started_) {
      drain_requested_ = true;
      drain_ready_ = true;
      return;
    }
    if (drain_requested_) {
      drain_cv_.wait(lock, [this]() -> bool { return drain_ready_; });
      return;
    }
    drain_requested_ = true;
  }

  context_.Post([this]() -> void { BeginDrainOnContext(); });
  std::unique_lock lock(drain_mutex_);
  drain_cv_.wait(lock, [this]() -> bool { return drain_ready_; });
}

void ConnectionIoLoop::FinishDrain() {
  BeginDrain();
  {
    std::unique_lock lock(drain_mutex_);
    drain_cv_.wait(lock, [this]() -> bool { return live_connections_ == 0; });
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

void ConnectionIoLoop::PostStartConnection(io::Socket client_socket) {
  auto socket_holder = std::make_shared<io::Socket>(std::move(client_socket));
  context_.Post([this, socket_holder]() -> void {
    CleanupClosedConnections();
    StartConnection(std::move(*socket_holder));
  });
}

void ConnectionIoLoop::RethrowIfFailed() const {
  if (error_) {
    std::rethrow_exception(error_);
  }
}

void ConnectionIoLoop::StartConnection(io::Socket client_socket) {
  {
    std::lock_guard lock(drain_mutex_);
    if (drain_requested_) {
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

void ConnectionIoLoop::CleanupClosedConnections() {
  std::erase_if(connections_, [](const ConnectionEntry &entry) -> bool {
    return entry.connection_->IsClosed() && entry.task_.Done();
  });
}

void ConnectionIoLoop::CloseConnections() {
  for (auto &entry : connections_) {
    if (!entry.connection_->IsClosed()) {
      entry.connection_->Close();
    }
  }
  CleanupClosedConnections();
}

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
