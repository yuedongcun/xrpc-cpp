/**
 * @file rpc_server_impl.cpp
 * @brief Implements the server runtime lifecycle and component coordination.
 *
 * `RpcServer::Impl` coordinates the listening socket, accept loop, connection
 * I/O loops, worker pool, service registry, and optional service
 * registration.
 *
 * Server lifecycle:
 *
 *   Created -> Listening -> Running -> Stopping -> Stopped
 *
 * `Run()` drives the accept `UringContext` on the calling thread. Accepted
 * sockets are distributed across the connection I/O loops, while RPC handlers
 * execute on the worker pool.
 *
 * Graceful shutdown stops new work first, drains admitted worker jobs and
 * existing connections, then stops the I/O loops before the runtime reaches
 * `Stopped`.
 */

#include "server/rpc_server_impl.h"

#include <cassert>
#include <exception>
#include <memory>
#include <string_view>
#include <utility>

#include "common/xrpc_exception.h"
#include "io/socket.h"
#include "naming/consul/consul_registrar.h"

namespace xrpc {

RpcServer::Impl::Impl(ServerConfig config) : config_(std::move(config)), worker_pool_(config_.worker_pool_) {
  connection_io_loops_.reserve(config_.connection_io_.threads_);
  for (std::size_t index = 0; index < config_.connection_io_.threads_; ++index) {
    connection_io_loops_.push_back(
        std::make_unique<ConnectionIoLoop>(registry_, worker_pool_, config_.connection_io_.connection_));
  }
}

RpcServer::Impl::~Impl() { Stop(); }

auto RpcServer::Impl::RegisterMethod(MethodRegistration registration) -> Status {
  std::lock_guard lock(lifecycle_mutex_);
  if (state_ != State::Created && state_ != State::Listening) {
    return {StatusCode::FailedPrecondition, "RpcServer::RegisterMethod must be called before Run"};
  }

  auto invoke = std::move(registration.invoke_);
  RequestHandler handler = [invoke = std::move(invoke)](const RequestEnvelope &request) -> ResponseEnvelope {
    ResponseEnvelope response;
    response.request_id_ = request.request_id_;
    StatusOr<std::string> result = invoke(request.payload_);
    if (!result.ok()) {
      response.status_ = result.status();
      return response;
    }
    response.payload_ = std::move(result).value();
    return response;
  };

  return registry_.Register(registration.service_name_, registration.method_name_, std::move(handler));
}

auto RpcServer::Impl::Listen(std::string_view host, std::uint16_t port) -> Status {
  std::lock_guard lock(lifecycle_mutex_);
  if (state_ != State::Created) {
    return {StatusCode::FailedPrecondition, "RpcServer::Listen requires a newly created server"};
  }

  io::Socket socket;
  Status status = socket.Bind(host, port);
  if (!status.ok()) {
    state_ = State::Stopped;
    return status;
  }
  status = socket.Listen(config_.listen_.backlog_);
  if (!status.ok()) {
    state_ = State::Stopped;
    return status;
  }
  StatusOr<std::uint16_t> local_port = socket.LocalPort();
  if (!local_port.ok()) {
    state_ = State::Stopped;
    return local_port.status();
  }

  std::string listen_host(host);
  listen_socket_ = std::move(socket);
  listen_host_ = std::move(listen_host);
  port_ = std::move(local_port).value();
  state_ = State::Listening;
  return Status::Ok();
}

auto RpcServer::Impl::Run() -> Status {
  {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_ != State::Listening) {
      return {StatusCode::FailedPrecondition, "RpcServer::Run must be called once after Listen"};
    }

    try {
      std::optional<ConsulRegistrar::Options> registration_options;
      if (ServiceRegistrationEnabled(config_)) {
        StatusOr<ConsulRegistrar::Options> resolved = ResolveRegistrarOptions(config_, listen_host_, port_);
        if (!resolved.ok()) {
          ShutdownComponentsBestEffort();
          state_ = State::Stopped;
          return resolved.status();
        }
        registration_options.emplace(std::move(resolved).value());
        StatusOr<ConsulHttpClient> http_client = ConsulHttpClient::Create(config_.consul_.agent_address_);
        if (!http_client.ok()) {
          ShutdownComponentsBestEffort();
          state_ = State::Stopped;
          return http_client.status();
        }
        registrar_ = std::make_unique<ConsulRegistrar>(std::move(http_client).value());
      }

      StartConnectionLoops();
      StartAcceptLoop();

      if (registration_options.has_value()) {
        const Status status = registrar_->Register(*registration_options);
        if (!status.ok()) {
          ShutdownComponentsBestEffort();
          state_ = State::Stopped;
          return {status.code(), "Consul service registration failed: " + status.message()};
        }
      }
    } catch (const std::exception &) {  // XRPC_EXCEPTION_GUARD: translate startup failure after rollback
      ShutdownComponentsBestEffort();
      state_ = State::Stopped;
      return CaughtExceptionToStatus("server startup failed");
    }
    state_ = State::Running;
  }

  Status failure = Status::Ok();
  try {
    accept_context_.Run();
    accept_task_->Wait();
    accept_task_->Result();
  } catch (const std::exception &) {  // XRPC_EXCEPTION_GUARD: preserve runtime failure through shutdown
    failure = CaughtExceptionToStatus("server runtime failed");
  }

  Status shutdown_status = CompleteShutdown();
  if (failure.ok() && !shutdown_status.ok()) {
    failure = std::move(shutdown_status);
  }
  return failure;
}

void RpcServer::Impl::Stop() {
  std::lock_guard lock(lifecycle_mutex_);
  switch (state_) {
    case State::Created:
    case State::Listening:
      ShutdownComponentsBestEffort();
      state_ = State::Stopped;
      return;

    case State::Running:
      state_ = State::Stopping;
      RequestGracefulStop();
      return;

    case State::Stopping:
    case State::Stopped:
      return;
  }
}

auto RpcServer::Impl::port() const -> std::uint16_t { return port_; }

void RpcServer::Impl::StartAcceptLoop() {
  assert(listen_socket_.valid());
  assert(!accept_task_.has_value());

  accept_stopped_ = false;
  accept_task_.emplace(AcceptLoop());
  accept_context_.Post([this]() -> void { accept_task_->Start(); });
}

auto RpcServer::Impl::AcceptLoop() -> runtime::Task<void> {
  try {
    while (!accept_stopped_) {
      const io::IoResult accept_result = co_await accept_context_.Accept(listen_socket_.fd());
      if (accept_result.result_ < 0) {
        if (!accept_stopped_) {
          StopAcceptingOnContext();
        }
        break;
      }

      io::Socket client_socket(accept_result.result_);
      DispatchAcceptedConnection(std::move(client_socket));
    }
  } catch (const std::exception &) {  // XRPC_EXCEPTION_GUARD: stop context before coroutine propagation
    accept_context_.RequestStop();
    throw;
  }
  accept_context_.RequestStop();
}

void RpcServer::Impl::RequestGracefulStop() {
  accept_context_.Post([this]() -> void {
    // Withdraw the endpoint before closing local admission. There is currently
    // no discovery-propagation delay, but this ordering minimizes the window in
    // which clients can newly discover an instance that has already stopped
    // accepting work.
    (void)TryDeregisterService();
    worker_pool_.CloseSubmissions();
    StopAcceptingOnContext();
  });
}

void RpcServer::Impl::StopAcceptingOnContext() {
  if (accept_stopped_) {
    return;
  }

  accept_stopped_ = true;
  if (listen_socket_.valid()) {
    accept_context_.CancelFd(listen_socket_.fd());
    listen_socket_.Close();
  }
}

void RpcServer::Impl::DispatchAcceptedConnection(io::Socket socket) {
  assert(!connection_io_loops_.empty());
  connection_io_loops_[next_connection_io_loop_]->PostStartConnection(std::move(socket));
  next_connection_io_loop_ = (next_connection_io_loop_ + 1) % connection_io_loops_.size();
}

void RpcServer::Impl::StartConnectionLoops() {
  for (auto &loop : connection_io_loops_) {
    loop->Start();
  }
}

void RpcServer::Impl::BeginConnectionDrain() {
  for (auto &loop : connection_io_loops_) {
    loop->BeginDrain();
  }
}

auto RpcServer::Impl::FinishConnectionDrain() -> Status {
  Status failure = Status::Ok();

  for (auto &loop : connection_io_loops_) {
    Status status = loop->FinishDrain();
    if (failure.ok() && !status.ok()) {
      failure = std::move(status);
    }
  }

  try {
    accept_context_.RequestStop();
  } catch (const std::exception &) {  // XRPC_EXCEPTION_GUARD: convert runtime shutdown failure
    if (failure.ok()) {
      failure = CaughtExceptionToStatus("failed to stop accept I/O context");
    }
  }
  return failure;
}

auto RpcServer::Impl::TryDeregisterService() noexcept -> Status {
  if (!registrar_ || !registrar_->registered()) {
    return Status::Ok();
  }

  try {
    return registrar_->Deregister();
  } catch (const std::exception &) {  // XRPC_EXCEPTION_GUARD: noexcept status adapter
    return CaughtExceptionToStatus("Consul service deregistration failed");
  }
}

/**
 * @brief Completes server shutdown and publishes the terminal lifecycle state.
 *
 * Shutdown failures are preserved as a status, but the lifecycle is always
 * transitioned to `Stopped` before the status is returned.
 */
auto RpcServer::Impl::CompleteShutdown() -> Status {
  {
    std::lock_guard lock(lifecycle_mutex_);
    assert(state_ == State::Running || state_ == State::Stopping);
    state_ = State::Stopping;
  }

  Status status = ShutdownComponents();

  {
    std::lock_guard lock(lifecycle_mutex_);
    state_ = State::Stopped;
  }
  return status;
}

/**
 * @brief Shuts down server components in graceful-drain order.
 *
 * New RPC admission and accepting stop first. Existing connections begin
 * draining while already admitted worker jobs are allowed to finish, after
 * which connection I/O loops complete their drain and stop.
 *
 * Cleanup continues after individual failures and returns the first failure
 * after all shutdown steps have been attempted.
 */
auto RpcServer::Impl::ShutdownComponents() -> Status {
  Status failure = Status::Ok();
  auto attempt = [&failure](auto &&action) -> void {
    try {
      action();
    } catch (const std::exception &) {  // XRPC_EXCEPTION_GUARD: convert failure and continue shutdown
      if (failure.ok()) {
        failure = CaughtExceptionToStatus("server component shutdown failed");
      }
    }
  };

  // Service discovery withdrawal precedes local admission shutdown. This is
  // idempotent when the normal Stop() path already deregistered the instance.
  (void)TryDeregisterService();
  worker_pool_.CloseSubmissions();
  attempt([this]() -> void { StopAcceptingOnContext(); });
  attempt([this]() -> void { BeginConnectionDrain(); });

  attempt([this]() -> void { worker_pool_.DrainAndJoin(); });
  Status drain_status = FinishConnectionDrain();
  if (failure.ok() && !drain_status.ok()) {
    failure = std::move(drain_status);
  }
  return failure;
}

void RpcServer::Impl::ShutdownComponentsBestEffort() noexcept {
  try {
    (void)ShutdownComponents();
  } catch (...) {  // XRPC_EXTERNAL_EXCEPTION_BOUNDARY: noexcept destruction fallback
    const std::exception_ptr ignored = std::current_exception();
    (void)ignored;
  }
}

}  // namespace xrpc
