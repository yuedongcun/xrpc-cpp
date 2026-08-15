#include "server/rpc_server_impl.h"

#include <fcntl.h>

#include <cassert>
#include <exception>
#include <memory>
#include <string_view>
#include <utility>

#include "common/xrpc_exception.h"
#include "discovery/consul_registrar.h"
#include "io/socket.h"
#include "rpc/raw_message.h"

namespace xrpc {

RpcServer::Impl::Impl(const RpcServerOptions &options)
    : config_(NormalizeServerOptions(options)), executor_(config_.worker_threads_, config_.max_pending_jobs_) {
  connection_io_loops_.reserve(config_.io_threads_);
  for (std::size_t index = 0; index < config_.io_threads_; ++index) {
    connection_io_loops_.push_back(
        std::make_unique<ConnectionIoLoop>(registry_, executor_, config_.connection_limits_, config_.protocol_limits_));
  }
}

RpcServer::Impl::~Impl() {
  Stop();
  std::unique_lock lock(lifecycle_mutex_);
  if (state_ == State::Stopping) {
    lifecycle_cv_.wait(lock, [this]() -> bool { return state_ == State::Stopped; });
  }
}

void RpcServer::Impl::RegisterMethod(MethodRegistration registration) {
  std::lock_guard lock(lifecycle_mutex_);
  if (state_ != State::Created) {
    throw LifecycleException("RpcServer::RegisterMethod must be called before Listen");
  }

  auto invoke = std::move(registration.invoke_);
  RawHandler handler = [invoke = std::move(invoke)](const RawRequest &request) -> RawResponse {
    RawResponse response;
    response.request_id_ = request.request_id_;
    StatusOr<std::string> result = invoke(request.payload_);
    if (!result.ok()) {
      response.status_ = result.status();
      return response;
    }
    response.payload_ = std::move(result).value();
    return response;
  };

  registry_.RegisterRaw(registration.service_name_, registration.method_name_, std::move(handler));
}

void RpcServer::Impl::Listen(std::string_view host, std::uint16_t port) {
  std::lock_guard lock(lifecycle_mutex_);
  if (state_ != State::Created) {
    throw LifecycleException("RpcServer::Listen requires a newly created server");
  }

  try {
    listen_socket_.Bind(host, port);
    listen_socket_.Listen(config_.backlog_);
    port_ = listen_socket_.LocalPort();
    RegisterServiceIfEnabled(host);
  } catch (...) {
    ShutdownComponentsBestEffort();
    state_ = State::Stopped;
    throw;
  }

  state_ = State::Listening;
}

void RpcServer::Impl::Run() {
  {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_ != State::Listening) {
      throw LifecycleException("RpcServer::Run must be called once after Listen");
    }

    try {
      StartConnectionLoops();  // create connection loop jthreads and run
      StartAcceptLoop();       // does not create jthread
    } catch (...) {
      ShutdownComponentsBestEffort();
      state_ = State::Stopped;
      throw;
    }
    state_ = State::Running;
  }

  std::exception_ptr failure;
  try {
    accept_context_.Run();  // accept loop starts here, not return until Stop
    accept_task_->Wait();
    accept_task_->Result();
  } catch (...) {
    failure = std::current_exception();
  }

  try {
    CompleteShutdown();
  } catch (...) {
    if (!failure) {
      failure = std::current_exception();
    }
  }
  if (failure) {
    std::rethrow_exception(failure);
  }
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
      executor_.CloseSubmissions();
      RequestStopAccepting();
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
          StopAccepting();
        }
        break;
      }

      io::Socket client_socket(accept_result.result_);
      ConfigureAcceptedSocket(client_socket.fd());
      DispatchAcceptedConnection(std::move(client_socket));
    }
  } catch (...) {
    accept_context_.Stop();
    throw;
  }
  accept_context_.Stop();
}

void RpcServer::Impl::RequestStopAccepting() {
  accept_context_.Post([this]() -> void { StopAccepting(); });
}

void RpcServer::Impl::StopAccepting() {
  if (accept_stopped_) {
    return;
  }

  accept_stopped_ = true;
  if (listen_socket_.valid()) {
    accept_context_.CancelFd(listen_socket_.fd());
    listen_socket_.Close();
  }
}

void RpcServer::Impl::ConfigureAcceptedSocket(int fd) const {
  const int fd_flags = ::fcntl(fd, F_GETFD);
  if (fd_flags >= 0) {
    (void)::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC);
  }

  const int status_flags = ::fcntl(fd, F_GETFL);
  if (status_flags >= 0) {
    (void)::fcntl(fd, F_SETFL, status_flags | O_NONBLOCK);
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

void RpcServer::Impl::FinishConnectionDrain() {
  std::exception_ptr failure;
  auto attempt = [&failure](auto &&action) {
    try {
      action();
    } catch (...) {
      if (!failure) {
        failure = std::current_exception();
      }
    }
  };

  for (auto &loop : connection_io_loops_) {
    attempt([&loop]() { loop->FinishDrain(); });
  }
  for (const auto &loop : connection_io_loops_) {
    attempt([&loop]() { loop->RethrowIfFailed(); });
  }
  attempt([this]() { accept_context_.Stop(); });

  if (failure) {
    std::rethrow_exception(failure);
  }
}

void RpcServer::Impl::RegisterServiceIfEnabled(std::string_view host) {
  if (!ServiceRegistrationEnabled(config_)) {
    return;
  }
  if (!registrar_) {
    registrar_ = std::make_unique<ConsulRegistrar>(config_.consul_.agent_address_);
  }
  const Status status = registrar_->Register(ResolveRegistrarOptions(config_, host, port_));
  if (!status.ok()) {
    throw TransportException(status.code(), "Consul service registration failed: " + status.message());
  }
}

auto RpcServer::Impl::TryDeregisterService() noexcept -> Status {
  if (!registrar_ || !registrar_->registered()) {
    return Status::Ok();
  }

  try {
    return registrar_->Deregister();
  } catch (...) {
    return CaughtExceptionToStatus("Consul service deregistration failed");
  }
}

void RpcServer::Impl::CompleteShutdown() {
  {
    std::lock_guard lock(lifecycle_mutex_);
    assert(state_ == State::Running || state_ == State::Stopping);
    state_ = State::Stopping;
  }

  std::exception_ptr failure;
  try {
    ShutdownComponents();
  } catch (...) {
    failure = std::current_exception();
  }

  {
    std::lock_guard lock(lifecycle_mutex_);
    state_ = State::Stopped;
  }
  lifecycle_cv_.notify_all();
  if (failure) {
    std::rethrow_exception(failure);
  }
}

void RpcServer::Impl::ShutdownComponents() {
  std::exception_ptr failure;
  auto attempt = [&failure](auto &&action) -> void {
    try {
      action();
    } catch (...) {
      if (!failure) {
        failure = std::current_exception();
      }
    }
  };

  executor_.CloseSubmissions();
  attempt([this]() -> void { StopAccepting(); });
  attempt([this]() -> void { BeginConnectionDrain(); });
  (void)TryDeregisterService();

  // Stop drains every admitted worker job and joins all workers. Connection loops and mailboxes must remain alive
  // until it returns because completed handlers post encoded responses back to their originating I/O loop.
  attempt([this]() -> void { executor_.Stop(); });
  attempt([this]() -> void { FinishConnectionDrain(); });

  if (failure) {
    std::rethrow_exception(failure);
  }
}

void RpcServer::Impl::ShutdownComponentsBestEffort() noexcept {
  try {
    ShutdownComponents();
  } catch (...) {
  }
}

}  // namespace xrpc
