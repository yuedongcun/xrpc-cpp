#include "rpc/server/server_runtime.h"

#include <cassert>
#include <string_view>
#include <utility>

#include "rpc/xrpc_exception.h"

namespace xrpc {
namespace {

auto MakeDispatchHandler(ServiceRegistry &registry) -> RawHandler {
  RawHandler handler = [&registry](RawRequest request) -> RawResponse { return registry.Dispatch(std::move(request)); };
  return handler;
}

}  // namespace

/**
 * @brief Builds all server runtime components from normalized options.
 *
 * The runtime wires the registry into `TcpServer` through a raw handler, while the TCP server owns connection I/O
 * and the executor owns method-dispatch worker threads.
 */
RpcServer::ServerRuntime::ServerRuntime(const RpcServerOptions &options)
    : config_(NormalizeServerOptions(options)),
      executor_(config_.worker_threads_, config_.max_pending_jobs_global_),
      server_(accept_context_, MakeDispatchHandler(registry_), executor_, config_.transport_) {}

/** @brief Stops an unstarted runtime or waits for an active Run() coordinator. */
RpcServer::ServerRuntime::~ServerRuntime() {
  Stop();
  std::unique_lock lock(lifecycle_mutex_);
  if (state_ == State::Stopping) {
    lifecycle_cv_.wait(lock, [this]() -> bool { return state_ == State::Stopped; });
  }
}

/**
 * @brief Registers one public method adapter before the server starts listening.
 *
 * The typed public registration is converted to a raw handler that preserves the request id and delegates payload
 * decoding/encoding to the captured method adapter.
 */
void RpcServer::ServerRuntime::RegisterMethod(MethodRegistration registration) {
  std::lock_guard lock(lifecycle_mutex_);
  if (state_ != State::Created) {
    throw LifecycleException("RpcServer::RegisterMethod must be called before Listen");
  }

  auto invoke = std::move(registration.invoke_);
  RawHandler handler = [invoke = std::move(invoke)](RawRequest request) -> RawResponse {
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

/**
 * @brief Binds the listener and performs optional Consul registration.
 *
 * The operation commits `Created -> Listening` only after binding and optional registration both succeed. Failures
 * close the partially initialized runtime and commit the terminal state.
 */
void RpcServer::ServerRuntime::Listen(std::string_view host, std::uint16_t port) {
  std::lock_guard lock(lifecycle_mutex_);
  if (state_ != State::Created) {
    throw LifecycleException("RpcServer::Listen requires a newly created server");
  }

  try {
    server_.Listen(host, port);
    RegisterServiceIfEnabled(host);
  } catch (...) {
    ShutdownComponentsBestEffort();
    state_ = State::Stopped;
    throw;
  }

  state_ = State::Listening;
}

/**
 * @brief Runs the accept loop until shutdown.
 *
 * The accept coroutine is started before the accept context enters its run loop. Both normal exit and exceptional exit
 * flow through shutdown so workers, connections, and Consul registration are cleaned up.
 */
void RpcServer::ServerRuntime::Run() {
  {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_ != State::Listening) {
      throw LifecycleException("RpcServer::Run must be called once after Listen");
    }

    try {
      server_task_.emplace(server_.Run());
      StartServerTaskOnAcceptContext();
    } catch (...) {
      ShutdownComponentsBestEffort();
      state_ = State::Stopped;
      throw;
    }

    state_ = State::Running;
  }

  std::exception_ptr failure;
  try {
    accept_context_.Run();
    server_task_->Wait();
    server_task_->Result();
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

/**
 * @brief Closes admission and asks the accept loop to begin graceful shutdown.
 */
void RpcServer::ServerRuntime::Stop() {
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
      RequestServerStopOnAcceptContext();
      return;

    case State::Stopping:
    case State::Stopped:
      return;
  }
}

/**
 * @brief Returns the bound listener port.
 */
auto RpcServer::ServerRuntime::port() const -> std::uint16_t { return server_.port(); }

/**
 * @brief Combines connection backpressure and worker-pool diagnostics for the public stats API.
 */
auto RpcServer::ServerRuntime::stats() const -> RpcServerStats {
  const ServerBackpressureSnapshot backpressure_snapshot = server_.stats();
  const ThreadPoolExecutorSnapshot executor_snapshot = executor_.stats();
  return RpcServerStats{
      .rejected_by_inflight_limit_ = backpressure_snapshot.rejected_by_inflight_limit_,
      .rejected_by_global_pending_limit_ = executor_snapshot.rejected_by_pending_limit_,
      .closed_by_write_queue_high_watermark_ = backpressure_snapshot.closed_by_write_queue_high_watermark_,
      .max_observed_inflight_ = backpressure_snapshot.max_observed_inflight_,
      .max_observed_write_queue_bytes_ = backpressure_snapshot.max_observed_write_queue_bytes_,
      .max_observed_worker_queue_depth_ = executor_snapshot.max_observed_worker_queue_depth_,
  };
}

/**
 * @brief Registers the running server in Consul when service registration is enabled.
 */
void RpcServer::ServerRuntime::RegisterServiceIfEnabled(std::string_view host) {
  if (!ServiceRegistrationEnabled(config_)) {
    return;
  }
  if (!registrar_) {
    registrar_ = std::make_unique<ConsulRegistrar>(config_.consul_address_);
  }
  const Status status = registrar_->Register(ResolveRegistrarOptions(config_, host, server_.port()));
  if (!status.ok()) {
    throw TransportException(status.code(), "Consul service registration failed: " + status.message());
  }
}

/** @brief Completes the ordered graceful-drain sequence owned by Run(). */
void RpcServer::ServerRuntime::CompleteShutdown() {
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

/** @brief Stops components while keeping connection loops alive until worker completions drain. */
void RpcServer::ServerRuntime::ShutdownComponents() {
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

  executor_.CloseSubmissions();
  attempt([this]() { server_.StopAccepting(); });
  attempt([this]() { server_.BeginDrain(); });
  (void)TryDeregisterService();
  attempt([this]() { executor_.Stop(); });
  attempt([this]() { server_.FinishDrain(); });
  attempt([this]() { accept_context_.Stop(); });

  if (failure) {
    std::rethrow_exception(failure);
  }
}

/** @brief Suppresses cleanup failures for pre-run and partial-start shutdown. */
void RpcServer::ServerRuntime::ShutdownComponentsBestEffort() noexcept {
  try {
    ShutdownComponents();
  } catch (...) {
  }
}

/**
 * @brief Deregisters the Consul service if the registrar still owns one.
 */
auto RpcServer::ServerRuntime::TryDeregisterService() noexcept -> Status {
  if (!registrar_ || !registrar_->registered()) {
    return Status::Ok();
  }

  try {
    return registrar_->Deregister();
  } catch (...) {
    return CaughtExceptionToStatus("Consul service deregistration failed");
  }
}

/**
 * @brief Posts accept coroutine startup onto the event-loop thread.
 */
void RpcServer::ServerRuntime::StartServerTaskOnAcceptContext() {
  accept_context_.Post([this]() { server_task_->Start(); });
}

/**
 * @brief Posts TCP server stop onto the event-loop thread.
 */
void RpcServer::ServerRuntime::RequestServerStopOnAcceptContext() {
  accept_context_.Post([this]() { server_.StopAccepting(); });
}

}  // namespace xrpc
