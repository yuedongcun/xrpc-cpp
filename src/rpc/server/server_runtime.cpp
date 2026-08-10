#include "rpc/server/server_runtime.h"

#include <string_view>
#include <utility>

#include "rpc/xrpc_exception.h"

#include "rpc/naming/consul_agent_client.h"

namespace xrpc {

/**
 * @brief Builds all server runtime components from normalized options.
 *
 * The runtime wires the registry into `TcpServer` through a raw handler, while the TCP server owns connection I/O
 * and the executor owns method-dispatch worker threads.
 */
RpcServer::ServerRuntime::ServerRuntime(const RpcServerOptions &options)
    : config_(NormalizeServerOptions(options)),
      executor_(config_.worker_threads_, config_.max_pending_jobs_global_),
      server_(
          accept_context_, [this](RawRequest request) { return registry_.Dispatch(std::move(request)); }, executor_,
          config_.transport_) {}

/**
 * @brief Performs best-effort shutdown during runtime destruction.
 */
RpcServer::ServerRuntime::~ServerRuntime() { ShutdownBestEffort(); }

/**
 * @brief Registers one public method adapter before the server starts listening.
 *
 * The typed public registration is converted to a raw handler that preserves the request id and delegates payload
 * decoding/encoding to the captured method adapter.
 */
void RpcServer::ServerRuntime::RegisterMethod(MethodRegistration registration) {
  std::lock_guard<std::mutex> lock(lifecycle_mutex_);
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
 * The state transition goes `Created -> Starting -> Listening`; failures stop any partially initialized runtime and
 * leave the runtime stopped.
 */
void RpcServer::ServerRuntime::Listen(std::string_view host, std::uint16_t port) {
  std::lock_guard operation_lock(lifecycle_operation_mutex_);
  {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_ != State::Created) {
      throw LifecycleException("RpcServer::Listen requires a newly created server");
    }
    state_ = State::Starting;
  }

  try {
    server_.Listen(host, port);
    RegisterServiceIfEnabled(host);
  } catch (...) {
    StopRuntime(false);
    std::lock_guard lock(lifecycle_mutex_);
    state_ = State::Stopped;
    throw;
  }

  std::lock_guard lock(lifecycle_mutex_);
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
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (state_ != State::Listening) {
      throw LifecycleException("RpcServer::Run must be called once after Listen");
    }
    state_ = State::Running;
  }

  try {
    server_task_.emplace(server_.Run());
    StartServerTaskOnAcceptContext();
    accept_context_.Run();
    WaitForServerTaskCompletion();
    if (server_task_.has_value()) {
      server_task_->Result();
    }
  } catch (...) {
    ShutdownBestEffort(false);
    throw;
  }

  ShutdownBestEffort(false);
}

/**
 * @brief Requests shutdown through the same best-effort path used by destruction.
 */
void RpcServer::ServerRuntime::Stop() { ShutdownBestEffort(); }

/**
 * @brief Returns the bound listener port.
 */
auto RpcServer::ServerRuntime::port() const -> std::uint16_t { return server_.port(); }

/**
 * @brief Combines TCP backpressure and worker-pool diagnostics for the public stats API.
 */
auto RpcServer::ServerRuntime::stats() const -> RpcServerStats {
  const ServerBackpressureSnapshot backpressure_snapshot = server_.stats();
  const ThreadPoolExecutorSnapshot executor_snapshot = executor_.stats();
  return RpcServerStats{
      .rejected_by_inflight_limit_ = backpressure_snapshot.rejected_by_inflight_limit_,
      .rejected_by_global_pending_limit_ = backpressure_snapshot.rejected_by_global_pending_limit_,
      .closed_by_write_queue_high_watermark_ = backpressure_snapshot.closed_by_write_queue_high_watermark_,
      .max_observed_inflight_ = backpressure_snapshot.max_observed_inflight_,
      .max_observed_write_queue_bytes_ = backpressure_snapshot.max_observed_write_queue_bytes_,
      .worker_jobs_rejected_ = executor_snapshot.rejected_jobs_,
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
    registrar_ = std::make_unique<ConsulRegistrar>(std::make_unique<ConsulAgentClient>(config_.consul_address_));
  }
  const Status status = registrar_->Register(ResolveRegistrarOptions(config_, host, server_.port()));
  if (!status.ok()) {
    throw TransportException(status.code(), "Consul service registration failed: " + status.message());
  }
}

/**
 * @brief Performs ordered shutdown and records the first cleanup status.
 *
 * When shutdown is entered from inside the run loop, TCP server stop is posted onto the event-loop thread before the
 * accept context is stopped. External callers can stop the server directly before waking the accept context.
 */
auto RpcServer::ServerRuntime::Shutdown(bool run_loop_active) noexcept -> Status {
  std::lock_guard operation_lock(lifecycle_operation_mutex_);
  bool stop_from_running_context = false;
  {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_ == State::Stopped && (!registrar_ || !registrar_->registered())) {
      return shutdown_status_;
    }
    stop_from_running_context = run_loop_active && state_ == State::Running;
    state_ = State::Stopping;
  }

  Status status = TryDeregisterService();
  try {
    StopRuntime(stop_from_running_context);
  } catch (...) {
    if (status.ok()) {
      status = CaughtExceptionToStatus("server runtime shutdown failed");
    }
  }

  std::lock_guard lock(lifecycle_mutex_);
  shutdown_status_ = status;
  state_ = State::Stopped;
  return shutdown_status_;
}

/**
 * @brief Runs shutdown while preserving the public void-returning API.
 */
void RpcServer::ServerRuntime::ShutdownBestEffort(bool run_loop_active) noexcept {
  const Status status = Shutdown(run_loop_active);
  if (status.ok()) {
    return;
  }

  // Stop(), Run() teardown and the destructor preserve the public void API.
  // Shutdown() stores the status internally; there is no reporting channel here.
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
 * @brief Stops the TCP server and event loop for the current shutdown context.
 */
void RpcServer::ServerRuntime::StopRuntime(bool run_loop_active) {
  if (run_loop_active) {
    RequestServerStopOnAcceptContext();
    accept_context_.Stop();
    return;
  }

  server_.Stop();
  accept_context_.Stop();
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
  // TcpServer owns the accept operation, so it is stopped from the accept-context thread.
  // The post must happen before Stop(), which closes the post queue and wakes the loop.
  accept_context_.Post([this]() { server_.Stop(); });
}

/**
 * @brief Waits for the accept coroutine to complete if it was created.
 */
void RpcServer::ServerRuntime::WaitForServerTaskCompletion() const {
  if (server_task_.has_value()) {
    server_task_->Wait();
  }
}

}  // namespace xrpc
