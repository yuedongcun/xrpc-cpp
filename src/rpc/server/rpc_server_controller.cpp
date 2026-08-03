#include "rpc/server/rpc_server_controller.h"

#include <string_view>
#include <thread>
#include <utility>

#include <xrpc/xrpc_exception.h>

#include "observability/rpc_metrics.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_error.h"
#include "rpc/naming/consul_agent_client.h"
#include "rpc/protocol_adapter.h"

namespace xrpc {

/**
 * @brief Builds all server runtime components from normalized options.
 *
 * The controller wires the registry into `TcpServer` through a raw handler, while the TCP server owns connection I/O
 * and the executor owns method-dispatch worker threads.
 */
RpcServer::ServerController::ServerController(const RpcServerOptions &options)
    : config_(NormalizeServerOptions(options)),
      executor_(ResolveWorkerCount(config_.worker_threads_), config_.max_pending_jobs_global_),
      server_(
          context_, [this](RawRequest request) { return registry_.Dispatch(std::move(request)); }, executor_,
          ServerBackpressureLimits{
              .max_inflight_per_connection_ = config_.max_inflight_per_connection_,
              .max_write_queue_bytes_per_connection_ = config_.max_write_queue_bytes_per_connection_,
          },
          config_.connection_io_threads_, config_.protocol_limits_, config_.connection_idle_timeout_),
      listen_backlog_(config_.listen_backlog_) {}

/**
 * @brief Performs best-effort shutdown during controller destruction.
 */
RpcServer::ServerController::~ServerController() { ShutdownBestEffort(); }

/**
 * @brief Registers one public method adapter before the server starts listening.
 *
 * The typed public registration is converted to a raw handler that preserves the request id and delegates payload
 * decoding/encoding to the captured method adapter.
 */
void RpcServer::ServerController::RegisterMethodRegistration(MethodRegistration registration) {
  std::lock_guard<std::mutex> lock(lifecycle_mutex_);
  if (state_ != State::Created) {
    throw LifecycleException("RpcServer::RegisterMethod must be called before Listen");
  }

  auto invoke = std::move(registration.invoke_);
  RawHandler handler = [invoke = std::move(invoke)](RawRequest request) -> RawResponse {
    RawResponse response;
    response.request_id_ = request.request_id_;
    response.payload_ = invoke(request.payload_);
    return response;
  };

  registry_.RegisterRaw(registration.service_name_, registration.method_name_, std::move(handler));
}

/**
 * @brief Binds the listener and performs optional Consul registration.
 *
 * The state transition goes `Created -> Starting -> Listening`; failures stop any partially initialized runtime and
 * leave the controller stopped.
 */
void RpcServer::ServerController::Listen(std::string_view host, std::uint16_t port) {
  std::lock_guard operation_lock(lifecycle_operation_mutex_);
  {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_ != State::Created) {
      throw LifecycleException("RpcServer::Listen requires a newly created server");
    }
    state_ = State::Starting;
  }

  try {
    server_.Listen(host, port, listen_backlog_);
    RegisterServiceIfEnabled(host);
  } catch (...) {
    StopRuntime(false);
    std::lock_guard lock(lifecycle_mutex_);
    state_ = State::Stopped;
    throw;
  }

  std::lock_guard lock(lifecycle_mutex_);
  state_ = State::Listening;
  RecordServerDraining(false);
}

/**
 * @brief Runs the accept loop until shutdown.
 *
 * The accept coroutine is started on the `UringContext` thread before the context enters its run loop. Both normal exit
 * and exceptional exit flow through shutdown so workers, connections, and Consul registration are cleaned up.
 */
void RpcServer::ServerController::Run() {
  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (state_ != State::Listening) {
      throw LifecycleException("RpcServer::Run must be called once after Listen");
    }
    state_ = State::Running;
  }

  try {
    server_task_.emplace(server_.Run());
    StartServerTaskOnContext();
    context_.Run();
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
void RpcServer::ServerController::Stop() { ShutdownBestEffort(); }

/**
 * @brief Returns the bound listener port.
 */
auto RpcServer::ServerController::port() const -> std::uint16_t { return server_.port(); }

/**
 * @brief Combines TCP backpressure and worker-pool diagnostics for the public stats API.
 */
auto RpcServer::ServerController::stats() const -> RpcServerStats {
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
 * @brief Dispatches a raw request through the service registry.
 */
auto RpcServer::ServerController::Dispatch(const RawRequest &request) const -> RawResponse {
  return registry_.Dispatch(request);
}

/**
 * @brief Dispatches a decoded protocol request and returns protocol response fields.
 */
auto RpcServer::ServerController::Dispatch(const ProtocolRequest &request) const -> ProtocolResponse {
  return ToProtocolResponse(Dispatch(ToRawRequest(request)));
}

/**
 * @brief Decodes, dispatches, and re-encodes one request frame.
 *
 * This helper is intentionally narrow and is used by tests and adapter paths. Streaming connections use `RpcSession`
 * and `TcpConnection` instead.
 */
auto RpcServer::ServerController::DispatchFrame(std::string_view frame_bytes) const -> std::string {
  FrameCodec codec(config_.protocol_limits_);
  DecodeResult decoded = codec.TryDecode(frame_bytes);
  if (decoded.error_ != ProtocolError::Ok || !decoded.request_.has_value()) {
    return {};
  }

  return codec.EncodeResponse(Dispatch(*decoded.request_));
}

/**
 * @brief Registers the running server in Consul when service registration is enabled.
 */
void RpcServer::ServerController::RegisterServiceIfEnabled(std::string_view host) {
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
 * context is stopped. External callers can stop the server directly before waking the context.
 */
auto RpcServer::ServerController::Shutdown(bool run_loop_active) noexcept -> Status {
  std::lock_guard operation_lock(lifecycle_operation_mutex_);
  bool stop_from_running_context = false;
  {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_ == State::Stopped && (!registrar_ || !registrar_->registered())) {
      return shutdown_status_;
    }
    stop_from_running_context = run_loop_active && state_ == State::Running;
    state_ = State::Stopping;
    RecordServerDraining(true);
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
  RecordServerDraining(false);
  return shutdown_status_;
}

/**
 * @brief Runs shutdown while preserving the public void-returning API.
 */
void RpcServer::ServerController::ShutdownBestEffort(bool run_loop_active) noexcept {
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
auto RpcServer::ServerController::TryDeregisterService() noexcept -> Status {
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
 * @brief Resolves zero worker count to a conservative hardware concurrency default.
 */
auto RpcServer::ServerController::ResolveWorkerCount(std::size_t worker_threads) -> std::size_t {
  if (worker_threads > 0) {
    return worker_threads;
  }

  const auto hc = std::thread::hardware_concurrency();
  return hc == 0 ? 1U : static_cast<std::size_t>(hc);
}

/**
 * @brief Stops the TCP server and event loop for the current shutdown context.
 */
void RpcServer::ServerController::StopRuntime(bool run_loop_active) {
  if (run_loop_active) {
    RequestServerStopOnContext();
    context_.Stop();
    return;
  }

  server_.Stop();
  context_.Stop();
}

/**
 * @brief Posts accept coroutine startup onto the event-loop thread.
 */
void RpcServer::ServerController::StartServerTaskOnContext() {
  context_.Post([this]() { server_task_->Start(); });
}

/**
 * @brief Posts TCP server stop onto the event-loop thread.
 */
void RpcServer::ServerController::RequestServerStopOnContext() {
  // TcpServer owns io_uring operations, so the running server is stopped from
  // the UringContext thread. Post must happen before context Stop, which closes
  // the post queue and wakes the loop.
  context_.Post([this]() { server_.Stop(); });
}

/**
 * @brief Waits for the accept coroutine to complete if it was created.
 */
void RpcServer::ServerController::WaitForServerTaskCompletion() const {
  if (server_task_.has_value()) {
    server_task_->Wait();
  }
}

}  // namespace xrpc
