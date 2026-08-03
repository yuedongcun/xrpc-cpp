#include "observability/rpc_metrics.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include <xrpc/metrics.h>

namespace xrpc {
namespace {

/** @brief Client RPC completion counter emitted with service, method, and status labels. */
constexpr std::string_view CLIENT_RPC_COMPLETED_TOTAL = "xrpc_client_rpc_completed_total";

/** @brief Client RPC failure counter emitted with service, method, and status labels. */
constexpr std::string_view CLIENT_RPC_FAILED_TOTAL = "xrpc_client_rpc_failed_total";

/** @brief Client-side end-to-end RPC latency histogram. */
constexpr std::string_view CLIENT_RPC_LATENCY_SECONDS = "xrpc_client_rpc_latency_seconds";

/** @brief Counter for transport failover attempts after an endpoint returns a retriable result. */
constexpr std::string_view CLIENT_RPC_FAILOVER_ATTEMPT_TOTAL = "xrpc_client_rpc_failover_attempt_total";

/** @brief Counter for failover attempts blocked by request commit state. */
constexpr std::string_view CLIENT_RPC_FAILOVER_BLOCKED_TOTAL = "xrpc_client_rpc_failover_blocked_total";

/** @brief Gauge containing the number of endpoints in the latest resolver snapshot. */
constexpr std::string_view CLIENT_RESOLVER_ENDPOINTS = "xrpc_client_resolver_endpoints";

/** @brief Counter for resolver refresh failures. */
constexpr std::string_view CLIENT_RESOLVER_REFRESH_FAILED_TOTAL = "xrpc_client_resolver_refresh_failed_total";

/** @brief Gauge for server RPCs currently being dispatched by service and method. */
constexpr std::string_view SERVER_RPC_INFLIGHT = "xrpc_server_rpc_inflight";

/** @brief Server RPC completion counter emitted with service, method, and status labels. */
constexpr std::string_view SERVER_RPC_COMPLETED_TOTAL = "xrpc_server_rpc_completed_total";

/** @brief Server RPC failure counter emitted with service, method, and status labels. */
constexpr std::string_view SERVER_RPC_FAILED_TOTAL = "xrpc_server_rpc_failed_total";

/** @brief Server-side handler/dispatch latency histogram. */
constexpr std::string_view SERVER_RPC_LATENCY_SECONDS = "xrpc_server_rpc_latency_seconds";

/** @brief Counter for requests rejected by named server backpressure guards. */
constexpr std::string_view SERVER_BACKPRESSURE_REJECTED_TOTAL = "xrpc_server_backpressure_rejected_total";

/** @brief Counter for closed connections labeled by close reason. */
constexpr std::string_view SERVER_CONNECTION_CLOSED_TOTAL = "xrpc_server_connection_closed_total";

/** @brief Counter for worker-job state transitions. */
constexpr std::string_view SERVER_WORKER_JOBS_TOTAL = "xrpc_server_worker_jobs_total";

/** @brief Gauge for queued-or-running logical worker jobs. */
constexpr std::string_view SERVER_WORKER_PENDING_JOBS = "xrpc_server_worker_pending_jobs";

/** @brief Gauge set to one while the server is draining and zero otherwise. */
constexpr std::string_view SERVER_DRAINING = "xrpc_server_draining";

/** @brief Protects the in-process inflight map used to derive per-method gauges. */
std::mutex server_inflight_mutex;

/** @brief Current server inflight count by `service\nmethod` key. */
std::unordered_map<std::string, std::uint64_t> server_inflight_by_method;

/**
 * @brief Converts a public status enum into the stable metric label value.
 *
 * @param code Public RPC status code.
 * @return Snake-case label value.
 */
[[nodiscard]] auto StatusCodeLabel(StatusCode code) -> std::string_view {
  switch (code) {
    case StatusCode::Ok:
      return "ok";
    case StatusCode::Cancelled:
      return "cancelled";
    case StatusCode::InvalidArgument:
      return "invalid_argument";
    case StatusCode::DeadlineExceeded:
      return "deadline_exceeded";
    case StatusCode::NotFound:
      return "not_found";
    case StatusCode::AlreadyExists:
      return "already_exists";
    case StatusCode::PermissionDenied:
      return "permission_denied";
    case StatusCode::ResourceExhausted:
      return "resource_exhausted";
    case StatusCode::FailedPrecondition:
      return "failed_precondition";
    case StatusCode::Unimplemented:
      return "unimplemented";
    case StatusCode::Internal:
      return "internal";
    case StatusCode::Unavailable:
      return "unavailable";
    case StatusCode::DataLoss:
      return "data_loss";
    case StatusCode::Unauthenticated:
      return "unauthenticated";
  }
  return "unknown";
}

/**
 * @brief Converts request commit state into the stable failover metric label.
 *
 * @param state Request commit state returned by transport attempts.
 * @return Snake-case label value.
 */
[[nodiscard]] auto CommitStateLabel(RequestCommitState state) -> std::string_view {
  switch (state) {
    case RequestCommitState::NotSent:
      return "not_sent";
    case RequestCommitState::MaybeSent:
      return "maybe_sent";
  }
  return "unknown";
}

/**
 * @brief Converts a duration to seconds for Prometheus histogram samples.
 *
 * @param elapsed Duration measured by a client or server call path.
 * @return Duration in seconds.
 */
[[nodiscard]] auto Seconds(std::chrono::nanoseconds elapsed) -> double {
  return static_cast<double>(elapsed.count()) / 1'000'000'000.0;
}

/**
 * @brief Builds the internal map key for per-method server inflight accounting.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @return Stable composite key that cannot collide between service and method boundaries.
 */
[[nodiscard]] auto ServerInflightKey(std::string_view service_name, std::string_view method_name) -> std::string {
  std::string key;
  key.reserve(service_name.size() + method_name.size() + 1);
  key.append(service_name);
  key.push_back('\n');
  key.append(method_name);
  return key;
}

/**
 * @brief Updates the current inflight count for one server method.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @param delta Positive to increment, negative to decrement by one.
 * @return Updated inflight count.
 */
[[nodiscard]] auto AddServerInflight(std::string_view service_name, std::string_view method_name, int delta)
    -> std::uint64_t {
  std::lock_guard lock(server_inflight_mutex);
  std::uint64_t &value = server_inflight_by_method[ServerInflightKey(service_name, method_name)];
  if (delta > 0) {
    value += static_cast<std::uint64_t>(delta);
  } else if (value > 0) {
    --value;
  }
  return value;
}

/**
 * @brief Publishes the current server inflight gauge for one method.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @param value Current inflight count.
 */
void SetServerInflight(std::string_view service_name, std::string_view method_name, std::uint64_t value) {
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 2> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "method", .value_ = method_name},
  }};
  sink->SetGauge(SERVER_RPC_INFLIGHT, static_cast<double>(value), labels);
}

}  // namespace

/**
 * @brief Records a completed client RPC, regardless of whether its final status is OK.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @param status_code Final public RPC status.
 */
void RecordClientRpcCompleted(std::string_view service_name, std::string_view method_name, StatusCode status_code) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 3> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "method", .value_ = method_name},
      {.name_ = "status_code", .value_ = StatusCodeLabel(status_code)},
  }};
  sink->AddCounter(CLIENT_RPC_COMPLETED_TOTAL, 1.0, labels);
}

/**
 * @brief Records a client RPC that completed with a non-OK status.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @param status_code Failure status.
 */
void RecordClientRpcFailed(std::string_view service_name, std::string_view method_name, StatusCode status_code) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 3> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "method", .value_ = method_name},
      {.name_ = "status_code", .value_ = StatusCodeLabel(status_code)},
  }};
  sink->AddCounter(CLIENT_RPC_FAILED_TOTAL, 1.0, labels);
}

/**
 * @brief Records client-side RPC latency.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @param status_code Final public RPC status.
 * @param elapsed End-to-end client latency.
 */
void RecordClientRpcLatency(std::string_view service_name, std::string_view method_name, StatusCode status_code,
                            std::chrono::nanoseconds elapsed) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 3> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "method", .value_ = method_name},
      {.name_ = "status_code", .value_ = StatusCodeLabel(status_code)},
  }};
  sink->ObserveHistogram(CLIENT_RPC_LATENCY_SECONDS, Seconds(elapsed), labels);
}

/**
 * @brief Records that the client tried another endpoint after a transport attempt.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @param status_code Status that triggered failover consideration.
 * @param commit_state Whether the request may have reached the server.
 */
void RecordClientRpcFailoverAttempt(std::string_view service_name, std::string_view method_name, StatusCode status_code,
                                    RequestCommitState commit_state) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 4> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "method", .value_ = method_name},
      {.name_ = "status_code", .value_ = StatusCodeLabel(status_code)},
      {.name_ = "commit_state", .value_ = CommitStateLabel(commit_state)},
  }};
  sink->AddCounter(CLIENT_RPC_FAILOVER_ATTEMPT_TOTAL, 1.0, labels);
}

/**
 * @brief Records that failover was considered but blocked by commit state or policy.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @param status_code Status that triggered failover consideration.
 * @param commit_state Whether the request may have reached the server.
 */
void RecordClientRpcFailoverBlocked(std::string_view service_name, std::string_view method_name, StatusCode status_code,
                                    RequestCommitState commit_state) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 4> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "method", .value_ = method_name},
      {.name_ = "status_code", .value_ = StatusCodeLabel(status_code)},
      {.name_ = "commit_state", .value_ = CommitStateLabel(commit_state)},
  }};
  sink->AddCounter(CLIENT_RPC_FAILOVER_BLOCKED_TOTAL, 1.0, labels);
}

/**
 * @brief Records the endpoint count from the latest resolver snapshot.
 *
 * @param service_name Service or target label value.
 * @param resolver_type Resolver implementation label.
 * @param endpoint_count Number of endpoints visible to the channel.
 */
void RecordClientResolverEndpoints(std::string_view service_name, std::string_view resolver_type,
                                   std::size_t endpoint_count) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 2> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "resolver_type", .value_ = resolver_type},
  }};
  sink->SetGauge(CLIENT_RESOLVER_ENDPOINTS, static_cast<double>(endpoint_count), labels);
}

/**
 * @brief Records a failed resolver refresh.
 *
 * @param service_name Service or target label value.
 * @param resolver_type Resolver implementation label.
 * @param status_code Failure category.
 */
void RecordClientResolverRefreshFailed(std::string_view service_name, std::string_view resolver_type,
                                       StatusCode status_code) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 3> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "resolver_type", .value_ = resolver_type},
      {.name_ = "status_code", .value_ = StatusCodeLabel(status_code)},
  }};
  sink->AddCounter(CLIENT_RESOLVER_REFRESH_FAILED_TOTAL, 1.0, labels);
}

/**
 * @brief Increments and publishes the server inflight gauge for one method.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 */
void IncrementServerRpcInflight(std::string_view service_name, std::string_view method_name) {
  if (!MetricsEnabled()) {
    return;
  }
  SetServerInflight(service_name, method_name, AddServerInflight(service_name, method_name, 1));
}

/**
 * @brief Decrements and publishes the server inflight gauge for one method.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 */
void DecrementServerRpcInflight(std::string_view service_name, std::string_view method_name) {
  if (!MetricsEnabled()) {
    return;
  }
  SetServerInflight(service_name, method_name, AddServerInflight(service_name, method_name, -1));
}

/**
 * @brief Records a completed server RPC, regardless of whether its final status is OK.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @param status_code Final public RPC status.
 */
void RecordServerRpcCompleted(std::string_view service_name, std::string_view method_name, StatusCode status_code) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 3> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "method", .value_ = method_name},
      {.name_ = "status_code", .value_ = StatusCodeLabel(status_code)},
  }};
  sink->AddCounter(SERVER_RPC_COMPLETED_TOTAL, 1.0, labels);
}

/**
 * @brief Records a server RPC that completed with a non-OK status.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @param status_code Failure status.
 */
void RecordServerRpcFailed(std::string_view service_name, std::string_view method_name, StatusCode status_code) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 3> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "method", .value_ = method_name},
      {.name_ = "status_code", .value_ = StatusCodeLabel(status_code)},
  }};
  sink->AddCounter(SERVER_RPC_FAILED_TOTAL, 1.0, labels);
}

/**
 * @brief Records server-side handler and dispatch latency.
 *
 * @param service_name Service label value.
 * @param method_name Method label value.
 * @param status_code Final public RPC status.
 * @param elapsed Server-side elapsed time.
 */
void RecordServerRpcLatency(std::string_view service_name, std::string_view method_name, StatusCode status_code,
                            std::chrono::nanoseconds elapsed) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 3> labels{{
      {.name_ = "service", .value_ = service_name},
      {.name_ = "method", .value_ = method_name},
      {.name_ = "status_code", .value_ = StatusCodeLabel(status_code)},
  }};
  sink->ObserveHistogram(SERVER_RPC_LATENCY_SECONDS, Seconds(elapsed), labels);
}

/**
 * @brief Records a server request rejected by a backpressure guard.
 *
 * @param reason Rejection reason label.
 */
void RecordServerBackpressureRejected(std::string_view reason) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 1> labels{{
      {.name_ = "reason", .value_ = reason},
  }};
  sink->AddCounter(SERVER_BACKPRESSURE_REJECTED_TOTAL, 1.0, labels);
}

/**
 * @brief Records a server connection close reason.
 *
 * @param reason Close reason label.
 */
void RecordServerConnectionClosed(std::string_view reason) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 1> labels{{
      {.name_ = "reason", .value_ = reason},
  }};
  sink->AddCounter(SERVER_CONNECTION_CLOSED_TOTAL, 1.0, labels);
}

/**
 * @brief Records one worker-job state transition.
 *
 * @param state Worker state label such as `submitted`, `completed`, or `rejected`.
 */
void RecordServerWorkerJob(std::string_view state) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  const std::array<MetricLabel, 1> labels{{
      {.name_ = "state", .value_ = state},
  }};
  sink->AddCounter(SERVER_WORKER_JOBS_TOTAL, 1.0, labels);
}

/**
 * @brief Publishes the current global worker pending-job count.
 *
 * @param pending_jobs Queued-or-running logical worker jobs.
 */
void RecordServerWorkerPendingJobs(std::size_t pending_jobs) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  sink->SetGauge(SERVER_WORKER_PENDING_JOBS, static_cast<double>(pending_jobs), MetricLabels{});
}

/**
 * @brief Publishes whether the server is draining.
 *
 * @param draining true while graceful shutdown/drain logic is active.
 */
void RecordServerDraining(bool draining) {
  if (!MetricsEnabled()) {
    return;
  }
  const std::shared_ptr<MetricSink> sink = GetMetricSink();
  if (!sink) {
    return;
  }
  sink->SetGauge(SERVER_DRAINING, draining ? 1.0 : 0.0, MetricLabels{});
}

}  // namespace xrpc
