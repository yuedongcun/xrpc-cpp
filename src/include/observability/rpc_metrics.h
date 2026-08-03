#pragma once

#include <chrono>
#include <cstddef>
#include <string_view>

#include <xrpc/status.h>

#include "rpc/raw_call_result.h"

namespace xrpc {

/**
 * @brief Records a completed client RPC counter sample.
 *
 * Metric helpers centralize label names and no-op behavior. Callers that would do expensive work only for metrics
 * should check `MetricsEnabled()` first.
 */
void RecordClientRpcCompleted(std::string_view service_name, std::string_view method_name, StatusCode status_code);

/** @brief Records a failed client RPC counter sample. */
void RecordClientRpcFailed(std::string_view service_name, std::string_view method_name, StatusCode status_code);

/** @brief Records client-side end-to-end RPC latency. */
void RecordClientRpcLatency(std::string_view service_name, std::string_view method_name, StatusCode status_code,
                            std::chrono::nanoseconds elapsed);

/** @brief Records a client failover attempt after one endpoint call failed. */
void RecordClientRpcFailoverAttempt(std::string_view service_name, std::string_view method_name, StatusCode status_code,
                                    RequestCommitState commit_state);

/** @brief Records a failover that was blocked because retrying could duplicate execution. */
void RecordClientRpcFailoverBlocked(std::string_view service_name, std::string_view method_name, StatusCode status_code,
                                    RequestCommitState commit_state);

/** @brief Records the current endpoint count for a resolver snapshot. */
void RecordClientResolverEndpoints(std::string_view service_name, std::string_view resolver_type,
                                   std::size_t endpoint_count);

/** @brief Records a resolver refresh failure. */
void RecordClientResolverRefreshFailed(std::string_view service_name, std::string_view resolver_type,
                                       StatusCode status_code);

/** @brief Increments server-side in-flight gauge for one service/method pair. */
void IncrementServerRpcInflight(std::string_view service_name, std::string_view method_name);

/** @brief Decrements server-side in-flight gauge for one service/method pair. */
void DecrementServerRpcInflight(std::string_view service_name, std::string_view method_name);

/** @brief Records a completed server RPC counter sample. */
void RecordServerRpcCompleted(std::string_view service_name, std::string_view method_name, StatusCode status_code);

/** @brief Records a failed server RPC counter sample. */
void RecordServerRpcFailed(std::string_view service_name, std::string_view method_name, StatusCode status_code);

/** @brief Records server-side handler/dispatch latency for one RPC. */
void RecordServerRpcLatency(std::string_view service_name, std::string_view method_name, StatusCode status_code,
                            std::chrono::nanoseconds elapsed);

/** @brief Records a server request rejected by a named backpressure guard. */
void RecordServerBackpressureRejected(std::string_view reason);

/** @brief Records a server connection close reason. */
void RecordServerConnectionClosed(std::string_view reason);

/** @brief Records a worker job state transition such as submitted, completed, or rejected. */
void RecordServerWorkerJob(std::string_view state);

/** @brief Records the current global worker pending-job gauge. */
void RecordServerWorkerPendingJobs(std::size_t pending_jobs);

/** @brief Records whether the server is draining. */
void RecordServerDraining(bool draining);

}  // namespace xrpc
