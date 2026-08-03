#include <xrpc/metrics.h>

#include <atomic>
#include <mutex>
#include <utility>

namespace xrpc {
namespace {

/** @brief Serializes access to the process-wide metric sink pointer. */
std::mutex sink_mutex;

/** @brief Process-wide sink used by hot paths to publish metric samples. */
std::shared_ptr<MetricSink> active_sink;

/** @brief Fast-path flag that lets callers skip locking when metrics are disabled. */
std::atomic_bool sink_enabled{false};

}  // namespace

/**
 * @brief Installs or replaces the process-wide metric sink.
 *
 * The shared pointer is protected by a mutex, while `sink_enabled` gives hot metric paths a cheap
 * acquire-load test before they pay for locking or label construction.
 *
 * @param sink Sink implementation to install, or null to disable metrics.
 */
void SetMetricSink(std::shared_ptr<MetricSink> sink) {
  std::lock_guard lock(sink_mutex);
  active_sink = std::move(sink);
  sink_enabled.store(static_cast<bool>(active_sink), std::memory_order_release);
}

/** @brief Removes the process-wide metric sink. */
void ResetMetricSink() { SetMetricSink(nullptr); }

/** @return true when a metric sink is currently installed. */
auto MetricsEnabled() -> bool { return sink_enabled.load(std::memory_order_acquire); }

/**
 * @brief Returns shared ownership of the active metric sink.
 *
 * @return Installed sink, or null when metrics are disabled.
 */
auto GetMetricSink() -> std::shared_ptr<MetricSink> {
  std::lock_guard lock(sink_mutex);
  return active_sink;
}

}  // namespace xrpc
