#pragma once

#include <memory>
#include <span>
#include <string_view>

namespace xrpc {

/**
 * @brief One name/value label attached to a metric sample.
 *
 * Labels are borrowed for the duration of a `MetricSink` call. Sink implementations must copy labels they retain after
 * returning.
 */
struct MetricLabel {
  /** @brief Label key such as `service`, `method`, or `status`. */
  std::string_view name_;

  /** @brief Label value borrowed from the caller for the duration of the sink call. */
  std::string_view value_;
};

/** @brief Borrowed span of labels attached to one metric operation. */
using MetricLabels = std::span<const MetricLabel>;

/**
 * @brief Process-wide metric sink interface used by server, client, and exporter code.
 *
 * Design note:
 * - Ownership: the process-wide sink is shared_ptr-managed so exporters can install or remove a sink while request
 *   paths only borrow it briefly.
 * - Labels: `MetricLabels` is a borrowed span; sinks must copy labels that outlive the call.
 * - Hot path: `MetricsEnabled()` lets callers skip expensive label construction when no sink is active.
 */
class MetricSink {
 public:
  virtual ~MetricSink() = default;

  /**
   * @brief Adds `value` to a monotonically increasing counter series.
   *
   * @param name Metric name.
   * @param value Counter delta.
   * @param labels Borrowed labels for this sample.
   */
  virtual void AddCounter(std::string_view name, double value, MetricLabels labels) = 0;

  /**
   * @brief Sets a gauge series to `value`.
   *
   * @param name Metric name.
   * @param value Current gauge value.
   * @param labels Borrowed labels for this sample.
   */
  virtual void SetGauge(std::string_view name, double value, MetricLabels labels) = 0;

  /**
   * @brief Records one histogram observation.
   *
   * @param name Metric name.
   * @param value Observed value.
   * @param labels Borrowed labels for this sample.
   */
  virtual void ObserveHistogram(std::string_view name, double value, MetricLabels labels) = 0;
};

/**
 * @brief Installs or replaces the process-wide metric sink.
 *
 * @param sink Shared sink implementation, or null to disable metrics.
 */
void SetMetricSink(std::shared_ptr<MetricSink> sink);

/** @brief Removes the process-wide metric sink. */
void ResetMetricSink();

/** @return true when a metric sink is installed. */
[[nodiscard]] auto MetricsEnabled() -> bool;

/** @return Shared ownership of the current metric sink, or null when metrics are disabled. */
[[nodiscard]] auto GetMetricSink() -> std::shared_ptr<MetricSink>;

}  // namespace xrpc
