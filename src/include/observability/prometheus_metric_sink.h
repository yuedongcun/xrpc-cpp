#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include <xrpc/metrics.h>

namespace xrpc {

// Design note:
// - Ownership: the sink stores its own copy of metric labels because MetricLabels
//   are borrowed spans.
// - Threading: one mutex protects all series; metrics are optional, so simple
//   correctness is preferred over high-cardinality write throughput.
// - Ordering: std::map plus sorted labels produce stable Prometheus text output.
class PrometheusMetricSink final : public MetricSink {
 public:
  /** @brief Adds to a counter series identified by name and labels. */
  void AddCounter(std::string_view name, double value, MetricLabels labels) override;

  /** @brief Sets a gauge series identified by name and labels. */
  void SetGauge(std::string_view name, double value, MetricLabels labels) override;

  /** @brief Observes one histogram value for the series identified by name and labels. */
  void ObserveHistogram(std::string_view name, double value, MetricLabels labels) override;

  /** @return Prometheus text exposition for all stored series. */
  [[nodiscard]] auto Render() const -> std::string;

  /**
   * @brief Stored metric label sorted as part of series identity.
   *
   * Labels are sorted as part of `SeriesKey` ordering so equivalent label sets produce stable Prometheus output and map
   * lookups.
   */
  struct Label {
    std::string name_;
    std::string value_;

    /** @return Strict ordering by name, then value. */
    [[nodiscard]] auto operator<(const Label &other) const -> bool {
      if (name_ != other.name_) {
        return name_ < other.name_;
      }
      return value_ < other.value_;
    }
  };

 private:
  /** @brief Metric name plus sorted labels used as map key. */
  struct SeriesKey {
    std::string name_;
    std::vector<Label> labels_;

    /** @return Strict ordering by metric name and sorted labels. */
    [[nodiscard]] auto operator<(const SeriesKey &other) const -> bool {
      if (name_ != other.name_) {
        return name_ < other.name_;
      }

      std::size_t index = 0;
      while (index < labels_.size() && index < other.labels_.size()) {
        if (labels_[index] < other.labels_[index]) {
          return true;
        }
        if (other.labels_[index] < labels_[index]) {
          return false;
        }
        ++index;
      }
      return labels_.size() < other.labels_.size();
    }
  };

  struct CounterSeries {
    std::vector<Label> labels_;
    double value_ = 0.0;
  };

  struct GaugeSeries {
    std::vector<Label> labels_;
    double value_ = 0.0;
  };

  struct HistogramSeries {
    std::vector<Label> labels_;
    std::vector<std::uint64_t> bucket_counts_;
    double sum_ = 0.0;
    std::uint64_t count_ = 0;
  };

  /** @brief Copies borrowed labels and builds the stable map key for one series. */
  [[nodiscard]] static auto MakeKey(std::string_view name, MetricLabels labels) -> SeriesKey;

  mutable std::mutex mutex_;
  std::map<SeriesKey, CounterSeries> counters_;
  std::map<SeriesKey, GaugeSeries> gauges_;
  std::map<SeriesKey, HistogramSeries> histograms_;
};

}  // namespace xrpc
