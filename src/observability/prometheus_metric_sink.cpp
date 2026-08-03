#include "observability/prometheus_metric_sink.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>

namespace xrpc {
namespace {

/**
 * @brief Default latency histogram bucket upper bounds in seconds.
 *
 * The buckets cover sub-millisecond calls through multi-second tail latency while keeping the
 * exported series count small enough for embeddable tests and local deployments.
 */
constexpr std::array<double, 14> HISTOGRAM_BUCKETS_SECONDS{
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
};

/** @brief Prometheus `le` label text matching `HISTOGRAM_BUCKETS_SECONDS`. */
constexpr std::array<std::string_view, 14> HISTOGRAM_BUCKET_LABELS{
    "0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10",
};

/**
 * @brief Validates a floating-point metric sample.
 *
 * @param value Metric value provided by callers.
 * @return true when the value can be emitted in Prometheus text format.
 */
[[nodiscard]] auto IsUsableValue(double value) -> bool { return std::isfinite(value); }

/**
 * @brief Appends a Prometheus label value with text-format escaping.
 *
 * @param out Destination stream.
 * @param value Raw label value.
 */
void AppendEscaped(std::ostream &out, std::string_view value) {
  for (const char ch : value) {
    switch (ch) {
      case '\\':
        out << "\\\\";
        break;
      case '"':
        out << "\\\"";
        break;
      case '\n':
        out << "\\n";
        break;
      default:
        out << ch;
        break;
    }
  }
}

/**
 * @brief Appends a complete Prometheus label set.
 *
 * The optional extra label is used for histogram bucket `le` labels without mutating the stored
 * series label vector.
 *
 * @param out Destination stream.
 * @param labels Stored series labels sorted by name/value.
 * @param extra_name Optional additional label name.
 * @param extra_value Optional additional label value.
 */
void AppendLabels(std::ostream &out, const std::vector<PrometheusMetricSink::Label> &labels,
                  std::string_view extra_name = {}, std::string_view extra_value = {}) {
  if (labels.empty() && extra_name.empty()) {
    return;
  }

  out << '{';
  bool first = true;
  for (const PrometheusMetricSink::Label &label : labels) {
    if (!first) {
      out << ',';
    }
    first = false;
    out << label.name_ << "=\"";
    AppendEscaped(out, label.value_);
    out << '"';
  }
  if (!extra_name.empty()) {
    if (!first) {
      out << ',';
    }
    out << extra_name << "=\"";
    AppendEscaped(out, extra_value);
    out << '"';
  }
  out << '}';
}

/**
 * @brief Appends a metric value with enough precision for round-tripping.
 *
 * @param out Destination stream.
 * @param value Floating-point metric value.
 */
void AppendValue(std::ostream &out, double value) { out << std::setprecision(17) << value; }

/**
 * @brief Appends a Prometheus `# TYPE` line.
 *
 * @param out Destination stream.
 * @param name Metric name.
 * @param type Prometheus metric type text.
 */
void AppendTypeLine(std::ostream &out, std::string_view name, std::string_view type) {
  out << "# TYPE " << name << ' ' << type << '\n';
}

}  // namespace

/**
 * @brief Builds the stable map key for one metric series.
 *
 * Labels are copied and sorted so callers may pass labels in any order while the sink still merges
 * samples for the same logical series.
 *
 * @param name Metric name.
 * @param labels Borrowed labels supplied for this sample.
 * @return Owned series key suitable for map lookup.
 */
auto PrometheusMetricSink::MakeKey(std::string_view name, MetricLabels labels) -> SeriesKey {
  SeriesKey key;
  key.name_ = std::string(name);
  key.labels_.reserve(labels.size());
  for (const MetricLabel &label : labels) {
    key.labels_.push_back(Label{.name_ = std::string(label.name_), .value_ = std::string(label.value_)});
  }
  std::ranges::sort(key.labels_, [](const Label &lhs, const Label &rhs) { return lhs < rhs; });
  return key;
}

/**
 * @brief Adds a delta to a counter series.
 *
 * @param name Counter metric name.
 * @param value Counter delta. Non-finite values are ignored.
 * @param labels Labels identifying the counter series.
 */
void PrometheusMetricSink::AddCounter(std::string_view name, double value, MetricLabels labels) {
  if (!IsUsableValue(value)) {
    return;
  }

  std::lock_guard lock(mutex_);
  SeriesKey key = MakeKey(name, labels);
  CounterSeries &series = counters_[key];
  if (series.labels_.empty()) {
    series.labels_ = key.labels_;
  }
  series.value_ += value;
}

/**
 * @brief Sets a gauge series to its current value.
 *
 * @param name Gauge metric name.
 * @param value Gauge value. Non-finite values are ignored.
 * @param labels Labels identifying the gauge series.
 */
void PrometheusMetricSink::SetGauge(std::string_view name, double value, MetricLabels labels) {
  if (!IsUsableValue(value)) {
    return;
  }

  std::lock_guard lock(mutex_);
  SeriesKey key = MakeKey(name, labels);
  GaugeSeries &series = gauges_[key];
  if (series.labels_.empty()) {
    series.labels_ = key.labels_;
  }
  series.value_ = value;
}

/**
 * @brief Records one observation in a histogram series.
 *
 * Bucket counts are stored as non-cumulative values internally and converted to cumulative
 * Prometheus buckets during rendering.
 *
 * @param name Histogram metric name.
 * @param value Observation value in seconds. Non-finite values are ignored.
 * @param labels Labels identifying the histogram series.
 */
void PrometheusMetricSink::ObserveHistogram(std::string_view name, double value, MetricLabels labels) {
  if (!IsUsableValue(value)) {
    return;
  }

  std::lock_guard lock(mutex_);
  SeriesKey key = MakeKey(name, labels);
  HistogramSeries &series = histograms_[key];
  if (series.labels_.empty()) {
    series.labels_ = key.labels_;
  }
  if (series.bucket_counts_.empty()) {
    series.bucket_counts_.resize(HISTOGRAM_BUCKETS_SECONDS.size() + 1);
  }

  const auto bucket = std::ranges::lower_bound(HISTOGRAM_BUCKETS_SECONDS, value);
  const std::size_t bucket_index = bucket == HISTOGRAM_BUCKETS_SECONDS.end()
                                       ? HISTOGRAM_BUCKETS_SECONDS.size()
                                       : static_cast<std::size_t>(bucket - HISTOGRAM_BUCKETS_SECONDS.begin());
  ++series.bucket_counts_[bucket_index];
  series.sum_ += value;
  ++series.count_;
}

/**
 * @brief Renders all metric series in Prometheus text exposition format.
 *
 * @return Snapshot string suitable for an HTTP scrape response.
 */
auto PrometheusMetricSink::Render() const -> std::string {
  std::lock_guard lock(mutex_);
  std::ostringstream out;

  std::string current_metric;
  for (const auto &[key, series] : counters_) {
    if (key.name_ != current_metric) {
      current_metric = key.name_;
      AppendTypeLine(out, key.name_, "counter");
    }
    out << key.name_;
    AppendLabels(out, series.labels_);
    out << ' ';
    AppendValue(out, series.value_);
    out << '\n';
  }

  current_metric.clear();
  for (const auto &[key, series] : gauges_) {
    if (key.name_ != current_metric) {
      current_metric = key.name_;
      AppendTypeLine(out, key.name_, "gauge");
    }
    out << key.name_;
    AppendLabels(out, series.labels_);
    out << ' ';
    AppendValue(out, series.value_);
    out << '\n';
  }

  current_metric.clear();
  for (const auto &[key, series] : histograms_) {
    if (key.name_ != current_metric) {
      current_metric = key.name_;
      AppendTypeLine(out, key.name_, "histogram");
    }

    std::uint64_t cumulative = 0;
    for (std::size_t i = 0; i < HISTOGRAM_BUCKETS_SECONDS.size(); ++i) {
      cumulative += series.bucket_counts_[i];
      out << key.name_ << "_bucket";
      AppendLabels(out, series.labels_, "le", HISTOGRAM_BUCKET_LABELS[i]);
      out << ' ' << cumulative << '\n';
    }
    cumulative += series.bucket_counts_[HISTOGRAM_BUCKETS_SECONDS.size()];
    out << key.name_ << "_bucket";
    AppendLabels(out, series.labels_, "le", "+Inf");
    out << ' ' << cumulative << '\n';

    out << key.name_ << "_sum";
    AppendLabels(out, series.labels_);
    out << ' ';
    AppendValue(out, series.sum_);
    out << '\n';

    out << key.name_ << "_count";
    AppendLabels(out, series.labels_);
    out << ' ' << series.count_ << '\n';
  }

  return out.str();
}

}  // namespace xrpc
