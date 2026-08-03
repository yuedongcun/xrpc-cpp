#include <gtest/gtest.h>

#include <array>
#include <string>

#include "observability/prometheus_metric_sink.h"

namespace {

[[nodiscard]] auto Contains(const std::string &text, const std::string &needle) -> bool {
  return text.find(needle) != std::string::npos;
}

}  // namespace

TEST(PrometheusMetricSinkTest, RendersCountersAndGauges) {
  xrpc::PrometheusMetricSink sink;
  const std::array<xrpc::MetricLabel, 2> labels{{
      {.name_ = "service", .value_ = "EchoService"},
      {.name_ = "method", .value_ = "Echo"},
  }};

  sink.AddCounter("xrpc_test_requests_total", 1.0, labels);
  sink.AddCounter("xrpc_test_requests_total", 2.0, labels);
  sink.SetGauge("xrpc_test_inflight", 7.0, labels);

  const std::string output = sink.Render();

  EXPECT_TRUE(Contains(output, "# TYPE xrpc_test_requests_total counter\n"));
  EXPECT_TRUE(Contains(output, "xrpc_test_requests_total{method=\"Echo\",service=\"EchoService\"} 3\n"));
  EXPECT_TRUE(Contains(output, "# TYPE xrpc_test_inflight gauge\n"));
  EXPECT_TRUE(Contains(output, "xrpc_test_inflight{method=\"Echo\",service=\"EchoService\"} 7\n"));
}

TEST(PrometheusMetricSinkTest, RendersHistogramBuckets) {
  xrpc::PrometheusMetricSink sink;
  const std::array<xrpc::MetricLabel, 1> labels{{
      {.name_ = "status_code", .value_ = "ok"},
  }};

  sink.ObserveHistogram("xrpc_test_latency_seconds", 0.001, labels);
  sink.ObserveHistogram("xrpc_test_latency_seconds", 0.02, labels);

  const std::string output = sink.Render();

  EXPECT_TRUE(Contains(output, "# TYPE xrpc_test_latency_seconds histogram\n"));
  EXPECT_TRUE(Contains(output, "xrpc_test_latency_seconds_bucket{status_code=\"ok\",le=\"0.001\"} 1\n"));
  EXPECT_TRUE(Contains(output, "xrpc_test_latency_seconds_bucket{status_code=\"ok\",le=\"0.025\"} 2\n"));
  EXPECT_TRUE(Contains(output, "xrpc_test_latency_seconds_bucket{status_code=\"ok\",le=\"+Inf\"} 2\n"));
  EXPECT_TRUE(Contains(output, "xrpc_test_latency_seconds_sum{status_code=\"ok\"} 0.021"));
  EXPECT_TRUE(Contains(output, "xrpc_test_latency_seconds_count{status_code=\"ok\"} 2\n"));
}

TEST(PrometheusMetricSinkTest, EscapesLabelValues) {
  xrpc::PrometheusMetricSink sink;
  const std::array<xrpc::MetricLabel, 1> labels{{
      {.name_ = "message", .value_ = "quote\" slash\\ newline\n"},
  }};

  sink.AddCounter("xrpc_test_escaped_total", 1.0, labels);

  const std::string output = sink.Render();

  EXPECT_TRUE(Contains(output, "message=\"quote\\\" slash\\\\ newline\\n\""));
}
