#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <xrpc/metrics.h>

#include "observability/rpc_metrics.h"

namespace {

struct RecordedLabel {
  std::string name_;
  std::string value_;
};

struct RecordedMetric {
  std::string kind_;
  std::string name_;
  double value_ = 0.0;
  std::vector<RecordedLabel> labels_;
};

class RecordingMetricSink final : public xrpc::MetricSink {
 public:
  void AddCounter(std::string_view name, double value, xrpc::MetricLabels labels) override {
    Record("counter", name, value, labels);
  }

  void SetGauge(std::string_view name, double value, xrpc::MetricLabels labels) override {
    Record("gauge", name, value, labels);
  }

  void ObserveHistogram(std::string_view name, double value, xrpc::MetricLabels labels) override {
    Record("histogram", name, value, labels);
  }

  [[nodiscard]] auto events() const -> std::vector<RecordedMetric> {
    std::lock_guard lock(mutex_);
    return events_;
  }

 private:
  void Record(std::string_view kind, std::string_view name, double value, xrpc::MetricLabels labels) {
    RecordedMetric event;
    event.kind_ = kind;
    event.name_ = name;
    event.value_ = value;
    event.labels_.reserve(labels.size());
    for (const xrpc::MetricLabel &label : labels) {
      event.labels_.push_back(RecordedLabel{.name_ = std::string(label.name_), .value_ = std::string(label.value_)});
    }

    std::lock_guard lock(mutex_);
    events_.push_back(std::move(event));
  }

  mutable std::mutex mutex_;
  std::vector<RecordedMetric> events_;
};

class MetricsTest : public testing::Test {
 protected:
  void TearDown() override { xrpc::ResetMetricSink(); }
};

[[nodiscard]] auto FindMetric(const std::vector<RecordedMetric> &events, std::string_view name)
    -> const RecordedMetric * {
  for (const RecordedMetric &event : events) {
    if (event.name_ == name) {
      return &event;
    }
  }
  return nullptr;
}

[[nodiscard]] auto HasLabel(const RecordedMetric &event, std::string_view name, std::string_view value) -> bool {
  for (const RecordedLabel &label : event.labels_) {
    if (label.name_ == name && label.value_ == value) {
      return true;
    }
  }
  return false;
}

}  // namespace

TEST_F(MetricsTest, DefaultSinkIsDisabled) {
  xrpc::ResetMetricSink();

  EXPECT_FALSE(xrpc::MetricsEnabled());
  EXPECT_EQ(xrpc::GetMetricSink(), nullptr);

  xrpc::RecordServerBackpressureRejected("global_pending");
  EXPECT_FALSE(xrpc::MetricsEnabled());
}

TEST_F(MetricsTest, SetAndResetSinkControlsRecording) {
  const auto sink = std::make_shared<RecordingMetricSink>();
  xrpc::SetMetricSink(sink);

  ASSERT_TRUE(xrpc::MetricsEnabled());
  ASSERT_EQ(xrpc::GetMetricSink(), sink);

  xrpc::RecordClientRpcCompleted("EchoService", "Echo", xrpc::StatusCode::Ok);

  const std::vector<RecordedMetric> events = sink->events();
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events[0].kind_, "counter");
  EXPECT_EQ(events[0].name_, "xrpc_client_rpc_completed_total");
  EXPECT_DOUBLE_EQ(events[0].value_, 1.0);
  EXPECT_TRUE(HasLabel(events[0], "service", "EchoService"));
  EXPECT_TRUE(HasLabel(events[0], "method", "Echo"));
  EXPECT_TRUE(HasLabel(events[0], "status_code", "ok"));

  xrpc::ResetMetricSink();
  EXPECT_FALSE(xrpc::MetricsEnabled());
  EXPECT_EQ(xrpc::GetMetricSink(), nullptr);
}

TEST_F(MetricsTest, RpcMetricsUseExpectedNamesAndLabels) {
  const auto sink = std::make_shared<RecordingMetricSink>();
  xrpc::SetMetricSink(sink);

  xrpc::RecordClientRpcLatency("EchoService", "Echo", xrpc::StatusCode::DeadlineExceeded,
                               std::chrono::milliseconds(25));
  xrpc::RecordClientRpcFailoverBlocked("EchoService", "Echo", xrpc::StatusCode::Unavailable,
                                       xrpc::RequestCommitState::MaybeSent);
  xrpc::RecordServerBackpressureRejected("inflight_limit");
  xrpc::RecordServerConnectionClosed("backpressure");
  xrpc::RecordServerWorkerJob("submitted");
  xrpc::RecordServerWorkerPendingJobs(3);
  xrpc::RecordServerRpcFailed("EchoService", "Echo", xrpc::StatusCode::Internal);

  const std::vector<RecordedMetric> events = sink->events();
  ASSERT_EQ(events.size(), 7U);

  const RecordedMetric *latency = FindMetric(events, "xrpc_client_rpc_latency_seconds");
  ASSERT_NE(latency, nullptr);
  EXPECT_EQ(latency->kind_, "histogram");
  EXPECT_DOUBLE_EQ(latency->value_, 0.025);
  EXPECT_TRUE(HasLabel(*latency, "status_code", "deadline_exceeded"));

  const RecordedMetric *failover_blocked = FindMetric(events, "xrpc_client_rpc_failover_blocked_total");
  ASSERT_NE(failover_blocked, nullptr);
  EXPECT_TRUE(HasLabel(*failover_blocked, "commit_state", "maybe_sent"));

  const RecordedMetric *backpressure = FindMetric(events, "xrpc_server_backpressure_rejected_total");
  ASSERT_NE(backpressure, nullptr);
  EXPECT_TRUE(HasLabel(*backpressure, "reason", "inflight_limit"));

  const RecordedMetric *connection_closed = FindMetric(events, "xrpc_server_connection_closed_total");
  ASSERT_NE(connection_closed, nullptr);
  EXPECT_TRUE(HasLabel(*connection_closed, "reason", "backpressure"));

  const RecordedMetric *worker_jobs = FindMetric(events, "xrpc_server_worker_jobs_total");
  ASSERT_NE(worker_jobs, nullptr);
  EXPECT_TRUE(HasLabel(*worker_jobs, "state", "submitted"));

  const RecordedMetric *worker_pending = FindMetric(events, "xrpc_server_worker_pending_jobs");
  ASSERT_NE(worker_pending, nullptr);
  EXPECT_EQ(worker_pending->kind_, "gauge");
  EXPECT_DOUBLE_EQ(worker_pending->value_, 3.0);

  const RecordedMetric *server_failed = FindMetric(events, "xrpc_server_rpc_failed_total");
  ASSERT_NE(server_failed, nullptr);
  EXPECT_TRUE(HasLabel(*server_failed, "status_code", "internal"));
}
