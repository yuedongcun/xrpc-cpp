#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include <xrpc/metrics.h>
#include <xrpc/prometheus_exporter.h>

#include "io/socket.h"
#include "observability/rpc_metrics.h"

namespace {

[[nodiscard]] auto HttpGet(std::uint16_t port, const std::string &path) -> std::string {
  xrpc::io::Socket socket;
  socket.Connect("127.0.0.1", port, std::chrono::milliseconds(1000));
  socket.SetReadWriteTimeout(std::chrono::milliseconds(1000));
  socket.WriteAll("GET " + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");

  std::string response;
  char buffer[1024];
  while (true) {
    const ssize_t received = socket.Read(buffer, sizeof(buffer));
    if (received == 0) {
      return response;
    }
    response.append(buffer, static_cast<std::size_t>(received));
  }
}

[[nodiscard]] auto Contains(const std::string &text, const std::string &needle) -> bool {
  return text.find(needle) != std::string::npos;
}

}  // namespace

TEST(PrometheusExporterTest, ExposesMetricsOverHttp) {
  xrpc::PrometheusExporterOptions options;
  options.port_ = 0;
  xrpc::PrometheusExporter exporter(options);

  const xrpc::Status status = exporter.Start();
  ASSERT_TRUE(status.ok()) << status.message();
  ASSERT_NE(exporter.port(), 0);
  ASSERT_TRUE(xrpc::MetricsEnabled());

  xrpc::RecordServerBackpressureRejected("global_pending");
  xrpc::RecordServerConnectionClosed("backpressure");
  xrpc::RecordServerWorkerJob("submitted");
  xrpc::RecordServerWorkerPendingJobs(2);

  const std::string response = HttpGet(exporter.port(), "/metrics");

  EXPECT_TRUE(Contains(response, "HTTP/1.1 200 OK\r\n"));
  EXPECT_TRUE(Contains(response, "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"));
  EXPECT_TRUE(Contains(response, "# TYPE xrpc_server_backpressure_rejected_total counter\n"));
  EXPECT_TRUE(Contains(response, "xrpc_server_backpressure_rejected_total{reason=\"global_pending\"} 1\n"));
  EXPECT_TRUE(Contains(response, "# TYPE xrpc_server_connection_closed_total counter\n"));
  EXPECT_TRUE(Contains(response, "xrpc_server_connection_closed_total{reason=\"backpressure\"} 1\n"));
  EXPECT_TRUE(Contains(response, "# TYPE xrpc_server_worker_jobs_total counter\n"));
  EXPECT_TRUE(Contains(response, "xrpc_server_worker_jobs_total{state=\"submitted\"} 1\n"));
  EXPECT_TRUE(Contains(response, "# TYPE xrpc_server_worker_pending_jobs gauge\n"));
  EXPECT_TRUE(Contains(response, "xrpc_server_worker_pending_jobs 2\n"));

  exporter.Stop();
  EXPECT_FALSE(xrpc::MetricsEnabled());
}

TEST(PrometheusExporterTest, ExposesReadinessEndpoint) {
  xrpc::PrometheusExporterOptions options;
  options.port_ = 0;
  xrpc::PrometheusExporter exporter(options);

  const xrpc::Status status = exporter.Start();
  ASSERT_TRUE(status.ok()) << status.message();

  const std::string response = HttpGet(exporter.port(), "/-/ready");

  EXPECT_TRUE(Contains(response, "HTTP/1.1 200 OK\r\n"));
  EXPECT_TRUE(Contains(response, "ready\n"));
}

TEST(PrometheusExporterTest, RejectsInvalidOptions) {
  xrpc::PrometheusExporterOptions options;
  options.metrics_path_ = "metrics";
  xrpc::PrometheusExporter exporter(options);

  const xrpc::Status status = exporter.Start();

  EXPECT_EQ(status.code(), xrpc::StatusCode::InvalidArgument);
}
