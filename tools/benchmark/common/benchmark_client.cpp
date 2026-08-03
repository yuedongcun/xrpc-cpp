#include "common/benchmark_client.h"

#include <memory>
#include <stdexcept>
#include <utility>

#include <xrpc/prometheus_exporter.h>

#include "common/firehose_client.h"
#include "common/rpc_client_benchmark.h"

namespace xrpc::benchmark {

auto RunBenchmark(const BenchmarkConfig &config) -> BenchmarkStats {
  std::unique_ptr<PrometheusExporter> metrics_exporter;
  if (config.metrics_port_ != 0) {
    PrometheusExporterOptions metrics_options;
    metrics_options.host_ = config.metrics_host_;
    metrics_options.port_ = config.metrics_port_;
    metrics_exporter = std::make_unique<PrometheusExporter>(std::move(metrics_options));
    const Status status = metrics_exporter->Start();
    if (!status.ok()) {
      throw std::runtime_error("failed to start metrics exporter: " + status.message());
    }
  }

  if (config.client_mode_ == BenchmarkClientMode::RpcClient) {
    return RunRpcClientBenchmark(config);
  }
  return RunFirehoseBenchmark(config);
}

}  // namespace xrpc::benchmark
