#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "common/benchmark_config.h"

namespace xrpc {
class PrometheusExporter;
}  // namespace xrpc

namespace xrpc::benchmark {

class BenchmarkServer final {
 public:
  explicit BenchmarkServer(const BenchmarkServerConfig &config);
  ~BenchmarkServer();

  BenchmarkServer(const BenchmarkServer &) = delete;
  auto operator=(const BenchmarkServer &) -> BenchmarkServer & = delete;

  void Start(const std::string &host, std::uint16_t port);
  void Stop();
  [[nodiscard]] auto port() const -> std::uint16_t;
  [[nodiscard]] auto stats() const -> RpcServerStats;

 private:
  // RawRuntime is benchmark-only glue for the raw protocol workload. It avoids
  // restoring broader production abstractions just to run capacity tests.
  struct RawRuntime;

  std::unique_ptr<RpcServer> server_;
  std::unique_ptr<RawRuntime> raw_runtime_;
  std::unique_ptr<PrometheusExporter> metrics_exporter_;
  std::string metrics_host_;
  std::uint16_t metrics_port_;
  std::uint64_t delay_us_;
  std::size_t listen_backlog_;
  std::jthread server_thread_;
  bool started_ = false;
};

}  // namespace xrpc::benchmark
