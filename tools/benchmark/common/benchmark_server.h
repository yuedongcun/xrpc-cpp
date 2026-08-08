#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "common/benchmark_config.h"

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
  std::unique_ptr<RpcServer> server_;
  std::uint64_t delay_us_;
  std::size_t listen_backlog_;
  std::jthread server_thread_;
  bool started_ = false;
};

}  // namespace xrpc::benchmark
