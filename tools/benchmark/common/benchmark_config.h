#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include <xrpc/rpc_server.h>

namespace xrpc::benchmark {

// Workload controls the server/client serialization path under test.
enum class BenchmarkWorkload : std::uint8_t {
  Protobuf,
};

// Client mode separates server-capacity load generation from the production client path.
enum class BenchmarkClientMode : std::uint8_t {
  Firehose,
  RpcClient,
};

// Shared client benchmark configuration for firehose and production-client runs.
struct BenchmarkConfig {
  std::size_t payload_size_ = 64;
  std::string host_ = "127.0.0.1";
  std::uint16_t port_ = 9010;
  BenchmarkClientMode client_mode_ = BenchmarkClientMode::Firehose;
  std::size_t client_threads_ = 1;
  std::size_t firehose_connections_ = 1;
  std::uint64_t duration_s_ = 0;
  BenchmarkWorkload workload_ = BenchmarkWorkload::Protobuf;
  std::size_t firehose_inflight_ = 0;
  std::size_t firehose_io_threads_ = 0;
};

// Server benchmark configuration mirrors RpcServerOptions while keeping
// benchmark-only artificial delay separate.
struct BenchmarkServerConfig {
  std::string host_ = "127.0.0.1";
  std::uint16_t port_ = 9010;
  std::uint64_t server_delay_us_ = 0;
  BenchmarkWorkload workload_ = BenchmarkWorkload::Protobuf;
  RpcServerOptions server_options_;
};

[[nodiscard]] auto ParseBenchmarkClientConfig(int argc, char **argv) -> BenchmarkConfig;
[[nodiscard]] auto ParseBenchmarkServerConfig(int argc, char **argv) -> BenchmarkServerConfig;
[[nodiscard]] auto ParseBenchmarkClientMode(std::string_view value) -> BenchmarkClientMode;
[[nodiscard]] auto ParseBenchmarkWorkload(std::string_view value) -> BenchmarkWorkload;
[[nodiscard]] auto ToString(BenchmarkClientMode mode) -> std::string_view;
[[nodiscard]] auto ToString(BenchmarkWorkload workload) -> std::string_view;
[[nodiscard]] auto ClientUsage(const char *program) -> std::string;
[[nodiscard]] auto ServerUsage(const char *program) -> std::string;

}  // namespace xrpc::benchmark
