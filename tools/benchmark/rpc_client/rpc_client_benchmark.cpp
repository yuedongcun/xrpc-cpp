#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <exception>
#include <latch>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <xrpc/rpc_client.h>

#include "benchmark_stats.h"
#include "proto/echo.pb.h"

namespace xrpc::benchmark {

namespace {

constexpr std::string_view SERVICE_NAME = "BenchmarkService";
constexpr std::string_view METHOD_NAME = "Echo";
constexpr auto CALL_TIMEOUT = std::chrono::seconds(5);

struct ClientConfig final {
  std::string host_ = "127.0.0.1";
  std::uint16_t port_ = 9010;
  std::uint64_t duration_s_ = 0;
  std::size_t payload_size_ = 64;
  std::size_t threads_ = 1;
};

struct RpcClientWorker final {
  BenchmarkRecorder recorder_;
  std::jthread thread_;
  std::exception_ptr exception_;
};

auto ParseUnsigned(std::string_view value, const char *name) -> std::uint64_t {
  std::uint64_t result = 0;
  const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
  if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
    throw std::invalid_argument(std::string("invalid value for ") + name);
  }
  return result;
}

void RequireKeyValue(std::string_view arg) {
  if (!arg.starts_with("--") || arg.find('=') == std::string_view::npos) {
    throw std::invalid_argument("arguments must use --key=value format");
  }
}

void ParseArg(ClientConfig &config, std::string_view arg) {
  RequireKeyValue(arg);
  const std::size_t eq = arg.find('=');
  const std::string_view key = arg.substr(2, eq - 2);
  const std::string_view value = arg.substr(eq + 1);

  if (key == "host") {
    config.host_ = std::string(value);
  } else if (key == "port") {
    config.port_ = static_cast<std::uint16_t>(ParseUnsigned(value, "port"));
  } else if (key == "duration_s") {
    config.duration_s_ = ParseUnsigned(value, "duration_s");
  } else if (key == "payload_size") {
    config.payload_size_ = static_cast<std::size_t>(ParseUnsigned(value, "payload_size"));
  } else if (key == "threads") {
    config.threads_ = static_cast<std::size_t>(ParseUnsigned(value, "threads"));
  } else {
    throw std::invalid_argument(std::string("unknown argument: --") + std::string(key));
  }
}

auto ParseConfig(int argc, char **argv) -> ClientConfig {
  ClientConfig config;
  for (int i = 1; i < argc; ++i) {
    ParseArg(config, argv[i]);
  }
  if (config.port_ == 0) {
    throw std::invalid_argument("port must be greater than 0");
  }
  if (config.duration_s_ == 0) {
    throw std::invalid_argument("duration_s must be greater than 0");
  }
  if (config.payload_size_ == 0) {
    throw std::invalid_argument("payload_size must be greater than 0");
  }
  if (config.threads_ == 0) {
    throw std::invalid_argument("threads must be greater than 0");
  }
  return config;
}

auto Usage(const char *program) -> std::string {
  return std::string("Usage: ") + program + " --host=IP --port=N --duration_s=N --payload_size=N --threads=N";
}

auto MakeClient(const ClientConfig &config) -> std::unique_ptr<RpcClient> {
  RpcClientOptions options;
  options.target_ = "list://" + config.host_ + ":" + std::to_string(config.port_);
  options.timeout_ = std::chrono::duration_cast<std::chrono::milliseconds>(CALL_TIMEOUT);
  options.max_inflight_per_endpoint_ = std::max<std::size_t>(1024, config.threads_);

  auto client = std::make_unique<RpcClient>(options);
  const Status init_status = client->Init();
  if (!init_status.ok()) {
    throw std::runtime_error("RpcClient initialization failed: " + init_status.message());
  }
  return client;
}

void WarmClient(RpcClient &client, const std::string &message) {
  EchoRequest request;
  request.set_message(message);
  StatusOr<EchoResponse> result =
      client.Call<EchoResponse>(std::string(SERVICE_NAME), std::string(METHOD_NAME), request);
  if (!result.ok()) {
    throw std::runtime_error("RpcClient warmup call failed: " + result.status().message());
  }
  if (result.value().message() != message) {
    throw std::runtime_error("RpcClient warmup response payload mismatch");
  }
}

void RunWorker(RpcClient &client, std::string message, std::latch &start_latch, std::latch &ready_latch,
               const std::chrono::steady_clock::time_point &deadline, BenchmarkRecorder &recorder) {
  EchoRequest request;
  request.set_message(message);
  ready_latch.count_down();
  start_latch.wait();

  while (true) {
    const auto started_at = std::chrono::steady_clock::now();
    if (started_at >= deadline) {
      return;
    }

    StatusOr<EchoResponse> result =
        client.Call<EchoResponse>(std::string(SERVICE_NAME), std::string(METHOD_NAME), request);
    const auto finished_at = std::chrono::steady_clock::now();
    const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(finished_at - started_at);
    if (result.ok() && result.value().message() == message) {
      recorder.RecordSuccess(latency);
    } else {
      recorder.RecordFailure(latency);
    }
  }
}

}  // namespace

auto RunRpcClientBenchmark(const ClientConfig &config) -> BenchmarkStats {
  const std::string message(config.payload_size_, 'x');
  std::unique_ptr<RpcClient> client = MakeClient(config);
  WarmClient(*client, message);

  std::vector<RpcClientWorker> workers(config.threads_);
  std::latch ready_latch(config.threads_);
  std::latch start_latch(1);
  std::chrono::steady_clock::time_point deadline;

  for (RpcClientWorker &worker : workers) {
    RpcClientWorker *worker_ptr = &worker;
    worker.thread_ = std::jthread([&client, &message, &start_latch, &ready_latch, &deadline, worker_ptr]() {
      try {
        RunWorker(*client, message, start_latch, ready_latch, deadline, worker_ptr->recorder_);
      } catch (...) {
        worker_ptr->exception_ = std::current_exception();
      }
    });
  }

  ready_latch.wait();
  const auto wall_start = std::chrono::steady_clock::now();
  deadline = wall_start + std::chrono::seconds(config.duration_s_);
  start_latch.count_down();
  for (RpcClientWorker &worker : workers) {
    if (worker.thread_.joinable()) {
      worker.thread_.join();
    }
    if (worker.exception_ != nullptr) {
      std::rethrow_exception(worker.exception_);
    }
  }
  const auto wall_end = std::chrono::steady_clock::now();

  BenchmarkRecorder aggregate;
  for (RpcClientWorker &worker : workers) {
    aggregate.MergeFrom(std::move(worker.recorder_));
  }
  return aggregate.Finalize(std::chrono::duration_cast<std::chrono::nanoseconds>(wall_end - wall_start));
}

}  // namespace xrpc::benchmark

auto main(int argc, char **argv) -> int {
  try {
    const xrpc::benchmark::ClientConfig config = xrpc::benchmark::ParseConfig(argc, argv);
    std::printf("client=rpc_client host=%s port=%u duration_s=%llu payload_size=%zu threads=%zu\n",
                config.host_.c_str(), config.port_, static_cast<unsigned long long>(config.duration_s_),
                config.payload_size_, config.threads_);
    const xrpc::benchmark::BenchmarkStats stats = xrpc::benchmark::RunRpcClientBenchmark(config);
    xrpc::benchmark::PrintStats(stats);
    return 0;
  } catch (const std::exception &ex) {
    std::fprintf(stderr, "%s\n%s\n", ex.what(), xrpc::benchmark::Usage(argv[0]).c_str());
    return 1;
  }
}
