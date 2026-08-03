#include "common/rpc_client_benchmark.h"

#include <algorithm>
#include <chrono>
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

#include "proto/echo.pb.h"

namespace xrpc::benchmark {

namespace {

constexpr std::string_view SERVICE_NAME = "BenchmarkService";
constexpr std::string_view METHOD_NAME = "Echo";
constexpr auto CALL_TIMEOUT = std::chrono::seconds(5);

struct RpcClientWorker final {
  BenchmarkRecorder recorder_;
  std::jthread thread_;
  std::exception_ptr exception_;
};

auto MakeClient(const BenchmarkConfig &config) -> std::unique_ptr<RpcClient> {
  RpcClientOptions options;
  options.target_ = "list://" + config.host_ + ":" + std::to_string(config.port_);
  options.timeout_ = std::chrono::duration_cast<std::chrono::milliseconds>(CALL_TIMEOUT);
  options.max_inflight_per_endpoint_ = std::max<std::size_t>(1024, config.client_threads_);

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

auto RunRpcClientBenchmark(const BenchmarkConfig &config) -> BenchmarkStats {
  const std::string message(config.payload_size_, 'x');
  std::unique_ptr<RpcClient> client = MakeClient(config);
  WarmClient(*client, message);

  std::vector<RpcClientWorker> workers(config.client_threads_);
  std::latch ready_latch(config.client_threads_);
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
