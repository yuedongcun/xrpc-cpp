#include "common/benchmark_server.h"

#include <chrono>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>

#include "common/task.h"
#include "io/uring_context.h"
#include "proto/echo.pb.h"
#include "rpc/server/server_config.h"
#include "transport/tcp_server.h"
#include "transport/thread_pool_executor.h"

namespace xrpc::benchmark {
namespace {

auto ResolveWorkerCount(std::size_t worker_threads) -> std::size_t {
  if (worker_threads > 0) {
    return worker_threads;
  }

  const auto hardware_threads = std::thread::hardware_concurrency();
  return hardware_threads == 0 ? 1U : static_cast<std::size_t>(hardware_threads);
}

}  // namespace

struct BenchmarkServer::RawRuntime final {
  // The raw benchmark bypasses RpcServer's typed service registry but still uses
  // the same io_uring server, executor, and backpressure components.
  RawRuntime(RawHandler handler, const RpcServerOptions &options)
      : config_(NormalizeServerOptions(options)),
        executor_(ResolveWorkerCount(config_.worker_threads_), config_.max_pending_jobs_global_),
        server_(context_, std::move(handler), executor_,
                ServerBackpressureLimits{
                    .max_inflight_per_connection_ = config_.max_inflight_per_connection_,
                    .max_write_queue_bytes_per_connection_ = config_.max_write_queue_bytes_per_connection_,
                },
                config_.connection_io_threads_, config_.protocol_limits_, config_.connection_idle_timeout_),
        listen_backlog_(config_.listen_backlog_) {}

  ~RawRuntime() { Stop(); }

  RawRuntime(const RawRuntime &) = delete;
  auto operator=(const RawRuntime &) -> RawRuntime & = delete;

  void Listen(const std::string &host, std::uint16_t port) { server_.Listen(host, port, listen_backlog_); }

  void Run() {
    server_task_.emplace(server_.Run());
    context_.Post([this]() { server_task_->Start(); });
    context_.Run();
    if (server_task_.has_value()) {
      server_task_->Wait();
      server_task_->Result();
    }
  }

  void Stop() {
    context_.Post([this]() { server_.Stop(); });
    context_.Stop();
  }

  [[nodiscard]] auto port() const -> std::uint16_t { return server_.port(); }

  [[nodiscard]] auto stats() const -> RpcServerStats {
    const ServerBackpressureSnapshot backpressure_snapshot = server_.stats();
    const ThreadPoolExecutorSnapshot executor_snapshot = executor_.stats();
    return RpcServerStats{
        .rejected_by_inflight_limit_ = backpressure_snapshot.rejected_by_inflight_limit_,
        .rejected_by_global_pending_limit_ = backpressure_snapshot.rejected_by_global_pending_limit_,
        .closed_by_write_queue_high_watermark_ = backpressure_snapshot.closed_by_write_queue_high_watermark_,
        .max_observed_inflight_ = backpressure_snapshot.max_observed_inflight_,
        .max_observed_write_queue_bytes_ = backpressure_snapshot.max_observed_write_queue_bytes_,
        .worker_jobs_rejected_ = executor_snapshot.rejected_jobs_,
        .max_observed_worker_queue_depth_ = executor_snapshot.max_observed_worker_queue_depth_,
    };
  }

  ServerConfig config_;
  io::UringContext context_;
  ThreadPoolExecutor executor_;
  TcpServer server_;
  std::optional<runtime::Task<void>> server_task_;
  std::size_t listen_backlog_ = 0;
};

auto MakeRawEchoHandler(std::uint64_t delay_us) -> RawHandler {
  return [delay_us](RawRequest request) -> RawResponse {
    if (delay_us > 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
    }

    RawResponse response;
    response.request_id_ = request.request_id_;
    response.payload_ = std::move(request.payload_);
    return response;
  };
}

auto MakeTypedEchoHandler(std::uint64_t delay_us) {
  return [delay_us](const xrpc::benchmark::EchoRequest &request) -> xrpc::benchmark::EchoResponse {
    if (delay_us > 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
    }
    xrpc::benchmark::EchoResponse response;
    response.set_message(request.message());
    return response;
  };
}

BenchmarkServer::BenchmarkServer(const BenchmarkServerConfig &config)
    : delay_us_(config.server_delay_us_), listen_backlog_(config.server_options_.listen_backlog_) {
  if (config.workload_ == BenchmarkWorkload::Raw) {
    RawHandler handler = MakeRawEchoHandler(delay_us_);
    raw_runtime_ = std::make_unique<RawRuntime>(std::move(handler), config.server_options_);
    return;
  }

  server_ = std::make_unique<RpcServer>(config.server_options_);
  server_->RegisterMethod<xrpc::benchmark::EchoRequest, xrpc::benchmark::EchoResponse>("BenchmarkService", "Echo",
                                                                                       MakeTypedEchoHandler(delay_us_));
}

BenchmarkServer::~BenchmarkServer() { Stop(); }

void BenchmarkServer::Start(const std::string &host, std::uint16_t port) {
  if (started_) {
    return;
  }
  if (raw_runtime_) {
    raw_runtime_->Listen(host, port);
    server_thread_ = std::jthread([this]() { raw_runtime_->Run(); });
  } else {
    server_->Listen(host, port);
    server_thread_ = std::jthread([this]() { server_->Run(); });
  }
  started_ = true;
}

void BenchmarkServer::Stop() {
  if (!started_) {
    return;
  }
  if (raw_runtime_) {
    raw_runtime_->Stop();
  } else {
    server_->Stop();
  }
  server_thread_.request_stop();
  if (server_thread_.joinable()) {
    server_thread_.join();
  }
  started_ = false;
}

auto BenchmarkServer::port() const -> std::uint16_t {
  if (raw_runtime_) {
    return raw_runtime_->port();
  }
  return server_->port();
}

auto BenchmarkServer::stats() const -> RpcServerStats {
  if (raw_runtime_) {
    return raw_runtime_->stats();
  }
  return server_->stats();
}

}  // namespace xrpc::benchmark
