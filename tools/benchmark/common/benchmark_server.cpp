#include "common/benchmark_server.h"

#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>

#include "proto/echo.pb.h"

namespace xrpc::benchmark {
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
  server_ = std::make_unique<RpcServer>(config.server_options_);
  server_->RegisterMethod<xrpc::benchmark::EchoRequest, xrpc::benchmark::EchoResponse>("BenchmarkService", "Echo",
                                                                                       MakeTypedEchoHandler(delay_us_));
}

BenchmarkServer::~BenchmarkServer() { Stop(); }

void BenchmarkServer::Start(const std::string &host, std::uint16_t port) {
  if (started_) {
    return;
  }
  server_->Listen(host, port);
  server_thread_ = std::jthread([this]() { server_->Run(); });
  started_ = true;
}

void BenchmarkServer::Stop() {
  if (!started_) {
    return;
  }
  server_->Stop();
  server_thread_.request_stop();
  if (server_thread_.joinable()) {
    server_thread_.join();
  }
  started_ = false;
}

auto BenchmarkServer::port() const -> std::uint16_t {
  return server_->port();
}

auto BenchmarkServer::stats() const -> RpcServerStats {
  return server_->stats();
}

}  // namespace xrpc::benchmark
