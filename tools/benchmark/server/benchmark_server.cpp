#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include <xrpc/rpc_server.h>

#include "proto/echo.pb.h"

namespace xrpc::benchmark {
namespace {

constexpr std::string_view SERVICE_NAME = "BenchmarkService";
constexpr std::string_view METHOD_NAME = "Echo";

struct ServerConfig final {
  std::string host_ = "127.0.0.1";
  std::uint16_t port_ = 9010;
  std::uint64_t delay_us_ = 0;
  RpcServerOptions options_;
};

std::atomic<bool> stop_requested{false};

void HandleSignal(int signal) {
  (void)signal;
  stop_requested.store(true, std::memory_order_relaxed);
}

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

void ParseArg(ServerConfig &config, std::string_view arg) {
  RequireKeyValue(arg);
  const std::size_t eq = arg.find('=');
  const std::string_view key = arg.substr(2, eq - 2);
  const std::string_view value = arg.substr(eq + 1);

  if (key == "host") {
    config.host_ = std::string(value);
  } else if (key == "port") {
    config.port_ = static_cast<std::uint16_t>(ParseUnsigned(value, "port"));
  } else if (key == "delay_us") {
    config.delay_us_ = ParseUnsigned(value, "delay_us");
  } else if (key == "worker_threads") {
    config.options_.worker_threads_ = static_cast<std::size_t>(ParseUnsigned(value, "worker_threads"));
  } else if (key == "io_threads") {
    config.options_.connection_io_threads_ = static_cast<std::size_t>(ParseUnsigned(value, "io_threads"));
  } else if (key == "listen_backlog") {
    config.options_.listen_backlog_ = static_cast<std::size_t>(ParseUnsigned(value, "listen_backlog"));
  } else {
    throw std::invalid_argument(std::string("unknown argument: --") + std::string(key));
  }
}

auto ParseConfig(int argc, char **argv) -> ServerConfig {
  ServerConfig config;
  for (int i = 1; i < argc; ++i) {
    ParseArg(config, argv[i]);
  }
  if (config.options_.connection_io_threads_ == 0) {
    throw std::invalid_argument("io_threads must be greater than 0");
  }
  if (config.options_.listen_backlog_ == 0) {
    throw std::invalid_argument("listen_backlog must be greater than 0");
  }
  return config;
}

auto Usage(const char *program) -> std::string {
  return std::string("Usage: ") + program +
         " [--host=IP] [--port=N] [--delay_us=N] [--worker_threads=N] [--io_threads=N] [--listen_backlog=N]";
}

auto MakeEchoHandler(std::uint64_t delay_us) {
  return [delay_us](const EchoRequest &request) -> EchoResponse {
    if (delay_us > 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
    }
    EchoResponse response;
    response.set_message(request.message());
    return response;
  };
}

}  // namespace
}  // namespace xrpc::benchmark

auto main(int argc, char **argv) -> int {
  try {
    xrpc::benchmark::ServerConfig config = xrpc::benchmark::ParseConfig(argc, argv);
    xrpc::StatusOr<xrpc::RpcServer> server_result = xrpc::RpcServer::Create(config.options_);
    if (!server_result.ok()) {
      throw std::runtime_error(server_result.status().message());
    }
    xrpc::RpcServer server = std::move(server_result).value();
    xrpc::Status status = server.RegisterMethod<xrpc::benchmark::EchoRequest, xrpc::benchmark::EchoResponse>(
        std::string(xrpc::benchmark::SERVICE_NAME), std::string(xrpc::benchmark::METHOD_NAME),
        xrpc::benchmark::MakeEchoHandler(config.delay_us_));
    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }

    std::signal(SIGINT, xrpc::benchmark::HandleSignal);
    std::signal(SIGTERM, xrpc::benchmark::HandleSignal);

    status = server.Listen(config.host_, config.port_);
    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }
    std::jthread server_thread([&server]() { (void)server.Run(); });

    const xrpc::StatusOr<std::uint16_t> port_result = server.port();
    if (!port_result.ok()) {
      throw std::runtime_error(port_result.status().message());
    }

    std::printf(
        "ready host=%s port=%u worker_threads=%zu connection_io_threads=%zu delay_us=%llu "
        "max_inflight_per_connection=%zu max_write_queue_bytes_per_connection=%zu max_pending_jobs_global=%zu\n",
        config.host_.c_str(), port_result.value(), config.options_.worker_threads_,
        config.options_.connection_io_threads_, static_cast<unsigned long long>(config.delay_us_),
        config.options_.max_inflight_per_connection_, config.options_.max_write_queue_bytes_per_connection_,
        config.options_.max_pending_jobs_global_);
    std::fflush(stdout);

    while (!xrpc::benchmark::stop_requested.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    server.Stop();
    server_thread.request_stop();
    if (server_thread.joinable()) {
      server_thread.join();
    }

    const xrpc::RpcServerStats stats = server.stats();
    std::printf(
        "backpressure rejected_inflight=%llu rejected_global_pending=%llu closed_write_queue=%llu "
        "max_inflight=%llu max_write_queue_bytes=%llu\n",
        static_cast<unsigned long long>(stats.rejected_by_inflight_limit_),
        static_cast<unsigned long long>(stats.rejected_by_global_pending_limit_),
        static_cast<unsigned long long>(stats.closed_by_write_queue_high_watermark_),
        static_cast<unsigned long long>(stats.max_observed_inflight_),
        static_cast<unsigned long long>(stats.max_observed_write_queue_bytes_));
    std::printf("worker_queue max_depth=%llu\n",
                static_cast<unsigned long long>(stats.max_observed_worker_queue_depth_));
    std::fflush(stdout);
    return 0;
  } catch (const std::exception &ex) {
    std::fprintf(stderr, "%s\n%s\n", ex.what(), xrpc::benchmark::Usage(argv[0]).c_str());
    return 1;
  }
}
