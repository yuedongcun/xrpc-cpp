#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include <xrpc/rpc_server.h>

#include "common/log.h"
#include "proto/echo.pb.h"
#include "server/runtime_access.h"
#include "server/stats_output.h"

namespace xrpc::benchmark {
namespace {

constexpr std::string_view SERVICE_NAME = "BenchmarkService";
constexpr std::string_view METHOD_NAME = "Echo";

struct ServerConfig final {
  std::string host_ = "127.0.0.1";
  std::uint16_t port_ = 9010;
  std::uint64_t delay_us_ = 0;
  RpcServerOptions options_;
  io::UringBufferPoolConfig buffer_pool_;
  std::string stats_file_;
};

std::atomic<bool> stop_requested{false};
std::atomic<bool> stats_requested{false};
std::atomic<bool> stats_window_requested{false};

void HandleSignal(int signal) {
  (void)signal;
  stop_requested.store(true, std::memory_order_relaxed);
}

void HandleStatsSignal(int signal) {
  if (signal == SIGUSR2) {
    stats_window_requested.store(true, std::memory_order_relaxed);
  }
  stats_requested.store(true, std::memory_order_relaxed);
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
  } else if (key == "stats_file") {
    config.stats_file_ = std::string(value);
    if (config.stats_file_.empty()) {
      throw std::invalid_argument("stats_file must not be empty");
    }
  } else if (key == "port") {
    config.port_ = static_cast<std::uint16_t>(ParseUnsigned(value, "port"));
  } else if (key == "delay_us") {
    config.delay_us_ = ParseUnsigned(value, "delay_us");
  } else if (key == "worker_threads") {
    config.options_.worker_threads_ = static_cast<std::size_t>(ParseUnsigned(value, "worker_threads"));
  } else if (key == "io_threads") {
    config.options_.connection_io_threads_ = static_cast<std::size_t>(ParseUnsigned(value, "io_threads"));
  } else if (key == "recv_buffer_count" || key == "recv_buffer_size") {
    const auto dimension = ParseUnsigned(value, "receive buffer dimension");
    if (dimension > std::numeric_limits<std::uint32_t>::max()) {
      throw std::invalid_argument("receive buffer dimension exceeds uint32 range");
    }
    if (key == "recv_buffer_count") {
      config.buffer_pool_.buffer_count_ = static_cast<std::uint32_t>(dimension);
    } else {
      config.buffer_pool_.buffer_size_ = static_cast<std::uint32_t>(dimension);
    }
  } else if (key == "max_inflight_per_connection") {
    config.options_.max_inflight_per_connection_ =
        static_cast<std::size_t>(ParseUnsigned(value, "max_inflight_per_connection"));
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
  const auto pool_status = config.buffer_pool_.Validate();
  if (!pool_status.ok()) {
    throw std::invalid_argument(pool_status.message());
  }
  return config;
}

auto Usage(const char *program) -> std::string {
  return std::string("Usage: ") + program +
         " [--host=IP] [--port=N] [--delay_us=N] [--worker_threads=N] [--io_threads=N] "
         "[--max_inflight_per_connection=N] [--listen_backlog=N] [--stats_file=PATH] "
         "[--recv_buffer_count=N] [--recv_buffer_size=BYTES]";
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
  xrpc::LoggingRuntime logging(argv[0]);
  try {
    xrpc::benchmark::ServerConfig config = xrpc::benchmark::ParseConfig(argc, argv);
    xrpc::StatusOr<xrpc::RpcServer> server_result =
        xrpc::ServerRuntimeAccess::CreateWithBufferPool(config.options_, config.buffer_pool_);
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
    if (!config.stats_file_.empty()) {
      std::signal(SIGUSR1, xrpc::benchmark::HandleStatsSignal);
      std::signal(SIGUSR2, xrpc::benchmark::HandleStatsSignal);
    }

    status = server.Listen(config.host_, config.port_);
    if (!status.ok()) {
      throw std::runtime_error(status.message());
    }
    std::jthread server_thread([&server]() -> void { (void)server.Run(); });

    const xrpc::StatusOr<std::uint16_t> port_result = server.port();
    if (!port_result.ok()) {
      throw std::runtime_error(port_result.status().message());
    }

    std::printf(
        "ready host=%s port=%u worker_threads=%zu connection_io_threads=%zu delay_us=%llu "
        "max_inflight_per_connection=%zu max_write_queue_bytes_per_connection=%zu max_pending_jobs_global=%zu "
        "recv_buffer_count=%u recv_buffer_size=%u\n",
        config.host_.c_str(), port_result.value(), config.options_.worker_threads_,
        config.options_.connection_io_threads_, static_cast<unsigned long long>(config.delay_us_),
        config.options_.max_inflight_per_connection_, config.options_.max_write_queue_bytes_per_connection_,
        config.options_.max_pending_jobs_global_, config.buffer_pool_.buffer_count_, config.buffer_pool_.buffer_size_);
    std::fflush(stdout);

    while (!xrpc::benchmark::stop_requested.load(std::memory_order_relaxed)) {
      if (xrpc::benchmark::stats_requested.exchange(false, std::memory_order_relaxed)) {
        status = xrpc::benchmark::WriteStatsSnapshot(
            server, config.stats_file_,
            xrpc::benchmark::stats_window_requested.exchange(false, std::memory_order_relaxed));
        if (!status.ok()) {
          std::fprintf(stderr, "%s\n", status.message().c_str());
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    server.Stop();
    server_thread.request_stop();
    if (server_thread.joinable()) {
      server_thread.join();
    }

    return status.ok() ? 0 : 1;
  } catch (const std::exception &ex) {
    std::fprintf(stderr, "%s\n%s\n", ex.what(), xrpc::benchmark::Usage(argv[0]).c_str());
    return 1;
  }
}
