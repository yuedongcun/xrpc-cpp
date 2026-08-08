#include "common/benchmark_config.h"

#include <charconv>
#include <stdexcept>
#include <string_view>

namespace xrpc::benchmark {

namespace {

auto ParseUnsigned(std::string_view value, const char *name) -> std::uint64_t {
  std::uint64_t result = 0;
  const auto parse_result = std::from_chars(value.data(), value.data() + value.size(), result);
  if (parse_result.ec != std::errc{} || parse_result.ptr != value.data() + value.size()) {
    throw std::invalid_argument(std::string("invalid value for ") + name);
  }
  return result;
}

void RequireKeyValue(std::string_view arg) {
  if (!arg.starts_with("--") || arg.find('=') == std::string_view::npos) {
    throw std::invalid_argument("arguments must use --key=value format");
  }
}

void ParseClientArg(BenchmarkConfig &config, std::string_view arg) {
  RequireKeyValue(arg);
  const std::size_t eq = arg.find('=');
  const std::string_view key = arg.substr(2, eq - 2);
  const std::string_view value = arg.substr(eq + 1);

  if (key == "payload_size") {
    config.payload_size_ = static_cast<std::size_t>(ParseUnsigned(value, "payload_size"));
  } else if (key == "host") {
    config.host_ = std::string(value);
  } else if (key == "port") {
    config.port_ = static_cast<std::uint16_t>(ParseUnsigned(value, "port"));
  } else if (key == "client_mode") {
    config.client_mode_ = ParseBenchmarkClientMode(value);
  } else if (key == "client_threads") {
    config.client_threads_ = static_cast<std::size_t>(ParseUnsigned(value, "client_threads"));
  } else if (key == "firehose_connections") {
    config.firehose_connections_ = static_cast<std::size_t>(ParseUnsigned(value, "firehose_connections"));
  } else if (key == "duration_s") {
    config.duration_s_ = ParseUnsigned(value, "duration_s");
  } else if (key == "workload") {
    config.workload_ = ParseBenchmarkWorkload(value);
  } else if (key == "firehose_inflight") {
    config.firehose_inflight_ = static_cast<std::size_t>(ParseUnsigned(value, "firehose_inflight"));
  } else if (key == "firehose_io_threads") {
    config.firehose_io_threads_ = static_cast<std::size_t>(ParseUnsigned(value, "firehose_io_threads"));
  } else {
    throw std::invalid_argument(std::string("unknown argument: --") + std::string(key));
  }
}

void ParseServerArg(BenchmarkServerConfig &config, std::string_view arg) {
  RequireKeyValue(arg);
  const std::size_t eq = arg.find('=');
  const std::string_view key = arg.substr(2, eq - 2);
  const std::string_view value = arg.substr(eq + 1);

  if (key == "host") {
    config.host_ = std::string(value);
  } else if (key == "port") {
    config.port_ = static_cast<std::uint16_t>(ParseUnsigned(value, "port"));
  } else if (key == "service_name") {
    config.server_options_.service_name_ = std::string(value);
  } else if (key == "service_id") {
    config.server_options_.service_id_ = std::string(value);
  } else if (key == "service_address") {
    config.server_options_.service_address_ = std::string(value);
  } else if (key == "service_port") {
    config.server_options_.service_port_ = static_cast<std::uint16_t>(ParseUnsigned(value, "service_port"));
  } else if (key == "consul_address") {
    config.server_options_.consul_address_ = std::string(value);
  } else if (key == "consul_timeout_ms") {
    config.server_options_.consul_timeout_ = std::chrono::milliseconds(ParseUnsigned(value, "consul_timeout_ms"));
  } else if (key == "server_delay_us") {
    config.server_delay_us_ = ParseUnsigned(value, "server_delay_us");
  } else if (key == "worker_threads") {
    config.server_options_.worker_threads_ = static_cast<std::size_t>(ParseUnsigned(value, "worker_threads"));
  } else if (key == "connection_io_threads") {
    config.server_options_.connection_io_threads_ =
        static_cast<std::size_t>(ParseUnsigned(value, "connection_io_threads"));
  } else if (key == "workload") {
    config.workload_ = ParseBenchmarkWorkload(value);
  } else if (key == "listen_backlog") {
    config.server_options_.listen_backlog_ = static_cast<std::size_t>(ParseUnsigned(value, "listen_backlog"));
  } else {
    throw std::invalid_argument(std::string("unknown argument: --") + std::string(key));
  }
}

}  // namespace

auto ParseBenchmarkClientMode(std::string_view value) -> BenchmarkClientMode {
  if (value == "firehose") {
    return BenchmarkClientMode::Firehose;
  }
  if (value == "rpc_client") {
    return BenchmarkClientMode::RpcClient;
  }
  throw std::invalid_argument("client_mode must be one of: firehose, rpc_client");
}

auto ParseBenchmarkWorkload(std::string_view value) -> BenchmarkWorkload {
  if (value == "protobuf") {
    return BenchmarkWorkload::Protobuf;
  }
  throw std::invalid_argument("workload must be protobuf");
}

auto ToString(BenchmarkClientMode mode) -> std::string_view {
  switch (mode) {
    case BenchmarkClientMode::Firehose:
      return "firehose";
    case BenchmarkClientMode::RpcClient:
      return "rpc_client";
  }
  throw std::logic_error("unknown benchmark client mode");
}

auto ToString(BenchmarkWorkload workload) -> std::string_view {
  switch (workload) {
    case BenchmarkWorkload::Protobuf:
      return "protobuf";
  }
  throw std::logic_error("unknown benchmark workload");
}

auto ParseBenchmarkClientConfig(int argc, char **argv) -> BenchmarkConfig {
  BenchmarkConfig config;
  for (int i = 1; i < argc; ++i) {
    ParseClientArg(config, argv[i]);
  }

  if (config.payload_size_ == 0) {
    throw std::invalid_argument("payload_size must be greater than 0");
  }
  if (config.port_ == 0) {
    throw std::invalid_argument("port must be greater than 0");
  }
  if (config.duration_s_ == 0) {
    throw std::invalid_argument("duration_s must be greater than 0");
  }
  if (config.client_threads_ == 0) {
    throw std::invalid_argument("client_threads must be greater than 0");
  }
  if (config.client_mode_ == BenchmarkClientMode::Firehose) {
    if (config.firehose_connections_ == 0) {
      throw std::invalid_argument("firehose_connections must be greater than 0");
    }
    if (config.firehose_inflight_ == 0) {
      throw std::invalid_argument("firehose_inflight must be greater than 0");
    }
    if (config.firehose_inflight_ < config.firehose_connections_) {
      throw std::invalid_argument("firehose_inflight must be greater than or equal to firehose_connections");
    }
  }

  return config;
}

auto ParseBenchmarkServerConfig(int argc, char **argv) -> BenchmarkServerConfig {
  BenchmarkServerConfig config;
  for (int i = 1; i < argc; ++i) {
    ParseServerArg(config, argv[i]);
  }

  if (config.server_options_.listen_backlog_ == 0) {
    throw std::invalid_argument("listen_backlog must be greater than 0");
  }
  if (config.server_options_.connection_io_threads_ == 0) {
    throw std::invalid_argument("connection_io_threads must be greater than 0");
  }

  return config;
}

auto ClientUsage(const char *program) -> std::string {
  return std::string("Usage: ") + program +
         " [--payload_size=N] [--host=IP] [--port=N]"
         " [--duration_s=N] [--workload=protobuf]"
         " [--client_mode=firehose|rpc_client] [--client_threads=N]"
         " [--firehose_connections=N] [--firehose_inflight=N] [--firehose_io_threads=N]";
}

auto ServerUsage(const char *program) -> std::string {
  return std::string("Usage: ") + program +
         " [--host=IP] [--port=N] [--server_delay_us=N]"
         " [--service_name=NAME] [--service_id=ID] [--service_address=IP] [--service_port=N]"
         " [--consul_address=HOST:PORT] [--consul_timeout_ms=N]"
         " [--worker_threads=N] [--connection_io_threads=N] [--listen_backlog=N] [--workload=protobuf]";
}

}  // namespace xrpc::benchmark
