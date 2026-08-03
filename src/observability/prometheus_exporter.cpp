#include <xrpc/prometheus_exporter.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <xrpc/metrics.h>
#include <xrpc/xrpc_exception.h>

#include "io/socket.h"
#include "io/socket_error.h"
#include "observability/prometheus_metric_sink.h"

namespace xrpc {
namespace {

/** @brief Maximum request header bytes accepted by the embedded scrape server. */
constexpr std::size_t MAX_HTTP_REQUEST_BYTES = 8192;

/** @brief Timeout used by shutdown wakeup connections. */
constexpr std::chrono::milliseconds STOP_WAKE_TIMEOUT{200};

/**
 * @brief Validates exporter options before binding the listener.
 *
 * @param options User-facing exporter options.
 * @return OK when the exporter can attempt to bind, otherwise invalid-argument status.
 */
[[nodiscard]] auto ValidateOptions(const PrometheusExporterOptions &options) -> Status {
  if (options.host_.empty()) {
    return {StatusCode::InvalidArgument, "PrometheusExporter host must not be empty"};
  }
  if (options.metrics_path_.empty() || options.metrics_path_.front() != '/') {
    return {StatusCode::InvalidArgument, "PrometheusExporter metrics_path must start with /"};
  }
  if (options.listen_backlog_ <= 0) {
    return {StatusCode::InvalidArgument, "PrometheusExporter listen_backlog must be greater than 0"};
  }
  if (options.io_timeout_ < std::chrono::milliseconds::zero()) {
    return {StatusCode::InvalidArgument, "PrometheusExporter io_timeout must not be negative"};
  }
  return Status::Ok();
}

/**
 * @brief Converts wildcard binds to a loopback address usable for shutdown wakeups.
 *
 * @param bind_host Configured listen host.
 * @return Host to connect to from `Stop()`.
 */
[[nodiscard]] auto ConnectHost(std::string_view bind_host) -> std::string_view {
  if (bind_host == "0.0.0.0") {
    return "127.0.0.1";
  }
  return bind_host;
}

/**
 * @brief Returns the HTTP reason phrase used in generated responses.
 *
 * @param status_code HTTP status code.
 * @return Short reason phrase for the status line.
 */
[[nodiscard]] auto ReasonPhrase(int status_code) -> std::string_view {
  switch (status_code) {
    case 200:
      return "OK";
    case 400:
      return "Bad Request";
    case 404:
      return "Not Found";
    case 405:
      return "Method Not Allowed";
    case 431:
      return "Request Header Fields Too Large";
    case 500:
      return "Internal Server Error";
    default:
      return "Unknown";
  }
}

/**
 * @brief Builds a complete HTTP/1.1 response with a fixed content length.
 *
 * @param status_code HTTP status code.
 * @param content_type Response content type.
 * @param body Response body bytes.
 * @return Serialized HTTP response.
 */
[[nodiscard]] auto HttpResponse(int status_code, std::string_view content_type, std::string_view body) -> std::string {
  std::ostringstream out;
  out << "HTTP/1.1 " << status_code << ' ' << ReasonPhrase(status_code) << "\r\n";
  out << "Content-Type: " << content_type << "\r\n";
  out << "Content-Length: " << body.size() << "\r\n";
  out << "Connection: close\r\n\r\n";
  out << body;
  return out.str();
}

/**
 * @brief Reads one HTTP request header block from a scrape client.
 *
 * @param socket Accepted client socket.
 * @return Request bytes through the header terminator, empty partial request on EOF, or nullopt when too large.
 */
[[nodiscard]] auto ReadHttpRequest(io::Socket &socket) -> std::optional<std::string> {
  std::string request;
  char buffer[1024];
  while (request.size() < MAX_HTTP_REQUEST_BYTES) {
    const ssize_t received = socket.Read(buffer, sizeof(buffer));
    if (received == 0) {
      break;
    }
    request.append(buffer, static_cast<std::size_t>(received));
    if (request.find("\r\n\r\n") != std::string::npos) {
      return request;
    }
  }
  if (request.size() >= MAX_HTTP_REQUEST_BYTES) {
    return std::nullopt;
  }
  return request;
}

/**
 * @brief Extracts the path from a minimal HTTP GET request line.
 *
 * The exporter only accepts GET. A syntactically valid non-GET request returns an empty path so the
 * caller can map it to 405, while malformed request lines return nullopt.
 *
 * @param request Request bytes containing at least the first line.
 * @return Parsed path without query string, empty path for non-GET, or nullopt for bad requests.
 */
[[nodiscard]] auto RequestPath(std::string_view request) -> std::optional<std::string_view> {
  const std::size_t line_end = request.find("\r\n");
  if (line_end == std::string_view::npos) {
    return std::nullopt;
  }
  const std::string_view request_line = request.substr(0, line_end);
  const std::size_t method_end = request_line.find(' ');
  if (method_end == std::string_view::npos) {
    return std::nullopt;
  }
  if (request_line.substr(0, method_end) != "GET") {
    return std::string_view{};
  }

  const std::size_t path_start = method_end + 1;
  const std::size_t path_end = request_line.find(' ', path_start);
  if (path_end == std::string_view::npos || path_end == path_start) {
    return std::nullopt;
  }
  std::string_view path = request_line.substr(path_start, path_end - path_start);
  if (const std::size_t query_start = path.find('?'); query_start != std::string_view::npos) {
    path = path.substr(0, query_start);
  }
  return path;
}

}  // namespace

class PrometheusExporter::ExporterRuntime final {
 public:
  /**
   * @brief Creates the private runtime for one exporter facade.
   *
   * @param options Bind address, scrape path, backlog, and socket timeout.
   */
  explicit ExporterRuntime(PrometheusExporterOptions options) : options_(std::move(options)) {}

  /** @brief Stops the scrape thread before releasing listener and metric sink state. */
  ~ExporterRuntime() { Stop(); }

  /**
   * @brief Binds the scrape listener, installs a Prometheus sink, and starts the serve thread.
   *
   * @return `Status::Ok()` on success, otherwise validation or bind/listen failure status.
   */
  [[nodiscard]] auto Start() -> Status {
    std::lock_guard lock(state_mutex_);
    if (running_.load(std::memory_order_acquire)) {
      return Status::Ok();
    }

    Status validation_status = ValidateOptions(options_);
    if (!validation_status.ok()) {
      return validation_status;
    }

    try {
      auto sink = std::make_shared<PrometheusMetricSink>();
      io::Socket listener;
      listener.Bind(options_.host_, options_.port_);
      listener.Listen(options_.listen_backlog_);

      port_ = listener.LocalPort();
      listener_.emplace(std::move(listener));
      sink_ = std::move(sink);
      running_.store(true, std::memory_order_release);
      server_thread_ = std::jthread([this]() { ServeLoop(); });
      SetMetricSink(sink_);
      return Status::Ok();
    } catch (...) {
      running_.store(false, std::memory_order_release);
      listener_.reset();
      sink_.reset();
      port_ = 0;
      return CaughtExceptionToStatus(StatusCode::Unavailable, "failed to start PrometheusExporter");
    }
  }

  /**
   * @brief Stops the serve thread and removes the metric sink installed by this runtime.
   *
   * Stop wakes a blocking `Accept()` by opening a short-lived loopback connection to the listener.
   */
  void Stop() {
    std::shared_ptr<PrometheusMetricSink> sink_to_reset;
    std::string host;
    std::uint16_t port = 0;
    {
      std::lock_guard lock(state_mutex_);
      if (!running_.load(std::memory_order_acquire)) {
        return;
      }
      running_.store(false, std::memory_order_release);
      host = options_.host_;
      port = port_;
    }

    server_thread_.request_stop();
    WakeAcceptLoop(host, port);
    if (server_thread_.joinable()) {
      server_thread_.join();
    }

    {
      std::lock_guard lock(state_mutex_);
      if (listener_.has_value()) {
        listener_->Close();
        listener_.reset();
      }
      sink_to_reset = sink_;
      sink_.reset();
      port_ = 0;
    }

    if (GetMetricSink() == sink_to_reset) {
      ResetMetricSink();
    }
  }

  /** @return Bound scrape port, or zero when the exporter is stopped. */
  [[nodiscard]] auto port() const -> std::uint16_t {
    std::lock_guard lock(state_mutex_);
    return port_;
  }

 private:
  /**
   * @brief Accepts scrape connections until shutdown is requested.
   *
   * Individual connection failures are swallowed so one malformed scrape cannot stop the exporter.
   */
  void ServeLoop() {
    while (running_.load(std::memory_order_acquire)) {
      try {
        io::Socket client = listener_->Accept();
        if (!running_.load(std::memory_order_acquire)) {
          return;
        }
        HandleClient(std::move(client));
      } catch (...) {
        if (!running_.load(std::memory_order_acquire)) {
          return;
        }
      }
    }
  }

  /**
   * @brief Reads and handles one scrape HTTP request.
   *
   * @param client Accepted scrape socket.
   */
  void HandleClient(io::Socket client) {
    try {
      client.SetReadWriteTimeout(options_.io_timeout_);
      const std::optional<std::string> request = ReadHttpRequest(client);
      if (!request.has_value()) {
        client.WriteAll(HttpResponse(431, "text/plain; charset=utf-8", "request too large\n"));
        return;
      }

      const std::optional<std::string_view> path = RequestPath(*request);
      if (!path.has_value()) {
        client.WriteAll(HttpResponse(400, "text/plain; charset=utf-8", "bad request\n"));
        return;
      }
      if (path->empty()) {
        client.WriteAll(HttpResponse(405, "text/plain; charset=utf-8", "method not allowed\n"));
        return;
      }
      if (*path == options_.metrics_path_) {
        client.WriteAll(HttpResponse(200, "text/plain; version=0.0.4; charset=utf-8", sink_->Render()));
        return;
      }
      if (*path == "/-/ready") {
        client.WriteAll(HttpResponse(200, "text/plain; charset=utf-8", "ready\n"));
        return;
      }
      client.WriteAll(HttpResponse(404, "text/plain; charset=utf-8", "not found\n"));
    } catch (...) {
      return;
    }
  }

  /**
   * @brief Opens a short connection to the listener so a blocking accept can observe shutdown.
   *
   * @param host Configured bind host.
   * @param port Bound listen port.
   */
  static void WakeAcceptLoop(std::string_view host, std::uint16_t port) {
    try {
      io::Socket wake_socket;
      wake_socket.Connect(ConnectHost(host), port, STOP_WAKE_TIMEOUT);
    } catch (...) {
      return;
    }
  }

  PrometheusExporterOptions options_;
  mutable std::mutex state_mutex_;
  std::shared_ptr<PrometheusMetricSink> sink_;
  std::optional<io::Socket> listener_;
  std::jthread server_thread_;
  std::atomic_bool running_{false};
  std::uint16_t port_ = 0;
};

/**
 * @brief Creates an exporter facade with a private runtime object.
 *
 * @param options Bind address, scrape path, backlog, and socket timeout.
 */
PrometheusExporter::PrometheusExporter(PrometheusExporterOptions options)
    : runtime_(std::make_unique<ExporterRuntime>(std::move(options))) {}

/** @brief Stops the exporter runtime if it is still running. */
PrometheusExporter::~PrometheusExporter() = default;

/** @return Status from binding the listener and starting the scrape thread. */
auto PrometheusExporter::Start() -> Status { return runtime_->Start(); }

/** @brief Requests exporter shutdown and joins the scrape thread. */
void PrometheusExporter::Stop() { runtime_->Stop(); }

/** @return Bound scrape port, or zero when the exporter is stopped. */
auto PrometheusExporter::port() const -> std::uint16_t { return runtime_->port(); }

}  // namespace xrpc
