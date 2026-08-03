#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include <xrpc/status.h>

namespace xrpc {

/**
 * @brief Configuration for the embedded Prometheus scrape endpoint.
 *
 * The exporter is intentionally small: it serves the current process metric sink over HTTP and does not share the RPC
 * server's io_uring runtime. `io_timeout_` bounds individual blocking socket operations performed by the exporter
 * thread.
 */
struct PrometheusExporterOptions {
  /** @brief Local address to bind. */
  std::string host_ = "127.0.0.1";

  /** @brief Local scrape port. A zero port lets the OS choose an available port. */
  std::uint16_t port_ = 9101;

  /** @brief HTTP path that returns Prometheus text exposition. */
  std::string metrics_path_ = "/metrics";

  /** @brief Listen backlog for the exporter socket. */
  int listen_backlog_ = 16;

  /** @brief Timeout for individual blocking socket reads and writes. */
  std::chrono::milliseconds io_timeout_{2000};
};

/**
 * @brief Minimal HTTP server that exposes the installed metric sink to Prometheus.
 *
 * The exporter owns one background thread and a listening socket. It is non-copyable and non-movable because the
 * runtime keeps thread and file-descriptor ownership internally. `Stop()` is idempotent and also runs from the
 * destructor.
 */
class PrometheusExporter final {
 public:
  /**
   * @brief Creates an exporter with explicit options.
   *
   * @param options Bind address, path, backlog, and socket timeout.
   */
  explicit PrometheusExporter(PrometheusExporterOptions options = {});

  /** @brief Stops the exporter thread if it is still running. */
  ~PrometheusExporter();

  PrometheusExporter(const PrometheusExporter &) = delete;
  auto operator=(const PrometheusExporter &) -> PrometheusExporter & = delete;

  PrometheusExporter(PrometheusExporter &&) noexcept = delete;
  auto operator=(PrometheusExporter &&) noexcept -> PrometheusExporter & = delete;

  /**
   * @brief Binds the scrape socket and starts the exporter thread.
   *
   * @return `Status::Ok()` on success, or a bind/listen/runtime status on failure.
   */
  [[nodiscard]] auto Start() -> Status;

  /** @brief Requests exporter shutdown and joins the exporter thread. */
  void Stop();

  /** @return The bound scrape port after `Start()` succeeds. */
  [[nodiscard]] auto port() const -> std::uint16_t;

 private:
  class ExporterRuntime;

  std::unique_ptr<ExporterRuntime> runtime_;
};

}  // namespace xrpc
