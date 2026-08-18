#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <xrpc/method_registration.h>
#include <xrpc/protocol_options.h>
#include <xrpc/status_or.h>

namespace xrpc {

struct RpcServerOptions {
  std::size_t worker_threads_ = 0;

  std::size_t connection_io_threads_ = 1;

  std::size_t listen_backlog_ = 128;

  std::size_t max_inflight_per_connection_ = 128;

  std::size_t max_write_queue_bytes_per_connection_ = 8U * 1024U * 1024U;

  std::size_t max_pending_jobs_global_ = 10000;

  std::size_t max_payload_size_ = DEFAULT_MAX_PAYLOAD_SIZE;

  std::string service_name_;

  std::string service_id_;

  std::string service_address_;

  std::uint16_t service_port_ = 0;

  std::string consul_address_{"127.0.0.1:8500"};

  std::chrono::milliseconds consul_timeout_{1000};
};

class RpcServer final {
 public:
  [[nodiscard]] static auto Create(const RpcServerOptions &options = {}) -> StatusOr<RpcServer>;

  ~RpcServer();

  RpcServer(const RpcServer &) = delete;
  auto operator=(const RpcServer &) -> RpcServer & = delete;

  RpcServer(RpcServer &&) noexcept;

  auto operator=(RpcServer &&) noexcept -> RpcServer &;

  template <typename Request, typename Response, typename Func>
  [[nodiscard]] auto RegisterMethod(std::string service_name, std::string method_name, Func func) -> Status {
    StatusOr<MethodRegistration> registration =
        MakeMethodRegistration<Request, Response>(std::move(service_name), std::move(method_name), std::move(func));
    if (!registration.ok()) {
      return registration.status();
    }
    return RegisterMethod(std::move(registration).value());
  }

  [[nodiscard]] auto Listen(std::string_view host, std::uint16_t port) -> Status;

  [[nodiscard]] auto Run() -> Status;

  void Stop();

  [[nodiscard]] auto port() const -> StatusOr<std::uint16_t>;

 private:
  class Impl;

  explicit RpcServer(std::unique_ptr<Impl> impl);

  [[nodiscard]] auto RegisterMethod(MethodRegistration registration) -> Status;

  std::unique_ptr<Impl> impl_;
};

}  // namespace xrpc
