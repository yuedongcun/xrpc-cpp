#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <utility>

#include <xrpc/rpc_options.h>
#include <xrpc/status.h>

namespace xrpc {

struct Endpoint {
  std::string host_;

  std::uint16_t port_ = 0;

  auto operator==(const Endpoint &other) const -> bool = default;
};

struct RpcClientOptions {
  std::string target_;
  std::string consul_address_{"127.0.0.1:8500"};
  std::chrono::milliseconds discovery_refresh_interval_{5000};

  std::chrono::milliseconds timeout_{0};

  std::size_t max_payload_size_ = DEFAULT_MAX_PAYLOAD_SIZE;

  std::size_t max_inflight_per_endpoint_ = 1024;
};

class RpcClient final {
 public:
  [[nodiscard]] static auto Create(const RpcClientOptions &options) -> StatusOr<RpcClient>;

  ~RpcClient();

  RpcClient(const RpcClient &) = delete;
  auto operator=(const RpcClient &) -> RpcClient & = delete;

  RpcClient(RpcClient &&) noexcept;

  auto operator=(RpcClient &&) noexcept -> RpcClient &;

  [[nodiscard]] auto CallPayload(std::string service_name, std::string method_name, std::string payload)
      -> StatusOr<std::string> {
    return CallPayload(std::move(service_name), std::move(method_name), std::move(payload), CallOptions{});
  }

  [[nodiscard]] auto CallPayload(std::string service_name, std::string method_name, std::string payload,
                                 const CallOptions &options) -> StatusOr<std::string>;

  template <typename Resp, typename Req>
  [[nodiscard]] auto Call(std::string service_name, std::string method_name, const Req &request) -> StatusOr<Resp> {
    return Call<Resp>(std::move(service_name), std::move(method_name), request, CallOptions{});
  }

  template <typename Resp, typename Req>
  [[nodiscard]] auto Call(std::string service_name, std::string method_name, const Req &request,
                          const CallOptions &options) -> StatusOr<Resp> {
    std::string payload;
    try {
      payload = request.SerializeAsString();
    } catch (const std::exception &exception) {
      return StatusOr<Resp>(Status{StatusCode::Internal, exception.what()});
    } catch (...) {
      return StatusOr<Resp>(Status{StatusCode::Internal, "failed to serialize typed request"});
    }

    StatusOr<std::string> payload_result =
        CallPayload(std::move(service_name), std::move(method_name), std::move(payload), options);
    if (!payload_result.ok()) {
      return StatusOr<Resp>(payload_result.status());
    }

    try {
      Resp response;
      if (!response.ParseFromString(payload_result.value())) {
        return StatusOr<Resp>(Status(StatusCode::DataLoss, "failed to decode typed response"));
      }
      return StatusOr<Resp>(std::move(response));
    } catch (const std::exception &exception) {
      return StatusOr<Resp>(Status{StatusCode::Internal, exception.what()});
    } catch (...) {
      return StatusOr<Resp>(Status{StatusCode::Internal, "failed to decode typed response"});
    }
  }

 private:
  class Impl;

  explicit RpcClient(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace xrpc
