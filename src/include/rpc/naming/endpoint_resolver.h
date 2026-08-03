#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <xrpc/rpc_client.h>
#include <xrpc/status.h>

namespace xrpc {

/** @brief Resolver target kinds supported by v1 clients. */
enum class ResolverKind : std::uint8_t {
  /** @brief Static endpoint list parsed from the client target string. */
  StaticList,

  /** @brief Consul service discovery target. */
  Consul,
};

/** @brief Snapshot of resolver refresh diagnostics. */
struct ResolverStatsSnapshot {
  /** @brief Successful refresh operations since resolver construction. */
  std::uint64_t refresh_success_count_ = 0;

  /** @brief Failed refresh operations since resolver construction. */
  std::uint64_t refresh_failure_count_ = 0;

  /** @brief Refresh operations that produced no usable endpoints. */
  std::uint64_t empty_snapshot_count_ = 0;
};

/**
 * @brief Endpoint discovery interface used by `RpcClient::ClientRuntime`.
 *
 * Design note:
 * - Ownership: `RpcClientRuntime` owns one resolver chosen from the target string.
 * - Snapshot: `Snapshot()` returns a copy so channel routing can update without holding resolver locks.
 * - Failure: `Start()` may fail, but runtime can still defer the first resolver failure and let calls report endpoint
 *   unavailability.
 */
class EndpointResolver {
 public:
  virtual ~EndpointResolver() = default;

  /** @brief Starts resolver work and performs any required initial refresh. */
  [[nodiscard]] virtual auto Start() -> Status = 0;

  /** @brief Stops background resolver work. */
  virtual void Stop() = 0;

  /** @return Copy of the most recently known endpoint list. */
  [[nodiscard]] virtual auto Snapshot() const -> std::vector<Endpoint> = 0;

  /** @return Last refresh error text, or empty string when the last refresh succeeded. */
  [[nodiscard]] virtual auto last_error() const -> std::string = 0;

  /** @return Concrete resolver kind. */
  [[nodiscard]] virtual auto kind() const -> ResolverKind = 0;

  /** @return Resolver refresh counters. Implementations without counters return zeros. */
  [[nodiscard]] virtual auto stats() const -> ResolverStatsSnapshot { return {}; }
};

/** @brief Public resolver construction options before target parsing. */
struct ResolverOptions {
  std::string target_;
  std::string consul_address_;
  std::chrono::milliseconds discovery_refresh_interval_{5000};
};

/** @brief Parsed resolver configuration used to construct a concrete resolver. */
struct ResolverConfig {
  ResolverKind kind_ = ResolverKind::StaticList;
  std::vector<Endpoint> static_endpoints_;
  std::string consul_service_name_;
  std::string consul_address_;
  std::chrono::milliseconds discovery_refresh_interval_{0};
};

/** @return Canonicalized endpoints with invalid and duplicate entries removed. */
[[nodiscard]] auto CanonicalizeEndpoints(std::vector<Endpoint> endpoints) -> std::vector<Endpoint>;

/** @return Parsed resolver config from public resolver options. */
[[nodiscard]] auto NormalizeResolverOptions(const ResolverOptions &options) -> ResolverConfig;

/** @return Concrete resolver implementation for the target in `options`. */
[[nodiscard]] auto MakeEndpointResolver(const ResolverOptions &options) -> std::unique_ptr<EndpointResolver>;

}  // namespace xrpc
