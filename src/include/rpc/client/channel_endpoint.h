#pragma once

#include <memory>
#include <mutex>
#include <string>

#include <xrpc/rpc_client.h>

#include "rpc/client/tcp_transport.h"

namespace xrpc {

/**
 * @brief Mutable per-endpoint runtime state shared by routing snapshots.
 *
 * The mutex protects transport creation and replacement for that endpoint only. Keeping this state behind a shared
 * pointer lets old routing snapshots finish in-flight calls while a new resolver snapshot removes or replaces the
 * endpoint.
 */
struct EndpointRuntimeState final {
  /** @brief Per-endpoint mutex for lazy connection and transport replacement. */
  std::mutex mutex_;

  /** @brief Open transport for this endpoint, created lazily on first use. */
  std::unique_ptr<TcpTransport> transport_;
};

/**
 * @brief Immutable endpoint view copied into a routing snapshot.
 *
 * Callers load these entries without holding `ClientChannel`'s state mutex. `runtime_state_` remains shared so
 * transports survive snapshot replacement until the last in-flight caller releases the old view.
 */
struct ActiveEndpointSnapshot final {
  /** @brief Stable endpoint identity, currently `host:port`. */
  std::string endpoint_id_;

  /** @brief Endpoint address used to open the transport. */
  Endpoint endpoint_;

  /** @brief Shared mutable state for lazy transport creation and reuse. */
  std::shared_ptr<EndpointRuntimeState> runtime_state_;
};

}  // namespace xrpc
