#pragma once

#include <cstdint>
#include <string>

#include <xrpc/status.h>

namespace xrpc {

/**
 * @brief Protocol-independent request consumed by the RPC core.
 *
 * This type is the boundary between wire decoding and method dispatch. It keeps protocol header fields and opaque
 * payload bytes together without exposing protobuf header details to the service registry.
 */
struct RawRequest {
  /** @brief Request id copied from the wire frame. */
  std::uint64_t request_id_ = 0;

  /** @brief Service namespace selected by the client. */
  std::string service_name_;

  /** @brief Method name inside the service. */
  std::string method_name_;

  /** @brief Opaque request body bytes. */
  std::string payload_;
};

/**
 * @brief Protocol-independent response produced by the RPC core.
 *
 * The transport maps `status_` into protocol response metadata. `payload_` is meaningful only when `status_.ok()` is
 * true.
 */
struct RawResponse {
  /** @brief Request id echoed from the corresponding request. */
  std::uint64_t request_id_ = 0;

  /** @brief Application or framework status for this call. */
  Status status_;

  /** @brief Opaque response body bytes for successful calls. */
  std::string payload_;
};

}  // namespace xrpc
