/**
 * @file rpc_envelope.h
 * @brief Defines protocol-level RPC request and response envelopes.
 */

#pragma once

#include <cstdint>
#include <string>

#include <xrpc/status.h>

namespace xrpc {

/**
 * @brief Protocol-level request produced after frame decoding.
 *
 * `payload_` remains the serialized user Protobuf request. The registered RPC
 * method converts it into the concrete application message type.
 */
struct RequestEnvelope {
  /** Multiplexing identifier copied into the corresponding response. */
  std::uint64_t request_id_ = 0;

  /** Routing metadata decoded from `RequestMetadata`. */
  std::string service_name_;
  std::string method_name_;

  /** Serialized user request message. */
  std::string payload_;
};

/**
 * @brief Protocol-level response before wire-frame encoding.
 *
 * `status_` describes RPC execution, while `payload_` contains the serialized
 * user response when the call produced one.
 */
struct ResponseEnvelope {
  /** Request identifier used by the client to match multiplexed responses. */
  std::uint64_t request_id_ = 0;

  Status status_;

  /** Serialized user response message. */
  std::string payload_;
};

}  // namespace xrpc
