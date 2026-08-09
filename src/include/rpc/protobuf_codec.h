#pragma once

#include <string>
#include <string_view>

#include "rpc/xrpc_exception.h"

namespace xrpc {

/**
 * @brief Thin protobuf adapter used by tests and internal exception-based paths.
 *
 * Public client/server APIs convert serialization failures to `StatusOr` or RPC status responses. This helper is kept
 * narrow for code that already uses exceptions at an internal boundary.
 */
struct ProtobufCodec {
  /**
   * @brief Serializes a protobuf-compatible message.
   *
   * @throws ProtocolException when the message cannot serialize.
   */
  template <typename T>
  static auto Encode(const T &msg) -> std::string {
    std::string payload;
    if (!msg.SerializeToString(&payload)) {
      throw ProtocolException(StatusCode::Internal, "failed to serialize protobuf message");
    }
    return payload;
  }

  /**
   * @brief Parses a protobuf-compatible message from raw bytes.
   *
   * @throws ProtocolException when the payload cannot be parsed as `T`.
   */
  template <typename T>
  static auto Decode(std::string_view payload) -> T {
    T msg;
    if (!msg.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
      throw ProtocolException(StatusCode::InvalidArgument, "failed to parse protobuf message");
    }
    return msg;
  }
};

}  // namespace xrpc
