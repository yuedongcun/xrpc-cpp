#pragma once

#include "protocol/protocol_message.h"
#include "rpc/raw_message.h"

namespace xrpc {

/**
 * @brief Converts a decoded protocol request to the raw RPC core representation.
 */
[[nodiscard]] auto ToRawRequest(const ProtocolRequest &request) -> RawRequest;

/** @brief Moves a decoded protocol request into the raw RPC core representation. */
[[nodiscard]] auto ToRawRequest(ProtocolRequest &&request) -> RawRequest;

/** @brief Converts a raw RPC request back into protocol-level fields. */
[[nodiscard]] auto ToProtocolRequest(const RawRequest &request) -> ProtocolRequest;

/** @brief Converts a raw RPC response into protocol-level fields. */
[[nodiscard]] auto ToProtocolResponse(const RawResponse &response) -> ProtocolResponse;

/** @brief Moves a raw RPC response into protocol-level fields. */
[[nodiscard]] auto ToProtocolResponse(RawResponse &&response) -> ProtocolResponse;

/** @brief Converts a protocol-level response back to the raw RPC core representation. */
[[nodiscard]] auto ToRawResponse(const ProtocolResponse &response) -> RawResponse;

}  // namespace xrpc
