#pragma once

#include <cstddef>

namespace xrpc {

/**
 * @brief Default maximum payload size accepted by public client and server options.
 *
 * The value does not include the fixed wire header or protobuf metadata header. Internal protocol limits combine this
 * payload cap with header limits to derive the maximum frame size.
 */
inline constexpr std::size_t DefaultMaxPayloadSize = 4U * 1024U * 1024U;

}  // namespace xrpc
