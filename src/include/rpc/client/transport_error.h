#pragma once

#include <xrpc/status.h>

#include "io/socket_error.h"

namespace xrpc {

/**
 * @brief Converts a socket-layer error into the public status model.
 *
 * Keeping this adapter in the client module keeps transport call sites from depending on the concrete socket exception
 * hierarchy.
 */
[[nodiscard]] inline auto ToStatus(const io::SocketError &error) -> Status { return error.status(); }

}  // namespace xrpc
