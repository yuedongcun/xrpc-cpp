#pragma once

#include <functional>

#include "rpc/raw_message.h"

namespace xrpc {

/**
 * @brief Server-side raw handler signature used after protocol decoding.
 *
 * Handlers take ownership of the decoded request so hot paths can move payload bytes into responses or downstream work
 * without an extra copy. Typed public method registrations are adapted into this raw signature by
 * `MakeMethodRegistration()`.
 */
using RawHandler = std::function<RawResponse(RawRequest)>;

}  // namespace xrpc
