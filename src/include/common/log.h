#pragma once

#include <string_view>

namespace xrpc {

/** Writes one error line to stderr synchronously. Messages must not contain payloads or secrets. */
void LogError(std::string_view message) noexcept;

}  // namespace xrpc
