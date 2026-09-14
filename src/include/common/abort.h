#pragma once

#include <string_view>

namespace xrpc {

[[noreturn]] void Abort(std::string_view reason) noexcept;

}  // namespace xrpc
