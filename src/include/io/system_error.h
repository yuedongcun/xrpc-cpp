#pragma once

#include <string>
#include <string_view>

namespace xrpc::io {

// Formats a positive errno value; zero omits the system error description.
[[nodiscard]] auto MakeSystemErrorMessage(std::string_view action, int error_code) -> std::string;

}  // namespace xrpc::io
