#include "common/log.h"

#include <algorithm>
#include <cstdio>
#include <limits>

namespace xrpc {

void LogError(std::string_view message) noexcept {
  const auto length =
      static_cast<int>(std::min(message.size(), static_cast<std::size_t>(std::numeric_limits<int>::max())));
  // A single stdio call serializes each line against other stdio writers.
  (void)std::fprintf(stderr, "[ERROR] %.*s\n", length, message.empty() ? "" : message.data());
}

}  // namespace xrpc
