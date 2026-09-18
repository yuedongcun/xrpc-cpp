#include "io/system_error.h"

#include <system_error>

namespace xrpc::io {

auto MakeSystemErrorMessage(std::string_view action, int error_code) -> std::string {
  std::string message(action);
  message.append(" failed");
  if (error_code != 0) {
    message.append(": ");
    message.append(std::error_code(error_code, std::generic_category()).message());
  }
  return message;
}

}  // namespace xrpc::io
