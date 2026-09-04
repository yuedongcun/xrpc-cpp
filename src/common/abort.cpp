#include "common/abort.h"

#include <cerrno>
#include <cstdlib>
#include <string_view>

#include <sys/uio.h>
#include <unistd.h>

namespace xrpc {

void Abort(std::string_view reason) noexcept {
  constexpr std::string_view prefix = "xrpc abort: ";
  constexpr std::string_view newline = "\n";
  iovec output[] = {
      {.iov_base = const_cast<char *>(prefix.data()), .iov_len = prefix.size()},
      {.iov_base = const_cast<char *>(reason.data()), .iov_len = reason.size()},
      {.iov_base = const_cast<char *>(newline.data()), .iov_len = newline.size()},
  };
  while (::writev(STDERR_FILENO, output, 3) < 0 && errno == EINTR) {
  }
  std::abort();
}

}  // namespace xrpc
