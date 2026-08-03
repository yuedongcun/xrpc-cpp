#include <xrpc/status.h>

auto main() -> int {
  const xrpc::Status status = xrpc::Status::Ok();
  return status.ok() ? 0 : 1;
}
