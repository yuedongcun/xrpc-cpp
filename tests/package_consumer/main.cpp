#include <xrpc/rpc_server.h>

auto main() -> int {
  xrpc::RpcServerOptions options;
  options.worker_threads_ = 1;
  auto server = xrpc::RpcServer::Create(options);
  return server.ok() ? 0 : 1;
}
