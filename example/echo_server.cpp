#include <proto/echo.pb.h>
#include <xrpc/rpc_server.h>

#include <cstdint>
#include <iostream>
#include <string>

namespace {

auto Echo(const xrpc::example::EchoRequest &request) -> xrpc::example::EchoResponse {
  xrpc::example::EchoResponse response;
  response.set_message("echo: " + request.message());
  return response;
}

}  // namespace

auto main() -> int {
  constexpr std::uint16_t port = 9000;

  xrpc::RpcServer server;
  server.RegisterMethod<xrpc::example::EchoRequest, xrpc::example::EchoResponse>("EchoService", "Echo", Echo);
  server.Listen("127.0.0.1", port);

  std::cout << "echo server listening on 127.0.0.1:" << port << '\n';
  server.Run();
  return 0;
}
