#include <proto/echo.pb.h>
#include <xrpc/rpc_server.h>

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>

namespace {

auto Echo(const xrpc::example::EchoRequest &request) -> xrpc::example::EchoResponse {
  xrpc::example::EchoResponse response;
  response.set_message("echo: " + request.message());
  return response;
}

}  // namespace

auto main() -> int {
  constexpr std::uint16_t port = 9000;

  auto server_result = xrpc::RpcServer::Create();
  if (!server_result.ok()) {
    std::cerr << "create failed: " << server_result.status().message() << '\n';
    return 1;
  }
  xrpc::RpcServer server = std::move(server_result).value();
  xrpc::Status status =
      server.RegisterMethod<xrpc::example::EchoRequest, xrpc::example::EchoResponse>("EchoService", "Echo", Echo);
  if (!status.ok()) {
    std::cerr << "register failed: " << status.message() << '\n';
    return 1;
  }
  status = server.Listen("127.0.0.1", port);
  if (!status.ok()) {
    std::cerr << "listen failed: " << status.message() << '\n';
    return 1;
  }

  std::cout << "echo server listening on 127.0.0.1:" << port << '\n';
  status = server.Run();
  if (!status.ok()) {
    std::cerr << "server failed: " << status.message() << '\n';
    return 1;
  }
  return 0;
}
