#include <proto/echo.pb.h>
#include <xrpc/rpc_client.h>

#include <cstdint>
#include <iostream>

auto main() -> int {
  constexpr std::uint16_t port = 9000;

  xrpc::RpcClient client("127.0.0.1", port);
  const xrpc::Status init_status = client.Init();
  if (!init_status.ok()) {
    std::cerr << "init failed: " << init_status.message() << '\n';
    return 1;
  }

  xrpc::example::EchoRequest request;
  request.set_message("hello from Call");

  const auto response = client.Call<xrpc::example::EchoResponse>("EchoService", "Echo", request);
  if (!response.ok()) {
    std::cerr << "call failed: " << response.status().message() << '\n';
    return 1;
  }

  std::cout << "response: " << response.value().message() << '\n';
  return 0;
}
