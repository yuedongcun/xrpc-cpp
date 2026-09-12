#include <proto/echo.pb.h>
#include <xrpc/rpc_client.h>

#include <cstdint>
#include <iostream>
#include <utility>

#include "common/log.h"

auto main(int argc, char **argv) -> int {
  xrpc::LoggingRuntime logging(argv[0]);
  (void)argc;
  constexpr std::uint16_t port = 9000;

  xrpc::RpcClientOptions options;
  options.target_ = "list://127.0.0.1:" + std::to_string(port);
  auto client_result = xrpc::RpcClient::Create(options);
  if (!client_result.ok()) {
    std::cerr << "create failed: " << client_result.status().message() << '\n';
    return 1;
  }
  xrpc::RpcClient client = std::move(client_result).value();

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
