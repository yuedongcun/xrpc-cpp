#include <gtest/gtest.h>

#include <cstdint>
#include <exception>
#include <string>
#include <string_view>
#include <thread>

#include "io/socket.h"
#include "io/socket_error.h"

namespace {

auto ReadExactly(xrpc::io::Socket &socket, std::size_t expected_size) -> std::string {
  std::string result;
  char chunk[16];

  while (result.size() < expected_size) {
    const ssize_t received = socket.Read(chunk, sizeof(chunk));
    if (received == 0) {
      break;
    }
    result.append(chunk, static_cast<std::size_t>(received));
  }

  return result;
}

}  // namespace

TEST(IoSocketTest, BindsAcceptsConnectsAndExchangesBytes) {
  xrpc::io::Socket listener;
  listener.Bind("127.0.0.1", 0);
  listener.Listen(1);
  const std::uint16_t port = listener.LocalPort();

  std::exception_ptr server_error;
  std::jthread server_thread([&]() {
    try {
      xrpc::io::Socket accepted = listener.Accept();
      const std::string received = ReadExactly(accepted, 4);
      EXPECT_EQ(received, "ping");

      accepted.WriteAll("pong");
      accepted.Close();
      accepted.Close();
    } catch (...) {
      server_error = std::current_exception();
    }
  });

  xrpc::io::Socket client;
  client.Connect("127.0.0.1", port);
  client.WriteAll("ping");
  client.ShutdownWrite();

  const std::string response = ReadExactly(client, 4);
  EXPECT_EQ(response, "pong");
  client.Close();
  client.Close();

  server_thread.join();
  if (server_error) {
    std::rethrow_exception(server_error);
  }

  listener.Close();
  listener.Close();
}

TEST(IoSocketTest, InvalidAddressProducesStructuredError) {
  xrpc::io::Socket socket;

  try {
    socket.Connect("not-an-ip-address", 9010);
    FAIL() << "expected SocketError";
  } catch (const xrpc::io::SocketError &error) {
    EXPECT_EQ(error.code(), xrpc::io::SocketErrorCode::InvalidAddress);
    EXPECT_EQ(error.system_error(), 0);
  }
}
