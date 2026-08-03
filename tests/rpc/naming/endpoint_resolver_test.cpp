#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include <xrpc/xrpc_exception.h>

#include "rpc/naming/endpoint_resolver.h"

TEST(EndpointResolverTest, ListTargetCanonicalizesSnapshot) {
  std::unique_ptr<xrpc::EndpointResolver> resolver = xrpc::MakeEndpointResolver(xrpc::ResolverOptions{
      .target_ = "list://127.0.0.1:9002, 127.0.0.1:9001,127.0.0.1:9002",
  });

  const xrpc::Status status = resolver->Start();
  ASSERT_TRUE(status.ok()) << status.message();

  const std::vector<xrpc::Endpoint> snapshot = resolver->Snapshot();
  ASSERT_EQ(snapshot.size(), 2U);
  EXPECT_EQ(snapshot[0].host_, "127.0.0.1");
  EXPECT_EQ(snapshot[0].port_, 9001);
  EXPECT_EQ(snapshot[1].host_, "127.0.0.1");
  EXPECT_EQ(snapshot[1].port_, 9002);
}

TEST(EndpointResolverTest, RejectsInvalidTargetScheme) {
  EXPECT_THROW(static_cast<void>(xrpc::MakeEndpointResolver(xrpc::ResolverOptions{
                   .target_ = "dns://EchoService",
               })),
               xrpc::ConfigException);
}

TEST(EndpointResolverTest, RejectsEmptyConsulServiceName) {
  EXPECT_THROW(static_cast<void>(xrpc::MakeEndpointResolver(xrpc::ResolverOptions{
                   .target_ = "consul://",
               })),
               xrpc::ConfigException);
}

TEST(EndpointResolverTest, RejectsConsulTargetWithoutConsulAddress) {
  EXPECT_THROW(static_cast<void>(xrpc::MakeEndpointResolver(xrpc::ResolverOptions{
                   .target_ = "consul://EchoService",
               })),
               xrpc::ConfigException);
}

TEST(EndpointResolverTest, RejectsNegativeRefreshInterval) {
  EXPECT_THROW(static_cast<void>(xrpc::MakeEndpointResolver(xrpc::ResolverOptions{
                   .target_ = "list://127.0.0.1:9000",
                   .discovery_refresh_interval_ = std::chrono::milliseconds(-1),
               })),
               xrpc::ConfigException);
}
