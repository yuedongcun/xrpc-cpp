#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "rpc/client/endpoint_state_table.h"

namespace {

auto MakeEndpoint(std::string host, std::uint16_t port) -> xrpc::Endpoint {
  xrpc::Endpoint endpoint;
  endpoint.host_ = std::move(host);
  endpoint.port_ = port;
  return endpoint;
}

}  // namespace

TEST(EndpointStateTableTest, UpdateEndpointsPublishesActiveSnapshotAndLookup) {
  const xrpc::Endpoint endpoint0 = MakeEndpoint("127.0.0.1", 10001);
  const xrpc::Endpoint endpoint1 = MakeEndpoint("127.0.0.1", 10002);

  xrpc::EndpointStateTable table;
  table.UpdateEndpoints({endpoint0, endpoint1});

  const std::vector<std::string> &active_endpoint_ids = table.ActiveEndpointIds();
  ASSERT_EQ(active_endpoint_ids.size(), 2U);
  EXPECT_EQ(active_endpoint_ids[0], xrpc::EndpointStateTable::MakeEndpointId(endpoint0));
  EXPECT_EQ(active_endpoint_ids[1], xrpc::EndpointStateTable::MakeEndpointId(endpoint1));

  const xrpc::Endpoint *found_endpoint0 = table.FindEndpoint(active_endpoint_ids[0]);
  ASSERT_NE(found_endpoint0, nullptr);
  EXPECT_EQ(*found_endpoint0, endpoint0);
}

TEST(EndpointStateTableTest, UpdateEndpointsReportsAndCleansUpRemovedEndpoints) {
  const xrpc::Endpoint endpoint0 = MakeEndpoint("127.0.0.1", 10001);
  const xrpc::Endpoint endpoint1 = MakeEndpoint("127.0.0.1", 10002);

  xrpc::EndpointStateTable table;
  table.UpdateEndpoints({endpoint0, endpoint1});
  EXPECT_TRUE(table.TakeDrainedEndpointIds().empty());

  table.UpdateEndpoints({endpoint1});
  const std::vector<std::string> drained_endpoint_ids = table.TakeDrainedEndpointIds();
  ASSERT_EQ(drained_endpoint_ids.size(), 1U);
  EXPECT_EQ(drained_endpoint_ids[0], xrpc::EndpointStateTable::MakeEndpointId(endpoint0));

  table.CleanupDrainedEndpoints();
  EXPECT_EQ(table.ActiveEndpointIds().size(), 1U);
  EXPECT_EQ(table.FindEndpoint(xrpc::EndpointStateTable::MakeEndpointId(endpoint0)), nullptr);
  const xrpc::Endpoint *found_endpoint1 = table.FindEndpoint(xrpc::EndpointStateTable::MakeEndpointId(endpoint1));
  ASSERT_NE(found_endpoint1, nullptr);
  EXPECT_EQ(*found_endpoint1, endpoint1);
}

TEST(EndpointStateTableTest, MakeEndpointIdUsesV1HostPortFormat) {
  const xrpc::Endpoint endpoint = MakeEndpoint("127.0.0.1", 10001);

  EXPECT_EQ(xrpc::EndpointStateTable::MakeEndpointId(endpoint), "127.0.0.1:10001");
}
