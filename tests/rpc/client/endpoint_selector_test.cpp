#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "rpc/client/endpoint_selector.h"

TEST(EndpointSelectorTest, StickySelectionUsesCallerKeyAndActiveSnapshotOrder) {
  const std::vector<std::string> endpoint_ids = {"127.0.0.1:10001", "127.0.0.1:10002"};
  const std::vector<xrpc::EndpointSelector::HashRingEntry> hash_ring =
      xrpc::EndpointSelector::BuildHashRing(endpoint_ids);

  const auto first_index = xrpc::EndpointSelector::SelectStickyStartIndex("alpha", hash_ring, endpoint_ids);
  ASSERT_TRUE(first_index.has_value());
  EXPECT_EQ(endpoint_ids[*first_index], endpoint_ids[0]);

  const auto second_index = xrpc::EndpointSelector::SelectStickyStartIndex("sticky-0", hash_ring, endpoint_ids);
  ASSERT_TRUE(second_index.has_value());
  EXPECT_EQ(endpoint_ids[*second_index], endpoint_ids[1]);
}

TEST(EndpointSelectorTest, StickySelectionReturnsNullWhenNoEndpointsOrNoKey) {
  const std::vector<std::string> endpoint_ids = {"127.0.0.1:10001"};
  const std::vector<xrpc::EndpointSelector::HashRingEntry> hash_ring =
      xrpc::EndpointSelector::BuildHashRing(endpoint_ids);

  EXPECT_FALSE(xrpc::EndpointSelector::SelectStickyStartIndex("", hash_ring, endpoint_ids).has_value());
  EXPECT_FALSE(xrpc::EndpointSelector::SelectStickyStartIndex("sticky", {}, {}).has_value());
}

TEST(EndpointSelectorTest, RoundRobinSelectionAdvancesAndWraps) {
  xrpc::EndpointSelector selector;

  EXPECT_EQ(selector.SelectRoundRobinStartIndex(3), 0U);
  EXPECT_EQ(selector.SelectRoundRobinStartIndex(3), 1U);
  EXPECT_EQ(selector.SelectRoundRobinStartIndex(3), 2U);
  EXPECT_EQ(selector.SelectRoundRobinStartIndex(3), 0U);
}
