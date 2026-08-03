#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "rpc/client/endpoint_selector.h"

namespace {

constexpr std::size_t VIRTUAL_NODE_COUNT = 128;
constexpr std::uint64_t FNV1A_OFFSET_BASIS = 14695981039346656037ULL;
constexpr std::uint64_t FNV1A_PRIME = 1099511628211ULL;

auto Fnv1a64(std::string_view value) -> std::uint64_t {
  std::uint64_t hash = FNV1A_OFFSET_BASIS;
  for (const char ch : value) {
    hash ^= static_cast<unsigned char>(ch);
    hash *= FNV1A_PRIME;
  }
  return hash;
}

auto RouteStickyKey(const std::vector<std::string> &endpoint_ids, std::string_view sticky_key) -> std::string {
  std::vector<std::pair<std::uint64_t, std::string>> ring;
  for (const std::string &endpoint_id : endpoint_ids) {
    for (std::size_t vnode = 0; vnode < VIRTUAL_NODE_COUNT; ++vnode) {
      ring.emplace_back(Fnv1a64(endpoint_id + "#" + std::to_string(vnode)), endpoint_id);
    }
  }
  std::sort(ring.begin(), ring.end());
  const std::uint64_t key_hash = Fnv1a64(sticky_key);
  const auto it = std::lower_bound(ring.begin(), ring.end(), std::pair<std::uint64_t, std::string>{key_hash, {}});
  return it == ring.end() ? ring.front().second : it->second;
}

auto StickyKeyForEndpoint(std::string_view endpoint_id, const std::vector<std::string> &endpoint_ids) -> std::string {
  for (std::size_t i = 0; i < 10000; ++i) {
    const std::string candidate = "sticky-" + std::to_string(i);
    if (RouteStickyKey(endpoint_ids, candidate) == endpoint_id) {
      return candidate;
    }
  }
  throw std::runtime_error("failed to find sticky key for endpoint");
}

}  // namespace

TEST(EndpointSelectorTest, StickySelectionUsesHashRingAndPreservesEndpointOrder) {
  const std::vector<std::string> endpoint_ids = {"127.0.0.1:10001", "127.0.0.1:10002"};
  const std::string sticky_for_first = StickyKeyForEndpoint(endpoint_ids[0], endpoint_ids);
  const std::string sticky_for_second = StickyKeyForEndpoint(endpoint_ids[1], endpoint_ids);

  const std::vector<xrpc::EndpointSelector::HashRingEntry> hash_ring =
      xrpc::EndpointSelector::BuildHashRing(endpoint_ids);

  const auto first_index = xrpc::EndpointSelector::SelectStickyStartIndex(sticky_for_first, hash_ring, endpoint_ids);
  ASSERT_TRUE(first_index.has_value());
  EXPECT_EQ(endpoint_ids[*first_index], endpoint_ids[0]);

  const auto second_index = xrpc::EndpointSelector::SelectStickyStartIndex(sticky_for_second, hash_ring, endpoint_ids);
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
