#include "client/endpoint_selector.h"

#include <algorithm>
#include <iterator>
#include <ranges>
#include <string>

namespace xrpc {

namespace {

/** @brief Number of virtual ring positions assigned to each active endpoint. */
constexpr std::size_t VIRTUAL_NODE_COUNT = 128;

/** @brief FNV-1a 64-bit offset basis used for stable sticky routing hashes. */
constexpr std::uint64_t FNV1A_OFFSET_BASIS = 14695981039346656037ULL;

/** @brief FNV-1a 64-bit prime used for stable sticky routing hashes. */
constexpr std::uint64_t FNV1A_PRIME = 1099511628211ULL;

}  // namespace

/**
 * @brief Computes a stable 64-bit FNV-1a hash for routing keys and virtual nodes.
 *
 * The selector intentionally avoids `std::hash` because its value is implementation-specific and
 * may vary across standard-library versions. Stable hashing keeps sticky routing reproducible.
 *
 * @param value Key bytes to hash.
 * @return Deterministic unsigned 64-bit hash.
 */
auto EndpointSelector::Fnv1a64(std::string_view value) -> std::uint64_t {
  std::uint64_t hash = FNV1A_OFFSET_BASIS;
  for (const char ch : value) {
    hash ^= static_cast<unsigned char>(ch);
    hash *= FNV1A_PRIME;
  }
  return hash;
}

/**
 * @brief Builds the consistent-hash ring for the current active endpoint snapshot.
 *
 * Each endpoint receives multiple virtual nodes to reduce remapping when endpoint membership
 * changes. Entries are sorted by hash, with endpoint id as the deterministic tie breaker.
 *
 * @param active_endpoint_ids Endpoint ids currently eligible for new calls.
 * @return Sorted hash-ring entries used by sticky-key lookups.
 */
auto EndpointSelector::BuildHashRing(const std::vector<std::string> &active_endpoint_ids)
    -> std::vector<HashRingEntry> {
  std::vector<HashRingEntry> hash_ring;
  hash_ring.reserve(active_endpoint_ids.size() * VIRTUAL_NODE_COUNT);
  for (const std::string &endpoint_id : active_endpoint_ids) {
    for (std::size_t vnode = 0; vnode < VIRTUAL_NODE_COUNT; ++vnode) {
      hash_ring.push_back(HashRingEntry{
          .hash_ = Fnv1a64(endpoint_id + "#" + std::to_string(vnode)),
          .endpoint_id_ = endpoint_id,
      });
    }
  }
  std::ranges::sort(hash_ring, [](const HashRingEntry &lhs, const HashRingEntry &rhs) {
    if (lhs.hash_ != rhs.hash_) {
      return lhs.hash_ < rhs.hash_;
    }
    return lhs.endpoint_id_ < rhs.endpoint_id_;
  });
  return hash_ring;
}

/**
 * @brief Maps a sticky key to the starting endpoint index for one call attempt sequence.
 *
 * The returned index is only the first candidate. The caller may still walk subsequent active
 * endpoints when the preferred endpoint is unavailable or saturated.
 *
 * @param sticky_key Optional caller-provided routing key.
 * @param hash_ring Sorted ring built from the same active endpoint snapshot.
 * @param active_endpoint_ids Active endpoint ids in retry order.
 * @return Starting index in `active_endpoint_ids`, or empty when sticky routing is disabled.
 */
auto EndpointSelector::SelectStickyStartIndex(std::string_view sticky_key, const std::vector<HashRingEntry> &hash_ring,
                                              const std::vector<std::string> &active_endpoint_ids)
    -> std::optional<std::size_t> {
  if (sticky_key.empty() || hash_ring.empty()) {
    return std::nullopt;
  }

  const std::uint64_t key_hash = Fnv1a64(sticky_key);
  const auto ring_it =
      std::ranges::lower_bound(hash_ring, key_hash, {}, [](const HashRingEntry &entry) { return entry.hash_; });
  const std::string &endpoint_id = ring_it == hash_ring.end() ? hash_ring.front().endpoint_id_ : ring_it->endpoint_id_;
  const auto endpoint_it = std::ranges::find(active_endpoint_ids, endpoint_id);
  if (endpoint_it == active_endpoint_ids.end()) {
    return std::nullopt;
  }

  return static_cast<std::size_t>(std::distance(active_endpoint_ids.begin(), endpoint_it));
}

/**
 * @brief Chooses the next round-robin starting index for calls without a sticky key.
 *
 * The counter only needs relaxed atomic ordering because it is a fairness hint, not a correctness
 * dependency. Transport state performs the real concurrency admission checks later.
 *
 * @param active_endpoint_count Number of endpoints eligible for new calls.
 * @return Starting index in the active endpoint vector, or zero for an empty snapshot.
 */
auto EndpointSelector::SelectRoundRobinStartIndex(std::size_t active_endpoint_count) -> std::size_t {
  if (active_endpoint_count == 0) {
    return 0;
  }

  const std::size_t current = next_endpoint_index_.fetch_add(1, std::memory_order_relaxed) % active_endpoint_count;
  return current;
}

}  // namespace xrpc
