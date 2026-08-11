#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace xrpc {

/**
 * @brief Endpoint selection policy used by `ClientChannel`.
 *
 * Sticky routing uses a deterministic hash ring so the same key starts at the same endpoint while the active endpoint
 * set is unchanged. Non-sticky routing uses atomic round-robin start positions so concurrent callers spread their first
 * attempts without a channel-wide mutex.
 */
class EndpointSelector final {
 public:
  /** @brief One entry in the sticky-routing hash ring. */
  struct HashRingEntry {
    /** @brief FNV-1a hash of the endpoint id. */
    std::uint64_t hash_ = 0;

    /** @brief Stable endpoint id associated with `hash_`. */
    std::string endpoint_id_;
  };

  EndpointSelector() = default;

  /** @return Sorted hash-ring entries for the active endpoint ids. */
  [[nodiscard]] static auto BuildHashRing(const std::vector<std::string> &active_endpoint_ids)
      -> std::vector<HashRingEntry>;

  /**
   * @brief Selects the first endpoint index for a sticky key.
   *
   * @return Index into `active_endpoint_ids`, or nullopt when no active endpoint can be selected.
   */
  [[nodiscard]] static auto SelectStickyStartIndex(std::string_view sticky_key,
                                                   const std::vector<HashRingEntry> &hash_ring,
                                                   const std::vector<std::string> &active_endpoint_ids)
      -> std::optional<std::size_t>;

  /** @return Round-robin start index in the range `[0, active_endpoint_count)`. */
  [[nodiscard]] auto SelectRoundRobinStartIndex(std::size_t active_endpoint_count) -> std::size_t;

 private:
  /** @brief Computes the FNV-1a 64-bit hash used by sticky routing. */
  [[nodiscard]] static auto Fnv1a64(std::string_view value) -> std::uint64_t;

  std::atomic<std::size_t> next_endpoint_index_{0};
};

}  // namespace xrpc
