/**
 * @file static_discovery.h
 * @brief Defines discovery backed by a static endpoint list.
 */

#pragma once

#include <string_view>
#include <vector>

#include "naming/service_discovery.h"

namespace xrpc {

/**
 * @brief Publishes an immutable snapshot parsed from a configured endpoint list.
 *
 * Construction parses and validates the complete target. `Start()` and
 * `Stop()` have no runtime work because the snapshot never changes.
 */
class StaticDiscovery final : public ServiceDiscovery {
 public:
  explicit StaticDiscovery(std::string_view target);

  [[nodiscard]] auto Start() -> Status override;
  void Stop() override;
  [[nodiscard]] auto Snapshot() const -> std::shared_ptr<const DiscoverySnapshot> override;
  [[nodiscard]] auto last_error() const -> std::string override;

 private:
  std::shared_ptr<const DiscoverySnapshot> snapshot_;
};

}  // namespace xrpc
