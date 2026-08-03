#include "rpc/client/endpoint_state_table.h"

#include <string>
#include <unordered_set>
#include <utility>

namespace xrpc {

/**
 * @brief Applies a new discovery snapshot while preserving state for draining endpoints.
 *
 * Endpoints present in the new snapshot become active immediately. Endpoints that disappear are
 * marked draining so their transports can be closed after outstanding calls have been accounted for.
 *
 * @param endpoints Resolver-provided endpoint snapshot in caller-visible priority order.
 */
void EndpointStateTable::UpdateEndpoints(const std::vector<Endpoint> &endpoints) {
  std::unordered_set<std::string> next_endpoint_ids;
  next_endpoint_ids.reserve(endpoints.size());

  active_endpoint_ids_.clear();
  active_endpoint_ids_.reserve(endpoints.size());

  for (const Endpoint &endpoint : endpoints) {
    const std::string endpoint_id = MakeEndpointId(endpoint);
    next_endpoint_ids.insert(endpoint_id);
    active_endpoint_ids_.push_back(endpoint_id);

    auto &entry = endpoint_entries_[endpoint_id];
    entry.endpoint_ = endpoint;
    entry.draining_ = false;
  }

  for (auto &entry : endpoint_entries_) {
    if (next_endpoint_ids.contains(entry.first)) {
      continue;
    }
    if (!entry.second.draining_) {
      drained_endpoint_ids_.push_back(entry.first);
    }
    entry.second.draining_ = true;
  }
}

/**
 * @brief Returns the current active endpoint ids in routing order.
 *
 * @return Reference valid until the next `UpdateEndpoints()` call on this table.
 */
auto EndpointStateTable::ActiveEndpointIds() const -> const std::vector<std::string> & { return active_endpoint_ids_; }

/**
 * @brief Looks up an endpoint record by its stable id.
 *
 * @param endpoint_id Value produced by `MakeEndpointId()`.
 * @return Pointer to the endpoint address, or null when the id is unknown.
 */
auto EndpointStateTable::FindEndpoint(const std::string &endpoint_id) const -> const Endpoint * {
  const auto it = endpoint_entries_.find(endpoint_id);
  if (it == endpoint_entries_.end()) {
    return nullptr;
  }
  return &it->second.endpoint_;
}

/**
 * @brief Moves out endpoint ids that just transitioned into draining state.
 *
 * @return Drained endpoint ids that the channel should stop using for new calls.
 */
auto EndpointStateTable::TakeDrainedEndpointIds() -> std::vector<std::string> {
  std::vector<std::string> drained_endpoint_ids = std::move(drained_endpoint_ids_);
  drained_endpoint_ids_.clear();
  return drained_endpoint_ids;
}

/**
 * @brief Removes endpoint records that have already been marked draining.
 *
 * This is called after the channel has closed or released the corresponding transport state.
 */
void EndpointStateTable::CleanupDrainedEndpoints() {
  for (auto it = endpoint_entries_.begin(); it != endpoint_entries_.end();) {
    if (it->second.draining_) {
      it = endpoint_entries_.erase(it);
      continue;
    }
    ++it;
  }
}

/**
 * @brief Builds the stable string id used as the endpoint table and transport map key.
 *
 * @param endpoint Endpoint address in normalized host/port form.
 * @return `host:port` endpoint id.
 */
auto EndpointStateTable::MakeEndpointId(const Endpoint &endpoint) -> std::string {
  return endpoint.host_ + ":" + std::to_string(endpoint.port_);
}

}  // namespace xrpc
