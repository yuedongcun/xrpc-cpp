#include "server/stats_output.h"

#include <filesystem>
#include <fstream>

#include <nlohmann/json.hpp>

#include "common/xrpc_exception.h"
#include "server/runtime_stats.h"

namespace xrpc::benchmark {
namespace {

auto ToJson(const io::UringStatsSnapshot &snapshot, std::size_t loop_id) -> nlohmann::json {
  const auto &c = snapshot.counters_;
  nlohmann::json output{{"loop_id", loop_id},
                        {"window_id", snapshot.window_id_},
                        {"peaks",
                         {{"staged_operations", snapshot.peaks_.staged_operations_},
                          {"cq_ready_sampled", snapshot.peaks_.cq_ready_sampled_}}},
                        {"counters",
                         {{"prepared_accept_sqes", c.prepared_accept_sqes_},
                          {"prepared_recv_sqes", c.prepared_recv_sqes_},
                          {"prepared_multishot_recv_sqes", c.prepared_multishot_recv_sqes_},
                          {"prepared_send_sqes", c.prepared_send_sqes_},
                          {"prepared_cancel_sqes", c.prepared_cancel_sqes_},
                          {"prepared_wakeup_sqes", c.prepared_wakeup_sqes_},
                          {"submit_calls", c.submit_calls_},
                          {"submitted_sqes", c.submitted_sqes_},
                          {"recv_cqes", c.recv_cqes_},
                          {"received_bytes", c.received_bytes_},
                          {"provided_buffer_enobufs", c.provided_buffer_enobufs_}}},
                        {"gauges",
                         {{"staged_operations", snapshot.staged_operations_},
                          {"active_recv_requests", snapshot.active_recv_requests_},
                          {"cq_ready", snapshot.cq_ready_}}}};
  if (snapshot.buffer_pool_) {
    const auto &pool = *snapshot.buffer_pool_;
    output["counters"]["buffer_acquires"] = pool.acquires_;
    output["counters"]["buffer_returns"] = pool.returns_;
    output["gauges"]["buffer_capacity"] = pool.capacity_;
    output["gauges"]["buffer_outstanding_leases"] = pool.outstanding_leases_;
    output["peaks"]["buffer_outstanding_leases"] = pool.outstanding_leases_peak_;
  }
  return output;
}

}  // namespace

auto WriteStatsSnapshot(RpcServer &server, const std::string &path, bool start_window) -> Status {
  try {
    const auto result = ServerStatsAccess::Snapshot(server, start_window);
    nlohmann::json output{{"schema_version", 2}, {"scope", "connection_io_loops"}};
    if (!result.ok()) {
      output["error"] = result.status().message();
    } else {
      output["loops"] = nlohmann::json::array();
      for (std::size_t index = 0; index < result.value().size(); ++index) {
        output["loops"].push_back(ToJson(result.value()[index], index));
      }
    }
    const std::string temporary = path + ".tmp";
    std::ofstream file;
    file.exceptions(std::ios::failbit | std::ios::badbit);
    file.open(temporary);
    file << output.dump() << '\n';
    file.close();
    std::filesystem::rename(temporary, path);
    return Status::Ok();
  } catch (...) {  // XRPC_EXTERNAL_EXCEPTION_BOUNDARY: benchmark statistics output
    return CaughtExceptionToStatus("failed to write benchmark statistics");
  }
}

}  // namespace xrpc::benchmark
