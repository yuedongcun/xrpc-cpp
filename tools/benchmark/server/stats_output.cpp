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
                        {"peaks", nlohmann::json::object()},
                        {"counters", {{"provided_buffer_enobufs", c.provided_buffer_enobufs_}}},
                        {"gauges", nlohmann::json::object()}};
  if (snapshot.buffer_pool_) {
    const auto &pool = *snapshot.buffer_pool_;
    output["gauges"]["buffer_capacity"] = pool.capacity_;
    output["gauges"]["buffer_size"] = pool.buffer_size_;
    output["gauges"]["buffer_outstanding_leases"] = pool.outstanding_leases_;
    output["peaks"]["buffer_outstanding_leases"] = pool.outstanding_leases_peak_;
  }
  return output;
}

auto ToJson(const WorkerPoolStatsSnapshot &pool) -> nlohmann::json {
  nlohmann::json worker{{"window_id", pool.window_id_},
                        {"pending_logical_jobs", pool.pending_logical_jobs_},
                        {"queues", nlohmann::json::array()}};
  for (std::size_t index = 0; index < pool.queues_.size(); ++index) {
    const auto &q = pool.queues_[index];
    worker["queues"].push_back(
        {{"worker_id", index},
         {"gauges",
          {{"queued_batches", q.queued_batches_},
           {"queued_logical_jobs", q.queued_logical_jobs_},
           {"pending_batches", q.pending_batches_},
           {"pending_logical_jobs", q.pending_logical_jobs_}}},
         {"peaks",
          {{"queued_batches", q.queued_batches_peak_}, {"queued_logical_jobs", q.queued_logical_jobs_peak_}}}});
  }
  return worker;
}

}  // namespace

auto WriteStatsSnapshot(RpcServer &server, const std::string &path, bool start_window) -> Status {
  try {
    const auto result = ServerStatsAccess::Snapshot(server, start_window);
    nlohmann::json output{{"schema_version", 5}, {"scope", "server_runtime"}};
    if (!result.ok()) {
      output["error"] = result.status().message();
    } else {
      output["worker_pool"] = ToJson(result.value().worker_pool_);
      output["loops"] = nlohmann::json::array();
      for (std::size_t index = 0; index < result.value().loops_.size(); ++index) {
        const auto &loop = result.value().loops_[index];
        auto encoded = ToJson(loop.uring_, index);
        encoded["gauges"]["live_connections"] = loop.live_connections_;
        encoded["gauges"]["pending_write_bytes"] = loop.pending_write_bytes_;
        encoded["peaks"]["pending_write_bytes"] = loop.pending_write_bytes_peak_;
        output["loops"].push_back(std::move(encoded));
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
