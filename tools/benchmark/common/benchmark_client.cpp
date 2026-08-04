#include "common/benchmark_client.h"

#include "common/firehose_client.h"
#include "common/rpc_client_benchmark.h"

namespace xrpc::benchmark {

auto RunBenchmark(const BenchmarkConfig &config) -> BenchmarkStats {
  if (config.client_mode_ == BenchmarkClientMode::RpcClient) {
    return RunRpcClientBenchmark(config);
  }
  return RunFirehoseBenchmark(config);
}

}  // namespace xrpc::benchmark
