# 性能测试工具

本文说明性能测试工具的使用方式、配置矩阵和结果记录规则。

当前性能测试只支持独立服务端模式：

- `xrpc_benchmark_server`：独立服务端进程
- `xrpc_benchmark_client`：独立客户端进程
- `run_suite.py`：启动服务端、执行客户端矩阵、保存审计结果

## 构建

```bash
cmake -S . -B build -DXRPC_BUILD_TOOLS=ON -DXRPC_BUILD_TESTS=OFF -DXRPC_BUILD_EXAMPLES=OFF
cmake --build build --target xrpc_benchmark_server xrpc_benchmark_client
```

生成的可执行文件位于 `build/tools/benchmark/`。

## 服务端

启动性能测试服务端：

```bash
./build/tools/benchmark/xrpc_benchmark_server --host=127.0.0.1 --port=9010
```

可选参数：

- `--server_delay_us=N`
- `--metrics_host=IP`
- `--metrics_port=N`
- `--service_name=NAME`
- `--service_id=ID`
- `--service_address=IP`
- `--service_port=N`
- `--consul_address=HOST:PORT`
- `--consul_timeout_ms=N`
- `--worker_threads=N`
- `--connection_io_threads=N`
- `--listen_backlog=N`
- `--workload=protobuf|raw`

服务端启动成功后会输出：

```text
ready host=127.0.0.1 port=9010 workload=raw worker_threads=0 connection_io_threads=1 metrics_host=0.0.0.0 metrics_port=0 delay_us=0
```

独立启动服务端时，`worker_threads=0` 表示由运行时使用 `hardware_concurrency()` 自动确定。测试套件运行器不使用隐式值，工作线程数来自配置文件。
`connection_io_threads=1` 表示 1 个连接 I/O 事件循环，不包含接收事件循环。服务端始终使用独立的接收事件循环建立连接，再以轮询方式把文件描述符分发给连接 I/O 事件循环。

`metrics_port=0` 是默认值，表示不启用 Prometheus 指标导出器，以免影响常规性能测试。需要调试或高可用指标时可显式开启：

```bash
./build/tools/benchmark/xrpc_benchmark_server \
  --host=127.0.0.1 \
  --port=9010 \
  --metrics_host=0.0.0.0 \
  --metrics_port=9101
```

## 客户端

客户端性能测试提供两条互补路径：

- `client_mode=firehose`：使用 epoll 管理专用发压连接，通过固定在途请求数持续向服务端施压，用于测量
  服务端容量。它支持 `raw` 和 `protobuf` 两种负载。
- `client_mode=rpc_client`：使用正式的 `RpcClient::Call()` 发起类型化回显调用，用于测量真实客户端路径的
  稳态延迟和并发扩展能力，不用于推断服务端上限。

`firehose_connections` 只属于性能测试发压器，不是正式 `RpcClient` 的连接模型。类型化 Firehose
会预先序列化请求以隔离发压端开销，但服务端仍经过方法查找、类型擦除分发、请求 Protobuf 解析和响应
Protobuf 序列化。

示例：

```bash
./build/tools/benchmark/xrpc_benchmark_client \
  --client_mode=firehose \
  --workload=protobuf \
  --duration_s=30 \
  --payload_size=128 \
  --firehose_connections=3 \
  --firehose_inflight=384 \
  --host=127.0.0.1 \
  --port=9010
```

客户端参数：

- `--client_mode=firehose|rpc_client`
- `--client_threads=N`
- `--metrics_host=IP`
- `--metrics_port=N`
- `--payload_size=N`
- `--host=IP`
- `--port=N`
- `--duration_s=N`
- `--workload=protobuf|raw`
- `--firehose_connections=N`
- `--firehose_inflight=N`
- `--firehose_io_threads=N`

`firehose_io_threads=0` 表示自动选择。测试套件运行器在配置了客户端 CPU 亲和性时，会取
客户端 CPU 数量和连接数的较小值；否则由客户端进程按当前 CPU 亲和性和连接数确定。
`rpc_client` 模式要求 `workload=protobuf`，`client_threads` 表示共享一个 `RpcClient` 的同步调用线程数。

## 测试套件运行器

测试套件运行器只读取配置文件执行性能测试矩阵。负载、持续时间、工作线程数、
Firehose 参数和 CPU 绑定都来自 JSON 配置。

标准命令模板：

```bash
./tools/benchmark/run_suite.py \
  --config tools/benchmark/configs/v1_server_capacity.json \
  --build
```

需要性能剖析时，在模板后追加 `--case` 和 `--client-perf` / `--server-perf`。

只校验配置文件，不构建、不启动服务端、不运行压测：

```bash
./tools/benchmark/run_suite.py \
  --config tools/benchmark/configs/v1_smoke.json \
  --validate-config
```

当前提供的配置：

| 配置 | 用途 |
| --- | --- |
| `tools/benchmark/configs/v1_smoke.json` | XRPC v1 官方冒烟测试，验证性能测试链路，不作为性能结论 |
| `tools/benchmark/configs/v1_protobuf_smoke.json` | 验证类型化 Firehose、Protobuf 服务端路径和响应校验 |
| `tools/benchmark/configs/v1_server_capacity.json` | 原始 Firehose 核心传输容量探针 |
| `tools/benchmark/configs/v1_raw_high_load.json` | 原始 Firehose 高负载上限复测 |
| `tools/benchmark/configs/v1_protobuf_capacity.json` | 类型化 Protobuf 服务端容量与延迟曲线 |
| `tools/benchmark/configs/v1_protobuf_steady_state.json` | 类型化 Protobuf 低延迟点与饱和点稳态复测 |
| `tools/benchmark/configs/v1_protobuf_ceiling_connections.json` | 固定总并发下扫描类型化 Firehose 连接数 |
| `tools/benchmark/configs/v1_protobuf_ceiling_load.json` | 按每连接并发上限扫描类型化服务端吞吐平台 |
| `tools/benchmark/configs/v1_protobuf_ceiling_boundary.json` | 在默认全局待处理上限附近扫描无拒绝边界 |
| `tools/benchmark/configs/v1_protobuf_ceiling_verify.json` | 长时重复验证类型化服务端无拒绝吞吐上限 |
| `tools/benchmark/configs/v1_protobuf_overload_probe.json` | 越过默认全局待处理上限，验证过载拒绝边界 |
| `tools/benchmark/configs/v1_rpc_client.json` | 正式 `RpcClient` 稳态延迟与并发扩展曲线 |

配置文件字段：

- `description`
- `client_mode`
- `client_threads`
- `workload`
- `warmup_duration`
- `repetitions`
- `firehose_cases`，显式的 `(firehose_connections, firehose_inflight)` case 列表
- `firehose_connections`，firehose 配置中可以是整数或整数列表
- `firehose_inflight`，firehose 配置中可以是整数或整数列表
- `duration`
- `payload_size`
- `server_delay_us`
- `server_worker_threads`
- `server_connection_io_threads`
- `server_listen_backlog`
- `server_lifecycle`
- `server_cpus`
- `client_cpus`
- `host`
- `port`
- `run_timeout`

官方 v1 配置通过 CTest 做轻量校验：

- `benchmark_v1_smoke_config_test`
- `benchmark_v1_server_capacity_config_test`
- `benchmark_v1_raw_high_load_config_test`
- `benchmark_v1_protobuf_smoke_config_test`
- `benchmark_v1_protobuf_capacity_config_test`
- `benchmark_v1_protobuf_steady_state_config_test`
- `benchmark_v1_protobuf_ceiling_connections_config_test`
- `benchmark_v1_protobuf_ceiling_load_config_test`
- `benchmark_v1_protobuf_ceiling_boundary_config_test`
- `benchmark_v1_protobuf_ceiling_verify_config_test`
- `benchmark_v1_protobuf_overload_probe_config_test`
- `benchmark_v1_rpc_client_config_test`

`raw` 只做负载回显，用于隔离网络、协议和调度路径成本，不代表类型化 Protobuf RPC。`protobuf`
使用同一个 Firehose 发压器，但服务端运行正式 `RpcServer` 的类型化回显路径。配置中的
`payload_size` 在 `protobuf` 模式下表示回显消息字段长度，实际序列化负载会包含 Protobuf
字段开销。

`warmup_duration` 会在每次正式测量前使用相同负载预热服务端；`repetitions` 为每个测试用例
生成独立运行，并在汇总中报告中位数和范围。运行器按重复轮次交错执行矩阵用例，降低
持续负载下温度和调频对用例顺序的系统性影响。`server_lifecycle=per_case` 时，每次重复
都会启动全新的服务端。每轮结束后会增量写入 `runs.csv` 和 `summary.md`，异常中止时保留已完成结果。

`firehose_cases` 用于连接数和在途请求数必须成对变化的非笛卡尔积扫描。设置后会覆盖
`firehose_connections` 和 `firehose_inflight` 生成的矩阵。

`server_lifecycle=per_case` 表示每个矩阵用例启动一个全新的性能测试服务端，这是标准性能对比口径。
`server_lifecycle=per_suite` 表示整个矩阵复用一个长生命周期服务端，只用于观察连续用例对服务端状态的影响。

`server_cpus` 和 `client_cpus` 可以同时设置为 `null` 表示不绑核；官方配置显式写出 CPU 集合。运行器只校验 CPU 列表格式和客户端、服务端是否重叠，不解析机器拓扑。

要改变测试口径，请新增或修改 `tools/benchmark/configs/*.json`，不要在运行器命令行临时拼接矩阵参数。最终配置会写入 `config.json`、`summary.md` 和原始命令日志。

结果写入 `build/benchmark-results/<timestamp>/`：

- `summary.md`：每组成功 QPS 中位数、范围、失败数和 p99
- `runs.csv`：每次运行的结构化结果
- `logs/`：服务端和客户端的原始输出
- `profiles/`：使用 `--client-perf` 或 `--server-perf` 时生成的 `perf.data` 和 perf 日志
- `environment.json`：提交、工作区、CPU、内核和编译器信息
- `config.json`：本次 suite 参数

`summary.md` 和 `runs.csv` 还会记录客户端总 CPU、最高客户端线程 CPU、服务端总 CPU 和最高服务端线程 CPU。CPU 百分比以一个逻辑 CPU 为 100%。`runs.csv` 中的 `client_thread_cpu` 和 `server_thread_cpu` 保留每个线程标识符的明细。客户端线程 CPU 在客户端进程运行期间周期性采样，进程退出前最后一次快照作为结束点；开启 `--client-perf` 时，运行器会采集 perf 包装进程的子进程，避免把 perf 自身当作性能测试客户端。

启用 `--client-perf` 或 `--server-perf` 时，每次性能剖析都会保留一组可审计产物：

- `.data`：原始 `perf record` 数据
- `.report.txt`：`perf report --stdio` 文本报告
- `.script.txt`：`perf script` 展开的原始栈
- `.folded`：FlameGraph 折叠栈
- `.flamegraph.svg`：浏览器可打开的火焰图
- `.artifacts.log`：后处理命令和错误日志

例如某个独立用例的服务端性能剖析结果：

```bash
xdg-open build/benchmark-results/<timestamp>/profiles/server_fhconn-<m>_fh-<k>.flamegraph.svg
less build/benchmark-results/<timestamp>/profiles/server_fhconn-<m>_fh-<k>.report.txt
```

生成 SVG 依赖本机可执行的 `stackcollapse-perf.pl` 和 `flamegraph.pl`。如果找不到 FlameGraph 工具，运行器仍会保留 `.data`、`.report.txt` 和 `.script.txt`，并把原因写入 `.artifacts.log`。

如果只想运行某个实验设置，并在本次运行中采集性能剖析数据，请使用 `--case`：

```bash
./tools/benchmark/run_suite.py \
  --config tools/benchmark/configs/v1_server_capacity.json \
  --server-perf \
  --case=firehose_connections=<connections>,firehose_inflight=<inflight>
```
