# Benchmark 工具

这里保留三条性能测试路径：

- `firehose.json`：使用 benchmark 专用 Firehose 客户端持续施压，测量服务端完整 Protobuf RPC 路径的吞吐量与延迟。
- `connection-scale.json`：每条连接固定一个在途 RPC，逐步增加同时活跃的 TCP 连接数，观察连接规模下的服务端路径。
- `client.json`：使用正式 `RpcClient::Call()`，观察公开客户端 API 的延迟和并发扩展。

Firehose 是发压工具，不是生产客户端模型。生产客户端路径请看 `client.json`。

## 构建

```bash
make release
```

生成的可执行文件位于 `build-release/tools/benchmark/`：

- `xrpc_benchmark_server`
- `xrpc_benchmark_firehose`
- `xrpc_benchmark_client`

## 运行套件

benchmark 必须使用 Release 构建。日常开发使用 `build/`，性能测试使用独立的 `build-release/`。

```bash
make release
```

服务端容量测试：

```bash
./tools/benchmark/runner/run_suite.py \
  --config tools/benchmark/configs/firehose.json
```

活跃连接规模测试：

```bash
./tools/benchmark/runner/run_suite.py \
  --config tools/benchmark/configs/connection-scale.json
```

正式客户端路径测试：

```bash
./tools/benchmark/runner/run_suite.py \
  --config tools/benchmark/configs/client.json
```

## 单独启动

服务端：

```bash
./build-release/tools/benchmark/xrpc_benchmark_server \
  --host=127.0.0.1 \
  --port=9010
```

Firehose 客户端：

```bash
./build-release/tools/benchmark/xrpc_benchmark_firehose \
  --duration_s=30 \
  --payload_size=128 \
  --connections=12 \
  --inflight=1536 \
  --host=127.0.0.1 \
  --port=9010
```

正式 `RpcClient` 客户端：

```bash
./build-release/tools/benchmark/xrpc_benchmark_client \
  --threads=24 \
  --duration_s=10 \
  --payload_size=128 \
  --host=127.0.0.1 \
  --port=9010
```

## 结果

### 服务端容量

服务端容量测试固定 12 条 TCP 连接、128 字节 Protobuf Echo payload、3 个 Connection I/O 线程和 3 个 Worker 线程。每个工作点预热 3 秒、测量 30 秒并重复 3 次；测试顺序按固定种子在各轮间改变，表中报告中位数和三轮最小值—最大值。

benchmark 将每连接 inflight 上限设为 2,048，避免公开 API 的默认上限先于运行时容量截断测试。该设置仅用于容量测试，不改变 `RpcServerOptions` 的默认值。

| 全局并发请求数 | QPS 中位数 | QPS 范围 | p99 中位数 | p99 范围 | 失败数 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 24 | 44,198 | 44,193–45,061 | 0.98 ms | 0.93–0.99 ms | 0 |
| 48 | 91,993 | 90,298–94,274 | 0.88 ms | 0.87–0.92 ms | 0 |
| 96 | 181,768 | 175,461–188,363 | 0.88 ms | 0.84–0.92 ms | 0 |
| 192 | 175,148 | 167,555–185,207 | 1.96 ms | 1.57–2.18 ms | 0 |
| 384 | 256,151 | 247,831–274,315 | 3.17 ms | 2.84–3.28 ms | 0 |
| 768 | 335,793 | 314,402–380,125 | 5.62 ms | 4.98–6.08 ms | 0 |
| 1,536 | 476,714 | 460,927–489,932 | 6.94 ms | 6.48–7.96 ms | 0 |
| 3,072 | 572,899 | 560,058–607,174 | 11.23 ms | 9.97–11.28 ms | 0 |
| 6,144 | 648,161 | 619,058–653,570 | 19.62 ms | 18.58–23.58 ms | 0 |
| 8,192 | 652,368 | 627,799–652,569 | 26.02 ms | 25.67–29.28 ms | 0 |
| 12,288 | 686,641 | 638,013–734,983 | 40.59 ms | 38.81–40.60 ms | 709 |

### 活跃连接规模

该测试使用相同的 payload 和服务端线程配置，每条连接固定一个在途 RPC，扫描 12 到 3,072 条同时活跃的 TCP 连接。连接数和全局并发请求数同步增长，因此它用于观察连接规模压力，不与固定 12 条连接的服务端容量测试混为一谈。

Firehose 在计时前完成全部连接建立，随后直接进行 30 秒测量。该测试不单独启动 warmup 客户端，避免 warmup 和正式测量的两批大规模连接在单机 loopback 上同时占用本地 TCP 连接槽。

| 活跃连接数 | QPS 中位数 | QPS 范围 | p99 中位数 | p99 范围 | 失败数 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 12 | 24,032 | 23,121–24,674 | 0.86 ms | 0.82–0.89 ms | 0 |
| 48 | 45,783 | 42,084–46,031 | 2.27 ms | 2.20–2.47 ms | 0 |
| 192 | 58,836 | 56,675–60,653 | 9.33 ms | 8.98–10.49 ms | 0 |
| 768 | 66,881 | 57,738–71,952 | 27.06 ms | 24.86–30.85 ms | 0 |
| 1,536 | 65,153 | 60,967–73,752 | 50.00 ms | 42.93–55.04 ms | 0 |
| 3,072 | 48,774 | 48,497–52,603 | 113.02 ms | 110.03–118.00 ms | 0 |

4,096 连接探测在 Firehose 建立 RPC 之前由本机 loopback `connect()` 返回 `EADDRNOTAVAIL`，因此不作为服务端结果。要继续扩大连接数，需要使用更多客户端源地址或独立压测机。

## I/O 统计快照

给 runner 增加 `--collect-io-stats` 可在结果 JSON 的每个工作点中输出 `io_stats`：

```bash
python3 tools/benchmark/runner/run_suite.py \
  --config tools/benchmark/configs/firehose.json \
  --build-dir build-release --collect-io-stats \
  --output-json .local-perf/uring-stats.json
```

runner 为 benchmark server 提供临时 `--stats_file`，通过 `SIGUSR1` 请求普通快照，通过 `SIGUSR2` 开始新峰值窗口并获取起始快照。信号处理器只设置标志，主控制线程请求各 Connection I/O loop 在所属线程复制数据，然后以临时文件加 rename 发布 JSON。采集请求有超时；失败会使本次测量失败，不会填入假零值。默认不开启采集，便于运行旧版本对照。

`before` 在预热客户端退出后采集，并在每个 loop 上将峰值重置为当时的当前值；已有的 buffer 租约也计入新窗口。累计计数不重置。`after` 在测量客户端退出后采集，同一 loop 的 `window_id` 必须相同，否则拒绝本次结果。窗口包含测量连接的建立、请求处理及关闭，也包含边界采集的唤醒开销；不是仅有业务处理的精确时间窗口。各 loop 独立采样，accept loop 不计入本阶段统计。

输出包含原始 `before/after`、逐 loop 的累计计数差值与边界当前值、计数总和以及派生比例。`prepared_multishot_recv_sqes` 是 `prepared_recv_sqes` 的子集；`recv_cqes` 包含 EOF、错误和取消。`submit_calls` 统计 liburing 调用及重试，不代表 `io_uring_enter` 系统调用数。当前值不取差值，不当作峰值；`peaks` 保留逐 loop 的窗口最大值，不取差值，也不将各 loop 峰值相加冒充全局同时峰值。没有分母的比例输出 `null`。统计 JSON 的 `schema_version` 为 3，runner 与启用统计的服务端应使用匹配版本。

采集代码集中在内部 `server/runtime_stats.h`、benchmark 的 `server/stats_output.cpp` 和 runner 的 `io_stats.py`。本阶段已加入 staged 队列精确峰值、每批 CQE 处理前及快照时采样的 CQ 长度峰值、buffer 借用/归还累计计数、当前及窗口峰值租约数，以及 provided-buffer 接收的 `ENOBUFS` 次数。租约数仅包括用户态已领取的 buffer，不包括内核已选中但 CQE 尚未消费的 buffer，不能用池容量减租约数推算空闲容量。写侧新增逐 loop 的 `pending_write_bytes` 当前值与窗口峰值，包含已预留、排队和正在发送的响应字节，不含 worker 已编码但尚未投递到连接的响应。此峰值是同一 loop 内各连接的同时总量。

`worker_pool` 独立输出已有 admission 总数 `pending_logical_jobs`（包含容量预留、排队和执行中 RPC），以及逐 worker 的 `queued_batches`、`queued_logical_jobs`、`pending_batches` 当前值和排队窗口峰值。一个 batch 可包含多个 RPC。采集复用既有队列锁，逐 worker 采样不构成全局原子快照；峰值也不跨 worker 求和。总快照 scope 为 `server_runtime`，I/O 计数汇总仍只包含 Connection I/O loops。
