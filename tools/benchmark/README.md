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

服务端容量测试固定 12 条 TCP 连接、128 字节 Protobuf Echo payload、3 个 Connection I/O 线程和 3 个 Worker 线程。每个工作点预热 3 秒、测量 30 秒并重复 3 次；测试顺序按固定种子在各轮间改变，表中报告中位数和三轮最小值—最大值。

活跃连接规模测试使用相同的 payload 和服务端线程配置，每条连接固定一个在途 RPC，扫描 12 到 8,192 条同时活跃的 TCP 连接。连接数和全局并发请求数会同步增长；它用于观察连接规模压力，不与固定 12 条连接的服务端容量测试混为一谈。

benchmark 将每连接 inflight 上限设为 2,048，避免公开 API 的默认上限先于运行时容量截断测试。该设置仅用于容量测试，不改变 `RpcServerOptions` 的默认值。

| 全局并发请求数 | QPS 中位数 | QPS 范围 | p99 中位数 | p99 范围 | 失败数 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 24 | 45,494 | 43,834–45,498 | 0.91 ms | 0.89–0.95 ms | 0 |
| 48 | 90,694 | 88,676–93,050 | 0.92 ms | 0.88–0.92 ms | 0 |
| 96 | 173,328 | 170,856–173,711 | 0.95 ms | 0.93–0.97 ms | 0 |
| 192 | 183,611 | 170,403–190,588 | 1.64 ms | 1.61–1.93 ms | 0 |
| 384 | 250,179 | 238,508–259,668 | 3.23 ms | 2.99–4.02 ms | 0 |
| 768 | 345,010 | 310,971–381,499 | 5.63 ms | 4.11–6.36 ms | 0 |
| 1,536 | 478,421 | 412,544–484,576 | 6.56 ms | 6.54–8.05 ms | 0 |
| 3,072 | 495,905 | 444,350–551,911 | 14.69 ms | 11.33–15.02 ms | 0 |
| 6,144 | 574,785 | 533,648–662,834 | 22.70 ms | 19.49–24.82 ms | 0 |
| 8,192 | 606,053 | 561,771–676,874 | 26.16 ms | 22.52–33.12 ms | 0 |
| 12,288 | 697,597 | 620,139–764,726 | 32.67 ms | 28.90–52.27 ms | 32,902 |
