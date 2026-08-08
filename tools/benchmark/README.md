# Benchmark 工具

这里保留两条性能测试路径：

- `v1_firehose.json`：使用 benchmark 专用 Firehose 客户端持续施压，观察服务端类型化 Protobuf RPC 路径的容量和延迟。
- `v1_client.json`：使用正式 `RpcClient::Call()`，观察公开客户端 API 的延迟和并发扩展。

Firehose 是发压工具，不是生产客户端模型。生产客户端路径请看 `v1_client.json`。

## 构建

```bash
make
```

生成的可执行文件位于 `build/tools/benchmark/`：

- `xrpc_benchmark_server`
- `xrpc_benchmark_client`

## 运行套件

服务端容量测试：

```bash
./tools/benchmark/run_suite.py \
  --config tools/benchmark/configs/v1_firehose.json \
  --build
```

正式客户端路径测试：

```bash
./tools/benchmark/run_suite.py \
  --config tools/benchmark/configs/v1_client.json \
  --build
```

只校验配置文件，不构建、不启动服务端：

```bash
./tools/benchmark/run_suite.py \
  --config tools/benchmark/configs/v1_firehose.json \
  --validate-config
```

## 单独启动

服务端：

```bash
./build/tools/benchmark/xrpc_benchmark_server \
  --host=127.0.0.1 \
  --port=9010 \
  --workload=protobuf
```

Firehose 客户端：

```bash
./build/tools/benchmark/xrpc_benchmark_client \
  --client_mode=firehose \
  --workload=protobuf \
  --duration_s=30 \
  --payload_size=128 \
  --firehose_connections=12 \
  --firehose_inflight=1536 \
  --host=127.0.0.1 \
  --port=9010
```

正式 `RpcClient` 客户端：

```bash
./build/tools/benchmark/xrpc_benchmark_client \
  --client_mode=rpc_client \
  --workload=protobuf \
  --client_threads=24 \
  --duration_s=10 \
  --payload_size=128 \
  --host=127.0.0.1 \
  --port=9010
```

## 结果

套件运行结果写入 `build/benchmark-results/<timestamp>/`：

- `summary.md`：QPS、p99、失败数和 CPU 摘要
- `runs.csv`：每轮运行的结构化结果
- `logs/`：服务端和客户端原始输出
- `environment.json`：提交、工作区、CPU、内核和编译器信息
- `config.json`：本次运行使用的配置

性能结论和解释见 [docs/performance.md](../../docs/performance.md)。
