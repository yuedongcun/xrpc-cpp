# Benchmark 工具

这里保留两条性能测试路径：

- `firehose.json`：使用 benchmark 专用 Firehose 客户端持续施压，观察服务端类型化 Protobuf RPC 路径的容量和延迟。
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

套件运行器直接在 stdout 打印每轮结果和最终 summary，不写入结果文件。
