# xRPC

xRPC 是一个基于 `io_uring` 和 C++20 协程构建的 Linux RPC 框架。

项目采用 Linux 原生异步 I/O 驱动网络运行时，探索高并发 RPC 系统的网络传输与运行时设计。

## 核心特性

- **io_uring + C++20 协程**：基于 Linux 原生异步 I/O 构建服务端网络运行时，以协程组织连接的异步读写与生命周期。
- **多 I/O Loop 并发模型**：Accept 与连接 I/O 分离；每条连接建立后固定归属于一个 I/O Loop，由少量 I/O 线程驱动大量并发连接。
- **I/O 与业务执行分离**：Connection I/O Loop 专注网络收发与连接状态，业务 Handler 在独立 worker 线程执行，响应完成后回到原 I/O Loop 写回。
- **多路复用客户端**：多个同步 RPC 调用复用同一 TCP 连接，通过 request ID 独立匹配响应。
- **Consul 服务发现**：客户端通过 Consul 动态发现服务实例并维护 Endpoint 状态，调用时完成目标选择与连接复用。

## 架构

xRPC 将连接管理、网络 I/O 与 RPC 执行分离：客户端通过服务发现选择 Endpoint，服务端由独立 Accept Loop 接收连接，并将连接分配到多个 Connection I/O Loop。

一次 RPC 在所属 I/O Loop 中完成收包与解码，随后交由共享 Worker Pool 执行业务逻辑；响应编码完成后回到原 I/O Loop，并通过对应连接写回客户端。

![xRPC Runtime Architecture](docs/architecture-overview.svg)

## 快速开始

需要支持 `io_uring` 的 Linux、C++20 编译器、CMake 3.20+ 和 GNU Make；依赖源码已固定在 `third_party/`。

```bash
make
```

启动服务端：

```bash
./build/example/xrpc_echo_server
```

另一个终端启动客户端：

```bash
./build/example/xrpc_echo_client
```

最小 API 示例：

服务端：

```cpp
xrpc::RpcServer server;
server.RegisterMethod<EchoRequest, EchoResponse>(
    "EchoService", "Echo", [](const EchoRequest &request) {
      EchoResponse response;
      response.set_message("echo: " + request.message());
      return response;
    });
server.Listen("127.0.0.1", 9000);
server.Run();
```

客户端：

```cpp
xrpc::RpcClient client("127.0.0.1", 9000);
auto response = client.Call<EchoResponse>("EchoService", "Echo", request);
```

## 性能

在 Intel Core i7-9750H、WSL2 loopback 环境中，使用 128 字节 Protobuf Echo 消息时：

| 工作点 | QPS 中位数 | p99 中位数 | 失败数 |
| --- | ---: | ---: | ---: |
| 低延迟 | 96,103 | 0.86 ms | 0 |
| 饱和点 | 422,391 | 8.14 ms | 0 |

这是单机 loopback 结果，不代表跨主机网络延迟。benchmark 不做绑核、控频、NUMA 调优或 perf 编排，只固定负载参数。

复现服务端容量测试：

```bash
./tools/benchmark/runner/run_suite.py \
  --config tools/benchmark/configs/firehose.json \
  --build
```

复现正式客户端路径测试：

```bash
./tools/benchmark/runner/run_suite.py \
  --config tools/benchmark/configs/client.json \
  --build
```

## 项目边界

xRPC 当前面向 Linux，提供基于 TCP 的请求—响应式 RPC：每次调用对应一个请求和一个响应，支持类型化 Protobuf 消息和原始字节载荷。

传输使用项目自定义的 XRPC 帧协议，不兼容 gRPC；TLS、身份认证、流式 RPC 和跨语言代码生成暂未提供。

## 文档

- [架构设计](docs/architecture.md)
- [测试说明](tests/README.md)
- [Benchmark 工具](tools/benchmark/README.md)

## 许可证

xRPC 使用 [MIT 许可证](LICENSE)。仓库内引入的工具保留其自身许可证，详见[第三方声明](THIRD_PARTY_NOTICES.md)。
