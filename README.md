# XRPC

XRPC 是一个基于 `io_uring` 构建的 Linux C++20 RPC 框架，提供类型化 Protobuf 调用、多路复用客户端连接、Consul 服务发现、有界服务端背压，以及可复现的性能测试工具。

项目重点展示 RPC 系统在传输、调度、所有权和性能工程上的完整实现，不兼容 gRPC 线协议。

## 核心特性

- 基于紧凑自定义帧协议的类型化一元 RPC 与原始字节调用
- 使用 `io_uring` 完成 accept、read、write、唤醒和定时操作
- 独立 accept loop、可配置 connection I/O loops 与 handler worker pool
- 多个并发调用复用同一条生产客户端连接
- 支持静态端点列表和 Consul 服务发现
- 支持粘性路由、调用超时、连接复用和有界在途请求
- 提供连接级与全局背压，并暴露拒绝计数器
- 覆盖单元、运行时、传输、集成和端到端测试
- 分离生产客户端延迟测试与服务端容量测试

## 架构概览

```text
RpcClient
   │  路由 / 服务发现 / 超时控制
   ▼
ClientChannel ── 多路复用 TCP 传输 ──► XRPC 帧协议
                                           │
                                           ▼
                                    accept io_uring loop
                                           │
                                    轮询分配连接所有权
                                           │
                              connection io_uring loops（1..N）
                                           │
                                  有界 handler worker pool
                                           │
                                  类型化 Protobuf 服务注册表
```

所有权、并发模型、协议和关闭流程详见[架构设计](docs/architecture.md)。

## 环境要求

- 支持 `io_uring` 的 Linux
- 支持 C++20 的编译器（开发环境使用 Clang 20，也支持较新的 GCC）
- CMake 3.20 或更高版本
- GNU Make（用于构建仓库内固定版本的 liburing）
- 性能测试工具需要 Python 3

Protocol Buffers、liburing、nlohmann/json 和 GoogleTest 的固定版本源码位于
`third_party/`，正常构建不会从网络下载依赖，也不需要系统安装对应的开发包。

Ubuntu 安装命令：

```bash
sudo apt-get update
sudo apt-get install build-essential cmake python3
```

## 构建

```bash
make
```

库目标名为 `xrpc::xrpc`。示例程序位于 `build/example/`，benchmark 可执行文件位于 `build/tools/benchmark/`。

## 回显示例

启动服务端：

```bash
./build/example/xrpc_echo_server
```

在另一个终端启动客户端：

```bash
./build/example/xrpc_echo_client
```

服务端可以直接使用生成的 Protobuf 类型注册方法：

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

客户端发起类型化调用并获得 `StatusOr<Response>`：

```cpp
xrpc::RpcClient client("127.0.0.1", 9000);
auto response = client.Call<EchoResponse>("EchoService", "Echo", request);
```

完整程序见 [example](example/)。

## 测试

```bash
ctest --test-dir build/tests --output-on-failure -LE "external|tooling"
```

默认测试集不依赖在线外部服务。需要 Consul 的测试单独执行：

```bash
XRPC_ENABLE_CONSUL_TESTS=1 \
  ctest --test-dir build/tests --output-on-failure -L external
```

CTest 标签、测试布局和运行方法见[测试说明](tests/README.md)。

## 性能测试

XRPC 将性能测试拆成三个层次：

1. 类型化 Firehose 衡量完整类型化服务端路径的容量，不包含正式客户端开销。
2. 正式 `RpcClient` 衡量公开同步客户端路径。
3. 原始 Firehose 隔离传输层和运行时的性能上界。

在 Intel Core i7-9750H、WSL2 loopback 环境中，服务端与发压器各绑定三个物理核，使用 128 字节 Protobuf Echo 消息时，类型化服务端路径达到：

| 工作点 | QPS 中位数 | p99 中位数 | 失败数 |
| --- | ---: | ---: | ---: |
| 低延迟 | 96,103 | 0.86 ms | 0 |
| 饱和点 | 422,391 | 8.14 ms | 0 |

每个数值均来自三轮测试的中位数，每轮使用全新服务端。这是同机 loopback 结果，不代表跨主机网络延迟。详细方法、复现命令和限制见[性能说明](docs/performance.md)。

运行冒烟测试套件：

```bash
./tools/benchmark/run_suite.py \
  --config tools/benchmark/configs/v1_smoke.json \
  --build
```

复现 README 中的代表性性能结果：

```bash
./tools/benchmark/run_suite.py \
  --config tools/benchmark/configs/v1_protobuf_steady_state.json \
  --build

./tools/benchmark/run_suite.py \
  --config tools/benchmark/configs/v1_protobuf_ceiling_verify.json \
  --build

./tools/benchmark/run_suite.py \
  --config tools/benchmark/configs/v1_rpc_client.json \
  --build
```

完整配置、测试环境和结果解释见[性能说明](docs/performance.md)。

## 当前边界

XRPC 当前支持 Linux、TCP、一元请求和自定义 Protobuf 线协议。TLS、认证、流式 RPC、跨语言代码生成和 gRPC 兼容不在当前范围内。项目用于展示可审查的系统框架与工程实现，不承诺生产 SLA。

## 安装与使用

```bash
cmake --install build --prefix /tmp/xrpc-install
```

下游 CMake 工程可以这样使用：

```cmake
find_package(xrpc CONFIG REQUIRED)
target_link_libraries(my_service PRIVATE xrpc::xrpc)
```

仓库内 vendor 依赖用于从 xrpc 源码树构建。当前安装包保持原有边界，仍由下游
环境提供 Protocol Buffers、liburing 和 nlohmann/json；将安装包做成依赖自包含
产物不在本次 vendor 化范围内。

## 文档

- [架构设计](docs/architecture.md)
- [性能说明](docs/performance.md)
- [性能测试工具](tools/benchmark/README.md)
- [测试说明](tests/README.md)

## 许可证

XRPC 使用 [MIT 许可证](LICENSE)。仓库内引入的工具保留其自身许可证，详见[第三方声明](THIRD_PARTY_NOTICES.md)。
