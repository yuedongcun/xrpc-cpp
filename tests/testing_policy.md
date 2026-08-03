# XRPC 测试规范

本文规定测试代码的组织和编写方式，并记录日常测试命令、持续集成门禁、高可用与性能测试执行口径。

## 核心原则

1. 生产 API 必须表达生产语义。
2. 仅测试使用的编排逻辑必须放在 `tests/test_support`。
3. 测试不能给生产类引入特殊生命周期语义。
4. 只有在生产环境同样有意义时，才允许暴露只读诊断接口。
5. 长时间运行的异步 loop 必须通过真实生命周期 API 停止，例如 `Stop`、`Close` 或 `Cancel`。
6. 优先直接测试纯模块，不要让所有测试都经过网络集成链路。
7. 生产头文件中避免 `ForTest` API。
8. 生产逻辑中避免 `#ifdef TEST` 或 `#ifdef TESTING` 分支。
9. 集成测试应该验证行为，而不是内部实现顺序。
10. 异步测试必须具备确定性关闭和超时保护。
11. 每个核心语义保留一个代表性测试；不要为工具冒烟测试、禁用场景或旧测试框架层重复创建独立目标。

## 测试支持代码归属

`tests/test_support/` 存放仅供测试使用的辅助代码，例如：

- 测试框架；
- 运行时辅助工具；
- 套接字辅助工具；
- 空闲端口辅助工具；
- 测试服务端包装器；
- 测试客户端辅助工具；
- 条件等待辅助工具。

生产代码不应暴露仅供测试使用的生命周期控制。

## 目录布局

`tests/` 是测试根目录，内部目录与生产源码模块对应：

- `tests/common/`
- `tests/io/`
- `tests/transport/`
- `tests/protocol/`
- `tests/rpc/client/`
- `tests/rpc/server/`
- `tests/rpc/naming/`
- `tests/observability/`

特殊测试基础设施放在 `tests/test_support/`。

工具、高可用和性能测试检查可以使用 CTest 标签注册到 `tests/CMakeLists.txt`，但除非确实验证 XRPC 行为，否则不要额外创建 C++ 冒烟测试包装器。

不要使用 `tests/unit/`、`tests/component/` 或 `tests/integration/` 作为主要索引，测试类型由 CTest 标签管理。

## `ForTest` 命名规则

- 公开生产 API 不能使用 `ForTest`。
- `src/include/` 中的内部生产头文件也应避免 `ForTest`。
- `tests/test_support` 可以使用 `TestHarness`、`Fixture`、`Fake`、`TestUtil`。

不推荐：

```cpp
TcpServer::RunConnectionsForTest(1);
TcpServer::ConnectionCountForTest();
TcpConnection::InjectBytesForTest();
```

推荐：

```cpp
TcpServer::Run();
TcpServer::Stop();
TcpServer::ConnectionCount();
```

## 诊断接口规则

如果只读可观测性接口在生产环境有实际意义，就可以保留：

```cpp
ConnectionCount();
PendingRequestCount();
IsRunning();
```

这类 API 不使用 `ForTest` 后缀。改变生命周期、执行次数或事件顺序的 API 不应出现在生产类中。

## 异步测试规则

长时间运行的 coroutine loop 必须通过真实生命周期 API 停止：

```cpp
server.Run();
server.Stop();

connection.Run();
connection.Close();
```

不推荐：

```cpp
RunConnectionsForTest(N);
RunUntilOneRequestForTest();
AcceptNThenExit();
```

## 推荐测试层次

### A. 纯逻辑单元测试

不使用 socket、`io_uring` 或 coroutine runtime：

- `FrameCodec`
- `ByteBuffer`
- `ProtobufCodec`
- `ServiceRegistry`
- `ServiceRegistry::Dispatch`
- `RpcSession::FeedBytes`
- `RpcSession::EncodeResponse`

### B. 运行时与 I/O 测试

- `Task<T>`、`Task<void>`
- 嵌套 `co_await`
- 异常传播
- `UringContext::Nop`
- `UringContext::Accept/Recv/Send`

必要时使用 `tests/test_support` helper。

### C. 传输集成测试

- `TcpServer::Run/Stop`
- `TcpConnection::Run/Close`
- 对端关闭
- 多连接
- 连接清理
- 有代表性的背压行为

使用真实生命周期 API，不能使用仅测试控制 loop。

### D. RPC 端到端测试

```text
RpcClient -> TcpServer -> TcpConnection -> RpcSession -> RpcServer -> handler -> response
```

优先使用公开 API 和行为断言。除非第二个测试证明不同的公开契约，否则不要在多个 e2e 文件中重复相同的端点路由、超时或背压语义。

## CTest 测试标签

CTest 标签是可执行的测试治理层。每个注册测试至少应有一个层次标签和一个领域标签。

层次标签：

- `unit`：纯逻辑或隔离组件测试。
- `runtime`：coroutine、`io_uring`、event loop 或 executor 行为。
- `integration`：多个内部组件协同运行。
- `e2e`：跨越客户端和服务端边界的公开或接近公开 RPC 路径。
- `external`：需要 Consul 等在线外部依赖。
- `tooling`：验证脚本、benchmark 配置或 HA dry-run 编排。

常见领域标签：

- `protocol`
- `io`
- `io_uring`
- `transport`
- `rpc`
- `client`
- `server`
- `naming`
- `consul`
- `observability`
- `prometheus`
- `benchmark`
- `ha`
- `concurrency`
- `lifecycle`

标准命令：

```bash
# 默认本地正确性测试集。
make test

# 只运行纯单元测试。
make test-unit

# 只运行运行时和事件循环测试。
make test-runtime

# 运行不依赖在线外部服务的集成和端到端测试。
make test-integration
make test-e2e

# 运行注册到 CTest 的工具检查。
make test-tooling

# 运行在线 Consul 测试，需要 127.0.0.1:8500 上存在 Consul。
make test-external
```

超时规则：

- `unit` 测试应保持小而快，通常使用短超时。
- `integration`、`runtime` 和 `e2e` 可以使用更长超时，但仍必须确定性关闭。
- `external` 可以使用更长超时，但除非显式启用，否则必须从默认本地 CI 中排除。
- `tooling` 优先使用 dry-run 或配置校验；重量级 benchmark 不属于默认 CTest。

## 异步边界

当前边界：

- 异步侧：
  - `io::UringContext` awaitable API；
  - `TcpServer` coroutine loop；
  - `TcpConnection` coroutine loop。
- 同步侧：
  - `FrameCodec`；
  - `ProtobufCodec`；
  - `RpcSession::HandleBytes`；
  - `RpcServer::Dispatch*`；
  - 当前用户 handler。

规则：

- 同步核心不能调用异步 API。
- 异步外壳可以调用同步核心。
- 公开同步 API 必须明确所有阻塞边界。
- 同步核心内部不能隐藏 `BlockOn`。
