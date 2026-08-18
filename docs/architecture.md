# XRPC 架构设计

## 设计目标

XRPC 是一个仅支持 Linux、基于 C++20 和 `io_uring` 构建的一元 RPC 框架。
项目有意保持范围较小，集中展示四个容易审查的设计：

- 异步 I/O 负责 socket 和事件循环工作；
- 同步代码负责分帧、分发和用户 handler；
- 一条连接在同一时刻只有一个 I/O 所有者；
- 有界队列把过载转化为明确的状态码或连接关闭。

公开调用使用 Protobuf 类型消息，但 XRPC 线协议是私有协议，不兼容 gRPC。

## 请求生命周期

一次普通的类型化调用经过下面的路径：

```text
RpcClient::Call
  -> 端点选择与 request id 分配
  -> 多路复用 TCP 连接
  -> 服务端 connection I/O loop
  -> RpcFrameStream 增量分帧与解码
  -> 有界 handler worker pool
  -> 响应编码
  -> 原 connection I/O loop
  -> 客户端 reader 按 request id 匹配
  -> 等待中的调用者得到 StatusOr<Response>
```

最重要的边界是 I/O 外壳和同步核心之间的边界。I/O loop 将完整的协议
消息提交给 worker pool；handler 不在 socket loop 中执行。完成结果返回
给连接所有者，由连接所有者保证响应顺序并串行化写操作。

## 服务端并发模型

`RpcServer` 是公开的 move-only facade，其 controller 拥有：

- 一个 accept `IoUringContext`；
- 一个或多个 connection I/O loops；
- 一个有界 handler worker pool；
- 类型化服务注册表；
- 可选的 Consul 注册器；
- 资源和生命周期状态。

accept loop 以 round-robin 方式分配新 socket。分配完成后，一条连接的
socket、读缓冲区、写队列和在途状态都由一个 connection I/O loop 独占。
这个所有权规则避免了全局 socket 锁。worker 线程获得请求处理工作，
但不接管连接本身；它们把响应工作返回给连接所有者。

## 并发契约

XRPC 通过线程归属管理可变状态。连接状态只由所属 I/O 线程修改；
跨线程协作通过 `Post()`、`Stop()` 和 `DispatchMailbox::Submit()` 等明确入口完成。

| 组件 | 状态所有者 | 跨线程行为 |
| --- | --- | --- |
| `RpcServer` | 调用 `Run()` 的线程负责服务端生命周期 | 其他线程只能调用线程安全、幂等的 `Stop()` |
| `ConnectionIoLoop` | 所属 I/O 线程管理连接集合 | `RpcServer::Impl` 串行投递新连接和 drain 命令 |
| `ServerConnection` | 所属 I/O 线程管理 socket、读写队列、在途请求和生命周期 | worker 只能通过 `DispatchMailbox` 回投完成结果 |
| `ThreadPoolExecutor` | worker 线程执行已接纳的 handler | 多个 I/O 线程可以并发提交任务；shutdown coordinator 关闭 admission 并等待完成 |
| `ServiceRegistry` | 启动前由 server 注册方法，运行期不再修改 | worker 可以并发查找并执行 handler |
| `UringContext` | 调用 `Run()` 的线程提交和处理 I/O | 其他线程可以调用 `Post()` 和 `Stop()` |
| `RpcClient` | client 自身协调 endpoint 和 transport | 多个调用者可以并发发起 RPC；析构和移动不能与调用并发 |
| `ServiceDiscovery` | 后台 refresh 线程更新 endpoint 快照 | client 调用线程可以并发读取快照和最近错误 |

用户 handler 可能在多个 worker 线程上并发执行，其捕获的业务状态由应用自行同步。

服务端遵守三个关键不变量：

1. connection drain 开始后，不再接纳新的连接和 RPC；
2. worker 不直接修改 connection，只把结果回投到原 I/O 线程；
3. 已接纳的 handler 和 response flush 完成后，connection I/O loop 才停止。

`Task`、`UringAwaitable` 和 `Socket` 是单所有者对象。除 `Task::Wait()` 和
`WaitFor()` 可以跨线程等待完成外，同一个实例不应由多个线程同时操作。

## 客户端并发模型

`RpcClient` 负责端点发现、端点健康状态、路由、request id 分配和可复用
transport。多个应用线程可以共享同一个 client 实例。

每个 transport 通过 request id 多路复用调用。reader 解码响应并唤醒
对应的同步调用者。静态端点和 Consul 端点都会生成端点快照；sticky key
可以选择稳定端点，transport 错误会先更新端点状态，再影响下一次选择。

## 同步核心与异步外壳

同步核心包含不依赖事件循环也能理解的部分：

- fixed header 和 frame 编解码；
- Protobuf 元数据和 payload 处理；
- 请求分发和服务查找；
- status 与 exception 映射；
- 用户 handler 执行。

异步外壳包含 `io_uring` 操作、socket 所有权、连接 loop、唤醒、定时器
和关闭协调。外壳可以调用同步核心，但同步核心不会隐藏阻塞式的事件循环操作。

## 线协议

每个 frame 的布局是：

```text
24-byte fixed header | protobuf metadata header | opaque payload
```

fixed header 包含 magic、协议版本、消息类型、flags、元数据长度、payload
长度和 request id。类型化调用使用生成的 Protobuf 代码序列化请求和响应；
raw 调用不解释 payload，主要用于传输实验。

fixed header、元数据 header、payload 和完整 frame 都有大小限制。无效的
magic、版本、长度或消息类型会转化为协议错误，而不是触发不受控的内存分配。

## 背压

服务端保护三类资源：

1. 每条连接允许的最大在途 handler job 数；
2. worker pool 允许的最大 pending job 数；
3. 每条连接允许排队的最大响应字节数。

请求容量耗尽时，只要协议仍允许返回响应，服务端就返回 `ResourceExhausted`。
响应队列超过字节上限时，服务端关闭连接以释放内存。这些限制属于运行时行为，
不只是 benchmark 参数。

## 失败与关闭

客户端 timeout、transport error 和协议 error 是不同的失败类别。尚未发送的
请求可以在没有 wire response 的情况下完成；已经发送的请求则通过 request id
匹配响应。

服务端采用 graceful drain：`Stop()` 先关闭新请求的 admission 并停止 accept，
已经进入 worker 的 handler 继续执行，response 仍通过原 Connection I/O Loop 写回。
所有已接收请求完成后才停止 connection loops 和 worker，随后 `Run()` 返回。
`Stop()` 是线程安全且幂等的；如果 handler 永远不返回，`Run()` 也会继续等待。

## 当前范围

当前实现支持 Linux、TCP、一元 RPC、类型化 Protobuf、raw payload、静态端点、
可选 Consul 发现和有界服务端资源。当前不提供流式 RPC、TLS、认证、跨语言 stub
生成或稳定的 gRPC 线协议兼容性。
