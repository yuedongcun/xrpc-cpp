# xRPC 架构

## 项目定位

xRPC 是一个面向 Linux 的 C++20 RPC 教学项目。它使用 TCP 和项目自定义的 Protobuf 帧协议，提供同步的一次请求、一次响应调用，并把重点放在服务端网络运行时、并发边界和资源控制上。

项目不试图覆盖完整的 RPC 产品能力。当前设计优先回答这些问题：

- 如何用 `io_uring` 和协程驱动大量服务端连接；
- 如何把连接 I/O 与用户 Handler 分离；
- 如何让多个同步客户端调用复用一条 TCP 连接；
- 如何通过不可变快照组织服务发现和路由；
- 如何让过载和关闭行为具有明确边界。

## 总体结构

![xRPC Runtime Architecture](architecture-overview.svg)

当前客户端和服务端共享线协议，但采用不同的网络执行模型：

```text
RpcClient                                      RpcServer
─────────                                      ─────────
同步调用线程                                   Server Run 线程
  │                                              │
  ├─ ServiceDiscovery                            └─ AcceptLoop / io_uring
  ├─ RoutingSnapshot                                  │
  └─ TcpTransport                                      ▼
       ├─ 调用线程阻塞写                    ConnectionIoLoop[]
       └─ 每个 transport 一条 reader 线程        │
                                                    ├─ ServerConnection[]
                                                    └─ DispatchMailbox
                                                          ▲
                                                          │
                                                     WorkerPool
                                                          │
                                                   ServiceRegistry
                                                   + User Handler
```

服务端通过 `UringContext` 和协程驱动 Accept、Recv 与 Send。客户端公开 API 同步阻塞，底层由调用线程发送请求，并由专用 reader 线程接收和匹配响应。

## 所有权结构

服务端主要所有权如下：

```text
RpcServer
└─ RpcServer::Impl
   ├─ ServiceRegistry
   ├─ WorkerPool
   ├─ ConsulRegistrar（可选）
   ├─ accept UringContext + listen socket + AcceptLoop Task
   └─ ConnectionIoLoop[]
      ├─ UringContext + I/O thread
      ├─ DispatchMailbox
      └─ ServerConnection[]
         ├─ socket
         ├─ RpcFrameStream
         ├─ inflight accounting
         └─ response write queue
```

客户端主要所有权如下：

```text
RpcClient
└─ RpcClient::Impl
   ├─ ServiceDiscovery
   └─ atomic shared_ptr<RoutingSnapshot>
      ├─ immutable DiscoverySnapshot
      ├─ TcpTransport[]
      └─ consistent-hash ring
```

旧快照可以被正在执行的调用继续持有；新快照只在端点集合完整构建后一次性发布。

## 一次 RPC 的完整路径

```text
用户线程
  │
  ├─ Protobuf request 序列化为 payload
  ├─ RpcClient::Impl 取得路由快照并选择 Endpoint
  ├─ FrameCodec 编码 request frame
  └─ TcpTransport 写入连接
         │
         ▼
服务端 Connection I/O 线程
  │
  ├─ co_await Recv
  ├─ RpcFrameStream 拼接 TCP 字节流
  ├─ FrameCodec 解出 RequestEnvelope
  └─ WorkerPool::TrySubmitBatch
         │
         ▼
Worker 线程
  │
  ├─ ServiceRegistry::Dispatch
  ├─ Protobuf payload 反序列化
  ├─ User Handler
  ├─ Protobuf response 序列化
  └─ FrameCodec 编码 response frame
         │
         ▼
DispatchMailbox
  │
  └─ 回投到原 Connection I/O 线程
         │
         ├─ response write queue
         └─ co_await Send
                │
                ▼
客户端 reader 线程
  │
  ├─ FrameCodec 解出 ResponseEnvelope
  ├─ 按 request_id 查找 PendingCall
  └─ 唤醒对应的同步调用线程
```

请求可以在 Worker 线程中并发完成，因此响应不依赖请求到达顺序；客户端使用 `request_id` 完成多路复用匹配。

## 关键边界

- `UringContext` 只提供 I/O 执行、跨线程 `Post()` 和停止机制，不理解 RPC。
- `ServerConnection` 拥有单条连接的状态、在途请求计数和写队列。
- `WorkerPool` 负责全局任务准入和执行，不操作 socket。
- `DispatchMailbox` 是 Worker 线程返回原连接 I/O 线程的交接点。
- `ServiceRegistry` 在 `Run()` 前完成注册，运行期间只做 service/method 查找和调用。
- `RpcClient::Impl` 负责发现、路由和安全 failover；`TcpTransport` 只负责一个 Endpoint 的网络调用。
- `FrameCodec` 只处理完整帧；服务端 TCP 字节流缓冲由 `RpcFrameStream` 负责。

## 当前范围

当前实现支持 Linux、TCP、同步单次请求—响应调用、类型化 Protobuf、原始字节载荷、静态 Endpoint、Consul 服务发现和有界服务端资源。

当前不提供流式 RPC、TLS、身份认证、异步客户端 API、跨语言 stub 生成，也不兼容 gRPC 线协议。

## 继续阅读

- [I/O 运行时](io-runtime.md)
- [服务端运行时](server-runtime.md)
- [客户端运行时](client-runtime.md)
- [线协议](wire-protocol.md)
