# 服务端运行时

## 组件与所有权

`RpcServer` 是公开 facade，内部实现集中在 `RpcServer::Impl`。`Impl` 负责组件组装和生命周期协调，不参与每次 RPC 的具体 dispatch。

```text
RpcServer::Impl
├─ ServerConfig
├─ ServiceRegistry
├─ WorkerPool
├─ ConsulRegistrar（可选）
├─ listen socket
├─ accept UringContext
├─ AcceptLoop Task
└─ ConnectionIoLoop[]
   ├─ connection UringContext
   ├─ I/O thread
   ├─ DispatchMailbox
   └─ ServerConnection[]
```

这里有三个真正的执行域：

- **Server Run 线程**：驱动 Accept `UringContext`，接收连接并协调关闭；
- **Connection I/O 线程**：每个 `ConnectionIoLoop` 一个，负责所属连接的收发和状态；
- **Worker 线程**：由 `WorkerPool` 管理，执行服务查找、Protobuf 转换、用户 Handler 和响应编码。

## 启动与监听

服务端生命周期是：

```text
Created → Listening → Running → Stopping → Stopped
```

各公开操作的边界如下：

- `Create()` 归一化配置并创建内部组件；
- `RegisterMethod()` 在 `Created` 和 `Listening` 阶段可用；
- `Listen()` 只 bind/listen 本地 socket，不启动 Accept，也不发布 Consul 服务；
- `Run()` 冻结方法注册，启动连接 I/O loops 和 AcceptLoop，并按需注册 Consul；
- `Stop()` 是线程安全、幂等的关闭请求；
- `Run()` 返回表示正常 graceful shutdown 已经完成，或运行时失败已经完成清理。

`AcceptLoop()` 在 Server Run 线程上反复 `co_await Accept()`。新 socket 以 round-robin 方式交给一个 `ConnectionIoLoop::PostStartConnection()`，从此连接固定归属于该 I/O 线程。

## 连接 I/O 域

一个 `ConnectionIoLoop` 拥有一个 `UringContext`、一条 I/O 线程、一个 `DispatchMailbox` 和若干 `ServerConnection`。

它的线程边界是：

```text
Server Run / Accept 线程
  └─ PostStartConnection(socket)
         │
         ▼
Connection I/O 线程
  ├─ 创建 ServerConnection
  ├─ 启动 connection Run Task
  ├─ 维护 connections_
  └─ 清理已经 Closed 且 Task 已完成的连接
```

`connections_` 和每条连接的可变网络状态只由 I/O 线程访问。跨线程控制通过 `UringContext::Post()` 转交，不在每个连接字段上加锁。

## 单条连接状态机

`ServerConnection` 使用三个生命周期状态：

```text
Active
  ├─ peer EOF / BeginDrain / worker admission closed ──→ Draining
  └─ protocol/socket/backpressure fatal error ─────────→ Closed

Draining
  ├─ 接收已 admitted worker completion
  ├─ flush 已排队 response
  └─ inflight == 0 且 write queue 为空 ────────────────→ Closed
```

状态语义：

- `Active`：继续 Recv、解析和提交新请求；
- `Draining`：不再读取或提交新请求，但允许已接收请求完成并写回；
- `Closed`：socket 已关闭，不再启动新的 I/O。

`inflight_requests_`、`write_loop_active_`、`write_queue_` 和 `pending_write_bytes_` 是与生命周期正交的资源状态，不是额外 lifecycle state。

## 请求 dispatch

服务端收到字节后的主路径如下：

```text
co_await Recv
  ↓
RpcFrameStream::FeedBytes
  ↓
零个、一个或多个 RequestEnvelope
  ↓
per-connection batch admission
  ↓
WorkerPool::TrySubmitBatch
  ↓
ServiceRegistry::Dispatch
  ↓
Protobuf request ParseFromArray
  ↓
User Handler
  ↓
Protobuf response SerializeToString
  ↓
FrameCodec::Encode
```

一次 `FeedBytes()` 解出的 request batch 采用 all-or-nothing 准入：整批满足单连接 inflight 上限才进入 WorkerPool；否则整批返回 `ResourceExhausted`。WorkerPool 对 batch 代表的逻辑 RPC 数做全局容量检查。

`ServiceRegistry` 直接维护 `service_name → method_name → handler`。`Run()` 开始后注册被冻结，因此 Worker 线程只读 registry，不需要在 dispatch 热路径加锁。

## Worker completion 回投

Worker 线程不直接修改连接，也不直接发送 socket：

```text
Worker thread
  └─ DispatchMailbox::Submit(DispatchCompletion)
         │
         ├─ mutex 下加入 completion batch
         └─ context.Post(ProcessCompletionsOnContext)
                    │
                    ▼
原 Connection I/O 线程
  ├─ lock weak ServerConnection
  ├─ 释放 inflight accounting
  ├─ response 加入 write queue
  └─ 启动或继续 WriteLoop coroutine
```

每个 `ConnectionIoLoop` 有自己的 mailbox，因此 completion 天然回到产生请求的 I/O 域。多个 Worker submission 可以合并到一次 posted callback 中。

## 写路径

每条连接最多有一个活跃的 `WriteLoop()` coroutine。新的 response 只进入 `write_queue_`；如果 writer 已经运行，不再创建第二个 writer。

写协程会把相邻 response frame 合并为不超过 64 KiB 的发送 batch，并处理 partial send。响应可能按 Worker 完成顺序写出，客户端依靠 `request_id` 匹配，而不要求请求与响应严格同序。

## 背压

服务端保留三个相互独立的资源上限：

| 上限 | 所有者 | 保护的资源 | 过载行为 |
| --- | --- | --- | --- |
| `max_inflight_per_connection` | `ServerConnection` | 单连接已接收但未完成的 RPC | 整批返回 `ResourceExhausted` |
| `max_pending_jobs_global` | `WorkerPool` | 全局 admitted 逻辑 RPC | 返回 `ResourceExhausted` |
| `max_write_queue_bytes_per_connection` | `ServerConnection` | 慢客户端积压的 response bytes | 关闭该连接 |

第三个上限不能由 inflight 数替代：Worker completion 释放 inflight 后，response 仍可能因客户端读取过慢而停留在写队列中。

## Graceful shutdown

正常停止顺序是：

```text
Stop()
  ↓
关闭 WorkerPool 新提交准入
  ↓
停止 Accept，不再建立新连接
  ↓
所有 ServerConnection 进入 Draining
  ↓
WorkerPool::DrainAndJoin
  ├─ 已 admitted Handler 继续执行
  ├─ response 继续编码
  └─ completion 继续通过 DispatchMailbox 回投
  ↓
ConnectionIoLoop::FinishDrain
  ├─ 等待 inflight 清零
  ├─ 等待 response flush 或连接失败关闭
  ├─ 停止 UringContext
  └─ join I/O thread
  ↓
Stopped，Run() 返回
```

关键 invariant 是：WorkerPool 排空之前，Connection I/O loops 和 mailbox 必须仍然可用。否则已完成的 Handler 无法把 response 返回原连接。

如果 Handler 永远不返回，graceful shutdown 也会一直等待。当前没有 shutdown timeout 或 force-close 公共机制。

## Consul 注册

配置了 `service_name_` 时，`Run()` 将实际监听地址和端口注册到 Consul Agent。注册内容对应一个服务实例，而不是一个 RPC method，并附带 TCP 健康检查：Consul 周期性连接服务端口，只把通过检查的实例提供给客户端健康查询。

停止时服务端尝试 deregister。Consul 失败不会改变本地连接 drain 的顺序。
