# 客户端运行时

## 当前模型

`RpcClient` 提供同步、可并发调用的一次请求—响应 API。当前客户端没有使用服务端的 `io_uring` 运行时，而是采用更直接的模型：

```text
caller thread
  ├─ 服务发现与路由
  ├─ 阻塞式 connect / send
  └─ 等待 PendingCall

TcpTransport reader thread
  ├─ 阻塞式 recv
  ├─ response frame decode
  ├─ request_id 匹配
  └─ 唤醒 caller
```

多个调用线程可以共享一个 `RpcClient`，同一 Endpoint 的并发调用复用一个 `TcpTransport` 和一条 TCP 连接。

## 组件边界

```text
RpcClient
└─ RpcClient::Impl
   ├─ ServiceDiscovery
   │  ├─ StaticDiscovery
   │  └─ ConsulDiscovery
   ├─ RoutingSnapshot
   │  ├─ DiscoverySnapshot
   │  ├─ TcpTransport[]
   │  └─ HashRingEntry[]
   ├─ round-robin counter
   └─ request-id counter
```

- `RpcClient` 负责公开 Protobuf 消息 API 和异常到 `Status` 的边界；
- `RpcClient::Impl` 负责发现、路由、request ID 和安全 failover；
- `ServiceDiscovery` 发布完整、不可变的 Endpoint 集合；
- `RoutingSnapshot` 把同一版 Endpoint、transport 和一致性哈希环绑定在一起；
- `TcpTransport` 负责一个 Endpoint 的连接、多路复用和失败传播。

## 服务发现

`target_` 决定发现方式：

```text
list://127.0.0.1:9000,127.0.0.1:9001
  └─ StaticDiscovery

consul://echo-service
  └─ ConsulDiscovery
```

`StaticDiscovery` 在构造时解析、排序并去重固定列表，此后始终返回同一个快照。

`ConsulDiscovery` 首先执行一次立即查询，然后由一条 refresh thread 等待服务变化。查询带有 `passing=true`，因此只发布通过 Consul 健康检查的实例。查询失败时保留上一版可用快照，并记录最近错误供调用失败信息使用。

发现层通过 `atomic<shared_ptr<const DiscoverySnapshot>>` 发布新快照。读者取得 `shared_ptr` 后，该次调用看到的 Endpoint 列表在整个路由过程中保持不变。

### Consul 变化如何到达客户端

Consul 不会主动向 xRPC 推送实例上下线通知。客户端使用 Consul blocking query，在 HTTP 请求中携带上一次响应返回的查询索引：

```text
GET /v1/health/service/echo-service?passing=true&index=1234&wait=5s
```

这条请求会在健康实例集合发生变化时提前返回；如果没有变化，最多等待 5 秒后返回，客户端随后立即发起下一次查询。响应头 `X-Consul-Index` 提供下一次查询使用的新索引。

```text
服务端注册、主动注销或健康检查状态变化
  ↓
Consul 更新健康查询结果和 index
  ↓
客户端 blocking query 返回
  ↓
解析所有 passing 实例
  ↓
原子发布新的 DiscoverySnapshot
  ↓
后续调用据此构建并使用新的 RoutingSnapshot
```

正常注销和健康检查失败最终都表现为 Endpoint 从新快照中消失。已经取得旧快照的调用可以继续使用其中的 Endpoint 和 `TcpTransport`；新快照只影响后续路由，不会在调用过程中替换其视图。

Consul 暂时不可用时，refresh thread 保留上一份成功快照，并在 1 秒后重试。这保证短暂的控制面故障不会立即清空客户端路由，但旧 Endpoint 在此期间也可能已经失效，实际连接错误仍由 `TcpTransport` 和 failover 规则处理。

## 路由快照

`RpcClient::Impl::ResolveRoutingSnapshot()` 把发现快照转换为调用所需的完整路由视图：

```text
DiscoverySnapshot
  ↓
比较 snapshot pointer identity
  ↓ changed
routing_update_mutex
  ↓
复用未变化 Endpoint 的 TcpTransport
  ↓
构建 consistent-hash ring
  ↓
atomic publish RoutingSnapshot
```

稳定路径只需要两次原子 shared pointer 操作：读取 discovery snapshot，再读取已经匹配的 routing snapshot。只有 membership 变化时才进入 mutex 保护的重建路径。

锁内的第二次检查用于避免多个并发调用依次重复构建同一版路由。新的 snapshot 在 Endpoint、transport 和 hash ring 都准备完成后才发布；正在执行的调用仍可持有旧 snapshot，并安全使用其中的旧 transport。

Endpoint 未变化时复用已有 `TcpTransport`，因此一次 Consul refresh 不会无条件断开所有连接。

## Endpoint 选择

普通调用使用原子计数器选择 round-robin 起点：

```text
start = next_endpoint_index % endpoint_count
```

设置 `sticky_key_` 时，客户端使用包含虚拟节点的一致性哈希环选择起点。哈希环属于 `RoutingSnapshot`，其中的 endpoint index 与同一 snapshot 的 Endpoint 和 transport 数组使用相同下标空间。

选择结果只是第一次尝试的起点。是否继续尝试其他 Endpoint 由请求提交状态决定。

## 单个 Endpoint transport

一个 `TcpTransport` 最多维护一条到目标 Endpoint 的连接：

```text
TcpTransport
├─ socket + state_mutex
├─ write_mutex
├─ reader jthread
├─ pending_mutex
└─ request_id → PendingCall
```

调用流程是：

1. `FrameCodec` 编码 request frame；
2. 按需建立 TCP 连接并启动唯一 reader thread；
3. 在写入前注册 `PendingCall`，避免极快响应先于 pending map 可见；
4. 通过 `write_mutex_` 串行写完整 frame；
5. 调用线程等待自己的 `PendingCall`；
6. reader thread 持续解码 response，按 `request_id` 唤醒正确调用者。

写操作必须串行，否则两个线程的 frame bytes 可能在同一 TCP 字节流中交错。响应读取只由 reader thread 完成，所以多个调用可以并发等待，且响应可以乱序到达。

`max_inflight_per_endpoint` 限制 pending map 中的同步调用数量。达到上限的调用在发送前返回 `ResourceExhausted`。

## Timeout 与 deadline

用户配置的是相对 timeout。一次调用开始时，客户端把它转换为绝对 deadline：

```text
timeout
  ↓ now + timeout
deadline
  ├─ connect 使用剩余时间
  ├─ send 使用剩余时间
  └─ PendingCall 等待到同一 deadline
```

failover 不会为每个 Endpoint 重新获得一份完整 timeout；所有尝试共享最初的总时间预算。timeout 为零时不设置 deadline。

请求已经发出后 caller 超时，会从 pending map 移除对应 request ID。之后到达的迟到 response 找不到 pending entry，会被 reader 安全忽略。

## 安全 failover

xRPC 区分两个提交状态：

```text
NotSent
  请求字节没有提交到网络
  → 可以尝试下一个 Endpoint

MaybeSent
  对端可能已经收到全部或部分请求
  → 必须停止 failover，避免非幂等 RPC 重复执行
```

典型 `NotSent` 失败包括连接建立失败、发送前 deadline 到期和单 Endpoint inflight 已满。发送开始后的 socket 错误、等待响应超时和 reader 观察到的连接失败属于 `MaybeSent`。

这个分类比“失败就重试”保守，但它给没有幂等声明的 RPC 一个清楚的安全边界。

## 连接失败与关闭

reader 观察到 EOF、协议错误或 socket 错误时，只关闭自己对应的 socket generation，并把当前所有 pending calls 完成为 `Unavailable/MaybeSent`。下一次调用发现 socket 无效时会 lazy reconnect。

销毁 `RpcClient` 时：

1. 停止并 join discovery refresh thread；
2. 释放 routing snapshot；
3. 各 `TcpTransport` 关闭 socket；
4. 唤醒 pending callers；
5. join reader thread。

应用必须保证销毁或移动 `RpcClient` 时，没有其他线程仍在调用它。客户端不提供服务端式 graceful drain 状态机。
