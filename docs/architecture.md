# XRPC 架构设计

## 项目边界

XRPC 是一个仅支持 Linux 的一元 RPC 框架。框架负责传输、分帧、路由、handler 分发、服务发现、背压和指标链路，并使用 Protocol Buffers 完成类型化 payload 序列化。

## 服务端所有权

`RpcServer` 是公开的 move-only facade，其内部 controller 拥有：

- 一个 accept `IoUringContext`；
- 一组数量可配置的 connection I/O loops；
- 一个有界 handler worker pool；
- 类型化服务注册表；
- 可选的 Consul 注册器；
- 运行时指标与资源保护计数器。

accept loop 通过 round-robin 将新连接分配给 connection loop。一条连接始终由一个 I/O loop 独占，因此 socket 和写队列的修改由所有权串行化，不需要全局锁。解码后的请求提交到 worker pool，完成记录再把编码后的响应送回原 connection loop，由其按序排空写队列。

## 客户端所有权

`RpcClient` 拥有一个内部运行时，其中包含端点发现、端点健康状态、路由、request id 分配和可复用 transport。多个应用线程可以共享同一个客户端。transport 以 request id 多路复用在途调用，并在收到响应后唤醒对应的同步调用线程。

静态 target 和 Consul target 都会生成端点快照。路由可以使用单次调用指定的 sticky key；传输错误会先更新端点状态，再影响后续路由决策。

## 线协议

每个 frame 由固定头、XRPC header 和 payload 组成。XRPC header 携带 request id、服务名、方法名、消息类型和状态信息。类型化调用使用生成的 Protobuf 类型序列化请求并解析响应；原始字节调用不解释 payload 内容。

该协议是 XRPC 的私有协议，不兼容 HTTP/2 或 gRPC。

## 背压

服务端实施三层资源限制：

1. 每条连接允许的最大在途 handler job 数；
2. worker pool 全局最大 pending job 数；
3. 每条连接允许排队的最大响应字节数。

请求超过请求数限制时，服务端会尽可能返回 `ResourceExhausted`。响应队列超过字节水位时，服务端关闭该连接。公开统计接口和 Prometheus 指标会暴露拒绝数和高水位计数器。

## 关闭流程

关闭时先停止接收新连接，注销可选的 Consul 服务，再停止 connection loops，并 join worker 与运行时线程。`Stop()` 是尽力而为且幂等的，可以在另一个线程阻塞于 `Run()` 时调用。

## 有意保留的限制

当前版本不提供流式调用、TLS、认证、跨语言 stub 生成或稳定的线协议兼容保证。这些边界让运行时保持足够小，使调度、所有权和性能行为仍然可以被完整审查。
