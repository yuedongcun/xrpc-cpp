# I/O 运行时

## 作用范围

`src/io/` 提供 xRPC 服务端使用的 Linux I/O 基础设施。它负责 `io_uring` 提交与完成、协程恢复、跨线程唤醒和 socket 生命周期，但不理解 RPC 请求、服务注册或业务 Handler。

当前客户端没有使用这套运行时。客户端网络模型见[客户端运行时](client-runtime.md)。

## 核心对象

```text
UringContext
└─ Runtime
   ├─ io_uring
   ├─ eventfd
   ├─ posted callback queue
   └─ pending Operation

Task<T>
└─ coroutine frame / promise / completion state

UringAwaitable
└─ shared_ptr<AwaitableState>

Operation
├─ kernel operation information
├─ completion category
└─ weak_ptr<AwaitableState>
```

### `UringContext`

一个 `UringContext` 对应一个单线程事件循环。调用 `Run()` 的线程是它的执行线程；`Accept()`、`Recv()`、`Send()`、`SleepFor()` 和 `CancelFd()` 都要求在该线程上调用。

`Post()` 和 `RequestStop()` 是跨线程入口：其他线程可以投递回调或请求停止，回调最终仍由 `Run()` 所在线程执行。

### `Task<T>`

`Task<T>` 拥有 C++20 coroutine handle，并以初始挂起方式创建。调用方通过 `Start()` 显式启动，或通过 `co_await` 把执行权转入子协程。

生产路径中的主要 Task 是：

- 服务端 `AcceptLoop()`；
- 每条 `ServerConnection::Run()` 读协程；
- 每条连接按需创建的 `DrainWriteQueue()` 写协程。

### `UringAwaitable`、`AwaitableState` 与 `Operation`

一次普通异步 I/O 当前使用两个状态对象：

- `Operation` 保存提交给内核和解释 CQE 所需的信息；
- `AwaitableState` 保存返回给协程的 `IoResult` 和 continuation。

`UringAwaitable` 通过 `shared_ptr` 持有 `AwaitableState`，`Operation` 只持有 `weak_ptr`。因此如果协程帧和 awaitable 在 CQE 到达前已经销毁，内核操作仍可完成和回收，但完成路径不会恢复失效的 coroutine。

## I/O 提交与完成

以 `Recv()` 为例：

```text
ServerConnection coroutine
  │
  ├─ context.Recv(fd, buffer, size)
  │    ├─ 创建 AwaitableState
  │    ├─ 创建 Operation
  │    ├─ 准备 SQE
  │    └─ io_uring_submit
  │
  └─ co_await UringAwaitable
          │
          ▼
        kernel
          │
          ▼
         CQE
          │
          ├─ ProcessCqe 恢复 Operation ownership
          ├─ CQE 转换为 IoResult
          ├─ weak_ptr 锁定 AwaitableState
          ├─ 写入结果
          └─ resume continuation
```

`Accept`、`Recv`、`Send` 和 `Timeout` 共用 awaitable completion 路径。`Cancel` 和 `Wakeup` 是控制操作，不恢复业务协程：

- Cancel CQE 只确认取消请求的结果；
- Wakeup CQE 负责处理 `eventfd`，执行 posted callbacks，并决定是否重新提交 wakeup poll。

## 跨线程 `Post()`

`UringContext` 使用一个受 mutex 保护的 callback queue 和一个 `eventfd`：

```text
其他线程
  │
  ├─ Post(callback)
  ├─ callback 入队
  └─ write(eventfd)
         │
         ▼
io_uring poll CQE
  │
  ├─ 读取 eventfd
  ├─ 取出 callback batch
  └─ 在 Run 线程执行 callback
```

服务端通过这个边界交接新连接、关闭命令和 Worker completion。`Post()` 保证工作切换到 I/O 线程，不使被调用对象本身变成可任意并发访问的对象。

## 停止与取消

`RequestStop()` 的含义是请求事件循环停止，而不是立即销毁 ring：

1. 停止接受新的 posted callbacks；
2. 唤醒 `Run()`；
3. 执行已经入队的 callbacks；
4. 取消仍在等待的 timeout operation；
5. 继续处理已提交 I/O 的 CQE；
6. pending I/O 和 wakeup poll 都结束后，`Run()` 才返回。

连接关闭时，所属 I/O 线程通过 `CancelFd()` 请求取消该 fd 上的 pending operation。取消本身也会产生 CQE，因此仍由 event loop 完成回收。

## Socket 边界

`io::Socket` 是文件描述符的 RAII 包装，用于 bind、listen、阻塞式 connect 以及显式 close。服务端连接的数据收发由 `UringContext` 完成；当前客户端的阻塞 transport 直接使用 socket API。

同步和异步发送路径都使用 `MSG_NOSIGNAL`。客户端提前断开时，写操作以 `EPIPE` 等普通 I/O 错误返回，不会因为 `SIGPIPE` 终止整个进程。

## 线程契约

| 操作 | 调用线程 | 并发语义 |
| --- | --- | --- |
| `Run()` | 唯一 owner 线程 | 不可重入 |
| `Accept/Recv/Send/SleepFor/CancelFd` | `Run()` 线程 | 依赖线程封闭，不额外加锁 |
| `Post()` | 任意控制/工作线程 | callback 在 `Run()` 线程执行 |
| `RequestStop()` | owner 或其他线程 | 线程安全、幂等请求 |

这套设计的核心不是让所有 I/O 对象都线程安全，而是让可变 I/O 状态只属于一个 event-loop 线程，并通过少数交接点接收跨线程命令。
