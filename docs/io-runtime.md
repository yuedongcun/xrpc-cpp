# I/O 运行时

## 核心模型

`UringContext` 是 xRPC 对 Linux io_uring 的底层运行时封装。它管理一个 io_uring ring 以及围绕该 ring 的事件循环，并向协程提供 `Accept()`、`Recv()` 和 `Send()` 等异步操作。借助 C++20 协程，服务端可以把这套异步完成过程写成顺序代码；读取数据时，核心调用可以简化为：

```cpp
const io::IoResult result = co_await context.Recv(fd, buffer, size);
```

这里的 `context` 是当前连接所属 I/O 线程上的 `UringContext`。这行代码表示：通过该 `UringContext` 从 socket `fd` 异步读取最多 `size` 字节，并把数据写入 `buffer`。`Recv()` 创建一个尚未启动的 io_uring read operation；执行 `co_await` 时才登记等待者、准备 SQE 并进入 staged submission。内核完成读取后，运行时恢复该协程，表达式最终得到一个 `IoResult`，其中包含传输字节数或 I/O 错误。

它看起来像普通的同步调用：

```text
发起读取
→ 等到读取完成
→ 检查 result
→ 继续处理收到的数据
```

但等待期间不会阻塞 I/O 线程。同一个 `UringContext` 可以继续处理其他连接，直到当前读取的 CQE 到达。

```mermaid
sequenceDiagram
    participant C as 连接协程
    participant U as UringContext
    participant K as Linux io_uring

    C->>U: Recv(fd, buffer, size)
    U-->>C: 返回 UringAwaitable
    C->>U: co_await 登记 waiter，prepare + stage
    Note over C: 挂起当前协程
    U->>K: event-loop turn 末尾统一 submit
    Note over U: 继续驱动其他连接
    K-->>U: 返回 recv CQE
    U->>U: CQE 转换为 IoResult
    U-->>C: 写入结果并恢复协程
    Note over C: 继续处理收到的数据
```

`RpcServer::Run()` 所在线程驱动 Accept 使用的 `UringContext`；每个 Connection I/O Loop 则各自创建一个 I/O 线程和 `UringContext`，通过自己的 io_uring ring 驱动所属连接，并在 CQE 到达时恢复等待该操作的连接协程。

从使用方式看，它与 Asio 的 coroutine I/O 模型相似：异步操作通过 `co_await` 挂起，并由事件循环在完成事件到达后恢复。不同的是，xRPC 不提供通用异步框架，而是直接围绕 Linux `io_uring` 实现服务端需要的最小能力。

当前客户端仍使用阻塞式 transport，不经过这套运行时。客户端模型见[客户端运行时](client-runtime.md)。

## 事件循环与线程模型

`UringContext::Run()` 在调用线程中执行事件循环：它首先 stage 并提交用于跨线程唤醒的 eventfd poll，然后等待 CQE。每个有界 event-loop turn 至少处理一个 CQE，并继续处理不超过 ring submission capacity 的 ready CQE；turn 中产生的 SQE 最后统一提交。Runtime 绝不会携带 staged SQE 进入下一次 blocking wait。停止请求发出，并且所有普通异步 I/O 和 eventfd poll 都结束后，`Run()` 才返回。

```mermaid
flowchart TD
  A[stage 初始 eventfd poll] --> B[FlushSubmissions]
  B --> C[blocking wait: 至少一个 CQE]
  C --> D[处理 bounded CQE turn]
  D --> E[resume coroutine / drain Post callbacks]
  E --> F[新 operation prepare + stage]
  F --> G[FlushSubmissions]
  G --> H{停止且 pending 已清空?}
  H -- 否 --> C
  H -- 是 --> I[Run 返回]
```

`UringContext` 不是进程级的全局对象，服务端会按职责创建多个实例：

| 事件循环 | `Run()` 所在线程 | 负责的 I/O |
| --- | --- | --- |
| Accept context | `RpcServer::Run()` 的调用线程 | 监听 socket 的异步 Accept |
| Connection context | 每个 Connection I/O Loop 自己的 `std::jthread` | 该 Loop 所属连接的 Recv、Send 和取消 |

这里的“单线程”不表示 I/O 串行执行。一个 ring 中可以同时存在多条连接的 pending operation；线程提交操作后继续处理其他连接，内核完成某项 I/O 时再通过 CQE 通知它。

`Accept()`、`Recv()` 和 `Send()` 只构造 deferred awaitable；它们的 `await_suspend()` 与 `CancelFd()` 只能由对应 `UringContext::Run()` 的线程调用。这样，ring 的操作状态以及上层连接的可变 I/O 状态都可以遵守线程封闭，不需要在每次读写周围加锁。

`Post()` 和 `RequestStop()` 是跨线程控制入口。其他线程不能直接提交 socket I/O，而是通过 `Post()` 把工作交给 context 线程；`RequestStop()` 只负责发出停止请求。跨线程入口负责传递命令，不会改变连接状态仍由所属 I/O 线程维护这一约束。

## 协程如何等待 I/O

当前生产代码中只有三个协程函数，并且都运行在服务端 I/O 路径：

| 协程函数 | 等待的 I/O | 职责 |
| --- | --- | --- |
| `RpcServer::Impl::AcceptLoop()` | `AcceptMultishot()` | 持续接收新 TCP 连接 |
| `ServerConnection::ReadLoop()` | `RecvProvided()` | 读取并解析一条连接上的请求字节流 |
| `ServerConnection::WriteLoop()` | 写队列通知、`Send()` | 等待并按顺序发送该连接写队列中的响应 |

三者都返回 `Task<void>`。当前客户端不使用协程，生产路径也没有其他返回 `Task` 的函数。下面是连接读协程的简化结构：

```cpp
auto ServerConnection::ReadLoop() -> runtime::Task<void> {
  while (...) {
    io::IoResult result = co_await context_.RecvProvided(fd);

    // 把 result.buffer_.Bytes() 交给 RpcFrameStream，再归还 buffer
  }
}
```

调用协程函数时，编译器会把函数转换为状态机，并创建 coroutine frame，用来保存局部变量、当前执行位置和 promise。`Task<void>` 持有这个 frame 的 coroutine handle；xRPC 的 `Task` 初始处于挂起状态，由所属 runtime 通过 `Start()` 启动。

每条服务端连接固定拥有一条读协程和一条写协程。读协程通过 `RecvProvided()` 等待内核网络输入；写协程在有数据时通过 `Send()` 等待 socket，在队列为空时通过一个单等待者 awaiter 等待用户态入队通知。两种等待都只挂起当前协程，不阻塞 Connection I/O 线程。

当前服务端 recv 使用单次 provided-buffer receive；accept 和 eventfd poll 使用 multishot。每个 Connection I/O Loop 注册独立的 buffer pool，默认 2048 × 16 KiB（32 MiB 数据区），由该 Loop 的连接共享；注册失败会使初始化失败，没有普通 `Recv()` 回退。`RpcFrameStream::FeedBytes()` 仍复制输入，返回后立即归还 buffer，再分发请求，因此这一步还不是零复制。

单次 provided-buffer recv 因池耗尽返回 `ENOBUFS` 时，服务端记录错误并立即关闭连接，不等待归还或重试。保留 `provided_buffer_enobufs` 耗尽计数和 buffer pool 占用统计，用于压测容量配置。其他接收错误同样记录后关闭；正常 EOF 和取消不记录错误。

协程返回类型需要向编译器提供 `promise_type`。在 xRPC 中，`Task<void>::promise_type` 实际指向存放在 coroutine frame 内的 `TaskPromise<void>`。它参与整个协程从创建到结束的过程：

```mermaid
flowchart TB
  call["调用协程函数"] --> frame["创建 coroutine frame<br/>构造 promise"]
  frame --> result["get_return_object()<br/>产生 Task&lt;void&gt;"]
  result --> initial["initial_suspend()<br/>新协程保持挂起"]
  initial --> start["Task::Start()<br/>恢复并执行函数体"]
  start --> outcome{"执行结果"}
  outcome -->|正常返回| returned["return_void()"]
  outcome -->|未捕获异常| failed["unhandled_exception()<br/>保存 exception_ptr"]
  returned --> final["final_suspend()<br/>发布完成并恢复 continuation"]
  failed --> final
```

这些函数是 C++ 协程协议规定的 promise 钩子，编译器会在相应阶段调用它们：

| Promise 钩子 | 在 xRPC 中的作用 |
| --- | --- |
| `get_return_object()` | 创建持有 coroutine handle 的 `Task<void>` |
| `initial_suspend()` | 返回 `suspend_always`，让 Task 创建后先保持挂起 |
| `return_void()` | 处理协程正常执行到 `co_return` |
| `unhandled_exception()` | 保存未捕获异常，供 Task 的观察者重新抛出 |
| `final_suspend()` | 发布完成状态，并恢复等待该 Task 的 continuation |

这里的 coroutine promise 是 C++ 协程协议的一部分，与线程同步中常见的 `std::promise` 没有关系。它管理整个协程的生命周期；协程运行过程中遇到的每一次 `co_await`，则由对应的 awaiter 管理。

C++20 只定义协程的语言协议，不提供事件循环或网络运行时。编译器看到 `co_await expression` 时，会从表达式取得一个 awaiter，并按标准协议调用 `await_ready()`、`await_suspend()` 和 `await_resume()`。这些名称来自 C++ 协程协议，不是 xRPC 自行约定的接口。

一个类型可以通过 `operator co_await` 返回单独的 awaiter，也可以直接实现这三个方法。xRPC 采用后者：`UringContext::Recv()` 返回的 `UringAwaitable` 本身就是 awaiter。因此下面的代码：

```cpp
const io::IoResult result =
    co_await context_->Recv(fd, buffer, size);
```

可以近似理解为编译器生成了以下控制流程：

```cpp
UringAwaitable awaiter = context_->Recv(fd, buffer, size);

if (!awaiter.await_ready()) {
  // current_handle 由编译器从当前 coroutine frame 构造。
  const bool should_suspend = awaiter.await_suspend(current_handle);
  if (should_suspend) {
    // 挂起当前协程，并把控制权返回给调用方。
  }
}

const io::IoResult result = awaiter.await_resume();
```

这是便于理解的等价流程，不是编译器实际生成的完整 C++ 源码。`UringAwaitable` 的三个方法分别控制这一个 I/O 等待点：

| 方法 | 作用 |
| --- | --- |
| `await_ready()` | one-shot operation 尚未启动，固定返回 `false` |
| `await_suspend(handle)` | 保存当前协程 handle，将 operation 交给 Runtime 准备并 stage |
| `await_resume()` | 同步完成或 CQE 恢复后返回 `IoResult` |

挂起只会保存连接协程的执行状态并把控制权交还给事件循环，不会阻塞 I/O 线程。该线程可以继续处理其他连接；对应 CQE 到达后，`UringContext` 写入 `IoResult` 并恢复保存的 handle，连接协程随后从 `co_await` 之后继续执行。

## I/O 完成后如何恢复协程

`co_await` 挂起协程后，Linux 只知道某个 io_uring operation 已经完成，并不知道对应哪个 C++ coroutine frame。`UringContext` 必须把 CQE 转换为 `IoResult`，找到等待该结果的 coroutine handle，并在 I/O 线程上恢复它。

### 单一 Operation 状态

一次 one-shot awaitable I/O 只有一个状态对象 `Operation`。它同时保存请求参数、completion 结果和 coroutine waiter：

```mermaid
classDiagram
  direction LR

  class UringContext {
    +Accept() UringAwaitable
    +Recv() UringAwaitable
    +Send() UringAwaitable
  }

  class UringAwaitable {
    -unique_ptr unstarted_operation_
    -Operation* active_operation_
    +await_ready() bool
    +await_suspend(handle) bool
    +await_resume() IoResult
  }

  class Operation {
    +OperationType type_
    +CompletionCategory completion_category_
    +int fd_
    +buffer_
    +size_t length_
    +IoResult result_
    +coroutine_handle continuation_
  }

  UringContext --> UringAwaitable : 创建并返回
  UringAwaitable --> Operation : 启动前唯一拥有
  UringContext --> Operation : stage/CQE 生命周期
  UringAwaitable ..> Operation : completion resume 期间借用
```

`Accept()`、`Recv()` 和 `Send()` 只构造尚未启动的 `Operation`，由返回的 `UringAwaitable` 以 `unique_ptr` 唯一拥有。没有执行 `co_await` 的 awaitable 不会产生 SQE。

`await_suspend()` 先登记 coroutine handle，再将 ownership 转给 Runtime。Runtime 准备 SQE 并放入统一的 staged submission；event-loop turn 结束时才调用 `io_uring_submit()`。提交后的 `user_data` 保存 `Operation*`，CQE 到达后 completion handler 恢复唯一 ownership。

completion handler 写入 `Operation::result_` 后同步恢复协程。在 `continuation.resume()` 返回前，handler 的 `unique_ptr` 保证 `Operation` 仍然有效，因此 `await_resume()` 可以通过短暂的非拥有指针读取结果；resume 返回后，handler 销毁 `Operation`。

`AcceptMultishot()` 复用同一 awaitable 接收多个结果：CQE 带 `IORING_CQE_F_MORE` 时保留 Operation，最终 CQE 才回收。结果通过 awaitable 的单结果槽交付；MORE 为真时，消费者必须同步处理并在恢复调用返回前重新等待同一操作，不能改为挂起在其他异步操作上。停止接纳后，AcceptLoop 继续消费到最终 CQE，并关闭晚到的已接受 socket。

### 正常完成路径

`UringContext::Recv()` 等接口只创建 deferred `Operation`。真正启动发生在 `await_suspend()`，顺序固定为 waiter established、operation staged、event-turn submit、CQE、resume：

完整的恢复路径如下：

```mermaid
sequenceDiagram
  participant C as 连接协程
  participant A as UringAwaitable
  participant U as UringContext
  participant K as Linux io_uring

  C->>U: Recv(fd, buffer, size)
  U->>U: 创建 deferred Operation
  U-->>C: 返回 UringAwaitable

  C->>A: await_suspend(handle)
  A->>U: 转移 Operation ownership
  U->>U: prepare SQE + stage
  Note over C: 协程挂起，I/O 线程继续运行
  U->>K: event-loop turn 末尾统一 submit

  K-->>U: 返回 CQE 和 Operation*
  U->>U: 恢复 ownership，写入 IoResult
  U-->>C: continuation_.resume()
  C->>A: await_resume() 借用 Operation 读取结果

  Note over C: 从 co_await 之后继续执行
```

### 边界时序

context 已经停止接受 operation 时，Runtime 不执行 ownership transfer，而是在未启动的 `Operation` 中写入 `ECANCELED`，让 `await_suspend()` 返回 `false`；协程不挂起，直接由 `await_resume()` 取得同步结果。

没有 cancellation framework 之前，pending I/O coroutine 不允许被提前销毁。销毁仍持有 active operation 借用指针的 `UringAwaitable` 会调用 `Abort()`，而不是静默丢弃 completion。pending coroutine 只能在所属 I/O-loop 线程上恢复或结束；跨线程并发销毁 coroutine frame 属于非法用法。正常服务端路径会保留连接及其 Task，直到相关 I/O 完成或显式 fd cancellation 产生 terminal CQE。

## CQE 的三种处理方式

`Operation::completion_category_` 将 CQE 分成三类，它区分的是“收到 CQE 后运行时要做什么”。

| 类别 | 来源 | CQE 到达后的行为 |
| --- | --- | --- |
| Awaitable I/O | `Accept`、`AcceptMultishot`、`Recv`、`RecvProvided`、`Send` | 转换为 `IoResult`，再恢复等待协程 |
| Cancel | `CancelFd()` | 确认取消请求，不直接恢复业务协程 |
| Wakeup | `eventfd` poll | 排空 eventfd 与 posted callback，或推进停止流程 |

Cancel CQE 只表示“取消请求已经被内核处理”。被取消的 `Accept`、`Recv` 或 `Send` 仍会各自产生 completion；原协程由那一条 I/O completion 恢复，而不是由 Cancel CQE 恢复。

Wakeup CQE 是 `Post()` 与 `RequestStop()` 的共同落点。正常运行时，`UringContext` 排空 eventfd 和 callback queue，带 MORE 的 multishot poll 保持活动，不重新提交；只有收到最终完成且仍在运行时才重新注册。停止时取消活动 poll，等待其最终 CQE 后释放操作。

## 跨线程控制与停止

`Accept()`、`Recv()` 和 `Send()` 只创建 deferred awaitable；真正的 operation admission 发生在 `await_suspend()`，且只能运行在 `UringContext::Run()` 所在线程。`CancelFd()` 同样只能由该线程调用。其他线程不能启动 socket I/O，只能通过 `Post()` 或 `RequestStop()` 发送控制命令。

### Post()

`Post()` 将 callback 放入 mutex 保护的队列。队列从空变为非空时，调用方写入 eventfd；已经挂起的 eventfd poll 因而产生 Wakeup CQE，最终由 I/O 线程取出并执行 callback：

```text
其他线程
  ↓
Post(callback)
  ↓
callback queue + eventfd write
  ↓
Wakeup CQE
  ↓
I/O 线程执行 callback
```

服务端通过这条路径把已接受 socket 交给 Connection I/O Loop，也把 Worker 产生的 completion 直接交回原连接的 I/O 线程。completion 只携带 `ConnectionId`；连接仍由所属 I/O loop 唯一拥有，其可变状态也只由该线程修改。

### 停止请求、I/O 取消与资源释放

`UringContext` 负责在给定 fd 上提交和完成异步 I/O，但不管理 socket 的生命周期。socket 由上层连接对象拥有，并由其决定何时关闭。

因此，运行时停止、pending I/O 取消和 fd 释放是三个独立问题：

| 接口                | 职责                                                         |
| ----------------- | ---------------------------------------------------------- |
| `RequestStop()`   | 请求 event loop 停止；`Run()` 会继续处理 CQE，直到所有已提交 operation 完成后返回 |
| `CancelFd(fd)`    | 取消指定 fd 上尚未完成的 I/O，使对应 operation 尽快产生 completion           |
| `Socket::Close()` | 关闭并释放 socket fd，由 socket owner 负责调用                        |

`RequestStop()` 不会直接终止 pending I/O。它只表达 `UringContext` 的停止意图，并通过 eventfd 唤醒 `Run()`。在停止请求发出后，`Run()` 仍会继续处理已有 CQE，直到 pending operation 归零且 wakeup poll 结束后才返回。

`CancelFd(fd)` 也不会停止整个 context。它只针对指定 fd 提交取消操作。取消请求本身会产生 Cancel CQE，被取消的原始 I/O 也会产生自己的 CQE，因此这些 completion 仍需要由 `Run()` 正常处理，相关 operation 才能完成回收。

连接何时取消 I/O 和关闭 socket 属于上层连接生命周期策略。正常 shutdown 可以先停止接收新请求并排空已经接收的工作；发生连接错误或要求立即停止时，也可以直接终止连接。`UringContext` 不参与这些策略判断，只负责已提交 I/O 的执行、取消和 completion 处理。

因此：

* `RequestStop()` 管理 `UringContext` / event loop 的生命周期；
* `CancelFd(fd)` 管理指定 fd 上 pending I/O 的取消；
* `Socket::Close()` 管理 socket fd 的生命周期。

三者职责独立，由上层 runtime 在关闭流程中协调使用。

## 运行时约束

* 一个 `UringContext` 只由一条 `Run()` 线程驱动；同一个 ring 中可以同时存在多条 pending I/O。
* `Accept()`、`Recv()` 和 `Send()` 等异步操作通过 `UringAwaitable` 与 `co_await` 挂起调用方；I/O 线程本身不执行阻塞式等待。
* `UringContext` 负责 SQE 提交、CQE 处理、协程恢复和跨线程唤醒；`Task` 负责 coroutine frame。
* 连接状态、RPC 帧解析、业务调度、服务发现和客户端路由属于上层 runtime，不属于 I/O 层。
