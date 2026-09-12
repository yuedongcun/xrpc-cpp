# io_uring 高级优化：设计与演进记录

> 草稿，随实现持续更新，发布前统一审核。本文区分已实现的行为、待定设计和待验证的收益。

本文记录 xRPC 从 one-shot 接收到 provided buffer pool、再到 multishot 的优化过程。每一步先建立可理解的生命周期，再用针对性的测试和 benchmark 检查结果。

## 实现组织

内部接口保留在 `src/include/io/uring/context.h` 和 `src/include/io/uring/buffer_pool.h`；实现对应 `src/io/uring/context.cpp` 和 `buffer_pool.cpp`。`UringContext` 直接拥有 ring、buffer pool、eventfd 和操作状态，不再通过 `Runtime` 对象间接管理。

`context.cpp` 按职责分区，相关实现尽量集中；新增机制时可按关联关系调整分区与函数顺序。理解整体调度可从 `Run()` 入手；追踪单个 I/O 可沿 `TryStartOperation()`、`FlushSubmissionBatch()`、`ProcessCqe()` 查看提交、所有权转移与完成处理。buffer 的注册、领取和归还集中在 `buffer_pool.cpp`。

## 优化起点

原来的 `ServerConnection` 持有一个 16 KiB 的 `std::string read_buffer_`，在连接构造时分配，之后每次 `Recv()` 复用。不存在“每次接收重新分配 16 KiB buffer”的问题。

原接收路径仍有以下开销：

- 每次接收创建一个 `Operation`，提交一次 one-shot recv。
- 每次完成后恢复读协程，处理数据，再发起下一次接收。
- `FrameStream::FeedBytes()` 复制输入，保存解析和拼帧所需的数据。
- 每个连接长期独占接收 buffer，包括空闲连接。

发送侧的 `PendingWrite::bytes_` 是待发送响应的所有权，与接收 buffer 不同。本轮先聚焦接收路径。

## 第一步：provided buffer pool（已实现）

基础池见提交 `9b588a4`，服务端接入见提交 `f7f6a4a`。

每个 `ConnectionIoLoop` 创建自己的 provided buffer pool，默认包含 512 块 16 KiB buffer，共 8 MiB 数据区。数据内存在池初始化时统一分配，由该 Loop 下的连接共享。池向对应 io_uring 注册，当前注册失败会使初始化失败，没有自动回退。

`ServerConnection` 移除独占的 `read_buffer_`，改用 one-shot `RecvProvided()`。内核选择池内 buffer；完成后，`IoResult` 携带一个可移动的 `UringBuffer` 租约。租约管理归还责任，不重新分配数据内存。

接收流程：

```text
RecvProvided → CQE 携带 buffer ID → 获取租约
             → FeedBytes 复制输入 → 归还 buffer → 分发请求
```

池的使用和归还限制在所属 I/O 线程，池必须活得比所有租约更久。正常路径在 `FeedBytes()` 返回后立即归还，提前退出则由租约析构归还。

multishot 池耗尽返回最终 `ENOBUFS` 时，服务端保留连接，等待 pool 归还进展后重新提交接收。context 提供 `WaitForBufferReturnSince`，基准是该请求启动时的累计归还数；已发生的归还不会漏掉。进展不代表 buffer 预留，重试仍可能耗尽。关闭、drain 和 context 停止均取消等待。其他接收错误通过 glog 记录后关闭；正常 EOF 和取消不记录错误。

这一阶段主要改变内存归属，并为 multishot 做准备。每次 recv 的 `Operation` 分配、提交以及 `FeedBytes()` 复制仍然存在，尚无 benchmark 证明独立性能收益。连接数较少时，预分配共享池甚至可能比原方案占用更多内存。

验证记录：构建、8 个默认测试和修改文件的 clang-tidy 已通过；新增单 buffer 测试覆盖持有租约时耗尽、数据未被覆盖、归还后复用及 EOF。

## 第二步：multishot recv（底层与服务端已接入）

目标是通过一次提交持续获得多次接收完成，减少重复提交和操作对象创建。每份接收结果仍需要处理 CQE，输入复制也不会因此自动消失。

当前范围选择：

| 路径 | 计划 | 考虑 |
| --- | --- | --- |
| recv | 优先实现 multishot | 长连接持续接收，已有 buffer pool 基础 |
| accept | 已改造，连接建立收益待测 | 主要改善新连接接入，应单独测量 |
| send | 保持当前队列驱动 | 每次发送的数据和长度由响应决定 |
| eventfd poll | 已改为 multishot | 减少跨线程唤醒后的重新提交 |
| recvmsg / 业务 socket poll | 暂不引入 | 当前接收路径没有对应需求 |

### 当前接口

`ServerConnection::ReadLoop()` 创建 `RecvProvidedMultishot()`，重复 `co_await` 同一个 awaitable 等待数据。`IoResult::has_more_` 表示该操作是否仍在活动，成功收到数据时也必须检查；成功的最终 CQE 处理完数据后，连接若仍为 Active，就创建新的接收操作。

退出读循环时，`Close()` 已请求取消，或 `BeginDrain()` 已通过 `shutdown(SHUT_RD)` 停止读取。只要最近的结果仍有 `has_more_`，读协程就继续等待并归还晚到的 buffer，直到最终 CQE 才销毁 awaitable。业务排空不额外取消发送操作。具体流程见 [ReadLoop 实现](../src/server/server_connection.cpp)。

### 生命周期与并发协议

one-shot 路径仍然每收到一个 awaitable CQE 就减少 pending 计数并回收 `Operation`。新增的 multishot 完成分支只有收到不带 `IORING_CQE_F_MORE` 的最终 CQE 才减少计数并回收操作。最终 CQE 交付本次结果并结束这个 awaitable；EOF、错误、`ENOBUFS` 和取消都由调用方作为终止结果处理。

当前协议如下：

1. `RecvProvidedMultishot()` 返回一个可重复等待的 awaitable，首次 `co_await` 才提交；每个 awaitable 最多一个活动接收操作、一个未完成的等待。调用方保证同一个 fd 不存在竞争的接收者。
2. 重复 `co_await`、完成处理、取消和启动后的析构都在所属 I/O 线程执行。检查结果、登记 waiter 和完成交付之间没有跨线程访问，因此不加锁。
3. multishot awaitable 只有一个结果和一个等待者，没有用户态结果队列、容量配置或容量溢出策略。runtime 统一从共享 CQ 取完成事件，根据 `Operation::awaitable_` 找到 awaitable，将结果交给等待协程并同步恢复它。
4. 消费者必须同步处理数据，再次 `co_await` 同一个仍在活动的 awaitable，或在让出执行权前请求取消。runtime 在 `resume()` 返回后才处理下一条 CQE，因此下次交付时等待者已经登记。活动接收期间不能改为等待其他异步操作；实现会检查该协议，违反时终止程序，避免静默丢失数据。尚未分发的完成事件留在内核 CQ 中。
5. `CancelFd(fd)` 只请求取消。读协程通过同一个 awaitable 等待原接收操作的最终 CQE，之后得到 `ECANCELED`；晚到的成功完成仍要提取并归还 buffer。取消请求自己的 CQE 不代表接收操作已经释放。
6. awaitable 直接保存接收状态，`Operation` 只借用 awaitable 指针。awaitable 应保持到最终接收 CQE，不能在活动操作期间移动或销毁；最终完成恢复协程后，runtime 不再访问可能已经被协程销毁的 awaitable。
7. `RequestStop()` 保持原有的“停止接纳并排空”语义，不替调用方取消活动接收。调用方显式 `CancelFd()` 并通过同一个 awaitable 排空，随后销毁 awaitable、关闭 fd；context 继续存活直至剩余 CQE（包括取消请求的完成）排空。
8. `ENOBUFS` 和其他接收错误结束本次请求；底层返回结果，不关闭 socket、不自动重试。服务端对最终 `ENOBUFS` 等待归还进展后提交新请求，其他错误沿用日志和关闭当前连接的策略。

最初实现为支持消费者任意异步挂起，增加了 16 槽结果队列和独立的 `UringReceiver` 类。设计复核后，确认当前 `ReadLoop` 在两次接收等待之间只有同步处理，因此移除队列和独立 receiver 类，复用 `UringAwaitable` 表达持续接收操作。

关闭采用 I/O 排空语义：请求取消并不立即释放资源，必须等完成事件收尾。业务请求是否执行完、响应是否发送完属于服务端的业务排空策略；超时后的进程强制终止交给部署层。

底层测试覆盖多次接收、EOF、池耗尽后的租约复用、取消并排空后析构，以及只请求取消却提前析构的协议检查。服务端已接入 multishot，测试覆盖大请求跨 buffer 接收及累计超过池容量的复用、客户端保持打开时的协议错误关闭，以及排空期间工作线程响应的交付。已完成一轮小规模 one-shot 对照，参数、结果范围和测量限制见 [初始性能记录](multishot-recv-smoke.md)；开启提交、队列和 buffer 统计后的对照见 [可观测指标对照](multishot-recv-observability.md)。

multishot recv 要求内核支持该操作，项目内 liburing 手册标注从 Linux 6.0 开始提供；当前接口不自动回退到 one-shot，不支持时通过接收错误返回。后续继续验证关闭与接收交错的边界，再进行对照测量。

### Buffer 耗尽恢复压力记录（2026-09-13）

本次使用 Release firehose，在本机 12 个在线 CPU 上将服务端固定到 CPU 0--5、客户端固定到 CPU 6--11。测量使用 1 个 Connection I/O loop、2 个服务端 worker、2 个客户端 I/O 线程、8 条 TCP 连接、128 个总在途请求、4 KiB payload、1 秒预热和 3 秒测量；每种配置运行 3 次，以下为中位数。默认 pool 是 512 块 16 KiB buffer（8 MiB），受限 pool 是 4 块 1 KiB buffer（4 KiB）。

| pool | QPS | p99 | 失败请求 | ENOBUFS / 成功请求 |
| --- | ---: | ---: | ---: | ---: |
| 默认 512 x 16 KiB | 22,350 | 11.17 ms | 0 | 0 |
| 受限 4 x 1 KiB | 18,551 | 28.40 ms | 0 | 6.9--7.2 |

受限配置的三次测量都没有零推进连接；每次结束时 active recv、用户态租约和 buffer-return waiter 均归零，且 buffer acquire 与 return 计数相等。因此 `ENOBUFS` 后等待归还进展、重提 multishot recv 的链路能恢复，也没有遗留资源。

但 `buffer_return_waits` 与 `ENOBUFS` 次数相等，`buffer_return_wait_suspensions` 为零。这说明处理 `ENOBUFS` CQE 时，同批 CQE 的 buffer 归还往往已经推进 generation；恢复逻辑正确识别到进展后立即重提，而非挂起。它避免了漏通知和死锁，却在极小 pool 下形成高频“最终 ENOBUFS -- 立即重提”的循环，带来吞吐下降和尾延迟上升。

结论：该机制可作为默认大 pool 下的正确性兜底，不能作为小 pool 或严格内存上限下的压力调节方案。若这类配置需要成为常规部署模式，应单独设计接收准入或限速策略；仅把 generation 等待改得更复杂，不能保证重提时有可用 buffer。

## 第三步：multishot accept 与 eventfd poll

`AcceptMultishot(listen_fd)` 为一个监听 socket 创建持续 accept 请求，所有新连接通过同一 awaitable 逐次交付。成功结果中的 `result_` 是新连接 fd，调用方立即交给 `Socket` 管理。它复用 recv 的同步消费协议：处理本次连接后，继续等待同一 awaitable；成功结果也检查 `has_more_`，最终成功完成后若仍接纳连接，就提交新请求。

停止接纳和释放 accept 请求是两个时刻。`StopAcceptingOnContext()` 设置停止状态、取消并关闭 listener，此时内核 CQ 中仍可能有成功 accept。AcceptLoop 必须领取并关闭这些新 fd，继续等待到不带 `MORE` 的最终 CQE，才能销毁 awaitable。分发连接发生异常时，也先取消并排空 accept，再传播原异常。

这里的 poll 专指 context 内部 eventfd 的 `POLLIN` 监听。每个 context 提交一个 multishot poll；`Post()` 将回调放入加锁队列，再写 eventfd。poll CQE 唤醒 Run 线程，读取 eventfd 计数、处理回调队列。计数和回调不要求一一对应，合并唤醒不会丢回调；回调执行期间新增的 Post 会再次写入 eventfd。

CQE 带 `IORING_CQE_F_MORE` 时，原 poll 仍存在，不重提、不销毁 operation。不带 `MORE` 时才释放 operation，若 context 仍运行就重新布置监听。`Operation::multishot_` 明确标记持续请求，包括没有协程消费者的 wakeup poll；不能仅凭 `awaitable_` 是否非空判断生命周期。

`RequestStop()` 写 eventfd 唤醒 Run 线程。停止路径显式取消仍活动的 poll，且只发送一次取消请求。取消请求自己的 CQE 只确认取消命令完成；原 poll 的最终 CQE 才清除 `wakeup_poll_pending_` 并释放其 operation。Run 同时排空取消命令和原请求，不会因为停止标志已设置就提前返回。

验证重点是一次 accept SQE 交付多个连接并取消排空、连续跨线程 Post 使用同一个 poll，以及服务端停止回归。长连接 recv benchmark 无法证明 accept 的性能收益；连接建立负载及高频 Post 的专项性能测量仍待进行。

当前验证：Debug 完整构建、11 个非 Consul CTest 目标通过。新增测试覆盖一个 accept SQE 接收两条连接后取消、积压连接期间取消并消费晚到 CQE、连续 32 次 Post 仍只提交一个 wakeup poll 后正常停止。没有自动回退到 one-shot；部署内核必须支持这两种 multishot 操作。

## 如何验证收益

每一步保留可比较的提交，只运行与当前问题相关的测试和小规模 benchmark，阶段完成后再扩大验证范围。

- 正确性：同一请求多次完成、buffer 归还、最终 CQE、关闭与取消交错、池耗尽，以及连接间数据隔离。
- 长连接收发：固定连接数、请求大小和并发度，对比吞吐、延迟、CPU 开销及提交次数。
- 内存：分别观察少连接、大量空闲连接和大量活跃连接，区分共享池预分配与实际占用。
- accept 若后续改造，单独用连接建立负载验证。

记录内核、构建配置和 benchmark 参数，结果出来前不填写提升百分比。

## 后续更新

后续补充最终接口、状态转换、关键取舍、测试结果和性能数据。发布前核对代码是否与本文一致，并整理仍属草案的内容。

相关代码与文档：

- [buffer pool 接口](../src/include/io/uring/buffer_pool.h)
- [io_uring context 实现](../src/io/uring/context.cpp)
- [连接读写协程](../src/server/server_connection.cpp)
- [I/O runtime 说明](io-runtime.md)
- [项目内 liburing recv 手册](../third_party/liburing/man/io_uring_prep_recv.3)
