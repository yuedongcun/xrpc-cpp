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

池耗尽返回 `ENOBUFS` 时，目前采用简单策略：通过 `LogError()` 同步向 stderr 输出连接 ID、fd、buffer group、errno，关闭当前连接。暂不引入资源等待队列或自动重试。正常 EOF 和取消不记录错误。同步日志在高频错误下可能影响 I/O 线程，后续根据实际需要演进。

这一阶段主要改变内存归属，并为 multishot 做准备。每次 recv 的 `Operation` 分配、提交以及 `FeedBytes()` 复制仍然存在，尚无 benchmark 证明独立性能收益。连接数较少时，预分配共享池甚至可能比原方案占用更多内存。

验证记录：构建、8 个默认测试和修改文件的 clang-tidy 已通过；新增单 buffer 测试覆盖持有租约时耗尽、数据未被覆盖、归还后复用及 EOF。

## 第二步：multishot recv（底层已实现，服务端待接入）

目标是通过一次提交持续获得多次接收完成，减少重复提交和操作对象创建。每份接收结果仍需要处理 CQE，输入复制也不会因此自动消失。

当前范围选择：

| 路径 | 计划 | 考虑 |
| --- | --- | --- |
| recv | 优先实现 multishot | 长连接持续接收，已有 buffer pool 基础 |
| accept | 后续独立评估 | 主要改善新连接接入，应单独测量 |
| send | 保持当前队列驱动 | 每次发送的数据和长度由响应决定 |
| recvmsg / 显式 poll | 暂不引入 | 当前接收路径没有对应需求 |

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
8. `ENOBUFS` 和其他接收错误均为终止结果，不自动重试。底层返回错误，不关闭 socket、不打日志；后续服务端接入时沿用日志和关闭当前连接的策略。

最初实现为支持消费者任意异步挂起，增加了 16 槽结果队列和独立的 `UringReceiver` 类。设计复核后，确认当前 `ReadLoop` 在两次接收等待之间只有同步处理，因此移除队列和独立 receiver 类，复用 `UringAwaitable` 表达持续接收操作。

关闭采用 I/O 排空语义：请求取消并不立即释放资源，必须等完成事件收尾。业务请求是否执行完、响应是否发送完属于服务端的业务排空策略；超时后的进程强制终止交给部署层。

底层测试覆盖多次接收、EOF、池耗尽后的租约复用、取消并排空后析构，以及只请求取消却提前析构的协议检查。服务端已接入 multishot，测试覆盖大请求跨 buffer 接收及累计超过池容量的复用、客户端保持打开时的协议错误关闭，以及排空期间工作线程响应的交付。已完成一轮小规模 one-shot 对照，参数、结果范围和测量限制见 [性能记录](multishot-recv-smoke.md)。

multishot recv 要求内核支持该操作，项目内 liburing 手册标注从 Linux 6.0 开始提供；当前接口不自动回退到 one-shot，不支持时通过接收错误返回。后续继续验证关闭与接收交错的边界，再进行对照测量。

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
