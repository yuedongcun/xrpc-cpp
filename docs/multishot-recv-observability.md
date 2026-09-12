# Multishot recv：开启统计后的性能与压力对照

测量代码：`9105a0f`。one-shot 对照仅将当前 `ReadLoop()` 替换为 `9a8e891` 的 one-shot 读循环，其余源代码、库对象、写侧统计及客户端二进制保持一致。

环境：WSL2 / Linux 6.6.87.2，Clang 20，Release（`-O3 -DNDEBUG`），TCP loopback。服务端固定 CPU 0–5、客户端固定 CPU 6–11。服务端 3 个 Connection I/O 线程、3 个 Worker；客户端 3 个 I/O 线程。

负载：12 条连接，128 字节 Echo payload，在途 RPC 总数 96 / 768。每次启动新服务端，预热 2 秒、测量 10 秒；每个工作点每版本重复 3 次。相邻两版本成对运行，第二轮反转版本先后及工作点顺序。所有测量均开启统计采集。

## 吞吐、延迟及 CPU

表中为逐轮指标中位数，括号为三轮最小值–最大值。CPU 是前后快照后采样的服务端累计 CPU 时间差，再除以本轮成功 RPC 数；包含窗口内连接生命周期及采集开销。

| 在途 RPC | 版本 | QPS | p99 µs | CPU µs / 成功 RPC | 失败 RPC |
| --- | --- | --- | --- | --- | --- |
| 96 | oneshot | 170,279 (168,953–197,471) | 1,022 (845–1,026) | 12.83 (12.05–13.46) | 0 |
| 96 | multishot | 179,565 (168,539–186,150) | 952 (888–1,060) | 13.14 (11.83–13.23) | 0 |
| 768 | oneshot | 307,304 (305,712–337,690) | 6,115 (5,302–9,098) | 11.08 (10.73–11.44) | 0 |
| 768 | multishot | 333,332 (318,016–340,618) | 5,234 (4,926–5,515) | 10.88 (10.70–10.91) | 0 |

## 提交与接收

下表对每轮计数先按成功 RPC 数归一化，再取三轮中位数。`submit_calls` 是 liburing 调用数，包含重试，不是系统调用数。recv SQE 是 prepared 口径，submitted SQE 是提交成功口径。

| 在途 RPC | 版本 | recv SQE / 千 RPC | submit 调用 / 千 RPC | submitted SQE / 千 RPC | SQE / submit 调用 | CQE / recv SQE |
| --- | --- | --- | --- | --- | --- | --- |
| 96 | oneshot | 125.0070 | 114.38 | 335.90 | 2.94 | 1.0 |
| 96 | multishot | 0.0067 | 82.40 | 207.40 | 2.52 | 18,706.9 |
| 768 | oneshot | 60.7056 | 44.09 | 153.46 | 3.43 | 1.0 |
| 768 | multishot | 0.0036 | 30.91 | 84.88 | 2.75 | 17,878.8 |

## 队列与缓冲池压力

每个峰值列报告三轮中任何单个 loop / worker 观测到的最大值，各 loop / worker 的峰值没有相加。CQ 列为处理批次前和快照时的采样峰值，可能漏掉批次中的瞬时峰值。租约列仅计用户态已领取的 buffer。

| 在途 RPC | 版本 | staged 峰值 | CQ 采样峰值 | buffer 租约峰值 | 待发送字节峰值 | worker 排队 batch 峰值 | worker 排队 RPC 峰值 | ENOBUFS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 96 | oneshot | 9 | 7 | 1 | 4,960 | 4 | 32 | 0 |
| 96 | multishot | 5 | 6 | 1 | 4,960 | 4 | 32 | 0 |
| 768 | oneshot | 9 | 9 | 1 | 38,130 | 15 | 269 | 0 |
| 768 | multishot | 5 | 13 | 1 | 39,370 | 16 | 290 | 0 |

## 结果解释

所有 12 组测量无失败 RPC、无 `ENOBUFS`。每组 multishot 在测量窗口只准备 12 个 recv SQE，对应 12 条连接；one-shot 每次完成后重新准备接收。所有组的 prepared SQE 分类总和与 submitted SQE 数一致，buffer 借用/归还数平衡，结束快照中活动接收与未归还租约均为零。

按每千次成功 RPC 归一化，两个工作点的 submit 调用中位数分别减少约 28% / 30%，成功提交 SQE 数分别减少约 38% / 45%。这确认了提交工作量减少；未测实际系统调用数。

QPS 中位数分别高约 5.5% / 8.5%，但三轮范围重叠。CPU/RPC 在低并发略高、高并发略低，未表现出一致改善。不能据此确认稳定吞吐或 CPU 收益。

用户态 buffer 租约峰值均为 1，与读循环同步消费并立即归还的方式一致；它不代表内核只同时选用了一个 buffer。较高并发下，两种版本都有 worker 排队，multishot 的 CQ 采样峰值和 worker 排队峰值并未一起下降。降低提交工作并不意味着所有队列积压都会降低。

## 测量口径和限制

窗口从预热客户端退出后的起始快照到测量客户端退出后的结束快照，包含测量连接建立、关闭及边界采集开销。Connection I/O loops 和 Worker 队列独立采样，不是全局原子快照。accept loop 不计入 I/O 汇总。

不能从 recv SQE 降幅直接推断提交调用或系统调用同幅下降；发送与 worker 完成投递产生的 wakeup SQE 仍会参与提交。不能用容量减用户态租约数推算内核可用 buffer 数。

只有三轮、每轮十秒，且运行于共享 WSL2 环境；吞吐和延迟结果属于初步对照，不能作为稳定收益承诺。未追踪 io_uring_enter 系统调用数。

本地原始结果、配置、环境信息、对照补丁、构建与交替测量脚本保存在 `.local-perf/multishot-observability/`。该目录不纳入版本控制。

```bash
cmake --build build-release --parallel 4 --target xrpc_benchmark_server xrpc_benchmark_firehose
python3 .local-perf/multishot-observability/build_oneshot.py
python3 .local-perf/multishot-observability/compare.py
python3 .local-perf/multishot-observability/summarize.py
```
