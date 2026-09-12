# Multishot recv 小规模性能对照

测量代码：`dc64f78`。one-shot 对照使用相同代码和库对象，仅将 `ReadLoop()` 替换为 `9a8e891` 的 one-shot 实现；客户端二进制相同。

环境：WSL2，Linux 6.6.87.2，Clang 20，Release（`-O3 -DNDEBUG`），TCP loopback。服务端固定 CPU 0–5，客户端固定 CPU 6–11。服务端 3 个 Connection I/O 线程、3 个 Worker 线程，客户端 3 个 I/O 线程。

负载：12 条连接、128 字节 payload，全局在途请求数 96 和 768；每次启动新服务端，预热 1 秒、测量 5 秒，每个工作点各重复 3 次。两版本按轮次交替先后顺序。

| 在途请求 | 版本 | QPS 中位数（范围） | p99 µs 中位数（范围） | 服务端 CPU 秒中位数 | 失败请求 |
| --- | --- | --- | --- | --- | --- |
| 96 | oneshot | 175,698 (175,622–194,124) | 922 (833–925) | 13.42 | 0 |
| 96 | multishot | 187,919 (178,938–197,548) | 855 (798–896) | 13.27 | 0 |
| 768 | oneshot | 345,657 (322,954–388,145) | 6,510 (5,668–8,075) | 21.72 | 0 |
| 768 | multishot | 350,963 (347,884–385,189) | 5,136 (4,850–5,315) | 21.83 | 0 |

CPU 秒是服务端各线程累计 CPU 时间，包含预热和测量阶段，采样于进程停止前；不是单独测量阶段的 CPU 使用率，也没有按成功请求数归一化。

这轮用于确认两种接收路径能在相同负载下工作并取得初步数据。测量时间短、样本少，QPS 范围存在重叠；不足以确认稳定的吞吐提升。未测 SQE 数量、io_uring_enter 次数或其他系统调用次数。后续应延长预热和测量时间、增加交替轮次，并单独验证提交开销。

本地复现配置、对照补丁、环境记录、脚本与原始结果位于 `.local-perf/multishot-smoke/`，该目录不纳入版本控制。运行流程：

```bash
cmake --build build-release --parallel 4 --target xrpc_benchmark_server xrpc_benchmark_firehose
python3 .local-perf/multishot-smoke/build_oneshot.py
python3 .local-perf/multishot-smoke/compare.py
python3 .local-perf/multishot-smoke/summarize.py
```

`build_oneshot.py` 使用临时 worktree `/tmp/xrpc-multishot-oneshot` 中的 one-shot 读循环；对应差异保存在 `oneshot-readloop.patch`。
