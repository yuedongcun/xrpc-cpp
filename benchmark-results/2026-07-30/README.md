# 性能测试结果 — 2026-07-30

本目录包含 XRPC 公开性能摘要对应的脱敏审计产物。每个 suite 保留最终解析的 `config.json`、逐轮 `runs.csv` 和生成的 `summary.md`。

## 测试环境

- CPU：Intel Core i7-9750H，6 个物理核 / 12 个逻辑 CPU
- 操作系统：WSL2、Linux 6.6.87.2、x86-64
- 编译器：Clang 20.1.8
- 服务端亲和性：逻辑 CPU `0,2,4`
- 客户端亲和性：逻辑 CPU `6,8,10`
- 服务端：3 个 worker 线程和 3 个 connection I/O loops
- Payload：128 字节 Echo message 字段
- 传输：同机 Linux loopback

这些结果采集自最终预发布实现，当时 benchmark 修改仍位于私有工作树中。随后，结果对应的代码、配置和摘要在生成公开快照前一起提交。公开产物有意省略私有 commit id、hostname、完整内核 CPU 漏洞输出、原始日志和 perf 二进制。

## 包含的测试组

| 目录 | 用途 |
| --- | --- |
| `protobuf-capacity-formal-20260730` | 类型化服务端容量曲线 |
| `protobuf-steady-state-formal-20260730` | 低延迟点与饱和点稳态测试 |
| `protobuf-ceiling-verify-interleaved-20260730` | 长时间无拒绝上限验证 |
| `protobuf-overload-probe-20260730` | 背压边界探针 |
| `rpc-client-formal-v2-20260730` | 生产 `RpcClient` 扩展能力 |
| `raw-high-load-formal-v2-20260730` | 传输层与运行时上界 |

摘要中仍会引用 `logs/`，因为 benchmark runner 通常会生成日志。公开快照不包含日志，保留的结构化 CSV 是公开聚合结果的数据来源。
