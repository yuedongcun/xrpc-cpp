# 服务端性能优化历程

本文收敛塑造当前服务端运行时的主要优化实验。历史 QPS 使用不断变化的 raw-only workload，只用于支持单项工程决策，不能替代当前公开容量结论。最终分层口径以 `current_benchmark_results.md` 为准。

## 测量纪律

实验使用独立服务端进程、独立发压器进程、CPU 亲和性、预热、多轮重复、精确响应校验和 `perf` profile。只有重复结果超过正常波动范围，且不破坏低负载矩阵时，改动才会保留。

吞吐和尾延迟必须一起判断。如果更高 QPS 只来自排队加深、p99 增大或越过背压边界，就不能视为更好的工作点。

## 保留的优化

### 批量排空写队列

把已排队响应帧合并成有上限的 write，减少小粒度 send。再结合把读缓冲区从 4 KiB 提高到 16 KiB，早期 raw benchmark 中位数从约 228k 提高到 276k QPS，提升 21%。

### 将请求所有权移动给处理器

让 worker 路径按值消费请求，消除一次可避免的 payload 拷贝。同口径 profile run 的 QPS 提升约 9%，高负载点 p99 降低约 13%。

### 工作任务批处理

把一批已解码请求一次提交给 worker pool，在不把 handler 移到 I/O 线程的前提下，摊薄 queue、mutex、condition variable 和类型擦除开销。该 raw workload 的重复高负载中位数提升约 31%，p99 基本不变。

### 响应帧批处理

每个 worker batch 返回一个 response frame batch，减少 completion queue 节点和写队列操作。重复 raw 高负载结果提升约 10%，p99 小幅改善；profile 中 completion submit 和 write drain 成本按预期下降。

## 放弃的优化

一些看似合理的微优化反而降低了性能：

- 更大的 move-only inline task 对象让吞吐下降约 7%。
- 请求头缓存引入 shared ownership 和 view 生命周期复杂度，却没有稳定收益。
- 等整个 worker batch 完成后再统一提交 response，降低了响应可见性并恶化 p99。
- 当读写路径不再是主要成本后，继续扩大 batch 只会增加内存或排队延迟。
- 用纯 round-robin 取代短队列优先的 worker 选择会破坏负载均衡。
- 专用 response fast path 增加了分支，却没有消除主要分配和组帧成本。

最后一轮连续尝试了十个方案，没有保留任何一个。这是有价值的停止信号：profile 和重复测量表明，继续添加局部捷径只会用代码清晰度和尾延迟交换噪声。

## 最终设计原则

1. 保持 connection I/O 单一所有者，不在 I/O loop 中执行 handler。
2. 只在已有所有权边界上批处理，不跨越响应可见性边界。
3. 不为了未经证明的一次拷贝引入 shared ownership 或 borrowed view。
4. 每次结构优化后重新 profile，因为主要热点会转移。
5. 分离服务端容量、生产客户端和 raw runtime benchmark。
6. 发布结果必须同时给出 workload、CPU 亲和性、重复次数、失败数和 p99，不能只给脱离上下文的峰值 QPS。
