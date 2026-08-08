# 性能说明

本文记录 benchmark 要回答的问题、代表性结果和复现命令。所有数字都来自
单机测量，不是生产容量承诺。

## 测量内容

### 生产 `RpcClient`

`client.json` 调用公开的类型化 `RpcClient::Call()` API，包含客户端
Protobuf 编码、端点选择、连接复用、请求/响应匹配、解码和唤醒同步调用者。
它最接近真实客户端路径，但不能代表服务端最大吞吐。

### 类型化 Firehose

类型化 Firehose 发送预序列化的 Echo request frame，并校验完整响应。服务端
仍然执行分帧、方法查找、Protobuf 解析、handler 分发、响应序列化和组帧。
它用于隔离服务端容量和生产客户端开销。

## 测试环境与口径

公开结果采集于：

- Intel Core i7-9750H，6 个物理核、12 个逻辑 CPU；
- WSL2 Linux loopback；
- Clang 20.1.8；
- 3 个服务端 worker 线程和 3 个 connection I/O loops；
- 128 字节 Echo message 字段；
- 每个 case 使用全新服务端，正式测量前预热，重复 3 轮；
- 记录 QPS 中位数、p99 和失败数。

benchmark 不做绑核、控频、NUMA 调优或 perf 编排。换机器后应重新生成结果，
不能期待得到完全相同的数字。

## 代表性结果

`firehose.json` 的代表性服务端工作点：

| 工作点 | QPS 中位数 | p99 中位数 | 失败数 |
| --- | ---: | ---: | ---: |
| 低延迟，48 in-flight | 96,103 | 0.86 ms | 0 |
| 饱和点，1536 in-flight | 414,743 | 8.08 ms | 0 |

`client.json` 在 24 个调用线程下达到 40,930 QPS，p99 为 0.96 ms，
失败数为 0。

## 性能工程取舍

最终保留的优化都沿着已有的 ownership 边界展开：

- 有界 write batching 减少小粒度 socket write；
- 请求所有权移动到 worker job，避免 payload 拷贝；
- worker batching 摊薄队列和同步开销；
- response batching 减少 completion queue 和 write queue 操作。

以下方案因为增加复杂度或恶化 p99 而放弃：

- 更大的 inline task；
- 通过 shared ownership 和 borrowed view 实现请求 header cache；
- 等待整个 worker batch 完成后再提交所有响应；
- 不受约束或过大的 batching。

判断优化时必须同时看 QPS、p99、失败数、背压和多轮稳定性。单纯通过加深队列
得到更高 QPS，不自动意味着工作点更好。

Firehose 发压器使用少量 `epoll` I/O loops，而不是每条连接创建两个线程，避免
高连接数实验中发压端先成为瓶颈。它是 benchmark 专用实现，不是生产
`RpcClient` 的连接模型。

## 复现

先构建全部开发目标：

```bash
make
```

快速验证 benchmark 链路：

```bash
./tools/benchmark/runner/run_suite.py \
  --config tools/benchmark/configs/firehose.json \
  --build
```

复现代表性公开结果：

```bash
./tools/benchmark/runner/run_suite.py \
  --config tools/benchmark/configs/firehose.json \
  --build

./tools/benchmark/runner/run_suite.py \
  --config tools/benchmark/configs/client.json \
  --build
```

运行器会在 stdout 打印每轮结果和最终 summary；需要保留原始输出时，
可自行重定向到本地文件。

`--build` 路径使用当前本地 CMake 配置。若编译器或 CPU 布局不同，应把结果
作为新的测量保存，而不是把它当作参考数字的逐字复现。

## 限制

- 结果是单机 loopback 测量；
- workload 是 Echo，不代表真实业务流量；
- CPU 调度、频率、温度、内核和后台负载都会影响结果；
- 项目不承诺生产 SLA，也不兼容 gRPC 线协议。
