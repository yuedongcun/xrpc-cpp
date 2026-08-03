# XRPC v1 Prometheus 调试查询

这些查询用于本地高可用和调试，不是生产仪表盘规范。性能测试基线默认不启用指标；只有显式开启 Prometheus 指标导出器时才有数据。

## 客户端

客户端 QPS：

```promql
sum(rate(xrpc_client_rpc_completed_total[1m]))
```

客户端失败 QPS：

```promql
sum(rate(xrpc_client_rpc_failed_total[1m]))
```

客户端 p99 延迟：

```promql
histogram_quantile(
  0.99,
  sum(rate(xrpc_client_rpc_latency_seconds_bucket[1m])) by (le)
)
```

客户端故障转移尝试：

```promql
sum(rate(xrpc_client_rpc_failover_attempt_total[1m])) by (status_code, commit_state)
```

被安全规则阻止的客户端故障转移：

```promql
sum(rate(xrpc_client_rpc_failover_blocked_total[1m])) by (status_code, commit_state)
```

解析器端点数量：

```promql
xrpc_client_resolver_endpoints
```

解析器刷新失败：

```promql
sum(rate(xrpc_client_resolver_refresh_failed_total[1m])) by (service, resolver_type, status_code)
```

## 服务端

服务端 QPS：

```promql
sum(rate(xrpc_server_rpc_completed_total[1m]))
```

服务端失败 QPS：

```promql
sum(rate(xrpc_server_rpc_failed_total[1m]))
```

服务端处理器 p99 延迟：

```promql
histogram_quantile(
  0.99,
  sum(rate(xrpc_server_rpc_latency_seconds_bucket[1m])) by (le)
)
```

按方法统计服务端在途请求：

```promql
sum(xrpc_server_rpc_inflight) by (service, method)
```

背压拒绝速率：

```promql
sum(rate(xrpc_server_backpressure_rejected_total[1m])) by (reason)
```

连接关闭速率：

```promql
sum(rate(xrpc_server_connection_closed_total[1m])) by (reason)
```

工作任务速率：

```promql
sum(rate(xrpc_server_worker_jobs_total[1m])) by (state)
```

待处理工作任务数：

```promql
xrpc_server_worker_pending_jobs
```

服务端排空状态：

```promql
xrpc_server_draining
```

## 高可用冒烟检查

服务端 A 和 B 都处理过流量：

```promql
sum by (instance) (xrpc_server_rpc_completed_total{service="BenchmarkService",method="Echo"})
```

客户端观测到解析器端点变化：

```promql
xrpc_client_resolver_endpoints{service=~"xrpc-.*"}
```

故障转移保持在安全边界内：

```promql
sum by (commit_state) (xrpc_client_rpc_failover_attempt_total)
```

非预期的故障转移阻止：

```promql
sum by (commit_state) (xrpc_client_rpc_failover_blocked_total)
```

## 解读规则

- `xrpc_server_backpressure_rejected_total > 0` 表示这是过载或失败测试，不是干净的性能 baseline。
- `xrpc_client_rpc_failover_blocked_total > 0` 通常表示请求可能已经发出，因此自动切换端点的重试被有意阻止。
- `xrpc_client_resolver_endpoints == 0` 时，客户端应该快速返回 `Unavailable`，不能静默使用旧端点。
- `xrpc_server_worker_pending_jobs` 持续增长而 QPS 不再扩展，通常指向 worker 饱和或慢 handler。
- 解读客户端失败前，应把 `xrpc_server_connection_closed_total{reason="backpressure"}` 与背压计数器关联分析。
