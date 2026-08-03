# XRPC 可观测性环境

本目录提供 XRPC 高可用和调试测试使用的本地可观测性依赖。

## 服务

- Consul：服务发现和服务端注册测试依赖。
- Prometheus：指标抓取与查询界面。

Consul 以开发模式启动，在 `127.0.0.1:8500` 暴露 HTTP，与 XRPC 当前默认 `consul_address_` 一致。Prometheus 界面位于 `127.0.0.1:9090`。

常用 XRPC 调试查询见 `prometheus/xrpc_v1_debug_queries.md`。

## 启动

```bash
cd tools/observability
docker compose up -d
```

## 检查

```bash
curl --noproxy '*' http://127.0.0.1:8500/v1/status/leader
curl --noproxy '*' http://127.0.0.1:9090/-/ready
```

浏览器入口：

- Consul：`http://127.0.0.1:8500`
- Prometheus：`http://127.0.0.1:9090`

## 停止

```bash
cd tools/observability
docker compose down
```

同时删除 Prometheus 本地数据：

```bash
cd tools/observability
docker compose down -v
```

## XRPC 指标目标

Prometheus 配置会抓取以下本地 XRPC 调试目标：

- `host.docker.internal:19100`：HA/调试测试中的 benchmark 客户端指标。
- `host.docker.internal:19101`：benchmark 服务端 A。
- `host.docker.internal:19102`：benchmark 服务端 B。

显式启用指标并启动性能测试服务端：

```bash
./build/tools/benchmark/xrpc_benchmark_server \
  --host=127.0.0.1 \
  --port=9010 \
  --metrics_host=0.0.0.0 \
  --metrics_port=19101
```

如果 XRPC 运行在 WSL 主机上，而 Prometheus 运行在 Docker 中，XRPC 指标端点必须绑定到 Docker 可以访问的地址。如果主机命名空间中只绑定 `127.0.0.1`，Prometheus 容器可能无法访问。建议本地开发时把调试指标端点绑定到 `0.0.0.0`，同时使用仅允许本机访问的防火墙，并且只用于开发环境。

## 高可用冒烟测试

构建性能测试工具并启动可观测性环境，然后运行：

```bash
./tools/ha/consul_failover_smoke.py
```

只校验参数并打印计划，不启动依赖 Consul 的进程：

```bash
./tools/ha/consul_failover_smoke.py --dry-run
```

冒烟测试依次启动服务端 A、基于 Consul 的客户端和服务端 B，然后停止服务端 A，并检查：

- Consul 健康实例数从 1 变为 2，再变回 1；
- 客户端调用在有界失败范围内继续进行；
- 客户端指标记录成功 RPC；
- 服务端 A 和 B 的指标都记录成功 RPC。

脚本默认每次生成唯一的 Consul 服务发现名称，RPC 方法保持为 `BenchmarkService/Echo`。

## Consul 说明

该 compose 环境取代“系统已经在 `127.0.0.1:8500` 运行 Consul”的隐式假设。

执行 Consul 在线集成测试前运行：

```bash
cd tools/observability
docker compose up -d consul
XRPC_ENABLE_CONSUL_TESTS=1 ctest --test-dir ../../build/tests --output-on-failure -R consul
```
