# 测试说明

## 目录

测试目录与生产源码模块对应：

```text
common/      status 和 task 等纯逻辑
io/          io_uring 和 socket 行为
transport/   buffer、executor 和 TCP transport
protocol/    frame 和协议行为
rpc/         client、server 和 naming 行为
test_support/仅供测试使用的辅助代码
```

## 运行测试

构建并运行默认测试集：

```bash
make test
```

默认测试集不包含在线 Consul 测试。常用 CTest 筛选命令：

```bash
ctest --test-dir build/tests --output-on-failure -L unit
ctest --test-dir build/tests --output-on-failure -L runtime
ctest --test-dir build/tests --output-on-failure -L integration -LE external
ctest --test-dir build/tests --output-on-failure -L e2e -LE external
```

在线 Consul 测试要求 `127.0.0.1:8500` 上运行 Consul：

```bash
XRPC_ENABLE_CONSUL_TESTS=1 \
  ctest --test-dir build/tests --output-on-failure -L external
```

## 测试规则

- 优先测试公开行为，不依赖内部实现顺序；
- 长时间运行的异步测试必须确定性关闭并设置超时；
- 仅测试使用的 helper 放在 `tests/test_support/`；
- 生产 API 不暴露只为测试存在的生命周期方法；
- 外部测试必须显式启用，普通本地 CI 不依赖运行中的 Consul。

CTest 使用以下层次标签：`unit`、`runtime`、`integration`、`e2e` 和 `external`。
