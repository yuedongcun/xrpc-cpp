# 测试说明

`tests/` 只保留最少但够用的测试集：默认测试覆盖核心 contract、协议边界、真实 TCP / io_uring 主路径、RpcClient 调用链路和服务端生命周期；`external` 只保留一个真实 Consul 联调。

## 保留范围

默认测试：

- `common/task_test.cpp`
- `io/uring_awaitable_test.cpp`
- `protocol/protocol_test.cpp`
- `server/server_connection_io_uring_test.cpp`
- `server/rpc_frame_stream_test.cpp`
- `server/rpc_server_lifecycle_test.cpp`
- `client/endpoint_selector_test.cpp`
- `client/rpc_client_endpoint_test.cpp`
- `client/rpc_client_thread_safety_test.cpp`

外部测试：

- `naming/consul_resolver_integration_test.cpp`

`tests/package_consumer/` 保留为安装后 `find_package(xrpc CONFIG REQUIRED)` 的消费验证，不算入默认测试集。

## 运行方式

默认测试：

```bash
ctest --test-dir build/tests --output-on-failure -LE external
```

外部 Consul 测试：

```bash
XRPC_ENABLE_CONSUL_TESTS=1 \
  ctest --test-dir build/tests --output-on-failure -L external
```

## 原则

- 测公开 contract，不测内部实现细节；
- 上层测试覆盖主链路，下层测试只保留真正独立的边界；
- 异步和网络测试用明确事件同步，不依赖固定 sleep；
- 测试名直接表达行为，例如 `RejectsOversizedPayload`。
