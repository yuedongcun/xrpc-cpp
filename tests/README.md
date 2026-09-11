# 测试说明

按改动所在模块查看测试即可；默认套件覆盖协议、异步 I/O、客户端与服务端主路径，Consul 联调单独运行。

## 保留范围

| 改动内容 | 主要测试 | 核心行为 |
| --- | --- | --- |
| 协程基础 | `common/task_test.cpp` | 返回值、等待、异常传播 |
| io_uring / buffer pool | `io/uring_awaitable_test.cpp` | 收发、停止、operation 生命周期、buffer 耗尽与归还复用 |
| 线协议 | `protocol/protocol_test.cpp` | 编解码、所有截断位置、长度和非法数据 |
| 接收数据拼帧 | `server/rpc_frame_stream_test.cpp` | 跨次输入保留半包、多帧解析、错误关闭 |
| 连接读写 | `server/server_connection_io_uring_test.cpp` | pipeline、异步响应、背压、关闭 |
| 服务端 API | `server/rpc_server_lifecycle_test.cpp` | 启停、优雅排空、注册时机、配置校验 |
| 客户端 | `client/rpc_client_*_test.cpp` | 路由、切换、连接复用、并发调用 |

外部测试：

- `naming/consul_discovery_integration_test.cpp`

`tests/package_consumer/` 保留为安装后 `find_package(xrpc CONFIG REQUIRED)` 的消费验证，不算入默认测试集。

## 运行方式

日常只编译、运行相关测试，例如 buffer pool：

```bash
cmake --build build --parallel --target io_uring_awaitable_test
ctest --test-dir build/tests --output-on-failure -R '^io_uring_awaitable_test$'
```

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
- 同类参数使用表驱动并附场景标签；删除已被完整覆盖的重复案例；
- 保留取消、归还、背压和关闭的时序测试，它们验证不同的生命周期风险；
- 异步和网络测试用明确事件同步，不依赖固定 sleep；
- 测试名直接表达行为，例如 `RejectsOversizedPayload`。
