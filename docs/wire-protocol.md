# xRPC 线协议

xRPC 在 TCP 字节流之上定义请求—响应帧，用固定前缀划分消息边界，用 metadata 表达 RPC 语义，并用 payload 承载用户 Protobuf 消息。

## 一次 RPC 在网络上如何表示

一次调用发送一个 Request frame，并接收一个具有相同 `request_id` 的 Response frame。两种 frame 使用相同的物理布局：

```text
┌────────────────────────┬────────────────────────┬────────────────────────┐
│ FrameHeader            │ Metadata bytes         │ Payload bytes          │
│ 24 bytes               │ metadata_size bytes    │ payload_size bytes     │
└────────────────────────┴────────────────────────┴────────────────────────┘
```

Request 和 Response 的区别是 metadata 与 payload 所表达的内容：

| 内容 | Request frame | Response frame |
| --- | --- | --- |
| `message_type` | `Request` | `Response` |
| Metadata 类型 | `RequestMetadata` | `ResponseMetadata` |
| Metadata 字段 | `service_name`, `method_name` | `error_code`, `error_text` |
| Payload | 序列化的用户请求 | 序列化的用户响应 |

Request frame 的编码过程如下：

```text
RequestEnvelope
├─ request_id
├─ service_name
├─ method_name
└─ payload                  已序列化的用户请求
        │
        ▼ FrameCodec::Encode()
        │
        ├─ FrameHeader
        │    ├─ magic             = XRPC
        │    ├─ version           = 1
        │    ├─ message_type      = Request
        │    ├─ flags             = 0
        │    ├─ metadata_size     = Metadata bytes 长度
        │    ├─ payload_size      = Payload bytes 长度
        │    └─ request_id        = RequestEnvelope.request_id
        │
        ├─ Metadata bytes
        │    └─ RequestMetadata::SerializeToString()
        │         ├─ service_name = RequestEnvelope.service_name
        │         └─ method_name  = RequestEnvelope.method_name
        │
        └─ Payload bytes
             └─ RequestEnvelope.payload，不做再次序列化

        三个区段按顺序拼接
        ▼
┌────────────────────────┬────────────────────────┬────────────────────────┐
│ FrameHeader            │ Metadata bytes         │ Payload bytes          │
└────────────────────────┴────────────────────────┴────────────────────────┘
                           Request frame
```

解码沿相反方向还原相同字段：

```text
                           Request frame
┌────────────────────────┬────────────────────────┬────────────────────────┐
│ FrameHeader            │ Metadata bytes         │ Payload bytes          │
└────────────────────────┴────────────────────────┴────────────────────────┘
        │
        ▼ FrameCodec::Decode()
        │
        ├─ FrameHeader
        │    ├─ 验证 magic、version 和 message_type
        │    ├─ 用 metadata_size 和 payload_size 确定区段边界
        │    └─ 取出 request_id
        │
        ├─ Metadata bytes
        │    └─ RequestMetadata::ParseFromArray()
        │         ├─ 取出 service_name
        │         └─ 取出 method_name
        │
        └─ Payload bytes
             └─ 复制字节，不解析用户 Protobuf 类型

        三部分合并为
        ▼
RequestEnvelope
├─ request_id            来自 FrameHeader
├─ service_name          来自 RequestMetadata
├─ method_name           来自 RequestMetadata
└─ payload               来自 Payload bytes
```

`RequestEnvelope` 是进程内的协议对象，不是额外的 wire section。Response 使用相同过程，只是改用 `ResponseEnvelope` 和 `ResponseMetadata`。

一条 TCP 连接可以同时承载多个未完成调用。Worker 完成请求的先后顺序不固定，因此 Response frame 不保证与 Request frame 按相同顺序返回；客户端依靠 `request_id` 将每个响应交给对应调用。

## FrameHeader

每个 frame 都以 24 字节固定前缀开始。多字节整数使用网络字节序：

```text
0               4               5               6               8                 12                16                24
┌───────────────┬───────────────┬───────────────┬───────────────┬─────────────────┬─────────────────┬─────────────────┐
│ magic         │ version       │ message_type  │ flags         │ metadata_size   │ payload_size    │ request_id      │
└───────────────┴───────────────┴───────────────┴───────────────┴─────────────────┴─────────────────┴─────────────────┘
     4 bytes         1 byte          1 byte          2 bytes          4 bytes           4 bytes           8 bytes
```

| 字段 | 当前语义 |
| --- | --- |
| `magic` | 固定为 ASCII `XRPC`，用于识别 xRPC frame |
| `version` | 当前版本为 `1` |
| `message_type` | `Request = 1`，`Response = 2` |
| `flags` | 编码时固定为 `0`，当前没有定义行为 |
| `metadata_size` | 紧随固定前缀的 metadata 字节数 |
| `payload_size` | metadata 之后的用户 payload 字节数 |
| `request_id` | 由客户端分配，服务端复制到对应响应 |

长度字段使接收端能够从 TCP 字节流中确定完整 frame 的边界：

```text
frame size = 24 + metadata_size + payload_size
```

## Metadata 与 payload

Request metadata 描述服务端路由目标：

```proto
message RequestMetadata {
  string service_name = 1;
  string method_name = 2;
}
```

Response metadata 描述 RPC 执行结果：

```proto
message ResponseMetadata {
  int32 error_code = 1;
  string error_text = 2;
}
```

状态为 `Ok` 且没有错误文本时，`ResponseMetadata` 全部采用 Protobuf 默认值，其编码结果为空，因此 response 的 `metadata_size` 可以为 `0`。解码到未定义的状态码时，客户端将其转换为 `DataLoss`，不会把未知整数暴露为 `StatusCode`。

payload 是已经序列化的用户请求或响应。协议层把它视为不透明字节，不知道具体的 Protobuf message 类型。因此一个 frame 中可能同时存在两层 Protobuf 编码：

| 数据 | 定义者 | 用途 |
| --- | --- | --- |
| `RequestMetadata` / `ResponseMetadata` | xRPC | 服务、方法和 RPC 状态 |
| request / response payload | 应用 | 用户业务消息 |

## FrameCodec 的边界

`FrameCodec` 在 Envelope 与完整 frame bytes 之间转换。它负责固定 Header 和 xRPC Metadata，但始终把用户 payload 当作不透明字节，不依赖具体的业务消息类型。

用户 Protobuf 消息的序列化和解析位于协议层之外：客户端调用代码处理用户 Request 和 Response，服务端注册方法的类型适配器完成对应转换。

## TCP 字节流

TCP 不保留 frame 边界：一次 `recv` 可能只得到部分 frame，也可能同时得到多个 frame。服务端通过 `RpcFrameStream` 缓冲收到的字节，并反复调用 `FrameCodec::Decode()`：

```text
recv bytes
  ↓
RpcFrameStream buffer
  ↓
FrameCodec::Decode()
  ├─ NeedMoreData    → 保留现有字节，等待下一次 recv
  ├─ Ok              → 消费一个 frame，继续解码
  └─ protocol error  → 关闭当前连接
```

因此一次 `FeedBytes()` 可以解出零个、一个或多个 `RequestEnvelope`。

## 限制与失败语义

- Metadata 最大 64 KiB。
- Payload 默认最大 4 MiB，可通过 client/server options 调整。
- 长度在内容分配和 Protobuf 解析前检查。
- `NeedMoreData` 表示 frame 尚未接收完整，不是协议错误。
- 其他非法 frame 会终止当前连接；xRPC 不尝试跳过错误数据或重新同步字节流。

## 协议边界

当前协议版本为 `1`，每次调用只包含一个 Request frame 和一个 Response frame，不支持 streaming、heartbeat 或协议协商。

xRPC 使用 Protobuf 序列化 metadata 和用户 payload，但 TCP 帧格式、请求匹配和错误语义均由项目自行定义，因此不能与 gRPC 直接通信。

当前只保证本仓库同版本客户端与服务端互通。其他语言可以按本文实现协议，但项目暂不提供跨语言或跨版本兼容性保证。
