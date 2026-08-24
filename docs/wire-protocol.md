# xRPC 线协议

## 协议定位

xRPC 在 TCP 之上使用项目自定义的二进制帧协议。Protobuf 用于 RPC metadata 和用户消息序列化，但这不是 gRPC 协议，也没有跨语言兼容承诺。

当前协议版本为 1，只支持两种消息：

```text
Request  = 1
Response = 2
```

每次调用对应一个 Request frame 和一个 Response frame，不支持 streaming 或 heartbeat frame。

## 帧布局

每个 frame 由三段组成：

```text
FrameHeader (24 bytes) | Protobuf metadata | serialized user payload
```

固定前缀的布局如下，所有多字节整数使用网络字节序：

```text
offset  size  field
──────  ────  ─────────────
0       4     magic = "XRPC"
4       1     version = 1
5       1     message_type
6       2     flags
8       4     metadata_size
12      4     payload_size
16      8     request_id
```

`flags` 当前编码为 0，尚未定义行为。`request_id` 由客户端分配，服务端原样复制到对应 response，用于在一条多路复用连接上匹配调用。

## Metadata 与 payload

Request metadata 是内部 Protobuf 消息：

```proto
message RequestMetadata {
  string service_name = 1;
  string method_name = 2;
}
```

Response metadata 保存 RPC 状态：

```proto
message ResponseMetadata {
  int32 error_code = 1;
  string error_text = 2;
}
```

OK 且没有错误文本的 response metadata 使用 Protobuf 默认值，可编码为空字节串。

payload 是用户请求或响应消息的已序列化字节。协议层只复制和传输它，不知道具体 Protobuf message 类型。

因此一次类型化调用有两层 Protobuf：

```text
RPC metadata
  └─ xRPC 内部 RequestMetadata / ResponseMetadata

user payload
  └─ 应用自己的 Request / Response message
```

它们用途不同：metadata 用于路由和状态，payload 用于业务数据。

## 请求编码

客户端类型化调用的编码路径是：

```text
用户 Request message
  ↓ SerializeAsString
payload bytes
  ↓
RequestEnvelope
  ├─ request_id
  ├─ service_name
  ├─ method_name
  └─ payload
  ↓ FrameCodec::Encode
RequestMetadata SerializeToString
  ↓
FrameHeader + metadata bytes + payload bytes
```

`FrameCodec` 一次分配完整 frame 字符串，然后把固定前缀、metadata 和 payload 写到最终位置。

## 请求解码与业务调用

服务端收到完整 Request frame 后：

```text
FrameHeader
  ├─ 验证 magic / version / type
  ├─ 验证 metadata_size
  └─ 验证 payload_size
         ↓
RequestMetadata ParseFromArray
         ↓
RequestEnvelope
         ↓ ServiceRegistry
注册方法的类型适配器
         ↓ user Request::ParseFromArray(payload)
User Handler
```

这里 `FrameCodec` 只解码 xRPC metadata；用户 request 的 Protobuf 反序列化在 Worker 线程执行的注册方法适配器中完成，不占用 Connection I/O 线程。

## 响应编码与解码

Handler 返回后，Worker 线程执行：

```text
用户 Response message
  ↓ SerializeToString
ResponseEnvelope
  ├─ request_id
  ├─ Status
  └─ payload
  ↓ FrameCodec::Encode
ResponseMetadata + payload
  ↓
完整 Response frame
```

frame bytes 通过 `DispatchMailbox` 返回原 Connection I/O 线程并写入 socket。

客户端 reader 解码 `ResponseMetadata` 和 payload，根据 `request_id` 找到等待中的调用。公开类型化 `Call<Resp>()` 最后再通过 `Resp::ParseFromString()` 解析用户 response。

## TCP 字节流与增量解码

TCP 不保留消息边界，一次 recv 可能得到半个 frame、一个 frame 或多个 frame。

服务端使用 `RpcFrameStream` 保存每条连接的未消费字节：

```text
recv bytes
  ↓ append ByteBuffer
  ↓
FrameCodec::Decode(readable bytes)
  ├─ NeedMoreData → 保留现有字节，等待下一次 recv
  ├─ Ok           → 消费一个 frame，继续解码
  └─ protocol error → 永久关闭该 stream 和连接
```

一次 `FeedBytes()` 可以返回多个 `RequestEnvelope`。客户端 reader 采用相同的“缓冲字节、反复解完整 frame”思路，但直接在 `TcpTransport::ReaderLoop()` 中维护 buffer。

## 大小限制

协议在分配和解析内容前检查：

- metadata 默认最大 64 KiB；
- request/response payload 默认最大 4 MiB，可通过 client/server options 调整；
- metadata 和 payload 长度必须能由 32 位 wire 字段表达；
- 完整 frame 长度计算必须能由本机 `size_t` 表达。

这些限制分别保护协议控制数据和用户数据。固定前缀不需要单独配置，总 frame 上限由三段长度共同决定。

## 错误语义

`FrameCodec::Decode()` 可能返回：

- `NeedMoreData`：正常的 TCP 流状态，不是错误；
- `InvalidMagic`：不是 xRPC frame；
- `UnsupportedVersion`：协议版本不支持；
- `InvalidMessageType`：消息类型不是 Request 或 Response；
- `FrameTooLarge`：超过配置上限或长度不可表示；
- `DecodeError`：Protobuf metadata 无法解析；
- `EncodeError`：保留的编码错误类别。

服务端遇到不可恢复的 frame error 会关闭连接。客户端遇到无效 response frame 会使当前 transport 失败，并唤醒该连接上的 pending calls。

## 兼容性边界

协议当前适合本仓库内同版本 client/server 学习和实验。虽然 payload 使用 Protobuf，跨语言实现仍需复制固定前缀、metadata schema、状态码、大小限制和失败语义；项目目前不把这些细节承诺为稳定公共协议。
