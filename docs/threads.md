```txt
                       xRPC Server
┌───────────────────────────────────────────────────────────────┐
│                                                               │
│  Thread A：Run / Accept Thread                                │
│  ─────────────────────────────                                │
│  RpcServer::Run()                                             │
│      │                                                        │
│      └── accept_context_.Run()                                │
│              │                                                │
│              ├── AcceptLoop coroutine                         │
│              │     └── co_await Accept(listen_fd)             │
│              │                                                │
│              ├── 处理 Accept CQE                              │
│              ├── 得到 client socket                           │
│              └── round-robin 分配连接                         │
│                    │                                          │
│        ┌───────────┼───────────┐                              │
│        ▼           ▼           ▼                              │
│                                                               │
│  Thread B       Thread C       Thread D                       │
│  Conn I/O #0    Conn I/O #1    Conn I/O #2                    │
│  ───────────    ───────────    ───────────                    │
│  UringContext   UringContext   UringContext                   │
│  Run()          Run()          Run()                          │
│                                                               │
│  Conn 1         Conn 2         Conn 3                         │
│  Conn 4         Conn 5         Conn 6                         │
│  Conn 7         ...            ...                            │
│                                                               │
│  Recv CQE                                                     │
│    ↓                                                          │
│  resume ServerConnection                                      │
│    ↓                                                          │
│  协议解析                                                     │
│    ↓                                                          │
│  RequestEnvelope                                              │
│    ↓                                                          │
│  TrySubmitBatch ───────────────────────┐                      │
│                                        │                      │
│                                        ▼                      │
│  Thread E   Thread F   Thread G   Thread H                    │
│  Worker #0  Worker #1  Worker #2  Worker #3                   │
│  ────────   ────────   ────────   ────────                    │
│                                                               │
│              ServiceRegistry::Dispatch                        │
│                        ↓                                      │
│                   User Handler                                │
│                        ↓                                      │
│                   ResponseEnvelope                            │
│                        ↓                                      │
│                encode response bytes                          │
│                        ↓                                      │
│                DispatchMailbox::Submit                        │
│                        ↓                                      │
│                original context.Post                          │
│                        │                                      │
│        ┌───────────────┘                                      │
│        ▼                                                      │
│  原 Connection I/O Thread                                     │
│        ↓                                                      │
│  OnEncodedDispatchComplete                                    │
│        ↓                                                      │
│  write queue                                                  │
│        ↓                                                      │
│  co_await Send()                                              │
│        ↓                                                      │
│  io_uring                                                     │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```
