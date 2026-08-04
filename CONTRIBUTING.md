# 参与贡献

XRPC 面向 Linux 和 C++20。公开 API 头文件放在 `include/xrpc/`，内部头文件放在 `src/include/`，实现文件放在 `src/`。

## 本地检查

配置并构建全部开发目标：

```bash
make
```

运行默认测试集：

```bash
make test
```

提交前可以运行完整的本地检查：

```bash
make ci
```

测试代码请参考[测试说明](tests/README.md)。源码注释使用简短英文，重点说明公开 API 契约、所有权、线程安全、失败语义和不直观的性能取舍，不要复述显而易见的代码。不要提交生成的 Protobuf 文件、构建产物、性能测试日志或本地配置。
