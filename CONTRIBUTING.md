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

测试代码必须遵守[测试规范](tests/testing_policy.md)。不要提交生成的 Protobuf 文件、构建产物、性能测试日志或本地配置。
