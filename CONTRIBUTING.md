# 参与贡献

XRPC 面向 Linux 和 C++20。公开 API 头文件放在 `include/xrpc/`，内部头文件放在 `src/include/`，实现文件放在 `src/`。

## 本地检查

配置并构建全部开发目标：

```bash
cmake -S . -B build \
  -DXRPC_BUILD_EXAMPLES=ON \
  -DXRPC_BUILD_TESTS=ON \
  -DXRPC_BUILD_TOOLS=ON
cmake --build build --parallel
```

运行默认测试集：

```bash
ctest --test-dir build/tests --output-on-failure -LE "external|tooling"
```

提交修改前格式化项目源码：

```bash
make format
```

只有安装了所需 Clang 20 工具时才运行 clang-tidy：

```bash
make check-clang-tidy
```

测试代码必须遵守[测试规范](tests/testing_policy.md)。不要提交生成的 Protobuf 文件、构建产物、性能测试日志或本地配置。
