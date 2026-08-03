# 第三方声明

XRPC 将固定版本的 Protocol Buffers、liburing、nlohmann/json 和 GoogleTest
源码直接保存在 `third_party/`。具体版本、提交和上游地址见
`third_party/DEPENDENCIES.md`。各依赖保留其原始许可证文件：

- Protocol Buffers：BSD-3-Clause；
- nlohmann/json：MIT；
- GoogleTest：BSD-3-Clause；
- liburing：MIT 或 LGPL-2.1-only，其中部分 Linux UAPI 头文件使用带
  Linux-syscall-note 例外的 GPL-2.0 与 MIT 双许可证。

`build_support/run_clang_tidy.py` 源自 LLVM Project 的 `run-clang-tidy.py`，保留了原始 SPDX 标识和版权声明。该文件使用带 LLVM 例外条款的 Apache-2.0 许可证，详见 <https://llvm.org/LICENSE.txt>。
