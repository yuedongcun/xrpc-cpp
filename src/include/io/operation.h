#pragma once

#include <cstddef>
#include <cstdint>

#include <linux/time_types.h>

#include "io/io_result.h"
#include "io/uring_awaitable.h"

namespace xrpc::io {

struct Operation {
  OperationType type_ = OperationType::Nop;

  int fd_ = -1;

  void *buffer_ = nullptr;

  std::size_t length_ = 0;

  __kernel_timespec timeout_{};

  detail::AwaitableState *awaitable_state_ = nullptr;
};

}  // namespace xrpc::io
