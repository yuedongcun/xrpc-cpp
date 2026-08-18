#pragma once

#include <cstddef>
#include <cstdint>

namespace xrpc::io {

enum class OperationType : std::uint8_t {

  Accept = 0,

  Recv,

  Send,

  Timeout,

  Nop,

  Cancel,

  Wakeup,
};

struct IoResult {
  OperationType type_ = OperationType::Nop;

  int fd_ = -1;

  int result_ = 0;

  int error_code_ = 0;

  std::size_t bytes_transferred_ = 0;
};

}  // namespace xrpc::io
