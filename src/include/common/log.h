#pragma once

#include <glog/logging.h>

namespace xrpc {

/**
 * Owns process-wide glog initialization and shutdown, with stderr output.
 * Create once on the program entry thread, before services and their threads.
 * Requires glog to be uninitialized; hosts managing glog directly need no guard.
 */
class LoggingRuntime final {
 public:
  explicit LoggingRuntime(const char *program_name);
  ~LoggingRuntime();

  LoggingRuntime(const LoggingRuntime &) = delete;
  auto operator=(const LoggingRuntime &) -> LoggingRuntime & = delete;
  LoggingRuntime(LoggingRuntime &&) = delete;
  auto operator=(LoggingRuntime &&) -> LoggingRuntime & = delete;
};

}  // namespace xrpc
