#include "common/log.h"

#include "common/xrpc_exception.h"

namespace xrpc {

LoggingRuntime::LoggingRuntime(const char *program_name) {
  if (google::IsGoogleLoggingInitialized()) {
    throw LifecycleException("LoggingRuntime requires glog to be uninitialized");
  }
  google::InitGoogleLogging(program_name);
  FLAGS_logtostderr = true;
}

LoggingRuntime::~LoggingRuntime() { google::ShutdownGoogleLogging(); }

}  // namespace xrpc
