#include <gmock/gmock.h>

#include "common/log.h"

auto main(int argc, char **argv) -> int {
  xrpc::LoggingRuntime logging(argv[0]);
  testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
