#include <gtest/gtest.h>

#include <cstdlib>

#include "common/log.h"
#include "common/xrpc_exception.h"

TEST(LoggingRuntimeTest, RejectsDuplicateInitializationWithoutChangingExistingLogging) {
  ASSERT_TRUE(google::IsGoogleLoggingInitialized());
  const bool stderr_enabled = FLAGS_logtostderr;
  EXPECT_THROW(xrpc::LoggingRuntime duplicate("duplicate"), xrpc::LifecycleException);
  EXPECT_TRUE(google::IsGoogleLoggingInitialized());
  EXPECT_EQ(FLAGS_logtostderr, stderr_enabled);
}

TEST(LoggingRuntimeTest, InitializesAndShutsDownAtScopeExit) {
  // Use a child process so the test runner's glog lifetime remains intact.
  EXPECT_EXIT(
      {
        google::ShutdownGoogleLogging();
        {
          xrpc::LoggingRuntime logging("scope-test");
          if (!google::IsGoogleLoggingInitialized() || !FLAGS_logtostderr) {
            std::exit(1);
          }
        }
        std::exit(google::IsGoogleLoggingInitialized() ? 1 : 0);
      },
      testing::ExitedWithCode(0), "");
}
