#include <memory>

#include <gtest/gtest.h>

#include <xrpc/status_or.h>

namespace xrpc {
namespace {

TEST(StatusOrTest, StoresValue) {
  StatusOr<int> result(42);

  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.value(), 42);
}

TEST(StatusOrTest, StoresErrorStatus) {
  StatusOr<int> result(Status(StatusCode::Unavailable, "service unavailable"));

  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), StatusCode::Unavailable);
  EXPECT_EQ(result.status().message(), "service unavailable");
}

TEST(StatusOrTest, MovesValueFromRvalueStatusOr) {
  StatusOr<std::unique_ptr<int>> result(std::make_unique<int>(7));

  std::unique_ptr<int> value = std::move(result).value();

  ASSERT_NE(value, nullptr);
  EXPECT_EQ(*value, 7);
}

}  // namespace
}  // namespace xrpc
