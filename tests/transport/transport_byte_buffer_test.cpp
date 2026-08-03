#include <gtest/gtest.h>

#include <xrpc/xrpc_exception.h>

#include "transport/byte_buffer.h"

namespace xrpc {

TEST(ByteBufferTest, AppendExposesReadableBytes) {
  ByteBuffer buffer;

  buffer.Append("hello");

  EXPECT_EQ(buffer.ReadableSize(), 5U);
  EXPECT_EQ(buffer.ReadableBytes(), "hello");
  EXPECT_FALSE(buffer.Empty());
}

TEST(ByteBufferTest, MultipleAppendsAreAccumulated) {
  ByteBuffer buffer;

  buffer.Append("he");
  buffer.Append("llo");

  EXPECT_EQ(buffer.ReadableSize(), 5U);
  EXPECT_EQ(buffer.ReadableBytes(), "hello");
}

TEST(ByteBufferTest, ConsumeRemovesPrefixAndKeepsTail) {
  ByteBuffer buffer;

  buffer.Append("hello world");
  buffer.Consume(6);

  EXPECT_EQ(buffer.ReadableSize(), 5U);
  EXPECT_EQ(buffer.ReadableBytes(), "world");

  buffer.Append("!");

  EXPECT_EQ(buffer.ReadableSize(), 6U);
  EXPECT_EQ(buffer.ReadableBytes(), "world!");
}

TEST(ByteBufferTest, ConsumeAllClearsBuffer) {
  ByteBuffer buffer;

  buffer.Append("abc");
  buffer.Consume(3);

  EXPECT_TRUE(buffer.Empty());
  EXPECT_EQ(buffer.ReadableSize(), 0U);
  EXPECT_EQ(buffer.ReadableBytes(), "");
}

TEST(ByteBufferTest, ZeroLengthOperationsAreNoop) {
  ByteBuffer buffer;

  buffer.Append("");
  buffer.Consume(0);

  EXPECT_TRUE(buffer.Empty());
  EXPECT_EQ(buffer.ReadableSize(), 0U);
  EXPECT_EQ(buffer.ReadableBytes(), "");
}

TEST(ByteBufferTest, IllegalConsumeThrows) {
  ByteBuffer buffer;

  buffer.Append("abc");

  EXPECT_THROW(buffer.Consume(4), LifecycleException);
  EXPECT_EQ(buffer.ReadableBytes(), "abc");
}

}  // namespace xrpc
