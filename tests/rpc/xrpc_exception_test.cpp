#include <gtest/gtest.h>

#include <new>
#include <stdexcept>

#include <xrpc/xrpc_exception.h>

#include "io/socket_error.h"

TEST(XrpcExceptionTest, PreservesStructuredStatus) {
  const xrpc::TransportException exception(xrpc::StatusCode::DeadlineExceeded, "connect timed out");

  EXPECT_EQ(exception.code(), xrpc::StatusCode::DeadlineExceeded);

  const xrpc::Status status = exception.status();
  EXPECT_EQ(status.code(), xrpc::StatusCode::DeadlineExceeded);
  EXPECT_EQ(status.message(), "connect timed out");
}

TEST(XrpcExceptionTest, ExceptionToStatusPreservesXrpcExceptionCode) {
  const xrpc::ProtocolException exception(xrpc::StatusCode::InvalidArgument, "bad frame");

  const xrpc::Status status = xrpc::ExceptionToStatus(exception);
  EXPECT_EQ(status.code(), xrpc::StatusCode::InvalidArgument);
  EXPECT_EQ(status.message(), "bad frame");
}

TEST(XrpcExceptionTest, SocketErrorIsStructuredTransportException) {
  const xrpc::io::SocketError exception(xrpc::io::SocketErrorCode::ReadTimeout, 0, "read timed out");

  const xrpc::Status status = xrpc::ExceptionToStatus(exception);
  EXPECT_EQ(status.code(), xrpc::StatusCode::DeadlineExceeded);
  EXPECT_EQ(status.message(), "read timed out");
}

TEST(XrpcExceptionTest, ExceptionToStatusMapsInvalidArgument) {
  const std::invalid_argument exception("bad input");

  const xrpc::Status status = xrpc::ExceptionToStatus(exception);
  EXPECT_EQ(status.code(), xrpc::StatusCode::InvalidArgument);
  EXPECT_EQ(status.message(), "bad input");
}

TEST(XrpcExceptionTest, ExceptionToStatusMapsBadAlloc) {
  const std::bad_alloc exception;

  const xrpc::Status status = xrpc::ExceptionToStatus(exception);
  EXPECT_EQ(status.code(), xrpc::StatusCode::ResourceExhausted);
  EXPECT_EQ(status.message(), "memory allocation failed");
}

TEST(XrpcExceptionTest, CurrentExceptionToStatusMapsCurrentException) {
  xrpc::Status status;
  try {
    throw xrpc::LifecycleException("server is already running");
  } catch (...) {
    status = xrpc::CurrentExceptionToStatus();
  }

  EXPECT_EQ(status.code(), xrpc::StatusCode::FailedPrecondition);
  EXPECT_EQ(status.message(), "server is already running");
}

TEST(XrpcExceptionTest, CaughtExceptionToStatusPreservesStructuredStatus) {
  xrpc::Status status;
  try {
    throw xrpc::ProtocolException(xrpc::StatusCode::DataLoss, "bad frame");
  } catch (...) {
    status = xrpc::CaughtExceptionToStatus("fallback should not be used");
  }

  EXPECT_EQ(status.code(), xrpc::StatusCode::DataLoss);
  EXPECT_EQ(status.message(), "bad frame");
}

TEST(XrpcExceptionTest, CaughtExceptionToStatusUsesFallbackCodeForNonStandardException) {
  xrpc::Status status;
  try {
    throw 42;
  } catch (...) {
    status = xrpc::CaughtExceptionToStatus(xrpc::StatusCode::Unavailable, "transport boundary failed");
  }

  EXPECT_EQ(status.code(), xrpc::StatusCode::Unavailable);
  EXPECT_EQ(status.message(), "transport boundary failed");
}

TEST(XrpcExceptionTest, CurrentExceptionToStatusHandlesNonStandardException) {
  xrpc::Status status;
  try {
    throw 42;
  } catch (...) {
    status = xrpc::CurrentExceptionToStatus();
  }

  EXPECT_EQ(status.code(), xrpc::StatusCode::Internal);
  EXPECT_EQ(status.message(), "unknown non-standard exception");
}
