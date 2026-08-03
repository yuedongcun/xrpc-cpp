#include <gtest/gtest.h>

#include <stdexcept>

#include <xrpc/xrpc_exception.h>

#include "rpc/raw_message.h"
#include "rpc/server/service_registry.h"

namespace {

auto MakeOkHandler() -> xrpc::RawHandler {
  return [](const xrpc::RawRequest &request) -> xrpc::RawResponse {
    xrpc::RawResponse response;
    response.request_id_ = request.request_id_;
    return response;
  };
}

}  // namespace

TEST(ServiceRegistryTest, RegistersOneServiceOneMethod) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", MakeOkHandler());

  EXPECT_NE(registry.FindMethod("EchoService", "Echo"), nullptr);
}

TEST(ServiceRegistryTest, RegistersMultipleMethodsUnderOneService) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", MakeOkHandler());
  registry.RegisterRaw("EchoService", "Ping", MakeOkHandler());

  EXPECT_NE(registry.FindMethod("EchoService", "Echo"), nullptr);
  EXPECT_NE(registry.FindMethod("EchoService", "Ping"), nullptr);
}

TEST(ServiceRegistryTest, SameMethodNameAcrossDifferentServicesDoesNotConflict) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", MakeOkHandler());
  registry.RegisterRaw("PingService", "Echo", MakeOkHandler());

  EXPECT_NE(registry.FindMethod("EchoService", "Echo"), nullptr);
  EXPECT_NE(registry.FindMethod("PingService", "Echo"), nullptr);
}

TEST(ServiceRegistryTest, DuplicateMethodRegistrationFails) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", MakeOkHandler());

  EXPECT_THROW(registry.RegisterRaw("EchoService", "Echo", MakeOkHandler()), xrpc::ConfigException);
}

TEST(ServiceRegistryTest, UnknownServiceReturnsNotFound) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", MakeOkHandler());

  EXPECT_EQ(registry.FindService("UnknownService"), nullptr);
  EXPECT_EQ(registry.FindMethod("UnknownService", "Echo"), nullptr);
}

TEST(ServiceRegistryTest, UnknownMethodReturnsNotFound) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", MakeOkHandler());

  EXPECT_EQ(registry.FindMethod("EchoService", "UnknownMethod"), nullptr);
}

TEST(ServiceRegistryTest, DispatchesRegisteredMethodAndPreservesRequestId) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", [](const xrpc::RawRequest &request) -> xrpc::RawResponse {
    xrpc::RawResponse response;
    response.request_id_ = request.request_id_;
    response.payload_ = request.payload_;
    return response;
  });

  xrpc::RawRequest request;
  request.request_id_ = 42;
  request.service_name_ = "EchoService";
  request.method_name_ = "Echo";
  request.payload_ = "hello";

  const xrpc::RawResponse response = registry.Dispatch(request);
  EXPECT_EQ(response.request_id_, 42U);
  EXPECT_TRUE(response.status_.ok());
  EXPECT_EQ(response.payload_, "hello");
}

TEST(ServiceRegistryTest, DispatchReturnsUnknownServiceWhenServiceMissing) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", MakeOkHandler());

  xrpc::RawRequest request;
  request.request_id_ = 7;
  request.service_name_ = "UnknownService";
  request.method_name_ = "Echo";

  const xrpc::RawResponse response = registry.Dispatch(request);
  EXPECT_EQ(response.request_id_, 7U);
  EXPECT_EQ(response.status_.code(), xrpc::StatusCode::NotFound);
  EXPECT_EQ(response.status_.message(), "unknown service");
}

TEST(ServiceRegistryTest, DispatchReturnsUnknownMethodWhenMethodMissing) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", MakeOkHandler());

  xrpc::RawRequest request;
  request.request_id_ = 8;
  request.service_name_ = "EchoService";
  request.method_name_ = "UnknownMethod";

  const xrpc::RawResponse response = registry.Dispatch(request);
  EXPECT_EQ(response.request_id_, 8U);
  EXPECT_EQ(response.status_.code(), xrpc::StatusCode::Unimplemented);
  EXPECT_EQ(response.status_.message(), "unknown method");
}

TEST(ServiceRegistryTest, DispatchConvertsHandlerExceptionToErrorResponse) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", [](const xrpc::RawRequest &) -> xrpc::RawResponse {
    throw std::runtime_error("handler failed");
  });

  xrpc::RawRequest request;
  request.request_id_ = 9;
  request.service_name_ = "EchoService";
  request.method_name_ = "Echo";

  const xrpc::RawResponse response = registry.Dispatch(request);
  EXPECT_EQ(response.request_id_, 9U);
  EXPECT_EQ(response.status_.code(), xrpc::StatusCode::Internal);
  EXPECT_EQ(response.status_.message(), "handler failed");
}

TEST(ServiceRegistryTest, DispatchConvertsXrpcExceptionToStructuredErrorResponse) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", [](const xrpc::RawRequest &) -> xrpc::RawResponse {
    throw xrpc::TransportException(xrpc::StatusCode::Unavailable, "upstream unavailable");
  });

  xrpc::RawRequest request;
  request.request_id_ = 11;
  request.service_name_ = "EchoService";
  request.method_name_ = "Echo";

  const xrpc::RawResponse response = registry.Dispatch(request);
  EXPECT_EQ(response.request_id_, 11U);
  EXPECT_EQ(response.status_.code(), xrpc::StatusCode::Unavailable);
  EXPECT_EQ(response.status_.message(), "upstream unavailable");
}

TEST(ServiceRegistryTest, DispatchConvertsInvalidRequestExceptionToInvalidArgument) {
  xrpc::ServiceRegistry registry;
  registry.RegisterRaw("EchoService", "Echo", [](const xrpc::RawRequest &) -> xrpc::RawResponse {
    throw std::invalid_argument("invalid request");
  });

  xrpc::RawRequest request;
  request.request_id_ = 10;
  request.service_name_ = "EchoService";
  request.method_name_ = "Echo";

  const xrpc::RawResponse response = registry.Dispatch(request);
  EXPECT_EQ(response.status_.code(), xrpc::StatusCode::InvalidArgument);
  EXPECT_EQ(response.status_.message(), "invalid request");
}
