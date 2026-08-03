#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

#include <xrpc/status.h>

#include "rpc/naming/consul_agent_client.h"
#include "rpc/naming/consul_registrar.h"

namespace {

class FakeAgentClient final : public xrpc::ConsulAgentClientInterface {
 public:
  [[nodiscard]] auto RegisterService(const std::string &payload, std::chrono::milliseconds) const
      -> xrpc::Status override {
    ++register_calls_;
    last_register_payload_ = payload;
    if (!register_status_.ok()) {
      return register_status_;
    }
    return xrpc::Status::Ok();
  }

  [[nodiscard]] auto DeregisterService(const std::string &service_id, std::chrono::milliseconds) const
      -> xrpc::Status override {
    ++deregister_calls_;
    last_deregister_service_id_ = service_id;
    if (!deregister_status_.ok()) {
      return deregister_status_;
    }
    return xrpc::Status::Ok();
  }

  [[nodiscard]] auto PassTTL(const std::string &, std::chrono::milliseconds) const -> xrpc::Status override {
    return xrpc::Status::Ok();
  }

  mutable int register_calls_ = 0;
  mutable int deregister_calls_ = 0;
  mutable std::string last_register_payload_;
  mutable std::string last_deregister_service_id_;
  xrpc::Status register_status_ = xrpc::Status::Ok();
  xrpc::Status deregister_status_ = xrpc::Status::Ok();
};

}  // namespace

TEST(ConsulRegistrarTest, RegisterAndDeregisterAreIdempotent) {
  auto fake_client = std::make_unique<FakeAgentClient>();
  FakeAgentClient *raw = fake_client.get();
  xrpc::ConsulRegistrar registrar(std::move(fake_client));

  const xrpc::ConsulRegistrar::Options options{
      .service_name_ = "EchoService",
      .service_id_ = "EchoService_127.0.0.1_9000_1",
      .service_address_ = "127.0.0.1",
      .service_port_ = 9000,
      .timeout_ = std::chrono::milliseconds(1000),
  };

  const xrpc::Status register_status = registrar.Register(options);
  ASSERT_TRUE(register_status.ok()) << register_status.message();
  EXPECT_TRUE(registrar.registered());
  EXPECT_EQ(raw->register_calls_, 1);
  EXPECT_NE(raw->last_register_payload_.find("\"Name\":\"EchoService\""), std::string::npos);
  EXPECT_NE(raw->last_register_payload_.find("\"Port\":9000"), std::string::npos);

  const xrpc::Status deregister_status = registrar.Deregister();
  EXPECT_TRUE(deregister_status.ok()) << deregister_status.message();
  EXPECT_FALSE(registrar.registered());
  EXPECT_EQ(raw->deregister_calls_, 1);
  EXPECT_EQ(raw->last_deregister_service_id_, "EchoService_127.0.0.1_9000_1");

  EXPECT_TRUE(registrar.Deregister().ok());
  EXPECT_EQ(raw->deregister_calls_, 1);
}

TEST(ConsulRegistrarTest, DeregisterFailureKeepsRegistrationForRetry) {
  auto fake_client = std::make_unique<FakeAgentClient>();
  FakeAgentClient *raw = fake_client.get();
  xrpc::ConsulRegistrar registrar(std::move(fake_client));

  const xrpc::ConsulRegistrar::Options options{
      .service_name_ = "EchoService",
      .service_id_ = "EchoService_127.0.0.1_9000_1",
      .service_address_ = "127.0.0.1",
      .service_port_ = 9000,
      .timeout_ = std::chrono::milliseconds(1000),
  };
  ASSERT_TRUE(registrar.Register(options).ok());

  raw->deregister_status_ = xrpc::Status(xrpc::StatusCode::Unavailable, "mock deregister failure");
  const xrpc::Status failed_status = registrar.Deregister();
  EXPECT_FALSE(failed_status.ok());
  EXPECT_TRUE(registrar.registered());
  EXPECT_EQ(raw->deregister_calls_, 1);
  EXPECT_EQ(registrar.last_error(), "mock deregister failure");

  raw->deregister_status_ = xrpc::Status::Ok();
  const xrpc::Status retry_status = registrar.Deregister();
  EXPECT_TRUE(retry_status.ok()) << retry_status.message();
  EXPECT_FALSE(registrar.registered());
  EXPECT_EQ(raw->deregister_calls_, 2);
}

TEST(ConsulRegistrarTest, RegisterFailureReturnsStatusAndKeepsUnregistered) {
  auto fake_client = std::make_unique<FakeAgentClient>();
  FakeAgentClient *raw = fake_client.get();
  raw->register_status_ = xrpc::Status(xrpc::StatusCode::Unavailable, "mock register failure");
  xrpc::ConsulRegistrar registrar(std::move(fake_client));

  const xrpc::ConsulRegistrar::Options options{
      .service_name_ = "EchoService",
      .service_id_ = "EchoService_127.0.0.1_9000_1",
      .service_address_ = "127.0.0.1",
      .service_port_ = 9000,
      .timeout_ = std::chrono::milliseconds(1000),
  };

  const xrpc::Status status = registrar.Register(options);
  EXPECT_FALSE(status.ok());
  EXPECT_FALSE(registrar.registered());
  EXPECT_NE(registrar.last_error().find("mock register failure"), std::string::npos);
  EXPECT_EQ(raw->register_calls_, 1);
}

TEST(ConsulRegistrarTest, RegisterRejectsInvalidOptions) {
  auto fake_client = std::make_unique<FakeAgentClient>();
  FakeAgentClient *raw = fake_client.get();
  xrpc::ConsulRegistrar registrar(std::move(fake_client));

  const xrpc::ConsulRegistrar::Options options{
      .service_name_ = "",
      .service_id_ = "EchoService_127.0.0.1_9000_1",
      .service_address_ = "127.0.0.1",
      .service_port_ = 9000,
      .timeout_ = std::chrono::milliseconds(1000),
  };

  const xrpc::Status status = registrar.Register(options);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), xrpc::StatusCode::InvalidArgument);
  EXPECT_EQ(raw->register_calls_, 0);
  EXPECT_FALSE(registrar.registered());
}
