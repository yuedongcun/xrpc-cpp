#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include <xrpc/status.h>
#include <xrpc/status_or.h>
#include <xrpc/xrpc_exception.h>

#include "rpc/naming/consul_http_client.h"
#include "rpc/naming/consul_resolver.h"

namespace {

class FakeConsulHttpClient final : public xrpc::ConsulHttpClientInterface {
 public:
  [[nodiscard]] auto Get(std::string_view, std::chrono::milliseconds) const
      -> xrpc::StatusOr<xrpc::ConsulHttpResponse> override {
    const int current = calls_.fetch_add(1);
    if (current == 0) {
      xrpc::ConsulHttpResponse response;
      response.status_code_ = 200;
      response.headers_.emplace("x-consul-index", "10");
      response.body_ = R"([{"Node":{"Address":"127.0.0.1"},"Service":{"Address":"","Port":9001}}])";
      return xrpc::StatusOr<xrpc::ConsulHttpResponse>(std::move(response));
    }
    return xrpc::StatusOr<xrpc::ConsulHttpResponse>(xrpc::Status(xrpc::StatusCode::Unavailable, "mock consul failure"));
  }
  [[nodiscard]] auto Put(std::string_view, std::string_view, std::chrono::milliseconds) const
      -> xrpc::StatusOr<xrpc::ConsulHttpResponse> override {
    return xrpc::StatusOr<xrpc::ConsulHttpResponse>(xrpc::Status(xrpc::StatusCode::Unavailable, "unused"));
  }

 private:
  mutable std::atomic_int calls_{0};
};

class EmptySnapshotConsulHttpClient final : public xrpc::ConsulHttpClientInterface {
 public:
  [[nodiscard]] auto Get(std::string_view, std::chrono::milliseconds) const
      -> xrpc::StatusOr<xrpc::ConsulHttpResponse> override {
    xrpc::ConsulHttpResponse response;
    response.status_code_ = 200;
    response.headers_.emplace("x-consul-index", "20");
    response.body_ = "[]";
    return xrpc::StatusOr<xrpc::ConsulHttpResponse>(std::move(response));
  }
  [[nodiscard]] auto Put(std::string_view, std::string_view, std::chrono::milliseconds) const
      -> xrpc::StatusOr<xrpc::ConsulHttpResponse> override {
    return xrpc::StatusOr<xrpc::ConsulHttpResponse>(xrpc::Status(xrpc::StatusCode::Unavailable, "unused"));
  }
};

}  // namespace

TEST(ConsulResolverTest, KeepsLastGoodSnapshotWhenRefreshFails) {
  auto http_client = std::make_unique<FakeConsulHttpClient>();
  xrpc::ConsulResolver resolver("EchoService", std::move(http_client), std::chrono::milliseconds(20));

  const xrpc::Status start_status = resolver.Start();
  ASSERT_TRUE(start_status.ok()) << start_status.message();

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  const auto snapshot = resolver.Snapshot();
  ASSERT_EQ(snapshot.size(), 1U);
  EXPECT_EQ(snapshot[0].host_, "127.0.0.1");
  EXPECT_EQ(snapshot[0].port_, 9001);

  const std::string last_error = resolver.last_error();
  EXPECT_FALSE(last_error.empty());
  EXPECT_NE(last_error.find("mock consul failure"), std::string::npos);

  resolver.Stop();
}

TEST(ConsulResolverTest, EmptyHealthyInstancesProduceEmptySnapshot) {
  auto http_client = std::make_unique<EmptySnapshotConsulHttpClient>();
  xrpc::ConsulResolver resolver("EchoService", std::move(http_client), std::chrono::milliseconds(20));

  const xrpc::Status start_status = resolver.Start();
  ASSERT_TRUE(start_status.ok()) << start_status.message();
  resolver.Stop();

  const auto snapshot = resolver.Snapshot();
  EXPECT_TRUE(snapshot.empty());
  EXPECT_TRUE(resolver.last_error().empty());

  const xrpc::ResolverStatsSnapshot stats = resolver.stats();
  EXPECT_GE(stats.refresh_success_count_, 1U);
  EXPECT_EQ(stats.refresh_failure_count_, 0U);
  EXPECT_GE(stats.empty_snapshot_count_, 1U);
}

TEST(ConsulResolverTest, RejectsEmptyServiceNameAtConstruction) {
  auto http_client = std::make_unique<EmptySnapshotConsulHttpClient>();
  EXPECT_THROW(static_cast<void>(xrpc::ConsulResolver("", std::move(http_client), std::chrono::milliseconds(20))),
               xrpc::ConfigException);
}

TEST(ConsulResolverTest, RejectsNegativeRefreshIntervalAtConstruction) {
  auto http_client = std::make_unique<EmptySnapshotConsulHttpClient>();
  EXPECT_THROW(
      static_cast<void>(xrpc::ConsulResolver("EchoService", std::move(http_client), std::chrono::milliseconds(-1))),
      xrpc::ConfigException);
}
