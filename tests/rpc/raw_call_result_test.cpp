#include <gtest/gtest.h>

#include <utility>

#include "rpc/raw_call_result.h"

namespace xrpc {
namespace {

TEST(RawCallResultTest, SuccessIsNotAFailureRetryDecision) {
  RawResponse response;
  response.request_id_ = 1;
  response.status_ = Status::Ok();

  const RawCallResult result = MakeCallSuccess(response.request_id_, std::move(response));

  EXPECT_TRUE(result.HasResponse());
  EXPECT_FALSE(result.HasFailure());
  EXPECT_FALSE(result.CanRetryWithoutDuplicateRequest());
  EXPECT_FALSE(result.MustStopRetryToAvoidDuplicateRequest());
}

TEST(RawCallResultTest, NotSentFailureCanRetryWithoutDuplicateRequest) {
  const RawCallResult result =
      MakeCallFailure(1, Status(StatusCode::Unavailable, "connect failed"), RequestCommitState::NotSent);

  EXPECT_FALSE(result.HasResponse());
  EXPECT_TRUE(result.HasFailure());
  EXPECT_TRUE(result.CanRetryWithoutDuplicateRequest());
  EXPECT_FALSE(result.MustStopRetryToAvoidDuplicateRequest());
}

TEST(RawCallResultTest, MaybeSentFailureMustStopRetryToAvoidDuplicateRequest) {
  const RawCallResult result =
      MakeCallFailure(1, Status(StatusCode::Unavailable, "send failed"), RequestCommitState::MaybeSent);

  EXPECT_FALSE(result.HasResponse());
  EXPECT_TRUE(result.HasFailure());
  EXPECT_FALSE(result.CanRetryWithoutDuplicateRequest());
  EXPECT_TRUE(result.MustStopRetryToAvoidDuplicateRequest());
}

}  // namespace
}  // namespace xrpc
