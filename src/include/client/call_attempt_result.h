/** @file call_attempt_result.h @brief Defines the result of one endpoint call attempt. */

#pragma once

#include <cstdint>
#include <optional>
#include <utility>

#include <xrpc/status.h>

#include "protocol/rpc_envelope.h"

namespace xrpc {

enum class RequestCommitState : std::uint8_t {

  NotSent = 0,

  MaybeSent = 1,
};

struct CallFailure {
  Status status_;

  RequestCommitState commit_state_ = RequestCommitState::NotSent;
};

struct CallAttemptResult {
  explicit CallAttemptResult(ResponseEnvelope response) : response_(std::move(response)) {}

  explicit CallAttemptResult(CallFailure failure) : failure_(std::move(failure)) {}

  std::optional<ResponseEnvelope> response_;

  std::optional<CallFailure> failure_;

  [[nodiscard]] auto HasResponse() const -> bool { return response_.has_value(); }

  [[nodiscard]] auto HasFailure() const -> bool { return failure_.has_value(); }

  [[nodiscard]] auto CanRetryWithoutDuplicateRequest() const -> bool {
    return HasFailure() && failure().commit_state_ == RequestCommitState::NotSent;
  }

  [[nodiscard]] auto MustStopRetryToAvoidDuplicateRequest() const -> bool {
    return HasFailure() && failure().commit_state_ == RequestCommitState::MaybeSent;
  }

  [[nodiscard]] auto response() const -> const ResponseEnvelope & { return *response_; }

  [[nodiscard]] auto failure() const -> const CallFailure & { return *failure_; }
};

[[nodiscard]] inline auto MakeCallFailure(Status status, RequestCommitState commit_state) -> CallAttemptResult {
  return CallAttemptResult(CallFailure{.status_ = std::move(status), .commit_state_ = commit_state});
}

[[nodiscard]] inline auto MakeCallSuccess(ResponseEnvelope response) -> CallAttemptResult {
  return CallAttemptResult(std::move(response));
}

}  // namespace xrpc
