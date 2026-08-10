#pragma once

#include <cstdint>
#include <optional>
#include <utility>

#include <xrpc/status.h>

#include "rpc/raw_message.h"

namespace xrpc {

/**
 * @brief Tracks whether a failed call can be retried without duplicate execution risk.
 *
 * `MaybeSent` means the request may already have reached the server. Client retry logic must stop in that state because
 * retrying on another endpoint could execute a non-idempotent method twice.
 */
enum class RequestCommitState : std::uint8_t {
  /** @brief The request bytes were not sent and failover is safe. */
  NotSent = 0,

  /** @brief Some or all request bytes may have reached the endpoint. */
  MaybeSent = 1,
};

/**
 * @brief Transport-level failure plus retry safety metadata.
 *
 * Keeping `commit_state_` with failures lets `ClientChannel` make retry decisions centrally, independent of which
 * transport stage produced the failure.
 */
struct CallFailure {
  /** @brief Status returned to the public call if retry cannot produce a response. */
  Status status_;

  /** @brief Whether the request may already have reached the server. */
  RequestCommitState commit_state_ = RequestCommitState::NotSent;
};

/**
 * @brief Raw transport call outcome before conversion to public `StatusOr`.
 *
 * Exactly one of `response_` or `failure_` is populated. The explicit optional fields keep retry decision code
 * straightforward and avoid requiring callers to inspect a variant visitor for the common success/failure split.
 */
struct RawCallResult {
  /** @brief Constructs a successful raw call result. */
  explicit RawCallResult(RawResponse response) : response_(std::move(response)) {}

  /** @brief Constructs a failed raw call result. */
  explicit RawCallResult(CallFailure failure) : failure_(std::move(failure)) {}

  /** @brief Raw response when the endpoint returned a protocol response. */
  std::optional<RawResponse> response_;

  /** @brief Failure status and commit state when the call attempt failed. */
  std::optional<CallFailure> failure_;

  /** @return true when this result contains `response_`. */
  [[nodiscard]] auto HasResponse() const -> bool { return response_.has_value(); }

  /** @return true when this result contains `failure_`. */
  [[nodiscard]] auto HasFailure() const -> bool { return failure_.has_value(); }

  /** @return true when retrying cannot duplicate server execution. */
  [[nodiscard]] auto CanRetryWithoutDuplicateRequest() const -> bool {
    return HasFailure() && failure().commit_state_ == RequestCommitState::NotSent;
  }

  /** @return true when retrying could duplicate server execution and must stop. */
  [[nodiscard]] auto MustStopRetryToAvoidDuplicateRequest() const -> bool {
    return HasFailure() && failure().commit_state_ == RequestCommitState::MaybeSent;
  }

  /** @return Stored raw response. Call only when `HasResponse()` is true. */
  [[nodiscard]] auto response() const -> const RawResponse & { return *response_; }

  /** @return Stored failure. Call only when `HasFailure()` is true. */
  [[nodiscard]] auto failure() const -> const CallFailure & { return *failure_; }
};

/** @return Failed raw call result with retry-safety metadata. */
[[nodiscard]] inline auto MakeCallFailure(Status status, RequestCommitState commit_state) -> RawCallResult {
  return RawCallResult(CallFailure{.status_ = std::move(status), .commit_state_ = commit_state});
}

/** @return Successful raw call result. */
[[nodiscard]] inline auto MakeCallSuccess(RawResponse response) -> RawCallResult {
  return RawCallResult(std::move(response));
}

}  // namespace xrpc
