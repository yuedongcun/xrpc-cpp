/**
 * @file tcp_transport.h
 * @brief Declares one endpoint's blocking, multiplexed TCP connection.
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include <xrpc/status.h>

#include "io/socket.h"
#include "protocol/frame_codec.h"
#include "protocol/rpc_envelope.h"

namespace xrpc {

/**
 * @brief Internal call options shared by all endpoint attempts of one call.
 *
 * The absolute deadline is computed once before failover begins, so retries
 * consume the original call budget instead of receiving a fresh timeout. A
 * missing deadline means the call has no timeout.
 */
struct EffectiveCallOptions {
  std::optional<std::chrono::steady_clock::time_point> deadline_;

  std::string sticky_key_;
};

enum class RequestCommitState : std::uint8_t {
  /** No request bytes were committed; trying another endpoint is safe. */
  NotSent = 0,

  /** The peer may have received the request; retrying could execute it twice. */
  MaybeSent = 1,
};

struct CallFailure {
  Status status_;

  RequestCommitState commit_state_ = RequestCommitState::NotSent;
};

/**
 * @brief Contains either a response or a failure from one endpoint attempt.
 *
 * Exactly one of `response_` and `failure_` is populated.
 */
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

/**
 * @brief Owns one lazily connected TCP transport for a single endpoint.
 *
 * Multiple caller threads may execute `Call()` concurrently. Request writes
 * are serialized onto one socket, while a dedicated reader thread decodes
 * responses and matches them to pending calls by request id.
 *
 * `state_mutex_` protects the socket and reader-thread lifecycle,
 * `write_mutex_` permits at most one active frame write, and `pending_mutex_`
 * protects request registration and response matching. Each PendingCall owns
 * the condition variable that wakes its synchronous caller.
 *
 * A transport failure completes all pending calls. Destruction requires that
 * application threads no longer call this transport.
 */
class TcpTransport final {
 public:
  TcpTransport(std::string host, std::uint16_t port, ProtocolLimits protocol_limits = {},
               std::size_t max_inflight_per_endpoint = 1024);

  ~TcpTransport();

  /** Performs one endpoint attempt and reports whether a failed request may be retried. */
  [[nodiscard]] auto Call(const RequestEnvelope &request, const EffectiveCallOptions &options) -> CallAttemptResult;

  // Internal failure and lifecycle cleanup; not a separate concurrent API.
  void Close();

 private:
  struct PendingCall final {
    std::uint64_t request_id_ = 0;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::optional<CallAttemptResult> result_;
  };

  /** Lazily connects and starts the endpoint's sole response reader. */
  void EnsureConnectedWithTimeout(std::chrono::milliseconds timeout);

  [[nodiscard]] auto ConnectedFd() const -> int;

  void JoinReaderIfStopped(std::unique_lock<std::mutex> &lock);

  /** Reads and matches all responses for the current socket generation. */
  void ReaderLoop(int fd);

  /** Closes only the socket generation owned by the failing reader. */
  void CloseFromReader(int fd, const Status &status);

  void CloseSocketLocked();

  [[nodiscard]] auto TryRegisterPending(std::uint64_t request_id, std::shared_ptr<PendingCall> pending) -> bool;

  [[nodiscard]] auto RemovePending(std::uint64_t request_id) -> bool;

  void CompletePending(std::uint64_t request_id, CallAttemptResult result);

  /** Completes every registered call after the connection becomes unusable. */
  void FailAllPending(const Status &status, RequestCommitState commit_state);

  /** Waits until the reader, timeout path, or connection failure completes the call. */
  [[nodiscard]] auto WaitForResult(const std::shared_ptr<PendingCall> &pending, std::uint64_t request_id,
                                   const EffectiveCallOptions &options) -> CallAttemptResult;

  /** Writes one frame and classifies failures by whether bytes may have reached the peer. */
  [[nodiscard]] auto WriteRequestFrame(std::uint64_t request_id, std::string_view frame,
                                       const EffectiveCallOptions &options) -> std::optional<CallAttemptResult>;

  std::string host_;

  std::uint16_t port_ = 0;

  ProtocolLimits protocol_limits_;

  std::size_t max_inflight_per_endpoint_ = 0;

  mutable std::mutex state_mutex_;

  io::Socket socket_;

  std::jthread reader_thread_;

  std::mutex write_mutex_;

  std::mutex pending_mutex_;

  std::unordered_map<std::uint64_t, std::shared_ptr<PendingCall>> pending_calls_;
};

}  // namespace xrpc
