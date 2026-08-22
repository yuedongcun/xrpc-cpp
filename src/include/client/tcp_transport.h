/** @file tcp_transport.h @brief Declares the blocking TCP transport used by RpcClient. */

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

#include "client/call_attempt_result.h"
#include "client/effective_call_options.h"
#include "io/socket.h"
#include "protocol/frame_codec.h"
#include "protocol/rpc_envelope.h"

namespace xrpc {

/**
 * @brief One endpoint's blocking, multiplexed client transport.
 *
 * Multiple callers may invoke `Call()` concurrently. The transport serializes
 * socket writes, protects pending-call matching, and uses one reader thread
 * for responses. Destruction is a lifecycle operation and must not overlap
 * calls from application threads.
 */
class TcpTransport final {
 public:
  TcpTransport(std::string host, std::uint16_t port, ProtocolLimits protocol_limits = {},
               std::size_t max_inflight_per_endpoint = 1024);

  ~TcpTransport();

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

  void EnsureConnectedWithTimeout(std::chrono::milliseconds timeout);

  [[nodiscard]] auto ConnectedFd() const -> int;

  void JoinReaderIfStopped(std::unique_lock<std::mutex> &lock);

  void ReaderLoop(int fd);

  void CloseFromReader(int fd, const Status &status);

  void CloseSocketLocked();

  [[nodiscard]] auto TryRegisterPending(std::uint64_t request_id, std::shared_ptr<PendingCall> pending) -> bool;

  [[nodiscard]] auto RemovePending(std::uint64_t request_id) -> bool;

  void CompletePending(std::uint64_t request_id, CallAttemptResult result);

  void FailAllPending(const Status &status, RequestCommitState commit_state);

  [[nodiscard]] auto WaitForResult(const std::shared_ptr<PendingCall> &pending, std::uint64_t request_id,
                                   const EffectiveCallOptions &options) -> CallAttemptResult;

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
