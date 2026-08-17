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

#include "client/effective_call_options.h"
#include "client/raw_call_result.h"
#include "io/socket.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_message.h"

namespace xrpc {

/**
 * @brief Blocking client transport for one TCP endpoint.
 *
 * Design note:
 * - Ownership: one `TcpTransport` owns one socket and one reader thread for a single endpoint.
 * - Threading: callers may issue concurrent calls; writes are serialized by `write_mutex_`, and the reader thread
 *   resolves `PendingCall` objects by request id.
 * - Failure: write failures before bytes are sent are retryable; failures after a request may have reached the endpoint
 *   are marked `MaybeSent`.
 * - Shutdown: `Close()` and reader-side failures wake all pending callers before the socket and reader thread are
 *   joined.
 */
class TcpTransport final {
 public:
  /**
   * @brief Creates a disconnected transport for one endpoint.
   *
   * @param host Endpoint host.
   * @param port Endpoint TCP port.
   * @param protocol_limits Frame limits applied to responses from this endpoint.
   * @param max_inflight_per_endpoint Maximum pending calls allowed on this transport.
   */
  TcpTransport(std::string host, std::uint16_t port, ProtocolLimits protocol_limits = {},
               std::size_t max_inflight_per_endpoint = 1024);

  /** @brief Closes the socket, wakes pending calls, and joins the reader thread. */
  ~TcpTransport();

  /**
   * @brief Sends one request and waits for its response or failure.
   *
   * @return Raw response or failure with retry-safety metadata.
   */
  [[nodiscard]] auto Call(const RawRequest &request, const EffectiveCallOptions &options) -> RawCallResult;

  /** @brief Closes this endpoint transport and completes all pending calls with failure. */
  void Close();

 private:
  /**
   * @brief One waiter per in-flight request.
   *
   * `ReaderLoop()`, `FailAllPending()`, or the caller timing out locally resolves the request and removes it from
   * `pending_calls_`.
   */
  struct PendingCall final {
    std::uint64_t request_id_ = 0;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::optional<RawCallResult> result_;
  };

  /** @brief Opens the socket using a relative connection timeout. */
  void EnsureConnectedWithTimeout(std::chrono::milliseconds timeout);

  /** @brief Returns the connected fd while holding the state mutex contract. */
  [[nodiscard]] auto ConnectedFd() const -> int;

  /** @brief Joins a stopped reader thread after releasing/reacquiring the state lock as needed. */
  void JoinReaderIfStopped(std::unique_lock<std::mutex> &lock);

  /** @brief Reads response frames and completes matching pending calls. */
  void ReaderLoop(int fd);

  /** @brief Closes this transport from the reader thread and fails pending calls. */
  void CloseFromReader(int fd, const Status &status);

  /** @brief Closes the socket while the state mutex is held. */
  void CloseSocketLocked();

  /** @brief Adds a pending call if the per-endpoint in-flight limit permits it. */
  [[nodiscard]] auto TryRegisterPending(std::uint64_t request_id, std::shared_ptr<PendingCall> pending) -> bool;

  /** @brief Removes a pending call by request id. */
  [[nodiscard]] auto RemovePending(std::uint64_t request_id) -> bool;

  /** @brief Completes one pending call and wakes its waiter. */
  void CompletePending(std::uint64_t request_id, RawCallResult result);

  /** @brief Completes every pending call with the same failure status. */
  void FailAllPending(const Status &status, RequestCommitState commit_state);

  /** @brief Waits for response, local timeout, or transport shutdown. */
  [[nodiscard]] auto WaitForResult(const std::shared_ptr<PendingCall> &pending, std::uint64_t request_id,
                                   const EffectiveCallOptions &options) -> RawCallResult;

  /** @brief Writes one request frame and reports whether failure happened before or after bytes may have been sent. */
  [[nodiscard]] auto WriteRequestFrame(std::uint64_t request_id, std::string_view frame,
                                       const EffectiveCallOptions &options) -> std::optional<RawCallResult>;

  /** @brief Endpoint host used for reconnect attempts. */
  std::string host_;

  /** @brief Endpoint TCP port. */
  std::uint16_t port_ = 0;

  /** @brief Protocol limits used by the frame codec. */
  ProtocolLimits protocol_limits_;

  /** @brief Maximum pending calls allowed before returning resource-exhausted. */
  std::size_t max_inflight_per_endpoint_ = 0;

  /** @brief Protects socket state and reader-thread lifecycle. */
  mutable std::mutex state_mutex_;

  /** @brief Connected socket, or invalid socket when closed. */
  io::Socket socket_;

  /** @brief Reader thread that decodes responses and completes pending calls. */
  std::jthread reader_thread_;

  /** @brief Serializes writes so request frames are not interleaved. */
  std::mutex write_mutex_;

  /** @brief Protects the pending call map. */
  std::mutex pending_mutex_;

  /** @brief Pending calls keyed by request id. */
  std::unordered_map<std::uint64_t, std::shared_ptr<PendingCall>> pending_calls_;
};

}  // namespace xrpc
