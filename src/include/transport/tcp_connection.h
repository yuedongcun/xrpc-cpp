#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/task.h"
#include "io/socket.h"
#include "io/uring_context.h"
#include "protocol/frame_codec.h"
#include "rpc/handler.h"
#include "rpc/server/rpc_session.h"
#include "transport/connection_close_reason.h"
#include "transport/server_backpressure.h"
#include "transport/server_io_stats.h"
#include "transport/thread_pool_executor.h"

namespace xrpc {

class DispatchCompletionQueue;

/**
 * @brief Dependencies and limits injected into one accepted connection.
 *
 * Stats pointers may be null; `TcpConnection` then owns local stats for tests. In production, the owning `TcpServer`
 * passes shared stats objects so connection-level rejections and write behavior can be observed at the server facade.
 */
struct TcpConnectionOptions final {
  /** @brief Per-connection inflight and write-queue limits. */
  ServerBackpressureLimits limits_;

  /** @brief Optional shared backpressure stats sink. */
  ServerBackpressureStats *backpressure_stats_ = nullptr;

  /** @brief Optional shared I/O stats sink. */
  ServerIoStats *io_stats_ = nullptr;

  /** @brief Completion queue used by worker threads to return encoded responses. */
  std::shared_ptr<DispatchCompletionQueue> completion_queue_;

  /** @brief Frame limits applied by the connection's `RpcSession`. */
  ProtocolLimits protocol_limits_;

  /** @brief Idle timeout for this connection. Zero disables idle cleanup. */
  std::chrono::milliseconds idle_timeout_{0};
};

/**
 * @brief Event-loop-owned server-side TCP connection.
 *
 * Design note:
 * - Ownership: one shared `TcpConnection` owns its socket, session, write queue, idle timer task, and outstanding
 *   dispatch accounting.
 * - Threading: socket/session/write state stays on the `UringContext` thread; handlers run on `ThreadPoolExecutor` and
 *   return through `DispatchCompletionQueue`.
 * - Backpressure: reads can continue only while inflight jobs and queued writes stay within `TcpConnectionOptions`
 *   limits.
 * - Shutdown: `Close()` records one reason, drains or rejects pending work, and lets the coroutine finish on the
 *   event-loop thread.
 */
class TcpConnection final : public std::enable_shared_from_this<TcpConnection> {
 public:
  /** @brief Creates a connection from an accepted socket and injected runtime dependencies. */
  TcpConnection(io::UringContext &context, RawHandler handler, ThreadPoolExecutor &executor, io::Socket socket,
                TcpConnectionOptions options);

  /** @brief Releases owned socket and coroutine task state. */
  ~TcpConnection();

  TcpConnection(const TcpConnection &) = delete;
  auto operator=(const TcpConnection &) -> TcpConnection & = delete;

  TcpConnection(TcpConnection &&) noexcept = delete;
  auto operator=(TcpConnection &&) noexcept -> TcpConnection & = delete;

  /**
   * @brief Runs the connection read loop coroutine.
   *
   * @return Task that completes when the connection closes and in-flight work has been accounted for.
   */
  [[nodiscard]] auto Run() -> runtime::Task<void>;

  /** @brief Closes the connection with the first recorded close reason. */
  void Close(ConnectionCloseReason reason = ConnectionCloseReason::SocketError);

  /** @return true after the connection has entered the closed state. */
  [[nodiscard]] auto IsClosed() const -> bool { return closed_; }

  /** @return First close reason recorded for this connection. */
  [[nodiscard]] auto close_reason() const -> ConnectionCloseReason { return close_reason_; }

  /** @return Underlying socket fd, or -1 after the socket has been closed. */
  [[nodiscard]] auto fd() const -> int { return socket_.fd(); }

 private:
  friend class DispatchCompletionQueue;

  /** @brief Handles a worker-completed encoded response on the event-loop thread. */
  void OnEncodedDispatchComplete(std::string &&response_bytes, std::size_t completed_jobs);

  /** @brief Accounts a worker encode failure on the event-loop thread. */
  void OnDispatchEncodeFailure(std::size_t completed_jobs);

  /** @brief Releases completed dispatch jobs from this connection's in-flight count. */
  void ReleaseDispatchJobs(std::size_t completed_jobs);

  /** @brief Adds encoded response bytes to the write queue if capacity allows. */
  [[nodiscard]] auto EnqueueWrite(std::string bytes) -> bool;

  /** @brief Reserves write-queue bytes or closes the connection on high watermark. */
  [[nodiscard]] auto TryReserveWriteBytes(std::size_t bytes) -> bool;

  /** @brief Releases bytes after they leave the write queue. */
  void ReleaseWriteBytes(std::size_t bytes);

  /** @brief Sends queued response frames until the queue is empty or the socket fails. */
  [[nodiscard]] auto DrainWriteQueue() -> runtime::Task<void>;

  /** @brief Dispatches decoded requests or closes the connection after protocol failure. */
  [[nodiscard]] auto HandleFeedResult(SessionFeedResult &&feed) -> bool;

  /** @brief Submits a batch of decoded requests to the worker pool. */
  [[nodiscard]] auto SubmitDispatchBatch(std::vector<RawRequest> requests) -> bool;

  /** @brief Produces an immediate resource-exhausted response for a rejected request. */
  [[nodiscard]] auto RejectRequestDueToBackpressure(RawRequest &&request, std::string message) -> bool;

  /** @brief Runs the raw handler and converts thrown failures to raw responses. */
  [[nodiscard]] auto DispatchOnWorker(RawRequest request) const -> RawResponse;

  /** @brief Encodes a raw response on a worker before handing bytes to the event loop. */
  [[nodiscard]] auto EncodeResponseOnWorker(RawResponse &&response) const -> std::string;

  /** @brief Records the first close reason and leaves it unchanged afterward. */
  void SetClosedReason(ConnectionCloseReason reason);

  /** @brief Finishes shutdown after peer EOF once queued and dispatched work drains. */
  void TryFinishAfterPeerClosed();

  /** @brief Starts the idle timer coroutine when idle cleanup is enabled. */
  void StartIdleTimerIfNeeded();

  /** @brief Waits for idle timeout generations and closes inactive connections. */
  [[nodiscard]] auto RunIdleTimer() -> runtime::Task<void>;

  /** @brief Marks activity so stale idle-timer wakeups can be ignored. */
  void TouchActivity() noexcept;

  /** @return true while reads, writes, worker dispatch, or timers still need cleanup. */
  [[nodiscard]] auto HasPendingWork() const noexcept -> bool;

  /** @brief Event loop that owns this connection's socket operations. */
  io::UringContext *context_;

  /** @brief Worker-to-event-loop completion queue. */
  std::shared_ptr<DispatchCompletionQueue> completion_queue_;

  /** @brief Worker pool used for handler dispatch and response encoding. */
  ThreadPoolExecutor *executor_ = nullptr;

  /** @brief Raw handler dispatched for decoded requests. */
  RawHandler handler_;

  /** @brief Per-connection protocol session and decode buffer owner. */
  RpcSession session_;

  /** @brief Protocol frame limits copied into worker encode paths. */
  ProtocolLimits protocol_limits_;

  /** @brief Accepted client socket. */
  io::Socket socket_;

  /** @brief Read buffer reused by asynchronous receive operations. */
  std::string read_buffer_;

  /** @brief Encoded response frames waiting for socket send. */
  std::deque<std::string> write_queue_;

  /** @brief Active write-drain coroutine, when a drain is in progress. */
  std::optional<runtime::Task<void>> write_task_;

  /** @brief Idle timeout coroutine, when idle cleanup is enabled. */
  std::optional<runtime::Task<void>> idle_timer_task_;

  /** @brief True while `DrainWriteQueue()` owns the socket send path. */
  bool write_in_progress_ = false;

  /** @brief True after peer EOF; connection waits for pending work before final close. */
  bool peer_read_closed_ = false;

  /** @brief Number of handler jobs submitted to workers but not yet completed on the loop. */
  std::size_t pending_dispatch_jobs_ = 0;

  /** @brief Total bytes currently queued in `write_queue_`. */
  std::size_t pending_write_bytes_ = 0;

  /** @brief Idle timeout for this connection. Zero disables idle cleanup. */
  std::chrono::milliseconds idle_timeout_{0};

  /** @brief Incremented on activity so the idle timer can detect stale wakeups. */
  std::uint64_t activity_generation_ = 0;

  /** @brief Per-connection backpressure limits. */
  ServerBackpressureLimits limits_;

  /** @brief Shared backpressure stats, or `owned_backpressure_stats_` in tests. */
  ServerBackpressureStats *backpressure_stats_ = nullptr;

  /** @brief Local stats used when no shared backpressure stats were provided. */
  ServerBackpressureStats owned_backpressure_stats_;

  /** @brief Shared I/O stats, or `owned_io_stats_` in tests. */
  ServerIoStats *io_stats_ = nullptr;

  /** @brief Local stats used when no shared I/O stats were provided. */
  ServerIoStats owned_io_stats_;

  /** @brief Closed flag owned by the event-loop thread. */
  bool closed_ = false;

  /** @brief First close reason recorded for diagnostics and tests. */
  ConnectionCloseReason close_reason_ = ConnectionCloseReason::None;
};

}  // namespace xrpc
