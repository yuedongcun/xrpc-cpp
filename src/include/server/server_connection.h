#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/task.h"
#include "io/socket.h"
#include "io/uring_context.h"
#include "protocol/frame_codec.h"
#include "protocol/protocol_message.h"
#include "server/rpc_frame_stream.h"
#include "server/server_backpressure.h"
#include "server/thread_pool_executor.h"

namespace xrpc {

class DispatchMailbox;
class ServiceRegistry;

/**
 * @brief Static limits applied to one accepted connection.
 */
struct ServerConnectionConfig final {
  /** @brief Per-connection inflight and write-queue limits. */
  ConnectionBackpressureLimits limits_;

  /** @brief Frame limits applied by the connection's `RpcFrameStream`. */
  ProtocolLimits protocol_limits_;
};

/**
 * @brief Event-loop-owned server-side TCP connection.
 *
 * Design note:
 * - Ownership: one shared `ServerConnection` owns its socket, frame stream, write queue, and outstanding dispatch
 *   accounting.
 * - Threading: socket/framing/write state stays on the `UringContext` thread; handlers run on `ThreadPoolExecutor`
 *   and return through `DispatchMailbox`.
 * - Backpressure: reads can continue only while inflight jobs and queued writes stay within `ServerConnectionConfig`
 *   limits.
 * - Shutdown: `BeginDrain()` stops reads and preserves admitted response writes; `Close()` performs the terminal
 *   socket transition on the event-loop thread.
 */
class ServerConnection final : public std::enable_shared_from_this<ServerConnection> {
 public:
  /** @brief Creates a connection from an accepted socket and injected runtime dependencies. */
  ServerConnection(io::UringContext &context, ServiceRegistry &registry, ThreadPoolExecutor &executor,
                   DispatchMailbox &mailbox, io::Socket socket, ServerConnectionConfig config,
                   std::function<void()> on_closed);

  /** @brief Releases owned socket and coroutine task state. */
  ~ServerConnection();

  ServerConnection(const ServerConnection &) = delete;
  auto operator=(const ServerConnection &) -> ServerConnection & = delete;

  ServerConnection(ServerConnection &&) noexcept = delete;
  auto operator=(ServerConnection &&) noexcept -> ServerConnection & = delete;

  /**
   * @brief Runs the connection read loop coroutine.
   *
   * @return Task that completes when the connection closes and in-flight work has been accounted for.
   */
  [[nodiscard]] auto Run() -> runtime::Task<void>;

  /** @brief Closes the connection and cancels pending socket operations. */
  void Close();

  /** @brief Stops reading new requests while allowing admitted responses to drain. */
  void BeginDrain();

  /** @return true after the connection has entered the closed state. */
  [[nodiscard]] auto IsClosed() const -> bool { return state_ == State::Closed; }

 private:
  friend class DispatchMailbox;

  enum class State : std::uint8_t {
    Active,
    Draining,
    Closed,
  };

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
  [[nodiscard]] auto HandleFeedResult(FrameStreamFeedResult &&feed) -> bool;

  /** @brief Submits a batch of decoded requests to the worker pool. */
  [[nodiscard]] auto SubmitDispatchBatch(std::vector<RawRequest> requests) -> bool;

  /** @brief Dispatches and encodes one admitted batch on a worker thread. */
  void ExecuteDispatchBatchOnWorker(const std::weak_ptr<ServerConnection> &target, std::vector<RawRequest> &requests);

  /** @brief Produces an immediate resource-exhausted response for a rejected request. */
  [[nodiscard]] auto RejectRequestDueToBackpressure(RawRequest &&request, std::string message) -> bool;

  /** @brief Encodes a raw response on a worker before handing bytes to the event loop. */
  [[nodiscard]] auto EncodeResponseOnWorker(RawResponse &&response) const -> std::string;

  /** @brief Finishes graceful drain after admitted work and response writes complete. */
  void TryFinishDrain();

  /** @brief Event loop that owns this connection's socket operations. */
  io::UringContext *context_;

  /** @brief Worker-to-event-loop dispatch mailbox owned by the connection I/O loop. */
  DispatchMailbox *mailbox_ = nullptr;

  /** @brief Worker pool used for handler dispatch and response encoding. */
  ThreadPoolExecutor *executor_ = nullptr;

  /** @brief Registered RPC methods dispatched by worker threads. */
  ServiceRegistry *registry_ = nullptr;

  /** @brief Per-connection RPC framing state and decode buffer owner. */
  RpcFrameStream frame_stream_;

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

  /** @brief True while `DrainWriteQueue()` owns the socket send path. */
  bool write_in_progress_ = false;

  /** @brief Number of handler jobs submitted to workers but not yet completed on the loop. */
  std::size_t inflight_requests_ = 0;

  /** @brief Total bytes currently queued in `write_queue_`. */
  std::size_t pending_write_bytes_ = 0;

  /** @brief Per-connection backpressure limits. */
  ConnectionBackpressureLimits limits_;

  /** @brief Owner callback invoked exactly once when the connection closes. */
  std::function<void()> on_closed_;

  /** @brief Lifecycle state modified only by the owning event-loop thread. */
  State state_ = State::Active;
};

}  // namespace xrpc
