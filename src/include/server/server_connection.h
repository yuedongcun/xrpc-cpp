/** @file server_connection.h @brief Declares one server-side RPC connection state machine. */

#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/task.h"
#include "io/socket.h"
#include "io/uring_context.h"
#include "protocol/frame_codec.h"
#include "protocol/rpc_envelope.h"
#include "server/connection_backpressure.h"
#include "server/rpc_frame_stream.h"
#include "server/worker_pool.h"

namespace xrpc {

class DispatchMailbox;
class ConnectionIoLoop;
class ServiceRegistry;

struct ServerConnectionConfig final {
  ConnectionBackpressureLimits limits_;

  ProtocolLimits protocol_limits_;
};

/**
 * @brief Owns the state machine for one server-side RPC connection.
 *
 * All mutable connection state is confined to its `UringContext` thread.
 * Worker threads never access it directly: they return encoded completions
 * through `DispatchMailbox`, which invokes the completion methods on this
 * connection's I/O thread.
 */
class ServerConnection final : public std::enable_shared_from_this<ServerConnection> {
 public:
  ServerConnection(io::UringContext &context, ServiceRegistry &registry, WorkerPool &worker_pool,
                   DispatchMailbox &mailbox, io::Socket socket, ServerConnectionConfig config,
                   std::function<void()> on_closed);

  ~ServerConnection();

  ServerConnection(const ServerConnection &) = delete;
  auto operator=(const ServerConnection &) -> ServerConnection & = delete;

  ServerConnection(ServerConnection &&) noexcept = delete;
  auto operator=(ServerConnection &&) noexcept -> ServerConnection & = delete;

  /**
   * @brief Starts the connection's read and write coroutines.
   *
   * The owning `ConnectionIoLoop` calls this once after taking ownership of
   * the connection. Both coroutines then remain alive until `Close()` wakes or
   * cancels their current wait and they return.
   */
  void Start();

  void Close();

  void BeginDrain();

  [[nodiscard]] auto IsClosed() const -> bool { return state_ == State::Closed; }

 private:
  friend class ConnectionIoLoop;
  friend class DispatchMailbox;

  enum class State : std::uint8_t {
    Active,
    Draining,
    Closed,
  };

  class WriteQueueAwaiter final {
   public:
    explicit WriteQueueAwaiter(ServerConnection &connection) : connection_(connection) {}

    [[nodiscard]] auto await_ready() const noexcept -> bool;

    void await_suspend(std::coroutine_handle<> continuation) const noexcept;

    void await_resume() const noexcept {}

   private:
    ServerConnection &connection_;
  };

  /** @brief Reads and decodes requests until the connection stops receiving. */
  [[nodiscard]] auto ReadLoop() -> runtime::Task<void>;

  void OnEncodedDispatchComplete(std::string &&response_bytes, std::size_t completed_jobs);

  void OnDispatchEncodeFailure(std::size_t completed_jobs);

  void ReleaseDispatchJobs(std::size_t completed_jobs);

  [[nodiscard]] auto EnqueueWrite(std::string bytes) -> bool;

  [[nodiscard]] auto TryReserveWriteBytes(std::size_t bytes) -> bool;

  void ReleaseWriteBytes(std::size_t bytes);

  /**
   * @brief Waits for and sends responses until the connection closes.
   *
   * An empty queue suspends this coroutine on `WriteQueueAwaiter`; socket
   * backpressure suspends it on `UringContext::Send()`.
   */
  [[nodiscard]] auto WriteLoop() -> runtime::Task<void>;

  void WakeWriteLoop();

  /** @return true after both connection I/O coroutines have completed. */
  [[nodiscard]] auto CanBeCollected() const -> bool;

  [[nodiscard]] auto HandleFeedResult(FrameStreamFeedResult &&feed) -> bool;

  [[nodiscard]] auto SubmitDispatchBatch(std::vector<RequestEnvelope> requests) -> bool;

  void ExecuteDispatchBatchOnWorker(const std::weak_ptr<ServerConnection> &target,
                                    std::vector<RequestEnvelope> &requests);

  [[nodiscard]] auto RejectForBackpressure(RequestEnvelope &&request, std::string message) -> bool;

  [[nodiscard]] auto EncodeResponseOnWorker(ResponseEnvelope &&response) const -> std::string;

  void TryFinishDrain();

  io::UringContext *context_;

  DispatchMailbox *mailbox_ = nullptr;

  WorkerPool *worker_pool_ = nullptr;

  ServiceRegistry *registry_ = nullptr;

  RpcFrameStream frame_stream_;

  ProtocolLimits protocol_limits_;

  io::Socket socket_;

  std::string read_buffer_;

  std::deque<std::string> write_queue_;

  std::coroutine_handle<> write_queue_waiter_;

  std::size_t inflight_requests_ = 0;

  std::size_t pending_write_bytes_ = 0;

  ConnectionBackpressureLimits limits_;

  std::function<void()> on_closed_;

  State state_ = State::Active;

  runtime::Task<void> read_loop_task_;

  runtime::Task<void> write_loop_task_;
};

}  // namespace xrpc
