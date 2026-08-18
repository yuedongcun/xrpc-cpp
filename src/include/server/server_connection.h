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
#include "server/connection_backpressure.h"
#include "server/rpc_frame_stream.h"
#include "server/thread_pool_executor.h"

namespace xrpc {

class DispatchMailbox;
class ServiceRegistry;

struct ServerConnectionConfig final {
  ConnectionBackpressureLimits limits_;

  ProtocolLimits protocol_limits_;
};

class ServerConnection final : public std::enable_shared_from_this<ServerConnection> {
 public:
  ServerConnection(io::UringContext &context, ServiceRegistry &registry, ThreadPoolExecutor &executor,
                   DispatchMailbox &mailbox, io::Socket socket, ServerConnectionConfig config,
                   std::function<void()> on_closed);

  ~ServerConnection();

  ServerConnection(const ServerConnection &) = delete;
  auto operator=(const ServerConnection &) -> ServerConnection & = delete;

  ServerConnection(ServerConnection &&) noexcept = delete;
  auto operator=(ServerConnection &&) noexcept -> ServerConnection & = delete;

  [[nodiscard]] auto Run() -> runtime::Task<void>;

  void Close();

  void BeginDrain();

  [[nodiscard]] auto IsClosed() const -> bool { return state_ == State::Closed; }

 private:
  friend class DispatchMailbox;

  enum class State : std::uint8_t {
    Active,
    Draining,
    Closed,
  };

  void OnEncodedDispatchComplete(std::string &&response_bytes, std::size_t completed_jobs);

  void OnDispatchEncodeFailure(std::size_t completed_jobs);

  void ReleaseDispatchJobs(std::size_t completed_jobs);

  [[nodiscard]] auto EnqueueWrite(std::string bytes) -> bool;

  [[nodiscard]] auto TryReserveWriteBytes(std::size_t bytes) -> bool;

  void ReleaseWriteBytes(std::size_t bytes);

  [[nodiscard]] auto DrainWriteQueue() -> runtime::Task<void>;

  [[nodiscard]] auto HandleFeedResult(FrameStreamFeedResult &&feed) -> bool;

  [[nodiscard]] auto SubmitDispatchBatch(std::vector<RawRequest> requests) -> bool;

  void ExecuteDispatchBatchOnWorker(const std::weak_ptr<ServerConnection> &target, std::vector<RawRequest> &requests);

  [[nodiscard]] auto RejectRequestDueToBackpressure(RawRequest &&request, std::string message) -> bool;

  [[nodiscard]] auto EncodeResponseOnWorker(RawResponse &&response) const -> std::string;

  void TryFinishDrain();

  io::UringContext *context_;

  DispatchMailbox *mailbox_ = nullptr;

  ThreadPoolExecutor *executor_ = nullptr;

  ServiceRegistry *registry_ = nullptr;

  RpcFrameStream frame_stream_;

  ProtocolLimits protocol_limits_;

  io::Socket socket_;

  std::string read_buffer_;

  std::deque<std::string> write_queue_;

  std::optional<runtime::Task<void>> write_task_;

  bool write_in_progress_ = false;

  std::size_t inflight_requests_ = 0;

  std::size_t pending_write_bytes_ = 0;

  ConnectionBackpressureLimits limits_;

  std::function<void()> on_closed_;

  State state_ = State::Active;
};

}  // namespace xrpc
