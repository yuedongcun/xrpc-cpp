#include "client/tcp_transport.h"

#include <cerrno>
#include <cstring>
#include <utility>
#include <vector>

#include "common/xrpc_exception.h"

#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "io/socket_error.h"
#include "protocol/protocol_message.h"
#include "rpc/protocol_adapter.h"

namespace xrpc {
namespace {

/**
 * @brief Builds a deadline-exceeded status for a socket action.
 *
 * @param action Action name used in the diagnostic message.
 * @return Public timeout status.
 */
auto TimeoutStatus(std::string_view action) -> Status {
  return {StatusCode::DeadlineExceeded, std::string(action) + " timed out"};
}

/**
 * @brief Maps a socket errno value to the public transport status contract.
 *
 * @param action Socket action that failed.
 * @param error Positive errno value captured immediately after the syscall.
 * @return Deadline status for timeout-like errors, otherwise unavailable status.
 */
auto IoStatus(std::string_view action, int error) -> Status {
  if (error == EAGAIN || error == EWOULDBLOCK) {
    return TimeoutStatus(action);
  }
  return {StatusCode::Unavailable, std::string(action) + " failed: " + std::strerror(error)};
}

/**
 * @brief Builds the status used when the server closes the client connection.
 *
 * @param action Socket action observing peer close.
 * @return Unavailable status.
 */
auto PeerClosedStatus(std::string_view action) -> Status {
  return {StatusCode::Unavailable, std::string(action) + " failed: peer closed connection"};
}

/**
 * @brief Computes the remaining timeout for a socket write operation.
 *
 * @param options Effective call options containing either a duration or absolute deadline.
 * @return Remaining positive duration, or the configured timeout when no deadline is installed.
 */
auto RemainingTimeout(const EffectiveCallOptions &options) -> std::chrono::milliseconds {
  if (!options.deadline_.has_value()) {
    return options.timeout_;
  }

  const auto now = std::chrono::steady_clock::now();
  if (now >= *options.deadline_) {
    return std::chrono::milliseconds(1);
  }

  return std::chrono::duration_cast<std::chrono::milliseconds>(*options.deadline_ - now);
}

/**
 * @brief Converts a C++ timeout duration to a POSIX socket `timeval`.
 *
 * @param timeout Timeout duration. Non-positive values map to a zero timeval.
 * @return `timeval` accepted by `setsockopt`.
 */
auto ToTimeval(std::chrono::milliseconds timeout) -> timeval {
  timeval tv{};
  if (timeout <= std::chrono::milliseconds::zero()) {
    return tv;
  }

  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
  const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(timeout - seconds);
  tv.tv_sec = static_cast<time_t>(seconds.count());
  tv.tv_usec = static_cast<suseconds_t>(micros.count());
  if (tv.tv_sec == 0 && tv.tv_usec == 0) {
    tv.tv_usec = 1;
  }
  return tv;
}

/**
 * @brief Applies a send timeout to the connected socket.
 *
 * @param fd Connected socket file descriptor.
 * @param timeout Send timeout to apply.
 * @return Empty on success, otherwise the mapped failure status.
 */
auto SetSendTimeout(int fd, std::chrono::milliseconds timeout) -> std::optional<Status> {
  const timeval tv = ToTimeval(timeout);
  if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
    return IoStatus("setsockopt(SO_SNDTIMEO)", errno);
  }
  return std::nullopt;
}

/**
 * @brief Writes a complete encoded frame to a blocking socket.
 *
 * @param fd Connected socket file descriptor.
 * @param frame Encoded protocol frame.
 * @return Empty on success, otherwise timeout/unavailable status.
 */
auto SendAll(int fd, std::string_view frame) -> std::optional<Status> {
  std::size_t written = 0;
  while (written < frame.size()) {
    const ssize_t sent = ::send(fd, frame.data() + written, frame.size() - written, MSG_NOSIGNAL);
    if (sent > 0) {
      written += static_cast<std::size_t>(sent);
      continue;
    }
    if (sent == 0) {
      return PeerClosedStatus("send");
    }
    if (errno == EINTR) {
      continue;
    }
    return IoStatus("send", errno);
  }
  return std::nullopt;
}

/**
 * @brief Receives bytes from a socket, retrying interrupted syscalls.
 *
 * @param fd Connected socket file descriptor.
 * @param buffer Destination buffer.
 * @param buffer_size Destination capacity.
 * @return Bytes read, zero on peer close, or -1 with `errno` set.
 */
auto RecvSome(int fd, char *buffer, std::size_t buffer_size) -> ssize_t {
  while (true) {
    const ssize_t received = ::recv(fd, buffer, buffer_size, 0);
    if (received >= 0) {
      return received;
    }
    if (errno == EINTR) {
      continue;
    }
    return -1;
  }
}

/**
 * @brief Converts a successfully decoded protocol response into the raw client model.
 *
 * @param decoded Frame decoder result with optional response payload.
 * @return Raw response when the frame contained a response message.
 */
auto DecodeResponse(const DecodeResult &decoded) -> std::optional<RawResponse> {
  if (!decoded.response_.has_value()) {
    return std::nullopt;
  }
  return ToRawResponse(*decoded.response_);
}

}  // namespace

/**
 * @brief Creates a TCP transport for one resolved endpoint.
 *
 * @param host Endpoint host name or numeric address.
 * @param port Endpoint TCP port.
 * @param protocol_limits Frame and payload limits used by encoder/decoder.
 * @param max_inflight_per_endpoint Maximum pending calls allowed on this connection.
 */
TcpTransport::TcpTransport(std::string host, std::uint16_t port, ProtocolLimits protocol_limits,
                           std::size_t max_inflight_per_endpoint)
    : host_(std::move(host)),
      port_(port),
      protocol_limits_(protocol_limits),
      max_inflight_per_endpoint_(max_inflight_per_endpoint) {}

/** @brief Closes the socket and fails pending calls before destruction completes. */
TcpTransport::~TcpTransport() { Close(); }

/**
 * @brief Sends one raw RPC request and waits for the matching response.
 *
 * The pending call is registered before the write so the reader thread can complete even a very
 * fast response without racing the caller. Write failures after a partial send are marked
 * `MaybeSent` to prevent unsafe client retries.
 *
 * @param request Raw request metadata and payload.
 * @param options Effective deadline and routing options.
 * @return Successful response or failure with request commit state.
 */
auto TcpTransport::Call(const RawRequest &request, const EffectiveCallOptions &options) -> RawCallResult {
  auto fail = [&request](Status status, RequestCommitState state) -> RawCallResult {
    return MakeCallFailure(std::move(status), state);
  };

  FrameCodec codec(protocol_limits_);
  std::string request_frame;
  try {
    request_frame = codec.EncodeRequest(ToProtocolRequest(request));
  } catch (...) {
    return fail(CaughtExceptionToStatus("failed to encode request frame"), RequestCommitState::NotSent);
  }

  try {
    EnsureConnectedWithTimeout(options.timeout_);
  } catch (const io::SocketError &error) {
    return fail(error.status(), RequestCommitState::NotSent);
  } catch (...) {
    return fail(CaughtExceptionToStatus("transport connect failed"), RequestCommitState::NotSent);
  }

  auto pending = std::make_shared<PendingCall>();
  if (!TryRegisterPending(request.request_id_, pending)) {
    return fail({StatusCode::ResourceExhausted, "client max in-flight per endpoint exceeded"},
                RequestCommitState::NotSent);
  }

  // Register before writing so a very fast response cannot race the caller and
  // arrive before the pending map contains this request id.
  if (std::optional<RawCallResult> write_failure = WriteRequestFrame(request.request_id_, request_frame, options)) {
    return std::move(*write_failure);
  }

  return WaitForResult(pending, request.request_id_, options);
}

/**
 * @brief Opens the socket and starts the background response reader when disconnected.
 *
 * @param timeout Connect timeout.
 * @throws io::SocketError when the socket cannot connect.
 */
void TcpTransport::EnsureConnectedWithTimeout(std::chrono::milliseconds timeout) {
  std::unique_lock lock(state_mutex_);
  if (socket_.valid()) {
    return;
  }

  JoinReaderIfStopped(lock);

  io::Socket socket;
  socket.Connect(host_, port_, timeout);
  socket_ = std::move(socket);
  const int fd = socket_.fd();
  reader_thread_ = std::jthread([this, fd]() -> void { ReaderLoop(fd); });
}

/** @return Connected socket file descriptor, or -1 when disconnected. */
auto TcpTransport::ConnectedFd() const -> int {
  std::lock_guard lock(state_mutex_);
  return socket_.valid() ? socket_.fd() : -1;
}

/**
 * @brief Joins a previous reader thread before a reconnect attempt.
 *
 * @param lock State mutex lock held by the caller and temporarily released while joining.
 */
void TcpTransport::JoinReaderIfStopped(std::unique_lock<std::mutex> &lock) {
  if (!reader_thread_.joinable()) {
    return;
  }
  if (reader_thread_.get_id() == std::this_thread::get_id()) {
    return;
  }

  std::jthread reader = std::move(reader_thread_);
  lock.unlock();
  reader.join();
  lock.lock();
}

/**
 * @brief Closes the transport and completes all pending calls with unavailable status.
 *
 * Close is safe to call from user threads and avoids joining the reader when invoked by that reader.
 */
void TcpTransport::Close() {
  std::jthread reader;
  {
    std::lock_guard lock(state_mutex_);
    CloseSocketLocked();
    if (reader_thread_.joinable() && reader_thread_.get_id() != std::this_thread::get_id()) {
      reader = std::move(reader_thread_);
    }
  }

  FailAllPending({StatusCode::Unavailable, "transport closed"}, RequestCommitState::MaybeSent);

  if (reader.joinable()) {
    reader.join();
  }
}

/**
 * @brief Reads response frames and completes pending calls until the socket fails or closes.
 *
 * @param fd File descriptor captured when the reader thread started; used to avoid closing a newer
 * connection after reconnect.
 */
void TcpTransport::ReaderLoop(int fd) {
  FrameCodec codec(protocol_limits_);
  std::string buffer;
  char chunk[4096];

  while (true) {
    // Decode Loop
    while (true) {
      const DecodeResult decoded = codec.TryDecode(buffer);
      if (decoded.error_ == ProtocolError::NeedMoreData) {
        break;
      }
      if (decoded.error_ != ProtocolError::Ok) {
        CloseFromReader(fd, {StatusCode::DataLoss, "failed to decode response frame"});
        return;
      }

      std::optional<RawResponse> response = DecodeResponse(decoded);
      if (!response.has_value()) {
        CloseFromReader(fd, {StatusCode::DataLoss, "response frame did not contain ProtocolResponse"});
        return;
      }

      const std::uint64_t request_id = response->request_id_;
      CompletePending(request_id, MakeCallSuccess(std::move(*response)));
      buffer.erase(0, decoded.consumed_);
    }

    const ssize_t received = RecvSome(fd, chunk, sizeof(chunk));
    if (received > 0) {
      buffer.append(chunk, static_cast<std::size_t>(received));
      continue;
    }
    if (received == 0) {
      CloseFromReader(fd, PeerClosedStatus("recv"));
      return;
    }

    CloseFromReader(fd, IoStatus("recv", errno));
    return;
  }
}

/**
 * @brief Closes the socket observed by the reader and fails all pending calls.
 *
 * @param fd File descriptor associated with the failing reader thread.
 * @param status Failure status to deliver to pending callers.
 */
void TcpTransport::CloseFromReader(int fd, const Status &status) {
  {
    std::lock_guard lock(state_mutex_);
    if (socket_.valid() && socket_.fd() == fd) {
      CloseSocketLocked();
    }
  }
  FailAllPending(status, RequestCommitState::MaybeSent);
}

/** @brief Shuts down and closes the connected socket while `state_mutex_` is held. */
void TcpTransport::CloseSocketLocked() {
  if (!socket_.valid()) {
    return;
  }
  try {
    socket_.ShutdownReadWrite();
  } catch (...) {
    // Shutdown only wakes blocked I/O; closing the descriptor remains required.
  }
  socket_.Close();
}

/**
 * @brief Adds a pending call to the request-id map if endpoint capacity allows it.
 *
 * @param request_id Request id used to match the response frame.
 * @param pending Shared wait state owned by caller and reader thread.
 * @return true when the call was accepted, false when the per-endpoint limit is full.
 */
auto TcpTransport::TryRegisterPending(std::uint64_t request_id, std::shared_ptr<PendingCall> pending) -> bool {
  pending->request_id_ = request_id;
  std::lock_guard lock(pending_mutex_);
  if (pending_calls_.size() >= max_inflight_per_endpoint_) {
    return false;
  }
  pending_calls_[request_id] = std::move(pending);
  return true;
}

/**
 * @brief Removes a pending call from the request-id map.
 *
 * @param request_id Request id to remove.
 * @return true when the request was still pending.
 */
auto TcpTransport::RemovePending(std::uint64_t request_id) -> bool {
  std::lock_guard lock(pending_mutex_);
  return pending_calls_.erase(request_id) > 0;
}

/**
 * @brief Completes one pending call and wakes its waiting caller.
 *
 * @param request_id Request id carried by the response frame.
 * @param result Final call result.
 */
void TcpTransport::CompletePending(std::uint64_t request_id, RawCallResult result) {
  std::shared_ptr<PendingCall> pending;
  {
    std::lock_guard lock(pending_mutex_);
    auto it = pending_calls_.find(request_id);
    if (it == pending_calls_.end()) {
      return;
    }
    pending = std::move(it->second);
    pending_calls_.erase(it);
  }

  {
    std::lock_guard lock(pending->mutex_);
    pending->result_.emplace(std::move(result));
  }
  pending->cv_.notify_one();
}

/**
 * @brief Completes every pending call with the same transport failure.
 *
 * @param status Failure status to deliver.
 * @param commit_state Whether requests may have reached the server.
 */
void TcpTransport::FailAllPending(const Status &status, RequestCommitState commit_state) {
  std::vector<std::shared_ptr<PendingCall>> pending_calls;
  {
    std::lock_guard lock(pending_mutex_);
    pending_calls.reserve(pending_calls_.size());
    for (auto &[_, pending] : pending_calls_) {
      pending_calls.push_back(std::move(pending));
    }
    pending_calls_.clear();
  }

  for (const auto &pending : pending_calls) {
    {
      std::lock_guard lock(pending->mutex_);
      pending->result_.emplace(CallFailure{.status_ = status, .commit_state_ = commit_state});
    }
    pending->cv_.notify_one();
  }
}

/**
 * @brief Waits for the reader thread or local deadline to complete a pending call.
 *
 * If a deadline fires while the request is still pending, the call is removed from the map and the
 * result is `MaybeSent` because the request frame was already written.
 *
 * @param pending Shared wait state registered in `pending_calls_`.
 * @param request_id Request id to remove on local timeout.
 * @param options Effective deadline information.
 * @return Reader-provided result or local deadline failure.
 */
auto TcpTransport::WaitForResult(const std::shared_ptr<PendingCall> &pending, std::uint64_t request_id,
                                 const EffectiveCallOptions &options) -> RawCallResult {
  std::unique_lock lock(pending->mutex_);
  const auto has_result = [&pending]() -> bool { return pending->result_.has_value(); };

  if (options.deadline_.has_value()) {
    if (!pending->cv_.wait_until(lock, *options.deadline_, has_result)) {
      if (RemovePending(request_id)) {
        // The request was written, so a late server execution is possible even
        // though this caller timed out locally.
        return MakeCallFailure({StatusCode::DeadlineExceeded, "RPC deadline exceeded"}, RequestCommitState::MaybeSent);
      }
      pending->cv_.wait(lock, has_result);
    }
  } else {
    pending->cv_.wait(lock, has_result);
  }

  return std::move(*pending->result_);
}

/**
 * @brief Writes an encoded request frame and maps write failures to call failures.
 *
 * @param request_id Request id used to remove the pending call on pre-send failures.
 * @param frame Encoded request frame.
 * @param options Effective deadline information.
 * @return Empty on successful write, otherwise the final call failure.
 */
auto TcpTransport::WriteRequestFrame(std::uint64_t request_id, std::string_view frame,
                                     const EffectiveCallOptions &options) -> std::optional<RawCallResult> {
  std::lock_guard write_lock(write_mutex_);

  const int fd = ConnectedFd();
  if (fd < 0) {
    (void)RemovePending(request_id);
    return MakeCallFailure({StatusCode::Unavailable, "transport not connected"}, RequestCommitState::NotSent);
  }

  if (std::optional<Status> timeout_status = SetSendTimeout(fd, RemainingTimeout(options))) {
    (void)RemovePending(request_id);
    return MakeCallFailure(std::move(*timeout_status), RequestCommitState::NotSent);
  }

  if (std::optional<Status> send_status = SendAll(fd, frame)) {
    (void)RemovePending(request_id);
    Close();
    // send() may fail after writing a prefix of the frame. Treat it as MaybeSent
    // so RpcClient::Impl does not retry and risk duplicate execution.
    return MakeCallFailure(std::move(*send_status), RequestCommitState::MaybeSent);
  }

  return std::nullopt;
}

}  // namespace xrpc
