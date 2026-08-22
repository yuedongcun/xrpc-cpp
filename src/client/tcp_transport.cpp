/** @file tcp_transport.cpp @brief Implements the blocking TCP client transport. */

#include "client/tcp_transport.h"

#include <cerrno>
#include <cstring>
#include <exception>
#include <utility>
#include <vector>

#include "common/xrpc_exception.h"

#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "protocol/rpc_envelope.h"

namespace xrpc {
namespace {

auto TimeoutStatus(std::string_view action) -> Status {
  return {StatusCode::DeadlineExceeded, std::string(action) + " timed out"};
}

auto IoStatus(std::string_view action, int error) -> Status {
  if (error == EAGAIN || error == EWOULDBLOCK) {
    return TimeoutStatus(action);
  }
  return {StatusCode::Unavailable, std::string(action) + " failed: " + std::strerror(error)};
}

auto PeerClosedStatus(std::string_view action) -> Status {
  return {StatusCode::Unavailable, std::string(action) + " failed: peer closed connection"};
}

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

auto SetSendTimeout(int fd, std::chrono::milliseconds timeout) -> std::optional<Status> {
  const timeval tv = ToTimeval(timeout);
  if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
    return IoStatus("setsockopt(SO_SNDTIMEO)", errno);
  }
  return std::nullopt;
}

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

}  // namespace

TcpTransport::TcpTransport(std::string host, std::uint16_t port, ProtocolLimits protocol_limits,
                           std::size_t max_inflight_per_endpoint)
    : host_(std::move(host)),
      port_(port),
      protocol_limits_(protocol_limits),
      max_inflight_per_endpoint_(max_inflight_per_endpoint) {}

TcpTransport::~TcpTransport() { Close(); }

auto TcpTransport::Call(const RequestEnvelope &request, const EffectiveCallOptions &options) -> CallAttemptResult {
  auto fail = [&request](Status status, RequestCommitState state) -> CallAttemptResult {
    return MakeCallFailure(std::move(status), state);
  };

  FrameCodec codec(protocol_limits_);
  std::string request_frame;
  try {
    request_frame = codec.Encode(request);
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

  if (std::optional<CallAttemptResult> write_failure = WriteRequestFrame(request.request_id_, request_frame, options)) {
    return std::move(*write_failure);
  }

  return WaitForResult(pending, request.request_id_, options);
}

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

auto TcpTransport::ConnectedFd() const -> int {
  std::lock_guard lock(state_mutex_);
  return socket_.valid() ? socket_.fd() : -1;
}

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

void TcpTransport::ReaderLoop(int fd) {
  FrameCodec codec(protocol_limits_);
  std::string buffer;
  char chunk[4096];

  while (true) {
    while (true) {
      FrameDecodeResult decoded = codec.Decode(buffer);
      if (decoded.error_ == ProtocolError::NeedMoreData) {
        break;
      }
      if (decoded.error_ != ProtocolError::Ok) {
        CloseFromReader(fd, {StatusCode::DataLoss, "failed to decode response frame"});
        return;
      }

      if (!decoded.response_.has_value()) {
        CloseFromReader(fd, {StatusCode::DataLoss, "response frame did not contain an RPC response"});
        return;
      }

      const std::uint64_t request_id = decoded.response_->request_id_;
      CompletePending(request_id, MakeCallSuccess(std::move(*decoded.response_)));
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

void TcpTransport::CloseFromReader(int fd, const Status &status) {
  {
    std::lock_guard lock(state_mutex_);
    if (socket_.valid() && socket_.fd() == fd) {
      CloseSocketLocked();
    }
  }
  FailAllPending(status, RequestCommitState::MaybeSent);
}

void TcpTransport::CloseSocketLocked() {
  if (!socket_.valid()) {
    return;
  }
  try {
    socket_.ShutdownReadWrite();
  } catch (...) {
    const std::exception_ptr ignored = std::current_exception();
    (void)ignored;
  }
  socket_.Close();
}

auto TcpTransport::TryRegisterPending(std::uint64_t request_id, std::shared_ptr<PendingCall> pending) -> bool {
  pending->request_id_ = request_id;
  std::lock_guard lock(pending_mutex_);
  if (pending_calls_.size() >= max_inflight_per_endpoint_) {
    return false;
  }
  pending_calls_[request_id] = std::move(pending);
  return true;
}

auto TcpTransport::RemovePending(std::uint64_t request_id) -> bool {
  std::lock_guard lock(pending_mutex_);
  return pending_calls_.erase(request_id) > 0;
}

void TcpTransport::CompletePending(std::uint64_t request_id, CallAttemptResult result) {
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

auto TcpTransport::WaitForResult(const std::shared_ptr<PendingCall> &pending, std::uint64_t request_id,
                                 const EffectiveCallOptions &options) -> CallAttemptResult {
  std::unique_lock lock(pending->mutex_);
  const auto has_result = [&pending]() -> bool { return pending->result_.has_value(); };

  if (options.deadline_.has_value()) {
    if (!pending->cv_.wait_until(lock, *options.deadline_, has_result)) {
      if (RemovePending(request_id)) {
        return MakeCallFailure({StatusCode::DeadlineExceeded, "RPC deadline exceeded"}, RequestCommitState::MaybeSent);
      }
      pending->cv_.wait(lock, has_result);
    }
  } else {
    pending->cv_.wait(lock, has_result);
  }

  return std::move(*pending->result_);
}

auto TcpTransport::WriteRequestFrame(std::uint64_t request_id, std::string_view frame,
                                     const EffectiveCallOptions &options) -> std::optional<CallAttemptResult> {
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

    return MakeCallFailure(std::move(*send_status), RequestCommitState::MaybeSent);
  }

  return std::nullopt;
}

}  // namespace xrpc
