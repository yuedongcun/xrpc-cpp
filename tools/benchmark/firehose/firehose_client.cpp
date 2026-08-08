#include <protocol/xrpc/xrpc_header.pb.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <latch>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "benchmark_stats.h"
#include "proto/echo.pb.h"
#include "protocol/fixed_header.h"
#include "protocol/message_type.h"

namespace xrpc::benchmark {

namespace {

constexpr std::string_view SERVICE_NAME = "BenchmarkService";
constexpr std::string_view METHOD_NAME = "Echo";
constexpr std::size_t SOCKET_BUFFER_SIZE = 64U * 1024U;
constexpr std::size_t MAX_WRITE_BATCH_BYTES = 64U * 1024U;
constexpr int MAX_EPOLL_EVENTS = 256;

struct FirehoseConfig final {
  std::string host_ = "127.0.0.1";
  std::uint16_t port_ = 9010;
  std::uint64_t duration_s_ = 0;
  std::size_t payload_size_ = 64;
  std::size_t firehose_connections_ = 1;
  std::size_t firehose_inflight_ = 0;
  std::size_t firehose_io_threads_ = 0;
};

struct FirehoseSlot final {
  std::uint64_t generation_ = 1;
  std::uint64_t request_id_ = 0;
  std::chrono::steady_clock::time_point begin_;
  bool in_flight_ = false;
};

struct DecodedFirehoseResponse final {
  std::uint64_t request_id_ = 0;
  bool ok_ = false;
  std::size_t consumed_ = 0;
};

struct BenchmarkPayload final {
  std::string request_;
  std::string expected_response_;
};

auto Percentile(const std::vector<std::chrono::nanoseconds> &sorted, double ratio) -> std::chrono::nanoseconds {
  if (sorted.empty()) {
    return std::chrono::nanoseconds(0);
  }
  const auto idx = static_cast<std::size_t>((sorted.size() - 1) * ratio);
  return sorted[idx];
}

auto ParseUnsigned(std::string_view value, const char *name) -> std::uint64_t {
  std::uint64_t result = 0;
  const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
  if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
    throw std::invalid_argument(std::string("invalid value for ") + name);
  }
  return result;
}

void RequireKeyValue(std::string_view arg) {
  if (!arg.starts_with("--") || arg.find('=') == std::string_view::npos) {
    throw std::invalid_argument("arguments must use --key=value format");
  }
}

void ParseArg(FirehoseConfig &config, std::string_view arg) {
  RequireKeyValue(arg);
  const std::size_t eq = arg.find('=');
  const std::string_view key = arg.substr(2, eq - 2);
  const std::string_view value = arg.substr(eq + 1);

  if (key == "host") {
    config.host_ = std::string(value);
  } else if (key == "port") {
    config.port_ = static_cast<std::uint16_t>(ParseUnsigned(value, "port"));
  } else if (key == "duration_s") {
    config.duration_s_ = ParseUnsigned(value, "duration_s");
  } else if (key == "payload_size") {
    config.payload_size_ = static_cast<std::size_t>(ParseUnsigned(value, "payload_size"));
  } else if (key == "connections") {
    config.firehose_connections_ = static_cast<std::size_t>(ParseUnsigned(value, "connections"));
  } else if (key == "inflight") {
    config.firehose_inflight_ = static_cast<std::size_t>(ParseUnsigned(value, "inflight"));
  } else if (key == "io_threads") {
    config.firehose_io_threads_ = static_cast<std::size_t>(ParseUnsigned(value, "io_threads"));
  } else {
    throw std::invalid_argument(std::string("unknown argument: --") + std::string(key));
  }
}

auto ParseConfig(int argc, char **argv) -> FirehoseConfig {
  FirehoseConfig config;
  for (int i = 1; i < argc; ++i) {
    ParseArg(config, argv[i]);
  }
  if (config.port_ == 0) {
    throw std::invalid_argument("port must be greater than 0");
  }
  if (config.duration_s_ == 0) {
    throw std::invalid_argument("duration_s must be greater than 0");
  }
  if (config.payload_size_ == 0) {
    throw std::invalid_argument("payload_size must be greater than 0");
  }
  if (config.firehose_connections_ == 0) {
    throw std::invalid_argument("connections must be greater than 0");
  }
  if (config.firehose_inflight_ == 0) {
    throw std::invalid_argument("inflight must be greater than 0");
  }
  if (config.firehose_inflight_ < config.firehose_connections_) {
    throw std::invalid_argument("inflight must be greater than or equal to connections");
  }
  return config;
}

auto Usage(const char *program) -> std::string {
  return std::string("Usage: ") + program +
         " --host=IP --port=N --duration_s=N --payload_size=N --connections=N --inflight=N [--io_threads=N]";
}

auto ResolveFirehoseIoThreads(const FirehoseConfig &config) -> std::size_t {
  if (config.firehose_io_threads_ != 0) {
    return std::max<std::size_t>(1, std::min(config.firehose_io_threads_, config.firehose_connections_));
  }
  return 1;
}

auto ConnectBlocking(std::string_view host, std::uint16_t port) -> int {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error(std::string("socket failed: ") + std::strerror(errno));
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  const std::string host_name(host);
  if (::inet_pton(AF_INET, host_name.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    throw std::runtime_error("invalid host address");
  }

  while (true) {
    if (::connect(fd, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) == 0) {
      return fd;
    }
    if (errno == EINTR) {
      continue;
    }
    const int error = errno;
    ::close(fd);
    throw std::runtime_error(std::string("connect failed: ") + std::strerror(error));
  }
}

void SetNonBlocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    throw std::runtime_error(std::string("fcntl(F_GETFL) failed: ") + std::strerror(errno));
  }
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    throw std::runtime_error(std::string("fcntl(F_SETFL) failed: ") + std::strerror(errno));
  }
}

void CloseFd(int &fd) {
  if (fd < 0) {
    return;
  }
  (void)::shutdown(fd, SHUT_RDWR);
  (void)::close(fd);
  fd = -1;
}

auto BuildRequestHeaderBytes() -> std::string {
  RpcRequestHeader header;
  header.set_service_name(std::string(SERVICE_NAME));
  header.set_method_name(std::string(METHOD_NAME));

  std::string bytes;
  header.SerializeToString(&bytes);
  return bytes;
}

auto BuildBenchmarkPayload(const FirehoseConfig &config) -> BenchmarkPayload {
  const std::string message(config.payload_size_, 'x');
  EchoRequest request;
  request.set_message(message);
  EchoResponse response;
  response.set_message(message);
  return {
      .request_ = request.SerializeAsString(),
      .expected_response_ = response.SerializeAsString(),
  };
}

void AppendRequestFrame(std::uint64_t request_id, std::string_view request_header, std::string_view payload,
                        std::string &frame) {
  FixedHeader header;
  header.message_type_ = MessageType::Request;
  header.request_id_ = request_id;
  header.header_len_ = static_cast<std::uint32_t>(request_header.size());
  header.payload_len_ = static_cast<std::uint32_t>(payload.size());

  std::array<char, FixedHeader::SIZE> fixed_header{};
  FixedHeader::EncodeTo(header, fixed_header.data());
  frame.append(fixed_header.data(), fixed_header.size());
  frame.append(request_header);
  frame.append(payload);
}

auto TryDecodeResponse(std::string_view buffer, std::string_view expected_payload)
    -> std::optional<DecodedFirehoseResponse> {
  if (buffer.size() < FixedHeader::SIZE) {
    return std::nullopt;
  }

  std::optional<FixedHeader> header = FixedHeader::Decode(buffer.substr(0, FixedHeader::SIZE));
  if (!header.has_value()) {
    throw std::runtime_error("response contains an invalid fixed header");
  }
  if (header->version_ != FixedHeader::VERSION || header->message_type_ != MessageType::Response) {
    throw std::runtime_error("response contains an invalid message type");
  }

  const std::size_t total_size = FixedHeader::SIZE + static_cast<std::size_t>(header->header_len_) +
                                 static_cast<std::size_t>(header->payload_len_);
  if (buffer.size() < total_size) {
    return std::nullopt;
  }

  const std::string_view header_bytes = buffer.substr(FixedHeader::SIZE, header->header_len_);
  const std::string_view payload = buffer.substr(FixedHeader::SIZE + header->header_len_, header->payload_len_);

  bool ok = false;
  if (header_bytes.empty()) {
    ok = payload == expected_payload;
  } else {
    RpcResponseHeader response_header;
    if (!response_header.ParseFromArray(header_bytes.data(), static_cast<int>(header_bytes.size()))) {
      throw std::runtime_error("response contains an invalid RPC status header");
    }
    ok = response_header.error_code() == 0 && payload == expected_payload;
  }

  return DecodedFirehoseResponse{
      .request_id_ = header->request_id_,
      .ok_ = ok,
      .consumed_ = total_size,
  };
}

class EpollFirehoseConnection final {
 public:
  EpollFirehoseConnection(std::string host, std::uint16_t port, std::string request_payload,
                          std::string expected_response_payload, std::size_t target_inflight)
      : host_(std::move(host)),
        port_(port),
        request_payload_(std::move(request_payload)),
        expected_response_payload_(std::move(expected_response_payload)),
        request_header_(BuildRequestHeaderBytes()),
        slots_(target_inflight) {
    if (target_inflight == 0) {
      throw std::invalid_argument("firehose per-connection inflight must be greater than 0");
    }
    free_slots_.reserve(slots_.size());
    for (std::size_t i = 0; i < slots_.size(); ++i) {
      free_slots_.push_back(i);
    }
    read_buffer_.reserve(SOCKET_BUFFER_SIZE);
    write_buffer_.reserve(MAX_WRITE_BATCH_BYTES + FixedHeader::SIZE + request_header_.size() + request_payload_.size());
  }

  EpollFirehoseConnection(const EpollFirehoseConnection &) = delete;
  auto operator=(const EpollFirehoseConnection &) -> EpollFirehoseConnection & = delete;

  ~EpollFirehoseConnection() { Close(); }

  void Connect() {
    fd_ = ConnectBlocking(host_, port_);
    SetNonBlocking(fd_);
  }

  [[nodiscard]] auto fd() const -> int { return fd_; }
  [[nodiscard]] auto closed() const -> bool { return closed_; }
  [[nodiscard]] auto Submitted() const -> std::size_t { return submitted_.load(std::memory_order_relaxed); }
  [[nodiscard]] auto Completed() const -> std::size_t { return completed_.load(std::memory_order_relaxed); }
  [[nodiscard]] auto Success() const -> std::size_t { return success_.load(std::memory_order_relaxed); }
  [[nodiscard]] auto Failed() const -> std::size_t { return failed_.load(std::memory_order_relaxed); }

  void MarkDeadline(std::chrono::steady_clock::time_point now, std::chrono::steady_clock::time_point deadline) {
    if (now >= deadline) {
      sending_done_ = true;
    }
  }

  [[nodiscard]] auto IsDrained() const -> bool { return sending_done_ && inflight_ == 0 && !HasPendingWrite(); }

  [[nodiscard]] auto Events(std::chrono::steady_clock::time_point now,
                            std::chrono::steady_clock::time_point deadline) const -> std::uint32_t {
    std::uint32_t events = EPOLLIN | EPOLLRDHUP | EPOLLERR | EPOLLHUP;
    if (WantsWrite(now, deadline)) {
      events |= EPOLLOUT;
    }
    return events;
  }

  void PumpWrites(std::chrono::steady_clock::time_point now, std::chrono::steady_clock::time_point deadline) {
    while (!closed_) {
      FillWriteBuffer(now, deadline);
      if (!HasPendingWrite()) {
        return;
      }
      const bool flushed = FlushWriteBuffer();
      if (!flushed || HasPendingWrite()) {
        return;
      }
      now = std::chrono::steady_clock::now();
      MarkDeadline(now, deadline);
      if (!CanSubmit(now, deadline)) {
        return;
      }
    }
  }

  void ReadAvailable() {
    std::array<char, SOCKET_BUFFER_SIZE> chunk{};
    while (!closed_) {
      const ssize_t received = ::recv(fd_, chunk.data(), chunk.size(), 0);
      if (received > 0) {
        read_buffer_.append(chunk.data(), static_cast<std::size_t>(received));
        DecodeBufferedResponses();
        continue;
      }
      if (received == 0) {
        FailOutstanding();
        return;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }
      FailOutstanding();
      return;
    }
  }

  void HandlePeerClosed() {
    if (!IsDrained()) {
      ReadAvailable();
    }
    if (!IsDrained()) {
      FailOutstanding();
    }
  }

  void AppendStats(BenchmarkStats &stats, std::vector<std::chrono::nanoseconds> &latencies) const {
    stats.success_calls_ += success_count_;
    stats.failed_calls_ += failed_count_;
    stats.total_latency_ += total_latency_;
    latencies.insert(latencies.end(), latencies_.begin(), latencies_.end());
  }

  void Close() {
    closed_ = true;
    CloseFd(fd_);
  }

 private:
  [[nodiscard]] auto CanSubmit(std::chrono::steady_clock::time_point now,
                               std::chrono::steady_clock::time_point deadline) const -> bool {
    return !sending_done_ && now < deadline && !free_slots_.empty();
  }

  [[nodiscard]] auto HasPendingWrite() const -> bool { return write_offset_ < write_buffer_.size(); }

  [[nodiscard]] auto WantsWrite(std::chrono::steady_clock::time_point now,
                                std::chrono::steady_clock::time_point deadline) const -> bool {
    return HasPendingWrite() || CanSubmit(now, deadline);
  }

  void FillWriteBuffer(std::chrono::steady_clock::time_point now, std::chrono::steady_clock::time_point deadline) {
    if (!CanSubmit(now, deadline)) {
      return;
    }
    CompactWriteBuffer();

    while (CanSubmit(now, deadline) && write_buffer_.size() - write_offset_ < MAX_WRITE_BATCH_BYTES) {
      const std::size_t slot_index = free_slots_.back();
      free_slots_.pop_back();
      FirehoseSlot &slot = slots_[slot_index];
      slot.in_flight_ = true;
      slot.request_id_ = slot.generation_ * slots_.size() + slot_index;
      slot.begin_ = now;
      ++inflight_;

      AppendRequestFrame(slot.request_id_, request_header_, request_payload_, write_buffer_);
      submitted_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  auto FlushWriteBuffer() -> bool {
    while (write_offset_ < write_buffer_.size()) {
      const ssize_t sent =
          ::send(fd_, write_buffer_.data() + write_offset_, write_buffer_.size() - write_offset_, MSG_NOSIGNAL);
      if (sent > 0) {
        write_offset_ += static_cast<std::size_t>(sent);
        continue;
      }
      if (sent == 0) {
        return false;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        CompactWriteBuffer();
        return false;
      }
      FailOutstanding();
      return false;
    }

    write_buffer_.clear();
    write_offset_ = 0;
    return true;
  }

  void DecodeBufferedResponses() {
    while (!closed_) {
      const std::string_view readable(read_buffer_.data() + read_offset_, read_buffer_.size() - read_offset_);
      std::optional<DecodedFirehoseResponse> decoded;
      try {
        decoded = TryDecodeResponse(readable, expected_response_payload_);
      } catch (...) {
        FailOutstanding();
        return;
      }
      if (!decoded.has_value()) {
        CompactReadBuffer();
        return;
      }
      CompleteSlot(decoded->request_id_, decoded->ok_);
      read_offset_ += decoded->consumed_;
    }
  }

  void CompleteSlot(std::uint64_t request_id, bool ok) {
    const auto now = std::chrono::steady_clock::now();
    const auto slot_index = static_cast<std::size_t>(request_id % slots_.size());
    FirehoseSlot &slot = slots_[slot_index];

    bool matched = false;
    std::chrono::nanoseconds latency{0};
    if (slot.in_flight_ && slot.request_id_ == request_id) {
      matched = true;
      latency = std::chrono::duration_cast<std::chrono::nanoseconds>(now - slot.begin_);
      slot.in_flight_ = false;
      ++slot.generation_;
      --inflight_;
      free_slots_.push_back(slot_index);
    }

    RecordCompletion(matched && ok, latency);
    if (!matched) {
      FailOutstanding();
    }
  }

  void RecordCompletion(bool ok, std::chrono::nanoseconds latency) {
    latencies_.push_back(latency);
    total_latency_ += latency;
    ++completed_count_;
    completed_.fetch_add(1, std::memory_order_relaxed);
    if (ok) {
      ++success_count_;
      success_.fetch_add(1, std::memory_order_relaxed);
    } else {
      ++failed_count_;
      failed_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  void FailOutstanding() {
    if (closed_) {
      return;
    }
    const auto now = std::chrono::steady_clock::now();
    for (FirehoseSlot &slot : slots_) {
      if (!slot.in_flight_) {
        continue;
      }
      const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(now - slot.begin_);
      slot.in_flight_ = false;
      ++slot.generation_;
      --inflight_;
      RecordCompletion(false, latency);
    }
    sending_done_ = true;
    write_buffer_.clear();
    write_offset_ = 0;
    Close();
  }

  void CompactReadBuffer() {
    if (read_offset_ == 0) {
      return;
    }
    if (read_offset_ == read_buffer_.size()) {
      read_buffer_.clear();
      read_offset_ = 0;
      return;
    }
    if (read_offset_ >= SOCKET_BUFFER_SIZE || read_offset_ * 2 >= read_buffer_.size()) {
      read_buffer_.erase(0, read_offset_);
      read_offset_ = 0;
    }
  }

  void CompactWriteBuffer() {
    if (write_offset_ == 0) {
      return;
    }
    if (write_offset_ == write_buffer_.size()) {
      write_buffer_.clear();
      write_offset_ = 0;
      return;
    }
    if (write_offset_ >= MAX_WRITE_BATCH_BYTES) {
      write_buffer_.erase(0, write_offset_);
      write_offset_ = 0;
    }
  }

  std::string host_;
  std::uint16_t port_ = 0;
  std::string request_payload_;
  std::string expected_response_payload_;
  std::string request_header_;
  int fd_ = -1;

  std::vector<FirehoseSlot> slots_;
  std::vector<std::size_t> free_slots_;
  std::size_t inflight_ = 0;
  bool sending_done_ = false;
  bool closed_ = false;

  std::string read_buffer_;
  std::size_t read_offset_ = 0;
  std::string write_buffer_;
  std::size_t write_offset_ = 0;

  std::atomic<std::size_t> submitted_{0};
  std::atomic<std::size_t> completed_{0};
  std::atomic<std::size_t> success_{0};
  std::atomic<std::size_t> failed_{0};

  std::vector<std::chrono::nanoseconds> latencies_;
  std::size_t completed_count_ = 0;
  std::size_t success_count_ = 0;
  std::size_t failed_count_ = 0;
  std::chrono::nanoseconds total_latency_{0};
};

class EpollFirehoseWorker final {
 public:
  EpollFirehoseWorker() = default;
  EpollFirehoseWorker(const EpollFirehoseWorker &) = delete;
  auto operator=(const EpollFirehoseWorker &) -> EpollFirehoseWorker & = delete;

  void AddConnection(std::unique_ptr<EpollFirehoseConnection> connection) {
    connections_.push_back(std::move(connection));
  }

  void ConnectAll() {
    for (auto &connection : connections_) {
      connection->Connect();
    }
  }

  void Start(std::latch &start_latch, std::chrono::steady_clock::time_point deadline) {
    thread_ = std::jthread([this, &start_latch, deadline] {
      try {
        start_latch.wait();
        Run(deadline);
      } catch (...) {
        exception_ = std::current_exception();
      }
    });
  }

  void Join() {
    if (thread_.joinable()) {
      thread_.join();
    }
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
  }

  [[nodiscard]] auto Submitted() const -> std::size_t {
    std::size_t total = 0;
    for (const auto &connection : connections_) {
      total += connection->Submitted();
    }
    return total;
  }

  [[nodiscard]] auto Completed() const -> std::size_t {
    std::size_t total = 0;
    for (const auto &connection : connections_) {
      total += connection->Completed();
    }
    return total;
  }

  [[nodiscard]] auto Success() const -> std::size_t {
    std::size_t total = 0;
    for (const auto &connection : connections_) {
      total += connection->Success();
    }
    return total;
  }

  [[nodiscard]] auto Failed() const -> std::size_t {
    std::size_t total = 0;
    for (const auto &connection : connections_) {
      total += connection->Failed();
    }
    return total;
  }

  void AppendStats(BenchmarkStats &stats, std::vector<std::chrono::nanoseconds> &latencies) const {
    for (const auto &connection : connections_) {
      connection->AppendStats(stats, latencies);
    }
  }

 private:
  void Run(std::chrono::steady_clock::time_point deadline) {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
      throw std::runtime_error(std::string("epoll_create1 failed: ") + std::strerror(errno));
    }

    auto now = std::chrono::steady_clock::now();
    for (auto &connection : connections_) {
      connection->PumpWrites(now, deadline);
      AddToEpoll(*connection, now, deadline);
    }

    std::array<epoll_event, MAX_EPOLL_EVENTS> events{};
    while (!AllDone()) {
      now = std::chrono::steady_clock::now();
      for (auto &connection : connections_) {
        if (connection->closed()) {
          continue;
        }
        connection->MarkDeadline(now, deadline);
        UpdateEpoll(*connection, now, deadline);
      }

      const int timeout_ms = EpollTimeout(now, deadline);
      const int ready = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), timeout_ms);
      if (ready < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error(std::string("epoll_wait failed: ") + std::strerror(errno));
      }

      for (int i = 0; i < ready; ++i) {
        auto *connection = static_cast<EpollFirehoseConnection *>(events[static_cast<std::size_t>(i)].data.ptr);
        if (connection == nullptr || connection->closed()) {
          continue;
        }

        const std::uint32_t event_mask = events[static_cast<std::size_t>(i)].events;
        now = std::chrono::steady_clock::now();
        connection->MarkDeadline(now, deadline);
        if ((event_mask & EPOLLOUT) != 0) {
          connection->PumpWrites(now, deadline);
        }
        if ((event_mask & EPOLLIN) != 0) {
          connection->ReadAvailable();
          now = std::chrono::steady_clock::now();
          connection->MarkDeadline(now, deadline);
          connection->PumpWrites(now, deadline);
        }
        if ((event_mask & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) {
          connection->HandlePeerClosed();
        }
        UpdateEpoll(*connection, now, deadline);
      }
    }

    for (auto &connection : connections_) {
      connection->Close();
    }
    ::close(epoll_fd_);
    epoll_fd_ = -1;
  }

  void AddToEpoll(EpollFirehoseConnection &connection, std::chrono::steady_clock::time_point now,
                  std::chrono::steady_clock::time_point deadline) {
    epoll_event event{};
    event.events = connection.Events(now, deadline);
    event.data.ptr = &connection;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, connection.fd(), &event) < 0) {
      throw std::runtime_error(std::string("epoll_ctl(ADD) failed: ") + std::strerror(errno));
    }
    current_events_.push_back(event.events);
  }

  void UpdateEpoll(EpollFirehoseConnection &connection, std::chrono::steady_clock::time_point now,
                   std::chrono::steady_clock::time_point deadline) {
    if (connection.closed()) {
      return;
    }
    const std::size_t index = ConnectionIndex(connection);
    const std::uint32_t new_events = connection.Events(now, deadline);
    if (current_events_[index] == new_events) {
      return;
    }

    epoll_event event{};
    event.events = new_events;
    event.data.ptr = &connection;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, connection.fd(), &event) < 0) {
      throw std::runtime_error(std::string("epoll_ctl(MOD) failed: ") + std::strerror(errno));
    }
    current_events_[index] = new_events;
  }

  [[nodiscard]] auto ConnectionIndex(const EpollFirehoseConnection &connection) const -> std::size_t {
    for (std::size_t i = 0; i < connections_.size(); ++i) {
      if (connections_[i].get() == &connection) {
        return i;
      }
    }
    throw std::logic_error("epoll connection was not owned by the worker");
  }

  [[nodiscard]] auto AllDone() const -> bool {
    for (const auto &connection : connections_) {
      if (!connection->closed() && !connection->IsDrained()) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] static auto EpollTimeout(std::chrono::steady_clock::time_point now,
                                         std::chrono::steady_clock::time_point deadline) -> int {
    if (now >= deadline) {
      return 1000;
    }
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    return static_cast<int>(std::clamp<std::int64_t>(remaining.count(), 1, 100));
  }

  std::vector<std::unique_ptr<EpollFirehoseConnection>> connections_;
  std::vector<std::uint32_t> current_events_;
  std::jthread thread_;
  std::exception_ptr exception_;
  int epoll_fd_ = -1;
};

auto PerConnectionInflight(const FirehoseConfig &config, std::size_t connection_index) -> std::size_t {
  const std::size_t base = config.firehose_inflight_ / config.firehose_connections_;
  const std::size_t remainder = config.firehose_inflight_ % config.firehose_connections_;
  return base + (connection_index < remainder ? 1 : 0);
}

template <typename Runner>
void ReportFirehoseProgress(const FirehoseConfig &config, const std::vector<std::unique_ptr<Runner>> &runners,
                            std::atomic<bool> &done, std::condition_variable &done_cv, std::mutex &done_mu,
                            std::chrono::steady_clock::time_point start_time) {
  std::unique_lock lock(done_mu);
  while (!done_cv.wait_for(lock, std::chrono::seconds(1), [&done]() { return done.load(std::memory_order_relaxed); })) {
    lock.unlock();

    std::size_t submitted = 0;
    std::size_t completed = 0;
    std::size_t success = 0;
    std::size_t failed = 0;
    for (const auto &runner : runners) {
      submitted += runner->Submitted();
      completed += runner->Completed();
      success += runner->Success();
      failed += runner->Failed();
    }

    const auto elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start_time);
    const auto elapsed_seconds = static_cast<double>(elapsed.count());
    const double qps = elapsed_seconds <= 0.0 ? 0.0 : static_cast<double>(completed) / elapsed_seconds;
    const std::size_t inflight = submitted >= completed ? submitted - completed : 0;

    std::fprintf(stderr,
                 "[progress] elapsed_s=%.0f firehose_inflight=%zu submitted=%zu completed=%zu in_flight=%zu "
                 "success=%zu failed=%zu qps=%.2f\n",
                 elapsed_seconds, config.firehose_inflight_, submitted, completed, inflight, success, failed, qps);
    lock.lock();
  }
}

template <typename Runner>
auto FinalizeFirehoseStats(const std::vector<std::unique_ptr<Runner>> &runners, std::chrono::nanoseconds wall_time)
    -> BenchmarkStats {
  BenchmarkStats stats;
  std::vector<std::chrono::nanoseconds> latencies;
  for (const auto &runner : runners) {
    runner->AppendStats(stats, latencies);
  }

  stats.total_calls_ = stats.success_calls_ + stats.failed_calls_;
  std::ranges::sort(latencies);
  stats.p50_latency_ = Percentile(latencies, 0.50);
  stats.p95_latency_ = Percentile(latencies, 0.95);
  stats.p99_latency_ = Percentile(latencies, 0.99);
  if (wall_time > std::chrono::nanoseconds::zero()) {
    const double seconds = static_cast<double>(wall_time.count()) / 1'000'000'000.0;
    stats.qps_ = static_cast<double>(stats.total_calls_) / seconds;
  }
  return stats;
}

auto RunEpollFirehoseBenchmark(const FirehoseConfig &config) -> BenchmarkStats {
  const std::size_t io_threads = ResolveFirehoseIoThreads(config);
  std::vector<std::unique_ptr<EpollFirehoseWorker>> workers;
  workers.reserve(io_threads);
  for (std::size_t i = 0; i < io_threads; ++i) {
    workers.push_back(std::make_unique<EpollFirehoseWorker>());
  }

  const BenchmarkPayload payload = BuildBenchmarkPayload(config);
  for (std::size_t i = 0; i < config.firehose_connections_; ++i) {
    const std::size_t inflight = PerConnectionInflight(config, i);
    auto connection = std::make_unique<EpollFirehoseConnection>(config.host_, config.port_, payload.request_,
                                                                payload.expected_response_, inflight);
    workers[i % workers.size()]->AddConnection(std::move(connection));
  }

  for (auto &worker : workers) {
    worker->ConnectAll();
  }

  const auto wall_start = std::chrono::steady_clock::now();
  const auto deadline = wall_start + std::chrono::seconds(config.duration_s_);
  std::atomic<bool> progress_done{false};
  std::condition_variable progress_done_cv;
  std::mutex progress_done_mu;
  std::jthread progress_thread(
      [&] { ReportFirehoseProgress(config, workers, progress_done, progress_done_cv, progress_done_mu, wall_start); });

  std::latch start_latch(1);
  try {
    for (auto &worker : workers) {
      worker->Start(start_latch, deadline);
    }
    start_latch.count_down();
    for (auto &worker : workers) {
      worker->Join();
    }
  } catch (...) {
    start_latch.count_down();
    for (auto &worker : workers) {
      worker->Join();
    }
    progress_done.store(true, std::memory_order_relaxed);
    progress_done_cv.notify_all();
    progress_thread.request_stop();
    if (progress_thread.joinable()) {
      progress_thread.join();
    }
    throw;
  }

  const auto wall_end = std::chrono::steady_clock::now();
  progress_done.store(true, std::memory_order_relaxed);
  progress_done_cv.notify_all();
  progress_thread.request_stop();
  if (progress_thread.joinable()) {
    progress_thread.join();
  }

  return FinalizeFirehoseStats(workers, std::chrono::duration_cast<std::chrono::nanoseconds>(wall_end - wall_start));
}

}  // namespace

auto RunFirehoseBenchmark(const FirehoseConfig &config) -> BenchmarkStats { return RunEpollFirehoseBenchmark(config); }

}  // namespace xrpc::benchmark

auto main(int argc, char **argv) -> int {
  try {
    const xrpc::benchmark::FirehoseConfig config = xrpc::benchmark::ParseConfig(argc, argv);
    std::printf(
        "client=firehose host=%s port=%u duration_s=%llu payload_size=%zu connections=%zu inflight=%zu "
        "io_threads=%zu\n",
        config.host_.c_str(), config.port_, static_cast<unsigned long long>(config.duration_s_), config.payload_size_,
        config.firehose_connections_, config.firehose_inflight_, config.firehose_io_threads_);
    const xrpc::benchmark::BenchmarkStats stats = xrpc::benchmark::RunFirehoseBenchmark(config);
    xrpc::benchmark::PrintStats(stats);
    return 0;
  } catch (const std::exception &ex) {
    std::fprintf(stderr, "%s\n%s\n", ex.what(), xrpc::benchmark::Usage(argv[0]).c_str());
    return 1;
  }
}
