/** @file latency_trace.cpp @brief Writes sampled fixed-size trace records to thread-local binary files. */

#include "common/latency_trace.h"

#ifdef XRPC_ENABLE_LATENCY_TRACE

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>

#include <sys/prctl.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

namespace xrpc::diagnostics {
namespace {

constexpr std::size_t BUFFER_RECORDS = 4096;

struct TraceRecord final {
  std::uint64_t request_id_ = 0;
  std::uint64_t timestamp_ns_ = 0;
  std::uint32_t value_ = 0;
  std::uint16_t stage_ = 0;
  std::uint16_t reserved_ = 0;
};

static_assert(sizeof(TraceRecord) == 24);

struct TraceConfig final {
  std::string prefix_;
  std::uint64_t sample_mask_ = 1023;
  bool enabled_ = false;
};

auto ParseSampleMask() noexcept -> std::uint64_t {
  const char *shift_text = std::getenv("XRPC_LATENCY_TRACE_SAMPLE_SHIFT");
  if (shift_text == nullptr || *shift_text == '\0') {
    return 1023;
  }
  char *end = nullptr;
  errno = 0;
  const unsigned long shift = std::strtoul(shift_text, &end, 10);
  if (errno != 0 || end == shift_text || *end != '\0' || shift > 20) {
    return 1023;
  }
  return shift == 0 ? 0 : (std::uint64_t{1} << shift) - 1;
}

auto Config() noexcept -> const TraceConfig & {
  static const TraceConfig config = [] {
    TraceConfig result;
    const char *prefix = std::getenv("XRPC_LATENCY_TRACE_PREFIX");
    if (prefix != nullptr && *prefix != '\0') {
      result.prefix_ = prefix;
      result.sample_mask_ = ParseSampleMask();
      result.enabled_ = true;
    }
    return result;
  }();
  return config;
}

auto MixRequestId(std::uint64_t value) noexcept -> std::uint64_t {
  value ^= value >> 30U;
  value *= 0xbf58476d1ce4e5b9ULL;
  value ^= value >> 27U;
  value *= 0x94d049bb133111ebULL;
  return value ^ (value >> 31U);
}

class ThreadTraceBuffer final {
 public:
  ThreadTraceBuffer() noexcept {
    const TraceConfig &config = Config();
    if (!config.enabled_) {
      return;
    }
    path_ = config.prefix_ + ".pid" + std::to_string(static_cast<long long>(::getpid())) + ".tid" +
            std::to_string(static_cast<long long>(::syscall(SYS_gettid))) + ".bin";
    file_ = std::fopen(path_.c_str(), "ab");
  }

  ~ThreadTraceBuffer() {
    Flush();
    if (file_ != nullptr) {
      std::fclose(file_);
    }
  }

  void Append(TraceRecord record) noexcept {
    if (file_ == nullptr) {
      return;
    }
    records_[size_++] = record;
    if (size_ == records_.size()) {
      Flush();
    }
  }

 private:
  void Flush() noexcept {
    if (file_ == nullptr || size_ == 0) {
      return;
    }
    (void)std::fwrite(records_.data(), sizeof(TraceRecord), size_, file_);
    size_ = 0;
  }

  std::string path_;
  std::FILE *file_ = nullptr;
  std::array<TraceRecord, BUFFER_RECORDS> records_{};
  std::size_t size_ = 0;
};

thread_local ThreadTraceBuffer thread_trace_buffer;

}  // namespace

auto LatencyTraceEnabled() noexcept -> bool { return Config().enabled_; }

auto LatencyTraceSampled(std::uint64_t request_id) noexcept -> bool {
  const TraceConfig &config = Config();
  return config.enabled_ && (MixRequestId(request_id) & config.sample_mask_) == 0;
}

auto LatencyNowNs() noexcept -> std::uint64_t {
  timespec timestamp{};
  if (::clock_gettime(CLOCK_MONOTONIC_RAW, &timestamp) != 0) {
    return 0;
  }
  return static_cast<std::uint64_t>(timestamp.tv_sec) * 1'000'000'000ULL +
         static_cast<std::uint64_t>(timestamp.tv_nsec);
}

void RecordLatencyTrace(LatencyStage stage, std::uint64_t request_id, std::uint32_t value,
                        std::uint64_t timestamp_ns) noexcept {
  if (!LatencyTraceSampled(request_id)) {
    return;
  }
  if (timestamp_ns == 0) {
    timestamp_ns = LatencyNowNs();
  }
  thread_trace_buffer.Append(TraceRecord{.request_id_ = request_id,
                                         .timestamp_ns_ = timestamp_ns,
                                         .value_ = value,
                                         .stage_ = static_cast<std::uint16_t>(stage)});
}

void SetLatencyTraceThreadName(std::string_view name) noexcept {
  constexpr std::size_t MAX_THREAD_NAME_BYTES = 15;
  char buffer[MAX_THREAD_NAME_BYTES + 1]{};
  const std::size_t size = std::min(name.size(), MAX_THREAD_NAME_BYTES);
  std::memcpy(buffer, name.data(), size);
  (void)::prctl(PR_SET_NAME, buffer, 0, 0, 0);
}

}  // namespace xrpc::diagnostics

#endif
