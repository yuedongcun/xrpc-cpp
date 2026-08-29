/** @file latency_trace.h @brief Local-only sampled latency trace primitives. */

#pragma once

#include <cstdint>

namespace xrpc::diagnostics {

enum class LatencyStage : std::uint16_t {
  ClientCreated = 1,
  ClientSent = 2,
  ClientRecv = 3,
  ClientComplete = 4,

  ServerRecv = 10,
  ServerDecoded = 11,
  WorkerEnqueue = 12,
  WorkerStart = 13,
  RequestStart = 14,
  DispatchEnd = 15,
  EncodeEnd = 16,
  MailboxSubmit = 17,
  MailboxDrain = 18,
  WriteEnqueue = 19,
  ServerSend = 20,
};

#ifdef XRPC_ENABLE_LATENCY_TRACE

[[nodiscard]] auto LatencyTraceEnabled() noexcept -> bool;

[[nodiscard]] auto LatencyTraceSampled(std::uint64_t request_id) noexcept -> bool;

[[nodiscard]] auto LatencyNowNs() noexcept -> std::uint64_t;

void RecordLatencyTrace(LatencyStage stage, std::uint64_t request_id, std::uint32_t value = 0,
                        std::uint64_t timestamp_ns = 0) noexcept;

#else

[[nodiscard]] inline auto LatencyTraceEnabled() noexcept -> bool { return false; }

[[nodiscard]] inline auto LatencyTraceSampled(std::uint64_t) noexcept -> bool { return false; }

[[nodiscard]] inline auto LatencyNowNs() noexcept -> std::uint64_t { return 0; }

inline void RecordLatencyTrace(LatencyStage, std::uint64_t, std::uint32_t = 0, std::uint64_t = 0) noexcept {}

#endif

}  // namespace xrpc::diagnostics
