#include "protocol/frame_codec.h"

#include <protocol/xrpc/xrpc_header.pb.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "common/xrpc_exception.h"

namespace xrpc {
namespace {

auto FitsProtobufParseArray(std::string_view bytes) -> bool {
  return bytes.size() <= static_cast<std::size_t>(std::numeric_limits<int>::max());
}

auto FrameSize(std::size_t header_size, std::size_t payload_size) -> std::uint64_t {
  return static_cast<std::uint64_t>(FixedHeader::SIZE) + static_cast<std::uint64_t>(header_size) +
         static_cast<std::uint64_t>(payload_size);
}

struct FrameView {
  ProtocolError error_;
  std::size_t consumed_ = 0;
  FixedHeader header_;
  std::string_view header_bytes_;
  std::string_view payload_;
};

void ValidateEncodedFrameSize(std::size_t header_size, std::size_t payload_size, const ProtocolLimits &limits) {
  if (header_size > std::numeric_limits<std::uint32_t>::max() || header_size > limits.max_header_size_) {
    throw ProtocolException(StatusCode::ResourceExhausted, "protocol header exceeds configured limit");
  }
  if (payload_size > std::numeric_limits<std::uint32_t>::max() || payload_size > limits.max_payload_size_) {
    throw ProtocolException(StatusCode::ResourceExhausted, "protocol payload exceeds configured limit");
  }
  if (FrameSize(header_size, payload_size) > limits.max_frame_size_) {
    throw ProtocolException(StatusCode::ResourceExhausted, "protocol frame exceeds configured limit");
  }
}

auto BuildFrame(const FixedHeader &hdr, std::string_view header_bytes, std::string_view payload,
                const ProtocolLimits &limits) -> std::string {
  ValidateEncodedFrameSize(header_bytes.size(), payload.size(), limits);
  const std::size_t total_size = FixedHeader::SIZE + header_bytes.size() + payload.size();
  std::string result;
  result.resize(total_size);

  char *write = result.data();
  FixedHeader::EncodeTo(hdr, write);
  write += FixedHeader::SIZE;
  if (!header_bytes.empty()) {
    std::memcpy(write, header_bytes.data(), header_bytes.size());
  }
  write += header_bytes.size();
  if (!payload.empty()) {
    std::memcpy(write, payload.data(), payload.size());
  }

  return result;
}

auto EncodeMessage(const RawRequest &request, const ProtocolLimits &limits) -> std::string {
  RpcRequestHeader pb_hdr;
  pb_hdr.set_service_name(request.service_name_);
  pb_hdr.set_method_name(request.method_name_);

  std::string header_bytes;
  pb_hdr.SerializeToString(&header_bytes);

  FixedHeader hdr;
  hdr.message_type_ = MessageType::Request;
  hdr.request_id_ = request.request_id_;
  hdr.header_len_ = static_cast<uint32_t>(header_bytes.size());
  hdr.payload_len_ = static_cast<uint32_t>(request.payload_.size());

  return BuildFrame(hdr, header_bytes, request.payload_, limits);
}

auto EncodeResponseHeader(const RawResponse &response) -> std::string {
  if (response.status_.code() == StatusCode::Ok && response.status_.message().empty()) {
    return {};
  }

  RpcResponseHeader pb_hdr;
  pb_hdr.set_error_code(static_cast<std::int32_t>(response.status_.code()));
  pb_hdr.set_error_text(response.status_.message());

  std::string header_bytes;
  pb_hdr.SerializeToString(&header_bytes);
  return header_bytes;
}

auto EncodeMessage(const RawResponse &response, const ProtocolLimits &limits) -> std::string {
  std::string header_bytes = EncodeResponseHeader(response);

  FixedHeader hdr;
  hdr.message_type_ = MessageType::Response;
  hdr.request_id_ = response.request_id_;
  hdr.header_len_ = static_cast<uint32_t>(header_bytes.size());
  hdr.payload_len_ = static_cast<uint32_t>(response.payload_.size());

  return BuildFrame(hdr, header_bytes, response.payload_, limits);
}

auto DecodeRequest(const FixedHeader &hdr, std::string_view header_bytes, std::string_view payload)
    -> std::optional<RawRequest> {
  RpcRequestHeader pb_hdr;
  if (!FitsProtobufParseArray(header_bytes) ||
      !pb_hdr.ParseFromArray(header_bytes.data(), static_cast<int>(header_bytes.size()))) {
    return std::nullopt;
  }

  RawRequest req;
  req.request_id_ = hdr.request_id_;
  req.service_name_ = pb_hdr.service_name();
  req.method_name_ = pb_hdr.method_name();
  req.payload_ = std::string(payload);

  return req;
}

auto DecodeRequest(const FixedHeader &hdr, std::string_view header_bytes, std::string_view payload,
                   RequestHeaderDecodeCache &cache) -> std::optional<RawRequest> {
  if (cache.has_value_ && header_bytes == cache.header_bytes_) {
    RawRequest req;
    req.request_id_ = hdr.request_id_;
    req.service_name_ = cache.service_name_;
    req.method_name_ = cache.method_name_;
    req.payload_ = std::string(payload);
    return req;
  }

  std::optional<RawRequest> decoded = DecodeRequest(hdr, header_bytes, payload);
  if (!decoded) {
    return std::nullopt;
  }

  cache.header_bytes_.assign(header_bytes);
  cache.service_name_ = decoded->service_name_;
  cache.method_name_ = decoded->method_name_;
  cache.has_value_ = true;
  return decoded;
}

auto DecodeStatus(std::int32_t code, std::string message) -> Status {
  switch (static_cast<StatusCode>(code)) {
    case StatusCode::Ok:
      return Status::Ok();
    case StatusCode::Cancelled:
    case StatusCode::InvalidArgument:
    case StatusCode::DeadlineExceeded:
    case StatusCode::NotFound:
    case StatusCode::AlreadyExists:
    case StatusCode::PermissionDenied:
    case StatusCode::ResourceExhausted:
    case StatusCode::FailedPrecondition:
    case StatusCode::Unimplemented:
    case StatusCode::Internal:
    case StatusCode::Unavailable:
    case StatusCode::DataLoss:
    case StatusCode::Unauthenticated:
      return {static_cast<StatusCode>(code), std::move(message)};
  }
  return {StatusCode::DataLoss, "response contains an invalid RPC status code"};
}

auto DecodeResponse(const FixedHeader &hdr, std::string_view header_bytes, std::string_view payload)
    -> std::optional<RawResponse> {
  RpcResponseHeader pb_hdr;
  if (!FitsProtobufParseArray(header_bytes) ||
      !pb_hdr.ParseFromArray(header_bytes.data(), static_cast<int>(header_bytes.size()))) {
    return std::nullopt;
  }

  RawResponse resp;
  resp.request_id_ = hdr.request_id_;
  resp.status_ = DecodeStatus(pb_hdr.error_code(), pb_hdr.error_text());
  resp.payload_ = std::string(payload);

  return resp;
}

auto ReadFrame(std::string_view buf, const ProtocolLimits &limits) -> FrameView {
  if (buf.size() < FixedHeader::SIZE) {
    return {.error_ = ProtocolError::NeedMoreData};
  }

  auto opt_hdr = FixedHeader::Decode(buf.substr(0, FixedHeader::SIZE));
  if (!opt_hdr) {
    return {.error_ = ProtocolError::InvalidMagic};
  }

  const auto &hdr = *opt_hdr;

  if (hdr.version_ != FixedHeader::VERSION) {
    return {.error_ = ProtocolError::UnsupportedVersion};
  }

  if (hdr.header_len_ > limits.max_header_size_ || hdr.payload_len_ > limits.max_payload_size_ ||
      FrameSize(hdr.header_len_, hdr.payload_len_) > limits.max_frame_size_) {
    return {.error_ = ProtocolError::FrameTooLarge};
  }

  const std::size_t total =
      FixedHeader::SIZE + static_cast<std::size_t>(hdr.header_len_) + static_cast<std::size_t>(hdr.payload_len_);
  if (buf.size() < total) {
    return {.error_ = ProtocolError::NeedMoreData};
  }

  return {
      .error_ = ProtocolError::Ok,
      .consumed_ = total,
      .header_ = hdr,
      .header_bytes_ = buf.substr(FixedHeader::SIZE, hdr.header_len_),
      .payload_ = buf.substr(FixedHeader::SIZE + hdr.header_len_, hdr.payload_len_),
  };
}

}  // namespace

auto MakeProtocolLimits(std::size_t max_payload_size) -> ProtocolLimits {
  if (max_payload_size == 0) {
    throw ConfigException("protocol max_payload_size must be greater than 0");
  }
  if (max_payload_size > std::numeric_limits<std::uint32_t>::max()) {
    throw ConfigException("protocol max_payload_size must fit in uint32");
  }

  ProtocolLimits limits;
  limits.max_payload_size_ = max_payload_size;
  limits.max_frame_size_ = FixedHeader::SIZE + limits.max_header_size_ + limits.max_payload_size_;
  return limits;
}

FrameCodec::FrameCodec(ProtocolLimits limits) : limits_(limits) {
  if (limits_.max_frame_size_ < FixedHeader::SIZE) {
    throw ConfigException("protocol max_frame_size must include the fixed header");
  }
}

auto FrameCodec::EncodeRequest(const RawRequest &request) -> std::string { return EncodeMessage(request, limits_); }

auto FrameCodec::EncodeResponse(const RawResponse &response) -> std::string { return EncodeMessage(response, limits_); }

auto FrameCodec::TryDecode(std::string_view buf) -> DecodeResult { return TryDecode(buf, nullptr); }

auto FrameCodec::TryDecode(std::string_view buf, RequestHeaderDecodeCache &request_header_cache) -> DecodeResult {
  return TryDecode(buf, &request_header_cache);
}

auto FrameCodec::TryDecodeRequest(std::string_view buf, RequestHeaderDecodeCache &request_header_cache)
    -> RequestDecodeResult {
  const FrameView frame = ReadFrame(buf, limits_);
  if (frame.error_ != ProtocolError::Ok) {
    return {.error_ = frame.error_, .consumed_ = frame.consumed_, .request_ = std::nullopt};
  }

  switch (frame.header_.message_type_) {
    case MessageType::Request:
      break;
    case MessageType::Heartbeat:
    case MessageType::HeartbeatAck:
      return {.error_ = ProtocolError::UnsupportedMessageType, .consumed_ = frame.consumed_, .request_ = std::nullopt};
    default:
      return {.error_ = ProtocolError::InvalidMessageType, .consumed_ = frame.consumed_, .request_ = std::nullopt};
  }

  std::optional<RawRequest> request =
      DecodeRequest(frame.header_, frame.header_bytes_, frame.payload_, request_header_cache);
  if (!request) {
    return {.error_ = ProtocolError::DecodeError, .consumed_ = frame.consumed_, .request_ = std::nullopt};
  }

  return {.error_ = ProtocolError::Ok, .consumed_ = frame.consumed_, .request_ = std::move(request)};
}

auto FrameCodec::TryDecode(std::string_view buf, RequestHeaderDecodeCache *request_header_cache) -> DecodeResult {
  const FrameView frame = ReadFrame(buf, limits_);
  if (frame.error_ != ProtocolError::Ok) {
    return {.error_ = frame.error_, .consumed_ = frame.consumed_};
  }

  switch (frame.header_.message_type_) {
    case MessageType::Request: {
      std::optional<RawRequest> request;
      if (request_header_cache != nullptr) {
        request = DecodeRequest(frame.header_, frame.header_bytes_, frame.payload_, *request_header_cache);
      } else {
        request = DecodeRequest(frame.header_, frame.header_bytes_, frame.payload_);
      }
      if (!request) {
        return {.error_ = ProtocolError::DecodeError, .consumed_ = frame.consumed_};
      }
      return {.error_ = ProtocolError::Ok, .consumed_ = frame.consumed_, .request_ = std::move(request)};
    }

    case MessageType::Response: {
      std::optional<RawResponse> response = DecodeResponse(frame.header_, frame.header_bytes_, frame.payload_);
      if (!response) {
        return {.error_ = ProtocolError::DecodeError, .consumed_ = frame.consumed_};
      }
      return {.error_ = ProtocolError::Ok, .consumed_ = frame.consumed_, .response_ = std::move(response)};
    }

    case MessageType::Heartbeat:
    case MessageType::HeartbeatAck:
      return {.error_ = ProtocolError::UnsupportedMessageType, .consumed_ = frame.consumed_};

    default:
      return {.error_ = ProtocolError::InvalidMessageType, .consumed_ = frame.consumed_};
  }
}

}  // namespace xrpc
