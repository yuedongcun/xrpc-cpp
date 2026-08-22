/**
 * @file frame_codec.cpp
 * @brief Implements bounded xRPC frame construction, validation, and metadata decoding.
 */

#include "protocol/frame_codec.h"

#include <protocol/xrpc/frame_metadata.pb.h>

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

auto FrameSize(std::size_t metadata_size, std::size_t payload_size) -> std::uint64_t {
  return static_cast<std::uint64_t>(FrameHeader::SIZE) + static_cast<std::uint64_t>(metadata_size) +
         static_cast<std::uint64_t>(payload_size);
}

struct FrameView {
  ProtocolError error_;
  std::size_t consumed_ = 0;
  FrameHeader header_;
  std::string_view metadata_bytes_;
  std::string_view payload_;
};

void ValidateEncodedFrameSize(std::size_t metadata_size, std::size_t payload_size, const ProtocolLimits &limits) {
  if (metadata_size > std::numeric_limits<std::uint32_t>::max() || metadata_size > limits.max_metadata_size_) {
    throw ProtocolException(StatusCode::ResourceExhausted, "protocol metadata exceeds configured limit");
  }
  if (payload_size > std::numeric_limits<std::uint32_t>::max() || payload_size > limits.max_payload_size_) {
    throw ProtocolException(StatusCode::ResourceExhausted, "protocol payload exceeds configured limit");
  }
  if (FrameSize(metadata_size, payload_size) > std::numeric_limits<std::size_t>::max()) {
    throw ProtocolException(StatusCode::ResourceExhausted, "protocol frame exceeds the addressable size");
  }
}

auto BuildFrame(const FrameHeader &header, std::string_view metadata_bytes, std::string_view payload,
                const ProtocolLimits &limits) -> std::string {
  ValidateEncodedFrameSize(metadata_bytes.size(), payload.size(), limits);

  // Allocate the complete frame once, then write each wire section directly
  // into its final position.
  const std::size_t total_size = FrameHeader::SIZE + metadata_bytes.size() + payload.size();
  std::string result;
  result.resize(total_size);

  char *write = result.data();
  FrameHeader::EncodeTo(header, write);
  write += FrameHeader::SIZE;
  if (!metadata_bytes.empty()) {
    std::memcpy(write, metadata_bytes.data(), metadata_bytes.size());
  }
  write += metadata_bytes.size();
  if (!payload.empty()) {
    std::memcpy(write, payload.data(), payload.size());
  }

  return result;
}

auto BuildRequestFrame(const RequestEnvelope &request, const ProtocolLimits &limits) -> std::string {
  RequestMetadata metadata;
  metadata.set_service_name(request.service_name_);
  metadata.set_method_name(request.method_name_);

  std::string metadata_bytes;
  metadata.SerializeToString(&metadata_bytes);

  FrameHeader header;
  header.message_type_ = MessageType::Request;
  header.request_id_ = request.request_id_;
  header.metadata_size_ = static_cast<uint32_t>(metadata_bytes.size());
  header.payload_size_ = static_cast<uint32_t>(request.payload_.size());

  return BuildFrame(header, metadata_bytes, request.payload_, limits);
}

auto EncodeResponseMetadata(const ResponseEnvelope &response) -> std::string {
  // The default Protobuf message already represents an OK response without
  // status text, so its zero-byte encoding can be omitted from the frame.
  if (response.status_.code() == StatusCode::Ok && response.status_.message().empty()) {
    return {};
  }

  ResponseMetadata metadata;
  metadata.set_error_code(static_cast<std::int32_t>(response.status_.code()));
  metadata.set_error_text(response.status_.message());

  std::string metadata_bytes;
  metadata.SerializeToString(&metadata_bytes);
  return metadata_bytes;
}

auto BuildResponseFrame(const ResponseEnvelope &response, const ProtocolLimits &limits) -> std::string {
  std::string metadata_bytes = EncodeResponseMetadata(response);

  FrameHeader header;
  header.message_type_ = MessageType::Response;
  header.request_id_ = response.request_id_;
  header.metadata_size_ = static_cast<uint32_t>(metadata_bytes.size());
  header.payload_size_ = static_cast<uint32_t>(response.payload_.size());

  return BuildFrame(header, metadata_bytes, response.payload_, limits);
}

auto DecodeRequestEnvelope(const FrameHeader &header, std::string_view metadata_bytes, std::string_view payload)
    -> std::optional<RequestEnvelope> {
  RequestMetadata metadata;
  if (!FitsProtobufParseArray(metadata_bytes) ||
      !metadata.ParseFromArray(metadata_bytes.data(), static_cast<int>(metadata_bytes.size()))) {
    return std::nullopt;
  }

  RequestEnvelope req;
  req.request_id_ = header.request_id_;
  req.service_name_ = metadata.service_name();
  req.method_name_ = metadata.method_name();
  req.payload_ = std::string(payload);

  return req;
}

auto DecodeStatus(std::int32_t code, std::string message) -> Status {
  // Unknown wire integers must not escape as application-visible StatusCode
  // values.
  switch (static_cast<StatusCode>(code)) {
    case StatusCode::Ok:
      return Status::Ok();
    case StatusCode::InvalidArgument:
    case StatusCode::DeadlineExceeded:
    case StatusCode::NotFound:
    case StatusCode::ResourceExhausted:
    case StatusCode::FailedPrecondition:
    case StatusCode::Unimplemented:
    case StatusCode::Internal:
    case StatusCode::Unavailable:
    case StatusCode::DataLoss:
      return {static_cast<StatusCode>(code), std::move(message)};
  }
  return {StatusCode::DataLoss, "response contains an invalid RPC status code"};
}

auto DecodeResponseEnvelope(const FrameHeader &header, std::string_view metadata_bytes, std::string_view payload)
    -> std::optional<ResponseEnvelope> {
  ResponseMetadata metadata;
  if (!FitsProtobufParseArray(metadata_bytes) ||
      !metadata.ParseFromArray(metadata_bytes.data(), static_cast<int>(metadata_bytes.size()))) {
    return std::nullopt;
  }

  ResponseEnvelope resp;
  resp.request_id_ = header.request_id_;
  resp.status_ = DecodeStatus(metadata.error_code(), metadata.error_text());
  resp.payload_ = std::string(payload);

  return resp;
}

/**
 * Validates framing and returns non-owning views into the caller's buffer.
 * Metadata and payload decoding begin only after a complete bounded frame is
 * available.
 */
auto ReadFrame(std::string_view buf, const ProtocolLimits &limits) -> FrameView {
  if (buf.size() < FrameHeader::SIZE) {
    return {.error_ = ProtocolError::NeedMoreData};
  }

  std::optional<FrameHeader> decoded_header = FrameHeader::Decode(buf.substr(0, FrameHeader::SIZE));
  if (!decoded_header) {
    return {.error_ = ProtocolError::InvalidMagic};
  }

  const FrameHeader &header = *decoded_header;

  if (header.version_ != FrameHeader::VERSION) {
    return {.error_ = ProtocolError::UnsupportedVersion};
  }

  if (header.metadata_size_ > limits.max_metadata_size_ || header.payload_size_ > limits.max_payload_size_ ||
      FrameSize(header.metadata_size_, header.payload_size_) > std::numeric_limits<std::size_t>::max()) {
    return {.error_ = ProtocolError::FrameTooLarge};
  }

  const std::size_t total = FrameHeader::SIZE + static_cast<std::size_t>(header.metadata_size_) +
                            static_cast<std::size_t>(header.payload_size_);
  if (buf.size() < total) {
    return {.error_ = ProtocolError::NeedMoreData};
  }

  return {
      .error_ = ProtocolError::Ok,
      .consumed_ = total,
      .header_ = header,
      .metadata_bytes_ = buf.substr(FrameHeader::SIZE, header.metadata_size_),
      .payload_ = buf.substr(FrameHeader::SIZE + header.metadata_size_, header.payload_size_),
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
  return limits;
}

FrameCodec::FrameCodec(ProtocolLimits limits) : limits_(limits) {}

auto FrameCodec::Encode(const RequestEnvelope &request) const -> std::string {
  return BuildRequestFrame(request, limits_);
}

auto FrameCodec::Encode(const ResponseEnvelope &response) const -> std::string {
  return BuildResponseFrame(response, limits_);
}

auto FrameCodec::Decode(std::string_view buf) const -> FrameDecodeResult {
  const FrameView frame = ReadFrame(buf, limits_);
  if (frame.error_ != ProtocolError::Ok) {
    return {.error_ = frame.error_, .consumed_ = frame.consumed_};
  }

  switch (frame.header_.message_type_) {
    case MessageType::Request: {
      std::optional<RequestEnvelope> request =
          DecodeRequestEnvelope(frame.header_, frame.metadata_bytes_, frame.payload_);
      if (!request) {
        return {.error_ = ProtocolError::DecodeError, .consumed_ = frame.consumed_};
      }
      return {.error_ = ProtocolError::Ok, .consumed_ = frame.consumed_, .request_ = std::move(request)};
    }

    case MessageType::Response: {
      std::optional<ResponseEnvelope> response =
          DecodeResponseEnvelope(frame.header_, frame.metadata_bytes_, frame.payload_);
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
