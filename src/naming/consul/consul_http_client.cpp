#include "naming/consul/consul_http_client.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

#include "common/xrpc_exception.h"

#include "io/socket.h"

namespace xrpc {

namespace {

/** @brief Socket read buffer size used while parsing HTTP/1.1 responses. */
constexpr std::size_t READ_BUFFER_SIZE = 4096;

/**
 * @brief Removes leading and trailing ASCII whitespace from a borrowed string view.
 *
 * @param value Input view to trim.
 * @return View into `value` with surrounding whitespace removed.
 */
auto Trim(std::string_view value) -> std::string_view {
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())) != 0) {
    value.remove_prefix(1);
  }
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())) != 0) {
    value.remove_suffix(1);
  }
  return value;
}

/**
 * @brief Lowercases an HTTP header name or value for case-insensitive matching.
 *
 * @param value Text to normalize.
 * @return Lowercase copy of `value`.
 */
auto ToLower(std::string value) -> std::string {
  std::ranges::transform(value, value.begin(),
                         [](unsigned char ch) -> char { return static_cast<char>(std::tolower(ch)); });
  return value;
}

/**
 * @brief Parses the port component of a Consul agent address.
 *
 * @param port_text Decimal port text from `host:port`.
 * @return TCP port in host byte order.
 * @throws ConfigException when the port is absent, invalid, or out of range.
 */
auto ParsePort(std::string_view port_text) -> std::uint16_t {
  int parsed_port = 0;
  const char *begin = port_text.data();
  const char *end = port_text.data() + port_text.size();
  const auto result = std::from_chars(begin, end, parsed_port);
  if (result.ec != std::errc{} || result.ptr != end || parsed_port <= 0 || parsed_port > 65535) {
    throw ConfigException("invalid consul address port");
  }
  return static_cast<std::uint16_t>(parsed_port);
}

/**
 * @brief Parses an HTTP `Content-Length` value.
 *
 * @param value Header value after trimming.
 * @return Declared body length in bytes.
 * @throws ProtocolException when the value is not a decimal byte count.
 */
auto ParseContentLength(std::string_view value) -> std::size_t {
  std::size_t length = 0;
  const char *begin = value.data();
  const char *end = value.data() + value.size();
  const auto result = std::from_chars(begin, end, length);
  if (result.ec != std::errc{} || result.ptr != end) {
    throw ProtocolException(StatusCode::DataLoss, "invalid Content-Length header");
  }
  return length;
}

/**
 * @brief Parses one chunk-size line from a chunked HTTP response.
 *
 * Chunk extensions are ignored after the first semicolon because Consul responses only require
 * the size for this client.
 *
 * @param line Chunk-size line without the trailing CRLF.
 * @return Chunk payload length in bytes.
 * @throws ProtocolException when the size is not valid hexadecimal text.
 */
auto ParseChunkSize(std::string_view line) -> std::size_t {
  const std::size_t semicolon = line.find(';');
  if (semicolon != std::string_view::npos) {
    line = line.substr(0, semicolon);
  }
  line = Trim(line);

  std::size_t size = 0;
  const char *begin = line.data();
  const char *end = line.data() + line.size();
  const auto result = std::from_chars(begin, end, size, 16);
  if (result.ec != std::errc{} || result.ptr != end) {
    throw ProtocolException(StatusCode::DataLoss, "invalid chunk size");
  }
  return size;
}

/**
 * @brief Reads one socket chunk and appends it to an HTTP parsing buffer.
 *
 * @param socket Connected Consul socket.
 * @param buffer Accumulated response bytes.
 * @throws TransportException when the server closes before the expected response bytes arrive.
 */
void ReadMore(io::Socket &socket, std::string &buffer) {
  char chunk[READ_BUFFER_SIZE];
  const ssize_t received = socket.Read(chunk, sizeof(chunk));
  if (received <= 0) {
    throw TransportException(StatusCode::Unavailable, "unexpected EOF while reading HTTP response");
  }
  buffer.append(chunk, static_cast<std::size_t>(received));
}

/**
 * @brief Reads an HTTP response body until the Consul server closes the connection.
 *
 * @param socket Connected Consul socket.
 * @param buffer Body bytes already read after the header separator.
 * @return Complete response body.
 * @throws TransportException when socket reads fail before EOF.
 */
auto ReadUntilClose(io::Socket &socket, std::string buffer) -> std::string {
  char chunk[READ_BUFFER_SIZE];
  while (true) {
    const ssize_t received = socket.Read(chunk, sizeof(chunk));
    if (received == 0) {
      return buffer;
    }
    if (received < 0) {
      throw TransportException(StatusCode::Unavailable, "failed to read HTTP response body");
    }
    buffer.append(chunk, static_cast<std::size_t>(received));
  }
}

/**
 * @brief Reads an HTTP response body with a declared content length.
 *
 * @param socket Connected Consul socket.
 * @param buffer Body bytes already read after the header separator.
 * @param content_length Expected body length in bytes.
 * @return Body truncated to exactly `content_length` bytes.
 */
auto ReadContentLengthBody(io::Socket &socket, std::string buffer, std::size_t content_length) -> std::string {
  while (buffer.size() < content_length) {
    ReadMore(socket, buffer);
  }
  buffer.resize(content_length);
  return buffer;
}

/**
 * @brief Decodes an HTTP response body using `Transfer-Encoding: chunked`.
 *
 * @param socket Connected Consul socket.
 * @param buffer Body bytes already read after the header separator.
 * @return Concatenated chunk payload bytes.
 * @throws ProtocolException when chunk framing is malformed.
 */
auto ReadChunkedBody(io::Socket &socket, std::string buffer) -> std::string {
  std::string body;
  while (true) {
    std::size_t line_end = buffer.find("\r\n");
    while (line_end == std::string::npos) {
      ReadMore(socket, buffer);
      line_end = buffer.find("\r\n");
    }

    const std::size_t chunk_size = ParseChunkSize(std::string_view(buffer.data(), line_end));
    buffer.erase(0, line_end + 2);
    if (chunk_size == 0) {
      return body;
    }

    while (buffer.size() < chunk_size + 2) {
      ReadMore(socket, buffer);
    }
    body.append(buffer.data(), chunk_size);
    if (buffer.substr(chunk_size, 2) != "\r\n") {
      throw ProtocolException(StatusCode::DataLoss, "invalid chunk terminator");
    }
    buffer.erase(0, chunk_size + 2);
  }
}

}  // namespace

/**
 * @brief Creates a minimal HTTP/1.1 client for a Consul agent address.
 *
 * @param address Consul agent address in `host:port` form.
 * @throws ConfigException when the address cannot be parsed.
 */
ConsulHttpClient::ConsulHttpClient(const std::string &address) {
  const std::string_view address_view(address);
  const std::size_t colon = address_view.rfind(':');
  if (colon == std::string_view::npos || colon == 0 || colon + 1 >= address_view.size()) {
    throw ConfigException("consul_address must be host:port");
  }
  host_ = std::string(address_view.substr(0, colon));
  port_ = ParsePort(address_view.substr(colon + 1));
}

/**
 * @brief Sends an HTTP GET request to the configured Consul agent.
 *
 * @param path Absolute Consul API path.
 * @param timeout Connect and socket I/O timeout.
 * @return Parsed HTTP response, or a public status mapped from server/protocol failures.
 */
auto ConsulHttpClient::Get(std::string_view path, std::chrono::milliseconds timeout) const
    -> StatusOr<ConsulHttpResponse> {
  return SendRequest("GET", path, "", timeout);
}

/**
 * @brief Sends an HTTP PUT request to the configured Consul agent.
 *
 * @param path Absolute Consul API path.
 * @param body Request body bytes, usually JSON for registration calls.
 * @param timeout Connect and socket I/O timeout.
 * @return Parsed HTTP response, or a public status mapped from server/protocol failures.
 */
auto ConsulHttpClient::Put(std::string_view path, std::string_view body, std::chrono::milliseconds timeout) const
    -> StatusOr<ConsulHttpResponse> {
  return SendRequest("PUT", path, body, timeout);
}

/**
 * @brief Performs one blocking HTTP/1.1 request and parses status, headers, and body.
 *
 * This client intentionally implements only the small HTTP subset needed for Consul agent/catalog
 * calls: status line parsing, case-insensitive headers, fixed-length bodies, chunked bodies, and
 * connection-close bodies.
 *
 * @param method HTTP method text such as `GET` or `PUT`.
 * @param path Absolute Consul API path.
 * @param body Request body bytes.
 * @param timeout Connect and socket I/O timeout.
 * @return Parsed response, or a status converted from any caught exception.
 */
auto ConsulHttpClient::SendRequest(std::string_view method, std::string_view path, std::string_view body,
                                   std::chrono::milliseconds timeout) const -> StatusOr<ConsulHttpResponse> {
  try {
    io::Socket socket;
    socket.Connect(host_, port_, timeout);
    socket.SetReadWriteTimeout(timeout);

    std::string request;
    request.reserve(method.size() + path.size() + host_.size() + body.size() + 128);
    request.append(method);
    request.push_back(' ');
    request.append(path);
    request.append(" HTTP/1.1\r\nHost: ");
    request.append(host_);
    request.append(":");
    request.append(std::to_string(port_));
    request.append("\r\nConnection: close\r\nAccept: application/json\r\nContent-Length: ");
    request.append(std::to_string(body.size()));
    request.append("\r\n\r\n");
    request.append(body);
    socket.WriteAll(request);

    std::string buffer;
    std::size_t header_end = std::string::npos;
    while (header_end == std::string::npos) {
      ReadMore(socket, buffer);
      header_end = buffer.find("\r\n\r\n");
    }

    const std::string header_block = buffer.substr(0, header_end);
    std::string body_buffer = buffer.substr(header_end + 4);

    const std::size_t status_line_end = header_block.find("\r\n");
    const std::string_view status_line = std::string_view(header_block).substr(0, status_line_end);
    const std::size_t first_space = status_line.find(' ');
    const std::size_t second_space = status_line.find(' ', first_space + 1);
    if (first_space == std::string_view::npos || second_space == std::string_view::npos) {
      throw ProtocolException(StatusCode::DataLoss, "invalid HTTP status line");
    }
    int status_code = 0;
    const std::string_view status_code_text = status_line.substr(first_space + 1, second_space - first_space - 1);
    const auto status_result =
        std::from_chars(status_code_text.data(), status_code_text.data() + status_code_text.size(), status_code);
    if (status_result.ec != std::errc{}) {
      throw ProtocolException(StatusCode::DataLoss, "invalid HTTP status code");
    }

    ConsulHttpResponse response;
    response.status_code_ = status_code;
    std::size_t line_start = status_line_end == std::string::npos ? header_block.size() : status_line_end + 2;
    while (line_start < header_block.size()) {
      const std::size_t line_end = header_block.find("\r\n", line_start);
      const std::string_view line = line_end == std::string::npos
                                        ? std::string_view(header_block).substr(line_start)
                                        : std::string_view(header_block).substr(line_start, line_end - line_start);
      const std::size_t colon = line.find(':');
      if (colon != std::string_view::npos) {
        response.headers_.emplace(ToLower(std::string(Trim(line.substr(0, colon)))),
                                  std::string(Trim(line.substr(colon + 1))));
      }
      if (line_end == std::string::npos) {
        break;
      }
      line_start = line_end + 2;
    }

    const auto content_length_it = response.headers_.find("content-length");
    const auto transfer_encoding_it = response.headers_.find("transfer-encoding");
    if (content_length_it != response.headers_.end()) {
      response.body_ =
          ReadContentLengthBody(socket, std::move(body_buffer), ParseContentLength(content_length_it->second));
    } else if (transfer_encoding_it != response.headers_.end() &&
               ToLower(transfer_encoding_it->second).find("chunked") != std::string::npos) {
      response.body_ = ReadChunkedBody(socket, std::move(body_buffer));
    } else {
      response.body_ = ReadUntilClose(socket, std::move(body_buffer));
    }

    return StatusOr<ConsulHttpResponse>(std::move(response));
  } catch (...) {
    return StatusOr<ConsulHttpResponse>(CaughtExceptionToStatus(StatusCode::Unavailable, "HTTP request failed"));
  }
}

}  // namespace xrpc
