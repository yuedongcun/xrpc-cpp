#pragma once

#include <exception>
#include <optional>
#include <utility>

#include <xrpc/status.h>

namespace xrpc {

template <typename T>
class [[nodiscard]] StatusOr final {
 public:
  explicit StatusOr(T value) : value_(std::move(value)) {}

  explicit StatusOr(Status status) : status_(RequireErrorStatus(std::move(status))) {}

  [[nodiscard]] auto ok() const -> bool { return status_.ok(); }

  [[nodiscard]] auto status() const -> const Status & { return status_; }

  [[nodiscard]] auto value() const & -> const T & {
    RequireValue();
    return *value_;
  }

  [[nodiscard]] auto value() & -> T & {
    RequireValue();
    return *value_;
  }

  [[nodiscard]] auto value() && -> T && {
    RequireValue();
    return std::move(*value_);
  }

 private:
  [[nodiscard]] static auto RequireErrorStatus(Status status) -> Status {
    if (status.ok()) {
      std::terminate();
    }
    return status;
  }

  void RequireValue() const {
    if (!value_.has_value()) {
      std::terminate();
    }
  }

  Status status_;
  std::optional<T> value_;
};

}  // namespace xrpc
