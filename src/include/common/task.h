#pragma once

#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <exception>
#include <mutex>
#include <optional>
#include <utility>

#include "common/xrpc_exception.h"

namespace xrpc::runtime {

template <typename T = void>
class Task;

namespace task_detail {

class TaskCompletionState {
 public:
  void NotifyCompleted() noexcept {
    {
      std::lock_guard<std::mutex> lock(completion_mutex_);
      completed_ = true;
    }
    completion_cv_.notify_all();
  }

  void WaitCompleted() const {
    std::unique_lock<std::mutex> lock(completion_mutex_);
    completion_cv_.wait(lock, [this]() -> bool { return completed_; });
  }

  template <typename Rep, typename Period>
  [[nodiscard]] auto WaitCompletedFor(const std::chrono::duration<Rep, Period> &timeout) const -> bool {
    std::unique_lock<std::mutex> lock(completion_mutex_);
    return completion_cv_.wait_for(lock, timeout, [this]() -> bool { return completed_; });
  }

 private:
  mutable std::mutex completion_mutex_;
  mutable std::condition_variable completion_cv_;
  bool completed_ = false;
};

template <typename T>
struct TaskStorage {
  std::optional<T> value_;

  template <typename U>
  void ReturnValue(U &&value) {
    value_.emplace(std::forward<U>(value));
  }

  auto Result() -> T {
    if (!value_.has_value()) {
      throw LifecycleException("task result is not available");
    }
    return std::move(*value_);
  }
};

template <>
struct TaskStorage<void> {
  void ReturnValue() noexcept {}

  void Result() const noexcept {}
};

struct FinalAwaiter {
  auto await_ready() const noexcept -> bool { return false; }

  template <typename Promise>
  auto await_suspend(std::coroutine_handle<Promise> handle) const noexcept -> std::coroutine_handle<> {
    handle.promise().NotifyCompleted();
    std::coroutine_handle<> continuation = handle.promise().continuation_;
    if (!continuation) {
      return std::noop_coroutine();
    }
    return continuation;
  }

  void await_resume() const noexcept {}
};

template <typename T>
class TaskPromise : public TaskStorage<T>, public TaskCompletionState {
 public:
  auto get_return_object() noexcept -> Task<T>;

  auto initial_suspend() noexcept -> std::suspend_always { return {}; }

  auto final_suspend() noexcept -> FinalAwaiter { return {}; }

  template <typename U>
  void return_value(U &&value) {
    this->ReturnValue(std::forward<U>(value));
  }

  void unhandled_exception() noexcept { exception_ = std::current_exception(); }

  std::coroutine_handle<> continuation_;
  std::exception_ptr exception_;
};

template <>
class TaskPromise<void> : public TaskStorage<void>, public TaskCompletionState {
 public:
  auto get_return_object() noexcept -> Task<void>;

  auto initial_suspend() noexcept -> std::suspend_always { return {}; }

  auto final_suspend() noexcept -> FinalAwaiter { return {}; }

  void return_void() noexcept { this->ReturnValue(); }

  void unhandled_exception() noexcept { exception_ = std::current_exception(); }

  std::coroutine_handle<> continuation_;
  std::exception_ptr exception_;
};

template <typename T>
class TaskBase {
 public:
  using promise_type = TaskPromise<T>;
  using handle_type = std::coroutine_handle<promise_type>;

  explicit TaskBase(handle_type handle) noexcept : handle_(handle) {}

  TaskBase(const TaskBase &) = delete;
  auto operator=(const TaskBase &) -> TaskBase & = delete;

  TaskBase(TaskBase &&other) noexcept
      : handle_(std::exchange(other.handle_, nullptr)), started_(std::exchange(other.started_, false)) {}

  auto operator=(TaskBase &&other) noexcept -> TaskBase & {
    if (this != &other) {
      Reset();
      handle_ = std::exchange(other.handle_, nullptr);
      started_ = std::exchange(other.started_, false);
    }

    return *this;
  }

  ~TaskBase() { Reset(); }

  auto Done() const noexcept -> bool { return !handle_ || handle_.done(); }

  auto Start() -> void {
    if (!handle_ || started_ || handle_.done()) {
      return;
    }
    started_ = true;
    handle_.resume();
  }

  void Wait() const {
    if (handle_) {
      Promise().WaitCompleted();
    }
  }

  template <typename Rep, typename Period>
  [[nodiscard]] auto WaitFor(const std::chrono::duration<Rep, Period> &timeout) const -> bool {
    if (!handle_) {
      return true;
    }
    return Promise().WaitCompletedFor(timeout);
  }

 protected:
  auto ReleaseHandle() noexcept -> handle_type { return std::exchange(handle_, nullptr); }

  auto Handle() const noexcept -> handle_type { return handle_; }

  auto Promise() const -> promise_type & { return handle_.promise(); }

  void RethrowIfFailed() const {
    if (handle_ && Promise().exception_) {
      std::rethrow_exception(Promise().exception_);
    }
  }

 private:
  void Reset() {
    if (handle_) {
      handle_.destroy();
      handle_ = nullptr;
    }
    started_ = false;
  }

  handle_type handle_;
  bool started_ = false;
};

}  // namespace task_detail

template <typename T>
class Task final : public task_detail::TaskBase<T> {
 public:
  using promise_type = typename task_detail::TaskBase<T>::promise_type;
  using handle_type = typename task_detail::TaskBase<T>::handle_type;

  struct Awaiter {
    handle_type handle_;

    ~Awaiter() {
      if (handle_) {
        handle_.destroy();
      }
    }

    auto await_ready() const noexcept -> bool { return !handle_ || handle_.done(); }

    auto await_suspend(std::coroutine_handle<> continuation) const -> bool {
      handle_.promise().continuation_ = continuation;
      handle_.resume();
      return true;
    }

    auto await_resume() const -> T {
      if (handle_.promise().exception_) {
        std::rethrow_exception(handle_.promise().exception_);
      }
      return handle_.promise().Result();
    }
  };

  using task_detail::TaskBase<T>::TaskBase;

  auto operator co_await() && noexcept -> Awaiter { return Awaiter{this->ReleaseHandle()}; }

  auto operator co_await() const & = delete;

  auto Result() -> T {
    this->RethrowIfFailed();
    return this->Promise().Result();
  }

  auto Get() -> T {
    Start();
    Wait();
    return Result();
  }

  using task_detail::TaskBase<T>::Done;
  using task_detail::TaskBase<T>::Start;
  using task_detail::TaskBase<T>::Wait;
  using task_detail::TaskBase<T>::WaitFor;
};

template <typename T>
auto task_detail::TaskPromise<T>::get_return_object() noexcept -> Task<T> {
  return Task<T>{std::coroutine_handle<task_detail::TaskPromise<T>>::from_promise(*this)};
}

template <>
class Task<void> final : public task_detail::TaskBase<void> {
 public:
  using promise_type = typename task_detail::TaskBase<void>::promise_type;
  using handle_type = typename task_detail::TaskBase<void>::handle_type;

  struct Awaiter {
    handle_type handle_;

    ~Awaiter() {
      if (handle_) {
        handle_.destroy();
      }
    }

    auto await_ready() const noexcept -> bool { return !handle_ || handle_.done(); }

    auto await_suspend(std::coroutine_handle<> continuation) const -> bool {
      handle_.promise().continuation_ = continuation;
      handle_.resume();
      return true;
    }

    void await_resume() const {
      if (handle_.promise().exception_) {
        std::rethrow_exception(handle_.promise().exception_);
      }
      handle_.promise().Result();
    }
  };

  using task_detail::TaskBase<void>::TaskBase;

  auto operator co_await() && noexcept -> Awaiter { return Awaiter{this->ReleaseHandle()}; }

  auto operator co_await() const & = delete;

  auto Result() -> void {
    this->RethrowIfFailed();
    this->Promise().Result();
  }

  auto Get() -> void {
    Start();
    Wait();
    Result();
  }

  using task_detail::TaskBase<void>::Done;
  using task_detail::TaskBase<void>::Start;
  using task_detail::TaskBase<void>::Wait;
  using task_detail::TaskBase<void>::WaitFor;
};

inline auto task_detail::TaskPromise<void>::get_return_object() noexcept -> Task<void> {
  return Task<void>{std::coroutine_handle<task_detail::TaskPromise<void>>::from_promise(*this)};
}

template <typename T>
auto SyncWait(Task<T> task) -> T {
  return task.Get();
}

inline auto SyncWait(Task<void> task) -> void { task.Get(); }

}  // namespace xrpc::runtime
