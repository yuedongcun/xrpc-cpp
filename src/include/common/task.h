/**
 * @file task.h
 * @brief Defines the coroutine `Task<T>` primitive used by the xRPC runtime.
 *
 * `Task<T>` owns a coroutine handle and manages the lifetime of its coroutine
 * frame. The promise stores the coroutine result, completion state,
 * continuation, and captured exception.
 *
 * Object model:
 *
 *   Task<T>
 *      |
 *      | owns
 *      v
 *   coroutine_handle<TaskPromise<T>>
 *      |
 *      v
 *   coroutine frame
 *      |
 *      +-- TaskPromise<T>
 *            +-- TaskStorage<T>
 *            +-- TaskCompletionState
 *            +-- continuation_
 *            +-- exception_
 *
 * Execution:
 *
 *   create coroutine
 *        |
 *        v
 *   initial_suspend
 *        |
 *        v
 *   Task owns suspended coroutine
 *        |
 *        +---- Start() / Get() ----> resume
 *        |
 *        `---- co_await -----------> transfer handle to Awaiter
 *                                      |
 *                                      v
 *                                   resume
 *                                      |
 *                                      v
 *                                 final_suspend
 *                                      |
 *                         +------------+------------+
 *                         |                         |
 *                         v                         v
 *                  publish completion       resume continuation
 *
 * `Wait()` and `WaitFor()` observe the completion state from blocking code.
 * `Result()` observes the stored result or rethrows a captured exception.
 *
 * Tasks start suspended and have single ownership of their coroutine handle.
 * Awaiting a task transfers that ownership to the awaiter. Completion is
 * published before the continuation is resumed.
 *
 * This file defines task lifetime and completion mechanics only; scheduling,
 * I/O completion, and thread affinity are provided by the runtime components
 * that resume the coroutine.
 */

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
    handle.promise().completion_.NotifyCompleted();
    std::coroutine_handle<> continuation = handle.promise().continuation_;
    if (!continuation) {
      return std::noop_coroutine();
    }
    return continuation;
  }

  void await_resume() const noexcept {}
};

template <typename T>
class TaskPromise {
 public:
  auto get_return_object() noexcept -> Task<T>;

  auto initial_suspend() noexcept -> std::suspend_always { return {}; }

  auto final_suspend() noexcept -> FinalAwaiter { return {}; }

  template <typename U>
  void return_value(U &&value) {
    storage_.ReturnValue(std::forward<U>(value));
  }

  void unhandled_exception() noexcept { exception_ = std::current_exception(); }

  TaskStorage<T> storage_;
  TaskCompletionState completion_;
  std::coroutine_handle<> continuation_;
  std::exception_ptr exception_;
};

template <>
class TaskPromise<void> {
 public:
  auto get_return_object() noexcept -> Task<void>;

  auto initial_suspend() noexcept -> std::suspend_always { return {}; }

  auto final_suspend() noexcept -> FinalAwaiter { return {}; }

  void return_void() noexcept { storage_.ReturnValue(); }

  void unhandled_exception() noexcept { exception_ = std::current_exception(); }

  TaskStorage<void> storage_;
  TaskCompletionState completion_;
  std::coroutine_handle<> continuation_;
  std::exception_ptr exception_;
};

}  // namespace task_detail

template <typename T>
class Task final {
 public:
  using promise_type = task_detail::TaskPromise<T>;
  using handle_type = std::coroutine_handle<promise_type>;

  explicit Task(handle_type handle) noexcept : handle_(handle) {}

  Task(const Task &) = delete;
  auto operator=(const Task &) -> Task & = delete;

  Task(Task &&other) noexcept
      : handle_(std::exchange(other.handle_, nullptr)), started_(std::exchange(other.started_, false)) {}

  auto operator=(Task &&other) noexcept -> Task & {
    if (this != &other) {
      Reset();
      handle_ = std::exchange(other.handle_, nullptr);
      started_ = std::exchange(other.started_, false);
    }
    return *this;
  }

  ~Task() { Reset(); }

  struct Awaiter {
    handle_type handle_;

    ~Awaiter() {
      if (handle_) {
        handle_.destroy();
      }
    }

    auto await_ready() const noexcept -> bool { return !handle_ || handle_.done(); }

    auto await_suspend(std::coroutine_handle<> continuation) const noexcept -> std::coroutine_handle<> {
      handle_.promise().continuation_ = continuation;
      return handle_;
    }

    auto await_resume() const -> T {
      if (handle_.promise().exception_) {
        std::rethrow_exception(handle_.promise().exception_);
      }
      return handle_.promise().storage_.Result();
    }
  };

  auto operator co_await() && noexcept -> Awaiter { return Awaiter{ReleaseHandle()}; }

  auto operator co_await() const & = delete;

  auto Result() -> T {
    RethrowIfFailed();
    return Promise().storage_.Result();
  }

  auto Get() -> T {
    Start();
    Wait();
    return Result();
  }

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
      Promise().completion_.WaitCompleted();
    }
  }

  template <typename Rep, typename Period>
  [[nodiscard]] auto WaitFor(const std::chrono::duration<Rep, Period> &timeout) const -> bool {
    if (!handle_) {
      return true;
    }
    return Promise().completion_.WaitCompletedFor(timeout);
  }

 private:
  auto ReleaseHandle() noexcept -> handle_type { return std::exchange(handle_, nullptr); }

  auto Promise() const -> promise_type & { return handle_.promise(); }

  void RethrowIfFailed() const {
    if (handle_ && Promise().exception_) {
      std::rethrow_exception(Promise().exception_);
    }
  }

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

template <typename T>
auto task_detail::TaskPromise<T>::get_return_object() noexcept -> Task<T> {
  return Task<T>{std::coroutine_handle<task_detail::TaskPromise<T>>::from_promise(*this)};
}

template <>
class Task<void> final {
 public:
  using promise_type = task_detail::TaskPromise<void>;
  using handle_type = std::coroutine_handle<promise_type>;

  explicit Task(handle_type handle) noexcept : handle_(handle) {}

  Task(const Task &) = delete;
  auto operator=(const Task &) -> Task & = delete;

  Task(Task &&other) noexcept
      : handle_(std::exchange(other.handle_, nullptr)), started_(std::exchange(other.started_, false)) {}

  auto operator=(Task &&other) noexcept -> Task & {
    if (this != &other) {
      Reset();
      handle_ = std::exchange(other.handle_, nullptr);
      started_ = std::exchange(other.started_, false);
    }
    return *this;
  }

  ~Task() { Reset(); }

  struct Awaiter {
    handle_type handle_;

    ~Awaiter() {
      if (handle_) {
        handle_.destroy();
      }
    }

    auto await_ready() const noexcept -> bool { return !handle_ || handle_.done(); }

    auto await_suspend(std::coroutine_handle<> continuation) const noexcept -> std::coroutine_handle<> {
      handle_.promise().continuation_ = continuation;
      return handle_;
    }

    void await_resume() const {
      if (handle_.promise().exception_) {
        std::rethrow_exception(handle_.promise().exception_);
      }
      handle_.promise().storage_.Result();
    }
  };

  auto operator co_await() && noexcept -> Awaiter { return Awaiter{ReleaseHandle()}; }

  auto operator co_await() const & = delete;

  auto Result() -> void {
    RethrowIfFailed();
    Promise().storage_.Result();
  }

  auto Get() -> void {
    Start();
    Wait();
    Result();
  }

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
      Promise().completion_.WaitCompleted();
    }
  }

  template <typename Rep, typename Period>
  [[nodiscard]] auto WaitFor(const std::chrono::duration<Rep, Period> &timeout) const -> bool {
    if (!handle_) {
      return true;
    }
    return Promise().completion_.WaitCompletedFor(timeout);
  }

 private:
  auto ReleaseHandle() noexcept -> handle_type { return std::exchange(handle_, nullptr); }

  auto Promise() const -> promise_type & { return handle_.promise(); }

  void RethrowIfFailed() const {
    if (handle_ && Promise().exception_) {
      std::rethrow_exception(Promise().exception_);
    }
  }

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

inline auto task_detail::TaskPromise<void>::get_return_object() noexcept -> Task<void> {
  return Task<void>{std::coroutine_handle<task_detail::TaskPromise<void>>::from_promise(*this)};
}

template <typename T>
auto SyncWait(Task<T> task) -> T {
  return task.Get();
}

inline auto SyncWait(Task<void> task) -> void { task.Get(); }

}  // namespace xrpc::runtime
