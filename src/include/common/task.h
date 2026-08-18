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

// Design note:
// - Ownership: Task owns a coroutine handle until it is started, awaited, or destroyed.
// - Completion: the promise records completion before resuming a continuation so
//   blocking Wait() and co_await observe the same terminal state.
// - Failure: exceptions are stored in the promise and rethrown by Get().
// - Scope: coroutine mechanics stay in this file; callers should use Task like a
//   small move-only future.
template <typename T = void>
class Task;

namespace task_detail {

// Shared completion primitive used by Task::Wait() and the final coroutine
// suspension point. It deliberately does not own the coroutine handle.
class TaskCompletionState {
 public:
  /** @brief Marks the task complete and wakes all blocking waiters. */
  void NotifyCompleted() noexcept {
    {
      std::lock_guard<std::mutex> lock(completion_mutex_);
      completed_ = true;
    }
    completion_cv_.notify_all();
  }

  /** @brief Blocks until the associated coroutine reaches final suspension. */
  void WaitCompleted() const {
    std::unique_lock<std::mutex> lock(completion_mutex_);
    completion_cv_.wait(lock, [this]() -> bool { return completed_; });
  }

  /** @return true when completion is observed before `timeout` expires. */
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

  /** @brief Stores a value returned by the coroutine. */
  template <typename U>
  void ReturnValue(U &&value) {
    value_.emplace(std::forward<U>(value));
  }

  /** @return Stored coroutine result, moved out exactly once. */
  auto Result() -> T {
    if (!value_.has_value()) {
      throw LifecycleException("task result is not available");
    }
    return std::move(*value_);
  }
};

template <>
struct TaskStorage<void> {
  /** @brief Records a void coroutine return. */
  void ReturnValue() noexcept {}

  /** @brief Void result accessor used to share promise code with value tasks. */
  void Result() const noexcept {}
};

struct FinalAwaiter {
  /** @return false so final suspension always runs completion bookkeeping. */
  auto await_ready() const noexcept -> bool { return false; }

  /** @brief Marks completion and resumes the awaiting continuation, if any. */
  template <typename Promise>
  auto await_suspend(std::coroutine_handle<Promise> handle) const noexcept -> std::coroutine_handle<> {
    // Mark completion before resuming the awaiting coroutine so blocking Wait()
    // and co_await observe the same terminal state.
    handle.promise().NotifyCompleted();
    std::coroutine_handle<> continuation = handle.promise().continuation_;
    if (!continuation) {
      return std::noop_coroutine();
    }
    return continuation;
  }

  /** @brief Final suspend has no user-visible result. */
  void await_resume() const noexcept {}
};

template <typename T>
class TaskPromise : public TaskStorage<T>, public TaskCompletionState {
 public:
  /** @return Owning `Task<T>` associated with this promise. */
  auto get_return_object() noexcept -> Task<T>;

  /** @brief Starts tasks lazily; callers must explicitly start or await them. */
  auto initial_suspend() noexcept -> std::suspend_always { return {}; }

  /** @brief Runs shared completion bookkeeping at coroutine final suspension. */
  auto final_suspend() noexcept -> FinalAwaiter { return {}; }

  /** @brief Stores the value returned by `co_return`. */
  template <typename U>
  void return_value(U &&value) {
    this->ReturnValue(std::forward<U>(value));
  }

  /** @brief Captures an exception so `Get()` or `co_await` can rethrow it. */
  void unhandled_exception() noexcept { exception_ = std::current_exception(); }

  std::coroutine_handle<> continuation_;
  std::exception_ptr exception_;
};

template <>
class TaskPromise<void> : public TaskStorage<void>, public TaskCompletionState {
 public:
  /** @return Owning `Task<void>` associated with this promise. */
  auto get_return_object() noexcept -> Task<void>;

  /** @brief Starts tasks lazily; callers must explicitly start or await them. */
  auto initial_suspend() noexcept -> std::suspend_always { return {}; }

  /** @brief Runs shared completion bookkeeping at coroutine final suspension. */
  auto final_suspend() noexcept -> FinalAwaiter { return {}; }

  /** @brief Records a `co_return` from a void coroutine. */
  void return_void() noexcept { this->ReturnValue(); }

  /** @brief Captures an exception so `Get()` or `co_await` can rethrow it. */
  void unhandled_exception() noexcept { exception_ = std::current_exception(); }

  std::coroutine_handle<> continuation_;
  std::exception_ptr exception_;
};

template <typename T>
class TaskBase {
 public:
  using promise_type = TaskPromise<T>;
  using handle_type = std::coroutine_handle<promise_type>;

  /** @brief Takes ownership of a newly created coroutine handle. */
  explicit TaskBase(handle_type handle) noexcept : handle_(handle) {}

  TaskBase(const TaskBase &) = delete;
  auto operator=(const TaskBase &) -> TaskBase & = delete;

  /** @brief Moves coroutine ownership from another task base. */
  TaskBase(TaskBase &&other) noexcept
      : handle_(std::exchange(other.handle_, nullptr)), started_(std::exchange(other.started_, false)) {}

  /** @brief Replaces this task's owned coroutine with another task base's coroutine. */
  auto operator=(TaskBase &&other) noexcept -> TaskBase & {
    if (this != &other) {
      Reset();
      handle_ = std::exchange(other.handle_, nullptr);
      started_ = std::exchange(other.started_, false);
    }

    return *this;
  }

  /** @brief Destroys an unstarted or detached coroutine frame still owned by this task. */
  ~TaskBase() { Reset(); }

  /** @return true when there is no coroutine handle or the coroutine has completed. */
  auto Done() const noexcept -> bool { return !handle_ || handle_.done(); }

  /** @brief Starts the coroutine if it has not already started. */
  auto Start() -> void {
    if (!handle_ || started_ || handle_.done()) {
      return;
    }
    started_ = true;
    handle_.resume();
  }

  /** @brief Blocks until the coroutine completes. */
  void Wait() const {
    if (handle_) {
      Promise().WaitCompleted();
    }
  }

  /** @return true when the coroutine completes before `timeout`. */
  template <typename Rep, typename Period>
  [[nodiscard]] auto WaitFor(const std::chrono::duration<Rep, Period> &timeout) const -> bool {
    if (!handle_) {
      return true;
    }
    return Promise().WaitCompletedFor(timeout);
  }

 protected:
  /** @brief Releases coroutine ownership to an awaiter. */
  auto ReleaseHandle() noexcept -> handle_type { return std::exchange(handle_, nullptr); }

  /** @return Raw coroutine handle owned by this task. */
  auto Handle() const noexcept -> handle_type { return handle_; }

  /** @return Promise associated with the owned coroutine handle. */
  auto Promise() const -> promise_type & { return handle_.promise(); }

  /** @brief Rethrows any exception captured by the promise. */
  void RethrowIfFailed() const {
    if (handle_ && Promise().exception_) {
      std::rethrow_exception(Promise().exception_);
    }
  }

 private:
  /** @brief Destroys the owned coroutine frame and clears start state. */
  void Reset() {
    if (handle_) {
      // Task owns an unstarted or detached coroutine handle. Awaiting an rvalue
      // transfers that ownership to Awaiter via ReleaseHandle().
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

    /** @brief Destroys the child coroutine if the parent await is abandoned. */
    ~Awaiter() {
      if (handle_) {
        // If the awaiting coroutine is abandoned before completion, destroy the
        // child coroutine rather than leaking its frame.
        handle_.destroy();
      }
    }

    /** @return true when there is no coroutine to resume or it is already complete. */
    auto await_ready() const noexcept -> bool { return !handle_ || handle_.done(); }

    /** @brief Stores the awaiting continuation and starts the child coroutine. */
    auto await_suspend(std::coroutine_handle<> continuation) const -> bool {
      handle_.promise().continuation_ = continuation;
      handle_.resume();
      return true;
    }

    /** @return Result produced by the child coroutine, rethrowing captured failures. */
    auto await_resume() const -> T {
      if (handle_.promise().exception_) {
        std::rethrow_exception(handle_.promise().exception_);
      }
      return handle_.promise().Result();
    }
  };

  using task_detail::TaskBase<T>::TaskBase;

  // Task is single-shot when awaited: only rvalues can transfer the coroutine
  // handle into an Awaiter.
  /** @return Awaiter that takes ownership of this task's coroutine handle. */
  auto operator co_await() && noexcept -> Awaiter { return Awaiter{this->ReleaseHandle()}; }

  auto operator co_await() const & = delete;

  /** @return Coroutine result after completion, rethrowing captured failures. */
  auto Result() -> T {
    this->RethrowIfFailed();
    return this->Promise().Result();
  }

  /** @brief Starts the task, waits for completion, and returns the result. */
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

/** @return Owning `Task<T>` for a newly created value coroutine promise. */
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

    /** @brief Destroys the child coroutine if the parent await is abandoned. */
    ~Awaiter() {
      if (handle_) {
        // Mirrors the value Task awaiter: abandoning the await destroys the
        // child coroutine frame.
        handle_.destroy();
      }
    }

    /** @return true when there is no coroutine to resume or it is already complete. */
    auto await_ready() const noexcept -> bool { return !handle_ || handle_.done(); }

    /** @brief Stores the awaiting continuation and starts the child coroutine. */
    auto await_suspend(std::coroutine_handle<> continuation) const -> bool {
      handle_.promise().continuation_ = continuation;
      handle_.resume();
      return true;
    }

    /** @brief Rethrows any captured exception after the child coroutine completes. */
    void await_resume() const {
      if (handle_.promise().exception_) {
        std::rethrow_exception(handle_.promise().exception_);
      }
      handle_.promise().Result();
    }
  };

  using task_detail::TaskBase<void>::TaskBase;

  // Task is single-shot when awaited: only rvalues can transfer the coroutine
  // handle into an Awaiter.
  /** @return Awaiter that takes ownership of this task's coroutine handle. */
  auto operator co_await() && noexcept -> Awaiter { return Awaiter{this->ReleaseHandle()}; }

  auto operator co_await() const & = delete;

  /** @brief Checks completion result and rethrows captured failures. */
  auto Result() -> void {
    this->RethrowIfFailed();
    this->Promise().Result();
  }

  /** @brief Starts the task, waits for completion, and checks the result. */
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

/** @return Owning `Task<void>` for a newly created void coroutine promise. */
inline auto task_detail::TaskPromise<void>::get_return_object() noexcept -> Task<void> {
  return Task<void>{std::coroutine_handle<task_detail::TaskPromise<void>>::from_promise(*this)};
}

/** @return Result of running `task` to completion on the current thread. */
template <typename T>
auto SyncWait(Task<T> task) -> T {
  return task.Get();
}

/** @brief Runs a void task to completion on the current thread. */
inline auto SyncWait(Task<void> task) -> void { task.Get(); }

}  // namespace xrpc::runtime
