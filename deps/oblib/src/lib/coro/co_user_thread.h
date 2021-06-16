/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef CO_USER_THREAD_H
#define CO_USER_THREAD_H

#include <functional>
#include "lib/ob_errno.h"
#include "lib/coro/co_config.h"
#include "lib/coro/thread.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/utility/ob_hang_fatal_error.h"

namespace oceanbase {
namespace lib {

// A compatible default runnable thread implementation. Some module
// doesn't need use co-routine relating features and the normal way to
// create a scheduler and a worker is too complicate for that
// situation, therefore use CoUserThread is a convenience way to that
// person.
//
// Don't use this class directly, see @CoKThread instead.
template <class Sched>
class CoUserThreadTemp : public Sched {
  using RunFuncT = std::function<void()>;

public:
  explicit CoUserThreadTemp(const RunFuncT& runnable, int64_t stack_size = 0)
      : worker_(*this, runnable), stack_size_(stack_size), max_tasks_(INT64_MAX), cur_tasks_(0)
  {}
  int prepare()
  {
    int ret = common::OB_SUCCESS;
    if (OB_SUCC(Sched::prepare())) {
      ret = worker_.init(stack_size_);
    }
    return ret;
  }

  void set_max_tasks(int64_t tasks)
  {
    max_tasks_ = tasks;
  }

  int submit(RunFuncT&& func)
  {
    int ret = common::OB_SUCCESS;
    if (ATOMIC_FAA(&cur_tasks_, 1) < max_tasks_) {
      ret = CoBaseSched::submit([this, func] {
        func();
        ATOMIC_DEC(&cur_tasks_);
      });
    } else {
      ret = common::OB_SIZE_OVERFLOW;
      ATOMIC_DEC(&cur_tasks_);
    }
    return ret;
  }
  int submit(const RunFuncT& func)
  {
    int ret = common::OB_SUCCESS;
    if (ATOMIC_FAA(&cur_tasks_, 1) < max_tasks_) {
      ret = CoBaseSched::submit([this, func] {
        func();
        ATOMIC_DEC(&cur_tasks_);
      });
    } else {
      ret = common::OB_SIZE_OVERFLOW;
      ATOMIC_DEC(&cur_tasks_);
    }
    return ret;
  }

  int64_t get_cur_tasks() const
  {
    return cur_tasks_;
  }

private:
  typename Sched::Worker worker_;
  int64_t stack_size_;
  int64_t max_tasks_;
  int64_t cur_tasks_;
};

template <class Thread>
class CoKThreadTemp {
public:
  using RunFuncT = std::function<void()>;

  explicit CoKThreadTemp(int64_t n_threads = 1)
      : n_threads_(n_threads), threads_(nullptr), stack_size_(0), stop_(true), idx_(0), thread_max_tasks_(INT64_MAX)
  {}
  virtual ~CoKThreadTemp();

  /// \brief Set number of threads for running.
  ///
  /// When set before threads are running, this function simply set
  /// local varible which would be read for \c run().
  ///
  /// When set after threads are running, this function would adjust
  /// real threads count other than set local variable.
  ///
  /// \param n_threads Number of threads to set.
  ///
  /// \return Return OB_SUCCESS if threads count has successfully
  ///         adjust to that number, i.e. there are such exact number
  ///         of threads are running if it has started, or would run
  ///         after call \c start() function.
  int set_thread_count(int64_t n_threads);

  int inc_thread_count(int64_t inc = 1);

  /// \brief Set stack size for creating routine.
  ///
  /// Set stack size for creating routine which would finally call the
  /// \c run function. Don't be confused with stack size of creating
  /// thread and stack size of creating routine. Thread stack is only
  /// used for routine schedule so that it can be very small, whereas
  /// routine stack is used by outsider within the \c run function
  /// which contributes mostly.
  ///
  /// \param size The size of routine stack in bytes.
  ///
  void set_stack_size(int64_t size)
  {
    stack_size_ = size;
  }

  int init();
  virtual int start();
  virtual void stop();
  virtual void wait();
  void destroy();

  pid_t get_tid()
  {
    OB_ASSERT(n_threads_ > 0);
    return threads_[0]->get_tid();
  }

public:
  void set_thread_max_tasks(uint64_t cnt);

  int submit_to(uint64_t idx, RunFuncT& func);
  // template <class Functor>
  // int submit(Functor &&func);
  template <class Functor>
  int submit(const Functor& func);

  int64_t get_cur_tasks() const;

protected:
  virtual bool has_set_stop() const
  {
    return ATOMIC_LOAD(&stop_);
  }
  bool& has_set_stop()
  {
    return stop_;
  }
  int64_t get_thread_count() const
  {
    return n_threads_;
  }

private:
  virtual void run(int64_t idx) = 0;

  /// \brief Create thread with start entry \c entry.
  int create_thread(Thread*& thread, std::function<void()> entry);

  /// \brief Destroy thread.
  void destroy_thread(Thread* thread);

private:
  int64_t n_threads_;
  Thread** threads_;
  int64_t stack_size_;
  bool stop_;
  // protect for thread count changing.
  common::SpinRWLock lock_;
  uint64_t idx_;
  int64_t thread_max_tasks_;
};

template <class Thread>
CoKThreadTemp<Thread>::~CoKThreadTemp()
{
  stop();
  wait();
  destroy();
}

template <class Thread>
int CoKThreadTemp<Thread>::inc_thread_count(int64_t inc)
{
  return set_thread_count(n_threads_ + inc);
}

template <class Thread>
int CoKThreadTemp<Thread>::set_thread_count(int64_t n_threads)
{
  int ret = common::OB_SUCCESS;
  common::SpinWLockGuard g(lock_);
  if (!stop_) {
    if (n_threads < n_threads_) {
      for (auto i = n_threads; i < n_threads_; i++) {
        threads_[i]->stop();
      }
      for (auto i = n_threads; i < n_threads_; i++) {
        auto thread = threads_[i];
        thread->wait();
        thread->destroy();
        thread->~Thread();
        threads_[i] = nullptr;
      }
      n_threads_ = n_threads;
    } else if (n_threads == n_threads_) {
    } else {
      auto new_threads = reinterpret_cast<Thread**>(::malloc(sizeof(Thread*) * n_threads));
      if (new_threads == nullptr) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMSET(new_threads, 0, sizeof(Thread*) * n_threads);
        MEMCPY(new_threads, threads_, sizeof(Thread*) * n_threads_);
        for (auto i = n_threads_; i < n_threads; i++) {
          Thread* thread = nullptr;
          if (OB_FAIL(create_thread(thread, [this, i] { this->run(i); }))) {
            break;
          } else {
            new_threads[i] = thread;
          }
        }
        if (OB_FAIL(ret)) {
          for (auto i = n_threads_; i < n_threads; i++) {
            if (new_threads[i] != nullptr) {
              destroy_thread(new_threads[i]);
              new_threads[i] = nullptr;
            }
          }
          ::free(new_threads);
        } else {
          threads_ = new_threads;
          n_threads_ = n_threads;
        }
      }
    }
  } else {
    n_threads_ = n_threads;
  }
  return ret;
}

template <class Thread>
int CoKThreadTemp<Thread>::init()
{
  return common::OB_SUCCESS;
}

template <class Thread>
int CoKThreadTemp<Thread>::start()
{
  int ret = common::OB_SUCCESS;
  threads_ = reinterpret_cast<Thread**>(::malloc(sizeof(Thread*) * n_threads_));
  if (threads_ == nullptr) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  } else {
    stop_ = false;
    MEMSET(threads_, 0, sizeof(Thread*) * n_threads_);
    for (int i = 0; i < n_threads_; i++) {
      Thread* thread = nullptr;
      if (OB_FAIL(create_thread(thread, [this, i] {
            try {
              this->run(i);
            } catch (common::OB_BASE_EXCEPTION& except) {
              UNUSED(except);
            }
          }))) {
        break;
      } else {
        threads_[i] = thread;
      }
    }
    if (OB_FAIL(ret)) {
      CoKThreadTemp<Thread>::stop();
      CoKThreadTemp<Thread>::wait();
      CoKThreadTemp<Thread>::destroy();
    } else {
      stop_ = false;
    }
  }
  return ret;
}

template <class Thread>
int CoKThreadTemp<Thread>::create_thread(Thread*& thread, std::function<void()> entry)
{
  int ret = common::OB_SUCCESS;
  thread = nullptr;

  const auto buf = ::malloc(sizeof(Thread));
  if (buf != nullptr) {
    thread = new (buf) Thread(entry, stack_size_);
    thread->set_max_tasks(thread_max_tasks_);
  } else {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  }
  if (OB_SUCC(ret) && OB_FAIL(thread->start())) {
    thread->~Thread();
    ::free(thread);
    thread = nullptr;
  }
  return ret;
}

template <class Thread>
void CoKThreadTemp<Thread>::destroy_thread(Thread* thread)
{
  thread->stop();
  thread->wait();
  thread->destroy();
  thread->~Thread();
}

template <class Thread>
void CoKThreadTemp<Thread>::wait()
{
  if (threads_ != nullptr) {
    for (int i = 0; i < n_threads_; i++) {
      if (threads_[i] != nullptr) {
        threads_[i]->wait();
      }
    }
  }
}

template <class Thread>
void CoKThreadTemp<Thread>::stop()
{
  common::SpinRLockGuard g(lock_);
  stop_ = true;
  if (OB_NOT_NULL(threads_)) {
    for (int i = 0; i < n_threads_; i++) {
      if (threads_[i] != nullptr) {
        threads_[i]->stop();
      }
    }
  }
}

template <class Thread>
void CoKThreadTemp<Thread>::destroy()
{
  if (threads_ != nullptr) {
    for (int i = 0; i < n_threads_; i++) {
      if (threads_[i] != nullptr) {
        threads_[i]->destroy();
        threads_[i]->~Thread();
        ::free(threads_[i]);
        threads_[i] = nullptr;
      }
    }
    free(threads_);
    threads_ = nullptr;
  }
}

template <class Sched>
void CoKThreadTemp<Sched>::set_thread_max_tasks(uint64_t cnt)
{
  thread_max_tasks_ = cnt;
}

// template <class Sched>
// int CoKThreadTemp<Sched>::submit_to(uint64_t idx, RunFuncT &func)
// {
//   int ret = common::OB_SUCCESS;
//   common::SpinRLockGuard g(lock_);
//   if (idx >= n_threads_) {
//     ret = common::OB_SIZE_OVERFLOW;
//   } else {
//     ret = threads_[idx]->submit(func);
//   }
//   return ret;
// }

// template <class Sched>
// template <class Functor>
// int CoKThreadTemp<Sched>::submit(Functor &&func)
// {
//   common::SpinRLockGuard g(lock_);
//   const uint64_t idx = idx_++ % n_threads_;
//   return threads_[idx]->submit(std::forward(func));
// }

template <class Sched>
template <class Functor>
int CoKThreadTemp<Sched>::submit(const Functor& func)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == threads_)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  } else {
    common::SpinRLockGuard g(lock_);
    const uint64_t idx = ATOMIC_FAA(&idx_, 1);
    for (uint64_t i = 0; i < n_threads_; i++) {
      const uint64_t new_idx = (i + idx) % n_threads_;
      if (OB_SUCC(threads_[new_idx]->submit(func))) {
        break;
      }
    }
  }
  return ret;
}

template <class Sched>
int64_t CoKThreadTemp<Sched>::get_cur_tasks() const
{
  int64_t total = 0;
  for (uint64_t i = 0; i < n_threads_; i++) {
    total += threads_[i]->get_cur_tasks();
  }
  return total;
}

}  // namespace lib
}  // namespace oceanbase

#include "lib/coro/co_set_sched.h"

namespace oceanbase {
namespace lib {
using CoUserThread = CoUserThreadTemp<CoSetSched>;
using CoXThread = CoKThreadTemp<CoUserThread>;
using CoKThread = CoXThread;
}  // namespace lib
}  // namespace oceanbase

#endif /* CO_USER_THREAD_H */
