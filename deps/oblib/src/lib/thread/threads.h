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

#ifndef USER_THREAD_H
#define USER_THREAD_H

#include <functional>
#include "lib/ob_errno.h"
#include "lib/thread/thread.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/lock/ob_spin_rwlock.h"

extern int64_t global_thread_stack_size;
namespace oceanbase {
namespace lib {
class ObPThread;

class IRunWrapper;
class Threads
{
public:
  friend class ObPThread;
  explicit Threads(int64_t n_threads = 1)
      : n_threads_(n_threads),
        init_threads_(n_threads),
        threads_(nullptr),
        stack_size_(global_thread_stack_size),
        stop_(true),
        run_wrapper_(nullptr)
  {}
  virtual ~Threads();
  static IRunWrapper *&get_expect_run_wrapper();

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
  int do_set_thread_count(int64_t n_threads);
  int set_thread_count(int64_t n_threads);
  int inc_thread_count(int64_t inc = 1);
  int thread_recycle();

  int init();
  // IRunWrapper 用于创建多租户线程时指定租户上下文
  // cgroup_ctrl 和IRunWrapper配合使用，实现多租户线程的CPU隔离
  void set_run_wrapper(IRunWrapper *run_wrapper)
  {
    run_wrapper_ = run_wrapper;
  }
  IRunWrapper * get_run_wrapper()
  {
    return run_wrapper_;
  }
  virtual int start();
  virtual void stop();
  virtual void wait();
  void destroy();
  virtual void run(int64_t idx);

public:
  template <class Functor>
  int submit(const Functor &func)
  {
    UNUSED(func);
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual bool has_set_stop() const
  {
    IGNORE_RETURN lib::Thread::update_loop_ts();
    return ATOMIC_LOAD(&stop_);
  }
  bool &has_set_stop()
  {
    IGNORE_RETURN lib::Thread::update_loop_ts();
    return stop_;
  }
  pthread_t get_pthread(int64_t idx)
  {
    pthread_t pth = 0;
    if (idx < n_threads_) {
      pth = threads_[idx]->get_pthread();
    }
    return pth;
  }
  int64_t get_thread_count() const { return n_threads_; }
protected:
  uint64_t get_thread_idx() const { return thread_idx_; }
  void set_thread_idx(int64_t idx) { thread_idx_ = idx; }

private:
  virtual void run1() {}

  int do_thread_recycle();
  /// \brief Create thread
  int create_thread(Thread *&thread, int64_t idx);

  /// \brief Destroy thread.
  void destroy_thread(Thread *thread);

private:
  static thread_local uint64_t thread_idx_;
  int64_t n_threads_;
  int64_t init_threads_;
  Thread **threads_;
  int64_t stack_size_;
  bool stop_;
  // protect for thread count changing.
  common::SpinRWLock lock_ __attribute__((__aligned__(16)));
  // tenant ctx
  IRunWrapper *run_wrapper_;
};

class ObPThread : public Threads
{
public:
  ObPThread(void *(*start_routine) (void *), void *arg)
    : start_routine_(start_routine), arg_(arg)
  {}
  void run1() override
  {
    start_routine_(arg_);
  }
  int try_wait();
private:
  void *(*start_routine_)(void *);
  void *arg_;
};

using ThreadPool = Threads;

}  // lib
}  // oceanbase


#endif /* USER_THREAD_H */
