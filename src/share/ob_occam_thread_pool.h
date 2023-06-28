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

/**
 * ObOccamThreadPool follows the Occam's razor principle and value semantics.
 * It only requires the minimum necessary information, and then things will be done.
 *
 * Occam’s razor, also spelled Ockham’s razor, also called law of economy or law of parsimony,
 * principle stated by the Scholastic philosopher William of Ockham (1285–1347/49) that
 * “plurality should not be posited without necessity.”
 * The principle gives precedence to simplicity: of two competing theories,
 * the simpler explanation of an entity is to be preferred.
 * The principle is also expressed as “Entities are not to be multiplied beyond necessity.”
 **/

#ifndef OCEANBASE_LIB_THREAD_OB_EASY_THREAD_POOL_H
#define OCEANBASE_LIB_THREAD_OB_EASY_THREAD_POOL_H

#include <assert.h>
#include "lib/function/ob_function.h"
#include "lib/future/ob_future.h"
#include "lib/guard/ob_shared_guard.h"
#include "lib/container/ob_se_array.h"
#include "lib/thread/threads.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/thread/ob_thread_name.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace common
{

namespace occam
{

struct DefaultAllocator : public ObIAllocator {
  void *alloc(const int64_t size) {
#ifdef UNITTEST_DEBUG
    total_alive_num++;
#endif
    return ob_malloc(size, SET_USE_UNEXPECTED_500("OccamThreadPool"));
  }
  void* alloc(const int64_t size, const ObMemAttr &attr) override {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override {
#ifdef UNITTEST_DEBUG
    total_alive_num--;
#endif
    ob_free(ptr);
  }
#ifdef UNITTEST_DEBUG
  int total_alive_num = 0;
#endif
  static DefaultAllocator &get_default_allocator() {
    static DefaultAllocator default_allocator;
    return default_allocator;
  }
};

#define DEFAULT_ALLOCATOR occam::DefaultAllocator::get_default_allocator()

class ObOccamThread : public share::ObThreadPool
{
  static uint64_t get_id_count() {
    static uint64_t id_count = 0;
    return ATOMIC_AAF(&id_count, 1);
  }
public:
  ObOccamThread() : id_(get_id_count()), is_inited_(false), is_stopped_(false) {}
  ~ObOccamThread() {
    destroy();
  }
  template <typename T>
  int init_and_start(T &&func, bool need_set_tenant_ctx = true) {
    if (need_set_tenant_ctx) {
      share::ObThreadPool::set_run_wrapper(MTL_CTX());
    }
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      OCCAM_LOG(WARN, "init twice", K(this), K_(id), K(ret));
    } else if (OB_FAIL(func_.assign(std::forward<T>(func)))) {
      OCCAM_LOG(WARN, "assign function failed", K(this), K_(id), K(ret));
    } else if (OB_FAIL(share::ObThreadPool::init())) {
      OCCAM_LOG(WARN, "ObThreadPool::init failed", K(this), K_(id), K(ret));
    } else if (OB_FAIL(share::ObThreadPool::start())) {
      OCCAM_LOG(WARN, "start function failed", K(this), K_(id), K(ret));
    } else {
      OCCAM_LOG(INFO, "init thread success", K(this), K_(id), K(ret));
      is_inited_ = true;
    }
    return ret;
  }
  void stop() {
    OCCAM_LOG(INFO, "occam thread marked stopped", K(this), K_(id));
    ATOMIC_SET(&is_stopped_, true);
    share::ObThreadPool::stop();
  }
  void destroy() {
    if (is_inited_) {
      stop();
      share::ObThreadPool::wait();
      share::ObThreadPool::destroy();
      func_.reset();
      is_inited_ = false;
    }
  }
  bool is_stopped() const { return ATOMIC_LOAD(&is_stopped_); }
  void run1() {
    lib::set_thread_name("Occam");
    if (func_.is_valid()) {
      OCCAM_LOG(INFO, "thread is running function");
      func_();
    }
  }
  uint64_t get_id() const { return id_; }
private:
  ObFunction<void()> func_;// valid after init
  uint64_t id_;
  bool is_inited_;
  bool is_stopped_;
};

enum class TASK_PRIORITY
{
  EXTREMELY_HIGH = 0,
  HIGH,
  NORMAL,
  LOW,
  EXTREMELY_LOW,
  LEVEL_COUNT,
};

// tuple unpacked
template<int ...> struct seq {};
template<int N,int ...S> struct gens : gens < N - 1,N - 1,S... > {};
template<int ...S> struct gens <0,S... > { typedef seq<S...> type; };
// gens<3>::type = gens<2,2,3>::type = gens<1,1,2,3>::type = gens<0,0,1,2,3>::type
// while gens<0, S...>::type is specialized, equl to seq<S...>::type
// so gens<3>::type = seq<1,2,3>::type

// function call with tuple wrapper
template<typename R,
         typename F,
         typename...Args,
         int ...S,
         typename std::enable_if<
              !std::is_void<R>::value, bool
           >::type = true>
inline void CallWithTupleUnpack(seq<S...>,
                         std::tuple<Args...> &tpl,
                         F &func,
                         ObPromise<R> &promise)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(promise.set(func(std::get<S>(tpl) ...)))) {
    OCCAM_LOG(ERROR, "set promise failed", K(ret));
  }
}

// function call with tuple wrapper
template<typename R,
         typename F,
         typename...Args,
         int ...S,
         typename std::enable_if<
              std::is_void<R>::value, bool
           >::type = true>
inline void CallWithTupleUnpack(seq<S...>,
                         std::tuple<Args...> &tpl,
                         F &func,
                         ObPromise<R> &promise)
{
  int ret = OB_SUCCESS;
  func(std::get<S>(tpl) ...);
  if (OB_FAIL(promise.set())) {
    OCCAM_LOG(ERROR, "set promise failed", K(ret));
  }
}

class ObOccamThreadPool
{
public:
  ObOccamThreadPool() :
    thread_num_(0),
    queue_size_(0),
    total_task_count_(0),
    is_inited_(false),
    is_stopped_(false) {}
  ~ObOccamThreadPool() { destroy(); }
  int init(int64_t thread_num, int64_t queue_size_square_of_2 = 10)
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
    } else {
      int step = 0;
      int queue_init_idx = 0;
      int thread_init_idx = 0;
      auto check_queue_size_is_pow_of_2_and_in_allowed_range = [](const int64_t queue_size) {
        int64_t mask = 1;
        int count_valid_bit = 0;
        for (int64_t idx = 0; idx < 63; ++idx) {
          if (queue_size & (mask << idx)) {
            ++count_valid_bit;
          }
        }
        return count_valid_bit == 1 && queue_size <= 65536;
      };
      if (!check_queue_size_is_pow_of_2_and_in_allowed_range(1 << queue_size_square_of_2)) {
        ret = OB_INVALID_ARGUMENT;
        OCCAM_LOG(ERROR, "queue size not satisfied", K(ret));
      } else if (++step && OB_FAIL(cv_.init(ObWaitEventIds::DYNAMIC_THREAD_POOL_COND_WAIT))) {
        OCCAM_LOG(WARN, "init cv failed", K(ret));
      } else if (++step && OB_ISNULL(threads_ = (occam::ObOccamThread *)DEFAULT_ALLOCATOR.alloc(sizeof(occam::ObOccamThread) * thread_num))) {
        // step 2 failed
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ++step;// step 3
        for (; queue_init_idx < static_cast<int>(occam::TASK_PRIORITY::LEVEL_COUNT) && OB_SUCC(ret); ++queue_init_idx) {
          ret = queues_[queue_init_idx].init(1 << queue_size_square_of_2);
        }
        if (OB_SUCC(ret)) {
          ++step;// step 4
          for (; thread_init_idx < thread_num && OB_SUCC(ret); ++thread_init_idx) {
            new(&threads_[thread_init_idx]) occam::ObOccamThread();
            uint64_t thread_id = threads_[thread_init_idx].get_id();
            ret = threads_[thread_init_idx].init_and_start([this, thread_id]() { this->keep_fetching_task_until_stop_(thread_id); });
          }
          if (OB_SUCC(ret)) {
            step = 0; // step done
            is_inited_ = true;
            OCCAM_LOG(INFO, "init occam thread pool success", K(ret), K(thread_num), K(queue_size_square_of_2), K(lbt()));
          }
        }
        thread_num_ = thread_num;
        queue_size_ = 1 << queue_size_square_of_2;
      }
      if (OB_FAIL(ret)) {
        switch (step) {// error handle
        case 4:
          for (; thread_init_idx > 0; --thread_init_idx) {
            threads_[thread_init_idx - 1].destroy();
          }
        case 3:
          for (; queue_init_idx > 0; --queue_init_idx) {
            queues_[queue_init_idx - 1].destroy();
          }
        case 2:
          DEFAULT_ALLOCATOR.free(threads_);
          threads_ = nullptr;
        case 1:
        default:
          break;
        }
      }
    }
    return ret;
  }
  void stop()
  {
    if (is_inited_) {
      int ret = OB_SUCCESS;
      {
        ObThreadCondGuard guard(cv_);
        is_stopped_ = true;
      }
      if (OB_FAIL(cv_.broadcast())) {
        OCCAM_LOG(ERROR, "cv broadcast failed", K(ret));
      }
      for (int64_t idx = 0; idx < thread_num_; ++idx) {
        threads_[idx].stop();
      }
    }
  }
  void wait()
  {
    if (is_inited_) {
      for (int64_t idx = 0; idx < thread_num_; ++idx) {
        threads_[idx].wait();
      }
    }
  }
  void destroy()
  {
    stop();
    wait();
    if (is_inited_) {
      for (int64_t idx = 0; idx < thread_num_; ++idx) {
        threads_[idx].destroy();
      }
      DEFAULT_ALLOCATOR.free(threads_);
      for (int idx = 0; idx < static_cast<int>(occam::TASK_PRIORITY::LEVEL_COUNT); ++idx) {
        queues_[idx].destroy();
      }
      is_inited_ = false;
    }
  }


  template<TASK_PRIORITY PRIORITY = TASK_PRIORITY::NORMAL,
           class F,
           class... Args>
  int commit_task(ObFuture<typename std::result_of<F(Args...)>::type> &future,
                  F &&f,
                  Args &&...args)
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else {
      using result_type = typename std::result_of<F(Args...)>::type;
      OCCAM_LOG(DEBUG, "commit task");
      ObPromise<result_type> promise;
      if (OB_FAIL(promise.init())) {
        OCCAM_LOG(WARN, "init promise failed", K(ret));
      } else {
        ObFunction<void()> function;
        std::tuple<Args...> args_tuple = std::tuple<Args...>(std::forward<Args>(args)...);
        if (OB_FAIL(function.assign([f, args_tuple, promise]() mutable {
          occam::CallWithTupleUnpack<result_type>(typename occam::gens<sizeof...(Args)>::type(),
                                                              args_tuple,
                                                              f,
                                                              promise);
        }))) {
          OCCAM_LOG(WARN, "assign function failed", K(ret));
        } else if (OB_FAIL(queues_[static_cast<int>(PRIORITY)].push_task(function))) {
          OCCAM_LOG(WARN, "push task failed", K(ret));
        } else {
          future = promise.get_future();
          {
            ObThreadCondGuard guard(cv_);
            total_task_count_++;
          }
          if (OB_FAIL(cv_.signal())) {
            OCCAM_LOG(ERROR, "cv signal failed", K(ret));
          }
        }
      }
    }
    return ret;
  }
private:
  void keep_fetching_task_until_stop_(const uint64_t thread_id)
  {
    int ret = OB_SUCCESS;
    ObFunction<void()> function;
    while (true) { // keep fetching task and do the task until thread pool is stopped
      IGNORE_RETURN lib::Thread::update_loop_ts(ObTimeUtility::fast_current_time());
      bool is_stopped = false;
      {
        ObThreadCondGuard guard(cv_);
        is_stopped = is_stopped_;
      }
      if (is_stopped) { // no lock and memory barrier here, so need sequential consistency
        break;
      } else {
        // try fetch a task
        for (int64_t idx = 0; idx < static_cast<int64_t>(occam::TASK_PRIORITY::LEVEL_COUNT); ++idx) { // fetch task from high priorty to low
          if (OB_FAIL(queues_[idx].pop_task(function, thread_id))) {
            if (ret != OB_EAGAIN) { // unexcepted error
              OCCAM_LOG(ERROR, "unexpected inner error", K(ret));
            } else {} // OB_EAGAIN means this queue is empty, so try fetch next queue
          } else { // successfully fetch a task
            {
              ObThreadCondGuard guard(cv_);
              total_task_count_--;
            }
            OCCAM_LOG(DEBUG, "successfully fetch a task", K(thread_id));
            break; // break the fetch loop to execute the task
          }
        }
        // process with the task
        if (OB_SUCC(ret)) { // do the task
          if (OB_UNLIKELY(!function.is_valid())) {
            OCCAM_LOG(ERROR, "fetch a invalid task", K(thread_id), K(function));
          } else {
            function();
            OCCAM_LOG(DEBUG, "execute task done", K(thread_id), K(function));
          }
          function.reset();
        } else if (ret == OB_EAGAIN) { // all queue empty, waiting for someone commit task
          // OCCAM_LOG(DEBUG, "no task in queue, waiting...", K(thread_id));
          ObThreadCondGuard guard(cv_);
          while (total_task_count_ == 0 && !is_stopped_) {
            if (OB_FAIL(cv_.wait())) {
              OCCAM_LOG(ERROR, "cv_ wait return err code", K(ret));
            }
          }
        } else { // unknown error
          OCCAM_LOG(ERROR, "some error not expected", K(ret));
        }
      }
    }
  }
private:
  struct InnerTaskQueue
  {
    InnerTaskQueue() :
      queue_size_(0),
      mask_value_(0),
      buffer_(nullptr),
      lock_(common::ObLatchIds::THREAD_POOL_LOCK),
      head_(0),
      tail_(0) {}
    ~InnerTaskQueue() { destroy(); }
    int init(const int64_t queue_size)
    {
      int ret = OB_SUCCESS;
      queue_size_ = queue_size;
      mask_value_ = queue_size - 1;
      if (OB_ISNULL(buffer_ = (ObFunction<void()> *)DEFAULT_ALLOCATOR.alloc(sizeof(ObFunction<void()>) * queue_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OCCAM_LOG(WARN, "alloc buffer memory failed", K(lbt()));
      } else {
        for (int64_t idx = 0; idx < queue_size_; ++idx) {
          new(&buffer_[idx]) ObFunction<void()>();
        }
      }
      return ret;
    }
    void destroy()
    {
      if (!OB_ISNULL(buffer_)) {
        ObSpinLockGuard lk(lock_);
        assert(tail_ >= head_);
        for (int64_t idx = head_; idx < tail_; ++idx) {
          buffer_[idx & mask_value_].reset();
        }
        DEFAULT_ALLOCATOR.free(buffer_);
        buffer_ = nullptr;
        tail_ = 0;
        head_ = 0;
      }
    }
    int pop_task(ObFunction<void()> &function, const int64_t thread_id)
    {
      int ret = OB_SUCCESS;
      ObSpinLockGuard lk(lock_);
      // OCCAM_LOG(DEBUG, "try fetch task", K_(head), K(tail_), K(thread_id));
      assert(tail_ >= head_);
      if (head_ == tail_) {// queue is empty
        ret = OB_EAGAIN;
        // OCCAM_LOG(WARN, "no task in queue", K_(head), K(tail_), K(thread_id));
      } else if (OB_SUCC(function.assign(std::move((buffer_[head_ & mask_value_]))))) {
        ++head_;
        OCCAM_LOG(DEBUG, "fetch task success", KP(this), K_(head), K(tail_), K(thread_id), K(function), K(queue_size_));
      } else {
        OCCAM_LOG(WARN, "fetch task failed", K_(head), K(tail_), K(thread_id));
      }
      return ret;
    }
    int push_task(const ObFunction<void()> &function)
    {
      int ret = OB_SUCCESS;
      ObSpinLockGuard lk(lock_);
      assert(tail_ >= head_);
      if (tail_ - head_ == queue_size_) {// queue is full
        ret = OB_BUF_NOT_ENOUGH;
        OCCAM_LOG(WARN, "queue is full", K(ret), K_(head), K_(tail), K(lbt()));
      } else if (!function.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        OCCAM_LOG(WARN, "invalid argument", K(ret), K_(head), K_(tail), K(lbt()), K(function));
      } else if (OB_SUCC(buffer_[tail_ & mask_value_].assign(function))) {
        ++tail_;
        OCCAM_LOG(DEBUG, "push task success", KP(this), K_(head), K_(tail), K(function), K(queue_size_));
      } else {
        OCCAM_LOG(WARN, "push task failed", K(ret), K_(head), K_(tail), K(lbt()));
      }
      return ret;
    }
  private:
    int64_t queue_size_;
    int64_t mask_value_;
    ObFunction<void()> *buffer_;
    ObSpinLock lock_;
    int64_t head_;
    int64_t tail_;
  };
  int64_t thread_num_;
  int64_t queue_size_;
  occam::ObOccamThread *threads_;
  InnerTaskQueue queues_[static_cast<int>(occam::TASK_PRIORITY::LEVEL_COUNT)];
  int64_t total_task_count_;
  ObThreadCond cv_;
  bool is_inited_;
  bool is_stopped_;
};
}// namespace occam_thread_pool

class ObOccamThreadPool
{
public:
  ObOccamThreadPool() :
    thread_num_(0),
    queue_size_square_of_2_(0) {}
  int init_and_start(int thread_num, int queue_size_square_of_2 = 10)
  {
    int ret = OB_SUCCESS;
    ret = ob_make_shared<occam::ObOccamThreadPool>(thread_pool_);
    if (OB_FAIL(ret)) {
      OCCAM_LOG(WARN, "make shared failed");
    } else if (OB_FAIL(thread_pool_->init(thread_num, queue_size_square_of_2))) {
      thread_pool_.reset();
      OCCAM_LOG(WARN, "thread_pool_ init failed",
                  K(thread_pool_), K(thread_num_), K(queue_size_square_of_2_));
    } else {
      OCCAM_LOG(INFO, "thread_pool_ init success",
                  K(thread_pool_), K(thread_num_), K(queue_size_square_of_2_));
    }
    return ret;
  }
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::NORMAL,
           class F,
           class... Args>
  int commit_task(ObFuture<typename std::result_of<F(Args...)>::type> &future,
                  F &&f,
                  Args &&...args)
  {
    int ret = OB_SUCCESS;
    if (OB_LIKELY(!thread_pool_.is_valid())) {
      ret = OB_NOT_INIT;
      OCCAM_LOG(WARN, "thread_pool_ not valid", K(thread_pool_), K(lbt()));
    } else if (OB_FAIL(thread_pool_->commit_task<PRIORITY>(future,
                                                           std::forward<F>(f),
                                                           std::forward<Args>(args)...))) {
      OCCAM_LOG(WARN, "commit task failed");
    }
    return ret;
  }
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::NORMAL,
           class F,
           class... Args>
  int commit_task_ignore_ret(F &&f, Args &&...args)
  {
    int ret = OB_SUCCESS;
    using result_type = typename std::result_of<F(Args...)>::type;
    ObFuture<result_type> future;
    if (OB_LIKELY(!thread_pool_.is_valid())) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(thread_pool_->commit_task<PRIORITY>(future,
                                                           std::forward<F>(f),
                                                           std::forward<Args>(args)...))) {
      OCCAM_LOG(WARN, "commit task failed");
    }
    return ret;
  }

public:
  ObSharedGuard<occam::ObOccamThreadPool> thread_pool_;
private:
  int thread_num_;
  int queue_size_square_of_2_;
};

#undef DEFAULT_ALLOCATOR

}// namespace common
}// namespace oceanbase

#endif
