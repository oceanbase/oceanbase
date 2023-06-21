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
 * ObOccamTimer follows the Occam's razor principle and value semantics.
 * It only requires the minimum necessary information, and then things will be done.
 * 
 * Occam’s razor, also spelled Ockham’s razor, also called law of economy or law of parsimony, 
 * principle stated by the Scholastic philosopher William of Ockham (1285–1347/49) that 
 * “plurality should not be posited without necessity.”
 * The principle gives precedence to simplicity: of two competing theories,
 * the simpler explanation of an entity is to be preferred.
 * The principle is also expressed as “Entities are not to be multiplied beyond necessity.”
 **/

#ifndef OCEANBASE_LIB_TASK_OB_EASY_TIMER_H
#define OCEANBASE_LIB_TASK_OB_EASY_TIMER_H

#include <type_traits>
#include <typeinfo>
#include <unistd.h>
#include <utility>
#include "lib/guard/ob_weak_guard.h"
#include "lib/guard/ob_shared_guard.h"
#include "lib/guard/ob_unique_guard.h"
#include "lib/guard/ob_scope_guard.h"
#include "lib/ob_errno.h"
#include "share/ob_occam_thread_pool.h"
#include "lib/task/ob_timer.h"
#include "lib/future/ob_future.h"
#include "storage/tx/ob_time_wheel.h"
#include "common/ob_clock_generator.h"
#include "share/ob_occam_time_guard.h"
#include "share/ob_delegate.h"

namespace oceanbase
{
namespace common
{

class ObOccamTimer;
class ObOccamTimerTaskRAIIHandle;

namespace occam
{

#define DEFAULT_ALLOCATOR occam::DefaultAllocator::get_default_allocator()

class ObOccamTimerTask : public ObTimeWheelTask
{
  friend class ObOccamTimerTaskRAIIHandle;
  friend class ObOccamTimer;
  struct TaskWrapper
  {
    TaskWrapper(ObSharedGuard<ObFunction<bool()>> function,
                ObOccamTimerTask *p_task,
                const bool need_delete,
                const int64_t schedule_time)
    : function_(function),// construct function will success always
    p_task_(p_task),
    need_delete_(need_delete),
    schedule_time_(schedule_time) {}
    bool operator()() {
      TIMEGUARD_INIT(OCCAM, 1_s, 30_s);
      bool stop_flag = true;
      {
        ObSharedGuard<ObFunction<bool()>> func_shared_ptr = function_.upgrade();
        // thread pool will execute the task only if task's valid ref count != 0,
        if (OB_LIKELY(func_shared_ptr.is_valid())) {
          int64_t delay_time = ObClockGenerator::getRealClock() - schedule_time_;
          if (OB_UNLIKELY(delay_time > 50_ms)) {
            OCCAM_LOG_RET(WARN, OB_ERR_UNEXPECTED, "task executed too delay", K(delay_time), K(*p_task_));
          }
          stop_flag = (*func_shared_ptr)();// run the task
          if (__time_guard__.is_timeout()) {
            OCCAM_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "run this task use too much time", K(*p_task_));
          }
        } else {
          OCCAM_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "func_shared_ptr_ upgrade failed, task has been stopped", K(function_));
        }
      }
      if (need_delete_) {// can use task_ pointer here, cause it;s promised it won't be free in other place
        CLICK();
        OCCAM_LOG(DEBUG, "delete task in thread pool", K(*p_task_), K(function_));
        {
          ObSpinLockGuard lg(p_task_->lock_);// make sure runTimerTask() finished, or just hang here(expected very quickly)
        }
        CLICK();
        p_task_->~ObOccamTimerTask();
        CLICK();
        DEFAULT_ALLOCATOR.free(p_task_);
      }
      return stop_flag;
    }
    ObWeakGuard<ObFunction<bool()>> function_;// the enclosed package to run
    ObOccamTimerTask *p_task_;// 1. to add lock(sync with schedule thread and execute thread) 2. free this ptr if one-shot task
    const bool need_delete_;// one-shot task need free it's memory when executed done
    const int64_t schedule_time_;// for detect if execute time far away from schedule time 
  };
public:
  ObOccamTimerTask(occam::TASK_PRIORITY task_priority,
                   bool *timer_running_flag,
                   int64_t *total_running_count,
                   occam::ObOccamThreadPool *thread_pool,
                   ObTimeWheel *time_wheel,
                   const int64_t time_interval_us,
                   const int64_t timer_precision,
                   const bool with_handle_protected,
                   const char *function_class,
                   const char *function_name,
                   const char *file,
                   const int64_t line) :
    time_wheel_(time_wheel),
    thread_pool_(thread_pool),
    timer_running_flag_(timer_running_flag),
    total_running_count_(total_running_count),
    task_priority_(task_priority),
    is_running_(false),
    with_handle_protected_(with_handle_protected),
    time_interval_us_(time_interval_us),
    timer_precision_(timer_precision),
    function_class_(function_class),
    function_name_(function_name),
    file_(file),
    line_(line),
    lock_(ObLatchIds::TIME_WHEEL_TASK_LOCK)
  {}
  ~ObOccamTimerTask() {
    time_wheel_ = nullptr;
    thread_pool_ = nullptr;
    ATOMIC_DEC(total_running_count_);
  };
  int init()
  {
    int ret = ob_alloc_shared<ObFunction<bool()>>(func_shared_ptr_, DEFAULT_ALLOCATOR);
    if (OB_SUCC(ret)) {
      new(func_shared_ptr_.get_ptr()) ObFunction<bool()>();
    }
    return ret;
  }
  void set_scheduled() { ATOMIC_STORE(&is_scheduled_, true); }
public:
  void runTimerTask() override {
    TIMEGUARD_INIT(OCCAM, 100_ms);
    bool try_delete_this_task_if_there_is_no_handle = false;
    bool need_free_task = false;
    int ret = OB_SUCCESS;
    {
      ObSpinLockGuard lg(lock_);
      int64_t scheduled_delay = ObClockGenerator::getRealClock() - expected_run_ts_us_;
      if (OB_UNLIKELY(scheduled_delay > timer_precision_ + 100_ms)) {
        if (OB_UNLIKELY(future_.is_valid())) {// first time schedule
          OCCAM_LOG(WARN, "task scheduled too delay", K(scheduled_delay),
                      K(expected_run_ts_us_), K(ObClockGenerator::getRealClock()), KR(ret), K(*this));
        }
      }
      if (OB_UNLIKELY(ATOMIC_LOAD(timer_running_flag_) == false)) {// timer is stopped, not allow to coimmit task and schedule again
        if (future_.is_valid()) {
          if (CLICK_FAIL(future_.wait())) {// waiting for thread pool run the task done
            OCCAM_LOG(ERROR, "future wait failed, some error occured and could not handle", KR(ret), K(*this));
          }
        }
        try_delete_this_task_if_there_is_no_handle = true;
      } else if (OB_UNLIKELY(thread_pool_ == nullptr || time_wheel_ == nullptr)) {// means this task is stopped by handle
        OCCAM_LOG(INFO, "thread_pool_ or time_wheel_ is null", KR(ret), K(*this));
      } else {
        bool allow_to_commit = false;
        bool allow_to_register_next_task = false;
        if (!future_.is_valid()) {// first call runTimerTask(), future_ is not inited yet
          allow_to_commit = true;
          allow_to_register_next_task = true;
        } else if (OB_UNLIKELY(!future_.is_ready())) {// will not commit new task to thread pool, but will schedule timer task again
          allow_to_commit = false;
          allow_to_register_next_task = true;
          OCCAM_LOG(WARN, "last commit task not scheduled yet, give up committing new task this time", KR(ret), K(*this));
        } else {// not first time to run task, and last committed task in thread pool has run done
          bool *stop_task = nullptr;
          if (CLICK_FAIL(future_.get(stop_task))) {// some unknown error occured, task scheduled must stop
            allow_to_commit = false;
            allow_to_register_next_task = false;
            OCCAM_LOG(ERROR, "fail to get value from future", KR(ret), K(*this));
          } else if (OB_UNLIKELY(*stop_task)) {// task marked stop, won't schedule again
            allow_to_commit = false;
            allow_to_register_next_task = false;
            OCCAM_LOG(INFO, "task marked stop, won't schedule again", KR(ret), K(*this));
          } else {// this is normal path, keep scheduling
            allow_to_commit = true;
            allow_to_register_next_task = true;
          }
        }
        if (OB_LIKELY(allow_to_commit)) {
          if (OB_UNLIKELY(!func_shared_ptr_.is_valid())) {
            OCCAM_LOG(ERROR, "func_shared_ptr_ not valid", KR(ret), K(*this));
          } else if (CLICK_FAIL(commit_task_to_thread_pool_())) {
            OCCAM_LOG(WARN, "fail to commit task to thread pool", KR(ret), K(*this));
          }
        }
        CLICK();
        if (OB_LIKELY(allow_to_register_next_task && time_interval_us_ != 0)) {
          int temp_ret = OB_SUCCESS;
          CLICK();
          int64_t next_expected_run_ts_us = 0;
          do {
            next_expected_run_ts_us = expected_run_ts_us_ + (((ObClockGenerator::getRealClock() - expected_run_ts_us_) / time_interval_us_) + 1) * time_interval_us_;
            if (OB_UNLIKELY(OB_SUCCESS !=
              (temp_ret =
              time_wheel_->schedule(this, next_expected_run_ts_us - ObClockGenerator::getRealClock())))) {
              OCCAM_LOG(WARN, "timer schedule task failed", K(temp_ret), KR(ret), K(*this));
            } else {
              OCCAM_LOG(DEBUG, "schedule task", KR(ret), K(*this));
            }
          } while (OB_INVALID_ARGUMENT == temp_ret);// means time delay arg is negative
          CLICK();
          if (OB_UNLIKELY(OB_SUCCESS != ret)) {// register next task failed
            OCCAM_LOG(ERROR, "fail to register next timer task", K(temp_ret), KR(ret), K(*this));
          } else {
            expected_run_ts_us_ = next_expected_run_ts_us;
          }
        } else if (!allow_to_register_next_task && time_interval_us_ != 0) {// a repeat task is stopped
          try_delete_this_task_if_there_is_no_handle = true;
        }
      }
  #ifdef UNITTEST_DEBUG
      ob_usleep(10_ms);
  #endif
      // try delete task if:
      // 1. timer is stopped, and this task is not in thread pool
      // 2. a repeat task is marked stopped, and this task is not in thread pool
      if (try_delete_this_task_if_there_is_no_handle) {
        if (with_handle_protected_) {// mark handle just need destroy the task, don't need stop it
          OCCAM_LOG(INFO, "this task will not scheduled again, will destroy in handle", K(ret), K(*this));
          time_wheel_ = nullptr;
          thread_pool_ = nullptr;
        } else {// means there is no handle
          need_free_task = true;
        }
      }
      ATOMIC_SET(&is_running_, false);
    }// lock guard destroyed here
    if (OB_UNLIKELY(need_free_task)) {
      OCCAM_LOG(INFO, "destroy task in runTimerTask()", K(ret), K(*this));
      this->~ObOccamTimerTask();
      DEFAULT_ALLOCATOR.free(this);// I'm sure this is the last line about this task, so it's safe here
    }
  }
  int commit_task_to_thread_pool_() {
    TIMEGUARD_INIT(OCCAM, 100_ms);
    int ret = common::OB_SUCCESS;
    if (!func_shared_ptr_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      OCCAM_LOG(ERROR, "function shared ptr is invalid", K(ret), K(*this));
    } else {
      bool need_delete = !with_handle_protected_ && time_interval_us_ == 0;// no handle protected and is one-shot scheduled task
      ObOccamTimerTask *task = this;
      int64_t schedule_time = ObClockGenerator::getRealClock();
      TaskWrapper commit_task(func_shared_ptr_, task, need_delete, schedule_time);
#ifdef UNITTEST_DEBUG
      OCCAM_LOG(DEBUG, "print size of", K(sizeof(commit_task)));
#endif
      switch (task_priority_) {
        case occam::TASK_PRIORITY::EXTREMELY_HIGH:
          ret = thread_pool_->commit_task<occam::TASK_PRIORITY::EXTREMELY_HIGH>(future_, commit_task);
          break;
        case occam::TASK_PRIORITY::HIGH:
          ret = thread_pool_->commit_task<occam::TASK_PRIORITY::HIGH>(future_, commit_task);
          break;
        case occam::TASK_PRIORITY::NORMAL:
          ret = thread_pool_->commit_task<occam::TASK_PRIORITY::NORMAL>(future_, commit_task);
          break;
        case occam::TASK_PRIORITY::LOW:
          ret = thread_pool_->commit_task<occam::TASK_PRIORITY::LOW>(future_, commit_task);
          break;
        case occam::TASK_PRIORITY::EXTREMELY_LOW:
          ret = thread_pool_->commit_task<occam::TASK_PRIORITY::EXTREMELY_LOW>(future_, commit_task);
          break;
        default:
          OCCAM_LOG(ERROR, "unrecognized task priority", K(ret), K(*this));
          break;
      }
      if (CLICK_FAIL(ret)) {// the task queue may be full
        OCCAM_LOG(WARN, "commit task to thread pool failed", K(ret), K(*this));
      }
    }
    return ret;
  }
  virtual uint64_t hash() const { return 0; }
  virtual void begin_run() { ATOMIC_SET(&is_running_, true); }
  bool is_running() const { return ATOMIC_LOAD(&is_running_); }
  void set_expected_run_ts(const int64_t time_point_us) { ATOMIC_STORE(&expected_run_ts_us_, time_point_us); }
  int64_t get_expected_run_ts() const { return ATOMIC_LOAD(&expected_run_ts_us_); }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    auto find_last_path = [](const char *path) -> const char * {
      const char *ret = path;
      const char *iter = path;
      if (OB_NOT_NULL(path)) {
        while (*iter != '\0') {
          if (*iter == '/') {
            ret = (*(iter + 1) == '\0') ? iter : (iter + 1);
          }
          ++iter;
        }
      }
      return ret;
    };
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos, "{this:0x%lx, ", (unsigned long)this);
    common::databuff_printf(buf, buf_len, pos, "caller:%s:%s:%ld, ", find_last_path(file_), function_name_, line_);
    common::databuff_printf(buf, buf_len, pos, "function_type:%s, ", function_class_);
    common::databuff_printf(buf, buf_len, pos, "timer_running_flag:%d, ", ATOMIC_LOAD(timer_running_flag_));
    common::databuff_printf(buf, buf_len, pos, "total_running_count:%ld, ", ATOMIC_LOAD(total_running_count_));
    common::databuff_printf(buf, buf_len, pos, "func_shared_ptr_.ptr:0x%lx, ", (unsigned long)func_shared_ptr_.get_ptr());
    common::databuff_printf(buf, buf_len, pos, "time_wheel:0x%lx, ", (unsigned long)time_wheel_);
    common::databuff_printf(buf, buf_len, pos, "thread_pool:0x%lx, ", (unsigned long)thread_pool_);
    common::databuff_printf(buf, buf_len, pos, "is_running:%d, ", is_running_);
    if (time_interval_us_ < 1_s) {
      common::databuff_printf(buf, buf_len, pos, "time_interval:%.2lfms, ", time_interval_us_ * 1.0 / 1_ms);
    } else {
      common::databuff_printf(buf, buf_len, pos, "time_interval:%.2lfs, ", time_interval_us_ * 1.0 / 1_s);
    }
    common::databuff_printf(buf, buf_len, pos, "expected_run_time:%s, ", common::ObTime2Str::ob_timestamp_str_range<HOUR, MSECOND>(expected_run_ts_us_));
    common::databuff_printf(buf, buf_len, pos, "task_priority:%d, ", (int)task_priority_);
    common::databuff_printf(buf, buf_len, pos, "with_handle_protected:%d}", (int)with_handle_protected_);
    return pos;
  }
public:
  ObFuture<bool> future_;// 用于判断上一次提交的异步任务是否已经执行结束
  ObTimeWheel *time_wheel_;
  occam::ObOccamThreadPool *thread_pool_;// 用于提交异步任务，被设置为nullptr时，定时任务将停止调度，将由task_handle清理本定时任务
  bool *timer_running_flag_;// 用于判断timer是否被整体停止
  int64_t *total_running_count_;// timer判断是否还有任务没有析构
  ObSharedGuard<ObFunction<bool()>> func_shared_ptr_;// 用于保存执行的定时任务执行体
  occam::TASK_PRIORITY task_priority_;// 该定时任务提交到异步线程池的执行等级的优先级
  bool is_running_;// 用于从time wheel安全摘掉task指针不得不添加的状态
  bool with_handle_protected_;// 表明该对象是否有句柄保护，没有的话需要在time wheel里销毁
  const int64_t time_interval_us_;// 下一次被timewheel调度的间隔
  int64_t expected_run_ts_us_;// 下一次被timewheel调度的时间点
  const int64_t timer_precision_;// timer的精度，用于确定定时任务调度和执行的延迟范围，超过精度+阈值时打印报警日志
  /***********for debug************/
  const char *function_class_;
  const char *function_name_;
  const char *file_;
  const int64_t line_;
  /********************************/
  ObSpinLock lock_;
};

}// namespace occam_timer

class ObOccamTimerTaskRAIIHandle
{
  friend class ObOccamTimer;
public:
  ObOccamTimerTaskRAIIHandle() : task_(nullptr), is_inited_(false), is_running_(false), lock_(ObLatchIds::TIME_WHEEL_TASK_LOCK) { OCCAM_LOG(INFO, "task handle constructed", K(*this)); }
  ObOccamTimerTaskRAIIHandle(ObOccamTimerTaskRAIIHandle&&) = delete;
  ObOccamTimerTaskRAIIHandle(const ObOccamTimerTaskRAIIHandle&) = delete;
  ObOccamTimerTaskRAIIHandle& operator=(const ObOccamTimerTaskRAIIHandle&) = delete;
  TO_STRING_KV(KP(this), KPC_(task), K_(is_inited), K_(is_running));
  ~ObOccamTimerTaskRAIIHandle() {
    if (OB_NOT_NULL(task_)) {
      (void)stop_and_wait();
    }
    is_inited_ = false;
    OCCAM_LOG(INFO, "task handle destructed", K(*this));
  }
  bool is_running() const {
    bool ret = false;
    ObSpinLockGuard lg(lock_);
    if (!is_inited_) {
      ret = false;
    } else {
      ret = is_running_;
    }
    return ret;
  }
  int reschedule_after(int64_t us)
  {
    int ret = OB_SUCCESS;
    TIMEGUARD_INIT(OCCAM, 10_ms);
    ObSpinLockGuard lg(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      OCCAM_LOG(ERROR, "not init", KR(ret), K(*this));
    } else if (!is_running_) {
      ret = OB_NOT_RUNNING;
      OCCAM_LOG(ERROR, "not running", KR(ret), K(*this));
    } else if (us <= 0) {
      ret = OB_INVALID_ARGUMENT;
      OCCAM_LOG(ERROR, "invalid argument", KR(ret), K(*this));
    } else if (OB_ISNULL(task_)) {
      ret = OB_BAD_NULL_ERROR;
      OCCAM_LOG(WARN, "not allow to reschedule task cause task is null", KR(ret), K(*this));
    } else if (CLICK_FAIL(remove_task_from_timewheel_())) {
      OCCAM_LOG(WARN, "fail to remove task from timewheel", KR(ret), K(*this));
    } else {
      ObSpinLockGuard lg(task_->lock_);
      if (OB_ISNULL(task_->time_wheel_)) {
        ret = OB_OP_NOT_ALLOW;
        OCCAM_LOG(WARN, "not allow to reschedule task cause timewheel is null", KR(ret), K(*this));
      } else if (task_->time_interval_us_ == 0 && task_->future_.is_valid()) {
        ret = OB_OP_NOT_ALLOW;
        OCCAM_LOG(WARN, "this is one shot task, and has been scheduled", KR(ret), K(*this));
      } else if (CLICK_FAIL(task_->time_wheel_->schedule(task_, us))) {
        OCCAM_LOG(ERROR, "fail to reschedule task", KR(ret), K(*this));
      } else {
        task_->expected_run_ts_us_ = ObClockGenerator::getRealClock() + us;
      }
    }
    return ret;
  }
  void stop() {
    int ret = OB_SUCCESS;
    TIMEGUARD_INIT(OCCAM, 100_ms);
    ObSpinLockGuard lg(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      OCCAM_LOG(ERROR, "not init", KR(ret), K(*this));
    } else if (!is_running_) {
      ret = OB_NOT_RUNNING;
      OCCAM_LOG(WARN, "not running", KR(ret), K(*this));
    } else if (OB_ISNULL(task_)) {
      ret = OB_BAD_NULL_ERROR;
      OCCAM_LOG(ERROR, "task is null", KR(ret), K(*this));
    } else {
      ret = remove_task_from_timewheel_();// promise won't schedule the task again
      if (CLICK_FAIL(ret) && ret != OB_OP_NOT_ALLOW) {
        OCCAM_LOG(ERROR, "fail to remove task from timewheel", K(*this));
      } else {
        OCCAM_LOG(INFO, "stop task", K(*this));
        ObSpinLockGuard lg(task_->lock_);
        task_->func_shared_ptr_.~ObSharedGuard();
        is_running_ = false;
      }
    }
  }
  void wait() {
    int ret = OB_SUCCESS;
    ObSpinLockGuard lg(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      OCCAM_LOG(WARN, "not init", KR(ret), K(*this));
    } else if (is_running_) {
      ret = OB_ERR_UNEXPECTED;
      OCCAM_LOG(ERROR, "still running, maybe not call stop()", KR(ret), K(*this));
    } else if (OB_ISNULL(task_)) {
      ret = OB_BAD_NULL_ERROR;
      OCCAM_LOG(WARN, "task is null, maybe has been called wait()?", KR(ret), K(*this));
    } else if (!function_weak_guard_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      OCCAM_LOG(ERROR, "function_weak_guard_ is invalid, maybe task never running or not call stop()", KR(ret), K(*this));
    } else if (OB_UNLIKELY(function_weak_guard_.upgradeable())) {// wait executing task finished
      int64_t start_waiting_time = ObClockGenerator::getRealClock();
      // meaning this function is executing on thread pool, waiting it done
      while (function_weak_guard_.upgradeable()) { ob_usleep(5); }
      int64_t end_waiting_time = ObClockGenerator::getRealClock();
      OCCAM_LOG(INFO, "waiting for task executing done, cost time:",
                   K(end_waiting_time - start_waiting_time), K(*this));
    }
    if (OB_SUCC(ret)) {
      OCCAM_LOG(INFO, "free task", K(*this));
      task_->~ObOccamTimerTask();
      DEFAULT_ALLOCATOR.free(task_);
      task_ = nullptr;
    }
  }
  void stop_and_wait() {
    int ret = OB_SUCCESS;
    {
      ObSpinLockGuard lg(lock_);
      if (!is_inited_) {
        ret = OB_NOT_INIT;
        OCCAM_LOG(WARN, "not init", KR(ret), K(*this));
      } else if (!is_running_) {
        ret = OB_NOT_RUNNING;
        OCCAM_LOG(WARN, "still running, maybe not call stop()", KR(ret), K(*this));
      }
    }
    if (OB_SUCC(ret)) {
      stop();
      wait();
    }
  }
private:
  int remove_task_from_timewheel_()
  {
    int ret = OB_SUCCESS;
    while (true) {
      ObSpinLockGuard lg(task_->lock_);
      function_weak_guard_ = task_->func_shared_ptr_;
      if (OB_ISNULL(task_->time_wheel_)) {
        ret = OB_OP_NOT_ALLOW;
        OCCAM_LOG(INFO, "task stopped inside itself", K(*this));
        break;// no need cancle task, cause the task stop itself already, and time wheel may not exist anymore
      } else {
        task_->set_scheduled();
        ret = task_->time_wheel_->cancel(task_);
        if (OB_TIMER_TASK_HAS_NOT_SCHEDULED == ret) {
          OCCAM_LOG(INFO, "task return not scheduled, need try again", K(*this));
        } else if (OB_SUCCESS != ret) {
          OCCAM_LOG(ERROR, "cancel error", K(*this));
        } else if (!task_->is_running()) {
          OCCAM_LOG(DEBUG, "cancel task success", K(*this));
          break;// successfully cancle task, others maybe failed
        }
      }
    }
    return ret;
  }
private:
  occam::ObOccamTimerTask* task_;
  ObWeakGuard<ObFunction<bool()>> function_weak_guard_;// for wait
  bool is_inited_;
  bool is_running_;
  mutable ObSpinLock lock_;// this lock prevent stop_or_wait interface be called concurrently
};

class ObOccamTimer
{
public:
  ObOccamTimer() : total_running_task_count_(0), precision_(0), is_running_(false) {}
  ~ObOccamTimer() { destroy(); }
  int init_and_start(ObOccamThreadPool &pool, const int64_t precision, const char *name)
  {
    TIMEGUARD_INIT(OCCAM, 100_ms);
    int ret = OB_SUCCESS;
    ret = ob_make_shared<ObTimeWheel>(timer_shared_ptr_);
    if (CLICK_FAIL(ret)) {
      OCCAM_LOG(WARN, "init time wheel failed", K(ret));
    } else if (CLICK_FAIL(timer_shared_ptr_->init(precision, 1, name))) {
      OCCAM_LOG(WARN, "init timer_shared_ptr_ failed", K(ret));
    } else if (CLICK_FAIL(thread_pool_shared_ptr_.assign(pool.thread_pool_))) {
      OCCAM_LOG(WARN, "assign timer_shared_ptr_ failed", K(ret));
    } else if (CLICK_FAIL(timer_shared_ptr_->start())) {
      OCCAM_LOG(WARN, "timer_shared_ptr_ start failed", K(ret));
    } else {
      precision_ = precision;
      OCCAM_LOG(INFO, "init ObOccamTimer success", K(ret));
      ATOMIC_STORE(&is_running_, true);
    }
    return OB_SUCCESS;
  }
  int init_and_start(const int64_t worker_number,
                     const int64_t precision,
                     const char *name,
                     const int64_t queue_size_square_of_2 = 10)
  {
    TIMEGUARD_INIT(OCCAM, 100_ms);
    int ret = OB_SUCCESS;
    if (CLICK_FAIL(ob_make_shared<occam::ObOccamThreadPool>(thread_pool_shared_ptr_))) {
      OCCAM_LOG(WARN, "create thread pool failed", K(ret));
    } else if (CLICK_FAIL(ob_make_shared<ObTimeWheel>(timer_shared_ptr_))) {
      OCCAM_LOG(WARN, "create time wheel failed", K(ret));
    } else if (CLICK_FAIL(thread_pool_shared_ptr_->init(worker_number, queue_size_square_of_2))) {
      OCCAM_LOG(WARN, "init thread_pool_shared_ptr_ failed", K(ret));
    } else if (CLICK_FAIL(timer_shared_ptr_->init(precision, 1, name))) {
      OCCAM_LOG(WARN, "init timer_shared_ptr_ failed", K(ret));
    } else if (CLICK_FAIL(timer_shared_ptr_->start())) {
      OCCAM_LOG(WARN, "timer_shared_ptr_ start failed", K(ret));
    } else {
      precision_ = precision;
      OCCAM_LOG(INFO, "init ObOccamTimer success", K(ret));
      ATOMIC_STORE(&is_running_, true);
    }
    return OB_SUCCESS;
  }
  void stop()
  {
    ATOMIC_STORE(&is_running_, false);
    int64_t last_print_time = 0;
    while (ATOMIC_LOAD(&total_running_task_count_) != 0) {
      int64_t current_time = ObClockGenerator::getCurrentTime();
      if (current_time - last_print_time > 500_ms) {// print log every 500ms
        last_print_time = current_time;
        OCCAM_LOG(INFO, "OccamTimr waiting running task finished",
                    K(ATOMIC_LOAD(&total_running_task_count_)), KP(this));
      }
    }
    if (timer_shared_ptr_.is_valid()) {
      timer_shared_ptr_->stop();
    }
    if (thread_pool_shared_ptr_.is_valid()) {
      thread_pool_shared_ptr_->stop();
    }
  }
  void wait()
  {
    if (timer_shared_ptr_.is_valid()) {
      timer_shared_ptr_->wait();
    }
    if (thread_pool_shared_ptr_.is_valid()) {
      thread_pool_shared_ptr_->wait();
    }
  }
  void destroy()
  {
    stop();
    wait();
    if (thread_pool_shared_ptr_.is_valid()) {
      thread_pool_shared_ptr_->destroy();
    }
    if (timer_shared_ptr_.is_valid()) {
      timer_shared_ptr_->destroy();
    }
  }
  bool is_running() const { return ATOMIC_LOAD(&is_running_); };
  // Returned value is ignored
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::HIGH,
           class F>
  int schedule_task_ignore_handle_repeat(const int64_t schedule_interval_us,
                                         F &&func,
                                         /************for debug*************/
                                         const char *function_name = __builtin_FUNCTION(),
                                         const char *file = __builtin_FILE(),
                                         const int64_t line = __builtin_LINE()
                                         /*********************************/) {
    char stack_buffer[sizeof(ObOccamTimerTaskRAIIHandle)];
    new(stack_buffer) ObOccamTimerTaskRAIIHandle();// will not call it's destruction
    ObOccamTimerTaskRAIIHandle *p_handle = reinterpret_cast<ObOccamTimerTaskRAIIHandle *>(stack_buffer);
    return schedule_task_<PRIORITY>(*p_handle,
                                    false,
                                    0,
                                    schedule_interval_us,
                                    true,
                                    false,
                                    function_name,
                                    file,
                                    line,
                                    std::forward<F>(func));
  }
  // Returned value is ignored
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::HIGH,
           class F>
  int schedule_task_repeat(ObOccamTimerTaskRAIIHandle &task_desc,
                           const int64_t schedule_interval_us,
                           F &&func,
                           /************for debug*************/
                           const char *function_name = __builtin_FUNCTION(),
                           const char *file = __builtin_FILE(),
                           const int64_t line = __builtin_LINE()
                           /*********************************/) {
    return schedule_task_<PRIORITY>(task_desc,
                                    true,
                                    0,
                                    schedule_interval_us,
                                    true,
                                    false,
                                    function_name,
                                    file,
                                    line,
                                    std::forward<F>(func));
  }
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::HIGH,
           class F>
  int schedule_task_ignore_handle_repeat_spcifiy_first_delay(const int64_t first_delay_us,
                                                             const int64_t schedule_interval_us,
                                                             F &&func,
                                                             /************for debug*************/
                                                             const char *function_name = __builtin_FUNCTION(),
                                                             const char *file = __builtin_FILE(),
                                                             const int64_t line = __builtin_LINE()
                                                             /*********************************/) {
    char stack_buffer[sizeof(ObOccamTimerTaskRAIIHandle)];
    new(stack_buffer) ObOccamTimerTaskRAIIHandle();// will not call it's destruction
    ObOccamTimerTaskRAIIHandle *p_handle = reinterpret_cast<ObOccamTimerTaskRAIIHandle *>(stack_buffer);
    return schedule_task_<PRIORITY>(*p_handle,
                                    false,
                                    first_delay_us,
                                    schedule_interval_us,
                                    true,
                                    false,
                                    function_name,
                                    file,
                                    line,
                                    std::forward<F>(func));
  }
  // Returned value is ignored
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::HIGH,
           class F>
  int schedule_task_repeat_spcifiy_first_delay(ObOccamTimerTaskRAIIHandle &task_desc,
                                               const int64_t first_delay_us,
                                               const int64_t schedule_interval_us,
                                               F &&func,
                                               /************for debug*************/
                                               const char *function_name = __builtin_FUNCTION(),
                                               const char *file = __builtin_FILE(),
                                               const int64_t line = __builtin_LINE()
                                               /*********************************/) {
    return schedule_task_<PRIORITY>(task_desc,
                                    true,
                                    first_delay_us,
                                    schedule_interval_us,
                                    true,
                                    false,
                                    function_name,
                                    file,
                                    line,
                                    std::forward<F>(func));
  }
  // Returned value is ignored
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::HIGH,
           class F>
  int schedule_task_ignore_handle_repeat_and_immediately(const int64_t schedule_interval_us,
                                                         F &&func,
                                                         /************for debug*************/
                                                         const char *function_name = __builtin_FUNCTION(),
                                                         const char *file = __builtin_FILE(),
                                                         const int64_t line = __builtin_LINE()
                                                         /*********************************/) {
    char stack_buffer[sizeof(ObOccamTimerTaskRAIIHandle)];
    new(stack_buffer) ObOccamTimerTaskRAIIHandle();// will not call it's destruction
    ObOccamTimerTaskRAIIHandle *p_handle = reinterpret_cast<ObOccamTimerTaskRAIIHandle *>(stack_buffer);
    return schedule_task_<PRIORITY>(*p_handle,
                                    false,
                                    0,
                                    schedule_interval_us,
                                    true,
                                    true,
                                    function_name,
                                    file,
                                    line,
                                    std::forward<F>(func));
  }
  // Returned value is ignored
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::HIGH,
           class F>
  int schedule_task_repeat_and_immediately(ObOccamTimerTaskRAIIHandle &task_desc,
                                           const int64_t schedule_interval_us,
                                           F &&func,
                                           /************for debug*************/
                                           const char *function_name = __builtin_FUNCTION(),
                                           const char *file = __builtin_FILE(),
                                           const int64_t line = __builtin_LINE()
                                           /*********************************/) {
    return schedule_task_<PRIORITY>(task_desc,
                                    true,
                                    0,
                                    schedule_interval_us,
                                    true,
                                    true,
                                    function_name,
                                    file,
                                    line,
                                    std::forward<F>(func));
  }
  // Returned value is ignored
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::HIGH,
           class F,
           class... Args>
  int schedule_task_after(ObOccamTimerTaskRAIIHandle &task_desc,
                          const int64_t run_delay_us,
                          F &&func,
                          /************for debug*************/
                          const char *function_name = __builtin_FUNCTION(),
                          const char *file = __builtin_FILE(),
                          const int64_t line = __builtin_LINE()
                          /*********************************/) {
    return schedule_task_<PRIORITY>(task_desc,
                                    true,
                                    0,
                                    run_delay_us,
                                    false,
                                    false,
                                    function_name,
                                    file,
                                    line,
                                    std::forward<F>(func));
  }
  // Returned value is ignored, and will not return RAII handle
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::HIGH,
           class F>
  int schedule_task_ignore_handle_after(const int64_t run_delay_us,
                                        F &&func,
                                        /************for debug*************/
                                        const char *function_name = __builtin_FUNCTION(),
                                        const char *file = __builtin_FILE(),
                                        const int64_t line = __builtin_LINE()
                                        /*********************************/) {
    char stack_buffer[sizeof(ObOccamTimerTaskRAIIHandle)];
    new(stack_buffer) ObOccamTimerTaskRAIIHandle();// will not call it's destruction
    ObOccamTimerTaskRAIIHandle *p_handle = reinterpret_cast<ObOccamTimerTaskRAIIHandle *>(stack_buffer);
    return schedule_task_<PRIORITY>(*p_handle,
                                    false,
                                    0,
                                    run_delay_us,
                                    false,
                                    false,
                                    function_name,
                                    file,
                                    line,
                                    std::forward<F>(func));
  }
private:
  // Returned value is ignored
  template<occam::TASK_PRIORITY PRIORITY = occam::TASK_PRIORITY::HIGH,
           class F>
  int schedule_task_(ObOccamTimerTaskRAIIHandle &task_desc,
                     const bool with_handle_protected,
                     const int64_t first_time_delay_us,
                     const int64_t time_interval_us,
                     const bool repeatly,
                     const bool immediately,
                     const char *function_name,
                     const char *file,
                     const int64_t line,
                     F &&func) {
    static_assert(std::is_same<typename std::result_of<F()>::type, bool>::value, "the function must return bool type, false means keep scheduling this task, true means stop the task");
    TIMEGUARD_INIT(OCCAM, 100_ms);
    int ret = OB_SUCCESS;
    ATOMIC_INC(&total_running_task_count_);
    if (OB_UNLIKELY(ATOMIC_LOAD(&is_running_) == false)) {
      ret = OB_NOT_RUNNING;
      ATOMIC_DEC(&total_running_task_count_);
      OCCAM_LOG(WARN, "timer not running anymore", K(ret), KP(this));
    } else if (OB_LIKELY(task_desc.is_running())) {
      ret = OB_INVALID_ARGUMENT;
      OCCAM_LOG(WARN, "task_desc is running, can't use this handle", K(ret), KP(this));
    } else {
      occam::ObOccamTimerTask *task = nullptr;
      if (OB_ISNULL(task = (occam::ObOccamTimerTask *)DEFAULT_ALLOCATOR.alloc(sizeof(occam::ObOccamTimerTask)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        ATOMIC_DEC(&total_running_task_count_);
        OCCAM_LOG(ERROR, "alloc memory failed", K(ret));
      } else {
        int64_t repeat_time_delay_us = repeatly ? time_interval_us : 0;
        new(task) occam::ObOccamTimerTask(PRIORITY,
                                          &is_running_,
                                          &total_running_task_count_,// will dec in ~ObOccamTimerTask()
                                          thread_pool_shared_ptr_.get_ptr(),
                                          timer_shared_ptr_.get_ptr(),
                                          repeat_time_delay_us,
                                          precision_,
                                          with_handle_protected,
                                          typeid(F).name(),
                                          function_name,
                                          file,
                                          line);
        auto task_scop_guard = MAKE_SCOPE(task, [](occam::ObOccamTimerTask *task) {
          task->~ObOccamTimerTask();
          DEFAULT_ALLOCATOR.free(task);
        });
        if (CLICK_FAIL(task_scop_guard->init())) {
          OCCAM_LOG(ERROR, "init task failed", K(ret));
        } else if (CLICK_FAIL(task_scop_guard->func_shared_ptr_->assign(std::forward<F>(func)))) {
          OCCAM_LOG(ERROR, "assign function to shared ptr failed", K(ret)); 
        } else {
          if (immediately) {
            task_scop_guard->expected_run_ts_us_ = ObClockGenerator::getRealClock();
            task_scop_guard->runTimerTask();
            task_desc.task_ = task_scop_guard.fetch_resource();
            task_desc.is_inited_ = true;
            task_desc.is_running_ = true;
          } else {
            int64_t first_time_delay = (first_time_delay_us == 0 ? time_interval_us : first_time_delay_us);
            task_scop_guard->expected_run_ts_us_ = ObClockGenerator::getRealClock() + first_time_delay;
            if (CLICK_FAIL(timer_shared_ptr_->schedule(task_scop_guard.resource(), first_time_delay))) {
              OCCAM_LOG(ERROR, "schdule task failed", K(ret));
            } else {
              OCCAM_LOG(DEBUG, "create task and scheduled success", K(*task_scop_guard.resource()));
              task_desc.task_ = task_scop_guard.fetch_resource();
              task_desc.is_inited_ = true;
              task_desc.is_running_ = true;
            }
          }
        }
      }
    }
    return ret;
  }
private:
  int64_t total_running_task_count_;
  int64_t precision_;
  bool is_running_;
  ObSharedGuard<ObTimeWheel> timer_shared_ptr_;
  ObSharedGuard<occam::ObOccamThreadPool> thread_pool_shared_ptr_;
public:
  DELEGATE((*thread_pool_shared_ptr_), commit_task);
};

#undef DEFAULT_ALLOCATOR

}// namespace common
}// namespace oceanbase

#endif
