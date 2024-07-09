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

#include "lib/task/ob_timer.h"
#include "lib/task/ob_timer_monitor.h"
#include "lib/time/ob_time_utility.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/string/ob_string.h"
#include "lib/stat/ob_diagnose_info.h"
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace common
{
using namespace obutil;
using namespace lib;


int ObTimer::init(const char* thread_name, const ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    tokens_ = reinterpret_cast<Token*>(ob_malloc(sizeof(Token) * max_task_num_, attr));
    if (nullptr == tokens_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(ERROR, "failed to alloc memory", K(ret));
    } else {
      for (int i = 0; i < max_task_num_; i++) {
        new (&tokens_[i]) Token();
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
        is_destroyed_ = false;
        is_stopped_ = true;
        has_running_repeat_task_ = false;
        thread_name_ = thread_name;
        ret = create();
      }
    }
  }
  return ret;
}

ObTimer::~ObTimer()
{
  destroy();
}

int ObTimer::create()
{
  int ret = OB_SUCCESS;
  ObMonitor<Mutex>::Lock guard(monitor_);
  if (is_stopped_) {
    if (OB_FAIL(ThreadPool::start())) {
      OB_LOG(ERROR, "failed to start timer thread", K(ret));
    } else {
      is_stopped_ = false;
      monitor_.notify_all();
      OB_LOG(INFO, "ObTimer create success", KP(this), K_(thread_id), KCSTRING(lbt()));
    }
  }
  return ret;
}

int ObTimer::start()
{
  int ret = OB_SUCCESS;
  ObMonitor<Mutex>::Lock guard(monitor_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (is_stopped_) {
    is_stopped_ = false;
    monitor_.notify_all();
    OB_LOG(INFO, "ObTimer start success", KP(this), K_(thread_id), KCSTRING(lbt()));
  }
  return ret;
}

void ObTimer::stop()
{
  {
    ObMonitor<Mutex>::Lock guard(monitor_);
    if (!is_stopped_) {
      is_stopped_ = true;
      monitor_.notify_all();
      OB_LOG(INFO, "ObTimer stop success", KP(this), K_(thread_id));
    }
  }
  cancel_all();
}

void ObTimer::wait()
{
  ObMonitor<Mutex>::Lock guard(monitor_);
  while (running_task_ != NULL) {
    static const int64_t WAIT_INTERVAL_US = 2000000; // 2s
    (void)monitor_.timed_wait(ObSysTime(WAIT_INTERVAL_US));
  }
}

void ObTimer::destroy()
{
  if (!is_destroyed_ && is_inited_) {
    {
      ObMonitor<Mutex>::Lock guard(monitor_);
      is_stopped_ = true;
      is_destroyed_ = true;
      is_inited_ = false;
      has_running_repeat_task_=false;
      monitor_.notify_all();
    }
    ThreadPool::wait();
    {
      ObMonitor<Mutex>::Lock guard(monitor_);
      for (int64_t i = 0; i < tasks_num_; ++i) {
        ATOMIC_STORE(&(tokens_[i].task->timer_), nullptr);
      }
      tasks_num_ = 0;
    }
    if (nullptr != tokens_) {
      for (int i = 0; i < max_task_num_; i++) {
        tokens_[i].~Token();
      }
      ob_free(tokens_);
      tokens_ = NULL;
    }
    OB_LOG(INFO, "ObTimer destroy", KP(this), K_(thread_id));
  }
  ThreadPool::destroy();
}

bool ObTimer::task_exist(const ObTimerTask &task)
{
  bool ret = false;
  ObMonitor<Mutex>::Lock guard(monitor_);
  for (int pos = 0; pos < tasks_num_; ++pos) {
    if (tokens_[pos].task == &task) {
      ret = true;
      break;
    }
  }
  return ret;
}

int ObTimer::schedule(ObTimerTask &task, const int64_t delay, const bool repeate /*=false*/, const bool immediate /*=false*/)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ret = schedule_task(task, delay, repeate, immediate);
  }
  return ret;
}

int ObTimer::schedule_repeate_task_immediately(ObTimerTask &task, const int64_t delay)
{
  int ret = OB_SUCCESS;
  const bool schedule_immediately = true;
  const bool repeate = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ret = schedule_task(task, delay, repeate, schedule_immediately);
  }
  return ret;
}

int ObTimer::schedule_task(ObTimerTask &task, const int64_t delay, const bool repeate,
    const bool is_scheduled_immediately)
{
  int ret = OB_SUCCESS;
  ObMonitor<Mutex>::Lock guard(monitor_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (delay < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(delay));
  } else if (tasks_num_ >= max_task_num_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "too much timer task", K(ret), K_(tasks_num), "max_task_num", max_task_num_);
  } else {
    int64_t time = ObSysTime::now(ObSysTime::Monotonic).toMicroSeconds();
    if(!is_scheduled_immediately) {
      time += delay;
    }
    if (is_stopped_) {
      ret = OB_CANCELED;
      OB_LOG(WARN, "schedule task on stopped timer", K(ret), K(task));
    } else {
      ret = insert_token(Token(time, repeate ? delay : 0, &task));
      if (OB_SUCC(ret)) {
        if (0 == wakeup_time_ || wakeup_time_ >= time) {
          monitor_.notify();
        }
      } else {
        OB_LOG(WARN, "insert token failed", K(ret), K(task));
      }
    }
  }
  return ret;
}

int ObTimer::insert_token(const Token &token)
{
  int ret = OB_SUCCESS;
  int64_t max_task_num= max_task_num_;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    if (has_running_repeat_task_) {
      max_task_num = max_task_num_ - 1;
    }
    if (tasks_num_ >= max_task_num) {
      OB_LOG(WARN, "tasks_num_ exceed max_task_num", K_(tasks_num), "max_task_num", max_task_num_);
      ret = OB_ERR_UNEXPECTED;
    } else {
      int64_t pos = 0;
      for (pos = 0; pos < tasks_num_; ++pos) {
        if (token.scheduled_time <= tokens_[pos].scheduled_time) {
          break;
        }
      }
      for (int64_t i = tasks_num_; i > pos; --i) {
        tokens_[i] = tokens_[i - 1];
      }
      tokens_[pos] = token;
      ++tasks_num_;
      ATOMIC_STORE(&(token.task->timer_), this);
    }
  }
  return ret;
}

int ObTimer::cancel_task(const ObTimerTask &task)
{
  int ret = OB_SUCCESS;
  ObMonitor<Mutex>::Lock guard(monitor_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (&task == uncanceled_task_) {
      // repeat cancel, do-nothing
  } else {
    if (&task == running_task_) {
      if (uncanceled_task_ != NULL) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        // the token corresponding to running_task_ has been removed in run1(),
        // so no need remove here
        uncanceled_task_ = &const_cast<ObTimerTask&>(task);
        OB_LOG(INFO, "cancel task", KP(this), K_(thread_id), K(wakeup_time_), K(tasks_num_), K(task));
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_STORE(&const_cast<ObTimerTask&>(task).timer_, nullptr);
      // for any tokens_[i].task == &task, we need remove tokens_[i]
      int64_t i = 0;
      while(i < tasks_num_) {
        if (tokens_[i].task == &task) {
          if (i + 1 < tasks_num_) {
            memmove(&tokens_[i], &tokens_[i + 1], sizeof(tokens_[0]) * (tasks_num_ - i - 1));
          }
          --tasks_num_;
          OB_LOG(INFO, "cancel task", KP(this), K_(thread_id), K(wakeup_time_), K(tasks_num_), K(task));
        } else {
          ++i;
        }
      }
    }
  }
  return ret;
}

int ObTimer::wait_task(const ObTimerTask &task)
{
  int ret = OB_SUCCESS;
  ObMonitor<Mutex>::Lock guard(monitor_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    do {
      bool exist = &task == running_task_;
      if (!exist) exist = &task == uncanceled_task_;
      for (int64_t i = 0; !exist && i < tasks_num_; ++i) {
        if (&task == tokens_[i].task) {
          exist = true;
          break;
        }
      }
      if (!exist) {
        break;
      } else {
        (void)monitor_.timed_wait(ObSysTime(10 * 1000/*10ms*/));
      }
    } while (true);
  }
  return ret;
}

int ObTimer::cancel(const ObTimerTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cancel_task(task))) {
    OB_LOG(WARN, "failed to cancel_task", K(ret), K(task));
  // @TODO: nijia.nj
  //} else if (OB_FAIL(wait_task(task))) {
  //  OB_LOG(WARN, "failed to wait_task", K(ret), K(task));
  }
  return ret;
}

void ObTimer::cancel_all()
{
  int ret = OB_SUCCESS;
  do {
    ObMonitor<Mutex>::Lock guard(monitor_);
    for (int64_t i = 0; i < tasks_num_; ++i) {
      ATOMIC_STORE(&(tokens_[i].task->timer_), nullptr);
    }
    tasks_num_ = 0;
    if (running_task_ != NULL) {
      if (uncanceled_task_ == running_task_) {
        // do-nothing
      } else if (uncanceled_task_ != NULL) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ATOMIC_STORE(&running_task_->timer_, nullptr);
        uncanceled_task_ = running_task_;
      }
      if (OB_SUCC(ret)) {
        (void)monitor_.timed_wait(ObSysTime(10 * 1000/*10ms*/));
      }
    } else {
      if (uncanceled_task_ != NULL) {
        ret = OB_ERR_UNEXPECTED;
      }
      break;
    }
  } while (OB_SUCC(ret));
  OB_LOG(INFO, "cancel all", K(ret), KP(this), K_(thread_id), K(wakeup_time_), K(tasks_num_));
}

void ObTimer::run1()
{
  int tmp_ret = OB_SUCCESS;
  Token token(0, 0, NULL);
  thread_id_ = syscall(__NR_gettid);
  OB_LOG(INFO, "timer thread started", KP(this), "tid", syscall(__NR_gettid), KCSTRING(lbt()));
  if (OB_NOT_NULL(thread_name_)) {
    set_thread_name(thread_name_);
  } else {
    set_thread_name("ObTimer");
  }
  while (true) {
    IGNORE_RETURN lib::Thread::update_loop_ts();
    {
      ObMonitor<Mutex>::Lock guard(monitor_);
      static const int64_t STATISTICS_INTERVAL_US = 600L * 1000 * 1000; // 10m
      if (REACH_TIME_INTERVAL(STATISTICS_INTERVAL_US)) {
        OB_LOG(INFO, "dump timer info", KP(this), K_(tasks_num), K_(wakeup_time));
        for (int64_t i = 0; i < tasks_num_; i++) {
          OB_LOG(INFO, "dump timer task info", K(tokens_[i]));
        }
      }

      if (is_destroyed_) {
        break;
      }

      //add repeated task to tasks_ again
      if (token.delay != 0 && !is_stopped_) {
        has_running_repeat_task_ = false;
        token.scheduled_time = ObSysTime::now(ObSysTime::Monotonic).toMicroSeconds() + token.delay;
        if (OB_SUCCESS != (tmp_ret = insert_token(
            Token(token.scheduled_time, token.delay, token.task)))) {
          OB_LOG_RET(WARN, tmp_ret, "insert token error", K(tmp_ret), K(token));
        }
      }
      running_task_ = NULL;
      if (is_stopped_) {
        monitor_.notify_all();
      }
      while (!is_destroyed_ && is_stopped_) {
        monitor_.wait();
      }
      if (is_destroyed_) {
        break;
      }
      if (0 == tasks_num_) {
        wakeup_time_ = 0;
        monitor_.wait();
      }
      if (is_stopped_) {
        continue;
      }
      if (is_destroyed_) {
        break;
      }
      while (tasks_num_ > 0 && !is_destroyed_ && !is_stopped_) {
        const int64_t now = ObSysTime::now(ObSysTime::Monotonic).toMicroSeconds();
        if (tokens_[0].scheduled_time <= now) {
          token = tokens_[0];
          running_task_ = token.task;
          memmove(tokens_, tokens_ + 1, (tasks_num_ - 1) * sizeof(tokens_[0]));
          --tasks_num_;
          ATOMIC_STORE(&(token.task->timer_), nullptr);
          if (token.delay != 0) {
            has_running_repeat_task_ = true;
          }
          break;
        }
        if (is_stopped_) {
          continue;
        } else if (is_destroyed_) {
          break;
        } else {
          wakeup_time_ = tokens_[0].scheduled_time;
          {
            // clock safty check. @see
            const int64_t rt1 = ObTimeUtility::current_time();
            const int64_t rt2 = ObTimeUtility::current_time_coarse();
            const int64_t delta = rt1 > rt2 ? (rt1 - rt2) : (rt2 - rt1);
            static const int64_t MAX_REALTIME_DELTA1 = 20000; // 20ms
            static const int64_t MAX_REALTIME_DELTA2 = 500000; // 500ms
            if (delta > MAX_REALTIME_DELTA1) {
              OB_LOG_RET(WARN, OB_ERR_SYS, "Hardware clock skew", K(rt1), K(rt2), K_(wakeup_time), K(now));
            } else if (delta > MAX_REALTIME_DELTA2) {
              OB_LOG_RET(ERROR, OB_ERR_SYS, "Hardware clock error", K(rt1), K(rt2), K_(wakeup_time), K(now));
            }
          }
          monitor_.timed_wait(ObSysTime(wakeup_time_ - now));
        }
      }
    }

    if (NULL == running_task_)
    {
      // If running_task_ is NULL, the token is not associated with any task,
      // so we reset it to avoid the task being scheduled unexpectedly.
      token.reset();
    }

    if (token.task != NULL && running_task_ != NULL && !is_destroyed_ && !is_stopped_) {
      bool timeout_check = token.task->timeout_check();
      const int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
      if (timeout_check) {
        ObTimerMonitor::get_instance().start_task(thread_id_, start_time, token.delay, token.task);
      }
      token.task->runTimerTask();
      ObMonitor<Mutex>::Lock guard(monitor_);
      running_task_ = NULL;
      if (token.task == uncanceled_task_) {
        uncanceled_task_ = NULL;
        token.delay = 0;
        monitor_.notify_all();
      }

      const int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
      const int64_t elapsed_time = end_time - start_time;
      EVENT_ADD(SYS_TIME_MODEL_DB_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_DB_CPU, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_BKGD_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_BKGD_CPU, elapsed_time);
      if (timeout_check) {
        ObTimerMonitor::get_instance().end_task(thread_id_, end_time);
      }

      if (elapsed_time > ELAPSED_TIME_LOG_THREASHOLD) {
        OB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "timer task cost too much time", "task", to_cstring(*token.task),
            K(start_time), K(end_time), K(elapsed_time), KP(this), K_(thread_id));
      }
    }
  }
  OB_LOG(INFO, "timer thread exit", KP(this), K_(thread_id));
}

void ObTimer::dump() const
{
  for (int64_t i = 0; i < tasks_num_; ++i) {
    printf("%ld : %ld %ld %p\n", i, tokens_[i].scheduled_time, tokens_[i].delay, tokens_[i].task);
  }
}

bool ObTimer::inited() const
{
  return is_inited_;
}

} /* common */
} /* chunkserver */
