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

namespace oceanbase {
namespace common {
using namespace tbutil;
using namespace lib;

const int32_t ObTimer::MAX_TASK_NUM;

int ObTimer::init(const char* thread_name)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
    is_destroyed_ = false;
    is_stopped_ = true;
    has_running_repeat_task_ = false;
    thread_name_ = thread_name;
    ret = create();
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
  Monitor<Mutex>::Lock guard(monitor_);
  if (is_stopped_) {
    if (OB_FAIL(ThreadPool::start())) {
      OB_LOG(ERROR, "failed to start timer thread", K(ret));
    } else {
      is_stopped_ = false;
      monitor_.notifyAll();
      OB_LOG(INFO, "ObTimer create success", KP(this), K_(thread_id), K(lbt()));
    }
  }
  return ret;
}

int ObTimer::start()
{
  int ret = OB_SUCCESS;
  Monitor<Mutex>::Lock guard(monitor_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (is_stopped_) {
    is_stopped_ = false;
    monitor_.notifyAll();
    OB_LOG(INFO, "ObTimer start success", KP(this), K_(thread_id), K(lbt()));
  }
  return ret;
}

void ObTimer::stop()
{
  Monitor<Mutex>::Lock guard(monitor_);
  if (!is_stopped_) {
    is_stopped_ = true;
    monitor_.notifyAll();
    OB_LOG(INFO, "ObTimer stop success", KP(this), K_(thread_id));
  }
}

void ObTimer::wait()
{
  Monitor<Mutex>::Lock guard(monitor_);
  while (has_running_task_) {
    static const int64_t WAIT_INTERVAL_US = 2000000;  // 2s
    (void)monitor_.timedWait(Time(WAIT_INTERVAL_US));
  }
}

void ObTimer::destroy()
{
  if (!is_destroyed_ && is_inited_) {
    Monitor<Mutex>::Lock guard(monitor_);
    is_stopped_ = true;
    is_destroyed_ = true;
    is_inited_ = false;
    has_running_repeat_task_ = false;
    monitor_.notifyAll();
    tasks_num_ = 0;
    OB_LOG(INFO, "ObTimer destroy", KP(this), K_(thread_id));
  }
  ThreadPool::wait();
  ThreadPool::destroy();
}

bool ObTimer::task_exist(const ObTimerTask& task)
{
  bool ret = false;
  Monitor<Mutex>::Lock guard(monitor_);
  for (int pos = 0; pos < tasks_num_; ++pos) {
    if (tokens_[pos].task == &task) {
      ret = true;
      break;
    }
  }
  return ret;
}

int ObTimer::schedule(ObTimerTask& task, const int64_t delay, const bool repeate /*=false*/)
{
  int ret = OB_SUCCESS;
  const bool schedule_immediately = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ret = schedule_task(task, delay, repeate, schedule_immediately);
  }
  return ret;
}

int ObTimer::schedule_repeate_task_immediately(ObTimerTask& task, const int64_t delay)
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

int ObTimer::schedule_task(
    ObTimerTask& task, const int64_t delay, const bool repeate, const bool is_scheduled_immediately)
{
  int ret = OB_SUCCESS;
  Monitor<Mutex>::Lock guard(monitor_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (delay < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(delay));
  } else if (tasks_num_ >= MAX_TASK_NUM) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "too much timer task", K(ret), K_(tasks_num), "max_task_num", MAX_TASK_NUM);
  } else {
    int64_t time = Time::now(Time::Monotonic).toMicroSeconds();
    if (!is_scheduled_immediately) {
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

int ObTimer::insert_token(const Token& token)
{
  int ret = OB_SUCCESS;
  int32_t max_task_num = MAX_TASK_NUM;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    if (has_running_repeat_task_) {
      max_task_num = MAX_TASK_NUM - 1;
    }
    if (tasks_num_ >= max_task_num) {
      OB_LOG(WARN, "tasks_num_ exceed max_task_num", K_(tasks_num), "max_task_num", MAX_TASK_NUM);
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
    }
  }
  return ret;
}

int ObTimer::cancel(const ObTimerTask& task)
{
  int ret = OB_SUCCESS;
  Monitor<Mutex>::Lock guard(monitor_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    int64_t pos = -1;
    for (int64_t i = 0; i < tasks_num_; ++i) {
      if (&task == tokens_[i].task) {
        pos = i;
        break;
      }
    }
    if (pos != -1) {
      tokens_[pos].task->cancelCallBack();
      memmove(&tokens_[pos], &tokens_[pos + 1], sizeof(tokens_[0]) * (tasks_num_ - pos - 1));
      --tasks_num_;
      OB_LOG(INFO, "cancel task", KP(this), K_(thread_id), K(pos), K(wakeup_time_), K(tasks_num_), K(task));
    }
  }
  return ret;
}

void ObTimer::cancel_all()
{
  Monitor<Mutex>::Lock guard(monitor_);
  tasks_num_ = 0;
  OB_LOG(INFO, "cancel all", KP(this), K_(thread_id), K(wakeup_time_), K(tasks_num_));
}

void ObTimer::run1()
{
  int tmp_ret = OB_SUCCESS;
  Token token(0, 0, NULL);
  thread_id_ = syscall(__NR_gettid);
  OB_LOG(INFO, "timer thread started", KP(this), "tid", syscall(__NR_gettid), K(lbt()));
  if (OB_NOT_NULL(thread_name_)) {
    set_thread_name(thread_name_);
  } else {
    set_thread_name("ObTimer");
  }
  while (true) {
    {
      Monitor<Mutex>::Lock guard(monitor_);
      static const int64_t STATISTICS_INTERVAL_US = 600L * 1000 * 1000;  // 10m
      if (REACH_TIME_INTERVAL(STATISTICS_INTERVAL_US)) {
        OB_LOG(INFO, "dump timer info", KP(this), K_(tasks_num), K_(wakeup_time));
        for (int64_t i = 0; i < tasks_num_; i++) {
          OB_LOG(INFO, "dump timer task info", K(tokens_[i]));
        }
      }

      if (is_destroyed_) {
        break;
      }
      // add repeated task to tasks_ again
      if (token.delay != 0 && token.task->need_retry()) {
        has_running_repeat_task_ = false;
        token.scheduled_time = Time::now(Time::Monotonic).toMicroSeconds() + token.delay;
        if (OB_SUCCESS != (tmp_ret = insert_token(Token(token.scheduled_time, token.delay, token.task)))) {
          OB_LOG(WARN, "insert token error", K(tmp_ret), K(token));
        }
      }
      has_running_task_ = false;
      if (is_stopped_) {
        monitor_.notifyAll();
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
        const int64_t now = Time::now(Time::Monotonic).toMicroSeconds();
        if (tokens_[0].scheduled_time <= now) {
          has_running_task_ = true;
          token = tokens_[0];
          memmove(tokens_, tokens_ + 1, (tasks_num_ - 1) * sizeof(tokens_[0]));
          --tasks_num_;
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
            const int64_t rt1 = ObTimeUtility::current_time();
            const int64_t rt2 = ObTimeUtility::current_time_coarse();
            const int64_t delta = rt1 > rt2 ? (rt1 - rt2) : (rt2 - rt1);
            static const int64_t MAX_REALTIME_DELTA1 = 20000;   // 20ms
            static const int64_t MAX_REALTIME_DELTA2 = 500000;  // 500ms
            if (delta > MAX_REALTIME_DELTA1) {
              OB_LOG(WARN, "Hardware clock skew", K(rt1), K(rt2), K_(wakeup_time), K(now));
            } else if (delta > MAX_REALTIME_DELTA2) {
              OB_LOG(ERROR, "Hardware clock error", K(rt1), K(rt2), K_(wakeup_time), K(now));
            }
          }
          monitor_.timedWait(Time(wakeup_time_ - now));
        }
      }
    }

    if (token.task != NULL && has_running_task_ && !is_destroyed_ && !is_stopped_) {
      bool timeout_check = token.task->timeout_check();
      const int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
      if (timeout_check) {
        ObTimerMonitor::get_instance().start_task(thread_id_, start_time, token.delay, token.task);
      }
      token.task->runTimerTask();

      const int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
      const int64_t elapsed_time = end_time - start_time;
      EVENT_ADD(SYS_TIME_MODEL_DB_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_DB_CPU, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_BKGD_TIME, elapsed_time);
      EVENT_ADD(SYS_TIME_MODEL_BKGD_CPU, elapsed_time);
      if (timeout_check) {
        ObTimerMonitor::get_instance().end_task(thread_id_, end_time);
      }

      if (elapsed_time > 1000 * 1000) {
        OB_LOG(WARN,
            "timer task cost too much time",
            "task",
            to_cstring(*token.task),
            K(start_time),
            K(end_time),
            K(elapsed_time),
            KP(this),
            K_(thread_id));
      }
    }
  }
  OB_LOG(INFO, "timer thread exit", KP(this), K_(thread_id));
}

void ObTimer::dump() const
{
  for (int32_t i = 0; i < tasks_num_; ++i) {
    printf("%d : %ld %ld %p\n", i, tokens_[i].scheduled_time, tokens_[i].delay, tokens_[i].task);
  }
}

bool ObTimer::inited() const
{
  return is_inited_;
}

}  // namespace common
}  // namespace oceanbase
