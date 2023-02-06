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

#include "lib/thread/ob_thread_name.h"
#include "ob_time_wheel.h"
#include "lib/oblog/ob_trace_log.h"
#include "common/ob_clock_generator.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{

namespace common
{

using namespace obutil;

void ObTimeWheelTask::reset()
{
  // twtask
  magic_number_ = 0x00006b7361747774;
  bucket_idx_ = INVALID_BUCKET_IDX;
  run_ticket_ = 0;
  scan_ticket_ = 0;
  is_scheduled_ = false;
  ObDLinkBase<ObTimeWheelTask>::reset();
}

int ObTimeWheelTask::schedule(const int64_t bucket_idx, const int64_t run_ticket)
{
  int ret = OB_SUCCESS;

  if (bucket_idx < 0 || run_ticket <= 0) {
    TRANS_LOG(WARN, "invalid argument", K(bucket_idx), K(run_ticket));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_scheduled_) {
    TRANS_LOG(WARN, "task has already been scheduled", "task", *this);
    ret = OB_TIMER_TASK_HAS_SCHEDULED;
  } else {
    bucket_idx_ = bucket_idx;
    run_ticket_ = run_ticket;
    is_scheduled_ = true;
  }

  return ret;
}

int ObTimeWheelTask::cancel()
{
  int ret = OB_SUCCESS;

  if (!is_scheduled_) {
    ret = OB_TIMER_TASK_HAS_NOT_SCHEDULED;
  } else {
    is_scheduled_ = false;
    if (NULL != prev_ && NULL != next_) {
      ObDLinkBase<ObTimeWheelTask>::unlink();
    } else if (NULL == prev_ && NULL == next_) {
      // do nothing
    } else {
      TRANS_LOG(WARN, "unlink error", KP(prev_), KP(next_));
      BACKTRACE(ERROR, true, "task unlink error, prev_ or next_ may be null");
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

void ObTimeWheelTask::runTask()
{
  runTimerTask();
}

int64_t ObTimeWheelTask::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos,
      "[prev=%p, next=%p]"
      "[magic_number=%ld, bucket_idx=%ld, run_ticket=%ld, scan_ticket=%ld, is_scheduled=%d]",
      prev_, next_, magic_number_, bucket_idx_, run_ticket_, scan_ticket_, is_scheduled_);
  return pos;
}

int TimeWheelBase::init(const int64_t precision, const char *name)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "TimeWheelBase inited twice");
    ret = OB_INIT_TWICE;
  } else if (precision <= 0 || OB_ISNULL(name)) {
    TRANS_LOG(WARN, "invalid argument", K(precision), KP(name));
    ret = OB_INVALID_ARGUMENT;
  } else {
    precision_ = precision;
    start_ticket_ = ObClockGenerator::getRealClock() / precision_;
    scan_ticket_ = start_ticket_;
    tname_[sizeof(tname_) - 1] = '\0';
    (void)snprintf(tname_, sizeof(tname_) - 1, "%s", name);
    is_inited_ = true;
    TRANS_LOG(INFO, "TimeWheelBase inited success", K_(precision), K_(start_ticket), K_(scan_ticket));
  }

  return ret;
}

int TimeWheelBase::schedule_(ObTimeWheelTask *task, const int64_t run_ticket)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "TimeWheelBase not inited");
    ret = OB_NOT_INIT;
  } else if (has_set_stop()) {
    TRANS_LOG(WARN, "TimeWheelBase is not running");
    ret = OB_NOT_RUNNING;
  } else if (NULL == task || run_ticket < 0) {
    TRANS_LOG(WARN, "invalid argument", KP(task), K(run_ticket));
    ret = OB_INVALID_ARGUMENT;
  } else if (run_ticket < start_ticket_) {
    TRANS_LOG(ERROR, "clock out of order", K(run_ticket), K_(start_ticket), "task", *task);
    ret = OB_CLOCK_OUT_OF_ORDER;
  } else {
    bool need_retry = true;
    while (OB_SUCC(ret) && need_retry) {
      int64_t tmp_run_ticket = run_ticket;
      const int64_t tmp_scan_ticket = ATOMIC_LOAD(&scan_ticket_);
      if (tmp_scan_ticket >= run_ticket) {
        tmp_run_ticket = tmp_scan_ticket + 1;
      }
      const int64_t idx = (tmp_run_ticket - start_ticket_) % MAX_BUCKET;
      TaskBucket *bucket = &(buckets_[idx]);
      task->lock();
      bucket->lock();
      // scan_ticket_还没有跨过即将插入的桶，不需要重试
      if (ATOMIC_LOAD(&scan_ticket_) < tmp_run_ticket) {
        need_retry = false;
        if (OB_SUCCESS != (ret = task->schedule(idx, tmp_run_ticket))) {
          TRANS_LOG(WARN, "schedule error", KR(ret), "task", *task);
        } else {
          bucket->list_.add_last(task);
          if (OB_ISNULL(task->get_next())) {
            TRANS_LOG(ERROR, "task next pointer is NULL");
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_ISNULL(task->get_prev())) {
            TRANS_LOG(ERROR, "task prev pointer is NULL");
            ret = OB_ERR_UNEXPECTED;
          } else {
            // do nothing
          }
        }
      }
      bucket->unlock();
      task->unlock();
    }
  }

  return ret;
}

int TimeWheelBase::schedule(ObTimeWheelTask *task, const int64_t delay)
{
  int ret = OB_SUCCESS;
  int64_t run_ticket = 0;

  if (!is_inited_) {
    TRANS_LOG(WARN, "TimeWheelBase not inited");
    ret = OB_NOT_INIT;
  } else if (has_set_stop()) {
    TRANS_LOG(WARN, "TimeWheelBase is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(task) || delay < 0) {
    TRANS_LOG(WARN, "invalid argument", KP(task), K(delay));
    ret = OB_INVALID_ARGUMENT;
  } else {
    run_ticket = (ObClockGenerator::getRealClock() + delay + precision_ - 1) / precision_;
    if (OB_SUCCESS != (ret = schedule_(task, run_ticket))) {
      TRANS_LOG(WARN, "schedule error", KR(ret), "task", *task, K(delay));
    }
  }

  return ret;
}

int TimeWheelBase::cancel(ObTimeWheelTask *task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "TimeWheelBase not inited");
    ret = OB_NOT_INIT;
  // do not check transaction timer is running or not
  // we can unregister timeout task successful always
  } else if (OB_ISNULL(task)) {
    TRANS_LOG(WARN, "invalid argument", KP(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    task->lock();
    if (!task->is_scheduled()) {
      ret = OB_TIMER_TASK_HAS_NOT_SCHEDULED;
    } else {
      const int64_t idx = task->get_bucket_idx();
      if (ObTimeWheelTask::INVALID_BUCKET_IDX == idx) {
        TRANS_LOG(ERROR, "invalid bucket index", K(idx));
        ret = OB_ERR_UNEXPECTED;
      } else {
        TaskBucket *bucket = &(buckets_[idx]);
        bucket->lock();
        task->cancel();
        bucket->unlock();
      }
    }
    task->unlock();
  }

  return ret;
}

int TimeWheelBase::scan()
{
  int ret = OB_SUCCESS;
  int64_t sleep_us = 0;
  const int64_t WARN_RUNTIME_US = 100 * 1000;

  while (OB_SUCC(ret) && !has_set_stop()) {
    const int64_t cur_ts = ObClockGenerator::getRealClock();
    const int64_t cur_ticket = cur_ts / precision_;

    while (OB_SUCC(ret) && !has_set_stop() && scan_ticket_ <= cur_ticket) {
      bool need_retry = true;
      const int64_t idx = (scan_ticket_ - start_ticket_) % MAX_BUCKET;
      TaskBucket *bucket = &(buckets_[idx]);
      while (OB_SUCC(ret) && !has_set_stop() && need_retry) {
        bool need_run = false;
        bucket->lock();
        ObTimeWheelTask *task = bucket->list_.get_first();
        if (NULL == task) {
          TRANS_LOG(WARN, "task is NULL", KP(task));
          ret = OB_ERR_UNEXPECTED;
        } else if (bucket->list_.get_header() == task) {
          // bucket is empty
          if (!bucket->list_.is_empty()) {
            TRANS_LOG(ERROR, "bucket is empty");
            ret = OB_ERR_UNEXPECTED;
          } else {
            need_retry = false;
          }
        } else {
          // trylock, avoid deadlock
          if (0 == task->trylock()) {
            const ObTimeWheelTask *const tmp_task = bucket->list_.remove(task);
            if (OB_ISNULL(tmp_task)) {
              TRANS_LOG(ERROR, "task is NULL");
              ret = OB_ERR_UNEXPECTED;
            } else if (task->get_scan_ticket() == scan_ticket_) {
              bucket->list_.add_first(task);
              need_retry = false;
            } else if (task->get_run_ticket() <= scan_ticket_) {
              task->cancel();
              task->begin_run();
              need_run = true;
            } else {
              task->set_scan_ticket(scan_ticket_);
              bucket->list_.add_last(task);
            }
            task->unlock();
          } // try lock success
        } // bucket is not empty
        bucket->unlock();
        if (need_run) {
          const int64_t start = ObTimeUtility::current_time();
          task->runTask();
          const int64_t end = ObTimeUtility::current_time();
          //task执行完之后，ctx内存可能已经释放，此时不能再打印task对象信息；
          if (end - start >= WARN_RUNTIME_US) {
            TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "timer task use too much time", K(end), K(start), "delta", end - start);
          }
        }
      }
      (void)ATOMIC_FAA(&scan_ticket_, 1);
    }

    sleep_us = precision_ - (ObClockGenerator::getRealClock() - cur_ts);
    if (sleep_us > 0) {
      if (sleep_us > MAX_SCAN_SLEEP) {
        ObClockGenerator::usleep(MAX_SCAN_SLEEP);
      } else {
        ObClockGenerator::usleep(sleep_us);
      }
    }
  } // while

  return ret;
}

void TimeWheelBase::run1()
{
  lib::set_thread_name(tname_);
  scan();
}

TimeWheelBase *TimeWheelBase::alloc(const char *name)
{
  TimeWheelBase *base = NULL;
  void *ptr = share::mtl_malloc(sizeof(TimeWheelBase), name);
  if (OB_ISNULL(ptr)) {
    TRANS_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "alloc TimeWheelBase failed");
  } else if (OB_ISNULL(base = new(ptr) TimeWheelBase())) {
    TRANS_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "construct TimeWheelBase failed");
    share::mtl_free(ptr);
    ptr = NULL;
  }
  return base;
}

void TimeWheelBase::free(TimeWheelBase *base)
{
  if (OB_NOT_NULL(base)) {
    base->~TimeWheelBase();
    share::mtl_free(base);
  }
}

int ObTimeWheel::init(const int64_t precision, const int64_t real_thread_num, const char *name)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTimeWheel init twice");
    ret = OB_INIT_TWICE;
  } else if (precision <= 0 || real_thread_num <= 0 || MAX_THREAD_NUM < real_thread_num
      || OB_ISNULL(name) || strlen(name) == 0) {
    TRANS_LOG(WARN, "invalid argument", K(precision), K(real_thread_num), KP(name));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; i < real_thread_num && OB_SUCCESS == ret; ++i) {
      if (OB_ISNULL(tw_base_[i] = TimeWheelBase::alloc(name))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "TimeWheelBase alloc failed", K(ret));
      } else if (OB_SUCCESS != (ret = tw_base_[i]->init(precision, name))) {
        TRANS_LOG(WARN, "TimeWheelBase init error", KR(ret));
      } else {
        tw_base_[i]->set_run_wrapper(MTL_CTX());
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < real_thread_num; ++i) {
        if (OB_NOT_NULL(tw_base_[i])) {
          TimeWheelBase::free(tw_base_[i]);
          tw_base_[i] = NULL;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    real_thread_num_ = real_thread_num;
    precision_ = precision;
    tname_[sizeof(tname_) - 1] = '\0';
    (void)snprintf(tname_, sizeof(tname_) - 1, "%s", name);
    TRANS_LOG(INFO, "ObTimeWheel init success", K(precision), K(real_thread_num));
  }

  return ret;
}

void ObTimeWheel::reset()
{
  is_inited_ = false;
  is_running_ = false;
  real_thread_num_ = 0;
  precision_ = 1;
  memset(tname_, 0, sizeof(tname_));
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    tw_base_[i] = NULL;
  }
}

int ObTimeWheel::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTimeWheel is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObTimeWheel is already running", "timer_name", tname_);
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t i = 0; i < real_thread_num_ && OB_SUCCESS == ret; ++i) {
      if (OB_SUCCESS != (ret = tw_base_[i]->start())) {
        TRANS_LOG(WARN, "TimeWheelBase start error", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < real_thread_num_; ++i) {
        if (NULL != tw_base_[i]) {
          (void)tw_base_[i]->stop();
          (void)tw_base_[i]->wait();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_running_ = true;
    TRANS_LOG(INFO, "ObTimeWheel start success", "timer_name", tname_);
  }

  return ret;
}

int ObTimeWheel::stop()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTimeWheel is not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObTimeWheel already has been stopped", "timer_name", tname_);
    ret = OB_NOT_RUNNING;
  } else {
    for (int64_t i = 0; i < real_thread_num_; ++i) {
      if (NULL != tw_base_[i]) {
        (void)tw_base_[i]->stop();
      }
    }
    is_running_ = false;
    TRANS_LOG(INFO, "ObTimeWheel stop success", "timer_name", tname_);
  }

  return ret;
}

int ObTimeWheel::wait()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTimeWheel is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObTimeWheel is already running", "timer_name", tname_);
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < real_thread_num_; ++i) {
      if (NULL != tw_base_[i]) {
        tw_base_[i]->wait();
      }
    }
  }
  if (OB_SUCC(ret)) {
    TRANS_LOG(INFO, "ObTimeWheel wait success");
  }

  return ret;
}

void ObTimeWheel::destroy()
{
  int tmp_ret = OB_SUCCESS;

  if (is_inited_) {
    if (is_running_) {
      if (OB_SUCCESS != (tmp_ret = stop())) {
        TRANS_LOG_RET(WARN, tmp_ret, "ObTimeWheel stop error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG_RET(WARN, tmp_ret, "ObTimeWheel wait error", K(tmp_ret));
      } else {
        // do nothing
      }
    }
    for (int64_t i = 0; i < real_thread_num_; ++i) {
      if (OB_NOT_NULL(tw_base_[i])) {
        TimeWheelBase::free(tw_base_[i]);
        tw_base_[i] = NULL;
      }
    }
    real_thread_num_ = 0;
    is_inited_ = false;
    TRANS_LOG(INFO, "ObTimeWheel destroy success");
  }
}
int ObTimeWheel::schedule(ObTimeWheelTask *task, const int64_t delay)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTimeWheel not init");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObTimeWheel is not running", "timer_name", tname_);
    ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(task) || delay <= 0) {
    TRANS_LOG(WARN, "invalid argument", KP(task), K(delay));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS !=
      (ret = tw_base_[task->hash() % real_thread_num_]->schedule(task, delay))) {
    TRANS_LOG(WARN, "TimeWheelBase schedule error", KR(ret), "task", *task, K(delay));
  } else {
    //do nothing
  }

  return ret;
}

int ObTimeWheel::cancel(ObTimeWheelTask *task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTimeWheel not init");
    ret = OB_NOT_INIT;
  // do not check transaction timer is running or not
  // we can unregister timeout task successful always
  } else if (OB_ISNULL(task)) {
    TRANS_LOG(WARN, "invalid argument", KP(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = tw_base_[task->hash() % real_thread_num_]->cancel(task))) {
    TRANS_LOG(DEBUG, "TimeWheelBase cancel error", KR(ret), "task", *task);
  } else {
    //do nothing
  }

  return ret;
}

} // common
} // oceanbase
