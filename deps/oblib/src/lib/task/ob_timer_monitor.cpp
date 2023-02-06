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

#include "lib/task/ob_timer_monitor.h"

namespace oceanbase
{
namespace common
{

ObTimerMonitor::ObTimerMonitor()
  : inited_(false),
    timer_(),
    monitor_task_(*this),
    records_(),
    tail_(0)
{
  memset(&records_, 0, sizeof(records_));
}

ObTimerMonitor::~ObTimerMonitor()
{
}

ObTimerMonitor& ObTimerMonitor::get_instance()
{
  static ObTimerMonitor instance_;
  return instance_;
}

int ObTimerMonitor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "init twice", K(ret));
  } else if (OB_FAIL(timer_.init())) {
    OB_LOG(ERROR, "fail to init timer", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObTimerMonitor::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(timer_.schedule(monitor_task_, CHECK_INTERVAL, true))) {
    OB_LOG(ERROR, "fail to schedule task", K(ret));
  }

  return ret;
}

void ObTimerMonitor::wait()
{
  timer_.wait();
}


void ObTimerMonitor::stop()
{
  timer_.stop();
}

void ObTimerMonitor::destroy()
{
  timer_.destroy();
}

int64_t ObTimerMonitor::find_pos(const int64_t thread_id) const
{
  int64_t tail = ATOMIC_LOAD(&tail_);
  int64_t pos = -1;

  for (int64_t i = 0; i < tail; ++i) {
    if (thread_id == records_[i].thread_id_) {
      pos = i;
      break;
    }
  }

  return pos;
}

int64_t ObTimerMonitor::record_new_thread(const int64_t thread_id)
{
  int64_t tail = ATOMIC_LOAD(&tail_);
  int64_t pos = -1;

  while (tail < MAX_MONITOR_THREAD_NUM) {
    int64_t new_tail = -1;
    if (tail == (new_tail = ATOMIC_VCAS(&tail_, tail, tail + 1))) {
      pos = tail;
      break;
    } else {
      tail = new_tail;
    }
  }

  if (pos >= 0) {
    records_[pos].thread_id_ = thread_id;
  }

  return pos;
}

void ObTimerMonitor::start_task(const int64_t thread_id,
                                const int64_t start_time,
                                const int64_t interval,
                                const ObTimerTask* task)
{
  int64_t pos = find_pos(thread_id);
  if (pos < 0) {
    pos = record_new_thread(thread_id);
  }

  if (pos >= 0) {
    records_[pos].seq_++;
    MEM_BARRIER();

    records_[pos].start_time_ = start_time;
    records_[pos].interval_ = interval;
    records_[pos].task_ = task;

    MEM_BARRIER();
    records_[pos].seq_++;
  }
}

void ObTimerMonitor::end_task(const int64_t thread_id, const int64_t end_time)
{
  UNUSED(end_time);

  int64_t pos = find_pos(thread_id);
  if (pos >= 0) {
    records_[pos].seq_++;
    MEM_BARRIER();

    int64_t cost_time = end_time - records_[pos].start_time_;
    if (0 == ATOMIC_FAA(&records_[pos].task_cnt_, 1L)) {
      records_[pos].cost_time_ = cost_time;
    } else {
      records_[pos].cost_time_ += cost_time;
    }

    records_[pos].start_time_ = 0;
    records_[pos].interval_ = 0;
    records_[pos].task_ = NULL;

    MEM_BARRIER();
    records_[pos].seq_++;
  }
}

void ObTimerMonitor::dump(const bool print_trace)
{
  static const int64_t TIMER_TIMEOUT_MIN = 1L * 60L * 1000L * 1000L; // 1 min
  static const int64_t TIMER_TIMEOUT_MAX = 15L * 60L * 1000L * 1000L; // 15 mins

  int64_t tail = ATOMIC_LOAD(&tail_);
  const int64_t curr_time = ObTimeUtility::current_time();
  TimerRecord record;
  int64_t seq = 0;

  for (int64_t i = 0; i < tail; ++i) {
    do {
      seq = records_[i].seq_;
      MEM_BARRIER();
      record = records_[i];
      MEM_BARRIER();
    } while (seq & 1 || seq != records_[i].seq_);

    if (record.task_ != NULL && 0 != record.interval_) {
      int64_t timeout = std::max(std::min(record.interval_ * 100, TIMER_TIMEOUT_MAX), TIMER_TIMEOUT_MIN);
      if (curr_time > record.start_time_ + timeout) {
        // timeout
        OB_LOG_RET(ERROR, OB_ERR_TOO_MUCH_TIME, "TIMER TASK TIMEOUT: timer task cost too much time", K(record));
      }
    }

    if (print_trace) {
      int64_t avg_time = 0 == record.task_cnt_ ? 0 : record.cost_time_ / record.task_cnt_;
      OB_LOG(INFO, "TIMER THREAD STAT: ",
             "thread_id", record.thread_id_,
             "task_cnt", record.task_cnt_,
             "avg_time", avg_time);

      records_[i].task_cnt_ = 0;
    }
  }
}

ObTimerMonitor::ObTimerMonitorTask::ObTimerMonitorTask(ObTimerMonitor &monitor)
  : monitor_(monitor),
    running_cnt_(0)
{
}

ObTimerMonitor::ObTimerMonitorTask::~ObTimerMonitorTask()
{
}

void ObTimerMonitor::ObTimerMonitorTask::runTimerTask()
{
  bool print_trace = false;
  if (0 == running_cnt_ % 6) {
    print_trace = true;
  }
  monitor_.dump(print_trace);
  running_cnt_++;
}


}
}
