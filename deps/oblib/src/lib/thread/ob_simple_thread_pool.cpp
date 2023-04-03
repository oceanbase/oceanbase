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

#define USING_LOG_PREFIX COMMON
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/ob_simple_thread_pool.h"

namespace oceanbase
{
namespace common
{
ObSimpleThreadPool::ObSimpleThreadPool()
    : name_("unknown"), is_inited_(false), total_thread_num_(0), active_thread_num_(0),
      last_adjust_ts_(0)
{
}

ObSimpleThreadPool::~ObSimpleThreadPool()
{
  if (is_inited_) {
    destroy();
  }
}

int ObSimpleThreadPool::init(const int64_t thread_num, const int64_t task_num_limit, const char* name, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (thread_num <= 0 || task_num_limit <= 0 || thread_num > MAX_THREAD_NUM || OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(queue_.init(task_num_limit, name, tenant_id))) {
    LOG_WARN("task queue init failed", K(ret), K(task_num_limit));
  } else {
    is_inited_ = true;
    name_ = name;
    total_thread_num_ = thread_num;
    active_thread_num_ = thread_num;
    set_thread_count(thread_num);
    last_adjust_ts_ = ObTimeUtility::current_time();
    if (OB_FAIL(lib::ThreadPool::start())) {
      LOG_WARN("start thread pool fail", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  } else {
    LOG_INFO("simple thread pool init success", KCSTRING(name), K(thread_num), K(task_num_limit));
  }
  return ret;
}

void ObSimpleThreadPool::destroy()
{
  is_inited_ = false;
  lib::ThreadPool::stop();
  lib::ThreadPool::wait();
  queue_.destroy();
}

int ObSimpleThreadPool::push(void *task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else if (has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  } else {
    ret = queue_.push(task);
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

void ObSimpleThreadPool::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_idx = get_thread_idx();
  int64_t start_ts = 0;
  int64_t wakeup_ts = 0;
  int64_t handle_ts = 0;
  int64_t idle_ts = 0;
  int64_t run_ts = 0;
  if (NULL != name_) {
    lib::set_thread_name(name_, thread_idx);
  }
  while (!has_set_stop() && !(OB_NOT_NULL(&lib::Thread::current()) ? lib::Thread::current().has_set_stop() : false)) {
    void *task = NULL;
    const int64_t old_thread_num = active_thread_num_;
    int64_t new_thread_num = old_thread_num;
    const int64_t curr_thread_num = get_thread_count();
    if (!adaptive_strategy_.is_valid()) {
      if (OB_SUCC(queue_.pop(task, QUEUE_WAIT_TIME))) {
        handle(task);
      }
    } else if (curr_thread_num != total_thread_num_) {
      ATOMIC_STORE(&total_thread_num_, curr_thread_num);
      ATOMIC_STORE(&active_thread_num_, curr_thread_num);
      if (OB_SUCC(queue_.pop(task, QUEUE_WAIT_TIME))) {
        handle(task);
      }
    } else if (thread_idx >= old_thread_num) {
      usleep((10 + thread_idx - old_thread_num) * 1000);
    } else {
      void *task = NULL;
      const int64_t least_thread_num = adaptive_strategy_.get_least_thread_num();
      const int64_t estimate_ts = adaptive_strategy_.get_estimate_ts();
      const int64_t expand_ts =
          adaptive_strategy_.get_estimate_ts() * adaptive_strategy_.get_expand_rate() / 100;
      const int64_t shrink_ts =
          adaptive_strategy_.get_estimate_ts() * adaptive_strategy_.get_shrink_rate() / 100;
      start_ts = ObTimeUtility::current_time();
      if (OB_SUCC(queue_.pop(task, QUEUE_WAIT_TIME))) {
        wakeup_ts = ObTimeUtility::current_time();
        handle(task);
        handle_ts = ObTimeUtility::current_time();
      } else {
        wakeup_ts = ObTimeUtility::current_time();
        handle_ts = wakeup_ts;
      }
      idle_ts += (wakeup_ts - start_ts);
      run_ts += (handle_ts - wakeup_ts);
      if (idle_ts + run_ts > estimate_ts) {
        if (run_ts > expand_ts) {
          const int64_t cur_ts = ObTimeUtility::current_time();
          const int64_t last_ts = ATOMIC_LOAD(&last_adjust_ts_);
          if ((cur_ts - last_ts > estimate_ts / 4) && ATOMIC_BCAS(&last_adjust_ts_, last_ts, cur_ts)) {
            new_thread_num = old_thread_num + 2;
            if (new_thread_num >= total_thread_num_) {
              new_thread_num = total_thread_num_;
            }
          }
        } else if (run_ts < shrink_ts) {
          const int64_t cur_ts = ObTimeUtility::current_time();
          const int64_t last_ts = ATOMIC_LOAD(&last_adjust_ts_);
          if ((cur_ts - last_ts > estimate_ts / 6) && ATOMIC_BCAS(&last_adjust_ts_, last_ts, cur_ts)) {
            if (old_thread_num > least_thread_num) {
              new_thread_num = old_thread_num - 1;
            }
          }
        } else {
          // do nothing
        }
        if (new_thread_num > old_thread_num) {
          if (ATOMIC_BCAS(&active_thread_num_, old_thread_num, new_thread_num) &&
              REACH_TIME_INTERVAL(100000)) {
            LOG_INFO("activate work thread", KCSTRING(name_), K(new_thread_num), K(idle_ts), K(run_ts));
          }
        } else if (new_thread_num < old_thread_num) {
          if (ATOMIC_BCAS(&active_thread_num_, old_thread_num, new_thread_num) &&
              REACH_TIME_INTERVAL(100000)) {
            LOG_INFO("inactivate work thread", KCSTRING(name_), K(new_thread_num), K(idle_ts), K(run_ts));
          }
        } else {
          // do thing
        }
        idle_ts = 0;
        run_ts = 0;
      }
    }
  }
  if (has_set_stop()) {
    void *task = NULL;
    while (OB_SUCC(queue_.pop(task))) {
      handle_drop(task);
    }
  }
}

int ObSimpleThreadPool::set_adaptive_strategy(const ObAdaptiveStrategy &strategy)
{
  int ret = OB_SUCCESS;
  if (!strategy.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KCSTRING(name_), K(strategy));
  } else {
    adaptive_strategy_ = strategy;
    LOG_INFO("set thread pool adaptive strategy success", KCSTRING(name_), K(strategy));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
