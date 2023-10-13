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

#include <errno.h>
#include "lib/oblog/ob_trace_log.h"
#include "common/ob_queue_thread.h"
#include "lib/utility/utility.h"

extern "C" {
int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
void ob_pthread_join(void *ptr);
}

namespace oceanbase
{
namespace common
{
ObCond::ObCond(const int64_t spin_wait_num) : spin_wait_num_(spin_wait_num),
                                              bcond_(false),
                                              last_waked_time_(0)
{
  pthread_mutex_init(&mutex_, NULL);
  if (0 != pthread_cond_init(&cond_, NULL)) {
    _OB_LOG_RET(ERROR, common::OB_ERR_SYS, "pthread_cond_init failed");
  }
}

ObCond::~ObCond()
{
  pthread_mutex_destroy(&mutex_);
  pthread_cond_destroy(&cond_);
}

void ObCond::signal()
{
  if (false == ATOMIC_CAS(&bcond_, false, true)) {
    __sync_synchronize();
    (void)pthread_mutex_lock(&mutex_);
    (void)pthread_cond_signal(&cond_);
    (void)pthread_mutex_unlock(&mutex_);
  }
}

int ObCond::wait()
{
  int ret = OB_SUCCESS;
  bool need_wait = true;
  if ((last_waked_time_ + BUSY_INTERVAL) > ::oceanbase::common::ObTimeUtility::current_time()) {
    //return OB_SUCCESS;
    for (int64_t i = 0; i < spin_wait_num_; i++) {
      if (true == ATOMIC_CAS(&bcond_, true, false)) {
        need_wait = false;
        break;
      }
      PAUSE();
    }
  }
  if (need_wait) {
    pthread_mutex_lock(&mutex_);
    while (OB_SUCC(ret) && false == ATOMIC_CAS(&bcond_, true, false)) {
      int tmp_ret = ob_pthread_cond_wait(&cond_, &mutex_);
      if (ETIMEDOUT == tmp_ret) {
        ret = OB_TIMEOUT;
        break;
      }
    }
    pthread_mutex_unlock(&mutex_);
  }
  if (OB_SUCC(ret)) {
    last_waked_time_ = ::oceanbase::common::ObTimeUtility::current_time();
  }
  return ret;
}
int ObCond::timedwait(const int64_t time_us)
{
  int ret = OB_SUCCESS;
  bool need_wait = true;
  if ((last_waked_time_ + BUSY_INTERVAL) > ::oceanbase::common::ObTimeUtility::current_time()) {
    for (int64_t i = 0; i < spin_wait_num_; i++) {
      if (true == ATOMIC_CAS(&bcond_, true, false)) {
        need_wait = false;
        break;
      }
      PAUSE();
    }
  }
  if (need_wait) {
    int64_t abs_time = time_us + ::oceanbase::common::ObTimeUtility::current_time();
    struct timespec ts;
    ts.tv_sec = abs_time / 1000000;
    ts.tv_nsec = (abs_time % 1000000) * 1000;
    pthread_mutex_lock(&mutex_);
    while (OB_SUCC(ret) && false == ATOMIC_CAS(&bcond_, true, false)) {
      int tmp_ret = ob_pthread_cond_timedwait(&cond_, &mutex_, &ts);
      if (ETIMEDOUT == tmp_ret) {
        ret = OB_TIMEOUT;
        break;
      }
    }
    pthread_mutex_unlock(&mutex_);
  }
  if (OB_SUCC(ret)) {
    last_waked_time_ = ::oceanbase::common::ObTimeUtility::current_time();
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

S2MQueueThread::S2MQueueThread() : thread_num_(0),
                                   thread_conf_iter_(0),
                                   thread_conf_lock_(ObLatchIds::DEFAULT_DRW_LOCK),
                                   queued_num_(),
                                   queue_rebalance_(false)
{
  memset((void *)each_queue_len_, 0, sizeof(each_queue_len_));
}

S2MQueueThread::~S2MQueueThread()
{
  destroy();
}

int S2MQueueThread::init(const int64_t thread_num, const int64_t task_num_limit,
                         const bool queue_rebalance, const bool dynamic_rebalance)
{
  int ret = OB_SUCCESS;
  queue_rebalance_ = queue_rebalance;
  if (0 != thread_num_) {
    ret = OB_INIT_TWICE;
  } else if (0 >= thread_num
             || MAX_THREAD_NUM < thread_num
             || 0 >= task_num_limit) {
    _OB_LOG(WARN, "invalid param, thread_num=%ld task_num_limit=%ld",
              thread_num, task_num_limit);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = launch_thread_(thread_num, task_num_limit))) {
    _OB_LOG(WARN, "launch thread fail, ret=%d thread_num=%ld task_num_limit=%ld", ret, thread_num,
              task_num_limit);
  } else if (OB_SUCCESS != (ret = balance_filter_.init(thread_num_, dynamic_rebalance))) {
    _OB_LOG(WARN, "init balance_filter fail, ret=%d thread_num=%ld", ret, thread_num_);
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    destroy();
  } else {
    thread_conf_iter_ = 0;
  }
  return ret;
}

void S2MQueueThread::destroy()
{
  for (int64_t i = 0; i < thread_num_; i++) {
    ThreadConf &tc = thread_conf_array_[i];
    tc.run_flag = false;
    tc.stop_flag = true;
    tc.queue_cond.signal();
    ob_pthread_join(tc.pd);
  }
  for (int64_t i = 0; i < thread_num_; i++) {
    ThreadConf &tc = thread_conf_array_[i];
    tc.high_prio_task_queue.destroy();
    tc.spec_task_queue.destroy();
    tc.comm_task_queue.destroy();
    tc.low_prio_task_queue.destroy();
  }
  balance_filter_.destroy();
  thread_num_ = 0;
}

int64_t S2MQueueThread::get_queued_num() const
{
  return queued_num_.value();
}

int S2MQueueThread::wakeup()
{
  int err = OB_SUCCESS;
  if (thread_num_ > 0) {
    DRWLock::RDLockGuard guard(thread_conf_lock_);
    for (int64_t i = 0; i < thread_num_; i++) {
      thread_conf_array_[i % thread_num_].queue_cond.signal();
    }
  }
  return err;
}

int S2MQueueThread::push(void *task, const int64_t prio)
{
  int ret = OB_SUCCESS;
  int queue_id = -1;
  if (0 >= thread_num_) {
    ret = OB_NOT_INIT;
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    RDLockGuard guard(thread_conf_lock_);
    uint64_t i = ATOMIC_AAF(&thread_conf_iter_, 1);
    ThreadConf &tc = thread_conf_array_[i % thread_num_];
    switch (prio) {
      case HIGH_PRIV:
        ret = tc.high_prio_task_queue.push(task);
        queue_id = HIGH_PRIO_QUEUE;
        break;
      case NORMAL_PRIV:
        ret = tc.comm_task_queue.push(task);
        queue_id = NORMAL_PRIO_QUEUE;
        break;
      case LOW_PRIV:
        ret = tc.low_prio_task_queue.push(task);
        queue_id = LOW_PRIO_QUEUE;
        break;
      default:
        ret = OB_NOT_SUPPORTED;
    }
    if (OB_SUCC(ret)) {
      tc.queue_cond.signal();
    }
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_EAGAIN;
    }
  }
  if (OB_SUCC(ret)) {
    if (queue_id >= 0 && queue_id < QUEUE_COUNT) {
      each_queue_len_[queue_id].inc(1);
    }
    queued_num_.inc(1);
  }
  return ret;
}

int S2MQueueThread::push(void *task, const uint64_t task_sign, const int64_t prio)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == task_sign) {
    ret = push(task, prio);
  } else if (0 >= thread_num_) {
    ret = OB_NOT_INIT;
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t filted_task_sign = balance_filter_.filt(task_sign);
    RDLockGuard guard(thread_conf_lock_);
    ThreadConf &tc = thread_conf_array_[filted_task_sign % thread_num_];
    _OB_LOG(TRACE, "req_sign=%lu,%lu hash=%lu", filted_task_sign % thread_num_, filted_task_sign,
              task_sign);
    if (OB_SUCCESS != (ret = tc.spec_task_queue.push(task))) {
      //_OB_LOG(WARN, "push task to queue fail, ret=%d", ret);
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_EAGAIN;
      }
    } else {
      tc.queue_cond.signal();
      queued_num_.inc(1);
      each_queue_len_[HOTSPOT_QUEUE].inc(1);
    }
  }
  return ret;
}

int S2MQueueThread::set_prio_quota(v4si &quota)
{
  int ret = OB_SUCCESS;
  if (0 >= thread_num_) {
    ret = OB_NOT_INIT;
  } else {
    WRLockGuard guard(thread_conf_lock_);
    for (int64_t i = 0; i < thread_num_; i++) {
      ThreadConf &tc = thread_conf_array_[i];
      tc.scheduler_.set_quota(quota);
    }
  }
  return ret;
}

int S2MQueueThread::add_thread(const int64_t thread_num, const int64_t task_num_limit)
{
  int ret = OB_SUCCESS;
  if (0 >= thread_num_) {
    ret = OB_NOT_INIT;
  } else if (0 >= thread_num) {
    _OB_LOG(WARN, "invalid param, thread_num=%ld", thread_num);
    ret = OB_INVALID_ARGUMENT;
  } else {
    WRLockGuard guard(thread_conf_lock_);
    int64_t prev_thread_num = thread_num_;
    ret = launch_thread_(thread_num, task_num_limit);
    balance_filter_.add_thread(thread_num_ - prev_thread_num);
  }
  return ret;
}

int S2MQueueThread::sub_thread(const int64_t thread_num)
{
  int ret = OB_SUCCESS;
  int64_t prev_thread_num = 0;
  int64_t cur_thread_num = 0;
  if (0 >= thread_num_) {
    ret = OB_NOT_INIT;
  } else {
    WRLockGuard guard(thread_conf_lock_);
    if (0 >= thread_num
        || thread_num >= thread_num_) {
      _OB_LOG(WARN, "invalid param, thread_num=%ld thread_num_=%ld",
                thread_num, thread_num_);
      ret = OB_INVALID_ARGUMENT;
    } else {
      prev_thread_num = thread_num_;
      thread_num_ -= thread_num;
      cur_thread_num = thread_num_;
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = cur_thread_num; i < prev_thread_num; i++) {
      balance_filter_.sub_thread(1);
      ThreadConf &tc = thread_conf_array_[i];
      tc.run_flag = false;
      tc.stop_flag = true;
      tc.queue_cond.signal();
      ob_pthread_join(tc.pd);
      tc.high_prio_task_queue.destroy();
      tc.spec_task_queue.destroy();
      tc.comm_task_queue.destroy();
      tc.low_prio_task_queue.destroy();
      _OB_LOG(INFO, "join thread succ, index=%ld", i);
    }
  }
  return ret;
}

void *S2MQueueThread::rebalance_(int64_t &idx, const ThreadConf &cur_thread)
{
  void *ret = NULL;
  // Note: debug only
  RLOCAL(int64_t, rebalance_counter);
  for (uint64_t i = 1; (int64_t)i <= thread_num_; i++) {
    RDLockGuard guard(thread_conf_lock_);
    uint64_t balance_idx = (cur_thread.index + i) % thread_num_;
    ThreadConf &tc = thread_conf_array_[balance_idx];
    int64_t try_queue_idx = -1;
    if (tc.using_flag
       ) { //&& (tc.last_active_time + THREAD_BUSY_TIME_LIMIT) < ::oceanbase::common::ObTimeUtility::current_time())
      continue;
    }
    if (NULL == ret) {
      try_queue_idx = HIGH_PRIO_QUEUE;
      IGNORE_RETURN tc.high_prio_task_queue.pop(ret);
    }
    if (NULL == ret) {
      try_queue_idx = NORMAL_PRIO_QUEUE;
      IGNORE_RETURN tc.comm_task_queue.pop(ret);
    }
    if (NULL != ret) {
      idx = try_queue_idx;
      each_queue_len_[idx].inc(-1);
      if (0 == (rebalance_counter++ % 10000)) {
        _OB_LOG(INFO,
                  "task has been rebalance between threads rebalance_counter=%ld cur_idx=%ld balance_idx=%ld",
                  *(&rebalance_counter), cur_thread.index, balance_idx);
      }
      break;
    }
  }
  return ret;
}

int S2MQueueThread::launch_thread_(const int64_t thread_num, const int64_t task_num_limit)
{
  int ret = OB_SUCCESS;
  int64_t thread_num2launch = std::min(MAX_THREAD_NUM - thread_num_, thread_num);
  for (int64_t i = 0; OB_SUCC(ret) && i < thread_num2launch; i++) {
    ThreadConf &tc = thread_conf_array_[thread_num_];
    tc.index = thread_num_;
    tc.run_flag = true;
    tc.stop_flag = false;
    tc.using_flag = false;
    tc.last_active_time = 0;
    tc.host = this;
    if (OB_SUCCESS != (ret = tc.high_prio_task_queue.init(task_num_limit))) {
      _OB_LOG(WARN, "high prio task queue init fail, task_num_limit=%ld", task_num_limit);
      break;
    }
    if (OB_SUCCESS != (ret = tc.spec_task_queue.init(task_num_limit))) {
      _OB_LOG(WARN, "spec task queue init fail, task_num_limit=%ld", task_num_limit);
      break;
    }
    if (OB_SUCCESS != (ret = tc.comm_task_queue.init(task_num_limit))) {
      _OB_LOG(WARN, "comm task queue init fail, task_num_limit=%ld", task_num_limit);
      break;
    }
    if (OB_SUCCESS != (ret = tc.low_prio_task_queue.init(task_num_limit))) {
      _OB_LOG(WARN, "low prio task queue init fail, task_num_limit=%ld", task_num_limit);
      break;
    }
    if (OB_FAIL(ob_pthread_create(&(tc.pd), thread_func_, &tc))) {
      _OB_LOG(WARN, "ob_pthread_create fail, ret=%d", ret);
      tc.high_prio_task_queue.destroy();
      tc.spec_task_queue.destroy();
      tc.comm_task_queue.destroy();
      tc.low_prio_task_queue.destroy();
      break;
    }
    //cpu_set_t cpu_set;
    //CPU_ZERO(&cpu_set);
    //CPU_SET(thread_num_ % get_cpu_num(), &cpu_set);
    //tmp_ret = pthread_setaffinity_np(tc.pd, sizeof(cpu_set), &cpu_set);
    _OB_LOG(INFO, "create thread succ, index=%ld ret=%d", thread_num_, ret);
    thread_num_ += 1;
  }
  return ret;
}

void *S2MQueueThread::thread_func_(void *data)
{
  ThreadConf *const tc = (ThreadConf *)data;
  if (NULL == tc
      || NULL == tc->host) {
    _OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "thread_func param null pointer");
  } else {
    tc->host->thread_index() = tc->index;
    void *pdata = tc->host->on_begin();
    bool last_rebalance_got = false;
    while (tc->run_flag)
      // && (0 != tc->high_prio_task_queue.get_total()
      //     || 0 != tc->spec_task_queue.get_total()
      //     || 0 != tc->comm_task_queue.get_total()
      //     || 0 != tc->low_prio_task_queue.get_total()))
    {
      void *task = NULL;
      int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
      int64_t idx = -1;
      tc->using_flag = true;
      switch (tc->scheduler_.get()) {
        case 0:
          IGNORE_RETURN tc->high_prio_task_queue.pop(task);
          if (NULL != task) {
            idx = 0;
          }
          break;
        case 1:
          IGNORE_RETURN tc->spec_task_queue.pop(task);
          if (NULL != task) {
            idx = 1;
          }
          break;
        case 2:
          IGNORE_RETURN tc->comm_task_queue.pop(task);
          if (NULL != task) {
            idx = 2;
          }
          break;
        case 3:
          IGNORE_RETURN tc->low_prio_task_queue.pop(task);
          if (NULL != task) {
            idx = 3;
          }
          break;
        default:
          ;
      };
      if (NULL == task) {
        IGNORE_RETURN tc->high_prio_task_queue.pop(task);
        if (NULL != task) {
          idx = 0;
        }
      }
      if (NULL == task) {
        IGNORE_RETURN tc->spec_task_queue.pop(task);
        if (NULL != task) {
          idx = 1;
        }
      }
      if (NULL == task) {
        IGNORE_RETURN tc->comm_task_queue.pop(task);
        if (NULL != task) {
          idx = 2;
        }
      }
      if (NULL == task) {
        IGNORE_RETURN tc->low_prio_task_queue.pop(task);
        if (NULL != task) {
          idx = 3;
        }
      }
      tc->using_flag = false; // not need strict consist, so do not use barrier
      if (idx >= 0 && idx < QUEUE_COUNT) {
        tc->host->each_queue_len_[idx].inc(-1);
      }
      if (NULL != task
          || (tc->host->queue_rebalance_
              && (last_rebalance_got || TC_REACH_TIME_INTERVAL(QUEUE_WAIT_TIME))
              && (last_rebalance_got = (NULL != (task = tc->host->rebalance_(idx, *tc)))))) {
        tc->host->queued_num_.inc(-1);
        tc->host->handle_with_stopflag(task, pdata, tc->stop_flag);
        tc->last_active_time = ::oceanbase::common::ObTimeUtility::current_time();
      } else {
        tc->queue_cond.timedwait(QUEUE_WAIT_TIME);
      }
      tc->host->on_iter();
      v4si queue_len =
      {
        tc->high_prio_task_queue.get_total(),
        tc->spec_task_queue.get_total(),
        tc->comm_task_queue.get_total(),
        tc->low_prio_task_queue.get_total(),
      };
      tc->scheduler_.update(idx, ::oceanbase::common::ObTimeUtility::current_time() - start_time, queue_len);
    }
    tc->host->on_end(pdata);
  }
  return NULL;
}

int64_t S2MQueueThread::get_thread_index() const
{
  int64_t ret = const_cast<S2MQueueThread &>(*this).thread_index();
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

const int64_t M2SQueueThread::QUEUE_WAIT_TIME = 100 * 1000;

M2SQueueThread::M2SQueueThread() : inited_(false),
                                   pd_(nullptr),
                                   run_flag_(true),
                                   queue_cond_(),
                                   task_queue_(),
                                   idle_interval_(INT64_MAX),
                                   last_idle_time_(0)
{
}

M2SQueueThread::~M2SQueueThread()
{
  destroy();
}

int M2SQueueThread::init(const int64_t task_num_limit,
                         const int64_t idle_interval)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  run_flag_ = true;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_SUCCESS != (ret = task_queue_.init(task_num_limit))) {
    _OB_LOG(WARN, "task_queue init fail, ret=%d task_num_limit=%ld", ret, task_num_limit);
  } else if (OB_FAIL(ob_pthread_create(&pd_, thread_func_, this))) {
    _OB_LOG(WARN, "ob_pthread_create fail, ret=%d", ret);
  } else {
    inited_ = true;
    idle_interval_ = idle_interval;
    last_idle_time_ = 0;
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void M2SQueueThread::destroy()
{
  if (nullptr != pd_) {
    run_flag_ = false;
    queue_cond_.signal();
    ob_pthread_join(pd_);
    pd_ = nullptr;
  }

  task_queue_.destroy();

  inited_ = false;
}

int M2SQueueThread::push(void *task)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_SUCCESS != (ret = task_queue_.push(task))) {
      //_OB_LOG(WARN, "push task to queue fail, ret=%d", ret);
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_EAGAIN;
      }
    } else {
      queue_cond_.signal();
    }
  }
  return ret;
}

int64_t M2SQueueThread::get_queued_num() const
{
  return task_queue_.get_total();
}

void *M2SQueueThread::thread_func_(void *data)
{
  M2SQueueThread *const host = (M2SQueueThread *)data;
  if (NULL == host) {
    _OB_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "thread_func param null pointer");
  } else {
    void *pdata = host->on_begin();
    while (host->run_flag_)
      //&& 0 != host->task_queue_.get_total())
    {
      void *task = NULL;
      IGNORE_RETURN host->task_queue_.pop(task);
      if (NULL != task) {
        host->handle(task, pdata);
      } else if ((host->last_idle_time_ + host->idle_interval_) <= ::oceanbase::common::ObTimeUtility::current_time()) {
        host->on_idle();
        host->last_idle_time_ = ::oceanbase::common::ObTimeUtility::current_time();
      } else {
        host->queue_cond_.timedwait(std::min(QUEUE_WAIT_TIME, host->idle_interval_));
      }
    }
    host->on_end(pdata);
  }
  return NULL;
}


}
}
