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
 *
 * A simple timer based on HashMap implementation
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_timer.h"

#include "logservice/common_util/ob_log_time_utils.h"
#include "ob_log_fetcher_err_handler.h"          // IObLogErrHandler
#include "ob_log_config.h"            // ObLogFetcherConfig

using namespace oceanbase::common;
namespace oceanbase
{
namespace logfetcher
{

int64_t ObLogFixedTimer::g_wait_time = ObLogFetcherConfig::default_timer_task_wait_time_msec * _MSEC_;

ObLogFixedTimer::ObLogFixedTimer() :
    inited_(false),
    tid_(0),
    err_handler_(NULL),
    task_queue_(),
    task_cond_(),
    allocator_(),
    stop_flag_(true)
{}

ObLogFixedTimer::~ObLogFixedTimer()
{
  destroy();
}

int ObLogFixedTimer::init(IObLogErrHandler &err_handler, const int64_t max_task_count)
{
  int ret = OB_SUCCESS;
  int64_t queue_task_size = sizeof(QTask);

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(max_task_count <= 0)) {
    LOG_ERROR("invalid argument", K(max_task_count));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(task_queue_.init(max_task_count, global_default_allocator,
      ObModIds::OB_LOG_TIMER))) {
    LOG_ERROR("init task queue fail", KR(ret), K(max_task_count));
  } else if (OB_FAIL(allocator_.init(queue_task_size, ObModIds::OB_LOG_TIMER))) {
    LOG_ERROR("init allocator fail", KR(ret), K(queue_task_size));
  } else {
    tid_ = 0;
    err_handler_ = &err_handler;
    stop_flag_ = true;
    inited_ = true;

    LOG_INFO("init oblog timer succ", K(max_task_count));
  }

  return ret;
}

void ObLogFixedTimer::destroy()
{
  LOG_INFO("destroy oblog timer begin");
  stop();

  destroy_all_tasks_();

  inited_ = false;
  stop_flag_ = true;
  tid_ = 0;
  err_handler_ = NULL;
  task_queue_.destroy();
  allocator_.destroy();

  LOG_INFO("destroy oblog timer succ");
}

int ObLogFixedTimer::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
  } else if (stop_flag_) {
    stop_flag_ = false;

    int pthread_ret = pthread_create(&tid_, NULL, thread_func_, this);

    if (OB_UNLIKELY(0 != pthread_ret)) {
      LOG_ERROR("create timer thread fail", K(pthread_ret), KERRNOMSG(pthread_ret));
      ret = OB_ERR_UNEXPECTED;
    } else {
      LOG_INFO("start oblog timer succ");
    }
  }

  return ret;
}

void ObLogFixedTimer::stop()
{
  if (inited_) {
    stop_flag_ = true;

    if (0 != tid_) {
      int pthread_ret = pthread_join(tid_, NULL);
      if (0 != pthread_ret) {
        LOG_ERROR_RET(OB_ERR_SYS, "pthread_join fail", K(tid_), K(pthread_ret), KERRNOMSG(pthread_ret));
      }

      tid_ = 0;

      LOG_INFO("stop oblog timer succ");
    }
  }
}

void ObLogFixedTimer::mark_stop_flag()
{
  stop_flag_ = true;
}

void *ObLogFixedTimer::thread_func_(void *args)
{
  ObLogFixedTimer *self = static_cast<ObLogFixedTimer *>(args);

  if (NULL != self) {
    self->run();
  }

  return NULL;
}

void ObLogFixedTimer::run()
{
  int ret = OB_SUCCESS;

  LOG_INFO("oblog timer thread start");

  while (OB_SUCCESS == ret && ! stop_flag_) {
    QTask *queue_task = NULL;

    if (OB_FAIL(next_queue_task_(queue_task))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("next_queue_task_ fail", KR(ret), K(queue_task));
      }
    } else if (OB_ISNULL(queue_task)) {
      LOG_ERROR("invalid queue task", K(queue_task));
      ret = OB_ERR_UNEXPECTED;
    } else {
      queue_task->task_.process_timer_task();
      free_queue_task_(queue_task);
      queue_task = NULL;

      if (REACH_TIME_INTERVAL_THREAD_LOCAL(STAT_INTERVAL)) {
        _LOG_INFO("[STAT] [TIMER] TASK_COUNT=%ld", task_queue_.get_total());
      }
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
    err_handler_->handle_error(ret, "oblog timer thread exits, err=%d", ret);
  }

  LOG_INFO("oblog timer thread exits", KR(ret), K_(stop_flag));
}

int ObLogFixedTimer::schedule(ObLogTimerTask *task)
{
  int ret = OB_SUCCESS;
  QTask *queue_task = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init");
    ret = OB_NOT_INIT;
	} else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(queue_task = alloc_queue_task_(*task))) {
    LOG_ERROR("allocate queue task fail", K(task));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(push_queue_task_(*queue_task))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push_queue_task_ fail", KR(ret), K(queue_task));
    }
  }

  if (OB_SUCCESS != ret && NULL != queue_task) {
    free_queue_task_(queue_task);
    queue_task = NULL;
  }

  return ret;
}

void ObLogFixedTimer::configure(const ObLogFetcherConfig &config)
{
  int64_t timer_task_wait_time_msec = config.timer_task_wait_time_msec;

  ATOMIC_STORE(&g_wait_time, timer_task_wait_time_msec * _MSEC_);
  LOG_INFO("[CONFIG]", K(timer_task_wait_time_msec));
}

ObLogFixedTimer::QTask *ObLogFixedTimer::alloc_queue_task_(ObLogTimerTask &timer_task)
{
  QTask *queue_task = NULL;
  void *ptr = allocator_.alloc();

  if (NULL != ptr) {
    queue_task = new (ptr) QTask(timer_task);
  }
  return queue_task;
}

void ObLogFixedTimer::free_queue_task_(QTask *task)
{
  if (NULL != task) {
    task->~QTask();
    allocator_.free(task);
    task = NULL;
  }
}

int ObLogFixedTimer::push_queue_task_(QTask &task)
{
  int ret = OB_SUCCESS;

  while (OB_SIZE_OVERFLOW == (ret = task_queue_.push(&task)) && ! stop_flag_) {
    task_cond_.timedwait(COND_WAIT_TIME);
  }

  if (OB_SUCCESS != ret && stop_flag_) {
    ret = OB_IN_STOP_STATE;
  }

  if (OB_SUCCESS == ret) {
    task_cond_.signal();
  }

  return ret;
}

int ObLogFixedTimer::next_queue_task_(QTask *&task)
{
  int ret = OB_SUCCESS;
  task = NULL;

  while (OB_ENTRY_NOT_EXIST == (ret = task_queue_.pop(task)) && ! stop_flag_) {
    task_cond_.timedwait(COND_WAIT_TIME);
  }

  if (OB_SUCCESS != ret && stop_flag_) {
    ret = OB_IN_STOP_STATE;
  }

  if (OB_SUCCESS == ret) {
    task_cond_.signal();

    if (OB_ISNULL(task)) {
      LOG_ERROR("invalid task popped from queue", K(task));
      ret = OB_ERR_UNEXPECTED;
    } else {
      int64_t cur_time = get_timestamp();
      int64_t delay = task->out_timestamp_ - cur_time;

      // Assuming the delay is not too long, otherwise the thread would stuck here
      if (delay > 0) {
        usec_sleep(delay);
      }
    }
  }

  return ret;
}

void ObLogFixedTimer::destroy_all_tasks_()
{
  int ret = OB_SUCCESS;
  QTask *task = NULL;

  while (OB_SUCC(task_queue_.pop(task))) {
    if (OB_NOT_NULL(task)) {
      free_queue_task_(task);
      task = NULL;
    }
  }
}

///////////////////////////////////// QTask ///////////////////////////////////////

ObLogFixedTimer::QTask::QTask(ObLogTimerTask &task) : task_(task)
{
  int64_t wait_time = ATOMIC_LOAD(&ObLogFixedTimer::g_wait_time);
  out_timestamp_ = get_timestamp() + wait_time;
}

}
}
