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

#include "ob_dynamic_thread_pool.h"
#include "lib/thread/ob_thread_name.h"

extern "C" {
int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
void ob_pthread_join(void *ptr);
}

namespace oceanbase
{
namespace common
{

ObDynamicThreadInfo::ObDynamicThreadInfo()
  : tid_(nullptr),
    idx_(-1),
    pool_(NULL),
    is_stop_(false),
    is_alive_(false),
    error_thread_(false)
{
}



ObDynamicThreadPool::ObDynamicThreadPool()
  : is_inited_(false),
    is_stop_(false),
    thread_num_(0),
    task_queue_(),
    cond_(),
    need_idle_(true),
    start_thread_num_(0),
    stop_thread_num_(0),
    thread_name_(nullptr)
{
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    thread_infos_[i].idx_ = i;
    thread_infos_[i].pool_ = this;
  }
}

ObDynamicThreadPool::~ObDynamicThreadPool()
{
  if (!is_stop_) {
    COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "ObDynamicThreadPool is destruction before stop");
    is_stop_ = true;
  }
  destroy();
}

int ObDynamicThreadPool::init(const char* thread_name)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_FAIL(task_queue_.init(MAX_TASK_NUM))) {
    COMMON_LOG(WARN, "failed to init task queue", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::DYNAMIC_THREAD_POOL_COND_WAIT))) {
    STORAGE_LOG(WARN, "failed to init cond", K(ret));
  } else if (OB_FAIL(task_thread_cond_.init(ObWaitEventIds::DYNAMIC_THREAD_POOL_COND_WAIT))) {
    STORAGE_LOG(WARN, "failed to init cond", K(ret));
  }

  if (OB_SUCC(ret)) {
    thread_name_ = thread_name;
    is_inited_ = true;
    is_stop_ = false;
    ATOMIC_STORE(&thread_num_ , 0);

    if (OB_FAIL(start())) {
      COMMON_LOG(WARN, "failed to start dynamic thread pool", K(ret));
      is_stop_ = true;
      is_inited_ = false;
    } else {
      COMMON_LOG(INFO, "succeed to init dynamic thread pool", K(ret));
    }
  }
  return ret;
}

int ObDynamicThreadPool::set_task_thread_num(const int64_t thread_num)
{
  int ret = OB_SUCCESS;
  const int64_t cur_thread_num = ATOMIC_LOAD(&thread_num_);

  if (thread_num < 0 || thread_num > MAX_THREAD_NUM) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid thread num", K(ret), K(thread_num));
  } else if (cur_thread_num != thread_num) {
    COMMON_LOG(INFO, "set dynamic thread pool thread num", K(cur_thread_num), K(thread_num));
    ATOMIC_STORE(&thread_num_ , thread_num);
    wakeup();
  }
  return ret;
}

void ObDynamicThreadPool::destroy()
{
  is_stop_ = true;
  if (is_inited_) {
    stop();
    wait();
    is_inited_ = false;
    COMMON_LOG(INFO, "thread pool is destroyed", K(*this));
  }
}

void ObDynamicThreadPool::stop()
{
  is_stop_ = true;
  wakeup();
}

void ObDynamicThreadPool::wakeup()
{
  ObThreadCondGuard guard(cond_);
  need_idle_ = false;
  cond_.broadcast();
}

void ObDynamicThreadPool::task_thread_idle()
{
  int tmp_ret = OB_SUCCESS;
  ObThreadCondGuard guard(task_thread_cond_);
  if (OB_SUCCESS != (tmp_ret = task_thread_cond_.wait(DEFAULT_CHECK_TIME_MS))) {
    if (OB_TIMEOUT != tmp_ret) {
      STORAGE_LOG_RET(WARN, tmp_ret, "failed to idle", K(tmp_ret));
    }
  }
}

void ObDynamicThreadPool::run1()
{
  int tmp_ret = OB_SUCCESS;
  const uint64_t idx = get_thread_idx();
  if (OB_NOT_NULL(thread_name_)) {
    lib::set_thread_name(thread_name_, idx);
  }
  while (!is_stop_) {
    if (OB_SUCCESS != (tmp_ret = check_thread_status())) {
      COMMON_LOG_RET(WARN, tmp_ret, "failed to check_thread_status", K(tmp_ret));
    }
    ObThreadCondGuard guard(cond_);
    if (need_idle_) {
      if (OB_SUCCESS != (tmp_ret = cond_.wait(DEFAULT_CHECK_TIME_MS))) {
        if (OB_TIMEOUT != tmp_ret) {
          STORAGE_LOG_RET(WARN, tmp_ret, "failed to idle", K(tmp_ret));
        }
      }
    }
    need_idle_ = true;
  }
  if (OB_SUCCESS != (tmp_ret = stop_all_threads())) {
    COMMON_LOG_RET(WARN, tmp_ret, "failed to stop all threads", K(tmp_ret));
  }
}

int ObDynamicThreadPool::check_thread_status()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t thread_num = ATOMIC_LOAD(&thread_num_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_THREAD_NUM; ++i) {
    ObDynamicThreadInfo &thread_info = thread_infos_[i];
    if (i < thread_num) {
      if (!thread_info.is_alive_) {
        if (OB_SUCCESS != (tmp_ret = start_thread(thread_info))) {
          COMMON_LOG(WARN, "failed to start thread", K(tmp_ret));
        }
      }
    } else {
      if (thread_info.is_alive_) {
        if (OB_SUCCESS != (tmp_ret = stop_thread(thread_info))) {
          COMMON_LOG(WARN, "failed to start thread", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObDynamicThreadPool::stop_all_threads()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();

  COMMON_LOG(INFO, "start stop all thread", KP(this));
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_THREAD_NUM; ++i) {
    ObDynamicThreadInfo &thread_info = thread_infos_[i];
    thread_info.is_stop_ = true;
  }
  task_thread_cond_.broadcast();

  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_THREAD_NUM; ++i) {
    ObDynamicThreadInfo &thread_info = thread_infos_[i];
    if (thread_info.is_alive_) {
      if (OB_SUCCESS != (tmp_ret = stop_thread(thread_info))) {
        COMMON_LOG(WARN, "failed to start thread", K(tmp_ret));
        ret = OB_SUCC(ret)? tmp_ret : ret;
      }
    }
  }

  int64_t cost_ts = ObTimeUtility::current_time() - start_ts;

  COMMON_LOG(INFO, "finish stop all thread", KP(this), K(cost_ts));
  return ret;
}

int ObDynamicThreadPool::start_thread(ObDynamicThreadInfo &thread_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (thread_info.is_alive_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "thread is alive, cannot start twice", K(ret), K(thread_info));
  } else if (thread_info.error_thread_) {
    ret = OB_ERR_SYS;
    COMMON_LOG(ERROR, "this thread failed during startup, cannot start again", K(ret), K(thread_info));
  } else {
    thread_info.is_stop_ = false;
    if (OB_FAIL(ob_pthread_create(&thread_info.tid_, task_thread_func, &thread_info))) {
      thread_info.error_thread_ = true;
      COMMON_LOG(ERROR, "failed to create thread", K(ret), K(thread_info));
    } else {
      thread_info.is_alive_ = true;
      ++start_thread_num_;
      COMMON_LOG(INFO, "succeed to start thread", K(thread_info));
    }
  }

  return ret;
}

int ObDynamicThreadPool::stop_thread(ObDynamicThreadInfo &thread_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!thread_info.is_alive_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "thread is not alive, cannot stop", K(ret), K(thread_info));
  } else {
    thread_info.is_stop_ = true;
    ob_pthread_join(thread_info.tid_);
    thread_info.is_alive_ = false;
    ++stop_thread_num_;
    COMMON_LOG(INFO, "succeed to stop thread", K(thread_info));
  }

  return ret;
}

int ObDynamicThreadPool::pop_task(ObDynamicThreadTask *&task)
{
  int ret = OB_SUCCESS;
  task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (is_stop_) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_FAIL(task_queue_.pop(task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "failed to pop task", K(ret));
    }
  }

  return ret;
}

int ObDynamicThreadPool::add_task(ObDynamicThreadTask *task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (is_stop_) {
    ret = OB_IN_STOP_STATE;
    COMMON_LOG(WARN, "thread pool is stopped", K(ret));
  } else if (OB_FAIL(task_queue_.push(task))) {
    COMMON_LOG(WARN, "failed to push task", K(ret));
  } else {
    ObThreadCondGuard guard(task_thread_cond_);
    task_thread_cond_.signal();
  }
  return ret;
}

void *ObDynamicThreadPool::task_thread_func(void *data)
{
  int tmp_ret = OB_SUCCESS;

  if (NULL == data) {
    tmp_ret = OB_ERR_SYS;
    COMMON_LOG_RET(ERROR, tmp_ret, "data must not null", K(tmp_ret), K(data));
  } else {
    ObDynamicThreadInfo *thread_info = reinterpret_cast<ObDynamicThreadInfo *>(data);
    ObDynamicThreadTask *task = NULL;

    while (!thread_info->is_stop_) {
      task = NULL;
      if (NULL != thread_info->pool_) {
        if (OB_SUCCESS != (tmp_ret = thread_info->pool_->pop_task(task))) {
          if (OB_ENTRY_NOT_EXIST == tmp_ret) {
            thread_info->pool_->task_thread_idle();
          } else if (OB_IN_STOP_STATE == tmp_ret) {
            break;
          } else {
            COMMON_LOG_RET(WARN, tmp_ret, "failed to pop task", K(tmp_ret));
          }
        } else if (OB_SUCCESS != (tmp_ret = task->process(thread_info->is_stop_))) {
          COMMON_LOG_RET(WARN, tmp_ret, "failed to process task", K(tmp_ret), K(*thread_info));
        }
      }
    }
    COMMON_LOG(INFO, "task thread exits", K(*thread_info));
  }
  return NULL;
}

}
}
