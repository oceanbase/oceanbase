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
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/thread/ob_dynamic_thread_pool.h"

namespace oceanbase
{
namespace common
{
template <class T>
ObSimpleThreadPoolBase<T>::ObSimpleThreadPoolBase()
    : ObSimpleDynamicThreadPool(),
      name_("unknown"), is_inited_(false)
{
}

template <class T>
ObSimpleThreadPoolBase<T>::~ObSimpleThreadPoolBase()
{
  if (is_inited_) {
    destroy();
  }
}

template <class T>
int ObSimpleThreadPoolBase<T>::init(const int64_t thread_num, const int64_t task_num_limit, const char* name, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (thread_num <= 0 || task_num_limit <= 0 || thread_num > MAX_THREAD_NUM || OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(queue_.init(task_num_limit, name, tenant_id))) {
    COMMON_LOG(WARN, "task queue init failed", K(ret), K(task_num_limit));
  } else {
    is_inited_ = true;
    name_ = name;
    if (OB_FAIL(ObSimpleDynamicThreadPool::init(thread_num, name))) {
      COMMON_LOG(WARN, "dyna,ic thread pool init fail", K(ret));
    } else if (OB_FAIL(ObSimpleDynamicThreadPool::start())) {
      COMMON_LOG(WARN, "start thread pool fail", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  } else {
    COMMON_LOG(INFO, "simple thread pool init success", KCSTRING(name), K(thread_num), K(task_num_limit));
  }
  return ret;
}

template <class T>
void ObSimpleThreadPoolBase<T>::destroy()
{
  is_inited_ = false;
  ObSimpleDynamicThreadPool::destroy();
  queue_.destroy();
}

template <class T>
int ObSimpleThreadPoolBase<T>::push(void *task)
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
    try_expand_thread_count();
  }
  return ret;
}

template <class T>
int ObSimpleThreadPoolBase<T>::push(ObLink *task, const int priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else if (has_set_stop()) {
    ret = OB_IN_STOP_STATE;
  } else {
    ret = queue_.push(task, priority);
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_EAGAIN;
    }
    try_expand_thread_count();
  }
  return ret;
}

template <class T>
void ObSimpleThreadPoolBase<T>::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_idx = get_thread_idx();
  if (NULL != name_) {
    lib::set_thread_name(name_, thread_idx);
  }
  while (!has_set_stop() && !(OB_NOT_NULL(&lib::Thread::current()) ? lib::Thread::current().has_set_stop() : false)) {
    void *task = NULL;
    const int64_t curr_thread_num = get_thread_count();
    int64_t pop_before_ts = ObTimeUtility::current_time();
    if (OB_SUCC(queue_.pop(task, QUEUE_WAIT_TIME))) {
      IGNORE_RETURN inc_thread_idle_time(ObTimeUtility::current_time() - pop_before_ts);
      int64_t running_thread_cnt = inc_running_thread_cnt(1);
      if (running_thread_cnt == curr_thread_num
          && get_queue_num() > 0) {
        try_inc_thread_count(1);
      }
      handle(task);
      IGNORE_RETURN inc_running_thread_cnt(-1);
    } else {
      IGNORE_RETURN inc_thread_idle_time(ObTimeUtility::current_time() - pop_before_ts);
    }
  }
  if (has_set_stop()) {
    void *task = NULL;
    while (OB_SUCC(queue_.pop(task))) {
      handle_drop(task);
    }
  }
}

} // namespace common
} // namespace oceanbase
