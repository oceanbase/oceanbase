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

#define USING_LOG_PREFIX SHARE

#include "lib/thread/ob_async_task_queue.h"
#include "lib/profile/ob_trace_id.h"
namespace oceanbase
{
using namespace common;
using namespace lib;
namespace share
{
ObAsyncTaskQueue::ObAsyncTaskQueue()
  : is_inited_(false),
    queue_(),
    allocator_()
{
}

ObAsyncTaskQueue::~ObAsyncTaskQueue()
{
  int ret = destroy();
  if (OB_FAIL(ret)) {
    LOG_WARN("destroy failed", K(ret));
  }
}

int ObAsyncTaskQueue::init(const int64_t thread_cnt, const int64_t queue_size, const char *thread_name, const int64_t page_size)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task queue has already been initialized", K(ret));
  } else if (thread_cnt <= 0|| queue_size <= 0 || 0 != (queue_size & (queue_size - 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(thread_cnt), K(queue_size), K(ret));
  } else if (OB_FAIL(allocator_.init(TOTAL_LIMIT, HOLD_LIMIT, page_size))) {
    LOG_WARN("allocator init failed", "total limit", static_cast<int64_t>(TOTAL_LIMIT),
        "hold limit", static_cast<int64_t>(HOLD_LIMIT),
        "page size",  static_cast<int64_t>(PAGE_SIZE), K(ret));
  } else if (OB_FAIL(queue_.init(queue_size))) {
    LOG_WARN("queue init failed", K(queue_size), K(ret));
  } else if (OB_FAIL(create(thread_cnt, thread_name))) {
    LOG_WARN("create async task thread failed", K(ret), K(thread_cnt));
  } else {
    allocator_.set_attr(SET_USE_500("AsyncTaskQueue"));
    is_inited_ = true;
  }
  return ret;
}

int ObAsyncTaskQueue::destroy()
{
  int ret = ObReentrantThread::destroy();
  if (OB_FAIL(ret)) {
    LOG_WARN("reentrant thread thread failed", K(ret));
  }
  if (is_inited_) {
    queue_.destroy();
    allocator_.destroy();
    is_inited_ = false;
  }
  return ret;
}

int ObAsyncTaskQueue::push(const ObAsyncTask &task)
{
  int ret = OB_SUCCESS;
  ObAsyncTask *task_ptr = NULL;
  const int64_t buf_size = task.get_deep_copy_size();
  char *buf = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == (buf = static_cast<char *>(allocator_.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocator alloc memory failed", K(buf_size), K(ret));
  } else if (NULL == (task_ptr = task.deep_copy(buf, buf_size))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task deep copy failed", K(ret));
    allocator_.free(buf);
    buf = NULL;
  } else {
    task_ptr->set_retry_times(task.get_retry_times());
    task_ptr->set_retry_interval(task.get_retry_interval());
    if (OB_FAIL(queue_.push(task_ptr))) {
      LOG_WARN("push task to queue failed", K(ret), "queue_size", queue_.size());
      task_ptr->~ObAsyncTask();
      allocator_.free(buf);
      buf = NULL;
    }
  }
  return ret;
}

void ObAsyncTaskQueue::run2()
{
  int ret = OB_SUCCESS;
  LOG_INFO("async task queue start");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObAddr zero_addr;
    while (!stop_) {
      IGNORE_RETURN lib::Thread::update_loop_ts(ObTimeUtility::fast_current_time());
      if (REACH_TIME_INTERVAL(600 * 1000 * 1000)) {
        //每隔一段时间，打印队列的大小
        LOG_INFO("[ASYNC TASK QUEUE]", "queue_size", queue_.size());
      }
      ObAsyncTask *task = NULL;
      ret = pop(task);
      if (OB_FAIL(ret))  {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("pop task from queue failed", K(ret));
        }
      } else if (NULL == task) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pop return a null task", K(ret));
      } else {
        bool rescheduled = false;
        if (task->get_last_execute_time() > 0) {
          while (!stop_ && OB_SUCC(ret)) {
            int64_t now = ObTimeUtility::current_time();
            int64_t sleep_time = task->get_last_execute_time() + task->get_retry_interval() - now;
            if (sleep_time > 0) {
              ::usleep(static_cast<int32_t>(MIN(sleep_time, SLEEP_INTERVAL)));
            } else {
              break;
            }
          }
        }
        // generate trace id
        ObCurTraceId::init(zero_addr);
        // just do it
        ret = task->process();
        if (OB_FAIL(ret)) {
          LOG_WARN("task process failed, start retry", "max retry time",
              task->get_retry_times(), "retry interval", task->get_retry_interval(),
              K(ret));
          if (task->get_retry_times() > 0) {
            task->set_retry_times(task->get_retry_times() - 1);
            task->set_last_execute_time(ObTimeUtility::current_time());
            if (OB_FAIL(queue_.push(task))) {
              LOG_ERROR("push task to queue failed", K(ret));
            } else {
              rescheduled = true;
            }
          }
        }
        if (!rescheduled) {
          task->~ObAsyncTask();
          allocator_.free(task);
          task = NULL;
        }
      }
    }
  }
  LOG_INFO("async task queue stop");
}
int ObAsyncTaskQueue::start()
{
  return logical_start();
}
void ObAsyncTaskQueue::stop()
{
  logical_stop();
}
void ObAsyncTaskQueue::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    logical_wait();
    ObAsyncTask *task = NULL;
    while (queue_.size() > 0 && OB_SUCCESS == pop(task)) {
      task->~ObAsyncTask();
      allocator_.free(task);
      task = NULL;
    }
  }
}

int ObAsyncTaskQueue::pop(ObAsyncTask *&task)
{
  int ret = OB_SUCCESS;
  void *vp = NULL;
  const int64_t timeout = 1000 * 1000;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret = queue_.pop(vp, timeout);
    if (OB_FAIL(ret)) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("queue pop failed", K(ret));
      }
    } else {
      task = static_cast<ObAsyncTask *>(vp);
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase
