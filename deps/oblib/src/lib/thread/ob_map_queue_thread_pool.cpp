/**
 * Copyright (c) 2023 OceanBase
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
#include "lib/oblog/ob_log_module.h"
#include "lib/thread/ob_map_queue_thread_pool.h"

namespace oceanbase
{
namespace common
{
ObMapQueueThreadPool::ObMapQueueThreadPool() :
    is_inited_(false),
    name_(nullptr)
{
}

ObMapQueueThreadPool::~ObMapQueueThreadPool()
{
  if (is_inited_) {
    destroy();
  }
}

int ObMapQueueThreadPool::init(const uint64_t tenant_id, const int64_t thread_num, const char *label)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LIB_LOG(ERROR, "init twice", K(ret));
  } else if (OB_UNLIKELY(0 >= thread_num) || OB_UNLIKELY(thread_num > MAX_THREAD_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid argument", K(ret), K(thread_num));
  } else {
    set_thread_count(thread_num);
    name_ = label;

    for (int64_t index = 0; OB_SUCC(ret) && index < thread_num; index++) {
      if (OB_FAIL(tc_[index].init(label, index, this))) {
        LIB_LOG(ERROR, "init queue fail", K(ret), K(index), K(label));
      }
    }

    if (OB_FAIL(ret)) {
      destroy();
    } else {
      is_inited_ = true;
      LIB_LOG(INFO, "ObMapQueueThreadPool init success", K(tenant_id), K(thread_num));
    }
  }

  return ret;
}

void ObMapQueueThreadPool::destroy()
{
  stop();

  for (int64_t index = 0; index < get_thread_count(); index++) {
    tc_[index].destroy();
  }
  name_ = nullptr;
  is_inited_ = false;
}

int ObMapQueueThreadPool::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "not inited", K(ret));
  } else {
    if (OB_FAIL(lib::ThreadPool::start())) {
      LIB_LOG(ERROR, "ThreadPool start failed", K(ret));
    }
  }

  return ret;
}

void ObMapQueueThreadPool::stop()
{
  if (is_inited_) {
    lib::ThreadPool::stop();
    lib::ThreadPool::wait();
  }
}

void ObMapQueueThreadPool::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_index = get_thread_idx();
  if (nullptr != name_) {
    lib::set_thread_name(name_, thread_index);
  }

  if (OB_UNLIKELY(! is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "ObMapQueueThreadPool is not inited", K(ret));
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= get_thread_count())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "invalid thread index", K(ret), K(thread_index), "thread_num", get_thread_count());
  } else {
    while (! has_set_stop() && OB_SUCC(ret)) {
      void *task = NULL;

      if (OB_FAIL(next_task_(thread_index, task))) {
        if (OB_IN_STOP_STATE != ret) {
          LIB_LOG(ERROR, "next_task_ fail", K(ret), K(thread_index));
        }
      } else {
        handle(task, has_set_stop());
      }
    }
  }
}

int ObMapQueueThreadPool::pop(const int64_t thread_index, void *&data)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "not init", K(ret));
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= get_thread_count())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "invalid thread index", K(ret), K(thread_index), "thread_num", get_thread_count());
  } else {
    ret = tc_[thread_index].queue_.pop(data);
  }

  return ret;
}

void ObMapQueueThreadPool::cond_timedwait(
    const int64_t thread_index,
    const int64_t wait_time)
{
  if (OB_UNLIKELY(! is_inited_)) {
    LIB_LOG_RET(ERROR, OB_NOT_INIT, "not init");
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= get_thread_count())) {
    LIB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid thread index", K(thread_index),
        "thread_num", get_thread_count());
  } else {
    tc_[thread_index].cond_.timedwait(wait_time);
  }
}

int ObMapQueueThreadPool::next_task_(const int64_t index, void *&task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(index < 0) || OB_UNLIKELY(index >= get_thread_count())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "invalid thread index", K(ret), K(index), "thread_num", get_thread_count());
  } else {
    ThreadConf &tc = tc_[index];
    while (! has_set_stop() && OB_SUCC(ret)) {
      task = NULL;

      if (OB_FAIL(tc.queue_.pop(task))) {
        if (OB_EAGAIN == ret) {
          // empty queue
          ret = OB_SUCCESS;
          tc.cond_.timedwait(DATA_OP_TIMEOUT);
        } else {
          LIB_LOG(ERROR, "pop task from queue fail", K(ret));
        }
      } else if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(ERROR, "pop invalid task", K(ret), K(task));
      } else {
        break;
      }
    }

    if (has_set_stop()) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

int ObMapQueueThreadPool::push(void *data, const uint64_t hash_val)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! is_inited_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(ERROR, "not init", K(ret));
  } else if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid argument", K(ret), K(data));
  } else {
    const int64_t target_index = static_cast<int64_t>(hash_val % get_thread_count());
    ThreadConf &tc = tc_[target_index];

    if (OB_FAIL(tc.queue_.push(data))) {
      LIB_LOG(ERROR, "push data fail", K(ret), K(data), K(target_index));
    } else {
      tc.cond_.signal();
    }
  }

  return ret;
}

///////////////////////////////////////////// ThreadConf /////////////////////////////////////////////

ObMapQueueThreadPool::ThreadConf::ThreadConf() :
    host_(NULL),
    thread_index_(0),
    queue_(),
    cond_()
{}

ObMapQueueThreadPool::ThreadConf::~ThreadConf()
{
  destroy();
}

int ObMapQueueThreadPool::ThreadConf::init(
    const char *label,
    const int64_t thread_index,
    HostType *host)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(thread_index < 0) || OB_ISNULL(host)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid argument", K(ret), K(thread_index), K(host));
  } else if (OB_FAIL(queue_.init(label))) {
    LIB_LOG(ERROR, "init queue fail", K(ret), K(label));
  } else {
    host_ = host;
    thread_index_ = thread_index;
  }

  return ret;
}

void ObMapQueueThreadPool::ThreadConf::destroy()
{
  host_ = NULL;
  thread_index_ = 0;
  queue_.destroy();
}

} // namespace common
} // namespace oceanbase
