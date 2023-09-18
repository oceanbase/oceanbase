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
 * Fetcher IDLE Pool
 * For processing partition tasks in IDLE status
 */

#define USING_LOG_PREFIX  OBLOG_FETCHER

#include "ob_log_fetcher_idle_pool.h"

#include "lib/oblog/ob_log_module.h"      // LOG_ERROR

#include "ob_log_instance.h"              // IObLogErrHandler
#include "ob_ls_worker.h"                 // IObLSWorker
#include "ob_log_trace_id.h"              // ObLogTraceIdGuard

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

ObLogFetcherIdlePool::ObLogFetcherIdlePool() :
    inited_(false),
    err_handler_(NULL),
    stream_worker_(NULL),
    start_lsn_locator_(NULL)
{
}

ObLogFetcherIdlePool::~ObLogFetcherIdlePool()
{
  destroy();
}

int ObLogFetcherIdlePool::init(const int64_t thread_num,
    IObLogErrHandler &err_handler,
    IObLSWorker &stream_worker,
    IObLogStartLSNLocator &start_lsn_locator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0) || OB_UNLIKELY(thread_num > MAX_THREAD_NUM)) {
    LOG_ERROR("invalid argument", K(thread_num));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(IdlePoolThread::init(thread_num, ObModIds::OB_LOG_FETCHER_IDLE_POOL))) {
    LOG_ERROR("init thread fail", KR(ret), K(thread_num));
  } else {
    reset_task_list_array_();

    err_handler_ = &err_handler;
    stream_worker_ = &stream_worker;
    start_lsn_locator_ = &start_lsn_locator;

    inited_ = true;

    LOG_INFO("init fetcher idle pool succ", K(thread_num), K(this));
  }
  return ret;
}

void ObLogFetcherIdlePool::destroy()
{
  stop();

  inited_ = false;
  IdlePoolThread::destroy();
  err_handler_ = NULL;
  stream_worker_ = NULL;
  start_lsn_locator_ = NULL;

  // TODO: Recycle all LSFetchCtx
  reset_task_list_array_();

  LOG_INFO("destroy fetcher idle pool succ");
}

int ObLogFetcherIdlePool::push(LSFetchCtx *task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument");
    ret = OB_INVALID_ARGUMENT;
  } else {
    task->dispatch_in_idle_pool();

    LOG_DEBUG("[STAT] [IDLE_POOL] [DISPATCH_IN]", K(task), KPC(task));

    if (OB_FAIL(IdlePoolThread::push(task, task->hash()))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push task fail", KR(ret), K(task), K(task->hash()));
      }
    } else {
      // success
    }
  }
  return ret;
}

int ObLogFetcherIdlePool::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(IdlePoolThread::start())) {
    LOG_ERROR("start thread fail", KR(ret));
  } else {
    LOG_INFO("start fetcher idle pool succ", "thread_num", get_thread_num());
  }
  return ret;
}

void ObLogFetcherIdlePool::stop()
{
  if (OB_LIKELY(inited_)) {
    IdlePoolThread::stop();
    LOG_INFO("stop fetcher idle pool succ");
  }
}

void ObLogFetcherIdlePool::mark_stop_flag()
{
  if (OB_LIKELY(inited_)) {
    IdlePoolThread::mark_stop_flag();
    LOG_INFO("mark fetcher idle pool stop");
  }
}

void ObLogFetcherIdlePool::run(const int64_t thread_index)
{
  int ret = OB_SUCCESS;
  ObLogTraceIdGuard trace_guard;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= get_thread_num())) {
    LOG_ERROR("invalid thread index", K(thread_index), K(get_thread_num()));
    ret = OB_ERR_UNEXPECTED;
  } else {
    LOG_INFO("fetcher idle pool thread start", K(thread_index));

    FetchTaskList &task_list = task_list_array_[thread_index];

    while (! stop_flag_ && OB_SUCCESS == ret) {
      if (OB_FAIL(do_retrieve_(thread_index, task_list))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("do retrieve new request fail", KR(ret));
        }
      } else if (OB_FAIL(do_request_(thread_index, task_list))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("do requests fail", KR(ret));
        }
      } else {
        // Wait for a fixed time or until a new task arrives
        cond_timedwait(thread_index, IDLE_WAIT_TIME);
      }
    }

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
      err_handler_->handle_error(ret, "fetcher idle pool thread exits, thread_index=%ld, err=%d",
          thread_index, ret);

      IdlePoolThread::mark_stop_flag();
    }

    LOG_INFO("fetcher idle pool thread exits", K(thread_index), KR(ret));
  }
}

void ObLogFetcherIdlePool::reset_task_list_array_()
{
  for (int64_t idx = 0; idx < MAX_THREAD_NUM; idx++) {
    task_list_array_[idx].reset();
  }
}

int ObLogFetcherIdlePool::do_retrieve_(const int64_t thread_index, FetchTaskList &list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else {
    while (! stop_flag_ && OB_SUCCESS == ret) {
      void *data = NULL;
      LSFetchCtx *task = NULL;

      if (OB_FAIL(pop(thread_index, data))) {
        if (OB_EAGAIN == ret) {
          // 无数据
        } else {
          LOG_ERROR("pop task from queue fail", KR(ret), K(thread_index));
        }
      } else if (OB_ISNULL(task = static_cast<LSFetchCtx *>(data))) {
        LOG_ERROR("task is NULL", K(task), K(data));
        ret = OB_ERR_UNEXPECTED;
      } else {
        list.add_head(*task);

        // Successfully acquired a task
        LOG_DEBUG("[STAT] [IDLE_POOL] [RETRIEVE]", K(task), K(thread_index),
            "count", list.count(), KPC(task));
      }
    }

    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    }

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }
  }
  return ret;
}

int ObLogFetcherIdlePool::do_request_(const int64_t thread_index, FetchTaskList &list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid handlers", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  } else {
    LSFetchCtx *task = list.head();

    while (OB_SUCCESS == ret && NULL != task) {
      LSFetchCtx *next = task->get_next();
      bool need_dispatch = false;

      if (OB_FAIL(handle_task_(task, need_dispatch))) {
        LOG_ERROR("handle task fail", KR(ret), K(task), KPC(task));
      } else if (need_dispatch) {
        // If it needs to be assigned to another thread, remove it from the linklist and then perform the assignment
        list.erase(*task);

        LOG_DEBUG("[STAT] [IDLE_POOL] [DISPATCH_OUT]", K(task), K(thread_index),
            "count", list.count(), KPC(task));

        const char *dispatch_reason = "SvrListReady";
        if (OB_FAIL(stream_worker_->dispatch_fetch_task(*task, dispatch_reason))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("dispatch fetch task fail", KR(ret), KPC(task), K(dispatch_reason));
          }
        } else {
          // You cannot continue to operate the task afterwards
        }
      }

      if (OB_SUCCESS == ret) {
        task = next;
      }
    }
  }
  return ret;
}

int ObLogFetcherIdlePool::handle_task_(LSFetchCtx *task, bool &need_dispatch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(start_lsn_locator_)) {
    LOG_ERROR("invalid handlers", K(start_lsn_locator_));
    ret = OB_INVALID_ERROR;
  } else if (OB_UNLIKELY(task->is_discarded())) {
    // If a task is deleted, assign it directly and recycle it during the assignment process
    need_dispatch = true;
    LOG_DEBUG("[STAT] [IDLE_POOL] [RECYCLE_FETCH_TASK]", K(task), KPC(task));
  } else {
    need_dispatch = false;
    const bool enable_continue_use_cache_server_list = (1 == TCONF.enable_continue_use_cache_server_list);

    if (OB_SUCCESS == ret) {
      // Update the server list
      // Requires a successful update of the server list before leaving the idle pool
      if (task->need_update_svr_list()) {
        if (OB_FAIL(task->update_svr_list())) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("update server list fail", KR(ret), KPC(task));
          }
        }
      }
      // locate the start LSN
      // Requires a successful location to leave the idle pool
      else if (task->need_locate_start_lsn()) {
        if (OB_FAIL(task->locate_start_lsn(*start_lsn_locator_))) {
          LOG_ERROR("locate start lsn fail", KR(ret), K(start_lsn_locator_), KPC(task));
        }
      } else if (task->need_locate_end_lsn()) {
        if (OB_FAIL(task->locate_end_lsn(*start_lsn_locator_))) {
          LOG_ERROR("locate end lsn fail", KR(ret), K(start_lsn_locator_), KPC(task));
        }
      } else {
        // After all the above conditions are met, allow distribution to the fetch log stream
        need_dispatch = true;
      }
    }

    if (enable_continue_use_cache_server_list) {
      need_dispatch = true;
      LOG_DEBUG("enable_continue_use_cache_server_list", KPC(task), K(need_dispatch));
    }
  }
  return ret;
}


}
}
