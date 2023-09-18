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
 * Fetcher DEAD Pool: For processing fetch log tasks that are in the process of being deleted
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_fetcher_dead_pool.h"

#include "lib/oblog/ob_log_module.h"      // LOG_ERROR
#include "lib/allocator/ob_mod_define.h"  // ObModIds

#include "ob_log_instance.h"              // IObLogErrHandler
#include "ob_log_ls_fetch_mgr.h"          // IObLogLSFetchMgr
#include "ob_log_fetcher.h"               // IObLogFetcher
#include "ob_log_trace_id.h"              // ObLogTraceIdGuard

namespace oceanbase
{
namespace libobcdc
{

ObLogFetcherDeadPool::ObLogFetcherDeadPool() :
    inited_(false),
    err_handler_(NULL),
    ls_fetch_mgr_(NULL)
{
}

ObLogFetcherDeadPool::~ObLogFetcherDeadPool()
{
  destroy();
}


int ObLogFetcherDeadPool::init(const int64_t thread_num,
    void *fetcher_host,
    IObLogLSFetchMgr &ls_fetch_mgr,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0) || OB_UNLIKELY(thread_num > MAX_THREAD_NUM)) {
    LOG_ERROR("invalid argument", K(thread_num));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(DeadPoolThread::init(thread_num, ObModIds::OB_LOG_FETCHER_DEAD_POOL))) {
    LOG_ERROR("init thread fail", KR(ret), K(thread_num));
  } else {
    reset_task_list_array_();

    fetcher_host_ = fetcher_host;
    err_handler_ = &err_handler;
    ls_fetch_mgr_ = &ls_fetch_mgr;
    inited_ = true;

    LOG_INFO("init fetcher dead pool succ", K(thread_num), K(this));
  }
  return ret;
}

void ObLogFetcherDeadPool::destroy()
{
  stop();

  inited_ = false;
  DeadPoolThread::destroy();
  err_handler_ = NULL;
  ls_fetch_mgr_ = NULL;
  reset_task_list_array_();

  LOG_INFO("destroy fetcher dead pool succ");
}

int ObLogFetcherDeadPool::push(LSFetchCtx *task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid argument", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! task->is_discarded())) {
    LOG_ERROR("invalid task which is not discarded", K(task), KPC(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    task->dispatch_in_dead_pool();

    LOG_DEBUG("[STAT] [DEAD_POOL] [DISPATCH_IN]", K(task), KPC(task));

    if (OB_FAIL(DeadPoolThread::push(task, task->hash()))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push task fail", KR(ret), K(task), K(task->hash()));
      }
    } else {
      // 成功
    }
  }
  return ret;
}

int ObLogFetcherDeadPool::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(DeadPoolThread::start())) {
    LOG_ERROR("start thread fail", KR(ret));
  } else {
    LOG_INFO("start fetcher dead pool succ", "thread_num", get_thread_num());
  }
  return ret;
}

void ObLogFetcherDeadPool::stop()
{
  if (OB_LIKELY(inited_)) {
    DeadPoolThread::stop();
    LOG_INFO("stop fetcher dead pool succ");
  }
}

void ObLogFetcherDeadPool::mark_stop_flag()
{
  if (OB_LIKELY(inited_)) {
    DeadPoolThread::mark_stop_flag();
    LOG_INFO("mark fetcher dead pool stop");
  }
}

void ObLogFetcherDeadPool::run(const int64_t thread_index)
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
    LOG_INFO("fetcher dead pool thread start", K(thread_index));

    FetchTaskList &task_list = task_list_array_[thread_index];

    while (! stop_flag_ && OB_SUCCESS == ret) {
      if (OB_FAIL(retrieve_task_list_(thread_index, task_list))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("retrieve task list fail", KR(ret));
        }
      } else if (OB_FAIL(handle_task_list_(thread_index, task_list))) {
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
      err_handler_->handle_error(ret, "fetcher dead pool thread exits, thread_index=%ld, err=%d",
          thread_index, ret);

      DeadPoolThread::mark_stop_flag();
    }

    LOG_INFO("fetcher dead pool thread exits", K(thread_index), KR(ret));
  }
}

void ObLogFetcherDeadPool::reset_task_list_array_()
{
  for (int64_t idx = 0; idx < MAX_THREAD_NUM; idx++) {
    task_list_array_[idx].reset();
  }
}

int ObLogFetcherDeadPool::retrieve_task_list_(const int64_t thread_index, FetchTaskList &list)
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
        LOG_DEBUG("[STAT] [DEAD_POOL] [RETRIEVE]", K(task), K(thread_index),
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

int ObLogFetcherDeadPool::handle_task_list_(const int64_t thread_index, FetchTaskList &list)
{
  int ret = OB_SUCCESS;
  LSFetchCtx *task = list.head();
  IObLogFetcher *fetcher = static_cast<IObLogFetcher *>(fetcher_host_);

  if (OB_ISNULL(fetcher)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("fetcher is nullptr", KR(ret), K(fetcher));
  } else {
    while (OB_SUCCESS == ret && NULL != task) {
      LSFetchCtx *next = task->get_next();

      // Recycle tasks that are not used by asynchronous requests
      if (! task->is_in_use()) {
        const logservice::TenantLSID &tls_id = task->get_tls_id();

        // Remove from list
        list.erase(*task);

        LOG_INFO("[STAT] [DEAD_POOL] [REMOVE_FETCH_TASK]", K(task), K(thread_index),
            KPC(task), K(list));

        // First perform offline operations to ensure resource recovery and dispatch offline tasks
        if (OB_FAIL(task->offline(stop_flag_))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("delete partition fail", KR(ret), K(task), KPC(task));
          }
        }
        // Then physically delete the partition from the partition fetch log manager
        else if (OB_FAIL(fetcher->remove_ls(tls_id))) {
          LOG_ERROR("remove partition fail", KR(ret), K(tls_id), K(task));
        } else {
          // You can't continue operating the task afterwards
          task = NULL;
        }
      } else {
        // The mission is still in use
        LOG_DEBUG("[STAT] [DEAD_POOL] [TASK_IN_USE]", K(task), KPC(task));
      }

      task = next;
    }
  }
  return ret;
}

}
}
