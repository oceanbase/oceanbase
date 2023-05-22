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

#include "ob_log_fetcher_err_handler.h"   // IObLogErrHandler
#include "ob_log_ls_fetch_mgr.h"          // IObLogLSFetchMgr
#include "ob_log_fetcher.h"               // IObLogFetcher

namespace oceanbase
{
namespace logfetcher
{

ObLogFetcherDeadPool::ObLogFetcherDeadPool() :
    inited_(false),
    tg_id_(-1),
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
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::LogFetcherDeadPool, tg_id_))) {
    LOG_ERROR("TG_CREATE_TENANT failed", KR(ret), K(thread_num));
  } else {
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
  if (-1 != tg_id_) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }

  err_handler_ = NULL;
  ls_fetch_mgr_ = NULL;

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

    LOG_TRACE("[STAT] [DEAD_POOL] [DISPATCH_IN]", K(task), KPC(task));

    if (OB_FAIL(TG_PUSH_TASK(tg_id_, task, task->hash()))) {
      LOG_ERROR("push task into thread queue fail", KR(ret), K(task));
    } else {
      // success
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
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("TG_SET_HANDLER_AND_START failed", KR(ret), K(tg_id_));
  } else {
    LOG_INFO("start fetcher dead pool succ", "thread_num", get_thread_cnt());
  }

  return ret;
}

void ObLogFetcherDeadPool::stop()
{
  if (OB_LIKELY(inited_)) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    LOG_INFO("stop fetcher dead pool succ");
  }
}

void ObLogFetcherDeadPool::mark_stop_flag()
{
  if (OB_LIKELY(inited_)) {
    LOG_INFO("mark fetcher dead pool stop");
  }
}

void ObLogFetcherDeadPool::handle(void *data, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const int64_t thread_index = get_thread_idx();
  LSFetchCtx *ls_fetch_ctx = nullptr;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= get_thread_cnt())) {
    LOG_ERROR("invalid thread index", K(thread_index), K(get_thread_cnt()));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(ls_fetch_ctx = static_cast<LSFetchCtx *>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls_fetch_ctx is nullptt", KR(ret), K(thread_index), K(get_thread_cnt()));
  } else {
    LOG_INFO("fetcher dead pool thread start", K(thread_index), "tls_id", ls_fetch_ctx->get_tls_id());

    if (OB_FAIL(handle_task_list_(thread_index, *ls_fetch_ctx, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("do requests fail", KR(ret));
      }
    } else {}

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
      err_handler_->handle_error(ret, "fetcher dead pool thread exits, thread_index=%ld, err=%d",
          thread_index, ret);
    }
  }
}

int ObLogFetcherDeadPool::handle_task_list_(
    const int64_t thread_index,
    LSFetchCtx &ls_fetch_ctx,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogFetcher *fetcher = static_cast<ObLogFetcher *>(fetcher_host_);
  bool is_removed = false;

  if (OB_ISNULL(fetcher)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("fetcher is nullptr", KR(ret), K(fetcher));
  } else {
    while (OB_SUCC(ret) && ! is_removed) {
      // Recycle tasks that are not used by asynchronous requests
      if (! ls_fetch_ctx.is_in_use()) {
        // copy TenantLSID to avoid recycle
        const logservice::TenantLSID tls_id = ls_fetch_ctx.get_tls_id();

        LOG_INFO("[STAT] [DEAD_POOL] [REMOVE_FETCH_TASK]", K(thread_index),
            K(ls_fetch_ctx));

        // First perform offline operations to ensure resource recovery and dispatch offline tasks
        if (OB_FAIL(ls_fetch_ctx.offline(stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("delete partition fail", KR(ret), K(ls_fetch_ctx));
          }
        }
        // Then physically delete the partition from the partition fetch log manager
        else if (OB_FAIL(fetcher->remove_ls_physically(tls_id.get_ls_id()))) {
          LOG_ERROR("remove partition fail", KR(ret), K(tls_id), K(ls_fetch_ctx));
        } else {
          // You can't continue operating the task afterwards
          is_removed = true;
        }
      } else {
        // sleep
        ob_usleep(IDLE_WAIT_TIME);
        // The mission is still in use
        LOG_TRACE("[STAT] [DEAD_POOL] [TASK_IN_USE]", K(ls_fetch_ctx));
      }
    } // while
  }

  return ret;
}

}
}
