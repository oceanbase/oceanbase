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

#include "ob_log_fetcher_err_handler.h"   // IObLogErrHandler
#include "ob_ls_worker.h"                 // IObLSWorker

using namespace oceanbase::common;

namespace oceanbase
{
namespace logfetcher
{

ObLogFetcherIdlePool::ObLogFetcherIdlePool() :
    inited_(false),
    tg_id_(-1),
    cfg_(nullptr),
    err_handler_(NULL),
    stream_worker_(NULL),
    start_lsn_locator_(NULL)
{
}

ObLogFetcherIdlePool::~ObLogFetcherIdlePool()
{
  destroy();
}

int ObLogFetcherIdlePool::init(
    const LogFetcherUser &log_fetcher_user,
    const int64_t thread_num,
    const ObLogFetcherConfig &cfg,
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
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::LogFetcherIdlePool, tg_id_))) {
    LOG_ERROR("TG_CREATE_TENANT failed", KR(ret), K(thread_num));
  } else {
    log_fetcher_user_ = log_fetcher_user;
    cfg_ = &cfg;
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
  cfg_ = nullptr;
  if (-1 != tg_id_) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }

  err_handler_ = NULL;
  stream_worker_ = NULL;
  start_lsn_locator_ = NULL;

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

    if (OB_FAIL(TG_PUSH_TASK(tg_id_, task, task->hash()))) {
      LOG_ERROR("push task into thread queue fail", KR(ret), K(task));
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
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("TG_SET_HANDLER_AND_START failed", KR(ret), K(tg_id_));
  } else {
    LOG_INFO("start fetcher idle pool succ", "thread_num", get_thread_cnt());
  }

  return ret;
}

void ObLogFetcherIdlePool::stop()
{
  if (OB_LIKELY(inited_)) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    LOG_INFO("stop fetcher idle pool succ");
  }
}

void ObLogFetcherIdlePool::mark_stop_flag()
{
  if (OB_LIKELY(inited_)) {
    LOG_INFO("mark fetcher idle pool stop");
  }
}

void ObLogFetcherIdlePool::handle(void *data, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const int64_t thread_index = get_thread_idx();
  LSFetchCtx *ls_fetch_ctx = nullptr;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogFetcherIdlePool is not inited", KR(ret));
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= get_thread_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid thread index", KR(ret), K(thread_index), K(get_thread_cnt()));
  } else if (OB_ISNULL(ls_fetch_ctx = static_cast<LSFetchCtx *>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls_fetch_ctx is nullptt", KR(ret), K(thread_index), K(get_thread_cnt()));
  } else {
    // TODO remvoe
    LOG_INFO("fetcher idle pool thread start", K(thread_index));

    if (OB_FAIL(do_request_(thread_index, *ls_fetch_ctx))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("do requests fail", KR(ret));
      }
    } else {}

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && NULL != err_handler_) {
      err_handler_->handle_error(ret, "fetcher idle pool thread exits, thread_index=%ld, err=%d",
          thread_index, ret);
    }
  }
}

int ObLogFetcherIdlePool::do_request_(const int64_t thread_index, LSFetchCtx &ls_fetch_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(stream_worker_)) {
    LOG_ERROR("invalid handlers", K(stream_worker_));
    ret = OB_INVALID_ERROR;
  } else {
    bool need_dispatch = false;

    while (OB_SUCC(ret) && ! need_dispatch) {
      if (OB_FAIL(handle_task_(&ls_fetch_ctx, need_dispatch))) {
        LOG_ERROR("handle task fail", KR(ret), K(ls_fetch_ctx));
      } else if (need_dispatch) {
        LOG_DEBUG("[STAT] [IDLE_POOL] [DISPATCH_OUT]", K(ls_fetch_ctx), K(thread_index),
            K(ls_fetch_ctx));
        const char *dispatch_reason = "SvrListReady";

        if (OB_FAIL(stream_worker_->dispatch_fetch_task(ls_fetch_ctx, dispatch_reason))) {
          LOG_ERROR("dispatch fetch task fail", KR(ret), K(ls_fetch_ctx), K(dispatch_reason));
        } else {
          // You cannot continue to operate the task afterwards
        }
      } else {
        // sleep
        ob_usleep(IDLE_WAIT_TIME);
      }
    } // while
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
  } else if (is_cdc(log_fetcher_user_) && OB_ISNULL(start_lsn_locator_)) {
    LOG_ERROR("invalid handlers", K(start_lsn_locator_));
    ret = OB_INVALID_ERROR;
  } else if (OB_UNLIKELY(task->is_discarded())) {
    // If a task is deleted, assign it directly and recycle it during the assignment process
    need_dispatch = true;
    LOG_DEBUG("[STAT] [IDLE_POOL] [RECYCLE_FETCH_TASK]", K(task), KPC(task));
  } else if (OB_ISNULL(cfg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ObLogFetcherConfig", KR(ret), K(cfg_));
  } else {
    need_dispatch = false;
    const bool enable_continue_use_cache_server_list = (1 == cfg_->enable_continue_use_cache_server_list);

    if (OB_SUCCESS == ret) {
      // Update the server list
      // Requires a successful update of the server list before leaving the idle pool
      if (task->need_update_svr_list()) {
        if (OB_FAIL(task->update_svr_list())) {
          LOG_ERROR("update server list fail", KR(ret), KPC(task));
        }
      }
      // locate the start LSN
      // Requires a successful location to leave the idle pool
      else if (task->need_locate_start_lsn()) {
        if (is_standby(log_fetcher_user_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("LS need to specifies start_lsn in the Physical standby", KR(ret), KPC(task));
        } else if (OB_FAIL(task->locate_start_lsn(*start_lsn_locator_))) {
          LOG_ERROR("locate start lsn fail", KR(ret), K(start_lsn_locator_), KPC(task));
        }
      } else if (task->need_locate_end_lsn()) {
        if (is_standby(log_fetcher_user_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("LS need to specifies start_lsn in the Physical standby", KR(ret), KPC(task));
        } else if (OB_FAIL(task->locate_end_lsn(*start_lsn_locator_))) {
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
