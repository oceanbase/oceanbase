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
 * Stream Worker
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_ls_worker.h"

#include "ob_log_timer.h"               // ObLogFixedTimer
#include "ob_log_instance.h"            // IObLogErrHandler
#include "ob_log_ls_fetch_ctx.h"        // LSFetchCtx
#include "ob_log_fetcher_idle_pool.h"   // IObLogFetcherIdlePool
#include "ob_log_fetcher_dead_pool.h"   // IObLogFetcherDeadPool
#include "ob_log_fetcher.h"             // get_fs_container_mgr
#include "ob_log_trace_id.h"            // ObLogTraceIdGuard

namespace oceanbase
{
namespace libobcdc
{

// Defining class global variables
int64_t ObLSWorker::g_blacklist_survival_time =
    ObLogConfig::default_blacklist_survival_time_sec * _SEC_;

bool ObLSWorker::g_print_stream_dispatch_info =
    ObLogConfig::default_print_stream_dispatch_info;

ObLSWorker::ObLSWorker() :
    inited_(false),
    stream_paused_(false),
    fetcher_resume_time_(OB_INVALID_TIMESTAMP),
    fetcher_host_(nullptr),
    idle_pool_(NULL),
    dead_pool_(NULL),
    err_handler_(NULL),
    timer_(),
    stream_task_seq_(0)
{}

ObLSWorker::~ObLSWorker()
{
  destroy();
}

int ObLSWorker::init(const int64_t worker_thread_num,
    const int64_t max_timer_task_count,
    void *fetcher_host,
    IObLogFetcherIdlePool &idle_pool,
    IObLogFetcherDeadPool &dead_pool,
    IObLogErrHandler &err_handler)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(worker_thread_num <= 0)
      || OB_UNLIKELY(worker_thread_num > IObLSWorker::MAX_THREAD_NUM)) {
    LOG_ERROR("invalid argument", K(worker_thread_num));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(timer_.init(err_handler, max_timer_task_count))) {
    LOG_ERROR("init timer fail", KR(ret), K(max_timer_task_count));
  }
  // Initializing the thread pool
  else if (OB_FAIL(ObMapQueueThreadPool::init(OB_SERVER_TENANT_ID, worker_thread_num,
      ObModIds::OB_LS_WORKER_THREAD))) {
    LOG_ERROR("init worker thread fail", KR(ret), K(worker_thread_num));
  } else {
    fetcher_host_ = fetcher_host;
    idle_pool_ = &idle_pool;
    dead_pool_ = &dead_pool;
    err_handler_ = &err_handler;

    stream_task_seq_ = 0;

    stream_paused_ = false;
    fetcher_resume_time_ = OB_INVALID_TIMESTAMP;
    inited_ = true;

    LOG_INFO("init stream worker succ", K(worker_thread_num), K(this));
  }

  return ret;
}

void ObLSWorker::destroy()
{
  stop();

  if (inited_) {
    LOG_INFO("destroy stream worker begin");
    inited_ = false;
    stream_paused_ = false;
    fetcher_resume_time_ = OB_INVALID_TIMESTAMP;
    ObMapQueueThreadPool::destroy();
    timer_.destroy();
    fetcher_host_ = nullptr;
    idle_pool_ = NULL;
    dead_pool_ = NULL;
    err_handler_ = NULL;
    stream_task_seq_ = 0;

    LOG_INFO("destroy stream worker succ");
  }
}

int ObLSWorker::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(timer_.start())) {
    LOG_ERROR("start timer thread fail", KR(ret));
  } else if (OB_FAIL(ObMapQueueThreadPool::start())) {
    LOG_ERROR("start stream worker fail", KR(ret));
  } else {
    LOG_INFO("start stream worker succ");
  }
  return ret;
}

void ObLSWorker::stop()
{
  if (OB_LIKELY(inited_)) {
    LOG_INFO("stop stream worker begin");
    mark_stop_flag();
    timer_.stop();
    ObMapQueueThreadPool::stop();
    LOG_INFO("stop stream worker succ");
  }
}

void ObLSWorker::mark_stop_flag()
{
  LOG_INFO("stream worker mark_stop_flag begin");
  timer_.mark_stop_flag();
  ObMapQueueThreadPool::has_set_stop() = true;
  LOG_INFO("stream worker mark_stop_flag end");
}

void ObLSWorker::pause()
{
  if (OB_LIKELY(inited_)) {
    ATOMIC_STORE(&stream_paused_, true);
    LOG_INFO("pause stream worker succ", K_(stream_paused));
  }
}

void ObLSWorker::resume(int64_t fetcher_resume_tstamp)
{
  if (OB_LIKELY(inited_)) {
    ATOMIC_STORE(&fetcher_resume_time_, fetcher_resume_tstamp);
    ATOMIC_STORE(&stream_paused_, false);
    LOG_INFO("resume stream worker succ", K_(stream_paused));
  }
}

int64_t ObLSWorker::get_fetcher_resume_tstamp()
{
  int64_t fetcher_resume_tstamp = ATOMIC_LOAD(&fetcher_resume_time_);
  return fetcher_resume_tstamp;
}

// TODO: Add monitoring log, print dispatch reason
int ObLSWorker::dispatch_fetch_task(LSFetchCtx &task, const char *dispatch_reason)
{
  int ret = OB_SUCCESS;

  // Mark out the reason for the assignment
  task.dispatch_out(dispatch_reason);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(idle_pool_) || OB_ISNULL(dead_pool_)) {
    ret = OB_INVALID_ERROR;
    LOG_ERROR("invalid handlers", KR(ret), K(idle_pool_), K(dead_pool_));
  }
  // Recycle deleted partitions and add them to DEAD POOL
  else if (OB_UNLIKELY(task.is_discarded())) {
    LOG_DEBUG("[STAT] [STREAM_WORKER] [RECYCLE_FETCH_TASK]", "task", &task, K(task));

    if (OB_FAIL(dead_pool_->push(&task))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_DEBUG("push task into dead pool fail", KR(ret), K(task));
      }
    }
  } else if (is_integrated_fetching_mode(task.get_fetching_mode())) {
    ObAddr request_svr;
    request_svr.reset();
    bool found_valid_svr = false;

    LOG_DEBUG("[STAT] [STREAM_WORKER] [DISPATCH_FETCH_TASK] begin to dispatch",
        "task", &task, K(task), K(dispatch_reason));

    // Get the next valid server for the service log
    while (OB_SUCCESS == ret && ! found_valid_svr && OB_SUCC(task.next_server(request_svr))) {
      // TODO add strategy
      //found_valid_svr = is_svr_avail_(*all_svr_cache_, request_svr);
      found_valid_svr = true;
      if (! found_valid_svr) {
        //  server is not available, blacklisted
        int64_t svr_service_time = 0;
        int64_t survival_time = ATOMIC_LOAD(&g_blacklist_survival_time);
        if (OB_FAIL(task.add_into_blacklist(request_svr, svr_service_time, survival_time))) {
          // add server to blacklist
          LOG_ERROR("not-avail server, task add into blacklist fail", KR(ret), K(task), K(request_svr),
                    "svr_service_time", TVAL_TO_STR(svr_service_time),
                    "survival_time", TVAL_TO_STR(survival_time));
        } else {
          LOG_DEBUG("not-avail server, task add into blacklist succ", KR(ret), K(task), K(request_svr),
                    "svr_service_time", TVAL_TO_STR(svr_service_time),
                    "survival_time", TVAL_TO_STR(survival_time));
        }

        LOG_WARN("[STAT] [STREAM_WORKER] [DISPATCH_FETCH_TASK] ignore not-avail server",
            K(request_svr), "tls_id", task.get_tls_id());
      }
    }

    // The server list is iterated over
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCCESS == ret) {
      // No server available, put it into idle pool
      if (! found_valid_svr) {
        LOG_DEBUG("[STAT] [STREAM_WORKER] [DISPATCH_FETCH_TASK] server list is used up, "
            "dispatch to idle pool", "task", &task, K(task));

        if (OB_FAIL(idle_pool_->push(&task))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("push into idle pool fail", KR(ret), K(task));
          }
        }
      } else {
        LOG_DEBUG("[STAT] [STREAM_WORKER] [DISPATCH_FETCH_TASK] dispatch to next server",
            K(request_svr), "task", &task, K(task));

        // Assigning tasks to the server
        if (OB_FAIL(dispatch_fetch_task_to_svr_(task, request_svr))) {
          LOG_ERROR("dispatch fetch task to server fail", KR(ret), K(request_svr), K(task));
        }
      }
    }
  } else if (is_direct_fetching_mode(task.get_fetching_mode())) {
    ObAddr dummy_addr(ObAddr::IPV4, "127.0.0.1", 1);
    if (OB_FAIL(dispatch_fetch_task_to_svr_(task, dummy_addr))) {
      LOG_ERROR("dispatch fetch task to invalid server fail", KR(ret), K(task), K(dummy_addr));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("the fetching mode of task is invalid", K(task));
  }

  return ret;
}

int ObLSWorker::dispatch_stream_task(FetchStream &task, const char *from_mod)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else {
    int64_t hash_val = ATOMIC_FAA(&stream_task_seq_, 1);
    bool print_stream_dispatch_info = ATOMIC_LOAD(&g_print_stream_dispatch_info);

    if (print_stream_dispatch_info) {
      LOG_INFO("[STAT] [STREAM_WORKER] [DISPATCH_STREAM_TASK]",
          "fetch_stream", &task, K(from_mod), K(hash_val), K(task));
    } else {
      LOG_DEBUG("[STAT] [STREAM_WORKER] [DISPATCH_STREAM_TASK]",
          "fetch_stream", &task, K(from_mod), K(hash_val), K(task));
    }

    // Rotating the task of fetching log streams to work threads
    if (OB_FAIL(ObMapQueueThreadPool::push(&task, hash_val))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("push stream task into thread queue fail", KR(ret));
      }
    }
  }
  return ret;
}

int ObLSWorker::hibernate_stream_task(FetchStream &task, const char *from_mod)
{
  int ret = OB_SUCCESS;
  bool print_stream_dispatch_info = ATOMIC_LOAD(&g_print_stream_dispatch_info);

  if (print_stream_dispatch_info) {
    LOG_INFO("[STAT] [STREAM_WORKER] [HIBERNATE_STREAM_TASK]",
        "task", &task, K(from_mod), K(task));
  } else {
    LOG_DEBUG("[STAT] [STREAM_WORKER] [HIBERNATE_STREAM_TASK]",
        "task", &task, K(from_mod), K(task));
  }

  if (OB_FAIL(timer_.schedule(&task))) {
    LOG_ERROR("schedule timer task fail", KR(ret));
  } else {
    // success
  }
  return ret;
}

// hendle function for thread pool
void ObLSWorker::handle(void *data,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool is_paused = ATOMIC_LOAD(&stream_paused_);
  FetchStream *task = static_cast<FetchStream *>(data);
  ObLogTraceIdGuard trace_guard;

  LOG_DEBUG("[STAT] [STREAM_WORKER] [HANDLE_STREAM_TASK]", K_(stream_paused), "thread_index", get_thread_idx(),
      K(task), KPC(task));

  if (OB_ISNULL(task)) {
    LOG_ERROR("invalid task", K(task), "thread_index", get_thread_idx());
    ret = OB_INVALID_ARGUMENT;
  }
  // If the stream task is currently suspended, the task is put to sleep
  // 1. DDL tasks are exempt from suspend and require always processing
  // 2. ready rpc(response already return) should always processing
  else if (OB_UNLIKELY(is_paused) && ! (task->is_sys_log_stream() || task->is_rpc_ready())) {
    LOG_DEBUG("[STAT] [STREAM_WORKER] [HIBERNATE_STREAM_TASK_ON_PAUSE]", K(task));

    if (OB_FAIL(hibernate_stream_task(*task, "PausedFetcher"))) {
      LOG_ERROR("hibernate_stream_task on pause fail", KR(ret), K(task), KPC(task));
    }
  } else if (OB_FAIL(task->handle(stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle fetch stream task fail", KR(ret), K(task));
    }
  } else {
    // Can no longer continue with the task
  }

  if (0 == get_thread_idx()) {
    if (REACH_TIME_INTERVAL(STAT_INTERVAL)) {
      print_stat_();
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && OB_NOT_NULL(err_handler_)) {
    err_handler_->handle_error(ret, "stream worker exits on error, err=%d, thread_index=%ld",
        ret, get_thread_idx());
    ObMapQueueThreadPool::has_set_stop() = true; // mark thread pool stop;
  }
}

void ObLSWorker::configure(const ObLogConfig & config)
{
  int64_t blacklist_survival_time_sec = config.blacklist_survival_time_sec;
  bool print_stream_dispatch_info = config.print_stream_dispatch_info;

  ATOMIC_STORE(&g_blacklist_survival_time, blacklist_survival_time_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(blacklist_survival_time_sec));

  ATOMIC_STORE(&g_print_stream_dispatch_info, print_stream_dispatch_info);
  LOG_INFO("[CONFIG]", K(print_stream_dispatch_info));
}

/*
bool ObLSWorker::is_svr_avail_(IObLogAllSvrCache &all_svr_cache, const common::ObAddr &request_svr)
{
  // TODO: Add other policies such as blacklisting policies
  return all_svr_cache.is_svr_avail(request_svr);
}
*/

int ObLSWorker::dispatch_fetch_task_to_svr_(LSFetchCtx &task, const common::ObAddr &request_svr)
{
  int ret = OB_SUCCESS;
  const logservice::TenantLSID &tls_id = task.get_tls_id();
  IObFsContainerMgr *fs_container_mgr = nullptr;
  FetchStreamContainer *fsc = nullptr;
  IObLogFetcher *fetcher = static_cast<IObLogFetcher *>(fetcher_host_);

  if (OB_ISNULL(fetcher)) {
    ret = OB_ERR_UNDEFINED;
    LOG_ERROR("fetcher is nullptr", KR(ret), K(fetcher));
  } else if (OB_FAIL(fetcher->get_fs_container_mgr(fs_container_mgr))) {
    LOG_ERROR("Fetcher get_fs_container_mgr fail", KR(ret));
  } else if (OB_FAIL(fs_container_mgr->get_fsc(tls_id, fsc))) {
    LOG_ERROR("FetchStreamContainerMgr get_fsc fail", KR(ret));
  } else {
    LOG_DEBUG("[STAT] [STREAM_WORKER] [DISPATCH_FETCH_TASK] dispatch to svr",
        "task", &task, K(task), K(request_svr));

    if (OB_FAIL(fsc->dispatch(task, request_svr))) {
      LOG_ERROR("FetchStreamContainer dispatch fail", KR(ret), K(task), K(request_svr));
    } else {
      // You cannot continue to operate on the task afterwards
    }
  }

  return ret;
}

void ObLSWorker::print_stat_()
{
}

}
}
