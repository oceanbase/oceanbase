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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_stream_worker.h"

#include "ob_log_timer.h"               // ObLogFixedTimer
#include "ob_log_instance.h"            // IObLogErrHandler
#include "ob_log_all_svr_cache.h"       // IObLogAllSvrCache
#include "ob_log_part_fetch_ctx.h"      // PartFetchCtx
#include "ob_log_fetcher_idle_pool.h"   // IObLogFetcherIdlePool
#include "ob_log_fetcher_dead_pool.h"   // IObLogFetcherDeadPool

namespace oceanbase
{
namespace liboblog
{

// Defining class global variables
int64_t ObLogStreamWorker::g_blacklist_survival_time =
    ObLogConfig::default_blacklist_survival_time_sec * _SEC_;

bool ObLogStreamWorker::g_print_stream_dispatch_info =
    ObLogConfig::default_print_stream_dispatch_info;

ObLogStreamWorker::ObLogStreamWorker() :
    inited_(false),
    stream_paused_(false),
    fetcher_resume_time_(OB_INVALID_TIMESTAMP),
    rpc_(NULL),
    idle_pool_(NULL),
    dead_pool_(NULL),
    svr_finder_(NULL),
    err_handler_(NULL),
    all_svr_cache_(NULL),
    heartbeater_(NULL),
    progress_controller_(NULL),
    timer_(),
    fs_pool_(),
    rpc_result_pool_(),
    svr_stream_map_(),
    svr_stream_pool_(),
    svr_stream_alloc_lock_(),
    stream_task_seq_(0)
{}

ObLogStreamWorker::~ObLogStreamWorker()
{
  destroy();
}

int ObLogStreamWorker::init(const int64_t worker_thread_num,
    const int64_t svr_stream_cached_count,
    const int64_t fetch_stream_cached_count,
    const int64_t rpc_result_cached_count,
    const int64_t max_timer_task_count,
    IObLogRpc &rpc,
    IObLogFetcherIdlePool &idle_pool,
    IObLogFetcherDeadPool &dead_pool,
    IObLogSvrFinder &svr_finder,
    IObLogErrHandler &err_handler,
    IObLogAllSvrCache &all_svr_cache,
    IObLogFetcherHeartbeatWorker &heartbeater,
    PartProgressController &progress_controller)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(worker_thread_num <= 0)
      || OB_UNLIKELY(worker_thread_num > IObLogStreamWorker::MAX_THREAD_NUM)) {
    LOG_ERROR("invalid argument", K(worker_thread_num));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(timer_.init(err_handler, max_timer_task_count))) {
    LOG_ERROR("init timer fail", KR(ret), K(max_timer_task_count));
  } else if (OB_FAIL(fs_pool_.init(fetch_stream_cached_count))) {
    LOG_ERROR("init fetch stream pool fail", KR(ret), K(fetch_stream_cached_count));
  } else if (OB_FAIL(rpc_result_pool_.init(rpc_result_cached_count))) {
    LOG_ERROR("init rpc result pool fail", KR(ret), K(rpc_result_cached_count));
  }
  // Initializing the thread pool
  else if (OB_FAIL(StreamWorkerThread::init(worker_thread_num,
      ObModIds::OB_LOG_STREAM_WORKER_THREAD))) {
    LOG_ERROR("init worker thread fail", KR(ret), K(worker_thread_num));
  } else if (OB_FAIL(svr_stream_map_.init(ObModIds::OB_LOG_SVR_STREAM_MAP))) {
    LOG_ERROR("init svr stream map fail", KR(ret));
  } else if (OB_FAIL(svr_stream_pool_.init(svr_stream_cached_count,
      ObModIds::OB_LOG_SVR_STREAM_POOL,
      OB_SERVER_TENANT_ID,
      SVR_STREAM_POOL_BLOCK_SIZE))) {
    LOG_ERROR("init svr stream pool fail", KR(ret));
  } else {
    rpc_ = &rpc;
    idle_pool_ = &idle_pool;
    dead_pool_ = &dead_pool;
    svr_finder_ = &svr_finder;
    err_handler_ = &err_handler;
    all_svr_cache_ = &all_svr_cache;
    heartbeater_ = &heartbeater;
    progress_controller_ = &progress_controller;

    stream_task_seq_ = 0;

    stream_paused_ = false;
    fetcher_resume_time_ = OB_INVALID_TIMESTAMP;
    inited_ = true;

    LOG_INFO("init stream worker succ", K(worker_thread_num), K(this));
  }

  return ret;
}

void ObLogStreamWorker::destroy()
{
  stop();

  inited_ = false;
  stream_paused_ = false;
  fetcher_resume_time_ = OB_INVALID_TIMESTAMP;
  StreamWorkerThread::destroy();

  // TODO：consideration of resource release globally
  free_all_svr_stream_();

  (void)svr_stream_map_.destroy();
  svr_stream_pool_.destroy();
  timer_.destroy();
  fs_pool_.destroy();

  rpc_ = NULL;
  idle_pool_ = NULL;
  dead_pool_ = NULL;
  svr_finder_ = NULL;
  err_handler_ = NULL;
  all_svr_cache_ = NULL;
  heartbeater_ = NULL;
  progress_controller_ = NULL;
  stream_task_seq_ = 0;

  LOG_INFO("destroy stream worker succ");
}

int ObLogStreamWorker::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(timer_.start())) {
    LOG_ERROR("start timer thread fail", KR(ret));
  } else if (OB_FAIL(StreamWorkerThread::start())) {
    LOG_ERROR("start stream worker fail", KR(ret));
  } else {
    LOG_INFO("start stream worker succ");
  }
  return ret;
}

void ObLogStreamWorker::stop()
{
  if (OB_LIKELY(inited_)) {
    StreamWorkerThread::stop();
    LOG_INFO("stop stream worker succ");
  }
}

void ObLogStreamWorker::mark_stop_flag()
{
  timer_.mark_stop_flag();
  StreamWorkerThread::mark_stop_flag();
}

void ObLogStreamWorker::pause()
{
  if (OB_LIKELY(inited_)) {
    ATOMIC_STORE(&stream_paused_, true);
    LOG_INFO("pause stream worker succ", K_(stream_paused));
  }
}

void ObLogStreamWorker::resume(int64_t fetcher_resume_tstamp)
{
  if (OB_LIKELY(inited_)) {
    ATOMIC_STORE(&fetcher_resume_time_, fetcher_resume_tstamp);
    ATOMIC_STORE(&stream_paused_, false);
    LOG_INFO("resume stream worker succ", K_(stream_paused));
  }
}

int64_t ObLogStreamWorker::get_fetcher_resume_tstamp()
{
  int64_t fetcher_resume_tstamp = ATOMIC_LOAD(&fetcher_resume_time_);
  return fetcher_resume_tstamp;
}

// TODO: Add monitoring log, print dispatch reason
int ObLogStreamWorker::dispatch_fetch_task(PartFetchCtx &task, const char *dispatch_reason)
{
  int ret = OB_SUCCESS;

  // Mark out the reason for the assignment
  task.dispatch_out(dispatch_reason);

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(idle_pool_) || OB_ISNULL(all_svr_cache_) || OB_ISNULL(dead_pool_)) {
    LOG_ERROR("invalid handlers", K(idle_pool_), K(all_svr_cache_), K(dead_pool_));
    ret = OB_INVALID_ERROR;
  }
  // Recycle deleted partitions and add them to DEAD POOL
  else if (OB_UNLIKELY(task.is_discarded())) {
    LOG_DEBUG("[STAT] [STREAM_WORKER] [RECYCLE_FETCH_TASK]", "task", &task, K(task));

    if (OB_FAIL(dead_pool_->push(&task))) {
      LOG_DEBUG("push task into dead pool fail", KR(ret), K(task));
    }
  } else {
    ObAddr svr;
    bool found_valid_svr = false;

    LOG_DEBUG("[STAT] [STREAM_WORKER] [DISPATCH_FETCH_TASK] begin to dispatch",
        "task", &task, K(task));

    // Get the next valid server for the service log
    while (OB_SUCCESS == ret && ! found_valid_svr && OB_SUCC(task.next_server(svr))) {
      found_valid_svr = is_svr_avail_(*all_svr_cache_, svr);
      if (! found_valid_svr) {
        //  server is not available, blacklisted
        int64_t svr_service_time = 0;
        int64_t survival_time = ATOMIC_LOAD(&g_blacklist_survival_time);
        if (task.add_into_blacklist(svr, svr_service_time, survival_time)) {
          // add server to blacklist
          LOG_ERROR("not-avail server, task add into blacklist fail", KR(ret), K(task), K(svr),
                    "svr_service_time", TVAL_TO_STR(svr_service_time),
                    "survival_time", TVAL_TO_STR(survival_time));
        } else {
          LOG_DEBUG("not-avail server, task add into blacklist succ", KR(ret), K(task), K(svr),
                    "svr_service_time", TVAL_TO_STR(svr_service_time),
                    "survival_time", TVAL_TO_STR(survival_time));
        }

        LOG_WARN("[STAT] [STREAM_WORKER] [DISPATCH_FETCH_TASK] ignore not-avail server",
            K(svr), "pkey", task.get_pkey());
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
          LOG_ERROR("push into idle pool fail", KR(ret), K(task));
        }
      } else {
        LOG_DEBUG("[STAT] [STREAM_WORKER] [DISPATCH_FETCH_TASK] dispatch to next server",
            K(svr), "task", &task, K(task));

        // Assigning tasks to the server
        if (OB_FAIL(dispatch_fetch_task_to_svr_(task, svr))) {
          LOG_ERROR("dispatch fetch task to server fail", KR(ret), K(svr), K(task));
        }
      }
    }
  }

  return ret;
}

int ObLogStreamWorker::dispatch_stream_task(FetchStream &task, const char *from_mod)
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
    if (OB_FAIL(StreamWorkerThread::push(&task, hash_val))) {
      LOG_ERROR("push stream task into thread queue fail", KR(ret));
    }
  }
  return ret;
}

int ObLogStreamWorker::hibernate_stream_task(FetchStream &task, const char *from_mod)
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
int ObLogStreamWorker::handle(void *data,
    const int64_t thread_index,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  bool is_paused = ATOMIC_LOAD(&stream_paused_);
  FetchStream *task = static_cast<FetchStream *>(data);

  LOG_DEBUG("[STAT] [STREAM_WORKER] [HANDLE_STREAM_TASK]", K_(stream_paused), K(thread_index),
      K(task), KPC(task));

  if (OB_ISNULL(task)) {
    LOG_ERROR("invalid task", K(task), K(thread_index));
    ret = OB_INVALID_ARGUMENT;
  }
  // If the stream task is currently suspended, the task is put to sleep
  // DDL tasks are exempt from suspend and require always processing
  else if (OB_UNLIKELY(is_paused) && ! task->is_ddl_stream()) {
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

  if (0 == thread_index) {
    if (REACH_TIME_INTERVAL(STAT_INTERVAL)) {
      print_stat_();
    }
  }

  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret && OB_NOT_NULL(err_handler_)) {
    err_handler_->handle_error(ret, "stream worker exits on error, err=%d, thread_index=%ld",
        ret, thread_index);
  }
  return ret;
}

void ObLogStreamWorker::configure(const ObLogConfig & config)
{
  int64_t blacklist_survival_time_sec = config.blacklist_survival_time_sec;
  bool print_stream_dispatch_info = config.print_stream_dispatch_info;

  ATOMIC_STORE(&g_blacklist_survival_time, blacklist_survival_time_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(blacklist_survival_time_sec));

  ATOMIC_STORE(&g_print_stream_dispatch_info, print_stream_dispatch_info);
  LOG_INFO("[CONFIG]", K(print_stream_dispatch_info));
}

bool ObLogStreamWorker::is_svr_avail_(IObLogAllSvrCache &all_svr_cache, const common::ObAddr &svr)
{
  // TODO: Add other policies such as blacklisting policies
  return all_svr_cache.is_svr_avail(svr);
}

int ObLogStreamWorker::dispatch_fetch_task_to_svr_(PartFetchCtx &task, const common::ObAddr &svr)
{
  int ret = OB_SUCCESS;
  SvrStream *svr_stream = NULL;

  if (OB_FAIL(get_svr_stream_(svr, svr_stream))) {
    LOG_ERROR("get_svr_stream_ fail", KR(ret), K(svr));
  } else if (OB_ISNULL(svr_stream)) {
    LOG_ERROR("invalid svr stream", K(svr_stream));
    ret = OB_ERR_UNEXPECTED;
  } else {
    LOG_DEBUG("[STAT] [STREAM_WORKER] [DISPATCH_FETCH_TASK] dispatch to svr_stream",
        "task", &task, K(svr_stream), KPC(svr_stream), K(task));

    if (OB_FAIL(svr_stream->dispatch(task))) {
      LOG_ERROR("dispatch to svr stream fail", KR(ret), K(task), K(svr_stream));
    } else {
      // You cannot continue to operate on the task afterwards
    }
  }
  return ret;
}

int ObLogStreamWorker::get_svr_stream_(const common::ObAddr &svr, SvrStream *&svr_stream)
{
  int ret = OB_SUCCESS;

  svr_stream = NULL;
  if (OB_FAIL(svr_stream_map_.get(svr, svr_stream))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get svr stream from map fail", KR(ret), K(svr));
    } else {
      // SvrStream does not exist, create one and insert it into the map
      ret = OB_SUCCESS;

      if (OB_FAIL(get_svr_stream_when_not_exist_(svr, svr_stream))) {
        LOG_ERROR("get_svr_stream_when_not_exist_ fail", KR(ret), K(svr));
      }
    }
  }

  return ret;
}

int ObLogStreamWorker::get_svr_stream_when_not_exist_(const common::ObAddr &svr,
    SvrStream *&svr_stream)
{
  int ret = OB_SUCCESS;
  svr_stream = NULL;

  // To ensure that SvrStream is allocated only once for the same server and to avoid wasting memory,
  // lock control is used here so that only one thread can allocate SvrStream at the same time after a new server is added.
  ObSpinLockGuard guard(svr_stream_alloc_lock_);

  if (OB_SUCC(svr_stream_map_.get(svr, svr_stream))) {
    // already created
  } else if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
    LOG_ERROR("get svr stream from map fail", KR(ret), K(svr));
  } else {
    ret = OB_SUCCESS;

    if (OB_ISNULL(rpc_)
        || OB_ISNULL(svr_finder_)
        || OB_ISNULL(heartbeater_)
        || OB_ISNULL(progress_controller_)) {
      LOG_ERROR("invalid handlers", K(rpc_), K(svr_finder_), K(heartbeater_),
          K(progress_controller_));
      ret = OB_INVALID_ERROR;
    } else if (OB_FAIL(svr_stream_pool_.alloc(svr_stream))) {
      LOG_ERROR("allocate svr stream from pool fail", K(svr_stream));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(svr_stream)) {
      LOG_ERROR("allocate svr stream from pool fail", K(svr_stream));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      svr_stream->reset(svr,
          *rpc_,
          fs_pool_,
          *svr_finder_,
          *heartbeater_,
          *this,
          rpc_result_pool_,
          *progress_controller_);

      // require must return OB_SUCCESS otherwise error
      if (OB_FAIL(svr_stream_map_.insert(svr, svr_stream))) {
        LOG_ERROR("insert into svr stream map fail", KR(ret), K(svr), K(svr_stream));
        free_svr_stream_(svr_stream);
        svr_stream = NULL;
      } else {
        LOG_INFO("[STAT] [SVR_STREAM] [ALLOC]", K(svr_stream), KPC(svr_stream));
      }
    }
  }

  return ret;
}

int ObLogStreamWorker::free_svr_stream_(SvrStream *svr_stream)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(svr_stream)) {
    svr_stream->reset();

    if (OB_FAIL(svr_stream_pool_.free(svr_stream))) {
      LOG_ERROR("free svr stream into pool fail", KR(ret), K(svr_stream));
    } else {
      svr_stream = NULL;
    }
  }

  return ret;
}

bool ObLogStreamWorker::SvrStreamFreeFunc::operator() (const common::ObAddr &key,
    SvrStream* svr_stream)
{
  UNUSED(key);

  if (NULL != svr_stream) {
    LOG_INFO("[STAT] [SVR_STREAM] [FREE]", K(svr_stream), KPC(svr_stream));
    svr_stream->reset();
    (void)pool_.free(svr_stream);
    svr_stream = NULL;
  }

  return true;
}

void ObLogStreamWorker::free_all_svr_stream_()
{
  int ret = OB_SUCCESS;
  if (svr_stream_map_.count() > 0) {
    SvrStreamFreeFunc func(svr_stream_pool_);

    if (OB_FAIL(svr_stream_map_.remove_if(func))) {
      LOG_ERROR("remove if from svr stream map fail", KR(ret));
    }
  }
}

void ObLogStreamWorker::print_stat_()
{
  int ret = OB_SUCCESS;
  SvrStreamStatFunc svr_stream_stat_func;

  int64_t alloc_count = svr_stream_pool_.get_alloc_count();
  int64_t free_count = svr_stream_pool_.get_free_count();
  int64_t fixed_count = svr_stream_pool_.get_fixed_count();
  int64_t used_count = alloc_count - free_count;
  int64_t dynamic_count = (alloc_count > fixed_count) ? alloc_count - fixed_count : 0;

  _LOG_INFO("[STAT] [SVR_STREAM_POOL] USED=%ld FREE=%ld FIXED=%ld DYNAMIC=%ld",
      used_count, free_count, fixed_count, dynamic_count);

  fs_pool_.print_stat();
  rpc_result_pool_.print_stat();

  // Statistics every SvrStream
  if (OB_FAIL(svr_stream_map_.for_each(svr_stream_stat_func))) {
    LOG_ERROR("for each svr stream map fail", KR(ret));
  }
}

}
}
