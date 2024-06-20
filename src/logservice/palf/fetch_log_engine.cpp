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

#include "fetch_log_engine.h"
#include "log_define.h"
#include "palf_handle_impl.h"
#include "palf_handle_impl_guard.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/ob_thread_mgr.h"
#include "share/ob_ls_id.h"
#include "palf_env_impl.h"

namespace oceanbase
{
using namespace common;
namespace palf
{
int FetchLogTask::set(const int64_t id,
                      const common::ObAddr &server,
                      const FetchLogType fetch_type,
                      const int64_t &proposal_id,
                      const LSN &prev_lsn,
                      const LSN &start_lsn,
                      const int64_t log_size,
                      const int64_t log_count,
                      const int64_t accepted_mode_pid)
{
  int ret = OB_SUCCESS;

  if (id < 0
      || !server.is_valid()
      || INVALID_PROPOSAL_ID == proposal_id
      || !start_lsn.is_valid()
      || log_size < 0
      || log_count < 0
      || INVALID_PROPOSAL_ID == accepted_mode_pid) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(id), K(server), K(proposal_id), K(prev_lsn),
             K(start_lsn), K(log_size), K(log_count), K(accepted_mode_pid));
  } else {
    timestamp_us_ = ObTimeUtility::current_time();
    id_ = id;
    server_ = server;
    fetch_type_ = fetch_type;
    proposal_id_ = proposal_id;
    prev_lsn_ = prev_lsn;
    start_lsn_ = start_lsn;
    log_size_ = log_size;
    log_count_ = log_count;
    accepted_mode_pid_ = accepted_mode_pid;
  }
  return ret;
}

void FetchLogTask::reset()
{
  timestamp_us_ = common::OB_INVALID_TIMESTAMP;
  id_ = -1;
  server_.reset();
  fetch_type_ = FETCH_LOG_FOLLOWER;
  proposal_id_ = INVALID_PROPOSAL_ID;
  prev_lsn_.reset();
  start_lsn_.reset();
  log_size_ = 0;
  log_count_ = 0;
  accepted_mode_pid_ = INVALID_PROPOSAL_ID;
}

bool FetchLogTask::is_valid() const
{
  return (id_ >= 0 && server_.is_valid() && start_lsn_.is_valid());
}

FetchLogTask& FetchLogTask::operator=(const FetchLogTask &task)
{
  if (&task != this) {
    this->timestamp_us_ = task.get_timestamp_us();
    this->id_ = task.get_id();
    this->server_ = task.get_server();
    this->fetch_type_ = task.get_fetch_type();
    this->proposal_id_ = task.get_proposal_id();
    this->prev_lsn_ = task.get_prev_lsn();
    this->start_lsn_ = task.get_start_lsn();
    this->log_size_ = task.get_log_size();
    this->log_count_ = task.get_log_count();
    this->accepted_mode_pid_ = task.get_accepted_mode_pid();
  }
  return *this;
}

FetchLogEngine::FetchLogEngine()
  : tg_id_(-1),
    is_inited_(false),
    palf_env_impl_(NULL),
    allocator_(NULL),
    replayable_point_(),
    cache_lock_(),
    fetch_task_cache_(),
    fetch_wait_cost_stat_("[PALF STAT FETCH LOG TASK IN QUEUE TIME]", PALF_STAT_PRINT_INTERVAL_US),
    fetch_log_cost_stat_("[PALF STAT FETCH LOG EXECUTE COST TIME]", PALF_STAT_PRINT_INTERVAL_US)
{}


int FetchLogEngine::init(IPalfEnvImpl *palf_env_impl,
                         common::ObILogAllocator *alloc_mgr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    PALF_LOG(WARN, "FetchLogEngine init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(palf_env_impl)
             || OB_ISNULL(alloc_mgr)) {
    PALF_LOG(WARN, "invalid argument", KP(palf_env_impl), K(alloc_mgr));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::LSFetchLogEngine, tg_id_))) {
    PALF_LOG(WARN, "ObSimpleThreadPool::init failed", K(ret));
  } else if (OB_FAIL(MTL_REGISTER_THREAD_DYNAMIC(0.5, tg_id_))) {
    PALF_LOG(WARN, "MTL_REGISTER_THREAD_DYNAMIC failed", K(ret), K(tg_id_));
  } else {
    palf_env_impl_ = palf_env_impl;
    allocator_ = alloc_mgr;
    is_inited_ = true;
  }
  if ((OB_FAIL(ret)) && (OB_INIT_TWICE != ret)) {
    PALF_LOG(WARN, "FetchLogEngine init failed", K(ret), KP(palf_env_impl));
    destroy();
  }

  return ret;
}

int FetchLogEngine::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "FetchLogEngine not inited!!!", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    PALF_LOG(ERROR, "start FetchLogEngine failed", K(ret));
  } else {
    PALF_LOG(INFO, "start FetchLogEngine success", K(ret), K(tg_id_));
  }
  return ret;
}

int FetchLogEngine::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "FetchLogEngine not inited!!!", K(ret));
  } else {
    TG_STOP(tg_id_);
    PALF_LOG(INFO, "stop FetchLogEngine success", K(tg_id_));
  }
  return ret;
}

int FetchLogEngine::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "FetchLogEngine not inited!!!", K(ret));
  } else {
    TG_WAIT(tg_id_);
    PALF_LOG(INFO, "wait FetchLogEngine success", K(tg_id_));
  }
  return ret;
}

void FetchLogEngine::destroy()
{
  stop();
  wait();
  is_inited_ = false;
  if (-1 != tg_id_) {
    MTL_UNREGISTER_THREAD_DYNAMIC(tg_id_);
    TG_DESTROY(tg_id_);
  }
  tg_id_ = -1;
  palf_env_impl_ = NULL;
  allocator_ = NULL;
  fetch_task_cache_.destroy();
  PALF_LOG(INFO, "destroy FetchLogEngine success", K(tg_id_));
}

int FetchLogEngine::submit_fetch_log_task(FetchLogTask *fetch_log_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "FetchLogEngine not init", K(ret));
  } else if (OB_ISNULL(fetch_log_task)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(fetch_log_task));
  } else if (OB_FAIL(push_task_into_cache_(fetch_log_task))) {
    PALF_LOG(WARN, "push_task_into_cache_ failed", K(ret), KPC(fetch_log_task));
  } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, fetch_log_task))) {
    PALF_LOG(WARN, "push failed", K(ret), KPC(fetch_log_task));
  } else {
    //do nothing
  }

  return ret;
}

int FetchLogEngine::push_task_into_cache_(FetchLogTask *fetch_log_task)
{
  // If this task exists in cache, it returns OB_ENTRY_EXIST.
  // If this task destn't exist in cache, and cache is full, just ignore it
  // and return OB_SUCCESS.
  int ret = OB_SUCCESS;
  SpinLockGuard lock_guard(cache_lock_);
  int64_t count = fetch_task_cache_.count();
  if (count >= MAX_CACHED_FETCH_TASK_NUM) {
    if (REACH_TENANT_TIME_INTERVAL(5 * 1000 * 1000)) {
      PALF_LOG(INFO, "fetch_task_cache_ is full", K(ret), K_(fetch_task_cache));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (fetch_task_cache_[i].get_id() == fetch_log_task->get_id()
          && fetch_task_cache_[i].get_server() == fetch_log_task->get_server()) {
        // found existed task for this <server, id>
        ret = OB_ENTRY_EXIST;
        break;
      }
    }
    if (OB_SUCCESS == ret) {
      fetch_task_cache_.push_back(*fetch_log_task);
    }
  }
  return ret;
}

int FetchLogEngine::try_remove_task_from_cache_(FetchLogTask *fetch_log_task)
{
  int ret = OB_SUCCESS;
  SpinLockGuard lock_guard(cache_lock_);
  int64_t count = fetch_task_cache_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (fetch_task_cache_[i].get_id() == fetch_log_task->get_id()
        && fetch_task_cache_[i].get_server() == fetch_log_task->get_server()) {
      // found existed task for this <server, id>
      if (OB_FAIL(fetch_task_cache_.remove(i))) {
        PALF_LOG(WARN, "fetch_task_cache_.remove failed", K(ret), K(i));
      }
      break;
    }
  }
  return ret;
}

void FetchLogEngine::handle(void *task)
{
  int ret = OB_SUCCESS;
  IPalfHandleImpl *palf_handle_impl = NULL;
  if (!is_inited_) {
    PALF_LOG(WARN, "FetchLogEngine not init");
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "invalid argument", KR(ret), KP(task));
  } else {
    int64_t handle_start_time_us = ObTimeUtility::current_time();
    FetchLogTask *fetch_log_task = static_cast<FetchLogTask *>(task);
    int64_t palf_id = -1;
    FetchLogStat fetch_stat;
    if (OB_ISNULL(fetch_log_task)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "fetch_log_task is NULL", KR(ret));
    } else {
      palf_id = fetch_log_task->get_id();
      PALF_LOG(INFO, "handle fetch_log_task", KPC(fetch_log_task));
      IPalfHandleImplGuard guard;
      if (OB_FAIL(palf_env_impl_->get_palf_handle_impl(palf_id, guard))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          PALF_LOG(ERROR, "PalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id));
        } else {
          PALF_LOG(WARN, "ObLogService has not existed!!!", K(ret), K(palf_id));
        }
      } else if (OB_ISNULL(palf_handle_impl = guard.get_palf_handle_impl())) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "invalid log service", K(ret), K(palf_id));
      } else if (OB_FAIL(palf_handle_impl->fetch_log_from_storage(fetch_log_task->get_server(),
                                                                  fetch_log_task->get_fetch_type(),
                                                                  fetch_log_task->get_proposal_id(),
                                                                  fetch_log_task->get_prev_lsn(),
                                                                  fetch_log_task->get_start_lsn(),
                                                                  fetch_log_task->get_log_size(),
                                                                  fetch_log_task->get_log_count(),
                                                                  fetch_log_task->get_accepted_mode_pid(),
                                                                  replayable_point_.atomic_load(),
                                                                  fetch_stat))) {
        PALF_LOG(WARN, "fetch_log_from_storage failed", K(ret), K(palf_id), KPC(fetch_log_task));
      } else {
        // do nothing
      }
    }
    // remove it from cache
    if (OB_NOT_NULL(fetch_log_task)) {
      (void) try_remove_task_from_cache_(fetch_log_task);
    }
    int64_t fetch_log_task_submit_time_us = fetch_log_task->get_timestamp_us();
    int64_t handle_finish_time_us = ObTimeUtility::current_time();
    int64_t fetch_wait_cost_time_us = handle_start_time_us - fetch_log_task_submit_time_us;
    int64_t handle_cost_time_us = handle_finish_time_us - handle_start_time_us;
    fetch_wait_cost_stat_.stat(fetch_wait_cost_time_us);
    fetch_log_cost_stat_.stat(handle_cost_time_us);
    if (REACH_TIME_INTERVAL(100 * 1000L)) {
      PALF_LOG(INFO, "handle fetch log task", K(ret), K(palf_id), K(handle_cost_time_us), KPC(fetch_log_task), K(fetch_stat));
    } else if (handle_cost_time_us > 200 * 1000L) {
      PALF_LOG(INFO, "handle fetch log task cost too much time", K(ret), K(palf_id), K(handle_cost_time_us),
               KPC(fetch_log_task), K(fetch_stat));
    }
    free_fetch_log_task(fetch_log_task);
  }
}

void FetchLogEngine::handle_drop(void *task)
{
  int ret = OB_SUCCESS;
  IPalfHandleImpl *palf_handle_impl = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "FetchLogEngine not init");
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(WARN, "invalid argument", KP(task));
  } else {
    FetchLogTask *fetch_log_task = static_cast<FetchLogTask *>(task);
    free_fetch_log_task(fetch_log_task);
  }
}

FetchLogTask *FetchLogEngine::alloc_fetch_log_task()
{
  return allocator_->alloc_palf_fetch_log_task();
}

void FetchLogEngine::free_fetch_log_task(FetchLogTask *task)
{
  allocator_->free_palf_fetch_log_task(task);
}

int FetchLogEngine::update_replayable_point(const share::SCN &replayable_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    replayable_point_.atomic_store(replayable_scn);
  }
  return ret;
}

} // namespace palf
} // namespace oceanbase
