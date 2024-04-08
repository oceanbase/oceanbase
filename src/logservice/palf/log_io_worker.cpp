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
#define USING_LOG_PREFIX PALF

#include "log_io_worker.h"
#include <time.h>                             // timespce
#include <sys/prctl.h>                        // prctl
#include "lib/ob_errno.h"                     // OB_SUCCESS
#include "lib/stat/ob_session_stat.h"         // Session
#include "lib/thread/ob_thread_name.h"        // set_thread_name
#include "share/rc/ob_tenant_base.h"          // mtl_free
#include "share/ob_throttling_utils.h"        //ObThrottlingUtils
#include "log_io_task.h"                      // LogIOTask
#include "palf_env_impl.h"                    // PalfEnvImpl
#include "log_throttle.h"                     // LogWritingThrottle

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
LogIOWorker::LogIOWorker()
    : log_io_worker_num_(-1),
      cb_thread_pool_tg_id_(-1),
      palf_env_impl_(NULL),
      do_task_used_ts_(0),
      do_task_count_(0),
      print_log_interval_(OB_INVALID_TIMESTAMP),
      last_working_time_(OB_INVALID_TIMESTAMP),
      throttle_(NULL),
      log_io_worker_queue_size_stat_("[PALF STAT LOG IO WORKER QUEUE SIZE]", PALF_STAT_PRINT_INTERVAL_US),
      purge_throttling_task_submitted_seq_(0),
      purge_throttling_task_handled_seq_(0),
      need_ignoring_throttling_(false),
      wait_cost_stat_("[PALF STAT IO TASK IN QUEUE TIME]", PALF_STAT_PRINT_INTERVAL_US),
      is_inited_(false)
{
}

LogIOWorker::~LogIOWorker()
{
  destroy();
}

int LogIOWorker::init(const LogIOWorkerConfig &config,
                      const int64_t tenant_id,
                      const int cb_thread_pool_tg_id,
                      ObIAllocator *allocator,
                      LogWritingThrottle *throttle,
                      const bool need_igore_throttle,
                      IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogIOWorker has been inited", K(ret));
  } else if (false == config.is_valid() || 0 >= cb_thread_pool_tg_id || OB_ISNULL(allocator)
      || OB_ISNULL(throttle) || OB_ISNULL(palf_env_impl)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument!!!", K(ret), K(config), K(cb_thread_pool_tg_id), KP(allocator),
        KP(throttle), KP(palf_env_impl));
  } else if (OB_FAIL(queue_.init(config.io_queue_capcity_, "IOWorkerLQ", tenant_id))) {
    PALF_LOG(ERROR, "io task queue init failed", K(ret), K(config));
  } else if (OB_FAIL(batch_io_task_mgr_.init(config.batch_width_,
                                             config.batch_depth_,
                                             allocator,
                                             &wait_cost_stat_))) {
    PALF_LOG(ERROR, "BatchLogIOFlushLogTaskMgr init failed", K(ret), K(config));
  } else {
    share::ObThreadPool::set_run_wrapper(MTL_CTX());
    log_io_worker_num_ = config.io_worker_num_;
    cb_thread_pool_tg_id_ = cb_thread_pool_tg_id;
    palf_env_impl_ = palf_env_impl;
    PALF_REPORT_INFO_KV(K_(log_io_worker_num), K_(cb_thread_pool_tg_id));
    throttle_ = throttle;
    log_io_worker_queue_size_stat_.set_extra_info(EXTRA_INFOS);
    purge_throttling_task_submitted_seq_ = 0;
    purge_throttling_task_handled_seq_ = 0;
    need_ignoring_throttling_ = need_igore_throttle;
    need_purging_throttling_func_ = [this](){
      return has_purge_throttling_tasks_() > 0;
    };
    if (!need_purging_throttling_func_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "generate need_purging_throttling_func_ failed!!!", K(ret), K(config));
    } else {
      is_inited_ = true;
      PALF_LOG(INFO, "LogIOWorker init success", K(ret), K(config), K(cb_thread_pool_tg_id),
               KPC(palf_env_impl));
    }
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void LogIOWorker::destroy()
{
  (void)stop();
  (void)wait();
  is_inited_ = false;
  need_ignoring_throttling_ = false;
  purge_throttling_task_handled_seq_ = 0;
  purge_throttling_task_submitted_seq_ = 0;
  last_working_time_ = OB_INVALID_TIMESTAMP;
  throttle_ = NULL;
  cb_thread_pool_tg_id_ = -1;
  palf_env_impl_ = NULL;
  log_io_worker_num_ = -1;
  queue_.destroy();
  batch_io_task_mgr_.destroy();
}

int LogIOWorker::submit_io_task(LogIOTask *io_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(io_task)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const bool need_purge_throttling = io_task->need_purge_throttling();
    // When 'need_ignoring_throttling_' is true, we no need to advance purge_throttling_task_handled_seq_
    if (!need_ignoring_throttling_ && need_purge_throttling) {
      // Mush hold lock, otherwise:
      // 1. At T1 timestamp, alloc sequence 8 to a LogIOTask1;
      // 2. At T2 timestamp, alloc sequence 9 to a LogIOTask2;
      // 3. At T3 timestamp, push LogIOTask2 into queue_;
      // 4. At T4 timestamp, push LogIOTask1 into queue_.
      //
      // After handle_io_task_with_throttling_, we need update purge_throttling_task_handled_seq_, and the
      // value has been reduced in above case.
      //
      // If push LogIOTask into queue_ failed, rollback 'purge_throttling_task_submitted_seq_' is incorrect.
      SpinLockGuard guard(lock_);
      const int64_t submit_seq = inc_and_fetch_purge_throttling_submitted_seq_();
      (void)io_task->set_submit_seq(submit_seq);
      PALF_LOG(INFO, "submit flush meta task success", KPC(io_task));
      if (OB_FAIL(queue_.push(io_task))) {
        PALF_LOG(WARN, "fail to push io task into queue", K(ret), KP(io_task));
        dec_purge_throttling_submitted_seq_();
      }
    } else {
      if (OB_FAIL(queue_.push(io_task))) {
        PALF_LOG(WARN, "fail to push io task into queue", K(ret), KP(io_task));
      }
    }
    PALF_LOG(TRACE, "after submit_io_task", KP(io_task));
  }
  return ret;
}

int LogIOWorker::notify_need_writing_throttling(const bool &need_throttling)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!need_ignoring_throttling_) {
    (void)throttle_->notify_need_writing_throttling(need_throttling);
  }
  return  ret;
}

void LogIOWorker::run1()
{
  lib::set_thread_name("IOWorker");
  (void) run_loop_();
}

int LogIOWorker::handle_io_task_with_throttling_(LogIOTask *io_task)
{
  int ret = OB_SUCCESS;
  const int64_t throttling_size = io_task->get_io_size();
  int tmp_ret = OB_SUCCESS;
  if (!need_ignoring_throttling_
      && OB_SUCCESS != (tmp_ret = throttle_->throttling(throttling_size, need_purging_throttling_func_, palf_env_impl_))) {
    LOG_ERROR_RET(tmp_ret, "failed to do_throttling", K(throttling_size));
  }
  const int64_t submit_seq = io_task->get_submit_seq();
  if (OB_FAIL(io_task->do_task(cb_thread_pool_tg_id_, palf_env_impl_))) {
    PALF_LOG(WARN, "LogIOTask do_task falied");
  } else if (!need_ignoring_throttling_) {
    const int64_t handled_seq = ATOMIC_LOAD(&purge_throttling_task_handled_seq_);
    const int64_t submitted_seq = ATOMIC_LOAD(&purge_throttling_task_submitted_seq_);
    // NB: for LogIOFlushLogTask, the submit_seq is always be zero.
    if (submit_seq > handled_seq && submit_seq <= submitted_seq) {
      ATOMIC_SET(&purge_throttling_task_handled_seq_, submit_seq);
    }
    if (submitted_seq < handled_seq) {
      PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                   "unexpected error, purge_throttling_task_submitted_seq_ is less than purge_throttling_task_handled_seq_",
                   KPC(this));
    }
    if (OB_SUCCESS != (tmp_ret = throttle_->after_append_log(throttling_size))) {
      LOG_ERROR_RET(tmp_ret, "after_append failed", KP(io_task));
    }
    PALF_LOG(TRACE, "handle_io_task_ success", K(submit_seq), KPC(this));
  }
  return ret;
}

int LogIOWorker::handle_io_task_(LogIOTask *io_task)
{
  int ret = OB_SUCCESS;
	int64_t start_ts = ObTimeUtility::current_time();
  wait_cost_stat_.stat(start_ts - io_task->get_init_task_ts());
  if (OB_FAIL(handle_io_task_with_throttling_(io_task))) {
    io_task->free_this(palf_env_impl_);
  }
	int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
	do_task_used_ts_ += cost_ts;
	do_task_count_ ++;
	if (palf_reach_time_interval(5 * 1000 * 1000, print_log_interval_)) {
		PALF_LOG(INFO, "[PALF STAT IO STAT]", K_(do_task_used_ts), K_(do_task_count),
				"average_cost_ts", do_task_used_ts_ / do_task_count_,
				"io_queue_size", queue_.size());
		do_task_count_ = 0;
		do_task_used_ts_ = 0;
	};
  return ret;
}

int LogIOWorker::run_loop_()
{
  int ret = OB_SUCCESS;

  while (false == has_set_stop()
      && false == (OB_NOT_NULL(&lib::Thread::current()) ? lib::Thread::current().has_set_stop() : false)) {
    void *task = NULL;
    if (OB_SUCC(queue_.pop(task, QUEUE_WAIT_TIME))) {
      ATOMIC_STORE(&last_working_time_, common::ObTimeUtility::fast_current_time());
      update_throttling_options_();
      ret = reduce_io_task_(task);
      ATOMIC_STORE(&last_working_time_, OB_INVALID_TIMESTAMP);
    }
    if (queue_.size() > 0) {
      log_io_worker_queue_size_stat_.stat(queue_.size());
    }
  }

  // After IOWorker has stopped, need clear queue_.
  if (true == has_set_stop()) {
    void *task = NULL;
    ObILogAllocator *allocator = palf_env_impl_->get_log_allocator();
    CLOG_LOG(INFO, "before LogIOWorker destory", KPC(this), KPC(allocator));
    while (OB_SUCC(queue_.pop(task))) {
      LogIOTask *io_task = reinterpret_cast<LogIOTask *>(task);
      ATOMIC_STORE(&last_working_time_, common::ObTimeUtility::fast_current_time());
      (void)handle_io_task_(io_task);
      ATOMIC_STORE(&last_working_time_, OB_INVALID_TIMESTAMP);
    }
    CLOG_LOG(INFO, "after LogIOWorker destory", KPC(this), KPC(allocator));
  }
  return ret;
}

bool LogIOWorker::need_reduce_(LogIOTask *io_task)
{
  bool bool_ret = false;
  switch (io_task->get_io_task_type()) {
    case LogIOTaskType::FLUSH_LOG_TYPE:
      bool_ret = true;
      break;
    case LogIOTaskType::FLUSH_META_TYPE:
    case LogIOTaskType::TRUNCATE_PREFIX_TYPE:
    case LogIOTaskType::TRUNCATE_LOG_TYPE:
    default:
      break;
  }
  // NB: when need writing throttling, don't reduce io task.
  return bool_ret && (need_ignoring_throttling_ || !throttle_->need_writing_throttling_notified());
}

int LogIOWorker::reduce_io_task_(void *task)
{
  OB_ASSERT(true == batch_io_task_mgr_.empty());
  int ret = OB_SUCCESS;
  LogIOTask *io_task = NULL;
  bool last_io_task_has_been_reduced = true;

  // termination conditions for aggregation:
  // 1. the top LogIOTask of 'queue_' can not be aggreated
  // 2. there is no usable BatchLogIOFlushLogTask in 'batch_io_task_mgr_'.
  // 3. there is no LogIOTask in 'queue_'
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret && true == last_io_task_has_been_reduced) {
    io_task = reinterpret_cast<LogIOTask *>(task);
    BatchLogIOFlushLogTask *batch_io_flush_task = NULL;
    if (OB_ISNULL(io_task)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "io task is nullptr, unexpected error!!!", K(ret));
    } else if (false == need_reduce_(io_task)) {
      last_io_task_has_been_reduced = false;
    } else {
      LogIOFlushLogTask *flush_log_task = reinterpret_cast<LogIOFlushLogTask *>(io_task);
      // When insert 'flush_log_task' to batch_io_task_mgr_ failed, need
      // stop aggreating.
      // 1. there is no available BatchLogIOFlushLogTask in 'batch_io_task_mgr_';
      // 2. there is full in each BatchLogIOFlushLogTask in 'batch_io_task_mgr_'.
      if (OB_SUCCESS != (tmp_ret = batch_io_task_mgr_.insert(flush_log_task))) {
        last_io_task_has_been_reduced = false;
        PALF_LOG(TRACE, "batch_io_task_mgr_ insert failed", K(tmp_ret));
      } else if (OB_SUCCESS == (tmp_ret = queue_.pop(task))) {
      // When 'queue_' is empty, stop aggreating.
        update_throttling_options_();
      } else {
      }
    }
  }

  if (OB_FAIL(batch_io_task_mgr_.handle(cb_thread_pool_tg_id_, palf_env_impl_))) {
    PALF_LOG(WARN, "batch_io_task_mgr_ handle failed", K(ret), K(batch_io_task_mgr_));
  }

  if (false == last_io_task_has_been_reduced && OB_NOT_NULL(io_task)) {
    ret = handle_io_task_(io_task);
  }
  PALF_LOG(TRACE, "reduce_io_task_ finished", K(ret), K(tmp_ret), KPC(this));
  return ret;
}

int LogIOWorker::update_throttling_options_()
{
  int ret = OB_SUCCESS;
  // For sys log stream, no need to update throttling options.
  if (!need_ignoring_throttling_ && OB_FAIL(throttle_->update_throttling_options(palf_env_impl_))) {
    LOG_WARN("failed to update_throttling_options");
  }
  return ret;
}

LogIOWorker::BatchLogIOFlushLogTaskMgr::BatchLogIOFlushLogTaskMgr()
  : handle_count_(0), usable_count_(0), batch_width_(0),
    wait_cost_stat_(NULL)
{}

LogIOWorker::BatchLogIOFlushLogTaskMgr::~BatchLogIOFlushLogTaskMgr()
{
  destroy();
}

int LogIOWorker::BatchLogIOFlushLogTaskMgr::init(int64_t batch_width,
                                                 int64_t batch_depth,
                                                 ObIAllocator *allocator,
                                                 ObMiniStat::ObStatItem *wait_cost_stat)
{
  int ret = OB_SUCCESS;
  batch_io_task_array_.set_allocator(allocator);
  if (OB_FAIL(batch_io_task_array_.init(batch_width))) {
    PALF_LOG(ERROR, "batch_io_task_array_ init failed", K(ret));
  } else {
    for (int i = 0; i < batch_width  && OB_SUCC(ret); i++) {
      bool last_io_task_push_success = false;
      char *ptr = reinterpret_cast<char*>(mtl_malloc(sizeof(BatchLogIOFlushLogTask), "LogIOTask"));
      BatchLogIOFlushLogTask *io_task = NULL;
      if (NULL == ptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(ERROR, "allocate memory failed", K(ret));
      } else if (FALSE_IT(io_task = new(ptr)(BatchLogIOFlushLogTask))) {
      } else if (OB_FAIL(io_task->init(batch_depth, allocator))) {
        PALF_LOG(ERROR, "BatchLogIOFlushLogTask init failed", K(ret));
        // NB: push batch will not failed becaue batch_io_task_array_ has reserved.
      } else if (OB_FAIL(batch_io_task_array_.push_back(io_task))) {
        PALF_LOG(ERROR, "batch_io_task_array_ push_back failed", K(ret), KP(io_task));
      } else {
        last_io_task_push_success = true;
        PALF_LOG(INFO, "BatchLogIOFlushLogTask init success", K(ret), K(i),
                 KP(io_task));
      }
      if (!last_io_task_push_success && NULL != io_task) {
        io_task->destroy();
        mtl_free(io_task);
        io_task = NULL;
      }
    }
    batch_width_ = usable_count_ = batch_width;
    wait_cost_stat_ = wait_cost_stat;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void LogIOWorker::BatchLogIOFlushLogTaskMgr::destroy()
{
  handle_count_ = batch_width_ = usable_count_ = 0;
  for (int i = 0; i < batch_io_task_array_.count(); i++) {
    BatchLogIOFlushLogTask *&io_task = batch_io_task_array_[i];
    if (NULL != io_task) {
      io_task->~BatchLogIOFlushLogTask();
      mtl_free(io_task);
      io_task = NULL;
    }
  }
  wait_cost_stat_ = NULL;
  batch_io_task_array_.destroy();
}

int LogIOWorker::BatchLogIOFlushLogTaskMgr::insert(LogIOFlushLogTask *io_task)
{
  int ret = OB_SUCCESS;
  BatchLogIOFlushLogTask *batch_io_task = NULL;
  const int64_t palf_id = io_task->get_palf_id();
  if (OB_FAIL(find_usable_batch_io_task_(palf_id, batch_io_task))) {
    PALF_LOG(WARN, "find_usable_batch_io_task_ failed", K(ret), K(palf_id));
  } else if (OB_FAIL(batch_io_task->push_back(io_task))) {
    PALF_LOG(TRACE, "push_back failed", K(ret), K(palf_id), KPC(io_task));
  } else {
  }
  return ret;
}

int LogIOWorker::BatchLogIOFlushLogTaskMgr::handle(const int64_t tg_id, IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  const int64_t count = batch_io_task_array_.count() - usable_count_;
  // Each BatchLogIOFlushLogTask is a set LogIOFlushLogTask of one palf instance,
  // even if execute 'do_task_' for one of LogIOFlushLogTask failed, we need
  // execute 'do_task_' for next LogIOFlushLogTask.
  const int64_t first_handle_ts = ObTimeUtility::fast_current_time();
  for (int64_t i = 0; i < count; i++) {
    BatchLogIOFlushLogTask *io_task = batch_io_task_array_[i];
    if (OB_ISNULL(io_task)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "BatchLogIOFlushLogTask in batch_io_task_array_ is nullptr, unexpected error!!!",
               K(ret), KP(io_task), K(i));
    } else if (OB_FAIL(statistics_wait_cost_(first_handle_ts, io_task))) {
      PALF_LOG(WARN, "do statistics failed", K(ret));
    } else if (OB_FAIL(io_task->do_task(tg_id, palf_env_impl))) {
      PALF_LOG(WARN, "do_task failed", K(ret), KP(io_task));
    } else {
      if (OB_NOT_NULL(wait_cost_stat_)) {
        wait_cost_stat_->stat(io_task->get_count(), io_task->get_accum_in_queue_time());
      }
      io_task->reset_accum_in_queue_time();
      PALF_LOG(TRACE, "BatchLogIOFlushLogTaskMgr::handle success", K(ret), K(handle_count_),
          KP(io_task));
    }
    if (OB_NOT_NULL(io_task)) {
      // 'handle_count_' used for statistics
      handle_count_ += io_task->get_count();
      io_task->reuse();
      usable_count_++;
    }
  }
  return ret;
}

bool LogIOWorker::BatchLogIOFlushLogTaskMgr::empty()
{
  return usable_count_ == batch_width_;
}

int LogIOWorker::BatchLogIOFlushLogTaskMgr::find_usable_batch_io_task_(
    const int64_t palf_id, BatchLogIOFlushLogTask *&batch_io_task)
{
  int ret = OB_SUCCESS;
  const int64_t count = batch_io_task_array_.count();
  bool found = false;
  // 1. check whether the same palf id already exist in 'batch_io_task_array_',
  // and get the BatchLogIOFlushLogTask in 'batch_io_task_array_' which has the
  // same palf id.
  for (int64_t i = 0; i < count && false == found; i++) {
    BatchLogIOFlushLogTask *tmp_task = batch_io_task_array_[i];
    if (OB_ISNULL(tmp_task)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "unexpected error, tmp_task is nullptr", K(ret), KP(tmp_task), K(i));
    } else if (palf_id == tmp_task->get_palf_id()) {
      found = true;
      batch_io_task = tmp_task;
    }
  }
  // 2. if there is no same palf id in 'batch_io_task_array_', get the first
  // available BatchLogIOFlushLogTask in 'batch_io_task_array_'.
  for (int64_t i = 0; usable_count_ > 0 && i < count && false == found; i++) {
    BatchLogIOFlushLogTask *tmp_task = batch_io_task_array_[i];
    if (OB_ISNULL(tmp_task)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "unexpected error, tmp_task is nullptr", K(ret), KP(tmp_task), K(i));
    } else if (INVALID_PALF_ID == tmp_task->get_palf_id()) {
      found = true;
      batch_io_task = tmp_task;
      usable_count_--;
    }
  }
  PALF_LOG(TRACE, "find_usable_batch_io_task_ finished", K(ret), K(usable_count_), K(count), KP(this));
  if (OB_SUCC(ret)) {
    ret = true == found ? OB_SUCCESS : OB_SIZE_OVERFLOW;
  }
  return ret;
}

int64_t LogIOWorker::inc_and_fetch_purge_throttling_submitted_seq_()
{
  return ATOMIC_AAF(&purge_throttling_task_submitted_seq_, 1);
}

void LogIOWorker::dec_purge_throttling_submitted_seq_()
{
  ATOMIC_DEC(&purge_throttling_task_submitted_seq_);
}

bool LogIOWorker::has_purge_throttling_tasks_() const
{
  const int64_t handled_seq = ATOMIC_LOAD(&purge_throttling_task_handled_seq_);
  const int64_t submitted_seq = ATOMIC_LOAD(&purge_throttling_task_submitted_seq_);
  if (submitted_seq < handled_seq) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                 "unexpected error, purge_throttling_task_submitted_seq_ is less than purge_throttling_task_handled_seq_",
                 KPC(this));
  }
  return submitted_seq != handled_seq;
}

int LogIOWorker::BatchLogIOFlushLogTaskMgr::statistics_wait_cost_(int64_t first_handle_ts, BatchLogIOFlushLogTask *batch_io_task)
{
  int ret = OB_SUCCESS;
  BatchLogIOFlushLogTask::BatchIOTaskArray batch_io_task_array;
  batch_io_task->get_io_task_array(batch_io_task_array);
  int64_t total_wait_cost = 0;
  int64_t cnt = batch_io_task_array.count();
  for (int64_t i = 0; i < cnt; i++) {
    LogIOFlushLogTask *io_flush_task = batch_io_task_array[i];
    if (OB_ISNULL(io_flush_task)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "io_flush_task is nullptr, unpexected error", K(ret), KP(io_flush_task), KPC(this));
      break;
    } else {
      int64_t init_task_ts = io_flush_task->get_init_task_ts();
      total_wait_cost += first_handle_ts - init_task_ts;
    }
  }
  if (OB_SUCC(ret) && cnt > 0) {
    wait_cost_stat_->stat(cnt, total_wait_cost);
  }
  return ret;
}

} // end namespace palf
} // end namespace oceanbase
