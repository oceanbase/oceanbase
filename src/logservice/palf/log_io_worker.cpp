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
#include "lib/thread/ob_thread_name.h"        // set_thread_name
#include "share/rc/ob_tenant_base.h"          // mtl_free
#include "share/ob_throttling_utils.h"        //ObThrottlingUtils
#include "log_io_task.h"                      // LogIOTask
#include "palf_env_impl.h"                    // PalfEnvImpl

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
void LogThrottlingStat::reset()
{
  start_ts_ = OB_INVALID_TIMESTAMP;
  stop_ts_ = OB_INVALID_TIMESTAMP;
  total_throttling_interval_ = 0;
  total_throttling_size_ = 0;
  total_throttling_task_cnt_ = 0;
  total_skipped_size_ = 0;
  total_skipped_task_cnt_ = 0;
  max_throttling_interval_ = 0;
}
void LogWritingThrottle::reset()
{
  last_update_ts_ = OB_INVALID_TIMESTAMP;
  need_writing_throttling_notified_ = false;
  ATOMIC_SET(&submitted_seq_, 0);
  ATOMIC_SET(&handled_seq_, 0);
  appended_log_size_cur_round_ = 0;
  decay_factor_ = 0;
  throttling_options_.reset();
  stat_.reset();
}

int LogWritingThrottle::update_throttling_options(IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(palf_env_impl)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("palf_env_impl is NULL", KPC(this));
  } else {
    int64_t cur_ts = ObClockGenerator::getClock();
    bool unused_has_freed_up_space = false;
    if ((cur_ts > last_update_ts_ + UPDATE_INTERVAL_US)
        && OB_FAIL(update_throtting_options_(palf_env_impl, unused_has_freed_up_space))) {
      LOG_WARN("failed to update_throttling_info", KPC(this));
    }
  }
  return ret;
}

int LogWritingThrottle::throttling(const int64_t throttling_size, IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(palf_env_impl)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (throttling_size < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid throttling_size", K(throttling_size), KPC(this));
  } else if (0 == throttling_size) {
    //no need throttling
  } else {
    if (need_throttling_()) {
      const int64_t cur_unrecyclable_size = throttling_options_.unrecyclable_disk_space_ + appended_log_size_cur_round_;
      const int64_t trigger_base_log_disk_size = throttling_options_.total_disk_space_ * throttling_options_.trigger_percentage_ /  100;
      int64_t time_interval = 0;
      if (OB_FAIL(ObThrottlingUtils::get_throttling_interval(THROTTLING_CHUNK_SIZE, throttling_size, trigger_base_log_disk_size,
                                                             cur_unrecyclable_size, decay_factor_, time_interval))) {
        LOG_WARN("failed to get_throttling_interval", KPC(this));
      }
      int64_t remain_interval_us = time_interval;
      bool has_freed_up_space = false;
      while (OB_SUCC(ret) && remain_interval_us > 0) {
        const int64_t real_interval = MIN(remain_interval_us, DETECT_INTERVAL_US);
        usleep(real_interval);
        remain_interval_us -= real_interval;
        if (remain_interval_us <= 0) {
          //do nothing
        } else if (OB_FAIL(update_throtting_options_(palf_env_impl, has_freed_up_space))) {
          LOG_WARN("failed to update_throttling_info_", KPC(this), K(time_interval), K(remain_interval_us));
        } else if (!need_throttling_() || has_freed_up_space) {
          LOG_TRACE("no need throttling or log disk has been freed up", KPC(this), K(time_interval), K(remain_interval_us));
          break;
        }
      }
      stat_.after_throttling(time_interval - remain_interval_us, throttling_size);
    } else if (need_throttling_with_options_()) {
      stat_.after_throttling(0, throttling_size);
    }

    if (stat_.has_ever_throttled()) {
      if (REACH_TIME_INTERVAL(2 * 1000 * 1000L)) {
         PALF_LOG(INFO, "[LOG DISK THROTTLING] [STAT]", KPC(this));
      }
    }
  }
  return ret;
}

int LogWritingThrottle::after_append_log(const int64_t log_size, const int64_t seq)
{
  appended_log_size_cur_round_ += log_size;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(log_size < 0 || seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(seq), K(log_size));
  } else if (seq > 0) {
    if (seq > ATOMIC_LOAD(&handled_seq_) && seq <= ATOMIC_LOAD(&submitted_seq_)) {
      ATOMIC_SET(&handled_seq_, seq);
    } else {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "unexpected seq", KPC(this), K(seq));
    }
  } else {/*do nothing*/}
  return ret;
}

int LogWritingThrottle::update_throtting_options_(IPalfEnvImpl *palf_env_impl, bool &has_freed_up_space)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = ObClockGenerator::getClock();
  if (ATOMIC_LOAD(&need_writing_throttling_notified_)) {
    PalfThrottleOptions new_throttling_options;
    if (OB_FAIL(palf_env_impl->get_throttling_options(new_throttling_options))) {
      PALF_LOG(WARN, "failed to get_writing_throttling_option");
    } else if (OB_UNLIKELY(!new_throttling_options.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(WARN, "options is invalid", K(new_throttling_options), KPC(this));
    } else {
      const bool need_throttling = new_throttling_options.need_throttling();
      const int64_t new_available_size_after_limit = new_throttling_options.get_available_size_after_limit();
      bool need_update_decay_factor = false;
      bool need_start_throttling = false;

      if (need_throttling) {
        if (!throttling_options_.need_throttling()) {
          need_start_throttling = true;
          need_update_decay_factor = true;
        } else {
          need_update_decay_factor = (throttling_options_.get_available_size_after_limit() != new_available_size_after_limit);
        }
        if (need_update_decay_factor) {
          if (OB_FAIL(ObThrottlingUtils::calc_decay_factor(new_available_size_after_limit, THROTTLING_DURATION_US,
                  THROTTLING_CHUNK_SIZE, decay_factor_))) {
            PALF_LOG(ERROR, "failed to calc_decay_factor", K(throttling_options_), "duration(s)",
                     THROTTLING_DURATION_US / (1000 * 1000), K(THROTTLING_CHUNK_SIZE));
          } else {
            PALF_LOG(INFO, "[LOG DISK THROTTLING] success to calc_decay_factor", K(decay_factor_), K(throttling_options_),
                     K(new_throttling_options), "duration(s)", THROTTLING_DURATION_US / (1000 * 1000L), K(THROTTLING_CHUNK_SIZE));
          }
        }

        if (OB_SUCC(ret)) {
          // update other field
          has_freed_up_space = new_throttling_options.unrecyclable_disk_space_ < throttling_options_.unrecyclable_disk_space_;
          bool has_unrecyclable_space_changed = new_throttling_options.unrecyclable_disk_space_ != throttling_options_.unrecyclable_disk_space_;
          if (has_unrecyclable_space_changed || need_start_throttling) {
            // reset appended_log_size_cur_round_ when unrecyclable_disk_space_ changed
            appended_log_size_cur_round_ = 0;
          }
          throttling_options_ = new_throttling_options;
          if (need_start_throttling) {
            stat_.start_throttling();
            PALF_LOG(INFO, "[LOG DISK THROTTLING] [START]", KPC(this),
            "duration(s)", THROTTLING_DURATION_US / (1000 * 1000L), K(THROTTLING_CHUNK_SIZE));
          }
        }
      } else {
        if (throttling_options_.need_throttling()) {
          PALF_LOG(INFO, "[LOG DISK THROTTLING] [STOP]", KPC(this),
          "duration(s)", THROTTLING_DURATION_US / (1000 * 1000L), K(THROTTLING_CHUNK_SIZE));
          clean_up_();
          stat_.stop_throttling();
        }
      }
    }
  } else {
    if (throttling_options_.need_throttling()) {
      PALF_LOG(INFO, "[LOG DISK THROTTLING] [STOP] no need throttling any more", KPC(this),
               "duration(s)", THROTTLING_DURATION_US / (1000 * 1000L), K(THROTTLING_CHUNK_SIZE));
      clean_up_();
      stat_.stop_throttling();
    }
  }
  if (OB_SUCC(ret)) {
    last_update_ts_ = cur_ts;
  }
  return ret;
}

void LogWritingThrottle::clean_up_()
{
  //do not reset submitted_seq_  && handled_seq_ && last_update_ts_ && stat_
  appended_log_size_cur_round_ = 0;
  decay_factor_ = 0;
  throttling_options_.reset();
}

LogIOWorker::LogIOWorker()
    : log_io_worker_num_(-1),
      cb_thread_pool_tg_id_(-1),
      palf_env_impl_(NULL),
      do_task_used_ts_(0),
      do_task_count_(0),
      print_log_interval_(OB_INVALID_TIMESTAMP),
      last_working_time_(OB_INVALID_TIMESTAMP),
      log_io_worker_queue_size_stat_("[PALF STAT LOG IO WORKER QUEUE SIZE]", PALF_STAT_PRINT_INTERVAL_US),
      is_inited_(false)
{
}

LogIOWorker::~LogIOWorker()
{
  destroy();
}

int LogIOWorker::init(const LogIOWorkerConfig &config,
                      const int64_t tenant_id,
                      int cb_thread_pool_tg_id,
                      ObIAllocator *allocator,
                      IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogIOWorker has been inited", K(ret));
  } else if (false == config.is_valid() || 0 >= cb_thread_pool_tg_id || OB_ISNULL(allocator)
      || OB_ISNULL(palf_env_impl)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument!!!", K(ret), K(config), K(cb_thread_pool_tg_id), KP(allocator),
        KP(palf_env_impl));
  } else if (OB_FAIL(queue_.init(config.io_queue_capcity_, "IOWorkerLQ", tenant_id))) {
    PALF_LOG(ERROR, "io task queue init failed", K(ret), K(config));
  } else if (OB_FAIL(batch_io_task_mgr_.init(config.batch_width_,
                                             config.batch_depth_,
                                             allocator))) {
    PALF_LOG(ERROR, "BatchLogIOFlushLogTaskMgr init failed", K(ret), K(config));
  } else {
    share::ObThreadPool::set_run_wrapper(MTL_CTX());
    log_io_worker_num_ = config.io_worker_num_;
    cb_thread_pool_tg_id_ = cb_thread_pool_tg_id;
    palf_env_impl_ = palf_env_impl;
    PALF_REPORT_INFO_KV(K_(log_io_worker_num), K_(cb_thread_pool_tg_id));
    log_io_worker_queue_size_stat_.set_extra_info(EXTRA_INFOS);
    is_inited_ = true;
    PALF_LOG(INFO, "LogIOWorker init success", K(ret), K(config), K(cb_thread_pool_tg_id),
             KPC(palf_env_impl));
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
  last_working_time_ = OB_INVALID_TIMESTAMP;
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
    if (need_purge_throttling) {
      ObSpinLockGuard guard(throttling_lock_);
      const int64_t submit_seq = throttle_.inc_and_fetch_submitted_seq();
      (void)io_task->set_submit_seq(submit_seq);
      PALF_LOG(INFO, "submit flush meta task success", KPC(io_task));
      if (OB_FAIL(queue_.push(io_task))) {
        PALF_LOG(WARN, "fail to push io task into queue", K(ret), KP(io_task));
        //rollback submit_seq
        (void)throttle_.dec_submitted_seq();
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
  } else {
    (void)throttle_.notify_need_writing_throttling(need_throttling);
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
  if (OB_SUCCESS != (tmp_ret = throttle_.throttling(throttling_size, palf_env_impl_))) {
    LOG_ERROR_RET(tmp_ret, "failed to do_throttling", K(throttling_size));
  }
  const int64_t submit_seq = io_task->get_submit_seq();
  if (OB_FAIL(io_task->do_task(cb_thread_pool_tg_id_, palf_env_impl_))) {
    PALF_LOG(WARN, "LogIOTask do_task falied");
  } else {
    if (OB_SUCCESS != (tmp_ret = throttle_.after_append_log(throttling_size, submit_seq))) {
      LOG_ERROR_RET(tmp_ret, "after_append failed", KP(io_task));
    }
    PALF_LOG(TRACE, "handle_io_task_ success", K(submit_seq));
  }
  return ret;
}

int LogIOWorker::handle_io_task_(LogIOTask *io_task)
{
  int ret = OB_SUCCESS;
	int64_t start_ts = ObTimeUtility::current_time();

  if (OB_FAIL(handle_io_task_with_throttling_(io_task))) {
    io_task->free_this(palf_env_impl_);
  }
	int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
	do_task_used_ts_ += cost_ts;
	do_task_count_ ++;
	if (palf_reach_time_interval(5 * 1000 * 1000, print_log_interval_)) {
		PALF_EVENT("io statistics", 0, K_(do_task_used_ts), K_(do_task_count),
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
  //do not reduce io when writing throttling is on
  return (bool_ret && (!throttle_.need_writing_throttling_notified()));
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
        PALF_LOG(WARN, "batch_io_task_mgr_ insert failed", K(tmp_ret));
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
    io_task = reinterpret_cast<LogIOFlushLogTask *>(io_task);
    ret = handle_io_task_(io_task);
  }
  PALF_LOG(TRACE, "reduce_io_task_ finished", K(ret), K(tmp_ret), KPC(this));
  return ret;
}

int LogIOWorker::update_throttling_options_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(throttle_.update_throttling_options(palf_env_impl_))) {
    LOG_WARN("failed to update_throttling_options");
  }
  return ret;
}

LogIOWorker::BatchLogIOFlushLogTaskMgr::BatchLogIOFlushLogTaskMgr()
  : handle_count_(0), has_batched_size_(0), usable_count_(0), batch_width_(0)
{}

LogIOWorker::BatchLogIOFlushLogTaskMgr::~BatchLogIOFlushLogTaskMgr()
{
  destroy();
}

int LogIOWorker::BatchLogIOFlushLogTaskMgr::init(int64_t batch_width,
                                                 int64_t batch_depth,
                                                 ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  batch_io_task_array_.set_allocator(allocator);
  if (OB_FAIL(batch_io_task_array_.init(batch_width))) {
    PALF_LOG(ERROR, "batch_io_task_array_ init failed", K(ret));
  } else {
    for (int i = 0; i < batch_width  && OB_SUCC(ret); i++) {
      char *ptr = reinterpret_cast<char*>(mtl_malloc(sizeof(BatchLogIOFlushLogTask), "LogIOTask"));
      BatchLogIOFlushLogTask *io_task = NULL;
      if (NULL == ptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(ERROR, "allocate memory failed", K(ret));
      } else if (FALSE_IT(io_task = new(ptr)(BatchLogIOFlushLogTask))) {
      } else if (OB_FAIL(io_task->init(batch_depth, allocator))) {
        PALF_LOG(ERROR, "BatchLogIOFlushLogTask init failed", K(ret));
      } else if (OB_FAIL(batch_io_task_array_.push_back(io_task))) {
        PALF_LOG(ERROR, "batch_io_task_array_ push_back failed", K(ret), KP(io_task));
      } else {
        PALF_LOG(INFO, "BatchLogIOFlushLogTask init success", K(ret), K(i),
                 KP(io_task));
      }
    }
    batch_width_ = usable_count_ = batch_width;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void LogIOWorker::BatchLogIOFlushLogTaskMgr::destroy()
{
  handle_count_ = has_batched_size_ = batch_width_ = usable_count_ = 0;
  for (int i = 0; i < batch_io_task_array_.count(); i++) {
    BatchLogIOFlushLogTask *&io_task = batch_io_task_array_[i];
    if (NULL != io_task) {
      io_task->~BatchLogIOFlushLogTask();
      mtl_free(io_task);
      io_task = NULL;
    }
  }
}

int LogIOWorker::BatchLogIOFlushLogTaskMgr::insert(LogIOFlushLogTask *io_task)
{
  int ret = OB_SUCCESS;
  BatchLogIOFlushLogTask *batch_io_task = NULL;
  const int64_t palf_id = io_task->get_palf_id();
  if (OB_FAIL(find_usable_batch_io_task_(palf_id, batch_io_task))) {
    PALF_LOG(WARN, "find_usable_batch_io_task_ failed", K(ret), K(palf_id));
  } else if (OB_FAIL(batch_io_task->push_back(io_task))) {
    PALF_LOG(ERROR, "batch_io_task must have enouch space to hold io_task, unexpected error!!!",
             K(ret), KP(batch_io_task));
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
  for (int64_t i = 0; i < count; i++) {
    BatchLogIOFlushLogTask *io_task = batch_io_task_array_[i];
    if (OB_ISNULL(io_task)) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "BatchLogIOFlushLogTask in batch_io_task_array_ is nullptr, unexpected error!!!",
               K(ret), KP(io_task), K(i));
    } else if (OB_FAIL(io_task->do_task(tg_id, palf_env_impl))) {
      PALF_LOG(WARN, "do_task failed", K(ret), KP(io_task));
    } else {
      PALF_LOG(TRACE, "BatchLogIOFlushLogTaskMgr::handle success", K(ret), K(has_batched_size_), KP(io_task));
    }
    if (OB_NOT_NULL(io_task)) {
      // 'handle_count_' and 'has_batched_size_' are used for statistics
      handle_count_ += io_task->get_count() <= 1 ? 0 : 1;
      has_batched_size_ += io_task->get_count() == 1 ? 0 : io_task->get_count();
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
} // end namespace palf
} // end namespace oceanbase
