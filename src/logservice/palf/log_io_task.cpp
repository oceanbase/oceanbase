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

#include "logservice/palf/log_io_task.h"
#include "lib/ob_errno.h"
#include "lib/alloc/alloc_assist.h" // MEMCPY
#include "share/ob_thread_define.h" // TGDefIDS
#include "share/ob_thread_mgr.h"    // TG_START
#include "share/rc/ob_tenant_base.h"// mtl_free
#include "log_io_task_cb_utils.h"   // LogFlushCbCtx...
#include "palf_handle_impl.h"       // PalfHandleImpl
#include "log_engine.h"             // LogEngine
#include "palf_handle_impl_guard.h" // PalfHandleImplGuard
#include "palf_env_impl.h"          // IPalfEnvImpl

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{

int push_task_into_cb_thread_pool(const int64_t tg_id, LogIOTask *io_task)
{
  int ret = OB_SUCCESS;
  if (-1 == tg_id || NULL == io_task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t print_log_interval = OB_INVALID_TIMESTAMP;
    while (OB_FAIL(TG_PUSH_TASK(tg_id, io_task))) {
      if (OB_IN_STOP_STATE == ret) {
        PALF_LOG(WARN, "thread_pool has been stopped, skip io_task", K(ret), K(tg_id));
        break;
      } else if (palf_reach_time_interval(5 * 1000 * 1000, print_log_interval)) {
        PALF_LOG(ERROR, "push io task failed", K(ret), K(tg_id));
      }
      ob_usleep(1000);
    }
  }
  return ret;
}

LogIOTask::LogIOTask(const int64_t palf_id, const int64_t palf_epoch)
{
	reset();
	palf_id_ = palf_id;
	palf_epoch_ = palf_epoch;
	init_task_ts_ = ObTimeUtility::current_time();
  submit_seq_ = 0;
}

LogIOTask::~LogIOTask()
{
	reset();
}

void LogIOTask::reset()
{
	palf_id_ = INVALID_PALF_ID;
	palf_epoch_ = -1;
	init_task_ts_ = OB_INVALID_TIMESTAMP;
  push_cb_into_cb_pool_ts_ = OB_INVALID_TIMESTAMP;
  submit_seq_ = 0;
}

// NB: if do_task failed, the caller(LogIOWorker) is responsible for freeing LogIOTask.
int LogIOTask::do_task(int tg_id, IPalfEnvImpl *palf_env_impl)
{
	int ret = OB_SUCCESS;
	int64_t do_task_ts = ObTimeUtility::current_time();
  const int64_t delay_ts = do_task_ts - init_task_ts_;
	constexpr int64_t MAX_DELAY_TIME = 100 * 1000;
	IPalfHandleImplGuard guard;
	int64_t palf_epoch = -1;
	if (delay_ts >= MAX_DELAY_TIME) {
		PALF_LOG(INFO, "[io delay]", K(do_task_ts), K(delay_ts));
	}
	if (OB_FAIL(palf_env_impl->get_palf_handle_impl(palf_id_, guard))) {
	  PALF_LOG(WARN, "get_palf_handle_impl failed", KPC(this));
	} else if (OB_FAIL(guard.get_palf_handle_impl()->get_palf_epoch(palf_epoch))) {
	  PALF_LOG(WARN, "get_palf_epoch failed", KPC(this));
	} else if (palf_epoch != palf_epoch_) {
	  ret = OB_STATE_NOT_MATCH;
	  PALF_LOG(WARN, "palf_epoch has been changed, drop task", KPC(this), K(palf_epoch));
	} else if (OB_FAIL(do_task_(tg_id, guard))) {
		PALF_LOG(WARN, "do_task_ failed", K(ret), K(tg_id), KPC(palf_env_impl));
	} else {}
	return ret;
}

// NB: after after_consume, the caller(LogIOCb) needs free LogIOTask.
int LogIOTask::after_consume(IPalfEnvImpl *palf_env_impl)
{
	int ret = OB_SUCCESS;
	int64_t after_consume_ts = ObTimeUtility::current_time();
  const int64_t delay_ts = after_consume_ts - push_cb_into_cb_pool_ts_;
  int64_t palf_epoch = -1;
  IPalfHandleImplGuard guard;
	constexpr int64_t MAX_DELAY_TIME = 100 * 1000;
	if (delay_ts >= MAX_DELAY_TIME) {
		PALF_LOG(INFO, "[io delay]", K(after_consume_ts), K(delay_ts));
	}
  if (OB_FAIL(palf_env_impl->get_palf_handle_impl(palf_id_, guard))) {
    PALF_LOG(WARN, "IPalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id_));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->get_palf_epoch(palf_epoch))) {
    PALF_LOG(WARN, "IPalfEnvImpl get_palf_epoch failed", K(ret), K(palf_id_));
  } else if (palf_epoch != palf_epoch_) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "palf_epoch has changed, drop task", K(ret), KPC(this));
  } else if (OB_FAIL(after_consume_(guard))) {
		PALF_LOG(WARN, "after_consume_ failed", K(ret), KPC(palf_env_impl));
	} else {}
	return ret;
}

LogIOTaskType LogIOTask::get_io_task_type()
{
	return get_io_task_type_();
}

bool LogIOTask::need_purge_throttling()
{
  return need_purge_throttling_();
}

int64_t LogIOTask::get_init_task_ts()
{
  return init_task_ts_;
}

void LogIOTask::free_this(IPalfEnvImpl *palf_env_impl)
{
  return free_this_(palf_env_impl);
}
int64_t LogIOTask::get_io_size()
{
  return get_io_size_();
}
int LogIOTask::push_task_into_cb_thread_pool_(const int64_t tg_id, LogIOTask *io_task)
{
  int ret = OB_SUCCESS;
  push_cb_into_cb_pool_ts_ = ObTimeUtility::current_time();
  return push_task_into_cb_thread_pool(tg_id, io_task);
}

LogIOFlushLogTask::LogIOFlushLogTask(const int64_t palf_id, const int64_t palf_epoch)
	: LogIOTask(palf_id, palf_epoch), flush_log_cb_ctx_(), write_buf_(), is_inited_(false)
{}

LogIOFlushLogTask::~LogIOFlushLogTask()
{
  destroy();
}

int LogIOFlushLogTask::init(const FlushLogCbCtx &flush_log_cb_ctx,
                            const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogIOFlushLogTask has inited", K(ret));
  } else if (false == flush_log_cb_ctx.is_valid() || false == write_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invaild arguments!!!", K(ret), K(write_buf), K(palf_id_), K(palf_epoch_));
  } else {
    flush_log_cb_ctx_ = flush_log_cb_ctx;
    write_buf_ = write_buf;
    is_inited_ = true;
    PALF_LOG(TRACE, "LogIOFlushLogTask init success", K(ret), K(flush_log_cb_ctx_), K(write_buf_));
  }
  return ret;
}

void LogIOFlushLogTask::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    write_buf_.reset();
    flush_log_cb_ctx_.reset();
    PALF_LOG(TRACE, "LogIOFlushLogTask destroy", KP(this));
  }
}

int LogIOFlushLogTask::do_task_(int tg_id, IPalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;
  const LSN flush_log_end_lsn = flush_log_cb_ctx_.lsn_ + flush_log_cb_ctx_.total_len_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOFlusLoghTask not inited", K(ret));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->inner_append_log(
                 flush_log_cb_ctx_.lsn_, write_buf_, flush_log_cb_ctx_.scn_))) {
    PALF_LOG(ERROR, "LogEngine pwrite failed", K(ret), K(write_buf_));
    // Advance reuse lsn for group_buffer firstly, then callback asynchronous.
  } else if (OB_FAIL(guard.get_palf_handle_impl()->advance_reuse_lsn(flush_log_end_lsn))) {
    PALF_LOG(ERROR, "advance_reuse_lsn failed", K(ret), K(flush_log_end_lsn), K_(flush_log_cb_ctx));
  } else if (OB_FAIL(push_task_into_cb_thread_pool_(tg_id, this))) {
    PALF_LOG(WARN, "push_task_into_cb_thread_pool failed", K(ret), K(tg_id), KP(this));
  } else {
  }
  return ret;
}

int LogIOFlushLogTask::after_consume_(IPalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard time_guard("after_consume log", 10 * 1000);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOFlushLogTask not inited", K(ret), KPC(this));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->inner_after_flush_log(flush_log_cb_ctx_))) {
    PALF_LOG(WARN, "PalfHandleImpl after_flush_log failed", K(ret));
  } else {
    PALF_LOG(TRACE, "LogIOFlushLogTask after_consume success", K(time_guard));
  }
  return ret;
}

void LogIOFlushLogTask::free_this_(IPalfEnvImpl *palf_env_impl)
{
  palf_env_impl->get_log_allocator()->free_log_io_flush_log_task(this);
}

int64_t LogIOFlushLogTask::get_io_size_() const
{
  return write_buf_.get_total_size();
}

LogIOTruncateLogTask::LogIOTruncateLogTask(const int64_t palf_id, const int64_t palf_epoch)
	: LogIOTask(palf_id, palf_epoch), truncate_log_cb_ctx_(), is_inited_(false)
{}

LogIOTruncateLogTask::~LogIOTruncateLogTask()
{
  destroy();
}

int LogIOTruncateLogTask::init(const TruncateLogCbCtx &truncate_log_cb_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (false == truncate_log_cb_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    truncate_log_cb_ctx_ = truncate_log_cb_ctx;
    is_inited_ = true;
  }
  return ret;
}

void LogIOTruncateLogTask::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
  }
}

int LogIOTruncateLogTask::do_task_(int tg_id, IPalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(guard.get_palf_handle_impl()->inner_truncate_log(truncate_log_cb_ctx_.lsn_))) {
    PALF_LOG(WARN, "PalfHandleImpl inner_truncate_log failed", K(ret), K(palf_id_),
             K_(truncate_log_cb_ctx));
  } else if (OB_FAIL(push_task_into_cb_thread_pool_(tg_id, this))) {
    PALF_LOG(WARN, "push_task_into_cb_thread_pool failed", K(ret), K(tg_id), KP(this));
  } else {
  }
  return ret;
}

int LogIOTruncateLogTask::after_consume_(IPalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;
  int64_t palf_epoch = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogITruncateLogTask not inited!!!", K(ret));
  } else if (OB_FAIL(
                 guard.get_palf_handle_impl()->inner_after_truncate_log(truncate_log_cb_ctx_))) {
    PALF_LOG(WARN, "PalfHandleImpl inner_after_truncate_log failed", K(ret));
  } else {
  }
  return ret;
}

void LogIOTruncateLogTask::free_this_(IPalfEnvImpl *palf_env_impl)
{
  palf_env_impl->get_log_allocator()->free_log_io_truncate_log_task(this);
}

LogIOFlushMetaTask::LogIOFlushMetaTask(const int64_t palf_id, const int64_t palf_epoch) :
	LogIOTask(palf_id, palf_epoch),
	flush_meta_cb_ctx_(),
  buf_(NULL),
  buf_len_(0),
  is_inited_(false)
{
}

LogIOFlushMetaTask::~LogIOFlushMetaTask()
{
  destroy();
}

int LogIOFlushMetaTask::init(const FlushMetaCbCtx &flush_meta_cb_ctx,
                             const char *buf,
                             const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogIOFlushMetaTask has inited!!!", K(ret));
  } else if (false == flush_meta_cb_ctx.is_valid()
             || NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(flush_meta_cb_ctx),
        KP(buf), K(buf_len));
  } else {
    flush_meta_cb_ctx_ = flush_meta_cb_ctx;
		buf_ = buf;
    buf_len_ = buf_len;
    is_inited_ = true;
  }
  return ret;
}

void LogIOFlushMetaTask::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    buf_len_ = 0;
    if (NULL != buf_) {
      mtl_free(const_cast<char*>(buf_));
      buf_ = NULL;
    }
    flush_meta_cb_ctx_.reset();
  }
}

int LogIOFlushMetaTask::do_task_(int tg_id, IPalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOFlushMetaTask not inited!!!", K(ret));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->inner_append_meta(buf_, buf_len_))) {
    PALF_LOG(ERROR, "PalfHandleImpl inner_append_meta failed", K(ret), K(palf_id_));
  } else if (OB_FAIL(push_task_into_cb_thread_pool_(tg_id, this))) {
    PALF_LOG(WARN, "push_task_into_cb_thread_pool failed", K(ret), K(tg_id), KP(this));
  } else {
  }
  return ret;
}

int LogIOFlushMetaTask::after_consume_(IPalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOFlushMetaTask not inited!!!", K(ret));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->inner_after_flush_meta(flush_meta_cb_ctx_))) {
    PALF_LOG(WARN, "PalfHandleImpl after_flush_meta failed", K(ret), KP(this));
  } else {
  }
  return ret;
}

void LogIOFlushMetaTask::free_this_(IPalfEnvImpl *palf_env_impl)
{
  palf_env_impl->get_log_allocator()->free_log_io_flush_meta_task(this);
}

LogIOTruncatePrefixBlocksTask::LogIOTruncatePrefixBlocksTask(const int64_t palf_id, const int64_t palf_epoch)
    : LogIOTask(palf_id, palf_epoch), truncate_prefix_blocks_ctx_(), is_inited_(false)
{}

LogIOTruncatePrefixBlocksTask::~LogIOTruncatePrefixBlocksTask()
{
  destroy();
}

int LogIOTruncatePrefixBlocksTask::init(const TruncatePrefixBlocksCbCtx &truncate_prefix_blocks_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogIOTruncatePrefixBlocksTask has inited!!!", K(ret));
  } else if (false == truncate_prefix_blocks_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(truncate_prefix_blocks_ctx));
  } else {
    truncate_prefix_blocks_ctx_ = truncate_prefix_blocks_ctx;
    is_inited_ = true;
  }
  return ret;
}

void LogIOTruncatePrefixBlocksTask::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    truncate_prefix_blocks_ctx_.reset();
    palf_id_ = -1;
  }
}

int LogIOTruncatePrefixBlocksTask::do_task_(int tg_id, IPalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOTruncatePrefixBlocksTask not inited!!!", K(ret));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->inner_truncate_prefix_blocks(
                 truncate_prefix_blocks_ctx_.lsn_))) {
    PALF_LOG(ERROR, "PalfHandleImpl inner_truncate_prefix_blocks failed", K(ret), K(palf_id_));
  } else if (OB_FAIL(push_task_into_cb_thread_pool_(tg_id, this))) {
    PALF_LOG(ERROR, "push_task_into_cb_thread_pool_ failed", K(ret), K(tg_id), KP(this));
  } else {
  }
  return ret;
}

int LogIOTruncatePrefixBlocksTask::after_consume_(IPalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOFlushMetaTask not inited!!!", K(ret));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->inner_after_truncate_prefix_blocks(
                 truncate_prefix_blocks_ctx_))) {
    PALF_LOG(WARN, "PalfHandleImpl inner_after_truncate_prefix_blocks failed", K(ret));
  } else {
  }
  return ret;
}

void LogIOTruncatePrefixBlocksTask::free_this_(IPalfEnvImpl *palf_env_impl)
{
  palf_env_impl->get_log_allocator()->free_log_io_truncate_prefix_blocks_task(this);
}

int64_t BatchLogIOFlushLogTask::SINGLE_TASK_MAX_SIZE = MAX_LOG_BUFFER_SIZE;

BatchLogIOFlushLogTask::BatchLogIOFlushLogTask()
    : io_task_array_(),
      log_write_buf_array_(),
      scn_array_(),
      lsn_array_(),
      palf_id_(INVALID_PALF_ID),
      accum_in_queue_time_(0),
      accum_size_(0),
      is_inited_(false)
{}

BatchLogIOFlushLogTask::~BatchLogIOFlushLogTask()
{
  destroy();
}

int BatchLogIOFlushLogTask::init(const int64_t batch_depth, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  io_task_array_.set_allocator(allocator);
  log_write_buf_array_.set_allocator(allocator);
  scn_array_.set_allocator(allocator);
  lsn_array_.set_allocator(allocator);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "BatchLogIOFlushLogTask init twice", K(ret), KPC(this));
  } else if (OB_FAIL(io_task_array_.init(batch_depth))) {
    PALF_LOG(ERROR, "BatchIOTaskArray init failed", K(ret));
  } else if (OB_FAIL(log_write_buf_array_.init(batch_depth))) {
    PALF_LOG(ERROR, "log_write_buf_array_ init failed", K(ret));
  } else if (OB_FAIL(scn_array_.init(batch_depth))) {
    PALF_LOG(ERROR, "scn_array_ init failed", K(ret));
  } else if (OB_FAIL(lsn_array_.init(batch_depth))) {
    PALF_LOG(ERROR, "lsn_array_ init failed", K(ret));
  } else {
    accum_size_ = 0;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void BatchLogIOFlushLogTask::reuse()
{
  io_task_array_.clear();
  log_write_buf_array_.clear();
  scn_array_.clear();
  lsn_array_.clear();
  palf_id_ = INVALID_PALF_ID;
  accum_size_ = 0;
}

void BatchLogIOFlushLogTask::destroy()
{
  is_inited_ = false;
  io_task_array_.destroy();
  log_write_buf_array_.destroy();
  scn_array_.destroy();
  lsn_array_.destroy();
  palf_id_ = INVALID_PALF_ID;
}

int BatchLogIOFlushLogTask::push_back(LogIOFlushLogTask *task)
{
  int ret = OB_SUCCESS;
  const int64_t palf_id = task->get_palf_id();
  const int64_t task_size = task->get_io_size_();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOFlushMetaTask not inited!!!", K(ret), KPC(this));
  } else if (palf_id_ != INVALID_PALF_ID && palf_id != palf_id_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "only same palf id can be batched", K(ret), KPC(task), K(palf_id));
  } else if (accum_size_ > SINGLE_TASK_MAX_SIZE) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(io_task_array_.push_back(task))) {
    PALF_LOG(WARN, "push failed", K(ret));
  } else {
    palf_id_ = palf_id;
    accum_size_ += task_size;
    PALF_LOG(TRACE, "push_back success", K(palf_id_), KPC(this));
  }
  return ret;
}

// Each LogIOFlusLoghTask will be free in this function.
int BatchLogIOFlushLogTask::do_task(int tg_id, IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOFlushMetaTask not inited!!!", K(ret), KPC(this));
  } else if (INVALID_PALF_ID == palf_id_ && true == io_task_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "BatchLogIOFlushLogTask is empty", K(ret), KPC(this));
  } else if (OB_FAIL(do_task_(tg_id, palf_env_impl))) {
    PALF_LOG(WARN, "do_task_ failed", K(ret));
    clear_memory_(palf_env_impl);
  } else {
  }
  return ret;
}

int BatchLogIOFlushLogTask::push_flush_cb_to_thread_pool_(int tg_id, IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  const int64_t count = io_task_array_.count();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOFlushMetaTask not inited!!!", K(ret), KPC(this));
  } else {
    const int64_t current_time = ObTimeUtility::current_time();
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      LogIOFlushLogTask *io_task = io_task_array_[i];
      if (NULL == io_task) {
        PALF_LOG(WARN, "io_task is nullptr, may be its' epoch has changed", K(ret), KP(io_task),
            KPC(this));
      } else if (FALSE_IT(io_task->push_cb_into_cb_pool_ts_ = current_time)) {
      } else if (OB_FAIL(push_task_into_cb_thread_pool(tg_id, io_task))) {
        // avoid memory leak when push task into cb thread pool failed.
        PALF_LOG(WARN, "push_task_into_cb_thread_pool failed", K(ret), KPC(this));
      } else {
        // task will be released in after_consume, this task no need be rleased in 'clear_memory_'
        // again.
        io_task_array_[i] = NULL;
      }
    }
	}
	return ret;
}

// Any LogIOFlusLoghTask has been push into cb queue, the slot of io_task_array_ will reset to NULL,
// any LogIOFlusLoghTask in io_task_array_ which is not NULL, will be released after do_task_.
int BatchLogIOFlushLogTask::do_task_(int tg_id, IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  int64_t palf_epoch = -1;
  IPalfHandleImplGuard guard;
  LSN flushed_log_end_lsn;
  const int64_t first_handle_ts = ObTimeUtility::fast_current_time();
  if (OB_FAIL(palf_env_impl->get_palf_handle_impl(palf_id_, guard))) {
    PALF_LOG(WARN, "IPalfEnvImpl get_palf_handle_impl failed", K(ret), K(palf_id_));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->get_palf_epoch(palf_epoch))) {
    PALF_LOG(WARN, "IPalfEnvImpl get_palf_epoch failed", K(ret), K(palf_id_));
  } else {
    const int64_t count = io_task_array_.count();
    bool has_valid_data = false;
    for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
      LogIOFlushLogTask *io_task = io_task_array_[i];
      if (OB_ISNULL(io_task)) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "io_task is nullptr, unpexected error", K(ret), KP(io_task), KPC(this));
      } else if (palf_epoch != io_task->get_palf_epoch()) {
        PALF_LOG(WARN, "palf_epoch has changed, drop task", K(ret), K(palf_id_), K(palf_epoch),
                 KPC(io_task), KPC(io_task));
        io_task->free_this(palf_env_impl);
        io_task_array_[i] = NULL;
      } else if (OB_FAIL(log_write_buf_array_.push_back(&io_task->write_buf_))) {
        PALF_LOG(ERROR, "log_write_buf_array_ push_back failed, unexpected error!!!", K(ret),
                 KPC(this));
      } else if (OB_FAIL(scn_array_.push_back(io_task->flush_log_cb_ctx_.scn_))) {
        PALF_LOG(ERROR, "flush_log_cb_ctx_array_ push_back failed, unexpected error!!!", K(ret),
                 KPC(this), KPC(io_task));
      } else if (OB_FAIL(lsn_array_.push_back(io_task->flush_log_cb_ctx_.lsn_))) {
        PALF_LOG(ERROR, "lsn_array_ push_back failed, unexpected error!!!", K(ret), KPC(this),
                 KPC(io_task));
      } else {
        accum_in_queue_time_ += first_handle_ts - io_task->get_init_task_ts();
        has_valid_data = true;
        flushed_log_end_lsn = io_task->flush_log_cb_ctx_.lsn_ + io_task->write_buf_.get_total_size();
      }
    }
    if (OB_SUCC(ret) && true == has_valid_data) {
      if (OB_FAIL(guard.get_palf_handle_impl()->inner_append_log(lsn_array_, log_write_buf_array_,
                                                                 scn_array_))) {
        PALF_LOG(ERROR, "inner_append_log failed", K(ret), KPC(this));
      } else if (OB_FAIL(guard.get_palf_handle_impl()->advance_reuse_lsn(flushed_log_end_lsn))) {
        PALF_LOG(ERROR, "advance_reuse_lsn failed", K(ret), K(flushed_log_end_lsn));
      } else if (OB_FAIL(push_flush_cb_to_thread_pool_(tg_id, palf_env_impl))) {
        PALF_LOG(ERROR, "push_flush_cb_to_thread_pool_ failed", K(ret), KPC(this));
      } else {
      }
    }
  }
  return ret;
}

void BatchLogIOFlushLogTask::clear_memory_(IPalfEnvImpl *palf_env_impl)
{
  const int64_t count = io_task_array_.count();
  for (int64_t i = 0; i < count; i++) {
    LogIOFlushLogTask *task = io_task_array_[i];
    if (NULL != task) {
      task->free_this(palf_env_impl);
      io_task_array_[i] = NULL;
    }
  }
}

LogIOFlashbackTask::LogIOFlashbackTask(const int64_t palf_id, const int64_t palf_epoch)
  : LogIOTask(palf_id, palf_epoch), flashback_ctx_(),  is_inited_(false)
{
}

LogIOFlashbackTask::~LogIOFlashbackTask()
{
  destroy();
}

int LogIOFlashbackTask::init(const FlashbackCbCtx &flashback_ctx,
                             const int64_t palf_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogIOFlashbackTask init failed", K(ret), KPC(this));
  } else if (false == flashback_ctx.is_valid() || false == is_valid_palf_id(palf_id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid arguments", K(ret), K(flashback_ctx), K(palf_id));
  } else {
    flashback_ctx_ = flashback_ctx;
    palf_id_ = palf_id;
    is_inited_ = true;
    PALF_LOG(INFO, "LogIOFlashbackTask init success", K(flashback_ctx_), K(palf_id_));
  }
  return ret;
}


void LogIOFlashbackTask::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    palf_id_ = INVALID_PALF_ID;
    flashback_ctx_.reset();
  }
}

int LogIOFlashbackTask::do_task_(int tg_id, IPalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOFlashbackTask not inited!!!", K(ret));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->inner_flashback(flashback_ctx_.flashback_scn_))) {
    PALF_LOG(WARN, "inner_flashback failed", KPC(this));
  } else if (OB_FAIL(push_task_into_cb_thread_pool_(tg_id, this))) {
    PALF_LOG(WARN, "push_flush_cb_to_thread_pool_ failed", K(ret));
  } else {
  }
  return ret;
}

int LogIOFlashbackTask::after_consume_(IPalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogIOFlushMetaTask not inited!!!", K(ret));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->inner_after_flashback(flashback_ctx_))) {
    PALF_LOG(ERROR, "PalfHandleImpl inner_after_flashback failed", K(ret));
  } else {
    PALF_LOG(INFO, "LogIOFlashbackTask after_consume success");
  }
  return ret;
}

void LogIOFlashbackTask::free_this_(IPalfEnvImpl *palf_env_impl)
{
  palf_env_impl->get_log_allocator()->free_log_io_flashback_task(this);
}

LogIOPurgeThrottlingTask::LogIOPurgeThrottlingTask(const int64_t palf_id, const int64_t palf_epoch) :
  LogIOTask(palf_id, palf_epoch), purge_ctx_(), is_inited_(false)
  {}

LogIOPurgeThrottlingTask::~LogIOPurgeThrottlingTask()
{
  destroy();
}

void LogIOPurgeThrottlingTask::reset()
{
  is_inited_ = false;
  purge_ctx_.reset();
  LogIOTask::reset();
}

void LogIOPurgeThrottlingTask::destroy()
{
  if (IS_INIT) {
    reset();
  }
}

int LogIOPurgeThrottlingTask::init(const PurgeThrottlingCbCtx & purge_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogIOPurgeThrottlingTask init failed",KPC(this));
  } else if (OB_UNLIKELY(!purge_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid purge_ctx", K(purge_ctx), KPC(this));
  } else {
    purge_ctx_ = purge_ctx;
    is_inited_ = true;
  }
  return ret;
}

int LogIOPurgeThrottlingTask::do_task_(int tg_id, IPalfHandleImplGuard &guard)
{
  UNUSED(guard);
  int ret = OB_SUCCESS;
  ret = push_task_into_cb_thread_pool_(tg_id, this);
  return ret;
}

int LogIOPurgeThrottlingTask::after_consume_(IPalfHandleImplGuard &guard)
{
  UNUSED(guard);
  return OB_SUCCESS;
}

void LogIOPurgeThrottlingTask::free_this_(IPalfEnvImpl *palf_env_impl)
{
  if (OB_ISNULL(palf_env_impl) || OB_ISNULL(palf_env_impl->get_log_allocator())) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "palf_env_impl or log_allocator is NULL", KPC(this), KP(palf_env_impl))
  } else {
    palf_env_impl->get_log_allocator()->free_log_io_purge_throttling_task(this);
  }
}

} // end namespace palf
} // end namespace oceanbase
