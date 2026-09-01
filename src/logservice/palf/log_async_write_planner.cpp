/**
 * Copyright (c) 2026 OceanBase
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

#include "log_async_write_planner.h"
#include "share/ob_errno.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "share/rc/ob_tenant_base.h"
#include "log_define.h"
#include "log_io_task.h"
#include "palf_handle_impl.h"
#include "palf_handle_impl_guard.h"

namespace oceanbase
{
namespace palf
{

using namespace common;

const int64_t LogAsyncWritePlanner::TASK_QUEUE_CAPACITY;
const int64_t LogAsyncWritePlanner::FRAGMENT_REF_QUEUE_SLACK;
const int64_t LogAsyncWritePlanner::MIN_FRAGMENT_MAX_SIZE;
const int64_t LogAsyncWritePlanner::FRAGMENT_REF_QUEUE_CAPACITY;
const int64_t LogAsyncWritePlanner::FRAGMENT_REF_TRACE_CAPACITY;
const int64_t LogAsyncWritePlanner::FRAGMENT_REF_TRACE_PRINT_INTERVAL_US;

LogAsyncWritePlanner::PendingSourceRange::PendingSourceRange()
  : begin_lsn_(), buf_(NULL), len_(0)
{
}

int LogAsyncWritePlanner::PendingSourceRange::init(const LSN &begin_lsn,
                                                   const char *buf,
                                                   const int64_t len)
{
  int ret = OB_SUCCESS;
  LSN end_lsn;
  if (!is_empty()) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "pending source range init twice", KR(ret), KPC(this));
  } else if (!begin_lsn.is_valid() || OB_ISNULL(buf) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid pending source range", KR(ret), K(begin_lsn), KP(buf), K(len));
  } else if (FALSE_IT(end_lsn = begin_lsn + static_cast<offset_t>(len))) {
  } else if (!end_lsn.is_valid() || end_lsn <= begin_lsn) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid pending source end lsn", KR(ret), K(begin_lsn), K(end_lsn), K(len));
  } else {
    begin_lsn_ = begin_lsn;
    buf_ = buf;
    len_ = len;
  }
  return ret;
}

int LogAsyncWritePlanner::PendingSourceRange::append(const LSN &begin_lsn,
                                                     const char *buf,
                                                     const int64_t len)
{
  int ret = OB_SUCCESS;
  if (!can_append(begin_lsn, buf, len)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "pending source range is not continuous",
             KR(ret), K(begin_lsn), KP(buf), K(len), KPC(this));
  } else {
    len_ += len;
  }
  return ret;
}

int LogAsyncWritePlanner::PendingSourceRange::consume(const int64_t len)
{
  int ret = OB_SUCCESS;
  LSN next_begin_lsn;
  if (!is_valid() || len <= 0 || len > len_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid pending source consume length", KR(ret), K(len), KPC(this));
  } else if (FALSE_IT(next_begin_lsn = begin_lsn_ + static_cast<offset_t>(len))) {
  } else if (!next_begin_lsn.is_valid() || next_begin_lsn <= begin_lsn_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid pending source next lsn", KR(ret), K(len), K(next_begin_lsn), KPC(this));
  } else if (len == len_) {
    reset();
  } else {
    begin_lsn_ = next_begin_lsn;
    buf_ += len;
    len_ -= len;
  }
  return ret;
}

void LogAsyncWritePlanner::PendingSourceRange::reset()
{
  begin_lsn_.reset();
  buf_ = NULL;
  len_ = 0;
}

bool LogAsyncWritePlanner::PendingSourceRange::is_valid() const
{
  bool valid = (begin_lsn_.is_valid() && OB_NOT_NULL(buf_) && len_ > 0);
  if (valid) {
    const LSN end_lsn = begin_lsn_ + static_cast<offset_t>(len_);
    valid = (end_lsn.is_valid() && end_lsn > begin_lsn_);
  }
  return valid;
}

bool LogAsyncWritePlanner::PendingSourceRange::is_empty() const
{
  return !begin_lsn_.is_valid() && OB_ISNULL(buf_) && 0 == len_;
}

LSN LogAsyncWritePlanner::PendingSourceRange::get_end_lsn() const
{
  LSN end_lsn;
  if (is_valid()) {
    end_lsn = begin_lsn_ + static_cast<offset_t>(len_);
  }
  return end_lsn;
}

bool LogAsyncWritePlanner::PendingSourceRange::can_append(const LSN &begin_lsn,
                                                          const char *buf,
                                                          const int64_t len) const
{
  bool can_append = false;
  if (is_valid() && begin_lsn.is_valid() && OB_NOT_NULL(buf) && len > 0) {
    const LSN end_lsn = begin_lsn + static_cast<offset_t>(len);
    can_append = (end_lsn.is_valid() && end_lsn > begin_lsn
                  && get_end_lsn() == begin_lsn && buf_ + len_ == buf);
  }
  return can_append;
}

PlannerStatus::PlannerStatus()
{
  reset();
}

void PlannerStatus::reset()
{
  planned_end_lsn_.reset();
  persisted_lsn_.reset();
  pending_task_count_ = 0;
  active_fragment_count_ = 0;
  has_pending_source_ = false;
}

void PlannerStatus::init(const LSN &planned_end_lsn,
                         const LSN &persisted_lsn,
                         const int64_t pending_task_count,
                         const int64_t active_fragment_count,
                         const bool has_pending_source)
{
  planned_end_lsn_ = planned_end_lsn;
  persisted_lsn_ = persisted_lsn;
  pending_task_count_ = pending_task_count;
  active_fragment_count_ = active_fragment_count;
  has_pending_source_ = has_pending_source;
}

LogAsyncWritePlanner::LogAsyncWritePlanner(PhysicalWriteFragmentPool &pool)
  : pool_(pool),
    inited_(false),
    fragment_max_size_(NORMAL_FRAGMENT_MAX_SIZE),
    wait_parent_max_size_(WAIT_PARENT_FRAGMENT_MAX_SIZE),
    aio_delay_us_(0),
    queue_allocator_(),
    fragment_ref_pool_(),
    persisted_lsn_(),
    pending_task_queue_(),
    fragment_ref_queue_(),
    last_fragment_ref_(),
    last_fragment_ref_item_(NULL),
    fragment_ref_trace_(),
    fragment_ref_trace_next_idx_(0),
    fragment_ref_trace_cnt_(0),
    last_fragment_ref_trace_print_ts_(0),
    queue_end_lsn_(),
    pending_sources_()
{
}

LogAsyncWritePlanner::~LogAsyncWritePlanner()
{
  (void)check_queue_empty_();
}

int LogAsyncWritePlanner::init()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (inited_) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "async write planner init twice", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid async write planner tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(queue_allocator_.init(NULL, common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                                           common::ObMemAttr(tenant_id, "PalfPlanQ")))) {
    PALF_LOG(WARN, "init async write planner allocator failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(fragment_ref_pool_.init(FRAGMENT_REF_QUEUE_CAPACITY, "PalfFragRef", tenant_id))) {
    PALF_LOG(WARN, "init async fragment ref pool failed",
             KR(ret), K(tenant_id), "capacity", FRAGMENT_REF_QUEUE_CAPACITY);
  } else if (OB_FAIL(pending_task_queue_.init(TASK_QUEUE_CAPACITY, &queue_allocator_,
                                              common::ObMemAttr(tenant_id, "PalfTaskQ")))) {
    PALF_LOG(WARN, "init pending task queue failed", KR(ret), "capacity", TASK_QUEUE_CAPACITY);
  } else if (OB_FAIL(fragment_ref_queue_.init(FRAGMENT_REF_QUEUE_CAPACITY, &queue_allocator_,
                                              common::ObMemAttr(tenant_id, "PalfFragRefQ")))) {
    PALF_LOG(WARN, "init fragment ref queue failed", KR(ret), "capacity", FRAGMENT_REF_QUEUE_CAPACITY);
  } else {
    inited_ = true;
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    reset();
  }
  return ret;
}

void LogAsyncWritePlanner::reset()
{
  (void)check_queue_empty_();
  pending_task_queue_.destroy();
  fragment_ref_queue_.destroy();
  fragment_ref_pool_.destroy();
  queue_allocator_.reset();
  persisted_lsn_.reset();
  fragment_max_size_ = NORMAL_FRAGMENT_MAX_SIZE;
  wait_parent_max_size_ = WAIT_PARENT_FRAGMENT_MAX_SIZE;
  aio_delay_us_ = 0;
  last_fragment_ref_.reset();
  last_fragment_ref_item_ = NULL;
  reset_fragment_ref_trace_();
  queue_end_lsn_.reset();
  for (int64_t i = 0; i < PENDING_SOURCE_CNT; ++i) {
    pending_sources_[i].reset();
  }
  inited_ = false;
}

int LogAsyncWritePlanner::update_fragment_size_limits(const int64_t fragment_max_size,
                                                      const int64_t wait_parent_max_size)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "update fragment size limits before planner init", KR(ret));
  } else if (fragment_max_size < MIN_FRAGMENT_MAX_SIZE
             || fragment_max_size > NORMAL_FRAGMENT_MAX_SIZE
             || wait_parent_max_size <= 0
             || wait_parent_max_size > NORMAL_FRAGMENT_MAX_SIZE
             || 0 != fragment_max_size % LOG_DIO_ALIGN_SIZE
             || 0 != wait_parent_max_size % LOG_DIO_ALIGN_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    // ERRSIM may be changed while the worker is running. Keep the last valid
    // pair and let ctx emit the rate-limited operator-facing warning.
    PALF_LOG(TRACE, "invalid async fragment size limits", KR(ret), K(fragment_max_size),
             K(wait_parent_max_size), K(MIN_FRAGMENT_MAX_SIZE), K(NORMAL_FRAGMENT_MAX_SIZE));
  } else {
    fragment_max_size_ = fragment_max_size;
    // The normal limit is the upper bound for every fragment. WAIT_PARENT has
    // an extra, possibly smaller cap because it blocks behind another AIO.
    wait_parent_max_size_ = MIN(fragment_max_size, wait_parent_max_size);
  }
  return ret;
}

int LogAsyncWritePlanner::update_aio_delay(const int64_t aio_delay_us)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "update AIO delay before planner init", KR(ret));
  } else if (aio_delay_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid async AIO delay", KR(ret), K(aio_delay_us));
  } else {
    aio_delay_us_ = aio_delay_us;
  }
  return ret;
}

void LogAsyncWritePlanner::init_plan_state(const LSN &log_tail)
{
  persisted_lsn_ = log_tail;
  queue_end_lsn_ = log_tail;
}

void LogAsyncWritePlanner::invalidate_plan_state()
{
  persisted_lsn_.reset();
  queue_end_lsn_.reset();
}

int LogAsyncWritePlanner::reset_after_tail_changed(IPalfHandleImpl *handle, const LSN &log_tail)
{
  int ret = OB_SUCCESS;
  const LSN tail_lsn = log_tail;
  bool reset_started = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "reset async write planner before init", KR(ret), K(tail_lsn));
  } else if (OB_ISNULL(handle) || !tail_lsn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid argument while resetting async write planner", KR(ret), KP(handle), K(tail_lsn));
  } else if (OB_FAIL(check_queue_empty_())) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "async write planner is not empty before tail reset", KR(ret), K(tail_lsn));
    }
  } else {
    init_plan_state(tail_lsn);
    reset_started = true;
    last_fragment_ref_.reset();
    last_fragment_ref_item_ = NULL;
    for (int64_t i = 0; i < PENDING_SOURCE_CNT; ++i) {
      pending_sources_[i].reset();
    }
    // tail 未 4K 对齐时, 下一次 DIO 会从所在页的页首开始重写.
    // 先从磁盘读取已持久化前缀并回填 group buffer, 避免零拷贝写覆盖旧数据.
    const offset_t prefix_len = static_cast<offset_t>(tail_lsn.val_ % LOG_DIO_ALIGN_SIZE);
    if (0 == prefix_len || 0 == tail_lsn.val_) {
      // tail 已在 DIO 边界上, 后续 fragment 不会重写前一个页面的有效前缀.
    } else {
      const LSN page_begin_lsn = tail_lsn - prefix_len;
      char *tail_page_buf = NULL;
      int64_t read_size = 0;
      if (OB_ISNULL(tail_page_buf = static_cast<char *>(
              share::mtl_malloc_align(LOG_DIO_ALIGN_SIZE, LOG_DIO_ALIGN_SIZE, "AsyncTailPrefix")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(ERROR, "alloc tail prefix read buffer failed", KR(ret), K(page_begin_lsn));
      } else if (OB_FAIL(handle->read_log_storage_tail_page(
                     page_begin_lsn, tail_page_buf, LOG_DIO_ALIGN_SIZE, read_size))) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "read log storage tail page failed", KR(ret), K(page_begin_lsn), K(tail_lsn), K(prefix_len));
        }
      } else if (read_size < prefix_len) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "tail page read size is shorter than prefix",
                 KR(ret), K(page_begin_lsn), K(tail_lsn), K(prefix_len), K(read_size));
      } else if (OB_FAIL(handle->fill_tail_prefix_after_reset(page_begin_lsn, tail_lsn, tail_page_buf, prefix_len))) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "fill tail prefix after reset failed", KR(ret), K(page_begin_lsn), K(tail_lsn), K(prefix_len));
        }
      }
      if (OB_NOT_NULL(tail_page_buf)) {
        share::mtl_free_align(tail_page_buf);
        tail_page_buf = NULL;
      }
    }
  }
  if (OB_SUCCESS != ret && reset_started) {
    invalidate_plan_state();
  }
  return ret;
}

int LogAsyncWritePlanner::admit_task(LogIOTask *task, bool &consumed)
{
  int ret = OB_SUCCESS;
  PendingSourceRange old_pending_sources[PENDING_SOURCE_CNT];
  const LSN planned_end_lsn = get_planned_end_lsn_();
  const LSN old_queue_end_lsn = queue_end_lsn_;
  consumed = false;
  // admission 对 pending source 和 task queue 是一个事务: 任一步失败都
  // 恢复 source/queue_end, task 所有权仍由调用者持有.
  for (int64_t i = 0; i < PENDING_SOURCE_CNT; ++i) {
    old_pending_sources[i] = pending_sources_[i];
  }
  if (!is_valid_() || OB_ISNULL(task) || LogIOTaskType::FLUSH_LOG_TYPE != task->get_io_task_type()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid async flush task", KR(ret), K(planned_end_lsn), K_(persisted_lsn), K_(queue_end_lsn),
             KP(task));
  } else {
    LogIOFlushLogTask *flush_task = static_cast<LogIOFlushLogTask *>(task);
    const LSN begin_lsn = flush_task->get_flush_begin_lsn();
    const LSN end_lsn = flush_task->get_flush_end_lsn();
    const LogWriteBuf &write_buf = flush_task->get_write_buf();
    if (!begin_lsn.is_valid() || !end_lsn.is_valid() || end_lsn <= begin_lsn || !write_buf.is_valid()
        || begin_lsn != queue_end_lsn_) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "invalid async planner state or task range when admitting task", KR(ret),
               K(write_buf), K(planned_end_lsn), K_(persisted_lsn), K_(queue_end_lsn), K(begin_lsn),
               K(end_lsn), KP(task));
    } else if (lsn_2_block(begin_lsn, PALF_BLOCK_SIZE) != lsn_2_block(persisted_lsn_, PALF_BLOCK_SIZE)) {
      // 下一 block 的 task 留在 producer queue, 等 block switch 完成后再接收.
    } else if (OB_FAIL(append_pending_write_buf_(begin_lsn, write_buf))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "append pending async write buffer failed", KR(ret), K(begin_lsn), K(end_lsn), KP(task));
      }
    } else if (queue_end_lsn_ != end_lsn) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "pending async write buffer range is not match with task", KR(ret),
               K_(queue_end_lsn), K(end_lsn), KP(task));
    } else if (OB_FAIL(pending_task_queue_.push(flush_task))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "push pending async flush task failed", KR(ret), KP(task));
      }
    } else {
      consumed = true;
      flush_task = NULL;
      task = NULL;
    }
  }
  if (OB_SUCCESS != ret && !consumed) {
    queue_end_lsn_ = old_queue_end_lsn;
    for (int64_t i = 0; i < PENDING_SOURCE_CNT; ++i) {
      pending_sources_[i] = old_pending_sources[i];
    }
  }
  return ret;
}

int LogAsyncWritePlanner::append_pending_write_buf_(const LSN &begin_lsn, const LogWriteBuf &write_buf)
{
  int ret = OB_SUCCESS;
  if (!begin_lsn.is_valid() || begin_lsn != queue_end_lsn_) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid pending async write buffer range", KR(ret), K(begin_lsn), K_(queue_end_lsn), K(write_buf));
  } else {
    const int64_t buf_count = write_buf.get_buf_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < buf_count; ++i) {
      const char *buf = NULL;
      int64_t len = 0;
      if (OB_FAIL(write_buf.get_write_buf(i, buf, len))) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "get pending async write buffer failed", KR(ret), K(i), K(write_buf));
        }
      } else if (OB_FAIL(append_pending_source_(queue_end_lsn_, buf, len))) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "append pending async source failed", KR(ret), K(i), KP(buf), K(len));
        }
      } else {
        queue_end_lsn_ = queue_end_lsn_ + static_cast<offset_t>(len);
      }
    }
  }
  return ret;
}

int LogAsyncWritePlanner::append_pending_source_(const LSN &begin_lsn,
                                                 const char *buf,
                                                 const int64_t len)
{
  int ret = OB_SUCCESS;
  if (!begin_lsn.is_valid() || begin_lsn != queue_end_lsn_ || OB_ISNULL(buf) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid pending async source", KR(ret), K(begin_lsn), KP(buf), K(len));
  // 两个 slot 始终按 LSN 紧凑排列. 已有第二段时先尝试延长第二段;
  // 只有第一段时则继续延长第一段, 新的不连续内存才占用空 slot.
  } else if (pending_sources_[1].can_append(begin_lsn, buf, len)) {
    if (OB_FAIL(pending_sources_[1].append(begin_lsn, buf, len))) {
      PALF_LOG(WARN, "append second pending async source failed",
               KR(ret), K(begin_lsn), KP(buf), K(len), K_(pending_sources));
    }
  } else if (pending_sources_[1].is_empty()
             && pending_sources_[0].can_append(begin_lsn, buf, len)) {
    if (OB_FAIL(pending_sources_[0].append(begin_lsn, buf, len))) {
      PALF_LOG(WARN, "append first pending async source failed",
               KR(ret), K(begin_lsn), KP(buf), K(len), K_(pending_sources));
    }
  } else if (pending_sources_[0].is_empty()) {
    if (OB_FAIL(pending_sources_[0].init(begin_lsn, buf, len))) {
      PALF_LOG(WARN, "init first pending async source failed",
               KR(ret), K(begin_lsn), KP(buf), K(len));
    }
  } else if (pending_sources_[1].is_empty()) {
    if (OB_FAIL(pending_sources_[1].init(begin_lsn, buf, len))) {
      PALF_LOG(WARN, "init second pending async source failed",
               KR(ret), K(begin_lsn), KP(buf), K(len));
    }
  } else {
    // 上层 group buffer 最多暴露两段物理内存; 第三段不连续地址违反输入契约.
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "pending async sources exceed two ranges",
             KR(ret), K(begin_lsn), KP(buf), K(len), K_(pending_sources));
  }
  return ret;
}

int LogAsyncWritePlanner::plan_pending_tasks()
{
  int ret = OB_SUCCESS;
  bool end = false;
  if (!is_valid_()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "plan pending tasks before async planner is ready", KR(ret),
             "planned_end_lsn", get_planned_end_lsn_(), K_(persisted_lsn), K_(queue_end_lsn));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !end && i < PENDING_SOURCE_CNT; ++i) {
      int tmp_ret = OB_SUCCESS;
      if (pending_sources_[i].is_empty()) {
        end = true;
      } else if (OB_TMP_FAIL(plan_source_range_(pending_sources_[i]))) {
        ret = tmp_ret;
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "plan pending async source range failed",
                   KR(ret), "planned_end_lsn", get_planned_end_lsn_(), K(i), K(pending_sources_[i]));
        }
      } else if (!pending_sources_[i].is_empty()) {
        // Keep the remaining source in place for the next drive.
        end = true;
      }
    }
    // 即使第一段已消费而后一段报错, 也要把剩余 source 前移到规划队首.
    if (pending_sources_[0].is_empty() && !pending_sources_[1].is_empty()) {
      pending_sources_[0] = pending_sources_[1];
      pending_sources_[1].reset();
    }
  }
  return ret;
}

int LogAsyncWritePlanner::advance_finished_fragment_prefix()
{
  int ret = OB_SUCCESS;
  bool stop = false;
  if (!is_valid_()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "advance fragment prefix before async planner is ready", KR(ret),
             "planned_end_lsn", get_planned_end_lsn_(), K_(persisted_lsn), K_(queue_end_lsn));
  }
  // ctx 可能已回收可见的 FINISHED slot. slot 仍存在时必须等到注入延迟
  // 到期; slot 已回收时依靠队列保存的 begin/end 继续推进有序持久化前缀.
  while (OB_SUCC(ret) && !stop) {
    QueuedFragmentRef *queued_ref = NULL;
    PhysicalWriteFragment *fragment = NULL;
    bool finish_visible = false;
    if (OB_FAIL(peek_head_fragment_for_advance_(queued_ref, fragment))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        stop = true;
      }
    } else if (OB_NOT_NULL(fragment)
               && OB_FAIL(is_fragment_finish_visible_(*fragment, finish_visible))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "check async fragment finish visibility failed", KR(ret), KPC(fragment));
      }
    } else if (OB_NOT_NULL(fragment) && !finish_visible) {
      stop = true;
    } else if (OB_ISNULL(queued_ref) || queued_ref->begin_lsn_ != persisted_lsn_) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "invalid queued fragment ref while advancing fragment prefix", KR(ret),
               KP(queued_ref), K_(persisted_lsn));
    } else if (OB_FAIL(fragment_ref_queue_.pop(queued_ref))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "pop finished async fragment ref failed", KR(ret));
      }
    } else {
      persisted_lsn_ = queued_ref->end_lsn_;
      if (queued_ref == last_fragment_ref_item_) {
        last_fragment_ref_item_ = NULL;
      }
      free_fragment_ref_item_(queued_ref);
      queued_ref = NULL;
    }
  }
  return ret;
}

int LogAsyncWritePlanner::peek_publishable_task(LogIOFlushLogTask *&task)
{
  int ret = OB_SUCCESS;
  task = NULL;
  if (!is_valid_()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "peek publishable task before async planner is ready", KR(ret),
             "planned_end_lsn", get_planned_end_lsn_(), K_(persisted_lsn), K_(queue_end_lsn));
  } else if (OB_FAIL(pending_task_queue_.head_unsafe(task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "peek pending async task failed", KR(ret));
      }
    } else {
      ret = OB_ITER_END;
    }
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "pending async task is null", KR(ret));
  } else if (task->get_flush_end_lsn() > persisted_lsn_) {
    ret = OB_ITER_END;
  }
  return ret;
}

void LogAsyncWritePlanner::pop_published_task()
{
  LogIOFlushLogTask *task = NULL;
  int tmp_ret = OB_SUCCESS;
  if (!is_valid_()) {
    PALF_LOG_RET(ERROR, OB_INVALID_ARGUMENT,
                 "pop published task before async planner is ready",
                 "planned_end_lsn", get_planned_end_lsn_(), K_(persisted_lsn), K_(queue_end_lsn));
  } else if (OB_TMP_FAIL(pending_task_queue_.pop(task))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG_RET(WARN, tmp_ret, "pop published async task failed", KR(tmp_ret));
    }
  } else {
    task = NULL;
  }
}

int LogAsyncWritePlanner::discard_all_tasks(IPalfEnvImpl *palf_env_impl,
                                            int64_t &discarded_task_count)
{
  int ret = OB_SUCCESS;
  discarded_task_count = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "discard async planner tasks before init", KR(ret));
  } else if (OB_ISNULL(palf_env_impl)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid palf env while discarding async planner tasks", KR(ret));
  } else {
    while (fragment_ref_queue_.get_total() > 0) {
      QueuedFragmentRef *queued_ref = NULL;
      const int tmp_ret = fragment_ref_queue_.pop(queued_ref);
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        PALF_LOG(ERROR, "pop fragment ref while discarding async planner failed",
                 KR(tmp_ret), "fragment_ref_count", fragment_ref_queue_.get_total());
        break;
      } else if (OB_ISNULL(queued_ref)) {
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
        PALF_LOG(ERROR, "null fragment ref while discarding async planner",
                 KR(ret), "fragment_ref_count", fragment_ref_queue_.get_total());
      } else {
        if (queued_ref == last_fragment_ref_item_) {
          last_fragment_ref_item_ = NULL;
        }
        free_fragment_ref_item_(queued_ref);
      }
    }

    for (int64_t i = 0; i < PENDING_SOURCE_CNT; ++i) {
      pending_sources_[i].reset();
    }
    persisted_lsn_.reset();
    queue_end_lsn_.reset();
    last_fragment_ref_.reset();
    last_fragment_ref_item_ = NULL;
    reset_fragment_ref_trace_();

    while (pending_task_queue_.get_total() > 0) {
      LogIOFlushLogTask *task = NULL;
      const int tmp_ret = pending_task_queue_.pop(task);
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        PALF_LOG(ERROR, "pop task while discarding async planner failed",
                 KR(tmp_ret), "pending_task_count", pending_task_queue_.get_total());
        break;
      } else {
        ++discarded_task_count;
        if (OB_ISNULL(task)) {
          ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
          PALF_LOG(ERROR, "null task while discarding async planner",
                   KR(ret), K(discarded_task_count));
        } else {
          task->free_this(palf_env_impl);
          task = NULL;
        }
      }
    }
  }
  return ret;
}

void LogAsyncWritePlanner::get_status(PlannerStatus &status) const
{
  const bool has_pending_source = (!pending_sources_[0].is_empty() || !pending_sources_[1].is_empty());
  status.init(get_planned_end_lsn_(),
              persisted_lsn_,
              pending_task_queue_.is_inited() ? pending_task_queue_.get_total() : 0,
              pool_.get_used_slot_count(),
              has_pending_source);
}

int LogAsyncWritePlanner::plan_source_range_(PendingSourceRange &source)
{
  int ret = OB_SUCCESS;
  bool end = false;
  if (!source.is_valid() || source.get_begin_lsn() != get_planned_end_lsn_()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid pending source before planning",
             KR(ret), K(source), "planned_end_lsn", get_planned_end_lsn_());
  }
  while (OB_SUCC(ret) && !end && !source.is_empty()) {
    FragmentRef ref;
    int64_t planned_len = 0;
    const LSN begin_lsn = source.get_begin_lsn();
    const char *buf = source.get_buf();
    const int64_t remaining_len = source.get_len();
    if (OB_FAIL(prepare_fragment_for_source_range_(begin_lsn, buf, remaining_len, ref, planned_len))) {
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
        end = true;
      } else {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "prepare async fragment for source range failed",
                   KR(ret), K(begin_lsn), KP(buf), K(remaining_len));
        }
      }
    } else if (planned_len <= 0 || planned_len > remaining_len) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "invalid async source plan length",
               KR(ret), K(begin_lsn), KP(buf), K(remaining_len), K(planned_len), K(ref));
    } else if (OB_FAIL(source.consume(planned_len))) {
      PALF_LOG(WARN, "consume planned async source failed",
               KR(ret), K(begin_lsn), KP(buf), K(remaining_len), K(planned_len), K(ref));
    }
  }
  return ret;
}

int LogAsyncWritePlanner::prepare_fragment_for_source_range_(const LSN &begin_lsn, const char *source_buf,
                                                             const int64_t max_len, FragmentRef &ref,
                                                             int64_t &planned_len)
{
  int ret = OB_SUCCESS;
  PhysicalWriteFragment *fragment = NULL;
  int tmp_ret = OB_SUCCESS;
  int64_t appendable_len = 0;
  int64_t data_len = 0;
  int64_t try_len = 0;
  bool need_alloc_new_fragment = false;
  ref.reset();
  planned_len = 0;
  if (!begin_lsn.is_valid() || OB_ISNULL(source_buf) || max_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid async fragment source range argument", KR(ret), K(begin_lsn), KP(source_buf), K(max_len));
  } else if (OB_TMP_FAIL(get_appendable_fragment_(fragment)) && OB_ENTRY_NOT_EXIST != tmp_ret) {
    ret = tmp_ret;
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "get appendable async fragment failed before preparing source range",
               KR(ret), K(begin_lsn), K(max_len));
    }
  } else if (OB_ISNULL(fragment)) {
    need_alloc_new_fragment = true;
  } else if (OB_FAIL(fragment->get_appendable_data_len(appendable_len))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "get appendable fragment length failed", KR(ret), K(begin_lsn), K(max_len), KPC(fragment));
    }
  } else if (FALSE_IT(try_len = MIN(max_len, appendable_len))) {
  } else if (try_len <= 0) {
    need_alloc_new_fragment = true;
  } else if (OB_FAIL(fragment->get_data_len(data_len))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "get appendable fragment data length failed",
               KR(ret), K(begin_lsn), K(max_len), KPC(fragment));
    }
  } else if (fragment->get_buf() + data_len != source_buf) {
    need_alloc_new_fragment = true;
  } else if (FALSE_IT(ref = fragment->get_fragment_ref())) {
  } else if (OB_FAIL(append_source_range_to_fragment_(*fragment, ref, begin_lsn, source_buf, try_len, planned_len))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "append source range to wait-parent fragment failed", KR(ret), K(ref), K(begin_lsn), K(try_len));
    }
  }
  if (OB_SUCC(ret) && need_alloc_new_fragment
      && OB_FAIL(alloc_new_fragment_(begin_lsn, source_buf, max_len, ref, planned_len))
      && OB_SIZE_OVERFLOW != ret) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "alloc new async fragment failed", KR(ret), K(begin_lsn), K(max_len));
    }
  }
  return ret;
}

int LogAsyncWritePlanner::alloc_new_fragment_(const LSN &begin_lsn, const char *source_buf,
                                              const int64_t max_len, FragmentRef &ref,
                                              int64_t &planned_len)
{
  int ret = OB_SUCCESS;
  PhysicalWriteFragment *parent = NULL;
  FragmentRef parent_ref;
  bool parent_finish_visible = false;
  int tmp_ret = OB_SUCCESS;
  int64_t fragment_max_size = fragment_max_size_;
  ref.reset();
  planned_len = 0;
  if (!begin_lsn.is_valid() || OB_ISNULL(source_buf) || max_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid new async fragment argument", KR(ret), K(begin_lsn), KP(source_buf), K(max_len));
  } else {
    bool need_wait_parent = false;
    // 规划顺序与 LSN 顺序一致, 只有最后一个未回收 fragment 可能与新
    // fragment 共用一个 DIO 尾页, 因此只检查 last_fragment_ref_.
    if (last_fragment_ref_.is_valid()
        && OB_TMP_FAIL(get_not_null_fragment_(last_fragment_ref_, parent))
        && OB_ENTRY_NOT_EXIST != tmp_ret) {
      ret = tmp_ret;
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "get parent async fragment failed", KR(tmp_ret), K_(last_fragment_ref), K(ref));
      }
    } else if (OB_NOT_NULL(parent)
               && OB_FAIL(need_wait_parent_for_tail_page_(*parent, begin_lsn, need_wait_parent))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "check async fragment tail page dependency failed",
                 KR(ret), K(begin_lsn), K_(last_fragment_ref), KPC(parent));
      }
    } else if (need_wait_parent
               && OB_FAIL(is_fragment_finish_visible_(*parent, parent_finish_visible))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "check parent async fragment finish visibility failed",
                 KR(ret), K(begin_lsn), K_(last_fragment_ref), KPC(parent));
      }
    } else if (need_wait_parent && !parent_finish_visible) {
      // parent 完成尚不可见时仍按同一尾页在写处理. child 必须
      // WAIT_PARENT, 并限制最大长度, 避免单个页依赖阻塞过多数据.
      parent_ref = last_fragment_ref_;
      fragment_max_size = wait_parent_max_size_;
    }
    if (FAILEDx(pool_.alloc_slot(begin_lsn, source_buf, max_len, fragment_max_size, parent_ref, ref, planned_len))) {
      planned_len = 0;
      if (OB_SIZE_OVERFLOW != ret) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "alloc async fragment slot failed",
                   KR(ret), K(begin_lsn), K(max_len), K(fragment_max_size), K(parent_ref));
        }
      }
    } else if (OB_FAIL(push_fragment_ref_(ref, begin_lsn, begin_lsn + static_cast<offset_t>(planned_len)))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "push new async fragment ref failed", KR(ret), K(ref));
      }
    } else {
      last_fragment_ref_ = ref;
    }
    if (OB_SUCCESS != ret && ref.is_valid()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(pool_.free_slot(ref))) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG_RET(WARN, tmp_ret, "free new async fragment after failure failed", KR(tmp_ret), K(ref));
        }
      }
      ref.reset();
      planned_len = 0;
    }
  }
  return ret;
}

int LogAsyncWritePlanner::is_fragment_finish_visible_(
    const PhysicalWriteFragment &fragment,
    bool &is_visible) const
{
  int ret = OB_SUCCESS;
  is_visible = false;
  if (fragment.is_finished()) {
    int64_t remaining_delay_us = 0;
    if (OB_FAIL(fragment.get_remaining_finish_delay(common::ObTimeUtility::current_time(),
                                                    aio_delay_us_,
                                                    remaining_delay_us))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "get async fragment finish delay failed",
                 KR(ret), K_(aio_delay_us), K(fragment));
      }
    } else {
      is_visible = (0 == remaining_delay_us);
    }
  }
  return ret;
}

int LogAsyncWritePlanner::push_fragment_ref_(const FragmentRef &ref, const LSN &begin_lsn, const LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  QueuedFragmentRef *queued_ref = NULL;
  if (!ref.is_valid() || !begin_lsn.is_valid() || !end_lsn.is_valid() || end_lsn <= begin_lsn) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid queued async fragment ref", KR(ret), K(ref), K(begin_lsn), K(end_lsn));
  } else if (OB_ISNULL(queued_ref = alloc_fragment_ref_item_())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(ERROR, "alloc queued async fragment ref failed", KR(ret), K(ref));
  } else {
    queued_ref->ref_ = ref;
    queued_ref->begin_lsn_ = begin_lsn;
    queued_ref->end_lsn_ = end_lsn;
    if (OB_FAIL(fragment_ref_queue_.push(queued_ref))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "push queued async fragment ref failed", KR(ret), KPC(queued_ref));
      }
      free_fragment_ref_item_(queued_ref);
      queued_ref = NULL;
    } else {
      last_fragment_ref_item_ = queued_ref;
      record_fragment_ref_trace_(*queued_ref);
    }
  }
  return ret;
}

void LogAsyncWritePlanner::reset_fragment_ref_trace_()
{
  for (int64_t i = 0; i < FRAGMENT_REF_TRACE_CAPACITY; ++i) {
    fragment_ref_trace_[i].reset();
  }
  fragment_ref_trace_next_idx_ = 0;
  fragment_ref_trace_cnt_ = 0;
  last_fragment_ref_trace_print_ts_ = 0;
}

void LogAsyncWritePlanner::record_fragment_ref_trace_(const QueuedFragmentRef &queued_ref)
{
  QueuedFragmentRefTraceEntry &entry = fragment_ref_trace_[fragment_ref_trace_next_idx_];
  entry.init(common::ObTimeUtility::current_time(), queued_ref);
  fragment_ref_trace_next_idx_ = (fragment_ref_trace_next_idx_ + 1) % FRAGMENT_REF_TRACE_CAPACITY;
  if (fragment_ref_trace_cnt_ < FRAGMENT_REF_TRACE_CAPACITY) {
    ++fragment_ref_trace_cnt_;
  }
}

void LogAsyncWritePlanner::print_fragment_ref_trace()
{
  if (fragment_ref_trace_cnt_ > 0
      && palf_reach_time_interval(FRAGMENT_REF_TRACE_PRINT_INTERVAL_US,
                                  last_fragment_ref_trace_print_ts_)) {
    const int64_t trace_start_idx = (fragment_ref_trace_next_idx_ + FRAGMENT_REF_TRACE_CAPACITY
        - fragment_ref_trace_cnt_) % FRAGMENT_REF_TRACE_CAPACITY;
    const int64_t remaining_cnt = FRAGMENT_REF_TRACE_CAPACITY - trace_start_idx;
    const int64_t first_part_cnt = fragment_ref_trace_cnt_ < remaining_cnt
        ? fragment_ref_trace_cnt_ : remaining_cnt;
    const int64_t second_part_cnt = fragment_ref_trace_cnt_ - first_part_cnt;
    common::ObArrayWrap<QueuedFragmentRefTraceEntry> first_part(
        fragment_ref_trace_ + trace_start_idx, first_part_cnt);
    common::ObArrayWrap<QueuedFragmentRefTraceEntry> second_part(fragment_ref_trace_, second_part_cnt);
    // Print the oldest entry first, followed by the wrapped part at physical index zero.
    if (0 == second_part_cnt) {
      PALF_LOG(INFO, "[PALF_DUMP][ASYNC FRAGMENT REF TRACE]", K(trace_start_idx),
               K_(fragment_ref_trace_next_idx), K(first_part));
    } else {
      PALF_LOG(INFO, "[PALF_DUMP][ASYNC FRAGMENT REF TRACE]", K(trace_start_idx),
               K_(fragment_ref_trace_next_idx), K(first_part), K(second_part));
    }
  }
}

LogAsyncWritePlanner::QueuedFragmentRef *LogAsyncWritePlanner::alloc_fragment_ref_item_()
{
  int ret = OB_SUCCESS;
  QueuedFragmentRef *item = NULL;
  if (OB_FAIL(fragment_ref_pool_.alloc(item))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "alloc async fragment ref from pool failed", KR(ret));
    }
  } else {
    item->reset();
  }
  return item;
}

void LogAsyncWritePlanner::free_fragment_ref_item_(QueuedFragmentRef *queued_ref)
{
  if (OB_NOT_NULL(queued_ref)) {
    queued_ref->reset();
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(fragment_ref_pool_.free(queued_ref))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG_RET(WARN, tmp_ret, "free async fragment ref failed", KR(tmp_ret), KPC(queued_ref));
      }
    }
  }
}

int LogAsyncWritePlanner::append_source_range_to_fragment_(PhysicalWriteFragment &fragment, const FragmentRef &ref,
                                                           const LSN &begin_lsn, const char *source_buf,
                                                           const int64_t max_len, int64_t &planned_len)
{
  int ret = OB_SUCCESS;
  int64_t appendable_len = 0;
  planned_len = 0;
  if (!fragment.is_appendable()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "async fragment is not appendable", KR(ret), K(ref), K(begin_lsn), K(max_len),
             KP(source_buf), KPC(&fragment));
  } else if (!fragment.get_begin_lsn().is_valid()) {
    appendable_len = max_len;
  } else if (OB_FAIL(fragment.get_appendable_data_len(appendable_len))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "get async fragment appendable len failed", KR(ret), K(ref), KPC(&fragment));
    }
  } else if (FALSE_IT(planned_len = MIN(max_len, appendable_len))) {
  } else if (planned_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid async fragment source binding", KR(ret), K(ref), K(begin_lsn), K(max_len),
             K(appendable_len), K(planned_len), KP(source_buf), KPC(&fragment));
  } else if (OB_FAIL(fragment.append_source(begin_lsn, planned_len, source_buf))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "append async fragment source failed", KR(ret), K(ref), K(begin_lsn), K(planned_len),
               KP(source_buf), KPC(&fragment));
    }
  } else if (ref.is_equal(last_fragment_ref_) && OB_NOT_NULL(last_fragment_ref_item_)) {
    last_fragment_ref_item_->end_lsn_ = begin_lsn + static_cast<offset_t>(planned_len);
  }
  return ret;
}

int LogAsyncWritePlanner::get_not_null_fragment_(const FragmentRef &ref, PhysicalWriteFragment *&fragment)
{
  int ret = OB_SUCCESS;
  fragment = NULL;
  if (!ref.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid async fragment ref", KR(ret), K(ref));
  } else if (OB_FAIL(pool_.get_fragment(ref, fragment))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      PALF_LOG(TRACE, "async fragment slot has been recycled", KR(ret), K(ref));
    } else if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "get async fragment from pool failed", KR(ret), K(ref));
    }
  } else if (OB_ISNULL(fragment)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "async fragment is null", KR(ret), K(ref));
  }
  return ret;
}

int LogAsyncWritePlanner::peek_head_fragment_for_advance_(QueuedFragmentRef *&queued_ref,
                                                            PhysicalWriteFragment *&fragment)
{
  int ret = OB_SUCCESS;
  queued_ref = NULL;
  fragment = NULL;
  if (OB_FAIL(fragment_ref_queue_.head_unsafe(queued_ref))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ITER_END;
    } else {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "peek async fragment ref failed", KR(ret));
      }
    }
  } else if (OB_ISNULL(queued_ref)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "queued async fragment ref is null", KR(ret));
  } else if (!queued_ref->ref_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "queued async fragment ref is invalid", KR(ret), KPC(queued_ref));
  } else if (OB_FAIL(get_not_null_fragment_(queued_ref->ref_, fragment))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "get queued async fragment failed", KR(ret), KPC(queued_ref));
      }
    }
  }
  return ret;
}

int LogAsyncWritePlanner::get_appendable_fragment_(PhysicalWriteFragment *&fragment)
{
  int ret = OB_SUCCESS;
  fragment = NULL;
  if (!last_fragment_ref_.is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(get_not_null_fragment_(last_fragment_ref_, fragment))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "get appendable async fragment failed", KR(ret), K_(last_fragment_ref));
      }
    }
  } else if (!fragment->is_appendable()) {
    fragment = NULL;
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

LSN LogAsyncWritePlanner::get_planned_end_lsn_() const
{
  LSN planned_end_lsn;
  if (!queue_end_lsn_.is_valid()) {
  } else if (!pending_sources_[0].is_empty()) {
    planned_end_lsn = pending_sources_[0].get_begin_lsn();
  } else if (!pending_sources_[1].is_empty()) {
    planned_end_lsn = pending_sources_[1].get_begin_lsn();
  } else {
    planned_end_lsn = queue_end_lsn_;
  }
  return planned_end_lsn;
}

bool LogAsyncWritePlanner::is_valid_() const
{
  // planner 的核心不变量:
  // 1. pending source 最多两段, 按 LSN 连续且从 slot 0 开始紧凑存放;
  // 2. pending source 尾端等于 queue_end_lsn_;
  // 3. persisted_lsn <= planned_end_lsn <= queue_end_lsn.
  const LSN planned_end_lsn = get_planned_end_lsn_();
  LSN pending_end_lsn;
  bool pending_source_valid = ((pending_sources_[0].is_empty() || pending_sources_[0].is_valid())
                               && (pending_sources_[1].is_empty() || pending_sources_[1].is_valid()));
  if (pending_source_valid && pending_sources_[0].is_empty() && !pending_sources_[1].is_empty()) {
    pending_source_valid = false;
  } else if (pending_source_valid && !pending_sources_[1].is_empty()
             && pending_sources_[0].get_end_lsn() != pending_sources_[1].get_begin_lsn()) {
    pending_source_valid = false;
  }
  if (pending_source_valid && !pending_sources_[1].is_empty()) {
    pending_end_lsn = pending_sources_[1].get_end_lsn();
  } else if (pending_source_valid && !pending_sources_[0].is_empty()) {
    pending_end_lsn = pending_sources_[0].get_end_lsn();
  }
  if (pending_source_valid && pending_end_lsn.is_valid()) {
    pending_source_valid = (pending_end_lsn == queue_end_lsn_);
  }
  return inited_ && planned_end_lsn.is_valid() && persisted_lsn_.is_valid()
         && queue_end_lsn_.is_valid() && persisted_lsn_ <= planned_end_lsn
         && planned_end_lsn <= queue_end_lsn_ && pending_source_valid
         && pending_task_queue_.is_inited() && fragment_ref_queue_.is_inited();
}

int LogAsyncWritePlanner::need_wait_parent_for_tail_page_(const PhysicalWriteFragment &parent,
                                                          const LSN &child_begin_lsn,
                                                          bool &need_wait_parent) const
{
  int ret = OB_SUCCESS;
  const LSN parent_begin_lsn = parent.get_begin_lsn();
  const LSN parent_end_lsn = parent.get_end_lsn();
  need_wait_parent = false;
  if (!child_begin_lsn.is_valid() || !parent_begin_lsn.is_valid()
      || !parent_end_lsn.is_valid() || parent_end_lsn <= parent_begin_lsn) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid fragment range while checking tail page dependency",
             KR(ret), K(child_begin_lsn), K(parent_begin_lsn), K(parent_end_lsn), K(parent));
  } else {
    const LSN parent_write_end_lsn(
        static_cast<offset_t>(upper_align(parent_end_lsn.val_, LOG_DIO_ALIGN_SIZE)));
    const LSN child_write_begin_lsn(
        static_cast<offset_t>(lower_align(child_begin_lsn.val_, LOG_DIO_ALIGN_SIZE)));
    // fragment 按 LSN 顺序规划, 唯一可能的重叠是 child 重写 parent 的
    // DIO 对齐尾页. 只要 child 页首早于 parent 对齐写入尾端就必须等待.
    need_wait_parent = child_write_begin_lsn < parent_write_end_lsn;
  }
  return ret;
}

int LogAsyncWritePlanner::check_queue_empty_() const
{
  int ret = OB_SUCCESS;
  const int64_t pending_task_cnt =
      pending_task_queue_.is_inited() ? pending_task_queue_.get_total() : 0;
  const int64_t fragment_ref_cnt =
      fragment_ref_queue_.is_inited() ? fragment_ref_queue_.get_total() : 0;
  bool has_pending_source = false;
  for (int64_t i = 0; i < PENDING_SOURCE_CNT; ++i) {
    has_pending_source = has_pending_source || !pending_sources_[i].is_empty();
  }
  if (pending_task_cnt > 0) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "pending async task queue is not empty when checking planner",
             KR(ret), K(pending_task_cnt), "planned_end_lsn", get_planned_end_lsn_(),
             K_(persisted_lsn), K_(queue_end_lsn));
  } else if (fragment_ref_cnt > 0) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "async fragment ref queue is not empty when checking planner",
             KR(ret), K(fragment_ref_cnt), K_(last_fragment_ref),
             "planned_end_lsn", get_planned_end_lsn_(), K_(persisted_lsn));
  } else if (has_pending_source) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "async pending source is not empty when checking planner",
             KR(ret), K_(pending_sources), "planned_end_lsn", get_planned_end_lsn_(),
             K_(persisted_lsn), K_(queue_end_lsn));
  }
  return ret;
}

} // namespace palf
} // namespace oceanbase
