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

#include "log_async_palf_ctx.h"
#include <cstring>
#include "share/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/rc/ob_tenant_base.h"  // MTL_ID
#include "log_reader_utils.h"          // ReadBufGuard
#include "log_io_context.h"            // LogIOContext
#include "log_define.h"
#include "log_io_task.h"
#include "palf_handle_impl.h"          // IPalfHandleImpl, PalfHandleImpl
#include "palf_handle_impl_guard.h"    // IPalfHandleImplGuard
#include "palf_env_impl.h"             // IPalfEnvImpl

namespace oceanbase
{
namespace palf
{

using namespace common;
using namespace share;

ERRSIM_POINT_DEF(ERRSIM_PALF_ASYNC_CTX_STAT_PRINT_INTERVAL_MS);
ERRSIM_POINT_DEF(ERRSIM_PALF_ASYNC_FRAGMENT_MAX_SIZE_IN_4K);
ERRSIM_POINT_DEF(ERRSIM_PALF_ASYNC_WAIT_PARENT_MAX_SIZE_IN_4K);
ERRSIM_POINT_DEF(ERRSIM_PALF_ASYNC_AIO_DELAY_US);

AsyncPalfIOCtx::AsyncPalfIOCtx()
  : qsync_lock_(common::ObLatchIds::OB_LOG_IO_WORKER_LOCK),
    is_inited_(false),
    palf_id_(-1),
    cb_tg_id_(-1),
    palf_env_impl_(NULL),
    palf_handle_guard_(),
    drive_waker_(NULL),
    task_queue_allocator_(),
    task_queue_item_pool_(),
    task_queue_(),
    inflight_aio_cnt_(0),
    available_flush_task_slot_count_(0),
    available_barrier_task_slot_count_(0),
    active_ref_(0),
    submit_fail_cnt_(0),
    complete_fail_cnt_(0),
    stale_completion_cnt_(0),
    published_entry_cnt_(0),
    block_switch_pending_since_ts_(OB_INVALID_TIMESTAMP),
    throttle_error_count_(0),
    last_throttle_error_ts_(OB_INVALID_TIMESTAMP),
    throttle_next_admit_ts_(0),
    throttle_wait_until_ts_(0),
    admit_bytes_(false /* is_counter */),
    publish_bytes_(false /* is_counter */),
    task_publish_us_(false /* is_counter */),
    submit_cnt_(true /* is_counter */),
    submit_fail_perf_cnt_(true /* is_counter */),
    submit_logical_bytes_(false /* is_counter */),
    submit_aio_bytes_(false /* is_counter */),
    complete_cnt_(true /* is_counter */),
    complete_bytes_(false /* is_counter */),
    aio_rt_us_(false /* is_counter */),
    fragment_recycle_delay_us_(false /* is_counter */),
    wait_parent_wake_cnt_(true /* is_counter */),
    wait_parent_wait_us_(false /* is_counter */),
    wait_parent_data_bytes_(false /* is_counter */),
    throttle_block_cnt_(true /* is_counter */),
    throttle_block_us_(false /* is_counter */),
    perf_reporter_("[PALF_DUMP][ASYNC PERF STAT]"),
    last_stat_print_ts_(OB_INVALID_TIMESTAMP),
    last_stat_print_interval_check_ts_(OB_INVALID_TIMESTAMP),
    last_errsim_options_refresh_ts_(OB_INVALID_TIMESTAMP),
    aio_delay_us_(0),
    stat_print_interval_us_(0),
    last_server_event_ts_(OB_INVALID_TIMESTAMP),
    pool_(),
    planner_(pool_),
    block_switch_pending_(false),
    current_write_block_id_(LOG_INVALID_BLOCK_ID),
    control_barrier_task_(NULL),
    popped_task_trace_(),
    popped_task_trace_next_idx_(0),
    popped_task_trace_cnt_(0),
    last_popped_task_trace_print_ts_(0),
    throttle_ctx_(),
    ignore_throttle_once_(false)
{
  perf_reporter_.add_item("admit_bytes", &admit_bytes_);
  perf_reporter_.add_item("publish_bytes", &publish_bytes_);
  perf_reporter_.add_item("task_publish_us", &task_publish_us_);
  perf_reporter_.add_item("submit_cnt", &submit_cnt_);
  perf_reporter_.add_item("submit_fail_cnt", &submit_fail_perf_cnt_);
  perf_reporter_.add_item("submit_logical_bytes", &submit_logical_bytes_);
  perf_reporter_.add_item("submit_aio_bytes", &submit_aio_bytes_);
  perf_reporter_.add_item("complete_cnt", &complete_cnt_);
  perf_reporter_.add_item("complete_bytes", &complete_bytes_);
  perf_reporter_.add_item("aio_rt_us", &aio_rt_us_);
  perf_reporter_.add_item("fragment_recycle_delay_us", &fragment_recycle_delay_us_);
  perf_reporter_.add_item("wait_parent_wake_cnt", &wait_parent_wake_cnt_);
  perf_reporter_.add_item("wait_parent_wait_us", &wait_parent_wait_us_);
  perf_reporter_.add_item("wait_parent_data_bytes", &wait_parent_data_bytes_);
  perf_reporter_.add_item("throttle_block_cnt", &throttle_block_cnt_);
  perf_reporter_.add_item("throttle_block_us", &throttle_block_us_);
}

AsyncPalfIOCtx::~AsyncPalfIOCtx()
{
  destroy();
}

void AsyncPalfIOCtx::free_this()
{
  AsyncPalfIOCtx *ctx = this;
  OB_DELETE(AsyncPalfIOCtx, "PalfAsyncCtx", ctx);
}

int AsyncPalfIOCtx::init(const int64_t palf_id,
                         const int cb_tg_id,
                         IPalfEnvImpl *palf_env_impl,
                         IAsyncDriveWaker *drive_waker,
                         const AsyncThrottleContext &throttle_ctx)
{
  common::ObQSyncLockWriteGuard guard(qsync_lock_);
  return init_(palf_id, cb_tg_id, palf_env_impl, drive_waker, throttle_ctx);
}

int AsyncPalfIOCtx::init_(const int64_t palf_id,
                           const int cb_tg_id,
                           IPalfEnvImpl *palf_env_impl,
                           IAsyncDriveWaker *drive_waker,
                           const AsyncThrottleContext &throttle_ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "async palf ctx init twice", KR(ret), K_(palf_id), K(palf_id));
  } else if (palf_id < 0 || 0 >= cb_tg_id || !is_valid_tenant_id(tenant_id)
             || OB_ISNULL(palf_env_impl) || OB_ISNULL(drive_waker)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid async palf ctx init argument", KR(ret), K(palf_id), K(cb_tg_id),
             K(tenant_id), KP(palf_env_impl), KP(drive_waker));
  } else if (OB_FAIL(task_queue_allocator_.init(NULL,
                                                 common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                 common::ObMemAttr(tenant_id, "PalfAsyncQ")))) {
    PALF_LOG(WARN, "async task queue allocator init failed",
             KR(ret), K(tenant_id), K(palf_id));
  } else if (OB_FAIL(task_queue_item_pool_.init(TASK_QUEUE_CAPACITY, "PalfAsyncQ", tenant_id))) {
    PALF_LOG(WARN, "async task queue item pool init failed",
             KR(ret), K(tenant_id), K(palf_id),
             "pool_count", TASK_QUEUE_CAPACITY);
  } else if (OB_FAIL(task_queue_.init(TASK_QUEUE_CAPACITY, &task_queue_allocator_,
                                      common::ObMemAttr(tenant_id, "PalfAsyncQ")))) {
    PALF_LOG(WARN, "async task queue init failed",
             KR(ret), K(tenant_id), K(palf_id), "capacity", TASK_QUEUE_CAPACITY);
  } else if (OB_FAIL(pool_.init())) {
    PALF_LOG(WARN, "physical fragment pool init failed", KR(ret), K(tenant_id), K(palf_id));
  } else if (OB_FAIL(planner_.init())) {
    PALF_LOG(WARN, "async write planner init failed", KR(ret), K(tenant_id), K(palf_id));
  } else if (OB_FAIL(palf_env_impl->get_palf_handle_impl(palf_id, palf_handle_guard_))) {
    PALF_LOG(WARN, "hold palf handle for async ctx failed", KR(ret), K(palf_id));
  } else if (!palf_handle_guard_.is_valid()
             || OB_ISNULL(palf_handle_guard_.get_palf_handle_impl())) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid held palf handle for async ctx",
             KR(ret), K(palf_id), K_(palf_handle_guard));
  } else {
    palf_id_ = palf_id;
    cb_tg_id_ = cb_tg_id;
    palf_env_impl_ = palf_env_impl;
    drive_waker_ = drive_waker;
    throttle_ctx_ = throttle_ctx;
    block_switch_pending_ = false;
    current_write_block_id_ = LOG_INVALID_BLOCK_ID;
    control_barrier_task_ = NULL;
    reset_popped_task_trace_();
    ignore_throttle_once_ = false;
    reset_diag_fields_();
    reset_perf_items_();
    last_stat_print_ts_ = OB_INVALID_TIMESTAMP;
    last_stat_print_interval_check_ts_ = OB_INVALID_TIMESTAMP;
    last_errsim_options_refresh_ts_ = OB_INVALID_TIMESTAMP;
    aio_delay_us_ = 0;
    stat_print_interval_us_ = ASYNC_CTX_STAT_PRINT_INTERVAL_US;
    last_server_event_ts_ = OB_INVALID_TIMESTAMP;
    ATOMIC_STORE(&inflight_aio_cnt_, 0);
    ATOMIC_STORE(&available_flush_task_slot_count_, FLUSH_LOG_TASK_QUEUE_CAPACITY);
    ATOMIC_STORE(&available_barrier_task_slot_count_, BARRIER_TASK_QUEUE_CAPACITY);
    ATOMIC_STORE(&active_ref_, 0);
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret) {
    destroy_();
  }
  return ret;
}

void AsyncPalfIOCtx::destroy()
{
  common::ObQSyncLockWriteGuard guard(qsync_lock_);
  destroy_();
}

void AsyncPalfIOCtx::destroy_()
{
  // destroy 前必须排空 inflight AIO 和 planner 队列. task_queue_ 中残留的
  // task 从未交给 planner; control_barrier_task_ 也仍由 ctx 持有, 因此销毁
  // 路径负责释放这两类 task. planner.reset() 只销毁已经排空的内部容器.
  AsyncQueueItem *item = NULL;
  LogIOTask *control_barrier_task = NULL;
  if (task_queue_.is_inited()) {
    const int64_t queue_cnt = task_queue_.get_total();
    if (queue_cnt > 0) {
      PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                   "async task queue is not empty when destroying ctx",
                   K_(palf_id), K(queue_cnt),
                   "task_queue_capacity", task_queue_.capacity());
    }
    while (OB_SUCCESS == pop_task_queue_item_(item)) {
      if (OB_NOT_NULL(item) && OB_NOT_NULL(item->payload_)) {
        LogIOTask *task = static_cast<LogIOTask *>(item->payload_);
        const LogIOTaskType task_type = task->get_io_task_type();
        const bool need_purge_throttling = task->need_purge_throttling();
        item->payload_ = NULL;
        record_purge_task_finished_(need_purge_throttling);
        task->free_this(palf_env_impl_);
        task = NULL;
        release_task_slot_(task_type);
      }
      free_task_queue_item_(item);
    }
  }
  control_barrier_task = control_barrier_task_;
  if (OB_NOT_NULL(control_barrier_task)) {
    PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                 "async control barrier task is not empty when destroying ctx",
                 K_(palf_id),
                 "task_type", log_io_task_type_str(control_barrier_task->get_io_task_type()),
                 KP(control_barrier_task));
    const bool need_purge_throttling = control_barrier_task->need_purge_throttling();
    const LogIOTaskType task_type = control_barrier_task->get_io_task_type();
    control_barrier_task_ = NULL;
    record_purge_task_finished_(need_purge_throttling);
    control_barrier_task->free_this(palf_env_impl_);
    control_barrier_task = NULL;
    release_task_slot_(task_type);
  }
  // planner_ 借用 pool_, 因此必须在其队列已排空后先 reset planner 销毁容器,
  // 再销毁 pool, 防止销毁阶段通过该引用访问已经失效的 slot.
  planner_.reset();
  pool_.destroy();
  task_queue_.destroy();
  task_queue_item_pool_.destroy();
  task_queue_allocator_.reset();
  block_switch_pending_ = false;
  current_write_block_id_ = LOG_INVALID_BLOCK_ID;
  control_barrier_task_ = NULL;
  reset_diag_fields_();
  reset_perf_items_();
  last_stat_print_ts_ = OB_INVALID_TIMESTAMP;
  last_stat_print_interval_check_ts_ = OB_INVALID_TIMESTAMP;
  last_errsim_options_refresh_ts_ = OB_INVALID_TIMESTAMP;
  aio_delay_us_ = 0;
  stat_print_interval_us_ = 0;
  last_server_event_ts_ = OB_INVALID_TIMESTAMP;
  cb_tg_id_ = -1;
  palf_handle_guard_.reset();
  palf_env_impl_ = NULL;
  drive_waker_ = NULL;
  is_inited_ = false;
  palf_id_ = -1;
  throttle_ctx_ = AsyncThrottleContext();
  ignore_throttle_once_ = false;
  ATOMIC_STORE(&inflight_aio_cnt_, 0);
  ATOMIC_STORE(&available_flush_task_slot_count_, 0);
  ATOMIC_STORE(&available_barrier_task_slot_count_, 0);
  reset_popped_task_trace_();
}

// planner 无效时, 在数据路径继续前用同一份 storage snapshot 重建 planner
// 和 block 状态. 调用点已保证旧 flush/AIO/publish 全部排空.
int AsyncPalfIOCtx::reset_async_state_after_tail_changed_(IPalfHandleImpl *handle,
    const LogStorage::AsyncStorageSnapshot &storage_snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(handle)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid handle for async state reset after tail changed",
             KR(ret), K_(palf_id), K(storage_snapshot));
  } else if (!is_inited_) {
    // storage tail 已经改变, 未初始化 ctx 无法重新同步 planner 位点;
    // 必须把 reset 失败返回调用方, 不能静默跳过.
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "reset_async_state_after_tail_changed_ on uninited ctx",
             KR(ret), K_(palf_id), K(storage_snapshot));
  } else {
    PlannerStatus planner_status;
    planner_.get_status(planner_status);
    const bool async_state_not_empty = (planner_status.get_pending_task_count() > 0
        || planner_status.get_active_fragment_count() > 0);
    const bool invalid_block_boundary = (0 == storage_snapshot.curr_block_writable_size
        && (!storage_snapshot.log_tail.is_valid()
            || 0 != lsn_2_offset(storage_snapshot.log_tail, PALF_BLOCK_SIZE)));
    if (async_state_not_empty || invalid_block_boundary) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "invalid async state when control barrier changes tail",
               KR(ret), K_(palf_id), K(storage_snapshot), K(planner_status),
               K(async_state_not_empty), K(invalid_block_boundary));
    } else if (OB_FAIL(planner_.reset_after_tail_changed(handle, storage_snapshot.log_tail))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "reset async planner after tail changed failed",
                 KR(ret), K_(palf_id), K(storage_snapshot));
      }
    } else {
      set_block_switch_pending_(0 == storage_snapshot.curr_block_writable_size);
      planner_.get_status(planner_status);
      if (block_switch_pending_) {
        // tail 位于 block 边界但尚未完成 switch 时, 当前可写 block 仍是
        // 边界左侧的旧 block; tail=0 表示首个 block 还没有建立.
        current_write_block_id_ = 0 == storage_snapshot.log_tail.val_
            ? LOG_INVALID_BLOCK_ID : lsn_2_block(storage_snapshot.log_tail, PALF_BLOCK_SIZE) - 1;
      } else if (OB_FAIL(refresh_current_write_block_id_(planner_status))) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "refresh async current write block after tail reset failed",
                   KR(ret), K_(palf_id), K(storage_snapshot), K(planner_status));
        }
      }
      if (OB_SUCC(ret)) {
        PALF_LOG(INFO, "async state reset after tail changed", K_(palf_id),
                 K(storage_snapshot), K_(block_switch_pending), K_(current_write_block_id));
      }
    }
  }
  return ret;
}

AsyncPalfIOCtx::AsyncQueueItem *AsyncPalfIOCtx::alloc_task_queue_item_(
    const QueueItemType type, LogIOTask *task)
{
  int ret = OB_SUCCESS;
  AsyncQueueItem *item = NULL;
  if (OB_ISNULL(task)) {
    PALF_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "null task while allocating async queue item",
                 K_(palf_id), "type", static_cast<int>(type));
  } else if (OB_FAIL(task_queue_item_pool_.alloc(item))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "alloc async task queue item from pool failed",
               KR(ret), K_(palf_id), "type", static_cast<int>(type));
    }
  } else {
    item->init(type, task->get_io_task_type(), static_cast<void *>(task));
  }
  return item;
}

int AsyncPalfIOCtx::try_reserve_task_slot(const LogIOTaskType task_type)
{
  common::ObQSyncLockReadGuard guard(qsync_lock_);
  return try_reserve_task_slot_(task_type);
}

int AsyncPalfIOCtx::try_reserve_task_slot_(const LogIOTaskType task_type)
{
  int ret = OB_SUCCESS;
  int64_t *available_slot_count = NULL;
  int64_t slot_capacity = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(TRACE, "reserve task slot on uninited async palf ctx",
             KR(ret), K_(palf_id), "task_type", static_cast<int>(task_type));
  } else if (LogIOTaskType::FLUSH_LOG_TYPE == task_type) {
    available_slot_count = &available_flush_task_slot_count_;
    slot_capacity = FLUSH_LOG_TASK_QUEUE_CAPACITY;
  } else if (is_barrier_task_type_(task_type)) {
    available_slot_count = &available_barrier_task_slot_count_;
    slot_capacity = BARRIER_TASK_QUEUE_CAPACITY;
  } else {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid async task type for slot reservation",
             KR(ret), K_(palf_id), "task_type", static_cast<int>(task_type));
  }
  if (OB_SUCC(ret)) {
    bool reserved = false;
    while (OB_SUCC(ret) && !reserved) {
      const int64_t available_count = ATOMIC_LOAD(available_slot_count);
      if (available_count <= 0) {
        ret = OB_SIZE_OVERFLOW;
        PALF_LOG(TRACE, "no async task slot available", KR(ret), K_(palf_id),
                 K(available_count), K(slot_capacity),
                 "task_queue_count", task_queue_.get_total());
      } else {
        reserved = ATOMIC_BCAS(available_slot_count, available_count, available_count - 1);
      }
    }
  }
  return ret;
}

void AsyncPalfIOCtx::release_task_slot(const LogIOTaskType task_type)
{
  common::ObQSyncLockReadGuard guard(qsync_lock_);
  release_task_slot_(task_type);
}

void AsyncPalfIOCtx::release_task_slot_(const LogIOTaskType task_type)
{
  int64_t *available_slot_count = NULL;
  int64_t slot_capacity = 0;
  if (LogIOTaskType::FLUSH_LOG_TYPE == task_type) {
    available_slot_count = &available_flush_task_slot_count_;
    slot_capacity = FLUSH_LOG_TASK_QUEUE_CAPACITY;
  } else if (is_barrier_task_type_(task_type)) {
    available_slot_count = &available_barrier_task_slot_count_;
    slot_capacity = BARRIER_TASK_QUEUE_CAPACITY;
  } else {
    PALF_LOG_RET(ERROR, OB_INVALID_ARGUMENT,
                 "invalid async task type for slot release",
                 K_(palf_id), "task_type", static_cast<int>(task_type));
  }
  if (OB_NOT_NULL(available_slot_count)) {
    bool released = false;
    while (!released) {
      const int64_t available_count = ATOMIC_LOAD(available_slot_count);
      if (available_count >= slot_capacity) {
        PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                     "async task slot release overflow",
                     K_(palf_id), K(available_count), K(slot_capacity),
                     "task_type", static_cast<int>(task_type));
        released = true;
      } else {
        released = ATOMIC_BCAS(available_slot_count, available_count, available_count + 1);
      }
    }
  }
}

void AsyncPalfIOCtx::free_task_queue_item_(AsyncQueueItem *&item)
{
  if (OB_NOT_NULL(item)) {
    item->reset();
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(task_queue_item_pool_.free(item))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG_RET(WARN, tmp_ret, "free async task queue item failed",
                     KR(tmp_ret), K_(palf_id), KPC(item));
      }
    } else {
      item = NULL;
    }
  }
}

int AsyncPalfIOCtx::peek_not_null_task_queue_item_(const QueueItemType type, AsyncQueueItem *&item)
{
  int ret = OB_SUCCESS;
  AsyncQueueItem *head_item = NULL;
  item = NULL;
  if (OB_FAIL(task_queue_.head_unsafe(head_item))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "peek async typed task queue item failed",
                 KR(ret), K_(palf_id), "type", static_cast<int>(type));
      }
    }
  } else if (OB_ISNULL(head_item) || OB_ISNULL(head_item->payload_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid async typed task queue item",
             KR(ret), K_(palf_id), "type", static_cast<int>(type), KPC(head_item));
  } else if (type != head_item->type_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    item = head_item;
  }
  return ret;
}

int AsyncPalfIOCtx::pop_task_queue_item_(AsyncQueueItem *&item)
{
  int ret = OB_SUCCESS;
  item = NULL;
  if (OB_FAIL(task_queue_.pop(item))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
    } else if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "pop async task queue item failed", KR(ret), K_(palf_id));
    }
  } else {
    record_popped_task_trace_(item);
  }
  return ret;
}

int AsyncPalfIOCtx::discard_queued_tasks_(int64_t &discarded_flush_count,
                                          int64_t &discarded_barrier_count)
{
  int ret = OB_SUCCESS;
  discarded_flush_count = 0;
  discarded_barrier_count = 0;
  while (task_queue_.get_total() > 0) {
    AsyncQueueItem *item = NULL;
    const int tmp_ret = pop_task_queue_item_(item);
    if (OB_SUCCESS != tmp_ret) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
      PALF_LOG(ERROR, "pop queued task after handle deletion failed",
               KR(tmp_ret), K_(palf_id), "task_queue_count", task_queue_.get_total());
      break;
    } else if (OB_ISNULL(item)) {
      ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      PALF_LOG(ERROR, "null queued task item after handle deletion",
               KR(ret), K_(palf_id));
    } else {
      const LogIOTaskType task_type = item->task_type_;
      LogIOTask *task = static_cast<LogIOTask *>(item->payload_);
      item->payload_ = NULL;
      if (LogIOTaskType::FLUSH_LOG_TYPE == task_type) {
        ++discarded_flush_count;
      } else {
        ++discarded_barrier_count;
      }
      if (OB_ISNULL(task)) {
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
        PALF_LOG(ERROR, "null queued task payload after handle deletion",
                 KR(ret), K_(palf_id), "task_type", log_io_task_type_str(task_type));
      } else {
        record_purge_task_finished_(task->need_purge_throttling());
        task->free_this(palf_env_impl_);
        task = NULL;
      }
      release_task_slot_(task_type);
      free_task_queue_item_(item);
    }
  }

  if (OB_NOT_NULL(control_barrier_task_)) {
    LogIOTask *task = control_barrier_task_;
    const LogIOTaskType task_type = task->get_io_task_type();
    control_barrier_task_ = NULL;
    ++discarded_barrier_count;
    record_purge_task_finished_(task->need_purge_throttling());
    task->free_this(palf_env_impl_);
    task = NULL;
    release_task_slot_(task_type);
  }
  return ret;
}

int AsyncPalfIOCtx::enqueue_task(LogIOTask *task)
{
  common::ObQSyncLockReadGuard guard(qsync_lock_);
  return enqueue_task_(task);
}

int AsyncPalfIOCtx::enqueue_task_(LogIOTask *&task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(TRACE, "enqueue task on uninited async palf ctx", KR(ret), K_(palf_id), KP(task));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "enqueue null async io task", KR(ret), K_(palf_id));
  } else {
    const bool need_purge_throttling = task->need_purge_throttling();
    AsyncQueueItem *item = alloc_task_queue_item_(get_queue_item_type_(task), task);
    if (OB_ISNULL(item)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PALF_LOG(ERROR, "alloc async task queue item failed", KR(ret), K_(palf_id));
    } else if (OB_FAIL(task_queue_.push(item))) {
      PALF_LOG(ERROR, "push async task queue item failed despite reserved task credit",
               KR(ret), K_(palf_id), "task_queue_count", task_queue_.get_total(),
               "task_queue_capacity", task_queue_.capacity(), KPC(task));
    } else {
      item = NULL;
      if (need_purge_throttling
          && has_async_throttle_()
          && OB_NOT_NULL(throttle_ctx_.purge_task_count)) {
        ATOMIC_INC(throttle_ctx_.purge_task_count);
      }
      task = NULL;
    }
    if (OB_NOT_NULL(item)) {
      free_task_queue_item_(item);
    }
  }
  return ret;
}

int AsyncPalfIOCtx::request_drive()
{
  common::ObQSyncLockReadGuard guard(qsync_lock_);
  return request_drive_();
}

int AsyncPalfIOCtx::request_drive_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(drive_waker_)) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "async drive waker is null", KR(ret), K_(palf_id));
  } else if (OB_FAIL(drive_waker_->wake_up_for_drive())) {
    if (OB_NOT_RUNNING != ret) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "wake async io worker for drive failed",
                 KR(ret), K_(palf_id));
      }
    }
  }
  return ret;
}

int AsyncPalfIOCtx::drive_write(int64_t &next_drive_interval_us)
{
  common::ObQSyncLockReadGuard guard(qsync_lock_);
  return drive_write_(next_drive_interval_us);
}

int AsyncPalfIOCtx::drive_write_(int64_t &next_drive_interval_us)
{
  int ret = OB_SUCCESS;
  next_drive_interval_us = INT64_MAX;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(TRACE, "drive write on uninited async palf ctx", KR(ret), K_(palf_id));
  } else {
    refresh_errsim_options_();
    bool need_drive_pass = true;
    need_drive_pass = 0 != get_pending_async_stage_count_()
        || OB_NOT_NULL(control_barrier_task_);
    if (need_drive_pass) {
      IPalfHandleImpl *handle = palf_handle_guard_.get_palf_handle_impl();
      int tmp_ret = OB_SUCCESS;
      // Step 0: the ctx pins its handle for the complete registration lifetime.
      // check_can_be_used() is still checked every round because the pin keeps
      // memory alive but does not make a permanently deleted PALF usable.
      if (OB_ISNULL(handle)) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "held null handle in drive_write",
                 KR(ret), K_(palf_id));
      } else if (!handle->check_can_be_used()) {
        if (OB_TMP_FAIL(discard_tasks_after_handle_deleted_(next_drive_interval_us))) {
          ret = tmp_ret;
          PALF_LOG(ERROR, "discard async tasks after handle deletion failed",
                   KR(ret), K_(palf_id));
        }
      } else {
        bool control_barrier_consumed = false;
        bool skip_data_drive = false;
        // 步骤 1：先取队首 barrier；如果前面的 flush、AIO、publish 都排空，
        // 就执行这个 barrier。barrier 可能改变 storage tail，数据路径下一轮再继续。
        if (OB_TMP_FAIL(pop_head_control_barrier_task_())) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          if (REACH_THREAD_TIME_INTERVAL(1_s)) {
            PALF_LOG(WARN, "pop async control barrier task failed",
                     KR(tmp_ret), K_(palf_id));
          }
        } else {
          if (OB_TMP_FAIL(execute_control_barrier_task_(control_barrier_consumed))) {
            ret = OB_SUCC(ret) ? tmp_ret : ret;
            if (REACH_THREAD_TIME_INTERVAL(1_s)) {
              PALF_LOG(WARN, "execute async control barrier task failed",
                       KR(ret), KR(tmp_ret), K_(palf_id));
            }
          }
          if (control_barrier_consumed) {
            next_drive_interval_us = 0;
            skip_data_drive = true;
          }
        }
        // 步骤 2：初始化或 tail reset 后 planner 可能还没有有效 tail。
        // 数据路径开始前，用当前 storage tail 重建 planner 状态。
        PlannerStatus planner_status;
        planner_.get_status(planner_status);
        if (!skip_data_drive && !planner_status.get_planned_end_lsn().is_valid()) {
          AsyncQueueItem *head_item = NULL;
          // 只有队首 flush task 才有数据路径上下文来刷新 planner。
          // 其他队首类型先等待 barrier/control 工作让 tail 变成有效状态。
          if (OB_TMP_FAIL(peek_not_null_task_queue_item_(QueueItemType::FLUSH_LOG, head_item))) {
            if (OB_ENTRY_NOT_EXIST != tmp_ret) {
              ret = OB_SUCC(ret) ? tmp_ret : ret;
              if (REACH_THREAD_TIME_INTERVAL(1_s)) {
                PALF_LOG(WARN, "peek async task queue head before planner refresh failed",
                         KR(tmp_ret), K_(palf_id));
              }
            }
            skip_data_drive = true;
          } else if (OB_TMP_FAIL(refresh_planner_state_before_drive_(handle))) {
            ret = OB_SUCC(ret) ? tmp_ret : ret;
            skip_data_drive = true;
            if (REACH_THREAD_TIME_INTERVAL(1_s)) {
              PALF_LOG(WARN, "refresh async planner state before drive failed",
                       KR(tmp_ret), K_(palf_id));
            }
          }
        }
        if (!skip_data_drive) {
          // 步骤 3：处理已完成的 AIO fragment，推进 planner 持久化前缀，
          // 释放完成的 fragment slot，并 publish 已持久化 task。
          if (OB_TMP_FAIL(drive_phase_completion_(handle))) {
            ret = OB_SUCC(ret) ? tmp_ret : ret;
            if (REACH_THREAD_TIME_INTERVAL(1_s)) {
              PALF_LOG(WARN, "drive async completion phase failed",
                       KR(ret), KR(tmp_ret), K_(palf_id));
            }
          }
          // 步骤 4：按需切 block，然后 admit/plan 队列里的 flush task，
          // 直到遇到真正的阻塞条件。即使本轮 completion 的 publish/free 失败，
          // 这里仍然可以继续推进已有队列。
          if (OB_TMP_FAIL(drive_phase_task_(handle))) {
            ret = OB_SUCC(ret) ? tmp_ret : ret;
            if (REACH_THREAD_TIME_INTERVAL(1_s)) {
              PALF_LOG(WARN, "drive async task phase failed",
                       KR(ret), KR(tmp_ret), K_(palf_id));
            }
          }
          // 步骤 5：提交 READY 或到重试时间的 FAILED fragment。
          // 本轮刚 admit 出来的 fragment 也可以在同一轮提交。
          if (OB_TMP_FAIL(drive_phase_fragment_(handle))) {
            ret = OB_SUCC(ret) ? tmp_ret : ret;
            if (REACH_THREAD_TIME_INTERVAL(1_s)) {
              PALF_LOG(WARN, "drive async fragment phase failed",
                       KR(ret), KR(tmp_ret), K_(palf_id));
            }
          }
        }
        // 步骤 6：计算最早需要再次推进的相对时间。barrier 已消费时保持 0，
        // 让调用方立即开始下一轮；其余情况同时考虑 fragment retry、AIO
        // 延迟注入和 throttle deadline。
        if (next_drive_interval_us > 0
            && OB_TMP_FAIL(get_next_drive_interval_(next_drive_interval_us))) {
          ret = OB_SUCC(ret) ? tmp_ret : ret;
          if (REACH_THREAD_TIME_INTERVAL(1_s)) {
            PALF_LOG(WARN, "get async next drive interval failed",
                     KR(ret), KR(tmp_ret), K_(palf_id));
          }
        }
      }
    } else {
      PALF_LOG(TRACE, "skip async drive write without pending work",
               K_(palf_id), K(need_drive_pass));
    }
  }
  print_async_ctx_stat_();
  return ret;
}

void AsyncPalfIOCtx::refresh_errsim_options_()
{
  if (palf_reach_time_interval(1_s, last_errsim_options_refresh_ts_)) {
    int tmp_ret = OB_SUCCESS;
    const int64_t fragment_max_size_in_4k =
        abs(static_cast<int64_t>(ERRSIM_PALF_ASYNC_FRAGMENT_MAX_SIZE_IN_4K));
    const int64_t wait_parent_max_size_in_4k =
        abs(static_cast<int64_t>(ERRSIM_PALF_ASYNC_WAIT_PARENT_MAX_SIZE_IN_4K));
    const int64_t aio_delay_us =
        abs(static_cast<int64_t>(ERRSIM_PALF_ASYNC_AIO_DELAY_US));
    const int64_t fragment_max_size = 0 == fragment_max_size_in_4k
        ? NORMAL_FRAGMENT_MAX_SIZE
        : fragment_max_size_in_4k * LOG_DIO_ALIGN_SIZE;
    const int64_t wait_parent_max_size = 0 == wait_parent_max_size_in_4k
        ? WAIT_PARENT_FRAGMENT_MAX_SIZE
        : wait_parent_max_size_in_4k * LOG_DIO_ALIGN_SIZE;
    if (OB_TMP_FAIL(planner_.update_fragment_size_limits(fragment_max_size,
                                                         wait_parent_max_size))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG_RET(WARN, tmp_ret, "update async fragment size limits from errsim failed",
                     KR(tmp_ret), K_(palf_id), K(fragment_max_size_in_4k),
                     K(wait_parent_max_size_in_4k), K(fragment_max_size),
                     K(wait_parent_max_size));
      }
    }
    if (OB_TMP_FAIL(planner_.update_aio_delay(aio_delay_us))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG_RET(WARN, tmp_ret, "update async AIO delay from errsim failed",
                     KR(tmp_ret), K_(palf_id), K(aio_delay_us));
      }
    } else {
      aio_delay_us_ = aio_delay_us;
    }
  }
}

// ASYNC-CTX-STAT: collect one control-plane snapshot on the worker thread.
// The collection is separate from logging so aggregation can be tested directly.
void AsyncPalfIOCtx::collect_stat_(AsyncCtxStatSnapshot &out)
{
  PlannerStatus planner_status;
  PhysicalWriteFragmentPoolStat fragment_pool_stat;
  planner_.get_status(planner_status);
  pool_.get_stat(fragment_pool_stat);
  out.init(palf_id_,
           task_queue_.get_total(),
           ATOMIC_LOAD(&inflight_aio_cnt_),
           planner_status.get_pending_task_count(),
           fragment_pool_stat,
           ATOMIC_LOAD(&submit_fail_cnt_),
           ATOMIC_LOAD(&complete_fail_cnt_),
           block_switch_pending_,
           planner_status.get_planned_end_lsn(),
           planner_status.get_persisted_lsn());
}

// ASYNC-CTX-STAT: throttled periodic dump of the async ctx state. Always-on
// debug info so a future silent stall is readable from observer.log instead
// of needing gdb. INFO normally (a healthy LS's advancing LSNs are useful
// for comparison); WARN when the snapshot looks_stuck().
void AsyncPalfIOCtx::print_async_ctx_stat_()
{
  static constexpr int64_t ASYNC_CTX_SERVER_EVENT_INTERVAL_US = 60LL * 1000 * 1000;
  if (stat_print_interval_us_ <= 0) {
    stat_print_interval_us_ = ASYNC_CTX_STAT_PRINT_INTERVAL_US;
  }
  if (palf_reach_time_interval(1_s, last_stat_print_interval_check_ts_)) {
    const int64_t errsim_interval_ms = abs(ERRSIM_PALF_ASYNC_CTX_STAT_PRINT_INTERVAL_MS);
    stat_print_interval_us_ = errsim_interval_ms > 0
        ? errsim_interval_ms * 1000
        : ASYNC_CTX_STAT_PRINT_INTERVAL_US;
  }
  // palf_reach_time_interval() updates the print timestamp internally and
  // returns true at most once per configured interval, so leaving this on every
  // drive round is cheap (the snapshot only runs when it fires).
  if (palf_reach_time_interval(stat_print_interval_us_, last_stat_print_ts_)) {
    const int64_t now = common::ObTimeUtility::current_time();
    int64_t oldest_task_age_us = 0;
    AsyncCtxStatSnapshot stat;
    collect_stat_(stat);
    if (task_queue_.get_total() > 0) {
      AsyncQueueItem *head_item = NULL;
      if (OB_SUCCESS == peek_not_null_task_queue_item_(QueueItemType::FLUSH_LOG, head_item)) {
        LogIOTask *head = static_cast<LogIOTask *>(head_item->payload_);
        if (head->get_init_task_ts() > 0) {
          oldest_task_age_us = now - head->get_init_task_ts();
        }
      }
    }
    if (stat.looks_stuck()) {
      PALF_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "[PALF_DUMP][ASYNC CTX STAT] possible stuck",
                   KP(this), K(stat), K_(pool), KPC(this));
      if (palf_reach_time_interval(ASYNC_CTX_SERVER_EVENT_INTERVAL_US, last_server_event_ts_)) {
        add_async_ctx_server_event_("ctx_possible_stuck", stat, OB_ERR_TOO_MUCH_TIME);
      }
    } else {
      PALF_LOG(INFO, "[PALF_DUMP][ASYNC CTX STAT]", KP(this), K(stat), K_(pool), KPC(this));
    }
    print_perf_stat_(now, oldest_task_age_us);
  }
  print_popped_task_trace_();
  planner_.print_fragment_ref_trace();
}

AsyncPalfIOCtx::PoppedTaskTraceEntry::PoppedTaskTraceEntry()
{
  reset();
}

void AsyncPalfIOCtx::PoppedTaskTraceEntry::reset()
{
  pop_ts = 0;
  queue_type = QueueItemType::FLUSH_LOG;
  task_type = LogIOTaskType::FLUSH_LOG_TYPE;
  begin_lsn.reset();
  data_len = 0;
  buf0 = NULL;
  buf1 = NULL;
  buf1_len = 0;
  task = NULL;
}

void AsyncPalfIOCtx::PoppedTaskTraceEntry::init(const int64_t new_pop_ts,
                                                const QueueItemType new_queue_type,
                                                const LogIOTaskType new_task_type,
                                                const LSN &new_begin_lsn,
                                                const int64_t new_data_len,
                                                const char *new_buf0,
                                                const char *new_buf1,
                                                const int64_t new_buf1_len,
                                                LogIOTask *new_task)
{
  pop_ts = new_pop_ts;
  queue_type = new_queue_type;
  task_type = new_task_type;
  begin_lsn = new_begin_lsn;
  data_len = new_data_len;
  buf0 = new_buf0;
  buf1 = new_buf1;
  buf1_len = new_buf1_len;
  task = new_task;
}

void AsyncPalfIOCtx::reset_popped_task_trace_()
{
  for (int64_t i = 0; i < POPPED_TASK_TRACE_CAPACITY; ++i) {
    popped_task_trace_[i].reset();
  }
  popped_task_trace_next_idx_ = 0;
  popped_task_trace_cnt_ = 0;
  last_popped_task_trace_print_ts_ = 0;
}

void AsyncPalfIOCtx::record_popped_task_trace_(AsyncQueueItem *item)
{
  LogIOTask *task = NULL;
  LogIOTaskType task_type = LogIOTaskType::FLUSH_LOG_TYPE;
  LSN begin_lsn;
  int64_t data_len = 0;
  const char *buf0 = NULL;
  const char *buf1 = NULL;
  int64_t buf0_len = 0;
  int64_t buf1_len = 0;
  int tmp_ret = OB_SUCCESS;
  const QueueItemType queue_type = OB_NOT_NULL(item) ? item->type_ : QueueItemType::BARRIER_TASK;
  if (OB_NOT_NULL(item) && OB_NOT_NULL(item->payload_)) {
    task = static_cast<LogIOTask *>(item->payload_);
    task_type = task->get_io_task_type();
    if (LogIOTaskType::FLUSH_LOG_TYPE == task_type) {
      const LogIOFlushLogTask *flush_task = static_cast<const LogIOFlushLogTask *>(task);
      const LogWriteBuf &write_buf = flush_task->get_write_buf();
      begin_lsn = flush_task->get_flush_begin_lsn();
      const LSN end_lsn = flush_task->get_flush_end_lsn();
      if (begin_lsn.is_valid() && end_lsn.is_valid() && end_lsn >= begin_lsn) {
        data_len = end_lsn - begin_lsn;
      }
      if (write_buf.get_buf_count() > 0) {
        tmp_ret = write_buf.get_write_buf(0, buf0, buf0_len);
        if (OB_SUCCESS != tmp_ret) {
          if (REACH_THREAD_TIME_INTERVAL(1_s)) {
            PALF_LOG_RET(WARN, tmp_ret, "get first popped task write buffer failed",
                         KR(tmp_ret), K_(palf_id), K(write_buf));
          }
        }
      }
      if (write_buf.get_buf_count() > 1) {
        tmp_ret = write_buf.get_write_buf(1, buf1, buf1_len);
        if (OB_SUCCESS != tmp_ret) {
          if (REACH_THREAD_TIME_INTERVAL(1_s)) {
            PALF_LOG_RET(WARN, tmp_ret, "get second popped task write buffer failed",
                         KR(tmp_ret), K_(palf_id), K(write_buf));
          }
        }
      }
    } else if (LogIOTaskType::TRUNCATE_LOG_TYPE == task_type) {
      const LogIOTruncateLogTask *truncate_task = static_cast<const LogIOTruncateLogTask *>(task);
      begin_lsn = truncate_task->get_truncate_lsn();
      data_len = 0;
    }
  }
  PoppedTaskTraceEntry &entry = popped_task_trace_[popped_task_trace_next_idx_];
  entry.init(common::ObTimeUtility::current_time(), queue_type,
             task_type, begin_lsn, data_len, buf0, buf1, buf1_len, task);
  popped_task_trace_next_idx_ = (popped_task_trace_next_idx_ + 1) % POPPED_TASK_TRACE_CAPACITY;
  if (popped_task_trace_cnt_ < POPPED_TASK_TRACE_CAPACITY) {
    ++popped_task_trace_cnt_;
  }
}

void AsyncPalfIOCtx::print_popped_task_trace_()
{
  if (popped_task_trace_cnt_ > 0
      && palf_reach_time_interval(POPPED_TASK_TRACE_PRINT_INTERVAL_US,
                                  last_popped_task_trace_print_ts_)) {
    const int64_t trace_start_idx = (popped_task_trace_next_idx_ + POPPED_TASK_TRACE_CAPACITY
        - popped_task_trace_cnt_) % POPPED_TASK_TRACE_CAPACITY;
    const int64_t remaining_cnt = POPPED_TASK_TRACE_CAPACITY - trace_start_idx;
    const int64_t first_part_cnt = popped_task_trace_cnt_ < remaining_cnt
        ? popped_task_trace_cnt_ : remaining_cnt;
    const int64_t second_part_cnt = popped_task_trace_cnt_ - first_part_cnt;
    common::ObArrayWrap<PoppedTaskTraceEntry> first_part(
        popped_task_trace_ + trace_start_idx, first_part_cnt);
    common::ObArrayWrap<PoppedTaskTraceEntry> second_part(popped_task_trace_, second_part_cnt);
    // The first part starts at the oldest entry; the second part continues from
    // physical index zero after the ring wraps.
    if (0 == second_part_cnt) {
      PALF_LOG(INFO, "[PALF_DUMP][ASYNC POPPED TASK TRACE]", K_(palf_id),
               K(trace_start_idx), K_(popped_task_trace_next_idx), K(first_part));
    } else {
      PALF_LOG(INFO, "[PALF_DUMP][ASYNC POPPED TASK TRACE]", K_(palf_id),
               K(trace_start_idx), K_(popped_task_trace_next_idx), K(first_part), K(second_part));
    }
  }
}

void AsyncPalfIOCtx::add_async_ctx_server_event_(const char *event,
                                                 const AsyncCtxStatSnapshot &stat,
                                                 const int ret)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(event)) {
    PALF_LOG_RET(ERROR, OB_INVALID_ARGUMENT,
                 "invalid null async ctx server event", KR(ret), K(stat));
  } else if (OB_TMP_FAIL(SERVER_EVENT_ADD("PALF_ASYNC_IO", event,
                                          "tenant_id", MTL_ID(),
                                          "palf_id", stat.get_palf_id(),
                                          "inflight_aio", stat.get_inflight_aio_cnt(),
                                          "pending_task", stat.get_pending_task_cnt(),
                                          "submit_fail", stat.get_submit_fail_cnt(),
                                          "ret", ret))) {
    PALF_LOG(WARN, "add async ctx server event failed", KR(tmp_ret), K(event), KR(ret), K(stat));
  }
}

void AsyncPalfIOCtx::reset_diag_fields_()
{
  ATOMIC_STORE(&submit_fail_cnt_, 0);
  ATOMIC_STORE(&complete_fail_cnt_, 0);
  ATOMIC_STORE(&stale_completion_cnt_, 0);
  ATOMIC_STORE(&published_entry_cnt_, 0);
  block_switch_pending_since_ts_ = OB_INVALID_TIMESTAMP;
  throttle_error_count_ = 0;
  last_throttle_error_ts_ = OB_INVALID_TIMESTAMP;
  throttle_next_admit_ts_ = 0;
  throttle_wait_until_ts_ = 0;
}

void AsyncPalfIOCtx::reset_perf_items_()
{
  admit_bytes_.reset();
  publish_bytes_.reset();
  task_publish_us_.reset();
  submit_cnt_.reset();
  submit_fail_perf_cnt_.reset();
  submit_logical_bytes_.reset();
  submit_aio_bytes_.reset();
  complete_cnt_.reset();
  complete_bytes_.reset();
  aio_rt_us_.reset();
  fragment_recycle_delay_us_.reset();
  wait_parent_wake_cnt_.reset();
  wait_parent_wait_us_.reset();
  wait_parent_data_bytes_.reset();
  throttle_block_cnt_.reset();
  throttle_block_us_.reset();
}

void AsyncPalfIOCtx::print_perf_stat_(const int64_t now_us, const int64_t oldest_task_age_us)
{
  perf_reporter_.print(now_us, oldest_task_age_us);
}

int AsyncPalfIOCtx::discard_tasks_after_handle_deleted_(int64_t &next_drive_interval_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t discarded_queued_flush_count = 0;
  int64_t discarded_barrier_count = 0;
  int64_t discarded_planner_task_count = 0;
  int64_t remaining_submitted_count = 0;
  int64_t inflight_aio_count = 0;
  next_drive_interval_us = INT64_MAX;

  /*
   * Legacy LogIOWorker resolves PalfHandleImpl independently in do_task() and
   * after_consume(). Once PalfEnv marks the handle deleted, either lookup is
   * rejected and the current owner frees the task without running the logical
   * callback. The async ctx keeps one guard only to pin PalfHandleImpl memory;
   * check_can_be_used()==false must retain the same permanent-delete behavior.
   *
   * This discard depends on the existing upper-layer lifecycle contract. In
   * normal LS GC, ObLogHandler::stop() has taken its write lock before removal,
   * so read-locked synchronous operations such as vote/config changes have
   * finished and no waiter still depends on an IO callback. The direct cleanup
   * used after failed LS creation exposes no concurrent waiter either. Without
   * this contract, both legacy and async workers could strand a synchronous
   * waiter by dropping its meta callback.
   *
   * Task treatment intentionally follows ownership boundaries:
   *  - queued flush tasks have not reached disk. SlidingWindow has already
   *    stopped during normal LS removal, so no caller still waits for their
   *    flush callback; tasks not yet admitted by the planner can be freed;
   *  - FLUSH_META, TRUNCATE_PREFIX, TRUNCATE_LOG, and FLASHBACK_LOG are
   *    synchronous barriers during normal operation. Dropping them before
   *    ObLogHandler::stop() would strand their waiters, which is why this path
   *    relies on the upper-layer stop contract described above. After stop,
   *    they have not run and can be freed without invoking their callbacks;
   *  - PURGE_THROTTLING has no durable IO result to preserve. Freeing it also
   *    releases its purge counter through record_purge_task_finished_();
   *  - ASYNC_MARK is consumed by LogAsyncIOWorker and never enters this ctx;
   *  - control_barrier_task_ has not entered do_task() and is handled likewise;
   *  - planner flush tasks keep group-buffer memory alive until every submitted
   *    AIO is closed, because fragments borrow that memory without copying;
   *  - submitted AIO is only polled/completed here, never retried or published;
   *  - tasks already transferred to the callback pool are no longer in this
   *    ctx and remain owned and freed by that pool.
   *
   * Skipping this cleanup would leave task credits or planner fragments behind,
   * causing unregister_palf_ctx_and_wait() to wait forever after handle deletion.
   */
  if (OB_TMP_FAIL(discard_queued_tasks_(discarded_queued_flush_count,
                                        discarded_barrier_count))) {
    ret = tmp_ret;
    PALF_LOG(ERROR, "discard queued async tasks after handle deletion failed",
             KR(tmp_ret), K_(palf_id));
  }
  if (OB_TMP_FAIL(poll_submitted_fragments_(remaining_submitted_count))) {
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "poll submitted AIO after handle deletion failed",
               KR(tmp_ret), K_(palf_id), K(remaining_submitted_count));
    }
  }

  inflight_aio_count = ATOMIC_LOAD(&inflight_aio_cnt_);
  if (inflight_aio_count < 0) {
    ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
    PALF_LOG(ERROR, "negative inflight AIO count after handle deletion",
             KR(ret), K_(palf_id), K(inflight_aio_count));
  } else if (0 == inflight_aio_count && remaining_submitted_count > 0) {
    ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
    PALF_LOG(ERROR, "submitted fragment remains without inflight AIO",
             KR(ret), K_(palf_id), K(remaining_submitted_count));
  } else if (0 == inflight_aio_count) {
    PlannerStatus planner_status;
    planner_.get_status(planner_status);
    if (planner_status.get_pending_task_count() > 0
        || planner_status.get_active_fragment_count() > 0
        || planner_status.has_pending_source()) {
      // No physical IO can still dereference the borrowed group-buffer memory.
      // Recycle fragment handles before planner frees the owning flush tasks.
      pool_.reuse();
      if (OB_TMP_FAIL(planner_.discard_all_tasks(palf_env_impl_,
                                                 discarded_planner_task_count))) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        PALF_LOG(ERROR, "discard planner tasks after handle deletion failed",
                 KR(tmp_ret), K_(palf_id), K(discarded_planner_task_count));
      }
      for (int64_t i = 0; i < discarded_planner_task_count; ++i) {
        release_task_slot_(LogIOTaskType::FLUSH_LOG_TYPE);
      }
      set_block_switch_pending_(false);
      current_write_block_id_ = LOG_INVALID_BLOCK_ID;
      throttle_next_admit_ts_ = 0;
      throttle_wait_until_ts_ = 0;
      ignore_throttle_once_ = false;
    }
  }

  if (discarded_queued_flush_count > 0 || discarded_barrier_count > 0
      || discarded_planner_task_count > 0) {
    PALF_LOG(INFO, "discard async tasks after palf handle deletion",
             K_(palf_id), K(discarded_queued_flush_count),
             K(discarded_barrier_count), K(discarded_planner_task_count),
             K(inflight_aio_count), K(remaining_submitted_count));
  }
  return ret;
}

// ===========================================================================
// Completion stage: close AIO attempts, advance persisted prefix, and publish.
// ===========================================================================
int AsyncPalfIOCtx::poll_submitted_fragment_(PhysicalWriteFragment &frag,
                                             const int64_t now)
{
  int ret = OB_SUCCESS;
  // 设备在 submit 后失败时, ObIOManager 可能只完成 ObIOResult 而不执行
  // LogAsyncIOCallback::inner_process. fragment 保留的 ObIOHandle 使 result
  // 继续有效, worker 可轮询并关闭这种“已完成但没有 callback”的 AIO,
  // 避免 fragment 永久停在 SUBMITTED 并阻塞 persisted prefix.
  if (frag.is_submitted()) {
    bool io_done = false;
    const FragmentRef fragment_ref = frag.get_fragment_ref();
    const int check_ret = frag.get_io_handle().check_is_finished(io_done);
    if (OB_SUCCESS != check_ret) {
      bool completed_by_me = false;
      if (OB_FAIL(handle_completed_fragment_(frag, fragment_ref, check_ret, now,
                                             true /* polled_completion */, completed_by_me))) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "handle failed async io check failed",
                   KR(ret), KR(check_ret), K_(palf_id), K(fragment_ref));
        }
      } else if (completed_by_me) {
        ret = check_ret;
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "check async io finished failed, retry fragment later",
                   KR(ret), K_(palf_id), K(fragment_ref));
        }
      }
    } else if (io_done) {
      const int io_ret = frag.get_io_handle().get_io_ret();
      bool completed_by_me = false;
      if (OB_FAIL(handle_completed_fragment_(frag, fragment_ref, io_ret, now,
                                             true /* polled_completion */, completed_by_me))
          && REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "handle polled async AIO completion failed",
                 KR(ret), K_(palf_id), KR(io_ret), K(fragment_ref));
      }
    }
    // io_done == false: still truly in flight; skip, retry next round.
  }
  return ret;
}

int AsyncPalfIOCtx::poll_submitted_fragments_(int64_t &remaining_submitted_count)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t now = common::ObTimeUtility::current_time();
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> submitted_fragments;
  const PhysicalWriteFragmentStateFilter submitted_filter(AsyncFragmentState::SUBMITTED);
  remaining_submitted_count = 0;
  if (OB_FAIL(pool_.collect_fragments(submitted_fragments, submitted_filter))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "collect submitted async fragments failed",
               KR(ret), K_(palf_id));
    }
  } else {
    for (int64_t i = 0; i < submitted_fragments.count(); ++i) {
      PhysicalWriteFragment *frag = submitted_fragments.at(i);
      if (OB_ISNULL(frag)) {
        ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
        PALF_LOG(ERROR, "null submitted async fragment", KR(ret), K_(palf_id), K(i));
      } else {
        if (OB_TMP_FAIL(poll_submitted_fragment_(*frag, now))) {
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
          if (REACH_THREAD_TIME_INTERVAL(1_s)) {
            PALF_LOG(WARN, "handle polled async AIO completion failed",
                     KR(tmp_ret), K_(palf_id), K(i));
          }
        }
        if (frag->is_submitted()) {
          ++remaining_submitted_count;
        }
      }
    }
  }
  return ret;
}

int AsyncPalfIOCtx::handle_completed_fragment_(PhysicalWriteFragment &frag,
                                               const FragmentRef &fragment_ref,
                                               const int io_ret,
                                               const int64_t finish_ts,
                                               const bool polled_completion,
                                               bool &completed_by_me)
{
  int ret = OB_SUCCESS;
  int64_t completed_data_len = 0;
  int64_t submit_ts = OB_INVALID_TIMESTAMP;
  completed_by_me = false;
  const int mark_ret = frag.mark_io_completed(fragment_ref,
                                              io_ret,
                                              finish_ts + FRAGMENT_RETRY_INTERVAL_US,
                                              finish_ts,
                                              completed_by_me,
                                              completed_data_len,
                                              submit_ts);
  if (OB_ENTRY_NOT_EXIST == mark_ret) {
    PALF_LOG(TRACE, "async AIO completion is stale",
             KR(mark_ret), K_(palf_id), KR(io_ret), K(fragment_ref), K(polled_completion));
  } else if (OB_SUCCESS != mark_ret) {
    ret = mark_ret;
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "mark async AIO completion failed",
               KR(ret), K_(palf_id), KR(io_ret), K(fragment_ref), K(polled_completion));
    }
  } else if (!completed_by_me) {
    PALF_LOG(TRACE, "async AIO completion already handled",
             K_(palf_id), KR(io_ret), K(fragment_ref), K(polled_completion));
  } else {
    if (polled_completion && OB_SUCCESS != io_ret) {
      frag.reset_io_handle();
    }
    if (OB_SUCCESS != io_ret) {
      ATOMIC_INC(&complete_fail_cnt_);
      PALF_LOG(TRACE, "async AIO completion failed",
               K_(palf_id), KR(io_ret), K(polled_completion));
    } else if (completed_data_len <= 0) {
      PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid completed async fragment data length",
                   K_(palf_id), K(completed_data_len), K(polled_completion));
    } else {
      complete_bytes_.record(finish_ts, completed_data_len);
    }
    complete_cnt_.record(finish_ts, 1);
    if (submit_ts > 0 && finish_ts >= submit_ts) {
      const int64_t aio_rt_us = finish_ts - submit_ts;
      aio_rt_us_.record(finish_ts, aio_rt_us);
    }
    ATOMIC_DEC(&inflight_aio_cnt_);
  }
  return ret;
}

int AsyncPalfIOCtx::drive_phase_completion_(IPalfHandleImpl *handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t remaining_submitted_count = 0;
  // 第一遍兜底轮询 callback 未执行但底层 IO 已结束的 SUBMITTED fragment.
  if (OB_TMP_FAIL(poll_submitted_fragments_(remaining_submitted_count))) {
    ret = tmp_ret;
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "poll submitted async fragments failed",
               KR(tmp_ret), K_(palf_id), K(remaining_submitted_count));
    }
  }
  // 第二遍回收完成延迟已到期的 FINISHED slot, 使用 planner 队列保留的
  // LSN 区间推进 persisted prefix, 再 publish 已持久化 task.
  if (OB_TMP_FAIL(pool_.free_all_finished_fragments(aio_delay_us_,
                                                    &fragment_recycle_delay_us_))) {
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "free finished async fragments failed",
               KR(tmp_ret), K_(palf_id));
    }
  }
  if (OB_TMP_FAIL(planner_.advance_finished_fragment_prefix())) {
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "drive async planner completion failed",
               KR(tmp_ret), K_(palf_id));
    }
  }
  if (OB_TMP_FAIL(try_publish_contiguous_prefix_(handle))) {
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "publish async contiguous prefix failed",
               KR(tmp_ret), K_(palf_id));
    }
  }
  return ret;
}

// ===========================================================================
// Fragment stage: submit READY and due-retry FAILED fragments.
// ===========================================================================
int AsyncPalfIOCtx::drive_phase_fragment_(IPalfHandleImpl *handle)
{
  int ret = OB_SUCCESS;
  // 提交全部 READY 或到重试时间的 fragment. 同页依赖由 WAIT_PARENT 状态保证,
  // 不要求所有 AIO 按 LSN 串行提交.
  int sret = OB_SUCCESS;
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> submit_fragments;
  if (OB_FAIL(pool_.collect_ready_fragments(submit_fragments,
                                            aio_delay_us_,
                                            &wait_parent_wake_cnt_,
                                            &wait_parent_wait_us_,
                                            &wait_parent_data_bytes_))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "collect ready async fragments failed", KR(ret), K_(palf_id));
    }
  } else if (OB_FAIL(append_due_failed_fragments_(submit_fragments))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "append due failed async fragments failed", KR(ret), K_(palf_id));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < submit_fragments.count(); ++i) {
      PhysicalWriteFragment *frag = submit_fragments.at(i);
      if (OB_ISNULL(frag)) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "null async submit candidate", KR(ret), K_(palf_id), K(i));
      } else {
        sret = submit_fragment_(handle, *frag);
        if (OB_FAIL(sret)) {
          if (is_submit_transient_error_(ret)) {
            PALF_LOG(TRACE, "submit async fragment transiently failed",
                     KR(ret), K_(palf_id), KPC(frag));
          } else if (REACH_THREAD_TIME_INTERVAL(1_s)) {
            PALF_LOG(WARN, "submit async fragment failed",
                     KR(ret), K_(palf_id), KPC(frag));
          }
        }
      }
    }
  }
  return ret;
}

int AsyncPalfIOCtx::append_due_failed_fragments_(
    common::ObIArray<PhysicalWriteFragment *> &submit_fragments)
{
  int ret = OB_SUCCESS;
  const int64_t now = common::ObTimeUtility::current_time();
  ObSEArray<PhysicalWriteFragment *, FRAGMENT_SLOT_CNT_PER_PALF> failed_fragments;
  const PhysicalWriteFragmentStateFilter failed_filter(AsyncFragmentState::FAILED);
  if (OB_FAIL(pool_.collect_fragments(failed_fragments, failed_filter))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "collect failed async fragments failed", KR(ret), K_(palf_id));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < failed_fragments.count(); ++i) {
      PhysicalWriteFragment *frag = failed_fragments.at(i);
      if (OB_ISNULL(frag)) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "null failed async fragment", KR(ret), K_(palf_id), K(i));
      } else if (now >= frag->get_next_retry_ts()
                 && OB_FAIL(submit_fragments.push_back(frag))) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "append due failed async fragment failed",
                   KR(ret), K_(palf_id), K(i), KPC(frag));
        }
      }
    }
  }
  return ret;
}

int AsyncPalfIOCtx::submit_fragment_(IPalfHandleImpl *handle, PhysicalWriteFragment &frag)
{
  int ret = OB_SUCCESS;
  const int64_t now = common::ObTimeUtility::current_time();

  // fragment 创建时 begin/buf 已按 DIO 对齐; 提交时只需要向上对齐尾部长度.
  const LSN write_begin_lsn = frag.get_begin_lsn();
  const LSN write_end_lsn = frag.get_end_lsn();
  const char *write_buf = frag.get_buf();
  const int64_t data_len = write_end_lsn - write_begin_lsn;
  const int64_t write_len = static_cast<int64_t>(upper_align(data_len, LOG_DIO_ALIGN_SIZE));
  const int64_t fragment_max_size = frag.get_fragment_max_size();
  const block_id_t fragment_block_id = lsn_2_block(write_begin_lsn, PALF_BLOCK_SIZE);
  const FragmentRef fragment_ref = frag.get_fragment_ref();
  AsyncPwriteRequest req;
  bool submit_state_prepared = false;
  if (OB_ISNULL(handle) || !frag.is_data_valid()
             || write_len <= 0 || write_len > fragment_max_size
             || LOG_INVALID_BLOCK_ID == current_write_block_id_
             || fragment_block_id != current_write_block_id_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid async fragment before submit",
             KR(ret), K_(palf_id), K_(current_write_block_id), K(fragment_block_id),
             K(write_begin_lsn), K(write_end_lsn), K(data_len), K(write_len), K(fragment_max_size), K(frag));
  } else if (OB_FAIL(frag.mark_submitted(fragment_ref, now))) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "mark fragment submitted failed before aio submit",
               KR(ret), K_(palf_id), K(frag));
    }
  } else {
    // 必须先把 fragment 置为 SUBMITTED 并增加 inflight, 再调用可能快速
    // callback 的底层 AIO. 同步提交失败会在函数出口转为 FAILED 并回滚计数.
    submit_state_prepared = true;
    ATOMIC_INC(&inflight_aio_cnt_);
    if (OB_FAIL(req.init(write_begin_lsn, write_buf, write_len, this, fragment_ref, now))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "init async pwrite request failed",
                 KR(ret), K_(palf_id), K(write_begin_lsn),
                 K(write_len), K(fragment_ref), K(frag));
      }
    } else if (OB_FAIL(handle->async_pwrite(req, frag.get_io_handle()))) {
      PALF_LOG(TRACE, "submit async pwrite failed",
               KR(ret), K_(palf_id), K(write_begin_lsn),
               K(write_len), K(frag));
    } else {
      submit_cnt_.record(now, 1);
      submit_logical_bytes_.record(now, data_len);
      submit_aio_bytes_.record(now, write_len);
    }
  }
  if (OB_SUCCESS != ret && submit_state_prepared) {
    // submit 前先增加 inflight, 保证快速 callback 观察到一致计数. 同步提交
    // 失败时先转 FAILED 再回滚计数; 瞬时错误下一轮直接重试, 其他错误短暂退避.
    ATOMIC_INC(&submit_fail_cnt_);
    submit_fail_perf_cnt_.record(now, 1);
    const bool is_transient = is_submit_transient_error_(ret);
    const int64_t next_retry_ts = is_transient ? now : now + FRAGMENT_RETRY_INTERVAL_US;
    int mark_ret = frag.mark_failed(fragment_ref, ret, next_retry_ts);
    if (OB_SUCCESS != mark_ret) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "mark fragment failed state failed",
                 KR(mark_ret), KR(ret), K(is_transient), K_(palf_id), K(frag));
      }
    } else if (is_transient) {
      PALF_LOG(TRACE, "submit transient failure, retry next round",
               KR(ret), K_(palf_id), K(frag));
    } else {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "submit device/fd error, periodic retry", KR(ret),
                 K_(palf_id), K(frag));
      }
    }
    ATOMIC_DEC(&inflight_aio_cnt_);
  }
  return ret;
}

// ===========================================================================
// Task stage: switch block, apply throttle, admit tasks, and plan fragments.
// ===========================================================================
int AsyncPalfIOCtx::drive_phase_task_(IPalfHandleImpl *handle)
{
  int ret = OB_SUCCESS;
  // 1. admission 前锁定 block 边界, 防止旧 block 尚未持久化排空时规划新 block.
  if (!block_switch_pending_
      && OB_FAIL(mark_block_switch_if_needed_())) {
    PALF_LOG(TRACE, "mark async block switch before admission failed",
             KR(ret), K_(palf_id));
  } else if (block_switch_pending_) {
    if (task_queue_.get_total() > 0 && OB_FAIL(try_switch_block_(handle))) {
      if (OB_EAGAIN == ret) {
        PALF_LOG(TRACE, "try async block switch pending",
                 KR(ret), K_(palf_id),
                 "task_queue_count", task_queue_.get_total());
      } else if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "try async block switch failed",
                 KR(ret), K_(palf_id),
                 "task_queue_count", task_queue_.get_total());
      }
    }
  }
  // 2. 先把 producer task 批量转入 planner, 再统一规划全部 pending source.
  if (OB_SUCC(ret) && !block_switch_pending_) {
    bool can_admit = false;
    check_throttle_admission_(can_admit);
    if (can_admit && OB_FAIL(admit_ready_tasks_())) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "admit async tasks failed",
                 KR(ret), K_(palf_id));
      }
    } else if (OB_FAIL(planner_.plan_pending_tasks())) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "plan pending async tasks failed", KR(ret), K_(palf_id));
      }
    } else if (OB_FAIL(mark_block_switch_if_needed_())) {
      PALF_LOG(TRACE, "mark async block switch failed", KR(ret), K_(palf_id));
    }
  }
  return ret;
}

void AsyncPalfIOCtx::check_throttle_admission_(bool &can_admit)
{
  // 非阻塞磁盘限流只控制本轮是否接收新 flush task. can_admit=false 时,
  // completion、publish、block switch、retry 和已有 pending source 仍继续推进.
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t next_admit_ts = 0;
  can_admit = false;
  if (OB_TMP_FAIL(can_admit_new_entry_(peek_pending_admit_bytes_(), throttle_next_admit_ts_,
                                      can_admit, next_admit_ts))) {
    ++throttle_error_count_;
    last_throttle_error_ts_ = common::ObTimeUtility::current_time();
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "throttle error, admission paused", KR(tmp_ret),
               K_(palf_id), "throttle_error_count", throttle_error_count_,
               "last_throttle_error_ts", last_throttle_error_ts_);
    }
  } else if (can_admit && throttle_error_count_ > 0) {
    PALF_LOG(INFO, "throttle recovered", K_(palf_id),
             "throttle_error_count", throttle_error_count_);
    throttle_error_count_ = 0;
    last_throttle_error_ts_ = OB_INVALID_TIMESTAMP;
  }
  throttle_next_admit_ts_ = next_admit_ts;
  if (OB_SUCCESS == tmp_ret) {
    if (!can_admit) {
      const int64_t now = common::ObClockGenerator::getClock();
      int64_t throttle_wait_us = 0;
      if (next_admit_ts > now) {
        const int64_t wait_begin_ts = MAX(now, throttle_wait_until_ts_);
        if (next_admit_ts > wait_begin_ts) {
          throttle_wait_us = next_admit_ts - wait_begin_ts;
          throttle_wait_until_ts_ = next_admit_ts;
        }
      }
      throttle_block_cnt_.record(now, 1);
      throttle_block_us_.record(now, throttle_wait_us);
    } else {
      throttle_wait_until_ts_ = 0;
    }
  }
}

int AsyncPalfIOCtx::can_admit_new_entry_(const int64_t logical_bytes,
                                         const int64_t current_next_admit_ts,
                                         bool &can_admit,
                                         int64_t &next_admit_ts)
{
  int ret = OB_SUCCESS;
  can_admit = true;
  next_admit_ts = 0;
  // 首次限流记录 deadline 并置 ignore_throttle_once_. deadline 到期前仅
  // probe 更新更早的截止时间, 不重复记 skipped-task; 到期后放行一次,
  // 下一轮重新计算限流. 探测报错时安装固定重试 deadline, 避免空转.
  if (ignore_throttle_once_) {
    const int64_t now = common::ObClockGenerator::getClock();
    if (current_next_admit_ts > 0 && current_next_admit_ts > now) {
      if (has_valid_async_throttle_admission_()) {
        int64_t delay_us = 0;
        if (OB_FAIL(throttle_ctx_.throttle->probe_admit_async(logical_bytes, *throttle_ctx_.purge_func,
                                               palf_env_impl_, can_admit, delay_us))) {
          PALF_LOG(TRACE, "probe async throttle failed", KR(ret), K_(palf_id), K(logical_bytes));
        } else if (!can_admit) {
          const int64_t new_next_admit_ts = common::ObClockGenerator::getClock()
              + (delay_us > 0 ? delay_us : 0);
          next_admit_ts = MIN(current_next_admit_ts, new_next_admit_ts);
        } else {
          ignore_throttle_once_ = false;
        }
      } else {
        can_admit = false;
        next_admit_ts = current_next_admit_ts;
      }
    } else {
      ignore_throttle_once_ = false;
    }
  } else if (has_valid_async_throttle_admission_()) {
    int64_t delay_us = 0;
    if (OB_FAIL(throttle_ctx_.throttle->try_admit_async(logical_bytes, *throttle_ctx_.purge_func,
                                           palf_env_impl_, can_admit, delay_us))) {
      PALF_LOG(TRACE, "throttle error", KR(ret), K_(palf_id), K(logical_bytes));
    } else if (!can_admit) {
      next_admit_ts = common::ObClockGenerator::getClock()
          + (delay_us > 0 ? delay_us : 0);
      ignore_throttle_once_ = true;
    }
  }
  if (OB_SUCCESS != ret) {
    const int64_t now = common::ObClockGenerator::getClock();
    can_admit = false;
    next_admit_ts = current_next_admit_ts > now
        ? current_next_admit_ts
        : now + THROTTLE_ERROR_RETRY_INTERVAL_US;
  }
  return ret;
}

bool AsyncPalfIOCtx::has_async_throttle_() const
{
  return OB_NOT_NULL(throttle_ctx_.throttle);
}

bool AsyncPalfIOCtx::has_valid_async_throttle_admission_() const
{
  return has_async_throttle_() && OB_NOT_NULL(palf_env_impl_)
      && OB_NOT_NULL(throttle_ctx_.purge_func) && throttle_ctx_.purge_func->is_valid();
}

int64_t AsyncPalfIOCtx::peek_pending_admit_bytes_()
{
  // 用队首 flush task 的 LSN 区间估算 admission 字节数. 空队列或 barrier
  // 不写数据日志, 返回 0; 只有非预期错误退化为单 task 最大值, 避免误绕过限流.
  int ret = OB_SUCCESS;
  int64_t bytes = 0;
  if (task_queue_.get_total() > 0) {
    AsyncQueueItem *head_item = NULL;
    if (OB_FAIL(peek_not_null_task_queue_item_(QueueItemType::FLUSH_LOG, head_item))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "peek async task queue item for throttle failed", KR(ret), K_(palf_id));
      }
    } else {
      LogIOFlushLogTask *flush_task = static_cast<LogIOFlushLogTask *>(head_item->payload_);
      const LSN begin_lsn = flush_task->get_flush_begin_lsn();
      const LSN end_lsn = flush_task->get_flush_end_lsn();
      if (!begin_lsn.is_valid() || !end_lsn.is_valid() || end_lsn <= begin_lsn) {
        ret = OB_ERR_UNEXPECTED;
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(ERROR, "invalid async flush task while estimating throttle bytes",
                   KR(ret), K_(palf_id),
                   K(begin_lsn), K(end_lsn), KP(flush_task));
        }
      } else {
        bytes = end_lsn - begin_lsn;
      }
    }
  }
  if (OB_FAIL(ret)) {
    bytes = MAX_LOG_BUFFER_SIZE;
  }
  return bytes;
}

int AsyncPalfIOCtx::try_publish_contiguous_prefix_(IPalfHandleImpl *handle)
{
  int ret = OB_SUCCESS;
  LogIOFlushLogTask *task = NULL;
  if (OB_ISNULL(handle)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid null handle while publishing async prefix", KR(ret), K_(palf_id));
  }
  while (OB_SUCC(ret)
         && OB_SUCCESS == planner_.peek_publishable_task(task)) {
    if (OB_FAIL(publish_one_task_(handle, task))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "publish one async task failed", KR(ret), K_(palf_id), KP(task));
      }
    }
  }
  return ret;
}

int AsyncPalfIOCtx::publish_one_task_(IPalfHandleImpl *handle, LogIOFlushLogTask *&task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(handle) || OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid argument while publishing async prefix",
             KR(ret), K_(palf_id), KP(handle), KP(task));
  } else {
    const LSN begin_lsn = task->get_flush_begin_lsn();
    const LSN end_lsn = task->get_flush_end_lsn();
    const int64_t init_task_ts = task->get_init_task_ts();
    int64_t flushed_bytes = 0;
    LSN buffer_reuse_lsn;
    if (!begin_lsn.is_valid() || !end_lsn.is_valid() || end_lsn <= begin_lsn) {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "invalid published async flush task",
               KR(ret), K_(palf_id), K(begin_lsn), K(end_lsn), KP(task));
    } else if (FALSE_IT(flushed_bytes = end_lsn - begin_lsn)) {
    } else if (FALSE_IT(buffer_reuse_lsn = LSN(static_cast<offset_t>(
                           lower_align(end_lsn.val_, LOG_DIO_ALIGN_SIZE))))) {
    // FIXME(shouju.zyp): publish 需要显式状态机. commit 或 reuse 位点推进成功后,
    // 重试时应只执行尚未完成的 callback 阶段.
    } else if (OB_FAIL(handle->commit_async_append(begin_lsn, end_lsn))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "commit_async_append failed, retry next round", KR(ret), K(begin_lsn), K(end_lsn), K_(palf_id));
      }
    } else if (OB_FAIL(handle->advance_reuse_lsn(buffer_reuse_lsn))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "advance buffer reuse lsn failed, retry next round",
                 KR(ret), K(end_lsn), K(buffer_reuse_lsn), K_(palf_id));
      }
    } else {
      // callback helper 会一直重试运行期入队失败; 只有 callback 线程池停止时
      // 才返回失败. 此时不会再执行 callback, PALF 必须直接释放 task.
      const int tmp_ret = push_task_into_cb_thread_pool(cb_tg_id_, task);
      if (OB_SUCCESS != tmp_ret) {
        PALF_LOG(ERROR, "push published async callback task failed while callback pool is stopping",
                 KR(tmp_ret), K_(palf_id), KP(task));
      }
      planner_.pop_published_task();
      if (OB_SUCCESS != tmp_ret) {
        task->free_this(palf_env_impl_);
      }
      task = NULL;
      release_task_slot_(LogIOTaskType::FLUSH_LOG_TYPE);
      ATOMIC_INC(&published_entry_cnt_);
      int64_t publish_cost_us = -1;
      const int64_t publish_ts = common::ObTimeUtility::current_time();
      if (init_task_ts > 0) {
        publish_cost_us = publish_ts - init_task_ts;
      }
      publish_bytes_.record(publish_ts, flushed_bytes);
      task_publish_us_.record(publish_ts, publish_cost_us);
      // Feed the throttle decay model with the just-flushed bytes.
      if (has_async_throttle_() && flushed_bytes > 0) {
        const int throttle_ret = throttle_ctx_.throttle->after_append_log(flushed_bytes);
        if (OB_SUCCESS != throttle_ret) {
          if (REACH_THREAD_TIME_INTERVAL(1_s)) {
            PALF_LOG(WARN, "after append log for async throttle failed",
                     KR(throttle_ret), K_(palf_id), K(flushed_bytes));
          }
        }
      }
      PALF_LOG(TRACE, "publish async flush task success", K_(palf_id), K(begin_lsn), K(end_lsn), K(buffer_reuse_lsn));
    }
  }
  return ret;
}

int AsyncPalfIOCtx::on_aio_complete(
    const AsyncIOCompletionEvent &event,
    bool &need_wake_worker)
{
  common::ObQSyncLockReadGuard guard(qsync_lock_);
  return on_aio_complete_(event, need_wake_worker);
}

int AsyncPalfIOCtx::on_aio_complete_(
    const AsyncIOCompletionEvent &event,
    bool &need_wake_worker)
{
  int ret = OB_SUCCESS;
  need_wake_worker = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "async aio complete on uninited ctx", KR(ret), K_(palf_id), K(event));
  } else {
    PhysicalWriteFragment *frag = NULL;
    int get_ret = pool_.get_fragment(event.ctx.fragment_ref, frag);
    if (OB_ENTRY_NOT_EXIST == get_ret) {
      PALF_LOG(TRACE, "stale async aio completion", KR(get_ret), K_(palf_id), K(event));
      // slot 已回收或 generation 不一致表示 stale completion. close-once 的
      // 获胜方已经减少 inflight, 当前 callback 不能重复修改计数.
      ATOMIC_INC(&stale_completion_cnt_);
    } else if (OB_SUCCESS != get_ret) {
      ret = get_ret;
      need_wake_worker = true;
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "get fragment for async aio completion failed",
                 KR(ret), K_(palf_id), K(event));
      }
    } else if (OB_ISNULL(frag)) {
      ret = OB_ERR_UNEXPECTED;
      need_wake_worker = true;
      PALF_LOG(ERROR, "async aio completion resolved null fragment",
               KR(ret), K_(palf_id), K(event));
    } else {
      const int64_t finish_ts = event.finish_ts > 0 ? event.finish_ts
                                                     : common::ObTimeUtility::current_time();
      bool completed_by_me = false;
      if (OB_FAIL(handle_completed_fragment_(*frag, event.ctx.fragment_ref, event.ret_code,
                                             finish_ts, false /* polled_completion */,
                                             completed_by_me))) {
        need_wake_worker = true;
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "handle callback async AIO completion failed",
                   KR(ret), K_(palf_id), K(event));
        }
      } else if (completed_by_me) {
        need_wake_worker = true;
      }
    }
  }
  return ret;
}

int AsyncPalfIOCtx::get_next_drive_interval_(int64_t &next_drive_interval_us)
{
  int ret = OB_SUCCESS;
  next_drive_interval_us = INT64_MAX;
  if (is_inited_) {
    PlannerStatus planner_status;
    planner_.get_status(planner_status);
    const int64_t now_us = common::ObTimeUtility::current_time();
    const int64_t throttle_now_us = common::ObTimeUtility::fast_current_time();
    int64_t fragment_drive_interval_us = INT64_MAX;
    if (planner_status.get_active_fragment_count() > 0
        && OB_FAIL(pool_.get_next_drive_interval(now_us,
                                                 aio_delay_us_,
                                                 fragment_drive_interval_us))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "get fragment next drive interval failed",
                 KR(ret), K_(palf_id), K_(aio_delay_us));
      }
    } else {
      next_drive_interval_us = MIN(next_drive_interval_us, fragment_drive_interval_us);
    }
    if (is_control_barrier_drained_()) {
      // 控制类 barrier 已经满足执行条件：前面的 flush、AIO、publish 都排空了，
      // 需要立刻再 drive 一轮，把 barrier 执行掉。
      next_drive_interval_us = 0;
    } else if (block_switch_pending_) {
      // 之前因为切 block 被挡住。只有旧 block 已经 publish 到末尾，
      // 且队列里还有后续任务时，才需要立即继续推进。
      const LSN new_block_min_lsn = planner_status.get_planned_end_lsn();
      if (!new_block_min_lsn.is_valid()
          || new_block_min_lsn.val_ <= 0
          || 0 != lsn_2_offset(new_block_min_lsn, PALF_BLOCK_SIZE)
          || LOG_INVALID_BLOCK_ID == current_write_block_id_) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "invalid async block switch state when getting drive interval",
                 KR(ret), K_(palf_id), K_(current_write_block_id),
                 K(new_block_min_lsn), K(planner_status));
      } else {
        const LSN persisted_lsn = planner_status.get_persisted_lsn();
        if (persisted_lsn.is_valid()
            && persisted_lsn == new_block_min_lsn
            && (task_queue_.get_total() > 0 || planner_status.has_pending_source())) {
          next_drive_interval_us = 0;
        }
      }
    } else if (task_queue_.get_total() > 0) {
      // producer 队列里还有新任务。限流未生效或 deadline 已到时立即推进；
      // 否则返回 deadline 的剩余时间。
      const int64_t throttle_interval_us = 0 == throttle_next_admit_ts_
          || throttle_next_admit_ts_ <= throttle_now_us
          ? 0
          : throttle_next_admit_ts_ - throttle_now_us;
      next_drive_interval_us = MIN(next_drive_interval_us, throttle_interval_us);
    }
  }
  return ret;
}

int64_t AsyncPalfIOCtx::get_inflight_count() const
{
  common::ObQSyncLockWriteGuard guard(qsync_lock_);
  return get_inflight_count_();
}

int64_t AsyncPalfIOCtx::get_inflight_count_() const
{
  return get_pending_async_stage_count_();
}

int64_t AsyncPalfIOCtx::get_oldest_pending_io_start_ts() const
{
  common::ObQSyncLockReadGuard guard(qsync_lock_);
  return get_oldest_pending_io_start_ts_();
}

int64_t AsyncPalfIOCtx::get_oldest_pending_io_start_ts_() const
{
  return pool_.get_oldest_pending_io_start_ts();
}

int64_t AsyncPalfIOCtx::get_pending_async_stage_count_() const
{
  PlannerStatus planner_status;
  planner_.get_status(planner_status);
  int64_t pending_count = planner_status.get_active_fragment_count()
                        + ATOMIC_LOAD(&inflight_aio_cnt_);
  if (is_inited_) {
    // task credit 覆盖从 worker 接收 task 到 callback 所有权交接完成的完整周期.
    pending_count += FLUSH_LOG_TASK_QUEUE_CAPACITY - ATOMIC_LOAD(&available_flush_task_slot_count_)
                   + BARRIER_TASK_QUEUE_CAPACITY - ATOMIC_LOAD(&available_barrier_task_slot_count_);
  }
  // unregister 查询时自身持有一个基准 pin; 超过一个表示仍有 callback 或
  // dispatch 调用方正在访问 ctx, 必须继续等待.
  const int64_t active_ref = ATOMIC_LOAD(&active_ref_);
  if (active_ref > 1) {
    ++pending_count;
  }
  return pending_count;
}

// ===========================================================================
// Admission: move queued tasks into zero-copy fragments.
// ===========================================================================

bool AsyncPalfIOCtx::is_control_barrier_drained_() const
{
  PlannerStatus planner_status;
  planner_.get_status(planner_status);
  const int64_t prior_pending = planner_status.get_pending_task_count()
                              + planner_status.get_active_fragment_count()
                              + ATOMIC_LOAD(&inflight_aio_cnt_);
  return OB_NOT_NULL(control_barrier_task_)
      && 0 == prior_pending;
}

int AsyncPalfIOCtx::pop_head_control_barrier_task_()
{
  int ret = OB_SUCCESS;
  AsyncQueueItem *item = NULL;
  AsyncQueueItem *popped = NULL;
  if (OB_NOT_NULL(control_barrier_task_)) {
    // 已暂存的 barrier 会阻止后续 task admission.
  } else if (OB_FAIL(peek_not_null_task_queue_item_(QueueItemType::BARRIER_TASK, item))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "peek async task queue for control barrier failed",
               KR(ret), K_(palf_id));
    }
  } else {
    LogIOTask *task = static_cast<LogIOTask *>(item->payload_);
    const LogIOTaskType task_type = task->get_io_task_type();
    if (OB_FAIL(pop_task_queue_item_(popped))) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "pop async control barrier failed",
                 KR(ret), K_(palf_id),
                 "task_queue_count", task_queue_.get_total(), KP(task));
      }
    } else {
      popped->payload_ = NULL;
      control_barrier_task_ = task;
      task = NULL;
      item = NULL;
      PALF_LOG(INFO, "async control barrier task queued",
               K_(palf_id),
               "task_type", log_io_task_type_str(task_type));
      free_task_queue_item_(popped);
    }
  }
  return ret;
}

int AsyncPalfIOCtx::execute_control_barrier_task_(bool &consumed)
{
  int ret = OB_SUCCESS;
  consumed = false;
  if (!is_control_barrier_drained_()) {
  } else {
    LogIOTask *task = control_barrier_task_;
    const bool need_purge_throttling = task->need_purge_throttling();
    const int64_t io_size = task->get_io_size();
    const LogIOTaskType task_type = task->get_io_task_type();
    const bool need_reset_tail = (LogIOTaskType::TRUNCATE_LOG_TYPE == task_type
        || LogIOTaskType::FLASHBACK_LOG_TYPE == task_type
        || LogIOTaskType::TRUNCATE_PREFIX_TYPE == task_type);

    PALF_LOG(TRACE, "async control barrier task starts running",
             K_(palf_id),
             "task_type", log_io_task_type_str(task_type));
    // 与 LogIOWorker 的所有权约定一致: do_task() 只执行一次. 成功后 task
    // 转交 callback pool; 失败时所有权仍在 ctx, 由当前路径释放. 两种结果
    // 都不会在 ctx 内重复执行 barrier.
    const int task_ret = task->do_task(cb_tg_id_, palf_env_impl_);
    control_barrier_task_ = NULL;
    consumed = true;
    if (OB_SUCCESS != task_ret) {
      ret = task_ret;
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "execute async control barrier task failed",
                 KR(ret), K_(palf_id), KPC(task));
      }
      task->free_this(palf_env_impl_);
      task = NULL;
    } else {
      task = NULL;
      if (need_reset_tail) {
        planner_.invalidate_plan_state();
        set_block_switch_pending_(false);
        current_write_block_id_ = LOG_INVALID_BLOCK_ID;
        PALF_LOG(TRACE, "invalidate async planner after control barrier task",
                 K_(palf_id), "task_type", log_io_task_type_str(task_type));
      }
      if (has_async_throttle_() && io_size > 0) {
        const int throttle_ret = throttle_ctx_.throttle->after_append_log(io_size);
        if (OB_SUCCESS != throttle_ret) {
          if (REACH_THREAD_TIME_INTERVAL(1_s)) {
            PALF_LOG(WARN, "after append log for control throttle failed",
                     KR(throttle_ret), K_(palf_id), K(io_size));
          }
        }
      }
    }
    release_task_slot_(task_type);
    record_purge_task_finished_(need_purge_throttling);
  }
  return ret;
}

void AsyncPalfIOCtx::record_purge_task_finished_(const bool need_purge_throttling)
{
  if (need_purge_throttling && has_async_throttle_() && OB_NOT_NULL(throttle_ctx_.purge_task_count)) {
    const int64_t purge_task_count = ATOMIC_SAF(throttle_ctx_.purge_task_count, 1);
    if (purge_task_count < 0) {
      PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
                   "purge task count is negative", K(purge_task_count), K_(palf_id));
    }
  }
}

int AsyncPalfIOCtx::admit_ready_tasks_()
{
  int ret = OB_SUCCESS;
  PlannerStatus planner_status;
  planner_.get_status(planner_status);
  if (block_switch_pending_) {
    // 队首 flush task 属于下一个 block, 在 try_switch_block_ 写完 header 前
    // 必须留在 producer queue, 不能提前进入 planner.
  } else if (!planner_status.get_planned_end_lsn().is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "async planner state is not ready for admission",
             KR(ret), K_(palf_id));
  } else if (OB_FAIL(drain_producer_queue_to_planner_())) {
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "admit producer queue to planner failed", KR(ret), K_(palf_id));
    }
  }
  return ret;
}

void AsyncPalfIOCtx::set_block_switch_pending_(const bool pending)
{
  block_switch_pending_ = pending;
  block_switch_pending_since_ts_ = pending
      ? common::ObTimeUtility::current_time()
      : OB_INVALID_TIMESTAMP;
}

int AsyncPalfIOCtx::mark_block_switch_if_needed_()
{
  int ret = OB_SUCCESS;
  PlannerStatus planner_status;
  planner_.get_status(planner_status);
  const LSN end_lsn = planner_status.get_planned_end_lsn();
  const bool is_block_boundary = (end_lsn.is_valid()
      && end_lsn.val_ > 0
      && 0 == lsn_2_offset(end_lsn, PALF_BLOCK_SIZE));
  const bool is_current_block_boundary = (is_block_boundary
      && LOG_INVALID_BLOCK_ID != current_write_block_id_
      && lsn_2_block(end_lsn - 1, PALF_BLOCK_SIZE) == current_write_block_id_);
  if (!block_switch_pending_ && is_current_block_boundary) {
    set_block_switch_pending_(true);
    PALF_LOG(TRACE, "mark async block switch",
             K_(palf_id), K(end_lsn),
             K_(current_write_block_id), K(planner_status));
  }
  return ret;
}

int AsyncPalfIOCtx::refresh_current_write_block_id_(const PlannerStatus &planner_status)
{
  int ret = OB_SUCCESS;
  const LSN persisted_lsn = planner_status.get_persisted_lsn();
  if (block_switch_pending_ || !persisted_lsn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid async state when refreshing current write block",
             KR(ret), K_(palf_id), K_(block_switch_pending), K(planner_status));
  } else if (0 == persisted_lsn.val_) {
    current_write_block_id_ = 0;
  } else {
    current_write_block_id_ = lsn_2_block(persisted_lsn, PALF_BLOCK_SIZE);
  }
  return ret;
}

// 这里只把 flush task 从 producer FIFO 交给 planner. barrier 保留在队首,
// 统一由 drive_write() 暂存并执行, 避免出现多个 barrier 所有权入口.
int AsyncPalfIOCtx::drain_producer_queue_to_planner_()
{
  int ret = OB_SUCCESS;
  bool stop = false;
  // 已暂存的 control task 会阻止 admission, 后续 task 继续留在 FIFO.
  while (OB_SUCC(ret) && !stop && OB_ISNULL(control_barrier_task_)
         && task_queue_.get_total() > 0) {
    AsyncQueueItem *item = NULL;
    AsyncQueueItem *popped = NULL;
    if (OB_FAIL(peek_not_null_task_queue_item_(QueueItemType::FLUSH_LOG, item))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
      stop = true;
    } else {
      LogIOTask *task = static_cast<LogIOTask *>(item->payload_);
      LogIOFlushLogTask *flush_task = static_cast<LogIOFlushLogTask *>(task);
      const LSN begin_lsn = flush_task->get_flush_begin_lsn();
      const LSN end_lsn = flush_task->get_flush_end_lsn();
      bool consumed = false;
      const int tmp_ret = planner_.admit_task(task, consumed);
      if (OB_EAGAIN == tmp_ret) {
        stop = true;
      } else if (OB_FAIL(tmp_ret)) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "planner admit task failed", KR(ret), K_(palf_id));
        }
      } else if (consumed) {
        if (begin_lsn.is_valid() && end_lsn.is_valid() && end_lsn > begin_lsn) {
          admit_bytes_.record(common::ObTimeUtility::current_time(), end_lsn - begin_lsn);
        }
        if (OB_FAIL(pop_task_queue_item_(popped))) {
          if (REACH_THREAD_TIME_INTERVAL(1_s)) {
            PALF_LOG(WARN, "pop admitted async task queue item failed",
                     KR(ret), K_(palf_id),
                     "task_queue_count", task_queue_.get_total(),
                     K(begin_lsn), K(end_lsn));
          }
        } else {
          popped->payload_ = NULL;
          item = NULL;
          flush_task = NULL;
          task = NULL;
          free_task_queue_item_(popped);
        }
      } else {
        // 没有报错但 consumed=false 表示 planner 暂时无法接收该 task;
        // 保留当前队首, 下一轮 drive 原地重试.
        stop = true;
      }
    }
  }
  return ret;
}

int AsyncPalfIOCtx::refresh_planner_state_before_drive_(IPalfHandleImpl *handle)
{
  int ret = OB_SUCCESS;
  LogStorage::AsyncStorageSnapshot storage_snapshot;
  if (OB_ISNULL(handle)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid handle for async planner state refresh",
             KR(ret), K_(palf_id));
  } else {
    handle->get_async_storage_snapshot(storage_snapshot);
    if (OB_FAIL(reset_async_state_after_tail_changed_(handle, storage_snapshot))
        && REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "refresh async planner state before drive failed, keep tasks pending",
               KR(ret), K_(palf_id));
    }
  }
  return ret;
}

// planned 数据全部持久化后才允许切 block. block header 的 SCN 取自下一条
// flush task; 队列里暂时没有 flush task 时保持 pending, 等新 task 到达后再通过
// prepare_async_block_for_write 原子完成 switch 和 header 写入.
int AsyncPalfIOCtx::try_switch_block_(IPalfHandleImpl *handle)
{
  int ret = OB_SUCCESS;
  PlannerStatus planner_status;
  planner_.get_status(planner_status);
  const LSN persisted_lsn = planner_status.get_persisted_lsn();
  const LSN planned_end_lsn = planner_status.get_planned_end_lsn();
  const bool persisted_to_end = (persisted_lsn.is_valid()
      && planned_end_lsn.is_valid()
      && persisted_lsn == planned_end_lsn);
  if (OB_ISNULL(handle) || !block_switch_pending_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "invalid async block switch state",
             KR(ret), K_(palf_id), KP(handle), K_(block_switch_pending),
             K(planner_status));
  } else if (persisted_to_end) {
    share::SCN scn;
    int tmp_ret = extract_first_queued_scn_(scn);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      // No flush task can provide the block header SCN yet.
    } else if (OB_FAIL(tmp_ret)) {
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "extract first queued scn failed while switching async block",
                 KR(ret), K_(palf_id));
      }
    } else if (OB_FAIL(handle->prepare_async_block_for_write(scn))) {
      if (OB_EAGAIN == ret) {
        PALF_LOG(TRACE, "async block switch pending", KR(ret), K_(palf_id));
      } else if (REACH_THREAD_TIME_INTERVAL(1_s)) {
        PALF_LOG(WARN, "switch async block failed", KR(ret), K_(palf_id));
      }
    } else {
      set_block_switch_pending_(false);
      if (OB_FAIL(refresh_current_write_block_id_(planner_status))) {
        if (REACH_THREAD_TIME_INTERVAL(1_s)) {
          PALF_LOG(WARN, "refresh async current write block after switch failed",
                   KR(ret), K_(palf_id), K(planner_status));
        }
      } else {
        PALF_LOG(TRACE, "async block switched", K(planned_end_lsn), K_(current_write_block_id), K_(palf_id), K(scn));
      }
    }
  }
  return ret;
}

int AsyncPalfIOCtx::extract_first_queued_scn_(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  AsyncQueueItem *item = NULL;
  if (OB_FAIL(peek_not_null_task_queue_item_(QueueItemType::FLUSH_LOG, item))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // No flush task can provide the next block header SCN yet.
    } else if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      PALF_LOG(WARN, "peek queued flush task for scn extraction failed", KR(ret), K_(palf_id));
    }
  } else {
    LogIOFlushLogTask *first_task = static_cast<LogIOFlushLogTask *>(item->payload_);
    scn = first_task->get_flush_log_cb_ctx().scn_;
  }
  return ret;
}

} // end namespace palf
} // end namespace oceanbase
