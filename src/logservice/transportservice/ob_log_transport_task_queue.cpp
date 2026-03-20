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

#define USING_LOG_PREFIX CLOG

#include <utility>

#include "ob_log_transport_task_queue.h"
#include "lib/oblog/ob_log.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "share/ob_define.h"
#include "share/ob_debug_sync.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/restoreservice/ob_log_restore_handler.h"

namespace oceanbase
{
namespace logservice
{

ObLogTransportTaskQueue::TransportTaskSlot::TransportTaskSlot()
  : task_bytes_(0),
    cached_bytes_ptr_(nullptr),
    is_submitted_(false)
{
}

ObLogTransportTaskQueue::TransportTaskSlot::~TransportTaskSlot()
{
  reset();
}

bool ObLogTransportTaskQueue::TransportTaskSlot::can_be_slid()
{
  return is_valid() && is_submitted_;
}

void ObLogTransportTaskQueue::TransportTaskSlot::reset()
{
  DEBUG_SYNC(common::LOG_TP_QUEUE_SLOT_RESET_BEGIN);
  if (task_holder_.ptr() != nullptr) {
    if (OB_NOT_NULL(cached_bytes_ptr_) && task_bytes_ > 0) {
      ATOMIC_AAF(cached_bytes_ptr_, -task_bytes_);
    }
    task_holder_.reset();
    task_bytes_ = 0;
    is_submitted_ = false;
  }
  DEBUG_SYNC(common::LOG_TP_QUEUE_SLOT_RESET_END);
}

bool ObLogTransportTaskQueue::TransportTaskSlot::is_valid() const
{
  return task_holder_.ptr() != nullptr;
}

ObLogTransportTaskQueue::SwRevertGuard::SwRevertGuard()
  : sw_(nullptr),
    slot_(nullptr),
    log_id_(OB_INVALID_LOG_ID)
{
}

ObLogTransportTaskQueue::SwRevertGuard::SwRevertGuard(
    palf::FixedSlidingWindow<TransportTaskSlot> &sw,
    const int64_t log_id,
    TransportTaskSlot *slot)
  : sw_(&sw),
    slot_(slot),
    log_id_(log_id)
{
}

ObLogTransportTaskQueue::SwRevertGuard::SwRevertGuard(SwRevertGuard &&other)
  : sw_(other.sw_),
    slot_(other.slot_),
    log_id_(other.log_id_)
{
  other.sw_ = nullptr;
  other.slot_ = nullptr;
  other.log_id_ = OB_INVALID_LOG_ID;
}

ObLogTransportTaskQueue::SwRevertGuard &ObLogTransportTaskQueue::SwRevertGuard::operator=(SwRevertGuard &&other)
{
  if (this != &other) {
    reset();
    sw_ = other.sw_;
    slot_ = other.slot_;
    log_id_ = other.log_id_;
    other.sw_ = nullptr;
    other.slot_ = nullptr;
    other.log_id_ = OB_INVALID_LOG_ID;
  }
  return *this;
}

ObLogTransportTaskQueue::SwRevertGuard::~SwRevertGuard()
{
  reset();
}

void ObLogTransportTaskQueue::SwRevertGuard::reset()
{
  if (OB_NOT_NULL(sw_)) {
    int ret = sw_->revert(log_id_);
    if (OB_SUCCESS != ret) {
      CLOG_LOG(ERROR, "revert fixed sliding window failed", KR(ret), K(log_id_));
    }
  }
  sw_ = nullptr;
  slot_ = nullptr;
  log_id_ = OB_INVALID_LOG_ID;
}

ObLogTransportTaskQueue::ObLogTransportTaskQueue()
  : sw_(),
    restore_handler_(nullptr),
    is_inited_(false),
    is_stopped_(false),
    alloc_mgr_(nullptr),
    queue_size_(OB_INVALID_SIZE),
    cached_bytes_(0),
    max_cached_bytes_(DEFAULT_MAX_CACHED_BYTES),
    process_lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
    lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
    self_heal_lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
    drop_stale_cnt_(0),
    drop_duplicate_cnt_(0),
    reject_out_of_window_cnt_(0),
    parse_fail_cnt_(0),
    early_drop_far_lsn_cnt_(0),
    evict_trigger_cnt_(0),
    evict_scan_slots_(0),
    evicted_slot_cnt_(0),
    evicted_task_bytes_(0),
    queue_stats_print_time_us_(0)
{
}

ObLogTransportTaskQueue::~ObLogTransportTaskQueue()
{
  destroy();
}

int ObLogTransportTaskQueue::init(common::ObILogAllocator *alloc_mgr,
                                  ObLogRestoreHandler *restore_handler,
                                  const int64_t queue_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(alloc_mgr) || OB_ISNULL(restore_handler)
             || queue_size <= 0 || 0 != (queue_size & (queue_size - 1))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid init argument", KR(ret), KP(alloc_mgr), KP(restore_handler), K(queue_size));
  } else if (OB_FAIL(sw_.init(palf::FIRST_VALID_LOG_ID, queue_size, alloc_mgr))) {
    CLOG_LOG(WARN, "init fixed sliding window failed", KR(ret));
  } else {
    alloc_mgr_ = alloc_mgr;
    restore_handler_ = restore_handler;
    queue_size_ = queue_size;
    is_inited_ = true;
    ATOMIC_STORE(&is_stopped_, false);
    cached_bytes_ = 0;
    max_cached_bytes_ = DEFAULT_MAX_CACHED_BYTES;
    drop_stale_cnt_ = 0;
    drop_duplicate_cnt_ = 0;
    reject_out_of_window_cnt_ = 0;
    parse_fail_cnt_ = 0;
    early_drop_far_lsn_cnt_ = 0;
    evict_trigger_cnt_ = 0;
    evict_scan_slots_ = 0;
    evicted_slot_cnt_ = 0;
    evicted_task_bytes_ = 0;
    queue_stats_print_time_us_ = 0;
  }
  return ret;
}

void ObLogTransportTaskQueue::stop()
{
  ATOMIC_STORE(&is_stopped_, true);
}

void ObLogTransportTaskQueue::destroy()
{
  common::SpinWLockGuard guard(lock_);
  sw_.destroy();
  is_inited_ = false;
  ATOMIC_STORE(&is_stopped_, true);
  alloc_mgr_ = nullptr;
  restore_handler_ = nullptr;
  queue_size_ = OB_INVALID_SIZE;
  cached_bytes_ = 0;
  max_cached_bytes_ = DEFAULT_MAX_CACHED_BYTES;
  drop_stale_cnt_ = 0;
  drop_duplicate_cnt_ = 0;
  reject_out_of_window_cnt_ = 0;
  parse_fail_cnt_ = 0;
  early_drop_far_lsn_cnt_ = 0;
  evict_trigger_cnt_ = 0;
  evict_scan_slots_ = 0;
  evicted_slot_cnt_ = 0;
  evicted_task_bytes_ = 0;
  queue_stats_print_time_us_ = 0;
}

void ObLogTransportTaskQueue::clear()
{
  DEBUG_SYNC(common::LOG_TP_QUEUE_CLEAR_BEFORE_QUEUE_LOCK);
  common::SpinWLockGuard guard(lock_);
  DEBUG_SYNC(common::LOG_TP_QUEUE_CLEAR_AFTER_QUEUE_LOCK);
  if (IS_NOT_INIT) {
    // do nothing
  } else {
    DEBUG_SYNC(common::LOG_TP_QUEUE_CLEAR_BEFORE_TRUNCATE);
    // destroy and init sliding window to clear all slots [begin_sn, end_sn)
    // because truncate_and_reset_begin_sn will not change begin_sn back to the original value
    int ret = OB_SUCCESS;
    sw_.destroy();
    if (OB_FAIL(sw_.init(palf::FIRST_VALID_LOG_ID, queue_size_, alloc_mgr_))) {
      CLOG_LOG(ERROR, "init sliding window failed", KR(ret), K_(queue_size));
    } else {
      CLOG_LOG(INFO, "clear sliding window success", K_(queue_size));
    }
    DEBUG_SYNC(common::LOG_TP_QUEUE_CLEAR_AFTER_TRUNCATE);
    ATOMIC_STORE(&cached_bytes_, 0);
  }
}

int ObLogTransportTaskQueue::push(ObLogTransportTaskHolder task_holder, const int64_t end_log_id)
{
  int ret = OB_SUCCESS;
  const ObLogTransportReq *task = task_holder.ptr();
  int64_t log_id = OB_INVALID_LOG_ID;
  common::ObMemAttr attr(MTL_ID(), "StandbyTpTask");
  common::SpinRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue not init", KR(ret));
  } else if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_IN_STOP_STATE;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue is in stop state", KR(ret));
  } else if (OB_ISNULL(task) || !task->is_valid() || end_log_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "task is null or log size is invalid", KR(ret), KPC(task), K(end_log_id));
  } else if (OB_FAIL(parse_group_entry_log_id_(task->log_data_, task->log_size_, log_id))) {
    ATOMIC_AAF(&parse_fail_cnt_, 1);
    CLOG_LOG(WARN, "parse group entry log id failed", KR(ret), K(task->log_size_), KPC(task));
  } else if (OB_FAIL(self_heal_())) {
    CLOG_LOG(ERROR, "self heal failed", KR(ret), K_(queue_size), KP_(alloc_mgr),
      K(end_log_id), KPC(task));
  } else {
    const int64_t task_bytes = task->log_size_;
    CLOG_LOG(INFO, "transport task push begin", K(log_id), K(end_log_id),
             "sw_begin_sn", sw_.get_begin_sn(), "sw_end_sn", sw_.get_end_sn(),
             "task_start_lsn", task->start_lsn_, "task_end_lsn", task->end_lsn_,
             "task_bytes", task_bytes);
    SwRevertGuard revert_guard;
    if (OB_FAIL(get_slot_guard_for_push_(log_id, revert_guard))) {
      CLOG_LOG(WARN, "get slot guard for push failed", KR(ret), K(log_id));
    } else {
      TransportTaskSlot *slot = revert_guard.get_slot();
      ObByteLockGuard slot_guard(slot->slot_lock_);
      if (slot->task_holder_.ptr() != nullptr) {
        ATOMIC_AAF(&drop_duplicate_cnt_, 1);
        ret = OB_ENTRY_EXIST;
        CLOG_LOG(WARN, "drop duplicate transport task", KR(ret), K(log_id), "sw_begin_sn", sw_.get_begin_sn(), "sw_end_sn", sw_.get_end_sn());
      } else if (OB_FAIL(task_holder.ensure_owned(attr))) {
        CLOG_LOG(WARN, "ensure transport task owned failed", KR(ret), K(log_id), K(task_bytes), KPC(task));
      } else {
        reserve_bytes_(task_bytes);
        slot->task_holder_ = std::move(task_holder);
        slot->task_bytes_ = task_bytes;
        slot->cached_bytes_ptr_ = &cached_bytes_;
      }
    }
  }
  return ret;
}

// Cannot be called concurrently, cause it will modify proposal_id_.
int ObLogTransportTaskQueue::process(const int64_t proposal_id,
                                     const int64_t end_log_id,
                                     int64_t &processed,
                                     const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  // serialize process to prevent concurrent process and slide
  common::ObSpinLockGuard process_lock_guard(process_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue not init or sliding window not init", KR(ret));
  } else if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_IN_STOP_STATE;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue is in stop state", KR(ret));
  } else if (proposal_id == palf::INVALID_PROPOSAL_ID || proposal_id < 0 || end_log_id < 0 || batch_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid process argument", KR(ret), K(proposal_id), K(end_log_id), K(batch_size));
  } else if (OB_FAIL(self_heal_())) {
    CLOG_LOG(ERROR, "self heal failed", KR(ret), K_(queue_size), KP_(alloc_mgr),
      K(proposal_id), K(end_log_id), K(batch_size));
  } else if (OB_FAIL(try_init_or_advance_base_log_id_(end_log_id))) {
    // Try to init sliding window or advance begin_sn to end_log_id + 1 if needed,
    // to prevent stuck when there are empty slots before end_log_id.
    CLOG_LOG(WARN, "try init or advance base log id failed", KR(ret), K(end_log_id));
  } else {
    processed = 0;
    (void)submit_tasks_sequentially_(proposal_id, batch_size, processed);
    ret = sw_.slide(0, this);
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    }
    if (palf::palf_reach_time_interval(1_s, queue_stats_print_time_us_)) {
      CLOG_LOG(INFO, "transport task queue stats", K(proposal_id), K(end_log_id), K(batch_size), K(processed), KPC(this));
    }
  }
  return ret;
}

int ObLogTransportTaskQueue::submit_tasks_sequentially_(const int64_t proposal_id, const int64_t batch_size, int64_t &processed)
{
  int ret = OB_SUCCESS;
  int64_t cur_sn = sw_.get_begin_sn();
  const int64_t end_sn = sw_.get_end_sn();
  while (OB_SUCC(ret) && cur_sn < end_sn && processed < batch_size) {
    TransportTaskSlot *slot = nullptr;
    const ObLogTransportReq *task = nullptr;
    SwRevertGuard revert_guard = SwRevertGuard();
    if (OB_FAIL(sw_.get(cur_sn, slot))) {
      CLOG_LOG(ERROR, "get slot failed", KR(ret), K(cur_sn));
    } else if (FALSE_IT(revert_guard = SwRevertGuard(sw_, cur_sn, slot))) { // do nothing
    } else if (!slot->is_valid()) {
      ret = OB_ITER_END;
    } else if (slot->is_submitted_) {
      cur_sn++;
    } else if (FALSE_IT(task = slot->task_holder_.ptr())) { // do nothing
    } else if (OB_FAIL(raw_write_(proposal_id, task->start_lsn_, task->scn_, task->log_data_, task->log_size_))) {
      CLOG_LOG(WARN, "raw write failed", KR(ret), K(proposal_id), K(cur_sn), KPC(task));
    } else {
      slot->is_submitted_ = true;
      cur_sn++;
      processed++;
    }
  }
  return ret;
}

void ObLogTransportTaskQueue::get_drop_stats(int64_t &drop_stale,
                                             int64_t &drop_duplicate,
                                             int64_t &reject_out_of_window,
                                             int64_t &parse_fail) const
{
  int64_t early_drop_far_lsn = 0;
  int64_t evict_trigger = 0;
  int64_t evict_scan_slots = 0;
  int64_t evicted_slot_cnt = 0;
  int64_t evicted_task_bytes = 0;
  get_drop_stats(drop_stale,
                 drop_duplicate,
                 reject_out_of_window,
                 parse_fail,
                 early_drop_far_lsn,
                 evict_trigger,
                 evict_scan_slots,
                 evicted_slot_cnt,
                 evicted_task_bytes);
}

void ObLogTransportTaskQueue::get_drop_stats(int64_t &drop_stale,
                                             int64_t &drop_duplicate,
                                             int64_t &reject_out_of_window,
                                             int64_t &parse_fail,
                                             int64_t &early_drop_far_lsn,
                                             int64_t &evict_trigger,
                                             int64_t &evict_scan_slots,
                                             int64_t &evicted_slot_cnt,
                                             int64_t &evicted_task_bytes) const
{
  drop_stale = ATOMIC_LOAD(&drop_stale_cnt_);
  drop_duplicate = ATOMIC_LOAD(&drop_duplicate_cnt_);
  reject_out_of_window = ATOMIC_LOAD(&reject_out_of_window_cnt_);
  parse_fail = ATOMIC_LOAD(&parse_fail_cnt_);
  early_drop_far_lsn = ATOMIC_LOAD(&early_drop_far_lsn_cnt_);
  evict_trigger = ATOMIC_LOAD(&evict_trigger_cnt_);
  evict_scan_slots = ATOMIC_LOAD(&evict_scan_slots_);
  evicted_slot_cnt = ATOMIC_LOAD(&evicted_slot_cnt_);
  evicted_task_bytes = ATOMIC_LOAD(&evicted_task_bytes_);
}

void ObLogTransportTaskQueue::inc_early_drop_far_lsn()
{
  ATOMIC_AAF(&early_drop_far_lsn_cnt_, 1);
}

int ObLogTransportTaskQueue::raw_write_(const int64_t proposal_id,
                                        const palf::LSN &lsn,
                                        const share::SCN &scn,
                                        const char *buf,
                                        const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(common::LOG_TP_QUEUE_SLIDING_CB_BEFORE_RAW_WRITE);
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  if (raw_write_test_hook_) {
    return raw_write_test_hook_(proposal_id, lsn, scn, buf, buf_size);
  }
#endif
  if (OB_NOT_NULL(restore_handler_)) {
    ret = restore_handler_->raw_write(proposal_id, lsn, scn, buf, buf_size);
  } else {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "restore handler is null", KR(ret));
  }
  return ret;
}

int ObLogTransportTaskQueue::sliding_cb(const int64_t sn, const palf::FixedSlidingWindowSlot *data)
{
  int ret = OB_SUCCESS;
  const TransportTaskSlot *slot = static_cast<const TransportTaskSlot*>(data);
  const ObLogTransportReq *task = slot->task_holder_.ptr();
  if (!slot->is_valid() || !slot->is_submitted_) {
    ret = OB_EAGAIN;
  } else {
    // slide out this task
  }
  return ret;
}

int ObLogTransportTaskQueue::parse_group_entry_log_id_(const char *buf,
                                                       const int64_t buf_size,
                                                       int64_t &log_id) const
{
  int ret = OB_SUCCESS;
  log_id = OB_INVALID_LOG_ID;
  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    palf::LogGroupEntry group_entry;
    int64_t pos = 0;
    if (OB_FAIL(group_entry.deserialize(buf, buf_size, pos))) {
      CLOG_LOG(WARN, "deserialize log group entry failed", KR(ret), K(buf_size));
    } else if (false == group_entry.check_integrity()) {
      ret = OB_INVALID_DATA;
      CLOG_LOG(WARN, "log group entry integrity check failed", KR(ret), K(buf_size));
    } else {
      log_id = group_entry.get_header().get_log_id();
    }
  }
  return ret;
}

int ObLogTransportTaskQueue::get_slot_guard_for_push_(const int64_t log_id, SwRevertGuard &revert_guard)
{
  int ret = OB_SUCCESS;
  revert_guard.reset();
  TransportTaskSlot *slot = nullptr;
  DEBUG_SYNC(common::LOG_TP_QUEUE_PUSH_BEFORE_SW_GET);
  if (OB_FAIL(sw_.get(log_id, slot))) {
    if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      ATOMIC_AAF(&drop_stale_cnt_, 1);
      CLOG_LOG(INFO, "drop stale transport task", K(log_id), "sw_begin_sn", sw_.get_begin_sn(), "sw_end_sn", sw_.get_end_sn());
    } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      ATOMIC_AAF(&reject_out_of_window_cnt_, 1);
      CLOG_LOG(WARN, "reject out of window transport task", K(log_id), "sw_begin_sn", sw_.get_begin_sn(), "sw_end_sn", sw_.get_end_sn());
    } else if (OB_ERR_UNEXPECTED == ret) {
      CLOG_LOG(ERROR, "get slot failed unexpectedly", KR(ret), K(log_id), "sw_begin_sn", sw_.get_begin_sn(), "sw_end_sn", sw_.get_end_sn());
    } else {
      CLOG_LOG(WARN, "get slot failed", KR(ret), K(log_id), "sw_begin_sn", sw_.get_begin_sn(), "sw_end_sn", sw_.get_end_sn());
    }
  } else {
    DEBUG_SYNC(common::LOG_TP_QUEUE_PUSH_AFTER_SW_GET);
    revert_guard = SwRevertGuard(sw_, log_id, slot);
  }
  return ret;
}

int ObLogTransportTaskQueue::try_init_or_advance_base_log_id_(const int64_t end_log_id)
{
  int ret = OB_SUCCESS;
  const int64_t new_begin_sn = end_log_id + 1;
  // Advance begin_sn only when end_log_id increases.
  // if sw is cleared, its begin_sn is -1, and the min value of new_begin_sn is 1, so we need to init and truncate
  if (sw_.get_begin_sn() >= new_begin_sn) { // do nothing
  } else if (OB_FAIL(sw_.truncate_and_reset_begin_sn(new_begin_sn))) {
    CLOG_LOG(ERROR, "truncate_and_reset_begin_sn failed", KR(ret), K(new_begin_sn),
              "sw_begin_sn", sw_.get_begin_sn(), "sw_end_sn", sw_.get_end_sn());
  } else {
    CLOG_LOG(INFO, "truncate_and_reset_begin_sn success", K(new_begin_sn),
              "sw_begin_sn", sw_.get_begin_sn(), "sw_end_sn", sw_.get_end_sn());
  }
  return ret;
}

int ObLogTransportTaskQueue::self_heal_()
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(self_heal_lock_);
  if (OB_FAIL(sw_.init(palf::FIRST_VALID_LOG_ID, queue_size_, alloc_mgr_)) && OB_INIT_TWICE != ret) {
    CLOG_LOG(ERROR, "re-init sliding window failed", KR(ret), K_(queue_size), KP_(alloc_mgr));
  } else if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    // do nothing
  }
  return ret;
}

void ObLogTransportTaskQueue::reserve_bytes_(const int64_t bytes)
{
  if (bytes > 0) {
    ATOMIC_AAF(&cached_bytes_, bytes);
  }
}

void ObLogTransportTaskQueue::release_bytes_(const int64_t bytes)
{
  if (bytes > 0) {
    ATOMIC_AAF(&cached_bytes_, -bytes);
  }
}

ObLogTransportTaskQueue::FirstSlotInfo ObLogTransportTaskQueue::get_first_slot_info_() const
{
  FirstSlotInfo info;
  if (IS_INIT) {
    TransportTaskSlot *slot = nullptr;
    int64_t begin_sn = sw_.get_begin_sn();
    if (OB_SUCCESS == const_cast<palf::FixedSlidingWindow<TransportTaskSlot>&>(sw_).get(begin_sn, slot)) {
      {
        ObByteLockGuard slot_guard(slot->slot_lock_);
        const ObLogTransportReq *task = slot->task_holder_.ptr();
        info.has_task_ = (task != nullptr);
        if (info.has_task_) {
          info.task_ptr_ = task;
          info.task_bytes_ = slot->task_bytes_;
          info.is_submitted_ = slot->is_submitted_;
        }
      }
      (void)const_cast<palf::FixedSlidingWindow<TransportTaskSlot>&>(sw_).revert(begin_sn);
    }
  }
  return info;
}

} // namespace logservice
} // namespace oceanbase
