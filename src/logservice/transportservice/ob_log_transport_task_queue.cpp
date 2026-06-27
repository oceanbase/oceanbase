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
#include "lib/alloc/alloc_assist.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_defer.h"
#include "share/ob_define.h"
#include "share/ob_debug_sync.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry.h"

namespace oceanbase
{
namespace logservice
{

LogReceivedTransportTask::LogReceivedTransportTask(
    share::ObLSID ls_id,
    palf::LSN start_lsn,
    palf::LSN end_lsn,
    share::SCN scn,
    const char *log_data,
    int64_t log_size)
  : ls_id_(ls_id),
    start_lsn_(start_lsn),
    end_lsn_(end_lsn),
    scn_(scn),
    log_data_(log_data),
    log_size_(log_size),
    ref_cnt_(0) {}

LogReceivedTransportTask::~LogReceivedTransportTask() {}

bool LogReceivedTransportTask::is_valid() const
{
  return ls_id_.is_valid()
      && start_lsn_.is_valid()
      && end_lsn_.is_valid()
      && scn_.is_valid()
      && end_lsn_ > start_lsn_
      && log_data_ != nullptr
      && log_size_ > 0
      && end_lsn_ == start_lsn_ + log_size_;
}

int64_t LogReceivedTransportTask::inc_ref()
{
  return ATOMIC_AAF(&ref_cnt_, 1);
}

int64_t LogReceivedTransportTask::dec_ref()
{
  return ATOMIC_SAF(&ref_cnt_, 1);
}

ObLogTransportTaskHandle::ObLogTransportTaskHandle()
  : task_(nullptr) {}

ObLogTransportTaskHandle::ObLogTransportTaskHandle(const ObLogTransportTaskHandle &other)
  : task_(nullptr)
{
  *this = other;
}

ObLogTransportTaskHandle &ObLogTransportTaskHandle::operator=(
    const ObLogTransportTaskHandle &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (other.is_valid()) {
      this->task_ = other.task_;
      this->task_->inc_ref();
    }
  }
  return *this;
}

ObLogTransportTaskHandle::~ObLogTransportTaskHandle()
{
  reset();
}

void ObLogTransportTaskHandle::reset()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(task_)) {
    if (OB_UNLIKELY(false == task_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "invalid transport task during reset", KR(ret), KPC(this));
    }
    int ref = task_->dec_ref();
    if (0 == ref) {
      task_->~LogReceivedTransportTask();
      share::mtl_free(task_);
    } else if (0 > ref) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "ref less than zero", KR(ret), KPC(this));
    }
    task_ = nullptr;
  }
}

bool ObLogTransportTaskHandle::is_valid() const
{
  return OB_NOT_NULL(task_) && task_->is_valid();
}

const LogReceivedTransportTask *ObLogTransportTaskHandle::task() const
{
  return task_;
}

int ObLogTransportTaskHandle::init(const ObLogTransportReq *task)
{
  int ret = OB_SUCCESS;
  common::ObMemAttr attr(MTL_ID(), "LogTpReceivedTk");
  LogReceivedTransportTask *copied = nullptr;
  char* log_data = nullptr;
  if (OB_ISNULL(task) || OB_UNLIKELY(false == task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid transport task for owned ptr", K(ret), KP(task));
  } else if (OB_NOT_NULL(this->task_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "handle is not empty", K(ret), KPC(this));
  } else if (OB_ISNULL(copied = static_cast<LogReceivedTransportTask*>(
      share::mtl_malloc(sizeof(LogReceivedTransportTask) + task->log_size_, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to allocate memory for copied transport task",
        K(ret), KP(task), KPC(task));
  } else {
    log_data = reinterpret_cast<char*>(copied) + sizeof(LogReceivedTransportTask);
    new (copied) LogReceivedTransportTask(task->ls_id_,
        task->start_lsn_, task->end_lsn_, task->scn_, log_data, task->log_size_);
    MEMCPY(const_cast<char*>(copied->log_data_), task->log_data_, task->log_size_);
    copied->inc_ref();
    this->task_ = copied;
  }
  return ret;
}

ObLogTransportTaskQueue::ObLogTransportTaskQueue()
  : task_map_(),
    is_inited_(false),
    is_stopped_(false),
    id_(0),
    next_submit_lsn_(),
    queue_size_(OB_INVALID_SIZE),
    cached_bytes_(0),
    max_cached_bytes_(DEFAULT_MAX_CACHED_BYTES),
    lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
    queued_end_lock_(common::ObLatchIds::TRANSPORT_SERVICE_TASK_LOCK),
    drop_duplicate_cnt_(0),
    early_drop_far_lsn_cnt_(0),
    total_inserted_cnt_(0),
    total_inserted_bytes_(0),
    total_processed_cnt_(0),
    total_success_cnt_(0),
    total_success_bytes_(0),
    total_skipped_cnt_(0) {}

ObLogTransportTaskQueue::~ObLogTransportTaskQueue()
{
  destroy();
}

int ObLogTransportTaskQueue::init(const int64_t id, const int64_t queue_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (id < 0 || queue_size <= 0 || 0 != (queue_size & (queue_size - 1))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid init argument", KR(ret), K(id), K(queue_size));
  } else if (OB_FAIL(task_map_.init("TransportQueMap", MTL_ID()))) {
    CLOG_LOG(WARN, "init task map failed", KR(ret));
  } else if (OB_FAIL(task_map_.resize(2 * queue_size))) {
    CLOG_LOG(WARN, "resize task map failed", KR(ret));
  } else if (OB_FAIL(task_map_.set_load_factor_lmt(0.0, 1.0))) {
    CLOG_LOG(WARN, "set load factor limit failed", KR(ret));
  } else {
    queue_size_ = queue_size;
    ATOMIC_STORE(&is_stopped_, false);
    id_ = id;
    next_submit_lsn_.reset();
    queued_end_lsn_.reset();
    queued_end_scn_.reset();
    cached_bytes_ = 0;
    max_cached_bytes_ = DEFAULT_MAX_CACHED_BYTES;
    drop_duplicate_cnt_ = 0;
    early_drop_far_lsn_cnt_ = 0;
    total_inserted_cnt_ = 0;
    total_inserted_bytes_ = 0;
    total_processed_cnt_ = 0;
    total_success_cnt_ = 0;
    total_success_bytes_ = 0;
    total_skipped_cnt_ = 0;
    is_inited_ = true;
    CLOG_LOG(INFO, "init transport task queue success", KP(this), KPC(this));
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    if (task_map_.is_inited()) {
      task_map_.destroy();
    }
  }
  return ret;
}

void ObLogTransportTaskQueue::stop()
{
  common::SpinWLockGuard guard(lock_);
  ATOMIC_STORE(&is_stopped_, true);
}

void ObLogTransportTaskQueue::destroy()
{
  common::SpinWLockGuard guard(lock_);
  if (IS_INIT) {
    CLOG_LOG(INFO, "destroy transport task queue", KP(this));
    is_inited_ = false;
    task_map_.destroy();
    ATOMIC_STORE(&is_stopped_, true);
    id_ = 0;
    next_submit_lsn_.reset();
    {
      common::ObSpinLockGuard end_guard(queued_end_lock_);
      queued_end_lsn_.reset();
      queued_end_scn_.reset();
    }
    queue_size_ = OB_INVALID_SIZE;
    cached_bytes_ = 0;
    max_cached_bytes_ = DEFAULT_MAX_CACHED_BYTES;
    drop_duplicate_cnt_ = 0;
    early_drop_far_lsn_cnt_ = 0;
    total_inserted_cnt_ = 0;
    total_inserted_bytes_ = 0;
    total_processed_cnt_ = 0;
    total_success_cnt_ = 0;
    total_success_bytes_ = 0;
    total_skipped_cnt_ = 0;
  }
}

void ObLogTransportTaskQueue::switch_to_follower()
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", KR(ret));
  } else {
    next_submit_lsn_.reset();
    {
      common::ObSpinLockGuard end_guard(queued_end_lock_);
      queued_end_lsn_.reset();
      queued_end_scn_.reset();
    }
    task_map_.clear();
    ATOMIC_STORE(&cached_bytes_, 0);
  }
  CLOG_LOG(INFO, "switch to follower", KR(ret), KP(this), KPC(this));
}

void ObLogTransportTaskQueue::switch_to_leader(const palf::LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", KR(ret));
  } else {
    next_submit_lsn_.atomic_store(end_lsn);
    {
      common::ObSpinLockGuard end_guard(queued_end_lock_);
      queued_end_lsn_ = end_lsn;
      queued_end_scn_.reset();
    }
  }
  CLOG_LOG(INFO, "switch to leader", KR(ret), KP(this), KPC(this));
}

int ObLogTransportTaskQueue::push(const ObLogTransportReq *task)
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  ObLogTransportTaskHandle handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue not init", KR(ret));
  } else if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_IN_STOP_STATE;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue is in stop state", KR(ret));
  } else if (OB_ISNULL(task) || !task->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "task is null or invalid", KR(ret), KP(task), KPC(task));
  } else if (OB_FAIL(can_push_task_(task))) {
    CLOG_LOG(WARN, "cannot push task", KR(ret), KPC(task), KPC(this));
  } else if (OB_FAIL(handle.init(task))) {
    CLOG_LOG(WARN, "copy transport task failed", KR(ret), KPC(task));
  } else if (OB_FAIL(task_map_.insert(LSNWarp(task->start_lsn_), handle))) {
    CLOG_LOG(WARN, "insert transport task failed", KR(ret), KPC(task));
    if (OB_ENTRY_EXIST == ret) {
      ATOMIC_AAF(&drop_duplicate_cnt_, 1);
      ret = OB_SUCCESS;
    }
  // possible race condition
  // - next_submit_lsn_ 100
  // - push task 100
  //
  // race timeline:
  // push                   process
  // can_push_task_() succ
  //                        update next_submit_lsn_ to 200
  //                        erase task 100
  // insert task 100
  //
  // without switch_to_follower() or node restart, task 100 will permanent exist
  } else if (OB_FAIL(can_push_task_(task))) {
    // OB_ENTRY_NOT_EXIST or OB_NOT_INIT. both two error code are ok.
    (void) task_map_.erase(LSNWarp(task->start_lsn_));
    CLOG_LOG(WARN, "task has been already processed",
        KR(ret), KPC(task), K(next_submit_lsn_.atomic_load()));
  } else {
    add_container_cached_bytes_(&cached_bytes_, task->log_size_);
    ATOMIC_AAF(&total_inserted_cnt_, 1);
    ATOMIC_AAF(&total_inserted_bytes_, task->log_size_);
    // Semi-sync: advance queued_end_ to the running MAX end position across the current
    // log and everything already accepted into the queue. This may be non-continuous:
    // a task that lands past a hole still advances the high-water-mark, and a later push
    // that fills the hole below the max must NOT lower it. The early ACK is optimistic;
    // holes are healed by retransmit and the durable position is the contiguous
    // next_submit_lsn_ tracked by the flush-ACK path.
    {
      common::ObSpinLockGuard end_guard(queued_end_lock_);
      if (queued_end_lsn_.is_valid() && task->end_lsn_ > queued_end_lsn_) {
        queued_end_lsn_ = task->end_lsn_;
        queued_end_scn_ = task->scn_;
      }
      // Below current max or uninitialized: do NOT advance, wait for switch_to_leader
    }
  }

  CLOG_LOG(TRACE, "push transport task", KR(ret), KPC(task), K(handle));
  return ret;
}

int ObLogTransportTaskQueue::can_push_task_(const ObLogTransportReq *task)
{
  int ret = OB_SUCCESS;
  palf::LSN next_submit_lsn = next_submit_lsn_.atomic_load();
  if (false == next_submit_lsn.is_valid()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "end_lsn is not set to queue", KR(ret), K(next_submit_lsn), KPC(task));
  } else if (task->start_lsn_ < next_submit_lsn) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
    CLOG_LOG(WARN, "task has been already submitted", KR(ret), KPC(task), K(next_submit_lsn));
  } else if (task->end_lsn_ - next_submit_lsn > max_cached_bytes_) {
    int64_t cached_bytes = ATOMIC_LOAD(&cached_bytes_);
    ATOMIC_AAF(&early_drop_far_lsn_cnt_, 1);
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
    CLOG_LOG(WARN, "task overflows lsn distance limit",
        KR(ret), K(cached_bytes), K(next_submit_lsn), KPC(task), K_(max_cached_bytes));
  }
  return ret;
}

int ObLogTransportTaskQueue::update_end_lsn(const palf::LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  TaskRemoveFunctor functor(end_lsn);
  palf::LSN next_submit_lsn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue not init", KR(ret));
  } else if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_IN_STOP_STATE;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue is in stop state", KR(ret));
  } else if (false == end_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid process argument", KR(ret), K(end_lsn));
  } else if (FALSE_IT(next_submit_lsn = next_submit_lsn_.atomic_load())) {
  } else if (false == next_submit_lsn.is_valid()) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(WARN, "next submit lsn is invalid", KR(ret), K(next_submit_lsn));
  } else if (end_lsn > next_submit_lsn) {
    next_submit_lsn_.atomic_store(end_lsn);
    if (OB_FAIL(task_map_.remove_if(functor))) {
      CLOG_LOG(WARN, "remove task from map failed", KR(ret), KPC(this));
    } else {
      CLOG_LOG(INFO, "update next submit lsn success", KR(ret), K(next_submit_lsn), K(end_lsn));
    }
    // DoRemoveIfOnBkt<Function> make sure when TaskRemoveFunctor return true, the kv is deleted
    ATOMIC_SAF(&cached_bytes_, functor.removed_bytes_);
    ATOMIC_AAF(&total_skipped_cnt_, functor.removed_cnt_);
  }
  return ret;
}

int ObLogTransportTaskQueue::front(ObLogTransportTaskHandle &handle)
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  palf::LSN next_submit_lsn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue not init or sliding window not init", KR(ret));
  } else if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_IN_STOP_STATE;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue is in stop state", KR(ret));
  } else if (handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid process argument", KR(ret), K(handle));
  } else if (FALSE_IT(next_submit_lsn = next_submit_lsn_.atomic_load())) {
  } else if (false == next_submit_lsn.is_valid()) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(WARN, "next submit lsn is invalid", KR(ret), K(next_submit_lsn));
  } else if (OB_FAIL(task_map_.get(LSNWarp(next_submit_lsn), handle))) {
    CLOG_LOG(WARN, "get task failed", KR(ret), K(next_submit_lsn));
  }
  return ret;
}

int ObLogTransportTaskQueue::success(const ObLogTransportTaskHandle &handle)
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  palf::LSN next_submit_lsn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue not init or sliding window not init", KR(ret));
  } else if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_IN_STOP_STATE;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue is in stop state", KR(ret));
  } else if (false == handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid handle", KR(ret), K(handle));
  } else if (FALSE_IT(next_submit_lsn = next_submit_lsn_.atomic_load())) {
  } else if (false == next_submit_lsn.is_valid()) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(WARN, "next submit lsn is invalid", KR(ret), K(next_submit_lsn));
  } else if (handle.task()->start_lsn_ != next_submit_lsn) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN,
        "handle and next_submit_lsn_ not match", KR(ret), K(handle), K(next_submit_lsn));
  } else {
    int tmp_ret = OB_SUCCESS;
    sub_container_cached_bytes_(&cached_bytes_, handle);
    next_submit_lsn_.atomic_store(next_submit_lsn + handle.task()->log_size_);
    ATOMIC_AAF(&total_success_cnt_, 1);
    ATOMIC_AAF(&total_success_bytes_, handle.task()->log_size_);
    // OB_ENTRY_NOT_EXIST or OB_NOT_INIT. both two error code are ok.
    if (OB_TMP_FAIL(task_map_.erase(LSNWarp(handle.task()->start_lsn_)))) {
      CLOG_LOG(WARN, "erase submitted task failed", KR(tmp_ret), K(handle));
    }
  }
  ATOMIC_AAF(&total_processed_cnt_, 1);
  return ret;
}

int ObLogTransportTaskQueue::failure(const ObLogTransportTaskHandle &handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue not init or sliding window not init", KR(ret));
  } else if (ATOMIC_LOAD(&is_stopped_)) {
    ret = OB_IN_STOP_STATE;
    CLOG_LOG(WARN, "ObLogTransportTaskQueue is in stop state", KR(ret));
  } else if (false == handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid handle", KR(ret), K(handle));
  }
  ATOMIC_AAF(&total_processed_cnt_, 1);
  return ret;
}

ObLogTransportTaskQueue::TaskRemoveFunctor::TaskRemoveFunctor(const palf::LSN &end_lsn)
  : end_lsn_(end_lsn), removed_cnt_(0), removed_bytes_(0) {}

ObLogTransportTaskQueue::TaskRemoveFunctor::~TaskRemoveFunctor()
{
  end_lsn_.reset();
  removed_cnt_ = 0;
  removed_bytes_ = 0;
}

bool ObLogTransportTaskQueue::TaskRemoveFunctor::operator()(const LSNWarp &key,
                                                            const ObLogTransportTaskHandle &value)
{
  int ret = OB_SUCCESS;
  bool remove = false;
  if (key.get_value() < end_lsn_) {
    remove = true;
  }
  if (remove) {
    ++removed_cnt_;
    removed_bytes_ += value.task()->log_size_;
  }
  return remove;
}

void ObLogTransportTaskQueue::add_container_cached_bytes_(int64_t *container_cached_bytes,
                                                          const int64_t bytes)
{
  if (OB_NOT_NULL(container_cached_bytes) && bytes > 0) {
    ATOMIC_AAF(container_cached_bytes, bytes);
  }
}

void ObLogTransportTaskQueue::sub_container_cached_bytes_(
    int64_t *container_cached_bytes,
    const ObLogTransportTaskHandle &task_handle)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(container_cached_bytes) && OB_NOT_NULL(task_handle.task())
      && task_handle.task()->log_size_ > 0) {
    const LogReceivedTransportTask *task = task_handle.task();
    const int64_t bytes = task->log_size_;
    const int64_t cached_bytes = ATOMIC_SAF(container_cached_bytes, bytes);
    if (OB_UNLIKELY(cached_bytes < 0)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "transport task queue cached bytes is negative", K(cached_bytes), K(bytes));
    }
  }
}

uint64_t ObLogTransportTaskQueue::count() const
{
  common::SpinRLockGuard guard(lock_);
  return task_map_.count();
}

// stats, not precise, can be slightly inconsistent with each other
void ObLogTransportTaskQueue::clear_stats()
{
  common::SpinRLockGuard guard(lock_);
  ATOMIC_STORE(&drop_duplicate_cnt_, 0);
  ATOMIC_STORE(&early_drop_far_lsn_cnt_, 0);
  ATOMIC_STORE(&total_inserted_cnt_, 0);
  ATOMIC_STORE(&total_inserted_bytes_, 0);
  ATOMIC_STORE(&total_processed_cnt_, 0);
  ATOMIC_STORE(&total_success_cnt_, 0);
  ATOMIC_STORE(&total_success_bytes_, 0);
  ATOMIC_STORE(&total_skipped_cnt_, 0);
}

} // namespace logservice
} // namespace oceanbase
