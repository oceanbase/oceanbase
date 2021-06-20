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

#include "storage/ob_replay_status.h"

#include "lib/stat/ob_session_stat.h"
#include "share/ob_errno.h"
#include "share/ob_tenant_mgr.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/ob_partition_service.h"
#include "storage/replayengine/ob_i_log_replay_engine.h"
#include "ob_safe_ref.h"

namespace oceanbase {
using namespace common;
namespace storage {
int64_t ObReplayLogTask::get_trans_inc_no() const
{
  int ret = OB_SUCCESS;
  int64_t inc_no = -1;
  int64_t pos = serialization::encoded_length_i64(log_type_);
  if (!ObStorageLogTypeChecker::is_trans_log(log_type_) ||
      OB_FAIL(serialization::decode_i64(log_buf_, log_size_, pos, &inc_no))) {
    inc_no = -1;
  }
  return inc_no;
}

void ObReplayLogTask::reset()
{
  log_type_ = storage::OB_LOG_UNKNOWN;
  log_submit_timestamp_ = common::OB_INVALID_TIMESTAMP;
  log_id_ = common::OB_INVALID_ID;
  log_size_ = 0;
  batch_committed_ = false;
  task_submit_timestamp_ = common::OB_INVALID_TIMESTAMP;
  log_buf_ = NULL;
  ref_buf_ = NULL;
}

int64_t ObReplayLogTask::get_trans_mutator_size() const
{
  int64_t trans_mutator_size = 0;
  if (ObStorageLogTypeChecker::has_trans_mutator(log_type_)) {
    trans_mutator_size = log_size_;
  }
  return trans_mutator_size;
}

int ObReplayTask::init(ObReplayStatus* replay_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(replay_status)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "invalid argument", K(type_), K(ret));
  } else {
    replay_status_ = replay_status;
  }
  return ret;
}

ObReplayTask::~ObReplayTask()
{
  reset();
}

void ObReplayTask::reuse()
{
  // attention: type_ and replay_status_ can not be reset
  enqueue_ts_ = 0;
  throttled_ts_ = OB_INVALID_TIMESTAMP;
}

void ObReplayTask::reset()
{
  reuse();
  type_ = INVALID_LOG_TASK;
  replay_status_ = NULL;
}

void ObSubmitReplayLogTask::reuse()
{
  ObReplayTask::reuse();
  storage_log_type_ = ObStorageLogType::OB_LOG_UNKNOWN;
  accum_checksum_ = 0;
  set_last_slide_out_log_info(0, 0);
  set_next_submit_log_info(0, 0);
}

void ObSubmitReplayLogTask::reset()
{
  ObReplayTask::reset();
  storage_log_type_ = ObStorageLogType::OB_LOG_UNKNOWN;
  accum_checksum_ = 0;
  set_last_slide_out_log_info(0, 0);
  set_next_submit_log_info(0, 0);
}

uint64_t ObSubmitReplayLogTask::get_next_submit_log_id() const
{
  struct types::uint128_t log_info;
  LOAD128(log_info, &next_submit_log_info_);
  return log_info.hi;
}

int64_t ObSubmitReplayLogTask::get_next_submit_log_ts() const
{
  struct types::uint128_t log_info;
  LOAD128(log_info, &next_submit_log_info_);
  return log_info.lo;
}

uint64_t ObSubmitReplayLogTask::get_last_slide_out_log_id() const
{
  struct types::uint128_t log_info;
  LOAD128(log_info, &last_slide_out_log_info_);
  return log_info.hi;
}

int64_t ObSubmitReplayLogTask::get_last_slide_out_log_ts() const
{
  struct types::uint128_t log_info;
  LOAD128(log_info, &last_slide_out_log_info_);
  return log_info.lo;
}

bool ObSubmitReplayLogTask::need_submit_log() const
{
  struct types::uint128_t next_submit_log_info;
  struct types::uint128_t last_slide_out_log_info;
  LOAD128(next_submit_log_info, &next_submit_log_info_);
  LOAD128(last_slide_out_log_info, &last_slide_out_log_info_);
  return (next_submit_log_info.hi <= last_slide_out_log_info.hi && 0 != last_slide_out_log_info.hi);
}

int64_t ObSubmitReplayLogTask::get_pending_submit_task_count() const
{
  // result_task_count is not accurate with error of 1
  struct types::uint128_t next_submit_log_info;
  struct types::uint128_t last_slide_out_log_info;
  LOAD128(next_submit_log_info, &next_submit_log_info_);
  LOAD128(last_slide_out_log_info, &last_slide_out_log_info_);
  const int64_t result_task_count =
      (next_submit_log_info.hi >= 1 && next_submit_log_info.hi - 1 < last_slide_out_log_info.hi)
          ? (last_slide_out_log_info.hi - (next_submit_log_info.hi - 1))
          : 0;
  return result_task_count;
}

void ObSubmitReplayLogTask::set_last_slide_out_log_info(const uint64_t log_id, const int64_t log_ts)
{
  struct types::uint128_t new_log_info;
  new_log_info.hi = log_id;
  new_log_info.lo = log_ts;

  struct types::uint128_t old_log_info;
  while (true) {
    LOAD128(old_log_info, &last_slide_out_log_info_);
    if (CAS128(&last_slide_out_log_info_, old_log_info, new_log_info)) {
      break;
    } else {
      PAUSE();
    }
  }
}

void ObSubmitReplayLogTask::set_next_submit_log_info(const uint64_t log_id, const int64_t log_ts)
{
  struct types::uint128_t new_log_info;
  new_log_info.hi = log_id;
  new_log_info.lo = log_ts;

  struct types::uint128_t old_log_info;
  while (true) {
    LOAD128(old_log_info, &next_submit_log_info_);
    if (CAS128(&next_submit_log_info_, old_log_info, new_log_info)) {
      break;
    } else {
      PAUSE();
    }
  }
}

int ObSubmitReplayLogTask::update_next_submit_log_info(const uint64_t log_id, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  struct types::uint128_t new_log_info;
  new_log_info.hi = log_id;
  new_log_info.lo = log_ts;

  struct types::uint128_t old_log_info;
  while (true) {
    LOAD128(old_log_info, &next_submit_log_info_);
    if (new_log_info.hi <= old_log_info.hi && new_log_info.lo <= old_log_info.lo) {
      break;
    } else if (new_log_info.hi <= old_log_info.hi || new_log_info.lo < old_log_info.lo) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR,
          "unexpected new log info",
          KR(ret),
          K(new_log_info.hi),
          K(new_log_info.lo),
          K(old_log_info.hi),
          K(old_log_info.lo));
    } else if (CAS128(&next_submit_log_info_, old_log_info, new_log_info)) {
      break;
    } else {
      PAUSE();
    }
  }
  return ret;
}

int ObSubmitReplayLogTask::update_last_slide_out_log_info(const uint64_t log_id, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  struct types::uint128_t new_log_info;
  new_log_info.hi = log_id;
  new_log_info.lo = log_ts;

  struct types::uint128_t old_log_info;
  while (true) {
    LOAD128(old_log_info, &last_slide_out_log_info_);
    if (new_log_info.hi <= old_log_info.hi && new_log_info.lo <= old_log_info.lo) {
      break;
    } else if (new_log_info.hi <= old_log_info.hi || new_log_info.lo < old_log_info.lo) {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR,
          "unexpected new log info",
          KR(ret),
          K(new_log_info.hi),
          K(new_log_info.lo),
          K(old_log_info.hi),
          K(old_log_info.lo));
    } else if (CAS128(&last_slide_out_log_info_, old_log_info, new_log_info)) {
      break;
    } else {
      PAUSE();
    }
  }
  return ret;
}

ObReplayLogTaskQueue::~ObReplayLogTaskQueue()
{
  reset();
}

int ObReplayLogTaskQueue::init(ObReplayStatus* replay_status, const int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(replay_status)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "invalid argument", K(type_), K(ret));
  } else if (OB_FAIL(ObReplayTask::init(replay_status))) {
    REPLAY_LOG(WARN, "ObReplayTask init error", K(type_), K(ret));
  } else {
    index_ = index;
  }
  return ret;
}

void ObReplayLogTaskQueue::reuse()
{
  // attention::do not reset replay_status_ and index_
  // just clear queues and fail_info
  ObLink* top_item = NULL;
  if (NULL != replay_status_) {
    while (NULL != (top_item = pop())) {
      wait_sync();  // wait ref_ = 0
      ObReplayLogTask* replay_task = static_cast<ObReplayLogTask*>(top_item);
      replay_status_->free_replay_task(replay_task);
    };
  }
  fail_info_.reset();
  ObReplayTask::reuse();
}

void ObReplayLogTaskQueue::reset()
{
  // attention::do not reset replay_status_ and index_
  // just clear queues and fail_info
  ObLink* top_item = NULL;
  if (NULL != replay_status_) {
    while (NULL != (top_item = pop())) {
      wait_sync();  // wait ref_ = 0
      ObReplayLogTask* replay_task = static_cast<ObReplayLogTask*>(top_item);
      replay_status_->free_replay_task(replay_task);
    };
  }
  fail_info_.reset();
  index_ = 0;
  ObReplayTask::reset();
}

ObReplayStatus::ObReplayStatus()
    : is_inited_(false),
      ref_cnt_(0),
      replay_err_info_(),
      last_task_info_(),
      post_barrier_status_(POST_BARRIER_FINISHED),
      pending_task_count_(0),
      pending_task_mutator_size_(0),
      pending_abort_task_count_(0),
      eagain_count_(0),
      eagain_start_ts_(OB_INVALID_TIMESTAMP),
      total_submitted_task_num_(0),
      total_replayed_task_num_(0),
      rwlock_(),
      is_enabled_(false),
      is_pending_(false),
      need_filter_trans_log_(false),
      can_receive_log_(true),
      tenant_id_(OB_INVALID_TENANT_ID),
      offline_partition_log_id_(OB_INVALID_ID),
      offline_partition_task_submitted_(false),
      rp_eg_(NULL),
      submit_log_info_rwlock_(),
      allocator_(NULL),
      last_replay_log_id_(common::OB_INVALID_ID)
{}

ObReplayStatus::~ObReplayStatus()
{
  destroy();
}

void ObReplayStatus::destroy()
{
  ObTimeGuard timeguard("destroy replay status", 3 * 1000 * 1000);
  timeguard.click();
  WLockGuard wlock_guard(get_rwlock());
  reset();
  timeguard.click();
}

int ObReplayStatus::init(const uint64_t tenant_id, replayengine::ObILogReplayEngine* rp_eg, SafeRef2& safe_ref)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id) || OB_ISNULL(rp_eg)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "invalid argument", K(tenant_id), K(rp_eg), K(ret));
  } else if (OB_FAIL(TMA_MGR_INSTANCE.get_tenant_replay_allocator(tenant_id, allocator_))) {
    REPLAY_LOG(WARN, "get_tenant_replay_allocator failed", K(ret), K(tenant_id));
  } else if (NULL == allocator_) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(WARN, "allocator_ is NULL", K(ret), K(tenant_id), KP(allocator_));
  } else if (OB_FAIL(submit_log_task_.init(this))) {
    REPLAY_LOG(WARN, "failed to init submit_log_task", K(ret));
  } else {
    rp_eg_ = rp_eg;
    safe_ref_ = safe_ref;
    for (int64_t i = 0; OB_SUCC(ret) && i < REPLAY_TASK_QUEUE_SIZE; ++i) {
      if (OB_FAIL(task_queues_[i].init(this, i))) {
        REPLAY_LOG(WARN, "failed to init task_queue", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

// The caller is responsible for locking
void ObReplayStatus::reset()
{
  // Do not reset the fields that need to be initialized by calling init: allocator_,
  // submit_log_task and some members of task_queues_
  replay_err_info_.reset();
  last_task_info_.reset();
  post_barrier_status_ = POST_BARRIER_FINISHED;
  pending_task_count_ = 0;
  pending_abort_task_count_ = 0;
  clear_pending_task_mutator_size_();
  eagain_count_ = 0;
  eagain_start_ts_ = OB_INVALID_TIMESTAMP;
  total_submitted_task_num_ = 0;
  total_replayed_task_num_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  offline_partition_log_id_ = OB_INVALID_ID;
  offline_partition_task_submitted_ = false;
  WLockGuard wlock_guard(get_submit_log_info_rwlock());
  submit_log_task_.reset();
  is_enabled_ = false;
  is_pending_ = false;
  need_filter_trans_log_ = false;
  can_receive_log_ = true;
  last_replay_log_id_ = common::OB_INVALID_ID;
  REPLAY_LOG(INFO, "reset replay status");
  if (OB_ISNULL(allocator_) && is_inited_) {
    REPLAY_LOG(ERROR, "allocator_ is NULL");
  } else {
    allocator_ = NULL;
  }

  for (int64_t i = 0; i < REPLAY_TASK_QUEUE_SIZE; ++i) {
    task_queues_[i].reset();
  }
}

// The caller is responsible for locking
void ObReplayStatus::reuse()
{
  // Do not reset the fields that need to be initialized by calling init: allocator_,
  // submit_log_task and some members of task_queues_
  replay_err_info_.reset();
  last_task_info_.reset();
  post_barrier_status_ = POST_BARRIER_FINISHED;
  pending_task_count_ = 0;
  pending_abort_task_count_ = 0;
  clear_pending_task_mutator_size_();
  eagain_count_ = 0;
  eagain_start_ts_ = OB_INVALID_TIMESTAMP;
  total_submitted_task_num_ = 0;
  total_replayed_task_num_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  offline_partition_log_id_ = OB_INVALID_ID;
  offline_partition_task_submitted_ = false;
  WLockGuard wlock_guard(get_submit_log_info_rwlock());
  submit_log_task_.reuse();
  is_enabled_ = false;
  need_filter_trans_log_ = false;
  is_pending_ = false;
  can_receive_log_ = true;
  last_replay_log_id_ = common::OB_INVALID_ID;
  REPLAY_LOG(INFO, "reuse replay status");
  if (OB_ISNULL(allocator_)) {
    REPLAY_LOG(ERROR, "allocator_ is NULL");
  }
  for (int64_t i = 0; i < REPLAY_TASK_QUEUE_SIZE; ++i) {
    task_queues_[i].reuse();
  }
}

void* ObReplayStatus::alloc_replay_task_buf(const int64_t size, const ObPartitionKey& pkey)
{
  void* ret_ptr = NULL;
  const bool is_table_inner_table = is_inner_table(pkey.get_table_id());
  if (is_inited_ && NULL != allocator_) {
    ret_ptr = allocator_->alloc_replay_task_buf(is_table_inner_table, size);
  }
  return ret_ptr;
}

bool ObReplayStatus::can_alloc_replay_task(const int64_t size, const common::ObPartitionKey& pkey)
{
  bool b_ret = false;
  const bool is_table_inner_table = is_inner_table(pkey.get_table_id());
  if (is_inited_ && NULL != allocator_) {
    b_ret = allocator_->can_alloc_replay_task(is_table_inner_table, size);
  }
  return b_ret;
}

void ObReplayStatus::free_replay_task(ObReplayLogTask* task)
{
  if (NULL != allocator_ && NULL != task) {
    const bool is_table_inner_table = is_inner_table(task->pk_.get_table_id());
    bool is_ref_self = task->is_ref_self();
    if (NULL != task->ref_buf_ && NULL != task->ref_buf_->buf_) {
      // The one who decrements the reference count to 0 is responsible for freeing memory
      if (0 == task->ref_buf_->dec_ref()) {
        ObReplayLogTask* to_free_task = static_cast<ObReplayLogTask*>(task->ref_buf_->buf_);
        to_free_task->~ObReplayLogTask();
        allocator_->free_replay_task(is_table_inner_table, to_free_task);
      }
    }

    if (!is_ref_self) {
      task->~ObReplayLogTask();
      allocator_->free_replay_task(is_table_inner_table, task);
    }
  }
}

void ObReplayStatus::add_task(ObReplayLogTask& task)
{
  inc_pending_task_count_(task.pk_);
  int64_t mutator_size = task.get_trans_mutator_size();
  if (mutator_size > 0) {
    inc_pending_task_mutator_size_(task, mutator_size);
  }
  if (ObStorageLogTypeChecker::is_trans_abort_log(task.log_type_)) {
    inc_pending_abort_task_count_(task.pk_);
  }
}

// This function is only provided to offline logs to be called after external processing, so
// dec_pendint_abort_task_count is not needed
void ObReplayStatus::dec_task_count(const common::ObPartitionKey& pkey)
{
  dec_pending_task_count_(pkey);
}

void ObReplayStatus::remove_task(ObReplayLogTask& task)
{
  int64_t mutator_size = task.get_trans_mutator_size();
  dec_pending_task_count_(task.pk_);
  if (ObStorageLogTypeChecker::is_trans_abort_log(task.log_type_)) {
    dec_pending_abort_task_count_(task.pk_);
  }
  if (mutator_size > 0) {
    dec_pending_task_mutator_size_(task, mutator_size);
  }
}

uint64_t ObReplayStatus::get_min_unreplay_log_id()
{
  uint64_t unreplay_log_id = UINT64_MAX;
  int64_t unused = INT64_MAX;
  get_min_unreplay_log(unreplay_log_id, unused);
  return unreplay_log_id;
}

// the invoker needs to lock
int64_t ObReplayStatus::get_min_unreplay_log_timestamp()
{
  uint64_t unused = UINT64_MAX;
  int64_t timestamp = INT64_MAX;
  get_min_unreplay_log(unused, timestamp);
  return timestamp;
}

void ObReplayStatus::get_min_unreplay_log(uint64_t& unreplay_log_id, int64_t& timestamp)
{
  unreplay_log_id = UINT64_MAX;
  timestamp = INT64_MAX;
  {
    RLockGuard Rlock_guard(get_submit_log_info_rwlock());
    uint64_t next_submit_log_id = get_next_submit_log_id();
    int64_t next_submit_log_ts = get_next_submit_log_ts();
    uint64_t last_slide_out_log_id = get_last_slide_out_log_id();
    int64_t last_slide_out_log_ts = get_last_slide_out_log_ts();

    if (next_submit_log_ts <= last_slide_out_log_ts) {
      unreplay_log_id = next_submit_log_id;
      timestamp = next_submit_log_ts;
    }
  }

  for (int64_t i = 0; i < REPLAY_TASK_QUEUE_SIZE; ++i) {
    ObReplayLogTask* replay_task = NULL;
    task_queues_[i].acquire_ref();
    // pop() return OB_EAGAIN only when the queue is empty.
    ObLink* top_item = task_queues_[i].top();
    if (NULL != top_item && NULL != (replay_task = static_cast<ObReplayLogTask*>(top_item))) {
      if (replay_task->log_submit_timestamp_ < timestamp) {
        unreplay_log_id = replay_task->log_id_;
        timestamp = replay_task->log_submit_timestamp_;
      }
    }
    task_queues_[i].release_ref();
  }
}

void ObReplayStatus::check_eagain_too_many_(const ObReplayStatus::CheckCanReplayResult& result)
{
  if (OB_SUCCESS == result.ret_code_ && 0 != eagain_count_) {
    eagain_count_ = 0;
    eagain_start_ts_ = OB_INVALID_TIMESTAMP;
  } else if (OB_EAGAIN == result.ret_code_) {
    eagain_count_++;
    if (OB_INVALID_TIMESTAMP == eagain_start_ts_) {
      eagain_start_ts_ = ObTimeUtil::current_time();
    }
    if ((eagain_count_ >= EAGAIN_COUNT_THRESHOLD) && (REACH_TIME_INTERVAL(1 * 1000 * 1000L))) {
      const int64_t cur_time = ObTimeUtil::current_time();
      if ((!result.need_wait_schema_refresh_ && (cur_time - eagain_start_ts_ > EAGAIN_INTERVAL_NORMAL_THRESHOLD)) ||
          (result.need_wait_schema_refresh_ && (cur_time - eagain_start_ts_ > EAGAIN_INTERVAL_BIG_THRESHOLD))) {
        REPLAY_LOG(ERROR,
            "retry submit on EAGAIN too many times",
            K(eagain_count_),
            K(eagain_start_ts_),
            K(last_task_info_),
            K(result));
      }
    }
  } else { /*do nothing*/
  }
}

int ObReplayStatus::can_replay_log(ObReplayStatus::CheckCanReplayResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!result.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "invalid argument", K(result), K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(WARN, "allocator_ is NULL", K(result), K(ret));
  } else if (OB_UNLIKELY(!is_enabled())) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "partition is offline, replay task is not expected", K(result), K(ret));
  } else {
    const ObStorageLogType log_type = result.task_info_.get_log_type();
    if ((GCTX.is_primary_cluster() || !is_inner_table(result.pkey_.get_table_id())) &&
        OB_UNLIKELY(is_tenant_out_of_memory_())) {
      ret = OB_EAGAIN;
      result.is_out_of_memstore_mem_ = true;
    } else if (OB_FAIL(check_barrier_(log_type, result.pkey_)) && OB_EAGAIN != ret) {
      REPLAY_LOG(WARN, "failed to check_barrier", K(ret), K(result), "replay status", *this);
    } else if (OB_EAGAIN == ret) {
      result.has_barrier_ = true;
      result.is_pre_barrier_ = ObStorageLogTypeChecker::is_pre_barrier_required_log(log_type);
    } else { /*do nothing*/
    }

    if (OB_SUCC(ret) && GCTX.is_standby_cluster()) {
      /*Check if there is a replay task needs to retry because the schema version is not new enough.
        If there is, then no subsequent replay task will be submitted to avoid exhausting the memory of replay
        engine.*/
      result.need_wait_schema_refresh_ = need_wait_schema_refresh_();
      if (result.need_wait_schema_refresh_) {
        ret = OB_EAGAIN;
      }
    }

    if (OB_SUCC(ret)) {
      REPLAY_LOG(DEBUG, "check can replay log success", K(ret), K(result), "replay status", *this);
    } else if (OB_EAGAIN == ret) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        REPLAY_LOG(INFO, "check can replay log return eagain", K(ret), K(result), "replay status", *this);
      }
    } else {
      REPLAY_LOG(WARN, "failed to check can replay log", K(ret), K(result), "replay status", *this);
    }
    result.ret_code_ = ret;
    check_eagain_too_many_(result);
  }

  return ret;
}

int ObReplayStatus::check_barrier_(const ObStorageLogType log_type, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(ERROR, "not inited", K(log_type), K(pkey), KR(ret));
  } else {
    ObReplayPostBarrierStatus post_barrier_status = ATOMIC_LOAD(&post_barrier_status_);
    if (POST_BARRIER_FINISHED == post_barrier_status) {
      if (ObStorageLogTypeChecker::is_pre_barrier_required_log(log_type)) {
        ret = has_pending_task(pkey) ? OB_EAGAIN : OB_SUCCESS;
      } else {
        ret = OB_SUCCESS;
      }
    } else if (POST_BARRIER_SUBMITTED == post_barrier_status) {
      ret = OB_EAGAIN;
    } else {
      ret = OB_ERR_UNEXPECTED;
      REPLAY_LOG(ERROR, "unknown start membership submit status", K(ret), K(log_type), K(pkey), "replay status", *this);
    }
  }
  return ret;
}

void ObReplayStatus::set_last_task_info(const ObReplayTaskInfo& task_info)
{
  last_task_info_ = task_info;
}

void ObReplayStatus::inc_pending_task_count_(const ObPartitionKey& pkey)
{
  UNUSED(pkey);
  (void)ATOMIC_FAA(&pending_task_count_, 1);
}

void ObReplayStatus::dec_pending_task_count_(const ObPartitionKey& pkey)
{
  int64_t ov = ATOMIC_FAA(&pending_task_count_, -1);
  if (OB_UNLIKELY(ov <= 0)) {
    REPLAY_LOG(
        ERROR, "decrease pending_task_count_ but pending_task_count_ <= 0", K(pkey), K(ov), "replay_status", *this);
  }
}

void ObReplayStatus::inc_x_total_submitted_task_num(int64_t x)
{
  (void)ATOMIC_FAA(&total_submitted_task_num_, x);
}

void ObReplayStatus::inc_pending_abort_task_count_(const ObPartitionKey& pkey)
{
  UNUSED(pkey);
  (void)ATOMIC_FAA(&pending_abort_task_count_, 1);
}

void ObReplayStatus::dec_pending_abort_task_count_(const ObPartitionKey& pkey)
{
  int64_t ov = ATOMIC_FAA(&pending_abort_task_count_, -1);
  if (OB_UNLIKELY(ov <= 0)) {
    REPLAY_LOG(ERROR,
        "decrease pending_abort_task_count_ but pending_abort_task_count_ <= 0",
        K(pkey),
        K(ov),
        "replay_status",
        *this);
  }
}

void ObReplayStatus::inc_pending_task_mutator_size_(ObReplayLogTask& task, const int64_t mutator_size)
{
  (void)ATOMIC_FAA(&pending_task_mutator_size_, mutator_size);
  if (OB_ISNULL(allocator_)) {
    REPLAY_LOG(ERROR, "allocator is NULL", K(task), "replay_status", *this);
  } else {
    allocator_->inc_pending_replay_mutator_size(mutator_size);
  }
}

void ObReplayStatus::dec_pending_task_mutator_size_(ObReplayLogTask& task, const int64_t mutator_size)
{
  int64_t ov = ATOMIC_FAA(&pending_task_mutator_size_, -mutator_size);
  if (OB_UNLIKELY(ov <= 0)) {
    REPLAY_LOG(ERROR, "pending_task_mutator_size_ is negative", K(task), K(ov), "replay_status", *this);
  }

  if (OB_ISNULL(allocator_)) {
    REPLAY_LOG(ERROR, "allocator is NULL", K(task), "replay_status", *this);
  } else {
    allocator_->dec_pending_replay_mutator_size(mutator_size);
  }
}

void ObReplayStatus::clear_pending_task_mutator_size_()
{
  int64_t ov = ATOMIC_TAS(&pending_task_mutator_size_, 0);

  if (OB_ISNULL(allocator_)) {
    if (is_inited_) {
      REPLAY_LOG(ERROR, "allocator is NULL", "replay_status", *this);
    }
  } else {
    allocator_->dec_pending_replay_mutator_size(ov);
  }
}

bool ObReplayStatus::has_pending_task(const ObPartitionKey& pkey)
{
  int64_t count = 0;
  bool bool_ret = (0 != (count = ATOMIC_LOAD(&pending_task_count_)));

  if (count > PENDING_COUNT_THRESHOLD && REACH_TIME_INTERVAL(1000 * 1000)) {
    REPLAY_LOG(WARN, "too many pending replay task", K(pkey), K(count), "replay_status", *this);
  }
  return bool_ret;
}

bool ObReplayStatus::has_pending_abort_task_(const ObPartitionKey& pkey) const
{
  int64_t count = 0;
  bool bool_ret = (0 != (count = ATOMIC_LOAD(&pending_abort_task_count_)));

  if (count > PENDING_COUNT_THRESHOLD && REACH_TIME_INTERVAL(1000 * 1000)) {
    REPLAY_LOG(WARN, "too many pending abort replay task", K(pkey), K(count));
  }
  return bool_ret;
}

int ObReplayStatus::enable(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(ERROR, "replay status not inited", K(is_inited_), K(pkey), KR(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "invalid pk", K(pkey), K(ret));
  } else if (OB_UNLIKELY(is_enabled_)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "replay status has already been enabled", K(pkey), KR(ret));
  } else {
    is_enabled_ = true;
    tenant_id_ = extract_tenant_id(pkey.table_id_);
  }
  return ret;
}

int ObReplayStatus::disable(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(ERROR, "replay status not inited", K(is_inited_), K(pkey), K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(WARN, "invalid pk", K(pkey), K(ret));
  } else {
    reuse();
    is_enabled_ = false;
  }
  return ret;
}

void ObReplayStatus::set_post_barrier_status(const PostBarrierStatus& post_barrier_status)
{
  post_barrier_status_ = post_barrier_status;
}

int ObReplayStatus::set_post_barrier_finished(const ObReplayTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  const ObStorageLogType log_type = task_info.get_log_type();
  if (!is_enabled()) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "replay status is not enabled", K(task_info), K(ret));
  } else if (OB_UNLIKELY(!(ObStorageLogTypeChecker::is_post_barrier_required_log(log_type))) ||
             OB_UNLIKELY(!(last_task_info_ == task_info))) {
    // post barrier check:include start_membership, partition_meta, add_partition_to_pg log
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(ERROR, "invalid argument", K_(last_task_info), K(task_info), K(ret));
  } else if (POST_BARRIER_SUBMITTED != post_barrier_status_) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "unexpected post_barrier_status_", K_(last_task_info), K_(post_barrier_status), K(ret));
  } else {
    post_barrier_status_ = POST_BARRIER_FINISHED;
  }
  return ret;
}

int ObReplayStatus::push_task(ObReplayLogTask& task, uint64_t task_sign)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(push_(task, task_sign))) {
    common::ObTenantStatEstGuard tenantguard(OB_SYS_TENANT_ID);
    EVENT_INC(CLOG_REPLAY_TASK_SUBMIT_COUNT);
  } else {
    REPLAY_LOG(ERROR, "failed to push task to replay status", K(task), K(ret));
  }
  return ret;
}
// for submit replay task
int ObReplayStatus::push_(ObReplayLogTask& task, uint64_t task_sign)
{
  int ret = OB_SUCCESS;
  uint64_t queue_idx = task_sign % REPLAY_TASK_QUEUE_SIZE;
  ObReplayLogTaskQueue& target_queue = task_queues_[queue_idx];
  if (OB_ISNULL(rp_eg_)) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(ERROR, "replay engine is NULL", K(task), K(ret));
  } else {
    target_queue.push(&task);
    if (target_queue.acquire_lease()) {
      /* The thread that gets the lease is responsible for encapsulating the task queue as a request
       * and placing it into replay engine*/
      inc_ref();  // Add the reference count first, if the task push fails, dec_ref() is required
      if (OB_FAIL(rp_eg_->submit_task_into_queue(&target_queue))) {
        REPLAY_LOG(ERROR, "failed to push batch task to replay engine", K(task), K(task_sign), K(ret));
        dec_ref();
      }
    }
  }
  return ret;
}

int ObReplayStatus::set_need_filter_trans_log(const ObPartitionKey& pkey, const bool need_filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rp_eg_)) {
    ret = OB_NOT_INIT;
    REPLAY_LOG(ERROR, "not init", K(rp_eg_), K(pkey), K(ret));
  } else {
    return rp_eg_->set_need_filter_trans_log(pkey, need_filter);
  }
  return ret;
}

int ObReplayStatus::check_and_submit_task(const ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts,
    const bool need_replay, const clog::ObLogType log_type, const int64_t next_replay_log_ts)
{
  int ret = OB_SUCCESS;
  // check when log slide out
  const int64_t last_slide_out_log_id = get_last_slide_out_log_id();
  if (OB_UNLIKELY(!is_enabled())) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(
        ERROR, "replay status is not enabled", K(need_replay), K(pkey), K(log_id), K(log_type), K(log_ts), K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || OB_INVALID_TIMESTAMP == log_ts || OB_INVALID_ID == log_id ||
                         OB_INVALID_TIMESTAMP == next_replay_log_ts || next_replay_log_ts > log_ts)) {
    ret = OB_INVALID_ARGUMENT;
    REPLAY_LOG(ERROR,
        "invalid arguments",
        K(need_replay),
        K(pkey),
        K(log_id),
        K(log_ts),
        K(log_type),
        K(next_replay_log_ts),
        K(ret));
  } else if (log_id != (last_slide_out_log_id + 1)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(
        ERROR, "invalid log id", K(ret), K(pkey), K(log_id), K(log_type), K(last_slide_out_log_id), K(last_task_info_));
  } else if (!need_replay) {
    if (submit_log_task_.need_submit_log()) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        REPLAY_LOG(WARN,
            "submit condition is not ready",
            K(ret),
            K(pkey),
            K(need_replay),
            K(log_id),
            K(log_type),
            K(last_slide_out_log_id),
            K(last_task_info_));
      }
    } else {
      // The minor freeze dump depends on this. After the Nop log slides out, the value returned by
      // get_min_unreplay_log_timestamp() should be INT64_MAX
      WLockGuard wlock_guard(get_submit_log_info_rwlock());
      const uint64_t old_next_submit_log_id = get_next_submit_log_id();
      const int64_t old_next_submit_log_ts = get_next_submit_log_ts();
      if (OB_FAIL(update_next_submit_log_info(log_id + 1, log_ts + 1))) {
        REPLAY_LOG(ERROR, "failed to update_next_submit_log_info", KR(ret), K(pkey), K(log_id), K(log_ts), K(log_type));
      } else if (OB_FAIL(update_last_slide_out_log_info(log_id, log_ts))) {
        REPLAY_LOG(ERROR, "failed to update_last_slide_out_log_info", KR(ret), K(pkey), K(log_id), K(log_type));
        set_next_submit_log_info(old_next_submit_log_id, old_next_submit_log_ts);
      } else { /*do nothing*/
      }
    }
  } else {
    {
      if (!submit_log_task_.need_submit_log()) {
        // here must modify log_ts first, or may lead to the rollback of min_unreplay_log_timestamp
        WLockGuard wlock_guard(get_submit_log_info_rwlock());
        const uint64_t old_next_submit_log_id = get_next_submit_log_id();
        const int64_t old_next_submit_log_ts = get_next_submit_log_ts();
        set_next_submit_log_info(log_id, next_replay_log_ts);
        if (OB_FAIL(update_last_slide_out_log_info(log_id, log_ts))) {
          REPLAY_LOG(
              ERROR, "failed to update_last_slide_out_log_info", KR(ret), K(pkey), K(log_id), K(log_ts), K(log_type));
          set_next_submit_log_info(old_next_submit_log_id, old_next_submit_log_ts);
        }
      } else {
        WLockGuard wlock_guard(get_submit_log_info_rwlock());
        if (OB_FAIL(update_last_slide_out_log_info(log_id, log_ts))) {
          REPLAY_LOG(
              ERROR, "failed to update_last_slide_out_log_info", KR(ret), K(pkey), K(log_id), K(log_ts), K(log_type));
        }
      }
    }
    if (submit_log_task_.acquire_lease()) {
      /* The thread that gets the lease is responsible for encapsulating the task queue as a request
       * and placing it into replay engine*/
      inc_ref();  // Add the reference count first, if the task push fails, dec_ref() is required
      if (OB_FAIL(rp_eg_->submit_task_into_queue(&submit_log_task_))) {
        REPLAY_LOG(ERROR, "failed to submit submit_log_task to replay engine", K(pkey), K(submit_log_task_), K(ret));
        dec_ref();
      }
    }
  }
  return ret;
}

int ObReplayStatus::submit_restore_task(const ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  WLockGuard wlock_guard(get_submit_log_info_rwlock());
  const int64_t last_slide_out_log_ts = get_last_slide_out_log_ts();
  if (OB_UNLIKELY(!is_enabled())) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR, "replay status is not enabled", K(pkey), K(log_id), K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || log_ts <= last_slide_out_log_ts)) {
    ret = OB_ERR_UNEXPECTED;
    REPLAY_LOG(ERROR,
        "invalid log id or pkey",
        K(ret),
        K(pkey),
        K(log_id),
        K(log_ts),
        K(last_task_info_),
        K(submit_log_task_));
  } else if (OB_FAIL(update_last_slide_out_log_info(log_id, log_ts))) {
    REPLAY_LOG(ERROR, "failed to update_last_slide_out_log_info", KR(ret), K(pkey), K(log_id), K(log_ts));
  } else {
    REPLAY_LOG(INFO, "succ to submit restore task", K(pkey), K(log_id), K(log_ts), K(submit_log_task_));
    if (submit_log_task_.acquire_lease()) {
      /* The thread that gets the lease is responsible for encapsulating the task queue as a request
       * and placing it into replay engine*/
      inc_ref();  // Add the reference count first, if the task push fails, dec_ref() is required
      if (OB_FAIL(rp_eg_->submit_task_into_queue(&submit_log_task_))) {
        REPLAY_LOG(ERROR, "failed to submit submit_log_task to replay engine", K(pkey), K(submit_log_task_), K(ret));
        dec_ref();
      }
    }
  }
  return ret;
}

// wlock outside
void ObReplayStatus::set_submit_log_task_info(const ObPartitionKey& pkey, common::ObBaseStorageInfo& clog_info)
{
  WLockGuard wlock_guard(get_submit_log_info_rwlock());
  set_next_submit_log_info(clog_info.get_last_replay_log_id() + 1, clog_info.get_submit_timestamp() + 1);
  set_last_slide_out_log_info(clog_info.get_last_replay_log_id(), clog_info.get_submit_timestamp());
  submit_log_task_.set_accum_checksum(clog_info.get_accumulate_checksum());
  REPLAY_LOG(INFO, "set submit log task info before enable", K(pkey), K(clog_info), K(submit_log_task_));
}

int ObReplayStatus::erase_pending(const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(rwlock_.wrlock(timeout))) {
    if (OB_SUCC(ret)) {
      is_pending_ = false;
    }

    while (OB_FAIL(rwlock_.unlock())) {
      REPLAY_LOG(ERROR, "erase pending rwlock unlock failed", K(ret), K(timeout), K(*this));
      usleep(100);
    }
  } else {
    REPLAY_LOG(WARN, "erase pending rwlock lock failed", K(ret), K(timeout), K(*this));
  }

  return ret;
}

int ObReplayStatus::set_pending(const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(rwlock_.wrlock(timeout))) {
    if (OB_SUCC(ret)) {
      is_pending_ = true;
    }

    while (OB_FAIL(rwlock_.unlock())) {
      REPLAY_LOG(ERROR, "set pending rwlock unlock failed", K(ret), K(timeout), K(*this));
      usleep(100);
    }
  } else {
    REPLAY_LOG(WARN, "set pending rwlock lock failed", K(ret), K(timeout), K(*this));
  }

  return ret;
}

bool ObReplayStatus::need_wait_schema_refresh_() const
{
  bool need_wait = false;
  for (int64_t i = 0; !need_wait && i < REPLAY_TASK_QUEUE_SIZE; ++i) {
    if (OB_TRANS_WAIT_SCHEMA_REFRESH == task_queues_[i].fail_info_.ret_code_) {
      need_wait = true;
    }
  }
  return need_wait;
}

bool ObReplayStatus::need_submit_log_task_without_lock()
{
  return submit_log_task_.need_submit_log();
}

bool ObReplayStatus::is_tenant_out_of_memory() const
{
  return is_tenant_out_of_memory_();
}

bool ObReplayStatus::is_tenant_out_of_memory_() const
{
  bool bool_ret = false;
  ObTenantManager& tenant_mgr = ObTenantManager::get_instance();
  if (OB_NOT_NULL(allocator_)) {
    bool_ret = tenant_mgr.is_rp_pending_log_too_large(tenant_id_, allocator_->get_pending_replay_mutator_size());
  }
  return bool_ret;
}

ObReplayStatus::CheckCanReplayResult::CheckCanReplayResult(common::ObPartitionKey pkey, ObReplayTaskInfo& task_info)
{
  ret_code_ = OB_SUCCESS;
  ;
  is_out_of_memstore_mem_ = false;
  has_barrier_ = false;
  is_pre_barrier_ = false;
  need_wait_schema_refresh_ = false;
  pkey_ = pkey;
  task_info_ = task_info;
}

void ObReplayStatus::CheckCanReplayResult::reset()
{
  ret_code_ = OB_SUCCESS;
  ;
  is_out_of_memstore_mem_ = false;
  has_barrier_ = false;
  is_pre_barrier_ = false;
  need_wait_schema_refresh_ = false;
  pkey_.reset();
  task_info_.reset();
}

bool ObReplayStatus::CheckCanReplayResult::is_valid() const
{
  return (
      pkey_.is_valid() && task_info_.is_valid(), ObStorageLogTypeChecker::is_valid_log_type(task_info_.get_log_type()));
}

}  // namespace storage
}  // namespace oceanbase
