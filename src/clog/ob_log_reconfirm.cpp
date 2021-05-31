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

#include "ob_log_reconfirm.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_spin_lock.h"
#include "ob_log_entry.h"
#include "ob_log_membership_mgr_V2.h"
#include "ob_log_sliding_window.h"
#include "ob_log_state_mgr.h"
#include "ob_i_log_engine.h"
#include "storage/blocksstable/ob_store_file_system.h"
#include "storage/ob_file_system_router.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"

namespace oceanbase {
using namespace common;
namespace clog {
int ObReconfirmLogInfoArray::init(const uint64_t start_id, common::ObILogAllocator* alloc_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == start_id || NULL == alloc_mgr) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(start_id), KP(alloc_mgr));
  } else if (NULL == (array_ptr_ = static_cast<ObReconfirmLogInfo*>(
                          alloc_mgr->ge_alloc(sizeof(ObReconfirmLogInfo) * RECONFIRM_LOG_ARRAY_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc ObReconfirmLogInfoArray failed", K(ret));
  } else {
    memset(array_ptr_, 0, sizeof(ObReconfirmLogInfo) * RECONFIRM_LOG_ARRAY_LENGTH);
    for (int64_t i = 0; i < RECONFIRM_LOG_ARRAY_LENGTH; ++i) {
      new (array_ptr_ + i) ObReconfirmLogInfo;
    }
  }
  if (OB_SUCC(ret)) {
    start_id_ = start_id;
    alloc_mgr_ = alloc_mgr;
    is_inited_ = true;
  }
  return ret;
}

void ObReconfirmLogInfoArray::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    for (uint64_t i = 0; i < RECONFIRM_LOG_ARRAY_LENGTH; ++i) {
      array_ptr_[i].free_log_entry_buf();
    }
    alloc_mgr_->ge_free(array_ptr_);
    start_id_ = OB_INVALID_ID;
    alloc_mgr_ = NULL;
    array_ptr_ = NULL;
  }
}

ObReconfirmLogInfo* ObReconfirmLogInfoArray::get_log_info(const int64_t idx)
{
  ObReconfirmLogInfo* ret_ptr = NULL;
  if (!is_inited_) {
    CLOG_LOG(WARN, "ObReconfirmLogInfoArray is not inited");
  } else if (idx < 0 || idx >= RECONFIRM_LOG_ARRAY_LENGTH) {
    CLOG_LOG(WARN, "invalid arguments", K(idx), K_(start_id));
  } else {
    ret_ptr = array_ptr_ + idx;
  }
  return ret_ptr;
}

int ObReconfirmLogInfoArray::reuse(const uint64_t new_start_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObReconfirmLogInfoArray is not inited", K(ret));
  } else if (OB_INVALID_ID == new_start_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(new_start_id), K(start_id_));
  } else {
    for (int64_t i = 0; i < RECONFIRM_LOG_ARRAY_LENGTH; ++i) {
      // it will free log_entry buf in reset()
      array_ptr_[i].reset();
    }
    ATOMIC_STORE(&start_id_, new_start_id);
  }
  return ret;
}

ObLogReconfirm::ObLogReconfirm()
    : state_(INITED),
      new_proposal_id_(),
      max_flushed_id_(OB_INVALID_ID),
      start_id_(OB_INVALID_ID),
      next_id_(OB_INVALID_ID),
      leader_ts_(OB_INVALID_TIMESTAMP),
      last_fetched_id_(OB_INVALID_ID),
      last_fetched_log_ts_(OB_INVALID_TIMESTAMP),
      last_ts_(OB_INVALID_TIMESTAMP),
      max_log_ack_list_(),
      max_log_ack_map_(),
      curr_member_list_(),
      majority_cnt_(0),
      log_info_array_(),
      sw_(NULL),
      state_mgr_(NULL),
      mm_(NULL),
      cascading_mgr_(NULL),
      log_engine_(NULL),
      alloc_mgr_(NULL),
      lock_(ObLatchIds::CLOG_RECONFIRM_LOCK),
      partition_key_(),
      self_(),
      fetch_max_log_id_ts_(OB_INVALID_TIMESTAMP),
      last_log_range_switch_ts_(OB_INVALID_TIMESTAMP),
      last_push_renew_ms_log_ts_(OB_INVALID_TIMESTAMP),
      last_renew_sync_standby_loc_ts_(OB_INVALID_TIMESTAMP),
      failover_truncate_log_id_(OB_INVALID_ID),
      max_membership_version_(OB_INVALID_TIMESTAMP),
      is_standby_reconfirm_(false),
      receive_previous_max_log_ts_(false),
      is_inited_(false)
{}

int ObLogReconfirm::init(ObILogSWForReconfirm* sw, ObILogStateMgrForReconfirm* state_mgr, ObILogMembershipMgr* mm,
    ObLogCascadingMgr* cascading_mgr, ObILogEngine* log_engine, common::ObILogAllocator* alloc_mgr,
    const ObPartitionKey& partition_key, const ObAddr& self, const int64_t last_replay_submit_timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sw) || OB_ISNULL(state_mgr) || OB_ISNULL(mm) || OB_ISNULL(log_engine) || OB_ISNULL(cascading_mgr) ||
      OB_ISNULL(alloc_mgr) || !partition_key.is_valid() || !self.is_valid()) {
    CLOG_LOG(WARN, "invalid arguments", K(partition_key));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    state_ = INITED;
    new_proposal_id_.reset();
    max_flushed_id_ = OB_INVALID_ID;
    start_id_ = OB_INVALID_ID;
    next_id_ = OB_INVALID_ID;
    leader_ts_ = OB_INVALID_TIMESTAMP;
    last_fetched_id_ = OB_INVALID_ID;
    last_fetched_log_ts_ = OB_INVALID_TIMESTAMP;
    last_ts_ = last_replay_submit_timestamp;
    max_log_ack_list_.reset();
    max_log_ack_map_.reset_all();
    curr_member_list_.reset();
    majority_cnt_ = 0;

    sw_ = sw;
    state_mgr_ = state_mgr;
    mm_ = mm;
    cascading_mgr_ = cascading_mgr;
    log_engine_ = log_engine;
    alloc_mgr_ = alloc_mgr;

    partition_key_ = partition_key;
    self_ = self;
    fetch_max_log_id_ts_ = OB_INVALID_TIMESTAMP;
    last_log_range_switch_ts_ = OB_INVALID_TIMESTAMP;
    last_push_renew_ms_log_ts_ = OB_INVALID_TIMESTAMP;
    last_renew_sync_standby_loc_ts_ = OB_INVALID_TIMESTAMP;
    is_inited_ = true;
  }
  return ret;
}

int ObLogReconfirm::init_reconfirm_()
{
  int ret = OB_SUCCESS;
  const bool need_async_replay = false;
  bool is_replayed = false;
  bool is_replay_failed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!(mm_->get_curr_member_list().contains(self_))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR,
        "I am leader, but curr_member_list doesn't contain myself, unexpected",
        K(ret),
        K_(partition_key),
        K_(self));
  } else if (OB_FAIL(sw_->submit_replay_task(need_async_replay, is_replayed, is_replay_failed)) &&
             OB_CLOG_SLIDE_TIMEOUT != ret) {
    CLOG_LOG(WARN, "submit_replay_task failed", K_(partition_key), K(ret));
  } else if (!state_mgr_->is_cluster_allow_handle_prepare()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "cluster state do not allow handle prepare", K_(partition_key), K(ret));
  } else if (GCTX.is_primary_cluster() && GCTX.need_sync_to_standby() &&
             !share::ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
             OB_FAIL(cascading_mgr_->leader_try_update_sync_standby_child(false))) {  // not trigger renew location
    // primary leader cannot get standby leader from location cache, need renew
    const int64_t now = ObTimeUtility::current_time();
    if (OB_INVALID_TIMESTAMP == last_renew_sync_standby_loc_ts_ ||
        now - last_renew_sync_standby_loc_ts_ >= PRIMARY_RENEW_LOCATION_TIME_INTERVAL) {
      (void)state_mgr_->try_renew_sync_standby_location();
      last_renew_sync_standby_loc_ts_ = now;
    }
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(WARN, "leader_try_update_sync_standby_child failed, try renew location", K_(partition_key), K(ret));
    }
  } else {
    is_standby_reconfirm_ = (STANDBY_LEADER == state_mgr_->get_role() ? true : false);
    int64_t now = ObTimeUtility::current_time();
    int64_t old_ts_ = state_mgr_->get_proposal_id().ts_;
    if (is_standby_reconfirm_) {
      // standby table
      old_ts_ = mm_->get_ms_proposal_id().ts_;
      max_membership_version_ = mm_->get_timestamp();
    }

    new_proposal_id_.ts_ = (old_ts_ < now) ? now : old_ts_ + 1;
    new_proposal_id_.addr_ = self_;
    start_id_ = sw_->get_start_id();
    max_flushed_id_ = sw_->get_max_log_id();
    majority_cnt_ = mm_->get_replica_num() / 2 + 1;

    const int64_t cluster_id = state_mgr_->get_self_cluster_id();
    bool is_all_flushed = false;
    uint64_t no_flushed_log_id = OB_INVALID_ID;
    if (OB_FAIL(check_log_flushed_(is_all_flushed, no_flushed_log_id))) {
      CLOG_LOG(WARN, "check_log_flushed_ failed", K(ret), K(partition_key_), K(start_id_), K(max_flushed_id_));
    } else if (!is_all_flushed) {
      ret = OB_EAGAIN;
      CLOG_LOG(INFO,
          "is_all_flushed false, in init_reconfirm_, need retry",
          K(ret),
          K(partition_key_),
          K(start_id_),
          K(max_flushed_id_),
          K(no_flushed_log_id));
    } else if (OB_FAIL(curr_member_list_.deep_copy(mm_->get_curr_member_list()))) {
      CLOG_LOG(ERROR, "deep_copy failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(curr_member_list_.remove_server(self_))) {
      CLOG_LOG(ERROR, "remove_server failed", K_(partition_key), K(ret), K_(curr_member_list), K_(self));
    } else if (is_standby_reconfirm_) {
      // standby-active non-private replica
      if (OB_FAIL(state_mgr_->handle_standby_prepare_rqst(new_proposal_id_, self_, cluster_id))) {
        CLOG_LOG(WARN, "handle_standby_prepare_rqst failed", K_(partition_key), K(ret), K_(self), K_(curr_member_list));
      }
    } else {
      if (OB_FAIL(state_mgr_->handle_prepare_rqst(new_proposal_id_, self_, cluster_id))) {
        CLOG_LOG(WARN, "handle_prepare_rqst failed", K_(partition_key), K(ret), K_(self), K_(curr_member_list));
      }
    }

    last_ts_ = sw_->get_last_submit_timestamp();
    if ((OB_INVALID_TIMESTAMP == last_ts_ || 0 == last_ts_) && !is_standby_reconfirm_) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "get invalid last_ts", K(ret), K_(partition_key), K_(last_ts));
    }
  }
  return ret;
}

int ObLogReconfirm::get_start_id_and_leader_ts_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    start_id_ = sw_->get_start_id();
    leader_ts_ = sw_->get_epoch_id();
    last_ts_ = sw_->get_last_submit_timestamp();
    bool found = false;
    while (!found && OB_SUCCESS == ret) {
      const int64_t* ref = NULL;
      ObLogTask* task = NULL;
      if (OB_ERROR_OUT_OF_RANGE == (ret = sw_->get_log_task(start_id_, task, ref))) {
        CLOG_LOG(INFO, "reconfirm get_start_id out of range", K_(partition_key), K(ret), K_(start_id));
        start_id_ = sw_->get_start_id();
        leader_ts_ = sw_->get_epoch_id();
        last_ts_ = sw_->get_last_submit_timestamp();
        ret = OB_SUCCESS;
      } else if (OB_ERR_NULL_VALUE == ret) {
        CLOG_LOG(INFO, "reconfirm scan sliding window, got null", K_(partition_key), K_(start_id));
        found = true;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        CLOG_LOG(ERROR, "reconfirm get_start_id failed", K_(partition_key), K(ret), K_(start_id));
      } else {
        task->lock();
        if (task->is_log_confirmed() && task->is_submit_log_exist()) {
          CLOG_LOG(INFO, "reconfirm scan sliding window, got confirmed log", K_(partition_key), K_(start_id));
          start_id_++;
          const int64_t log_epoch_id = task->get_epoch_id();
          leader_ts_ = std::max(log_epoch_id, leader_ts_);  // compatibility for old version NOP log
          last_ts_ = task->get_submit_timestamp();
        } else {
          CLOG_LOG(INFO, "reconfirm scan sliding window, got unconfirmed log", K_(partition_key), K_(start_id));
          found = true;
        }
        task->unlock();
      }
      if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_->revert_log_task(ref))) {
        CLOG_LOG(ERROR, "revert_log_task failed", K_(partition_key), K(tmp_ret));
      } else {
        ref = NULL;
      }
    }
    if (OB_SUCC(ret)) {
      next_id_ = start_id_;
    }
  }
  return ret;
}

int ObLogReconfirm::check_log_flushed_(bool& is_all_flushed, uint64_t& no_flushed_log_id)
{
  int ret = OB_SUCCESS;
  is_all_flushed = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    for (uint64_t log_id = start_id_; log_id <= max_flushed_id_ && OB_SUCCESS == ret && is_all_flushed; ++log_id) {
      const int64_t* ref = NULL;
      ObLogTask* log_task = NULL;
      if (OB_ERR_NULL_VALUE == (ret = sw_->get_log_task(log_id, log_task, ref))) {
        // log not exist
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        CLOG_LOG(ERROR, "get log from sliding window failed", K(ret), K(log_id), K_(partition_key));
      } else {
        log_task->lock();
        if (log_task->is_submit_log_exist()         // has log
            && !log_task->is_log_cursor_valid()) {  // not flushed
          is_all_flushed = false;
          no_flushed_log_id = log_id;
        }
        log_task->unlock();
      }

      if (NULL != ref) {
        sw_->revert_log_task(ref);
        ref = NULL;
      }
    }
  }
  return ret;
}

int ObLogReconfirm::init_log_info_range_(const uint64_t range_start_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == range_start_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(range_start_id));
  } else if (OB_FAIL(log_info_array_.reuse(range_start_id))) {
    CLOG_LOG(WARN, "log_info_array_.reuse failed", K(ret), K_(partition_key), K(range_start_id));
  } else {
    int64_t log_cnt = max_flushed_id_ - range_start_id + 1;
    log_cnt = std::min(RECONFIRM_LOG_ARRAY_LENGTH, log_cnt);
    for (uint64_t i = 0; i < log_cnt && OB_SUCC(ret); ++i) {
      const uint64_t log_id = range_start_id + i;
      const int64_t* ref = NULL;
      ObReconfirmLogInfo* log_info = NULL;
      ObLogTask* log_task = NULL;
      if (NULL == (log_info = log_info_array_.get_log_info(i))) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "get_log_info failed", K_(partition_key), K(ret), K(log_id), K(i));
      } else if (OB_ERR_NULL_VALUE == (ret = sw_->get_log_task(log_id, log_task, ref))) {
        // if it is empty for this log_id, generate a NOP log
        if (OB_SUCCESS != (ret = generate_nop_log_(log_id, i))) {
          CLOG_LOG(WARN, "generate_nop_log_ failed", K_(partition_key), K(ret), K(log_id), K(i));
        } else if (OB_SUCCESS != (ret = log_info->add_server(self_))) {
          CLOG_LOG(WARN, "ack_list add_server failed", K_(partition_key), K(ret), K(log_id), K_(self));
        } else {
          // do nothing
        }
      } else if (OB_ERROR_OUT_OF_RANGE == ret) {
        // this log has slide out, we need update next_id_ and retry
        // last_ts_ must be advanced by current last_submit_ts, or NOP log's log_ts maybe fallback
        const int64_t last_submit_ts = sw_->get_last_submit_timestamp();
        (void)try_update_last_ts_(last_submit_ts);
        ret = OB_EAGAIN;
      } else if (OB_FAIL(ret)) {
        CLOG_LOG(ERROR, "get log from sliding window failed", K(ret), K(log_id), K_(partition_key));
      } else {
        log_task->lock();
        log_task->set_replica_num(mm_->get_replica_num());
        if (log_task->is_log_confirmed() && log_task->is_submit_log_exist()) {
          log_info->set_map(CONFIRMED_TAG_BIT);
          log_info->set_leader_ts(log_task->get_epoch_id());
          log_info->set_confirmed_log_ts(log_task->get_submit_timestamp());
        } else if (!log_task->is_submit_log_exist()) {
          if (OB_SUCCESS != (ret = generate_nop_log_(log_id, i))) {
            CLOG_LOG(WARN, "generate_nop_log_ failed", K_(partition_key), K(ret), K(log_id), K(i));
          } else {
            log_info->add_server(self_);
          }
        } else if (!log_task->is_log_cursor_valid()) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "log_cursor is invalid, unexpected", K(ret), K(partition_key_), K(log_id));
        } else {
          ObLogEntry tmp_entry;
          ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID);
          ObReadBuf& rbuf = guard.get_read_buf();
          if (OB_UNLIKELY(!rbuf.is_valid())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            CLOG_LOG(WARN, "failed to alloc read_buf", K_(partition_key), K(ret), K(log_id));
          } else if (OB_FAIL(log_engine_->read_log_by_location(*log_task, rbuf, tmp_entry))) {
            CLOG_LOG(WARN, "read_log_by_location failed", K_(partition_key), K(ret), K(*log_task));
          } else if (tmp_entry.get_header().get_partition_key() != partition_key_) {
            ret = OB_ERR_UNEXPECTED;
            CLOG_LOG(ERROR, "read_log_by_location wrong log", K_(partition_key), K(ret), K(*log_task), K(tmp_entry));
          } else {
            const ObLogEntry& log_entry = tmp_entry;
            if (OB_FAIL(log_info->deep_copy_log(log_entry))) {
              CLOG_LOG(WARN, "deep copy log_entry failed", K_(partition_key), K(ret), K(log_id));
            } else {
              log_info->add_server(self_);
            }
          }
        }
        log_task->unlock();
      }
      // leader no need query followers for this log, it will update log's proposal_id and send out directly.
      if (OB_SUCCESS == ret && log_info->get_member_number() == majority_cnt_ &&
          !log_info->test_map(CONFIRMED_TAG_BIT)) {
        ObLogEntryHeader* header = const_cast<ObLogEntryHeader*>(&log_info->get_log_entry().get_header());
        // check if need generate truncate log during standby failover
        if (OB_FAIL(try_update_failover_truncate_log_id_(header))) {
          CLOG_LOG(WARN, "try_update_failover_truncate_log_id_ failed", K(ret), K_(partition_key), K(log_id));
        }
        header->set_proposal_id(state_mgr_->get_proposal_id());
        header->update_header_checksum();
        log_info->set_map(MAJORITY_TAG_BIT);
      }
      if (ref != NULL) {
        sw_->revert_log_task(ref);
        ref = NULL;
      }
    }
    const int64_t now = ObTimeUtility::current_time();
    int64_t switch_range_interval = 0;
    if (OB_INVALID_TIMESTAMP != last_log_range_switch_ts_) {
      // Count the total consumption time of log_info_range
      switch_range_interval = now - last_log_range_switch_ts_;
    }
    last_log_range_switch_ts_ = now;
    CLOG_LOG(INFO,
        "init_log_info_range finished",
        K_(partition_key),
        K(ret),
        K(range_start_id),
        K_(start_id),
        K_(next_id),
        K_(max_flushed_id),
        K_(last_log_range_switch_ts),
        K(switch_range_interval));
  }
  return ret;
}

int ObLogReconfirm::prepare_log_map_()
{
  int ret = OB_SUCCESS;
  bool is_all_flushed = false;
  uint64_t no_flushed_log_id = OB_INVALID_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_start_id_and_leader_ts_())) {
    CLOG_LOG(WARN,
        "get_start_id_and_leader_ts_ failed",
        K_(partition_key),
        K(ret),
        K_(start_id),
        K_(next_id),
        K_(leader_ts));
  } else if (start_id_ > max_flushed_id_ + 1) {
    CLOG_LOG(WARN, "wrong log range", K_(partition_key), K_(start_id), K_(max_flushed_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(check_log_flushed_(is_all_flushed, no_flushed_log_id))) {
    CLOG_LOG(WARN, "check_log_flushed_ failed", K(ret), K_(partition_key), K_(start_id), K_(max_flushed_id));
  } else if (!is_all_flushed) {
    ret = OB_EAGAIN;
    CLOG_LOG(INFO,
        "is_all_flushed false, need retry",
        K(ret),
        K_(partition_key),
        K_(start_id),
        K_(max_flushed_id),
        K(no_flushed_log_id));
  } else {
    CLOG_LOG(INFO, "prepare_log_map range", K_(partition_key), K_(start_id), K_(max_flushed_id));
    // start_id_ means sw's lower bound, Its relationship with max_flushed_id_ is as follows:
    // 1.start_id_ == max_flushed_id_ + 1: there is no log in sw, no need reconfirm.
    // 2.start_id_ <  max_flushed_id_ + 1: there are some logs in sw which need reconfirm.
    // 3.start_id_ >  max_flushed_id_ + 1: unexpected state
    if (start_id_ == max_flushed_id_ + 1) {
      CLOG_LOG(INFO, "there is no log to reconfirm", K_(partition_key), K_(start_id), K_(max_flushed_id));
    } else {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(log_info_array_.init(start_id_, alloc_mgr_))) {
          CLOG_LOG(WARN, "log_info_array_ init failed", K_(partition_key), K(ret));
        } else if (OB_FAIL(init_log_info_range_(next_id_))) {
          CLOG_LOG(WARN, "init_log_info_range_ failed", K_(partition_key), K(ret));
        } else {
          // do nothing
        }
      }
      if (OB_FAIL(ret)) {
        clear_memory_();
      }
    }
  }
  return ret;
}

void ObLogReconfirm::reset()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  if (state_ != INITED) {
    clear_memory_();
    state_ = INITED;
    new_proposal_id_.reset();
    max_flushed_id_ = OB_INVALID_ID;
    start_id_ = OB_INVALID_ID;
    next_id_ = OB_INVALID_ID;
    leader_ts_ = OB_INVALID_TIMESTAMP;
    last_fetched_id_ = OB_INVALID_ID;
    last_fetched_log_ts_ = OB_INVALID_TIMESTAMP;
    last_ts_ = OB_INVALID_TIMESTAMP;
    max_log_ack_list_.reset();
    max_log_ack_map_.reset_all();
    curr_member_list_.reset();
    majority_cnt_ = 0;
    fetch_max_log_id_ts_ = OB_INVALID_TIMESTAMP;
    last_log_range_switch_ts_ = OB_INVALID_TIMESTAMP;
    last_push_renew_ms_log_ts_ = OB_INVALID_TIMESTAMP;
    last_renew_sync_standby_loc_ts_ = OB_INVALID_TIMESTAMP;
    failover_truncate_log_id_ = OB_INVALID_ID;
    max_membership_version_ = OB_INVALID_TIMESTAMP;
    is_standby_reconfirm_ = false;
    receive_previous_max_log_ts_ = false;
  }
}

bool ObLogReconfirm::need_start_up()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  return state_ == INITED;
}

int ObLogReconfirm::try_fetch_log_()
{
  int ret = OB_SUCCESS;
  common::ObMemberList dst_member_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    uint64_t new_start_id = sw_->get_start_id();
    if (max_flushed_id_ < new_start_id) {
      CLOG_LOG(INFO, "reconfirm finished", K_(partition_key), K(new_start_id), K_(max_flushed_id), K_(start_id));
    } else if (OB_FAIL(curr_member_list_.deep_copy_to(dst_member_list))) {
      CLOG_LOG(ERROR, "curr_member_list_ deep_copy_to failed", K(ret), K(partition_key_), K(curr_member_list_));
    } else {
      uint64_t end_log_id = log_info_array_.get_end_id();
      end_log_id = std::min(max_flushed_id_ + 1, end_log_id);
      const uint64_t max_confirmed_log_id = sw_->get_max_confirmed_log_id();
      if (OB_SUCCESS != (ret = log_engine_->fetch_log_from_all_follower(dst_member_list,
                             partition_key_,
                             new_start_id,
                             end_log_id,
                             state_mgr_->get_proposal_id(),
                             max_confirmed_log_id))) {
        CLOG_LOG(WARN,
            "submit fetch_log failed",
            K_(partition_key),
            K(ret),
            K_(curr_member_list),
            K(new_start_id),
            K(end_log_id),
            "proposal_id",
            state_mgr_->get_proposal_id());
      } else {
        last_fetched_id_ = end_log_id - 1;
        last_fetched_log_ts_ = ObTimeUtility::current_time();
      }
      CLOG_LOG(INFO,
          "try_fetch_log_",
          K_(partition_key),
          K(ret),
          K_(curr_member_list),
          K(new_start_id),
          K(end_log_id),
          K_(max_flushed_id),
          K_(last_fetched_id));
    }
  }
  return ret;
}

int ObLogReconfirm::try_filter_invalid_log_()
{
  int ret = OB_SUCCESS;
  int64_t idx = next_id_ - log_info_array_.get_start_id();
  ObReconfirmLogInfo* log_info = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == (log_info = log_info_array_.get_log_info(idx))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_log_info failed", K_(partition_key), K(ret), K(idx));
  } else {
    ObLogEntry* log = const_cast<ObLogEntry*>(&log_info->get_log_entry());
    ObLogEntryHeader* header = const_cast<ObLogEntryHeader*>(&log->get_header());
    int64_t tmp_leader_ts = log->get_header().get_epoch_id();
    // check if need generate truncate log firstly
    if (next_id_ == failover_truncate_log_id_) {
      if (OB_LOG_NOP == header->get_log_type()) {
        const ObProposalID curr_proposal_id = state_mgr_->get_proposal_id();
        header->set_log_type(OB_LOG_TRUNCATE);       // update log_type
        header->set_epoch_id(curr_proposal_id.ts_);  // updatge epoch_id
        header->update_header_checksum();            // update header_checksum
        // advance leader_ts_ to filter subsequent logs
        leader_ts_ = header->get_epoch_id();
        CLOG_LOG(INFO,
            "generate truncate log in cluster flashback state, update leader_ts",
            K_(partition_key),
            K_(state),
            "log_id",
            next_id_,
            K(curr_proposal_id),
            K(*header),
            K_(leader_ts));
      } else {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR,
            "unexpected log_type when generate truncate log in cluster flashback state",
            K_(partition_key),
            K(next_id_),
            K(*header));
      }
    }

    if (tmp_leader_ts == leader_ts_) {
      // do nothing
    } else if (tmp_leader_ts > leader_ts_) {
      // Note that log_type cannot be restricted here, because there is a special scenario:
      // During the confirm log process, the order of trans_log->start_working->NOP may be encountered.
      // Assuming that start_working did not reach majority, but the subsequent NOP succeeds,
      // the epoch_id of the NOP will also be larger than the previous trans_log.
      // This situation also needs to update leader_ts.
      CLOG_LOG(INFO,
          "leader timestamp is changed by log during reconfirm",
          K_(partition_key),
          "log_header",
          log->get_header(),
          K_(leader_ts),
          K(tmp_leader_ts));
      // track START_WORKING's epoch_id
      leader_ts_ = tmp_leader_ts;
    } else {
      if (log->get_header().get_log_type() != OB_LOG_NOP) {
        CLOG_LOG(INFO,
            "find invalid log in reconfirmation, try replace with NOP!",
            K_(partition_key),
            "log_header",
            log->get_header(),
            K_(leader_ts));
      }
      ObNopLog nop_log;
      char body_buffer[BUF_SIZE];
      int64_t body_pos = 0;
      if (OB_SUCCESS != (ret = nop_log.serialize(body_buffer, BUF_SIZE, body_pos))) {
        CLOG_LOG(WARN, "serialize failed", K_(partition_key), K(ret));
      } else {
        ObLogEntryHeader header;
        ObLogEntry log_entry;
        const bool is_trans_log = false;
        if (log->get_buf() != NULL) {
          TMA_MGR_INSTANCE.free_log_entry_buf(const_cast<char*>(log->get_buf()));
        }
        if (OB_FAIL(header.generate_header(OB_LOG_NOP,  // log_type
                partition_key_,                         // partition_key
                next_id_,
                body_buffer,                    // buff
                body_pos,                       // data_len
                ObTimeUtility::current_time(),  // generation_timestamp
                leader_ts_,                     // epoch_id
                state_mgr_->get_proposal_id(),
                0,  // submit_timestamp
                state_mgr_->get_freeze_version(),
                is_trans_log))) {
          CLOG_LOG(ERROR, "generate_header failed", K_(partition_key), K(ret));
        } else if (OB_FAIL(log_entry.generate_entry(header, body_buffer))) {
          CLOG_LOG(ERROR, "generate_entry failed", K_(partition_key), K(ret));
        } else if (OB_FAIL(log_info->deep_copy_log(log_entry))) {
          CLOG_LOG(WARN, "copy log_entry failed", K_(partition_key), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogReconfirm::try_update_nop_or_truncate_timestamp(ObLogEntryHeader& header)
{
  int ret = OB_SUCCESS;
  if (is_nop_or_truncate_log(header.get_log_type())) {
    if (OB_FAIL(header.update_nop_or_truncate_submit_timestamp(last_ts_))) {
      CLOG_LOG(WARN, "update_nop_or_truncate_submit_timestamp failed", K(ret), K_(partition_key), K_(last_ts));
    } else {
      header.update_header_checksum();
    }
  } else {
    // not nop or truncate log, do nothing
  }
  return ret;
}

int ObLogReconfirm::try_update_last_ts_(const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TIMESTAMP == log_ts) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K_(partition_key), K(ret), K(log_ts), K_(next_id));
  } else if (log_ts < last_ts_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR,
        "unexpected log_ts when try update last_ts",
        K_(partition_key),
        K(ret),
        K_(next_id),
        K(log_ts),
        K_(last_ts));
  } else {
    last_ts_ = log_ts;
  }
  return ret;
}

int ObLogReconfirm::confirm_log_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    uint64_t confirm_end = last_fetched_id_ + 1;
    confirm_end = std::min(confirm_end, log_info_array_.get_end_id());
    while (next_id_ < confirm_end && OB_SUCCESS == ret) {
      int64_t idx = next_id_ - log_info_array_.get_start_id();
      ObReconfirmLogInfo* log_info = NULL;
      if (NULL == (log_info = log_info_array_.get_log_info(idx))) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "get_log_info failed", K_(partition_key), K(ret), K(idx));
      } else {
        if (log_info->test_map(CONFIRMED_TAG_BIT)) {
          CLOG_LOG(INFO, "the log does not need reconfirm", K_(partition_key), K(ret), K_(next_id));
          int64_t tmp_ts = log_info->get_leader_ts();
          if (leader_ts_ <= tmp_ts) {
            leader_ts_ = tmp_ts;
          } else {
            CLOG_LOG(ERROR, "invalid leader_ts", K_(partition_key), K_(leader_ts));
            ret = OB_ERR_UNEXPECTED;
          }
          const int64_t confirmed_log_ts = log_info->get_confirmed_log_ts();
          (void)try_update_last_ts_(confirmed_log_ts);
        } else if (!log_info->test_map(MAJORITY_TAG_BIT)) {
          CLOG_LOG(INFO, "this log does not get majority response", K_(partition_key), K(ret), K_(next_id));
          break;
        } else if (OB_SUCCESS != (ret = try_filter_invalid_log_())) {
          break;
        } else {
          ObLogEntry* log_ptr = const_cast<ObLogEntry*>(&log_info->get_log_entry());
          if (!log_ptr->check_integrity()) {
            ret = OB_ERR_UNEXPECTED;
            CLOG_LOG(ERROR, "reconfirming an invalid log entry", K(ret), "log_entry", *log_ptr);
          } else {
            ObLogEntryHeader* header = const_cast<ObLogEntryHeader*>(&log_ptr->get_header());
            try_update_nop_freeze_version_(*header);
            try_update_membership_status_(*log_ptr);
            if (OB_FAIL(try_update_nop_or_truncate_timestamp(*header))) {
              CLOG_LOG(WARN, "try_update_nop_or_truncate_timestamp fail", K(ret), K_(partition_key));
            } else if (OB_FAIL(sw_->submit_log(log_ptr->get_header(), log_ptr->get_buf(), NULL))) {
              CLOG_LOG(
                  ERROR, "submit log failed", K_(partition_key), K(ret), K_(next_id), K_(start_id), K_(max_flushed_id));
              break;
            } else {
              CLOG_LOG(TRACE, "submit log success", K_(partition_key), K_(next_id), K_(start_id), K_(max_flushed_id));
            }
          }
          if (OB_SUCC(ret)) {
            const int64_t log_submit_timestamp = log_ptr->get_header().get_submit_timestamp();
            (void)try_update_last_ts_(log_submit_timestamp);
          }
        }
        if (OB_SUCC(ret)) {
          next_id_++;
        }
      }
    }  // end while
    if (OB_SUCC(ret) && next_id_ <= max_flushed_id_ && next_id_ >= log_info_array_.get_end_id()) {
      // process next log_range
      if (OB_EAGAIN == (ret = init_log_info_range_(next_id_))) {
        // ret is EAGAIN when some log has slide out, need update next_id_ and retry
        const uint64_t new_start_id = sw_->get_start_id();
        if (new_start_id > next_id_) {
          next_id_ = new_start_id;
        }
      } else if (OB_FAIL(ret)) {
        CLOG_LOG(WARN, "init_log_info_range_ failed", K_(partition_key), K(ret));
      } else {
        // init_log_info_range_ success, trigger fetch log
        if (need_fetch_log_()) {
          (void)try_fetch_log_();
        }
      }
    }
  }
  return ret;
}

int ObLogReconfirm::fetch_max_log_id_()
{
  int ret = OB_SUCCESS;
  common::ObMemberList dst_member_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_->is_cluster_allow_handle_prepare()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "cluster state do not allow handle prepare", K_(partition_key), K(ret));
  } else if (OB_FAIL(curr_member_list_.deep_copy_to(dst_member_list))) {
    CLOG_LOG(ERROR, "curr_member_list_ deep_copy_to failed", K(ret), K(partition_key_), K(curr_member_list_));
  } else {
    CLOG_LOG(INFO, "send prepare rqst to follower", K_(partition_key), K_(curr_member_list));
    if (is_standby_reconfirm_) {
      ret = log_engine_->submit_standby_prepare_rqst(dst_member_list,
          partition_key_,
          mm_->get_ms_proposal_id());  // ms_proposal_id
    } else {
      ret = log_engine_->submit_prepare_rqst(dst_member_list, partition_key_, state_mgr_->get_proposal_id());
    }
  }
  return ret;
}

bool ObLogReconfirm::need_fetch_log_()
{
  bool bool_ret = false;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "ObLogReconfirm is not inited", K_(partition_key));
  } else {
    uint64_t new_start_id = sw_->get_start_id();
    bool_ret = (last_fetched_id_ == OB_INVALID_ID) || (new_start_id > last_fetched_id_) ||
               (ObTimeUtility::current_time() - last_fetched_log_ts_ >= REPEATED_FETCH_LOG_INTERVAL);
  }
  return bool_ret;
}

int ObLogReconfirm::reconfirm_log_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    if (need_fetch_log_()) {
      ret = try_fetch_log_();
    }
    if (OB_SUCC(ret)) {
      ret = confirm_log_();
    }
  }
  return ret;
}

int ObLogReconfirm::try_set_majority_ack_tag_of_max_log_id_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (max_log_ack_map_.test_map(MAJORITY_TAG_BIT)) {
  } else if (max_log_ack_list_.get_count() + 1 < majority_cnt_ ||
             (receive_previous_max_log_ts_ == false &&
                 ObTimeUtility::current_time() - state_mgr_->get_reconfirm_start_time() <=
                     RECEIVE_PREVIOUS_NEXT_REPLAY_LOG_INFO_INTERVAL &&
                 !state_mgr_->is_new_created_leader())) {
    CLOG_LOG(INFO,
        "max_log_ack_list not majority",
        K_(partition_key),
        K_(majority_cnt),
        K_(max_log_ack_list),
        K_(receive_previous_max_log_ts),
        "recofirm_start_time",
        state_mgr_->get_reconfirm_start_time(),
        "current_time",
        ObTimeUtility::current_time());
  } else {
    CLOG_LOG(INFO, "max_log_ack_list majority", K_(partition_key), K_(majority_cnt), K_(max_log_ack_list));
    max_log_ack_map_.set_map(MAJORITY_TAG_BIT);
  }
  return ret;
}

bool ObLogReconfirm::is_new_proposal_id_flushed_()
{
  bool bool_ret = false;
  if (is_standby_reconfirm_) {
    bool_ret = (new_proposal_id_ == mm_->get_ms_proposal_id());
  } else {
    bool_ret = (new_proposal_id_ == state_mgr_->get_proposal_id());
  }
  return bool_ret;
}

// new_start_id is sw's lower bound id (next log_id), max_flushed_id_ is max_log_id for reconfirm.
// their relationship is as following:
// 1.new_start_id <= max_flushed_id_     :  Indicates that pending logs have not yet been processed
// 2.new_start_id == max_flushed_id_ + 1 :  Indicates START_MEMBERSHIP log has not reached majority
// 3.new_start_id == max_flushed_id_ + 2 :  START_MEMBERSHIP log has reached majority
int ObLogReconfirm::reconfirm()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool need_async_replay = false;
  bool is_replayed = false;
  bool is_replay_failed = false;
  ObLockGuard<ObSpinLock> guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    uint64_t new_start_id = sw_->get_start_id();
    // For newly created partition, follower replica maybe not ready.
    // Leader need retry send prepare request quickly.
    const int64_t fetch_max_log_id_interval =
        state_mgr_->is_new_created_leader() ? (50 * 1000) : CLOG_RECONFIRM_FETCH_MAX_LOG_ID_INTERVAL;
    switch (state_) {
      case INITED: {
        if (OB_SUCCESS != (ret = init_reconfirm_())) {
          CLOG_LOG(WARN, "init reconfirm failed", K_(partition_key), K(ret));
        } else {
          state_ = FLUSHING_PREPARE_LOG;
        }
        break;
      }
      case FLUSHING_PREPARE_LOG: {
        if (is_new_proposal_id_flushed_()) {
          CLOG_LOG(INFO,
              "new_proposal_id is flushed, reconfirm come into FETCH_MAX_LSN state",
              K_(partition_key),
              K_(new_proposal_id));
          state_ = FETCH_MAX_LSN;
          if (OB_SUCCESS != (tmp_ret = fetch_max_log_id_())) {
            CLOG_LOG(WARN, "fetch_max_log_id_ failed", K_(partition_key), K(tmp_ret));
          } else {
            fetch_max_log_id_ts_ = ObTimeUtility::current_time();
          }
        } else {
          CLOG_LOG(INFO,
              "new_proposal_id is not flushed, waiting retry",
              K_(partition_key),
              K_(new_proposal_id),
              K_(is_standby_reconfirm),
              "curr_proposal_id",
              state_mgr_->get_proposal_id(),
              "ms_proposal_id",
              mm_->get_ms_proposal_id());
        }
        if (state_ != FETCH_MAX_LSN) {
          break;
        }
      }
      case FETCH_MAX_LSN: {
        if (OB_SUCCESS != (ret = try_set_majority_ack_tag_of_max_log_id_())) {
          CLOG_LOG(WARN, "try_set_majority_ack_tag_of_max_flushed_id_ failed", K_(partition_key), K(ret));
        } else if (!max_log_ack_map_.test_map(MAJORITY_TAG_BIT)) {
          if (ObTimeUtility::current_time() - fetch_max_log_id_ts_ >= fetch_max_log_id_interval) {
            if (OB_FAIL(fetch_max_log_id_())) {
              CLOG_LOG(WARN, "fetch_max_log_id_ failed", K_(partition_key), K(ret));
            } else {
              fetch_max_log_id_ts_ = ObTimeUtility::current_time();
            }
          }
        } else if (!is_standby_reconfirm_ && OB_SUCCESS != (ret = prepare_log_map_())) {
          CLOG_LOG(WARN, "preapre_log_map failed", K_(partition_key), K(ret));
        } else if (!is_standby_reconfirm_ && OB_SUCCESS != (ret = sw_->try_update_max_log_id(max_flushed_id_))) {
          CLOG_LOG(WARN, "try_update_max_log_id failed", K_(partition_key), K(ret));
        } else {
          CLOG_LOG(INFO, "Reconfirm come into RECONFIRMING state", K_(partition_key));
          state_ = RECONFIRMING;
        }
        if (state_ != RECONFIRMING) {
          break;
        }
      }
      case RECONFIRMING: {
        if (OB_FAIL(sw_->submit_replay_task(need_async_replay, is_replayed, is_replay_failed)) &&
            OB_CLOG_SLIDE_TIMEOUT != ret) {
          CLOG_LOG(WARN, "submit_replay_task failed", K_(partition_key), K(ret));
        } else if (is_standby_reconfirm_) {
          if (OB_SUCCESS != (ret = mm_->write_start_membership(OB_LOG_RENEW_MEMBERSHIP))) {
            CLOG_LOG(WARN, "write_start_membership failed", K_(partition_key), K(ret));
          } else {
            state_ = START_WORKING;
          }
        } else {
          ret = OB_SUCCESS;
          new_start_id = sw_->get_start_id();
          if (new_start_id < start_id_) {
            ret = OB_EAGAIN;
            CLOG_LOG(INFO,
                "there are confirmed logs in sw, try again",
                K_(partition_key),
                K(ret),
                K(new_start_id),
                K_(start_id));
          } else if (max_flushed_id_ >= new_start_id) {
            ret = reconfirm_log_();
          } else if ((max_flushed_id_ + 1) == new_start_id) {
            CLOG_LOG(INFO,
                "Reconfirm come into START_WORKING state",
                K_(partition_key),
                K_(start_id),
                K_(max_flushed_id),
                K_(state),
                K(ret),
                K(new_start_id),
                "start_working log_id",
                max_flushed_id_ + 1);
            state_ = START_WORKING;
            if (OB_INVALID_TIMESTAMP != last_ts_ && OB_FAIL(sw_->try_update_submit_timestamp(last_ts_))) {
              CLOG_LOG(ERROR, "sw update timestamp error", K(ret), K(last_ts_));
            } else {
              ret = mm_->write_start_membership(OB_LOG_START_MEMBERSHIP);
            }
            // free memory
            clear_memory_();
          } else {
            ret = OB_ERR_UNEXPECTED;
          }
        }

        if (state_ != START_WORKING) {
          break;
        }
      }
      case START_WORKING: {
        if (is_standby_reconfirm_) {
          if (mm_->is_renew_ms_log_majority_success()) {
            state_ = FINISHED;
          } else {
            if (REACH_TIME_INTERVAL(100 * 1000)) {
              CLOG_LOG(INFO, "renew_ms_log not majority, need wait", K_(partition_key));
            }
            const int64_t now = ObTimeUtility::current_time();
            if (OB_INVALID_TIMESTAMP == last_push_renew_ms_log_ts_ ||
                now - last_push_renew_ms_log_ts_ > REPEATED_FETCH_LOG_INTERVAL) {
              // retry send renew_ms_log every 1s
              last_push_renew_ms_log_ts_ = now;
              (void)mm_->check_renew_ms_log_sync_state();
            }
          }
        } else {
          if (max_flushed_id_ + 2 == new_start_id) {
            CLOG_LOG(INFO,
                "start working log finished",
                K_(partition_key),
                K(new_start_id),
                "start_working log_id",
                max_flushed_id_ + 1);
            state_ = FINISHED;
          }
        }
        if (state_ != FINISHED) {
          break;
        }
      }
      case FINISHED: {
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
    }
    if (state_ == FINISHED) {
      CLOG_LOG(INFO,
          "reconfirm finished succ",
          K_(partition_key),
          K(ret),
          K_(state),
          K(new_start_id),
          K_(max_flushed_id),
          K_(is_standby_reconfirm),
          K_(max_membership_version));
      ret = OB_SUCCESS;
    } else if (OB_SUCCESS != ret && OB_EAGAIN != ret) {
      CLOG_LOG(ERROR,
          "reconfirm failed",
          K_(partition_key),
          K(ret),
          K_(state),
          K(new_start_id),
          K_(max_flushed_id),
          K_(is_standby_reconfirm),
          K_(max_membership_version));
    } else {
      if (REACH_TIME_INTERVAL(10 * 1000)) {
        CLOG_LOG(INFO,
            "reconfirm waiting retry",
            K_(partition_key),
            K(ret),
            K_(state),
            K(new_start_id),
            K_(max_flushed_id),
            K_(is_standby_reconfirm),
            K_(max_membership_version));
      }
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObLogReconfirm::receive_max_log_id(const ObAddr& server, const uint64_t log_id, const int64_t max_log_ts)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_INVALID_ID == log_id) {
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(server), K(log_id), K(max_log_ts));
    ret = OB_INVALID_ARGUMENT;
  } else if (state_ != FETCH_MAX_LSN) {
    CLOG_LOG(INFO, "receive_max_log_id in wrong state", K_(partition_key), K_(state), K(server), K(log_id));
    ret = OB_STATE_NOT_MATCH;
  } else if (max_log_ack_map_.test_map(MAJORITY_TAG_BIT)) {
    CLOG_LOG(INFO, "already majority", K_(partition_key));
  } else if (!curr_member_list_.contains(server)) {
    CLOG_LOG(WARN, "server is not in curr_member_list", K_(partition_key), K_(curr_member_list), K(server));
  } else if (OB_SUCCESS != (ret = max_log_ack_list_.add_server(server))) {
    CLOG_LOG(WARN, "max_log_ack_list add_server failed", K_(partition_key), K(server));
  } else {
    max_flushed_id_ = (max_flushed_id_ < log_id) ? log_id : max_flushed_id_;

    if (is_previous_leader_(server) && OB_INVALID_TIMESTAMP != max_log_ts) {
      receive_previous_max_log_ts_ = true;
      sw_->try_update_submit_timestamp(max_log_ts);
    }
  }
  CLOG_LOG(INFO,
      "receive_max_log_id",
      K_(partition_key),
      K(ret),
      K(server),
      K(log_id),
      K(max_log_ts),
      "prev_leader",
      state_mgr_->get_previous_leader(),
      K_(state),
      K_(max_flushed_id));
  return ret;
}

int ObLogReconfirm::handle_standby_prepare_resp(const ObAddr& server, const ObProposalID& proposal_id,
    const uint64_t ms_log_id, const int64_t membership_version, const ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(ret),
        K_(partition_key),
        K(server),
        K(proposal_id),
        K(membership_version),
        K(member_list));
  } else if (state_ != FETCH_MAX_LSN) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(INFO,
        "receive_max_log_id in wrong state",
        K(ret),
        K_(partition_key),
        K_(state),
        K(server),
        K(proposal_id),
        K(membership_version),
        K(member_list));
  } else if (max_log_ack_map_.test_map(MAJORITY_TAG_BIT)) {
    CLOG_LOG(INFO, "already majority", K_(partition_key));
  } else if (!curr_member_list_.contains(server)) {
    CLOG_LOG(WARN,
        "server is not in curr_member_list",
        K_(partition_key),
        K_(curr_member_list),
        K(server),
        K(membership_version),
        K(member_list));
  } else if (OB_SUCCESS != (ret = max_log_ack_list_.add_server(server))) {
    CLOG_LOG(WARN, "max_log_ack_list add_server failed", K_(partition_key), K(server));
  } else {
    if (OB_INVALID_TIMESTAMP == max_membership_version_ || max_membership_version_ < membership_version) {
      max_membership_version_ = membership_version;
    }

    if (is_previous_leader_(server)) {
      receive_previous_max_log_ts_ = true;
    }
  }
  CLOG_LOG(INFO,
      "handle_standby_prepare_resp finished",
      K_(partition_key),
      K(ret),
      K(server),
      K(proposal_id),
      K(ms_log_id),
      K(membership_version),
      "prev_leader",
      state_mgr_->get_previous_leader(),
      K_(state),
      K_(max_membership_version),
      K(member_list));
  return ret;
}

int ObLogReconfirm::try_update_failover_truncate_log_id_(ObLogEntryHeader* header)
{
  int ret = OB_SUCCESS;
  if (NULL == header) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), KP(header));
  } else if (GCTX.is_in_flashback_state() &&
             !share::ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
             OB_LOG_NOP == header->get_log_type() && !(header->get_proposal_id()).is_valid() &&
             (OB_INVALID_ID == failover_truncate_log_id_ || header->get_log_id() < failover_truncate_log_id_)) {
    // In the FLASHBACK state, leader encountered the NOP log at this time, and proposal_id is an invalid value
    // Explain that this log is a NOP generated by itself,
    // use this log_id to update the log_id that truncate log will use to ensure that it reaches the minimum value
    failover_truncate_log_id_ = header->get_log_id();
    CLOG_LOG(INFO,
        "update truncate log_id in cluster flashback state",
        K_(partition_key),
        K_(state),
        K_(failover_truncate_log_id));
  }
  return ret;
}

int ObLogReconfirm::receive_log(const ObLogEntry& log_entry, const ObAddr& server)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLogType& log_type = log_entry.get_header().get_log_type();
  ObLockGuard<ObSpinLock> guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t idx = 0;
    uint64_t log_id = log_entry.get_header().get_log_id();
    uint64_t new_start_id = sw_->get_start_id();
    const uint64_t range_start_id = log_info_array_.get_start_id();
    const uint64_t range_end_id = log_info_array_.get_end_id();
    if (START_WORKING == state_) {
      CLOG_LOG(INFO, "reveive log in START_WORKING state, ignore", K_(state), K_(partition_key), K(log_id));
    } else if (state_ != RECONFIRMING) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "receive log in wrong state", K(ret), K_(state), K_(partition_key), K(server), K(log_id));
    } else if (OB_LOG_RENEW_MEMBERSHIP == log_type) {
      CLOG_LOG(WARN,
          "receive renew_ms_log in reconfirm state, ignore",
          K(ret),
          K_(state),
          K_(partition_key),
          K(server),
          K(log_entry));
    } else {
      if (log_id < new_start_id || log_id < start_id_ || log_id < range_start_id || log_id > max_flushed_id_ ||
          log_id >= range_end_id) {
        CLOG_LOG(INFO,
            "receive log out of range",
            K_(partition_key),
            K(log_id),
            K(server),
            K(new_start_id),
            K_(start_id),
            K(range_start_id),
            K(range_end_id),
            K_(max_flushed_id));
      } else {
        idx = log_id - log_info_array_.get_start_id();
        ObReconfirmLogInfo* log_info = NULL;
        if (NULL == (log_info = log_info_array_.get_log_info(idx))) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "get_log_info failed", K_(partition_key), K(ret), K(idx));
        } else {
          if (log_info->test_map(CONFIRMED_TAG_BIT) || log_info->test_map(MAJORITY_TAG_BIT)) {
            CLOG_LOG(TRACE,
                "reconfirm receive log already majority",
                K_(partition_key),
                K(server),
                K(log_id),
                K_(state),
                K(new_start_id));
          } else if (!curr_member_list_.contains(server)) {
            CLOG_LOG(WARN, "server is not in the curr_member_list", K_(partition_key), K_(curr_member_list), K(server));
          } else if (OB_SUCCESS != log_info->add_server(server)) {
            CLOG_LOG(WARN, "reconfirm receive replicated log", K_(partition_key), K(log_id), K(server));
          } else {
            ObLogEntry* log_ptr = const_cast<ObLogEntry*>(&log_info->get_log_entry());
            if (log_entry.get_header().get_log_type() == OB_LOG_NOT_EXIST) {
              CLOG_LOG(TRACE, "reconfirm receive log not exist", K_(partition_key), K(log_id), K(server));
            } else {
              if (log_ptr->get_header().get_proposal_id() >= log_entry.get_header().get_proposal_id()) {
                CLOG_LOG(TRACE,
                    "reconfirm receive log with small proposal_id",
                    K_(partition_key),
                    K(log_id),
                    K(server),
                    "received_proposal_id",
                    log_entry.get_header().get_proposal_id(),
                    "local_proposal_id",
                    log_ptr->get_header().get_proposal_id());
              } else {
                // try to alloc new memory firstly
                CLOG_LOG(TRACE,
                    "reconfirm receive log with larger proposal_id, try replace",
                    K_(partition_key),
                    K(server),
                    K(log_id),
                    K_(state));
                char* prev_buff = const_cast<char*>(log_ptr->get_buf());
                if (OB_FAIL(log_entry.deep_copy_to(*log_ptr))) {
                  CLOG_LOG(ERROR, "copy log_entry failed", K_(partition_key), K(ret));
                  if (OB_SUCCESS != (tmp_ret = log_info->remove_server(server))) {
                    CLOG_LOG(ERROR, "remove_server failed", K_(partition_key), K(tmp_ret), K(server), K(log_info));
                  }
                } else if (prev_buff != NULL) {
                  TMA_MGR_INSTANCE.free_log_entry_buf(prev_buff);
                  prev_buff = NULL;
                }
              }
            }
            if (log_info->get_member_number() == majority_cnt_) {
              CLOG_LOG(TRACE, "reconfirm finished majority fetch one log", K_(partition_key), K(log_id));
              ObLogEntryHeader* header = const_cast<ObLogEntryHeader*>(&log_ptr->get_header());
              // check if it need generate truncate log during standby failover
              if (OB_FAIL(try_update_failover_truncate_log_id_(header))) {
                CLOG_LOG(WARN, "try_update_failover_truncate_log_id_ failed", K(ret), K_(partition_key), K(log_id));
              }
              header->set_proposal_id(state_mgr_->get_proposal_id());
              header->update_header_checksum();  // update header_checksum
              log_info->set_map(MAJORITY_TAG_BIT);
            }
          }
        }
      }
    }
  }
  CLOG_LOG(TRACE, "receive log", K_(partition_key), K(ret), K(server), K_(state), "log_header", log_entry.get_header());
  return ret;
}

int ObLogReconfirm::generate_nop_log_(const uint64_t log_id, const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObNopLog nop_log;
  char body_buffer[BUF_SIZE];
  int64_t body_pos = 0;
  ObReconfirmLogInfo* log_info = NULL;
  if (NULL == (log_info = log_info_array_.get_log_info(idx))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_log_info failed", K_(partition_key), K(ret), K(idx));
  } else if (OB_SUCCESS != (ret = nop_log.serialize(body_buffer, BUF_SIZE, body_pos))) {
    CLOG_LOG(WARN, "serialize failed", K_(partition_key), K(ret));
  } else {
    ObLogEntryHeader header;
    ObLogEntry log_entry;
    // set proposal_id to 0, ensure that if there is a valid log, it will be rewrited
    // set epoch_id_ to 0, it will be filtered out later
    ObProposalID invalid_proposal_id;
    const bool is_trans_log = false;
    if (OB_FAIL(header.generate_header(OB_LOG_NOP,  // log_type
            partition_key_,                         // partition_key
            log_id,
            body_buffer,                    // buff
            body_pos,                       // data_len
            ObTimeUtility::current_time(),  // generation_timestamp
            leader_ts_,                     // epoch_id
            invalid_proposal_id,            // invalid proposal_id
            0,                              // submit_timestamp
            state_mgr_->get_freeze_version(),
            is_trans_log))) {
      CLOG_LOG(ERROR, "generate_header failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(log_entry.generate_entry(header, body_buffer))) {
      CLOG_LOG(ERROR, "generate_entry failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(log_info->deep_copy_log(log_entry))) {
      CLOG_LOG(WARN, "copy log_entry failed", K_(partition_key), K(ret));
    } else {
      log_info->set_leader_ts(0);
    }
  }
  return ret;
}

void ObLogReconfirm::try_update_nop_freeze_version_(ObLogEntryHeader& header)
{
  if (OB_LOG_NOP == header.get_log_type()) {
    header.update_freeze_version(state_mgr_->get_freeze_version());
    header.update_header_checksum();
  }
}

void ObLogReconfirm::try_update_membership_status_(const ObLogEntry& log_entry)
{
  if (OB_LOG_MEMBERSHIP == log_entry.get_header().get_log_type()) {
    mm_->reconfirm_update_status(log_entry);
  }
}

void ObLogReconfirm::clear_memory_()
{
  log_info_array_.destroy();
}

bool ObLogReconfirm::is_previous_leader_(const common::ObAddr& server) const
{
  return server == state_mgr_->get_previous_leader();
}
}  // namespace clog
}  // namespace oceanbase
