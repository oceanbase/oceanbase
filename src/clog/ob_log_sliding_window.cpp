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

#include "ob_log_sliding_window.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/ob_replica_define.h"
#include "common/ob_trace_profile.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_bg_thread_monitor.h"
#include "storage/ob_storage_log_type.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_freeze_info_snapshot_mgr.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "storage/transaction/ob_trans_ctx.h"
#include "storage/ob_partition_service.h"
#include "ob_i_log_engine.h"
#include "ob_log_replay_engine_wrapper.h"
#include "ob_log_callback_engine.h"
#include "ob_log_checksum_V2.h"
#include "ob_log_flush_task.h"
#include "ob_log_membership_mgr_V2.h"
#include "ob_log_restore_mgr.h"
#include "ob_log_state_mgr.h"
#include "ob_log_task.h"
#include "ob_clog_mgr.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace transaction;
namespace clog {
// Total size of logs to be replayed during restart
static int64_t pending_scan_confirmed_log_size = 0;
ObLogSlidingWindow::ObLogSlidingWindow()
    : tenant_id_(OB_INVALID_TENANT_ID),
      state_mgr_(NULL),
      replay_engine_(NULL),
      log_engine_(NULL),
      mm_(NULL),
      cascading_mgr_(NULL),
      partition_service_(NULL),
      alloc_mgr_(NULL),
      checksum_(NULL),
      cb_engine_(NULL),
      restore_mgr_(NULL),
      sw_(),
      self_(),
      partition_key_(),
      max_log_meta_info_(),
      next_replay_log_id_info_(),
      max_majority_log_(),
      leader_ts_(OB_INVALID_TIMESTAMP),
      saved_accum_checksum_(0),
      next_index_log_id_(OB_INVALID_ID),
      scan_next_index_log_id_(OB_INVALID_ID),
      last_flushed_log_id_(0),
      next_index_log_ts_(OB_INVALID_TIMESTAMP),
      switchover_info_lock_(common::ObLatchIds::CLOG_SWITCH_INFO_LOCK),
      leader_max_log_info_(),
      last_replay_log_(),
      fake_ack_info_mgr_(),
      last_slide_fid_(OB_INVALID_FILE_ID),
      check_can_receive_larger_log_warn_time_(OB_INVALID_TIMESTAMP),
      insert_log_try_again_warn_time_(OB_INVALID_TIMESTAMP),
      receive_confirmed_info_warn_time_(OB_INVALID_TIMESTAMP),
      get_end_log_id_warn_time_(OB_INVALID_TIMESTAMP),
      fetch_log_warn_time_(OB_INVALID_TIMESTAMP),
      update_log_task_log_time_(OB_INVALID_TIMESTAMP),
      sync_replica_reset_fetch_state_time_(OB_INVALID_TIMESTAMP),
      last_update_next_replay_log_id_info_ts_(OB_INVALID_TIMESTAMP),
      last_get_tenant_config_time_(OB_INVALID_TIMESTAMP),
      has_pop_task_(false),
      aggre_buffer_(NULL),
      aggre_buffer_cnt_(0),
      next_submit_aggre_buffer_(0),
      aggre_buffer_start_id_(0),
      leader_max_unconfirmed_log_cnt_(DEFAULT_LEADER_MAX_UNCONFIRMED_LOG_COUNT),
      last_archive_checkpoint_log_id_(OB_INVALID_ID),
      last_archive_checkpoint_ts_(0),
      last_update_archive_checkpoint_time_(OB_INVALID_TIMESTAMP),
      aggre_worker_(NULL),
      is_inited_(false)
{}

int ObLogSlidingWindow::init(ObLogReplayEngineWrapper* replay_engine, ObILogEngine* log_engine,
    ObILogStateMgrForSW* state_mgr, ObILogMembershipMgr* mm, ObLogCascadingMgr* cascading_mgr,
    storage::ObPartitionService* partition_service, common::ObILogAllocator* alloc_mgr, ObILogChecksum* checksum,
    ObILogCallbackEngine* cb_engine, ObLogRestoreMgr* restore_mgr, const common::ObAddr& self,
    const common::ObPartitionKey& key, const int64_t epoch_id, const uint64_t last_replay_log_id,
    const int64_t last_submit_ts, const int64_t accum_checksum)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = key.get_tenant_id();
  const bool is_pg = key.is_pg();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(replay_engine) || OB_ISNULL(log_engine) || OB_ISNULL(restore_mgr) || OB_ISNULL(state_mgr) ||
             OB_ISNULL(mm) || OB_ISNULL(cascading_mgr) || OB_ISNULL(partition_service) || OB_ISNULL(alloc_mgr) ||
             OB_ISNULL(checksum) || OB_ISNULL(cb_engine) || !self.is_valid() || !key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(sw_.init(last_replay_log_id + 1))) {
    CLOG_LOG(WARN, "sw init failed", "partition_key", key, K(ret), K(last_replay_log_id));
  } else if (OB_FAIL(max_log_meta_info_.init(key, last_replay_log_id, last_submit_ts))) {
    CLOG_LOG(
        WARN, "max_log_meta_info_ init failed", "partition_key", key, K(ret), K(last_replay_log_id), K(last_submit_ts));
  } else if (REPLICA_TYPE_LOGONLY != mm->get_replica_type() && !is_inner_table(key.get_table_id()) &&
             OB_FAIL(init_aggre_buffer_(last_replay_log_id + 1, tenant_id, key.is_pg()))) {
    CLOG_LOG(WARN, "init aggre buffer failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    state_mgr_ = state_mgr;
    replay_engine_ = replay_engine;
    log_engine_ = log_engine;
    mm_ = mm;
    cascading_mgr_ = cascading_mgr;
    partition_service_ = partition_service;
    alloc_mgr_ = alloc_mgr;
    checksum_ = checksum;
    cb_engine_ = cb_engine;
    restore_mgr_ = restore_mgr;
    self_ = self;
    partition_key_ = key;
    max_majority_log_.set(last_replay_log_id, last_submit_ts);
    leader_ts_ = epoch_id;
    saved_accum_checksum_ = accum_checksum;
    next_index_log_id_ = last_replay_log_id + 1;
    next_index_log_ts_ = last_submit_ts;
    last_replay_log_.set(last_replay_log_id, last_submit_ts);
    set_next_replay_log_id_info(last_replay_log_id + 1, last_submit_ts + 1);
    last_slide_fid_ = 0;
    aggre_worker_ = partition_service->get_clog_aggre_runnable();
    is_inited_ = true;
    CLOG_LOG(INFO,
        "ObSlidingWindow init",
        K(tenant_id),
        K_(partition_key),
        K(last_replay_log_id),
        K(next_index_log_id_),
        K(last_submit_ts),
        K_(aggre_buffer_cnt),
        K(is_pg));
  }
  if (OB_SUCCESS != ret && !is_inited_) {
    destroy();
  }
  return ret;
}

int ObLogSlidingWindow::init_aggre_buffer_(const uint64_t start_id, const uint64_t tenant_id, const bool is_pg)
{
  int ret = OB_SUCCESS;
  int64_t aggre_buffer_cnt = 0;
  ObMemAttr mem_attr(tenant_id, ObModIds::OB_LOG_AGGRE_BUFFER_ARRAY);
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    CLOG_LOG(WARN, "get tenant config failed", K(start_id), K(tenant_id), K(is_pg));
  } else {
    aggre_buffer_cnt = tenant_config->_clog_aggregation_buffer_amount;
  }
  if (0 == aggre_buffer_cnt && is_pg) {
    // always use aggre_buffer for partition group
    aggre_buffer_cnt = 4;
  }
  if (0 < aggre_buffer_cnt) {
    if (NULL == (aggre_buffer_ = (ObAggreBuffer*)ob_malloc(aggre_buffer_cnt * sizeof(ObAggreBuffer), mem_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "alloc memory failed", K(ret), K(start_id));
    } else {
      aggre_buffer_cnt_ = aggre_buffer_cnt;
      next_submit_aggre_buffer_ = start_id;
      aggre_buffer_start_id_ = start_id;
      for (int64_t i = 0; i < aggre_buffer_cnt_; i++) {
        new (aggre_buffer_ + i) ObAggreBuffer();
      }
      for (int64_t i = 0; (OB_SUCCESS == ret) && i < aggre_buffer_cnt_; i++) {
        if (OB_FAIL(aggre_buffer_[i].init(aggre_buffer_start_id_ + i, tenant_id))) {
          CLOG_LOG(ERROR, "aggre_buffer_ init failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      CLOG_LOG(INFO, "init aggre buffer", K(tenant_id), K(is_pg));
    }
  }
  return ret;
}

void ObLogSlidingWindow::destroy_aggre_buffer()
{
  const uint64_t tenant_id = partition_key_.get_tenant_id();
  if (NULL != aggre_buffer_) {
    for (int64_t i = 0; i < aggre_buffer_cnt_; i++) {
      aggre_buffer_[i].~ObAggreBuffer();
    }
    ob_free(aggre_buffer_);
    CLOG_LOG(INFO, "release aggre buffer", K(tenant_id), K(partition_key_), KP(aggre_buffer_));
    aggre_buffer_ = NULL;
    aggre_buffer_cnt_ = 0;
  }
}

int ObLogSlidingWindow::leader_takeover()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else {
    uint64_t max_log_id = OB_INVALID_ID;
    int64_t max_log_ts = OB_INVALID_TIMESTAMP;
    get_max_log_id_info(max_log_id, max_log_ts);
    try_update_max_majority_log(max_log_id, max_log_ts);
  }
  return ret;
}

int ObLogSlidingWindow::leader_active()
{
  int ret = OB_SUCCESS;
  const uint64_t max_log_id = get_max_log_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (NULL != aggre_buffer_) {
    next_submit_aggre_buffer_ = max_log_id + 1;
    aggre_buffer_start_id_ = max_log_id + 1;
    for (int64_t i = 0; i < aggre_buffer_cnt_; i++) {
      aggre_buffer_[i].reuse(aggre_buffer_start_id_ + i);
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t now = ObClockGenerator::getClock();
    last_archive_checkpoint_log_id_ = max_log_id;
    last_archive_checkpoint_ts_ = 0;
    last_update_archive_checkpoint_time_ = now;

    ATOMIC_STORE(&last_flushed_log_id_, 0);
  }
  return ret;
}

int ObLogSlidingWindow::leader_revoke()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogSlidingWindow is not inited", K(ret), K(partition_key_));
  } else if (NULL != aggre_buffer_) {
    const uint64_t max_log_id = get_max_log_id();
    if (OB_SUCCESS != (tmp_ret = try_freeze_aggre_buffer_(max_log_id))) {
      CLOG_LOG(ERROR, "try_freeze_aggre_buffer_ failed", K(ret), K(partition_key_), K(max_log_id));
    }
  }
  return ret;
}

uint64_t ObLogSlidingWindow::get_start_id() const
{
  return static_cast<uint64_t>(sw_.get_start_id());
}

uint64_t ObLogSlidingWindow::get_max_log_id() const
{
  return max_log_meta_info_.get_log_id();
}

uint64_t ObLogSlidingWindow::get_max_timestamp() const
{
  return max_log_meta_info_.get_timestamp();
}

void ObLogSlidingWindow::get_max_log_id_info(uint64_t& max_log_id, int64_t& max_log_ts) const
{
  (void)max_log_meta_info_.get_log_id_and_timestamp(max_log_id, max_log_ts);
}

void ObLogSlidingWindow::get_next_replay_log_id_info(uint64_t& next_log_id, int64_t& next_log_ts) const
{
  struct types::uint128_t next_log_id_info;
  LOAD128(next_log_id_info, &next_replay_log_id_info_);
  next_log_id = next_log_id_info.hi;
  next_log_ts = next_log_id_info.lo;
}

int64_t ObLogSlidingWindow::get_epoch_id() const
{
  return ATOMIC_LOAD(&leader_ts_);
}

int ObLogSlidingWindow::try_update_max_log_id(const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = max_log_meta_info_.try_update_log_id(log_id);
  }
  return ret;
}

int ObLogSlidingWindow::send_log_to_standby_cluster_(const uint64_t log_id, ObLogTask* log_task)
{
  // send log to standby cluster, caller guarantees this log has reached majority in local cluster.
  // caller need hold lock for log_task
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == log_task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObReadBuf rbuf;
    ObLogEntry tmp_entry;
    if (log_task->is_submit_log_body_exist()) {
      const char* log_buf = log_task->get_log_buf();
      int64_t log_buf_len = log_task->get_log_buf_len();

      ObLogEntryHeader header;
      ObVersion dummy_version(2, 0);
      if (OB_FAIL(header.generate_header(log_task->get_log_type(),
              partition_key_,
              log_id,
              log_buf,
              log_buf_len,
              log_task->get_generation_timestamp(),
              log_task->get_epoch_id(),
              log_task->get_proposal_id(),
              log_task->get_submit_timestamp(),
              dummy_version,
              log_task->is_trans_log()))) {
        CLOG_LOG(ERROR, "generate_header failed", K(ret), K(partition_key_), K(log_id), K(*log_task));
      } else if (OB_FAIL(tmp_entry.generate_entry(header, log_buf))) {
        CLOG_LOG(ERROR, "generate_entry failed", K_(partition_key), K(ret));
      } else {
      }
    } else {
      if (OB_FAIL(ObILogDirectReader::alloc_buf(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID, rbuf))) {
        CLOG_LOG(WARN, "failed to alloc read_buf", K_(partition_key), K(log_id), K(*log_task), K(ret));
      } else if (OB_FAIL(log_engine_->read_log_by_location(*log_task, rbuf, tmp_entry))) {
        CLOG_LOG(WARN, "read_log_by_location failed", K_(partition_key), K(ret), K(*log_task));
      } else if (tmp_entry.get_header().get_partition_key() != partition_key_) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "read_log_by_location wrong log", K_(partition_key), K(ret), K(*log_task), K(tmp_entry));
      } else {
        // do nothing
      }
    }

    int64_t pos = 0;
    char* serialize_buff = NULL;
    int64_t serialize_size = tmp_entry.get_serialize_size();
    if (OB_SUCC(ret)) {
      if (NULL == (serialize_buff = static_cast<char*>(alloc_mgr_->ge_alloc(serialize_size)))) {
        CLOG_LOG(ERROR, "alloc failed", K_(partition_key));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_SUCCESS != (ret = tmp_entry.serialize(serialize_buff, serialize_size, pos))) {
        CLOG_LOG(WARN, "submit_log serialize failed", K_(partition_key), K(ret));
      } else if (OB_FAIL(
                     submit_log_to_standby_children_(tmp_entry.get_header(), serialize_buff, serialize_size, true))) {
        CLOG_LOG(WARN, "submit_log_to_standby_children_ failed", K(ret), K_(partition_key), K(log_id));
      } else {
        CLOG_LOG(TRACE, "submit_log_to_standby_children_ succ", K(ret), K_(partition_key), K(log_id));
      }
    }

    if (NULL != serialize_buff) {
      alloc_mgr_->ge_free(serialize_buff);
      serialize_buff = NULL;
    }
    (void)ObILogDirectReader::free_buf(rbuf);
  }
  return ret;
}

int ObLogSlidingWindow::send_confirmed_info_to_standby_children_(const uint64_t log_id, ObLogTask* log_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_task)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!log_task->is_log_confirmed()) {
    // not confirmed, skip
  } else {
    ObConfirmedInfo confirmed_info;
    const int64_t data_checksum = log_task->get_data_checksum();
    const int64_t accum_checksum = log_task->get_accum_checksum();
    const int64_t epoch_id = log_task->get_epoch_id();
    const bool batch_committed = log_task->is_batch_committed();
    if (OB_FAIL(confirmed_info.init(data_checksum, epoch_id, accum_checksum))) {
      CLOG_LOG(ERROR, "confirmed_info init failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(submit_confirmed_info_to_standby_children_(log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN,
          "submit_confirmed_info_to_standby_children_ failed",
          K_(partition_key),
          K(ret),
          K(log_id),
          K(confirmed_info),
          K(batch_committed));
    } else {
      CLOG_LOG(TRACE,
          "submit_confirmed_info_to_standby_children_",
          K_(partition_key),
          K(ret),
          K(log_id),
          K(confirmed_info),
          K(batch_committed));
    }
  }
  return ret;
}

bool ObLogSlidingWindow::is_primary_need_send_log_to_standby_(ObLogTask* log_task) const
{
  // whether primary replica can transfer log to standby children
  bool bool_ret = false;
  if (NULL != log_task) {
    if (GCTX.is_primary_cluster() && cascading_mgr_->has_valid_async_standby_child() &&
        (log_task->is_local_majority_flushed() || log_task->is_log_confirmed())) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

bool ObLogSlidingWindow::is_mp_leader_waiting_standby_ack_(ObLogTask* log_task) const
{
  // whether primary leader has not received standby ack in max protection mode
  bool bool_ret = false;
  if (NULL != log_task) {
    if (GCTX.need_sync_to_standby() && LEADER == state_mgr_->get_role() &&
        !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
        !log_task->is_standby_majority_finished()) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

bool ObLogSlidingWindow::is_follower_need_send_log_to_standby_(ObLogTask* log_task) const
{
  // whether follower can transfer log to standby children
  // it must guarantee this log has been confirmed
  // standby follower no need wait confirmed actually
  // temporarily handle same as primary follower
  bool bool_ret = false;
  if (NULL != log_task) {
    if (FOLLOWER == state_mgr_->get_role() && cascading_mgr_->has_valid_async_standby_child() &&
        log_task->is_submit_log_exist() && log_task->is_flush_local_finished() && log_task->is_log_confirmed()) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObLogSlidingWindow::ack_log(const uint64_t log_id, const ObAddr& server, bool& majority)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;
  bool need_send_to_standby = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_ISNULL(mm_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ERROR_OUT_OF_RANGE == (ret = sw_.get(static_cast<int64_t>(log_id), log_data, ref))) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "ack log: get log from sliding window failed", K_(partition_key), K(log_id), K(ret));
  } else if (NULL == (log_task = static_cast<ObLogTask*>(log_data))) {
    ret = OB_ERR_NULL_VALUE;
    CLOG_LOG(WARN, "ack log: get null log", K_(partition_key), K(log_id));
  } else if (!log_task->is_submit_log_exist()) {
    ret = OB_ERR_NULL_VALUE;
    CLOG_LOG(WARN, "ack log: get null log", K_(partition_key), K(log_id));
  } else if (true == mm_->get_curr_member_list().contains(server) && self_ != server) {
    log_task->lock();
    if (OB_FAIL(log_task->ack_log(server))) {
      CLOG_LOG(WARN, "log_task->ack_log failed", K_(partition_key), K(ret), K(log_id), K(server));
    } else if (is_mp_leader_waiting_standby_ack_(log_task)) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(INFO, "not receive standby_ack, need wait", K_(partition_key), K(log_id), K(*log_task));
      }
      if (log_task->is_local_majority_flushed()) {
        need_send_to_standby = true;
      }
    } else {
      majority = log_task->try_set_majority_finished();
    }
    if (!need_send_to_standby && is_primary_need_send_log_to_standby_(log_task)) {
      need_send_to_standby = true;
    }
    if (need_send_to_standby && !log_task->is_already_send_to_standby()) {
      if (OB_SUCCESS != (tmp_ret = send_log_to_standby_cluster_(log_id, log_task))) {
        CLOG_LOG(WARN, "send_log_to_standby_cluster_ failed", K(tmp_ret), K_(partition_key), K(log_id));
      } else {
        // Mark as sent to avoid resending
        log_task->set_already_send_to_standby();
        CLOG_LOG(TRACE, "set_already_send_to_standby", K_(partition_key), K(log_id));
      }
    }

    log_task->unlock();
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
  } else {
    ref = NULL;
  }
  return ret;
}

int ObLogSlidingWindow::standby_ack_log(const uint64_t log_id, const ObAddr& server, bool& majority)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_ISNULL(mm_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ERROR_OUT_OF_RANGE == (ret = sw_.get(static_cast<int64_t>(log_id), log_data, ref))) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "ack log: get log from sliding window failed", K_(partition_key), K(log_id), K(ret));
  } else if (NULL == (log_task = static_cast<ObLogTask*>(log_data))) {
    ret = OB_ERR_NULL_VALUE;
    CLOG_LOG(WARN, "ack log: get null log", K_(partition_key), K(log_id));
  } else if (!log_task->is_submit_log_exist()) {
    ret = OB_ERR_NULL_VALUE;
    CLOG_LOG(WARN, "ack log: get null log", K_(partition_key), K(log_id));
  } else if (server == cascading_mgr_->get_sync_standby_child().get_server()) {
    log_task->lock();
    log_task->set_standby_majority_finished();
    majority = log_task->try_set_majority_finished();
    log_task->unlock();
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
  } else {
    ref = NULL;
  }
  return ret;
}

int ObLogSlidingWindow::fake_ack_log(const uint64_t log_id, const common::ObAddr& server, const int64_t receive_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_INVALID_ID == log_id || OB_INVALID_TIMESTAMP == receive_ts) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argumetns", K(ret), K(partition_key_), K(log_id), K(server), K(receive_ts));
  } else if (log_id != sw_.get_start_id()) {
    // log_id not match with current start_id, ignore
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(
          WARN, "log_id not match", K(ret), K(partition_key_), K(log_id), K(server), "start_id", sw_.get_start_id());
    }
  } else {
    (void)fake_ack_info_mgr_.process_fake_ack_req(log_id, server, receive_ts);
    CLOG_LOG(DEBUG, "process_fake_ack_req finished", K(partition_key_), K(log_id), K(server), K(receive_ts));
  }

  return ret;
}

bool ObLogSlidingWindow::is_fake_info_need_revoke(const uint64_t log_id, const int64_t current_time)
{
  bool bool_ret = false;

  if (IS_NOT_INIT) {
    bool_ret = true;
    CLOG_LOG(WARN, "sw not inited", K(partition_key_));
  } else if (OB_INVALID_ID == log_id || OB_INVALID_TIMESTAMP == current_time) {
    bool_ret = true;
    CLOG_LOG(WARN, "invalid argumetns", K(partition_key_), K(log_id), K(current_time));
  } else if (fake_ack_info_mgr_.get_log_id() != log_id) {
    // log_id not match, advise to revoke
    bool_ret = true;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(WARN,
          "log_id not match, need revoke",
          K(partition_key_),
          K(log_id),
          "fake log_id",
          fake_ack_info_mgr_.get_log_id());
    }
  } else {
    const common::ObMemberList& member_list = mm_->get_curr_member_list();
    const int64_t replica_num = mm_->get_replica_num();
    bool_ret = fake_ack_info_mgr_.check_fake_info_need_revoke(log_id, current_time, self_, member_list, replica_num);
    if (bool_ret && REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(WARN, "check_fake_info_need_revoke return true", K(partition_key_), K(log_id));
    }
  }

  return bool_ret;
}

// when base_timestamp !=0, it indicates that this log is transaction log.
int ObLogSlidingWindow::alloc_log_id_ts_(const int64_t base_timestamp, uint64_t& log_id, int64_t& submit_timestamp)
{
  int ret = OB_SUCCESS;
  int64_t wait_times = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    const uint64_t log_id_limit = sw_.get_start_id() + get_leader_max_unconfirmed_log_count();
    int64_t tmp_offset = 0;
    ret = max_log_meta_info_.alloc_log_id_ts(base_timestamp, log_id_limit, log_id, submit_timestamp, tmp_offset);
    if (OB_SUCCESS == ret && NULL != aggre_buffer_ && state_mgr_->is_leader_active()) {
      if (log_id < aggre_buffer_start_id_) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "alloc invalue log_id", K(log_id), K(aggre_buffer_start_id_));
      } else {
        if (AGGRE_BUFFER_FLAG != tmp_offset) {
          ret = fill_aggre_buffer_(log_id - 1, tmp_offset, NULL, 0, 0, NULL);
        }
        ObAggreBuffer* buffer = aggre_buffer_ + ((log_id - aggre_buffer_start_id_) % aggre_buffer_cnt_);
        buffer->wait(log_id, wait_times);
        if (wait_times > 0 && EXECUTE_COUNT_PER_SEC(4)) {
          CLOG_LOG(WARN, "clog wait aggre buffer", K_(partition_key), K(wait_times), K(log_id));
        }
        buffer->reuse(log_id + aggre_buffer_cnt_);
      }
    }
  }
  return ret;
}

int ObLogSlidingWindow::alloc_log_id_ts_(
    const int64_t base_timestamp, const int64_t size, uint64_t& log_id, int64_t& submit_timestamp, int64_t& offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    const uint64_t log_id_limit = sw_.get_start_id() + get_leader_max_unconfirmed_log_count();
    ret = max_log_meta_info_.alloc_log_id_ts(base_timestamp, size, log_id_limit, log_id, submit_timestamp, offset);
  }
  return ret;
}

int ObLogSlidingWindow::submit_aggre_log_(ObAggreBuffer* buffer, const uint64_t log_id, const int64_t submit_timestamp)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogSlidingWindow is not inited", K(ret), K(partition_key_));
  } else {
    ObLogEntryHeader header;
    bool need_replay = false;
    bool send_slave = true;
    ObAddr dummy_server;
    const int64_t dummy_cluster_id = state_mgr_->get_self_cluster_id();
    ObVersion dummy_version(2, 0);
    const bool is_trans_log = true;
    const bool is_confirmed = false;
    const int64_t unused_accum_checksum = 0;
    const bool unused_is_batch_committed = false;
    if (OB_FAIL(header.generate_header(OB_LOG_AGGRE,
            partition_key_,
            log_id,
            buffer->get_data(),
            buffer->get_data_size(),
            ObTimeUtility::current_time(),
            state_mgr_->get_proposal_id().ts_,
            state_mgr_->get_proposal_id(),
            submit_timestamp,
            dummy_version,
            is_trans_log))) {
      CLOG_LOG(ERROR, "generate_header failed", K(ret), K(partition_key_), K(log_id));
    } else if (OB_FAIL(submit_to_sliding_window_(header,
                   buffer->get_data(),
                   buffer->get_submit_cb(),
                   need_replay,
                   send_slave,
                   dummy_server,
                   dummy_cluster_id,
                   is_confirmed,
                   unused_accum_checksum,
                   unused_is_batch_committed))) {
      CLOG_LOG(ERROR, "submit_to_sliding_window_ failed", K(ret), K(partition_key_), K(log_id));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObLogSlidingWindow::fill_aggre_buffer_(const uint64_t log_id, const int64_t offset, const char* data,
    const int64_t data_size, const int64_t submit_timestamp, ObISubmitLogCb* cb)
{
  int ret = OB_SUCCESS;
  int64_t wait_times = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogSlidingWindow is not inited", K(ret), K(partition_key_));
  } else {
    int64_t ref_cnt = 0;
    ObAggreBuffer* buffer = aggre_buffer_ + ((log_id - aggre_buffer_start_id_) % aggre_buffer_cnt_);
    buffer->wait(log_id, wait_times);
    if (wait_times > 0 && EXECUTE_COUNT_PER_SEC(4)) {
      CLOG_LOG(WARN, "clog wait aggre buffer", K_(partition_key), K(wait_times), K(log_id));
    }
    if (NULL == data) {
      ref_cnt = buffer->ref(-offset);
    } else {
      buffer->fill(offset, data, data_size, submit_timestamp, cb);
      ref_cnt = buffer->ref(data_size + AGGRE_LOG_RESERVED_SIZE);
    }
    if (0 == ref_cnt) {
      if (OB_FAIL(submit_aggre_log_(buffer, log_id, buffer->get_max_submit_timestamp()))) {
        CLOG_LOG(WARN, "submit aggre log failed", K(ret), K(partition_key_), K(log_id));
      }
      if (REACH_TIME_INTERVAL(3000000)) {
        CLOG_LOG(INFO, "fill aggre log", "filling_rate_pct", (buffer->get_data_size() * 100) / AGGRE_BUFFER_SIZE);
      }
      buffer->reuse(log_id + aggre_buffer_cnt_);
    }
  }
  return ret;
}

int ObLogSlidingWindow::try_freeze_aggre_buffer_(const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  int64_t tmp_offset = 0;
  if (OB_BLOCK_SWITCHED == (ret = max_log_meta_info_.try_freeze(log_id, tmp_offset))) {
    if (OB_FAIL(fill_aggre_buffer_(log_id, tmp_offset, NULL, 0, 0, NULL))) {
      CLOG_LOG(ERROR, "fill_aggre_buffer_ failed", K(ret), K(partition_key_), K(log_id));
    }
  }
  return ret;
}

int ObLogSlidingWindow::submit_freeze_aggre_buffer_task_(const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  int64_t flush_interval = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (tenant_config.is_valid()) {
    flush_interval = tenant_config->_flush_clog_aggregation_buffer_timeout;
  }
  if (flush_interval <= 0) {
    inc_update(next_submit_aggre_buffer_, log_id + 1);
    if (OB_FAIL(try_freeze_aggre_buffer_(log_id + 1))) {
      CLOG_LOG(WARN, "try freeze aggre buffer failed", K(ret), K(partition_key_));
      // rewrite ret
      ret = OB_SUCCESS;
    }
  } else {
    uint64_t tmp_log_id = UINT64_MAX;
    while (true) {
      tmp_log_id = ATOMIC_LOAD(&last_flushed_log_id_);
      if (log_id <= tmp_log_id) {
        break;
      } else {
        if (ATOMIC_BCAS(&last_flushed_log_id_, tmp_log_id, log_id)) {
          CLOG_LOG(DEBUG, "flushed log id changed", K(partition_key_), K(tmp_log_id), K(log_id));
          break;
        }
      }
    }
    if (0 == tmp_log_id) {
      if (OB_FAIL(aggre_worker_->add_task(partition_key_, flush_interval))) {
        if (EXECUTE_COUNT_PER_SEC(8)) {
          CLOG_LOG(WARN, "add aggre task failed", K(ret), K(partition_key_), K(log_id));
        }
        // rewrite ret
        ret = OB_SUCCESS;
        while (true) {
          tmp_log_id = ATOMIC_LOAD(&last_flushed_log_id_);
          if (ATOMIC_BCAS(&last_flushed_log_id_, tmp_log_id, 0)) {
            CLOG_LOG(DEBUG, "flushed log id changed", K(partition_key_), K(tmp_log_id), "log_id", 0);
            break;
          }
        }
        inc_update(next_submit_aggre_buffer_, tmp_log_id + 1);
        if (OB_FAIL(try_freeze_aggre_buffer_(tmp_log_id + 1))) {
          CLOG_LOG(ERROR, "try_freeze_aggre_buffer_ failed", K(ret), K(partition_key_));
        } else {
          CLOG_LOG(DEBUG, "try freeze aggre buffer local", K(partition_key_), "log_id", tmp_log_id + 1);
        }
      } else {
        CLOG_LOG(DEBUG, "add aggre task success", K(partition_key_), K(log_id));
      }
    }
  }
  return ret;
}

int ObLogSlidingWindow::alloc_log_id(const int64_t base_timestamp, uint64_t& log_id, int64_t& submit_timestamp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (base_timestamp < 0) {
    TRANS_LOG(WARN, "invalid prepare version", K(base_timestamp));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = alloc_log_id_ts_(base_timestamp, log_id, submit_timestamp);
    if (OB_EAGAIN == ret && REACH_TIME_INTERVAL(1000 * 1000)) {
      const uint64_t start_log_id = sw_.get_start_id();
      state_mgr_->report_start_id_trace(start_log_id);
      CLOG_LOG(WARN,
          "alloc log id error, please attention!!!",
          K_(partition_key),
          K(base_timestamp),
          K(start_log_id),
          K(log_id));
    }
  }
  return ret;
}

int ObLogSlidingWindow::try_update_submit_timestamp(const int64_t base_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TIMESTAMP == base_ts) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "update timestamp error", K_(partition_key), K(ret), K(base_ts));
  } else {
    ret = max_log_meta_info_.try_update_timestamp(base_ts);
  }
  return ret;
}

// only called by ObExtLeaderHeartbeatHandler, it will double check leader
int ObLogSlidingWindow::get_next_timestamp(const uint64_t last_log_id, int64_t& res_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t last_log_id_dummy = OB_INVALID_ID;
  uint64_t next_log_id_dummy = OB_INVALID_ID;
  int64_t last_log_ts = OB_INVALID_TIMESTAMP;
  int64_t next_log_ts = OB_INVALID_TIMESTAMP;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    res_ts = OB_INVALID_TIMESTAMP;
    get_last_replay_log(last_log_id_dummy, last_log_ts);
    get_next_replay_log_id_info(next_log_id_dummy, next_log_ts);
    if (OB_INVALID_TIMESTAMP == last_log_ts || OB_INVALID_TIMESTAMP == next_log_ts) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(
          ERROR, "last_log_ts or next_log_ts is invalid", K(ret), K(partition_key_), K(last_log_ts), K(next_log_ts));
    } else {
      if (OB_LIKELY(is_empty())) {  // check empty first
        if (last_log_id == static_cast<uint64_t>(sw_.get_start_id() - 1)) {
          int64_t safe_cur_ts = next_log_ts - MAX_TIME_DIFF_BETWEEN_SERVER;
          res_ts = safe_cur_ts > last_log_ts ? safe_cur_ts : last_log_ts;
          CLOG_LOG(TRACE, "sw get next timestamp", K(partition_key_), K(res_ts), K(safe_cur_ts), K(last_log_ts));
        } else {
          ret = OB_EAGAIN;
        }
      } else {
        CLOG_LOG(TRACE, "sw not empty, get next log task timestamp", K(last_log_id), K(partition_key_));
        // not empty, try to get timestamp of the unconfirmed timestamp
        // this function is called under double check leader
        int64_t next_ts = OB_INVALID_TIMESTAMP;
        ObLogTask* task = NULL;
        const int64_t* ref = NULL;
        if (OB_FAIL(get_log_task(last_log_id + 1, task, ref))) {
          // in time interval [test sw is empty ~ log_task is put into sw], OB_ERR_NULL_VALUE is expected
          if (OB_ERR_NULL_VALUE == ret) {
            CLOG_LOG(INFO,
                "get log task error, log_task not put in sw yet",
                K(ret),
                K(partition_key_),
                K(last_log_id),
                "start_id",
                sw_.get_start_id(),
                "max_log_id",
                get_max_log_id());
            // heartbeat handler retry
            ret = OB_EAGAIN;
          } else if (OB_ERROR_OUT_OF_RANGE == ret) {
            CLOG_LOG(INFO, "get log task error, log slide out", K(ret), K(partition_key_), K(last_log_id));
            // heartbeat handler retry
            ret = OB_EAGAIN;
          } else {
            CLOG_LOG(WARN, "get log task error", K(ret), K(partition_key_), K(last_log_id));
          }
        } else {
          task->lock();
          if (task->is_submit_log_exist()) {
            next_ts = task->get_submit_timestamp();
            if (OB_INVALID_TIMESTAMP == next_ts) {
              ret = OB_ERR_UNEXPECTED;
              CLOG_LOG(WARN, "get invalid next_ts", K(partition_key_), K(last_log_id));
            } else {
              res_ts = std::min(next_ts - MAX_TIME_DIFF_BETWEEN_SERVER, last_log_ts);
            }
          } else {  // submit_log not exist
            // not leader now, maybe log_task is generated by a quicker confirm_info_packet
            ret = OB_NEED_RETRY;
            CLOG_LOG(INFO, "submit log not exist", K(ret));
          }
          task->unlock();
        }
        if (NULL != ref && OB_SUCCESS != (tmp_ret = revert_log_task(ref))) {
          CLOG_LOG(ERROR, "revert_log_task failed", K_(partition_key), K(tmp_ret));
        } else {
          ref = NULL;
        }
      }  // not empty
    }
  }
  return ret;
}

// only called by ObExtLeaderHeartbeatHandler to get next served log_id and ts based on keepalive ts
int ObLogSlidingWindow::get_next_served_log_info_by_next_replay_log_info(
    uint64_t& next_served_log_id, int64_t& next_served_log_ts)
{
  int ret = OB_SUCCESS;
  uint64_t next_replay_log_id = OB_INVALID_ID;
  int64_t next_replay_log_ts = OB_INVALID_TIMESTAMP;
  int64_t next_log_tstamp = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    get_next_replay_log_id_info(next_replay_log_id, next_replay_log_ts);

    // when next_log is start log in sw, try to retrieve its log_ts
    if (!is_empty() && next_replay_log_id == static_cast<uint64_t>(sw_.get_start_id())) {
      int tmp_ret = get_log_submit_tstamp_from_task_(next_replay_log_id, next_log_tstamp);
      if (OB_EAGAIN == tmp_ret) {
        CLOG_LOG(TRACE,
            "[GET_NEXT_SERVED_LOG_INFO] next log is not ready",
            K_(partition_key),
            K(next_replay_log_id),
            "start_id",
            sw_.get_start_id(),
            "max_log_id",
            get_max_log_id());
      } else if (OB_ERROR_OUT_OF_RANGE == tmp_ret) {
        CLOG_LOG(TRACE,
            "[GET_NEXT_SERVED_LOG_INFO] next log just slide out",
            K_(partition_key),
            K(next_replay_log_id),
            "start_id",
            sw_.get_start_id(),
            "max_log_id",
            get_max_log_id());
      }
    }

    if (OB_SUCCESS == ret) {
      next_served_log_id = next_replay_log_id;

      // If the next log is valid and effective, select the max value between lower bound of the next log and
      // follower-read ts
      if (OB_INVALID_TIMESTAMP != next_log_tstamp) {
        // Here minus the maximum clock offset between servers
        int64_t safe_next_log_tstamp = next_log_tstamp - MAX_TIME_DIFF_BETWEEN_SERVER;
        next_served_log_ts = std::max(safe_next_log_tstamp, next_replay_log_ts);
      } else {
        next_served_log_ts = next_replay_log_ts;
      }
    }

    CLOG_LOG(TRACE,
        "[GET_NEXT_SERVED_LOG_INFO]",
        K(ret),
        K_(partition_key),
        K(next_served_log_id),
        K(next_served_log_ts),
        K(next_replay_log_id),
        K(next_replay_log_ts),
        K(next_log_tstamp),
        "start_id",
        sw_.get_start_id(),
        "max_log_id",
        get_max_log_id());
  }
  return ret;
}

// return code:
// OB_EAGIN: log is not ready, need retry
// OB_ERROR_OUT_OF_RANGE: log has been slide out
// other code: failure
int ObLogSlidingWindow::get_log_submit_tstamp_from_task_(const uint64_t log_id, int64_t& log_tstamp)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogTask* task = NULL;
  const int64_t* ref = NULL;
  log_tstamp = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(get_log_task(log_id, task, ref))) {
    if (OB_ERR_NULL_VALUE == ret) {
      ret = OB_EAGAIN;
    } else if (OB_ERROR_OUT_OF_RANGE == ret) {
    } else {
      CLOG_LOG(WARN,
          "get log task error",
          K(ret),
          K(partition_key_),
          K(log_id),
          "start_id",
          sw_.get_start_id(),
          "max_log_id",
          get_max_log_id());
    }
  } else if (OB_ISNULL(task)) {
    CLOG_LOG(WARN, "invalid task after get_log_task", K(task), K(ret), K(log_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    task->lock();
    if (task->is_submit_log_exist()) {
      log_tstamp = task->get_submit_timestamp();
    } else {
      ret = OB_EAGAIN;
    }
    task->unlock();
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = revert_log_task(ref))) {
    CLOG_LOG(ERROR, "revert_log_task failed", K_(partition_key), K(tmp_ret), K(task));
  } else {
    ref = NULL;
    task = NULL;
  }
  return ret;
}

int ObLogSlidingWindow::submit_aggre_log(ObAggreBuffer* buffer, const int64_t base_timestamp)
{
  int ret = OB_SUCCESS;

  uint64_t tmp_log_id = OB_INVALID_ID;
  int64_t tmp_submit_timestamp = OB_INVALID_TIMESTAMP;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogSlidingWindow is not inited", K(ret), K(partition_key_));
  } else if (base_timestamp < 0 || base_timestamp > INT64_MAX / 2 || OB_ISNULL(buffer)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(partition_key_));
  } else {
    if (OB_FAIL(alloc_log_id_ts_(base_timestamp, tmp_log_id, tmp_submit_timestamp))) {
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        CLOG_LOG(WARN, "alloc_log_id failed", K_(partition_key), K(ret));
      }
    } else if (OB_FAIL(submit_aggre_log_(buffer, tmp_log_id, tmp_submit_timestamp))) {
      CLOG_LOG(WARN, "fill_aggre_buffer and submit failed", K(ret), K(partition_key_));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObLogSlidingWindow::submit_log(const ObLogType& log_type, const char* buff, const int64_t size,
    const int64_t base_timestamp, const bool is_trans_log, ObISubmitLogCb* cb, uint64_t& log_id, int64_t& log_timestamp)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  uint64_t tmp_log_id = OB_INVALID_ID;
  int64_t tmp_submit_timestamp = OB_INVALID_TIMESTAMP;
  bool need_replay = false;
  bool send_slave = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogSlidingWindow is not inited", K(ret), K(partition_key_));
  } else if (NULL == buff || size > OB_MAX_LOG_ALLOWED_SIZE || size < 0 || base_timestamp < 0 ||
             base_timestamp > INT64_MAX / 2) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(partition_key_));
  } else {
    const ObLogType real_log_type =
        (is_trans_log && aggre_buffer_cnt_ > 0 && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200 &&
            (size + AGGRE_LOG_RESERVED_SIZE) < AGGRE_BUFFER_LIMIT / 2)
            ? OB_LOG_AGGRE
            : log_type;

    if (OB_LOG_AGGRE == real_log_type) {
      // aggregate commit
      int64_t tmp_offset = 0;
      if (OB_ISNULL(aggre_buffer_)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "aggre_buffer_ is NULL, unexpected", K(ret), K(partition_key_));
      } else if (OB_FAIL(alloc_log_id_ts_(
                     base_timestamp, size + AGGRE_LOG_RESERVED_SIZE, tmp_log_id, tmp_submit_timestamp, tmp_offset)) &&
                 OB_BLOCK_SWITCHED != ret) {
        CLOG_LOG(TRACE,
            "alloc_log_id_ failed",
            K(ret),
            K(partition_key_),
            K(tmp_log_id),
            K(tmp_submit_timestamp),
            K(tmp_offset));
      } else if (OB_BLOCK_SWITCHED == ret) {
        if (AGGRE_BUFFER_FLAG != tmp_offset) {
          ret = fill_aggre_buffer_(tmp_log_id, tmp_offset, NULL, 0, 0, NULL);
        } else {
          ret = OB_SUCCESS;
        }
        tmp_log_id = tmp_log_id + 1;
        tmp_offset = 0;
      }
      if (OB_SUCC(ret)) {
        cb->submit_timestamp_ = tmp_submit_timestamp;
        ret = fill_aggre_buffer_(tmp_log_id, tmp_offset, buff, size, tmp_submit_timestamp, cb);
      }
      if (OB_SUCC(ret)) {
        if (OB_SUCCESS != (tmp_ret = try_freeze_aggre_buffer_(next_submit_aggre_buffer_))) {
          CLOG_LOG(ERROR, "try_freeze_aggre_buffer_ failed", K(ret), K(partition_key_));
        }
      }
    } else {
      // normal commit
      ObLogEntryHeader header;
      ObAddr dummy_server;
      const int64_t dummy_cluster_id = state_mgr_->get_self_cluster_id();
      // version is deprecated since 2.x
      ObVersion dummy_version(2, 0);

      // fix bug:1043693
      if (OB_LOG_START_MEMBERSHIP == real_log_type) {
        need_replay = true;
      }

      const bool is_confirmed = false;
      const int64_t unused_accum_checksum = 0;
      const bool unused_is_batch_committed = false;

      ObProposalID proposal_id = state_mgr_->get_proposal_id();
      if (OB_FAIL(alloc_log_id_ts_(base_timestamp, tmp_log_id, tmp_submit_timestamp))) {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          CLOG_LOG(WARN, "alloc_log_id failed", K_(partition_key), K(ret));
        }
      } else if (OB_FAIL(header.generate_header(real_log_type,  // real_log_type
                     partition_key_,                            // partition_key
                     tmp_log_id,                                // log_id
                     buff,                                      // buff
                     size,                                      // data_len
                     ObTimeUtility::current_time(),             // generation_timestamp
                     state_mgr_->get_proposal_id().ts_,         // epoch_id
                     state_mgr_->get_proposal_id(),             // proposal_id
                     tmp_submit_timestamp,                      // submit_timestamp
                     dummy_version,
                     is_trans_log))) {
      } else if (OB_FAIL(submit_to_sliding_window_(header,
                     buff,
                     cb,
                     need_replay,
                     send_slave,
                     dummy_server,
                     dummy_cluster_id,
                     is_confirmed,
                     unused_accum_checksum,
                     unused_is_batch_committed))) {
        CLOG_LOG(WARN, "submit_to_sliding_window failed", K_(partition_key), K(ret), K(tmp_log_id));
      }
    }
  }
  if (OB_SUCCESS == ret) {
    log_id = tmp_log_id;
    log_timestamp = tmp_submit_timestamp;
  }
  return ret;
}

int ObLogSlidingWindow::submit_log(const ObLogEntryHeader& header, const char* buff, ObISubmitLogCb* cb)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buff)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool need_replay = (cb == NULL);
    bool send_slave = true;
    ObAddr dummy_server;
    const bool is_confirmed = false;
    const int64_t unused_accum_checksum = 0;
    const bool unused_is_batch_committed = false;
    const int64_t dummy_cluster_id = state_mgr_->get_self_cluster_id();
    if (header.get_log_id() > max_log_meta_info_.get_log_id()) {
      CLOG_LOG(WARN,
          "submit_log log_id out_of_range",
          K_(partition_key),
          "log_id",
          header.get_log_id(),
          "max_log_id",
          max_log_meta_info_.get_log_id());
      ret = OB_ERROR_OUT_OF_RANGE;
    } else if (OB_FAIL(submit_to_sliding_window_(header,
                   buff,
                   cb,
                   need_replay,
                   send_slave,
                   dummy_server,
                   dummy_cluster_id,
                   is_confirmed,
                   unused_accum_checksum,
                   unused_is_batch_committed))) {
      CLOG_LOG(WARN,
          "submit_to_sliding_window failed",
          K_(partition_key),
          K(ret),
          "log_id",
          header.get_log_id(),
          "log_type",
          header.get_log_type());
    }
  }
  return ret;
}

int ObLogSlidingWindow::need_update_log_task_(
    const ObLogEntryHeader& header, const char* buff, ObLogTask& task, bool& log_need_update, bool& need_send_ack)
{
  int ret = OB_SUCCESS;
  const uint64_t log_id = header.get_log_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(state_mgr_) || OB_ISNULL(buff)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (task.is_log_confirmed()) {
    if (is_confirm_match_(
            log_id, header.get_data_checksum(), header.get_epoch_id(), task.get_data_checksum(), task.get_epoch_id())) {
      CLOG_LOG(DEBUG, "receive submit log after confirm log, match", K(header), K_(partition_key), K(task));
    } else {
      ret = OB_INVALID_LOG;
      CLOG_LOG(WARN, "receive submit log after confirm log, not match", K(ret), K(header), K_(partition_key), K(task));
    }
  }
  if (OB_SUCC(ret)) {
    if (!task.is_submit_log_exist()) {
      log_need_update = true;
    } else if (!task.is_log_confirmed()) {  // avoid confirmed log to be written twice
      ObProposalID old_pid = task.get_proposal_id();
      ObProposalID new_pid = header.get_proposal_id();
      if (old_pid >= new_pid) {
        if (partition_reach_time_interval(60 * 1000 * 1000, update_log_task_log_time_)) {
          CLOG_LOG(INFO,
              "receive log with smaller or same proposal_id, drop it",
              K_(partition_key),
              K(log_id),
              K(old_pid),
              K(new_pid),
              K(task));
        }
        if (old_pid == new_pid && task.is_flush_local_finished()) {
          need_send_ack = true;
        }
      } else {
        log_need_update = true;
      }
    } else if (state_mgr_->get_leader() != self_) {
      need_send_ack = true;
    }
  }
  return ret;
}

int ObLogSlidingWindow::update_log_task_(const ObLogEntryHeader& header, const char* buff, const bool need_copy,
    ObLogTask& task, bool& log_is_updated, bool& need_send_ack)
{
  int ret = OB_SUCCESS;
  log_is_updated = false;
  if (OB_FAIL(need_update_log_task_(header, buff, task, log_is_updated, need_send_ack))) {
    CLOG_LOG(WARN, "check need_update_log_task_ failed", K(ret), K_(partition_key), K(header), K(log_is_updated));
  } else if (log_is_updated) {
    if (OB_FAIL(task.set_log(header, buff, need_copy))) {
      CLOG_LOG(WARN, "set submit log to log task failed", K(ret), K(header), K_(partition_key));
    } else {
      task.reset_log_cursor();
    }
  }
  return ret;
}

int ObLogSlidingWindow::append_disk_log_to_sliding_window_(const ObLogEntry& log_entry, const ObLogCursor& log_cursor,
    const int64_t accum_checksum, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool is_logonly_replica = (mm_->get_replica_type() == REPLICA_TYPE_LOGONLY);
  const int64_t MAX_COPY_LOG_SIZE = 4 * 1024;  // 4k
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;
  bool need_copy = (log_entry.get_header().get_total_len() <= MAX_COPY_LOG_SIZE) &&
                   (log_id < ATOMIC_LOAD(&next_index_log_id_)) && (!is_logonly_replica);
  bool first_appended = false;
  bool log_is_updated = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  }
  if (pending_scan_confirmed_log_size >= 1 * 1024 * 1024 * 1024ll) {
    // allow submit to avoid sw hang, but do not copy log body
    need_copy = false;
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sw_.get(log_id, log_data, ref))) {
      CLOG_LOG(ERROR, "get log from sliding window failed", K(ret), K(partition_key_), K(log_id));
    } else if (NULL != log_data) {
      log_task = static_cast<ObLogTask*>(log_data);
      break;
    } else if (OB_FAIL(quick_generate_log_task_(log_entry, need_copy, log_task))) {
      CLOG_LOG(ERROR, "quick_generate_log_task_ failed", K(ret), K(partition_key_), K(log_id));
    } else if (OB_SUCCESS == (ret = sw_.set(log_id, log_task))) {
      first_appended = true;
      log_is_updated = true;
      break;
    } else if (OB_EAGAIN == ret) {
      log_task->destroy();
      ret = OB_SUCCESS;
    } else {
      log_task->destroy();
      CLOG_LOG(ERROR, "sw set failed", K(ret), K(partition_key_), K(log_id));
    }
    if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
      CLOG_LOG(ERROR, "revert failed", K(tmp_ret), K(partition_key_), K(log_id));
    } else {
      ref = NULL;
    }
  }

  if (OB_SUCC(ret) && !first_appended) {
    bool need_send_ack = false;
    log_task->lock();
    if (OB_FAIL(update_log_task_(
            log_entry.get_header(), log_entry.get_buf(), need_copy, *log_task, log_is_updated, need_send_ack))) {
      CLOG_LOG(ERROR, "update_log_task_ failed", K(ret), K(partition_key_), K(log_id));
    }
    log_task->unlock();
  }

  if (OB_SUCC(ret) && log_is_updated) {
    log_task->lock();
    log_task->set_log_cursor_without_stat(log_cursor);
    log_task->set_flush_local_finished();
    if (log_id < ATOMIC_LOAD(&next_index_log_id_)) {
      if (OB_FAIL(
              set_confirmed_info_without_lock_(log_entry.get_header(), accum_checksum, batch_committed, *log_task))) {
        CLOG_LOG(WARN,
            "failed to set_confirmed_info_without_lock_",
            K_(partition_key),
            K(log_entry),
            K(accum_checksum),
            K(batch_committed),
            KR(ret));
      } else if (!first_appended) {
        CLOG_LOG(ERROR,
            "append wrong log",
            K_(partition_key),
            K(log_id),
            K_(next_index_log_id),
            K(log_entry),
            K(log_cursor));
        ret = OB_ERR_UNEXPECTED;
      }
      ATOMIC_FAA(&pending_scan_confirmed_log_size,
          sizeof(ObLogTask) + (log_task->is_submit_log_body_exist() ? log_entry.get_header().get_data_len() : 0));
    }
    log_task->unlock();
  }
  bool need_check_succeeding_log = false;
  if (OB_SUCCESS != (tmp_ret = handle_first_index_log_(log_id, log_task, false, need_check_succeeding_log))) {
    CLOG_LOG(WARN, "handle_first_index_log_ failed", K_(partition_key), K(tmp_ret));
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
  } else {
    ref = NULL;
  }
  const bool do_pop = false;
  if (OB_SUCCESS != (tmp_ret = handle_succeeding_index_log_(log_id, need_check_succeeding_log, do_pop)) &&
      OB_ERROR_OUT_OF_RANGE != tmp_ret) {
    CLOG_LOG(WARN, "handle_succeeding_index_log_ failed", K_(partition_key), K(tmp_ret));
  }

  if (OB_SUCC(ret) && log_id >= sw_.get_start_id() + 200 && !ATOMIC_LOAD(&has_pop_task_)) {
    ATOMIC_STORE(&has_pop_task_, true);
    if (OB_SUCCESS != (tmp_ret = cb_engine_->submit_pop_task(partition_key_))) {
      CLOG_LOG(WARN, "submit_pop_task_ failed", K(tmp_ret), K(partition_key_));
      ATOMIC_STORE(&has_pop_task_, false);
    }
  }
  return ret;
}

int ObLogSlidingWindow::submit_to_sliding_window_(const ObLogEntryHeader& header, const char* buff, ObISubmitLogCb* cb,
    const bool need_replay, const bool send_slave, const ObAddr& server, const int64_t cluster_id,
    const bool is_confirmed, const int64_t accum_checksum, const bool is_batch_committed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;
  ObLogFlushTask* flush_task = NULL;
  bool log_task_need_update = false;
  bool locked = false;
  const uint64_t log_id = header.get_log_id();
  const bool need_copy = false;

  char* out = NULL;
  int64_t out_size = 0;
  char* serialize_buff = NULL;
  ObLogEntry new_log;
  bool standby_need_handle_index_log = false;
  bool standby_need_send_follower = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(log_engine_) || OB_ISNULL(buff)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(log_engine_), KP(buff), K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sw_.get(log_id, log_data, ref))) {
      if (OB_ERROR_OUT_OF_RANGE != ret) {
        CLOG_LOG(WARN, "get log from sliding window failed", K(ret), K_(partition_key), K(log_id));
      }
    } else if (NULL != log_data) {
      log_task = static_cast<ObLogTask*>(log_data);
      if (log_task->is_on_success_cb_called()) {
        if (!log_task->is_checksum_verified(header.get_data_checksum())) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "is_checksum_verified failed", K(ret), K(partition_key_), K(log_id), K(header), K(*log_task));
        }
      }
      break;
    } else {
      if (OB_FAIL(generate_null_log_task_(header, need_replay, log_task))) {
        CLOG_LOG(WARN, "generate_log_task failed", K(ret), K(log_id), K_(partition_key));
      } else {
        log_task->lock();
        locked = true;
        if (OB_SUCCESS == (ret = sw_.set(log_id, log_task))) {
          // if (NULL != log_task->get_trace_profile()
          //    && OB_FAIL(log_task->get_trace_profile()->trace(partition_key_, ON_SUBMIT_SW))) {
          //  CLOG_LOG(ERROR, "trace failed", K_(partition_key), K(ret));
          // } else {
          EVENT_INC(CLOG_SW_SUBMITTED_LOG_COUNT);
          EVENT_ADD(CLOG_SW_SUBMITTED_LOG_SIZE, header.get_data_len() + sizeof(ObLogEntryHeader));
          // }
          if (locked) {
            log_task->unlock();
            locked = false;
          }
          break;
          // If OB_EAGAIN is returned, it means that this log already exists in the sliding window due to concurrency,
          // try to read it again
        } else if (OB_EAGAIN == ret) {
          if (partition_reach_time_interval(60 * 1000 * 1000, insert_log_try_again_warn_time_)) {
            CLOG_LOG(INFO, "insert log to sliding conflict, try again", K(ret), K_(partition_key), K(header));
          }
          ret = OB_SUCCESS;
          if (locked) {
            log_task->unlock();
            locked = false;
          }
          log_task->destroy();
        } else {
          CLOG_LOG(WARN, "insert log to sliding failed", K(ret), K_(partition_key), K(header));
          if (locked) {
            log_task->unlock();
            locked = false;
          }
          log_task->destroy();
        }
      }
    }
    if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
      CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
    } else {
      ref = NULL;
    }
  }
  // if log exists, try to update it
  if (OB_SUCCESS == ret && !locked) {
    if (NULL != log_data) {
      log_task = static_cast<ObLogTask*>(log_data);
    }
    log_task->lock();
    locked = true;
    bool need_send_ack = false;
    ObProposalID proposal_id = state_mgr_->get_proposal_id();
    const ObAddr leader = state_mgr_->get_leader();
    const int64_t leader_cluster_id = state_mgr_->get_leader_cluster_id();
    if (OB_FAIL(need_update_log_task_(header, buff, *log_task, log_task_need_update, need_send_ack))) {
      CLOG_LOG(WARN, "set log to task failed", K(ret), K(log_id), K_(partition_key));
    } else if (need_send_ack && state_mgr_->can_send_log_ack(proposal_id) && server.is_valid() && server == leader &&
               OB_FAIL(log_engine_->submit_log_ack(server, leader_cluster_id, partition_key_, log_id, proposal_id))) {
      CLOG_LOG(WARN, "submit_log_ack failed", K_(partition_key), K(ret), K(server), K(log_id));
    } else if (need_send_ack && STANDBY_LEADER == state_mgr_->get_role()) {
      // standby leader try to set confirmed for a log that has reached majority
      // it maybe follower when receive this log, so it did not set confirmed in flush_cb
      // it has no chance to confirm this log in standby single replica node, so put it here
      if (!log_task->is_majority_finished()) {
        if (log_task->try_set_majority_finished()) {
          // it can execute set_log_confirmed only when try_set_majority_finished returns true (flush_cb, ack_log may
          // also does this) To avoid the problem of multiple threads concurrency that may cause the log to slide out
          // and cause an error
          if (can_set_log_confirmed_(log_task)) {
            // Set_log_confirmed can only be executed when confirmed info exists
            log_task->set_log_confirmed();
            CLOG_LOG(
                DEBUG, "standby_leader set_log_confirmed success", K(ret), K_(partition_key), K(log_id), K(*log_task));
          }
        }
      }

      if (log_task->is_log_confirmed()) {
        // After confirmation, it need to try to submit ilog and slide out.
        // Here it need to trigger handle_first_index_log for the following reasons:
        // Consider the scenario: standby leader (single replica mode) has received the confirmed info first,
        // and it has not yet been majority finished.
        // After set majority finished and set log confirmed in this function, fetch log will not be triggered again
        // There is no chance to trigger submission and write ilog in receive_confirmed_info
        standby_need_handle_index_log = true;
      }

      if (!log_task->is_majority_finished()) {
        // This log on standby_leader is not yet majority and needs to be forwarded to follower
        // When the standby leader is switched, the sw left boundary of the new leader may be smaller than the old one,
        // so part of the log may need to be majority again.
        // Rely on this forwarding to trigger re-collection of ack
        standby_need_send_follower = true;
      }
      // standby leadre replies ack to primary leader
      if (log_task->is_majority_finished()) {
        if (OB_FAIL(send_standby_log_ack_(server, cluster_id, log_id, proposal_id))) {
          CLOG_LOG(WARN,
              "send_standby_log_ack_ failed",
              K(ret),
              K_(partition_key),
              K(server),
              K(cluster_id),
              K(log_id),
              K(proposal_id));
        }
      }
    } else {
    }  // do nothing
  }

  if (OB_SUCCESS == ret && log_task_need_update) {
    int64_t pos = 0;
    if (OB_FAIL(new_log.generate_entry(header, buff))) {
      CLOG_LOG(ERROR, "generate_entry failed", K_(partition_key), K(ret));
    } else if (OB_ISNULL(alloc_mgr_)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (NULL == (serialize_buff = static_cast<char*>(alloc_mgr_->ge_alloc(new_log.get_serialize_size())))) {
      CLOG_LOG(WARN, "alloc memory failed", K_(partition_key), K(new_log));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(new_log.serialize(serialize_buff, new_log.get_serialize_size(), pos))) {
      CLOG_LOG(WARN, "submit_to_net_and_disk serialize failed", K_(partition_key), K(ret));
    } else {
      out = serialize_buff;
      out_size = new_log.get_serialize_size();
      if (OB_SUCC(ret)) {
        // if (NULL != log_task->get_trace_profile()
        //    && OB_FAIL(log_task->get_trace_profile()->trace(partition_key_, BEFORE_SUBMIT_TO_NET_AND_DISK))) {
        //  CLOG_LOG(ERROR, "trace failed", K_(partition_key), K(ret));
        if (OB_FAIL(prepare_flush_task_(new_log.get_header(), out, out_size, server, cluster_id, flush_task))) {
          CLOG_LOG(WARN, "submit_to_disk failed", K(ret), K_(partition_key), K(header));
          // } else if (NULL != log_task->get_trace_profile()
          //           && OB_FAIL(log_task->get_trace_profile()->trace(partition_key_, AFTER_SUBMIT_TO_NET_AND_DISK))) {
          //  CLOG_LOG(ERROR, "trace failed", K_(partition_key), K(ret));
        } else if (OB_FAIL(log_task->set_log(header, buff, need_copy))) {
          CLOG_LOG(ERROR, "set submit log to log task failed", K(ret), K(header), K_(partition_key));
        } else {
          log_task->reset_log_cursor();
          log_task->set_submit_cb(cb);
          CLOG_LOG(TRACE, "set log success", K(ret), K(log_id), K_(partition_key));
        }
      }
    }
  }

  if (OB_SUCCESS == ret && !log_task_need_update && standby_need_send_follower) {
    // log_task no need be updated, but standby_leader need transfer it to followers
    int64_t pos = 0;
    if (OB_FAIL(new_log.generate_entry(header, buff))) {
      CLOG_LOG(ERROR, "generate_entry failed", K_(partition_key), K(ret));
    } else if (NULL != serialize_buff) {
      // serialize_buff is expected to be empty
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "serialize_buff is not null, unexpected", K_(partition_key), K(ret));
    } else {
      int64_t serialize_size = new_log.get_serialize_size();
      if (NULL == (serialize_buff = static_cast<char*>(alloc_mgr_->ge_alloc(serialize_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        CLOG_LOG(ERROR, "alloc failed", K(ret), K_(partition_key));
      } else if (OB_FAIL(new_log.serialize(serialize_buff, serialize_size, pos))) {
        CLOG_LOG(WARN, "serialize log failed", K_(partition_key), K(ret));
      } else {
        // do nothing
      }
    }
  }

  bool is_log_majority = false;
  if (locked) {
    is_log_majority = (log_task->is_local_majority_flushed() || log_task->is_log_confirmed());
    log_task->unlock();
    locked = false;
  }

  if (OB_SUCC(ret) && is_confirmed) {
    // need set confirmed_info for fast recovery logs
    log_task->lock();
    if (OB_FAIL(set_confirmed_info_without_lock_(header, accum_checksum, is_batch_committed, *log_task))) {
      CLOG_LOG(WARN,
          "failed to set_confirmed_info_without_lock_",
          K_(partition_key),
          K(header),
          K(accum_checksum),
          K(is_batch_committed),
          KR(ret));
    }
    log_task->unlock();
  }
  bool need_check_succeeding_log = false;
  if (standby_need_handle_index_log) {
    // standby leader may run here to trigger generating ilog
    if (OB_SUCCESS != (tmp_ret = handle_first_index_log_(log_id, log_task, false, need_check_succeeding_log))) {
      CLOG_LOG(WARN, "handle_first_index_log_ failed", K_(partition_key), K(tmp_ret));
    }
  }

  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
  } else {
    ref = NULL;
  }

  if (standby_need_handle_index_log) {
    // standby leader may run here to trigger generating ilog
    const bool do_pop = true;
    if (OB_SUCCESS != (tmp_ret = handle_succeeding_index_log_(log_id, need_check_succeeding_log, do_pop)) &&
        OB_ERROR_OUT_OF_RANGE != tmp_ret) {
      CLOG_LOG(WARN, "handle_succeeding_index_log_ failed", K_(partition_key), K(tmp_ret));
    }
  }

  if (OB_SUCC(ret)) {
    if ((log_task_need_update || standby_need_send_follower)  // ensure new_log is valid
        && (send_slave || cascading_mgr_->has_valid_child() || cascading_mgr_->has_valid_async_standby_child() ||
               cascading_mgr_->has_valid_sync_standby_child())) {
      // send it to member_list/children
      if (OB_SUCCESS != (tmp_ret = submit_log_to_net_(
                             new_log.get_header(), serialize_buff, new_log.get_serialize_size(), is_log_majority))) {
        CLOG_LOG(WARN, "submit_to_net failed", K(tmp_ret), K_(partition_key), K(header));
      }
    }
  }

  if (NULL != flush_task) {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(log_engine_->submit_flush_task(flush_task))) {
        CLOG_LOG(ERROR, "log_engine submit_flush_task failed", K(ret), K(partition_key_), K(header));
      } else {
        EVENT_ADD(CLOG_SUBMIT_LOG_TOTAL_SIZE, out_size);
        EVENT_ADD(CLOG_TRANS_LOG_TOTAL_SIZE, header.get_data_len());
        CLOG_LOG(TRACE, "log_engine submit_flush_task success", K(ret), K(partition_key_), K(header));
      }
    } else {
      alloc_mgr_->free_log_flush_task(flush_task);
      flush_task = NULL;
    }
  }

  if (serialize_buff != NULL) {
    alloc_mgr_->ge_free(serialize_buff);
    serialize_buff = NULL;
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
  } else {
    ref = NULL;
  }

  if (NULL != serialize_buff) {
    alloc_mgr_->ge_free(serialize_buff);
    serialize_buff = NULL;
  }
  return ret;
}

int ObLogSlidingWindow::submit_confirmed_info_(
    const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool is_leader, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;
  bool locked = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sw_.get(log_id, log_data, ref))) {
      if (OB_ERROR_OUT_OF_RANGE != ret) {
        CLOG_LOG(WARN, "get log from sliding window failed", K(ret), K_(partition_key), K(log_id));
      }
    } else if (NULL != log_data) {
      break;
    } else {
      // only follower can achieve this branch, must be need_replay==true
      if (OB_FAIL(generate_log_task_(confirmed_info, true, batch_committed, log_task))) {
        CLOG_LOG(ERROR, "generate_log_task_ failed", K(ret), K(log_id), K_(partition_key));
      } else {
        log_task->lock();
        locked = true;
        if (OB_SUCCESS == (ret = sw_.set(log_id, log_task))) {
          break;
        } else if (OB_EAGAIN == ret) {
          if (partition_reach_time_interval(60 * 1000 * 1000, insert_log_try_again_warn_time_)) {
            CLOG_LOG(INFO,
                "insert log to sliding conflict, try again",
                K(ret),
                K_(partition_key),
                K(log_id),
                K(confirmed_info));
          }
          ret = OB_SUCCESS;
          if (locked) {
            log_task->unlock();
            locked = false;
          }
          log_task->destroy();
        } else {
          if (OB_ERROR_OUT_OF_RANGE == ret) {
            CLOG_LOG(INFO,
                "insert log to sliding failed, log already slide out",
                K(ret),
                K_(partition_key),
                K(log_id),
                K(confirmed_info));
          } else {
            CLOG_LOG(ERROR, "insert log to sliding failed", K(ret), K_(partition_key), K(log_id), K(confirmed_info));
          }
          if (locked) {
            log_task->unlock();
            locked = false;
          }
          log_task->destroy();
          log_task = NULL;
        }
      }
    }
    if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
      CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
    } else {
      ref = NULL;
    }
  }
  if (OB_SUCCESS == ret && !locked) {
    log_task = static_cast<ObLogTask*>(log_data);
    log_task->lock();
    locked = true;
    if (log_task->is_confirmed_info_exist()) {
      if (can_set_log_confirmed_(log_task)) {
        log_task->set_log_confirmed();
      }
    } else {
      /*
       * this assert maybe not true if this partition experience follower -> leader -> follower
       * when it is a follower: receive confirm_info -> create log_task
       * then it change to be the leader: clean_log -> reset confirm_info
       * then it change to be a follower: receive confirm_info -> assert(log_task has submit_log)
       * So we remove this assert.
       */
      if (log_task->is_submit_log_exist()) {
        if (!is_confirm_match_(log_id,
                log_task->get_data_checksum(),
                log_task->get_epoch_id(),
                confirmed_info.get_data_checksum(),
                confirmed_info.get_epoch_id())) {
          CLOG_LOG(INFO,
              "log_task and confirmed_info not match, reset",
              K_(partition_key),
              K(log_id),
              K(*log_task),
              K(confirmed_info));
          log_task->reset_log();
          log_task->reset_log_cursor();
        }
      } else {
        // for debug
        CLOG_LOG(TRACE,
            "this partition receive a confirm_info but no submit_log exist",
            K(log_id),
            K(confirmed_info),
            K(log_task),
            K_(partition_key));
      }
      log_task->set_confirmed_info(confirmed_info);

      if (can_set_log_confirmed_(log_task)) {
        log_task->set_log_confirmed();
      }

      if (batch_committed) {
        log_task->set_batch_committed();
      }
    }
  }
  if (locked) {
    log_task->unlock();
    locked = false;
  }
  bool need_check_succeeding_log = false;
  if (!is_leader && OB_SUCCESS == ret && NULL != log_task &&
      (OB_SUCCESS != (tmp_ret = handle_first_index_log_(log_id, log_task, true, need_check_succeeding_log)))) {
    CLOG_LOG(WARN, "handle_first_index_log_ failed", K_(partition_key), K(tmp_ret), K(log_id), K(confirmed_info));
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
  } else {
    ref = NULL;
  }
  const bool do_pop = true;
  if (!is_leader && OB_SUCCESS != (tmp_ret = handle_succeeding_index_log_(log_id, need_check_succeeding_log, do_pop)) &&
      OB_ERROR_OUT_OF_RANGE != tmp_ret) {
    CLOG_LOG(WARN, "handle_succeeding_index_log_ failed", K_(partition_key), K(tmp_ret), K(log_id), K(confirmed_info));
  }
  // confirmed log may be slide out during reconfirm
  if (is_leader && OB_ERROR_OUT_OF_RANGE == ret) {
    CLOG_LOG(INFO, "log task slide out while submnit confirm info", K(ret), K_(partition_key), K(log_id));
    ret = OB_SUCCESS;
  }
  return ret;
}

void* ObLogSlidingWindow::alloc_log_task_buf_()
{
  void* log_task = NULL;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "sw is not inited", K_(partition_key));
  } else if (NULL != (log_task = static_cast<ObLogTask*>(alloc_mgr_->alloc_log_task_buf()))) {
    // alloc success
  } else if (REPLAY == state_mgr_->get_state()) {
    // try to alloc from tenant 500 when in REPLAY state
    ObILogAllocator* server_tenant_allocator = NULL;
    if (OB_SUCCESS !=
        (tmp_ret = TMA_MGR_INSTANCE.get_tenant_log_allocator(OB_SERVER_TENANT_ID, server_tenant_allocator))) {
      CLOG_LOG(WARN, "get_tenant_log_allocator failed", K(tmp_ret), K_(partition_key));
    } else {
      log_task = server_tenant_allocator->alloc_log_task_buf();
    }
  }
  return log_task;
}

int ObLogSlidingWindow::generate_null_log_task_(
    const ObLogEntryHeader& header, const bool need_replay, ObLogTask*& log_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(alloc_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (log_task = static_cast<ObLogTask*>(alloc_log_task_buf_()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc memory failed", K(ret), K_(partition_key), K(header));
  } else {
    new (log_task) ObLogTask();
    // log_cursor_ is invalid initially, no need call task->reset_log_cursor
    if (OB_FAIL(log_task->init(NULL, mm_->get_replica_num(), need_replay))) {
      CLOG_LOG(WARN, "init log task fail", K_(partition_key), K(ret));
    } else {
      CLOG_LOG(TRACE, "generate_null_log_task_ success", K_(partition_key), K(ret), K(header));
    }
  }
  return ret;
}

int ObLogSlidingWindow::quick_generate_log_task_(
    const ObLogEntry& log_entry, const bool need_copy, ObLogTask*& log_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == (log_task = static_cast<ObLogTask*>(alloc_log_task_buf_()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "alloc memory failed", K(ret), K(partition_key_));
  } else {
    new (log_task) ObLogTask();
    if (OB_FAIL(log_task->quick_init(mm_->get_replica_num(), log_entry, need_copy))) {
      CLOG_LOG(WARN, "log_task quick_init failed", K(ret), K(partition_key_));
      log_task->destroy();
    } else {
      CLOG_LOG(TRACE, "quick_generate_log_task_ success", K(ret), K(partition_key_));
    }
  }
  return ret;
}

int ObLogSlidingWindow::generate_log_task_(const ObLogEntryHeader& header, const char* buff, const bool need_replay,
    const bool need_copy, ObLogTask*& log_task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_null_log_task_(header, need_replay, log_task))) {
    CLOG_LOG(WARN, "generate_null_log_task_ failed", K(ret), K(header));
  } else if (OB_FAIL(log_task->set_log(header, buff, need_copy))) {
    CLOG_LOG(WARN, "set log entry to log_task failed", K(ret), K_(partition_key));
    log_task->destroy();
  } else {
    CLOG_LOG(TRACE, "generate_log_task_ success", K_(partition_key), K(ret), K(header));
  }
  return ret;
}

int ObLogSlidingWindow::generate_log_task_(
    const ObConfirmedInfo& confirmed_info, const bool need_replay, const bool batch_committed, ObLogTask*& log_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(alloc_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (log_task = static_cast<ObLogTask*>(alloc_log_task_buf_()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc memory failed", K(ret), K_(partition_key), K(confirmed_info));
  } else {
    new (log_task) ObLogTask();
    if (OB_FAIL(log_task->init(NULL, mm_->get_replica_num(), need_replay))) {
      CLOG_LOG(WARN, "init log task fail", K_(partition_key), K(ret));
    } else {
      log_task->set_confirmed_info(confirmed_info);

      if (can_set_log_confirmed_(log_task)) {
        log_task->set_log_confirmed();
      }

      if (batch_committed) {
        log_task->set_batch_committed();
      }
    }
  }
  return ret;
}

int ObLogSlidingWindow::submit_confirmed_info_to_net_(
    const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mm_) || OB_ISNULL(log_engine_) || OB_ISNULL(cascading_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (restore_mgr_->is_archive_restoring()) {
  } else {
    if (OB_SUCCESS != (tmp_ret = submit_confirmed_info_to_member_list_(log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN, "submit_confirmed_info_to_member_list_ failed", K(tmp_ret), K(partition_key_), K(log_id));
    }

    if (OB_SUCCESS != (tmp_ret = submit_confirmed_info_to_local_children_(log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN, "submit_confirmed_info_to_local_children_ failed", K(tmp_ret), K(partition_key_), K(log_id));
    }

    // send it to standby children unconditionally
    if (OB_SUCCESS != (tmp_ret = submit_confirmed_info_to_standby_children_(log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN, "submit_confirmed_info_to_standby_children_ failed", K_(partition_key), K(tmp_ret));
    }
  }

  return ret;
}

// follower transfer confirmed_info to all children
int ObLogSlidingWindow::follower_transfer_confirmed_info_to_net_(
    const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mm_) || OB_ISNULL(log_engine_) || OB_ISNULL(cascading_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (restore_mgr_->is_archive_restoring()) {
  } else if (FOLLOWER != state_mgr_->get_role()) {
    // self is not follower, skip
  } else {
    if (OB_SUCCESS != (tmp_ret = submit_confirmed_info_to_local_children_(log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN, "submit_confirmed_info_to_local_children_ failed", K(tmp_ret), K(partition_key_), K(log_id));
    }

    // send it to standby children unconditionally
    if (OB_SUCCESS != (tmp_ret = submit_confirmed_info_to_standby_children_(log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN, "submit_confirmed_info_to_standby_children_ failed", K_(partition_key), K(tmp_ret));
    } else {
      CLOG_LOG(TRACE, "follower_transfer_confirmed_info_to_net_", K_(partition_key), K(log_id));
    }
  }

  return ret;
}

// send confirmed_info to curr_member_list
int ObLogSlidingWindow::submit_confirmed_info_to_member_list_(
    const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  if (LEADER == state_mgr_->get_role() ||
      (STANDBY_LEADER == state_mgr_->get_role() && can_transfer_confirmed_info_(log_id))) {
    // standby_leader need guarantee continuously confirmed before send
    ObCascadMemberList list;
    list.reset();

    const int64_t dst_cluster_id = state_mgr_->get_self_cluster_id();
    if (OB_FAIL(list.deep_copy(mm_->get_curr_member_list(), dst_cluster_id))) {
      CLOG_LOG(ERROR, "deep_copy failed", K_(partition_key), K(ret));
    } else if (list.is_valid()) {
      if (list.contains(self_) && OB_FAIL(list.remove_server(self_))) {
        CLOG_LOG(ERROR, "remove_server failed", K_(partition_key), K(ret), K(list), K_(self));
      } else if (OB_FAIL(log_engine_->submit_confirmed_info(
                     list, partition_key_, log_id, confirmed_info, batch_committed))) {
        CLOG_LOG(WARN,
            "log_engine submit_confirmed_info failed",
            K_(partition_key),
            K(ret),
            K(list),
            K(log_id),
            K(confirmed_info),
            K(batch_committed));
      }
    }
  }
  return ret;
}

// send confirmed info to children in same cluster
int ObLogSlidingWindow::submit_confirmed_info_to_local_children_(
    const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  if (cascading_mgr_->has_valid_child() && can_transfer_confirmed_info_(log_id)) {
    // standby_leader need guarantee continuously confirmed before send
    ObCascadMemberList children_list;
    if (OB_FAIL(cascading_mgr_->get_children_list(children_list))) {
      CLOG_LOG(ERROR, "get_children_list failed", K_(partition_key), K(ret));
    } else if (children_list.get_member_number() <= 0) {
      // no need send
    } else if (children_list.contains(self_) && OB_FAIL(children_list.remove_server(self_))) {
      CLOG_LOG(ERROR, "remove_server failed", K_(partition_key), K(ret), K(children_list), K_(self));
    } else if (OB_FAIL(log_engine_->submit_confirmed_info(
                   children_list, partition_key_, log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN,
          "log_engine submit_confirmed_info failed",
          K_(partition_key),
          K(ret),
          K(children_list),
          K(log_id),
          K(confirmed_info),
          K(batch_committed));
    } else {
      CLOG_LOG(TRACE,
          "submit_confirmed_info to children_list finished",
          K_(partition_key),
          K(ret),
          K(children_list),
          K(log_id));
    }
  }

  return ret;
}

int ObLogSlidingWindow::submit_confirmed_info_to_standby_children_(
    const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret) && LEADER == state_mgr_->get_role() && cascading_mgr_->has_valid_sync_standby_child()) {
    // leader send it to sync standby child
    share::ObCascadMember sync_child = cascading_mgr_->get_sync_standby_child();
    if (sync_child.is_valid()) {
      ObCascadMemberList sync_list;
      if (OB_FAIL(sync_list.add_member(sync_child))) {
        CLOG_LOG(WARN, "sync_list add_member failed", K_(partition_key), K(ret));
      } else if (OB_FAIL(log_engine_->submit_confirmed_info(
                     sync_list, partition_key_, log_id, confirmed_info, batch_committed))) {
        CLOG_LOG(WARN,
            "log_engine submit_confirmed_info failed",
            K_(partition_key),
            K(ret),
            K(sync_list),
            K(log_id),
            K(confirmed_info),
            K(batch_committed));
      } else {
        CLOG_LOG(DEBUG,
            "submit confirmed_info to sync child finish",
            K_(partition_key),
            K(ret),
            K(sync_list),
            K(log_id),
            K(confirmed_info),
            K(batch_committed));
      }
    }
  }

  // send it to async sntadby children unconditionally
  // standby_child is expected to be STANDBY_LEADER
  // if it has revoked, it will reject to avoid confirmed_info hole
  if (OB_SUCC(ret) && cascading_mgr_->has_valid_async_standby_child()) {
    ObCascadMemberList async_standby_children;
    if (OB_FAIL(cascading_mgr_->get_async_standby_children(async_standby_children))) {
      CLOG_LOG(ERROR, "get_async_standby_children failed", K_(partition_key), K(ret));
    } else if (async_standby_children.get_member_number() <= 0) {
      // no need send
    } else if (OB_FAIL(log_engine_->submit_confirmed_info(
                   async_standby_children, partition_key_, log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN,
          "log_engine submit_confirmed_info failed",
          K_(partition_key),
          K(ret),
          K(async_standby_children),
          K(log_id),
          K(confirmed_info),
          K(batch_committed));
    } else {
      CLOG_LOG(TRACE,
          "submit confirmed_info to async child finish",
          K_(partition_key),
          K(ret),
          K(async_standby_children),
          K(log_id),
          K(confirmed_info),
          K(batch_committed));
    }
  }

  return ret;
}

int ObLogSlidingWindow::submit_log_to_net_(const ObLogEntryHeader& header, const char* serialize_buff,
    const int64_t serialize_size, const bool is_log_majority)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(state_mgr_) || OB_ISNULL(log_engine_) || OB_ISNULL(serialize_buff) ||
             OB_ISNULL(cascading_mgr_) || serialize_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (restore_mgr_->is_archive_restoring()) {
  } else {
    // send log to member_list
    if (OB_SUCCESS != (tmp_ret = submit_log_to_member_list_(header, serialize_buff, serialize_size))) {
      CLOG_LOG(WARN, "submit_log_to_member_list_ failed", K(tmp_ret), K(partition_key_), K(header));
    }

    // send log to children in same cluster
    if (OB_SUCCESS != (tmp_ret = submit_log_to_local_children_(header, serialize_buff, serialize_size))) {
      CLOG_LOG(WARN, "submit_log_to_local_children_ failed", K(tmp_ret), K(partition_key_), K(header));
    }

    if (OB_SUCCESS !=
        (tmp_ret = submit_log_to_standby_children_(header, serialize_buff, serialize_size, is_log_majority))) {
      CLOG_LOG(WARN, "submit_log_to_standby_children_ failed", K(ret), K(partition_key_), K(header));
    }
  }
  return ret;
}

int ObLogSlidingWindow::submit_log_to_member_list_(
    const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size)
{
  int ret = OB_SUCCESS;
  if (LEADER == state_mgr_->get_role() || STANDBY_LEADER == state_mgr_->get_role()) {
    ObLogNetTask net_task(state_mgr_->get_proposal_id(), serialize_buff, serialize_size);
    ObCascadMemberList list;
    list.reset();

    const int64_t dst_cluster_id = state_mgr_->get_self_cluster_id();
    if (OB_FAIL(list.deep_copy(mm_->get_curr_member_list(), dst_cluster_id))) {
      CLOG_LOG(ERROR, "deep_copy failed", K_(partition_key), K(ret));
    } else if (list.is_valid()) {
      if (list.contains(self_) && OB_FAIL(list.remove_server(self_))) {
        CLOG_LOG(ERROR, "remove_server failed", K_(partition_key), K(ret), K(list), K_(self));
      } else if (OB_FAIL(log_engine_->submit_net_task(list, partition_key_, PUSH_LOG_ASYNC, &net_task))) {
        CLOG_LOG(WARN, "log_engine submit_net_task failed", K_(partition_key), K(ret), K(header));
      } else {
        CLOG_LOG(TRACE, "submit_log_to_member_list_ finish", K_(partition_key), K(ret), K(list), K(header));
      }
    }
  }
  return ret;
}

int ObLogSlidingWindow::submit_log_to_local_children_(
    const ObLogEntryHeader& header, const char* serialize_buff, const int64_t serialize_size)
{
  int ret = OB_SUCCESS;
  ObLogNetTask net_task(state_mgr_->get_proposal_id(), serialize_buff, serialize_size);

  if (cascading_mgr_->has_valid_child()) {
    ObCascadMemberList children_list;
    if (OB_FAIL(cascading_mgr_->get_children_list(children_list))) {
      CLOG_LOG(ERROR, "get_children_list failed", K_(partition_key), K(ret));
    } else if (children_list.get_member_number() <= 0) {
      // no need send
    } else if (children_list.contains(self_) && OB_FAIL(children_list.remove_server(self_))) {
      CLOG_LOG(ERROR, "remove_server failed", K_(partition_key), K(ret), K(children_list), K_(self));
    } else if (OB_FAIL(log_engine_->submit_net_task(children_list, partition_key_, PUSH_LOG_ASYNC, &net_task))) {
      CLOG_LOG(WARN, "log_engine submit_net_task failed", K_(partition_key), K(ret), K(header));
    } else {
      CLOG_LOG(TRACE, "submit log to children_list finish", K_(partition_key), K(ret), K(children_list), K(header));
    }
  }

  return ret;
}

// send log to standby children(sync/async)
int ObLogSlidingWindow::submit_log_to_standby_children_(const ObLogEntryHeader& header, const char* serialize_buff,
    const int64_t serialize_size, const bool is_log_majority)
{
  int ret = OB_SUCCESS;
  ObLogNetTask net_task(state_mgr_->get_proposal_id(), serialize_buff, serialize_size);

  if (NULL == serialize_buff || serialize_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCC(ret) && LEADER == state_mgr_->get_role() && cascading_mgr_->has_valid_sync_standby_child()) {
    share::ObCascadMember sync_child = cascading_mgr_->get_sync_standby_child();
    if (sync_child.is_valid()) {
      ObCascadMemberList sync_list;
      if (OB_FAIL(sync_list.add_member(sync_child))) {
        CLOG_LOG(WARN, "sync_list add_member failed", K_(partition_key), K(ret));
      } else if (OB_FAIL(log_engine_->submit_net_task(sync_list, partition_key_, PUSH_LOG_SYNC, &net_task))) {
        CLOG_LOG(WARN, "log_engine submit_net_task failed", K_(partition_key), K(ret), K(header));
      } else {
        CLOG_LOG(DEBUG,
            "submit log to sync child finish",
            K_(partition_key),
            K(ret),
            K(sync_list),
            K(header),
            K(serialize_size),
            KP(serialize_buff));
      }
    }
  }

  // send log to async standby children
  if (OB_SUCC(ret) && cascading_mgr_->has_valid_async_standby_child()) {
    if (common::INVALID_CLUSTER_TYPE == GCTX.get_cluster_type()) {
      // it must not send log to standby children when local cluster_type is unknown
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        CLOG_LOG(WARN, "invalid cluster_type, cannot send log to standby_children", K(partition_key_), K(header));
      }
    } else if (GCTX.is_primary_cluster() && !is_log_majority) {
      // if this log has not been majority, it cannot be send to standby children
    } else {
      ObCascadMemberList async_standby_children;
      if (OB_FAIL(cascading_mgr_->get_async_standby_children(async_standby_children))) {
        CLOG_LOG(ERROR, "get_async_standby_children failed", K_(partition_key), K(ret));
      } else if (async_standby_children.get_member_number() <= 0) {
        // no need send
      } else if (OB_FAIL(
                     log_engine_->submit_net_task(async_standby_children, partition_key_, PUSH_LOG_ASYNC, &net_task))) {
        CLOG_LOG(WARN, "log_engine submit_net_task failed", K_(partition_key), K(ret), K(header));
      } else {
        CLOG_LOG(TRACE,
            "submit log to async_standby_children finish",
            K_(partition_key),
            K(ret),
            K(async_standby_children),
            K(header));
      }
    }
  }

  return ret;
}

int ObLogSlidingWindow::prepare_flush_task_(const ObLogEntryHeader& header, char* serialize_buff,
    const int64_t serialize_size, const ObAddr& server, const int64_t cluster_id, ObLogFlushTask*& flush_task)
{
  int ret = OB_SUCCESS;
  int64_t pls_epoch = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_engine_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (flush_task = alloc_mgr_->alloc_log_flush_task())) {
    CLOG_LOG(WARN, "alloc_log_flush_task failed", K_(partition_key), K(header), K(server));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(state_mgr_->get_pls_epoch(pls_epoch))) {
    CLOG_LOG(WARN, "get_pls_epoch failed", K_(partition_key), K(ret));
  } else if (OB_FAIL(flush_task->init(header.get_log_type(),
                 header.get_log_id(),
                 header.get_proposal_id(),
                 partition_key_,
                 partition_service_,
                 server,
                 cluster_id,
                 log_engine_,
                 header.get_submit_timestamp(),
                 pls_epoch))) {
    CLOG_LOG(WARN,
        "ObLogFlushTask init failed",
        K_(partition_key),
        K(ret),
        K(header),
        KP_(partition_service),
        K(server),
        K_(log_engine));
  } else {
    flush_task->set(serialize_buff, serialize_size);
  }
  if (OB_SUCCESS != ret && NULL != flush_task) {
    alloc_mgr_->free_log_flush_task(flush_task);
    flush_task = NULL;
  }
  CLOG_LOG(TRACE, "prepare_flush_task_ finish", K(ret), K(header));
  return ret;
}

void ObLogSlidingWindow::set_next_replay_log_id_info(const uint64_t log_id, const int64_t log_ts)
{
  struct types::uint128_t next;
  next.hi = log_id;
  next.lo = log_ts;

  struct types::uint128_t last;
  while (true) {
    LOAD128(last, &next_replay_log_id_info_);
    if (CAS128(&next_replay_log_id_info_, last, next)) {
      break;
    } else {
      PAUSE();
    }
  }
}

void ObLogSlidingWindow::try_update_next_replay_log_info(
    const uint64_t log_id, const int64_t log_ts, const bool is_nop_or_truncate_log)
{
  // only follower call this function
  struct types::uint128_t next;
  next.hi = log_id;
  next.lo = log_ts;

  struct types::uint128_t last;
  while (true) {
    LOAD128(last, &next_replay_log_id_info_);
    if (next.hi <= last.hi && next.lo <= last.lo) {
      break;
    } else if (next.hi < last.hi || next.lo < last.lo) {
      if (!is_nop_or_truncate_log) {
        CLOG_LOG(ERROR,
            "next_replay_log_id_info_ non-atomic changed",
            K_(partition_key),
            K(last.hi),
            K(next.hi),
            K(last.lo),
            K(next.lo));
      }
      break;
    } else if (CAS128(&next_replay_log_id_info_, last, next)) {
      break;
    } else {
      PAUSE();
    }
  }

  record_last_update_next_replay_log_id_info_ts();
}

void ObLogSlidingWindow::try_update_next_replay_log_info_on_leader(const int64_t log_ts)
{
  // only leader call this function
  struct types::uint128_t last, next;
  try_update_submit_timestamp(log_ts);
  LOAD128(last, &next_replay_log_id_info_);
  next.hi = max_log_meta_info_.get_log_id() + 1;
  next.lo = log_ts;

  if (next.hi == last.hi && log_ts > last.lo) {
    (void)CAS128(&next_replay_log_id_info_, last, next);
  }
}

int ObLogSlidingWindow::process_sync_standby_max_confirmed_id(
    const uint64_t standby_max_confirmed_id, const uint64_t reconfirm_next_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_ID == standby_max_confirmed_id || OB_INVALID_ID == reconfirm_next_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(standby_max_confirmed_id), K(reconfirm_next_id));
  } else if (standby_max_confirmed_id < ATOMIC_LOAD(&next_index_log_id_)) {
    // no need process
  } else {
    const uint64_t max_log_id = max_log_meta_info_.get_log_id();
    uint64_t end_log_id = std::min(standby_max_confirmed_id, reconfirm_next_id - 1);
    end_log_id = std::min(end_log_id, max_log_id);
    for (uint64_t i = sw_.get_start_id(); i <= end_log_id; ++i) {
      ObILogExtRingBufferData* log_data = NULL;
      ObLogTask* log_task = NULL;
      const int64_t* ref = NULL;
      if (OB_SUCCESS != (tmp_ret = sw_.get(static_cast<int64_t>(i), log_data, ref))) {
        if (OB_ERROR_OUT_OF_RANGE != tmp_ret) {
          CLOG_LOG(WARN,
              "get log task from sliding window failed",
              K_(partition_key),
              K(tmp_ret),
              "start_log_id",
              sw_.get_start_id(),
              "current_log_id",
              i,
              "max_log_id",
              max_log_meta_info_.get_log_id());
        }
      } else if (NULL == (log_task = static_cast<ObLogTask*>(log_data))) {
        // log_task is NULL, skip
      } else {
        log_task->lock();
        if (GCTX.is_primary_cluster() && GCTX.need_sync_to_standby() &&
            !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
            log_task->is_submit_log_exist() && !log_task->is_standby_majority_finished()) {
          log_task->set_standby_majority_finished();
          CLOG_LOG(INFO,
              "log_task set_standby_majority_finished",
              K(standby_max_confirmed_id),
              K_(partition_key),
              K(reconfirm_next_id),
              K(max_log_id),
              "log_id",
              i,
              K(*log_task));
        }
        log_task->unlock();
      }
      if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
        CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
      } else {
        ref = NULL;
      }
    }
  }
  return ret;
}

int ObLogSlidingWindow::clean_log()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(state_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t need_reset_confirmed_info = false;
    for (uint64_t i = sw_.get_start_id(); i <= max_log_meta_info_.get_log_id(); ++i) {
      ObILogExtRingBufferData* log_data = NULL;
      ObLogTask* log_task = NULL;
      const int64_t* ref = NULL;
      if (OB_SUCCESS != (tmp_ret = sw_.get(static_cast<int64_t>(i), log_data, ref))) {
        CLOG_LOG(WARN,
            "get log task from sliding window failed",
            K_(partition_key),
            K(tmp_ret),
            "start_log_id",
            sw_.get_start_id(),
            "current_log_id",
            i,
            "max_log_id",
            max_log_meta_info_.get_log_id());
      } else if (NULL == (log_task = static_cast<ObLogTask*>(log_data))) {
        // begin clean confirm_info when it encounters empty log slot
        if (GCTX.is_standby_cluster() && !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
            !need_reset_confirmed_info) {
          need_reset_confirmed_info = true;
          CLOG_LOG(INFO, "log is NULL in sliding window", K_(partition_key), "log_id", i, K(need_reset_confirmed_info));
        } else {
          CLOG_LOG(
              DEBUG, "log is NULL in sliding window", K_(partition_key), "log_id", i, K(need_reset_confirmed_info));
        }
      } else {
        log_task->lock();
        if (GCTX.is_standby_cluster() && !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
            !need_reset_confirmed_info && !log_task->is_log_confirmed()) {
          // for standby non-private replica, begin clean confirm_info after first unconfirmed log_task is found
          need_reset_confirmed_info = true;
          CLOG_LOG(INFO,
              "found first not confirmed log_task, begin clear confirmed state",
              K_(partition_key),
              "log_id",
              i,
              K(*log_task));
        }
        log_task->reset_state(need_reset_confirmed_info);
        bool need_replay = false;
        if (!log_task->is_on_success_cb_called()) {
          need_replay = true;
          if (!log_task->is_on_finished_cb_called() &&
              (OB_SUCCESS != (tmp_ret = log_task->submit_log_finished_cb(partition_key_, i)))) {
            CLOG_LOG(ERROR, "submit_log_finished_cb failed", K_(partition_key), "log_id", i, K(tmp_ret));
          }
        }
        if (need_replay && state_mgr_->is_leader_active()) {
          CLOG_LOG(TRACE, "this log need replay", K_(partition_key), K(*log_task));
        }
        log_task->try_set_need_replay(need_replay);
        log_task->reset_submit_cb();
        log_task->unlock();
      }
      if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
        CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
      } else {
        ref = NULL;
      }
    }
    state_mgr_->reset_fetch_state();
    fake_ack_info_mgr_.reset();
  }
  return ret;
}

int ObLogSlidingWindow::restore_leader_try_confirm_log()
{
  int ret = OB_SUCCESS;
  uint64_t last_restore_log_id = OB_INVALID_ID;
  int64_t unused_version = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(
                 partition_service_->get_restore_replay_info(partition_key_, last_restore_log_id, unused_version))) {
    CLOG_LOG(WARN, "get_restore_replay_info failed", K_(partition_key), K(ret));
  } else if (OB_INVALID_ID == last_restore_log_id) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected last_restore_log_id", K_(partition_key), K(ret), K(last_restore_log_id));
  } else {
    const int64_t start_id = sw_.get_start_id();
    const int64_t MAX_PROCESS_CNT = DEFAULT_LEADER_MAX_UNCONFIRMED_LOG_COUNT;
    for (int64_t i = 0; i < MAX_PROCESS_CNT && OB_SUCC(ret); ++i) {
      int64_t log_id = start_id + i;
      if (ATOMIC_LOAD(&next_index_log_id_) > last_restore_log_id || log_id > last_restore_log_id) {
        CLOG_LOG(DEBUG, "there is no log to process", K_(partition_key), K(ret), K(log_id), K(last_restore_log_id));
        break;
      } else if (OB_FAIL(set_log_confirmed(log_id, false))) {
        CLOG_LOG(WARN, "set_log_confirmed failed", K_(partition_key), K(ret), K(log_id));
      } else {
        CLOG_LOG(INFO, "set_log_confirmed success", K_(partition_key), K(ret), K(log_id), K(last_restore_log_id));
      }
    }
  }
  return ret;
}

bool ObLogSlidingWindow::can_set_log_confirmed_(const ObLogTask* log_task) const
{
  bool bool_ret = false;
  if (NULL != log_task) {
    if (STANDBY_LEADER == state_mgr_->get_role() &&
        (!log_task->is_majority_finished() || !log_task->is_confirmed_info_exist())) {
      // standby_leader cannot set_log_confirmed before receive confirmed info or majority
      bool_ret = false;
      CLOG_LOG(DEBUG, "standby_leader cannot set_log_confirmed", K_(partition_key), K(*log_task));
    } else {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObLogSlidingWindow::set_log_confirmed(const uint64_t log_id, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sw_.get(static_cast<int64_t>(log_id), log_data, ref))) {
    CLOG_LOG(WARN, "get log task from sliding window failed", K_(partition_key), K(ret));
  } else if (NULL == (log_task = static_cast<ObLogTask*>(log_data))) {
    CLOG_LOG(WARN, "get NULL log from sliding window", K_(partition_key), K(log_id));
    ret = OB_ERR_UNEXPECTED;
  } else {
    log_task->lock();

    if (can_set_log_confirmed_(log_task)) {
      log_task->set_log_confirmed();
    }

    if (batch_committed) {
      log_task->set_batch_committed();
    }
    log_task->unlock();
  }
  bool need_check_succeeding_log = false;
  if (OB_SUCCESS != (tmp_ret = handle_first_index_log_(log_id, log_task, true, need_check_succeeding_log))) {
    CLOG_LOG(WARN, "handle_first_index_log_ failed", K_(partition_key), K(tmp_ret));
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
  } else {
    ref = NULL;
  }
  const bool do_pop = true;
  if (OB_SUCCESS != (tmp_ret = handle_succeeding_index_log_(log_id, need_check_succeeding_log, do_pop)) &&
      OB_ERROR_OUT_OF_RANGE != tmp_ret) {
    CLOG_LOG(WARN, "handle_succeeding_index_log_ failed", K_(partition_key), K(tmp_ret));
  }
  return ret;
}

int ObLogSlidingWindow::get_log(const uint64_t log_id, const uint32_t log_attr, bool& log_confirmed,
    ObLogCursor& log_cursor, int64_t& accum_checksum, bool& batch_committed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ERROR_OUT_OF_RANGE == (ret = sw_.get(static_cast<int64_t>(log_id), log_data, ref))) {
    CLOG_LOG(INFO,
        "ObLogSlidingWindow::get_log(), log_id is too small to be got from sliding window",
        K_(partition_key),
        K(log_id),
        "start_id",
        sw_.get_start_id());
  } else if (OB_FAIL(ret)) {
    CLOG_LOG(ERROR, "get log from sliding window failed,", K_(partition_key), K(ret));
  } else if (NULL == (log_task = static_cast<ObLogTask*>(log_data))) {
    ret = OB_ERR_NULL_VALUE;
    CLOG_LOG(DEBUG,
        "ObLogSlidingWindow::get_log(), log in sliding window is NULL",
        K_(partition_key),
        K(ret),
        K(log_id),
        "start_id",
        sw_.get_start_id());
  } else if ((NEED_CONFIRMED == log_attr) && !log_task->is_log_confirmed()) {
    // no need lock log_task when check log_confirmed state
    // Because the reset log_confirmed state operation is only performed in the upper write lock,
    // and the judgment logic here is mutually exclusive
    ret = OB_EAGAIN;
    CLOG_LOG(INFO,
        "log in sliding_window is not confirmed, try again",
        K(ret),
        K_(partition_key),
        K(log_id),
        K(log_attr),
        K(*log_task));
  } else if (NEED_MAJORITY == log_attr && (!log_task->is_local_majority_flushed() && !log_task->is_log_confirmed())) {
    // no need lock log_task when check log_confirmed state
    // Because the reset log_confirmed state operation is only performed in the upper write lock,
    // and the judgment logic here is mutually exclusive
    ret = OB_EAGAIN;
    CLOG_LOG(INFO,
        "log in sliding_window is not local majority flushed, try again",
        K(ret),
        K_(partition_key),
        K(log_id),
        K(log_attr),
        K(*log_task));
  } else if (log_task->is_pinned()) {
    ret = OB_EAGAIN;
  } else {
    log_task->lock();
    if (!log_task->is_submit_log_exist()) {
      ret = OB_ERR_NULL_VALUE;
      CLOG_LOG(DEBUG,
          "ObLogSlidingWindow::get_log(), log in sliding window is NULL",
          K(ret),
          K_(partition_key),
          K(ret),
          K(log_id),
          "start_id",
          sw_.get_start_id());
    } else if (log_task->is_flush_local_finished()) {
      if (OB_SUCCESS != (tmp_ret = log_cursor.deep_copy(log_task->get_log_cursor()))) {
        CLOG_LOG(ERROR, "deep_copy failed", K_(partition_key), K(tmp_ret));
      }
      log_confirmed = (log_id < ATOMIC_LOAD(&next_index_log_id_));
      if (log_confirmed) {
        accum_checksum = log_task->get_accum_checksum();
      }
      ret = OB_LOG_MISSING;
    } else {
      ret = OB_LOG_MISSING;
    }
    batch_committed = log_task->is_batch_committed();
    log_task->unlock();
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
  } else {
    ref = NULL;
  }
  return ret;
}

bool ObLogSlidingWindow::is_confirm_match_(const uint64_t log_id, const int64_t log_data_checksum,
    const int64_t log_epoch_id, const int64_t confirmed_info_data_checksum, const int64_t confirmed_info_epoch_id)
{
  bool bret = false;
  if ((log_data_checksum != confirmed_info_data_checksum) || (log_epoch_id != confirmed_info_epoch_id)) {
    CLOG_LOG(WARN,
        "confirm log not match",
        K_(partition_key),
        K(log_id),
        K(log_data_checksum),
        K(log_epoch_id),
        K(confirmed_info_data_checksum),
        K(confirmed_info_epoch_id));
  } else {
    bret = true;
  }
  return bret;
}

bool ObLogSlidingWindow::can_transfer_confirmed_info_(const uint64_t log_id)
{
  bool bool_ret = true;
  if (STANDBY_LEADER == state_mgr_->get_role()) {
    // standby_leader cannot transfer confirm_info directly
    // it must guarantee continuously confirmed
    if (get_next_index_log_id() <= log_id) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

// follower send confirmed log to standby children
// same with set_log_flushed_succ()
int ObLogSlidingWindow::follower_send_log_to_standby_children_(const uint64_t log_id, ObLogTask* log_task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == log_task) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), KP(log_task));
  } else if (FOLLOWER != state_mgr_->get_role() ||
             !cascading_mgr_->has_valid_async_standby_child()) {  // follower's standby children must be async
  } else {
    log_task->lock();
    if (is_follower_need_send_log_to_standby_(log_task) && !log_task->is_already_send_to_standby()) {
      if (OB_FAIL(send_log_to_standby_cluster_(log_id, log_task))) {
        CLOG_LOG(WARN, "send_log_to_standby_cluster_ failed", K(ret), K_(partition_key), K(log_id));
      } else {
        // mark tag to avoid resending
        log_task->set_already_send_to_standby();
        CLOG_LOG(TRACE, "set_already_send_to_standby", K_(partition_key), K(ret), K(log_id), K(*log_task));
      }
    } else {
      CLOG_LOG(DEBUG, "follower cannot send log to standby", K_(partition_key), K(ret), K(log_id), K(*log_task));
    }
    log_task->unlock();
  }
  return ret;
}

int ObLogSlidingWindow::receive_confirmed_info(
    const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  bool can_receive_log = true;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(replay_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "replay_engine_ is NULL", K(log_id), K_(partition_key), KR(ret));
  } else if (OB_FAIL(replay_engine_->check_can_receive_log(partition_key_, can_receive_log))) {
    CLOG_LOG(WARN, "failed to check_can_receive_log", K(log_id), K_(partition_key), KR(ret));
  } else if (!can_receive_log) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "can not receive log now", K(log_id), K_(partition_key), KR(ret));
    }
  } else if (OB_FAIL(submit_confirmed_info_(log_id, confirmed_info, false, batch_committed))) {
    CLOG_LOG(WARN,
        "submit_confirmed_info_ failed",
        K_(partition_key),
        K(ret),
        K(log_id),
        K(confirmed_info),
        K(batch_committed));
    // The reason why follower_send_log_to_standby_children_ is no longer called here is that the log may have popped
    // out at this time, It will cause the sending to fail, so the sending logic is placed in the submit_index_log
    // process
  } else if (OB_SUCCESS !=
             (tmp_ret = follower_transfer_confirmed_info_to_net_(log_id, confirmed_info, batch_committed))) {
    // follower can transfer confirmed_info to children unconditionally
    CLOG_LOG(WARN,
        "follower_transfer_confirmed_info_to_net_ failed",
        K_(partition_key),
        K(tmp_ret),
        K(log_id),
        K(confirmed_info),
        K(batch_committed));
  } else if (OB_FAIL(try_update_max_log_id(log_id))) {
    CLOG_LOG(ERROR, "try_update_max_log_id failed", K_(partition_key), K(ret), K(log_id));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogSlidingWindow::leader_submit_confirmed_info_(
    const uint64_t log_id, const ObLogTask* log_task, const int64_t accum_checksum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_task)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObConfirmedInfo confirmed_info;
    log_task->lock();
    const int64_t data_checksum = log_task->get_data_checksum();
    const int64_t epoch_id = log_task->get_epoch_id();
    const bool batch_committed = log_task->is_batch_committed();
    log_task->unlock();
    if (OB_FAIL(confirmed_info.init(data_checksum, epoch_id, accum_checksum))) {
      CLOG_LOG(ERROR, "confirmed_info init failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(submit_confirmed_info_(log_id, confirmed_info, true, batch_committed))) {
      CLOG_LOG(WARN,
          "submit_confirmed_info_ failed",
          K_(partition_key),
          K(ret),
          K(log_id),
          K(confirmed_info),
          K(batch_committed));
    } else if (OB_FAIL(submit_confirmed_info_to_net_(log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN,
          "submit_confirmed_info_to_net_ failed",
          K_(partition_key),
          K(ret),
          K(log_id),
          K(confirmed_info),
          K(batch_committed));
    }
  }
  return ret;
}

int ObLogSlidingWindow::standby_leader_transfer_confirmed_info_(const uint64_t log_id, const ObLogTask* log_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_task)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (STANDBY_LEADER != state_mgr_->get_role()) {
    // not standby_leader, skip
  } else {
    ObConfirmedInfo confirmed_info;
    log_task->lock();
    const int64_t data_checksum = log_task->get_data_checksum();
    const int64_t epoch_id = log_task->get_epoch_id();
    const int64_t accum_checksum = log_task->get_accum_checksum();
    const bool batch_committed = log_task->is_batch_committed();
    log_task->unlock();
    if (OB_FAIL(confirmed_info.init(data_checksum, epoch_id, accum_checksum))) {
      CLOG_LOG(ERROR, "confirmed_info init failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(submit_confirmed_info_to_net_(log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN,
          "submit_confirmed_info_to_net_ failed",
          K_(partition_key),
          K(ret),
          K(log_id),
          K(confirmed_info),
          K(batch_committed));
    } else {
      CLOG_LOG(DEBUG,
          "standby_leader_transfer_confirmed_info_ success",
          K_(partition_key),
          K(ret),
          K(log_id),
          K(confirmed_info),
          K(batch_committed));
    }
  }
  return ret;
}

int ObLogSlidingWindow::append_disk_log(const ObLogEntry& log_entry, const ObLogCursor& log_cursor,
    const int64_t accum_checksum, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  const uint64_t log_id = log_entry.get_header().get_log_id();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    if (OB_FAIL(append_disk_log_to_sliding_window_(log_entry, log_cursor, accum_checksum, batch_committed))) {
      CLOG_LOG(
          WARN, "append_disk_log_to_sliding_window_ failed", K_(partition_key), K(ret), K(log_entry), K(log_cursor));
    } else if (OB_FAIL(try_update_max_log_id(log_id))) {
      CLOG_LOG(ERROR, "try_update_max_log_id failed", K_(partition_key), K(ret), K(log_id));
    } else {
    }
  }
  return ret;
}

int ObLogSlidingWindow::get_next_replay_log_timestamp(int64_t& next_replay_log_timestamp) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    struct types::uint128_t next_log_id_info;
    LOAD128(next_log_id_info, &next_replay_log_id_info_);
    next_replay_log_timestamp = next_log_id_info.lo;
  }

  return ret;
}

int ObLogSlidingWindow::set_log_flushed_succ(const uint64_t log_id, const ObProposalID proposal_id,
    const ObLogCursor& log_cursor, const int64_t after_consume_timestamp, bool& majority)
{
  UNUSED(after_consume_timestamp);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    if (NULL != aggre_buffer_ && state_mgr_->is_leader_active() && proposal_id == state_mgr_->get_proposal_id()) {
      if (OB_FAIL(submit_freeze_aggre_buffer_task_(log_id))) {
        CLOG_LOG(WARN, "submit flush aggre buffer task failed", K(ret), K(log_id));
        // rewrite ret
        ret = OB_SUCCESS;
      }
    }
    if (OB_FAIL(sw_.get(log_id, log_data, ref))) {
      if (OB_ERROR_OUT_OF_RANGE != ret) {
        CLOG_LOG(WARN, "get log from sliding window failed", K_(partition_key), K(ret), K(log_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (NULL == (log_task = static_cast<ObLogTask*>(log_data))) {
      ret = OB_ERR_NULL_VALUE;
      CLOG_LOG(ERROR, "log in sliding window is NULL", K_(partition_key), K(ret));
    } else {
      log_task->lock();
      if (!log_task->is_submit_log_exist()) {
        // do nothing
      } else if (proposal_id != log_task->get_proposal_id()) {
        CLOG_LOG(INFO,
            "log is overwritten during disk flushing",
            K_(partition_key),
            "prev proposal id",
            proposal_id,
            "current id",
            log_task->get_proposal_id(),
            K(log_id));
        // } else if (NULL != log_task->get_trace_profile()
        //           && (OB_FAIL(log_task->get_trace_profile()->trace(partition_key_, ON_AFTER_CONSUME,
        //                                                            after_consume_timestamp))
        //               || OB_FAIL(log_task->get_trace_profile()->trace(partition_key_, ON_FLUSH_CB)))) {
        //  CLOG_LOG(ERROR, "trace failed", K_(partition_key), K(ret));
      } else {
        log_task->set_flush_local_finished();
        log_task->set_log_cursor(log_cursor);
        bool need_send_to_standby = false;
        if (is_mp_leader_waiting_standby_ack_(log_task)) {
          // leader does not receive standby ack
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            CLOG_LOG(INFO, "not receive standby_ack, need wait", K_(partition_key), K(log_id), K(*log_task));
          }
          if (log_task->is_local_majority_flushed()) {
            need_send_to_standby = true;
          }
        } else {
          majority = log_task->try_set_majority_finished();
        }

        if (!need_send_to_standby && is_primary_need_send_log_to_standby_(log_task)) {
          need_send_to_standby = true;
        }
        if (need_send_to_standby && !log_task->is_already_send_to_standby()) {
          if (OB_SUCCESS != (tmp_ret = send_log_to_standby_cluster_(log_id, log_task))) {
            CLOG_LOG(WARN, "send_log_to_standby_cluster_ failed", K(tmp_ret), K_(partition_key), K(log_id));
          } else {
            log_task->set_already_send_to_standby();
            CLOG_LOG(TRACE, "set_already_send_to_standby", K_(partition_key), K(ret), K(log_id), K(*log_task));
            if (OB_SUCCESS != (tmp_ret = send_confirmed_info_to_standby_children_(log_id, log_task))) {
              CLOG_LOG(
                  WARN, "send_confirmed_info_to_standby_children_ failed", K(tmp_ret), K_(partition_key), K(log_id));
            }
          }
        }
      }
      log_task->unlock();
    }
  }
  bool need_check_succeeding_log = false;
  if (OB_SUCCESS != (tmp_ret = handle_first_index_log_(log_id, log_task, true, need_check_succeeding_log))) {
    CLOG_LOG(WARN, "handle_first_index_log_ failed", K_(partition_key), K(tmp_ret));
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
  } else {
    ref = NULL;
  }
  const bool do_pop = true;
  if (OB_SUCCESS != (tmp_ret = handle_succeeding_index_log_(log_id, need_check_succeeding_log, do_pop)) &&
      OB_ERROR_OUT_OF_RANGE != tmp_ret) {
    CLOG_LOG(WARN, "handle_succeeding_index_log_ failed", K_(partition_key), K(tmp_ret));
  }
  if (OB_SUCC(ret)) {
    CLOG_LOG(DEBUG, "set log flushed succ", K_(partition_key), K(log_id), K(proposal_id), K(log_cursor));
  } else {
    CLOG_LOG(WARN, "set log flushed failed", K_(partition_key), K(ret), K(log_id), K(proposal_id), K(log_cursor));
  }
  return ret;
}

int ObLogSlidingWindow::send_standby_log_ack_(
    const ObAddr& server, const int64_t cluster_id, const uint64_t log_id, const ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_INVALID_CLUSTER_ID == cluster_id || OB_INVALID_ID == log_id ||
             !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K_(partition_key), K(server), K(cluster_id), K(log_id), K(proposal_id));
  } else if (OB_FAIL(log_engine_->submit_standby_log_ack(server, cluster_id, partition_key_, log_id, proposal_id))) {
    CLOG_LOG(WARN,
        "submit_standby_log_ack failed",
        K(ret),
        K(server),
        K(cluster_id),
        K(log_id),
        K(proposal_id),
        K_(partition_key));
  } else {
    CLOG_LOG(DEBUG,
        "submit_standby_log_ack success",
        K(ret),
        K(server),
        K(cluster_id),
        K(log_id),
        K(proposal_id),
        K_(partition_key));
  }
  return ret;
}

int ObLogSlidingWindow::receive_log_(const ObLogEntry& log_entry, const ObAddr& server, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const bool need_replay = true;
  bool can_receive_log = true;
  const bool is_confirmed = false;
  const int64_t unused_accum_checksum = 0;
  const bool unused_is_batch_committed = false;
  bool send_slave = false;
  if (STANDBY_LEADER == state_mgr_->get_role()) {
    // standby leader need send log to member_list
    send_slave = true;
  }
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_ISNULL(log_engine_) || OB_ISNULL(replay_engine_)) {
    CLOG_LOG(WARN, "invlaid argument", KP(log_engine_), K(server), KP_(replay_engine));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(replay_engine_->check_can_receive_log(partition_key_, can_receive_log))) {
    CLOG_LOG(WARN, "failed to check_can_receive_log", K(log_entry), K_(partition_key), KR(ret));
  } else if (!can_receive_log) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "can not receive log now", K(log_entry), K_(partition_key), KR(ret));
    }
  } else if (OB_FAIL(submit_to_sliding_window_(log_entry.get_header(),
                 log_entry.get_buf(),
                 NULL,
                 need_replay,
                 send_slave,
                 server,
                 cluster_id,
                 is_confirmed,
                 unused_accum_checksum,
                 unused_is_batch_committed))) {
    if (OB_ERROR_OUT_OF_RANGE == ret && log_id < static_cast<uint64_t>(sw_.get_start_id())) {
      ret = OB_SUCCESS;
      ObProposalID proposal_id = log_entry.get_header().get_proposal_id();
      if (FOLLOWER == state_mgr_->get_role()) {
        const ObAddr leader = state_mgr_->get_leader();
        const int64_t leader_cluster_id = state_mgr_->get_leader_cluster_id();
        if (state_mgr_->can_send_log_ack(proposal_id) && server == leader &&
            OB_FAIL(log_engine_->submit_log_ack(server, leader_cluster_id, partition_key_, log_id, proposal_id))) {
          CLOG_LOG(WARN, "submit_log_ack failed", K_(partition_key), K(ret), K(server), K(log_id));
        }
      } else if (STANDBY_LEADER == state_mgr_->get_role()) {
        if (OB_FAIL(send_standby_log_ack_(server, cluster_id, log_id, proposal_id))) {
          CLOG_LOG(WARN,
              "send_standby_log_ack_ failed",
              K(ret),
              K_(partition_key),
              K(server),
              K(cluster_id),
              K(log_id),
              K(proposal_id));
        }
      } else {
      }
    } else {
      CLOG_LOG(WARN, "submit_to_sliding_window_ failed", K_(partition_key), K(ret));
    }
  } else if (OB_FAIL(try_update_max_log_id(log_id))) {
    CLOG_LOG(ERROR, "try_update_max_log_id failed", K_(partition_key), K(ret), K(log_id));
  } else {
  }  // do nothing
  return ret;
}

int ObLogSlidingWindow::receive_recovery_log(
    const ObLogEntry& log_entry, const bool is_confirmed, const int64_t accum_checksum, const bool is_batch_committed)
{
  int ret = OB_SUCCESS;
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const bool send_slave = false;
  const bool need_replay = true;
  bool can_receive_log = true;
  const ObAddr dummy_server;
  const int64_t dummy_cluster_id = state_mgr_->get_self_cluster_id();
  const ObPushLogMode fake_push_mode = PUSH_LOG_ASYNC;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(log_entry), KR(ret));
  } else if (OB_ISNULL(log_engine_) || OB_ISNULL(replay_engine_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invlaid argument", KP(log_engine_), KP_(replay_engine), KR(ret));
  } else if (OB_FAIL(replay_engine_->check_can_receive_log(partition_key_, can_receive_log))) {
    CLOG_LOG(WARN, "failed to check_can_receive_log", K(log_entry), K_(partition_key), KR(ret));
  } else if (!can_receive_log) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "can not receive log now", K(log_entry), K_(partition_key), KR(ret));
    }
  } else if (OB_FAIL(submit_to_sliding_window_(log_entry.get_header(),
                 log_entry.get_buf(),
                 NULL,
                 need_replay,
                 send_slave,
                 dummy_server,
                 dummy_cluster_id,
                 is_confirmed,
                 accum_checksum,
                 is_batch_committed))) {
    CLOG_LOG(WARN, "failed to submit_to_sliding_window_", K_(partition_key), K(log_entry), KR(ret));
  } else if (OB_FAIL(try_update_max_log_id(log_id))) {
    CLOG_LOG(ERROR, "try_update_max_log_id failed", K_(partition_key), K(ret), K(log_id));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogSlidingWindow::receive_log(
    const ObLogEntry& log_entry, const ObAddr& server, const int64_t cluster_id, const ReceiveLogType type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || type <= RL_TYPE_UNKNOWN || type > FETCH_LOG) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(log_entry), K(server), K(type));
  } else if (OB_FAIL(receive_log_(log_entry, server, cluster_id))) {
    CLOG_LOG(WARN, "receive_log_ failed", K_(partition_key), K(ret), K(log_entry), K(server), K(type));
  }
  if (OB_SUCCESS == ret && PUSH_LOG == type) {
    if (OB_FAIL(state_mgr_->try_reset_fetch_state(log_entry.get_header().get_log_id()))) {
      CLOG_LOG(ERROR,
          "try_reset_fetch_state failed",
          K(ret),
          K(partition_key_),
          "log_id",
          log_entry.get_header().get_log_id());
    }
  }
  return ret;
}

int ObLogSlidingWindow::majority_cb(
    const uint64_t log_id, const bool batch_committed, const bool batch_first_participant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;
  BG_MONITOR_GUARD_DEFAULT(1000 * 1000);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sw_.get(log_id, log_data, ref))) {
  } else if (log_data == NULL) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    log_task = static_cast<ObLogTask*>(log_data);
    try_update_max_majority_log(log_id, log_task->get_submit_timestamp());
    if (OB_FAIL(log_task->submit_log_succ_cb(partition_key_, log_id, batch_committed, batch_first_participant))) {
      CLOG_LOG(WARN, "submit log majority_cb failed", K(ret), K_(partition_key), K(log_id), K(batch_committed));
      ret = OB_SUCCESS;
    }
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
  } else {
    ref = NULL;
  }
  return ret;
}

bool ObLogSlidingWindow::is_freeze_log_(const char* log_buf, const int64_t log_buf_len, int64_t& log_type) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(serialization::decode_i64(log_buf, log_buf_len, pos, &log_type))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K_(partition_key));
  } else {
    bool_ret = storage::ObStorageLogTypeChecker::is_freeze_log(log_type);
    CLOG_LOG(DEBUG, "is freee log", K(ret), K_(partition_key), K(log_type));
  }
  return bool_ret;
}

bool ObLogSlidingWindow::is_change_pg_log_(const char* log_buf, const int64_t log_buf_len, int64_t& log_type) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(serialization::decode_i64(log_buf, log_buf_len, pos, &log_type))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K_(partition_key));
  } else {
    bool_ret = (storage::ObStorageLogTypeChecker::is_add_partition_to_pg_log(log_type) ||
                storage::ObStorageLogTypeChecker::is_remove_partition_from_pg_log(log_type));
    CLOG_LOG(DEBUG, "is change pg log", K(ret), K_(partition_key), K(log_type));
  }
  return bool_ret;
}

bool ObLogSlidingWindow::is_offline_partition_log_(
    const char* log_buf, const int64_t log_buf_len, int64_t& log_type) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(serialization::decode_i64(log_buf, log_buf_len, pos, &log_type))) {
    CLOG_LOG(WARN, "deserialize failed", K(ret), K_(partition_key));
  } else {
    bool_ret = storage::ObStorageLogTypeChecker::is_offline_partition_log(log_type);
    CLOG_LOG(DEBUG, "is offline partition log", K(ret), K_(partition_key), K(log_type));
  }
  return bool_ret;
}

int ObLogSlidingWindow::get_replica_replay_type(ObReplicaReplayType& replay_type) const
{
  int ret = OB_SUCCESS;
  replay_type = INVALID_REPLICA_REPLAY_TYPE;
  if (OB_ISNULL(mm_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid argument", KP_(mm), K(ret));
  } else {
    const int32_t replica_type = mm_->get_replica_type();
    if ((REPLICA_TYPE_FULL == replica_type || REPLICA_TYPE_READONLY == replica_type)) {
      const ObReplicaProperty replica_property = mm_->get_replica_property();
      if (replica_property.get_memstore_percent() > 0) {
        replay_type = REPLICA_REPLAY_ALL_LOG;
      } else {
        // data replica
        if (state_mgr_->is_leader_taking_over()) {
          replay_type = REPLICA_REPLAY_ALL_LOG;
        } else {
          replay_type = REPLICA_REPLAY_PARTITIAL_LOG;
        }
      }
    } else if (REPLICA_TYPE_LOGONLY == replica_type) {
      replay_type = REPLICA_REPLAY_PARTITIAL_LOG;
    } else {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "not supported replica type", K(replica_type), KR(ret));
    }
  }
  return ret;
}

int ObLogSlidingWindow::need_replay_for_data_or_log_replica_(const bool is_trans_log, bool& need_replay) const
{
  int ret = OB_SUCCESS;
  ObReplicaReplayType replay_type = INVALID_REPLICA_REPLAY_TYPE;
  if (OB_FAIL(get_replica_replay_type(replay_type))) {
    CLOG_LOG(WARN, "failed to get replica replay type", K(replay_type), KR(ret));
  } else if (REPLICA_REPLAY_ALL_LOG == replay_type) {
    need_replay = true;
  } else if (REPLICA_REPLAY_PARTITIAL_LOG == replay_type) {
    need_replay = !is_trans_log;
  } else {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected replay type", K(replay_type), KR(ret));
  }
  return ret;
}

int ObLogSlidingWindow::get_log_meta_info(uint64_t log_id, bool& is_meta_log, int64_t& log_ts,
    int64_t& next_replay_log_ts_for_rg, int64_t& accum_checksum, ObLogType& log_type) const
{
  int ret = OB_SUCCESS;
  is_meta_log = false;
  ObICLogMgr* clog_mgr = NULL;
  if (OB_ISNULL(partition_service_) || OB_ISNULL(clog_mgr = partition_service_->get_clog_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid argument", K(partition_key_), K(log_id), KP(partition_service_), KP(clog_mgr), KR(ret));
  } else {
    clog::ObLogEntry log_entry;
    bool is_batch_committed = false;
    if (OB_FAIL(clog_mgr->query_log_info_with_log_id(
            partition_key_, log_id, log_entry, accum_checksum, is_batch_committed))) {
      if (OB_EAGAIN == ret) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          CLOG_LOG(WARN, "failed to query_log_info_with_log_id ", K(partition_key_), K(log_id), K(ret));
        }
      } else {
        CLOG_LOG(WARN, "failed to query_log_info_with_log_id ", K(partition_key_), K(log_id), K(ret));
      }
    } else if (OB_FAIL(log_entry.get_next_replay_ts_for_rg(next_replay_log_ts_for_rg))) {
    } else {
      log_type = log_entry.get_header().get_log_type();
      log_ts = log_entry.get_header().get_submit_timestamp();
      if (OB_LOG_SUBMIT == log_type) {
        int64_t pos = 0;
        int64_t log_type_in_buf = storage::OB_LOG_UNKNOWN;
        if (OB_FAIL(serialization::decode_i64(
                log_entry.get_buf(), log_entry.get_header().get_data_len(), pos, &log_type_in_buf))) {
          CLOG_LOG(WARN, "deserialize failed", KR(ret), K_(partition_key));
        } else {
          storage::ObStorageLogType storage_log_type = static_cast<storage::ObStorageLogType>(log_type_in_buf);
          if (storage::ObStorageLogTypeChecker::is_meta_log(storage_log_type)) {
            is_meta_log = true;
          } else {
            is_meta_log = false;
          }
        }
      } else if (OB_LOG_AGGRE == log_type) {
        is_meta_log = false;
      } else {
        is_meta_log = true;
      }
    }
  }
  return ret;
}

int ObLogSlidingWindow::try_submit_replay_task_(const uint64_t log_id, const ObLogTask& log_task)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", KR(ret), K_(partition_key), K(log_id), K(log_task));
  } else if (OB_ISNULL(state_mgr_) || OB_ISNULL(replay_engine_) || OB_ISNULL(log_engine_) || OB_ISNULL(checksum_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        KR(ret),
        K_(partition_key),
        K(log_task),
        KP_(state_mgr),
        KP_(replay_engine),
        KP_(log_engine),
        KP_(checksum));
  } else {
    const ObLogType header_log_type = log_task.get_log_type();
    const int64_t log_submit_timestamp = log_task.get_submit_timestamp();
    const int64_t next_replay_log_ts = log_task.get_next_replay_log_ts();
    bool need_replay = log_task.need_replay();
    const bool is_trans_log = log_task.is_trans_log();
    uint64_t last_replay_log_id = OB_INVALID_ID;
    int64_t last_replay_log_ts = OB_INVALID_TIMESTAMP;
    get_last_replay_log(last_replay_log_id, last_replay_log_ts);
    if ((OB_INVALID_ID == last_replay_log_id) || (OB_INVALID_TIMESTAMP == last_replay_log_ts) ||
        (log_id <= last_replay_log_id) || (log_submit_timestamp < last_replay_log_ts)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR,
          "the log shouldn't submit to replay engine",
          KR(ret),
          K_(partition_key),
          K(last_replay_log_id),
          K(last_replay_log_ts),
          K(log_id),
          K(log_submit_timestamp),
          K(log_task));
    } else if (!need_replay) {
      // skip logs those do not need replay
    } else if (OB_LOG_MEMBERSHIP == header_log_type || OB_LOG_START_MEMBERSHIP == header_log_type) {
      ObReadBuf rbuf;
      const char* log_buf = NULL;
      int64_t log_buf_len = 0;
      if (log_task.is_submit_log_body_exist()) {
        log_buf = log_task.get_log_buf();
        log_buf_len = log_task.get_log_buf_len();
      } else {
        ObLogEntry tmp_entry;
        // here we use alloc_buf instead of ObReadBufGuard because allcating may not be need where
        // submit_log_body exists
        if (OB_FAIL(ObILogDirectReader::alloc_buf(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID, rbuf))) {
          CLOG_LOG(WARN, "failed to alloc read_buf", K_(partition_key), K(log_id), K(log_task), K(ret));
        } else if (OB_FAIL(log_engine_->read_log_by_location(log_task, rbuf, tmp_entry))) {
          CLOG_LOG(WARN, "read_log_by_location failed", K_(partition_key), K(ret), K(log_task));
        } else if (tmp_entry.get_header().get_partition_key() != partition_key_) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "read_log_by_location wrong log", K_(partition_key), K(ret), K(log_task), K(tmp_entry));
        } else {
          log_buf = tmp_entry.get_buf();
          log_buf_len = tmp_entry.get_header().get_data_len();
        }
      }
      if (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_LOG_MEMBERSHIP == header_log_type) {
          need_replay = false;
          // membership log callback
          if (OB_SUCCESS != (tmp_ret = try_submit_mc_success_cb_(
                                 header_log_type, log_id, log_buf, log_buf_len, log_task.get_proposal_id()))) {
            CLOG_LOG(WARN, "try_submit_mc_success_cb_ failed", K(tmp_ret), K(partition_key_), K(log_id));
          }
        } else if (OB_LOG_START_MEMBERSHIP == header_log_type) {
          need_replay = true;
          CLOG_LOG(TRACE, "submit replay success", K(ret), K_(partition_key), K(log_id));
          // new primary cluster leader rely on start_woring callback to flush member_list to pg_meta
          if (!ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
              OB_SUCCESS != (tmp_ret = try_submit_mc_success_cb_(
                                 header_log_type, log_id, log_buf, log_buf_len, log_task.get_proposal_id()))) {
            CLOG_LOG(WARN, "try_submit_mc_success_cb_ failed", K(tmp_ret), K(partition_key_), K(log_id));
          }
        } else {
        }
      }
      // attention: free buf here
      (void)ObILogDirectReader::free_buf(rbuf);
    } else if (!is_log_type_need_replay(header_log_type)) {
      // this must be place after checking OB_LOG_MEMBERSHIP == header_log_type
      need_replay = false;
      CLOG_LOG(INFO,
          "try_submit_replay_task_ , no need to replay this type",
          K(need_replay),
          K_(partition_key),
          K(log_id),
          K(log_task));
    } else if (OB_FAIL(need_replay_for_data_or_log_replica_(is_trans_log, need_replay))) {
      CLOG_LOG(WARN,
          "failed to check need_replay_for_data_or_log_replica_",
          KR(ret),
          K(partition_key_),
          K(log_id),
          K(is_trans_log));
    } else { /*do nothing*/
    }

    if (OB_SUCC(ret)) {
      if (state_mgr_->is_offline()) {
        CLOG_LOG(
            WARN, "no need to submit log to replay when partition is offline", KR(ret), K(partition_key_), K(log_id));
      } else if (OB_FAIL(replay_engine_->submit_replay_log_task_sequentially(
                     partition_key_, log_id, log_submit_timestamp, need_replay, header_log_type, next_replay_log_ts))) {
        if (OB_EAGAIN != ret) {
          CLOG_LOG(WARN,
              "failed to submit replay task",
              K_(partition_key),
              K(ret),
              K(log_task),
              K(header_log_type),
              K(log_id));
        }
      } else {
      }
    }
  }
  return ret;
}

int ObLogSlidingWindow::follower_update_leader_next_log_info(
    const uint64_t leader_next_log_id, const int64_t leader_next_log_ts)
{
  int ret = OB_SUCCESS;
  // follower fetch leader next log info, already check inited
  const int32_t replica_type = mm_->get_replica_type();
  const ObAddr leader = state_mgr_->get_leader();
  const ObAddr parent = cascading_mgr_->get_parent_addr();

  if (OB_INVALID_ID == leader_next_log_id || OB_INVALID_TIMESTAMP == leader_next_log_ts) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K_(partition_key),
        K(ret),
        K(replica_type),
        K(leader),
        K(parent),
        K(leader_next_log_id),
        K(leader_next_log_ts));
  } else {
    uint64_t curr_leader_next_log_id = OB_INVALID_ID;
    int64_t curr_leader_next_log_ts = OB_INVALID_TIMESTAMP;
    get_next_replay_log_id_info(curr_leader_next_log_id, curr_leader_next_log_ts);

    if (leader_next_log_id == (get_last_replay_log_id() + 1) && leader_next_log_ts >= curr_leader_next_log_ts) {
      try_update_next_replay_log_info(leader_next_log_id, leader_next_log_ts);
      try_update_submit_timestamp(leader_next_log_ts);
      CLOG_LOG(DEBUG,
          "follower_update_leader_next_log_info next_log_ts",
          K_(partition_key),
          K(leader_next_log_id),
          K(curr_leader_next_log_id),
          K(curr_leader_next_log_ts),
          K(leader_next_log_ts));

      if (partition_reach_time_interval(1000 * 1000, sync_replica_reset_fetch_state_time_)) {
        state_mgr_->reset_fetch_state();
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObLogSlidingWindow::get_switchover_info(
    int64_t& switchover_epoch, uint64_t& leader_max_log_id, int64_t& leader_next_log_ts) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSlidingWindow is not init", K(ret), K_(partition_key));
  } else {
    switchover_info_lock_.lock();
    switchover_epoch = leader_max_log_info_.get_switchover_epoch();
    leader_max_log_id = leader_max_log_info_.get_max_log_id();
    leader_next_log_ts = leader_max_log_info_.get_next_log_ts();
    switchover_info_lock_.unlock();
  }
  return ret;
}

int ObLogSlidingWindow::follower_update_leader_max_log_info(
    const int64_t switchover_epoch, const uint64_t leader_max_log_id, const int64_t leader_next_log_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSlidingWindow is not init", K(ret), K_(partition_key));
  } else if (switchover_epoch <= 0 || leader_max_log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(switchover_epoch), K(leader_max_log_id));
  } else if (switchover_epoch < leader_max_log_info_.get_switchover_epoch()) {
    CLOG_LOG(WARN,
        "recv smaller switchover_epoch",
        K_(partition_key),
        K(switchover_epoch),
        K(leader_max_log_id),
        K(leader_next_log_ts),
        K_(leader_max_log_info));
  } else {
    switchover_info_lock_.lock();
    leader_max_log_info_.set_switchover_epoch(switchover_epoch);
    leader_max_log_info_.set_max_log_id(leader_max_log_id);
    leader_max_log_info_.set_next_log_ts(leader_next_log_ts);
    switchover_info_lock_.unlock();
    CLOG_LOG(DEBUG,
        "follower_update_leader_max_log_info succ",
        K_(partition_key),
        K(switchover_epoch),
        K(leader_max_log_id),
        K(leader_next_log_ts));
  }
  return ret;
}

// check if standby_leader need fetch log
bool ObLogSlidingWindow::is_standby_leader_need_fetch_log_(const uint64_t start_log_id)
{
  bool bool_ret = true;
  if (IS_NOT_INIT) {
  } else if (OB_INVALID_ID == start_log_id) {
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(start_log_id));
  } else if (STANDBY_LEADER != state_mgr_->get_role()) {
    // not stadnby_leader, skip
  } else {
    ObLogTask* log_task = NULL;
    const int64_t* ref = NULL;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = get_log_task(start_log_id, log_task, ref))) {
      // slide out or null
    } else if (log_task->is_submit_log_exist() && log_task->is_confirmed_info_exist()) {
      // no need fetch
      bool_ret = false;
    } else {
    }

    if (NULL != ref && OB_SUCCESS != (tmp_ret = revert_log_task(ref))) {
      CLOG_LOG(ERROR, "revert_log_task failed", K_(partition_key), K(tmp_ret));
    } else {
      ref = NULL;
    }
  }
  return bool_ret;
}

bool ObLogSlidingWindow::check_need_fetch_log_(const uint64_t start_log_id, bool& need_check_rebuild)
{
  bool bool_ret = true;
  bool is_tenant_out_of_mem = false;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
  } else if (OB_INVALID_ID == start_log_id) {
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(start_log_id));
  } else if (state_mgr_->is_offline()) {
    bool_ret = false;
    if (partition_reach_time_interval(60 * 1000 * 1000, fetch_log_warn_time_)) {
      CLOG_LOG(INFO,
          "the partition is offline, no need fetch log",
          K(bool_ret),
          K(partition_key_),
          "start_id",
          sw_.get_start_id(),
          "max_log_id",
          get_max_log_id(),
          "leader",
          state_mgr_->get_leader(),
          "parent",
          cascading_mgr_->get_parent());
    }
  } else if (!log_engine_->is_disk_space_enough()) {
    // clog disk is full, cannot fetch log
    bool_ret = false;
    need_check_rebuild = true;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN,
          "there is no space on log disk, no need fetch log, need check rebuild",
          K_(partition_key),
          K(need_check_rebuild));
    }
  } else if (OB_SUCCESS != (tmp_ret = replay_engine_->is_tenant_out_of_memory(partition_key_, is_tenant_out_of_mem))) {
    CLOG_LOG(WARN, "is_tenant_out_of_memory failed", K(tmp_ret), K_(partition_key));
  } else if (is_tenant_out_of_mem) {
    // tenant is out of memory, cannot fetch log
    bool_ret = false;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN,
          "tenant is out of memory, no need fetch log",
          K_(partition_key),
          K(start_log_id),
          K(need_check_rebuild));
    }
  } else if (get_next_index_log_id() > start_log_id) {
    // next_index_log_id is larger than start_log_id, need check rebuild
    bool_ret = false;
    need_check_rebuild = true;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(INFO,
          "next_index_log_id is larger than start_log_id, no need fetch log, need check rebuild",
          K_(partition_key),
          "next_index_log_id",
          get_next_index_log_id(),
          K(start_log_id),
          K(need_check_rebuild));
    }
  } else {
  }

  return bool_ret;
}

int ObLogSlidingWindow::follower_check_need_rebuild_(const uint64_t start_log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSlidingWindow is not init", K(ret), K_(partition_key));
  } else if (OB_INVALID_ID == start_log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K_(partition_key), K(start_log_id));
  } else if (LEADER != state_mgr_->get_role()) {
    ObAddr dst_server;
    int64_t dst_cluster_id = OB_INVALID_CLUSTER_ID;

    if (restore_mgr_->is_archive_restoring_log()) {
      // folloer fetch log from restore_leader during physical restoring
      dst_server = restore_mgr_->get_restore_leader();
      dst_cluster_id = state_mgr_->get_self_cluster_id();
    } else {
      ObCascadMember parent = cascading_mgr_->get_parent();
      if (parent.is_valid()) {
        dst_server = parent.get_server();
        dst_cluster_id = parent.get_cluster_id();
      }
    }

    if (!dst_server.is_valid()) {
      // dst_server is invalid, skip
    } else if (OB_FAIL(
                   log_engine_->submit_check_rebuild_req(dst_server, dst_cluster_id, partition_key_, start_log_id))) {
      CLOG_LOG(WARN,
          "submit_check_rebuild_req failed",
          K_(partition_key),
          K(ret),
          K(start_log_id),
          K(dst_server),
          K(dst_cluster_id));
    } else {
      CLOG_LOG(DEBUG,
          "submit_check_rebuild_req succ",
          K_(partition_key),
          K(ret),
          K(start_log_id),
          K(dst_server),
          K(dst_cluster_id));
    }
  } else {
  }
  return ret;
}

void ObLogSlidingWindow::start_fetch_log_from_leader(bool& is_fetched)
{
  int ret = OB_SUCCESS;
  const uint64_t start_id = sw_.get_start_id();
  uint64_t end_id = OB_INVALID_ID;
  is_fetched = false;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "ObLogSlidingWindow is not init", K_(partition_key));
  } else if (OB_FAIL(get_end_log_id_(start_id, end_id)) || start_id >= end_id) {
    CLOG_LOG(INFO, "no need fetch", K_(partition_key), K(ret), K(start_id), K(end_id));
  } else {
    do_fetch_log(start_id, end_id, TIMER_FETCH_LOG_EXECUTE_TYPE, is_fetched);
  }
}

int ObLogSlidingWindow::do_fetch_log(const uint64_t start_id, const uint64_t end_id,
    const enum ObFetchLogExecuteType& fetch_log_execute_type, bool& is_fetched)
{
  int ret = OB_SUCCESS;
  bool need_check_rebuild = false;
  is_fetched = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (start_id <= 0 || end_id <= 0 || start_id >= end_id || OB_ISNULL(state_mgr_) || OB_ISNULL(log_engine_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(start_id), K(end_id));
  } else if (!check_need_fetch_log_(start_id, need_check_rebuild)) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "start_id log_task is waiting local majority, no need fetch", K_(partition_key), K(start_id));
    }
  } else {
    const int64_t self_cluster_id = state_mgr_->get_self_cluster_id();
    ObAddr dst_server;
    int64_t dst_cluster_id = self_cluster_id;
    ObCascadMember parent = cascading_mgr_->get_parent();
    ObFetchLogType fetch_type = OB_FETCH_LOG_FOLLOWER_ACTIVE;

    if (parent.is_valid()) {
      dst_server = parent.get_server();
      dst_cluster_id = parent.get_cluster_id();
    } else if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) && FOLLOWER == state_mgr_->get_role()) {
      if (!restore_mgr_->is_archive_restoring()) {
        dst_server = state_mgr_->get_leader();
      }
    } else {
    }

    if (!dst_server.is_valid()) {
      if (restore_mgr_->is_archive_restoring()) {
        dst_server = restore_mgr_->get_restore_leader();
      }
    }

    if (dst_server.is_valid() && dst_server != self_) {
      is_fetched = true;
      if (restore_mgr_->is_archive_restoring()) {
        fetch_type = OB_FETCH_LOG_RESTORE_FOLLOWER;
      } else if (restore_mgr_->is_standby_restore_state()) {
        fetch_type = OB_FETCH_LOG_STANDBY_RESTORE;
      } else if (STANDBY_LEADER == state_mgr_->get_role() && dst_cluster_id != self_cluster_id) {
        fetch_type = OB_FETCH_LOG_STANDBY_REPLICA;
        CLOG_LOG(DEBUG,
            "standby fetch log from diff cluster",
            K_(partition_key),
            K(self_cluster_id),
            K(dst_cluster_id),
            K(dst_server),
            K(start_id),
            K(end_id));
      } else {
        // do nothing
      }

      if ((TIMER_FETCH_LOG_EXECUTE_TYPE == fetch_log_execute_type ||
              SLIDE_FETCH_LOG_EXECUTE_TYPE == fetch_log_execute_type) &&
          OB_FAIL(state_mgr_->try_set_fetched_max_log_id(end_id - 1))) {
        CLOG_LOG(ERROR, "try_set_fetched_max_log_id failed", K(ret), K(partition_key_), "max_log_id", end_id - 1);
      } else {
        const uint64_t max_confirmed_log_id = get_max_confirmed_log_id();
        ret = log_engine_->fetch_log_from_leader(dst_server,
            dst_cluster_id,
            partition_key_,
            fetch_type,
            start_id,
            end_id,
            state_mgr_->get_proposal_id(),
            mm_->get_replica_type(),
            max_confirmed_log_id);
      }
    }

    if (partition_reach_time_interval(60 * 1000 * 1000, fetch_log_warn_time_)) {
      CLOG_LOG(INFO,
          "fetch_log_from_leader_or_parent",
          K(ret),
          K(partition_key_),
          K(dst_server),
          K(dst_cluster_id),
          K(fetch_log_execute_type),
          K(fetch_type),
          K(start_id),
          K(end_id),
          K(is_fetched),
          "sw_start_id",
          get_start_id(),
          "max_log_id",
          get_max_log_id(),
          "next_ilog_id",
          ATOMIC_LOAD(&next_index_log_id_),
          "next_ilog_ts",
          ATOMIC_LOAD(&next_index_log_ts_),
          "leader",
          state_mgr_->get_leader(),
          "parent",
          cascading_mgr_->get_parent(),
          "replica_type",
          mm_->get_replica_type(),
          "last_fetched_max_log_id",
          state_mgr_->get_last_fetched_max_log_id(),
          "fetch_log_interval",
          state_mgr_->get_fetch_log_interval(),
          "fetch_window_size",
          state_mgr_->get_fetch_window_size());
    }
  }

  if (need_check_rebuild) {
    // follower check if need rebuild
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = follower_check_need_rebuild_(start_id))) {
      CLOG_LOG(WARN, "follower_check_need_rebuild_ failed", K(tmp_ret), K(partition_key_));
    }
  }

  return ret;
}

int ObLogSlidingWindow::sliding_cb(const int64_t sn, const ObILogExtRingBufferData* data)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(INFO, "not init", K_(partition_key), K(ret), K(sn));
  } else if (OB_ISNULL(state_mgr_) || OB_ISNULL(checksum_) || OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", K_(partition_key), K(sn), K(ret));
  } else if (state_mgr_->can_slide_sw()) {
    const ObLogTask* log_task = dynamic_cast<const ObLogTask*>(data);
    if (NULL == log_task) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "dynamic_cast return NULL", K_(partition_key), K(ret));
    } else {
      log_task->lock();
      const int64_t submit_timestamp = log_task->get_submit_timestamp();
      const uint64_t log_id = static_cast<uint64_t>(sn);

      if (OB_FAIL(try_submit_replay_task_(log_id, *log_task))) {
        if (OB_EAGAIN == ret) {
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            CLOG_LOG(
                WARN, "try submit_replay_task_ eagain", K_(partition_key), K(log_id), "log_task", *log_task, K(ret));
          }
        } else {
          CLOG_LOG(WARN, "try submit_replay_task_ failed", K_(partition_key), K(log_id), "log_task", *log_task, K(ret));
        }
      } else if (OB_FAIL(
                     checksum_->verify_accum_checksum(log_task->get_data_checksum(), log_task->get_accum_checksum()))) {
        CLOG_LOG(ERROR, "verify_accum_checksum failed", K_(partition_key), K(ret), K(log_id), K(*log_task));
      } else {
        advance_leader_ts(log_task->get_epoch_id());  // compatibility for old verison NOP
        if (state_mgr_->get_last_fetched_max_log_id() == log_id && (!state_mgr_->is_offline())) {
          const uint64_t start_id = log_id + 1;
          uint64_t ret_start_id = OB_INVALID_ID;
          uint64_t ret_end_id = OB_INVALID_ID;
          bool is_fetched = false;
          if (OB_SUCCESS != (tmp_ret = get_slide_log_range_(start_id, ret_start_id, ret_end_id))) {
            CLOG_LOG(WARN,
                "get_slide_log_range_ failed",
                K(tmp_ret),
                K_(partition_key),
                K(start_id),
                K(ret_start_id),
                K(ret_end_id));
          } else if (OB_INVALID_ID == ret_start_id || OB_INVALID_ID == ret_end_id || ret_start_id >= ret_end_id) {
            CLOG_LOG(INFO, "no need to fetch log", K_(partition_key), K(start_id), K(ret_start_id), K(ret_end_id));
          } else {
            state_mgr_->start_next_round_fetch();
            do_fetch_log(ret_start_id, ret_end_id, SLIDE_FETCH_LOG_EXECUTE_TYPE, is_fetched);
          }
          if (!is_fetched) {
            state_mgr_->reset_fetch_state();
          }
        }
        const bool is_nop_or_truncate_type = is_nop_or_truncate_log(log_task->get_log_type());
        try_update_next_replay_log_info(log_id + 1, submit_timestamp + 1, is_nop_or_truncate_type);
        // In order to avoid that the heartbeat arrives first and the ts carried is larger than submit_ts
        // and cause an error, update last_replay_log_ later
        if (log_id > get_last_replay_log_id()) {
          last_replay_log_.set(log_id, submit_timestamp);
          saved_accum_checksum_ = log_task->get_accum_checksum();
        }
        ATOMIC_STORE(&last_slide_fid_, log_task->get_log_cursor().file_id_);
        state_mgr_->reset_need_rebuild();
        if (scan_next_index_log_id_ != OB_INVALID_ID && log_id <= scan_next_index_log_id_) {
          ATOMIC_FAA(&pending_scan_confirmed_log_size,
              -(sizeof(ObLogTask) + (log_task->is_submit_log_body_exist() ? log_task->get_log_buf_len() : 0)));
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            CSR_LOG(INFO,
                "pending_scan_confirmed_log_size",
                K(pending_scan_confirmed_log_size),
                "M",
                pending_scan_confirmed_log_size / 1024 / 1024);
          }
        }
      }
      log_task->unlock();
      if (OB_SUCC(ret)) {
        try_update_submit_timestamp(submit_timestamp + 1);
      }
    }
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObLogSlidingWindow::submit_replay_task(const bool need_async, bool& is_replayed, bool& is_replay_failed)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(state_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!need_async) {
    const int64_t timeout = CLOG_REPLAY_DRIVER_RUN_THRESHOLD;
    const uint64_t prev_start_id = get_start_id();
    if (state_mgr_->can_slide_sw() && OB_FAIL(sw_.pop(false, timeout, is_replay_failed, this)) &&
        OB_CLOG_SLIDE_TIMEOUT != ret) {
      CLOG_LOG(WARN, "sliding window slide failed", K_(partition_key), K(ret));
    } else if (OB_CLOG_SLIDE_TIMEOUT == ret) {
      // do nothing
    }
    is_replayed = (prev_start_id != get_start_id());
  } else {
    is_replayed = false;
    is_replay_failed = false;
    int tmp_ret = OB_SUCCESS;

    if (!ATOMIC_LOAD(&has_pop_task_)) {
      ATOMIC_STORE(&has_pop_task_, true);
      if (OB_SUCCESS != (tmp_ret = cb_engine_->submit_pop_task(partition_key_))) {
        CLOG_LOG(WARN, "submit_pop_task_ failed", K(tmp_ret), K(partition_key_));
        ATOMIC_STORE(&has_pop_task_, false);
      }
    }
  }
  return ret;
}

int ObLogSlidingWindow::get_log_task(const uint64_t log_id, ObLogTask*& log_task, const int64_t*& ref) const
{
  int ret = OB_SUCCESS;
  ObILogExtRingBufferData* log_data = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_IS_INVALID_LOG_ID(log_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS == (ret = sw_.get(log_id, log_data, ref)) && log_data == NULL) {
    ret = OB_ERR_NULL_VALUE;
  } else if (OB_SUCC(ret)) {
    log_task = static_cast<ObLogTask*>(log_data);
  }
  return ret;
}

int ObLogSlidingWindow::check_left_bound_empty(bool& is_empty)
{
  int ret = OB_SUCCESS;
  is_empty = false;
  ObLogTask* log_task = NULL;
  const int64_t* ref = NULL;
  const uint64_t start_log_id = get_start_id();
  if (OB_FAIL(get_log_task(start_log_id, log_task, ref))) {
    if (OB_ERR_NULL_VALUE == ret) {
      ret = OB_SUCCESS;
      is_empty = true;
    } else if (OB_ERROR_OUT_OF_RANGE == ret) {
      // log may has already slid out
      ret = OB_EAGAIN;
    } else {
      CLOG_LOG(WARN, "failed to get_log_task", K_(partition_key), K(start_log_id), KR(ret));
    }
  } else {
    is_empty = false;
  }

  if (NULL != ref) {
    revert_log_task(ref);
  }
  return ret;
}

int ObLogSlidingWindow::revert_log_task(const int64_t* ref)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(ref)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(ret));
  } else {
    ref = NULL;
  }
  return ret;
}

// This function may take a long time and does not lock during execution
int ObLogSlidingWindow::truncate_first_stage(const common::ObBaseStorageInfo& base_storage_info)
{
  int ret = OB_SUCCESS;
  const uint64_t new_start_id = base_storage_info.get_last_replay_log_id() + 1;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sw_.truncate(new_start_id))) {
    CLOG_LOG(WARN, "sw truncate failed", K(ret), K(partition_key_), K(new_start_id));
  }
  CLOG_LOG(INFO, "truncate_first_stage", K(ret), K(partition_key_), K(new_start_id));
  return ret;
}

int ObLogSlidingWindow::truncate_second_stage(const common::ObBaseStorageInfo& base_storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const uint64_t new_start_id = base_storage_info.get_last_replay_log_id() + 1;
  const int64_t accum_checksum = base_storage_info.get_accumulate_checksum();
  const int64_t leader_ts = base_storage_info.get_epoch_id();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(checksum_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(try_update_max_log_id(new_start_id - 1))) {
    CLOG_LOG(ERROR, "try_update_max_log_id failed", K_(partition_key), K(ret), "log_id", new_start_id - 1);
  } else if (OB_FAIL(try_update_submit_timestamp(base_storage_info.get_submit_timestamp()))) {
    CLOG_LOG(ERROR, "try_update_submit_timestamp failed", K_(partition_key), K(ret), K(base_storage_info));
  } else {
    leader_ts_ = leader_ts;
    if (next_index_log_id_ <= new_start_id) {
      next_index_log_id_ = new_start_id;
      next_index_log_ts_ = base_storage_info.get_submit_timestamp();
      checksum_->set_accum_checksum(new_start_id, accum_checksum);
    } else {
      checksum_->set_verify_checksum(new_start_id, accum_checksum);
    }
    state_mgr_->reset_fetch_state();
    saved_accum_checksum_ = base_storage_info.get_accumulate_checksum();
    last_replay_log_.set(base_storage_info.get_last_replay_log_id(), base_storage_info.get_submit_timestamp());
    set_next_replay_log_id_info(
        base_storage_info.get_last_replay_log_id() + 1, base_storage_info.get_submit_timestamp() + 1);

    const int64_t* ref = NULL;
    ObILogExtRingBufferData* log_data = NULL;
    ObLogTask* log_task = NULL;
    if (OB_SUCCESS != (tmp_ret = sw_.get(new_start_id, log_data, ref))) {
      if (OB_ERROR_OUT_OF_RANGE != tmp_ret) {
        CLOG_LOG(WARN, "get log from sliding window failed", K(tmp_ret), K(partition_key_), K(new_start_id));
      }
    } else if (NULL == (log_task = static_cast<ObLogTask*>(log_data))) {
      tmp_ret = OB_ERR_NULL_VALUE;
      CLOG_LOG(TRACE, "log in sliding window is NULL", K(tmp_ret), K(partition_key_));
    }
    bool need_check_succeeding_log = false;
    if (OB_SUCCESS != (tmp_ret = handle_first_index_log_(new_start_id, log_task, true, need_check_succeeding_log))) {
      CLOG_LOG(WARN, "handle_first_index_log_ failed", K_(partition_key), K(tmp_ret));
    }
    if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
      CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
    } else {
      ref = NULL;
    }
    const bool do_pop = true;
    if (OB_SUCCESS != (tmp_ret = handle_succeeding_index_log_(new_start_id, need_check_succeeding_log, do_pop)) &&
        OB_ERROR_OUT_OF_RANGE != tmp_ret) {
      CLOG_LOG(WARN, "handle_succeeding_index_log_ failed", K_(partition_key), K(tmp_ret));
    }
  }
  CLOG_LOG(INFO,
      "truncate_second_stage",
      K_(partition_key),
      K(ret),
      K(new_start_id),
      K(accum_checksum),
      K(leader_ts),
      K_(next_index_log_id));
  return ret;
}

void ObLogSlidingWindow::destroy()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = sw_.destroy())) {
    CLOG_LOG(ERROR, "sw destroy failed", K_(partition_key), K(tmp_ret));
  }
  destroy_aggre_buffer();
  state_mgr_ = NULL;
  replay_engine_ = NULL;
  log_engine_ = NULL;
  mm_ = NULL;
  cascading_mgr_ = NULL;
  partition_service_ = NULL;
  alloc_mgr_ = NULL;
  checksum_ = NULL;
  self_.reset();
  max_log_meta_info_.reset();
  leader_max_log_info_.reset();
  leader_ts_ = OB_INVALID_TIMESTAMP;
  saved_accum_checksum_ = 0;
  next_index_log_id_ = OB_INVALID_ID;
  next_index_log_ts_ = OB_INVALID_TIMESTAMP;
  fake_ack_info_mgr_.reset();
  is_inited_ = false;
  CLOG_LOG(INFO, "ObLogSlidingWindow::destroy finished", K_(partition_key));
  partition_key_.reset();
}

int ObLogSlidingWindow::handle_first_index_log_(
    const uint64_t log_id, ObLogTask* log_task, const bool do_pop, bool& need_check_succeeding_log)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_replay_failed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(state_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != log_task && log_task->is_flush_local_finished() && log_task->is_log_confirmed()) {
    need_check_succeeding_log = false;
    if (ATOMIC_LOAD(&next_index_log_id_) > log_id && (test_and_set_index_log_submitted_(log_task))) {
      if (do_pop && state_mgr_->can_slide_sw() &&
          OB_SUCCESS != (tmp_ret = sw_.pop(false, CLOG_MAX_REPLAY_TIMEOUT, is_replay_failed, this)) &&
          OB_CLOG_SLIDE_TIMEOUT != tmp_ret) {
        CLOG_LOG(WARN, "pop failed", K_(partition_key), K(tmp_ret));
      }
    } else if (ATOMIC_LOAD(&next_index_log_id_) == log_id && (test_and_submit_index_log_(log_id, log_task, ret))) {
      need_check_succeeding_log = true;
      if (state_mgr_->can_slide_sw() &&
          OB_SUCCESS != (tmp_ret = sw_.pop(false, CLOG_MAX_REPLAY_TIMEOUT, is_replay_failed, this)) &&
          OB_CLOG_SLIDE_TIMEOUT != tmp_ret) {
        CLOG_LOG(WARN, "sliding window slide failed", K_(partition_key), K(tmp_ret), K(log_id), K(*log_task));
      }
    }
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN,
          "handle_first_index_log_ failed",
          K_(partition_key),
          K(ret),
          K(log_id),
          "sw_start_id",
          sw_.get_start_id(),
          K_(next_index_log_id));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObLogSlidingWindow::handle_succeeding_index_log_(
    const uint64_t id, const bool check_succeeding_log, const bool do_pop)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint64_t log_id = id;
  bool need_check_succeeding_log = check_succeeding_log;
  ObLogTask* log_task = NULL;
  bool is_replay_failed = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_IS_INVALID_LOG_ID(id)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    while (need_check_succeeding_log && OB_SUCCESS == ret) {
      const int64_t* ref = NULL;
      ObILogExtRingBufferData* data = NULL;
      ++log_id;
      if (ATOMIC_LOAD(&next_index_log_id_) == log_id &&
          OB_SUCCESS == (ret = sw_.get(static_cast<int64_t>(log_id), data, ref)) &&
          NULL != (log_task = static_cast<ObLogTask*>(data)) && log_task->is_flush_local_finished() &&
          log_task->is_log_confirmed() && test_and_submit_index_log_(log_id, log_task, ret)) {
        // do nothing
      } else {
        need_check_succeeding_log = false;
      }
      if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
        CLOG_LOG(ERROR, "revert failed", K_(partition_key), K(tmp_ret));
      } else {
        ref = NULL;
      }
    }
    if (do_pop && state_mgr_->can_slide_sw() &&
        OB_SUCCESS != (tmp_ret = sw_.pop(false, CLOG_MAX_REPLAY_TIMEOUT, is_replay_failed, this)) &&
        OB_CLOG_SLIDE_TIMEOUT != tmp_ret) {
      CLOG_LOG(WARN, "sliding window slide failed", K_(partition_key), K(tmp_ret));
    }
  }
  return ret;
}

bool ObLogSlidingWindow::test_and_set_index_log_submitted_(ObLogTask* log_task)
{
  // caller guarantees log_task is not NULL
  bool bool_ret = false;
  if (NULL != log_task) {
    log_task->lock();
    if (log_task->is_index_log_submitted()) {
      bool_ret = false;
    } else {
      log_task->set_index_log_submitted();
      bool_ret = true;
    }
    log_task->unlock();
  }
  return bool_ret;
}

bool ObLogSlidingWindow::test_and_submit_index_log_(const uint64_t log_id, ObLogTask* log_task, int& ret)
{
  // caller guarantees log_task is not NULL
  bool bool_ret = false;
  if (NULL != log_task) {
    int64_t accum_checksum = 0;

    if (log_task->try_pre_index_log_submitted()) {
      log_task->lock();
      if (log_task->is_index_log_submitted()) {
        bool_ret = false;
      } else if (OB_SUCCESS == (ret = submit_index_log_(log_id, log_task, accum_checksum))) {
        log_task->set_index_log_submitted();
        bool_ret = true;
      } else {
        CLOG_LOG(ERROR, "submit_index_log_ failed", K(ret), K_(partition_key), K(log_id), K(*log_task));
        bool_ret = false;
      }
      log_task->unlock();
      if (!log_task->is_index_log_submitted()) {
        log_task->reset_pre_index_log_submitted();
      }
    } else {
      // do nothing
    }

    bool is_archive_restoring = false;
    if (restore_mgr_->is_archive_restoring()) {
      is_archive_restoring = true;
    }

    int tmp_ret = OB_SUCCESS;
    if (bool_ret && (LEADER == state_mgr_->get_role() || is_archive_restoring)) {
      // leader generate confirmed_info
      if (OB_SUCCESS != (tmp_ret = leader_submit_confirmed_info_(log_id, log_task, accum_checksum))) {
        CLOG_LOG(ERROR,
            "leader_submit_confirmed_info_ failed, it is impossible",
            K(tmp_ret),
            K_(partition_key),
            K(log_id),
            K(*log_task));
      }
    }
    if (bool_ret) {
      const int64_t log_submit_ts = log_task->get_submit_timestamp();
      // inc next_index_log_id after Leader submit_confirm_info
      ATOMIC_INC(&next_index_log_id_);
      if (OB_INVALID_TIMESTAMP != log_submit_ts) {
        ATOMIC_STORE(&next_index_log_ts_, log_submit_ts);
      } else {
        CLOG_LOG(WARN, "log_submit_ts is invalid", K_(partition_key), K(log_task));
      }

      // follower send confirmed clog to standby children
      if (OB_SUCCESS != (tmp_ret = follower_send_log_to_standby_children_(log_id, log_task))) {
        CLOG_LOG(WARN, "follower_send_log_to_standby_children_ failed", K_(partition_key), K(tmp_ret), K(log_id));
      }
      // standby_leader send confirmed_info to follower after submit ilog
      // This step must be placed after advancing next_index_log_id
      if (OB_SUCCESS != (tmp_ret = standby_leader_transfer_confirmed_info_(log_id, log_task))) {
        CLOG_LOG(ERROR,
            "standby_leader_transfer_confirmed_info_ failed",
            K(tmp_ret),
            K_(partition_key),
            K(log_id),
            K(*log_task));
      }
    }
  }
  return bool_ret;
}

int ObLogSlidingWindow::submit_index_log_(const uint64_t log_id, const ObLogTask* log_task, int64_t& accum_checksum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_task) || OB_ISNULL(checksum_) || OB_ISNULL(log_engine_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t submit_timestamp = log_task->get_submit_timestamp();
    const file_id_t file_id = log_task->get_log_cursor().file_id_;
    const offset_t offset = log_task->get_log_cursor().offset_;
    const int32_t size = log_task->get_log_cursor().size_;
    const int64_t data_checksum = log_task->get_data_checksum();
    const int64_t log_accum_checksum = log_task->get_accum_checksum();
    ObLogCursorExt log_cursor_ext;

    bool need_skip_checksum_cmp = false;
    if (restore_mgr_->is_archive_restoring() && self_ == restore_mgr_->get_restore_leader()) {
      need_skip_checksum_cmp = true;
    }

    if (!is_valid_log_id(log_id) || common::OB_INVALID_FILE_ID == file_id || 0 == file_id || offset < 0 || size <= 0 ||
        common::OB_INVALID_TIMESTAMP == submit_timestamp) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(ret), K(*log_task));
    } else if (OB_FAIL(checksum_->acquire_accum_checksum(data_checksum, accum_checksum))) {
      // attention : acquire_accum_checksum() is not reentrant!!!
      // any failure must print ERROR
      CLOG_LOG(ERROR, "acquire_accum_checksum failed", K_(partition_key), K(ret), K(data_checksum));
    } else if (log_task->is_confirmed_info_exist() && !need_skip_checksum_cmp && accum_checksum != log_accum_checksum) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(
          ERROR, "accum_checksum not match with log_task", K_(partition_key), K(ret), K(accum_checksum), K(*log_task));
    } else if (OB_FAIL(log_cursor_ext.reset(
                   file_id, offset, size, accum_checksum, submit_timestamp, log_task->is_batch_committed()))) {
      CLOG_LOG(ERROR,
          "log_cursor_ext init failed",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(file_id),
          K(offset),
          K(size),
          K(submit_timestamp),
          K(accum_checksum));
    } else {
      if (false) {
        // if (OB_LOG_MEMBERSHIP == log_type || OB_LOG_START_MEMBERSHIP == log_type) {
        const ObMemberList curr_member_list = mm_->get_curr_member_list();
        const int64_t replica_num = mm_->get_replica_num();
        const int64_t membership_timestamp = mm_->get_timestamp();

        if (OB_FAIL(log_engine_->submit_cursor(
                partition_key_, log_id, log_cursor_ext, curr_member_list, replica_num, membership_timestamp))) {
          CLOG_LOG(ERROR,
              "submit_cursor failed",
              K(ret),
              K(partition_key_),
              K(log_id),
              K(log_cursor_ext),
              K(curr_member_list),
              K(replica_num),
              K(membership_timestamp));
        }
      } else {
        if (OB_FAIL(log_engine_->submit_cursor(partition_key_, log_id, log_cursor_ext))) {
          CLOG_LOG(ERROR, "submit_index_flush_task failed", K_(partition_key), K(ret), K(log_id), K(log_cursor_ext));
        }
      }
    }
  }
  return ret;
}

bool ObLogSlidingWindow::is_empty() const
{
  return get_max_log_id() == (static_cast<uint64_t>(sw_.get_start_id()) - 1);
}

int ObLogSlidingWindow::set_next_index_log_id(const uint64_t log_id, const int64_t accum_checksum)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_IS_INVALID_LOG_ID(log_id) || OB_ISNULL(checksum_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (log_id > ATOMIC_LOAD(&next_index_log_id_)) {
      const int64_t now = ObTimeUtility::current_time();
      ATOMIC_STORE(&next_index_log_id_, log_id);
      ATOMIC_STORE(&scan_next_index_log_id_, log_id);
      ATOMIC_STORE(&next_index_log_ts_, now);
      checksum_->set_accum_checksum(accum_checksum);
    }
  }
  CLOG_LOG(INFO,
      "set_next_index_log_id",
      K_(partition_key),
      K(ret),
      "next_index_log_id",
      ATOMIC_LOAD(&next_index_log_id_),
      K(log_id),
      K(accum_checksum),
      "next_index_log_ts",
      ATOMIC_LOAD(&next_index_log_ts_));
  return ret;
}

int ObLogSlidingWindow::try_submit_mc_success_cb_(const ObLogType& log_type, const uint64_t log_id, const char* log_buf,
    const int64_t log_buf_len, const common::ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(mm_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    mm_->submit_success_cb_task(log_type, log_id, log_buf, log_buf_len, proposal_id);
  }
  return ret;
}

uint64_t ObLogSlidingWindow::get_max_confirmed_log_id() const
{
  return ATOMIC_LOAD(&next_index_log_id_) - 1;
}

bool ObLogSlidingWindow::check_can_receive_larger_log(const uint64_t log_id)
{
  bool bool_ret = true;
  const uint64_t follower_max_unconfirmed_threshold = get_follower_max_unconfirmed_log_count();

  if (IS_NOT_INIT) {
    bool_ret = false;
  } else if (OB_IS_INVALID_LOG_ID(log_id) || OB_ISNULL(state_mgr_) || OB_ISNULL(replay_engine_)) {
    bool_ret = false;
  } else {
    if (log_id > get_start_id() + follower_max_unconfirmed_threshold) {
      bool_ret = false;
      if (partition_reach_time_interval(5 * 60 * 1000 * 1000, check_can_receive_larger_log_warn_time_)) {
        const uint64_t start_log_id = sw_.get_start_id();
        state_mgr_->report_start_id_trace(start_log_id);
        CLOG_LOG(WARN,
            "check_can_receive_log, now can not recieve larger log",
            K_(partition_key),
            K(log_id),
            "next_index_log_id",
            get_next_index_log_id(),
            "max_log_id",
            get_max_log_id(),
            K(start_log_id));
      }
    }
  }

  if (bool_ret && GCONF.__enable_block_receiving_clog) {
    int64_t pending_submit_task_count = INT64_MAX;
    int ret = OB_SUCCESS;
    if (OB_FAIL(replay_engine_->get_pending_submit_task_count(partition_key_, pending_submit_task_count))) {
      bool_ret = false;
      CLOG_LOG(WARN, "failed to get_pending_submit_task_count", KR(ret), K_(partition_key));
    } else {
      bool_ret = pending_submit_task_count < follower_max_unconfirmed_threshold;
      if (!bool_ret) {
        if (partition_reach_time_interval(5 * 60 * 1000 * 1000, check_can_receive_larger_log_warn_time_)) {
          CLOG_LOG(WARN,
              "check_can_receive_log, now can not recieve larger log because of pending too many task to submit",
              K_(partition_key),
              K(log_id),
              K(pending_submit_task_count));
        }
      }
    }
  }
  return bool_ret;
}

int ObLogSlidingWindow::get_end_log_id_(const uint64_t start_id, uint64_t& end_id)
{
  int ret = OB_SUCCESS;
  end_id = OB_INVALID_ID;
  const uint64_t clog_fetch_log_count =
      std::min(state_mgr_->get_fetch_window_size(), get_follower_max_unconfirmed_log_count());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_empty()) {
    end_id = start_id + clog_fetch_log_count;
  } else {
    const uint64_t max_log_id = std::min(get_max_log_id(), start_id + clog_fetch_log_count - 1);
    bool found = false;
    for (uint64_t i = start_id; !found && i <= max_log_id; ++i) {
      ObLogTask* log_task = NULL;
      const int64_t* ref = NULL;
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = get_log_task(i, log_task, ref))) {
        if (OB_ERROR_OUT_OF_RANGE == tmp_ret) {
          end_id = start_id;  // no need fetch
          found = true;
        } else {  // tmp_ret == OB_ERR_NULL_VALUE or others
          end_id = i + 1;
        }
      } else if (log_task->is_submit_log_exist() && log_task->is_log_confirmed()) {
        found = true;
      } else {
        end_id = i + 1;
      }
      if (NULL != ref && OB_SUCCESS != (tmp_ret = revert_log_task(ref))) {
        CLOG_LOG(ERROR, "revert_log_task failed", K_(partition_key), K(tmp_ret));
      } else {
        ref = NULL;
      }
    }
    if (end_id == max_log_id + 1) {
      end_id = start_id + clog_fetch_log_count;
    } else if (OB_INVALID_ID == end_id) {
      // Consider performing a truncate operation. At this time, the log corresponding to start_id actually exists and
      // is confirmed, but at this time next_index_log_id == start_id; in this case, fetch_log is required to fetch a
      // log from the host; this has two advantages: 1) Use receive_confirm_info to use worker threads to trigger
      // writing ilog, instead of using state management threads to write ilog here; 2) Send a fetch_log message to the
      // host, which triggers the parent to reply with the largest confirmed log_id;
      end_id = start_id + 1;
    }
    if (partition_reach_time_interval(30 * 1000 * 1000, get_end_log_id_warn_time_)) {
      CLOG_LOG(INFO, "get_end_log_id_", K_(partition_key), K(start_id), K(end_id), K(max_log_id));
    }
  }

  const uint64_t last_fetched_max_log_id = state_mgr_->get_last_fetched_max_log_id();
  if (OB_SUCC(ret) && OB_INVALID_ID != last_fetched_max_log_id) {
    if (sw_.get_start_id() > last_fetched_max_log_id) {
      state_mgr_->reset_fetch_state();
    } else if (end_id > last_fetched_max_log_id + 1) {
      end_id = last_fetched_max_log_id + 1;
    }
  }

  return ret;
}

int ObLogSlidingWindow::get_slide_log_range_(const uint64_t start_id, uint64_t& ret_start_id, uint64_t& ret_end_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ret_start_id = OB_INVALID_ID;
  ret_end_id = OB_INVALID_ID;
  ObLogTask* log_task = NULL;
  const int64_t* ref = NULL;
  const uint64_t clog_fetch_log_count =
      std::min(state_mgr_->get_fetch_window_size(), get_follower_max_unconfirmed_log_count());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (get_max_log_id() == (start_id - 1)) {
    // This is equivalent to judging whether the sliding window is empty
    // is_empty() cannot be called here, because get_slide_log_range_ is called in the sliding_cb function, and the log
    // has not moved out of the sliding window at this time (sw_.start_id_ unchanged) is_empty() will always return
    // false
    ret_start_id = start_id;
    ret_end_id = start_id + clog_fetch_log_count;
  } else {
    uint64_t max_log_id = get_max_log_id();
    bool found = false;
    // find ret_start_id
    for (uint64_t i = start_id; !found && i <= max_log_id; ++i) {
      if (OB_SUCCESS != (tmp_ret = get_log_task(i, log_task, ref))) {
        if (OB_ERROR_OUT_OF_RANGE == tmp_ret) {
          // log is slided out, continue
        } else {  // tmp_ret == OB_ERR_NULL_VALUE or others
          ret_start_id = i;
          found = true;
        }
      } else if (!log_task->is_submit_log_exist() || !log_task->is_log_confirmed()) {
        ret_start_id = i;
        found = true;
      } else {
        ret_start_id = i + 1;
      }
      if (NULL != ref && OB_SUCCESS != (tmp_ret = revert_log_task(ref))) {
        CLOG_LOG(ERROR, "revert_log_task failed", K(tmp_ret), K_(partition_key));
      } else {
        ref = NULL;
      }
    }

    if (!found) {
      // In the current sliding window until max_log_id are confirmed logs, you do not need to trigger the log fetching
      // through slide at this time, but should wait for subsequent timeout detection
      // At this time ret_end_id == OB_INVALID_ID, the caller will judge that there is no need to fetch logs at this
      // time
    } else {
      found = false;
      // find ret_end_id
      ret_end_id = ret_start_id + 1;
      max_log_id = std::min(get_max_log_id(), ret_start_id + clog_fetch_log_count - 1);
      for (uint64_t i = ret_start_id + 1; !found && i <= max_log_id; ++i) {
        if (OB_SUCCESS != (tmp_ret = get_log_task(i, log_task, ref))) {
          if (OB_ERROR_OUT_OF_RANGE == tmp_ret) {
            ret_end_id = ret_start_id;  // no need fetch
            found = true;
          } else {  // tmp_ret == OB_ERR_NULL_VALUE or others
            ret_end_id = i + 1;
          }
        } else if (log_task->is_submit_log_exist() && log_task->is_log_confirmed()) {
          found = true;
          ret_end_id = i;
        } else {
          ret_end_id = i + 1;
        }
        if (NULL != ref && OB_SUCCESS != (tmp_ret = revert_log_task(ref))) {
          CLOG_LOG(ERROR, "revert_log_task failed", K(tmp_ret), K_(partition_key));
        } else {
          ref = NULL;
        }
      }
      if (!found) {
        ret_end_id = ret_start_id + clog_fetch_log_count;
      }
    }
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(
          INFO, "get_slide_log_range", K_(partition_key), K(start_id), K(ret_start_id), K(ret_end_id), K(max_log_id));
    }
  }
  return ret;
}

int ObLogSlidingWindow::backfill_log(const uint64_t log_id, const common::ObProposalID& proposal_id,
    const char* serialize_buff, const int64_t serialize_size, const ObLogCursor& log_cursor, const bool is_leader,
    ObISubmitLogCb* submit_cb)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSlidingWindow is not inited", K(ret), K(partition_key_));
  } else if (!log_cursor.is_valid() || (is_leader && OB_ISNULL(submit_cb)) || (!is_leader && !OB_ISNULL(submit_cb)) ||
             NULL == serialize_buff || serialize_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(partition_key_),
        K(log_cursor),
        K(is_leader),
        KP(submit_cb),
        KP(serialize_buff),
        K(serialize_size));
  } else {
    if (NULL != aggre_buffer_ && state_mgr_->is_leader_active() && proposal_id == state_mgr_->get_proposal_id()) {
      const uint64_t tmp_id = log_id + 1;
      inc_update(next_submit_aggre_buffer_, tmp_id);
      if (OB_SUCCESS != (tmp_ret = try_freeze_aggre_buffer_(tmp_id))) {
        CLOG_LOG(ERROR, "try_freeze_aggre_buffer_ failed", K(tmp_ret), K(partition_key_));
      }
    }
    if (OB_FAIL(backfill_log_(serialize_buff, serialize_size, log_cursor, is_leader, submit_cb))) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "backfill_log_ failed", K(ret), K(partition_key_), K(log_cursor), K(is_leader));
      }
    }
  }

  return ret;
}

int ObLogSlidingWindow::backfill_confirmed(const uint64_t log_id, const bool batch_first_participant)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSlidingWindow is not inited", K(ret), K(partition_key_));
  } else if (OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_));
  } else if (OB_FAIL(backfill_confirmed_(log_id, batch_first_participant))) {
    CLOG_LOG(WARN, "backfill_confirmed_ failed", K(ret), K(partition_key_), K(log_id));
  }

  return ret;
}

int ObLogSlidingWindow::resubmit_log(const ObLogInfo& log_info, ObISubmitLogCb* cb)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogSlidingWindow is not inited", K(ret), K(partition_key_));
  } else if (!log_info.is_valid() || OB_ISNULL(cb)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argumetns", K(ret), K(partition_key_), K(log_info), KP(cb));
  } else if (OB_FAIL(resubmit_log_(log_info, cb))) {
    CLOG_LOG(WARN, "resubmit_log_ failed", K(ret), K(partition_key_), K(log_info));
  }

  return ret;
}

int ObLogSlidingWindow::backfill_log_(const char* serialize_buff, const int64_t serialize_size,
    const ObLogCursor& log_cursor, const bool is_leader, ObISubmitLogCb* submit_cb)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;
  const bool need_replay = !is_leader;
  // need_copy == false, this can save a memory copy
  const bool need_copy = false;
  const bool need_pinned = is_leader;

  ObLogEntry log_entry;
  int64_t pos = 0;
  if (OB_FAIL(log_entry.deserialize(serialize_buff, serialize_size, pos))) {
    CLOG_LOG(ERROR, "log_entry deserialize failed", K(ret), K(partition_key_));
  }
  const ObLogEntryHeader& header = log_entry.get_header();
  const char* buff = log_entry.get_buf();
  const uint64_t log_id = header.get_log_id();

  while (OB_SUCC(ret)) {
    if (OB_FAIL(sw_.get(log_id, log_data, ref))) {
      CLOG_LOG(WARN, "get log from sliding window failed", K(ret), K(partition_key_), K(log_id));
    } else if (NULL != log_data) {
      if (is_leader) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "leader backfill_log_, log exist", K(ret), K(partition_key_), K(log_id));
      } else {
        log_task = static_cast<ObLogTask*>(log_data);
        log_task->lock();
        // leader already resubmit_log when callback
        if (log_task->is_submit_log_exist()) {
          ret = OB_ENTRY_EXIST;
        } else if (OB_FAIL(log_task->set_log(header, buff, need_copy))) {
          CLOG_LOG(WARN, "log_task set_log failed", K(ret), K(partition_key_), K(header));
        } else {
          log_task->set_flush_local_finished();
          log_task->set_log_cursor(log_cursor);
        }
        log_task->unlock();
      }
      break;
    } else if (OB_FAIL(generate_backfill_log_task_(
                   header, buff, log_cursor, submit_cb, need_replay, need_copy, need_pinned, log_task))) {
      CLOG_LOG(WARN, "generate_backfill_log_task_ failed", K(ret), K(partition_key_), K(log_id));
    } else if (OB_FAIL(sw_.set(log_id, log_task))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;  // try get it, again
      }
      CLOG_LOG(TRACE, "insert log to sliding window failed", K(ret), K(partition_key_), K(log_id));
      log_task->destroy();
      log_task = NULL;
    } else if (!is_leader && OB_FAIL(try_update_max_log_id(log_id))) {
      CLOG_LOG(ERROR, "try_update_max_log_id failed", K(ret), K(partition_key_), K(log_id));
    } else {
      if (OB_SUCCESS != (tmp_ret = submit_log_to_local_children_(header, serialize_buff, serialize_size))) {
        CLOG_LOG(WARN, "submit_log_to_local_children_ failed", K(tmp_ret), K(partition_key_), K(header));
      }

      // send log to standby children
      log_task->lock();
      const bool is_log_majority = (log_task->is_local_majority_flushed() || log_task->is_log_confirmed());
      if (!log_task->is_already_send_to_standby()) {
        if (OB_SUCCESS !=
            (tmp_ret = submit_log_to_standby_children_(header, serialize_buff, serialize_size, is_log_majority))) {
          CLOG_LOG(WARN, "submit_log_to_standby_children_ failed", K(tmp_ret), K(partition_key_), K(header));
        } else {
          log_task->set_already_send_to_standby();
          CLOG_LOG(TRACE, "set_already_send_to_standby", K_(partition_key), K(ret), K(log_id), K(*log_task));
        }
      }
      log_task->unlock();

      // success
      break;
    }
    if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
      CLOG_LOG(ERROR, "revert failed", K(tmp_ret), K_(partition_key));
    } else {
      ref = NULL;
    }
  }
  bool need_check_succeeding_log = false;
  if (!is_leader && OB_SUCC(ret) &&
      OB_SUCCESS != (tmp_ret = handle_first_index_log_(log_id, log_task, true, need_check_succeeding_log))) {
    CLOG_LOG(WARN, "handle_first_index_log_ failed", K_(partition_key), K(tmp_ret));
  }
  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K(tmp_ret), K_(partition_key));
  } else {
    ref = NULL;
  }
  const bool do_pop = true;
  if (!is_leader && OB_SUCC(ret) &&
      OB_SUCCESS != (tmp_ret = handle_succeeding_index_log_(log_id, need_check_succeeding_log, do_pop)) &&
      OB_ERROR_OUT_OF_RANGE != tmp_ret) {
    CLOG_LOG(WARN, "handle_succeeding_index_log_ failed", K_(partition_key), K(tmp_ret));
  }

  if (OB_SUCCESS == ret && !is_leader) {
    if (OB_FAIL(state_mgr_->try_reset_fetch_state(log_entry.get_header().get_log_id()))) {
      CLOG_LOG(ERROR,
          "try_reset_fetch_state failed",
          K(ret),
          K(partition_key_),
          "log_id",
          log_entry.get_header().get_log_id());
    }
  }

  return ret;
}

int ObLogSlidingWindow::backfill_confirmed_(const uint64_t log_id, const bool batch_first_participant)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;
  const common::ObMemberList& member_list = mm_->get_curr_member_list();
  const bool batch_committed = true;
  bool majority = false;
  bool need_wait_standby_ack = false;

  if (OB_FAIL(sw_.get(log_id, log_data, ref))) {
    CLOG_LOG(ERROR, "get log from sliding_window failed", K(ret), K(partition_key_), K(log_id));
  } else if (NULL == log_data) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "the log not exist, unexpected", K(ret), K(partition_key_), K(log_id));
  } else {
    log_task = static_cast<ObLogTask*>(log_data);
    log_task->lock();
    // At this time, it has been confirmed that the log has formed a majority,
    // so don't care whether the ack_list is completely accurate
    for (int64_t index = 0; OB_SUCC(ret) && index < member_list.get_member_number(); index++) {
      ObAddr server;
      if (OB_SUCCESS == (ret = member_list.get_server_by_index(index, server))) {
        log_task->ack_log(server);
      } else {
        CLOG_LOG(ERROR, "get_server_by_index failed", K(ret), K(partition_key_), K(log_id));
      }
    }
    bool need_send_to_standby = false;
    if (is_mp_leader_waiting_standby_ack_(log_task)) {
      need_wait_standby_ack = true;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(INFO, "not receive standby_ack, need wait", K_(partition_key), K(log_id));
      }
      if (log_task->is_local_majority_flushed()) {
        need_send_to_standby = true;
      }
    } else {
      majority = log_task->try_set_majority_finished();
    }

    if (!need_send_to_standby && is_primary_need_send_log_to_standby_(log_task)) {
      need_send_to_standby = true;
    }
    if (need_send_to_standby && !log_task->is_already_send_to_standby()) {
      if (OB_SUCCESS != (tmp_ret = send_log_to_standby_cluster_(log_id, log_task))) {
        CLOG_LOG(WARN, "send_log_to_standby_cluster_ failed", K(tmp_ret), K_(partition_key), K(log_id));
      } else {
        log_task->set_already_send_to_standby();
        CLOG_LOG(TRACE, "set_already_send_to_standby", K_(partition_key), K(ret), K(log_id), K(*log_task));
      }
    }

    if (!majority && !need_wait_standby_ack) {
      CLOG_LOG(ERROR, "backfill_confirmed log is not majority", K(ret), K(partition_key_), K(*log_task));
    }
    log_task->reset_pinned();
    log_task->unlock();
  }

  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K(tmp_ret), K(partition_key_));
  } else {
    ref = NULL;
  }

  if (OB_SUCC(ret)) {
    if (majority) {
      if (OB_FAIL(majority_cb(log_id, batch_committed, batch_first_participant))) {
        CLOG_LOG(WARN, "majority_cb failed", K(ret), K(partition_key_), K(log_id));
      } else if (OB_FAIL(set_log_confirmed(log_id, batch_committed))) {
        CLOG_LOG(WARN, "set_log_confirmed failed", K(ret), K(partition_key_), K(log_id));
      } else { /*do nothing*/
      }
    } else {
      if (need_wait_standby_ack) {
        // need wait standby ack
      } else {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "backfill_confirmed log is not majority", K(ret), K(partition_key_), K(log_id));
      }
    }
  }

  return ret;
}

int ObLogSlidingWindow::resubmit_log_(const ObLogInfo& log_info, ObISubmitLogCb* cb)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const int64_t* ref = NULL;
  ObILogExtRingBufferData* log_data = NULL;
  ObLogTask* log_task = NULL;
  const char* buff = log_info.get_buf();
  const int64_t size = log_info.get_size();
  const uint64_t log_id = log_info.get_log_id();
  const bool need_replay = false;
  const bool send_slave = true;
  const ObAddr dummy_server;
  const int64_t dummy_cluster_id = state_mgr_->get_self_cluster_id();
  ObLogEntry log_entry;
  int64_t pos = 0;
  bool is_log_backfilled = false;
  bool is_log_majority = false;

  if (OB_FAIL(log_entry.deserialize(buff, size, pos))) {
    CLOG_LOG(WARN, "log_entry deserialize failed", K(ret), K(partition_key_), K(log_info));
  } else if (OB_FAIL(sw_.get(log_id, log_data, ref))) {
    CLOG_LOG(WARN, "get log from sliding_window failed", K(ret), K(partition_key_), K(log_id));
  } else if (NULL != log_data) {
    is_log_backfilled = true;
    log_task = static_cast<ObLogTask*>(log_data);
    log_task->reset_pinned();
    is_log_majority = (log_task->is_local_majority_flushed() || log_task->is_log_confirmed());
  }

  if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert(ref))) {
    CLOG_LOG(ERROR, "revert failed", K(tmp_ret), K(partition_key_));
  } else {
    ref = NULL;
  }

  // If the log has been backfilled, it means that the local disk flush is successful,
  // and resubmit only needs to send the log to all followers.
  if (is_log_backfilled) {
    if (OB_FAIL(submit_log_to_net_(log_entry.get_header(), buff, size, is_log_majority))) {
      CLOG_LOG(WARN, "submit_log_to_net_ failed", K(ret), K(partition_key_), K(log_id));
    }
  } else {
    const bool is_confirmed = false;
    const int64_t unused_accum_checksum = 0;
    const bool unused_is_batch_committed = false;
    if (OB_FAIL(submit_to_sliding_window_(log_entry.get_header(),
            log_entry.get_buf(),
            cb,
            need_replay,
            send_slave,
            dummy_server,
            dummy_cluster_id,
            is_confirmed,
            unused_accum_checksum,
            unused_is_batch_committed))) {
      CLOG_LOG(WARN, "submit_to_sliding_window_ failed", K(ret), K(partition_key_), K(log_id));
    }
  }

  return ret;
}

int ObLogSlidingWindow::generate_backfill_log_task_(const ObLogEntryHeader& header, const char* buff,
    const ObLogCursor& log_cursor, ObISubmitLogCb* submit_cb, const bool need_replay, const bool need_copy,
    const bool need_pinned, ObLogTask*& log_task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_null_log_task_(header, need_replay, log_task))) {
    CLOG_LOG(WARN, "generate_null_log_task_ failed", K(ret), K(partition_key_), K(header));
  } else if (OB_FAIL(log_task->set_log(header, buff, need_copy))) {
    CLOG_LOG(WARN, "set log entry to log_task failed", K(ret), K(partition_key_));
    log_task->destroy();
  } else if (OB_FAIL(log_task->set_log_cursor(log_cursor))) {
    CLOG_LOG(WARN, "set log cursor to log_task failed", K(ret), K(partition_key_));
    log_task->destroy();
  } else {
    log_task->set_flush_local_finished();
    log_task->set_submit_cb(submit_cb);
    if (need_pinned) {
      log_task->set_pinned();
    }
  }
  return ret;
}

int ObLogSlidingWindow::try_refresh_unconfirmed_log_count_()
{
  const int64_t now = ObTimeUtility::current_time();
  const uint64_t tenant_id = partition_key_.get_tenant_id();
  const uint64_t last_update_time = ATOMIC_LOAD(&last_get_tenant_config_time_);

  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TIMESTAMP != last_update_time && now - last_update_time < TENANT_CONFIG_REFRESH_INTERVAL) {
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (!tenant_config.is_valid()) {
      CLOG_LOG(
          WARN, "tenant not exist, use default max unconfirmed log count instead", K(tenant_id), K_(partition_key));
    } else {
      ATOMIC_STORE(&last_get_tenant_config_time_, now);
      ATOMIC_STORE(&leader_max_unconfirmed_log_cnt_, tenant_config->clog_max_unconfirmed_log_count);
    }
  }
  return ret;
}

uint64_t ObLogSlidingWindow::get_leader_max_unconfirmed_log_count()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = try_refresh_unconfirmed_log_count_())) {
    CLOG_LOG(WARN, "try_refresh_unconfirmed_log_count_ failed", K(tmp_ret), K_(partition_key));
  }
  return ATOMIC_LOAD(&leader_max_unconfirmed_log_cnt_);
}

uint64_t ObLogSlidingWindow::get_follower_max_unconfirmed_log_count()
{
  uint64_t follower_max_unconfirmed_threshold = 0;
  uint64_t leader_max_unconfirmed_log_cnt = get_leader_max_unconfirmed_log_count();
  // follower's sw capacity is set to twice of leader's
  follower_max_unconfirmed_threshold = 2 * leader_max_unconfirmed_log_cnt;
  return follower_max_unconfirmed_threshold;
}

void ObLogSlidingWindow::try_update_max_majority_log(const uint64_t log_id, const int64_t log_ts)
{
  max_majority_log_.inc_update(log_id, log_ts);
}

int ObLogSlidingWindow::check_if_all_log_replayed(bool& has_replayed) const
{
  int ret = OB_SUCCESS;
  has_replayed = false;
  if (is_empty()) {
    bool is_finished = false;
    if (OB_FAIL(replay_engine_->is_replay_finished(partition_key_, is_finished))) {
      CLOG_LOG(WARN, "replay engine check is replay finish failed", K(ret), K_(partition_key));
    } else if (is_finished) {
      has_replayed = true;
    }
  }
  return ret;
}

int ObLogSlidingWindow::try_freeze_aggre_buffer()
{
  int ret = OB_SUCCESS;
  uint64_t tmp_log_id = 0;
  while (true) {
    tmp_log_id = ATOMIC_LOAD(&last_flushed_log_id_);
    if (ATOMIC_BCAS(&last_flushed_log_id_, tmp_log_id, 0)) {
      CLOG_LOG(DEBUG, "flushed log id changed", K(partition_key_), K(tmp_log_id), "log_id", 0);
      break;
    }
  }
  if (NULL != aggre_buffer_ && state_mgr_->is_leader_active()) {
    inc_update(next_submit_aggre_buffer_, tmp_log_id + 1);
    if (OB_FAIL(try_freeze_aggre_buffer_(tmp_log_id + 1))) {
      CLOG_LOG(ERROR, "try_freeze_aggre_buffer_ failed", K(ret), K(partition_key_));
    } else {
      CLOG_LOG(DEBUG, "try freeze aggre buffer success", K(partition_key_), "log_id", tmp_log_id + 1);
    }
  }
  return ret;
}

int ObLogSlidingWindow::set_confirmed_info_without_lock_(
    const ObLogEntryHeader& header, const int64_t accum_checksum, const bool is_batch_committed, ObLogTask& log_task)
{
  int ret = OB_SUCCESS;
  ObConfirmedInfo confirmed_info;
  if (OB_FAIL(confirmed_info.init(header.get_data_checksum(), header.get_epoch_id(), accum_checksum))) {
    CLOG_LOG(ERROR, "confirmed_info init failed", K_(partition_key), K(header), KR(ret));
  } else {
    log_task.set_confirmed_info(confirmed_info);
    log_task.set_log_confirmed();
    if (is_batch_committed) {
      log_task.set_batch_committed();
    }
  }
  return ret;
}

}  // namespace clog
}  // namespace oceanbase
