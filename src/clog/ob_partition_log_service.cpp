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

#include "ob_partition_log_service.h"
#include <algorithm>
#include "share/ob_lease_struct.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/io/ob_io_manager.h"
#include "share/ob_cluster_version.h"
#include "share/ob_multi_cluster_util.h"
#include "election/ob_election.h"
#include "election/ob_election_mgr.h"
#include "election/ob_election_priority.h"
#include "storage/ob_storage_log_type.h"
#include "storage/transaction/ob_trans_log.h"
#include "storage/transaction/ob_trans_ctx.h"
#include "storage/transaction/ob_trans_log.h"
#include "observer/ob_server.h"
#include "ob_fetch_log_engine.h"
#include "ob_i_log_engine.h"
#include "ob_log_event_task_V2.h"
#include "ob_log_replay_engine_wrapper.h"
#include "ob_log_req.h"
#include "ob_remote_log_query_engine.h"
#include "common/ob_clock_generator.h"
#include "storage/transaction/ob_weak_read_util.h"
#include "archive/ob_archive_mgr.h"
#include "archive/ob_archive_restore_engine.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/ob_server_blacklist.h"
#include "lib/resource/ob_resource_mgr.h"
#include "share/ob_bg_thread_monitor.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace archive;
using namespace transaction;
using namespace election;
namespace clog {

void ObCandidateInfo::reset()
{
  role_ = INVALID_ROLE;
  is_tenant_active_ = false;
  is_dest_splitting_ = false;
  on_revoke_blacklist_ = false;
  on_loop_blacklist_ = false;
  replica_type_ = REPLICA_TYPE_MAX;
  server_status_ = share::RSS_INVALID;
  is_clog_disk_full_ = false;
  is_offline_ = false;
  is_need_rebuild_ = false;
  is_partition_candidate_ = false;
  is_disk_error_ = false;
  memstore_percent_ = -1;
}

int ObCursorArrayCache::init()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < SCAN_THREAD_CNT; i++) {
    cursor_start_log_id_[i] = OB_INVALID_ID;
    cursor_end_log_id_[i] = OB_INVALID_ID;
  }
  return ret;
}

ObPartitionLogService::ObPartitionLogService()
    : is_inited_(false),
      lock_(ObLatchIds::PARTITION_LOG_LOCK),
      sw_(),
      ms_task_mgr_(),
      state_mgr_(),
      reconfirm_(),
      log_engine_(NULL),
      replay_engine_(NULL),
      election_(NULL),
      election_mgr_(NULL),
      partition_service_(NULL),
      partition_key_(),
      mm_(),
      cascading_mgr_(),
      restore_mgr_(),
      broadcast_info_mgr_(NULL),
      fetch_log_engine_(NULL),
      alloc_mgr_(NULL),
      event_scheduler_(NULL),
      self_(),
      checksum_(),
      saved_base_storage_info_(),
      zone_priority_(UINT64_MAX),
      is_candidate_(true),  // default true for recording change
      last_rebuild_time_(OB_INVALID_TIMESTAMP),
      last_check_standby_ms_time_(OB_INVALID_TIMESTAMP),
      ack_log_time_(OB_INVALID_TIMESTAMP),
      recv_child_next_ilog_ts_time_(OB_INVALID_TIMESTAMP),
      submit_log_mc_time_(OB_INVALID_TIMESTAMP),
      in_sync_check_time_(OB_INVALID_TIMESTAMP),
      update_next_replay_log_time_(OB_INVALID_TIMESTAMP),
      need_rebuild_(false),
      send_reject_msg_warn_time_(OB_INVALID_TIMESTAMP),
      leader_max_log_interval_(OB_INVALID_TIMESTAMP),
      cannot_slide_sw_interval_(OB_INVALID_TIMESTAMP),
      remote_log_query_engine_(NULL),
      archive_mgr_(NULL),
      log_archive_status_(),
      is_in_leader_member_list_(true),
      last_leader_keepalive_time_(OB_INVALID_TIMESTAMP),
      last_check_state_time_(OB_INVALID_TIMESTAMP),
      event_task_lock_(),
      has_event_task_(false),
      max_flushed_ilog_id_(0),
      scan_confirmed_log_cnt_(0),
      election_has_removed_(true),
      cursor_array_cache_(NULL)
{}

int ObPartitionLogService::init(ObILogEngine* log_engine, ObLogReplayEngineWrapper* replay_engine,
    ObIFetchLogEngine* fetch_log_engine, election::ObIElectionMgr* election_mgr,
    storage::ObPartitionService* partition_service, ObILogCallbackEngine* cb_engine, common::ObILogAllocator* alloc_mgr,
    ObLogEventScheduler* event_scheduler, const ObAddr& self, const ObVersion& data_version,
    const ObPartitionKey& partition_key, const ObReplicaType replica_type, const ObReplicaProperty replica_property,
    const ObBaseStorageInfo& base_storage_info, const int16_t archive_restore_state, const bool need_skip_mlist_check,
    ObRemoteLogQueryEngine* remote_log_query_engine, ObArchiveMgr* archive_mgr,
    ObArchiveRestoreEngine* archive_restore_engine, const enum ObCreatePlsType& create_pls_type)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(log_engine) || OB_ISNULL(replay_engine) || OB_ISNULL(fetch_log_engine) ||
             OB_ISNULL(election_mgr) || OB_ISNULL(partition_service) || OB_ISNULL(cb_engine) || OB_ISNULL(alloc_mgr) ||
             OB_ISNULL(event_scheduler) || !self.is_valid() || !data_version.is_valid() || !partition_key.is_valid() ||
             OB_ISNULL(remote_log_query_engine) || OB_ISNULL(archive_mgr) || OB_ISNULL(archive_restore_engine)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(partition_key), KP(archive_mgr), KP(archive_restore_engine));
  } else if (OB_FAIL(checksum_.init(base_storage_info.get_last_replay_log_id() + 1,
                 base_storage_info.get_accumulate_checksum(),
                 partition_key))) {
    CLOG_LOG(WARN, "checksum init failed", K(partition_key), K(ret));
  } else if (OB_FAIL(
                 election_mgr->add_partition(partition_key, base_storage_info.get_replica_num(), this, election_))) {
    CLOG_LOG(WARN, "election_mgr->add_partition failed", K(partition_key), K(ret));
  } else if (OB_FAIL(state_mgr_.init(&sw_,
                 &reconfirm_,
                 log_engine,
                 &mm_,
                 &cascading_mgr_,
                 &restore_mgr_,
                 election_,
                 replay_engine,
                 partition_service,
                 alloc_mgr,
                 self,
                 base_storage_info.get_proposal_id(),
                 data_version,
                 partition_key,
                 create_pls_type))) {
    CLOG_LOG(WARN, "state_mgr init failed", K(partition_key), K(ret));
  } else if (OB_FAIL(ms_task_mgr_.init(log_engine,
                 &state_mgr_,
                 &mm_,
                 &cascading_mgr_,
                 cb_engine,
                 &restore_mgr_,
                 partition_service,
                 alloc_mgr,
                 self,
                 partition_key))) {
    CLOG_LOG(WARN, "ms_task_mgr init failed", K(partition_key), K(ret));
  } else if (OB_FAIL(reconfirm_.init(&sw_,
                 &state_mgr_,
                 &mm_,
                 &cascading_mgr_,
                 log_engine,
                 alloc_mgr,
                 partition_key,
                 self,
                 base_storage_info.get_submit_timestamp()))) {
    CLOG_LOG(WARN, "reconfirm init failed", K(partition_key), K(ret));
  } else if (OB_FAIL(sw_.init(replay_engine,
                 log_engine,
                 &state_mgr_,
                 &mm_,
                 &cascading_mgr_,
                 partition_service,
                 alloc_mgr,
                 &checksum_,
                 cb_engine,
                 &restore_mgr_,
                 self,
                 partition_key,
                 base_storage_info.get_epoch_id(),
                 base_storage_info.get_last_replay_log_id(),
                 base_storage_info.get_submit_timestamp(),
                 base_storage_info.get_accumulate_checksum()))) {
    CLOG_LOG(WARN, "sliding_window init failed", K(partition_key), K(ret));
  } else if (OB_FAIL(restore_mgr_.init(partition_service,
                 archive_restore_engine,
                 &sw_,
                 log_engine,
                 &mm_,
                 partition_key,
                 self,
                 archive_restore_state))) {
    CLOG_LOG(WARN, "restore_mgr init failed", K(partition_key), K(ret));
  } else if (OB_FAIL(mm_.init(base_storage_info.get_curr_member_list(),
                 base_storage_info.get_membership_timestamp(),
                 base_storage_info.get_membership_log_id(),
                 base_storage_info.get_ms_proposal_id(),
                 replica_type,
                 replica_property,
                 base_storage_info.get_replica_num(),
                 need_skip_mlist_check,
                 self,
                 partition_key,
                 &sw_,
                 &ms_task_mgr_,
                 &state_mgr_,
                 &cascading_mgr_,
                 cb_engine,
                 partition_service,
                 alloc_mgr))) {
    CLOG_LOG(WARN, "membership_mgr init failed", K(partition_key), K(base_storage_info), K(ret));
  } else if (OB_FAIL(
                 cascading_mgr_.init(self, partition_key, &sw_, &state_mgr_, &mm_, log_engine, partition_service))) {
    CLOG_LOG(WARN, "cascading_mgr_ init failed", K(partition_key), K(ret));
  } else if (OB_FAIL(saved_base_storage_info_.deep_copy(base_storage_info))) {
    CLOG_LOG(WARN, "base_storage_info deep_copy failed", K(partition_key), K(ret));
  } else if (REPLICA_TYPE_LOGONLY == replica_type && OB_FAIL(alloc_broadcast_info_mgr_(partition_key))) {
    CLOG_LOG(WARN, "alloc_broadcast_info_mgr_ failed", K(partition_key), K(ret));
  } else if (ASSIGN_PARTITION == create_pls_type && OB_FAIL(alloc_cursor_array_cache_(partition_key))) {
    CLOG_LOG(WARN, "alloc_cursor_array_cache_ failed", K(partition_key), K(ret));
  } else {
    log_engine_ = log_engine;
    replay_engine_ = replay_engine;
    election_mgr_ = election_mgr;
    partition_service_ = partition_service;
    partition_key_ = partition_key;
    fetch_log_engine_ = fetch_log_engine;
    alloc_mgr_ = alloc_mgr;
    event_scheduler_ = event_scheduler;
    self_ = self;
    remote_log_query_engine_ = remote_log_query_engine;
    archive_mgr_ = archive_mgr;
    max_flushed_ilog_id_ = 0;
    scan_confirmed_log_cnt_ = 0;
    is_inited_ = true;
    election_has_removed_ = false;
  }

  if (OB_SUCCESS != ret && !is_inited_) {
    // election obj need to be removed when pls init failed
    // state_mgr_ need to destroy before remove election, because it holds election pointer
    if (state_mgr_.is_inited()) {
      state_mgr_.destroy();
    }
    // remove election
    int tmp_ret = OB_SUCCESS;
    if (NULL != election_mgr && NULL != election_ &&
        OB_SUCCESS != (tmp_ret = election_mgr->remove_partition(partition_key))) {
      CLOG_LOG(WARN, "remove_partition from election_mgr failed", K(tmp_ret), K(partition_key));
    }
    // revert election
    if (NULL != election_mgr && NULL != election_) {
      election_mgr->revert_election(election_);
      election_ = NULL;
    } else {
      CLOG_LOG(WARN, "revert election to election_mgr failed", K_(partition_key), KP(election_mgr), KP_(election));
    }
    free_cursor_array_cache_();
    free_broadcast_info_mgr_();
  }
  FLOG_INFO("ObPartitionLogService init finished",
      K_(partition_key),
      K(ret),
      K(self),
      K(base_storage_info),
      K_(election_has_removed),
      K_(election),
      KP(this));
  return ret;
}

int ObPartitionLogService::get_log_id_timestamp(const int64_t base_timestamp, ObLogMeta& log_meta)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  static int64_t retry_count = 1;
  ret = E(EventTable::EN_BIG_ROW_REPLAY_FOR_MINORING) OB_SUCCESS;
  if (OB_SUCCESS != ret) {
    ATOMIC_FAA(&retry_count, 1);
    if (0 == (retry_count % 3)) {
      ret = OB_SUCCESS;
    } else {
      // ERRSIM: make retry times at least 3
      TRANS_LOG(WARN, "get log id and timestamp need retry", K(retry_count));
      return ret;
    }
  }
#endif
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (base_timestamp <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(base_timestamp));
  } else if (!log_engine_->is_disk_space_enough()) {
    ret = OB_LOG_OUTOF_DISK_SPACE;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "log outof disk space", K(ret), K(partition_key_));
    }
  } else if (lock_.try_rdlock()) {
    uint64_t log_id = 0;
    int64_t timestamp = 0;

    if (OB_FAIL(state_mgr_.check_role_leader_and_state())) {
      CLOG_LOG(WARN, "check_role_leader_and_state false", K_(partition_key), K(ret));
    } else if (state_mgr_.is_switching_cluster_replica(partition_key_.get_table_id())) {
      // non-private table's leader is in switching state, return -4203 to trigger retry in transaction module
      ret = OB_EAGAIN;
    } else if (!state_mgr_.can_submit_log()) {
      ret = OB_NOT_MASTER;
      CLOG_LOG(WARN,
          "can_submit_log return false",
          K_(partition_key),
          K(ret),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state());
    } else if (!mm_.is_state_init() || state_mgr_.is_pre_member_changing()) {
      if (partition_reach_time_interval(2 * 1000, submit_log_mc_time_)) {
        CLOG_LOG(INFO,
            "member change is going on",
            K_(partition_key),
            K(mm_.is_state_init()),
            K(state_mgr_.is_pre_member_changing()));
      }
      ret = OB_EAGAIN;
    } else if (OB_FAIL(sw_.alloc_log_id(base_timestamp, log_id, timestamp))) {
      if (OB_EAGAIN == ret) {
        ret = OB_TOO_LARGE_LOG_ID;
      }
    } else if (OB_FAIL(log_meta.set(log_id, timestamp, state_mgr_.get_proposal_id()))) {
      CLOG_LOG(WARN, "set log meta failed", K(ret), K(log_id), K(timestamp), K(state_mgr_.get_proposal_id()));
    } else {
    }
    lock_.rdunlock();
  } else {
    ret = OB_EAGAIN;
  }

  return ret;
}

int ObPartitionLogService::submit_aggre_log(ObAggreBuffer* buffer, const int64_t base_timestamp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!log_engine_->is_disk_space_enough()) {
    ret = OB_LOG_OUTOF_DISK_SPACE;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "log outof disk space", K(ret), K(partition_key_));
    }
  } else if (lock_.try_rdlock()) {
    if (OB_FAIL(state_mgr_.check_role_leader_and_state())) {
      CLOG_LOG(WARN, "check_role_leader_and_state false", K_(partition_key), K(ret));
    } else if (state_mgr_.is_switching_cluster_replica(partition_key_.get_table_id())) {
      // non-private table's leader is in switching state, return -4203 to trigger retry in transaction module
      ret = OB_EAGAIN;
    } else if (!state_mgr_.can_submit_log()) {
      ret = OB_NOT_MASTER;
      CLOG_LOG(WARN,
          "can_submit_log return false",
          K_(partition_key),
          K(ret),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state());
    } else if (!mm_.is_state_init() || state_mgr_.is_pre_member_changing()) {
      if (partition_reach_time_interval(2 * 1000, submit_log_mc_time_)) {
        CLOG_LOG(INFO,
            "member change is going on",
            K_(partition_key),
            K(mm_.is_state_init()),
            K(state_mgr_.is_pre_member_changing()));
      }
      ret = OB_EAGAIN;
    } else if (OB_FAIL(sw_.submit_aggre_log(buffer, base_timestamp))) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN,
            "submit_log failed",
            K(ret),
            K_(partition_key),
            "role",
            state_mgr_.get_role(),
            "state",
            state_mgr_.get_state());
      }
    }

    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      int tmp_ret = OB_SUCCESS;
      const uint32_t revoke_type = ObElection::RevokeType::SUBMIT_LOG_MEMORY_ALLOC_FAIL;
      if (OB_SUCCESS != (tmp_ret = election_mgr_->leader_revoke(partition_key_, revoke_type))) {
        CLOG_LOG(ERROR, "election_mgr_ leader_revoke failed", K(ret), K(tmp_ret), K(partition_key_), K(revoke_type));
      }
    }
    lock_.rdunlock();
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObPartitionLogService::submit_log(const char* buff, const int64_t size, const int64_t base_timestamp,
    ObISubmitLogCb* submit_cb, const bool is_trans_log, uint64_t& log_id, int64_t& log_timestamp)
{
  int ret = OB_SUCCESS;
  const ObLogType log_type = OB_LOG_SUBMIT;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buff) || size > OB_MAX_LOG_ALLOWED_SIZE || base_timestamp < 0 ||
             base_timestamp > INT64_MAX / 2 || OB_ISNULL(submit_cb)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K_(partition_key), KP(buff), K(size), K(base_timestamp), KP(submit_cb));
  } else if (!log_engine_->is_disk_space_enough()) {
    ret = OB_LOG_OUTOF_DISK_SPACE;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "log outof disk space", K(ret), K(partition_key_));
    }
  } else if (lock_.try_rdlock()) {
    if (NULL != submit_cb) {
      submit_cb->base_class_reset();
    }

    if (OB_FAIL(state_mgr_.check_role_leader_and_state())) {
      CLOG_LOG(WARN, "check_role_leader_and_state false", K_(partition_key), K(ret));
    } else if (state_mgr_.is_switching_cluster_replica(partition_key_.get_table_id())) {
      // non-private table's leader is in switching state, return -4203 to trigger retry in transaction module
      ret = OB_EAGAIN;
    } else if (!state_mgr_.can_submit_log()) {
      ret = OB_NOT_MASTER;
      CLOG_LOG(WARN,
          "can_submit_log return false",
          K_(partition_key),
          K(ret),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state());
    } else if (!mm_.is_state_init() || state_mgr_.is_pre_member_changing()) {
      if (partition_reach_time_interval(2 * 1000, submit_log_mc_time_)) {
        CLOG_LOG(INFO,
            "member change is going on",
            K_(partition_key),
            K(mm_.is_state_init()),
            K(state_mgr_.is_pre_member_changing()));
      }
      ret = OB_EAGAIN;
    } else if (OB_FAIL(sw_.submit_log(
                   log_type, buff, size, base_timestamp, is_trans_log, submit_cb, log_id, log_timestamp))) {
      const int tmp_ret = ret;
      if (OB_ALLOCATE_MEMORY_FAILED != ret) {
        ret = OB_EAGAIN;
      }
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN,
            "submit_log failed",
            K(ret),
            K(tmp_ret),
            K_(partition_key),
            KP(buff),
            K(size),
            "role",
            state_mgr_.get_role(),
            "state",
            state_mgr_.get_state());
      }
    }

    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      int tmp_ret = OB_SUCCESS;
      const uint32_t revoke_type = ObElection::RevokeType::SUBMIT_LOG_MEMORY_ALLOC_FAIL;
      if (OB_SUCCESS != (tmp_ret = election_mgr_->leader_revoke(partition_key_, revoke_type))) {
        CLOG_LOG(ERROR, "election_mgr_ leader_revoke failed", K(ret), K(tmp_ret), K(partition_key_), K(revoke_type));
      }
    }
    lock_.rdunlock();
  } else {
    ret = OB_EAGAIN;
  }
  if (OB_FAIL(ret) && OB_NOT_INIT != ret && OB_INVALID_ARGUMENT != ret && OB_NOT_MASTER != ret && OB_EAGAIN != ret &&
      OB_ALLOCATE_MEMORY_FAILED != ret) {
    CLOG_LOG(ERROR,
        "submit_log failed",
        K(ret),
        K_(partition_key),
        KP(buff),
        K(size),
        K(base_timestamp),
        K(is_trans_log),
        "role",
        state_mgr_.get_role(),
        "state",
        state_mgr_.get_state());
  }
  return ret;
}

int ObPartitionLogService::get_role(common::ObRole& role) const
{
  // this func returns real role
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    role = FOLLOWER;
    if (!restore_mgr_.is_archive_restoring()) {
      int64_t unused_epoch = 0;
      ObTsWindows unused_windows;
      if (OB_SUCCESS == get_role_unlock(unused_epoch, unused_windows)) {
        // LEADER or STANDBY_LEADER or RESTORE_LEADER, and lease is valid
        role = state_mgr_.get_role();
      } else {
        // lease is expired or FOLLOWER role
        role = FOLLOWER;
      }
    } else {
      role = restore_mgr_.get_role();
    }
  }
  return ret;
}

int ObPartitionLogService::get_role_unsafe(int64_t& leader_epoch, ObTsWindows& changing_leader_windows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_.is_leader_active() || state_mgr_.is_offline()) {
    ret = OB_NOT_MASTER;
  } else if (state_mgr_.is_changing_leader() && !check_election_leader_(leader_epoch, changing_leader_windows)) {
    // it is changing leader or lease is expired
    ret = OB_NOT_MASTER;
  } else {
    leader_epoch = state_mgr_.get_leader_epoch();
    if (!election::ObElection::is_valid_leader_epoch(leader_epoch)) {
      // leader_epoch is invalid
      ret = OB_NOT_MASTER;
      TRANS_LOG(WARN, "leader epoch not big than 0, unexpected error", K(ret), K(leader_epoch));
    }
  }
  return ret;
}

int ObPartitionLogService::get_role_unlock(int64_t& leader_epoch, ObTsWindows& changing_leader_windows) const
{
  // return -4038 when state is not ACTIVE or role is not LEADER, STANDBY_LEADER or RESTORE_LEADER
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = state_mgr_.check_role_elect_leader_and_state())) {
    ret = OB_NOT_MASTER;
  } else if (!check_election_leader_(leader_epoch, changing_leader_windows)) {
    ret = OB_NOT_MASTER;
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

// used for reporting meta_table
int ObPartitionLogService::get_role_for_partition_table(common::ObRole& role) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    role = FOLLOWER;
    if (!restore_mgr_.is_archive_restoring()) {
      int64_t unused_epoch = 0;
      ObTsWindows unused_windows;
      if (OB_SUCCESS == get_role_for_partition_table_unlock(unused_epoch, unused_windows)) {
        // LEADER or STANDBY_LEADER or RESTORE_LEADER, and lease is valid
        role = state_mgr_.get_role();
      } else {
        // lease is expired or role is FOLLOWER
        role = FOLLOWER;
      }
    } else {
      role = restore_mgr_.get_role();
    }
  }
  return ret;
}

int ObPartitionLogService::get_role_for_partition_table_unlock(
    int64_t& leader_epoch, ObTsWindows& changing_leader_windows) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    const common::ObRole role = state_mgr_.get_role();
    if (!common::is_leader_by_election(role)) {
      // check role
      ret = OB_NOT_MASTER;
    } else if (!check_election_leader_(leader_epoch, changing_leader_windows)) {
      // check election lease
      ret = OB_NOT_MASTER;
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPartitionLogService::get_role_and_last_leader_active_time(common::ObRole& role, int64_t& timestamp) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    role = FOLLOWER;
    timestamp = OB_INVALID_TIMESTAMP;

    if (!restore_mgr_.is_archive_restoring()) {
      int64_t unused_epoch = 0;
      ObTsWindows unused_windows;
      if (OB_SUCC(get_role_for_partition_table_unlock(unused_epoch, unused_windows))) {
        role = state_mgr_.get_role();
      } else if (OB_NOT_MASTER == ret) {
        ret = OB_SUCCESS;
      } else {
        // do nothing
      }
      timestamp = state_mgr_.get_last_leader_active_time();
    } else {
      role = restore_mgr_.get_role();
      timestamp = restore_mgr_.get_last_takeover_time();
    }
  }
  return ret;
}

int ObPartitionLogService::get_role_and_leader_epoch(common::ObRole& role, int64_t& leader_epoch)
{
  int64_t unused_takeover_time = 0;
  return get_role_and_leader_epoch(role, leader_epoch, unused_takeover_time);
}

int ObPartitionLogService::get_role_and_leader_epoch(
    common::ObRole& role, int64_t& leader_epoch, int64_t& takeover_time)
{
  int ret = OB_SUCCESS;
  takeover_time = OB_INVALID_TIMESTAMP;

  if (IS_NOT_INIT) {
    ret = OB_SUCCESS;
  } else {
    RLockGuard guard(lock_);
    (void)(get_role_and_leader_epoch_unlock_(role, leader_epoch, takeover_time));
  }
  return ret;
}

int ObPartitionLogService::add_member(const ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
{
  int ret = OB_SUCCESS;
  if (!member.is_valid() || quorum <= 0 || quorum > OB_MAX_MEMBER_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(member));
  } else {
    ret = change_member_(member, true, quorum, log_info);
  }
  return ret;
}

int ObPartitionLogService::remove_member(const ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
{
  int ret = OB_SUCCESS;
  if (!member.is_valid() || quorum <= 0 || quorum > OB_MAX_MEMBER_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(member), K(quorum));
  } else {
    ret = change_member_(member, false, quorum, log_info);
  }
  return ret;
}

// for migrate to check if remove member is done
//------------------------------------
//     return value          |           meaning
//      OB_SUCCESS           |  remove member finished, can add replica
//      OB_EAGAIN            |  just retry check
// OB_MEMBER_CHANGE_FAILED   |  remove member failed, retry remove member
//  OB_INVALID_ARGUMENT      |  not invalid arguments
//       OTHER               |         bug!!!!
int ObPartitionLogService::is_member_change_done(const obrpc::ObMCLogInfo& log_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogTask* task = NULL;
  const int64_t* ref = NULL;
  uint64_t last_replay_log_id = OB_INVALID_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!log_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(partition_key_), K(log_info));
  } else if (state_mgr_.is_can_elect_standby_leader()) {
    // member change for standby cluster
    if (ms_task_mgr_.is_renew_ms_log_majority_success(log_info.log_id_, log_info.timestamp_)) {
      CLOG_LOG(INFO, "standby member change finished", K(ret), K(partition_key_), K(log_info));
    } else {
      ret = OB_EAGAIN;
    }
  } else {
    if (OB_FAIL(get_storage_last_replay_log_id_(last_replay_log_id))) {
      CLOG_LOG(WARN, "get_storage_last_replay_log_id_ failed", K(ret), K(partition_key_));
    } else if (OB_ERROR_OUT_OF_RANGE == (ret = sw_.get_log_task(log_info.log_id_, task, ref))) {
      ObLogCursorExt log_cursor;
      log_cursor.reset();
      ret = OB_SUCCESS;
      if (OB_FAIL(log_engine_->get_cursor(partition_key_, log_info.log_id_, log_cursor))) {
        CLOG_LOG(WARN, "not hit in cursor cache", K_(partition_key), K(ret), K(log_info));
        if ((OB_CURSOR_NOT_EXIST == ret && log_info.log_id_ > last_replay_log_id) || OB_ERR_OUT_OF_UPPER_BOUND == ret ||
            OB_NEED_RETRY == ret) {
          ret = OB_EAGAIN;
        }
      } else {
        ObLogEntry log_entry;
        ObReadParam read_param;
        read_param.file_id_ = log_cursor.get_file_id();
        read_param.offset_ = log_cursor.get_offset();
        read_param.read_len_ = log_cursor.get_size();
        ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID);
        ObReadBuf& rbuf = guard.get_read_buf();
        if (OB_UNLIKELY(!rbuf.is_valid())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          CLOG_LOG(WARN, "failed to alloc read_buf", K_(partition_key), K(log_info), K(read_param), K(ret));
        } else if (OB_FAIL(log_engine_->read_log_by_location(read_param, rbuf, log_entry))) {
          CLOG_LOG(WARN, "read_log_by_location failed", K_(partition_key), K(ret), K(read_param));
        } else if (OB_LOG_MEMBERSHIP == log_entry.get_header().get_log_type() &&
                   log_info.timestamp_ == log_entry.get_header().get_submit_timestamp()) {
          CLOG_LOG(INFO, "member ship log is slide out", K_(partition_key), K(ret), K(log_entry));
          // SUCCESS
        } else {
          ret = OB_MEMBER_CHANGE_FAILED;
          CLOG_LOG(INFO, "log rewritten, member change failed", K_(partition_key), K(ret), K(log_info), K(log_entry));
        }
      }
    } else {
      RLockGuard guard(lock_);
      const uint64_t max_log_id = sw_.get_max_log_id();
      if (!state_mgr_.is_leader_active()) {
        ret = OB_EAGAIN;
      } else if (OB_ERR_NULL_VALUE == ret && log_info.log_id_ > max_log_id) {
        ret = OB_MEMBER_CHANGE_FAILED;
        CLOG_LOG(INFO,
            "leader active got null value, member change failed",
            K_(partition_key),
            K(ret),
            K(log_info),
            K(max_log_id));
      } else if (OB_ERR_NULL_VALUE == ret && log_info.log_id_ <= max_log_id) {
        ret = OB_EAGAIN;
        CLOG_LOG(INFO,
            "leader active got null value and log_id is less than max_log_id, need retry",
            K(ret),
            K(partition_key_),
            K(log_info),
            K(max_log_id));
      } else if (OB_SUCC(ret)) {
        task->lock();
        if (task->is_submit_log_exist()) {
          if (OB_LOG_MEMBERSHIP != task->get_log_type() || log_info.timestamp_ != task->get_submit_timestamp()) {
            ret = OB_MEMBER_CHANGE_FAILED;
            CLOG_LOG(INFO,
                "log in sliding window rewritten, member change failed",
                K_(partition_key),
                K(ret),
                K(log_info),
                K(task));
          } else {
            if (task->is_log_confirmed()) {
              CLOG_LOG(INFO,
                  "member ship log in sliding window confirmed, member change success",
                  K_(partition_key),
                  K(ret),
                  K(log_info),
                  K(task));
              // SUCCESS
            } else {
              ret = OB_EAGAIN;
              CLOG_LOG(TRACE,
                  "member ship log in sliding window not confirmed, need check again",
                  K_(partition_key),
                  K(ret),
                  K(log_info),
                  K(task));
            }
          }
        } else {
          ret = OB_MEMBER_CHANGE_FAILED;
          CLOG_LOG(INFO,
              "leader active just got confirm info, must not be member ship log",
              K_(partition_key),
              K(ret),
              K(log_info),
              K(task));
        }
        task->unlock();
      } else {
        CLOG_LOG(WARN, "get_log_task failed", K_(partition_key), K(ret), K(log_info));
      }
    }

    if (NULL != ref && OB_SUCCESS != (tmp_ret = sw_.revert_log_task(ref))) {
      CLOG_LOG(ERROR, "revert log task failed", K_(partition_key), K(tmp_ret), K(ref));
    } else {
      ref = NULL;
    }
  }

  // unexpected error code, please check it !
  if (OB_SUCCESS != ret && OB_EAGAIN != ret && OB_MEMBER_CHANGE_FAILED != ret && OB_INVALID_ARGUMENT != ret) {
    CLOG_LOG(
        ERROR, "is_member_change_done unexpected error code, please check it!", K(ret), K(partition_key_), K(log_info));
  }
  return ret;
}

int ObPartitionLogService::change_member_(
    const ObMember& member, const bool is_add_member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
{
  int ret = OB_SUCCESS;
  int64_t mc_timestamp = OB_INVALID_TIMESTAMP;
  ObMemberList member_list;
  ObProposalID proposal_id;
  bool is_standby_op = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!member.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(member), K(quorum), K(ret));
  } else {
    // ========== step 1 ===========
    // 1) check it can do chagne member
    // 2) get current membership_ts and member_list
    // 3) get current proposal_id
    //
    // step 1: both remove_member/add_member need exec
    do {
      RLockGuard guard(lock_);
      if (!state_mgr_.can_change_member()) {
        CLOG_LOG(WARN,
            "can not change_member",
            K_(partition_key),
            K(member),
            "role",
            state_mgr_.get_role(),
            "state",
            state_mgr_.get_state(),
            "is_changing_leader",
            state_mgr_.is_changing_leader(),
            K(is_add_member));
        ret = OB_STATE_NOT_MATCH;
      } else {
        mc_timestamp = mm_.get_timestamp();
        is_standby_op = (STANDBY_LEADER == state_mgr_.get_role() ? true : false);
        if (!is_standby_op) {
          proposal_id = state_mgr_.get_proposal_id();
        } else {
          proposal_id = mm_.get_ms_proposal_id();
        }
        if (OB_SUCCESS != (ret = member_list.deep_copy(mm_.get_curr_member_list()))) {
          CLOG_LOG(WARN, "member_list deep_copy failed", K_(partition_key), K(ret));
        } else {
          const int64_t member_num = member_list.get_member_number();
          if (is_add_member && quorum < (member_num + 1)) {
            // quorum is smaller than the number of new member_list
            ret = OB_NOT_SUPPORTED;
          } else if (!is_add_member && quorum < (member_num - 1)) {
            // quorum is smaller than the number of new member_list
            ret = OB_NOT_SUPPORTED;
          } else {
            // do nothing
          }
        }
      }
    } while (0);

    if (OB_SUCC(ret) && !is_add_member) {
      if (OB_FAIL(block_partition_split_())) {
        CLOG_LOG(WARN, "failed to block partition split", K(ret), K_(partition_key));
      }
    }

    // ========== step 2 ===========
    // check follower's state, it requires that follower's lags is behind leader's within 1000 entries
    //
    // step 2: both remove_member/add_member need exec
    if (OB_SUCCESS == ret && !check_remote_mc_ts_(member_list, member.get_server(), mc_timestamp)) {
      CLOG_LOG(WARN, "check_remote_mc_ts_ false", K_(partition_key), K(member_list), K(member), K(mc_timestamp));
      ret = OB_LOG_NOT_SYNC;
    }

    // ========== step 3 ===========
    // 1) Check that there are no ongoing membership changes
    // 2) stop submitting log
    //
    // step 3: only remove_member need exec
    if (OB_SUCC(ret) && !is_add_member) {
      WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
      if (OB_FAIL(ret)) {
        CLOG_LOG(
            WARN, "change_member wrlock failed", K(ret), K_(partition_key), K(member), K(is_add_member), K(quorum));
      } else if (state_mgr_.is_pre_member_changing()) {
        CLOG_LOG(WARN, "is_pre_member_changing true", K_(partition_key), K(member_list), K(member));
        ret = OB_STATE_NOT_MATCH;
      } else {
        state_mgr_.set_pre_member_changing();
      }
    }

    // ========== step 4 ===========
    // wait local logs confirmed, waiting 200ms at most without lock
    //
    // step 4: only remove_member need exec
    if (OB_SUCC(ret) && !is_add_member) {
      if (OB_FAIL(wait_log_confirmed_())) {
        CLOG_LOG(WARN, "wait_log_confirmed failed", K_(partition_key), K(ret), K(member_list), K(member));
        WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
        state_mgr_.reset_pre_member_changing();
        ret = OB_STATE_NOT_MATCH;
      }
    }

    // ========== step 5 ===========
    // check Follower' state, whether its logs are already confirmed by using sync RPC
    //
    // step 5: only remove_member need exec
    if (OB_SUCC(ret) && !is_add_member) {
      if (!check_remote_mc_ts_sync_(member_list, member.get_server(), mc_timestamp)) {
        CLOG_LOG(WARN, "check_remote_mc_ts_sync_ false", K_(partition_key), K(member_list), K(member), K(mc_timestamp));
        ret = OB_LOG_NOT_SYNC;
        WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
        state_mgr_.reset_pre_member_changing();
      }
    }

    // ========== step 6 ===========
    // 1) double check it can do member change
    // 2) double check current membership_ts is same with step 1
    // 3) double check current proposal_id is same with step 1
    // 4) execute member change
    //
    // step 6: both remove_member/add_member need exec
    if (OB_SUCC(ret)) {
      WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
      if (!state_mgr_.can_change_member()) {
        CLOG_LOG(WARN,
            "can not change_member",
            K_(partition_key),
            K(member),
            "role",
            state_mgr_.get_role(),
            "state",
            state_mgr_.get_state(),
            "is_changing_leader",
            state_mgr_.is_changing_leader(),
            K(is_add_member));
        ret = OB_STATE_NOT_MATCH;
      } else if (mc_timestamp != mm_.get_timestamp()) {
        CLOG_LOG(WARN,
            "timestamp not match",
            K_(partition_key),
            K(member),
            K(mc_timestamp),
            "current_timestamp",
            mm_.get_timestamp());
        ret = OB_STATE_NOT_MATCH;
      } else if (!is_standby_op && proposal_id != state_mgr_.get_proposal_id()) {
        ret = OB_STATE_NOT_MATCH;
        CLOG_LOG(WARN,
            "proposal_id not match",
            K(ret),
            K_(partition_key),
            K(member),
            K(proposal_id),
            "current_proposal_id",
            state_mgr_.get_proposal_id());
      } else if (is_standby_op && proposal_id != mm_.get_ms_proposal_id()) {
        ret = OB_STATE_NOT_MATCH;
        CLOG_LOG(WARN,
            "ms_proposal_id not match",
            K(ret),
            K_(partition_key),
            K(member),
            K(proposal_id),
            "current_proposal_id",
            mm_.get_ms_proposal_id());
      } else {
        if (is_add_member) {
          if (OB_SUCCESS != (ret = mm_.add_member(member, quorum, log_info))) {
            CLOG_LOG(WARN, "add member failed", K_(partition_key), K(ret), K(member_list), K(member));
          }
        } else {
          if (OB_SUCCESS != (ret = mm_.remove_member(member, quorum, log_info))) {
            CLOG_LOG(WARN, "remove member failed", K_(partition_key), K(ret), K(member_list), K(member));
          }
        }
      }
      if (!is_add_member) {
        // only remove_member need exec
        state_mgr_.reset_pre_member_changing();
      }
    }

    if (OB_SUCC(ret) && !is_add_member) {
      if (OB_FAIL(unblock_partition_split_())) {
        CLOG_LOG(WARN, "failed to block partition split", K(ret), K_(partition_key));
      }
    }
  }
  CLOG_LOG(INFO, "change_member function finished", K_(partition_key), K(ret), K(member), K(quorum), K(is_add_member));
  return ret;
}

int ObPartitionLogService::fill_base_storage_info(
    common::ObBaseStorageInfo& base_storage_info, uint64_t& sw_last_replay_log_id)
{
  int ret = OB_SUCCESS;
  uint64_t max_flushed_ilog_id = OB_INVALID_ID;

  if (OB_FAIL(query_max_flushed_ilog_id(max_flushed_ilog_id))) {
    CLOG_LOG(WARN, "query_max_flushed_ilog_id failed", K(ret), K(partition_key_));
  } else {
    // get max_flushed_ilog_id firstly
#ifdef ERRSIM
    if (ObServerConfig::get_instance()._ob_enable_rebuild_on_purpose) {
      // ERRSIM mode: if open _ob_enable_rebuild_on_purpose, skip checking max_flushed_ilog_id
      max_flushed_ilog_id = UINT64_MAX;
    }
#endif
    sw_last_replay_log_id = sw_.get_last_replay_log_id();
    const uint64_t last_replay_log_id = std::min(sw_last_replay_log_id, max_flushed_ilog_id);
    const int64_t last_submit_timestamp = 0;  // invalid timestamp
    const int64_t accumulate_checksum = 0;    // invalid checksum
    const int64_t epoch_id = sw_.get_epoch_id();
    const ObProposalID proposal_id = state_mgr_.get_proposal_id();

    int64_t replica_num = mm_.get_replica_num();
    int64_t ms_timestamp = mm_.get_timestamp();
    uint64_t mc_log_id = mm_.get_log_id();
    const ObProposalID ms_proposal_id = mm_.get_ms_proposal_id();

    common::ObMemberList member_list;
    if (mm_.get_curr_member_list().is_valid()) {
      if (OB_FAIL(member_list.deep_copy(mm_.get_curr_member_list()))) {
        CLOG_LOG(WARN, "member_list.deep_copy failed", K(ret), K_(partition_key));
      }
    } else if (restore_mgr_.is_standby_restore_state()) {
      // mock member_list for standby restoring replica
      ObAddr fake_server;
      if (OB_FAIL(fake_server.parse_from_cstring("127.0.0.1:12345"))) {
        STORAGE_LOG(WARN, "failed to parse fake server ip", K(ret), K_(partition_key));
      } else if (OB_FAIL(member_list.add_server(fake_server))) {
        STORAGE_LOG(WARN, "failed to add server", K(ret), K_(partition_key));
      } else {
        replica_num = 1;   // mock replica_num to 1
        ms_timestamp = 0;  // mock ms_timestamp to 0
      }
    }

    if (OB_SUCC(ret)) {
      ret = base_storage_info.init(epoch_id,
          proposal_id,
          last_replay_log_id,
          last_submit_timestamp,
          accumulate_checksum,
          replica_num,
          ms_timestamp,
          mc_log_id,
          member_list,
          ms_proposal_id);
    }
  }
  CLOG_LOG(INFO, "get_current_base_storage_info", K(ret), K_(partition_key), K(base_storage_info));
  return ret;
}

int ObPartitionLogService::get_base_storage_info(
    common::ObBaseStorageInfo& base_storage_info, uint64_t& sw_last_replay_log_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    if (OB_FAIL(fill_base_storage_info(base_storage_info, sw_last_replay_log_id))) {
      CLOG_LOG(WARN, "fill base storage info failed", K_(partition_key), K(ret));
    }
  }
  return ret;
}

common::ObPartitionKey ObPartitionLogService::get_partition_key() const
{
  return partition_key_;
}

int ObPartitionLogService::get_saved_base_storage_info(common::ObBaseStorageInfo& base_storage_info) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(base_storage_info.deep_copy(saved_base_storage_info_))) {
    CLOG_LOG(WARN, "base_storage_info deep_copy failed", K_(partition_key), K(ret), K(saved_base_storage_info_));
  }
  CLOG_LOG(INFO, "get_saved_base_storage_info", K_(partition_key), K(ret), K(base_storage_info));
  return ret;
}

int ObPartitionLogService::change_leader(const common::ObAddr& leader, ObTsWindows& changing_leader_windows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (leader == self_) {
    CLOG_LOG(INFO, "leader is self, do not need to change", K_(partition_key), K(leader), K_(self));
  } else {
    WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "change leader wrlock failed", K(ret), K_(partition_key), K(leader));
    } else if (!mm_.get_curr_member_list().contains(leader)) {
      ret = OB_ELECTION_WARN_NOT_CANDIDATE;
    } else if (!state_mgr_.can_change_leader()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "can not change leader",
          K(ret),
          K_(partition_key),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state(),
          "membership_mgr is_state_init",
          mm_.is_state_init(),
          "is_changing_leader",
          state_mgr_.is_changing_leader(),
          K_(self),
          "dst_server",
          leader);
    } else {
      ret = state_mgr_.change_leader_async(partition_key_, leader, changing_leader_windows);
    }
  }
  if (REACH_TIME_INTERVAL(10 * 1000)) {
    CLOG_LOG(INFO, "change leader async", K_(partition_key), K(ret), K_(self), K(leader));
  }
  return ret;
}

int ObPartitionLogService::change_restore_leader(const common::ObAddr& leader)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K_(partition_key), K(leader));
  } else if (!restore_mgr_.is_archive_restoring() || RESTORE_LEADER != restore_mgr_.get_role()) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(restore_mgr_.change_restore_leader(leader))) {
    CLOG_LOG(WARN, "change_restore_leader failed", K(ret), K_(partition_key), K(leader));
  } else {
    CLOG_LOG(INFO, "change_restore_leader success", K(ret), K_(partition_key), K(leader));
  }
  return ret;
}

int ObPartitionLogService::check_and_set_restore_progress()
{
  // responsible for switching state: RESTORE_LOG->NOT_RESTORE
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SYS_TENANT_ID == partition_key_.get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "sys partitions do not need to check restore progress", K(ret), K(partition_key_));
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid partition_service_ is NULL", K(ret), K_(partition_key));
  } else if (restore_mgr_.is_archive_restoring_log()) {
    bool has_replayed = false;
    bool has_pending_task = false;
    // RESTORE_LOG->DUMP_MEMTABLE
    if (!restore_mgr_.has_finished_fetching_log()) {
      // do nothing
    } else if (OB_FAIL(mm_.check_follower_has_pending_entry(has_pending_task))) {
      CLOG_LOG(WARN, "failed to check_follower_has_pending_entry", K(ret), K_(partition_key));
    } else if (has_pending_task) {
    } else if (OB_FAIL(sw_.check_if_all_log_replayed(has_replayed))) {
      CLOG_LOG(WARN, "failed to check_if_all_log_replayed", K(ret), K_(partition_key));
    } else if (!has_replayed) {
    } else {
      // check whether achived offline_log
      if (!restore_mgr_.has_fetched_enough_log()) {
        uint64_t offline_log_id = OB_INVALID_ID;
        if (OB_FAIL(partition_service_->get_offline_log_id(partition_key_, offline_log_id))) {
          CLOG_LOG(WARN, "failed to get_offline_log_id ", K_(partition_key), KR(ret));
        } else if (offline_log_id != OB_INVALID_ID) {
          restore_mgr_.set_fetch_log_result(OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH);
          CLOG_LOG(INFO, "partition has been dropped", K_(partition_key), K(offline_log_id));
        } else {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "log is not enough for restoring", K_(partition_key));
          SERVER_EVENT_ADD("clog_restore", "clog is not enough", "partition", partition_key_);
          const uint64_t tenant_id = partition_key_.get_tenant_id();
          int tmp_ret = OB_SUCCESS;
          int64_t job_id = 0;
          if (OB_SUCCESS != (tmp_ret = ObBackupInfoMgr::get_instance().get_restore_job_id(tenant_id, job_id))) {
            if (OB_ENTRY_NOT_EXIST != tmp_ret) {
              CLOG_LOG(WARN, "failed to get restore info", KR(ret), K(tmp_ret), K(tenant_id));
            } else {
              CLOG_LOG(INFO, "physical restore info not exist", K(tenant_id), K(tmp_ret));
            }
          } else if (OB_SUCCESS != (tmp_ret = ObRestoreFatalErrorReporter::get_instance().add_restore_error_task(
                                        tenant_id, PHYSICAL_RESTORE_MOD_CLOG, ret, job_id, MYADDR))) {
            CLOG_LOG(INFO, "failed to report restore error", KR(tmp_ret), K(tenant_id), KR(ret));
          } else { /*do nothing*/
          }
        }
      }

      if (OB_SUCC(ret) && restore_mgr_.has_fetched_enough_log()) {
        if (OB_FAIL(set_restore_flag_after_restore_log_())) {
          CLOG_LOG(WARN, "failed to check_and_set_restore_flag_after_restore_log", KR(ret), K_(partition_key));
        }
      }
    }
  } else if (restore_mgr_.is_archive_restoring_mlist()) {
    // RESTORE_MEMBER_LIST->NOT_RESTORE
    int tmp_ret = OB_SUCCESS;
    uint64_t last_restore_log_id = OB_INVALID_ID;
    int64_t unused_version = OB_INVALID_TIMESTAMP;
    const uint64_t last_slide_log_id = get_start_log_id_unsafe() - 1;
    const uint64_t ms_log_id = mm_.get_log_id();
    if (OB_SUCCESS != (tmp_ret = partition_service_->check_all_trans_in_trans_table_state(partition_key_)) &&
        (OB_ENTRY_NOT_EXIST != tmp_ret) && (OB_PARTITION_NOT_EXIST != tmp_ret)) {
      CLOG_LOG(WARN, "failed to wait_all_trans_clear", K_(partition_key), KR(tmp_ret));
    } else if (OB_SUCCESS != (tmp_ret = partition_service_->get_restore_replay_info(
                                  partition_key_, last_restore_log_id, unused_version))) {
      CLOG_LOG(WARN, "failed to get_restore_replay_info", K(tmp_ret), K_(partition_key), K(last_restore_log_id));
    } else if (OB_INVALID_ID == last_restore_log_id) {
      CLOG_LOG(WARN, "unexpected last_restore_log_id", K_(partition_key), K(last_restore_log_id));
    } else if (last_slide_log_id <= last_restore_log_id) {
      CLOG_LOG(INFO,
          "need wait start_working log slide out",
          K_(partition_key),
          K(last_slide_log_id),
          K(last_restore_log_id));
    } else if (ms_log_id > last_slide_log_id) {
      CLOG_LOG(INFO, "need wait membership_log_id slide out", K_(partition_key), K(last_slide_log_id), K(ms_log_id));
    } else {
      ObReplicaRestoreStatus flag = ObReplicaRestoreStatus::REPLICA_NOT_RESTORE;
      if (OB_FAIL(partition_service_->set_restore_flag(partition_key_, flag))) {
        CLOG_LOG(WARN, "failed to set_restore_flag", K_(partition_key), K(flag), KR(ret));
      } else {
        CLOG_LOG(INFO, "success to set_restore_flag", K_(partition_key), K(flag), KR(ret));
      }
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObPartitionLogService::set_restore_fetch_log_finished(ObArchiveFetchLogResult fetch_log_result)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(restore_mgr_.set_fetch_log_result(fetch_log_result))) {
    CLOG_LOG(WARN, "failed to set_fetch_log_result", K_(partition_key), K(fetch_log_result), KR(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObPartitionLogService::process_restore_takeover_msg(const int64_t send_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!restore_mgr_.is_archive_restoring()) {
    ret = OB_STATE_NOT_MATCH;
  } else if (ObTimeUtility::current_time() - send_ts > CASCAD_MSG_DELAY_TIME_THRESHOLD) {
    CLOG_LOG(WARN, "msg delay too much time, ignore", K_(partition_key), K(send_ts));
  } else if (OB_FAIL(restore_mgr_.leader_takeover())) {
    CLOG_LOG(WARN, "leader_takeover failed", K(ret), K_(partition_key));
  } else {
  }
  return ret;
}

int ObPartitionLogService::get_leader(common::ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    leader = state_mgr_.get_leader();
  }
  // CLOG_LOG(INFO, "get leader", K(ret), K_(partition_key), K(leader));
  return ret;
}

int ObPartitionLogService::get_clog_parent(common::ObAddr& parent, int64_t& cluster_id) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  parent.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (restore_mgr_.is_archive_restoring()) {
    parent = restore_mgr_.get_restore_leader();
    cluster_id = state_mgr_.get_self_cluster_id();
  } else {
    const share::ObCascadMember parent_member = cascading_mgr_.get_parent();
    if (parent_member.is_valid()) {
      parent = parent_member.get_server();
      cluster_id = parent_member.get_cluster_id();
    } else {
      // returns corresponding leader when parent is invalid
      share::ObCascadMember cascad_leader;
      (void)state_mgr_.get_cascad_leader(cascad_leader);
      parent = cascad_leader.get_server();
      cluster_id = cascad_leader.get_cluster_id();
    }
  }
  if (!parent.is_valid()) {
    ret = OB_NEED_RETRY;
  }
  CLOG_LOG(INFO, "get clog parent", K(ret), K_(partition_key), K(parent));
  return ret;
}

int ObPartitionLogService::get_leader_curr_member_list(common::ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_.can_get_leader_curr_member_list()) {
    CLOG_LOG(WARN, "it is not leader", K_(partition_key));
    ret = OB_STATE_NOT_MATCH;
  } else {
    ret = member_list.deep_copy(mm_.get_curr_member_list());
  }
  return ret;
}

int ObPartitionLogService::get_curr_member_list(common::ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = member_list.deep_copy(mm_.get_curr_member_list());
  }
  return ret;
}

int ObPartitionLogService::get_curr_member_list_for_report(common::ObMemberList& member_list) const
{
  // used for reporting meta table
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (state_mgr_.has_valid_member_list()) {
    ret = member_list.deep_copy(mm_.get_curr_member_list());
    CLOG_LOG(DEBUG, "get_curr_member_list_for_report", K_(partition_key), K(member_list));
  } else {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "get_curr_member_list_for_report return empty", K_(partition_key));
    }
  }
  return ret;
}

int ObPartitionLogService::try_get_curr_member_list(common::ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (lock_.try_rdlock()) {
    ret = member_list.deep_copy(mm_.get_curr_member_list());
    lock_.rdunlock();
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObPartitionLogService::get_replica_num(int64_t& replica_num) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    replica_num = mm_.get_replica_num();
  }
  return ret;
}

int ObPartitionLogService::try_get_replica_num(int64_t& replica_num) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (lock_.try_rdlock()) {
    replica_num = mm_.get_replica_num();
    lock_.rdunlock();
  } else {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObPartitionLogService::receive_log(const ObLogEntry& log_entry, const ObAddr& server, const int64_t cluster_id,
    const ObProposalID& proposal_id, const ObPushLogMode push_mode, const ReceiveLogType type)
{
  int ret = OB_SUCCESS;
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const ObLogType log_type = log_entry.get_header().get_log_type();
  uint32_t protection_level = 0;
  bool is_tenant_out_of_mem = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || (!proposal_id.is_valid() && !restore_mgr_.is_archive_restoring()) ||
             OB_ISNULL(log_engine_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(server), K(proposal_id));
  } else if (!log_entry.check_integrity() || log_entry.get_header().get_partition_key() != partition_key_) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(ERROR, "receive_log invalid log", K_(partition_key), K(ret), K(log_entry), K(server));
  } else if (OB_FAIL(replay_engine_->is_tenant_out_of_memory(partition_key_, is_tenant_out_of_mem))) {
    CLOG_LOG(WARN, "is_tenant_out_of_memory failed", K(ret), K_(partition_key));
  } else if (is_tenant_out_of_mem) {
    ret = OB_TENANT_OUT_OF_MEM;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "tenant is out of memory, cannot receive log", K(ret), K_(partition_key));
    }
  } else if (OB_LOG_RENEW_MEMBERSHIP == log_type) {
    // it should not receive renew_ms_log here
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR,
        "receive_log encounter renew_ms_log, unexpected",
        K_(partition_key),
        K(ret),
        K(log_entry),
        K(server),
        K(proposal_id),
        K(type));
  } else if (proposal_id > state_mgr_.get_proposal_id()) {
    WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN,
          "receive log wrlock failed",
          K(ret),
          K_(partition_key),
          K(log_entry),
          K(server),
          K(proposal_id),
          K(type));
    } else if (!restore_mgr_.is_archive_restoring()) {
      if (state_mgr_.can_handle_prepare_rqst(proposal_id, cluster_id)) {
        ret = state_mgr_.handle_prepare_rqst(proposal_id, server, cluster_id);
      }
    } else {
      // update proposal_id directly during physical restoring
      if (OB_FAIL(state_mgr_.update_proposal_id(proposal_id))) {
        CLOG_LOG(WARN, "update_proposal_id failed", K_(partition_key), K(ret), K(log_entry));
      }
    }
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    RLockGuard guard(lock_);
    if (cluster_id != state_mgr_.get_self_cluster_id() && STANDBY_LEADER != state_mgr_.get_role()) {
      // only STANDBY_LEADER can receive logs from different cluster
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "receive diff cluster log in unexpected role",
          K_(partition_key),
          K(ret),
          K(server),
          K(proposal_id),
          K(cluster_id),
          K(type),
          "self_cluster_id",
          state_mgr_.get_self_cluster_id());
    } else if (OB_FAIL(state_mgr_.get_standby_protection_level(protection_level))) {
      CLOG_LOG(WARN, "get_standby_protection_level failed", K_(partition_key), K(ret));
    } else if (PUSH_LOG_ASYNC == push_mode && STANDBY_LEADER == state_mgr_.get_role() &&
               (MAXIMUM_PROTECTION_LEVEL == protection_level || RESYNCHRONIZATION_LEVEL == protection_level)) {
      // standby_leader cannot receive ASYNC logs in max protection_mode to keep data consistent
      // this may occur when primary cluster switches its protection mode
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "receive log in unexpected protection_level, ignore",
          K_(partition_key),
          K(ret),
          K(log_entry),
          K(server),
          K(proposal_id),
          K(push_mode),
          K(type),
          K(protection_level));
    } else {
    }
  }

  if (OB_SUCC(ret)) {
    bool follower_can_receive_log = true;
    ObProposalID pid_in_log = log_entry.get_header().get_proposal_id();
    if (OB_LOG_MEMBERSHIP != log_entry.get_header().get_log_type() &&
        OB_LOG_START_MEMBERSHIP != log_entry.get_header().get_log_type()) {
      RLockGuard guard(lock_);
      if (!log_engine_->is_disk_space_enough()) {
        ret = OB_LOG_OUTOF_DISK_SPACE;
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "log outof disk space", K_(partition_key), K(server), K(ret));
        }
      } else if (!restore_mgr_.is_archive_restoring() &&
                 !state_mgr_.can_receive_log(proposal_id, pid_in_log, cluster_id)) {
        ret = OB_STATE_NOT_MATCH;
        CLOG_LOG(WARN,
            "can not receive_log",
            K(ret),
            K_(partition_key),
            K(server),
            "log_type",
            log_entry.get_header().get_log_type(),
            "log_id",
            log_entry.get_header().get_log_id(),
            "role",
            state_mgr_.get_role(),
            "state",
            state_mgr_.get_state());
      } else if (state_mgr_.get_role() == LEADER && state_mgr_.get_state() == RECONFIRM &&
                 try_update_freeze_version_(log_entry) &&
                 OB_SUCCESS != (ret = reconfirm_.receive_log(log_entry, server))) {
        CLOG_LOG(WARN, "reconfirm receive log failed", K_(partition_key), K(ret), K(server));
        reconfirm_.reset();
      } else if (!state_mgr_.is_standby_leader_can_receive_log_id(log_id)) {
        // standby_leader cannot receive log/confirmed_info beyond max_log_id when it is doing member change
        ret = OB_STATE_NOT_MATCH;
        CLOG_LOG(WARN,
            "standby_leader is changing member, can not receive log larger than max_log_id",
            K(ret),
            K_(partition_key),
            K(server),
            K(log_id),
            "max_log_id",
            sw_.get_max_log_id());
      } else if (LEADER != state_mgr_.get_role() &&
                 (follower_can_receive_log = sw_.check_can_receive_larger_log(log_id)) &&
                 try_update_freeze_version_(log_entry) &&
                 OB_SUCCESS != (ret = sw_.receive_log(log_entry, server, cluster_id, type))) {
        CLOG_LOG(WARN, "follower active receive log failed", K_(partition_key), K(ret), K(server), K(log_entry));
      } else {
      }
    } else {
      WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
      if (OB_FAIL(ret)) {
        CLOG_LOG(WARN,
            "receive log wrlock failed",
            K(ret),
            K_(partition_key),
            K(log_entry),
            K(server),
            K(proposal_id),
            K(type));
      } else if (!log_engine_->is_disk_space_enough()) {
        ret = OB_LOG_OUTOF_DISK_SPACE;
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "log outof disk space", K_(partition_key), K(server), K(ret));
        }
      } else if (!restore_mgr_.is_archive_restoring() &&
                 !state_mgr_.can_receive_log(proposal_id, pid_in_log, cluster_id)) {
        ret = OB_STATE_NOT_MATCH;
        CLOG_LOG(WARN,
            "can not receive_log",
            K(ret),
            K_(partition_key),
            "log_type",
            log_entry.get_header().get_log_type(),
            "role",
            state_mgr_.get_role(),
            "state",
            state_mgr_.get_state());
      } else if (state_mgr_.get_role() == LEADER && state_mgr_.get_state() == RECONFIRM &&
                 try_update_freeze_version_(log_entry) &&
                 OB_SUCCESS != (ret = reconfirm_.receive_log(log_entry, server))) {
        CLOG_LOG(WARN, "reconfirm receive log failed", K_(partition_key), K(ret), K(server));
        reconfirm_.reset();
      } else if (!state_mgr_.is_standby_leader_can_receive_log_id(log_id)) {
        // standby_leader cannot receive log/confirmed_info beyond max_log_id when it is doing member change
        ret = OB_STATE_NOT_MATCH;
        CLOG_LOG(WARN,
            "standby_leader is changing member, can not receive log larger than max_log_id",
            K(ret),
            K_(partition_key),
            K(server),
            K(log_id),
            "max_log_id",
            sw_.get_max_log_id());
      } else if (LEADER != state_mgr_.get_role() &&
                 (follower_can_receive_log = sw_.check_can_receive_larger_log(log_id)) &&
                 try_update_freeze_version_(log_entry) &&
                 OB_SUCCESS != (ret = mm_.receive_log(log_entry, server, cluster_id, type))) {
        CLOG_LOG(WARN, "follower active receive log failed", K_(partition_key), K(ret), K(server), K(log_entry));
      } else {
      }
    }
  }
  return ret;
}

// used for receiving renew_ms_log by standby replica
int ObPartitionLogService::receive_renew_ms_log(const ObLogEntry& log_entry, const ObAddr& server,
    const int64_t cluster_id, const ObProposalID& proposal_id, const ReceiveLogType type)
{
  int ret = OB_SUCCESS;
  const ObProposalID& header_proposal_id = log_entry.get_header().get_proposal_id();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || (!proposal_id.is_valid() && !restore_mgr_.is_archive_restoring()) ||
             OB_ISNULL(log_engine_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(server), K(proposal_id));
  } else if (!log_entry.check_integrity() || log_entry.get_header().get_partition_key() != partition_key_) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(ERROR, "receive_log invalid log", K_(partition_key), K(ret), K(log_entry), K(server));
  } else if (header_proposal_id <= mm_.get_ms_proposal_id()) {
    // no need update proposal_id
  } else {
    WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN,
          "receive log wrlock failed",
          K(ret),
          K_(partition_key),
          K(log_entry),
          K(server),
          K(proposal_id),
          K(header_proposal_id),
          K(type));
    } else if (!restore_mgr_.is_archive_restoring()) {
      if (state_mgr_.can_handle_standby_prepare_rqst(header_proposal_id, cluster_id)) {
        // The new leader is subject to the proposal_id, because parent may not be leader when it transfer log to child
        ret = state_mgr_.handle_standby_prepare_rqst(header_proposal_id, server, cluster_id);
      } else {
        // update proposal_id
        if (OB_FAIL(mm_.update_ms_proposal_id(header_proposal_id))) {
          CLOG_LOG(WARN, "mm_ update_ms_proposal_id failed", K_(partition_key), K(ret), K(log_entry));
        }
      }
    } else {
      // physical_restoring state, update proposal_id
      if (OB_FAIL(mm_.update_ms_proposal_id(header_proposal_id))) {
        CLOG_LOG(WARN, "mm_ update_ms_proposal_id failed", K_(partition_key), K(ret), K(log_entry));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObProposalID pid_in_log = log_entry.get_header().get_proposal_id();
    WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN,
          "receive log wrlock failed",
          K(ret),
          K_(partition_key),
          K(log_entry),
          K(server),
          K(proposal_id),
          K(type));
    } else if (!log_engine_->is_disk_space_enough()) {
      ret = OB_LOG_OUTOF_DISK_SPACE;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "log outof disk space", K_(partition_key), K(server), K(ret));
      }
    } else if (!state_mgr_.can_receive_renew_ms_log(proposal_id, pid_in_log, cluster_id)) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "can not receive_log",
          K(ret),
          K_(partition_key),
          "log_type",
          log_entry.get_header().get_log_type(),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state());
    } else if (try_update_freeze_version_(log_entry) &&
               OB_SUCCESS != (ret = mm_.receive_log(log_entry, server, cluster_id, type))) {
      CLOG_LOG(WARN, "follower active receive log failed", K_(partition_key), K(ret), K(server), K(log_entry));
    } else {
    }
  }
  return ret;
}

int ObPartitionLogService::receive_archive_log(const ObLogEntry& log_entry, const bool is_batch_committed)
{
  int ret = OB_SUCCESS;
  const ObAddr fake_server = self_;
  const ReceiveLogType fake_type = PUSH_LOG;
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const ObProposalID proposal_id = log_entry.get_header().get_proposal_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!log_entry.check_integrity() || log_entry.get_header().get_partition_key() != partition_key_) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(ERROR, "receive_log invalid log", K_(partition_key), K(ret), K(log_entry));
  } else if (!restore_mgr_.is_archive_restoring() || state_mgr_.is_offline() || !state_mgr_.is_follower_active() ||
             RESTORE_LEADER != restore_mgr_.get_role()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN,
        "receive_archive_log in unexpected state",
        K_(partition_key),
        K(ret),
        "is_archive_resotring",
        restore_mgr_.is_archive_restoring(),
        "role",
        restore_mgr_.get_role(),
        "is_offline",
        state_mgr_.is_offline());
  } else if (proposal_id > state_mgr_.get_proposal_id()) {
    WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
    if (OB_FAIL(state_mgr_.update_proposal_id(proposal_id))) {
      CLOG_LOG(WARN, "update_proposal_id failed", K_(partition_key), K(ret), K(log_entry));
    }
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    const int64_t self_cluster_id = state_mgr_.get_self_cluster_id();
    if (OB_LOG_MEMBERSHIP != log_entry.get_header().get_log_type() &&
        OB_LOG_START_MEMBERSHIP != log_entry.get_header().get_log_type()) {
      RLockGuard guard(lock_);
      if (!log_engine_->is_disk_space_enough()) {
        ret = OB_LOG_OUTOF_DISK_SPACE;
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "log outof disk space", K_(partition_key), K(ret));
        }
      } else if (state_mgr_.get_role() == FOLLOWER && sw_.check_can_receive_larger_log(log_id)) {
        if (try_update_freeze_version_(log_entry) &&
            OB_FAIL(sw_.receive_log(log_entry, fake_server, self_cluster_id, fake_type))) {
          CLOG_LOG(WARN, "follower active receive log failed", K_(partition_key), K(ret), K(log_entry));
        } else if (OB_FAIL(sw_.set_log_confirmed(log_id, is_batch_committed))) {
          if (OB_ERROR_OUT_OF_RANGE != ret) {
            CLOG_LOG(WARN, "sw_.set_log_confirmed failed", K_(partition_key), K(ret), K(log_entry));
          } else {
            // Its role may switch to LEADER during fetching log,
            // it may alredy received this log and slide it out, so we need avoid reporting error here
            ret = OB_SUCCESS;
          }
        } else {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            CLOG_LOG(INFO, "receive_archive_log succ", K_(partition_key), K(ret), K(log_entry));
          }
        }
      } else {
        ret = OB_EAGAIN;
      }
    } else {
      WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
      if (OB_FAIL(ret)) {
        CLOG_LOG(WARN, "receive log wrlock failed", K(ret), K_(partition_key), K(log_entry));
      } else if (!log_engine_->is_disk_space_enough()) {
        ret = OB_LOG_OUTOF_DISK_SPACE;
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "log outof disk space", K_(partition_key), K(ret));
        }
      } else if (state_mgr_.get_role() == FOLLOWER && sw_.check_can_receive_larger_log(log_id)) {
        if (try_update_freeze_version_(log_entry) &&
            OB_FAIL(mm_.receive_log(log_entry, fake_server, self_cluster_id, fake_type))) {
          CLOG_LOG(WARN, "follower active receive log failed", K_(partition_key), K(ret), K(log_entry));
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = sw_.set_log_confirmed(log_id, false))) {
            CLOG_LOG(WARN, "sw_.set_log_confirmed failed", K_(partition_key), K(tmp_ret), K(log_entry));
          }
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            CLOG_LOG(INFO, "receive_archive_log succ", K_(partition_key), K(ret), K(log_entry));
          }
        }
      } else {
        ret = OB_EAGAIN;
      }
    }
  }

  if (OB_ALLOCATE_MEMORY_FAILED == ret) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
      CLOG_LOG(WARN, "failed because of lack of memory", K_(partition_key), K(ret), K(log_entry));
    }
  }
  return ret;
}

int ObPartitionLogService::process_archive_checkpoint(const uint64_t next_log_id, const int64_t next_log_ts)

{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_ID == next_log_id || OB_INVALID_TIMESTAMP == next_log_ts) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(next_log_id), K(next_log_ts));
  } else if (!restore_mgr_.is_archive_restoring() || state_mgr_.is_offline() || !state_mgr_.is_follower_active() ||
             RESTORE_LEADER != restore_mgr_.get_role()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN,
        "process_archive_checkpoint in unexpected state",
        K_(partition_key),
        K(ret),
        "is_archive_resotring",
        restore_mgr_.is_archive_restoring(),
        "role",
        restore_mgr_.get_role(),
        "is_offline",
        state_mgr_.is_offline());
  } else {
    uint64_t curr_leader_next_log_id = OB_INVALID_ID;
    int64_t curr_leader_next_log_ts = OB_INVALID_TIMESTAMP;
    sw_.get_next_replay_log_id_info(curr_leader_next_log_id, curr_leader_next_log_ts);
    if (next_log_id != curr_leader_next_log_id) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        CLOG_LOG(WARN,
            "pre logs have not slide out",
            K(ret),
            K_(partition_key),
            K(next_log_id),
            K(next_log_ts),
            K(curr_leader_next_log_id));
      }
    } else if (OB_FAIL(sw_.follower_update_leader_next_log_info(next_log_id, next_log_ts))) {
      CLOG_LOG(WARN, "sw_.follower_update_leader_next_log_info failed", K(ret), K_(partition_key));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObPartitionLogService::get_restore_leader_info(bool& is_restore_leader, int64_t& leader_takeover_ts)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!restore_mgr_.is_archive_restoring()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "state not match", K(ret), K_(partition_key));
  } else {
    is_restore_leader = (RESTORE_LEADER == restore_mgr_.get_role());
    leader_takeover_ts = restore_mgr_.get_last_takeover_time();
  }
  return ret;
}

int ObPartitionLogService::fake_receive_log(
    const ObAddr& server, const uint64_t log_id, const ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !proposal_id.is_valid() || OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(server), K(proposal_id), K(log_id));
  } else {
    RLockGuard guard(lock_);
    if (proposal_id != state_mgr_.get_proposal_id()) {
      // proposal_id not match, skip
    } else if (state_mgr_.get_role() == FOLLOWER && ObReplicaTypeCheck::is_paxos_replica(mm_.get_replica_type()) &&
               state_mgr_.is_cluster_allow_vote()) {
      if (!log_engine_->is_disk_space_enough() || !sw_.check_can_receive_larger_log(log_id)) {
        // paxos replica need feedback to leader when it cannot receive log
        const ObProposalID self_proposal_id = state_mgr_.get_proposal_id();
        if (OB_FAIL(log_engine_->submit_fake_ack(server, partition_key_, log_id, self_proposal_id))) {
          CLOG_LOG(WARN, "notify_cannot_receive_log failed", K(ret), K_(partition_key), K(server), K(log_id));
        }
      } else {
        // follower need execute fetching immediately when it can receive log
        bool is_fetched = false;
        sw_.do_fetch_log(log_id, log_id + 1, POINT_FETCH_LOG_EXECUTE_TYPE, is_fetched);
      }
    }
  }
  return ret;
}

int ObPartitionLogService::ack_log(const ObAddr& server, const uint64_t log_id, const ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  bool is_in_curr_member_list = mm_.get_curr_member_list().contains(server);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_.can_receive_log_ack(proposal_id) || self_ == server) {
    if (partition_reach_time_interval(1000 * 1000, ack_log_time_)) {
      CLOG_LOG(WARN,
          "can not ack log",
          K_(partition_key),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state(),
          "current_proposal_id",
          state_mgr_.get_proposal_id(),
          K(server),
          K_(self),
          K(log_id),
          K(proposal_id));
    }
    ret = OB_STATE_NOT_MATCH;
  } else if (!is_in_curr_member_list) {
    // if not paxos memeber, record ts and skip majority count
  } else if (LEADER == state_mgr_.get_role() || STANDBY_LEADER == state_mgr_.get_role()) {
    // only leader need to count majority, other parent skip
    CLOG_LOG(DEBUG, "ack_log", K_(partition_key), K(server), K(log_id));
    bool majority = false;
    bool is_lease_valid = true;

    if (OB_SUCCESS != (ret = sw_.ack_log(log_id, server, majority))) {
      if (OB_CLOG_INVALID_ACK != ret) {
        CLOG_LOG(WARN, "sw ack_log failed", K_(partition_key), K(ret), K(log_id));
      }
    } else if (majority && is_lease_valid) {
      // this log has been majority
      if (state_mgr_.can_majority_cb(proposal_id)) {
        // LEADER executes majority_cb
        if (OB_SUCCESS != (ret = majority_cb_(log_id))) {
          CLOG_LOG(WARN, "majority cb failed", K_(partition_key), K(ret), K(log_id));
        }
      } else if (state_mgr_.can_standby_majority_cb(proposal_id)) {
        // STANDBY_LEADER executes standby_majority_cb
        share::ObCascadMember primary_leader = state_mgr_.get_primary_leader();
        if (!primary_leader.is_valid()) {
          // if current primary_leader is invalid, it can use prepared_primary_leader
          primary_leader = state_mgr_.get_prepared_primary_leader();
          if (primary_leader.is_valid() && primary_leader.get_server() != proposal_id.addr_) {
            CLOG_LOG(WARN,
                "prepared_primary_leader is not match with curr_proposal_id, ignore",
                K_(partition_key),
                K(ret),
                K(log_id),
                K(primary_leader),
                K(proposal_id));
            // reset it when it is not same with proposal_id
            primary_leader.reset();
          }
        }
        if (OB_FAIL(standby_majority_cb_(
                primary_leader.get_server(), primary_leader.get_cluster_id(), proposal_id, log_id))) {
          CLOG_LOG(WARN, "standby_majority_cb_ failed", K_(partition_key), K(ret), K(primary_leader), K(log_id));
        }
      } else {
        // do nothing
      }
    } else {
    }
  } else {
  }

  return ret;
}

int ObPartitionLogService::standby_ack_log(
    const ObAddr& server, const int64_t cluster_id, const uint64_t log_id, const ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (LEADER != state_mgr_.get_role()) {
    // self is not leader, ignore standby_ack
    // standby replica can be child of primary follower replica since 2.2.70
  } else if (!state_mgr_.can_receive_log_ack(proposal_id) || self_ == server) {
    ret = OB_STATE_NOT_MATCH;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN,
          "can not standby_ack_log",
          K(ret),
          K_(partition_key),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state(),
          "current_proposal_id",
          state_mgr_.get_proposal_id(),
          K(server),
          K_(self),
          K(log_id),
          K(proposal_id));
    }
  } else if (!GCTX.need_sync_to_standby() ||
             ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "need ignore standby_ack", K_(partition_key), K(server), K(cluster_id), K(log_id));
    }
  } else {
    share::ObCascadMember sync_child = cascading_mgr_.get_sync_standby_child();
    if (sync_child.get_server() != server) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        CLOG_LOG(WARN, "server is not curr sync_child, ignore", K_(partition_key), K(server), K(sync_child));
      }
    } else {
      bool majority = false;
      if (OB_SUCCESS != (ret = sw_.standby_ack_log(log_id, server, majority))) {
        if (OB_CLOG_INVALID_ACK != ret) {
          CLOG_LOG(WARN, "sw ack_log failed", K_(partition_key), K(ret), K(log_id));
        }
      } else if (majority && state_mgr_.can_majority_cb(proposal_id) && OB_SUCCESS != (ret = majority_cb_(log_id))) {
        CLOG_LOG(WARN, "majority cb failed", K_(partition_key), K(ret), K(log_id));
      } else {
      }
      CLOG_LOG(DEBUG, "standby_ack_log", K_(partition_key), K(server), K(cluster_id), K(log_id));
    }
  }

  return ret;
}

int ObPartitionLogService::fake_ack_log(const ObAddr& server, const uint64_t log_id, const ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  bool is_in_curr_member_list = mm_.get_curr_member_list().contains(server);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_.can_receive_log_ack(proposal_id) || self_ == server) {
    ret = OB_STATE_NOT_MATCH;
  } else if (LEADER != state_mgr_.get_role() || !is_in_curr_member_list) {
    // self is not leader or sender is not paxos memeber, ignore
  } else if (log_id != sw_.get_start_id()) {
    // log_id not match with current start_id, skip
  } else {
    const int64_t receive_ts = ObTimeUtility::current_time();
    if (OB_SUCCESS != (ret = sw_.fake_ack_log(log_id, server, receive_ts))) {
      CLOG_LOG(WARN, "sw fake_ack_log failed", K_(partition_key), K(ret), K(log_id));
    }
  }
  CLOG_LOG(DEBUG,
      "fake_ack_log finished",
      K(ret),
      K_(partition_key),
      K(server),
      K(proposal_id),
      K(log_id),
      "start_id",
      sw_.get_start_id());
  return ret;
}

int ObPartitionLogService::ack_renew_ms_log(
    const ObAddr& server, const uint64_t log_id, const int64_t submit_ts, const ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  bool is_in_curr_member_list = mm_.get_curr_member_list().contains(server);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_.can_receive_renew_ms_log_ack(ms_proposal_id) || self_ == server) {
    if (partition_reach_time_interval(1000 * 1000, ack_log_time_)) {
      CLOG_LOG(WARN,
          "can not ack log",
          K_(partition_key),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state(),
          "curr_proposal_id",
          state_mgr_.get_proposal_id(),
          "curr_ms_proposal_id",
          mm_.get_ms_proposal_id(),
          K(server),
          K_(self),
          K(log_id),
          K(ms_proposal_id));
    }
    ret = OB_STATE_NOT_MATCH;
  } else if (!is_in_curr_member_list) {
    // if not paxos memeber, record ts and skip majority count
  } else {
    // only leader need to count majority, other parent skip
    bool majority = false;
    if (OB_SUCCESS != (ret = ms_task_mgr_.ack_renew_ms_log(log_id, submit_ts, ms_proposal_id, server, majority))) {
      if (OB_CLOG_INVALID_ACK != ret) {
        CLOG_LOG(WARN, "sw ack_log failed", K_(partition_key), K(ret), K(log_id), K(ms_proposal_id));
      }
    } else if (majority && OB_SUCCESS != (ret = renew_ms_log_majority_cb_(log_id, submit_ts, ms_proposal_id))) {
      CLOG_LOG(WARN, "majority cb failed", K_(partition_key), K(ret), K(log_id));
    } else {
    }
    CLOG_LOG(INFO, "ack_renew_ms_log fnished", K_(partition_key), K(server), K(log_id), K(ms_proposal_id), K(majority));
  }

  return ret;
}

int ObPartitionLogService::resp_sw_info_when_get_log_(const ObAddr& server, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  RLockGuard rdlock_guard(lock_);
  bool is_leader = (LEADER == state_mgr_.get_role() || STANDBY_LEADER == state_mgr_.get_role());
  if (is_leader || cascading_mgr_.is_valid_child(server)) {
    uint64_t next_log_id = OB_INVALID_ID;
    int64_t next_log_ts = OB_INVALID_TIMESTAMP;
    sw_.get_next_replay_log_id_info(next_log_id, next_log_ts);
    if (OB_FAIL(log_engine_->send_keepalive_msg(
            server, cluster_id, partition_key_, next_log_id, next_log_ts, 0 /* deliver_cnt */))) {
      CLOG_LOG(WARN, "send_keepalive_msg failed", K(ret), K_(partition_key), K(server));
    }
  }
  return ret;
}

bool ObPartitionLogService::is_primary_need_sync_to_standby_() const
{
  bool bool_ret = false;
  if (GCTX.is_primary_cluster() && GCTX.need_sync_to_standby() &&
      !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    bool_ret = true;
  }
  return bool_ret;
}

int ObPartitionLogService::get_log(const common::ObAddr& server, const uint64_t start_log_id, const int64_t log_num,
    const ObFetchLogType fetch_type, const ObProposalID& proposal_id, const int64_t cluster_id,
    const common::ObReplicaType replica_type, const int64_t network_limit, const uint64_t max_confirmed_log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!server.is_valid()) || OB_UNLIKELY(start_log_id <= 0) || OB_UNLIKELY(log_num < 0) ||
             OB_FETCH_LOG_UNKNOWN == fetch_type || OB_ISNULL(log_engine_) || OB_ISNULL(fetch_log_engine_) ||
             OB_ISNULL(partition_service_) || network_limit < 0 || OB_INVALID_CLUSTER_ID == cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K_(partition_key),
        K(server),
        K(start_log_id),
        K(log_num),
        K(fetch_type),
        KP_(log_engine),
        KP_(fetch_log_engine),
        KP_(partition_service),
        K(network_limit),
        K(cluster_id));
  } else if (server == self_) {
    // ignore self_
  } else if (OB_FETCH_LOG_STANDBY_REPLICA == fetch_type && cluster_id == state_mgr_.get_self_cluster_id()) {
    CLOG_LOG(
        WARN, "receive same cluster_id standby fetch log req, unexpected", K_(partition_key), K(server), K(cluster_id));
  } else if (!state_mgr_.can_get_log(cluster_id)) {
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(WARN, "cannot get_log", K_(partition_key), K(server), K(cluster_id), K(fetch_type));
    }
  } else if (OB_FETCH_LOG_STANDBY_RESTORE == fetch_type) {
    RLockGuard guard(lock_);
    if (restore_mgr_.is_standby_restore_state()) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        CLOG_LOG(INFO, "self is still in restore state, ignore check restore msg", K_(partition_key), K(server));
      }
    } else if (LEADER == state_mgr_.get_role()) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        CLOG_LOG(INFO,
            "leader receive standby restore replica's fetch req, ignore",
            K_(partition_key),
            K(server),
            K(cluster_id));
      }
    } else if (STANDBY_LEADER == state_mgr_.get_role()) {
      bool is_in_member_list = mm_.get_curr_member_list().contains(server);
      if (OB_FAIL(log_engine_->notify_follower_log_missing(
              server, cluster_id, partition_key_, start_log_id, is_in_member_list, OB_STANDBY_RESTORE_MSG))) {
        CLOG_LOG(WARN, "notify_follower_log_missing failed", K_(partition_key), K(server));
      } else {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          CLOG_LOG(INFO, "notify follower restore finished", K_(partition_key), K(server));
        }
      }
    } else {
      // do nothing
    }
  } else {
    bool is_in_member_list = mm_.get_curr_member_list().contains(server);
    bool is_in_children_list = (cascading_mgr_.is_valid_child(server));
    if (OB_FETCH_LOG_FOLLOWER_ACTIVE == fetch_type || OB_FETCH_LOG_STANDBY_REPLICA == fetch_type) {
      RLockGuard guard(lock_);
      ret = cascading_mgr_.check_child_legal(server, cluster_id, replica_type);
    }
    // primary leader try set standby_ack according to standby_leader's max_confirmed_log_id
    // in leader reconfirm state
    if (OB_FETCH_LOG_STANDBY_REPLICA == fetch_type) {
      RLockGuard guard(lock_);
      ObCascadMember sync_child = cascading_mgr_.get_sync_standby_child();
      if (state_mgr_.is_leader_reconfirm() && is_primary_need_sync_to_standby_() &&
          OB_INVALID_ID != max_confirmed_log_id && server == sync_child.get_server() &&
          cluster_id == sync_child.get_cluster_id()) {
        int tmp_ret = OB_SUCCESS;
        uint64_t reconfirm_next_id = OB_INVALID_ID;
        if (OB_INVALID_ID == (reconfirm_next_id = reconfirm_.get_next_id())) {
          // reconfirm is initializing, skip
        } else if (OB_SUCCESS !=
                   (tmp_ret = sw_.process_sync_standby_max_confirmed_id(max_confirmed_log_id, reconfirm_next_id))) {
          CLOG_LOG(WARN,
              "process_sync_standby_max_confirmed_id failed",
              K(tmp_ret),
              K_(partition_key),
              K(server),
              K(cluster_id),
              K(max_confirmed_log_id),
              K(reconfirm_next_id));
        } else {
        }
      }
    }

    if (start_log_id > sw_.get_max_log_id() && OB_FETCH_LOG_LEADER_RECONFIRM != fetch_type) {
      // no log can be supplied for followers in this server, do nothing
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        CLOG_LOG(INFO,
            "wanted log are not supplied in this server",
            K(start_log_id),
            "sw_max_log_id",
            sw_.get_max_log_id(),
            K(server),
            K(fetch_type),
            K_(partition_key),
            K(is_in_member_list),
            K(is_in_children_list));
      }
    } else if (check_if_need_rebuild_(start_log_id, fetch_type)) {
      if (OB_FETCH_LOG_LEADER_RECONFIRM == fetch_type) {
        // new leader lagged behind too many, let the leader reconfirm fail
        // do nothing
        if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {
          CLOG_LOG(WARN,
              "new leader lagged behind too many",
              K_(partition_key),
              K(server),
              K(start_log_id),
              K(is_in_member_list),
              K(is_in_children_list));
        }
      } else {
        CLOG_LOG(INFO,
            "follower try to fetch too old log, need rebuild",
            K_(partition_key),
            K(server),
            K(start_log_id),
            K(is_in_member_list),
            K(is_in_children_list));
        (void)log_engine_->notify_follower_log_missing(
            server, cluster_id, partition_key_, start_log_id, is_in_member_list, OB_REBUILD_MSG);
      }
    } else if (log_num > 0) {
      uint64_t end_log_id = start_log_id + static_cast<uint64_t>(log_num);  // [start, end);
      const bool is_learner_fetching =
          (OB_FETCH_LOG_LEADER_RECONFIRM != fetch_type && !is_in_member_list && !is_in_children_list);
      const uint64_t end_limit_log_id =
          (is_learner_fetching ? (sw_.get_next_index_log_id()) : (sw_.get_max_log_id() + 1));
      if (OB_FETCH_LOG_LEADER_RECONFIRM != fetch_type) {
        end_log_id = MIN(end_log_id, end_limit_log_id);
      }
      uint64_t round_start_log_id = start_log_id;
      while (OB_SUCC(ret) && round_start_log_id < end_log_id) {
        uint64_t round_end_log_id = MIN(round_start_log_id + CLOG_ST_FETCH_LOG_COUNT, end_log_id);
        ObFetchLogTask* task = NULL;
        if (OB_UNLIKELY(NULL == (task = alloc_mgr_->alloc_fetch_log_task()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          CLOG_LOG(WARN, "alloc fetch_log_task failed", K(ret), K_(partition_key));
        } else if (OB_FAIL(task->init(partition_key_,
                       server,
                       cluster_id,
                       round_start_log_id,
                       round_end_log_id,
                       fetch_type,
                       proposal_id,
                       network_limit))) {
          CLOG_LOG(WARN,
              "init fetch log task error",
              K(ret),
              K_(partition_key),
              K(round_start_log_id),
              K(round_end_log_id),
              K(fetch_type),
              K(proposal_id));
        } else if (OB_FAIL(fetch_log_engine_->submit_fetch_log_task(task))) {
          CLOG_LOG(WARN, "submit fetch log task error", K(ret), K_(partition_key));
        } else {
          round_start_log_id = round_end_log_id;
          if (REACH_TIME_INTERVAL(50 * 1000)) {
            CLOG_LOG(INFO, "submit fetch log task succ", K_(partition_key), K(*task));
          }
        }
        if (OB_FAIL(ret) && NULL != task) {
          ob_slice_free_fetch_log_task(task);
          task = NULL;
        }
      }
    }
    if (!is_in_member_list && !is_in_children_list && cluster_version_before_2250_()) {
      (void)resp_sw_info_when_get_log_(server, cluster_id);
    }
    if (OB_FETCH_LOG_RESTORE_FOLLOWER == fetch_type) {
      int tmp_ret = OB_SUCCESS;
      if (RESTORE_LEADER == restore_mgr_.get_role()) {
        if (OB_SUCCESS !=
            (tmp_ret = log_engine_->send_restore_alive_msg(server, cluster_id, partition_key_, start_log_id))) {
          CLOG_LOG(WARN, "send_restore_alive_msg failed", K(tmp_ret), K(partition_key_), K(server));
        }
      }
      if (start_log_id > sw_.get_max_log_id() && sw_.get_max_log_id() <= sw_.get_max_confirmed_log_id() &&
          restore_mgr_.is_restore_log_finished()) {
        if (OB_SUCCESS !=
            (tmp_ret = log_engine_->notify_restore_log_finished(server, cluster_id, partition_key_, start_log_id))) {
          CLOG_LOG(WARN, "notify_restore_log_finished failed", K(tmp_ret), K(partition_key_), K(server));
        } else {
          CLOG_LOG(DEBUG, "notify_restore_log_finished", K(partition_key_), K(server));
        }
      }
    }
  }
  return ret;
}

int ObPartitionLogService::async_get_log(const common::ObAddr& server, const int64_t cluster_id,
    const uint64_t start_log_id, const uint64_t end_log_id, const ObFetchLogType fetch_type,
    const common::ObProposalID& proposal_id, const int64_t network_limit)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    uint32_t log_attr = ObFetchLogAttr::NO_DEMAND;
    if (OB_FETCH_LOG_STANDBY_REPLICA == fetch_type) {
      // standby replica's fetch log request, need guarantees log has reach majority in local cluster
      log_attr = ObFetchLogAttr::NEED_MAJORITY;
    }

    bool need_send_confirm_info = false;
    if (OB_FETCH_LOG_FOLLOWER_ACTIVE == fetch_type || OB_FETCH_LOG_RESTORE_FOLLOWER == fetch_type ||
        OB_FETCH_LOG_STANDBY_REPLICA == fetch_type) {
      need_send_confirm_info = true;
    }

    switch (fetch_type) {
      case OB_FETCH_LOG_FOLLOWER_ACTIVE:
      case OB_FETCH_LOG_RESTORE_FOLLOWER:
      case OB_FETCH_LOG_STANDBY_REPLICA:
        ret = get_log_follower_active_(
            server, cluster_id, start_log_id, end_log_id, fetch_type, log_attr, need_send_confirm_info, network_limit);
        break;
      case OB_FETCH_LOG_LEADER_RECONFIRM:
        ret = get_log_leader_reconfirm_(
            server, start_log_id, end_log_id, log_attr, need_send_confirm_info, proposal_id, network_limit);
        break;
      default:
        CLOG_LOG(ERROR, "wrong fetch log type", K_(partition_key), K(fetch_type));
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  }
  return ret;
}

int ObPartitionLogService::get_log_follower_active_(const common::ObAddr& server, const int64_t cluster_id,
    const uint64_t start_log_id, const uint64_t end_log_id, const ObFetchLogType& fetch_type, const uint32_t log_attr,
    const bool need_send_confirm_info, const int64_t network_limit)
{
  int ret = OB_SUCCESS;
  ObProposalID curr_proposal_id;
  bool can_get_log = false;

  do {
    RLockGuard guard(lock_);
    curr_proposal_id = state_mgr_.get_proposal_id();
    can_get_log = (state_mgr_.can_get_log(cluster_id) || restore_mgr_.is_archive_restoring() ||
                   cascading_mgr_.is_valid_child(server));
    //  need_log_confirmed = (state_mgr_.get_state() == RECONFIRM);
  } while (0);

  if (can_get_log) {
    ret = internal_fetch_log_(start_log_id,
        end_log_id,
        fetch_type,
        curr_proposal_id,
        server,
        cluster_id,
        log_attr,
        need_send_confirm_info,
        network_limit);
  }
  return ret;
}

int ObPartitionLogService::get_log_leader_reconfirm_(const common::ObAddr& server, const uint64_t start_log_id,
    const uint64_t end_log_id, const uint32_t log_attr, const bool need_send_confirm_info,
    const common::ObProposalID& proposal_id, const int64_t network_limit)
{
  int ret = OB_SUCCESS;
  ObProposalID curr_proposal_id;
  bool can_get_log = false;

  do {
    RLockGuard guard(lock_);
    curr_proposal_id = state_mgr_.get_proposal_id();
    can_get_log = state_mgr_.can_get_log_for_reconfirm(proposal_id);
  } while (0);

  if (can_get_log) {
    const int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    ret = internal_fetch_log_(start_log_id,
        end_log_id,
        OB_FETCH_LOG_LEADER_RECONFIRM,
        curr_proposal_id,
        server,
        cluster_id,
        log_attr,
        need_send_confirm_info,
        network_limit);
  }

  return ret;
}

int ObPartitionLogService::internal_fetch_log_(const uint64_t start_log_id, const uint64_t end_log_id,
    const ObFetchLogType& fetch_type, const ObProposalID& proposal_id, const common::ObAddr& server,
    const int64_t cluster_id, const uint32_t log_attr, const bool need_send_confirm_info, const int64_t network_limit)
{
  int ret = OB_SUCCESS;
  ObLogCursorExt cursor_array[ObCursorArrayCache::CURSOR_ARRAY_SIZE];
  ObGetCursorResult result;
  result.csr_arr_ = cursor_array;
  result.arr_len_ = ObCursorArrayCache::CURSOR_ARRAY_SIZE;
  result.ret_len_ = 0;
  uint64_t cursor_start_log_id = OB_INVALID_ID;
  for (uint64_t log_id = start_log_id; OB_SUCC(ret) && log_id < end_log_id; log_id++) {
    if (log_id < sw_.get_start_id()) {
      if (OB_FAIL(fetch_log_from_ilog_storage_(log_id,
              fetch_type,
              proposal_id,
              server,
              cluster_id,
              need_send_confirm_info,
              network_limit,
              result,
              cursor_start_log_id))) {
        CLOG_LOG(WARN,
            "fetch_log_from_ilog_storage_ failed",
            K(ret),
            K(partition_key_),
            K(log_id),
            K(proposal_id),
            K(server),
            K(need_send_confirm_info));
      }
    } else {
      if (OB_FAIL(fetch_log_from_sw_(
              log_id, fetch_type, proposal_id, server, cluster_id, log_attr, need_send_confirm_info, network_limit)) &&
          OB_ERROR_OUT_OF_RANGE != ret) {
        CLOG_LOG(WARN,
            "fetch_log_from_sw_ failed",
            K(ret),
            K(partition_key_),
            K(log_id),
            K(proposal_id),
            K(server),
            K(need_send_confirm_info));
      } else if (OB_ERROR_OUT_OF_RANGE == ret) {
        if (OB_FAIL(fetch_log_from_ilog_storage_(log_id,
                fetch_type,
                proposal_id,
                server,
                cluster_id,
                need_send_confirm_info,
                network_limit,
                result,
                cursor_start_log_id))) {
          CLOG_LOG(WARN,
              "fetch_log_from_ilog_storage_ failed",
              K(ret),
              K(partition_key_),
              K(log_id),
              K(proposal_id),
              K(server),
              K(need_send_confirm_info));
        }
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObPartitionLogService::fetch_log_from_sw_(const uint64_t log_id, const ObFetchLogType& fetch_type,
    const ObProposalID& proposal_id, const common::ObAddr& server, const int64_t cluster_id, const uint32_t log_attr,
    const bool need_send_confirm_info, const int64_t network_limit)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (OB_INVALID_ID == log_id || OB_FETCH_LOG_UNKNOWN == fetch_type ||
             (!proposal_id.is_valid() && !restore_mgr_.is_archive_restoring()) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(log_id), K(fetch_type), K(proposal_id), K(server));
  } else {
    ObLogCursor log_cursor;
    bool log_confirmed = false;
    int64_t accum_checksum = 0;
    bool batch_committed = false;

    if (OB_SUCCESS ==
        (ret = sw_.get_log(log_id, log_attr, log_confirmed, log_cursor, accum_checksum, batch_committed))) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "get_log return value unexpected", K(ret), K(partition_key_), K(log_id));
    } else if (OB_LOG_MISSING == ret && log_cursor.is_valid()) {
      const bool need_send_confirm_info_tmp = (need_send_confirm_info && log_confirmed);
      ret = get_log_with_cursor_(server,
          cluster_id,
          fetch_type,
          proposal_id,
          need_send_confirm_info_tmp,
          log_confirmed,
          log_cursor,
          accum_checksum,
          batch_committed,
          network_limit);
    } else if (OB_LOG_MISSING == ret || OB_ERR_NULL_VALUE == ret) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        CLOG_LOG(INFO,
            "getlog failed, log is not in sw",
            K(ret),
            K(partition_key_),
            K(log_id),
            K(fetch_type),
            K(proposal_id),
            K(server));
      }
      if (OB_FETCH_LOG_LEADER_RECONFIRM == fetch_type && OB_ERR_NULL_VALUE == ret) {
        ret = send_null_log_(server, cluster_id, log_id, proposal_id, network_limit);
      } else {
        // do nothing
        ret = OB_SUCCESS;
      }
    } else if (OB_EAGAIN == ret) {
      CLOG_LOG(INFO,
          "get log not confirmed or pinned",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(fetch_type),
          K(proposal_id),
          K(server),
          K(log_attr),
          K(log_confirmed));
      ret = OB_SUCCESS;
    } else if (OB_ERROR_OUT_OF_RANGE == ret) {
      CLOG_LOG(INFO,
          "log is not in sw, error_out_of_range",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(fetch_type),
          K(proposal_id),
          K(server));
    } else {
      CLOG_LOG(ERROR, "get log failed", K(ret), K(partition_key_), K(log_id), K(fetch_type), K(proposal_id), K(server));
    }
  }
  CLOG_LOG(TRACE,
      "fetch_log_from_sw_",
      K(ret),
      K(partition_key_),
      K(log_id),
      K(fetch_type),
      K(proposal_id),
      K(server),
      K(log_attr),
      K(need_send_confirm_info));
  return ret;
}

bool ObPartitionLogService::check_if_need_rebuild_(const uint64_t start_log_id, const ObFetchLogType fetch_type) const
{
  // caller guarantees (start_log_id <= max_log_id)
  bool need_rebuild = false;
  int ret = OB_SUCCESS;
  uint64_t last_replay_log_id = OB_INVALID_ID;
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  ObLogCursorExt log_cursor_ext;
  const uint64_t start_id = sw_.get_start_id();
  UNUSED(fetch_type);

  if (OB_FAIL(get_storage_last_replay_log_id_(last_replay_log_id))) {
    CLOG_LOG(WARN, "get_storage_last_replay_log_id_ failed", K(ret), K(partition_key_));
  } else if (start_log_id >= start_id) {
    need_rebuild = false;
    CLOG_LOG(INFO,
        "wanted log_id is greater than start_id, no need rebuild",
        K(start_log_id),
        K(start_id),
        K_(partition_key));
  } else if (start_log_id > last_replay_log_id) {
    need_rebuild = false;
    CLOG_LOG(INFO,
        "wanted log_id is greater than last_replay_log_id, no need rebuild",
        K(start_log_id),
        K(last_replay_log_id),
        K(start_id),
        K_(partition_key));
#ifdef ERRSIM
  } else if (ObServerConfig::get_instance()._ob_enable_rebuild_on_purpose && start_log_id < last_replay_log_id) {
    need_rebuild = true;
    CLOG_LOG(INFO, "req server need rebuild", K(ret), K(start_log_id), K(last_replay_log_id));
#endif
  } else if (OB_FAIL(log_engine_->get_cursor(partition_key_, start_log_id, log_cursor_ext))) {
    if (OB_ERR_OUT_OF_UPPER_BOUND == ret && start_log_id < start_id) {
      // this log does not have ilog and log_id is smaller than sw->start_id, need rebuild
      need_rebuild = true;
      CLOG_LOG(INFO,
          "fetch log err out of upper bound, need rebuild",
          K(ret),
          K(need_rebuild),
          K(partition_key_),
          K(start_log_id),
          K(start_id));
    } else if (OB_CURSOR_NOT_EXIST == ret) {
      // log cursor does not exist, need rebuild
      need_rebuild = true;
      CLOG_LOG(INFO,
          "fetch log cursor not exist, need rebuild",
          K(ret),
          K(need_rebuild),
          K(partition_key_),
          K(start_log_id),
          K(start_id));
    } else {
      CLOG_LOG(WARN, "get_cursor failed", K(ret), K(need_rebuild), K(partition_key_), K(start_log_id), K(start_id));
    }
  } else {
    const int64_t data_lag_threshold = ObServerConfig::get_instance().rebuild_replica_data_lag_threshold;
    if (data_lag_threshold > 0) {
      const uint32_t CLOG_FILE_AHEAD = (uint32_t)(data_lag_threshold / CLOG_FILE_SIZE);
      // check if need rebuild when config value is set
      min_file_id = log_engine_->get_clog_min_file_id();
      max_file_id = log_engine_->get_clog_max_file_id();
      file_id_t start_file_id = log_cursor_ext.get_file_id();
      if (max_file_id < start_file_id) {
        CLOG_LOG(WARN, "invalid max_file_id", K(partition_key_), K(max_file_id));
      } else if (start_file_id < min_file_id) {
        need_rebuild = true;
        CLOG_LOG(INFO,
            "log file of start_log_id is already recycled, need rebuild",
            K(partition_key_),
            K(start_log_id),
            K(start_file_id),
            K(min_file_id));
      } else if (max_file_id - start_file_id < CLOG_FILE_AHEAD) {
        // not reach threshold
      } else {
        need_rebuild = true;
      }
    }
  }

#ifdef ERRSIM
  if (ObServerConfig::get_instance()._ob_enable_rebuild_on_purpose &&
      (start_log_id < sw_.get_start_id() || OB_FETCH_LOG_RESTORE_FOLLOWER == fetch_type)) {
    need_rebuild = true;
    CLOG_LOG(INFO, "req server need rebuild", K(ret), K(start_log_id));
  }
#endif
  return need_rebuild;
}

int ObPartitionLogService::fetch_log_from_ilog_storage_(const uint64_t log_id, const ObFetchLogType& fetch_type,
    const ObProposalID& proposal_id, const ObAddr& server, const int64_t cluster_id, const bool need_send_confirm_info,
    const int64_t network_limit, ObGetCursorResult& result, uint64_t& cursor_start_log_id)
{
  int ret = OB_SUCCESS;
  ObLogCursorExt log_cursor_ext;
  bool read_from_clog = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (OB_INVALID_ID == log_id || OB_FETCH_LOG_UNKNOWN == fetch_type ||
             (!proposal_id.is_valid() && !restore_mgr_.is_archive_restoring()) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(partition_key_),
        K(log_id),
        K(fetch_type),
        K(proposal_id),
        K(server),
        K(need_send_confirm_info));
  } else if (OB_FAIL(
                 log_engine_->get_cursor_batch(partition_key_, log_id, log_cursor_ext, result, cursor_start_log_id))) {
    CLOG_LOG(WARN,
        "log_engine get_cursor_batch failed",
        K(ret),
        K(partition_key_),
        K(log_id),
        K(fetch_type),
        K(proposal_id),
        K(server),
        K(need_send_confirm_info));
  } else {
    ObLogEntry log_entry;
    ObReadParam read_param;
    read_param.file_id_ = log_cursor_ext.get_file_id();
    read_param.offset_ = log_cursor_ext.get_offset();
    read_param.read_len_ = log_cursor_ext.get_size();
    const uint64_t acc_cksm = log_cursor_ext.get_accum_checksum();
    const bool batch_committed = log_cursor_ext.is_batch_committed();

    ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID);
    ObReadBuf& rbuf = guard.get_read_buf();
    if (OB_UNLIKELY(!rbuf.is_valid())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "failed to alloc read_buf", K_(partition_key), K(log_id), K(read_param), K(ret));
    } else if (OB_FAIL(log_engine_->read_log_by_location(read_param, rbuf, log_entry))) {
      CLOG_LOG(WARN,
          "read log by cursor failed",
          K(ret),
          K(partition_key_),
          K(read_param),
          K(log_id),
          K(fetch_type),
          K(proposal_id),
          K(server),
          K(need_send_confirm_info));
      read_from_clog = false;
    } else if (OB_FETCH_LOG_STANDBY_REPLICA == fetch_type &&
               OB_FAIL(log_entry.update_header_proposal_id(proposal_id))) {
      // fix issue #29473286
      CLOG_LOG(WARN,
          "update_header_proposal_id failed",
          K(ret),
          K_(partition_key),
          K(log_id),
          K(fetch_type),
          K(proposal_id),
          K(log_entry));
    } else if (OB_FAIL(send_log_(server, cluster_id, log_entry, proposal_id, network_limit))) {
      CLOG_LOG(WARN,
          "send log to server failed",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(fetch_type),
          K(proposal_id),
          K(server),
          K(need_send_confirm_info));
    } else if (need_send_confirm_info) {
      if (OB_FAIL(send_confirm_info_(server, cluster_id, log_entry, acc_cksm, batch_committed))) {
        CLOG_LOG(WARN,
            "send confirm info failed",
            K(ret),
            K(partition_key_),
            K(log_id),
            K(fetch_type),
            K(proposal_id),
            K(server),
            K(need_send_confirm_info));
      }
    }
  }

  if ((OB_CURSOR_NOT_EXIST == ret || !read_from_clog) && OB_FETCH_LOG_LEADER_RECONFIRM != fetch_type) {
    const bool is_in_member_list = mm_.get_curr_member_list().contains(server);
    CLOG_LOG(INFO, "this replica need rebuild", K(ret), K(server), K(partition_key_), K(log_id), K(read_from_clog));
    uint64_t last_replay_log_id = OB_INVALID_ID;
    if (OB_FAIL(get_storage_last_replay_log_id_(last_replay_log_id))) {
      CLOG_LOG(ERROR,
          "get_storage_last_replay_log_id_ failed",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(fetch_type),
          K(proposal_id),
          K(server),
          K(need_send_confirm_info));
    } else if (log_id > last_replay_log_id) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR,
          "log_id is larger than last_replay_log_id, unexpected",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(last_replay_log_id));
    } else if (OB_FAIL(log_engine_->notify_follower_log_missing(
                   server, cluster_id, partition_key_, log_id, is_in_member_list, OB_REBUILD_MSG))) {
      CLOG_LOG(WARN,
          "notify follower log missing",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(fetch_type),
          K(proposal_id),
          K(server),
          K(need_send_confirm_info),
          K(is_in_member_list));
    } else {
      ret = OB_LOG_NEED_REBUILD;
    }
  }
  CLOG_LOG(TRACE,
      "fetch_log_from_ilog_storage_",
      K(ret),
      K(partition_key_),
      K(log_id),
      K(fetch_type),
      K(proposal_id),
      K(server),
      K(need_send_confirm_info));
  return ret;
}

int ObPartitionLogService::get_log_with_cursor_(const common::ObAddr& server, const int64_t cluster_id,
    const ObFetchLogType& fetch_type, const common::ObProposalID& proposal_id, const bool need_send_confirm_info,
    const bool log_confirmed, const ObLogCursor& log_cursor, const int64_t accum_checksum, const bool batch_committed,
    const int64_t network_limit)
{
  int ret = OB_SUCCESS;
  ObLogEntry log_entry;
  ObReadParam read_param;
  read_param.file_id_ = log_cursor.file_id_;
  read_param.offset_ = log_cursor.offset_;
  read_param.read_len_ = log_cursor.size_;

  ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID);
  ObReadBuf& rbuf = guard.get_read_buf();
  if (OB_UNLIKELY(!rbuf.is_valid())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to alloc read_buf", K_(partition_key), K(log_cursor), K(ret));
  } else if (OB_FAIL(log_engine_->read_log_by_location(read_param, rbuf, log_entry))) {
    CLOG_LOG(WARN, "read_log_by_location failed", K_(partition_key), K(ret), K(log_cursor));
  } else if (log_entry.get_header().get_partition_key() != partition_key_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(
        ERROR, "read_log_by_location wrong log", K_(partition_key), K(ret), K(log_entry), K(server), K(log_cursor));
  } else {
    bool is_reconfirm_allow_send = true;
    common::ObProposalID log_proposal_id = log_entry.get_header().get_proposal_id();
    if (!log_confirmed && state_mgr_.get_state() == RECONFIRM &&
        (reconfirm_.get_proposal_id() != proposal_id || log_proposal_id != proposal_id)) {
      is_reconfirm_allow_send = false;
    }
    if (!log_confirmed && !is_reconfirm_allow_send) {
      CLOG_LOG(WARN,
          "in reconfirm state, not allow send unconfirmed log with old proposal_id",
          K_(partition_key),
          K(server),
          "state_mgr proposal_id",
          proposal_id,
          "reconfirm proposal_id",
          reconfirm_.get_proposal_id(),
          K(log_proposal_id));
    } else if (OB_FETCH_LOG_STANDBY_REPLICA == fetch_type &&
               OB_FAIL(log_entry.update_header_proposal_id(proposal_id))) {
      // fix issue #29473286
      // Log header's proposal_id need be updated to current proposal_id when it is send by primary cluster to standby.
      CLOG_LOG(WARN,
          "update_header_proposal_id failed",
          K(ret),
          K_(partition_key),
          K(fetch_type),
          K(proposal_id),
          K(log_entry));
    } else if (OB_SUCCESS != (ret = send_log_(server, cluster_id, log_entry, proposal_id, network_limit))) {
      CLOG_LOG(WARN, "send_log failed", K_(partition_key), K(ret), K(server), K(proposal_id), K(log_cursor));
    } else if (need_send_confirm_info &&
               (OB_SUCCESS !=
                   (ret = send_confirm_info_(server, cluster_id, log_entry, accum_checksum, batch_committed)))) {
      CLOG_LOG(WARN, "send_confirm_info failed", K_(partition_key), K(ret), K(server), K(proposal_id), K(log_cursor));
    }
  }
  return ret;
}

// handle leader_prepare request
int ObPartitionLogService::get_max_log_id(
    const ObAddr& server, const int64_t cluster_id, const ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
  if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "get_max_log_id wrlock failed", K(ret), K_(partition_key), K(server), K(proposal_id));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(server), K(proposal_id));
  } else if (state_mgr_.can_handle_prepare_rqst(proposal_id, cluster_id)) {
    ret = state_mgr_.handle_prepare_rqst(proposal_id, server, cluster_id);
    CLOG_LOG(INFO, "handle_prepare_rqst finished", K(ret), K(partition_key_), K(server), K(cluster_id), K(proposal_id));
  }
  CLOG_LOG(INFO, "get_max_log_id finished", K(ret), K(partition_key_), K(server), K(proposal_id));
  return ret;
}

int ObPartitionLogService::receive_max_log_id(
    const ObAddr& server, const uint64_t log_id, const ObProposalID& proposal_id, const int64_t max_log_ts)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!state_mgr_.can_receive_max_log_id(proposal_id)) {
    ret = OB_STATE_NOT_MATCH;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "stale prepare_resp, discard", K(ret), K_(partition_key), K(server), K(proposal_id));
    }
  } else {
    ret = reconfirm_.receive_max_log_id(server, log_id, max_log_ts);
  }
  CLOG_LOG(INFO, "receive_max_log_id", K_(partition_key), K(ret), K(server), K(log_id), K(proposal_id), K(max_log_ts));
  return ret;
}

int ObPartitionLogService::handle_standby_prepare_req(
    const ObAddr& server, const int64_t cluster_id, const ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);

  if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "handle_standby_prepare_req wrlock failed", K(ret), K_(partition_key), K(server), K(proposal_id));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(server), K(proposal_id));
  } else if (state_mgr_.can_handle_standby_prepare_rqst(proposal_id, cluster_id)) {
    ret = state_mgr_.handle_standby_prepare_rqst(proposal_id, server, cluster_id);
    CLOG_LOG(INFO,
        "handle_standby_prepare_rqst finished",
        K(ret),
        K(partition_key_),
        K(server),
        K(cluster_id),
        K(proposal_id));
  } else {
    // do nothing
  }

  CLOG_LOG(INFO, "handle_standby_prepare_req finished", K(ret), K(partition_key_), K(server), K(proposal_id));

  return ret;
}

int ObPartitionLogService::handle_standby_prepare_resp(const ObAddr& server, const ObProposalID& proposal_id,
    const uint64_t ms_log_id, const int64_t membership_version, const ObMemberList& member_list)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(server), K(proposal_id));
  } else if (!state_mgr_.can_handle_standby_prepare_resp(proposal_id)) {
    ret = OB_STATE_NOT_MATCH;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "stale standby_prepare_response, ignore", K(ret), K_(partition_key), K(server), K(proposal_id));
    }
  } else {
    ret = reconfirm_.handle_standby_prepare_resp(server, proposal_id, ms_log_id, membership_version, member_list);
  }
  CLOG_LOG(INFO,
      "handle_standby_prepare_resp finished",
      K_(partition_key),
      K(ret),
      K(server),
      K(proposal_id),
      K(ms_log_id),
      K(membership_version),
      K(member_list));
  return ret;
}

int ObPartitionLogService::handle_query_sync_start_id_req(
    const ObAddr& server, const int64_t cluster_id, const int64_t send_ts)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  int64_t sync_cluster_id = OB_INVALID_CLUSTER_ID;
  RLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_INVALID_TIMESTAMP == send_ts) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(server), K(cluster_id), K(send_ts));
  } else if (now - send_ts > CASCAD_MSG_DELAY_TIME_THRESHOLD) {
    CLOG_LOG(WARN, "msg delay too much time, ignore", K_(partition_key), K(send_ts), "delay", now - send_ts);
  } else if (state_mgr_.get_self_cluster_id() == cluster_id || !GCTX.need_sync_to_standby() ||
             ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) ||
             LEADER != state_mgr_.get_role()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN,
        "recv unexpected query sync_start_id req",
        K(ret),
        K_(partition_key),
        K(server),
        K(cluster_id),
        "role",
        state_mgr_.get_role(),
        "mode",
        GCTX.get_protection_mode());
  } else if (OB_FAIL(cascading_mgr_.get_sync_standby_cluster_id(sync_cluster_id))) {
    CLOG_LOG(WARN, "get_sync_standby_cluster_id failed", K(ret), K_(partition_key));
  } else if (sync_cluster_id != cluster_id) {
    CLOG_LOG(WARN,
        "arg server is not sync cluster, ignore",
        K(ret),
        K_(partition_key),
        K(sync_cluster_id),
        K(server),
        K(cluster_id));
  } else {
    ObCascadMember sync_child = cascading_mgr_.get_sync_standby_child();
    if (!sync_child.is_valid() || sync_child.get_server() != server) {
      // update sync_starndby_child
      (void)cascading_mgr_.update_sync_standby_child(server, cluster_id);
    }
    const uint64_t max_log_id = sw_.get_max_log_id();
    if (OB_FAIL(log_engine_->submit_sync_start_id_resp(server, cluster_id, partition_key_, send_ts, max_log_id))) {
      CLOG_LOG(
          WARN, "submit_sync_start_id_resp failed", K(ret), K_(partition_key), K(max_log_id), K(server), K(cluster_id));
    }
  }
  CLOG_LOG(INFO, "handle_query_sync_start_id_req finished", K_(partition_key), K(ret), K(server), K(cluster_id));

  return ret;
}

int ObPartitionLogService::handle_sync_start_id_resp(
    const ObAddr& server, const int64_t cluster_id, const int64_t original_send_ts, const uint64_t sync_start_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_INVALID_TIMESTAMP == original_send_ts || OB_INVALID_ID == sync_start_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K_(partition_key),
        K(server),
        K(cluster_id),
        K(original_send_ts),
        K(sync_start_id));
  } else if (OB_FAIL(state_mgr_.handle_sync_start_id_resp(server, cluster_id, original_send_ts, sync_start_id))) {
    CLOG_LOG(WARN,
        "handle_sync_start_id_resp failed",
        K_(partition_key),
        K(ret),
        K(server),
        K(original_send_ts),
        K(sync_start_id));
  } else {
  }

  return ret;
}

int ObPartitionLogService::standby_update_protection_level()
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (GCTX.is_primary_cluster() || ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    // primary cluster or private tbl, skip
  } else if (OB_FAIL(state_mgr_.standby_update_protection_level())) {
    CLOG_LOG(INFO, "standby_update_protection_level failed", K_(partition_key), K(ret));
  } else {
  }

  return ret;
}

int ObPartitionLogService::get_standby_leader_protection_level(uint32_t& protection_level)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    int64_t unused_epoch = 0;
    ObTsWindows unused_windows;
    if (OB_FAIL(get_role_unlock(unused_epoch, unused_windows))) {  // check self is elected leader
      CLOG_LOG(WARN, "get_role failed or not master", K(ret), K(partition_key_));
    } else if (OB_FAIL(state_mgr_.get_standby_protection_level(protection_level))) {
      CLOG_LOG(WARN, "get_standby_protection_level failed", K_(partition_key), K(ret));
    } else {
      CLOG_LOG(DEBUG, "get_standby_protection_level success", K_(partition_key), K(protection_level));
    }
  }
  return ret;
}

int ObPartitionLogService::primary_process_protect_mode_switch()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!GCTX.is_primary_cluster() ||
             ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    // skip primary or private raplica
  } else {
    common::ObTimeGuard timeguard("primary_protect_mode_switch", 1000 * 1000);
    // need use wlock to protect role
    // Because it may be concurrent with standby_ack_log(), if sync_standby_child changes,
    // it will cause primary leader incorrectly advance the log_task state
    WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
    timeguard.click();
    if (LEADER != state_mgr_.get_role()) {
      // skip
    } else if (OB_FAIL(cascading_mgr_.primary_process_protect_mode_switch())) {
      CLOG_LOG(WARN, "primary_process_protect_mode_switch failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(state_mgr_.primary_process_protect_mode_switch())) {
      CLOG_LOG(WARN, "primary_process_protect_mode_switch failed", K_(partition_key), K(ret));
    } else {
      // do nothing
    }
  }
  CLOG_LOG(INFO, "primary_process_protect_mode_switch finished", K_(partition_key), K(ret));

  return ret;
}

int ObPartitionLogService::receive_confirmed_info(const common::ObAddr& server, const int64_t src_cluster_id,
    const uint64_t log_id, const ObConfirmedInfo& confirmed_info, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (log_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!log_engine_->is_disk_space_enough()) {
    ret = OB_LOG_OUTOF_DISK_SPACE;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "log outof disk space", K_(partition_key), K(server), K(ret));
    }
  } else if (!state_mgr_.can_receive_confirmed_info(src_cluster_id)) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN,
        "can not receive_confirmed_info",
        K(ret),
        K_(partition_key),
        K(server),
        K(src_cluster_id),
        "role",
        state_mgr_.get_role(),
        "state",
        state_mgr_.get_state(),
        K(log_id),
        K(confirmed_info));
  } else if (!sw_.check_can_receive_larger_log(log_id)) {
    ret = OB_TOO_LARGE_LOG_ID;
  } else if (!state_mgr_.is_standby_leader_can_receive_log_id(log_id)) {
    // standby_leader is doing member change, cannot receive log/confirmed_info beyond max_log_id
  } else {
    ret = sw_.receive_confirmed_info(log_id, confirmed_info, batch_committed);
  }
  return ret;
}

int ObPartitionLogService::receive_renew_ms_log_confirmed_info(const common::ObAddr& server, const uint64_t log_id,
    const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (log_id <= 0 || !ms_proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!state_mgr_.can_receive_renew_ms_log_confirmed_info()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN,
        "can not receive_renew_ms_log_confirmed_info",
        K(ret),
        K_(partition_key),
        "role",
        state_mgr_.get_role(),
        "state",
        state_mgr_.get_state(),
        K(log_id),
        K(ms_proposal_id),
        K(confirmed_info));
  } else {
    ret = ms_task_mgr_.receive_renew_ms_log_confirmed_info(log_id, ms_proposal_id, confirmed_info);
  }

  CLOG_LOG(INFO,
      "receive_renew_ms_log_confirmed_info finished",
      K(ret),
      K_(partition_key),
      K(server),
      K(log_id),
      K(ms_proposal_id),
      K(confirmed_info));
  return ret;
}

int ObPartitionLogService::append_disk_log(
    const ObLogEntry& log, const ObLogCursor& log_cursor, const int64_t accum_checksum, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_.can_append_disk_log()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    if ((OB_LOG_NOP == log.get_header().get_log_type()) ||
        (OB_LOG_START_MEMBERSHIP == log.get_header().get_log_type()) ||
        (OB_LOG_TRUNCATE == log.get_header().get_log_type()) ||
        (OB_LOG_RENEW_MEMBERSHIP == log.get_header().get_log_type()) ||
        (OB_LOG_SUBMIT == log.get_header().get_log_type()) || (OB_LOG_AGGRE == log.get_header().get_log_type()) ||
        (OB_LOG_MEMBERSHIP == log.get_header().get_log_type())) {
      uint64_t freeze_id = saved_base_storage_info_.get_last_replay_log_id();
      if (freeze_id != OB_INVALID_ID && log.get_header().get_log_id() <= freeze_id) {
        CLOG_LOG(DEBUG,
            "append disk log before freeze point, abort it",
            K_(partition_key),
            K(log.get_header()),
            K(freeze_id));
      } else if (OB_LOG_MEMBERSHIP != log.get_header().get_log_type() &&
                 OB_LOG_START_MEMBERSHIP != log.get_header().get_log_type() &&
                 OB_LOG_RENEW_MEMBERSHIP != log.get_header().get_log_type()) {
        // sw append_disk_log handles thread safety issues
        ret = sw_.append_disk_log(log, log_cursor, accum_checksum, batch_committed);
      } else {
        // ensure that the processing of multiple member changes are mutually exclusive
        WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
        ret = mm_.append_disk_log(log, log_cursor, accum_checksum, batch_committed);
      }
    } else if (OB_LOG_PREPARED == log.get_header().get_log_type()) {
      WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
      if (OB_SUCCESS != (tmp_ret = state_mgr_.update_proposal_id(log.get_header().get_proposal_id()))) {
        CLOG_LOG(INFO, "update_proposal_id failed", K_(partition_key), K(tmp_ret));
      }
    } else if (OB_LOG_STANDBY_PREPARED == log.get_header().get_log_type()) {
      WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
      if (OB_SUCCESS != (tmp_ret = mm_.update_ms_proposal_id(log.get_header().get_proposal_id()))) {
        CLOG_LOG(INFO, "update_ms_proposal_id failed", K_(partition_key), K(tmp_ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "unexpected log type", K(ret), "log_type", log.get_header().get_log_type());
    }

    if (OB_SUCC(ret) && log.get_header().get_log_id() < get_next_index_log_id()) {
      ATOMIC_INC(&scan_confirmed_log_cnt_);
    }
  }
  return ret;
}

int ObPartitionLogService::set_scan_disk_log_finished()
{
  int ret = OB_SUCCESS;
  WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    if (sw_.get_max_log_id() + 1 < sw_.get_next_index_log_id()) {
      CLOG_LOG(ERROR,
          "scan clog error",
          K_(partition_key),
          "start_id",
          sw_.get_start_id(),
          "max_log_id",
          sw_.get_max_log_id(),
          "next_index_log_id",
          sw_.get_next_index_log_id());
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = state_mgr_.set_scan_disk_log_finished();
      // record last_update_next_replay_log_id_info_ts_
      sw_.record_last_update_next_replay_log_id_info_ts();
      free_cursor_array_cache_();
    }
  }
  return ret;
}

int ObPartitionLogService::on_election_role_change()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(add_role_change_task_())) {
    CLOG_LOG(WARN, "add_role_change_task_ failed", K(ret), K(partition_key_));
  } else {
    CLOG_LOG(TRACE, "add_role_change_task_ success", K(ret), K(partition_key_));
  }
  return ret;
}

int ObPartitionLogService::on_change_leader_retry(const common::ObAddr& server, ObTsWindows& changing_leader_windows)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  storage::ObIPartitionGroup* partition = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (OB_FAIL(partition_service_->get_partition(partition_key_, guard)) ||
             NULL == (partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "this partition not existed", K(ret), K(partition_key_));
  } else if (OB_FAIL(partition->register_txs_change_leader(server, changing_leader_windows))) {
    CLOG_LOG(
        WARN, "register txs change leader failed", K(ret), K(partition_key_), K(server), K(changing_leader_windows));
  } else {
    CLOG_LOG(INFO,
        "partition on change leader fail and reregister windows",
        K(ret),
        K(partition_key_),
        K(server),
        K(changing_leader_windows));
  }

  return ret;
}

int ObPartitionLogService::switch_state()
{
  int ret = OB_SUCCESS;
  bool need_retry = false;
  ret = switch_state(need_retry);
  return ret;
}

int ObPartitionLogService::switch_state(bool& need_retry)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    bool state_changed = false;
    need_retry = false;
    do {
      RLockGuard guard(lock_);
      state_changed = state_mgr_.is_state_changed(need_retry);
    } while (0);
    if (state_changed) {
      WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
      if (OB_FAIL(ret)) {
        CLOG_LOG(WARN, "switch_state wrlock failed", K(ret), K_(partition_key));
      } else {
        ret = state_mgr_.switch_state(need_retry);
      }
    }
  }
  return ret;
}

int ObPartitionLogService::check_mc_and_sliding_window_state()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    check_state_();
  }
  return ret;
}

int ObPartitionLogService::check_state_()
{
  int ret = OB_SUCCESS;
  int mc_ret = OB_SUCCESS;
  int state_ret = OB_SUCCESS;
  const int64_t now = ObClockGenerator::getClock();
  if (now - last_check_state_time_ > CLOG_CHECK_STATE_INTERVAL) {
    bool state_changed = false;
    do {
      RLockGuard guard(lock_);
      state_changed = mm_.is_state_changed();
    } while (0);
    if (state_changed) {
      WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + now, ret);
      if (OB_FAIL(ret)) {
        CLOG_LOG(WARN, "check_sliding_window_state wrlock failed", K(ret), K_(partition_key));
      } else if (OB_SUCCESS != (mc_ret = mm_.switch_state())) {
        CLOG_LOG(WARN, "ObLogMembershipMgr switch state failed", K_(partition_key), K(mc_ret));
      } else {
      }
    }
    do {
      RLockGuard guard(lock_);
      state_changed = state_mgr_.check_sliding_window_state();
    } while (0);
    if (state_changed) {
      if (OB_SUCCESS != (state_ret = add_role_change_task_())) {
        CLOG_LOG(WARN, "add_state_change_event failed", K_(partition_key), K(state_ret));
      } else {
        CLOG_LOG(TRACE, "add_state_change_event succeed", K_(partition_key), K(state_ret));
      }
    }
    if (OB_SUCCESS != mc_ret || OB_SUCCESS != state_ret) {
      ret = OB_ERR_UNEXPECTED;
    }

    int tmp_ret = OB_SUCCESS;
    if (restore_mgr_.is_archive_restoring() && state_mgr_.is_follower_active()) {
      if (OB_SUCCESS != (tmp_ret = restore_mgr_.check_state())) {
        CLOG_LOG(WARN, "restore_mgr_.check_state failed", K_(partition_key), K(tmp_ret));
      }
    }

    if (state_mgr_.is_can_elect_standby_leader()) {
      const int64_t now = ObTimeUtility::current_time();
      if (STANDBY_LEADER == state_mgr_.get_role()) {
        if (ATOMIC_LOAD(&last_check_standby_ms_time_) == OB_INVALID_TIMESTAMP ||
            now - ATOMIC_LOAD(&last_check_standby_ms_time_) >= STANDBY_CHECK_MS_INTERVAL) {
          last_check_standby_ms_time_ = now;
          if (OB_SUCCESS != (tmp_ret = ms_task_mgr_.check_renew_ms_log_sync_state())) {
            CLOG_LOG(WARN, "check_renew_ms_log_sync_state failed", K(tmp_ret), K_(partition_key));
          }
        }
      }
    }

    if (GCTX.is_sync_level_on_standby() && GCTX.is_standby_cluster() && STANDBY_LEADER == state_mgr_.get_role()) {
      (void)state_mgr_.standby_leader_check_protection_level();
    }

    last_check_state_time_ = now;
  }
  return ret;
}

int ObPartitionLogService::try_exec_standby_restore_(const share::ObCascadMember& src_member)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!src_member.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (ATOMIC_LOAD(&last_rebuild_time_) == OB_INVALID_TIMESTAMP ||
        ObTimeUtility::current_time() - ATOMIC_LOAD(&last_rebuild_time_) >= REBUILD_REPLICA_INTERVAL) {
      if (OB_FAIL(partition_service_->restore_standby_replica(
              partition_key_, src_member.get_server(), src_member.get_cluster_id()))) {
        CLOG_LOG(WARN, "restore_standby_replica failed", K(ret), K_(partition_key), K(src_member));
      } else {
        ATOMIC_STORE(&last_rebuild_time_, ObTimeUtility::current_time());
        CLOG_LOG(INFO, "restore_standby_replica success", K(ret), K_(partition_key), K(src_member));
      }
    }
  }
  return ret;
}

int ObPartitionLogService::check_cascading_state()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    ret = cascading_mgr_.check_cascading_state();
  }
  return ret;
}

int ObPartitionLogService::flush_cb(const ObLogFlushCbArg& arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObAddr arg_leader = arg.leader_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_engine_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_LOG_PREPARED == arg.log_type_) {
    common::ObReplicaType replica_type = REPLICA_TYPE_MAX;
    ObProposalID proposal_id = arg.proposal_id_;
    uint64_t max_log_id = OB_INVALID_ID;
    int64_t max_log_ts = OB_INVALID_TIMESTAMP;
    do {
      RLockGuard guard(lock_);
      replica_type = mm_.get_replica_type();
      sw_.get_max_log_id_info(max_log_id, max_log_ts);
    } while (0);

    if (arg_leader != self_ && arg_leader.is_valid() && ObReplicaTypeCheck::is_paxos_replica_V2(replica_type)) {
      tmp_ret = log_engine_->prepare_response(
          arg_leader, arg.cluster_id_, partition_key_, proposal_id, max_log_id, max_log_ts);
      CLOG_LOG(INFO,
          "update proposal id succ, submit prepare response",
          K_(partition_key),
          K(tmp_ret),
          K(arg_leader),
          K(proposal_id),
          K(replica_type),
          K(max_log_id),
          K(max_log_ts));
    } else {
      CLOG_LOG(INFO, "refuse to submit prepare response", K_(partition_key), K(arg_leader), K_(self), K(replica_type));
    }
  } else if (OB_LOG_STANDBY_PREPARED == arg.log_type_) {
    common::ObReplicaType replica_type = REPLICA_TYPE_MAX;
    ObProposalID proposal_id = arg.proposal_id_;
    do {
      RLockGuard guard(lock_);
      replica_type = mm_.get_replica_type();
    } while (0);

    if (arg_leader != self_ && arg_leader.is_valid() && ObReplicaTypeCheck::is_paxos_replica_V2(replica_type) &&
        state_mgr_.is_can_elect_standby_leader()) {
      // standby-active non-private table
      ObProposalID ms_proposal_id = mm_.get_ms_proposal_id();
      if (proposal_id == ms_proposal_id) {
        // reply response only when proposal_id and ms_proposal_id are same
        const uint64_t ms_log_id = mm_.get_log_id();
        const int64_t membership_version = mm_.get_timestamp();
        const ObMemberList member_list = mm_.get_curr_member_list();
        tmp_ret = log_engine_->standby_prepare_response(
            arg_leader, arg.cluster_id_, partition_key_, proposal_id, ms_log_id, membership_version, member_list);
        CLOG_LOG(INFO,
            "update proposal id succ, submit standby prepare response",
            K_(partition_key),
            K(tmp_ret),
            K(arg_leader),
            K(proposal_id),
            K(ms_proposal_id),
            K(membership_version),
            K(member_list));
      }
    } else {
      CLOG_LOG(INFO, "refuse to submit prepare response", K_(partition_key), K(arg_leader), K_(self), K(replica_type));
    }
  } else if (OB_LOG_RENEW_MEMBERSHIP == arg.log_type_) {
    bool majority = false;
    RLockGuard guard(lock_);
    const int64_t begin_time = ObClockGenerator::getClock();
    if (OB_SUCCESS != (ret = ms_task_mgr_.set_renew_ms_log_flushed_succ(
                           arg.log_id_, arg.submit_timestamp_, arg.proposal_id_, majority))) {
      CLOG_LOG(
          WARN, "set_renew_ms_log_flushed_succ failed", K_(partition_key), K(ret), K(arg.log_type_), K(arg.log_id_));
    } else if (majority && state_mgr_.can_majority_cb_for_renew_ms_log(arg.proposal_id_) &&
               (OB_SUCCESS !=
                   (ret = renew_ms_log_majority_cb_(arg.log_id_, arg.submit_timestamp_, arg.proposal_id_)))) {
      CLOG_LOG(WARN, "majority_cb failed", K_(partition_key), K(ret));
    } else if (state_mgr_.can_send_renew_ms_log_ack(arg.proposal_id_) && arg_leader.is_valid()) {
      if (OB_SUCCESS !=
          (tmp_ret = log_engine_->submit_renew_ms_log_ack(
               arg_leader, arg.cluster_id_, partition_key_, arg.log_id_, arg.submit_timestamp_, arg.proposal_id_))) {
        CLOG_LOG(WARN,
            "submit_renew_ms_log_ack failed",
            K(tmp_ret),
            K(arg_leader),
            K(arg.log_id_),
            K(arg.proposal_id_),
            K_(partition_key));
      } else {
        CLOG_LOG(INFO,
            "submit_renew_ms_log_ack success",
            K(tmp_ret),
            K(arg_leader),
            K(arg.log_id_),
            K(arg.proposal_id_),
            K_(partition_key));
      }
      const int64_t end_time = ObClockGenerator::getClock();
      if (end_time - begin_time >= CLOG_PERF_WARN_THRESHOLD) {
        CLOG_LOG(WARN,
            "flush_cb cost too much time",
            K(partition_key_),
            K(arg),
            K(end_time),
            K(begin_time),
            "cost_time",
            end_time - begin_time);
      }
    } else {
    }
  } else {
    bool majority = false;
    RLockGuard guard(lock_);
    const int64_t begin_time = ObClockGenerator::getClock();
    bool is_lease_valid = true;

    if (OB_SUCCESS != (ret = sw_.set_log_flushed_succ(
                           arg.log_id_, arg.proposal_id_, arg.log_cursor_, arg.after_consume_timestamp_, majority))) {
      CLOG_LOG(WARN, "sw set_log_flushed_succ failed", K_(partition_key), K(ret), K(arg.log_type_), K(arg.log_id_));
    } else if (majority && ((state_mgr_.can_majority_cb(arg.proposal_id_) && is_lease_valid)) &&
               (OB_SUCCESS != (ret = majority_cb_(arg.log_id_)))) {
      CLOG_LOG(WARN, "majority_cb failed", K_(partition_key), K(ret));
    } else if (majority && state_mgr_.can_standby_majority_cb(arg.proposal_id_) &&
               (OB_SUCCESS !=
                   (ret = standby_majority_cb_(arg_leader, arg.cluster_id_, arg.proposal_id_, arg.log_id_)))) {
      CLOG_LOG(WARN, "majority_cb failed", K_(partition_key), K(ret));
    } else if (state_mgr_.can_send_log_ack(arg.proposal_id_) && arg_leader.is_valid()) {
      if (OB_SUCCESS != (tmp_ret = log_engine_->submit_log_ack(
                             arg_leader, arg.cluster_id_, partition_key_, arg.log_id_, arg.proposal_id_))) {
        CLOG_LOG(WARN,
            "submit_log_ack failed",
            K(tmp_ret),
            K(arg_leader),
            K(arg.log_id_),
            K(arg.proposal_id_),
            K_(partition_key));
      }
    } else {
    }

    const int64_t end_time = ObClockGenerator::getClock();
    if (end_time - begin_time >= CLOG_PERF_WARN_THRESHOLD) {
      CLOG_LOG(WARN,
          "flush_cb cost too much time",
          K(partition_key_),
          K(arg),
          K(end_time),
          K(begin_time),
          "cost_time",
          end_time - begin_time);
    }
  }
  return ret;
}

int ObPartitionLogService::renew_ms_log_flush_cb(const ObMsInfoTask& arg)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t begin_time = ObClockGenerator::getClock();
  ObAddr server = arg.get_server();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_engine_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool majority = false;
    RLockGuard guard(lock_);
    if (OB_FAIL(ms_task_mgr_.set_renew_ms_log_flushed_succ(
            arg.get_ms_log_id(), arg.get_mc_timestamp(), arg.get_ms_proposal_id(), majority))) {
      CLOG_LOG(WARN, "set_renew_ms_log_flushed_succ failed", K_(partition_key), K(ret), K(arg));
    } else if (majority && state_mgr_.can_majority_cb_for_renew_ms_log(arg.get_ms_proposal_id()) &&
               (OB_FAIL(
                   renew_ms_log_majority_cb_(arg.get_ms_log_id(), arg.get_mc_timestamp(), arg.get_ms_proposal_id())))) {
      CLOG_LOG(WARN, "majority_cb failed", K_(partition_key), K(ret));
    } else if (state_mgr_.can_send_renew_ms_log_ack(arg.get_ms_proposal_id()) && server.is_valid() && self_ != server) {
      if (OB_SUCCESS != (tmp_ret = log_engine_->submit_renew_ms_log_ack(server,
                             arg.get_cluster_id(),
                             partition_key_,
                             arg.get_ms_log_id(),
                             arg.get_mc_timestamp(),
                             arg.get_ms_proposal_id()))) {
        CLOG_LOG(WARN, "submit_renew_ms_log_ack failed", K(tmp_ret), K(server), K(arg), K_(partition_key));
      } else {
        CLOG_LOG(INFO, "submit_renew_ms_log_ack success", K(tmp_ret), K(server), K(arg), K_(partition_key));
      }
      const int64_t end_time = ObClockGenerator::getClock();
      if (end_time - begin_time >= CLOG_PERF_WARN_THRESHOLD) {
        CLOG_LOG(WARN,
            "renew_ms_log_flush_cb cost too much time",
            K(partition_key_),
            K(arg),
            "cost_time",
            end_time - begin_time);
      }
    } else {
    }
  }

  CLOG_LOG(INFO, "renew_ms_log_flush_cb finished", K(partition_key_), K(ret), K(arg));
  return ret;
}

int ObPartitionLogService::query_max_flushed_ilog_id(uint64_t& ret_max_ilog_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret));
  } else if (OB_FAIL(log_engine_->query_max_flushed_ilog_id(partition_key_, ret_max_ilog_id)) &&
             OB_PARTITION_NOT_EXIST != ret) {
    CLOG_LOG(ERROR, "query_max_flushed_ilog_id failed", K(ret), K(partition_key_));
  } else if (OB_PARTITION_NOT_EXIST == ret) {
    ret_max_ilog_id = 0;
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    ret_max_ilog_id = std::max(ret_max_ilog_id, saved_base_storage_info_.get_last_replay_log_id());
    ret_max_ilog_id = std::max(ret_max_ilog_id, ATOMIC_LOAD(&max_flushed_ilog_id_));
  }

  return ret;
}

// Because member change and switch leader are mutable ops, so this function can execute without lock
int ObPartitionLogService::on_get_election_priority(election::ObElectionPriority& priority)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(partition_service_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    priority.reset();
    const uint64_t zone_priority = get_zone_priority();
    const bool is_need_rebuild = state_mgr_.is_need_rebuild();
    const bool is_partition_candidate = partition_service_->is_election_candidate(partition_key_);
    const bool is_leader_revoke_recently = state_mgr_.is_leader_revoke_recently();
    bool is_standby_restore_finished = true;
    if (GCTX.can_be_parent_cluster() && !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
        restore_mgr_.is_standby_restore_state()) {
      // restoring replicas cannot be striong leader,
      // because its last_submit_ts maybe invalid, which will lead to reconfirm failure
      is_standby_restore_finished = false;
    }
    bool is_candidate = partition_service_->is_tenant_active(partition_key_) &&
                        ObReplicaTypeCheck::is_can_elected_replica(mm_.get_replica_type()) &&
                        OBSERVER.get_gctx().rs_server_status_ == share::RSS_IS_WORKING && is_partition_candidate &&
                        is_standby_restore_finished;
#ifdef ERRSIM
    if (ObServerConfig::get_instance()._ob_enable_rebuild_on_purpose) {
      is_candidate = is_candidate && (!is_need_rebuild);
    }
#endif
    bool is_tenant_out_of_mem = is_tenant_out_of_memory_();
    bool is_data_disk_error = false;
    bool is_clog_disk_error = log_engine_->is_clog_disk_error();
    const ObReplicaProperty replica_property = mm_.get_replica_property();
    const uint64_t log_id = sw_.get_max_confirmed_log_id();
    if (OB_SUCCESS != (tmp_ret = ObIOManager::get_instance().is_disk_error(is_data_disk_error))) {
      CLOG_LOG(WARN, "is_data_disk_error failed", K(tmp_ret), K_(partition_key));
    }
    if (is_candidate_ != is_candidate) {
      is_candidate_ = is_candidate;
      CLOG_LOG(INFO,
          "whether is_candidate changed",
          K_(partition_key),
          K(is_candidate),
          K(partition_service_->is_tenant_active(partition_key_)),
          "replica_type",
          mm_.get_replica_type(),
          K(zone_priority),
          "rs_server_status",
          OBSERVER.get_gctx().rs_server_status_,
          "is_service_started",
          partition_service_->is_service_started(),
          K(is_partition_candidate));
    }
    if (OB_SUCCESS != (ret = priority.init(is_candidate, mm_.get_timestamp(), log_id, zone_priority))) {
      CLOG_LOG(WARN, "priority init error", K_(partition_key), K(ret));
    } else {
      if (is_clog_disk_error) {
        priority.set_system_clog_disk_error();
      }
      if (is_data_disk_error) {
        priority.set_system_data_disk_error();
      }
      if (is_tenant_out_of_mem) {
        priority.set_system_tenant_out_of_memory();
      }
      if (is_leader_revoke_recently) {
        priority.set_system_in_election_blacklist();
      }
      if (0 == replica_property.get_memstore_percent()) {
        priority.set_system_without_memstore();
      }
      if (!partition_service_->is_service_started()) {
        priority.set_system_service_not_started();
      }
      if (is_need_rebuild) {
        priority.set_system_need_rebuild();
      }
    }
  }
  return ret;
}

int ObPartitionLogService::majority_cb_(const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  const bool batch_committed = false;
  const bool batch_first_participant = false;

  BG_MONITOR_GUARD_DEFAULT(1000 * 1000);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = sw_.majority_cb(log_id, batch_committed, batch_first_participant))) {
    CLOG_LOG(WARN, "majority_cb failed", K_(partition_key), K(ret), K(log_id));
  } else if (OB_SUCCESS != (ret = sw_.set_log_confirmed(log_id, batch_committed))) {
    CLOG_LOG(WARN, "set_log_confirmed failed!", K_(partition_key), K(ret), K(log_id));
  } else {
  }
  return ret;
}

int ObPartitionLogService::standby_majority_cb_(const common::ObAddr& server, const int64_t cluster_id,
    const common::ObProposalID& proposal_id, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(server), K(cluster_id), K(log_id));
  } else {
    // standby_leader send ack to primary parent
    int tmp_ret = OB_SUCCESS;
    if (server.is_valid() && OB_SUCCESS != (tmp_ret = log_engine_->submit_standby_log_ack(
                                                server, cluster_id, partition_key_, log_id, proposal_id))) {
      CLOG_LOG(WARN,
          "submit_standby_log_ack failed",
          K(tmp_ret),
          K(server),
          K(cluster_id),
          K(log_id),
          K(proposal_id),
          K_(partition_key));
    }
    const bool batch_committed = false;
    if (OB_SUCCESS != (tmp_ret = sw_.set_log_confirmed(log_id, batch_committed))) {
      if (OB_ERROR_OUT_OF_RANGE != tmp_ret) {
        // The standby leader has to wait for the confirmed info from primary to execute set_log_confirmed after
        // majority In a concurrent scenario, after this thread executes try_set_majority_finished, other threads may
        // execute set_log_confirmed, which triggers the log to slide out, so it is possible to encounter -4175 here
        ret = tmp_ret;
      }
      CLOG_LOG(WARN, "submit_confirm_log failed!", K_(partition_key), K(tmp_ret), K(ret), K(log_id));
    }
  }
  return ret;
}

int ObPartitionLogService::renew_ms_log_majority_cb_(
    const uint64_t log_id, const int64_t submit_timestamp, const common::ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = ms_task_mgr_.renew_ms_log_majority_cb(log_id, submit_timestamp, proposal_id))) {
    CLOG_LOG(WARN, "majority_cb failed", K_(partition_key), K(ret), K(log_id), K(submit_timestamp));
  } else if (OB_SUCCESS != (ret = ms_task_mgr_.set_renew_ms_log_confirmed(log_id, submit_timestamp, proposal_id))) {
    CLOG_LOG(WARN, "set_renew_ms_log_confirmed failed", K_(partition_key), K(ret), K(log_id), K(submit_timestamp));
  } else {
    CLOG_LOG(INFO, "renew_ms_log_majority_cb_ success", K_(partition_key), K(ret), K(log_id), K(submit_timestamp));
  }
  return ret;
}

int ObPartitionLogService::stop_election()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    do {
      RLockGuard guard(lock_);
      ret = state_mgr_.stop_election();
    } while (false);
    while (OB_SUCC(ret) && state_mgr_.is_leader_active()) {
      if (OB_FAIL(switch_state())) {
        CLOG_LOG(WARN, "switch_state failed after stop election", K(ret), K_(partition_key));
      } else if (state_mgr_.is_leader_active()) {
        // election should return not running
        // should be leader active after one cycle
        // sleep for further modify
        // should not come to this branch
        CLOG_LOG(ERROR, "still leader active after stop election", K(ret), K_(partition_key));
        usleep(100);
      }
    }
  }
  return ret;
}

int ObPartitionLogService::remove_election()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    // The reason why ret is not modified here is
    // According to the existing process of the upper layer, after the partition is deserialized from the file during
    // the recovery process There is a process of adding to pg_mgr without initialization And maybe before the
    // initialization, the partition is removed, and this interface is called at the same time At this time, if this
    // interface returns an error, the operation of removing the partition from the upper layer will fail
    CLOG_LOG(WARN, "pls not inited yet, just warn, return OB_SUCCESS", K_(partition_key));
  } else {
    // The reason for adding a write lock here is to hope that all read locks will be released when remove, and remove
    // when the old election is no longer used in other locations In order to provide a clear semantics to the outside
    // world That is, after remove_election, no one will hold an expired object Otherwise, it may happen that the same
    // pkey has a new object created, but the old object is still in use The outside may expect this interface to
    // provide the above-mentioned strict semantics, otherwise unexpected behavior may occur
    WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
    if (true == election_has_removed_) {
      CLOG_LOG(WARN, "election has been marked removed, should not call this again", K_(partition_key));
    } else if (NULL == election_mgr_) {
      // After the expected initialization, the election_mgr_ should not be empty when the function is called,
      // and the call stack needs to be checked when an incorrect calling method occurs
      CLOG_LOG(ERROR, "wrong way to call remove_election(), election_mgr_ is NULL", K_(partition_key));
    } else if (OB_FAIL(election_mgr_->remove_partition(partition_key_))) {
      CLOG_LOG(WARN, "remove partition from election_mgr failed", K(ret), K_(partition_key));
    } else {
      election_has_removed_ = true;
      FLOG_INFO(
          "remove partition from election_mgr success", K_(partition_key), K_(election_has_removed), K_(election));
    }
  }

  return ret;
}

void ObPartitionLogService::destroy()
{
  WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
  if (is_inited_) {
    is_inited_ = false;
    int tmp_ret = OB_SUCCESS;
    if (sw_.is_inited()) {
      sw_.destroy();
    }
    // Election is not removed from mgr, it may be because the addition to pg_mgr failed
    if (!election_has_removed_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = election_mgr_->remove_partition(partition_key_))) {
        CLOG_LOG(WARN, "remove_partition from election_mgr failed", K(tmp_ret), K_(partition_key));
      } else {
        election_has_removed_ = true;
        CLOG_LOG(WARN, "remove election in destroy(), probably add to pg_mgr failed", K(tmp_ret), K_(partition_key));
      }
    } else {
      FLOG_INFO("election has been removed", K_(partition_key), K_(election_has_removed), K_(election));
    }

    if (state_mgr_.is_inited()) {
      state_mgr_.destroy();
      if (NULL != election_) {
        if (NULL == election_mgr_) {
          CLOG_LOG(ERROR,
              "could not revert election, cause election_mgr_ is NULL",
              K_(partition_key),
              KP_(election_mgr),
              KP_(election));
        } else {
          election_mgr_->revert_election(election_);
          election_ = NULL;
        }
      } else {
        // do nothing
      }
    } else {
    }
    restore_mgr_.destroy();
    remote_log_query_engine_ = NULL;
    reconfirm_.reset();
    log_engine_ = NULL;
    replay_engine_ = NULL;
    // partition_service_ is a global signleton obj, no need set NULL
    // partition_service_ = NULL;
    if (mm_.is_inited()) {
      mm_.destroy();
    }
    if (cascading_mgr_.is_inited()) {
      cascading_mgr_.destroy();
    }
    fetch_log_engine_ = NULL;
    alloc_mgr_ = NULL;
    event_scheduler_ = NULL;
    self_.reset();
    // checksum_.destroy()
    saved_base_storage_info_.reset();
    zone_priority_ = UINT64_MAX;
    is_candidate_ = false;
    free_cursor_array_cache_();
    free_broadcast_info_mgr_();
    FLOG_INFO(
        "ObPartitionLogService destroy finished", K_(partition_key), K(lbt()), K_(election_has_removed), K_(election));
  } else {
    CLOG_LOG(WARN, "ObPartitionLogService destroy, no need exec", K_(partition_key), KP(this), K(lbt()));
  }
}

int ObPartitionLogService::set_election_leader(const common::ObAddr& leader, const int64_t lease_start)
{
  int ret = OB_SUCCESS;
  WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
  if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "wrlock failed", K(ret), K_(partition_key));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!leader.is_valid() || lease_start <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!restore_mgr_.is_archive_restoring()) {
    ret = state_mgr_.set_election_leader(leader, lease_start);
  } else {
    if (self_ == leader) {
      ret = restore_mgr_.leader_takeover();
    }
  }
  return ret;
}

bool ObPartitionLogService::check_election_leader_(int64_t& leader_epoch, ObTsWindows& changing_leader_windows) const
{
  // It returns OB_SUCCESS when changing leader
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  ObAddr leader;
  if (!ObReplicaTypeCheck::is_paxos_replica_V2(mm_.get_replica_type())) {
    ret = OB_NOT_MASTER;
  } else if (OB_SUCCESS != (ret = state_mgr_.get_election_leader(leader, leader_epoch, changing_leader_windows))) {
    CLOG_LOG(WARN, "get leader from election failed", K_(partition_key), K(ret));
  } else if (!leader.is_valid() || (self_ != leader) || (leader_epoch != state_mgr_.get_leader_epoch())) {
    CLOG_LOG(INFO,
        "now election has another leader",
        K_(partition_key),
        K(leader),
        K_(self),
        "current_leader_epoch",
        leader_epoch,
        "saved_leader_epoch",
        state_mgr_.get_leader_epoch());
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

int ObPartitionLogService::set_next_index_log_id(const uint64_t log_id, const int64_t accum_checksum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
    sw_.set_next_index_log_id(log_id, accum_checksum);
  }
  return ret;
}

int ObPartitionLogService::is_offline(bool& is_offline)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    is_offline = state_mgr_.is_offline();
  }
  return ret;
}

int ObPartitionLogService::set_offline()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
    state_mgr_.set_offline();
    const uint32_t reset_type = ObLogCascadingMgr::ResetPType::SELF_OFFLINE;
    cascading_mgr_.reset_parent(reset_type);
    share::ObCascadMemberList prev_children_list;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = cascading_mgr_.get_children_list(prev_children_list))) {
      CLOG_LOG(WARN, "get_children_list failed", K_(partition_key), K(tmp_ret));
    }
    cascading_mgr_.reset_children_list();
    cascading_mgr_.notify_change_parent(prev_children_list);
  }
  CLOG_LOG(INFO, "set_offline", K(ret), K_(partition_key));
  return ret;
}

int ObPartitionLogService::set_online(const ObBaseStorageInfo& base_storage_info, const ObVersion& memstore_version)
{
  int ret = OB_SUCCESS;
  WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_.is_offline()) {
    CLOG_LOG(INFO, "self is already online, skip", K(ret), K_(partition_key));
  } else {
    uint64_t next_replay_log_id = base_storage_info.get_last_replay_log_id() + 1;
    int64_t next_replay_log_ts = base_storage_info.get_submit_timestamp() + 1;
    sw_.set_next_replay_log_id_info(next_replay_log_id, next_replay_log_ts);
    if (base_storage_info.get_last_replay_log_id() + 1 >= sw_.get_start_id()) {
      // The migration logic guarantees that PartitionLogService must be offline when truncate_sw is executed
      // Partition in the offline state will not be elected as Leader, and will not submit_log
      // The thread that executes truncate is the thread of the migration task.
      // Since this operation may take a long time, the write lock needs to be released;
      lock_.wrunlock();
      if (OB_FAIL(truncate_first_stage_(base_storage_info))) {
        CLOG_LOG(WARN, "truncate_first_stage_ failed", K(ret), K_(partition_key), K(base_storage_info));
      }
      lock_.wrlock_with_retry(WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
      if (OB_SUCC(ret) && OB_FAIL(truncate_second_stage_(base_storage_info))) {
        CLOG_LOG(WARN, "truncate_second_stage_ failed", K(ret), K(partition_key_), K(base_storage_info));
      }
    } else {
      // do nothing
    }
    if (OB_SUCC(ret)) {
      (void)state_mgr_.update_freeze_version(memstore_version);
      state_mgr_.reset_need_rebuild();
      state_mgr_.set_online();
      in_sync_check_time_ = OB_INVALID_TIMESTAMP;
      common::ObAddr leader = state_mgr_.get_leader();
      if (!leader.is_valid()) {
        // leader is invalid, do nothing
      } else if (!cascading_mgr_.get_parent().is_valid() && FOLLOWER == state_mgr_.get_role()) {
        // request allocating parent when it is online
        int tmp_ret = OB_SUCCESS;
        const int64_t dst_cluster_id = state_mgr_.get_leader_cluster_id();
        if (OB_SUCCESS != (tmp_ret = cascading_mgr_.fetch_register_server(
                               leader, dst_cluster_id, state_mgr_.get_region(), state_mgr_.get_idc(), true, false))) {
          CLOG_LOG(WARN, "call fetch_register_server failed", K_(partition_key), K(tmp_ret), K(leader));
        } else {
          (void)cascading_mgr_.reset_last_request_state();
        }
      } else {
        // do nothing
      }
    }
  }
  CLOG_LOG(INFO, "set_online", K(ret), K_(partition_key), K(base_storage_info), K(memstore_version));
  return ret;
}

int ObPartitionLogService::is_in_sync(bool& is_sync) const
{
  // if there are too many partitions in one observer,
  // user can change max_safe_stale_time to avoid unsync ret
  int ret = OB_SUCCESS;
  is_sync = false;
  int64_t tenant_id = partition_key_.get_tenant_id();
  int64_t max_safe_stale_time = transaction::ObWeakReadUtil::max_stale_time_for_weak_consistency(tenant_id);
  max_safe_stale_time = std::min(max_safe_stale_time, MAX_SYNC_FALLBACK_INTERVAL);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K_(partition_key), K(ret));
  } else if (state_mgr_.is_need_rebuild()) {
    ret = OB_LOG_NOT_SYNC;
    CLOG_LOG(WARN, "partition_log_service need rebuild", K(ret), K_(partition_key));
  } else {
    int64_t next_replay_log_timestamp = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(sw_.get_next_replay_log_timestamp(next_replay_log_timestamp))) {
      CLOG_LOG(WARN, "sw get_next_replay_log_timestamp failed", K(ret), K_(partition_key));
    } else {
      is_sync = ((ObTimeUtility::current_time() - next_replay_log_timestamp) < max_safe_stale_time);
    }
  }
  return ret;
}

int ObPartitionLogService::is_log_sync_with_leader(bool& is_sync) const
{
  int ret = OB_SUCCESS;
  is_sync = false;
  ObAddr leader;
  int64_t dst_cluster_id = OB_INVALID_CLUSTER_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    leader = state_mgr_.get_leader();
    dst_cluster_id = state_mgr_.get_leader_cluster_id();
  }

  if (OB_SUCC(ret)) {
    int64_t unused_timestamp = OB_INVALID_TIMESTAMP;
    uint64_t leader_max_confirmed_log_id = OB_INVALID_ID;
    bool unused_remote_replica_is_normal = false;
    if (OB_FAIL(is_in_sync(is_sync))) {
      CLOG_LOG(WARN, "is_in_sync failed", K(ret), K(partition_key_));
    } else if (is_sync) {
      // already sync, do nothing
    } else if (!leader.is_valid()) {
      // leader is invalid, return false
      is_sync = false;
    } else if (OB_FAIL(log_engine_->get_remote_membership_status(leader,
                   dst_cluster_id,
                   partition_key_,
                   unused_timestamp,
                   leader_max_confirmed_log_id,
                   unused_remote_replica_is_normal))) {
      // return OB_SUCCESS when rpc failed to avoid migrate failure, this may occur when local leader is out of date
      CLOG_LOG(WARN,
          "get_remote_membership_status from leader failed",
          K(ret),
          K(leader),
          K(dst_cluster_id),
          K(partition_key_),
          K(unused_timestamp),
          K(leader_max_confirmed_log_id));
      is_sync = false;
      ret = OB_SUCCESS;
    } else {
      // 1000 is to set aside a sufficient threshold to be consistent with the pre-check for member changes
      is_sync = (leader_max_confirmed_log_id <= (get_next_index_log_id() - 1 + 1000));
    }
  }
  return ret;
}

int ObPartitionLogService::is_log_sync_with_primary(const int64_t switchover_epoch, bool& is_sync) const
{
  // caller guarantees self is non-private talbe
  int ret = OB_SUCCESS;
  int64_t local_switchover_epoch = OB_INVALID_TIMESTAMP;
  uint64_t leader_max_log_id = OB_INVALID_ID;
  int64_t leader_next_log_ts = OB_INVALID_TIMESTAMP;
  is_sync = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K_(partition_key), K(ret));
  } else if (switchover_epoch <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(ret), K(switchover_epoch));
  } else if (OB_FAIL(sw_.get_switchover_info(local_switchover_epoch, leader_max_log_id, leader_next_log_ts))) {
    CLOG_LOG(WARN, "sw_.get_switchover_info failed", K_(partition_key), K(ret));
  } else if (switchover_epoch != local_switchover_epoch) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(
        WARN, "switchover_epoch not match", K_(partition_key), K(ret), K(switchover_epoch), K(local_switchover_epoch));
  } else {
    int64_t local_max_confirmed_log_id = sw_.get_max_confirmed_log_id();
    uint64_t next_replay_log_id = OB_INVALID_ID;
    int64_t next_replay_log_ts = OB_INVALID_TIMESTAMP;
    sw_.get_next_replay_log_id_info(next_replay_log_id, next_replay_log_ts);
    if (local_max_confirmed_log_id != leader_max_log_id) {
      CLOG_LOG(WARN,
          "max_log_id not match with leader",
          K_(partition_key),
          K(ret),
          K(leader_max_log_id),
          K(local_max_confirmed_log_id));
    } else if (OB_INVALID_TIMESTAMP == next_replay_log_ts || next_replay_log_ts < leader_next_log_ts) {
      CLOG_LOG(WARN,
          "next_replay_log_ts not match with leader",
          K_(partition_key),
          K(ret),
          K(leader_next_log_ts),
          K(next_replay_log_ts),
          K(next_replay_log_id));
    } else {
      is_sync = true;
    }
  }

  if (OB_SUCC(ret) && !is_sync) {
    CLOG_LOG(INFO,
        "self is not sync with leader",
        K(ret),
        K_(partition_key),
        K(is_sync),
        "max_confirmed_log_id",
        sw_.get_max_confirmed_log_id(),
        K(leader_max_log_id));
  }
  return ret;
}

int ObPartitionLogService::is_need_rebuild(bool& need_rebuild) const
{
  int ret = OB_SUCCESS;
  need_rebuild = false;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K_(partition_key), K(ret));
  } else {
    need_rebuild = state_mgr_.is_need_rebuild();
  }
  return ret;
}

bool ObPartitionLogService::is_leader_active() const
{
  bool is_leader_active = false;
  RLockGuard guard(lock_);
  is_leader_active = state_mgr_.is_leader_active();
  return is_leader_active;
}

int ObPartitionLogService::check_self_is_election_leader_() const
{
  int ret = OB_SUCCESS;
  common::ObAddr election_leader;
  int64_t epoch = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(state_mgr_.get_election_leader(election_leader, epoch))) {
    CLOG_LOG(WARN, "get_leader_from_election failed", K_(partition_key), K(ret));
  } else if (!election_leader.is_valid() || election_leader != self_ || epoch != state_mgr_.get_leader_epoch()) {
    // already not leader, do nothing
    ret = OB_NOT_MASTER;
  } else {
    // self is leader
  }

  return ret;
}

bool ObPartitionLogService::is_paxos_offline_replica_() const
{
  bool bool_ret = false;
  RLockGuard guard(lock_);
  if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_.get_replica_type()) && state_mgr_.is_cluster_allow_vote() &&
      false == ATOMIC_LOAD(&is_in_leader_member_list_)) {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObPartitionLogService::has_valid_member_list() const
{
  bool bool_ret = false;
  if (IS_NOT_INIT) {
  } else {
    RLockGuard guard(lock_);
    bool_ret = state_mgr_.has_valid_member_list();
  }
  return bool_ret;
}

bool ObPartitionLogService::has_valid_next_replay_log_ts() const
{
  // Whether the data replica has not generated memtable, mainly includes the following situations:
  // 1. The follower of D replica, returns false;
  // 2. The leader of D replica: the active state returns true,
  //        restoring status returns true,
  //        Restore has not started in other states, and returns false.
  int bool_ret = false;
  if (IS_NOT_INIT) {
  } else {
    RLockGuard guard(lock_);
    if (0 != mm_.get_replica_property().get_memstore_percent()) {
      bool_ret = true;
    } else if (FOLLOWER == state_mgr_.get_role()) {
      bool_ret = false;
    } else if (LEADER == state_mgr_.get_role()) {
      if (state_mgr_.is_leader_active()) {
        bool_ret = true;
      } else if (state_mgr_.is_restoring()) {
        bool_ret = true;
      } else {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

int ObPartitionLogService::get_next_replay_info_for_data_replica_(
    uint64_t& next_replay_log_id, int64_t& next_replay_log_ts)
{
  int ret = OB_SUCCESS;
  if (LEADER == state_mgr_.get_role()) {
    if (state_mgr_.is_leader_active()) {
      // leader active state, return next_replay_ts
      (void)sw_.get_next_replay_log_id_info(next_replay_log_id, next_replay_log_ts);
    } else if (state_mgr_.is_restoring()) {
      // self is restoring, return last_replay_log_ts
      uint64_t last_replay_log_id = OB_INVALID_ID;
      int64_t last_replay_log_ts = OB_INVALID_TIMESTAMP;
      (void)sw_.get_last_replay_log(last_replay_log_id, last_replay_log_ts);
      if (OB_INVALID_ID == last_replay_log_id) {
      } else {
        next_replay_log_id = last_replay_log_id + 1;
        next_replay_log_ts = last_replay_log_ts + 1;
      }
    } else {
      // leader is not restoring, no need get from clog
    }
  } else {
    // empty pg or D replica
    (void)sw_.get_next_replay_log_id_info(next_replay_log_id, next_replay_log_ts);
  }
  return ret;
}

int ObPartitionLogService::get_next_replay_log_info(uint64_t& next_replay_log_id, int64_t& next_replay_log_timestamp)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    if (state_mgr_.is_offline()) {
      ret = OB_STATE_NOT_MATCH;
    } else if (state_mgr_.is_need_rebuild()) {
      ret = OB_STATE_NOT_MATCH;
    } else if (0 == mm_.get_replica_property().get_memstore_percent()) {
      // If it is a replica of D, when obtaining the standby machine-readable timestamp, the following situations need
      // to be distinguished: 1) In the non-<leader, active> and non-restore state, the time stamp read by the standby
      // machine needs to take the right boundary of its static data; 2) During the restore process (only the leader
      // will execute), the read timestamp of the standby machine needs to be the submit_timestamp of the
      // last_replay_log that has been submitted for playback; 3) In the state of <leader, active>, take
      // next_replay_log_ts.
      if (OB_FAIL(get_next_replay_info_for_data_replica_(next_replay_log_id, next_replay_log_timestamp))) {
        CLOG_LOG(WARN,
            "get_next_replay_info_for_data_replica_ failed",
            K_(partition_key),
            K(ret),
            K(next_replay_log_id),
            K(next_replay_log_timestamp));
      }
    } else if (state_mgr_.is_restoring()) {
      // self is restoring, return last_replay_log_ts
      uint64_t last_replay_log_id = OB_INVALID_ID;
      int64_t last_replay_log_ts = OB_INVALID_TIMESTAMP;
      (void)sw_.get_last_replay_log(last_replay_log_id, last_replay_log_ts);
      if (OB_INVALID_ID == last_replay_log_id) {
      } else {
        next_replay_log_id = last_replay_log_id + 1;
        next_replay_log_timestamp = last_replay_log_ts + 1;
      }
    } else {
      // normal replica, take next_replay_log_ts.
      (void)sw_.get_next_replay_log_id_info(next_replay_log_id, next_replay_log_timestamp);
    }
    if (OB_FAIL(ret)) {
      if (EXECUTE_COUNT_PER_SEC(10)) {
        CLOG_LOG(DEBUG,
            "get_next_replay_log_info error",
            K(ret),
            K_(partition_key),
            "is_offline",
            state_mgr_.is_offline(),
            "role",
            state_mgr_.get_role(),
            "state",
            state_mgr_.get_state());
      }
    }
  }

  return ret;
}

int ObPartitionLogService::get_follower_log_delay(int64_t& log_delay)
{
  int ret = OB_SUCCESS;
  int64_t next_replay_log_timestamp = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    if (state_mgr_.is_offline() || state_mgr_.is_need_rebuild() || FOLLOWER != state_mgr_.get_role()) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_FAIL(sw_.get_next_replay_log_timestamp(next_replay_log_timestamp))) {
      CLOG_LOG(WARN, "get_next_replay_log_timestamp failed", K_(partition_key), K(ret), K(next_replay_log_timestamp));
    } else {
      log_delay = ObTimeUtility::current_time() - next_replay_log_timestamp;
    }
  }
  return ret;
}

int ObPartitionLogService::process_keepalive_msg(const common::ObAddr& server, const int64_t cluster_id,
    const uint64_t next_log_id, const int64_t next_log_ts_lb, const uint64_t deliver_cnt)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  const common::ObAddr parent = cascading_mgr_.get_parent_addr();
  const common::ObAddr restore_leader = restore_mgr_.get_restore_leader();
  const common::ObReplicaType replica_type = mm_.get_replica_type();

  bool is_valid_parent = false;
  if (server == parent || cascading_mgr_.is_in_reregister_period()) {
    // parent match or in reregister period, allow to process
    is_valid_parent = true;
  }

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (LEADER == state_mgr_.get_role() || (parent.is_valid() && !is_valid_parent)) {
    if (partition_reach_time_interval(30 * 1000 * 1000, send_reject_msg_warn_time_)) {
      CLOG_LOG(WARN,
          "sender is unexpected, need reject",
          K_(partition_key),
          K(server),
          K(cluster_id),
          K(parent),
          K(restore_leader),
          K(next_log_id),
          K(next_log_ts_lb),
          "replica_type",
          replica_type);
    }
    ObReplicaMsgType msg_type = OB_REPLICA_MSG_TYPE_NOT_CHILD;
    (void)cascading_mgr_.reject_server(server, cluster_id, msg_type);
  } else if (deliver_cnt > MAX_DELIVER_CNT) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR,
        "deliver_cnt is larger than MAX_DELIVER_CNT, maybe loop appears",
        K(ret),
        K_(partition_key),
        K(next_log_id),
        K(next_log_ts_lb),
        K(deliver_cnt),
        "parent",
        cascading_mgr_.get_parent());
    // if predict there is loop, reset children_list to break it
    cascading_mgr_.reset_children_list();
  } else if (OB_FAIL(sw_.follower_update_leader_next_log_info(next_log_id, next_log_ts_lb))) {
    CLOG_LOG(WARN, "sw_.follower_update_leader_next_log_info failed", K(ret), K_(partition_key));
  } else if (!restore_mgr_.is_archive_restoring()) {
    // transfer keepalive to children in non-restoring state
    (void)send_keepalive_msg_to_children_(next_log_id, next_log_ts_lb, deliver_cnt);
    if (STANDBY_LEADER == state_mgr_.get_role()) {
      // standby_leader transfer keepalive to member_list
      (void)send_keepalive_msg_to_mlist_(next_log_id, next_log_ts_lb, deliver_cnt);
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionLogService::send_keepalive_msg_to_mlist_(
    const uint64_t next_log_id, const int64_t next_log_ts_lb, const uint64_t deliver_cnt) const
{
  UNUSED(deliver_cnt);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (LEADER == state_mgr_.get_role() || STANDBY_LEADER == state_mgr_.get_role()) {
    // send to curr_member_list and children_list
    const int64_t self_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    const common::ObMemberList member_list = mm_.get_curr_member_list();
    const int64_t member_cnt = member_list.get_member_number();
    for (int64_t i = 0; i < member_cnt && OB_SUCC(ret); ++i) {
      common::ObAddr server;
      if (OB_FAIL(member_list.get_server_by_index(i, server))) {
        CLOG_LOG(WARN, "get_server_by_index failed", K(ret), K_(partition_key), K(server));
      } else if (server == self_) {
        // skip self
      } else if (OB_FAIL(log_engine_->send_keepalive_msg(
                     server, self_cluster_id, partition_key_, next_log_id, next_log_ts_lb, 0 /* deliver_cnt */))) {
        CLOG_LOG(WARN, "send_keepalive_msg failed", K(ret), K_(partition_key), K(server));
      } else {
        // do nothing
      }
    }
  } else {
  }
  return ret;
}

int ObPartitionLogService::send_keepalive_msg_to_children_(
    const uint64_t next_log_id, const int64_t next_log_ts_lb, const uint64_t deliver_cnt) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObCascadMemberList children;
    if (OB_FAIL(cascading_mgr_.get_children_list(children))) {
      CLOG_LOG(WARN, "get_children_list failed", K_(partition_key), K(ret));
    } else {
      const int64_t child_cnt = children.get_member_number();
      for (int64_t i = 0; i < child_cnt; ++i) {
        share::ObCascadMember child;
        if (OB_FAIL(children.get_member_by_index(i, child))) {
          CLOG_LOG(WARN, "get_member_by_index failed", K(ret), K_(partition_key), K(child));
        } else if (!child.is_valid()) {
          CLOG_LOG(WARN, "invalid child", K(ret), K_(partition_key), K(child));
        } else if (OB_FAIL(log_engine_->send_keepalive_msg(child.get_server(),
                       child.get_cluster_id(),
                       partition_key_,
                       next_log_id,
                       next_log_ts_lb,
                       deliver_cnt + 1))) {
          CLOG_LOG(WARN, "send_keepalive_msg failed", K(ret), K_(partition_key), K(child));
        } else {
          // do nothing
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObCascadMemberList async_standby_children;
      if (OB_FAIL(cascading_mgr_.get_async_standby_children(async_standby_children))) {
        CLOG_LOG(WARN, "get_async_standby_children failed", K_(partition_key), K(ret));
      } else {
        const int64_t child_cnt = async_standby_children.get_member_number();
        for (int64_t i = 0; i < child_cnt; ++i) {
          share::ObCascadMember child;
          if (OB_FAIL(async_standby_children.get_member_by_index(i, child))) {
            CLOG_LOG(WARN, "get_member_by_index failed", K(ret), K_(partition_key), K(child));
          } else if (!child.is_valid()) {
            CLOG_LOG(WARN, "invalid child", K(ret), K_(partition_key), K(child));
          } else if (OB_FAIL(log_engine_->send_keepalive_msg(child.get_server(),
                         child.get_cluster_id(),
                         partition_key_,
                         next_log_id,
                         next_log_ts_lb,
                         deliver_cnt + 1))) {
            CLOG_LOG(WARN, "send_keepalive_msg failed", K(ret), K_(partition_key), K(child));
          } else {
            // do nothing
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionLogService::process_sync_log_archive_progress_msg(
    const common::ObAddr& server, const int64_t cluster_id, const ObPGLogArchiveStatus& status)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  const common::ObReplicaType replica_type = mm_.get_replica_type();
  const common::ObAddr parent = cascading_mgr_.get_parent_addr();

  bool is_valid_parent = false;
  if (server == parent || cascading_mgr_.is_in_reregister_period()) {
    // parent match or in reregister period, allow to process
    is_valid_parent = true;
  }

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (LEADER == state_mgr_.get_role() || (!parent.is_valid()) || (parent.is_valid() && !is_valid_parent)) {
    if (partition_reach_time_interval(30 * 1000 * 1000, send_reject_msg_warn_time_)) {
      CLOG_LOG(WARN,
          "sender is not my leader",
          K_(partition_key),
          K(server),
          K(cluster_id),
          K(parent),
          K(status),
          "replica_type",
          replica_type);
    }
  } else if (OB_FAIL(follower_update_log_archive_progress_(status))) {
    CLOG_LOG(WARN, "sw_.follower_update_log_archive_progress failed", K(ret), K_(partition_key), K(status));
  } else if (OB_FAIL(send_archive_progress_msg_to_children_(status))) {
    CLOG_LOG(WARN, "failed to send_archive_progress_msg_to_children_", K(ret), K_(partition_key), K(status));
  } else { /*do nothing*/
  }
  return ret;
}

int ObPartitionLogService::follower_update_log_archive_progress_(const ObPGLogArchiveStatus& status)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard wlock_guard(log_archive_status_rwlock_);
  if (log_archive_status_ < status) {
    log_archive_status_ = status;
  }
  return ret;
}

int ObPartitionLogService::process_leader_max_log_msg(const common::ObAddr& server, const int64_t switchover_epoch,
    const uint64_t leader_max_log_id, const int64_t leader_next_log_ts)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  const common::ObAddr parent = cascading_mgr_.get_parent_addr();
  const common::ObReplicaType replica_type = mm_.get_replica_type();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    if (LEADER == state_mgr_.get_role() || server != parent) {
      if (partition_reach_time_interval(30 * 1000 * 1000, send_reject_msg_warn_time_)) {
        CLOG_LOG(WARN,
            "sender is not my parent",
            K_(partition_key),
            K(server),
            "parent",
            parent,
            K(leader_max_log_id),
            K(replica_type));
      }
    } else if (OB_FAIL(
                   sw_.follower_update_leader_max_log_info(switchover_epoch, leader_max_log_id, leader_next_log_ts))) {
      CLOG_LOG(WARN, "sw_.follower_update_leader_max_log_info failed", K(ret), K_(partition_key));
    } else {
      // do nothing
    }

    (void)send_max_log_msg_to_children_(switchover_epoch, leader_max_log_id, leader_next_log_ts);

    if (STANDBY_LEADER == state_mgr_.get_role()) {
      (void)send_max_log_msg_to_mlist_(switchover_epoch, leader_max_log_id, leader_next_log_ts);
    }
  }
  return ret;
}

int ObPartitionLogService::send_max_log_msg_to_children_(
    const int64_t switchover_epoch, const uint64_t leader_max_log_id, const int64_t next_log_ts) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObCascadMemberList children;
    if (OB_FAIL(cascading_mgr_.get_children_list(children))) {
      CLOG_LOG(WARN, "get_children_list failed", K_(partition_key), K(ret));
    } else {
      const int64_t child_cnt = children.get_member_number();
      for (int64_t i = 0; i < child_cnt; ++i) {
        share::ObCascadMember child;
        if (OB_FAIL(children.get_member_by_index(i, child))) {
          CLOG_LOG(WARN, "get_member_by_index failed", K(ret), K_(partition_key), K(child));
        } else if (!child.is_valid()) {
          CLOG_LOG(WARN, "invalid child", K(ret), K_(partition_key), K(child));
        } else if (OB_FAIL(log_engine_->send_leader_max_log_msg(child.get_server(),
                       child.get_cluster_id(),
                       partition_key_,
                       switchover_epoch,
                       leader_max_log_id,
                       next_log_ts))) {
          CLOG_LOG(WARN, "send_leader_max_log_msg failed", K(ret), K_(partition_key), K(child));
        } else {
          CLOG_LOG(DEBUG,
              "send_max_log_msg_to_children_ succ",
              K(ret),
              K_(partition_key),
              K(children),
              K(switchover_epoch),
              K(leader_max_log_id),
              K(next_log_ts));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // send to sync standby child in max protection mode
      ObCascadMember sync_child = cascading_mgr_.get_sync_standby_child();
      if (sync_child.is_valid() && OB_FAIL(log_engine_->send_leader_max_log_msg(sync_child.get_server(),
                                       sync_child.get_cluster_id(),
                                       partition_key_,
                                       switchover_epoch,
                                       leader_max_log_id,
                                       next_log_ts))) {
        CLOG_LOG(WARN, "send_leader_max_log_msg failed", K(ret), K_(partition_key), K(sync_child));
      }
    }

    if (OB_SUCC(ret)) {
      // send to other standby children
      ObCascadMemberList async_standby_children;
      if (OB_FAIL(cascading_mgr_.get_async_standby_children(async_standby_children))) {
        CLOG_LOG(WARN, "get_async_standby_children failed", K_(partition_key), K(ret));
      } else {
        const int64_t child_cnt = async_standby_children.get_member_number();
        for (int64_t i = 0; i < child_cnt; ++i) {
          share::ObCascadMember child;
          if (OB_FAIL(async_standby_children.get_member_by_index(i, child))) {
            CLOG_LOG(WARN, "get_member_by_index failed", K(ret), K_(partition_key), K(child));
          } else if (!child.is_valid()) {
            CLOG_LOG(WARN, "invalid child", K(ret), K_(partition_key), K(child));
          } else if (OB_FAIL(log_engine_->send_leader_max_log_msg(child.get_server(),
                         child.get_cluster_id(),
                         partition_key_,
                         switchover_epoch,
                         leader_max_log_id,
                         next_log_ts))) {
            CLOG_LOG(WARN, "send_leader_max_log_msg failed", K(ret), K_(partition_key), K(child));
          } else {
            CLOG_LOG(DEBUG,
                "[HAOFAN] send_max_log_msg_to_children_ succ",
                K(ret),
                K_(partition_key),
                K(async_standby_children),
                K(switchover_epoch),
                K(leader_max_log_id),
                K(next_log_ts));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionLogService::send_max_log_msg_to_mlist_(
    const int64_t switchover_epoch, const uint64_t leader_max_log_id, const int64_t next_log_ts) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    const int64_t self_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    // send to paxos member_list
    const ObMemberList member_list = mm_.get_curr_member_list();
    const int64_t member_cnt = member_list.get_member_number();
    for (int64_t i = 0; i < member_cnt && OB_SUCC(ret); ++i) {
      ObMember member;
      if (OB_FAIL(member_list.get_member_by_index(i, member))) {
        CLOG_LOG(WARN, "get_server_by_index failed", K(ret), K_(partition_key));
      } else if (!member.is_valid()) {
        CLOG_LOG(WARN, "invalid member", K(ret), K_(partition_key), K(member));
      } else if (member.get_server() == self_) {
        // skip self
      } else if (OB_FAIL(log_engine_->send_leader_max_log_msg(member.get_server(),
                     self_cluster_id,
                     partition_key_,
                     switchover_epoch,
                     leader_max_log_id,
                     next_log_ts))) {
        CLOG_LOG(WARN, "send_leader_max_log_msg failed", K(ret), K_(partition_key), K(member));
      } else {
        CLOG_LOG(DEBUG,
            "leader_send_max_log_msg_to_mlist_ succ",
            K(ret),
            K_(partition_key),
            K(member_list),
            K(switchover_epoch),
            K(leader_max_log_id),
            K(next_log_ts));
      }
    }
  }
  return ret;
}

int ObPartitionLogService::migrate_set_base_storage_info(const common::ObBaseStorageInfo& base_storage_info)
{
  int ret = OB_SUCCESS;
  WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!state_mgr_.is_offline()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "not offline", K(ret), K_(partition_key));
  } else {
    // At this point, an empty memstore has been created using the parameter base_storage_info
    // In this case the dump must return base_storage_info based on this
    const uint64_t last_replay_log_id = base_storage_info.get_last_replay_log_id();
    const int64_t last_submit_timestamp = base_storage_info.get_submit_timestamp();
    const int64_t accum_checksum = base_storage_info.get_accumulate_checksum();
    sw_.set_last_replay_log(last_replay_log_id, last_submit_timestamp);
    sw_.set_saved_accum_checksum(accum_checksum);
    CLOG_LOG(INFO, "migrate_set_base_storage_info finished", K(ret), K_(partition_key), K(base_storage_info));
  }
  return ret;
}

int ObPartitionLogService::restore_replayed_log(const common::ObBaseStorageInfo& base_storage_info)
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "restore_replayed_log begin", K_(partition_key), K(base_storage_info));
  WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());

  // At this point, an empty memstore has been created using the parameter base_storage_info
  // In this case the dump must return base_storage_info based on this
  sw_.set_last_replay_log(base_storage_info.get_last_replay_log_id(), base_storage_info.get_submit_timestamp());
  sw_.set_saved_accum_checksum(base_storage_info.get_accumulate_checksum());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(disable_replay_())) {
    CLOG_LOG(WARN, "disable_replay_ failed", K(ret), K_(partition_key));
  } else if (OB_FAIL(replay_engine_->reset_partition(partition_key_))) {
    CLOG_LOG(WARN, "reset_partition failed", K(ret), K_(partition_key));
  } else if (!state_mgr_.set_restoring()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(replay_engine_) || OB_ISNULL(log_engine_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (base_storage_info.get_last_replay_log_id() + 1 >= sw_.get_start_id()) {
    // The migration logic guarantees that PartitionLogService must be offline when truncate_sw is executed
    // Partition in the offline state will not be elected as Leader, and will not submit_log
    // The thread that executes truncate is the thread of the migration task. Since this operation may take a long time,
    // the write lock needs to be released;
    lock_.wrunlock();
    if (OB_FAIL(truncate_first_stage_(base_storage_info))) {
      CLOG_LOG(WARN, "truncate_first_stage_ failed", K(ret), K_(partition_key), K(base_storage_info));
    }
    lock_.wrlock_with_retry(WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
    if (OB_SUCC(ret) && OB_FAIL(truncate_second_stage_(base_storage_info))) {
      CLOG_LOG(WARN, "truncate_second_stage_ failed", K(ret), K(partition_key_), K(base_storage_info));
    }
  } else {
    uint64_t start_id = base_storage_info.get_last_replay_log_id() + 1;
    const uint64_t max_log_id_to_submit = sw_.get_start_id() - 1;
    lock_.wrunlock();  // Release locks when iterating data to avoid reading locks for a long time and blocking other
                       // processes

    const int64_t start_ts = ObTimeUtility::current_time();
    bool is_not_timeout = false;
    const int64_t RETRY_SLEEP_TIME = 1 * 1000 * 1000;           // 1s
    const int64_t MAX_RETRY_COST_TIME = 10 * 60 * 1000 * 1000;  // 10min
    ObReplicaReplayType replay_type = INVALID_REPLICA_REPLAY_TYPE;
    if (OB_FAIL(sw_.get_replica_replay_type(replay_type))) {
      CLOG_LOG(WARN, "failed to get replica type", K(ret), K(partition_key_));
    } else if (REPLICA_REPLAY_ALL_LOG == replay_type) {
      do {
        if (OB_EAGAIN == ret) {
          usleep(RETRY_SLEEP_TIME);
        }
        ObLogCursorExt log_cursor_ext;
        if (OB_FAIL(log_engine_->get_cursor(partition_key_, max_log_id_to_submit, log_cursor_ext))) {
          if (OB_CURSOR_NOT_EXIST == ret) {
            // The first log ilog written by Partition has not been flushed or loaded by ilog_storage,
            // if a minor freeze occurs at this time
            // When the content of this log needs to be read, log_engine will return OB_CURSOR_NOT_EXIST;
            // In this scenario, OB_EAGAIN is also returned, and the upper layer will try again to read this data
            ret = OB_EAGAIN;
            CLOG_LOG(WARN,
                "ilog_storage get invalid log, cursor not exist",
                K(ret),
                K(partition_key_),
                K(max_log_id_to_submit));
          } else if (OB_NEED_RETRY == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
            ret = OB_EAGAIN;
          } else {
            CLOG_LOG(WARN, "log_engine get_cursor failed", K(ret), K(partition_key_), K(max_log_id_to_submit));
          }
        } else if (OB_FAIL(replay_engine_->submit_replay_log_task_by_restore(
                       partition_key_, max_log_id_to_submit, log_cursor_ext.get_submit_timestamp()))) {
          if (OB_EAGAIN == ret) {
            if (REACH_TIME_INTERVAL(100 * 1000)) {
              CLOG_LOG(
                  WARN, "failed to submit replay task by restore", K(ret), K_(partition_key), K(max_log_id_to_submit));
            }
          } else {
            CLOG_LOG(
                WARN, "failed to submit replay task by restore", K(ret), K_(partition_key), K(max_log_id_to_submit));
          }
        } else {
          sw_.set_last_replay_log(max_log_id_to_submit, log_cursor_ext.get_submit_timestamp());
          sw_.set_saved_accum_checksum(log_cursor_ext.get_accum_checksum());
          CLOG_LOG(INFO,
              "succ to submit replay task by restore",
              K_(partition_key),
              K(max_log_id_to_submit),
              K(log_cursor_ext));
        }
      } while (OB_EAGAIN == ret && (is_not_timeout = (ObTimeUtility::current_time() - start_ts) < MAX_RETRY_COST_TIME));

    } else if (REPLICA_REPLAY_PARTITIAL_LOG == replay_type) {
      uint64_t cur_log_id = start_id;
      bool need_replay = false;
      int64_t log_submit_timestamp = OB_INVALID_TIMESTAMP;
      int64_t next_replay_log_ts_for_rg = OB_INVALID_TIMESTAMP;
      int64_t accum_checksum = 0;
      do {
        if (OB_EAGAIN == ret) {
          usleep(RETRY_SLEEP_TIME);
        }
        bool is_meta_log = false;
        ObLogType log_type = OB_LOG_UNKNOWN;
        if (OB_FAIL(sw_.get_log_meta_info(
                cur_log_id, is_meta_log, log_submit_timestamp, next_replay_log_ts_for_rg, accum_checksum, log_type))) {
          if (OB_EAGAIN == ret) {
            if (REACH_TIME_INTERVAL(100 * 1000)) {
              CLOG_LOG(WARN, "failed to check is meta log", K(partition_key_), K(cur_log_id), K(ret));
            }
          } else {
            CLOG_LOG(WARN, "failed to check is meta log", K(partition_key_), K(cur_log_id), K(ret));
          }
        } else {
          need_replay = is_meta_log;
        }

        uint64_t last_replay_log_id = OB_INVALID_ID;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(replay_engine_->submit_replay_log_task_sequentially(partition_key_,
                       cur_log_id,
                       log_submit_timestamp,
                       need_replay,
                       log_type,
                       next_replay_log_ts_for_rg))) {
          if (OB_EAGAIN == ret) {
            if (REACH_TIME_INTERVAL(100 * 1000)) {
              CLOG_LOG(WARN, "failed to submit replay task step by step", K(ret), K_(partition_key), K(cur_log_id));
            }
          } else {
            CLOG_LOG(WARN, "failed to submit replay task step by step", K(ret), K_(partition_key), K(cur_log_id));
          }
        } else {
          sw_.set_last_replay_log(cur_log_id, log_submit_timestamp);
          sw_.set_saved_accum_checksum(accum_checksum);
          cur_log_id++;
        }
      } while ((OB_SUCC(ret) || OB_EAGAIN == ret) && (cur_log_id <= max_log_id_to_submit) &&
               (is_not_timeout = (ObTimeUtility::current_time() - start_ts < MAX_RETRY_COST_TIME)));

      if (OB_SUCC(ret) && cur_log_id > max_log_id_to_submit) {
        CLOG_LOG(INFO,
            "finished submitting log to replay engine ",
            K(ret),
            K(partition_key_),
            K(replay_type),
            K(max_log_id_to_submit),
            K(is_not_timeout));
      } else if (OB_FAIL(ret) && OB_EAGAIN != ret) {
        // do nothing
      } else {
        int64_t tmp_ret = ret;
        ret = OB_REPLAY_EAGAIN_TOO_MUCH_TIME;
        CLOG_LOG(WARN,
            "waiting too much time before finishing submitting log to replay engine ",
            KR(tmp_ret),
            KR(ret),
            K(partition_key_),
            K(replay_type),
            K(max_log_id_to_submit),
            K(is_not_timeout));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "unexpected replica type", K(ret), K(partition_key_), K(replay_type));
    }

    if (OB_SUCC(ret)) {
      // Wait for all log submission tasks to be completed
      bool is_submit_finished = false;
      do {
        if (OB_EAGAIN == ret) {
          // tenant maybe out of memory
          usleep(RETRY_SLEEP_TIME);
        }
        if (OB_FAIL(replay_engine_->is_submit_finished(partition_key_, is_submit_finished))) {
          CLOG_LOG(WARN, "failed to check is_submit_finished", K(ret), K_(partition_key), K(max_log_id_to_submit));
        } else if (!is_submit_finished) {
          ret = OB_EAGAIN;
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            CLOG_LOG(INFO, "wait submit finished", K(partition_key_), K(max_log_id_to_submit), K(ret));
          }
        } else { /*do nothing */
        }
      } while ((OB_SUCC(ret) || OB_EAGAIN == ret) && !is_submit_finished &&
               (is_not_timeout = ObTimeUtility::current_time() - start_ts < MAX_RETRY_COST_TIME));

      if (OB_SUCC(ret) && is_submit_finished) {
        CLOG_LOG(
            INFO, "finished submitting replay log task", K(partition_key_), K(replay_type), K(max_log_id_to_submit));
      } else if (OB_FAIL(ret) && OB_EAGAIN != ret) {
        // do nothing
      } else {
        int64_t tmp_ret = ret;
        ret = OB_REPLAY_EAGAIN_TOO_MUCH_TIME;
        CLOG_LOG(WARN,
            "waiting too much time before finishing submitting replay_task",
            K(ret),
            K(tmp_ret),
            K(partition_key_),
            K(replay_type),
            K(max_log_id_to_submit),
            K(is_not_timeout),
            K(is_submit_finished));
      }
    }

    lock_.wrlock_with_retry(WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = enable_replay_())) {
    CLOG_LOG(ERROR, "enable_replay_ failed", K(tmp_ret), K_(partition_key));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_CLOG_RESTORE_REPLAYED_LOG) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      CLOG_LOG(ERROR, "fake restore_replayed_log", K_(partition_key), K(ret));
    }
  }
#endif
  if (0 == mm_.get_replica_property().get_memstore_percent() && state_mgr_.is_leader_taking_over() && OB_SUCC(ret)) {
    // When D replica performs restore in the leader_taking_over phase,
    // it needs to be delayed until the active phase and then reset
  } else {
    state_mgr_.reset_restoring();
  }
  CLOG_LOG(INFO, "restore_replayed_log finished", K_(partition_key), K(ret), K(base_storage_info));
  return ret;
}

int ObPartitionLogService::truncate_first_stage_(const common::ObBaseStorageInfo& base_storage_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(sw_.truncate_first_stage(base_storage_info))) {
    CLOG_LOG(WARN, "sw truncate_first_stage failed", K(ret), K(partition_key_), K(base_storage_info));
  }
  CLOG_LOG(INFO, "truncate_first_stage_", K(ret), K(partition_key_), K(base_storage_info));
  return ret;
}

int ObPartitionLogService::truncate_second_stage_(const common::ObBaseStorageInfo& base_storage_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = mm_.set_membership_info(base_storage_info.get_curr_member_list(),
                                base_storage_info.get_membership_timestamp(),
                                base_storage_info.get_membership_log_id(),
                                base_storage_info.get_replica_num(),
                                base_storage_info.get_ms_proposal_id()))) {
    CLOG_LOG(WARN, "set_membership_info failed", K_(partition_key), K(ret), K(base_storage_info));
  } else if (OB_SUCCESS != (ret = sw_.truncate_second_stage(base_storage_info))) {
    CLOG_LOG(WARN, "sw truncate_second_stage failed", K_(partition_key), K(ret), K(base_storage_info));
  } else if (OB_SUCCESS != (ret = saved_base_storage_info_.deep_copy(base_storage_info))) {
    CLOG_LOG(WARN, "base_storage_info deep_copy failed", K_(partition_key), K(ret), K(base_storage_info));
  } else {
    // do nothing
  }
  CLOG_LOG(INFO,
      "truncate_second_stage_",
      K_(partition_key),
      K(ret),
      "last_replay_log_id",
      base_storage_info.get_last_replay_log_id(),
      "start_id",
      sw_.get_start_id());
  return ret;
}

bool ObPartitionLogService::is_freeze_log_(const char* buff, const int64_t size) const
{
  bool bool_ret = false;
  int64_t log_type = storage::OB_LOG_UNKNOWN;
  int64_t pos = 0;
  if (OB_SUCCESS != (serialization::decode_i64(buff, size, pos, &log_type))) {
    CLOG_LOG(ERROR, "deserialize failed", K_(partition_key));
    bool_ret = true;  // if error, try update data version
  } else {
    bool_ret = storage::ObStorageLogTypeChecker::is_freeze_log(log_type);
  }
  return bool_ret;
}

bool ObPartitionLogService::try_update_freeze_version_(const ObLogEntry& log_entry)
{
  bool bool_ret = true;
  ObLogType log_type = log_entry.get_header().get_log_type();
  if (OB_LOG_SUBMIT == log_type) {
    if (is_freeze_log_(log_entry.get_buf(), log_entry.get_header().get_data_len())) {
      state_mgr_.update_freeze_version(log_entry.get_header().get_freeze_version());
    }
  } else if (OB_LOG_MEMBERSHIP == log_type || OB_LOG_START_MEMBERSHIP == log_type || OB_LOG_NOP == log_type) {
    state_mgr_.update_freeze_version(log_entry.get_header().get_freeze_version());
  }
  return bool_ret;
}

int64_t ObPartitionLogService::get_membership_timestamp() const
{
  RLockGuard guard(lock_);
  return mm_.get_timestamp();
}

bool ObPartitionLogService::internal_check_remote_mc_ts_(
    const ObMemberList& member_list, const ObAddr& addr, const int64_t mc_timestamp, const uint64_t follower_max_gap)
{
  int tmp_ret = OB_SUCCESS;
  int64_t valid_num = 0;
  if (!addr.is_valid() || OB_ISNULL(log_engine_)) {
    tmp_ret = OB_INVALID_ARGUMENT;
  } else {
    ObAddr server;
    for (int64_t i = 0; i < member_list.get_member_number(); ++i) {
      server.reset();
      int64_t timestamp = OB_INVALID_TIMESTAMP;
      uint64_t follower_max_confirmed_log_id = OB_INVALID_ID;
      bool remote_replica_is_normal = false;
      if (OB_SUCCESS != (tmp_ret = member_list.get_server_by_index(i, server))) {
        CLOG_LOG(ERROR, "get_server_by_index failed", K_(partition_key), K(tmp_ret));
      } else if (server == self_ || server == addr) {
        CLOG_LOG(INFO, "server do not need check", K_(partition_key), K(server), K(addr), K_(self));
      } else if (OB_SUCCESS != (tmp_ret = log_engine_->get_remote_membership_status(server,
                                    state_mgr_.get_self_cluster_id(),
                                    partition_key_,
                                    timestamp,
                                    follower_max_confirmed_log_id,
                                    remote_replica_is_normal))) {
        CLOG_LOG(WARN, "get_remote_membership_status failed", K_(partition_key), K(tmp_ret), K(server));
      } else if (mc_timestamp != timestamp) {
        CLOG_LOG(
            INFO, "timestamp do not match", K_(partition_key), K(tmp_ret), K(server), K(mc_timestamp), K(timestamp));
      } else if (sw_.get_max_log_id() > follower_max_confirmed_log_id + follower_max_gap) {
        CLOG_LOG(INFO,
            "follower is not in sync",
            K_(partition_key),
            K(tmp_ret),
            K(server),
            K(follower_max_confirmed_log_id),
            "leader_max_log_id",
            sw_.get_max_log_id());
      } else if (!remote_replica_is_normal) {
        CLOG_LOG(INFO, "remote replica may be out_of_memory or disk is not enough", K_(partition_key), K(server));
      } else {
        valid_num++;
      }
    }
  }
  CLOG_LOG(INFO,
      "check_remote_mc_ts_",
      K_(partition_key),
      K(tmp_ret),
      K(valid_num),
      K(addr),
      K(member_list),
      K(mc_timestamp));
  return valid_num >= (mm_.get_replica_num() / 2);
}

int ObPartitionLogService::try_replay(const bool need_async, bool& is_replayed, bool& is_replay_failed)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    ret = sw_.submit_replay_task(need_async, is_replayed, is_replay_failed);
    if (!state_mgr_.can_slide_sw()) {
      if (partition_reach_time_interval(5 * 1000 * 1000, cannot_slide_sw_interval_)) {
        CLOG_LOG(WARN,
            "can_slide_sw return false",
            K_(partition_key),
            "role",
            state_mgr_.get_role(),
            "state",
            state_mgr_.get_state(),
            "is_replay_enabled",
            state_mgr_.is_replay_enabled());
      }
    }
  }
  return ret;
}

uint64_t ObPartitionLogService::get_last_slide_log_id()
{
  uint64_t last_slide_log_id = OB_INVALID_ID;
  RLockGuard guard(lock_);
  if (state_mgr_.is_offline()) {
    last_slide_log_id = OB_INVALID_ID;
  } else {
    last_slide_log_id = sw_.get_start_id() - 1;
  }
  return last_slide_log_id;
}

void ObPartitionLogService::get_last_replay_log(uint64_t& log_id, int64_t& ts)
{
  RLockGuard guard(lock_);
  sw_.get_last_replay_log(log_id, ts);
}

int64_t ObPartitionLogService::get_last_submit_timestamp() const
{
  RLockGuard guard(lock_);
  return sw_.get_last_submit_timestamp();
}

uint64_t ObPartitionLogService::get_start_log_id()
{
  RLockGuard guard(lock_);
  return sw_.get_start_id();
}

uint64_t ObPartitionLogService::get_start_log_id_unsafe()
{
  return sw_.get_start_id();
}

bool ObPartitionLogService::is_svr_in_member_list(const ObAddr& server) const
{
  bool bool_ret = false;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "not init", K_(partition_key), K_(is_inited));
  } else if (!server.is_valid()) {
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(server));
  } else {
    RLockGuard guard(lock_);
    bool_ret = mm_.get_curr_member_list().contains(server);
  }
  return bool_ret;
}

int ObPartitionLogService::get_dst_leader_candidate(common::ObMemberList& member_list) const
{
  int ret = OB_SUCCESS;
  member_list.reset();
  ObAddr addr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(mm_.get_replica_type())) {
    ret = OB_NOT_MASTER;
  } else {
    RLockGuard guard(lock_);
    ObMemberList elect_valid_candidates;
    if (OB_SUCCESS != (ret = state_mgr_.get_election_valid_candidates(elect_valid_candidates))) {
      CLOG_LOG(WARN, "get_election_valid_candidates failed", K_(partition_key), K(ret));
    } else {
      const common::ObMemberList& curr_member_list = mm_.get_curr_member_list();
      for (int64_t i = 0; i < curr_member_list.get_member_number() && (OB_SUCC(ret)); ++i) {
        addr.reset();
        if (OB_FAIL(curr_member_list.get_server_by_index(i, addr))) {
          CLOG_LOG(ERROR, "get_server_by_index failed", K(ret), K_(partition_key), K(curr_member_list), K(i));
        } else if (addr != self_ && elect_valid_candidates.contains(addr)) {
          if (OB_FAIL(member_list.add_server(addr))) {
            // should not be entry exist
            CLOG_LOG(ERROR, "member_list add_server failed", K(ret), K_(partition_key), K(member_list));
          }
        } else {
          // do nothing
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    RLockGuard guard(lock_);
    if (!state_mgr_.can_change_leader()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "can not change leader",
          K(ret),
          K_(partition_key),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state(),
          "membership_mgr is_state_init",
          mm_.is_state_init(),
          "is_changing_leader",
          state_mgr_.is_changing_leader(),
          K_(self));
    }
  }
  return ret;
}

int ObPartitionLogService::get_max_data_version(ObVersion& max_data_version) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    max_data_version = state_mgr_.get_freeze_version();
  }
  return ret;
}

int ObPartitionLogService::set_follower_active()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CLOG_LOG(INFO, "set_follower_active", K_(partition_key));
    WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
    state_mgr_.set_follower_active();
  }
  return ret;
}

int ObPartitionLogService::get_log_id_range(
    uint64_t& start_log_id, int64_t& start_ts, uint64_t& end_log_id, int64_t& end_ts)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("get_log_id_range", 1000 * 1000);
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    start_log_id = saved_base_storage_info_.get_last_replay_log_id();
    start_ts = saved_base_storage_info_.get_submit_timestamp();
    sw_.get_last_replay_log(end_log_id, end_ts);
  }
  CLOG_LOG(
      TRACE, "get log id range", K(ret), K_(partition_key), K(start_log_id), K(start_ts), K(end_log_id), K(end_ts));
  return ret;
}

int ObPartitionLogService::get_sw_max_log_id_info(uint64_t& sw_max_log_id, int64_t& sw_max_log_ts)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    sw_.get_max_log_id_info(sw_max_log_id, sw_max_log_ts);
  }
  CLOG_LOG(TRACE, "get sw max_log_id_info", K(ret), K_(partition_key), K(sw_max_log_id), K(sw_max_log_ts));
  return ret;
}

int ObPartitionLogService::get_sw_max_log_id(uint64_t& sw_max_log_id)
{
  int64_t sw_max_log_ts = OB_INVALID_TIMESTAMP;
  return get_sw_max_log_id_info(sw_max_log_id, sw_max_log_ts);
}

ObClogVirtualStat* ObPartitionLogService::alloc_clog_virtual_stat()
{
  int ret = OB_SUCCESS;
  ObClogVirtualStat* vitual_stat = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == (vitual_stat = op_reclaim_alloc(ObClogVirtualStat))) {
    CLOG_LOG(WARN, "alloc memory for vitual_stat failed", K(ret));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(vitual_stat->init(self_, partition_key_, &state_mgr_, &sw_, &mm_, &cascading_mgr_, this))) {
    CLOG_LOG(WARN, "vitual_stat init failed", K(ret), K_(partition_key));
    op_reclaim_free(vitual_stat);
    vitual_stat = NULL;
  } else {
  }
  return vitual_stat;
}

int ObPartitionLogService::revert_clog_virtual_stat(ObClogVirtualStat* virtual_stat)
{
  int ret = OB_SUCCESS;
  if (NULL == virtual_stat) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    op_reclaim_free(virtual_stat);
    virtual_stat = NULL;
  }
  return ret;
}

int ObPartitionLogService::set_member_list(const ObMemberList& member_list, const int64_t replica_num,
    const common::ObAddr& assigned_leader, const int64_t lease_start)
{
  // used for creating tenant in standby cluster or physical restoring
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!member_list.is_valid() || member_list.get_member_number() > replica_num) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(member_list));
  } else {
    WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
    if (GCTX.is_standby_cluster()) {
      int tmp_ret = OB_SUCCESS;
      ObAddr cur_leader;
      int64_t cur_leader_epoch = -1;
      if (OB_SUCCESS == (tmp_ret = state_mgr_.get_election_leader(cur_leader, cur_leader_epoch)) &&
          member_list.contains(cur_leader)) {
        // leader is valid, no need set
        CLOG_LOG(INFO,
            "leader is valid and in member_list, no need set election leader",
            K(ret),
            K_(partition_key),
            K(cur_leader),
            K(cur_leader_epoch),
            K(member_list));
      } else if (assigned_leader.is_valid() && lease_start >= 0) {
        // There is currently no leader, first set leader and then set member_list to avoid election devoting a new
        // leader The lease_start value may have expired when the retry is performed, and a check will be made in the
        // election Therefore, the set election leader may fail when retrying, and tmp_ret is used here to avoid
        // blocking the set_member_list later
        if (OB_SUCCESS != (tmp_ret = state_mgr_.standby_set_election_leader(assigned_leader, lease_start))) {
          CLOG_LOG(WARN,
              "standby_set_election_leader failed",
              K(tmp_ret),
              K_(partition_key),
              K(assigned_leader),
              K(lease_start));
        } else {
          CLOG_LOG(INFO,
              "standby_set_election_leader success",
              K(ret),
              K_(partition_key),
              K(assigned_leader),
              K(lease_start));
        }
      } else {
        // assigned_leader is invalid, skip
        CLOG_LOG(WARN,
            "assigned_leader is invlaid",
            K(ret),
            K_(partition_key),
            K(member_list),
            K(assigned_leader),
            K(lease_start));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(mm_.force_set_member_list(member_list, replica_num))) {
        CLOG_LOG(WARN, "mm_.force_set_member_list failed", K(ret), K_(partition_key), K(member_list), K(replica_num));
      } else if (OB_FAIL(state_mgr_.start_election())) {
        CLOG_LOG(WARN, "start_election failed", K_(partition_key), K(ret));
      } else if (OB_FAIL(state_mgr_.set_scan_disk_log_finished())) {
        CLOG_LOG(WARN, "set_scan_disk_log_finished failed", K_(partition_key), K(ret));
      } else {
        // do nothing
      }
    }
  }
  CLOG_LOG(INFO, "set_member_list finished", K(ret), K_(partition_key), K(member_list), K(replica_num));
  return ret;
}

int ObPartitionLogService::check_if_start_log_task_empty(bool& is_empty)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret), K_(partition_key));
  } else if (OB_FAIL(sw_.check_left_bound_empty(is_empty))) {
    if (OB_EAGAIN == ret) {
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        CLOG_LOG(WARN, "failed to check_left_bound_empty", K(ret), K_(partition_key));
      }
    } else {
      CLOG_LOG(WARN, "failed to check_left_bound_empty", K(ret), K_(partition_key));
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObPartitionLogService::try_freeze_aggre_buffer()
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  ret = sw_.try_freeze_aggre_buffer();
  return ret;
}

int ObPartitionLogService::get_last_archived_log_id(
    const int64_t incarnation, const int64_t archive_round, uint64_t& last_archived_log_id)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard rlock_guard(log_archive_status_rwlock_);
  if (incarnation != log_archive_status_.archive_incarnation_ ||
      archive_round != log_archive_status_.log_archive_round_) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN,
        "incarnation or archive round is not the same, need retry",
        K(ret),
        K_(partition_key),
        K(incarnation),
        K(archive_round),
        K(log_archive_status_));
  } else {
    last_archived_log_id = log_archive_status_.last_archived_log_id_;
  }
  return ret;
}

int ObPartitionLogService::force_set_as_single_replica()
{
  int ret = OB_SUCCESS;
  WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
  ret = mm_.force_set_as_single_replica();
  return ret;
}

int ObPartitionLogService::force_set_replica_num(const int64_t replica_num)
{
  int ret = OB_SUCCESS;
  WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
  ret = mm_.force_set_replica_num(replica_num);
  return ret;
}

int ObPartitionLogService::force_set_parent(const common::ObAddr& new_parent)
{
  int ret = OB_SUCCESS;
  if (!new_parent.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
    const uint32_t reset_type = ObLogCascadingMgr::ResetPType::ADMIN_CMD;
    cascading_mgr_.reset_parent(reset_type);
    const int64_t dst_cluster_id = state_mgr_.get_leader_cluster_id();
    int64_t next_replay_log_ts = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(sw_.get_next_replay_log_timestamp(next_replay_log_ts))) {
      CLOG_LOG(WARN, "sw get_next_replay_log_timestamp failed", K(ret), K_(partition_key));
    } else if (OB_FAIL(log_engine_->fetch_register_server(new_parent,
                   dst_cluster_id,
                   partition_key_,
                   state_mgr_.get_region(),
                   state_mgr_.get_idc(),
                   mm_.get_replica_type(),
                   next_replay_log_ts,
                   false,
                   false))) {
      CLOG_LOG(WARN, "fetch_register_server failed", K_(partition_key), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPartitionLogService::force_reset_parent()
{
  int ret = OB_SUCCESS;
  WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
  const uint32_t reset_type = ObLogCascadingMgr::ResetPType::ADMIN_CMD;
  cascading_mgr_.reset_parent(reset_type);
  return ret;
}

int ObPartitionLogService::force_set_server_list(const obrpc::ObServerList& server_list, const int64_t replica_num)
{
  int ret = OB_SUCCESS;
  int64_t unused_epoch = 0;
  ObTsWindows unused_windows;
  WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
  CLOG_LOG(INFO, "force_set_server_list", K(partition_key_), K(server_list), K(replica_num));
  if (OB_NOT_MASTER != (ret = get_role_unlock(unused_epoch, unused_windows))) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN,
        "this replica is Leader, not allow to force_set_server_list",
        K(ret),
        K(partition_key_),
        K(server_list),
        K(replica_num));
  } else {
    ret = mm_.force_set_server_list(server_list, replica_num);
  }
  return ret;
}

int ObPartitionLogService::send_log_(const common::ObAddr& server, const int64_t cluster_id,
    const ObLogEntry& log_entry, const common::ObProposalID& proposal_id, const int64_t network_limit)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || (!proposal_id.is_valid() && !restore_mgr_.is_archive_restoring()) ||
             OB_ISNULL(log_engine_) || OB_ISNULL(alloc_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObPushLogMode push_mode = PUSH_LOG_ASYNC;
    if (GCTX.need_sync_to_standby() && LEADER == state_mgr_.get_role() &&
        !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
        cascading_mgr_.get_sync_standby_child().get_server() == server) {
      // in max_protection mode, all logs that primary leader send to standby are set PUSH_LOG_SYNC
      push_mode = PUSH_LOG_SYNC;
    }

    int64_t pos = 0;
    char* serialize_buff = NULL;
    int64_t serialize_size = log_entry.get_serialize_size();
    if (NULL == (serialize_buff = static_cast<char*>(alloc_mgr_->ge_alloc(serialize_size)))) {
      CLOG_LOG(ERROR, "alloc failed", K_(partition_key));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_SUCCESS != (ret = log_entry.serialize(serialize_buff, serialize_size, pos))) {
      CLOG_LOG(WARN, "submit_log serialize failed", K_(partition_key), K(ret));
    } else {
      ObLogNetTask net_task(proposal_id, serialize_buff, serialize_size);
      if (OB_SUCCESS != (ret = log_engine_->submit_fetch_log_resp(
                             server, cluster_id, partition_key_, network_limit, push_mode, &net_task))) {
        CLOG_LOG(WARN, "submit_fetch_log_resp failed", K_(partition_key), K(ret), K(log_entry));
      } else {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          CLOG_LOG(INFO,
              "submit_fetch_log_resp succ",
              K(server),
              K(partition_key_),
              K(network_limit),
              "log_id",
              log_entry.get_header().get_log_id(),
              K(push_mode));
        }
      }
    }
    if (NULL != serialize_buff) {
      alloc_mgr_->ge_free(serialize_buff);
      serialize_buff = NULL;
    }
  }
  return ret;
}

int ObPartitionLogService::send_null_log_(const common::ObAddr& server, const int64_t cluster_id, const uint64_t log_id,
    const common::ObProposalID& proposal_id, const int64_t network_limit)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || log_id <= 0 || (!proposal_id.is_valid() && !restore_mgr_.is_archive_restoring())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObLogEntryHeader header;
    ObLogEntry log_entry;
    ObProposalID invalid_proposal_id;
    const bool is_trans_log = false;
    if (OB_SUCCESS != (ret = header.generate_header(OB_LOG_NOT_EXIST,  // log_type
                           partition_key_,                             // partition_key
                           log_id,                                     // log_id
                           NULL,                                       // buf
                           0,                                          // data_len
                           ObTimeUtility::current_time(),              // generation_timestamp
                           OB_INVALID_TIMESTAMP,                       // epoch_id
                           invalid_proposal_id,                        // proposal_id
                           OB_INVALID_TIMESTAMP,                       // submit_timestamp
                           ObVersion(),
                           is_trans_log))) {
      CLOG_LOG(WARN, "generate_header failed", K_(partition_key), K(ret));
    } else if (OB_SUCCESS != (ret = log_entry.generate_entry(header, NULL))) {
      CLOG_LOG(WARN, "generate_entry failed", K_(partition_key), K(ret));
    } else if (OB_SUCCESS != (ret = send_log_(server, cluster_id, log_entry, proposal_id, network_limit))) {
      CLOG_LOG(WARN, "submit_log failed", K_(partition_key), K(ret), K(log_entry), K(proposal_id));
    } else {
    }
  }
  return ret;
}

int ObPartitionLogService::send_confirm_info_(const common::ObAddr& server, const int64_t cluster_id,
    const ObLogEntry& log_entry, const int64_t accum_checksum, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  ObCascadMemberList list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_ISNULL(log_engine_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(list.add_member(ObCascadMember(server, cluster_id)))) {
    CLOG_LOG(WARN, "add_member failed", K_(partition_key), K(ret), K(server));
  } else {
    const uint64_t log_id = log_entry.get_header().get_log_id();
    ObConfirmedInfo confirmed_info;
    if (OB_SUCCESS !=
        (ret = confirmed_info.init(
             log_entry.get_header().get_data_checksum(), log_entry.get_header().get_epoch_id(), accum_checksum))) {
      CLOG_LOG(WARN, "confirmed_info init failed", K_(partition_key), K(ret));
    } else if (OB_SUCCESS != (ret = log_engine_->submit_confirmed_info(
                                  list, partition_key_, log_id, confirmed_info, batch_committed))) {
      CLOG_LOG(WARN,
          "log_engine submit_confirmed_info",
          K_(partition_key),
          K(ret),
          K(list),
          K(log_id),
          K(confirmed_info),
          K(batch_committed));
    }
  }
  return ret;
}

int ObPartitionLogService::get_next_timestamp(const uint64_t last_log_id, int64_t& res_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_ID == last_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "get next timestamp error", K(ret), K(last_log_id));
  } else {
    ret = sw_.get_next_timestamp(last_log_id, res_ts);
  }
  return ret;
}

int ObPartitionLogService::try_update_next_replay_log_ts_in_restore(const int64_t new_ts)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (restore_mgr_.can_update_next_replay_log_ts_in_restore()) {
    sw_.try_update_next_replay_log_info_on_leader(new_ts);
  } else { /*dp nothing*/
  }
  return ret;
}

int ObPartitionLogService::leader_update_next_replay_log_ts_under_lock()
{
  int ret = OB_SUCCESS;
  // lock to avoid leader switch
  RLockGuard guard(lock_);
  const int64_t now = ObClockGenerator::getClock();
  bool is_cluster_status_allow_update = false;
  if (!ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    if (GCTX.is_primary_cluster() || GCTX.is_in_flashback_state() || GCTX.is_in_cleanup_state()) {
      // non-private leader only in flashback/cleanup state can advance next_log_ts
      // it cannot do this in swithing state, which may lead conflict with new primary cluster
      is_cluster_status_allow_update = true;
    } else {
      is_cluster_status_allow_update = false;
    }
  } else {
    // cluster private table, allow leader update
    is_cluster_status_allow_update = true;
  }
  if (is_cluster_status_allow_update) {
    if (OB_FAIL(leader_update_next_replay_log_ts_(now))) {
      CLOG_LOG(WARN, "leader_update_next_replay_log_ts_ failed", K(ret), K(partition_key_), K(now));
    }
  }
  return ret;
}

int ObPartitionLogService::get_next_served_log_info_for_leader(
    uint64_t& next_served_log_id, int64_t& next_served_log_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    // try update next_log_ts firstly
    int tmp_ret = leader_update_next_replay_log_ts_under_lock();
    if (OB_SUCCESS != tmp_ret) {
      CLOG_LOG(WARN,
          "update next replay log ts fail when get_next_served_log_info_for_leader",
          K(tmp_ret),
          K(partition_key_));
    }

    ret = sw_.get_next_served_log_info_by_next_replay_log_info(next_served_log_id, next_served_log_ts);
  }
  return ret;
}

int ObPartitionLogService::get_pls_epoch(int64_t& pls_epoch) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(state_mgr_.get_pls_epoch(pls_epoch))) {
    CLOG_LOG(WARN, "get_pls_epoch failed", K(partition_key_), K(ret));
  } else {
    // do_nothing
  }
  return ret;
}

int ObPartitionLogService::allow_gc(bool& allowed)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(log_engine_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (state_mgr_.is_pre_member_changing()) {
    CLOG_LOG(INFO, "this partition is member changing , not allow gc", K(partition_key_));
    allowed = false;
  } else {
    allowed = true;
  }
  return ret;
}

int ObPartitionLogService::notify_log_missing(
    const common::ObAddr& src_server, const uint64_t start_log_id, const bool is_in_member_list, const int32_t msg_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K_(partition_key), K(ret));
  } else if (OB_INVALID_MSG_TYPE == msg_type) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K_(partition_key),
        K(ret),
        K(src_server),
        K(start_log_id),
        K(is_in_member_list),
        K(msg_type));
  } else if (OB_REBUILD_MSG == msg_type) {
    ATOMIC_STORE(&is_in_leader_member_list_, is_in_member_list);
    const common::ObAddr parent = cascading_mgr_.get_parent_addr();
    const common::ObReplicaType replica_type = mm_.get_replica_type();
    do {
      WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
      if (OB_FAIL(ret)) {
        CLOG_LOG(WARN, "notify_log_missing wlock failed", K(ret));
      } else if (start_log_id != sw_.get_start_id()) {
        ret = OB_STATE_NOT_MATCH;
        CLOG_LOG(INFO,
            "notify_log_missing, log id not match",
            K(ret),
            K_(partition_key),
            K(start_log_id),
            K(sw_.get_start_id()));
      } else if ((!ObReplicaTypeCheck::is_paxos_replica_V2(replica_type) && !parent.is_valid()) ||
                 (parent.is_valid() && src_server != parent)) {
        ret = OB_STATE_NOT_MATCH;
        CLOG_LOG(
            INFO, "notify_log_missing, parent not match", K_(partition_key), K(src_server), K(parent), K(replica_type));
      } else {
        // do nothing
      }
      // Set the need_rebuild state must be under write lock protection, and the above check conditions are passed
      // Otherwise, it is possible that after the judgment condition is passed, before the set_need_rebuild is executed,
      // the log is received, sliding_cb is executed, and the rebuild is no longer needed Need to ensure mutual
      // exclusion between set_need_rebuild and sliding_cb
      if (OB_SUCC(ret)) {
        state_mgr_.set_need_rebuild();
      }
    } while (0);

    if (OB_SUCC(ret)) {
      bool need_exec_rebuild = true;
      if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type) && !is_in_member_list) {
        if (GCTX.is_standby_cluster() && !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
          // non-private paxos replica in standby cluster, can do rebuild
        } else if (restore_mgr_.is_archive_restoring()) {
          // physical_restoring replica can do rebuild
        } else {
          // other paxos replicas that not in member_list, cannot do rebuild
          need_exec_rebuild = false;
          CLOG_LOG(INFO,
              "self is paxos replica, but not in leader's member_list, no need exec rebuild",
              K(ret),
              K_(partition_key),
              K_(self),
              K(replica_type),
              K(src_server),
              K(is_in_member_list));
        }
      }
      if (!need_exec_rebuild) {
        // no need exec rebuild
      } else if (ATOMIC_LOAD(&last_rebuild_time_) == OB_INVALID_TIMESTAMP ||
                 ObTimeUtility::current_time() - ATOMIC_LOAD(&last_rebuild_time_) >= REBUILD_REPLICA_INTERVAL) {
        if (OB_FAIL(partition_service_->handle_log_missing(partition_key_, self_))) {
          CLOG_LOG(WARN, "handle_log_missing failed", K(ret), K_(partition_key), K_(self));
        } else {
          ATOMIC_STORE(&last_rebuild_time_, ObTimeUtility::current_time());
          CLOG_LOG(INFO, "handle_log_missing success", K(ret), K_(partition_key), K_(self));
        }
      }
    }
    CLOG_LOG(INFO,
        "handle_log_missing finished",
        K(ret),
        K_(partition_key),
        K_(self),
        K(src_server),
        K(is_in_member_list),
        K(start_log_id),
        K(replica_type));
  } else if (OB_STANDBY_RESTORE_MSG == msg_type) {
    RLockGuard guard(lock_);
    const common::ObReplicaType replica_type = mm_.get_replica_type();
    if (!restore_mgr_.is_standby_restore_state()) {
      CLOG_LOG(INFO, "self is not in restore state, igore msg", K_(partition_key), K(src_server));
    } else if (FOLLOWER != state_mgr_.get_role()) {
      CLOG_LOG(WARN, "self is not follower, igore msg", K_(partition_key), K(src_server));
    } else if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type) && !is_in_member_list) {
      CLOG_LOG(INFO,
          "self is paxos replica, but not in standby_leader's member_list, cannot exec restore",
          K(ret),
          K_(partition_key),
          K_(self),
          K(replica_type),
          K(src_server),
          K(is_in_member_list));
    } else {
      share::ObCascadMember leader_member = state_mgr_.get_leader_member();
      if (leader_member.is_valid()) {
        if (OB_FAIL(try_exec_standby_restore_(leader_member))) {
          CLOG_LOG(WARN, "try_exec_standby_restore_ failed", K_(partition_key), K(ret));
        }
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionLogService::try_update_leader_from_loc_cache()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (state_mgr_.try_update_leader_from_loc_cache()) {
    CLOG_LOG(WARN, "try_update_leader_from_loc_cache failed", K_(partition_key), K(ret));
  } else {
    CLOG_LOG(DEBUG, "try_update_leader_from_loc_cache", K_(partition_key));
  }
  return ret;
}

bool ObPartitionLogService::is_self_lag_behind_(const int64_t arg_next_replay_log_ts)
{
  bool bool_ret = false;
  int tmp_ret = OB_SUCCESS;
  bool is_sync = false;
  if (OB_SUCCESS == (tmp_ret = is_in_sync(is_sync)) && is_sync) {
    // self sync
  } else {
    // self unsync, compare next_replay_log_ts
    int64_t self_next_replay_log_ts = OB_INVALID_TIMESTAMP;
    if (OB_SUCCESS != (tmp_ret = sw_.get_next_replay_log_timestamp(self_next_replay_log_ts))) {
      CLOG_LOG(WARN, "sw get_next_replay_log_timestamp failed", K(tmp_ret), K_(partition_key));
    } else if (OB_INVALID_TIMESTAMP != self_next_replay_log_ts && self_next_replay_log_ts <= arg_next_replay_log_ts) {
      bool_ret = true;
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        CLOG_LOG(WARN,
            "is_self_lag_behind_ return true",
            K(self_next_replay_log_ts),
            K(arg_next_replay_log_ts),
            K_(partition_key));
      }
    } else {
      // do nothing
    }
  }
  return bool_ret;
}

int ObPartitionLogService::fetch_register_server(const common::ObAddr& server, const common::ObReplicaType replica_type,
    const int64_t next_replay_log_ts, const bool is_request_leader, const bool is_need_force_register,
    const ObRegion& region, const int64_t cluster_id, const ObIDC& idc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || region.is_empty() || !ObReplicaTypeCheck::is_replica_type_valid(replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K_(partition_key),
        K(ret),
        K(server),
        K(replica_type),
        K(next_replay_log_ts),
        K(is_request_leader),
        K(is_need_force_register),
        K(region),
        K(cluster_id));
  } else if (server == self_) {
    // ignore self
  } else {
    const bool is_self_lag_behind = is_self_lag_behind_(next_replay_log_ts);
    const common::ObRegion self_region = state_mgr_.get_region();
    // lock to avoid role change
    RLockGuard guard(lock_);
    if (is_request_leader && LEADER != state_mgr_.get_role() && STANDBY_LEADER != state_mgr_.get_role()) {
      // self is not leader, ignore
    } else if (OB_FAIL(cascading_mgr_.process_fetch_reg_server_req(server,
                   replica_type,
                   is_self_lag_behind,
                   is_request_leader,
                   is_need_force_register,
                   region,
                   cluster_id,
                   idc,
                   self_region))) {
      CLOG_LOG(WARN,
          "process_fetch_reg_server_req failed",
          K_(partition_key),
          K(ret),
          K(server),
          K(idc),
          K(region),
          K(next_replay_log_ts),
          K(is_request_leader),
          K(is_need_force_register),
          K(cluster_id),
          K(self_region));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObPartitionLogService::fetch_register_server_resp_v2(const common::ObAddr& sender,
    const bool is_assign_parent_succeed, const ObCascadMemberList& candidate_list, const int32_t msg_type)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!sender.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(is_assign_parent_succeed), K(candidate_list));
  } else {
    const common::ObRegion self_region = state_mgr_.get_region();
    RLockGuard guard(lock_);
    if (OB_FAIL(cascading_mgr_.process_fetch_reg_server_resp(
            sender, is_assign_parent_succeed, candidate_list, msg_type, self_region))) {
      CLOG_LOG(WARN,
          "process_fetch_reg_server_resp failed",
          K_(partition_key),
          K(ret),
          K(is_assign_parent_succeed),
          K(candidate_list),
          K(msg_type),
          K(self_region));
    }
  }
  return ret;
}

int ObPartitionLogService::replace_sick_child(
    const common::ObAddr& sender, const int64_t cluster_id, const common::ObAddr& sick_child)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!sender.is_valid() || !sick_child.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(sender), K(sick_child));
  } else {
    RLockGuard guard(lock_);
    if (OB_FAIL(cascading_mgr_.try_replace_sick_child(sender, cluster_id, sick_child))) {
      CLOG_LOG(WARN, "try_replace_sick_child failed", K_(partition_key), K(ret), K(sender), K(sick_child));
    }
  }
  return ret;
}

int ObPartitionLogService::get_curr_leader_and_memberlist(common::ObAddr& leader, common::ObRole& role,
    common::ObMemberList& curr_member_list, common::ObChildReplicaList& children_list) const
{
  int ret = OB_SUCCESS;
  leader.reset();
  curr_member_list.reset();
  children_list.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K_(partition_key), K(ret));
  } else if (restore_mgr_.is_archive_restoring()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "self is archive restoring, cannot handle this req", K_(partition_key), K(ret));
  } else {
    RLockGuard guard(lock_);
    leader = state_mgr_.get_leader();
    role = state_mgr_.get_role();

    if (OB_FAIL(curr_member_list.deep_copy(mm_.get_curr_member_list()))) {
      CLOG_LOG(WARN, "copy curr_member_list failed", K(ret), K_(partition_key));
    } else if (OB_FAIL(cascading_mgr_.get_lower_level_replica_list(children_list))) {
      CLOG_LOG(WARN, "fail to deep copy member list", K(ret));
    } else {
    }
  }
  return ret;
}

bool ObPartitionLogService::check_remote_mc_ts_(
    const ObMemberList& member_list, const ObAddr& addr, const int64_t mc_timestamp)
{
  const uint64_t FOLLOWER_MAX_GAP = 1000;
  return internal_check_remote_mc_ts_(member_list, addr, mc_timestamp, FOLLOWER_MAX_GAP);
}

bool ObPartitionLogService::check_remote_mc_ts_sync_(
    const ObMemberList& member_list, const ObAddr& addr, const int64_t mc_timestamp)
{
  const uint64_t FOLLOWER_MAX_GAP = 0;
  return internal_check_remote_mc_ts_(member_list, addr, mc_timestamp, FOLLOWER_MAX_GAP);
}

int ObPartitionLogService::wait_log_confirmed_()
{
  int ret = OB_LOG_NOT_SYNC;
  const int64_t begin_time = ObTimeUtility::current_time();
  const int64_t MAX_WAIT_TIME = 400 * 1000;  // 400 ms, 200 ms for rpc, 200 ms for flush_log
  const int64_t MIN_WAIT_TIME = 100 * 1000;  // 100 ms
  while (OB_LOG_NOT_SYNC == ret) {
    if (sw_.get_next_index_log_id() >= sw_.get_max_log_id() + 1) {
      ret = OB_SUCCESS;
    } else if (ObTimeUtility::current_time() - begin_time >= MAX_WAIT_TIME) {
      break;
    } else {
      usleep(100);
    }
  }
  const int64_t wait_time = ObTimeUtility::current_time() - begin_time;
  if (wait_time < MIN_WAIT_TIME && wait_time >= 0) {
    usleep(static_cast<useconds_t>(MIN_WAIT_TIME - wait_time));
  }
  return ret;
}

int64_t ObPartitionLogService::get_zone_priority() const
{
  return ATOMIC_LOAD(&zone_priority_);
}

void ObPartitionLogService::set_zone_priority(const uint64_t zone_priority)
{
  ATOMIC_STORE(&zone_priority_, zone_priority);
}

int ObPartitionLogService::set_region(const common::ObRegion& region)
{
  int ret = OB_SUCCESS;
  bool is_changed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(region));
  } else {
    common::ObRegion old_region = state_mgr_.get_region();
    if (region != old_region) {
      if (OB_FAIL(state_mgr_.set_region(region, is_changed))) {
        CLOG_LOG(WARN, "state_mgr set_region failed", K_(partition_key), K(ret));
      } else if (is_changed) {
        // self region changed, need notify children
        RLockGuard guard(lock_);
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = cascading_mgr_.process_region_change(region))) {
          CLOG_LOG(WARN, "process_region_change failed", K_(partition_key), K(tmp_ret));
        }
        if (REACH_TIME_INTERVAL(50 * 1000)) {
          CLOG_LOG(
              INFO, "in set_region, region changed", K_(partition_key), "old_region", old_region, "new_region", region);
        }
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

const common::ObRegion& ObPartitionLogService::get_region() const
{
  return state_mgr_.get_region();
}

int ObPartitionLogService::set_idc(const common::ObIDC& idc)
{
  int ret = OB_SUCCESS;
  bool is_changed = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (idc.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(idc));
  } else {
    common::ObIDC old_idc = state_mgr_.get_idc();
    if (idc != old_idc) {
      if (OB_FAIL(state_mgr_.set_idc(idc, is_changed))) {
        CLOG_LOG(WARN, "state_mgr set_idc failed", K_(partition_key), K(ret));
      } else if (is_changed) {
        if (REACH_TIME_INTERVAL(50 * 1000)) {
          CLOG_LOG(INFO, "in set_idc, idc changed", K_(partition_key), "old_idc", old_idc, "new_idc", idc);
        }
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

const common::ObIDC& ObPartitionLogService::get_idc() const
{
  return state_mgr_.get_idc();
}

int ObPartitionLogService::process_replica_type_change_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_.get_replica_type())) {
      // non_paxos->paxos change, need start election
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = state_mgr_.start_election())) {
        CLOG_LOG(WARN, "start_election failed", K_(partition_key), K(tmp_ret));
      }
      // replica_type is changed to paxos, it need check whether its parent is leader
      const common::ObAddr leader = state_mgr_.get_leader();
      if (leader.is_valid() && ObReplicaTypeCheck::is_paxos_replica_V2(mm_.get_replica_type()) && leader != self_) {
        // paxos replica set parent to leader directly
        const int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
        (void)cascading_mgr_.set_parent(leader, cluster_id);
      }
    } else {
      // changed to non-paxos type, stop election and reset parent
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = state_mgr_.stop_election())) {
        CLOG_LOG(WARN, "stop_election failed", K_(partition_key), K(tmp_ret));
      }
      const uint32_t reset_type = ObLogCascadingMgr::ResetPType::REPLICA_TYPE_CHANGE;
      cascading_mgr_.reset_parent(reset_type);
    }
  }
  return ret;
}

int ObPartitionLogService::process_reject_msg(
    const common::ObAddr& server, const int32_t msg_type, const int64_t send_ts)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_REPLICA_MSG_TYPE_UNKNOWN == msg_type) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_INVALID_TIMESTAMP != send_ts && now - send_ts > CASCAD_MSG_DELAY_TIME_THRESHOLD) {
    CLOG_LOG(WARN, "msg is expired", K(ret), K_(partition_key), K(server), K(msg_type), K(send_ts));
  } else {
    RLockGuard guard(lock_);
    ret = cascading_mgr_.process_reject_msg(server, msg_type);
  }
  return ret;
}

int ObPartitionLogService::process_restore_log_finish_msg(const common::ObAddr& src_server, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  bool is_restore_log_finished = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (log_id < sw_.get_start_id()) {
    CLOG_LOG(
        WARN, "invalid argument", K(ret), K_(partition_key), K(src_server), K(log_id), "start_id", sw_.get_start_id());
  } else if (restore_mgr_.is_restore_log_finished()) {
    // already finished, skip
    is_restore_log_finished = true;
  } else {
    restore_mgr_.set_fetch_log_result(OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH);
  }
  CLOG_LOG(INFO,
      "recv restore log finished msg",
      K(ret),
      K_(partition_key),
      K(src_server),
      K(log_id),
      K(is_restore_log_finished));
  return ret;
}

// TODO: to be removed by haofan
int ObPartitionLogService::process_restore_alive_msg(const common::ObAddr& server, const uint64_t start_log_id)
{
  UNUSED(start_log_id);
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!restore_mgr_.is_archive_restoring()) {
    // not in restoring state, ignore
  } else if (restore_mgr_.get_restore_leader() != server) {
    // sender is not restore_leader, ignore
  } else {
    (void)restore_mgr_.record_leader_resp_time(server);
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO, "recv restore alive msg", K(ret), K_(partition_key), K(server), K(start_log_id));
    }
  }

  return ret;
}

int ObPartitionLogService::process_restore_alive_req(
    const common::ObAddr& server, const int64_t dst_cluster_id, const int64_t send_ts)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TIMESTAMP == send_ts) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K_(partition_key), K(server), K(send_ts));
  } else if (now - send_ts > CASCAD_MSG_DELAY_TIME_THRESHOLD) {
    CLOG_LOG(WARN, "msg is expired", K(ret), K_(partition_key), K(server), K(send_ts));
  } else if (!restore_mgr_.is_archive_restoring()) {
    // not in restoring state, ignore
    CLOG_LOG(DEBUG, "self is not in archive restoring state", K(ret), K_(partition_key), K(server), K(send_ts));
  } else if (restore_mgr_.get_role() != RESTORE_LEADER) {
    // self is not restore_leader, ignore
    CLOG_LOG(DEBUG, "self is not restore leader", K(ret), K_(partition_key), K(server), K(send_ts));
  } else if (OB_FAIL(log_engine_->send_restore_alive_resp(server, dst_cluster_id, partition_key_))) {
    CLOG_LOG(WARN, "send_restore_alive_resp failed", K(ret), K_(partition_key), K(server), K(send_ts));
  } else {
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(INFO, "send_restore_alive_resp success", K_(partition_key), K(server));
    }
  }

  return ret;
}

int ObPartitionLogService::process_restore_alive_resp(const common::ObAddr& server, const int64_t send_ts)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TIMESTAMP == send_ts) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K_(partition_key), K(server), K(send_ts));
  } else if (now - send_ts > CASCAD_MSG_DELAY_TIME_THRESHOLD) {
    CLOG_LOG(WARN, "msg is expired", K(ret), K_(partition_key), K(server), K(send_ts));
  } else if (!restore_mgr_.is_archive_restoring()) {
    // not in restoring state, ignore
    CLOG_LOG(DEBUG, "self is not in archive restoring state", K(ret), K_(partition_key), K(server), K(send_ts));
  } else if (restore_mgr_.get_restore_leader() != server) {
    // sender is not restore_leader, ignore
    CLOG_LOG(DEBUG, "server is not restore leader", K(ret), K_(partition_key), K(server), K(send_ts));
  } else {
    (void)restore_mgr_.record_leader_resp_time(server);
  }

  return ret;
}

int ObPartitionLogService::process_reregister_msg(
    const common::ObAddr& src_server, const share::ObCascadMember& new_leader, const int64_t send_ts)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!new_leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(src_server), K(new_leader));
  } else if (OB_INVALID_TIMESTAMP != send_ts && now - send_ts > CASCAD_MSG_DELAY_TIME_THRESHOLD) {
    CLOG_LOG(WARN, "msg is expired", K(ret), K_(partition_key), K(src_server), K(new_leader), K(send_ts));
  } else {
    RLockGuard guard(lock_);
    if (src_server != cascading_mgr_.get_parent_addr()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "sender is not my parent", K(ret), K_(partition_key));
    } else if (OB_FAIL(cascading_mgr_.process_reregister_msg(new_leader))) {
      CLOG_LOG(WARN, "process_reregister_msg failed", K(ret), K_(partition_key));
    } else {
      // do nothing
    }
  }
  return ret;
}

enum ObReplicaType ObPartitionLogService::get_replica_type()
{
  ObReplicaType replica_type = (ObReplicaType)mm_.get_replica_type();
  return replica_type;
}

int ObPartitionLogService::set_replica_type(const enum ObReplicaType replica_type)
{
  int ret = OB_SUCCESS;
  WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
  ObReplicaType old_replica_type = get_replica_type();
  if (!ObReplicaTypeCheck::is_replica_type_valid(replica_type)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid replica type", K(ret), K_(partition_key), K(replica_type));
  } else if (old_replica_type == replica_type) {
    CLOG_LOG(WARN,
        "replica type already equal, no need change",
        K(ret),
        K_(partition_key),
        K(replica_type),
        K(old_replica_type));
  } else if (!state_mgr_.can_set_replica_type()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "cannot set_replica_type now", K(ret), K(partition_key_));
  } else if (REPLICA_TYPE_LOGONLY == replica_type && OB_FAIL(alloc_broadcast_info_mgr_(partition_key_))) {
    CLOG_LOG(WARN, "alloc_broadcast_info_mgr_ failed", K(ret), K(partition_key_));
  } else if (OB_FAIL(mm_.set_replica_type(replica_type))) {
    CLOG_LOG(WARN, "membership_mgr set_replica_type failed", K(ret), K_(partition_key), K(replica_type));
    if (REPLICA_TYPE_LOGONLY != mm_.get_replica_type()) {
      free_broadcast_info_mgr_();
    }
  } else {
    // change success
    if (REPLICA_TYPE_LOGONLY == mm_.get_replica_type()) {
      sw_.destroy_aggre_buffer();
    } else {
      free_broadcast_info_mgr_();
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = process_replica_type_change_())) {
      CLOG_LOG(WARN, "process_replica_type_change_ failed", K_(partition_key), K(tmp_ret));
    }
  }
  return ret;
}

int ObPartitionLogService::check_is_normal_partition(bool& is_normal_partition) const
{
  int ret = OB_SUCCESS;
  bool is_out_of_memory = false;
  bool is_disk_not_enough = false;
  bool is_disk_error = false;
  bool is_clog_disk_error = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (OB_FAIL(replay_engine_->is_tenant_out_of_memory(partition_key_, is_out_of_memory))) {
    CLOG_LOG(ERROR, "is_tenant_out_of_memory failed", K(ret), K(partition_key_));
  } else if (OB_FAIL(ObIOManager::get_instance().is_disk_error(is_disk_error))) {
    CLOG_LOG(ERROR, "is_disk_error failed", K(ret), K(partition_key_));
  } else {
    is_clog_disk_error = log_engine_->is_clog_disk_error();
    is_disk_not_enough = !log_engine_->is_disk_space_enough();
    is_normal_partition = !(is_disk_not_enough || is_out_of_memory || is_disk_error || is_clog_disk_error);
  }
  return ret;
}

//==================== batch chagne member begin ====================
int ObPartitionLogService::pre_change_member(const int64_t quorum, const bool is_add_member, int64_t& mc_timestamp,
    ObMemberList& member_list, ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (quorum <= 0 || quorum > OB_MAX_MEMBER_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(partition_key_), K(quorum));
  } else {
    RLockGuard guard(lock_);
    if (!state_mgr_.can_change_member()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "can not change member",
          K(ret),
          K(partition_key_),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state(),
          "is_changing_leader",
          state_mgr_.is_changing_leader());
    } else {
      mc_timestamp = mm_.get_timestamp();
      bool is_standby_op = (STANDBY_LEADER == state_mgr_.get_role() ? true : false);
      if (!is_standby_op) {
        proposal_id = state_mgr_.get_proposal_id();
      } else {
        // standby non-private talbe's member change, use ms_proposal_id
        proposal_id = mm_.get_ms_proposal_id();
      }
      if (OB_SUCCESS != (ret = member_list.deep_copy(mm_.get_curr_member_list()))) {
        CLOG_LOG(WARN, "member_list deep_copy failed", K(ret), K(partition_key_));
      } else {
        const int64_t member_num = member_list.get_member_number();
        if (is_add_member && quorum < (member_num + 1)) {
          ret = OB_NOT_SUPPORTED;
          CLOG_LOG(
              INFO, "not supported operation", K(ret), K(partition_key_), K(is_add_member), K(quorum), K(member_num));
        } else if (!is_add_member && quorum < (member_num - 1)) {
          ret = OB_NOT_SUPPORTED;
          CLOG_LOG(
              INFO, "not supported operation", K(ret), K(partition_key_), K(is_add_member), K(quorum), K(member_num));
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObPartitionLogService::set_member_changing()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else {
    WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "set_member_changing wrlock failed", K(ret), K(partition_key_));
    } else if (state_mgr_.is_pre_member_changing()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "there is another member change operation in process", K(ret), K(partition_key_));
    } else {
      (void)state_mgr_.set_pre_member_changing();
    }
  }
  return ret;
}

int ObPartitionLogService::reset_member_changing()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else {
    WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
    if (!state_mgr_.is_pre_member_changing()) {
      // Maybe this member change failed in the pre-checking stage and did not enter the pre_member_changing state
      // So it is possible to come here
      CLOG_LOG(WARN, "wrong member changing state, maybe mc forward check failed", K(ret), K(partition_key_));
    } else {
      state_mgr_.reset_pre_member_changing();
    }
  }
  return ret;
}

int ObPartitionLogService::wait_log_confirmed(const int64_t begin_time, const int64_t max_wait_time)
{
  int ret = OB_LOG_NOT_SYNC;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else {
    while (OB_LOG_NOT_SYNC == ret) {
      if (sw_.get_next_index_log_id() >= sw_.get_max_log_id() + 1) {
        ret = OB_SUCCESS;
      } else if (ObClockGenerator::getClock() - begin_time >= max_wait_time) {
        break;
      } else {
        usleep(100);
      }
    }
  }
  return ret;
}

int ObPartitionLogService::batch_change_member(const common::ObMember& member, const int64_t quorum,
    const int64_t mc_timestamp, const common::ObProposalID& proposal_id, const bool is_add_member,
    obrpc::ObMCLogInfo& mc_log_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (!member.is_valid() || mc_timestamp <= 0 || !proposal_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(partition_key_), K(member), K(mc_timestamp), K(proposal_id));
  } else {
    WLockGuardWithRetry guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time());
    bool is_standby_op = (STANDBY_LEADER == state_mgr_.get_role() ? true : false);
    if (!state_mgr_.can_change_member()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "can not change member",
          K(ret),
          K(partition_key_),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state(),
          "is_changing_leader",
          state_mgr_.is_changing_leader());
    } else if (mc_timestamp != mm_.get_timestamp()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "version not match",
          K(ret),
          K(partition_key_),
          K(member),
          K(is_add_member),
          K(mc_timestamp),
          "current_timestamp",
          mm_.get_timestamp());
    } else if (!is_standby_op && proposal_id != state_mgr_.get_proposal_id()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "proposal_id not match",
          K(ret),
          K(partition_key_),
          K(proposal_id),
          "current_proposal_id",
          state_mgr_.get_proposal_id());
    } else if (is_standby_op && proposal_id != mm_.get_ms_proposal_id()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "ms_proposal_id not match",
          K(ret),
          K(partition_key_),
          K(proposal_id),
          "current_proposal_id",
          mm_.get_ms_proposal_id());
    } else if (is_add_member) {
      if (OB_FAIL(mm_.add_member(member, quorum, mc_log_info))) {
        CLOG_LOG(WARN, "add_member failed", K(ret), K(partition_key_), K(member), K(quorum));
      }
    } else {
      if (OB_FAIL(mm_.remove_member(member, quorum, mc_log_info))) {
        CLOG_LOG(WARN, "remove_member failed", K(ret), K(partition_key_), K(member), K(quorum));
      }
    }
  }
  return ret;
}

int ObPartitionLogService::get_partition_max_log_id(uint64_t& partition_max_log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "is_not_inited", K(ret), K(partition_key_));
  } else {
    partition_max_log_id = sw_.get_max_log_id();
  }
  return ret;
}

//==================== batch change member end ====================

bool ObPartitionLogService::need_skip_when_check_start_service() const
{
  return (is_paxos_offline_replica_() || is_no_update_next_replay_log_id_info_too_long_() ||
          restore_mgr_.is_archive_restoring());
}

bool ObPartitionLogService::is_no_update_next_replay_log_id_info_too_long_() const
{
  const int64_t TOO_LONG_TIME = 10 * 60 * 1000 * 1000;  // 10 minutes
  const int64_t last_update_next_replay_log_id_info_ts = sw_.get_last_update_next_replay_log_id_info_ts();
  return (last_update_next_replay_log_id_info_ts != OB_INVALID_TIMESTAMP) &&
         (ObTimeUtility::current_time() - last_update_next_replay_log_id_info_ts >= TOO_LONG_TIME);
}

int ObPartitionLogService::leader_update_next_replay_log_ts_(const int64_t new_ts)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_TIMESTAMP == new_ts) {
    ret = OB_INVALID_ARGUMENT;
  } else if (state_mgr_.is_offline() || !state_mgr_.is_leader_active()) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(check_self_is_election_leader_())) {
    CLOG_LOG(WARN, "check_self_is_election_leader_ failed", K_(partition_key), K(ret));
  } else {
    sw_.try_update_next_replay_log_info_on_leader(new_ts);
  }

  return ret;
}

int ObPartitionLogService::leader_keepalive(const int64_t keepalive_interval)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObClockGenerator::getClock();
  bool need_send_max_log_info = false;  // whether leader need send max_log_info to standby cluster

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else {
    // hold rdlock during keepAlive
    RLockGuard guard(lock_);
    if (state_mgr_.is_leader_active() && !state_mgr_.is_offline()) {
      bool is_cluster_status_allow_update = false;
      if (!ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
        if (GCTX.is_primary_cluster() || GCTX.is_in_flashback_state() || GCTX.is_in_cleanup_state()) {
          // non-private leader only in flashback/cleanup state can advance next_log_ts
          // it cannot do this in swithing state, which may lead conflict with new primary cluster
          is_cluster_status_allow_update = true;
        } else {
          is_cluster_status_allow_update = false;
          if (GCTX.is_in_standby_switching_state()) {
            // leader need send max_log info to standby replica during switching state
            need_send_max_log_info = true;
          }
        }
      } else {
        // cluster private table, allow leader update
        is_cluster_status_allow_update = true;
      }

      if (sw_.is_empty() && now - last_leader_keepalive_time_ < keepalive_interval) {
        // When the sliding window is empty, it will be sent every 100ms
      } else if (!sw_.is_empty() &&
                 (ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) ||
                     !GCTX.is_in_standby_switching_state()) &&
                 now - last_leader_keepalive_time_ < 10 * CLOG_LEADER_KEEPALIVE_INTERVAL) {
        // When the sliding window is not empty and it is private table or non-switching state,  it will be sent every
        // 1s
      } else {
        if (is_cluster_status_allow_update) {
          // advance next_log_ts
          const int64_t new_next_log_ts = now;
          if (OB_FAIL(leader_update_next_replay_log_ts_(new_next_log_ts))) {
            CLOG_LOG(WARN, "leader_update_next_replay_log_ts_ failed", K_(partition_key), K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          uint64_t next_log_id = OB_INVALID_ID;
          int64_t next_log_ts = OB_INVALID_TIMESTAMP;
          sw_.get_next_replay_log_id_info(next_log_id, next_log_ts);

          // send to curr_member_list and children_list
          const int64_t self_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
          const common::ObMemberList member_list = mm_.get_curr_member_list();
          const int64_t member_cnt = member_list.get_member_number();
          for (int64_t i = 0; i < member_cnt && OB_SUCC(ret); ++i) {
            common::ObAddr server;
            if (OB_FAIL(member_list.get_server_by_index(i, server))) {
              CLOG_LOG(WARN, "get_server_by_index failed", K(ret), K_(partition_key), K(server));
            } else if (server == self_) {
              // skip self
            } else if (OB_FAIL(log_engine_->send_keepalive_msg(
                           server, self_cluster_id, partition_key_, next_log_id, next_log_ts, 0 /* deliver_cnt */))) {
              CLOG_LOG(WARN, "send_keepalive_msg failed", K(ret), K_(partition_key), K(server));
            } else {
              // do nothing
            }
          }

          if (OB_SUCC(ret)) {
            ObCascadMember sync_child = cascading_mgr_.get_sync_standby_child();
            if (sync_child.is_valid() && OB_FAIL(log_engine_->send_keepalive_msg(sync_child.get_server(),
                                             sync_child.get_cluster_id(),
                                             partition_key_,
                                             next_log_id,
                                             next_log_ts,
                                             0 /* deliver_cnt */))) {
              CLOG_LOG(WARN, "send_keepalive_msg failed", K(ret), K_(partition_key), K(sync_child));
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(send_keepalive_msg_to_children_(next_log_id, next_log_ts, 0))) {
              CLOG_LOG(WARN, "send_keepalive_msg_to_children_ failed", K(ret), K_(partition_key));
            }
          }
        }

        if (OB_SUCC(ret)) {
          last_leader_keepalive_time_ = now;
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_send_max_log_info) {
    // send max_log info to standby replica, which is used to check sync state
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = leader_send_max_log_info())) {
      CLOG_LOG(WARN, "leader_send_max_log_info failed", K_(partition_key), K(tmp_ret));
    }
  }
  return ret;
}

// When primary cluster is switched to the switching state, the leader actively sends relevant information to the
// standby cluster, and the standby replica use this information to check whether the synchronization is completed
int ObPartitionLogService::leader_send_max_log_info()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (!GCTX.is_in_standby_switching_state()) {
    // cluster not in switching state, skip
  } else if (ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    // private table no need execute
  } else {
    bool is_online_leader = false;
    uint64_t max_log_id = OB_INVALID_ID;
    uint64_t next_log_id = OB_INVALID_ID;
    int64_t next_log_ts = OB_INVALID_TIMESTAMP;
    {
      RLockGuard guard(lock_);
      if (!state_mgr_.is_leader_active() || state_mgr_.is_offline()) {
        // self is follwer or offline, skip
        is_online_leader = false;
      } else {
        is_online_leader = true;
      }
    }
    if (OB_SUCC(ret) && is_online_leader) {
      // Add a write lock to obtain timestamp.
      // The write lock can ensure that the value obtained is the last advance value in the switching state.
      // Because pushing the standby machine to read the timestamp is performed under the read lock.
      WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
      sw_.get_next_replay_log_id_info(next_log_id, next_log_ts);
      // The write lock of pls should also be added when obtaining max_log_id, which is used for mutual exclusion with
      // alloc_log_id Add a write lock to ensure that you can get the last allocated log_id
      max_log_id = sw_.get_max_log_id();
    }
    // double check self is leader
    {
      RLockGuard guard(lock_);
      const int64_t switchover_epoch = GCTX.get_pure_switch_epoch();
      if (OB_SUCC(ret) && is_online_leader && (state_mgr_.is_leader_active() && !state_mgr_.is_offline())) {
        if (OB_FAIL(check_self_is_election_leader_())) {
          CLOG_LOG(WARN, "check_self_is_election_leader_ failed", K_(partition_key), K(ret));
        } else if (OB_INVALID_ID == max_log_id) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR, "max_log_id is invalid", K_(partition_key), K(ret), K(max_log_id));
        } else if (OB_FAIL(send_max_log_msg_to_mlist_(switchover_epoch, max_log_id, next_log_ts))) {
          CLOG_LOG(WARN, "send_max_log_msg_to_mlist_ failed", K(ret), K_(partition_key));
        } else if (OB_FAIL(send_max_log_msg_to_children_(switchover_epoch, max_log_id, next_log_ts))) {
          CLOG_LOG(WARN, "send_max_log_msg_to_children_ failed", K(ret), K_(partition_key));
        } else {
          // do nothing
        }
        if (partition_reach_time_interval(5 * 1000 * 1000, leader_max_log_interval_)) {
          CLOG_LOG(INFO,
              "leader_send_max_log_info finished",
              K(ret),
              K_(partition_key),
              K(switchover_epoch),
              K(max_log_id),
              K(next_log_ts));
        }
      }
    }
  }
  return ret;
}

int ObPartitionLogService::broadcast_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "is not inited", K(ret), K(partition_key_));
  } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260 || GCTX.is_primary_cluster()) {
    // Before the 226 version, the standby cluster did not have its own member_list,
    // and the logonly replica of the standby cluster can directly reclaim the logs according to the dump location,
    // so there is no need to broadcast
    // Starting from version 226, the standby cluster has member_list, which can be like the primary cluster
    RLockGuard guard(lock_);
    const ObMemberList member_list = mm_.get_curr_member_list();
    const ObReplicaType replica_type = mm_.get_replica_type();
    const uint64_t max_confirmed_log_id = sw_.get_max_confirmed_log_id();

    if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type) &&
        member_list.is_valid()  // standby replica's member_list maybe invalid
        && OB_FAIL(log_engine_->broadcast_info(member_list, partition_key_, replica_type, max_confirmed_log_id))) {
      CLOG_LOG(WARN,
          "broadcast_info failed",
          K(ret),
          K(partition_key_),
          K(member_list),
          K(replica_type),
          K(max_confirmed_log_id));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionLogService::archive_checkpoint(const int64_t archive_checkpoint_interval)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (OB_SYS_TENANT_ID == partition_key_.get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "sys partitions do not need to archive checkpoint", K(ret), K(partition_key_));
  } else {
    RLockGuard guard(lock_);
    const int64_t now = ObClockGenerator::getClock();
    int64_t last_archive_checkpoint_ts = sw_.get_last_update_archive_checkpoint_time();
    if (((now - last_archive_checkpoint_ts) >= archive_checkpoint_interval) && GCTX.is_primary_cluster() &&
        state_mgr_.is_leader_active() && !state_mgr_.is_offline()) {
      storage::ObIPartitionGroupGuard guard;
      int64_t checkpoint_ts = OB_INVALID_TIMESTAMP;
      // The error is generally that the archive mgr has not been initialized or the sending queue is full.
      // Wait a while and try again to avoid retrying too frequently.
      const int64_t max_delay_interval = 2 * 1000 * 1000LL;
      const int64_t wait_interval = std::min((archive_checkpoint_interval / 4), max_delay_interval);
      int64_t last_checkpoint_ts = sw_.get_last_archive_checkpoint_ts();
      ObPartitionLoopWorker* loop_worker = NULL;
      if ((OB_FAIL(partition_service_->get_partition(partition_key_, guard))) || NULL == guard.get_partition_group()) {
        CLOG_LOG(WARN, "invalid partition", K(ret), KP(partition_service_), K_(partition_key));
      } else if (!(guard.get_partition_group()->is_valid())) {
        ret = OB_INVALID_PARTITION;
        CLOG_LOG(WARN, "partition is invalid", K(ret), K_(partition_key));
      } else if (NULL == (loop_worker = guard.get_partition_group()->get_partition_loop_worker())) {
        ret = OB_INVALID_PARTITION;
        CLOG_LOG(WARN, "partition is invalid", K(ret), K_(partition_key));
      } else if (FALSE_IT(checkpoint_ts = loop_worker->get_last_checkpoint())) {
        CLOG_LOG(WARN, "failed to get_last_checkpoint", K(ret), K_(partition_key));
      } else if (0 >= checkpoint_ts) {
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          CLOG_LOG(WARN, "checkpoint_ts has not been generated", KR(ret), K(partition_key_), K(checkpoint_ts));
        }
        sw_.set_last_update_archive_checkpoint_time(last_archive_checkpoint_ts + wait_interval);
      } else if (checkpoint_ts > last_checkpoint_ts) {
        uint64_t max_log_id = OB_INVALID_ID;
        int64_t max_log_ts = OB_INVALID_TIMESTAMP;
        sw_.get_max_log_id_info(max_log_id, max_log_ts);
        if (sw_.is_empty() && max_log_id == sw_.get_last_archive_checkpoint_log_id()) {
          if (OB_FAIL(archive_mgr_->submit_checkpoint_task(partition_key_, max_log_id, max_log_ts, checkpoint_ts))) {
            if (OB_ENTRY_NOT_EXIST == ret || OB_LOG_ARCHIVE_NOT_RUNNING == ret || OB_EAGAIN == ret) {
              if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
                CLOG_LOG(WARN, "failed to submit_checkpoint_task", KR(ret), K(partition_key_));
              }
            } else {
              CLOG_LOG(WARN, "failed to submit_checkpoint_task", KR(ret), K(partition_key_));
            }
            sw_.set_last_update_archive_checkpoint_time(last_archive_checkpoint_ts + wait_interval);
          } else {
            sw_.set_last_archive_checkpoint_ts(checkpoint_ts);
            sw_.set_last_update_archive_checkpoint_time(now);
          }
        } else {
          // need submit clog checkpoint log
          loop_worker->set_force_write_checkpoint(true);
          sw_.set_last_archive_checkpoint_log_id(max_log_id + 1);
          sw_.set_last_archive_checkpoint_ts(checkpoint_ts);
          sw_.set_last_update_archive_checkpoint_time(now);
        }
      } else {
        if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
          CLOG_LOG(WARN,
              "checkpoint has not been pushed up",
              KR(ret),
              K(partition_key_),
              K(last_checkpoint_ts),
              K(checkpoint_ts));
        }
      }
    } else {
    }
  }
  return ret;
}

int ObPartitionLogService::sync_log_archive_progress()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (OB_SYS_TENANT_ID == partition_key_.get_tenant_id()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "sys partitions do not need to sys log archive progress", K(ret), K(partition_key_));
  } else {
    RLockGuard guard(lock_);
    if (GCTX.is_primary_cluster() && state_mgr_.is_leader_active() && !state_mgr_.is_offline()) {
      bool need_sync = true;
      if (OB_FAIL(sync_local_archive_progress_(need_sync))) {
        CLOG_LOG(WARN, "sync_local_archive_progress_ failed", K(ret), K_(partition_key));
      } else if (need_sync) {
        const int64_t self_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
        const common::ObMemberList member_list = mm_.get_curr_member_list();
        const int64_t member_cnt = member_list.get_member_number();
        for (int64_t i = 0; i < member_cnt && OB_SUCC(ret); ++i) {
          common::ObAddr server;
          if (OB_FAIL(member_list.get_server_by_index(i, server))) {
            CLOG_LOG(WARN, "sync_local_archive_progress_ failed", K(ret), K_(partition_key), K(server));
          } else if (server == self_) {
            // skip self
          } else if (OB_FAIL(log_engine_->send_sync_log_archive_progress_msg(
                         server, self_cluster_id, partition_key_, log_archive_status_))) {
            CLOG_LOG(WARN, "send_sync_log_archive_progress_msg failed", K(ret), K_(partition_key), K(server));
          } else { /*do nothing*/
          }
        }

        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = (send_archive_progress_msg_to_children_(log_archive_status_)))) {
          CLOG_LOG(WARN,
              "failed to send_archive_progress_msg_to_children_",
              K(tmp_ret),
              K_(partition_key),
              K(log_archive_status_));
          ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
        }
      }
    }
  }
  return ret;
}

int ObPartitionLogService::send_archive_progress_msg_to_children_(const ObPGLogArchiveStatus& status) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObCascadMemberList children;
    if (OB_FAIL(cascading_mgr_.get_children_list(children))) {
      CLOG_LOG(WARN, "get_children_list failed", K_(partition_key), K(ret));
    } else {
      const int64_t child_cnt = children.get_member_number();
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < child_cnt; ++i) {
        share::ObCascadMember child;
        if (OB_SUCCESS != (tmp_ret = (children.get_member_by_index(i, child)))) {
          CLOG_LOG(WARN, "get_member_by_index failed", K(tmp_ret), K_(partition_key), K(child));
        } else if (!child.is_valid()) {
          tmp_ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(WARN, "invalid child", K(tmp_ret), K_(partition_key), K(child));
        } else if (child.get_server() == self_) {
          // skip self
        } else if (OB_SUCCESS != (tmp_ret = (log_engine_->send_sync_log_archive_progress_msg(
                                      child.get_server(), child.get_cluster_id(), partition_key_, status)))) {
          CLOG_LOG(WARN, "send_sync_log_archive_progress_msg failed", K(tmp_ret), K_(partition_key), K(child));
        } else {
          // do nothing
        }
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObPartitionLogService::get_log_archive_status(ObPGLogArchiveStatus& status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "is not inited", K(ret), K(partition_key_));
  } else {
    SpinRLockGuard rlock_guard(log_archive_status_rwlock_);
    status = log_archive_status_;
  }
  return ret;
}
int ObPartitionLogService::update_broadcast_info(
    const common::ObAddr& server, const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "is not inited", K(ret), K(partition_key_));
  } else if (NULL == broadcast_info_mgr_) {
    // do nothing
  } else if (OB_FAIL(broadcast_info_mgr_->update_broadcast_info(server, replica_type, max_confirmed_log_id))) {
    CLOG_LOG(WARN,
        "update_broadcast_info failed",
        K(ret),
        K(partition_key_),
        K(server),
        K(replica_type),
        K(max_confirmed_log_id));
  }
  return ret;
}

int ObPartitionLogService::get_recyclable_log_id(uint64_t& log_id) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "is not inited", K(ret), K(partition_key_));
  } else if (NULL == broadcast_info_mgr_) {
    log_id = 0;
  } else if (OB_FAIL(broadcast_info_mgr_->get_recyclable_log_id(log_id))) {
    CLOG_LOG(WARN, "get_recyclable_log_id failed", K(ret), K(partition_key_));
  }
  return ret;
}

int ObPartitionLogService::get_candidate_info(ObCandidateInfo& info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    info.set_role(state_mgr_.get_role());
    info.set_tenant_active(partition_service_->is_tenant_active(partition_key_));
    info.set_on_revoke_blacklist(state_mgr_.is_leader_revoke_recently());
    info.set_replica_type(mm_.get_replica_type());
    info.set_server_status(OBSERVER.get_gctx().rs_server_status_);
    info.set_clog_disk_full(!log_engine_->is_disk_space_enough());
    info.set_offline(state_mgr_.is_offline());
    info.set_need_rebuild(state_mgr_.is_need_rebuild());
    info.set_is_partition_candidate(partition_service_->is_election_candidate(partition_key_));
    bool disk_error = false;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObIOManager::get_instance().is_disk_error(disk_error))) {
      CLOG_LOG(WARN, "is_disk_error failed", K(tmp_ret));
    }
    info.set_is_disk_error(disk_error);
    const ObReplicaProperty replica_property = mm_.get_replica_property();
    info.set_memstore_percent(replica_property.get_memstore_percent());
  }
  return ret;
}

int ObPartitionLogService::enable_replay_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CLOG_LOG(INFO, "enable_replay", K_(partition_key));
    state_mgr_.enable_replay();
  }
  return ret;
}

int ObPartitionLogService::disable_replay_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CLOG_LOG(INFO, "disable_replay", K_(partition_key));
    state_mgr_.disable_replay();
  }
  return ret;
}

int ObPartitionLogService::get_storage_last_replay_log_id_(uint64_t& last_replay_log_id) const
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  storage::ObIPartitionGroup* partition = NULL;
  int64_t unused = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (OB_FAIL(partition_service_->get_partition(partition_key_, guard)) ||
             NULL == (partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "this partition not existed", K(ret), K(partition_key_));
  } else if (OB_FAIL(partition->get_saved_last_log_info(last_replay_log_id, unused))) {
    CLOG_LOG(ERROR, "partition get_last_replay_log_id failed", K(ret), K(partition_key_));
  } else if (OB_INVALID_ID == last_replay_log_id) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "last_replay_log_id is invalid", K(ret), K(partition_key_));
  } else {
    // do nothing
  }
  return ret;
}

int ObPartitionLogService::query_confirmed_log(
    const uint64_t log_id, transaction::ObTransID& trans_id, int64_t& submit_timestamp)
{
  int ret = OB_SUCCESS;

  ObTsWindows unused_windows;
  int64_t leader_epoch1 = 0;
  int64_t leader_epoch2 = 0;
  ObLogEntry log_entry;
  common::ObMemberList member_list;
  ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID);
  ObReadBuf& rbuf = guard.get_read_buf();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(log_id));
  } else if (OB_UNLIKELY(!rbuf.is_valid())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "failed to alloc read_buf", K_(partition_key), K(log_id), K(ret));
  } else if (OB_FAIL(try_get_curr_member_list(member_list))) {
    CLOG_LOG(WARN, "try_get_curr_member_list failed", K(ret), K(partition_key_), K(log_id));
  } else if (OB_FAIL(get_role_unlock(leader_epoch1, unused_windows))) {
    CLOG_LOG(WARN, "get_role failed or not master", K(ret), K(partition_key_), K(log_id));
  } else if (OB_SUCC(get_confirmed_log_from_sw_(log_id, rbuf, log_entry))) {
    uint64_t unused_log_id = OB_INVALID_ID;
    if (OB_FAIL(transaction::ObTransLogParseUtils::parse_redo_prepare_log(
            log_entry, unused_log_id, submit_timestamp, trans_id))) {
      CLOG_LOG(ERROR, "parse_redo_prepare_log failed", K(ret), K(partition_key_), K(log_id));
      ret = OB_ERR_UNEXPECTED;
    } else if (unused_log_id != log_id) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "log_id not match", K(ret), K(partition_key_), K(log_id), K(unused_log_id));
    } else {
      CLOG_LOG(INFO,
          "get_confirmed_log_from_sw_ finished",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(trans_id),
          K(submit_timestamp));
    }
  } else if (OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
    // do nothing
    CLOG_LOG(INFO, "get_confirmed_log_from_sw_ eagain or not exist", K(ret), K(partition_key_), K(log_id));
  } else if (OB_ERROR_OUT_OF_RANGE == ret) {
    if (OB_FAIL(get_log_from_ilog_storage_(log_id, member_list, trans_id, submit_timestamp))) {
      CLOG_LOG(WARN, "get_log_from_ilog_storage_ failed", K(ret), K(partition_key_), K(log_id), K(submit_timestamp));
    } else {
      CLOG_LOG(INFO,
          "get_log_from_ilog_storage_ finished",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(trans_id),
          K(submit_timestamp));
    }
  } else {
    CLOG_LOG(ERROR, "get_confirmed_log_from_sw_ failed", K(ret), K(partition_key_), K(log_id));
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_SUCCESS == ret || OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = get_role_unlock(leader_epoch2, unused_windows))) {
      CLOG_LOG(WARN, "get_role failed or not master", K(ret), K(partition_key_), K(log_id));
      ret = tmp_ret;
    } else if (leader_epoch1 != leader_epoch2) {
      CLOG_LOG(WARN, "leader epoch changed, not master", K(ret), K(partition_key_), K(log_id));
      ret = OB_EAGAIN;
    }
  }

  return ret;
}

int ObPartitionLogService::get_confirmed_log_from_sw_(const uint64_t log_id, ObReadBuf& rbuf, ObLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  const uint32_t log_attr = ObFetchLogAttr::NEED_CONFIRMED;
  bool log_confirmed = false;
  ObLogCursor log_cursor;
  int64_t accum_checksum = 0;
  bool batch_committed = false;
  ObReadParam read_param;

  if (OB_SUCCESS == (ret = sw_.get_log(log_id, log_attr, log_confirmed, log_cursor, accum_checksum, batch_committed))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "get_log return value unexpected", K(ret), K(partition_key_), K(log_id));
  } else if (OB_EAGAIN == ret || OB_ERROR_OUT_OF_RANGE == ret) {
    // do nothing
  } else if (OB_ERR_NULL_VALUE == ret) {
    // When log_id is less than sw max_log_id, it means that this log_id has been allocated, so it needs to return
    // OB_EAGAIN Otherwise, you can safely return to OB_ENTRY_NOT_EXIST;
    if (log_id <= sw_.get_max_log_id()) {
      ret = OB_EAGAIN;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  } else if (OB_LOG_MISSING == ret && log_cursor.is_valid()) {
    read_param.file_id_ = log_cursor.file_id_;
    read_param.offset_ = log_cursor.offset_;
    read_param.read_len_ = log_cursor.size_;
    if (OB_FAIL(log_engine_->read_log_by_location(read_param, rbuf, log_entry))) {
      CLOG_LOG(WARN, "read_log_by_location failed", K(ret), K(partition_key_), K(read_param));
    }
  } else {
    CLOG_LOG(ERROR,
        "sw_ get_log error, unexpected error",
        K(ret),
        K(partition_key_),
        K(log_id),
        K(log_entry),
        K(log_confirmed),
        K(log_cursor),
        K(accum_checksum));
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

int ObPartitionLogService::get_log_from_ilog_storage_(const uint64_t log_id, const common::ObMemberList& member_list,
    transaction::ObTransID& trans_id, int64_t& submit_timestamp)
{
  int ret = OB_SUCCESS;
  ObLogCursorExt log_cursor;
  uint64_t unused_log_id = OB_INVALID_ID;

  if (OB_FAIL(log_engine_->get_cursor(partition_key_, log_id, log_cursor))) {
    if (OB_CURSOR_NOT_EXIST == ret) {
      CLOG_LOG(WARN, "ilog_storage get invalid log, cursor not exist", K(ret), K(partition_key_), K(log_id));
      if (OB_FAIL(remote_log_query_engine_->get_log(partition_key_, log_id, member_list, trans_id, submit_timestamp))) {
        if (OB_EAGAIN != ret) {
          CLOG_LOG(
              WARN, "remote_log_query_engine_ get_log failed", K(ret), K(partition_key_), K(log_id), K(member_list));
        } else if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(
              WARN, "remote_log_query_engine_ get_log failed", K(ret), K(partition_key_), K(log_id), K(member_list));
        }
      } else {
        CLOG_LOG(INFO,
            "remote_log_query_engine_ get_log finished",
            K(ret),
            K(partition_key_),
            K(log_id),
            K(trans_id),
            K(submit_timestamp));
      }
    } else if (OB_NEED_RETRY == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      ret = OB_EAGAIN;
    } else {
      CLOG_LOG(WARN, "ilog_storage_ get_cursor failed", K(ret), K(partition_key_), K(log_id));
    }
  } else {
    ObReadParam read_param;
    read_param.file_id_ = log_cursor.get_file_id();
    read_param.offset_ = log_cursor.get_offset();
    read_param.read_len_ = log_cursor.get_size();
    ObLogEntry log_entry;
    ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_CACHE_ID);
    ObReadBuf& rbuf = guard.get_read_buf();
    if (OB_UNLIKELY(!rbuf.is_valid())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "failed to alloc read_buf", K_(partition_key), K(log_id), K(read_param), K(ret));
    } else if (OB_FAIL(log_engine_->read_log_by_location(read_param, rbuf, log_entry))) {
      CLOG_LOG(WARN, "read_log_by_location failed", K(ret), K(partition_key_), K(read_param));
    } else if (OB_FAIL(transaction::ObTransLogParseUtils::parse_redo_prepare_log(
                   log_entry, unused_log_id, submit_timestamp, trans_id))) {
      CLOG_LOG(ERROR, "parse_redo_prepare_log failed", K(ret), K(partition_key_), K(log_id));
      ret = OB_ERR_UNEXPECTED;
    } else if (unused_log_id != log_id) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "log_id not match", K(ret), K(partition_key_), K(log_id), K(unused_log_id));
    } else {
      CLOG_LOG(DEBUG,
          "get_log_from_ilog_storage finished",
          K(ret),
          K(partition_key_),
          K(log_id),
          K(trans_id),
          K(submit_timestamp));
    }
  }
  return ret;
}

int ObPartitionLogService::add_role_change_task_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (!need_add_event_task()) {
    // do nothing
    CLOG_LOG(TRACE, "no need_add_event_task", K(ret), K(partition_key_), K(has_event_task_));
  } else {
    ObLogStateEventTaskV2* task = NULL;
    while (NULL == (task = alloc_mgr_->alloc_log_event_task())) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(ERROR, "alloc ObLogStateEventTaskV2 failed", K(ret), K(partition_key_));
      }
      usleep(1000);
    }
    if (OB_FAIL(task->init(partition_key_, event_scheduler_, partition_service_))) {
      CLOG_LOG(ERROR, "ObLogStateEventTaskV2 init failed", K(ret), K(partition_key_));
    } else if (OB_FAIL(event_scheduler_->add_state_change_event(task))) {
      CLOG_LOG(WARN, "add_state_change_event failed", K(ret), K(partition_key_));
    }
    if (OB_FAIL(ret)) {
      ob_slice_free_log_event_task(task);
      task = NULL;
    }
  }
  return ret;
}

bool ObPartitionLogService::need_add_event_task()
{
  bool bool_ret = false;
  if (IS_NOT_INIT) {
    bool_ret = false;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited");
  } else if (is_in_partition_service_()) {
    ObSpinLockGuard guard(event_task_lock_);
    if (has_event_task_) {
      bool_ret = false;
    } else {
      bool_ret = true;
      has_event_task_ = true;
    }
  }
  return bool_ret;
}

void ObPartitionLogService::reset_has_event_task()
{
  ObSpinLockGuard guard(event_task_lock_);
  has_event_task_ = false;
}

int ObPartitionLogService::backfill_log(
    const ObLogInfo& log_info, const ObLogCursor& log_cursor, const bool is_leader, ObISubmitLogCb* submit_cb)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_), K(log_info));
  } else if (!log_info.is_valid() || !log_cursor.is_valid() || (is_leader && OB_ISNULL(submit_cb)) ||
             (!is_leader && !OB_ISNULL(submit_cb))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(
        WARN, "invalid arguments", K(ret), K(partition_key_), K(log_info), K(log_cursor), K(is_leader), KP(submit_cb));
  } else if (!is_leader && !sw_.check_can_receive_larger_log(log_info.get_log_id())) {
    ret = OB_TOO_LARGE_LOG_ID;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "backfill_log too large log_id", K(ret), K(partition_key_), K(log_info));
    }
  } else {
    if (NULL != submit_cb) {
      submit_cb->base_class_reset();
    }
    const char* serialize_buff = log_info.get_buf();
    const int64_t serialize_size = log_info.get_size();
    const common::ObProposalID& proposal_id = log_info.get_proposal_id();

    RLockGuard guard(lock_);
    if (!state_mgr_.can_backfill_log(is_leader, proposal_id)) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "can_backfill_log failed",
          K(ret),
          K(partition_key_),
          K(is_leader),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state(),
          K(proposal_id),
          "curr_proposal_id",
          state_mgr_.get_proposal_id());
    } else if (OB_FAIL(sw_.backfill_log(log_info.get_log_id(),
                   proposal_id,
                   serialize_buff,
                   serialize_size,
                   log_cursor,
                   is_leader,
                   submit_cb))) {
      CLOG_LOG(WARN, "sw backfill_log failed", K(ret), K(partition_key_), K(log_info));
    }
    if (is_leader && OB_ALLOCATE_MEMORY_FAILED == ret) {
      int tmp_ret = OB_SUCCESS;
      const uint32_t revoke_type = ObElection::RevokeType::SUBMIT_LOG_MEMORY_ALLOC_FAIL;
      if (OB_SUCCESS != (tmp_ret = election_mgr_->leader_revoke(partition_key_, revoke_type))) {
        CLOG_LOG(ERROR, "election_mgr_ leader_revoke failed", K(ret), K(tmp_ret), K(partition_key_), K(revoke_type));
      }
    }
  }

  return ret;
}

int ObPartitionLogService::backfill_confirmed(const ObLogInfo& log_info, const bool batch_first_participant)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_), K(log_info));
  } else if (!log_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(log_info));
  } else {
    const uint64_t log_id = log_info.get_log_id();
    const common::ObProposalID proposal_id = log_info.get_proposal_id();

    RLockGuard guard(lock_);
    if (!state_mgr_.can_backfill_confirmed(proposal_id)) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "can_backfill_confirmed failed",
          K(ret),
          K(partition_key_),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state(),
          K(proposal_id),
          "curr_proposal_id",
          state_mgr_.get_proposal_id());
    } else if (OB_FAIL(sw_.backfill_confirmed(log_id, batch_first_participant))) {
      CLOG_LOG(WARN, "sw backfill_confirmed failed", K(ret), K(partition_key_), K(log_id));
    }
  }

  return ret;
}

int ObPartitionLogService::resubmit_log(const ObLogInfo& log_info, ObISubmitLogCb* submit_cb)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_), K(log_info));
  } else if (OB_ISNULL(submit_cb) || !log_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(log_info), KP(submit_cb));
  } else {
    if (NULL != submit_cb) {
      submit_cb->base_class_reset();
    }
    const common::ObProposalID proposal_id = log_info.get_proposal_id();

    RLockGuard guard(lock_);
    if (!state_mgr_.can_submit_with_assigned_id()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "can not submit log for assigned log_id",
          K(ret),
          K(partition_key_),
          "role",
          state_mgr_.get_role(),
          "is_offline",
          state_mgr_.is_offline(),
          "state",
          state_mgr_.get_state(),
          K(log_info));
    } else if (proposal_id != state_mgr_.get_proposal_id()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "proposal_id not match",
          K(ret),
          K(partition_key_),
          K(proposal_id),
          "curr_proposal_id",
          state_mgr_.get_proposal_id(),
          K(log_info));
    } else if (OB_FAIL(sw_.resubmit_log(log_info, submit_cb))) {
      CLOG_LOG(ERROR, "resubmit_log failed", K(ret), K(partition_key_), K(log_info));
    }

    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      int tmp_ret = OB_SUCCESS;
      const uint32_t revoke_type = ObElection::RevokeType::SUBMIT_LOG_MEMORY_ALLOC_FAIL;
      if (OB_SUCCESS != (tmp_ret = election_mgr_->leader_revoke(partition_key_, revoke_type))) {
        CLOG_LOG(ERROR, "election_mgr_ leader_revoke failed", K(ret), K(tmp_ret), K(partition_key_), K(revoke_type));
      }
    }
  }

  return ret;
}

int ObPartitionLogService::backfill_nop_log(const ObLogMeta& log_meta)
{
  int ret = OB_SUCCESS;

  static const int64_t BUF_SIZE = 2048;
  ObNopLog nop_log;
  char body_buffer[BUF_SIZE];
  int64_t body_pos = 0;
  common::ObVersion unused_version = ObVersion(1, 0);
  ObISubmitLogCb* submit_cb = NULL;
  ObLogEntryHeader header;
  const bool is_trans_log = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret));
  } else if (!log_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(log_meta));
  } else if (lock_.try_rdlock()) {
    if (!state_mgr_.can_submit_with_assigned_id()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "can not submit log for assigned log_id",
          K(ret),
          K(partition_key_),
          "role",
          state_mgr_.get_role(),
          "is_offline",
          state_mgr_.is_offline(),
          "state",
          state_mgr_.get_state(),
          K(log_meta));
    } else if (log_meta.get_proposal_id() != state_mgr_.get_proposal_id()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "proposal_id not match",
          K(ret),
          K(partition_key_),
          "curr_proposal_id",
          state_mgr_.get_proposal_id(),
          K(log_meta));
    } else if (OB_FAIL(nop_log.serialize(body_buffer, BUF_SIZE, body_pos))) {
      CLOG_LOG(WARN, "serialize failed", K(ret), K(partition_key_));
    } else if (OB_FAIL(header.generate_header(OB_LOG_NOP,  // log_type
                   partition_key_,                         // partition_key
                   log_meta.get_log_id(),
                   body_buffer,                      // buff
                   body_pos,                         // data_len
                   ObClockGenerator::getClock(),     // generation_timestamp
                   log_meta.get_proposal_id().ts_,   // epoch_id,
                   log_meta.get_proposal_id(),       // proposal_id,
                   log_meta.get_submit_timestamp(),  // submit_timestamp
                   unused_version,                   // ObVersion
                   is_trans_log))) {
      CLOG_LOG(ERROR, "generate_header failed", K(ret), K(partition_key_));
    } else if (OB_FAIL(sw_.submit_log(header, body_buffer, submit_cb))) {
      CLOG_LOG(WARN, "submit_log failed", K(ret), K(partition_key_));
    }

    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      int tmp_ret = OB_SUCCESS;
      const uint32_t revoke_type = ObElection::RevokeType::SUBMIT_LOG_MEMORY_ALLOC_FAIL;
      if (OB_SUCCESS != (tmp_ret = election_mgr_->leader_revoke(partition_key_, revoke_type))) {
        CLOG_LOG(ERROR, "election_mgr_ leader_revoke failed", K(ret), K(tmp_ret), K(partition_key_), K(revoke_type));
      }
    }
    lock_.rdunlock();
  } else {
    ret = OB_EAGAIN;
  }

  return ret;
}

int ObPartitionLogService::is_valid_member(const common::ObAddr& addr, bool& is_valid) const
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (!addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(partition_key_), K(addr));
  } else if (!state_mgr_.is_leader_active() && !state_mgr_.is_standby_leader_active()) {
    ret = OB_NOT_MASTER;
  } else {
    is_valid = mm_.get_curr_member_list().contains(addr);
  }

  return ret;
}

int ObPartitionLogService::change_quorum(const common::ObMemberList& curr_member_list, const int64_t curr_quorum,
    const int64_t new_quorum, obrpc::ObMCLogInfo& log_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (!curr_member_list.is_valid() || curr_quorum <= 0 || curr_quorum > OB_MAX_MEMBER_NUMBER ||
             new_quorum <= 0 || new_quorum > OB_MAX_MEMBER_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(curr_member_list), K(curr_quorum), K(new_quorum));
  } else {
    WLockGuardWithTimeout guard(lock_, WRLOCK_TIMEOUT_US + ObTimeUtility::current_time(), ret);
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "change quorum wrlock failed", K(ret), K(partition_key_));
    } else if (!state_mgr_.can_change_member()) {
      CLOG_LOG(WARN,
          "can not change_member",
          K(ret),
          K(partition_key_),
          "role",
          state_mgr_.get_role(),
          "state",
          state_mgr_.get_state(),
          "is_changing_leader",
          state_mgr_.is_changing_leader());
    } else if (OB_FAIL(mm_.change_quorum(curr_member_list, curr_quorum, new_quorum, log_info))) {
      CLOG_LOG(
          WARN, "change quorum failed", K(ret), K(partition_key_), K(curr_member_list), K(curr_quorum), K(new_quorum));
    }
  }
  CLOG_LOG(INFO, "change_quorum finished", K(ret), K(partition_key_), K(curr_quorum), K(new_quorum), K(log_info));
  return ret;
}

int ObPartitionLogService::check_can_receive_batch_log(const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObPartitionLogService is not inited", K(ret), K(partition_key_));
  } else if (OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(log_id));
  } else if (!sw_.check_can_receive_larger_log(log_id)) {
    ret = OB_TOO_LARGE_LOG_ID;
  } else {
    // do nothing
  }
  return ret;
}

bool ObPartitionLogService::is_in_partition_service_() const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;

  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;

  if (OB_FAIL(partition_service_->get_partition(partition_key_, guard))) {
    CLOG_LOG(WARN, "get_partition failed", K(ret), K(partition_key_));
  } else if (NULL == guard.get_partition_group()) {
    ret = OB_PARTITION_NOT_EXIST;
    CLOG_LOG(WARN, "partition not exist", K(ret), K(partition_key_));
  } else if (NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "get partition log service error", K(ret), K(partition_key_));
  } else {
    bool_ret = true;
  }

  return bool_ret;
}

int ObPartitionLogService::update_max_flushed_ilog_id(const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObPartitionLogService is not inited", K(ret));
  } else if (!is_valid_log_id(log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(partition_key_), K(log_id));
  } else {
    ATOMIC_STORE(&max_flushed_ilog_id_, log_id);
  }
  return ret;
}

int ObPartitionLogService::get_cursor_with_cache(
    const int64_t scan_thread_index, const uint64_t log_id, ObLogCursorExt& log_cursor)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  ObCursorArrayCache* const c = cursor_array_cache_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObPartitionLogService is not inited", K(ret));
  } else if (!is_valid_log_id(log_id) || scan_thread_index < 0 || scan_thread_index >= SCAN_THREAD_CNT) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(scan_thread_index), K(log_id));
  } else if (OB_ISNULL(cursor_array_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "cursor_array_cache_ is NULL, unexpected", K(ret), K(partition_key_));
  } else if (log_id >= c->cursor_start_log_id_[scan_thread_index] &&
             log_id < c->cursor_end_log_id_[scan_thread_index]) {
    log_cursor = c->cursor_array_[scan_thread_index][log_id - c->cursor_start_log_id_[scan_thread_index]];
  } else {
    ObGetCursorResult result;
    result.csr_arr_ = c->cursor_array_[scan_thread_index];
    result.arr_len_ = ObCursorArrayCache::CURSOR_ARRAY_SIZE;
    result.ret_len_ = 0;
    if (OB_FAIL(log_engine_->get_cursor_batch_from_file(partition_key_, log_id, result))) {
      CSR_LOG(ERROR, "get_cursor_batch_from_file failed", K(ret), K(partition_key_), K(log_id), K(result));
    } else {
      c->cursor_start_log_id_[scan_thread_index] = log_id;
      c->cursor_end_log_id_[scan_thread_index] = log_id + result.ret_len_;
      log_cursor = c->cursor_array_[scan_thread_index][log_id - c->cursor_start_log_id_[scan_thread_index]];
    }
  }

  return ret;
}

int ObPartitionLogService::reset_has_pop_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObPartitionLogService is not inited", K(ret));
  } else {
    sw_.reset_has_pop_task();
  }
  return ret;
}

int ObPartitionLogService::process_check_rebuild_req(
    const common::ObAddr& server, const uint64_t start_log_id, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!server.is_valid()) || OB_UNLIKELY(start_log_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(server), K(start_log_id));
  } else if (server == self_) {
    // ignore self_
  } else {
    if (start_log_id > sw_.get_max_log_id()) {
      CLOG_LOG(INFO,
          "wanted log are not supplied in this server",
          K(start_log_id),
          "sw_max_log_id",
          sw_.get_max_log_id(),
          K(server),
          K_(partition_key));
    } else if (check_if_need_rebuild_(start_log_id, OB_FETCH_LOG_FOLLOWER_ACTIVE)) {
      bool is_in_member_list = mm_.get_curr_member_list().contains(server);
      CLOG_LOG(INFO,
          "follower try to fetch too old log, need rebuild",
          K_(partition_key),
          K(server),
          K(start_log_id),
          K(is_in_member_list));
      (void)log_engine_->notify_follower_log_missing(
          server, cluster_id, partition_key_, start_log_id, is_in_member_list, OB_REBUILD_MSG);
    } else {
      // do nothing
    }
  }
  return ret;
}

void ObPartitionLogService::get_max_majority_log(uint64_t& log_id, int64_t& log_ts) const
{
  sw_.get_max_majority_log(log_id, log_ts);
}

uint64_t ObPartitionLogService::get_max_confirmed_log_id() const
{
  return sw_.get_max_confirmed_log_id();
}

int ObPartitionLogService::block_partition_split_()
{
  int ret = OB_SUCCESS;

  storage::ObIPartitionGroupGuard guard;
  if (OB_FAIL(partition_service_->get_partition(partition_key_, guard))) {
    CLOG_LOG(WARN, "invalid partition", K(ret), K(partition_service_), K_(partition_key));
  } else if ((NULL == guard.get_partition_group()) || (!(guard.get_partition_group()->is_valid()))) {
    ret = OB_INVALID_PARTITION;
    CLOG_LOG(WARN, "partition is invalid", K(ret), K_(partition_key));
  } else if (OB_FAIL(guard.get_partition_group()->block_partition_split_by_mc())) {
    CLOG_LOG(WARN, "block partiton split failed", K(ret), K_(partition_key));
  } else {
    CLOG_LOG(INFO, "block partition split success", K_(partition_key));
  }

  return ret;
}

int ObPartitionLogService::unblock_partition_split_()
{
  int ret = OB_SUCCESS;

  storage::ObIPartitionGroupGuard guard;
  if (OB_FAIL(partition_service_->get_partition(partition_key_, guard))) {
    CLOG_LOG(WARN, "get partition failed", K(ret), K_(partition_key));
  } else if (NULL == guard.get_partition_group()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "partition is null", K(ret), K_(partition_key));
  } else if (OB_FAIL(guard.get_partition_group()->unblock_partition_split_by_mc())) {
    CLOG_LOG(WARN, "unblock partition split failed", K(ret), K_(partition_key));
  } else {
    CLOG_LOG(INFO, "unblock partition split success", K_(partition_key));
  }
  return ret;
}

int ObPartitionLogService::set_archive_restore_state(const int16_t archive_restore_state)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = restore_mgr_.set_archive_restore_state(archive_restore_state);
  }
  return ret;
}

int ObPartitionLogService::sync_local_archive_progress_(bool& need_sync)
{
  int ret = OB_SUCCESS;
  ObPGLogArchiveStatus status;
  common::ObRole role;
  int64_t archive_epoch = OB_INVALID_TIMESTAMP;
  int64_t leader_epoch = OB_INVALID_TIMESTAMP;
  int64_t unused_takeover_time = OB_INVALID_ARGUMENT;
  need_sync = false;
  if (OB_ISNULL(archive_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "archive_mgr is null", KR(ret), K_(partition_key));
  } else if (OB_FAIL(archive_mgr_->get_log_archive_status(partition_key_, status, archive_epoch))) {
    CLOG_LOG(WARN, "failed to get_log_archive_status", KR(ret), K_(partition_key));
  } else if (share::ObLogArchiveStatus::INVALID == status.status_) {
    // do nothing when returned status.status is invalid
  } else if (OB_FAIL(get_role_and_leader_epoch_unlock_(role, leader_epoch, unused_takeover_time))) {
    CLOG_LOG(WARN, "failed to get_role_and_leader_epoch", KR(ret), K_(partition_key), K(status), K(archive_epoch));
  } else if ((!is_strong_leader(role) || archive_epoch != leader_epoch) && !status.is_stopped() &&
             !status.is_interrupted()) {
    // skip it
  } else {
    SpinWLockGuard wlock_guard(log_archive_status_rwlock_);
    if (log_archive_status_ < status) {
      if (status.is_stopped() || status.is_interrupted()) {
        // If the synchronized state is stopped or interrupted, only incarnation, round, status are updated
        log_archive_status_.archive_incarnation_ = status.archive_incarnation_;
        log_archive_status_.log_archive_round_ = status.log_archive_round_;
        log_archive_status_.status_ = status.status_;
      } else {
        log_archive_status_ = status;
      }
    }
    need_sync = true;
  }
  return ret;
}

bool ObPartitionLogService::is_archive_restoring() const
{
  int bool_ret = false;
  if (IS_NOT_INIT) {
  } else {
    bool_ret = restore_mgr_.is_archive_restoring();
  }
  return bool_ret;
}

common::ObAddr ObPartitionLogService::get_restore_leader() const
{
  common::ObAddr restore_leader;
  if (IS_NOT_INIT) {
  } else {
    restore_leader = restore_mgr_.get_restore_leader();
  }
  return restore_leader;
}

int ObPartitionLogService::set_restore_flag_after_restore_log_()
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  uint64_t last_restore_log_id = get_start_log_id_unsafe() - 1;
  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid partition_service_ is NULL", KR(ret), KP(partition_service_), K_(partition_key));
  } else if (OB_FAIL(partition_service_->get_partition(partition_key_, guard))) {
    CLOG_LOG(WARN, "failed to get_partition", KR(ret), KP(partition_service_), K_(partition_key));
  } else if (OB_ISNULL(partition = guard.get_partition_group()) || (!partition->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid partition", K(ret), K_(partition_key));
  } else if (OB_FAIL(partition->clear_trans_after_restore_log(last_restore_log_id))) {
    CLOG_LOG(ERROR, "failed to clear_trans_after_restore_log", K(ret), K_(partition_key), K(last_restore_log_id));
  } else {
    ObReplicaRestoreStatus flag = ObReplicaRestoreStatus::REPLICA_RESTORE_DUMP_MEMTABLE;
    if (OB_FAIL(partition_service_->set_restore_flag(partition_key_, flag))) {
      CLOG_LOG(WARN, "failed to set_restore_flag", K_(partition_key), K(flag), K(last_restore_log_id), KR(ret));
    } else {
      CLOG_LOG(INFO, "success to set_restore_flag", K_(partition_key), K(flag), K(last_restore_log_id), KR(ret));
    }
  }
  return ret;
}

int ObPartitionLogService::restore_leader_try_confirm_log()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    if (RESTORE_LEADER == restore_mgr_.get_role() && restore_mgr_.is_archive_restoring() &&
        restore_mgr_.is_restore_log_finished()) {
      (void)sw_.restore_leader_try_confirm_log();
    }
  }
  return ret;
}

bool ObPartitionLogService::is_standby_restore_state() const
{
  int bool_ret = false;
  if (IS_NOT_INIT) {
  } else {
    RLockGuard guard(lock_);
    bool_ret = restore_mgr_.is_standby_restore_state();
  }
  return bool_ret;
}

int ObPartitionLogService::check_and_try_leader_revoke(const ObElection::RevokeType& revoke_type)
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  bool need_revoke = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObPartitionLogService not inited", K(ret), K(partition_key_));
  } else if (FOLLOWER == state_mgr_.get_role()) {
    // do nothing
  } else {
    // TODO:consider more suitation
    bool majority_is_clog_disk_full = false;
    if (OB_FAIL(check_majority_replica_clog_disk_full_(majority_is_clog_disk_full))) {
      CLOG_LOG(ERROR, "check_majority_replica_clog_disk_full_ failed", K(ret));
    } else {
      need_revoke = !majority_is_clog_disk_full;
      CLOG_LOG(INFO, "partition may need revoke, ", "need revoke is ", need_revoke, "and revoke type is ", revoke_type);
    }
  }

  if (need_revoke && OB_FAIL(election_mgr_->leader_revoke(partition_key_, revoke_type))) {
    CLOG_LOG(ERROR, "election_mgr_ leader_revoke failed", K(ret), K(partition_key_));
  }
  return ret;
}

int ObPartitionLogService::alloc_cursor_array_cache_(const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;

  if (NULL != cursor_array_cache_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "cursor_array_cache_ is not NULL", K(ret), K(partition_key));
  } else if (NULL == (cursor_array_cache_ = (ObCursorArrayCache*)ob_malloc(
                          sizeof(ObCursorArrayCache), ObModIds::OB_PARTITION_LOG_SERVICE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "alloc ObCursorArrayCache failed", K(ret), K(partition_key));
  } else {
    cursor_array_cache_ = new (cursor_array_cache_) ObCursorArrayCache();

    if (OB_FAIL(cursor_array_cache_->init())) {
      CLOG_LOG(ERROR, "cursor_array_cache_ init failed", K(ret), K(partition_key));
      free_cursor_array_cache_();
    }
  }

  return ret;
}

void ObPartitionLogService::free_cursor_array_cache_()
{
  if (cursor_array_cache_ != NULL) {
    cursor_array_cache_->~ObCursorArrayCache();
    ob_free(cursor_array_cache_);
    cursor_array_cache_ = NULL;
  }
}

int ObPartitionLogService::alloc_broadcast_info_mgr_(const common::ObPartitionKey& partition_key)
{
  int ret = OB_SUCCESS;

  if (NULL != broadcast_info_mgr_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "broadcast_info_mgr_ is not NULL", K(ret), K(partition_key));
  } else if (NULL == (broadcast_info_mgr_ = (ObLogBroadcastInfoMgr*)ob_malloc(
                          sizeof(ObLogBroadcastInfoMgr), ObModIds::OB_PARTITION_LOG_SERVICE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "alloc ObLogBroadcastInfoMgr failed", K(ret), K(partition_key));
  } else {
    broadcast_info_mgr_ = new (broadcast_info_mgr_) ObLogBroadcastInfoMgr();

    if (OB_FAIL(broadcast_info_mgr_->init(partition_key, &mm_))) {
      CLOG_LOG(ERROR, "broadcast_info_mgr_ init failed", K(ret), K(partition_key));
      free_broadcast_info_mgr_();
    }
  }

  return ret;
}

void ObPartitionLogService::free_broadcast_info_mgr_()
{
  if (broadcast_info_mgr_ != NULL) {
    broadcast_info_mgr_->~ObLogBroadcastInfoMgr();
    ob_free(broadcast_info_mgr_);
    broadcast_info_mgr_ = NULL;
  }
}

int ObPartitionLogService::check_majority_replica_clog_disk_full_(bool& majority_is_clog_disk_full)
{
  int ret = OB_SUCCESS;
  const common::ObMemberList& member_list = mm_.get_curr_member_list();
  const int64_t member_number = member_list.get_member_number();
  const int64_t replica_num = mm_.get_replica_num();
  int64_t disk_full_cnt = 0;
  majority_is_clog_disk_full = false;
  for (int64_t i = 0; i < member_number && OB_SUCC(ret); i++) {
    common::ObAddr server;
    if (OB_FAIL(member_list.get_server_by_index(i, server))) {
      CLOG_LOG(ERROR, "get_server_by_index failed", K(ret), K(i), K(member_number));
    } else {
      ObCascadMember member(server, obrpc::ObRpcNetHandler::CLUSTER_ID);
      if (share::ObServerBlacklist::get_instance().is_clog_disk_full(member)) {
        disk_full_cnt++;
      }
    }
  }
  majority_is_clog_disk_full = (disk_full_cnt >= replica_num / 2 + 1);
  return ret;
}

// There are 3 conditions to judge if memory reached limit.
// 1. tenant's memory is not enough.
// 2. memory store's memory is not enough.
// 3. clog's memory is not enough.
// This function return true if any of conditions above is true.
bool ObPartitionLogService::is_tenant_out_of_memory_() const
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  const uint64_t tenant_id = partition_key_.get_tenant_id();
  // expect at least 2MB memory for every reconfirm log
  constexpr int64_t NEED_MEMORY = RECONFIRM_LOG_ARRAY_LENGTH * OB_MAX_LOG_BUFFER_SIZE;
  constexpr double MAX_PERCENTAGE = 0.99;

  if (IS_NOT_INIT) {
    // do nothing
  } else {
    // 1. check if tenant's memory is not enough
    lib::ObTenantResourceMgrHandle resource_handle;
    if (OB_FAIL(lib::ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id, resource_handle))) {
      CLOG_LOG(ERROR, "get_tenant_resource_mgr failed", KR(ret), K_(partition_key), K(tenant_id));
    } else if (!resource_handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "resource_handle is invalid", K_(partition_key), K(tenant_id));
    } else {
      const int64_t limit = resource_handle.get_memory_mgr()->get_limit();
      const int64_t hold = resource_handle.get_memory_mgr()->get_sum_hold();
      if ((double(hold) / limit) > MAX_PERCENTAGE && limit - hold < NEED_MEMORY) {
        bool_ret = true;
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(INFO, "tenant's memory not enough", K(limit), K(hold), K_(partition_key), K(tenant_id));
        }
      } else {
        // do nothing
      }
    }

    // 2. check if memory store's memory is not enough
    if (false == bool_ret) {
      bool is_tenant_out_of_mem = false;
      if (OB_FAIL(replay_engine_->is_tenant_out_of_memory(partition_key_, is_tenant_out_of_mem))) {
        CLOG_LOG(WARN, "is_tenant_out_of_memory failed", K(ret), K_(partition_key));
      } else if (true == is_tenant_out_of_mem) {
        bool_ret = is_tenant_out_of_mem;
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(INFO, "memory store's memory not enough", K_(partition_key), K(tenant_id));
        }
      } else {
        // do nothing
      }
    }

    // 3. check if clog's memory is not enough
    if (false == bool_ret) {
      if (nullptr != alloc_mgr_) {
        const int64_t limit = alloc_mgr_->get_clog_blk_alloc_mgr().limit();
        const int64_t hold = alloc_mgr_->get_clog_blk_alloc_mgr().hold();
        if ((double(hold) / limit) > MAX_PERCENTAGE && limit - hold < NEED_MEMORY) {
          bool_ret = true;
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            CLOG_LOG(INFO, "clog's memory not enough", K(limit), K(hold), K_(partition_key), K(tenant_id));
          }
        } else {
          // do nothing
        }
      }
    }
  }

  return bool_ret;
}

int ObPartitionLogService::get_role_and_leader_epoch_unlock_(
    common::ObRole& role, int64_t& leader_epoch, int64_t& takeover_time)
{
  int ret = OB_SUCCESS;
  role = FOLLOWER;
  int64_t unused_epoch = 0;
  ObTsWindows unused_windows;
  if (OB_SUCC(get_role_unlock(unused_epoch, unused_windows))) {
    role = state_mgr_.get_role();
  } else if (OB_NOT_MASTER == ret) {
    ret = OB_SUCCESS;
  } else {
  }
  leader_epoch = state_mgr_.get_leader_epoch();
  takeover_time = state_mgr_.get_last_leader_active_time();

  return ret;
}

}  // namespace clog
}  // namespace oceanbase
