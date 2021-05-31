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

#include "ob_log_state_mgr.h"
#include "ob_i_log_engine.h"
#include "ob_log_replay_engine_wrapper.h"
#include "ob_log_callback_engine.h"
#include "ob_log_entry.h"
#include "ob_log_flush_task.h"
#include "ob_log_membership_mgr_V2.h"
#include "ob_log_reconfirm.h"
#include "ob_log_sliding_window.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "storage/ob_replay_status.h"
#include "storage/ob_partition_service.h"
#include "share/ob_i_ps_cb.h"
#include "share/ob_multi_cluster_util.h"
#include "election/ob_election.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/random/ob_random.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase {
using namespace common;
using namespace election;
using namespace share;

namespace clog {
ObLogStateMgr::ObLogStateMgr()
    : sw_(NULL),
      reconfirm_(NULL),
      log_engine_(NULL),
      mm_(NULL),
      cascading_mgr_(NULL),
      replay_engine_(NULL),
      partition_service_(NULL),
      election_(NULL),
      alloc_mgr_(NULL),
      scan_disk_log_finished_(false),
      election_started_(false),
      partition_key_(),
      tmp_val_(0),
      curr_proposal_id_(),
      leader_(),
      previous_leader_(),
      primary_leader_(),
      prepared_primary_leader_(),
      region_(DEFAULT_REGION_NAME),
      leader_epoch_(OB_INVALID_TIMESTAMP),
      self_(),
      lock_(ObLatchIds::CLOG_STAT_MGR_LOCK),
      region_lock_(ObLatchIds::CLOG_LOCALITY_LOCK),
      idc_lock_(ObLatchIds::CLOG_IDC_LOCK),
      freeze_version_(),
      start_role_change_time_(OB_INVALID_TIMESTAMP),
      last_check_start_id_(OB_INVALID_ID),
      last_check_start_id_time_(OB_INVALID_TIMESTAMP),
      // FIXME(): Need to define a separate id
      fetch_state_lock_(ObLatchIds::CLOG_STAT_MGR_LOCK),
      curr_fetch_log_interval_(CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND),
      curr_round_first_fetch_(true),
      curr_round_fetch_begin_time_(OB_INVALID_TIMESTAMP),
      last_fetched_max_log_id_(OB_INVALID_ID),
      curr_fetch_window_size_(1),
      last_wait_standby_ack_start_time_(OB_INVALID_TIMESTAMP),
      last_check_pending_replay_cnt_(0),
      last_check_pending_replay_cnt_time_(OB_INVALID_TIMESTAMP),
      last_fake_push_time_(OB_INVALID_TIMESTAMP),
      last_leader_active_time_(OB_INVALID_TIMESTAMP),
      is_offline_(false),
      replay_enabled_(false),
      is_changing_leader_(false),
      role_change_time_(OB_INVALID_TIMESTAMP),
      send_log_ack_warn_time_(OB_INVALID_TIMESTAMP),
      receive_log_ack_warn_time_(OB_INVALID_TIMESTAMP),
      switch_state_warn_time_(OB_INVALID_TIMESTAMP),
      leader_sync_timeout_warn_time_(OB_INVALID_TIMESTAMP),
      reconfirm_start_time_(OB_INVALID_TIMESTAMP),
      on_leader_revoke_start_time_(OB_INVALID_TIMESTAMP),
      on_leader_takeover_start_time_(OB_INVALID_TIMESTAMP),
      last_renew_loc_time_(OB_INVALID_TIMESTAMP),
      last_renew_primary_leader_time_(OB_INVALID_TIMESTAMP),
      last_sync_start_id_request_time_(OB_INVALID_TIMESTAMP),
      standby_sync_lock_(),
      standby_sync_start_id_(OB_INVALID_ID),
      standby_protection_level_(MAXIMUM_PERFORMANCE_LEVEL),
      is_pre_member_changing_(false),
      is_new_created_leader_(false),
      need_rebuild_(false),
      is_elected_by_changing_leader_(false),
      is_restoring_(false),
      is_write_disabled_(false),
      pls_epoch_(OB_INVALID_TIMESTAMP),
      is_inited_(false)
{}

int ObLogStateMgr::init(ObILogSWForStateMgr* sw, ObILogReconfirm* reconfirm, ObILogEngine* log_engine,
    ObILogMembershipMgr* mm, ObLogCascadingMgr* cascading_mgr, ObLogRestoreMgr* restore_mgr,
    election::ObIElection* election, ObLogReplayEngineWrapper* replay_engine,
    storage::ObPartitionService* partition_service, common::ObILogAllocator* alloc_mgr, const common::ObAddr& self,
    const common::ObProposalID& proposal_id, const common::ObVersion& freeze_version,
    const common::ObPartitionKey& partition_key, const ObCreatePlsType& create_pls_type)
{
  int ret = OB_SUCCESS;
  if (NULL == sw || NULL == reconfirm || NULL == log_engine || NULL == mm || NULL == cascading_mgr ||
      NULL == restore_mgr || NULL == election || NULL == replay_engine || NULL == partition_service ||
      NULL == alloc_mgr || (!self.is_valid()) || (!partition_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(ERROR, "init twice", K(ret), K_(partition_key));
  } else {
    sw_ = sw;
    reconfirm_ = reconfirm;
    log_engine_ = log_engine;
    mm_ = mm;
    cascading_mgr_ = cascading_mgr;
    restore_mgr_ = restore_mgr;
    replay_engine_ = replay_engine;
    partition_service_ = partition_service;
    election_ = election;
    alloc_mgr_ = alloc_mgr;
    scan_disk_log_finished_ = false;
    election_started_ = false;
    partition_key_ = partition_key;
    role_ = FOLLOWER;
    state_ = REPLAY;
    curr_proposal_id_ = proposal_id;
    leader_.reset();
    previous_leader_.reset();
    primary_leader_.reset();
    prepared_primary_leader_.reset();
    region_ = DEFAULT_REGION_NAME;
    leader_epoch_ = OB_INVALID_TIMESTAMP;
    self_ = self;
    freeze_version_ = freeze_version;
    start_role_change_time_ = OB_INVALID_TIMESTAMP;
    last_check_start_id_ = sw_->get_start_id();
    last_check_start_id_time_ = ObClockGenerator::getClock();
    curr_fetch_log_interval_ = CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND;
    curr_round_first_fetch_ = true;
    curr_round_fetch_begin_time_ = OB_INVALID_TIMESTAMP;
    last_fetched_max_log_id_ = OB_INVALID_ID;
    curr_fetch_window_size_ = 1;
    last_wait_standby_ack_start_time_ = OB_INVALID_TIMESTAMP;
    last_check_pending_replay_cnt_ = 0;
    last_check_pending_replay_cnt_time_ = OB_INVALID_TIMESTAMP;
    last_fake_push_time_ = OB_INVALID_TIMESTAMP;
    last_leader_active_time_ = OB_INVALID_TIMESTAMP;
    is_offline_ = false;
    replay_enabled_ = true;
    is_changing_leader_ = false;
    role_change_time_ = OB_INVALID_TIMESTAMP;
    is_removed_ = false;
    need_rebuild_ = false;
    is_restoring_ = false;
    is_write_disabled_ = false;
    pls_epoch_ = ObTimeUtility::current_monotonic_time();
    create_pls_type_ = create_pls_type;
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

bool ObLogStateMgr::is_leader_taking_over_() const
{
  return (LEADER == role_ || STANDBY_LEADER == role_) && (TAKING_OVER == state_);
}

bool ObLogStateMgr::is_leader_reconfirm_() const
{
  return (LEADER == role_) && (RECONFIRM == state_);
}

bool ObLogStateMgr::is_standby_leader_reconfirm_() const
{
  return (STANDBY_LEADER == role_) && (RECONFIRM == state_);
}

bool ObLogStateMgr::is_follower_replay_() const
{
  return (FOLLOWER == role_) && (REPLAY == state_);
}

bool ObLogStateMgr::is_leader_revoking_() const
{
  return (LEADER == role_) && (REVOKING == state_);
}

bool ObLogStateMgr::is_standby_leader_revoking_() const
{
  return (STANDBY_LEADER == role_) && (REVOKING == state_);
}

bool ObLogStateMgr::is_follower_active_() const
{
  return (FOLLOWER == role_) && (ACTIVE == state_);
}

bool ObLogStateMgr::is_interim_state_() const
{
  return (is_leader_reconfirm_() || is_leader_taking_over_() || is_leader_revoking_() ||
          is_standby_leader_revoking_() || is_standby_leader_reconfirm_());
}

bool ObLogStateMgr::is_cluster_allow_vote() const
{
  bool bool_ret = true;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200) {
    bool_ret = ObMultiClusterUtil::is_cluster_allow_elect(partition_key_.get_table_id());
  }
  return bool_ret;
}

bool ObLogStateMgr::has_valid_member_list() const
{
  bool bool_ret = false;
  if (GCTX.is_primary_cluster() || ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    bool_ret = true;
  } else if (mm_->get_ms_proposal_id().is_valid()) {
    bool_ret = true;
  } else if (mm_->get_curr_member_list().contains(self_)) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

bool ObLogStateMgr::is_can_elect_standby_leader() const
{
  bool bool_ret = false;
  if (ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    bool_ret = false;
  } else if (GCTX.is_primary_cluster()) {
    bool_ret = false;
  } else if (GCTX.can_be_parent_cluster()) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObLogStateMgr::is_cluster_allow_handle_prepare() const
{
  bool bool_ret = true;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200) {
    if (ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
      bool_ret = true;
    } else {
      common::ObClusterType cluster_type = INVALID_CLUSTER_TYPE;
      share::ServerServiceStatus server_status = share::OBSERVER_INVALID_STATUS;
      GCTX.get_cluster_type_and_status(cluster_type, server_status);
      if (common::INVALID_CLUSTER_TYPE == cluster_type || share::OBSERVER_SWITCHING == server_status) {
        // not allowed in switching state, avoid proposal_id larger than new primary cluster
        bool_ret = false;
        if (!bool_ret && REACH_TIME_INTERVAL(100 * 1000)) {
          CLOG_LOG(WARN, "now can't handle prepare", K_(partition_key), K(cluster_type), K(server_status));
        }
      } else {
        bool_ret = true;
      }
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::can_submit_start_working_log() const
{
  bool bool_ret = true;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200) {
    if (GCTX.is_primary_cluster() || ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
      bool_ret = true;
    } else if (GCTX.is_in_flashback_state() || GCTX.is_in_cleanup_state()) {
      // flashback/cleanup cluster allow write start_working
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::can_append_disk_log() const
{
  return is_follower_replay_();
}

bool ObLogStateMgr::can_receive_recovery_log() const
{
  return is_follower_replay_();
}

bool ObLogStateMgr::can_majority_cb(const common::ObProposalID& proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id != curr_proposal_id_) {
    CLOG_LOG(INFO, "now can't majority_cb", K_(partition_key), K(proposal_id), K_(curr_proposal_id));
  } else {
    bool_ret = (is_leader_active_() || is_leader_reconfirm_());
  }
  return bool_ret;
}

bool ObLogStateMgr::can_standby_majority_cb(const common::ObProposalID& proposal_id) const
{
  // When standby_leader judges, there is no need to judge whether proposal_id and curr_proposal_id are consistent
  // Consider the scenario:
  // In the process of standby_leader writing a log locally, the primary cluster switches leader, and curr_proposal_id
  // is pushed up The proposal_id carried when flush_cb belongs to the old leader. After executing
  // try_set_majority_finished, failure of executing standby_majority_cb will result in unable to execute
  // set_log_confirmed
  UNUSED(proposal_id);
  bool bool_ret = (STANDBY_LEADER == role_);
  return bool_ret;
}

bool ObLogStateMgr::can_receive_log_ack(const common::ObProposalID& proposal_id) const
{
  bool bool_ret = true;
  if ((LEADER == role_ || STANDBY_LEADER == role_) && proposal_id != curr_proposal_id_) {
    bool_ret = false;
    if (partition_reach_time_interval(1000 * 1000, receive_log_ack_warn_time_)) {
      CLOG_LOG(INFO, "now can't receive_log_ack", K_(partition_key), K_(role), K(proposal_id), K_(curr_proposal_id));
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::can_send_log_ack(const common::ObProposalID& proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id != curr_proposal_id_) {
    if (partition_reach_time_interval(1000 * 1000, send_log_ack_warn_time_)) {
      CLOG_LOG(INFO, "now can't send_log_ack", K_(partition_key), K(proposal_id), K_(curr_proposal_id));
    }
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type())) {
  } else if (restore_mgr_->is_archive_restoring()) {
  } else {
    bool_ret = (is_follower_active_() || is_leader_revoking_());
  }
  return bool_ret;
}

bool ObLogStateMgr::can_send_standby_log_ack(const common::ObProposalID& proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id != curr_proposal_id_) {
    if (partition_reach_time_interval(1000 * 1000, send_log_ack_warn_time_)) {
      CLOG_LOG(INFO, "now can't send_log_ack", K_(partition_key), K(proposal_id), K_(curr_proposal_id));
    }
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type())) {
  } else if (restore_mgr_->is_archive_restoring()) {
  } else {
    bool_ret = (STANDBY_LEADER == role_);
  }
  return bool_ret;
}

bool ObLogStateMgr::can_receive_max_log_id(const common::ObProposalID& proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id != curr_proposal_id_) {
    CLOG_LOG(INFO, "now can't receive_max_log_id", K_(partition_key), K(proposal_id), K_(curr_proposal_id));
  } else {
    bool_ret = is_leader_reconfirm_();
  }
  return bool_ret;
}

bool ObLogStateMgr::can_majority_cb_for_renew_ms_log(const common::ObProposalID& ms_proposal_id) const
{
  bool bool_ret = false;
  common::ObProposalID curr_ms_proposal_id = mm_->get_ms_proposal_id();
  if (ms_proposal_id != curr_ms_proposal_id) {
    CLOG_LOG(INFO, "standby now can't majority_cb", K_(partition_key), K(ms_proposal_id), K(curr_ms_proposal_id));
  } else {
    bool_ret = (is_standby_leader_active_() || is_standby_leader_reconfirm_());
  }
  return bool_ret;
}

bool ObLogStateMgr::can_receive_renew_ms_log_ack(const common::ObProposalID& ms_proposal_id) const
{
  bool bool_ret = false;
  common::ObProposalID curr_ms_proposal_id = mm_->get_ms_proposal_id();
  if (STANDBY_LEADER == role_ && ms_proposal_id == curr_ms_proposal_id) {
    bool_ret = true;
  } else {
    bool_ret = false;
    if (partition_reach_time_interval(1000 * 1000, receive_log_ack_warn_time_)) {
      CLOG_LOG(INFO,
          "can't receive_renew_ms_log_ack",
          K_(partition_key),
          K_(role),
          K(ms_proposal_id),
          K(curr_ms_proposal_id));
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::can_send_renew_ms_log_ack(const common::ObProposalID& proposal_id) const
{
  bool bool_ret = false;
  common::ObProposalID curr_ms_proposal_id = mm_->get_ms_proposal_id();

  if (proposal_id != curr_ms_proposal_id) {
    if (partition_reach_time_interval(1000 * 1000, send_log_ack_warn_time_)) {
      CLOG_LOG(INFO, "standby now can't send_log_ack", K_(partition_key), K(proposal_id), K_(curr_proposal_id));
    }
  } else if (!is_can_elect_standby_leader()) {
    CLOG_LOG(INFO,
        "cluster state not match, now can't send_log_ack",
        K_(partition_key),
        K(proposal_id),
        K_(curr_proposal_id));
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type())) {
  } else if (restore_mgr_->is_archive_restoring()) {
  } else {
    bool_ret = (is_follower_active_() || is_standby_leader_revoking_());
  }
  return bool_ret;
}

bool ObLogStateMgr::can_handle_standby_prepare_resp(const common::ObProposalID& proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id != mm_->get_ms_proposal_id()) {
    CLOG_LOG(INFO,
        "proposal_id not match, can't handle standby_prepare_resp",
        K_(partition_key),
        K(proposal_id),
        "ms_proposal_id",
        mm_->get_ms_proposal_id());
  } else if (!is_can_elect_standby_leader()) {
    CLOG_LOG(INFO,
        "not standby non-private tbl, can't handle standby_prepare_resp",
        K_(partition_key),
        K(proposal_id),
        "ms_proposal_id",
        mm_->get_ms_proposal_id());
  } else {
    bool_ret = is_standby_leader_reconfirm_();
  }
  return bool_ret;
}

bool ObLogStateMgr::can_get_log(const int64_t cluster_id) const
{
  bool bool_ret = false;
  if (is_diff_cluster_req_in_disabled_state(cluster_id)) {
    bool_ret = false;
  } else {
    bool_ret = (is_leader_reconfirm_() || is_leader_taking_over_() || is_leader_active_() || is_leader_revoking_() ||
                is_standby_leader_revoking_() || is_standby_leader_reconfirm_() || is_standby_leader_active_() ||
                is_follower_active_());
  }
  return bool_ret;
}

bool ObLogStateMgr::can_get_log_for_reconfirm(const common::ObProposalID& proposal_id) const
{
  bool bool_ret = false;
  if (proposal_id != curr_proposal_id_) {
    CLOG_LOG(INFO, "now can't get_log", K_(partition_key), K(proposal_id), K_(curr_proposal_id));
  } else {
    bool_ret = (is_leader_revoking_() || is_follower_active_());
  }
  return bool_ret;
}

bool ObLogStateMgr::is_diff_cluster_req_in_disabled_state(const int64_t cluster_id) const
{
  bool bool_ret = false;
  if (!ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) && GCTX.is_in_disabled_state() &&
      cluster_id != get_self_cluster_id()) {
    bool_ret = true;
  }
  return bool_ret;
}

bool ObLogStateMgr::can_receive_log(const common::ObProposalID& proposal_id_in_req,
    const common::ObProposalID& proposal_id_in_log, const int64_t cluster_id) const
{
  bool bool_ret = false;
  if (!is_offline()) {
    if (is_diff_cluster_req_in_disabled_state(cluster_id)) {
      // non-private replica cannot receive log from a different cluster.
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        CLOG_LOG(INFO,
            "cannot receive_log from diff clulster in disabled state",
            K(partition_key_),
            K(cluster_id),
            "self_cluster_id",
            get_self_cluster_id());
      }
    } else if (LEADER == role_) {
      if (proposal_id_in_req != curr_proposal_id_ || proposal_id_in_log >= curr_proposal_id_) {
        CLOG_LOG(INFO,
            "now can't receive_log",
            K_(partition_key),
            K_(role),
            K(proposal_id_in_req),
            K(proposal_id_in_log),
            K_(curr_proposal_id));
      } else {
        bool_ret = (is_leader_reconfirm_() || is_leader_revoking_());
      }
    } else if (FOLLOWER == role_) {
      if (restore_mgr_->is_standby_restore_state()) {
        // standby replica cannot receive log before restoring finished, because its memtable may not be created
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          CLOG_LOG(INFO, "now can't receive_log, self_ in standby_restore state", K(partition_key_));
        }
      } else if (!proposal_id_in_log.is_valid()) {
        CLOG_LOG(INFO,
            "now can't receive_log, null log (OB_LOG_NOT_EXIST)",
            K(partition_key_),
            K(role_),
            K(proposal_id_in_log),
            K(proposal_id_in_req),
            K(curr_proposal_id_));
      } else if (self_ == proposal_id_in_req.addr_) {
        CLOG_LOG(INFO,
            "now can't receive_log, self_ in proposal id",
            K(partition_key_),
            K(role_),
            K(proposal_id_in_log),
            K(proposal_id_in_req),
            K(curr_proposal_id_));
      } else if (proposal_id_in_req != curr_proposal_id_) {
        CLOG_LOG(INFO,
            "now can't receive_log, proposal id not match",
            K(partition_key_),
            K(role_),
            K(proposal_id_in_req),
            K(proposal_id_in_log),
            K(curr_proposal_id_));
      } else {
        bool_ret = is_follower_active_();
      }
    } else if (STANDBY_LEADER == role_) {
      if (restore_mgr_->is_standby_restore_state()) {
        // standby replica cannot receive log before restoring finished, because its memtable may not be created
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          CLOG_LOG(INFO, "now can't receive_log, self_ in standby_restore state", K(partition_key_));
        }
      } else if (!proposal_id_in_log.is_valid()) {
        CLOG_LOG(INFO,
            "now can't receive_log, null log (OB_LOG_NOT_EXIST)",
            K(partition_key_),
            K(role_),
            K(proposal_id_in_log),
            K(proposal_id_in_req),
            K(curr_proposal_id_));
      } else if (self_ == proposal_id_in_req.addr_) {
        CLOG_LOG(INFO,
            "now can't receive_log, self_ in proposal id",
            K(partition_key_),
            K(role_),
            K(proposal_id_in_log),
            K(proposal_id_in_req),
            K(curr_proposal_id_));
      } else if (proposal_id_in_req != curr_proposal_id_) {
        CLOG_LOG(INFO,
            "now can't receive_log, proposal id not match",
            K(partition_key_),
            K(role_),
            K(proposal_id_in_req),
            K(proposal_id_in_log),
            K(curr_proposal_id_));
      } else {
        bool_ret = true;
      }
    } else {
      // do nothing
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::can_receive_renew_ms_log(const common::ObProposalID& proposal_id_in_req,
    const common::ObProposalID& proposal_id_in_log, const int64_t cluster_id) const
{
  bool bool_ret = false;
  ObProposalID curr_ms_proposal_id = mm_->get_ms_proposal_id();
  if (!is_offline()) {
    if (FOLLOWER != role_) {
      CLOG_LOG(INFO,
          "self is not follower, now can't receive_log",
          K_(partition_key),
          K_(role),
          K(proposal_id_in_req),
          K(proposal_id_in_log),
          K(curr_ms_proposal_id));
    } else {
      if (is_diff_cluster_req_in_disabled_state(cluster_id)) {
        // non-private replica cannot receive log from diff cluster
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          CLOG_LOG(INFO,
              "cannot receive_log from diff clulster in disabled state",
              K(partition_key_),
              K(cluster_id),
              "self_cluster_id",
              get_self_cluster_id());
        }
      } else if (!proposal_id_in_log.is_valid()) {
        CLOG_LOG(INFO,
            "now can't receive_log, null log (OB_LOG_NOT_EXIST)",
            K(partition_key_),
            K(role_),
            K(proposal_id_in_log),
            K(proposal_id_in_req),
            K(curr_proposal_id_));
      } else if (self_ == proposal_id_in_req.addr_) {
        CLOG_LOG(INFO,
            "now can't receive_log, self_ in proposal id",
            K(partition_key_),
            K(role_),
            K(proposal_id_in_log),
            K(proposal_id_in_req),
            K(curr_proposal_id_));
      } else if (proposal_id_in_req != curr_ms_proposal_id) {
        CLOG_LOG(INFO,
            "now can't receive_log, ms_proposal id not match",
            K(partition_key_),
            K(role_),
            K(proposal_id_in_req),
            K(proposal_id_in_log),
            K(curr_ms_proposal_id));
      } else {
        bool_ret = (is_follower_active_() || is_standby_leader_revoking_());
      }
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::can_change_member() const
{
  return (can_submit_log() || is_standby_leader_active_()) && !is_changing_leader();
}

bool ObLogStateMgr::can_change_leader() const
{
  return ((is_leader_active_() || is_standby_leader_active_()) && mm_->is_state_init() && !is_changing_leader());
}

bool ObLogStateMgr::can_get_leader_curr_member_list() const
{
  return (is_leader_reconfirm_() || is_leader_taking_over_() || is_leader_active_() || is_standby_leader_reconfirm_() ||
          is_standby_leader_active_());
}

bool ObLogStateMgr::can_handle_prepare_rqst(const common::ObProposalID& proposal_id, const int64_t cluster_id) const
{
  bool bool_ret = false;
  if (proposal_id < curr_proposal_id_) {
    CLOG_LOG(INFO, "invalid proposal_id", K_(partition_key), K(proposal_id), K_(curr_proposal_id));
  } else if (is_diff_cluster_req_in_disabled_state(cluster_id)) {
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(INFO,
          "cannot handle prepare rqst from diff clulster in disabled state",
          K(partition_key_),
          K(cluster_id),
          "self_cluster_id",
          get_self_cluster_id());
    }
  } else {
    // cannot submit prepare log in switching state
    bool_ret = ((is_leader_reconfirm_() || is_leader_active_() || (STANDBY_LEADER == role_) || is_follower_active_() ||
                    is_leader_revoking_()) &&
                is_cluster_allow_handle_prepare());
  }
  return bool_ret;
}

bool ObLogStateMgr::can_handle_standby_prepare_rqst(
    const common::ObProposalID& proposal_id, const int64_t cluster_id) const
{
  bool bool_ret = false;
  if (!is_can_elect_standby_leader()) {
    CLOG_LOG(INFO, "not standby non-private tbl, cannot handle standby prepare request", K_(partition_key));
  } else if (cluster_id != get_self_cluster_id()) {
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(INFO,
          "cannot handle standby prepare rqst from diff clulster",
          K(partition_key_),
          K(cluster_id),
          "self_cluster_id",
          get_self_cluster_id());
    }
  } else {
    if (proposal_id <= mm_->get_ms_proposal_id()) {
      CLOG_LOG(INFO,
          "new proposal_id is not larger than proposal_id, ignore",
          K_(partition_key),
          K(proposal_id),
          K_(curr_proposal_id),
          "ms_proposal_id",
          mm_->get_ms_proposal_id());
    } else {
      bool_ret = ((is_standby_leader_reconfirm_() || is_standby_leader_active_() || is_follower_active_() ||
                      is_standby_leader_revoking_()) &&
                  is_cluster_allow_handle_prepare());
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::can_slide_sw() const
{
  return (is_leader_reconfirm_() || is_leader_active_() || is_follower_active_() || (STANDBY_LEADER == role_) ||
             is_follower_replay_()) &&
         (!is_offline() && (is_replay_enabled()));
}

bool ObLogStateMgr::is_standby_leader_can_receive_log_id(const uint32_t log_id) const
{
  bool bool_ret = true;
  if (STANDBY_LEADER == role_ && is_pre_member_changing() && log_id > sw_->get_max_log_id()) {
    // standby leader cannot receive larger log during member changing
    bool_ret = false;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN,
          "standby_leader is changing member, can not receive log larger than max_log_id",
          K_(partition_key),
          K(log_id),
          "max_log_id",
          sw_->get_max_log_id());
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::can_receive_confirmed_info(const int64_t src_cluster_id) const
{
  return (((is_leader_revoking_() && src_cluster_id == get_self_cluster_id()) ||
              (is_follower_active_() && src_cluster_id == get_self_cluster_id()) ||
              (STANDBY_LEADER == role_ &&
                  src_cluster_id !=
                      get_self_cluster_id()))  // standby_leader can only receive confirmed_info from different cluster
          && !is_offline());
}

bool ObLogStateMgr::can_receive_renew_ms_log_confirmed_info() const
{
  return ((is_standby_leader_revoking_() || is_follower_active_()) && is_can_elect_standby_leader());
}

int ObLogStateMgr::switch_state(bool& need_retry)
{
  int ret = OB_SUCCESS;
  need_retry = false;

  // Need to divide the next cycle into the following two situations:
  // 1. Unstable state. For unstable states such as leader_revoke, leader_active, etc., try another round. If it can be
  // completed in the next round, the time cost of state switching will be changed from milliseconds to microseconds.
  // 2. Stable state, to avoid the election of a new state during this round of handover, and also reduce the delay time
  // of the next handover
  bool need_next_loop = false;
  ObCascadMember new_leader;
  ObAddr elect_real_leader;
  ObAddr previous_leader;
  int64_t round = 0;
  do {
    round++;
    const ObAddr old_leader = get_leader();
    const ObAddr old_primary_leader = get_primary_leader_addr();
    const common::ObRole old_role = role_;
    const int16_t old_state = state_;
    int64_t old_leader_epoch = leader_epoch_;
    const int64_t start_ts = ObClockGenerator::getClock();

    need_next_loop = false;
    if (is_follower_replay_()) {
      if (follower_replay_need_switch_()) {
        ret = replay_to_follower_active_();
        need_next_loop = true;  // next loop will check new leader
      }
    } else if (is_leader_revoking_() || is_standby_leader_revoking_()) {
      if (leader_revoking_need_switch_()) {
        ret = revoking_to_follower_active_();
        if (follower_active_need_switch_()) {
          need_next_loop = true;
        }
      }
    } else if (is_follower_active_()) {
      if (!restore_mgr_->is_archive_restoring()) {
        if (follower_active_need_switch_()) {
          new_leader.reset();
          previous_leader.reset();
          int64_t new_leader_epoch = OB_INVALID_TIMESTAMP;
          if (follower_need_update_role_(new_leader, elect_real_leader, previous_leader, new_leader_epoch)) {
            ret = follower_active_to_reconfirm_(new_leader_epoch, previous_leader);
          } else {
            set_leader_and_epoch_(new_leader, new_leader_epoch);
          }
          need_next_loop = true;  // 1) reconfirm or 2) fetch log from leader
        }
      } else {
        // skip check leader during phsyical restore
        need_next_loop = false;
      }
    } else if (is_leader_reconfirm_() || is_standby_leader_reconfirm_()) {
      if (is_reconfirm_initialize_()) {
        ret = reconfirm_->reconfirm();
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCCESS != ret || is_reconfirm_role_change_or_sync_timeout_()) {
        ret = reconfirm_to_follower_active_();
      } else if (is_reconfirm_not_eagain_(ret)) {
        if (OB_SUCC(ret)) {
          ret = reconfirm_to_taking_over_();
          need_next_loop = true;  // next loop whill check whether taking_over is done
        } else {
          ret = reconfirm_to_follower_active_();
        }
      }
    } else if (is_leader_taking_over_()) {
      if (leader_taking_over_need_switch_()) {
        ret = taking_over_to_leader_active_();
        bool is_error = false;
        if (leader_active_need_switch_(is_error)) {
          need_next_loop = true;
          if (is_error) {
            const uint32_t revoke_type = ObElection::RevokeType::CLOG_SW_TIMEOUT;
            revoke_leader_(revoke_type);
          }
        }
      }
    } else if (is_leader_active_()) {
      bool is_error = false;
      if (leader_active_need_switch_(is_error)) {
        if (is_error) {
          const uint32_t revoke_type = ObElection::RevokeType::CLOG_SW_TIMEOUT;
          revoke_leader_(revoke_type);
        }
        ret = leader_active_to_revoking_();
        need_next_loop = true;
      }
    } else if (is_standby_leader_active_()) {
      share::ObCascadMember new_primary_leader;
      bool is_error = false;
      if (leader_active_need_switch_(is_error)) {
        ret = leader_active_to_revoking_();
        need_next_loop = true;
      } else if (need_update_primary_leader_(new_primary_leader)) {
        set_primary_leader_(new_primary_leader);
      } else {
      }
    } else {
      // do nothing
    }

    if (LEADER == old_role && FOLLOWER == role_) {
      // LEADER -> FOLLOWER
    } else if (FOLLOWER == old_role && LEADER == role_) {
      // FOLLOWER -> LEADER, reset parent, set children to member_list
      const uint32_t reset_type = ObLogCascadingMgr::ResetPType::SELF_ELECTED;
      cascading_mgr_->reset_parent(reset_type);
    } else if (STANDBY_LEADER == old_role && FOLLOWER == role_) {
      // STANDBY_LEADER -> FOLLOWER
      if (cascading_mgr_->get_parent().is_valid() &&
          cascading_mgr_->get_parent().get_cluster_id() != get_self_cluster_id()) {
        const uint32_t reset_type = ObLogCascadingMgr::ResetPType::STANDBY_REVOKE;
        cascading_mgr_->reset_parent(reset_type);
      }
    } else if (FOLLOWER == old_role && STANDBY_LEADER == role_) {
      // FOLLOWER -> STANDBY_LEADER
      if (cascading_mgr_->get_parent().is_valid() &&
          cascading_mgr_->get_parent().get_cluster_id() == get_self_cluster_id()) {
        const uint32_t reset_type = ObLogCascadingMgr::ResetPType::SELF_ELECTED;
        cascading_mgr_->reset_parent(reset_type);
      }
    } else {
      // do nothing
    }

    if (!restore_mgr_->is_archive_restoring()) {
      if (leader_.is_valid() && get_leader() != old_leader) {
        if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type())) {
          if (self_ != get_leader()) {
            cascading_mgr_->set_parent(leader_);
          } else {
            const uint32_t reset_type = ObLogCascadingMgr::ResetPType::SELF_ELECTED;
            cascading_mgr_->reset_parent(reset_type);
          }
        } else {
          if (!cascading_mgr_->get_parent().is_valid() && !is_offline()) {
            (void)cascading_mgr_->check_parent_state(start_ts, get_region());
          }
        }
      }
      if (primary_leader_.is_valid() && get_primary_leader_addr() != old_primary_leader) {
        if (STANDBY_LEADER == role_ && !is_offline()) {
          (void)cascading_mgr_->check_parent_state(start_ts, get_region());
        }
      }
    }
    if (partition_reach_time_interval(30 * 1000 * 1000, switch_state_warn_time_)) {
      CLOG_LOG(INFO,
          "switch_state",
          K_(partition_key),
          K(ret),
          K(old_role),
          K_(role),
          K(old_state),
          K_(state),
          K(old_leader),
          K_(leader),
          K_(primary_leader),
          K(old_leader_epoch),
          K_(leader_epoch),
          K(need_next_loop),
          K_(is_offline),
          "replica_type",
          mm_->get_replica_type(),
          "diff",
          ObClockGenerator::getClock() - start_ts);
    }
  } while (need_next_loop && OB_SUCC(ret));

  need_retry = is_interim_state_();
  return ret;
}

bool ObLogStateMgr::check_sliding_window_state()
{
  bool state_changed = false;
  const int64_t now = ObTimeUtility::current_time();
  if (!restore_mgr_->is_archive_restoring()) {
    share::ObCascadMember new_primary_leader;
    if (is_follower_replay_()) {
      state_changed = follower_replay_need_switch_();
    } else if (is_leader_revoking_() || is_standby_leader_revoking_()) {
      state_changed = leader_revoking_need_switch_();
    } else if (need_update_leader_()) {
      state_changed = true;
    } else if (need_update_primary_leader_(new_primary_leader)) {
      state_changed = true;
    } else if (is_leader_active_()) {
      bool need_switch_leader_to_self = false;
      state_changed = check_leader_sliding_window_not_slide_(need_switch_leader_to_self);
      if (!state_changed) {
        if (!ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
            GCTX.is_in_standby_active_state()) {
          // leader need do role change: leader->follower->standby_leader
          state_changed = true;
        } else if (need_switch_leader_to_self) {
          state_changed = true;
        } else {
        }
      }
    } else if (is_follower_active_()) {
      (void)check_and_try_fetch_log_();
    } else if (is_standby_leader_active_()) {
      (void)check_and_try_fetch_log_();
      if (GCTX.is_in_flashback_state() || GCTX.is_primary_cluster() || GCTX.is_in_cleanup_state()) {
        state_changed = true;
      }
    } else {
      // do nothing;
    }
  } else {
    if (is_follower_replay_()) {
      state_changed = follower_replay_need_switch_();
    } else if (is_follower_active_() && restore_mgr_->is_archive_restoring()) {
      if (FOLLOWER == restore_mgr_->get_role()) {
        (void)check_and_try_fetch_log_();
      }
    } else {
      // do nothing
    }
  }
  return state_changed;
}

bool ObLogStateMgr::is_state_changed(bool& need_retry)
{
  bool state_changed = false;
  bool is_error = false;
  need_retry = false;
  if (is_follower_replay_()) {
    state_changed = follower_replay_need_switch_();
  } else if (is_leader_revoking_() || is_standby_leader_revoking_()) {
    state_changed = leader_revoking_need_switch_();
  } else if (is_follower_active_()) {
    state_changed = follower_active_need_switch_();
  } else if (is_leader_reconfirm_() || is_standby_leader_reconfirm_()) {
    state_changed = leader_reconfirm_need_switch_();
  } else if (is_leader_taking_over_()) {
    state_changed = leader_taking_over_need_switch_();
  } else if (is_leader_active_()) {
    state_changed = leader_active_need_switch_(is_error);
  } else if (is_standby_leader_active_()) {
    share::ObCascadMember new_primary_leader;
    if (leader_active_need_switch_(is_error)) {
      state_changed = true;
    } else if (need_update_primary_leader_(new_primary_leader)) {
      state_changed = true;
    } else {
    }
  } else {
    // do nothing
  }
  if (!state_changed) {
    need_retry = is_interim_state_();
  }
  return state_changed;
}

int ObLogStateMgr::set_scan_disk_log_finished()
{
  int ret = OB_SUCCESS;
  scan_disk_log_finished_ = true;
  CLOG_LOG(INFO, "scan disk log finished", K_(partition_key));
  return ret;
}

int ObLogStateMgr::handle_prepare_rqst(
    const common::ObProposalID& proposal_id, const common::ObAddr& new_leader, const int64_t cluster_id)
{
  // PREPARE log is transferred only in paxos member_list
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!proposal_id.is_valid() || !new_leader.is_valid()) {
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(proposal_id), K(new_leader));
    ret = OB_INVALID_ARGUMENT;
  } else {
    mm_->reset_follower_pending_entry();
    if (new_leader != self_ && LEADER == role_) {
      if (is_leader_reconfirm_()) {
        ret = reconfirm_to_follower_active_();
      } else if (is_leader_active_()) {
        ret = leader_active_to_revoking_();
      } else if (is_leader_taking_over_()) {
        ret = OB_STATE_NOT_MATCH;
      }
    }

    bool is_leader_changed = false;
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "state change failed", K_(partition_key), K(ret), K_(role), K_(state));
    } else {
      if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) && new_leader != get_leader() &&
          (ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) || GCTX.is_primary_cluster()) &&
          !restore_mgr_->is_archive_restoring()) {
        ObCascadMember new_leader_member(new_leader, cluster_id);
        leader_ = new_leader_member;
        is_leader_changed = true;
      }
      if (!ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) && GCTX.is_standby_cluster()) {
        // standby replcia need maintain prepared_primary_leader_, because when primary cluster is doing reconfirm,
        // standby_leader may still does't know current primary_leader, so it cannot reply standby_ack to primary.
        // Then priamry leader's reconfirm maybe slowed down, but standby_leader can send ack to
        // prepared_primary_leader_ to avoid this bad case.
        ObCascadMember new_leader_member(new_leader, cluster_id);
        prepared_primary_leader_ = new_leader_member;
        CLOG_LOG(INFO, "standby_leader update prepared_primary_leader", K_(partition_key), K_(prepared_primary_leader));
      }

      if (!log_engine_->is_disk_space_enough()) {
        ret = OB_LOG_OUTOF_DISK_SPACE;
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "log outof disk space, cannot write prepare log", K_(partition_key), K(ret));
        }
      } else if (OB_FAIL(submit_prepare_log_(proposal_id, new_leader, cluster_id, OB_LOG_PREPARED))) {
        if (OB_ALLOCATE_MEMORY_FAILED != ret) {
          CLOG_LOG(ERROR, "submit prepare log, failed", K_(partition_key), K(ret), K(proposal_id), K(new_leader));
        } else {
          CLOG_LOG(WARN, "submit prepare log, failed", K_(partition_key), K(ret), K(proposal_id), K(new_leader));
        }
      } else {
        curr_proposal_id_ = proposal_id;
      }
    }
    if (is_leader_changed && !restore_mgr_->is_archive_restoring() &&
        ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type())) {
      if (self_ != get_leader()) {
        cascading_mgr_->set_parent(leader_);
      } else {
        const uint32_t reset_type = ObLogCascadingMgr::ResetPType::SELF_ELECTED;
        cascading_mgr_->reset_parent(reset_type);
      }
    }
    CLOG_LOG(INFO,
        "handle_prepare_rqst",
        K_(partition_key),
        K(ret),
        K(proposal_id),
        K_(self),
        K(new_leader),
        K_(curr_proposal_id),
        "ms_proposal_id",
        mm_->get_ms_proposal_id());
  }
  return ret;
}

int ObLogStateMgr::handle_standby_prepare_rqst(
    const common::ObProposalID& proposal_id, const common::ObAddr& new_leader, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!proposal_id.is_valid() || !new_leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(proposal_id), K(new_leader));
  } else if (!is_can_elect_standby_leader()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "not standby non-private tbl", K(ret), K_(partition_key), K(proposal_id), K(new_leader));
  } else {
    mm_->reset_follower_pending_entry();
    if (new_leader != self_ && STANDBY_LEADER == role_) {
      if (is_standby_leader_reconfirm_()) {
        ret = reconfirm_to_follower_active_();
      } else if (is_standby_leader_active_()) {
        ret = leader_active_to_revoking_();
      } else if (is_leader_taking_over_()) {
        ret = OB_STATE_NOT_MATCH;
      }
    }

    bool is_leader_changed = false;
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "state change failed", K_(partition_key), K(ret), K_(role), K_(state));
    } else {
      if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) && new_leader != get_leader() &&
          !restore_mgr_->is_archive_restoring()) {
        ObCascadMember new_leader_member(new_leader, obrpc::ObRpcNetHandler::CLUSTER_ID);
        leader_ = new_leader_member;
        is_leader_changed = true;
      }
      if (OB_FAIL(submit_prepare_log_(proposal_id, new_leader, cluster_id, OB_LOG_STANDBY_PREPARED))) {
        if (OB_ALLOCATE_MEMORY_FAILED != ret) {
          CLOG_LOG(ERROR, "submit prepare log, failed", K_(partition_key), K(ret), K(proposal_id), K(new_leader));
        } else {
          CLOG_LOG(WARN, "submit prepare log, failed", K_(partition_key), K(ret), K(proposal_id), K(new_leader));
        }
      } else {
        // update ms_proposal_id
        mm_->update_ms_proposal_id(proposal_id);
      }
    }
    if (is_leader_changed && !restore_mgr_->is_archive_restoring() &&
        ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type())) {
      if (self_ != get_leader()) {
        cascading_mgr_->set_parent(leader_);
      } else {
        const uint32_t reset_type = ObLogCascadingMgr::ResetPType::SELF_ELECTED;
        cascading_mgr_->reset_parent(reset_type);
      }
    }
    CLOG_LOG(INFO,
        "handle_standby_prepare_rqst",
        K_(partition_key),
        K(ret),
        K(proposal_id),
        K(new_leader),
        K_(curr_proposal_id),
        "ms_proposal_id",
        mm_->get_ms_proposal_id());
  }
  return ret;
}

int ObLogStateMgr::update_proposal_id(const common::ObProposalID& proposal_id)
{
  int ret = OB_SUCCESS;
  if (!proposal_id.is_valid()) {
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(proposal_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (proposal_id < curr_proposal_id_) {
    CLOG_LOG(INFO, "update proposal id, failed", K_(partition_key), K(proposal_id), K_(curr_proposal_id));
    ret = OB_TERM_NOT_MATCH;
  } else {
    curr_proposal_id_ = proposal_id;
    CLOG_LOG(INFO, "update proposal id, success", K_(partition_key), K_(curr_proposal_id));
  }
  return ret;
}

int ObLogStateMgr::start_election()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) || election_started_) {
    // no need exec start election
  } else if (OB_FAIL(election_->start())) {
    CLOG_LOG(WARN, "start_election failed", K(ret), K_(partition_key));
  } else {
    election_started_ = true;
  }
  CLOG_LOG(INFO, "start_election finished", K(ret), K_(partition_key), "replica_type", mm_->get_replica_type());
  return ret;
}

int ObLogStateMgr::stop_election()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (!election_started_) {
    // no need exec stop election
  } else if (OB_FAIL(election_->stop())) {
    CLOG_LOG(WARN, "election stop failed", K_(partition_key), K(ret));
  } else {
    election_started_ = false;
  }
  CLOG_LOG(INFO, "stop_election finished", K(ret), K_(partition_key), "replica_type", mm_->get_replica_type());
  return ret;
}

int ObLogStateMgr::set_election_leader(const common::ObAddr& leader, const int64_t lease_start)
{
  int ret = OB_SUCCESS;
  int64_t leader_epoch = OB_INVALID_TIMESTAMP;
  CLOG_LOG(INFO, "start election", K_(partition_key), "replica_type", mm_->get_replica_type());
  if (!leader.is_valid() || lease_start <= 0) {
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(leader), K(lease_start));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type())) {
    if (OB_FAIL(election_->leader_takeover(leader, lease_start, leader_epoch))) {
      CLOG_LOG(ERROR, "leader_takeover failed", K_(partition_key), K(ret), K(leader), K(lease_start));
    } else if (OB_FAIL(election_->start())) {
      CLOG_LOG(ERROR, "election_ start failed", K_(partition_key), K(ret));
    } else {
      election_started_ = true;
    }
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    // this func is called by CLogMgr::create_partition to assign initial leader
    // and here it need call scan_disk_log_finished_
    set_scan_disk_log_finished();
    if (OB_INVALID_TIMESTAMP != leader_epoch) {
      // leader_takeover success
      if (leader == self_) {
        leader_epoch_ = leader_epoch;
        is_new_created_leader_ = true;
      } else {
        // follower set leader_epoch invalid
        leader_epoch_ = OB_INVALID_TIMESTAMP;
      }
    }
  }
  return ret;
}

const ObRegion& ObLogStateMgr::get_region() const
{
  RLockGuard guard(region_lock_);
  return region_;
}

int ObLogStateMgr::set_region(const common::ObRegion& region, bool& is_changed)
{
  int ret = OB_SUCCESS;
  is_changed = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(region));
  } else if (region == get_region()) {
    // region same, ignore
  } else {
    WLockGuard guard(region_lock_);
    region_ = region;
    is_changed = true;
  }
  return ret;
}

const ObIDC& ObLogStateMgr::get_idc() const
{
  RLockGuard guard(idc_lock_);
  return idc_;
}

int ObLogStateMgr::set_idc(const common::ObIDC& idc, bool& is_changed)
{
  int ret = OB_SUCCESS;
  is_changed = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (idc.is_empty()) {
    // idc not set, skip
  } else if (idc == get_idc()) {
    // idc same, ignore
  } else {
    WLockGuard guard(idc_lock_);
    idc_ = idc;
    is_changed = true;
  }
  return ret;
}

void ObLogStateMgr::destroy()
{
  is_inited_ = false;
  is_restoring_ = false;
  is_write_disabled_ = false;
  sw_ = NULL;
  reconfirm_ = NULL;
  log_engine_ = NULL;
  mm_ = NULL;
  replay_engine_ = NULL;
  election_ = NULL;
  partition_service_ = NULL;
  alloc_mgr_ = NULL;
  scan_disk_log_finished_ = false;
  election_started_ = false;
  partition_key_.reset();
  role_ = FOLLOWER;
  state_ = REPLAY;
  curr_proposal_id_.reset();
  leader_.reset();
  previous_leader_.reset();
  primary_leader_.reset();
  prepared_primary_leader_.reset();
  region_.reset();
  idc_.reset();
  leader_epoch_ = OB_INVALID_TIMESTAMP;
  self_.reset();
  freeze_version_ = ObVersion();
  last_check_start_id_ = OB_INVALID_ID;
  last_check_start_id_time_ = OB_INVALID_TIMESTAMP;
  curr_fetch_log_interval_ = CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND;
  curr_round_first_fetch_ = true;
  curr_round_fetch_begin_time_ = OB_INVALID_TIMESTAMP;
  last_fetched_max_log_id_ = OB_INVALID_ID;
  curr_fetch_window_size_ = 1;
  last_wait_standby_ack_start_time_ = OB_INVALID_TIMESTAMP;
  last_check_pending_replay_cnt_ = 0;
  last_check_pending_replay_cnt_time_ = OB_INVALID_TIMESTAMP;
  last_fake_push_time_ = OB_INVALID_TIMESTAMP;
  last_leader_active_time_ = OB_INVALID_TIMESTAMP;
  is_offline_ = false;
  replay_enabled_ = false;
  is_changing_leader_ = false;
  role_change_time_ = OB_INVALID_TIMESTAMP;
  pls_epoch_ = OB_INVALID_TIMESTAMP;
  is_removed_ = true;
}

int ObLogStateMgr::submit_prepare_log_(const common::ObProposalID& proposal_id, const common::ObAddr& new_leader,
    const int64_t cluster_id, const ObLogType log_type)
{
  int ret = OB_SUCCESS;

  ObLogEntryHeader log_entry_header;
  ObLogEntry log_entry;
  ObLogFlushTask* flush_task = NULL;
  int64_t pos = 0;
  char* buff = NULL;
  ObPreparedLog plog;
  char body_buffer[BUF_SIZE];
  int64_t body_pos = 0;
  const bool is_trans_log = false;

  if (!proposal_id.is_valid() || !new_leader.is_valid()) {
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(proposal_id), K(new_leader));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (buff = static_cast<char*>(alloc_mgr_->ge_alloc(OB_MAX_LOG_BUFFER_SIZE)))) {
    CLOG_LOG(WARN, "alloc failed", K_(partition_key));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(plog.serialize(body_buffer, BUF_SIZE, body_pos))) {
    CLOG_LOG(ERROR, "serialize failed", K_(partition_key), K(ret));
  } else if (OB_FAIL(log_entry_header.generate_header(log_type,
                 partition_key_,
                 OB_INVALID_ID,
                 body_buffer,
                 body_pos,
                 ObTimeUtility::current_time(),
                 OB_INVALID_TIMESTAMP,
                 proposal_id,
                 0, /* submit_timestamp */
                 freeze_version_,
                 is_trans_log))) {
    CLOG_LOG(WARN, "generate_header failed", K_(partition_key), K(ret), K(log_entry_header));
  } else if (OB_FAIL(log_entry.generate_entry(log_entry_header, body_buffer))) {
    CLOG_LOG(WARN, "generate_entry failed", K_(partition_key), K(ret));
  } else if (OB_FAIL(log_entry.serialize(buff, OB_MAX_LOG_BUFFER_SIZE, pos))) {
    CLOG_LOG(WARN, "log_entry serialize failed", K_(partition_key), K(ret));
  } else if (NULL == (flush_task = alloc_mgr_->alloc_log_flush_task())) {
    CLOG_LOG(WARN, "alloc flush task failed", K(partition_key_));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(flush_task->init(log_type,
                 OB_INVALID_ID,
                 proposal_id,
                 partition_key_,
                 partition_service_,
                 new_leader,
                 cluster_id,
                 log_engine_,
                 log_entry_header.get_submit_timestamp(),
                 pls_epoch_))) {
    CLOG_LOG(WARN, "flush_task init failed", K(partition_key_), K(ret));
  } else {
    flush_task->set(buff, pos);
    if (OB_FAIL(log_engine_->submit_flush_task(flush_task))) {
      alloc_mgr_->free_log_flush_task(flush_task);
      flush_task = NULL;
      CLOG_LOG(WARN, "submit flush task failed", K_(partition_key), K(ret));
    }
  }

  if (NULL != buff) {
    alloc_mgr_->ge_free(buff);
    buff = NULL;
  }
  return ret;
}

int ObLogStateMgr::replay_to_follower_active_()
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "replay_to_follower_active", K_(partition_key), "replica_type", mm_->get_replica_type());
  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) && !election_started_) {
    int64_t before_start_partition = common::ObTimeUtility::current_time();
    if (OB_FAIL(election_->start())) {
      CLOG_LOG(WARN, "election_ start failed", K_(partition_key), K(ret));
    } else {
      election_started_ = true;
    }
    int64_t duration = common::ObTimeUtility::current_time() - before_start_partition;
    if (duration > START_PARTITION_WARN_INTERVAL) {
      CLOG_LOG(WARN, "election_->start cost too much time", K_(partition_key), K(ret), K(duration));
    }
  }
  reset_status_();
  role_ = FOLLOWER;
  state_ = ACTIVE;
  if (ObCreatePlsType::ADD_PARTITION == create_pls_type_) {
    last_check_start_id_ = OB_INVALID_ID;
    last_check_start_id_time_ = OB_INVALID_TIMESTAMP;
  }

  return ret;
}

int ObLogStateMgr::revoking_to_follower_active_()
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "revoking_to_follower_active", K_(partition_key), K_(leader));
  role_ = FOLLOWER;
  state_ = ACTIVE;
  start_role_change_time_ = OB_INVALID_TIMESTAMP;
  return ret;
}

int ObLogStateMgr::try_renew_sync_standby_location()
{
  int ret = OB_SUCCESS;
  int64_t sync_cluster_id = OB_INVALID_CLUSTER_ID;
  if (!GCTX.need_sync_to_standby() || ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    // skip
  } else if (OB_FAIL(cascading_mgr_->get_sync_standby_cluster_id(sync_cluster_id)) ||
             OB_INVALID_CLUSTER_ID == sync_cluster_id) {
    CLOG_LOG(WARN, "get_sync_standby_cluster_id failed", K_(partition_key), K(ret), K(sync_cluster_id));
  } else {
    bool is_need_renew = true;
    ObAddr dst_leader;
    (void)async_get_dst_cluster_leader_from_loc_cache(is_need_renew, sync_cluster_id, dst_leader);
  }
  return ret;
}

int ObLogStateMgr::follower_active_to_reconfirm_(const int64_t new_leader_epoch, const ObAddr& previous_leader)
{
  int ret = OB_SUCCESS;
  reconfirm_start_time_ = ObTimeUtility::current_time();
  CLOG_LOG(INFO, "follower_active_to_reconfirm", K_(partition_key), K(new_leader_epoch), K(previous_leader));

  if (is_can_elect_standby_leader()) {
    if (OB_FAIL(sw_->clean_log())) {
      CLOG_LOG(ERROR, "clean sliding window failed", K_(partition_key), K(ret));
    } else {
      reset_status_();
      role_ = STANDBY_LEADER;
      state_ = RECONFIRM;
      ObCascadMember new_leader(self_, obrpc::ObRpcNetHandler::CLUSTER_ID);
      leader_ = new_leader;
      previous_leader_ = previous_leader;
      leader_epoch_ = new_leader_epoch;
      last_leader_active_time_ = ObTimeUtility::current_time();
      start_role_change_time_ = common::ObTimeUtility::current_time();
      (void)standby_update_protection_level();
      (void)mm_->reset_renew_ms_log_task();
      if (OB_FAIL(partition_service_->submit_pt_update_role_task(partition_key_))) {
        CLOG_LOG(WARN, "ps_cb->submit_pt_update_task failed", K_(partition_key), K(ret));
      }
    }
    CLOG_LOG(INFO, "follower_active_to_reconfirm, switch role to STANDBY_LEADER", K_(partition_key), K_(role));
  } else {
    if (OB_FAIL(sw_->clean_log())) {
      CLOG_LOG(ERROR, "clean sliding window failed", K_(partition_key), K(ret));
    } else {
      reset_status_();
      role_ = LEADER;
      state_ = RECONFIRM;
      ObCascadMember new_leader(self_, obrpc::ObRpcNetHandler::CLUSTER_ID);
      leader_ = new_leader;
      previous_leader_ = previous_leader;
      leader_epoch_ = new_leader_epoch;
      last_leader_active_time_ = ObTimeUtility::current_time();
      start_role_change_time_ = common::ObTimeUtility::current_time();
      if (OB_FAIL(partition_service_->submit_pt_update_role_task(partition_key_))) {
        CLOG_LOG(WARN, "ps_cb->submit_pt_update_task failed", K_(partition_key), K(ret));
      }
      (void)try_renew_sync_standby_location();
    }
  }

  return ret;
}

int ObLogStateMgr::reconfirm_to_follower_active_()
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "reconfirm_to_follower_active", K_(partition_key));

  if (STANDBY_LEADER == role_) {
    if (OB_FAIL(sw_->clean_log())) {
      CLOG_LOG(ERROR, "clean sliding window failed", K_(partition_key), K(ret));
    } else {
      const uint32_t revoke_type = ObElection::RevokeType::RECONFIRM_TIMEOUT;
      revoke_leader_(revoke_type);
      reset_status_();
      reconfirm_->reset();
      role_ = FOLLOWER;
      state_ = ACTIVE;
      start_role_change_time_ = OB_INVALID_TIMESTAMP;
      is_new_created_leader_ = false;
    }
    CLOG_LOG(INFO, "standby active non private tbl, reconfirm_to_follower_active", K_(partition_key));
  } else if (LEADER == role_) {
    if (OB_FAIL(sw_->clean_log())) {
      CLOG_LOG(ERROR, "clean sliding window failed", K_(partition_key), K(ret));
    } else {
      const uint32_t revoke_type = ObElection::RevokeType::RECONFIRM_TIMEOUT;
      revoke_leader_(revoke_type);
      reset_status_();
      reconfirm_->reset();
      role_ = FOLLOWER;
      state_ = ACTIVE;
      start_role_change_time_ = OB_INVALID_TIMESTAMP;
      is_new_created_leader_ = false;
    }
  } else {
    // unexpected state
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(ERROR,
        "unexpected cluster state",
        K_(partition_key),
        K(ret),
        "cluster type",
        GCTX.get_cluster_type(),
        "server status",
        GCTX.get_server_service_status());
  }
  return ret;
}

int ObLogStateMgr::reconfirm_to_taking_over_()
{
  int ret = OB_SUCCESS;
  const int64_t reconfirm_cost = ObTimeUtility::current_time() - reconfirm_start_time_;
  CLOG_LOG(INFO, "reconfirm_to_taking_over", K_(partition_key), K(reconfirm_cost));
  on_leader_takeover_start_time_ = ObTimeUtility::current_time();

  if (!ObReplicaTypeCheck::is_can_elected_replica(mm_->get_replica_type())) {
    CLOG_LOG(INFO, "invalid replica type to be elected", K_(partition_key), "replica_type", mm_->get_replica_type());
    const uint32_t revoke_type = ObElection::RevokeType::REPLICA_TYPE_DISALLOW;
    revoke_leader_(revoke_type);
  } else if (!mm_->get_curr_member_list().contains(self_)) {
    CLOG_LOG(INFO,
        "curr_member_list doesn't contain self, revoke",
        K_(partition_key),
        "curr_member_list",
        mm_->get_curr_member_list(),
        K_(self));
    const uint32_t revoke_type = ObElection::RevokeType::MEMBER_LIST_DISALLOW;
    revoke_leader_(revoke_type);
  } else if (OB_FAIL(on_leader_takeover_())) {
    CLOG_LOG(WARN, "on_leader_takeover_ failed, try again", K_(partition_key), K(ret));
  } else if (OB_FAIL(sw_->leader_takeover())) {
    CLOG_LOG(ERROR, "sw leader_active failed", K(ret), K(partition_key_));
  } else {
    reconfirm_->reset();
    // role_ = LEADER;
    state_ = TAKING_OVER;
    is_new_created_leader_ = false;
  }

  return ret;
}

int ObLogStateMgr::taking_over_to_leader_active_()
{
  int ret = OB_SUCCESS;
  const int64_t reconfirm_to_active_cost = ObTimeUtility::current_time() - reconfirm_start_time_;
  bool is_standby_tbl = is_can_elect_standby_leader();

  if (OB_FAIL(on_leader_active_())) {
    CLOG_LOG(WARN, "on_leader_active_ failed, try again", K_(partition_key), K(ret));
  } else if (!is_standby_tbl && OB_FAIL(sw_->leader_active())) {
    CLOG_LOG(ERROR, "sw leader_active failed", K(ret), K(partition_key_));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    if (0 == mm_->get_replica_property().get_memstore_percent()) {
      // D need reset restoring state in leader_active state
      reset_restoring();
    }
    // role_ = LEADER;
    state_ = ACTIVE;
    start_role_change_time_ = OB_INVALID_TIMESTAMP;
  }

  CLOG_LOG(INFO,
      "taking_over_to_leader_active",
      K(ret),
      K_(partition_key),
      K(reconfirm_to_active_cost),
      K(last_leader_active_time_),
      K(is_standby_tbl));
  return ret;
}

int ObLogStateMgr::leader_active_to_revoking_()
{
  int ret = OB_SUCCESS;
  on_leader_revoke_start_time_ = ObTimeUtility::current_time();

  if (STANDBY_LEADER == role_) {
    if (OB_FAIL(on_leader_revoke_())) {
      CLOG_LOG(WARN, "on_leader_revoke_ failed, try again", K_(partition_key), K(ret));
    } else if (OB_FAIL(sw_->clean_log())) {
      CLOG_LOG(ERROR, "clean sliding window failed", K_(partition_key), K(ret));
    } else {
      reset_status_();
      CLOG_LOG(INFO, "standby_leader swtich to REVOKING state", K_(partition_key));
    }
  } else if (LEADER == role_) {
    if (OB_FAIL(sw_->leader_revoke())) {
      CLOG_LOG(ERROR, "sw leader_revoke failed", K(ret), K(partition_key_));
    } else if (OB_FAIL(sw_->clean_log())) {
      CLOG_LOG(ERROR, "clean sliding window failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(on_leader_revoke_())) {
      CLOG_LOG(WARN, "on_leader_revoke_ failed, try again", K_(partition_key), K(ret));
    } else {
      reset_status_();
    }
  } else {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "unexpected role", K(ret), K_(partition_key), K_(role));
  }

  if (OB_SUCC(ret)) {
    bool is_leader = (LEADER == role_);
    // role_ = FOLLOWER;  // role will be changed later
    state_ = REVOKING;
    last_leader_active_time_ = OB_INVALID_TIMESTAMP;
    start_role_change_time_ = common::ObTimeUtility::current_time();
    if (is_leader && !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
      // reset standby children
      cascading_mgr_->reset_async_standby_children();
      cascading_mgr_->reset_sync_standby_child();
    }
    (void)reset_standby_protection_level_();  // reset protection_level
  }

  CLOG_LOG(
      INFO, "leader_active_to_revoking", K(ret), K(role_), K(state_), K_(partition_key), K(last_leader_active_time_));
  return ret;
}

void ObLogStateMgr::reset_status_()
{
  reset_need_rebuild();
  mm_->reset_status();
  last_check_start_id_ = sw_->get_start_id();
  last_check_start_id_time_ = ObClockGenerator::getClock();
  reset_fetch_state();
  last_wait_standby_ack_start_time_ = OB_INVALID_TIMESTAMP;
  last_check_pending_replay_cnt_ = 0;
  last_check_pending_replay_cnt_time_ = OB_INVALID_TIMESTAMP;
  last_fake_push_time_ = OB_INVALID_TIMESTAMP;
  leader_.reset();
  previous_leader_.reset();
  leader_epoch_ = OB_INVALID_TIMESTAMP;
  // Regardless of the success or failure of the leader switch, the current master will no longer be the leader, so when
  // the leader is switched to follower, Reset the changing_leader_ status
  is_changing_leader_ = false;
  is_pre_member_changing_ = false;
}

int ObLogStateMgr::on_leader_takeover_()
{
  int ret = OB_SUCCESS;
  if (LEADER != role_) {
    // only LEADER need call on_leader_takeover
    role_change_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(partition_service_->submit_pt_update_role_task(partition_key_))) {
      CLOG_LOG(WARN, "ps_cb->submit_pt_update_task failed", K_(partition_key), K(ret));
    }
  } else {
    int64_t before_takeover = ObTimeUtility::current_time();
    if (OB_FAIL(partition_service_->on_leader_takeover(partition_key_, role_, is_elected_by_changing_leader_))) {
      CLOG_LOG(WARN, "ps_cb->on_leader_takeover failed!", K_(partition_key), K(ret));
    } else {
      role_change_time_ = ObTimeUtility::current_time();
    }
    int64_t after_takeover = ObTimeUtility::current_time();
    if (after_takeover - before_takeover >= 1000) {
      CLOG_LOG(WARN,
          "ps_cb->on_leader_takeover cost too much time",
          K_(partition_key),
          K(before_takeover),
          K(after_takeover),
          "cost_time",
          after_takeover - before_takeover);
    }
  }
  CLOG_LOG(INFO, "on_leader_takeover finished!", K_(partition_key), K(ret));
  return ret;
}

int ObLogStateMgr::on_leader_active_()
{
  int ret = OB_SUCCESS;
  if (LEADER != role_) {
    // only LEADER need call on_leader_takeover
    role_change_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(partition_service_->submit_pt_update_role_task(partition_key_))) {
      CLOG_LOG(WARN, "ps_cb->submit_pt_update_task failed", K_(partition_key), K(ret));
    } else if (restore_mgr_->is_standby_restore_state() && STANDBY_LEADER == role_) {
      // standby_leader need trigger restore
      if (OB_FAIL(partition_service_->push_into_migrate_retry_queue(partition_key_, storage::RETRY_STANDBY_RESTORE))) {
        CLOG_LOG(WARN, "push_into_migrate_retry_queue failed", K_(partition_key), K(ret));
        // trigger restore failed, need revoke
        const uint32_t revoke_type = ObElection::RevokeType::STANDBY_RESTORE_FAIL;
        revoke_leader_(revoke_type);
      }
    } else {
    }

    if (STANDBY_LEADER == role_ && restore_mgr_->is_standby_restore_state() && !primary_leader_.is_valid()) {
      (void)try_update_leader_from_loc_cache();
    }
  } else {
    const int64_t before_active = ObTimeUtility::current_time();
    if (OB_FAIL(partition_service_->on_leader_active(partition_key_, role_, is_elected_by_changing_leader_))) {
      CLOG_LOG(WARN, "ps_cb->on_leader_active failed!", K_(partition_key), K(ret));
    }
    const int64_t diff = ObTimeUtility::current_time() - before_active;
    if (diff >= 1000) {
      CLOG_LOG(
          WARN, "ps_cb->on_leader_active cost too much time", K_(partition_key), K(before_active), "cost_time", diff);
    }
    EVENT_INC(CLOG_TO_LEADER_ACTIVE_COUNT);
    EVENT_ADD(CLOG_TO_LEADER_ACTIVE_TIME, ObTimeUtility::current_time() - reconfirm_start_time_);
  }
  CLOG_LOG(INFO, "on_leader_active finished!", K_(partition_key), K(role_), K(ret));
  return ret;
}

int ObLogStateMgr::on_leader_revoke_()
{
  int ret = OB_SUCCESS;
  if (LEADER != role_) {
    // only LEADER need call on_leader_revoke
    role_change_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(partition_service_->submit_pt_update_role_task(partition_key_))) {
      CLOG_LOG(WARN, "ps_cb->submit_pt_update_task failed", K_(partition_key), K(ret));
    }
  } else {
    int64_t before_revoke = ObTimeUtility::current_time();
    if (OB_FAIL(partition_service_->on_leader_revoke(partition_key_, role_))) {
      CLOG_LOG(WARN, "ps_cb->on_leader_revoke failed!", K_(partition_key), K(ret));
    } else {
      role_change_time_ = ObTimeUtility::current_time();
    }
    int64_t after_revoke = ObTimeUtility::current_time();
    if (after_revoke - before_revoke >= 5 * 1000) {
      CLOG_LOG(WARN,
          "partition_service_cb->on_leader_revoke cost too much time",
          K_(partition_key),
          K(before_revoke),
          K(after_revoke),
          "cost_time",
          after_revoke - before_revoke);
    }
  }
  CLOG_LOG(INFO, "on_leader_revoke finished", K_(partition_key), K(ret));
  return ret;
}

bool ObLogStateMgr::follower_replay_need_switch_()
{
  bool state_changed = false;
  if (scan_disk_log_finished_) {
    state_changed = true;
  }
  return state_changed;
}

bool ObLogStateMgr::leader_revoking_need_switch_()
{
  bool state_changed = false;
  if (LEADER != role_) {
    // only LEADER need check partition state
    state_changed = true;
  } else if (partition_service_->is_revoke_done(partition_key_)) {
    state_changed = true;
    EVENT_INC(ON_LEADER_REVOKE_COUNT);
    EVENT_ADD(ON_LEADER_REVOKE_TIME, ObTimeUtility::current_time() - on_leader_revoke_start_time_);
  } else {
    bool is_role_change_timeout = false;
    check_role_change_warn_interval_(is_role_change_timeout);
    if (is_role_change_timeout) {
      // revoke timeout, notify election to revoke
      const uint32_t revoke_type = ObElection::RevokeType::ROLE_CHANGE_TIMEOUT;
      revoke_leader_(revoke_type);
    }
  }
  return state_changed;
}

bool ObLogStateMgr::follower_active_need_switch_()
{
  bool state_changed = false;
  if (need_update_leader_()) {
    state_changed = true;
  }
  return state_changed;
}

bool ObLogStateMgr::leader_reconfirm_need_switch_()
{
  int state_changed = false;
  int ret = OB_SUCCESS;
  if (is_reconfirm_initialize_()) {
    state_changed = true;
  } else if (is_reconfirm_role_change_or_sync_timeout_()) {
    state_changed = true;
  } else if (is_reconfirm_not_eagain_(ret)) {
    state_changed = true;
  } else {
  }
  return state_changed;
}

int ObLogStateMgr::get_pending_replay_count_(int64_t& pending_replay_cnt) const
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard guard;
  storage::ObIPartitionGroup* partition = NULL;
  storage::ObReplayStatus* replay_status = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogStateMgr is not inited", K(ret), K(partition_key_));
  } else if (OB_FAIL(partition_service_->get_partition(partition_key_, guard)) ||
             NULL == (partition = guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "this partition not existed", K(ret), K(partition_key_));
  } else if (NULL == (replay_status = partition->get_replay_status())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get_replay_status return null", K(ret), K(partition_key_));
  } else {
    pending_replay_cnt = replay_status->get_pending_task_count();
  }
  return ret;
}

bool ObLogStateMgr::is_reconfirm_role_change_or_sync_timeout_()
{
  bool bool_ret = false;
  if (need_update_leader_()) {
    bool_ret = true;
  } else {
    const uint64_t start_id = sw_->get_start_id();
    const int64_t now = ObTimeUtility::current_time();
    if (last_check_start_id_ != start_id) {
      last_check_start_id_ = start_id;
      last_check_start_id_time_ = now;
      last_wait_standby_ack_start_time_ = OB_INVALID_TIMESTAMP;
    } else {
      int tmp_ret = OB_SUCCESS;
      bool is_waiting_standby_ack = false;
      // 1) primary leader counts the time waiting for the standby ack
      bool unused_bool = false;
      (void)primary_leader_check_start_log_state_(start_id, is_waiting_standby_ack, unused_bool);
      // 2) check if log sync timeout
      bool is_wait_replay_timeout = false;
      bool is_sw_timeout = false;
      int64_t pending_replay_cnt = 0;
      if (now - last_check_start_id_time_ > CLOG_LEADER_RECONFIRM_SYNC_TIMEOUT) {
        // start log is waiting more than 10s
        if (!sw_->is_empty()) {
          // sw is not empty, start log sync timeout
          is_sw_timeout = true;
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = get_pending_replay_count_(pending_replay_cnt))) {
          CLOG_LOG(ERROR, "get_pending_replay_count_ failed", K_(partition_key), K(tmp_ret));
          is_wait_replay_timeout = true;
        } else if (OB_INVALID_TIMESTAMP == last_check_pending_replay_cnt_time_ ||
                   last_check_pending_replay_cnt_ != pending_replay_cnt) {
          // replay task number is changing, need wait
          last_check_pending_replay_cnt_ = pending_replay_cnt;
          last_check_pending_replay_cnt_time_ = now;
        } else if (now - last_check_pending_replay_cnt_time_ <= CLOG_LEADER_RECONFIRM_SYNC_TIMEOUT) {
          // not reach timeout
        } else if (pending_replay_cnt <= 0) {
          // no replay task
        } else {
          // replay task number has not changed for more than 10s
          is_wait_replay_timeout = true;
        }
      }

      // self replay hang or not waiting standby ack, need revoke
      bool_ret = (is_wait_replay_timeout || (is_sw_timeout && !is_waiting_standby_ack));

      const uint64_t max_log_id = sw_->get_max_log_id();
      bool is_wait_replay = max_log_id < start_id ? true : false;

      if (bool_ret) {
        // reconfirm timeout, firstly check if partition exist
        bool is_exist = true;
        if (OB_SUCCESS != (tmp_ret = partition_service_->check_partition_exist(partition_key_, is_exist))) {
          CLOG_LOG(WARN, "check_partition_exist failed", K_(partition_key), K(tmp_ret));
        }
        if (is_exist) {
          CLOG_LOG(ERROR,
              "is_reconfirm_role_change_or_sync_timeout_",
              K_(partition_key),
              K(now),
              K(last_check_start_id_time_),
              K(max_log_id),
              K(start_id),
              K(is_wait_replay));
        } else {
          CLOG_LOG(WARN,
              "is_reconfirm_role_change_or_sync_timeout_, partition has been dropped",
              K_(partition_key),
              K(now),
              K(last_check_start_id_time_),
              K(max_log_id),
              K(start_id),
              K(is_wait_replay));
        }
        report_start_id_trace(start_id);
      } else {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(INFO,
              "leader reconfirm need wait",
              K_(partition_key),
              K(last_check_pending_replay_cnt_),
              K(pending_replay_cnt),
              K(now),
              K(last_check_start_id_time_),
              K(max_log_id),
              K(start_id),
              K(is_waiting_standby_ack),
              K(is_wait_replay_timeout));
        }
      }
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::is_reconfirm_initialize_()
{
  return reconfirm_->need_start_up();
}

bool ObLogStateMgr::is_reconfirm_not_eagain_(int& ret)
{
  bool bool_ret = false;
  ret = reconfirm_->reconfirm();
  if (OB_EAGAIN == ret) {
    if (REACH_TIME_INTERVAL(10 * 1000)) {
      CLOG_LOG(INFO, "Reconfirm EAGAIN", K_(partition_key));
    }
  } else if (OB_SUCC(ret)) {
    bool is_finish = false;
    if (OB_FAIL(replay_engine_->is_replay_finished(partition_key_, is_finish))) {
      CLOG_LOG(WARN, "replay engine check replay finish error", K(ret), K(partition_key_));
      bool_ret = true;
    } else {
      ret = (is_finish ? OB_SUCCESS : OB_EAGAIN);
      bool_ret = is_finish;
    }
  } else {
    CLOG_LOG(ERROR, "reconfirm failed", K(partition_key_), K(ret));
    bool_ret = true;
  }
  return bool_ret;
}

bool ObLogStateMgr::leader_taking_over_need_switch_()
{
  bool state_changed = false;
  if (LEADER != role_) {
    // only LEADER need check partition state
    state_changed = true;
  } else if (partition_service_->is_take_over_done(partition_key_)) {
    state_changed = true;
    EVENT_INC(ON_LEADER_TAKEOVER_COUNT);
    EVENT_ADD(ON_LEADER_TAKEOVER_TIME, ObTimeUtility::current_time() - on_leader_takeover_start_time_);
  } else {
    if (0 == mm_->get_replica_property().get_memstore_percent()) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        CLOG_LOG(INFO, "need wait leader takeover for data replica", K(partition_key_));
      }
    } else {
      bool is_role_change_timeout = false;
      check_role_change_warn_interval_(is_role_change_timeout);
      if (is_role_change_timeout) {
        const uint32_t revoke_type = ObElection::RevokeType::ROLE_CHANGE_TIMEOUT;
        revoke_leader_(revoke_type);
      }
    }
  }
  return state_changed;
}

bool ObLogStateMgr::leader_active_need_switch_(bool& is_error)
{
  bool state_changed = false;
  is_error = false;
  if (need_update_leader_()) {
    state_changed = true;
  } else if (LEADER == role_) {
    bool need_switch_leader_to_self = false;
    state_changed = check_leader_sliding_window_not_slide_(need_switch_leader_to_self);
    if (state_changed) {
      is_error = true;
    } else {
      // state_changed is false, check cluster type and status
      // if primary cluster switches to standby, clog need do role chagne: leader->follower->standby_leader
      if (!ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
          GCTX.is_in_standby_active_state()) {
        state_changed = true;
        CLOG_LOG(INFO, "cluster type is standby, leader need revoke", K_(role), K(state_changed), K_(partition_key));
      }
    }

    if (!state_changed && need_switch_leader_to_self) {
      // primary leader switch leader to self, trigger start log reach majority after
      // mode degraded (max_availability).
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = election_->change_leader_to_self_async())) {
        CLOG_LOG(WARN, "change_leader_to_self_async failed", K(tmp_ret), K_(partition_key));
      }
    }
  } else if (STANDBY_LEADER == role_) {
    if (GCTX.is_in_flashback_state() || GCTX.is_primary_cluster() || GCTX.is_in_cleanup_state()) {
      state_changed = true;
    }
  } else {
    // do nothing
  }
  return state_changed;
}

void ObLogStateMgr::primary_leader_check_start_log_state_(
    const uint64_t start_id, bool& is_waiting_standby_ack, bool& need_switch_leader_to_self)
{
  // primary leader check if log sync timeout because of standby cluster in max availablility mode
  if (LEADER == role_ && GCTX.is_in_max_availability_mode() &&
      !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    const int64_t* ref = NULL;
    ObLogTask* log_task = NULL;
    if (OB_SUCCESS == (sw_->get_log_task(start_id, log_task, ref))) {
      log_task->lock();

      if (GCTX.need_sync_to_standby() && log_task->is_local_majority_flushed() &&
          !log_task->is_standby_majority_finished()) {
        is_waiting_standby_ack = true;
        const int64_t net_timeout = GCTX.get_sync_standby_net_timeout();
        const int64_t now = ObTimeUtility::current_time();
        if (OB_INVALID_TIMESTAMP == last_wait_standby_ack_start_time_) {
          // update last_wait_standby_ack_start_time_
          last_wait_standby_ack_start_time_ = now;
        } else if (now - last_wait_standby_ack_start_time_ < net_timeout) {
          // not reach timeout, ignore
        } else {
          (void)GCTX.inc_sync_timeout_partition_cnt();
          if (REACH_TIME_INTERVAL(100 * 1000)) {
            CLOG_LOG(WARN,
                "leader's start log has not received standby_ack for timeout interval",
                K_(partition_key),
                K(start_id),
                K(*log_task),
                K_(last_wait_standby_ack_start_time),
                K(net_timeout));
          }
        }
        CLOG_LOG(DEBUG,
            "start log_task has not received standby_ack",
            K_(partition_key),
            K_(role),
            K_(state),
            K(start_id),
            K(*log_task),
            K_(last_wait_standby_ack_start_time));
      }
      if (!GCTX.need_sync_to_standby() && log_task->is_local_majority_flushed()) {
        // primary leader in max_availability mode, already degraded to max_perf level
        // start log reach majority, need swtich leader to self
        need_switch_leader_to_self = true;
        CLOG_LOG(INFO,
            "start log_task reach majority, primary leader need swtich to self",
            K_(partition_key),
            K_(role),
            K_(state),
            K(start_id),
            K(*log_task));
      }

      log_task->unlock();
    }
    if (NULL != ref) {
      sw_->revert_log_task(ref);
      ref = NULL;
    }
  }
}

int ObLogStateMgr::primary_process_protect_mode_switch()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (LEADER != role_ || ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    // not leader or private tbl, skip
  } else {
    // update last_check_start_id_time_ when mode switches
    // avoid unexpected revoke
    if (OB_INVALID_ID != last_check_start_id_) {
      const int64_t now = ObTimeUtility::current_time();
      last_check_start_id_time_ = now;
    }
    last_wait_standby_ack_start_time_ = OB_INVALID_TIMESTAMP;
  }
  return ret;
}

bool ObLogStateMgr::check_leader_sliding_window_not_slide_(bool& need_switch_leader_to_self)
{
  bool state_changed = false;
  const uint64_t start_id = sw_->get_start_id();
  const int64_t now = ObTimeUtility::current_time();
  if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
    CLOG_LOG(INFO, "check_leader_sliding_window_not_slide_", K_(partition_key));
  }
  if (last_check_start_id_ != start_id) {
    last_check_start_id_ = start_id;
    last_check_start_id_time_ = now;
    last_wait_standby_ack_start_time_ = OB_INVALID_TIMESTAMP;
  } else if (sw_->get_max_log_id() >= start_id) {
    // sw not empty, maybe exception occurs
    const int64_t revoke_wait_time = ObServerConfig::get_instance()._ob_clog_timeout_to_force_switch_leader;
    if (revoke_wait_time > 0) {
      if (now - last_check_start_id_time_ > revoke_wait_time) {
        if (sw_->is_fake_info_need_revoke(start_id, now)) {
          if (GCTX.need_sync_to_standby() && GCTX.is_in_max_availability_mode() &&
              !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
              is_log_local_majority_flushed_(start_id)) {
            // In sync mode, the leader of the non-private table is successful in the majority but has not received the
            // ack of the standby database Need revoke, because the standby replica relies inner sql to renew location
            // cache, which may be blocked by other requests
            if (REACH_TIME_INTERVAL(1000 * 1000)) {
              CLOG_LOG(WARN,
                  "leader_active wait standby_ack for start_id",
                  K_(partition_key),
                  K(now),
                  K(last_check_start_id_time_),
                  "sw max_log_id",
                  sw_->get_max_log_id(),
                  K(start_id));
              report_start_id_trace(start_id);
            }
          } else {
            state_changed = true;
            CLOG_LOG(ERROR,
                "leader_active_need_switch_",
                K_(partition_key),
                K(now),
                K(last_check_start_id_time_),
                "sw max_log_id",
                sw_->get_max_log_id(),
                K(start_id));
            report_start_id_trace(start_id);
          }
        } else {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            CLOG_LOG(WARN,
                "leader_active wait start log sync",
                K_(partition_key),
                K(now),
                K(last_check_start_id_time_),
                "sw max_log_id",
                sw_->get_max_log_id(),
                K(start_id));
            report_start_id_trace(start_id);
          }
        }
      }
    }

    if (now - last_check_start_id_time_ > CLOG_LEADER_ACTIVE_SYNC_TIMEOUT &&
        partition_reach_time_interval(CLOG_LEADER_ACTIVE_SYNC_TIMEOUT, leader_sync_timeout_warn_time_)) {
      report_start_id_trace(start_id);
    }

    if (now - last_check_start_id_time_ > CLOG_FAKE_PUSH_LOG_INTERVAL) {
      // leader's sw blocked more than 2s
      // 1) try send ping msg every 2s
      int tmp_ret = OB_SUCCESS;
      if (now - last_fake_push_time_ > CLOG_FAKE_PUSH_LOG_INTERVAL) {
        ObMemberList list;
        list.reset();
        if (OB_SUCCESS != (tmp_ret = list.deep_copy(mm_->get_curr_member_list()))) {
          CLOG_LOG(WARN, "deep_copy failed", K_(partition_key), K(tmp_ret));
        } else if (list.contains(self_) && OB_SUCCESS != (tmp_ret = list.remove_server(self_))) {
          CLOG_LOG(WARN, "list.remove_server failed", K_(partition_key), K(tmp_ret));
        } else if (!list.is_valid()) {
          // no member, skip
        } else if (OB_SUCCESS != (tmp_ret = log_engine_->submit_fake_push_log_req(
                                      list, partition_key_, start_id, curr_proposal_id_))) {
          CLOG_LOG(WARN, "submit_fake_push_log_req failed", K_(partition_key), K(tmp_ret));
        } else {
          last_fake_push_time_ = now;
          if (REACH_TIME_INTERVAL(100 * 1000)) {
            CLOG_LOG(INFO,
                "submit_fake_push_log_req succ",
                K_(partition_key),
                K(start_id),
                K(last_fake_push_time_),
                K(list));
          }
        }
      }
      // 2) primary leader count time for waiting standby ack in max protection mode
      bool unused_bool = false;
      (void)primary_leader_check_start_log_state_(start_id, unused_bool, need_switch_leader_to_self);
    }
  } else {
    // max_log_id < start_id, sw is empty
    last_check_start_id_time_ = now;
    last_wait_standby_ack_start_time_ = OB_INVALID_TIMESTAMP;
  }
  return state_changed;
}

void ObLogStateMgr::check_and_try_fetch_log_()
{
  if (!is_offline()) {
    const int64_t now = ObClockGenerator::getClock();
    const uint64_t start_id = sw_->get_start_id();
    uint64_t next_log_id = OB_INVALID_ID;
    int64_t next_log_ts = OB_INVALID_TIMESTAMP;
    (void)sw_->get_next_replay_log_id_info(next_log_id, next_log_ts);

    // sw blocks more than 4s, need fetch log from parent
    if (OB_INVALID_ID == last_check_start_id_ || OB_INVALID_TIMESTAMP == last_check_start_id_time_ ||
        (last_check_start_id_ == start_id
            // next_log_ts has not been updated more than 4s, need fetch from parent
            && now - next_log_ts > CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND && need_fetch_log_())) {
      bool is_fetched = false;
      sw_->start_fetch_log_from_leader(is_fetched);
      if (is_fetched) {
        last_check_start_id_ = start_id;
        last_check_start_id_time_ = now;

        do {
          ObSpinLockGuard guard(fetch_state_lock_);
          if (curr_round_first_fetch_) {
            curr_round_fetch_begin_time_ = now;
            curr_round_first_fetch_ = false;
          } else {
            curr_fetch_log_interval_ = std::min(2 * curr_fetch_log_interval_, CLOG_FETCH_LOG_INTERVAL_UPPER_BOUND);
            curr_fetch_window_size_ = 1;
          }
        } while (0);
      }
    } else if (last_check_start_id_ != start_id) {
      last_check_start_id_ = start_id;
      last_check_start_id_time_ = now;
    } else {
    }

    // check parent state
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200 ||
        !ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type())) {
      (void)cascading_mgr_->check_parent_state(now, get_region());
    }
  }
}

bool ObLogStateMgr::need_update_primary_leader_(share::ObCascadMember& new_primary_leader)
{
  bool bool_ret = false;
  if (STANDBY_LEADER == role_) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(standby_get_primary_leader_(new_primary_leader))) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "standby_get_primary_leader_ failed", K_(partition_key), K(ret));
      }
      if (primary_leader_.is_valid()) {
        bool_ret = true;
      }
    } else if (!new_primary_leader.is_valid()) {
      // new_primary_leader maybe invalid, skip
    } else if (new_primary_leader == primary_leader_) {
      // not changed, skip
    } else {
      bool_ret = true;
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::need_update_leader_()
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  ObCascadMember new_leader;
  ObAddr new_elect_real_leader;  // ignore it
  ObAddr previous_leader;        // ignore it
  int64_t new_leader_epoch = OB_INVALID_TIMESTAMP;

  if (OB_FAIL(get_elect_leader_(new_leader, new_elect_real_leader, previous_leader, new_leader_epoch))) {
    if (OB_ELECTION_WARN_INVALID_LEADER != ret) {
      // to avoid too much log while election has no leader
      CLOG_LOG(WARN, "get_elect_leader_ failed", K_(partition_key), K(ret), "replica_type", mm_->get_replica_type());
    }
    if (leader_.is_valid()) {
      // leader is valid now but get failed, return true
      bool_ret = true;
    }
  } else {
    if (new_leader.get_server() != leader_.get_server() || new_leader_epoch != leader_epoch_) {
      bool_ret = true;
    }
  }

  return bool_ret;
}

bool ObLogStateMgr::follower_need_update_role_(
    ObCascadMember& new_leader, ObAddr& elect_real_leader, ObAddr& previous_leader, int64_t& new_leader_epoch)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_elect_leader_(new_leader, elect_real_leader, previous_leader, new_leader_epoch))) {
    if (OB_ELECTION_WARN_INVALID_LEADER != ret) {
      CLOG_LOG(WARN, "get_elect_leader_ failed", K_(partition_key), K(ret));
    }
    new_leader.reset();
    new_leader_epoch = OB_INVALID_TIMESTAMP;
  } else {
    bool_ret = (self_ == new_leader.get_server());
  }
  return bool_ret;
}

void ObLogStateMgr::set_leader_and_epoch_(const ObCascadMember& new_leader, const int64_t new_leader_epoch)
{
  // update leader_
  leader_ = new_leader;
  leader_epoch_ = new_leader_epoch;
  reset_fetch_state();
}

void ObLogStateMgr::set_primary_leader_(const share::ObCascadMember& new_primary_leader)
{
  // update primary_leader_
  const share::ObCascadMember old_primary_leader = primary_leader_;
  primary_leader_ = new_primary_leader;
  if (REACH_TIME_INTERVAL(100 * 1000)) {
    CLOG_LOG(INFO, "update primary_leader", K_(partition_key), K(old_primary_leader), K_(primary_leader));
  }
}

int ObLogStateMgr::update_freeze_version(const common::ObVersion& freeze_version)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  CLOG_LOG(DEBUG,
      "update_freeze_version",
      K_(partition_key),
      "current freeze_version",
      freeze_version_,
      "new freeze_version",
      freeze_version);
  if (freeze_version > freeze_version_) {
    freeze_version_ = freeze_version;
  }
  return ret;
}

void ObLogStateMgr::report_start_id_trace(const uint64_t start_id)
{
  int ret = OB_SUCCESS;
  const int64_t* ref = NULL;
  ObLogTask* log_task = NULL;
  if (OB_SUCC(sw_->get_log_task(start_id, log_task, ref))) {
    log_task->lock();
    // log_task->report_trace();
    CLOG_LOG(INFO, "current log_task status", K_(partition_key), K_(role), K_(state), K(start_id), K(*log_task));
    log_task->unlock();
  } else {
    CLOG_LOG(WARN, "report_start_id_trace failed", K_(partition_key), K(ret), K(start_id));
  }
  if (NULL != ref) {
    sw_->revert_log_task(ref);
    ref = NULL;
  }
  return;
}

bool ObLogStateMgr::is_log_local_majority_flushed_(const uint64_t log_id)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  ObLogTask* task = NULL;
  const int64_t* ref = NULL;

  if (OB_FAIL(sw_->get_log_task(log_id, task, ref))) {
    if (OB_ERR_NULL_VALUE == ret) {
      bool_ret = false;
    } else if (OB_ERROR_OUT_OF_RANGE == ret) {
      bool_ret = true;
    } else {
      CLOG_LOG(WARN,
          "get log task error",
          K(ret),
          K(partition_key_),
          K(log_id),
          "start_id",
          sw_->get_start_id(),
          "max_log_id",
          sw_->get_max_log_id());
    }
  } else {
    task->lock();
    bool_ret = task->is_local_majority_flushed();
    task->unlock();
  }

  if (NULL != ref) {
    sw_->revert_log_task(ref);
    ref = NULL;
    task = NULL;
  }
  return bool_ret;
}

bool ObLogStateMgr::is_changing_leader() const
{
  return is_changing_leader_ == true;
}

int ObLogStateMgr::change_leader_async(
    const common::ObPartitionKey& partition_key, const ObAddr& leader, ObTsWindows& changing_leader_windows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) &&
             OB_FAIL(election_->change_leader_async(leader, changing_leader_windows))) {
    if (OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE == ret) {
      CLOG_LOG(WARN, "change leader too frequently", K(ret), K(partition_key), K(leader));
    } else if (OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER == ret) {
      CLOG_LOG(WARN, "change leader while current server not leader", K(ret), K(partition_key), K(leader));
    } else {
      CLOG_LOG(WARN, "change_leader_async failed", K(partition_key), K(ret), K(leader));
    }
  } else {
    is_changing_leader_ = true;
  }
  return ret;
}

int ObLogStateMgr::revoke_leader_(const uint32_t revoke_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type())) {
    election_->leader_revoke(revoke_type);
  } else {
    // do nothing
  }
  return ret;
}

int ObLogStateMgr::get_pls_epoch(int64_t& pls_epoch) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    pls_epoch = pls_epoch_;
  }
  return ret;
}

ObVersion ObLogStateMgr::get_freeze_version() const
{
  ObSpinLockGuard guard(lock_);
  return freeze_version_;
}

void ObLogStateMgr::check_role_change_warn_interval_(bool& is_role_change_timeout) const
{
  const int64_t current = ObTimeUtility::current_time();
  if (current - role_change_time_ > ROLE_CHANGE_WARN_INTERVAL) {
    is_role_change_timeout = true;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(ERROR,
          "role change cost too much time",
          K_(partition_key),
          K(current),
          K_(role_change_time),
          "interval",
          current - role_change_time_,
          K_(role),
          K_(state));
    }
  }
}

int ObLogStateMgr::get_elect_leader_(
    ObCascadMember& leader, ObAddr& elect_real_leader, ObAddr& previous_leader, int64_t& leader_epoch)
{
  // this func only check local cluster leader
  int ret = OB_SUCCESS;
  ObAddr tmp_addr;
  int64_t tmp_cluster_id = OB_INVALID_CLUSTER_ID;
  const bool is_paxos_replica = ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type());

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogStateMgr is not inited", K_(partition_key), K(ret));
  } else if (NULL != election_ && is_paxos_replica &&
             OB_SUCC(election_->get_leader(tmp_addr, previous_leader, leader_epoch, is_elected_by_changing_leader_))) {
    elect_real_leader = tmp_addr;
    tmp_cluster_id = get_self_cluster_id();
    leader.set_server(tmp_addr);
    leader.set_cluster_id(tmp_cluster_id);
  } else {
    tmp_addr.reset();
    leader.reset();
    leader_epoch = OB_INVALID_TIMESTAMP;

    // get leader from location cache without renew
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = partition_service_->nonblock_get_leader_by_election_from_loc_cache(
                           partition_key_, tmp_addr, false))) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "nonblock_get_leader_by_election_from_loc_cache failed", K_(partition_key), K(tmp_ret));
      }
    } else {
      tmp_cluster_id = get_self_cluster_id();
      leader.set_server(tmp_addr);
      leader.set_cluster_id(tmp_cluster_id);
    }

    if (OB_SUCCESS == tmp_ret) {
      if (self_ == leader.get_server()) {
        // location cache's leader is self, but election returns failure, need update leader
        // reset output leader
        leader.reset();
      } else if (mm_->get_curr_member_list().contains(leader.get_server())) {
        // CLOG_LOG(WARN, "curr_leader is in curr_member_list, not expected election error", K(ret),
        //         K_(partition_key), "curr_member_list", mm_->get_curr_member_list(), K(leader),
        //         K_(self));
        // leader.reset();
        ret = OB_SUCCESS;
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObLogStateMgr::standby_get_primary_leader_(share::ObCascadMember& new_primary_leader)
{
  int ret = OB_SUCCESS;

  if (STANDBY_LEADER == role_) {
    const int64_t now = ObTimeUtility::current_time();
    bool need_renew = false;

    if (!primary_leader_.is_valid()) {
      // only need renew when primary_leader_ is invalid
      need_renew = true;
    }

    if (need_renew) {
      if (OB_INVALID_TIMESTAMP != last_renew_primary_leader_time_ &&
          now - last_renew_primary_leader_time_ < RENEW_LOCATION_TIME_INTERVAL) {
        // not reach renew interval, cannot renew
        need_renew = false;
      }
    }
    if (need_renew) {
      last_renew_primary_leader_time_ = now;
    }

    int tmp_ret = OB_SUCCESS;
    // standby_leader refresh primary leader
    ObAddr new_primary_leader_addr;
    int64_t new_cluster_id = OB_INVALID_CLUSTER_ID;
    if (OB_SUCCESS != (tmp_ret = partition_service_->nonblock_get_strong_leader_from_loc_cache(
                           partition_key_, new_primary_leader_addr, new_cluster_id, need_renew))) {
      new_primary_leader_addr.reset();
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "get_leader_from_loc_cache across cluster failed", K_(partition_key), K(tmp_ret));
      }
    } else if (!new_primary_leader_addr.is_valid() || OB_INVALID_CLUSTER_ID == new_cluster_id) {
      // strong leader returned by location cache is invalid
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN,
            "location strong leader is invalid",
            K_(partition_key),
            K(new_primary_leader_addr),
            K(new_cluster_id));
      }
    } else {
      new_primary_leader.set_server(new_primary_leader_addr);
      new_primary_leader.set_cluster_id(new_cluster_id);
    }
  }

  return ret;
}

int ObLogStateMgr::try_update_leader_from_loc_cache()
{
  int ret = OB_SUCCESS;
  const bool is_paxos_replica = ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type());
  bool need_renew = true;

  int tmp_ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  if (STANDBY_LEADER == role_) {
    // standby_leader refresh primary leader
    if (need_renew && (OB_INVALID_TIMESTAMP == last_renew_primary_leader_time_ ||
                          now - last_renew_primary_leader_time_ >= RENEW_LOCATION_TIME_INTERVAL)) {
      last_renew_primary_leader_time_ = now;
    } else {
      // not reach renew interval, cannot renew
      need_renew = false;
    }

    ObAddr new_primary_leader;
    int64_t new_cluster_id = OB_INVALID_CLUSTER_ID;
    if (OB_SUCCESS != (tmp_ret = partition_service_->nonblock_get_strong_leader_from_loc_cache(
                           partition_key_, new_primary_leader, new_cluster_id, need_renew))) {
      new_primary_leader.reset();
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "get_leader_from_loc_cache across cluster failed", K_(partition_key), K(tmp_ret));
      }
    } else {
      // update primary_leader
      ObCascadMember new_leader_member(new_primary_leader, new_cluster_id);
      (void)set_primary_leader_(new_leader_member);
    }
  } else {
    // update local cluster leader
    if (need_renew &&
        (OB_INVALID_TIMESTAMP == last_renew_loc_time_ || now - last_renew_loc_time_ >= RENEW_LOCATION_TIME_INTERVAL)) {
      last_renew_loc_time_ = now;
    } else {
      // not reach renew interval, cannot renew
      need_renew = false;
    }

    ObAddr new_leader;
    int64_t new_cluster_id = OB_INVALID_CLUSTER_ID;
    if (OB_SUCCESS != (tmp_ret = partition_service_->nonblock_get_leader_by_election_from_loc_cache(
                           partition_key_, new_leader, need_renew))) {
      new_leader.reset();
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "nonblock_get_leader_by_election_from_loc_cache failed", K_(partition_key), K(tmp_ret));
      }
    } else {
      new_cluster_id = get_self_cluster_id();
      ObCascadMember new_leader_member(new_leader, new_cluster_id);
      leader_ = new_leader_member;
    }
  }

#if !defined(NDEBUG)
  if (REACH_TIME_INTERVAL(100 * 1000)) {
    CLOG_LOG(INFO, "try update leader from loc cache", K(ret), K_(partition_key), K_(leader), K_(primary_leader));
  }
#endif
  return ret;
}

int ObLogStateMgr::async_get_dst_cluster_leader_from_loc_cache(
    const bool is_need_renew, const int64_t cluster_id, ObAddr& leader)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_INVALID_CLUSTER_ID == cluster_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(partition_service_->nonblock_get_leader_by_election_from_loc_cache(
                 partition_key_, cluster_id, leader, is_need_renew))) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN,
          "nonblock_get_leader_by_election_from_loc_cache failed",
          K(ret),
          K_(partition_key),
          K(is_need_renew),
          K(cluster_id));
    }
  } else {
    // do nothing
  }

  return ret;
}

int64_t ObLogStateMgr::get_self_cluster_id() const
{
  return obrpc::ObRpcNetHandler::CLUSTER_ID;
}

int64_t ObLogStateMgr::get_leader_cluster_id() const
{
  return leader_.get_cluster_id();
}

void ObLogStateMgr::set_offline()
{
  CLOG_LOG(INFO, "set_offline", K_(partition_key));
  ATOMIC_STORE(&is_offline_, true);
}

void ObLogStateMgr::set_online()
{
  CLOG_LOG(INFO, "set_online", K_(partition_key));
  ATOMIC_STORE(&is_offline_, false);
}

int ObLogStateMgr::set_follower_active()
{
  int ret = OB_SUCCESS;
  replay_to_follower_active_();
  return ret;
}

int ObLogStateMgr::check_role_leader_and_state() const
{
  int ret = OB_SUCCESS;
  const int64_t tmp = ATOMIC_LOAD(&tmp_val_);
  if ((tmp >> 48) != 0) {  // check is_removed_
    ret = OB_NOT_MASTER;
  } else if (((tmp & ((1ll << 48) - 1)) >> 32) != ACTIVE) {  // check state_
    ret = OB_NOT_MASTER;
  } else if ((tmp & ((1ll << 32) - 1)) != LEADER) {  // check role_
    ret = OB_NOT_MASTER;
  }
  return ret;
}

int ObLogStateMgr::check_role_elect_leader_and_state() const
{
  int ret = OB_SUCCESS;
  const int64_t tmp = ATOMIC_LOAD(&tmp_val_);
  if ((tmp >> 48) != 0) {  // check is_removed_
    ret = OB_NOT_MASTER;
  } else if (((tmp & ((1ll << 48) - 1)) >> 32) != ACTIVE) {  // check state_
    ret = OB_NOT_MASTER;
  } else if ((tmp & ((1ll << 32) - 1)) != LEADER && (tmp & ((1ll << 32) - 1)) != STANDBY_LEADER) {  // check role_
    ret = OB_NOT_MASTER;
  }
  return ret;
}

bool ObLogStateMgr::can_set_offline() const
{
  // set_offline is not allowed in leader_active or leader_takeing_over
  // replica may replay offline log in leader_reconfirm state
  return (is_follower_replay_() || is_leader_revoking_() || is_standby_leader_revoking_() || is_follower_active_() ||
          is_leader_reconfirm_() || is_standby_leader_reconfirm_() || is_standby_leader_active_());
}

bool ObLogStateMgr::can_set_replica_type() const
{
  return is_follower_active_() || is_follower_replay_() || is_standby_leader_active_();
}

bool ObLogStateMgr::is_replaying() const
{
  return (is_follower_replay_() || is_follower_active_() || is_leader_reconfirm_() || is_standby_leader_reconfirm_());
}

int ObLogStateMgr::enable_replay()
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "enable_replay", K_(partition_key));
  ATOMIC_STORE(&replay_enabled_, true);
  return ret;
}

int ObLogStateMgr::disable_replay()
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO, "disable_replay", K_(partition_key));
  ATOMIC_STORE(&replay_enabled_, false);
  return ret;
}

bool ObLogStateMgr::is_replay_enabled() const
{
  return true == ATOMIC_LOAD(&replay_enabled_);
}

bool ObLogStateMgr::can_backfill_log(const bool is_leader, const common::ObProposalID& proposal_id) const
{
  bool bool_ret = true;
  if (!is_offline()) {
    if (proposal_id != curr_proposal_id_) {
      bool_ret = false;
    } else if (is_leader) {
      bool_ret = is_leader_active_();
    } else {
      bool_ret = is_follower_active_();
    }
  } else {
    bool_ret = false;
  }

  return bool_ret;
}

bool ObLogStateMgr::can_backfill_confirmed(const common::ObProposalID& proposal_id) const
{
  bool bool_ret = true;
  if (!is_offline()) {
    if (proposal_id != curr_proposal_id_) {
      bool_ret = false;
    } else {
      bool_ret = is_leader_active_();
    }
  } else {
    bool_ret = false;
  }

  return bool_ret;
}

int ObLogStateMgr::set_election_candidates(
    const int64_t replica_num, const ObMemberList& member_list, const int64_t membership_timestamp)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (replica_num <= 0 || !member_list.is_valid() || OB_INVALID_TIMESTAMP == membership_timestamp) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(
        WARN, "invalid argument", K_(partition_key), K(ret), K(replica_num), K(member_list), K(membership_timestamp));
  } else if (OB_FAIL(election_->set_candidate(replica_num, member_list, membership_timestamp))) {
    CLOG_LOG(WARN,
        "set_candidate failed",
        K_(partition_key),
        K(ret),
        K(replica_num),
        K(member_list),
        K(membership_timestamp));
  } else {
    // do nothing
  }

  return ret;
}

int ObLogStateMgr::set_election_replica_num(const int64_t replica_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (OB_FAIL(election_->set_replica_num(replica_num))) {
    CLOG_LOG(WARN, "set_replica_num failed", K_(partition_key), K(ret));
  } else {
    CLOG_LOG(INFO, "set_replica_num success", K_(partition_key), K(replica_num));
  }
  return ret;
}

int ObLogStateMgr::inc_election_replica_num()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (OB_FAIL(election_->inc_replica_num())) {
    CLOG_LOG(WARN, "inc_replica_num failed", K_(partition_key), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogStateMgr::dec_election_replica_num()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (OB_FAIL(election_->dec_replica_num())) {
    CLOG_LOG(WARN, "dec_replica_num failed", K_(partition_key), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogStateMgr::get_election_valid_candidates(ObMemberList& valid_candidate_list) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (OB_FAIL(election_->get_valid_candidate(valid_candidate_list))) {
    CLOG_LOG(WARN, "get_valid_candidate failed", K_(partition_key), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogStateMgr::get_election_leader(
    ObAddr& leader, int64_t& leader_epoch, ObTsWindows& changing_leader_windows) const
{
  int ret = OB_SUCCESS;
  bool is_elected_by_changing_leader = false;
  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (OB_FAIL(
                 election_->get_leader(leader, leader_epoch, is_elected_by_changing_leader, changing_leader_windows))) {
    CLOG_LOG(WARN, "get_leader failed", K_(partition_key), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogStateMgr::get_election_leader(ObAddr& leader, int64_t& leader_epoch) const
{
  int ret = OB_SUCCESS;
  ObAddr previous_leader;
  bool is_elected_by_changing_leader = false;
  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K_(partition_key), K(ret));
  } else if (OB_FAIL(election_->get_leader(leader, previous_leader, leader_epoch, is_elected_by_changing_leader))) {
    CLOG_LOG(WARN, "get_leader failed", K_(partition_key), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

bool ObLogStateMgr::is_leader_revoke_recently() const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "election_ is NULL", K(partition_key_), K(ret));
  } else {
    const int64_t last_leader_revoke_time = election_->get_last_leader_revoke_time();
    const int64_t remaining_ts = ObServerConfig::get_instance().election_blacklist_interval -
                                 (ObTimeUtility::current_time() - last_leader_revoke_time);
    if (remaining_ts > 0) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

bool ObLogStateMgr::is_archive_restoring() const
{
  bool bool_ret = false;
  if (!is_inited_) {
    // not inited
  } else {
    bool_ret = restore_mgr_->is_archive_restoring();
  }
  return bool_ret;
}

bool ObLogStateMgr::is_archive_restore_leader() const
{
  bool bool_ret = false;
  if (!is_inited_) {
    // not inited
  } else {
    bool_ret = (RESTORE_LEADER == restore_mgr_->get_role());
  }
  return bool_ret;
}

bool ObLogStateMgr::is_archive_restoring_log() const
{
  bool bool_ret = false;
  if (!is_inited_) {
    // not inited
  } else {
    bool_ret = restore_mgr_->is_archive_restoring_log();
  }
  return bool_ret;
}

int ObLogStateMgr::get_cascad_leader(ObCascadMember& cascad_leader_member) const
{
  // 1) for standby_leader, return primary_leader.
  // 2) others, return local leader;
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (STANDBY_LEADER == role_) {
    cascad_leader_member = primary_leader_;
  } else {
    cascad_leader_member = leader_;
  }
  return ret;
}

int ObLogStateMgr::standby_set_election_leader(const common::ObAddr& leader, const int64_t lease_start)
{
  int ret = OB_SUCCESS;
  int64_t leader_epoch = OB_INVALID_TIMESTAMP;

  if (!leader.is_valid() || lease_start <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K_(partition_key), K(leader), K(lease_start));
  } else if (OB_ISNULL(election_)) {
    ret = OB_ERR_UNEXPECTED;
    ;
    CLOG_LOG(ERROR, "election_ is NULL", K(ret), K_(partition_key));
  } else if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type())) {
    if (OB_FAIL(election_->leader_takeover(leader, lease_start, leader_epoch))) {
      CLOG_LOG(WARN, "leader_takeover failed", K_(partition_key), K(ret), K(leader), K(lease_start));
    }
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    if (OB_INVALID_TIMESTAMP != leader_epoch) {
      if (leader == self_) {
        leader_epoch_ = leader_epoch;
      } else {
        leader_epoch_ = OB_INVALID_TIMESTAMP;
      }
    }
  }

  CLOG_LOG(INFO, "standby_set_election_leader finished", K_(partition_key), K(ret), K(leader), K(lease_start));
  return ret;
}

int ObLogStateMgr::standby_update_protection_level()
{
  int ret = OB_SUCCESS;
  common::ObProtectionMode cur_mode = GCTX.get_protection_mode();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (GCTX.is_primary_cluster() || ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    // primary cluster or private tbl, skip
  } else {
    ObSpinLockGuard guard(standby_sync_lock_);
    if (GCTX.is_sync_level_on_standby()) {
      if (MAXIMUM_PERFORMANCE_LEVEL == standby_protection_level_) {
        standby_protection_level_ = RESYNCHRONIZATION_LEVEL;
        standby_sync_start_id_ = OB_INVALID_ID;
        if (STANDBY_LEADER == role_) {
          const share::ObCascadMember primary_leader = primary_leader_;
          const share::ObCascadMember parent = cascading_mgr_->get_parent();
          if (primary_leader.is_valid() && primary_leader == parent) {
            // no need reset parent
          } else {
            const uint32_t reset_type = ObLogCascadingMgr::ResetPType::STANDBY_PROTECTION_MODE_SWITCH;
            cascading_mgr_->reset_parent(reset_type);
          }
          int tmp_ret = OB_SUCCESS;
          if (OB_INVALID_ID == standby_sync_start_id_ && OB_SUCCESS != (tmp_ret = try_send_sync_start_id_request_())) {
            CLOG_LOG(WARN, "try_send_sync_start_id_request_ failed", K(tmp_ret), K_(partition_key));
          }
        }
      }
    } else {
      standby_protection_level_ = MAXIMUM_PERFORMANCE_LEVEL;
    }
  }
  CLOG_LOG(INFO,
      "standby_update_protection_level finished",
      K_(partition_key),
      K(ret),
      K(cur_mode),
      K_(standby_protection_level),
      K_(standby_sync_start_id));

  return ret;
}

int ObLogStateMgr::try_send_sync_start_id_request_()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  ObCascadMember primary_leader = get_primary_leader();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!primary_leader.is_valid()) {
    (void)try_update_leader_from_loc_cache();
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN,
          "primary_leader is invalid when try_send_sync_start_id_request_",
          K(ret),
          K_(partition_key),
          K(primary_leader));
    }
  } else if (OB_INVALID_TIMESTAMP == last_sync_start_id_request_time_ ||
             now - last_sync_start_id_request_time_ >= STANDBY_CHECK_SYNC_START_ID_INTERVAL) {
    if (OB_FAIL(log_engine_->standby_query_sync_start_id(
            primary_leader.get_server(), primary_leader.get_cluster_id(), partition_key_, now))) {
      CLOG_LOG(WARN, "standby_query_sync_start_id failed", K(ret), K_(partition_key), K(primary_leader));
    } else {
      last_sync_start_id_request_time_ = now;  // record last send_ts
    }
  } else {
  }

  CLOG_LOG(INFO,
      "try_send_sync_start_id_request_ finished",
      K(ret),
      K_(partition_key),
      K(primary_leader),
      K(last_sync_start_id_request_time_),
      K(standby_sync_start_id_));
  return ret;
}

int ObLogStateMgr::standby_leader_check_protection_level()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    const uint64_t max_confirmed_log_id = sw_->get_max_confirmed_log_id();
    ObSpinLockGuard guard(standby_sync_lock_);
    if (RESYNCHRONIZATION_LEVEL == standby_protection_level_ && STANDBY_LEADER == role_) {
      if (OB_INVALID_ID == standby_sync_start_id_) {
        // standby_sync_start_id is invalid, need get from primary cluster
        if (OB_FAIL(try_send_sync_start_id_request_())) {
          CLOG_LOG(WARN, "try_send_sync_start_id_request_ failed", K(ret), K_(partition_key));
        }
      } else if (max_confirmed_log_id >= standby_sync_start_id_) {
        standby_protection_level_ = MAXIMUM_PROTECTION_LEVEL;
        CLOG_LOG(INFO,
            "swith protection_level to max_protection",
            K_(partition_key),
            K_(standby_protection_level),
            K_(standby_sync_start_id),
            K(max_confirmed_log_id));
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObLogStateMgr::handle_sync_start_id_resp(
    const ObAddr& server, const int64_t cluster_id, const int64_t original_send_ts, const uint64_t sync_start_id)
{
  int ret = OB_SUCCESS;
  uint64_t max_confirmed_log_id = OB_INVALID_ID;

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
  } else if (original_send_ts != last_sync_start_id_request_time_) {
    // compare send_ts
    CLOG_LOG(WARN,
        "original_send_ts is not match with local val",
        K_(partition_key),
        K(server),
        K(original_send_ts),
        K(sync_start_id),
        K_(last_sync_start_id_request_time));
  } else if (get_self_cluster_id() == cluster_id || !GCTX.is_sync_level_on_standby() || STANDBY_LEADER != role_) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN,
        "recv unexpected sync_start_id resp",
        K(ret),
        K_(partition_key),
        K(server),
        K(cluster_id),
        K(role_),
        "mode",
        GCTX.get_protection_mode());
  } else {
    max_confirmed_log_id = sw_->get_max_confirmed_log_id();
    ObSpinLockGuard guard(standby_sync_lock_);
    if (RESYNCHRONIZATION_LEVEL != standby_protection_level_) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN,
          "recv sync_start_id resp not in resync level",
          K(ret),
          K_(partition_key),
          K(server),
          K(cluster_id),
          K_(standby_protection_level));
    } else {
      standby_sync_start_id_ = sync_start_id;
      if (max_confirmed_log_id >= standby_sync_start_id_) {
        // update protection_level
        standby_protection_level_ = MAXIMUM_PROTECTION_LEVEL;
      }
    }
  }

  CLOG_LOG(INFO,
      "handle_sync_start_id_resp finished",
      K_(partition_key),
      K(ret),
      K(server),
      K(cluster_id),
      K(sync_start_id),
      K_(standby_sync_start_id),
      K(max_confirmed_log_id),
      K_(standby_protection_level));
  return ret;
}

int ObLogStateMgr::get_standby_protection_level(uint32_t& protection_level) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObSpinLockGuard guard(standby_sync_lock_);
    protection_level = standby_protection_level_;
  }
  return ret;
}

int ObLogStateMgr::reset_standby_protection_level_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObSpinLockGuard guard(standby_sync_lock_);
    standby_protection_level_ = MAXIMUM_PERFORMANCE_LEVEL;
    CLOG_LOG(INFO, "reset_standby_protection_level_ finished", K_(partition_key), K_(standby_protection_level));
  }
  return ret;
}

bool ObLogStateMgr::need_fetch_log_() const
{
  bool bool_ret = false;
  const int64_t now = ObClockGenerator::getClock();
  if (OB_INVALID_ID == last_check_start_id_ || OB_INVALID_TIMESTAMP == last_check_start_id_time_) {
    bool_ret = true;
  } else {
    bool_ret = ((now - last_check_start_id_time_) >= CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND);
  }
  return bool_ret;
}

void ObLogStateMgr::reset_fetch_state_()
{
  last_fetched_max_log_id_ = OB_INVALID_ID;
  curr_fetch_log_interval_ = CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND;
  curr_round_first_fetch_ = true;
  curr_round_fetch_begin_time_ = OB_INVALID_TIMESTAMP;
  curr_fetch_window_size_ = 1;
}

void ObLogStateMgr::reset_fetch_state()
{
  ObSpinLockGuard guard(fetch_state_lock_);
  reset_fetch_state_();
}

void ObLogStateMgr::start_next_round_fetch()
{
  ObSpinLockGuard guard(fetch_state_lock_);
  const int64_t now = ObClockGenerator::getClock();
  const int64_t BUFFER_INTERVAL_US = 500 * 1000;
  if (OB_INVALID_TIMESTAMP != curr_round_fetch_begin_time_) {
    if (now - curr_round_fetch_begin_time_ + BUFFER_INTERVAL_US < CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND / 2) {
      curr_fetch_window_size_ = std::min(curr_fetch_window_size_ * 2, CLOG_ST_FETCH_LOG_COUNT * 4);
    } else if (now - curr_round_fetch_begin_time_ > CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND) {
      curr_fetch_window_size_ = std::max(curr_fetch_window_size_ / 2, 1UL);
    } else {
      // do nothing
    }

    curr_fetch_log_interval_ =
        std::min(std::max(now - curr_round_fetch_begin_time_ + BUFFER_INTERVAL_US, CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND),
            CLOG_FETCH_LOG_INTERVAL_UPPER_BOUND);
  }
  curr_round_first_fetch_ = false;
  curr_round_fetch_begin_time_ = now;
}

int ObLogStateMgr::try_reset_fetch_state(const uint64_t push_log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogStateMgr is not inited", K(ret));
  } else if (!is_valid_log_id(push_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(partition_key_), K(push_log_id));
  } else if (OB_INVALID_ID == get_last_fetched_max_log_id()) {
    // do nothing
  } else {
    ObSpinLockGuard guard(fetch_state_lock_);
    if (OB_INVALID_ID != last_fetched_max_log_id_ && push_log_id <= last_fetched_max_log_id_) {
      const uint64_t curr_last_fetched_max_log_id = last_fetched_max_log_id_;
      reset_fetch_state_();
      CLOG_LOG(TRACE,
          "receive push log in fetched window, stop fetching",
          K(partition_key_),
          K(curr_last_fetched_max_log_id),
          K(push_log_id));
    }
  }
  return ret;
}

int ObLogStateMgr::try_set_fetched_max_log_id(const uint64_t max_log_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogStateMgr is not inited", K(ret));
  } else if (!is_valid_log_id(max_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", K(ret), K(partition_key_), K(max_log_id));
  } else {
    ObSpinLockGuard guard(fetch_state_lock_);
    last_fetched_max_log_id_ = max_log_id;
  }
  return ret;
}

uint64_t ObLogStateMgr::get_last_fetched_max_log_id() const
{
  return ATOMIC_LOAD(&last_fetched_max_log_id_);
}

int64_t ObLogStateMgr::get_fetch_log_interval() const
{
  return ATOMIC_LOAD(&curr_fetch_log_interval_);
}

uint64_t ObLogStateMgr::get_fetch_window_size() const
{
  return ATOMIC_LOAD(&curr_fetch_window_size_);
}
}  // namespace clog
}  // namespace oceanbase
