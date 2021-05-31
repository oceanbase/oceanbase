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
#include "ob_log_membership_mgr_V2.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "storage/ob_partition_service.h"
#include "ob_log_callback_engine.h"
#include "ob_log_entry.h"
#include "ob_log_membership_task_mgr.h"
#include "ob_log_sliding_window.h"
#include "ob_log_state_mgr.h"

namespace oceanbase {
using namespace common;
namespace clog {
ObLogMembershipMgr::ObLogMembershipMgr()
    : is_inited_(false),
      need_check_cluster_type_(false),
      sw_(NULL),
      ms_task_mgr_(NULL),
      state_mgr_(NULL),
      cascading_mgr_(NULL),
      cb_engine_(NULL),
      partition_service_(NULL),
      alloc_mgr_(NULL),
      lock_(ObLatchIds::CLOG_MEMBERSHIP_MGR_LOCK),
      ms_state_(MS_INIT),
      curr_member_list_(),
      prev_member_list_(),
      membership_timestamp_(OB_INVALID_TIMESTAMP),
      membership_log_id_(OB_INVALID_ID),
      last_submit_cb_log_id_(OB_INVALID_ID),
      ms_type_(MS_TYPE_UNKNOWN),
      pending_member_for_add_(),
      pending_member_for_remove_(),
      pending_ms_timestamp_(OB_INVALID_TIMESTAMP),
      pending_ms_log_id_(OB_INVALID_ID),
      pending_ms_type_(MS_TYPE_UNKNOWN),
      pending_op_type_(MS_OP_LEADER),
      pending_replica_num_(0),
      on_success_called_(false),
      is_single_replica_mode_(false),
      has_follower_pending_entry_(false),
      replica_type_(REPLICA_TYPE_FULL),
      replica_property_(),
      replica_num_(0),
      self_(),
      partition_key_(),
      follower_pending_entry_(),
      follower_pending_entry_server_(),
      follower_pending_entry_server_cluster_id_(OB_INVALID_CLUSTER_ID),
      follower_pending_entry_type_(RL_TYPE_UNKNOWN)
{}

ObLogMembershipMgr::~ObLogMembershipMgr()
{
  destroy();
}

int ObLogMembershipMgr::init(const ObMemberList& member_list, const int64_t membership_timestamp,
    const uint64_t membership_log_id, const common::ObProposalID& ms_proposal_id, const ObReplicaType replica_type,
    const ObReplicaProperty replica_property, const int64_t replica_num, const bool need_skip_mlist_check,
    const ObAddr& self, const ObPartitionKey& partition_key, ObILogSWForMS* sw, ObLogMembershipTaskMgr* ms_task_mgr,
    ObILogStateMgrForMS* state_mgr, ObLogCascadingMgr* cascading_mgr, ObILogCallbackEngine* cb_engine,
    storage::ObPartitionService* partition_service, ObILogAllocator* alloc_mgr)
{
  int ret = OB_SUCCESS;
  bool is_private_tbl = share::ObMultiClusterUtil::is_cluster_private_table(partition_key.get_table_id());

  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_INVALID_TIMESTAMP == membership_timestamp || OB_INVALID_ID == membership_log_id ||
             !ObReplicaTypeCheck::is_replica_type_valid(replica_type) || !replica_property.is_valid() ||
             replica_num <= 0 || !self.is_valid() || !partition_key.is_valid() || NULL == sw || NULL == ms_task_mgr ||
             NULL == state_mgr || NULL == cascading_mgr || NULL == cb_engine || NULL == partition_service ||
             NULL == alloc_mgr) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR,
        "invalid arguments",
        K(ret),
        K(partition_key),
        K(membership_timestamp),
        K(membership_log_id),
        K(replica_type),
        K(replica_property),
        K(replica_num),
        K(self),
        KP(sw),
        KP(state_mgr),
        KP(cascading_mgr),
        KP(cb_engine),
        KP(partition_service),
        KP(alloc_mgr));
  } else if (!need_skip_mlist_check && OB_FAIL(curr_member_list_.deep_copy(member_list))) {
    CLOG_LOG(WARN, "curr_member_list_ deep_copy failed", K(partition_key), K(ret), K(member_list));
  } else {
    is_inited_ = true;
    sw_ = sw;
    ms_task_mgr_ = ms_task_mgr;
    state_mgr_ = state_mgr;
    cascading_mgr_ = cascading_mgr;
    cb_engine_ = cb_engine;
    partition_service_ = partition_service;
    alloc_mgr_ = alloc_mgr;
    ms_state_ = MS_INIT;
    membership_timestamp_ = membership_timestamp;
    membership_log_id_ = membership_log_id;
    last_submit_cb_log_id_ = membership_log_id;

    ms_proposal_id_ = ms_proposal_id;
    pending_member_for_add_.reset();
    pending_member_for_remove_.reset();
    pending_ms_timestamp_ = OB_INVALID_TIMESTAMP;
    pending_ms_log_id_ = OB_INVALID_ID;
    pending_replica_num_ = 0;
    on_success_called_ = false;
    replica_type_ = replica_type;
    replica_property_ = replica_property;
    replica_num_ = replica_num;
    self_ = self;
    partition_key_ = partition_key;

    CLOG_LOG(INFO, "ObLogMembershipMgr init replica_type", K_(partition_key), K_(replica_type), K(is_private_tbl));
    if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
      if (!is_private_tbl && !ms_proposal_id.is_valid() && !GCTX.is_primary_cluster() &&
          !curr_member_list_.contains(self_)) {
        // When the non-private table's ms_proposal_id is invalid and mlist does not contain itself,
        // only the primary cluster can pass mlist to election
        if (common::INVALID_CLUSTER_TYPE == GCTX.get_cluster_type()) {
          // If the cluster type has not been determined, record the status,
          // and then try to update the election after the type is determined
          need_check_cluster_type_ = true;
        }
      } else {
        set_election_candidate_();
      }
    }
  }
  return ret;
}

void ObLogMembershipMgr::destroy()
{
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
  } else {
    sw_ = NULL;
    ms_task_mgr_ = NULL;
    state_mgr_ = NULL;
    cb_engine_ = NULL;
    partition_service_ = NULL;
    ms_state_ = MS_INIT;
    curr_member_list_.reset();
    prev_member_list_.reset();
    membership_timestamp_ = OB_INVALID_TIMESTAMP;
    membership_log_id_ = OB_INVALID_ID;
    last_submit_cb_log_id_ = OB_INVALID_ID;
    pending_member_for_add_.reset();
    pending_member_for_remove_.reset();
    pending_ms_timestamp_ = OB_INVALID_TIMESTAMP;
    pending_ms_log_id_ = OB_INVALID_ID;
    pending_replica_num_ = 0;
    on_success_called_ = false;
    replica_type_ = REPLICA_TYPE_MAX;
    replica_property_.reset();
    replica_num_ = 0;
    self_.reset();
    partition_key_.reset();
    reset_follower_pending_entry();
    alloc_mgr_ = NULL;
    need_check_cluster_type_ = false;
    is_inited_ = false;
  }
}

bool ObLogMembershipMgr::is_state_changed()
{
  RLockGuard guard(lock_);
  bool bool_ret = false;
  switch (ATOMIC_LOAD(&ms_state_)) {
    case MS_INIT:
      bool_ret = check_follower_pending_entry_();
      if (!bool_ret) {
        // also return true when it need check cluster_type
        bool_ret = need_check_cluster_type_;
      }
      break;
    case MS_PENDING:
      bool_ret = check_barrier_condition_(pending_ms_log_id_);
      break;
    case MS_CHANGING:
      bool_ret = check_on_success_called_();
      break;
    default:
      break;
  }
  return bool_ret;
}

int ObLogMembershipMgr::force_set_replica_num(const int64_t replica_num)
{
  int ret = OB_SUCCESS;
  CLOG_LOG(INFO,
      "begin force_set_replica_num",
      K_(partition_key),
      "curr_replica_num",
      replica_num_,
      "new_replica_num",
      replica_num);
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key));
  } else if (replica_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "replica_num is invalid", K_(partition_key), K(ret), K(replica_num), K_(replica_num));
  } else if (replica_num < curr_member_list_.get_member_number()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN,
        "replica_num val is unexpected",
        K_(partition_key),
        K(ret),
        K(replica_num),
        K_(replica_num),
        K_(curr_member_list));
  } else {
    if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
      ret = state_mgr_->set_election_replica_num(replica_num);
    }
    replica_num_ = replica_num;
    CLOG_LOG(INFO, "force_set_replica_num success", K(ret), K_(partition_key), K_(replica_num), K_(curr_member_list));
  }
  return ret;
}

int ObLogMembershipMgr::force_set_as_single_replica()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key));
  } else {
    ObMemberList new_member_list;
    uint64_t new_membership_log_id = membership_log_id_;
    int64_t new_membership_timestamp = ObTimeUtility::current_time();

    if (OB_FAIL(new_member_list.add_member(ObMember(self_, new_membership_timestamp)))) {
      CLOG_LOG(ERROR, "add_self to member_list fail", K_(partition_key), K(ret));
    } else if (OB_FAIL(prev_member_list_.deep_copy(curr_member_list_))) {
      CLOG_LOG(WARN, "prev_member_list_ deep_copy failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(curr_member_list_.deep_copy(new_member_list))) {
      CLOG_LOG(WARN, "curr_member_list_ deep_copy failed", K_(partition_key), K(ret));
    } else {
      membership_timestamp_ = new_membership_timestamp;
      membership_log_id_ = new_membership_log_id;
      ms_type_ = MS_TYPE_FORCE_MEMBERSHIP;
      is_single_replica_mode_ = true;
      replica_num_ = 1;
      CLOG_LOG(INFO, "replica_type", K_(partition_key), K_(replica_type));
      if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
        set_election_candidate_(true);
      }
      CLOG_LOG(INFO, "force set single_replica", K(ret), K_(partition_key), K_(replica_num), K_(curr_member_list));
    }
  }
  return ret;
}

int ObLogMembershipMgr::force_set_server_list(const obrpc::ObServerList& server_list, const int64_t replica_num)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K(ret), K(partition_key_));
    // } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
    //  ret = OB_ERR_UNEXPECTED;
    //  CLOG_LOG(WARN, "This replica is not paxos replica, ignore force set", K(ret), K(partition_key_));
  } else {
    CLOG_LOG(INFO,
        "before force_set_server_list",
        K(partition_key_),
        K(server_list),
        K(prev_member_list_),
        K(curr_member_list_),
        K(replica_num_));

    ObMemberList new_member_list;
    const uint64_t new_membership_log_id = membership_log_id_;
    const int64_t new_membership_timestamp = membership_timestamp_;

    for (int64_t idx = 0; OB_SUCC(ret) && idx < server_list.count(); idx++) {
      const common::ObAddr& server = server_list[idx];
      if (curr_member_list_.contains(server)) {
        if (OB_FAIL(new_member_list.add_member(ObMember(server, new_membership_timestamp)))) {
          CLOG_LOG(ERROR, "new_member_list add_member failed", K(ret), K(partition_key_), K(server));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (new_member_list.get_member_number() != replica_num) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR,
            "new_member_list does not equal new replica_num",
            K(ret),
            K(partition_key_),
            K(new_member_list),
            K(replica_num));
      } else if (OB_FAIL(prev_member_list_.deep_copy(curr_member_list_))) {
        CLOG_LOG(ERROR,
            "prev_member_list_ deep_copy failed",
            K(ret),
            K(partition_key_),
            K(prev_member_list_),
            K(curr_member_list_));
      } else if (OB_FAIL(curr_member_list_.deep_copy(new_member_list))) {
        CLOG_LOG(ERROR,
            "curr_member_list_ deep_copy failed",
            K(ret),
            K(partition_key_),
            K(curr_member_list_),
            K(new_member_list));
      } else {
        membership_timestamp_ = new_membership_timestamp;
        membership_log_id_ = new_membership_log_id;
        ms_type_ = MS_TYPE_FORCE_MEMBERSHIP;
        replica_num_ = replica_num;
        if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
          set_election_candidate_(true);
        }
      }
    }
    CLOG_LOG(INFO,
        "after force_set_server_list",
        K(ret),
        K(partition_key_),
        K(server_list),
        K(prev_member_list_),
        K(curr_member_list_),
        K(replica_num_));
  }
  return ret;
}

bool ObLogMembershipMgr::check_barrier_condition_(const uint64_t log_id) const
{
  bool bool_ret = false;
  if (IS_NOT_INIT) {
    bool_ret = false;
  } else if (OB_INVALID_ID == log_id || OB_ISNULL(sw_)) {
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(log_id), KP(sw_));
    bool_ret = false;
  } else {
    bool_ret = (sw_->get_next_index_log_id() >= log_id);
  }
  return bool_ret;
}

bool ObLogMembershipMgr::check_on_success_called_()
{
  return ATOMIC_LOAD(&on_success_called_) == true;
}

int ObLogMembershipMgr::switch_state()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    WLockGuard guard(lock_);
    ObMsState saved_ms_state = ATOMIC_LOAD(&ms_state_);
    switch (ATOMIC_LOAD(&ms_state_)) {
      case MS_INIT:
        (void)try_check_cluster_state_();
        ret = handle_follower_pending_entry_();
        break;
      case MS_PENDING:
        ret = pending_to_changing_();
        break;
      case MS_CHANGING:
        ret = changing_to_init_();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
    CLOG_LOG(INFO, "ObLogMembershipMgr::switch_state", K_(partition_key), K(ret), K(saved_ms_state), K_(ms_state));
  }
  return ret;
}

int ObLogMembershipMgr::pending_to_changing_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ret = to_changing_();
  }
  CLOG_LOG(INFO, "pending_to_changing", K_(partition_key), K(ret));
  return ret;
}

int ObLogMembershipMgr::to_changing_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(sw_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!state_mgr_->can_submit_with_assigned_id()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "can_submit_with_assigned_id return false", K_(partition_key), K(ret));
  } else {
    bool is_standby_op = (MS_OP_STANDBY_LEADER == pending_op_type_);
    char buffer[MS_LOG_BUFFER_SIZE];
    ObLogEntryHeader log_entry_header;
    ObMemberList tmp_list;
    ObMemberList prev_tmp_list;
    if (OB_SUCCESS != (tmp_ret = tmp_list.deep_copy(curr_member_list_))) {
      CLOG_LOG(WARN, "tmp_list deep_copy failed", K_(partition_key), K(tmp_ret), K_(curr_member_list));
    }
    if (OB_SUCCESS != (tmp_ret = prev_tmp_list.deep_copy(prev_member_list_))) {
      CLOG_LOG(WARN, "prev_member_list deep_copy failed", K_(partition_key), K(tmp_ret), K_(prev_member_list));
    }
    generate_curr_member_list_();
    if (!is_standby_op) {
      if (OB_SUCCESS != (ret = generate_log_(log_entry_header,
                             buffer,
                             MS_LOG_BUFFER_SIZE,
                             OB_LOG_MEMBERSHIP,
                             pending_ms_log_id_,
                             pending_replica_num_,
                             pending_ms_timestamp_,
                             pending_ms_type_))) {
        CLOG_LOG(WARN, "generate_log failed", K_(partition_key), K(ret));
      } else if (OB_SUCCESS != (ret = sw_->submit_log(log_entry_header, buffer, this))) {
        CLOG_LOG(WARN, "submit_log failed", K_(partition_key), K(ret));
      }
    } else {
      ObRenewMembershipLog renew_ms_log;
      if (OB_SUCCESS != (ret = standby_generate_log_(log_entry_header,
                             renew_ms_log,
                             buffer,
                             MS_LOG_BUFFER_SIZE,
                             OB_LOG_RENEW_MEMBERSHIP,
                             pending_ms_log_id_,
                             pending_replica_num_,
                             pending_ms_timestamp_,
                             pending_ms_type_))) {
        CLOG_LOG(WARN, "generate_log failed", K_(partition_key), K(ret));
      } else if (OB_SUCCESS != (ret = ms_task_mgr_->submit_log(renew_ms_log, log_entry_header, buffer, this))) {
        CLOG_LOG(WARN, "submit_log failed", K_(partition_key), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_STORE(&ms_state_, MS_CHANGING);
      membership_timestamp_ = pending_ms_timestamp_;
      membership_log_id_ = pending_ms_log_id_;
      replica_num_ = pending_replica_num_;
      ms_type_ = pending_ms_type_;
      ms_proposal_id_ = log_entry_header.get_proposal_id();
      ATOMIC_STORE(&on_success_called_, false);
      reset_pending_status_();
      CLOG_LOG(INFO, "replica_type", K_(partition_key), K_(replica_type));
      if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
        set_election_candidate_();
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = curr_member_list_.deep_copy(tmp_list))) {
        CLOG_LOG(WARN, "curr_member_list_ deep_copy failed", K_(partition_key), K(tmp_ret), K(tmp_list));
      }
      if (OB_SUCCESS != (tmp_ret = prev_member_list_.deep_copy(prev_tmp_list))) {
        CLOG_LOG(WARN, "prev_member_list_ deep_copy failed", K_(partition_key), K(tmp_ret), K(prev_tmp_list));
      }
    }
  }
  return ret;
}

int ObLogMembershipMgr::changing_to_init_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ATOMIC_STORE(&ms_state_, MS_INIT);
    ATOMIC_STORE(&on_success_called_, false);
  }
  CLOG_LOG(INFO, "changing_to_init", K_(partition_key), K(ret));
  return ret;
}

int ObLogMembershipMgr::force_set_member_list(const common::ObMemberList& member_list, const int64_t replica_num)
{
  // Used by standby cluster/physical restore to force set member_list
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (replica_num <= 0 || replica_num > OB_MAX_MEMBER_NUMBER || member_list.get_member_number() > replica_num) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key));
  } else {
    WLockGuard guard(lock_);
    reset_status_without_lock_();
    replica_num_ = replica_num;
    if (OB_FAIL(curr_member_list_.deep_copy(member_list))) {
      CLOG_LOG(WARN, "curr_member_list_ deep_copy failed", K_(partition_key), K(ret), K(member_list));
    } else {
      if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
        set_election_candidate_(true);
      }
    }
  }
  CLOG_LOG(INFO, "force_set_member_list finished", K_(partition_key), K(ret), K(member_list), K(replica_num));
  return ret;
}

int ObLogMembershipMgr::set_membership_info(const common::ObMemberList& member_list, const int64_t membership_timestamp,
    const uint64_t membership_log_id, const int64_t replica_num, const common::ObProposalID& ms_proposal_id)
{
  // Migrator guarantees the base_storage_info is from same cluster.
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    if (!share::ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
        !GCTX.is_primary_cluster()) {
      if (common::INVALID_CLUSTER_TYPE == GCTX.get_cluster_type()) {
        // If the cluster type has not been determined, record the status,
        // and then try to update the election after the type is determined
        need_check_cluster_type_ = true;
      }
    }

    WLockGuard guard(lock_);
    if (OB_INVALID_TIMESTAMP == membership_timestamp || OB_INVALID_ID == membership_log_id || replica_num <= 0 ||
        replica_num > OB_MAX_MEMBER_NUMBER) {
      CLOG_LOG(ERROR, "invalid arguments", K_(partition_key));
      ret = OB_INVALID_ARGUMENT;
    } else if (!share::ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
               GCTX.is_standby_cluster() && ms_proposal_id < ms_proposal_id_) {
      // For non-private table in standby cluster, if argument ms_proposal_id is smaller than local, ignore
      CLOG_LOG(WARN,
          "arg ms_proposal_id is smaller than local ms_proposal_id_, ignore",
          K_(partition_key),
          K(ms_proposal_id),
          "local ms_proposal_id",
          ms_proposal_id_,
          K(member_list),
          K(membership_timestamp),
          K(membership_log_id),
          K(replica_num));
    } else if (membership_timestamp_ <= membership_timestamp) {
      reset_status_without_lock_();
      membership_timestamp_ = membership_timestamp;
      membership_log_id_ = membership_log_id;
      last_submit_cb_log_id_ = membership_log_id;
      replica_num_ = replica_num;
      ms_proposal_id_ = ms_proposal_id;
      if (OB_FAIL(curr_member_list_.deep_copy(member_list))) {
        CLOG_LOG(WARN, "curr_member_list_ deep_copy failed", K_(partition_key), K(ret), K(member_list));
      }
      if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
        set_election_candidate_();
      }
    } else {
    }

    CLOG_LOG(INFO,
        "set_membership_info finished",
        K(ret),
        K_(partition_key),
        K_(ms_proposal_id),
        K_(curr_member_list),
        K_(membership_log_id),
        K_(replica_num),
        K_(need_check_cluster_type));
  }
  return ret;
}

int ObLogMembershipMgr::add_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
{
  WLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  // Identifies whether it is a member change of the standby database
  bool is_standby_op = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(sw_) || !member.is_valid() || quorum <= 0 || quorum > OB_MAX_MEMBER_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(member), K(quorum), KP_(sw));
  } else if (MS_INIT != ATOMIC_LOAD(&ms_state_) || self_ == member.get_server()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "now can't add_member", K_(partition_key), K(ret), K(member), K_(self), K(quorum));
  } else if (curr_member_list_.get_member_number() >= quorum) {
    // member number exceeds quorum
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(
        ERROR, "now can't add_member, quorum too small", K_(partition_key), K(ret), K(quorum), K_(curr_member_list));
  } else if ((curr_member_list_.get_member_number() + 1) < (quorum / 2 + 1)) {
    // final member number is less than new majority value
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(
        ERROR, "now can't add_member, quorum too large", K_(partition_key), K(ret), K(quorum), K_(curr_member_list));
  } else if (quorum != replica_num_ && quorum != replica_num_ + 1) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "operation not support", K_(partition_key), K(ret), K(quorum), K(replica_num_));
  } else {
    is_standby_op = (STANDBY_LEADER == state_mgr_->get_role());
  }

  if (OB_SUCC(ret)) {
    if (!is_standby_op) {
      if (!state_mgr_->can_submit_log()) {
        ret = OB_STATE_NOT_MATCH;
        CLOG_LOG(WARN, "can_submit_log return false", K_(partition_key), K(ret));
      } else {
        const int64_t base_timestamp = 0;
        if (OB_SUCCESS != (ret = sw_->alloc_log_id(base_timestamp, pending_ms_log_id_, pending_ms_timestamp_))) {
          CLOG_LOG(WARN, "alloc_log_id failed", K_(partition_key), K(ret));
        }
      }
    } else {
      // standby member change
      pending_op_type_ = MS_OP_STANDBY_LEADER;
      const uint64_t max_log_id = sw_->get_max_log_id();
      // set pending_ms_log_id_ to max_log_id+1, which is convenient for barrier check
      pending_ms_log_id_ = max_log_id + 1;
      // use submit_timestamp to identify each renew_ms_log uniquely
      pending_ms_timestamp_ = ObTimeUtility::current_time();
      if (OB_INVALID_TIMESTAMP != membership_timestamp_ && membership_timestamp_ >= pending_ms_timestamp_) {
        pending_ms_timestamp_ = membership_timestamp_ + 1;
      }
    }

    if (OB_SUCC(ret)) {
      log_info.log_id_ = pending_ms_log_id_;
      log_info.timestamp_ = pending_ms_timestamp_;
      pending_member_for_add_ = member;
      pending_ms_type_ = MS_TYPE_ADD_MEMBER;
      pending_replica_num_ = quorum;
      if (check_barrier_condition_(pending_ms_log_id_) && (OB_SUCCESS == (ret = to_changing_()))) {
      } else {
        ATOMIC_STORE(&ms_state_, MS_PENDING);
      }

      if (MS_PENDING != ATOMIC_LOAD(&ms_state_)) {
        reset_pending_status_();
      }

      if (OB_SUCC(ret)) {
        if (!cascading_mgr_->is_valid_child(member.get_server())) {
          // new member not a child, no need remove
        } else if (OB_FAIL(cascading_mgr_->try_remove_child(member.get_server()))) {
          CLOG_LOG(WARN, "try_remove_child failed", K_(partition_key), K(ret), "server", member.get_server());
        }
      }
    }
  }
  CLOG_LOG(INFO, "add_member", K_(partition_key), K(ret), K(member), K(quorum), K(is_standby_op));
  return ret;
}

int ObLogMembershipMgr::remove_member(
    const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
{
  WLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  bool is_standby_op = false;  // whether it is standby operation
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(sw_) || !member.is_valid() || quorum <= 0 || quorum > OB_MAX_MEMBER_NUMBER ||
             !curr_member_list_.contains(member)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(member), K(quorum), KP_(sw), K(curr_member_list_));
  } else if (MS_INIT != ATOMIC_LOAD(&ms_state_) || self_ == member.get_server()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "now can't remove_member", K(ret), K_(partition_key), K_(ms_state), K(member), K_(self));
  } else if (curr_member_list_.get_member_number() - 1 > quorum ||
             curr_member_list_.get_member_number() > OB_MAX_MEMBER_NUMBER) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(
        WARN, "now can't remove_member, quorum too small", K_(partition_key), K(ret), K(quorum), K_(curr_member_list));
  } else if (curr_member_list_.get_member_number() <= (quorum / 2 + 1)) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(
        WARN, "now can't remove_member, quorum too large", K_(partition_key), K(ret), K(quorum), K_(curr_member_list));
  } else if (quorum != replica_num_ && quorum != replica_num_ - 1) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "operation not support", K_(partition_key), K(ret), K(quorum), K(replica_num_));
  } else {
    is_standby_op = (STANDBY_LEADER == state_mgr_->get_role());
  }

  if (OB_SUCC(ret)) {
    if (!is_standby_op) {
      if (!state_mgr_->can_submit_log()) {
        ret = OB_STATE_NOT_MATCH;
        CLOG_LOG(WARN, "can_submit_log return false", K_(partition_key), K(ret));
      } else {
        const int64_t base_timestamp = 0;
        if (OB_SUCCESS != (ret = sw_->alloc_log_id(base_timestamp, pending_ms_log_id_, pending_ms_timestamp_))) {
          CLOG_LOG(WARN, "alloc_log_id failed", K_(partition_key), K(ret));
        }
      }
    } else {
      pending_op_type_ = MS_OP_STANDBY_LEADER;
      const uint64_t max_log_id = sw_->get_max_log_id();
      // set pending_ms_log_id_ to max_log_id+1, which is convenient for barrier check
      pending_ms_log_id_ = max_log_id + 1;
      pending_ms_timestamp_ = ObTimeUtility::current_time();
      if (OB_INVALID_TIMESTAMP != membership_timestamp_ && membership_timestamp_ >= pending_ms_timestamp_) {
        pending_ms_timestamp_ = membership_timestamp_ + 1;
      }
    }

    if (OB_SUCC(ret)) {
      log_info.log_id_ = pending_ms_log_id_;
      log_info.timestamp_ = pending_ms_timestamp_;
      pending_member_for_remove_ = member;
      pending_ms_type_ = MS_TYPE_REMOVE_MEMBER;
      pending_replica_num_ = quorum;
      if (check_barrier_condition_(pending_ms_log_id_) && (OB_SUCCESS == (ret = to_changing_()))) {
      } else {
        ATOMIC_STORE(&ms_state_, MS_PENDING);
      }

      if (MS_PENDING != ATOMIC_LOAD(&ms_state_)) {
        reset_pending_status_();
      }
    }
  }
  CLOG_LOG(INFO, "remove_member", K_(partition_key), K(ret), K_(ms_state), K(member), K(quorum), K(is_standby_op));
  return ret;
}

const ObMemberList& ObLogMembershipMgr::get_curr_member_list() const
{
  return curr_member_list_;
}

int ObLogMembershipMgr::receive_log(
    const ObLogEntry& log_entry, const ObAddr& server, const int64_t cluster_id, const ReceiveLogType rl_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(server));
    ret = OB_INVALID_ARGUMENT;
  } else {
    WLockGuard guard(lock_);
    ret = receive_log_without_lock_(log_entry, server, cluster_id, rl_type);
  }
  return ret;
}

bool ObLogMembershipMgr::need_skip_apply_ms_log_(const ObMembershipLog& ms_log) const
{
  int bool_ret = false;
  const int64_t self_cluster_id = state_mgr_->get_self_cluster_id();
  if (share::ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    // private table cannot be skipped
    bool_ret = false;
  } else if (OB_INVALID_CLUSTER_ID == ms_log.get_cluster_id()) {
    // cluster_id in this ms_log is invalid, maybe generated by old version, need apply
    bool_ret = false;
  } else if (ms_log.get_cluster_id() == self_cluster_id) {
    // For non-private table, ms_log from same cluster need be applied
    bool_ret = false;
  } else {
    // non-private encounters different cluster ms log, need skip
    // For non-private table, ms_log from different cluster can be skipped
    bool_ret = true;
    CLOG_LOG(INFO,
        "non-private tbl encounter diff cluster ms log, need skip",
        K_(partition_key),
        K(self_cluster_id),
        K(ms_log));
  }
  return bool_ret;
}

int ObLogMembershipMgr::receive_log_without_lock_(
    const ObLogEntry& log_entry, const ObAddr& server, const int64_t cluster_id, const ReceiveLogType rl_type)
{
  int ret = OB_SUCCESS;
  const bool is_renew_ms_log = (OB_LOG_RENEW_MEMBERSHIP == log_entry.get_header().get_log_type());
  int64_t pos = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_ISNULL(state_mgr_) || OB_ISNULL(sw_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_renew_ms_log) {
    // renew_membership_log
    const uint64_t log_id = log_entry.get_header().get_log_id();
    ObRenewMembershipLog renew_ms_log;
    int64_t self_cluster_id = state_mgr_->get_self_cluster_id();
    if (OB_SUCCESS !=
        (ret = renew_ms_log.deserialize(log_entry.get_buf(), log_entry.get_header().get_data_len(), pos))) {
      CLOG_LOG(WARN, "renew_ms_log deserialize failed", K_(partition_key), K(ret), K(renew_ms_log));
    } else if (renew_ms_log.get_cluster_id() != self_cluster_id) {
      CLOG_LOG(WARN,
          "receive diff cluster renew_ms_log, ignore",
          K_(partition_key),
          K(renew_ms_log),
          K(log_id),
          K(log_entry),
          K(self_cluster_id));
    } else if (renew_ms_log.get_ms_proposal_id() >= ms_proposal_id_) {
      const uint64_t log_id = log_entry.get_header().get_log_id();
      bool need_check_barrier = true;
      if (MS_TYPE_START_MEMBERSHIP == renew_ms_log.get_type()) {
        // start_membership log in standby cluster no need check barrier condition
        need_check_barrier = false;
      }

      if (!need_check_barrier || (need_check_barrier && check_barrier_condition_(log_id))) {
        if (OB_FAIL(ms_task_mgr_->receive_renew_ms_log(log_entry, renew_ms_log, server, cluster_id, rl_type))) {
          CLOG_LOG(WARN, "receive_renew_ms_log failed", K_(partition_key), K(ret), K(log_id), K(server));
        } else {
          update_status_(log_entry, renew_ms_log);
          ATOMIC_STORE(&ms_state_, MS_INIT);
        }
      } else {
        if (has_follower_pending_entry_ && log_id > follower_pending_entry_.get_header().get_log_id()) {
          CLOG_LOG(INFO,
              "there is already a pending entry with smaller log_id, ignore",
              K_(partition_key),
              K(log_id),
              K(follower_pending_entry_));
        } else {
          reset_follower_pending_entry();
          if (OB_SUCCESS != (ret = log_entry.deep_copy_to(follower_pending_entry_))) {
            CLOG_LOG(WARN,
                "copy log entry failed",
                K_(partition_key),
                K(ret),
                K(server),
                K(log_entry.get_header()),
                K(renew_ms_log),
                K(rl_type));
          } else {
            has_follower_pending_entry_ = true;
            follower_pending_entry_server_ = server;
            follower_pending_entry_server_cluster_id_ = cluster_id;
            follower_pending_entry_type_ = rl_type;
          }
          CLOG_LOG(INFO,
              "restore follower pending entry",
              K_(partition_key),
              K(ret),
              K_(has_follower_pending_entry),
              K_(follower_pending_entry_server),
              K(renew_ms_log));
        }
      }
    } else {
    }
    CLOG_LOG(INFO,
        "ObLogMembershipMgr::receive_log",
        K_(partition_key),
        K(ret),
        K_(ms_state),
        K(log_entry),
        K(renew_ms_log),
        K(server));
  } else {
    // other ms_log
    ObMembershipLog ms_log;
    if (OB_SUCCESS != (ret = ms_log.deserialize(log_entry.get_buf(), log_entry.get_header().get_data_len(), pos))) {
      CLOG_LOG(WARN, "ms_log deserialize failed", K_(partition_key), K(ret), K(ms_log));
    } else {
      const uint64_t log_id = log_entry.get_header().get_log_id();
      bool need_check_barrier = true;
      if (state_mgr_->is_archive_restoring_log() && state_mgr_->is_archive_restore_leader()) {
        // ms log during physical restore no need check barrier condition
        need_check_barrier = false;
      }
      if (need_skip_apply_ms_log_(ms_log) || !need_check_barrier || check_barrier_condition_(log_id)) {
        if (OB_SUCCESS != (ret = sw_->receive_log(log_entry, server, cluster_id, rl_type))) {
          CLOG_LOG(WARN, "receive_log failed", K_(partition_key), K(ret), K(server));
        } else {
          if (need_skip_apply_ms_log_(ms_log)) {
            // need skip this ms log
          } else {
            update_status_(log_entry, ms_log);
            ATOMIC_STORE(&ms_state_, MS_INIT);
          }
          if (state_mgr_->is_archive_restoring_log()) {
            if (OB_FAIL(sw_->set_log_confirmed(log_id, false)) && OB_ERROR_OUT_OF_RANGE != ret) {
              CLOG_LOG(WARN, "set_log_confirmed failed", K_(partition_key), K(ret), K(log_entry), K(server));
            } else {
              // It may return OB_ERROR_OUT_OF_RANGE in a concurrent scenario, ignore it
              ret = OB_SUCCESS;
            }
          }
        }
        CLOG_LOG(INFO,
            "receive_log check_barrier_condition true",
            K_(partition_key),
            K(ret),
            K(server),
            K(log_entry.get_header()),
            K(ms_log),
            K(rl_type));
      } else {
        bool is_fetched = false;
        if (has_follower_pending_entry_ && log_id > follower_pending_entry_.get_header().get_log_id()) {
          if (state_mgr_->is_archive_restoring_log()) {
            // Return EAGAIN if rejected during physical restore
            ret = OB_EAGAIN;
          }
          (void)sw_->do_fetch_log(log_id, log_id + 1, POINT_FETCH_LOG_EXECUTE_TYPE, is_fetched);
        } else {
          if (has_follower_pending_entry_ && log_id < follower_pending_entry_.get_header().get_log_id()) {
            (void)sw_->do_fetch_log(follower_pending_entry_.get_header().get_log_id(),
                follower_pending_entry_.get_header().get_log_id() + 1,
                POINT_FETCH_LOG_EXECUTE_TYPE,
                is_fetched);
          }
          reset_follower_pending_entry();

          if (OB_SUCCESS != (ret = log_entry.deep_copy_to(follower_pending_entry_))) {
            CLOG_LOG(WARN,
                "copy log entry failed",
                K_(partition_key),
                K(ret),
                K(server),
                K(log_entry.get_header()),
                K(ms_log),
                K(rl_type));
          } else {
            has_follower_pending_entry_ = true;
            follower_pending_entry_server_ = server;
            follower_pending_entry_server_cluster_id_ = cluster_id;
            follower_pending_entry_type_ = rl_type;
          }
          CLOG_LOG(INFO,
              "restore follower pending entry",
              K_(partition_key),
              K(ret),
              K_(has_follower_pending_entry),
              K_(follower_pending_entry_server),
              K(ms_log));
        }
      }
    }
    if (OB_LOG_START_MEMBERSHIP != log_entry.get_header().get_log_type()) {
      // start_working log no need print info
      CLOG_LOG(INFO,
          "ObLogMembershipMgr::receive_log",
          K_(partition_key),
          K(ret),
          K_(ms_state),
          K(log_entry),
          K(ms_log),
          K(server));
    }
  }

  return ret;
}

int ObLogMembershipMgr::receive_recovery_log(
    const ObLogEntry& log_entry, const bool is_confirmed, const int64_t accum_checksum, const bool is_batch_committed)
{
  int ret = OB_SUCCESS;
  ObMembershipLog ms_log;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershioMgr is not inited", KR(ret), K_(partition_key), K(log_entry));
  } else if (OB_ISNULL(state_mgr_) || OB_ISNULL(sw_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "state_mgr_ is NULL or sw_ is NULL", KR(ret), K_(partition_key), K(log_entry));
  } else if (OB_FAIL(ms_log.deserialize(log_entry.get_buf(), log_entry.get_header().get_data_len(), pos))) {
    CLOG_LOG(WARN, "deserialize failed", K_(partition_key), K(ret), K(ms_log));
  } else {
    if (OB_SUCCESS != (ret = sw_->receive_recovery_log(log_entry, is_confirmed, accum_checksum, is_batch_committed))) {
      CLOG_LOG(WARN, "receive_log failed", K_(partition_key), K(ret), K(log_entry));
    } else {
      WLockGuard guard(lock_);
      update_status_(log_entry, ms_log);
    }
  }
  return ret;
}

int64_t ObLogMembershipMgr::get_timestamp() const
{
  return membership_timestamp_;
}

void ObLogMembershipMgr::reset_status()
{
  WLockGuard guard(lock_);
  reset_status_without_lock_();
}

int ObLogMembershipMgr::write_start_membership(const ObLogType& log_type)
{
  int ret = OB_SUCCESS;

  // debug sync test: hang before writing start_working log
  DEBUG_SYNC(BEFORE_WRITE_START_WORKING);

  char buffer[MS_LOG_BUFFER_SIZE];
  uint64_t log_id = OB_INVALID_ID;
  int64_t membership_timestamp = OB_INVALID_TIMESTAMP;
  int64_t log_size = 0;
  int64_t base_timestamp = 0;
  const bool is_trans_log = false;
  int64_t log_submit_timestamp = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObLogMembershipMgr is not inited", K(ret), K(partition_key_));
  } else if (OB_LOG_START_MEMBERSHIP == log_type) {
    // To update memberhsip_log_id_ and other member, wlock is needed
    WLockGuard guard(lock_);
    if (!state_mgr_->can_submit_start_working_log()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "can_submit_start_working_log return false", K_(partition_key), K(ret));
    } else if (OB_SUCCESS !=
               (ret = generate_start_log_(log_type, buffer, MS_LOG_BUFFER_SIZE, membership_timestamp, log_size))) {
      CLOG_LOG(WARN, "generate_log failed", K_(partition_key), K(ret));
    } else if (OB_SUCCESS !=
               (ret = sw_->submit_log(
                    log_type, buffer, log_size, base_timestamp, is_trans_log, NULL, log_id, log_submit_timestamp))) {
      CLOG_LOG(WARN, "submit_log failed", K_(partition_key), K(ret));
    } else {
      membership_timestamp_ = membership_timestamp;
      membership_log_id_ = log_id;
    }
  } else if (OB_LOG_RENEW_MEMBERSHIP == log_type) {
    // To update memberhsip_log_id_ and other member, wlock is needed
    WLockGuard guard(lock_);
    // standby_leader writes start_working log
    // set renew_ms_log_id to max_log_id+1
    const uint64_t max_log_id = sw_->get_max_log_id();
    log_id = max_log_id + 1;

    ObRenewMembershipLog renew_ms_log;
    if (!state_mgr_->is_can_elect_standby_leader()) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "is_can_elect_standby_leader return false", K_(partition_key), K(ret));
    } else if (OB_SUCCESS !=
               (ret = standby_generate_start_log_(
                    log_type, renew_ms_log, buffer, MS_LOG_BUFFER_SIZE, log_submit_timestamp, log_size))) {
      CLOG_LOG(WARN, "generate_log failed", K_(partition_key), K(ret));
    } else if (OB_SUCCESS !=
               (ret = ms_task_mgr_->submit_log(
                    renew_ms_log, log_type, buffer, log_size, log_id, log_submit_timestamp, is_trans_log, NULL))) {
      CLOG_LOG(WARN, "submit_log failed", K_(partition_key), K(ret));
    } else {
      membership_timestamp_ = log_submit_timestamp;
      membership_log_id_ = log_id;
    }
  } else {
    // do nothing
  }

  CLOG_LOG(INFO,
      "write start membership finished",
      K(ret),
      K_(partition_key),
      K(log_type),
      K(log_id),
      K(base_timestamp),
      K(log_submit_timestamp));
  return ret;
}

int ObLogMembershipMgr::on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type,
    const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed)
{
  UNUSED(log_id);
  UNUSED(partition_key);
  UNUSED(version);
  UNUSED(batch_committed);
  UNUSED(batch_last_succeed);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key));
    ret = OB_NOT_INIT;
  } else {
    WLockGuard guard(lock_);
    ATOMIC_STORE(&on_success_called_, true);
    submit_success_cb_task_(log_type);
    changing_to_init_();
  }
  return ret;
}

int ObLogMembershipMgr::append_disk_log(const ObLogEntry& log_entry, const ObLogCursor& log_cursor,
    const int64_t accum_checksum, const bool batch_committed)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const bool is_renew_ms_log = (OB_LOG_RENEW_MEMBERSHIP == log_entry.get_header().get_log_type());
  const uint64_t log_id = log_entry.get_header().get_log_id();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!log_cursor.is_valid() || OB_ISNULL(sw_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(log_cursor), KP(sw_));
  } else if (is_renew_ms_log) {
    // renew_membership_log
    ObRenewMembershipLog renew_ms_log;
    int64_t self_cluster_id = state_mgr_->get_self_cluster_id();
    if (OB_SUCCESS !=
        (ret = renew_ms_log.deserialize(log_entry.get_buf(), log_entry.get_header().get_data_len(), pos))) {
      CLOG_LOG(WARN, "renew_ms_log deserialize failed", K_(partition_key), K(ret), K(renew_ms_log));
    } else if (renew_ms_log.get_cluster_id() != self_cluster_id) {
      CLOG_LOG(WARN,
          "receive diff cluster renew_ms_log, ignore",
          K_(partition_key),
          K(renew_ms_log),
          K(log_id),
          K(log_entry),
          K(self_cluster_id));
    } else {
      if (renew_ms_log.get_ms_proposal_id() >= ms_proposal_id_) {
        // renew_ms_log no need be loaded into sliding window
        update_status_(log_entry, renew_ms_log);
      }
    }
  } else {
    // other ms_log
    ObMembershipLog ms_log;
    if (OB_SUCCESS != (ret = ms_log.deserialize(log_entry.get_buf(), log_entry.get_header().get_data_len(), pos))) {
      CLOG_LOG(ERROR, "deserialize failed", K_(partition_key), K(ret), K(log_entry));
    } else if (OB_SUCCESS != (ret = sw_->append_disk_log(log_entry, log_cursor, accum_checksum, batch_committed))) {
      CLOG_LOG(ERROR, "append_disk_log failed", K_(partition_key), K(ret));
    } else if (need_skip_apply_ms_log_(ms_log)) {
      CLOG_LOG(INFO, "need skip apply this ms log", K_(partition_key), K(ms_log));
    } else {
      update_status_(log_entry, ms_log);
    }
  }
  return ret;
}

int ObLogMembershipMgr::generate_log_(ObLogEntryHeader& log_entry_header, char* buffer, const int64_t buf_len,
    const ObLogType log_type, const uint64_t membership_log_id, const int64_t replica_num,
    const int64_t membership_timestamp, const int64_t ms_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buffer) || buf_len <= 0 || replica_num <= 0 || replica_num > OB_MAX_MEMBER_NUMBER ||
             OB_IS_INVALID_LOG_ID(membership_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), KP(buffer), K(buf_len), K(replica_num), K(membership_log_id));
  } else if (OB_LOG_MEMBERSHIP == log_type) {
    const int64_t cluster_id = state_mgr_->get_self_cluster_id();
    ObMembershipLog ms_log;
    ms_log.set_member_list(curr_member_list_);
    ms_log.set_prev_member_list(prev_member_list_);
    ms_log.set_timestamp(membership_timestamp);
    ms_log.set_type(ms_type);
    ms_log.set_replica_num(replica_num);
    ms_log.set_cluster_id(cluster_id);
    const bool is_trans_log = false;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = ms_log.serialize(buffer, buf_len, pos))) {
      CLOG_LOG(ERROR, "serialize failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(log_entry_header.generate_header(log_type,
                   partition_key_,
                   membership_log_id,
                   buffer,
                   pos,
                   ObTimeUtility::current_time(),      // generation_timestamp
                   state_mgr_->get_proposal_id().ts_,  // epoch_id
                   state_mgr_->get_proposal_id(),      // proposal_id
                   membership_timestamp,               // submit_timestamp
                   state_mgr_->get_freeze_version(),
                   is_trans_log))) {
      CLOG_LOG(WARN, "generate_header failed", K_(partition_key), K(ret), K(membership_log_id));
    }
  }

  return ret;
}

int ObLogMembershipMgr::standby_generate_log_(ObLogEntryHeader& log_entry_header, ObRenewMembershipLog& ms_log,
    char* buffer, const int64_t buf_len, const ObLogType log_type, const uint64_t membership_log_id,
    const int64_t replica_num, const int64_t membership_timestamp, const int64_t ms_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buffer) || buf_len <= 0 || replica_num <= 0 || replica_num > OB_MAX_MEMBER_NUMBER ||
             OB_IS_INVALID_LOG_ID(membership_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), KP(buffer), K(buf_len), K(replica_num), K(membership_log_id));
  } else if (OB_LOG_RENEW_MEMBERSHIP == log_type) {
    const int64_t cluster_id = state_mgr_->get_self_cluster_id();
    ms_log.set_member_list(curr_member_list_);
    ms_log.set_prev_member_list(prev_member_list_);
    ms_log.set_timestamp(membership_timestamp);
    ms_log.set_type(ms_type);
    ms_log.set_replica_num(replica_num);
    ms_log.set_cluster_id(cluster_id);
    ms_log.set_ms_proposal_id(ms_proposal_id_);

    const bool is_trans_log = false;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = ms_log.serialize(buffer, buf_len, pos))) {
      CLOG_LOG(ERROR, "serialize failed", K(ret), K(partition_key_));
    } else if (OB_FAIL(log_entry_header.generate_header(log_type,
                   partition_key_,
                   membership_log_id,
                   buffer,
                   pos,
                   ObTimeUtility::current_time(),  // generation_timestamp
                   ms_proposal_id_.ts_,            // epoch_id
                   ms_proposal_id_,                // proposal_id
                   membership_timestamp,           // submit_timestamp
                   state_mgr_->get_freeze_version(),
                   is_trans_log))) {
      CLOG_LOG(WARN, "generate_header failed", K_(partition_key), K(ret));
    } else {
      CLOG_LOG(INFO, "generate_header success", K_(partition_key), K(log_entry_header));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObLogMembershipMgr::generate_start_log_(
    const ObLogType log_type, char* buffer, const int64_t buf_len, int64_t& membership_timestamp, int64_t& log_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buffer) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_));
  } else if (OB_LOG_START_MEMBERSHIP == log_type) {
    // FIXME() : need guarantee membership_timestamp increasing monotonically
    membership_timestamp = ObTimeUtility::current_time();
    const int64_t cluster_id = state_mgr_->get_self_cluster_id();

    ObMembershipLog ms_log;
    ms_log.set_member_list(curr_member_list_);
    ms_log.set_prev_member_list(prev_member_list_);
    ms_log.set_timestamp(membership_timestamp);
    ms_log.set_type(MS_TYPE_START_MEMBERSHIP);
    ms_log.set_replica_num(replica_num_);
    ms_log.set_cluster_id(cluster_id);
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = ms_log.serialize(buffer, buf_len, pos))) {
      CLOG_LOG(ERROR, "serialize failed", K(ret), K(partition_key_));
    } else {
      log_size = pos;
    }
  } else if (OB_LOG_RENEW_MEMBERSHIP == log_type) {
    membership_timestamp = ObTimeUtility::current_time();
    if (membership_timestamp_ >= membership_timestamp) {
      membership_timestamp = membership_timestamp_ + 1;
    }
    const int64_t cluster_id = state_mgr_->get_self_cluster_id();

    ObRenewMembershipLog ms_log;
    ms_log.set_member_list(curr_member_list_);
    ms_log.set_prev_member_list(prev_member_list_);
    ms_log.set_timestamp(membership_timestamp);
    ms_log.set_type(MS_TYPE_START_MEMBERSHIP);
    ms_log.set_replica_num(replica_num_);
    ms_log.set_cluster_id(cluster_id);
    ms_log.set_ms_proposal_id(ms_proposal_id_);

    int64_t pos = 0;
    if (OB_SUCCESS != (ret = ms_log.serialize(buffer, buf_len, pos))) {
      CLOG_LOG(ERROR, "serialize failed", K(ret), K(partition_key_));
    } else {
      log_size = pos;
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObLogMembershipMgr::standby_generate_start_log_(const ObLogType log_type, ObRenewMembershipLog& ms_log,
    char* buffer, const int64_t buf_len, int64_t& membership_timestamp, int64_t& log_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buffer) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_));
  } else if (OB_LOG_RENEW_MEMBERSHIP == log_type) {
    membership_timestamp = ObTimeUtility::current_time();
    if (membership_timestamp_ >= membership_timestamp) {
      membership_timestamp = membership_timestamp_ + 1;
    }
    const int64_t cluster_id = state_mgr_->get_self_cluster_id();

    ms_log.set_member_list(curr_member_list_);
    ms_log.set_prev_member_list(prev_member_list_);
    ms_log.set_timestamp(membership_timestamp);
    ms_log.set_type(MS_TYPE_START_MEMBERSHIP);
    ms_log.set_replica_num(replica_num_);
    ms_log.set_cluster_id(cluster_id);
    ms_log.set_ms_proposal_id(ms_proposal_id_);

    int64_t pos = 0;
    if (OB_SUCCESS != (ret = ms_log.serialize(buffer, buf_len, pos))) {
      CLOG_LOG(ERROR, "serialize failed", K(ret), K(partition_key_));
    } else {
      log_size = pos;
    }
  } else {
    // do nothing
  }
  return ret;
}

bool ObLogMembershipMgr::is_renew_ms_log_majority_success() const
{
  bool bool_ret = false;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "ObLogMembershipMgr not inited", K_(partition_key));
  } else {
    bool_ret = ms_task_mgr_->is_renew_ms_log_majority_success();
  }
  return bool_ret;
}

int ObLogMembershipMgr::check_renew_ms_log_sync_state() const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ms_task_mgr_->check_renew_ms_log_sync_state())) {
    CLOG_LOG(WARN, "check_renew_ms_log_sync_state failed", K(ret), K_(partition_key));
  } else {
  }
  return ret;
}

int ObLogMembershipMgr::reset_renew_ms_log_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ms_task_mgr_->reset_renew_ms_log_task())) {
    CLOG_LOG(WARN, "reset_renew_ms_log_task failed", K(ret), K_(partition_key));
  } else {
  }
  return ret;
}

int ObLogMembershipMgr::get_curr_ms_log_body(
    ObLogEntryHeader& log_entry_header, char* buffer, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buffer) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), KP(buffer), K(buf_len));
  } else {
    RLockGuard guard(lock_);

    const int64_t cluster_id = state_mgr_->get_self_cluster_id();
    ObRenewMembershipLog ms_log;
    ms_log.set_member_list(curr_member_list_);
    ms_log.set_prev_member_list(prev_member_list_);
    ms_log.set_timestamp(membership_timestamp_);
    ms_log.set_type(ms_type_);
    ms_log.set_replica_num(replica_num_);
    ms_log.set_cluster_id(cluster_id);
    ms_log.set_ms_proposal_id(ms_proposal_id_);

    int64_t pos = 0;
    if (OB_SUCCESS != (ret = ms_log.serialize(buffer, buf_len, pos))) {
      CLOG_LOG(ERROR, "serialize failed", K(ret), K(partition_key_));
    } else if (OB_FAIL(log_entry_header.generate_header(OB_LOG_RENEW_MEMBERSHIP,
                   partition_key_,
                   membership_log_id_,
                   buffer,
                   pos,
                   membership_timestamp_,  // mock generation_timestamp
                   ms_proposal_id_.ts_,    // epoch_id
                   ms_proposal_id_,        // proposal_id
                   membership_timestamp_,  // submit_timestamp
                   state_mgr_->get_freeze_version(),
                   false))) {
      CLOG_LOG(WARN, "generate_header failed", K_(partition_key), K(ret));
    } else {
      CLOG_LOG(INFO, "generate_header success", K_(partition_key), K(log_entry_header));
    }
  }
  return ret;
}

void ObLogMembershipMgr::generate_curr_member_list_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key), K(ret));
    ret = OB_NOT_INIT;
  } else if (pending_member_for_add_.is_valid()) {
    if (OB_FAIL(prev_member_list_.deep_copy(curr_member_list_))) {
      CLOG_LOG(WARN, "prev_member_list_ deep_copy failed", K_(partition_key), K(ret), K_(curr_member_list));
    } else if (OB_FAIL(curr_member_list_.add_member(pending_member_for_add_))) {
      CLOG_LOG(WARN, "curr_member_list_ add_member failed", K_(partition_key), K(ret), K_(pending_member_for_add));
    } else if (OB_FAIL(cascading_mgr_->try_remove_child(pending_member_for_add_.get_server()))) {
      CLOG_LOG(ERROR,
          "cascading_mgr_->try_remove_child failed",
          K_(partition_key),
          K(ret),
          "server",
          pending_member_for_add_.get_server());
    } else {
      // do nothing
    }
  } else if (pending_member_for_remove_.is_valid()) {
    if (OB_FAIL(prev_member_list_.deep_copy(curr_member_list_))) {
      CLOG_LOG(WARN, "prev_member_list_ deep_copy failed", K_(partition_key), K(ret), K_(curr_member_list));
    } else if (OB_FAIL(curr_member_list_.remove_member(pending_member_for_remove_))) {
      CLOG_LOG(
          WARN, "curr_member_list_ remove_member failed", K_(partition_key), K(ret), K_(pending_member_for_remove));
    } else {
    }
  } else {
    // change quorum situation
    if (OB_FAIL(prev_member_list_.deep_copy(curr_member_list_))) {
      CLOG_LOG(WARN, "prev_member_list_ deep_copy failed", K_(partition_key), K(ret), K_(curr_member_list));
    } else {
      // do nothing, curr_member_list_ not change
    }
  }
}

void ObLogMembershipMgr::reset_pending_status_()
{
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key));
  } else {
    pending_member_for_add_.reset();
    pending_member_for_remove_.reset();
    pending_ms_timestamp_ = OB_INVALID_TIMESTAMP;
    pending_ms_log_id_ = OB_INVALID_ID;
    pending_replica_num_ = 0;
    pending_ms_type_ = MS_TYPE_UNKNOWN;
    pending_op_type_ = MS_OP_LEADER;
  }
}

void ObLogMembershipMgr::update_status_(const ObLogEntry& log_entry, const ObMembershipLog& ms_log)
{
  // The caller ensures that ms log must be written by this cluster
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key));
  } else {
    uint64_t new_membership_log_id = log_entry.get_header().get_log_id();
    int64_t new_membership_timestamp = ms_log.get_timestamp();
    int64_t new_replica_num = ms_log.get_replica_num();

    if (membership_timestamp_ < new_membership_timestamp) {
      membership_timestamp_ = new_membership_timestamp;
      membership_log_id_ = new_membership_log_id;
      // ms_proposal_id_ begins to be maintained since 2.2.60
      ms_proposal_id_ = log_entry.get_header().get_proposal_id();
      if (new_replica_num <= 0 || new_replica_num > OB_MAX_MEMBER_NUMBER) {
        // compatible with 1.4.0
        CLOG_LOG(WARN, "invalid replica_num in ms_log", K_(partition_key), K(new_replica_num));
      } else {
        replica_num_ = new_replica_num;
      }
      if (OB_FAIL(curr_member_list_.deep_copy(ms_log.get_member_list()))) {
        CLOG_LOG(WARN, "curr_member_list_ deep_copy failed", K_(partition_key), K(ret));
      } else if (OB_FAIL(prev_member_list_.deep_copy(ms_log.get_prev_member_list()))) {
        CLOG_LOG(WARN, "prev_member_list_ deep_copy failed", K_(partition_key), K(ret));
      }
      ms_type_ = ms_log.get_type();
      if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
        set_election_candidate_();
      }
      CLOG_LOG(INFO, "update_status finished", K_(partition_key), K_(ms_proposal_id), K_(membership_log_id), K(ms_log));
    }
  }
}

void ObLogMembershipMgr::update_status_(const ObLogEntry& log_entry, const ObRenewMembershipLog& ms_log)
{
  // The caller ensures that ms log must be written by this cluster
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key));
  } else {
    uint64_t new_membership_log_id = log_entry.get_header().get_log_id();
    int64_t new_membership_timestamp = ms_log.get_timestamp();
    int64_t new_replica_num = ms_log.get_replica_num();

    if (membership_timestamp_ < new_membership_timestamp) {
      membership_timestamp_ = new_membership_timestamp;
      membership_log_id_ = new_membership_log_id;
      // ms_proposal_id_ begins to be maintained since 2.2.60
      ms_proposal_id_ = ms_log.get_ms_proposal_id();
      if (new_replica_num <= 0 || new_replica_num > OB_MAX_MEMBER_NUMBER) {
        // compatible with 1.4.0
        CLOG_LOG(WARN, "invalid replica_num in ms_log", K_(partition_key), K(new_replica_num));
      } else {
        replica_num_ = new_replica_num;
      }
      if (OB_FAIL(curr_member_list_.deep_copy(ms_log.get_member_list()))) {
        CLOG_LOG(WARN, "curr_member_list_ deep_copy failed", K_(partition_key), K(ret));
      } else if (OB_FAIL(prev_member_list_.deep_copy(ms_log.get_prev_member_list()))) {
        CLOG_LOG(WARN, "prev_member_list_ deep_copy failed", K_(partition_key), K(ret));
      }
      ms_type_ = ms_log.get_type();
      if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
        set_election_candidate_();
      }
      CLOG_LOG(INFO, "update_status finished", K_(partition_key), K_(ms_proposal_id), K_(membership_log_id), K(ms_log));
    }
  }
}

void ObLogMembershipMgr::set_election_candidate_(const bool is_force_set)
{
  // Now the member_list must be own cluster
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key), K(ret));
  } else if (!curr_member_list_.is_valid()) {
    // member_list may be empty when standby cluster creates replica.
  } else if (!is_force_set && state_mgr_->is_archive_restoring()) {
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      CLOG_LOG(WARN, "now in archive restoring state, skip set_election_candidate", K_(partition_key), K(is_force_set));
    }
  } else if (OB_SUCCESS !=
             (ret = state_mgr_->set_election_candidates(replica_num_, curr_member_list_, membership_timestamp_))) {
    if (OB_PARTITION_NOT_EXIST == ret && state_mgr_->is_offline()) {
      CLOG_LOG(WARN,
          "partition already offline and removed",
          K_(partition_key),
          K(ret),
          "is_offline",
          state_mgr_->is_offline(),
          K_(curr_member_list));
    } else {
      CLOG_LOG(ERROR,
          "set_candidate failed",
          K_(partition_key),
          K(ret),
          "is_offline",
          state_mgr_->is_offline(),
          K_(curr_member_list));
    }
  } else {
    need_check_cluster_type_ = false;  // reset check cluster flag
    if (curr_member_list_.get_member_number() > 1) {
      is_single_replica_mode_ = false;
      CLOG_LOG(INFO, "set is_single_replica_mode=false", K_(partition_key), K_(replica_num), K_(curr_member_list));
    }
  }
  CLOG_LOG(INFO,
      "set_election_candidate_ finished",
      K_(partition_key),
      K(ret),
      K_(curr_member_list),
      K_(replica_type),
      K(is_force_set));
}

int64_t ObLogMembershipMgr::get_replica_num() const
{
  return replica_num_;
}

common::ObReplicaType ObLogMembershipMgr::get_replica_type() const
{
  return replica_type_;
}

ObReplicaProperty ObLogMembershipMgr::get_replica_property() const
{
  return replica_property_;
}

bool ObLogMembershipMgr::is_state_init() const
{
  RLockGuard guard(lock_);
  return MS_INIT == ATOMIC_LOAD(&ms_state_);
}

int ObLogMembershipMgr::check_follower_has_pending_entry(bool& has_pending)
{
  int ret = OB_SUCCESS;
  has_pending = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not is inited", KR(ret));
  } else {
    has_pending = (has_follower_pending_entry_);
  }
  return ret;
}

void ObLogMembershipMgr::submit_success_cb_task_(const ObLogType log_type)
{
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    tmp_ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key), K(tmp_ret));
  } else if (OB_ISNULL(state_mgr_) || OB_ISNULL(cb_engine_)) {
    tmp_ret = OB_INVALID_TIMESTAMP;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(tmp_ret));
  } else {
    while (OB_SUCCESS != (tmp_ret = cb_engine_->submit_member_change_success_cb_task(partition_key_,
                              log_type,
                              membership_log_id_,
                              membership_timestamp_,
                              replica_num_,
                              prev_member_list_,
                              curr_member_list_,
                              ms_proposal_id_))) {
      // observer no memory, try again
      CLOG_LOG(WARN, "submit_member_change_success_cb_task failed, try again", K_(partition_key), K(tmp_ret));
      usleep(1000);
    }
  }
  if (OB_SUCCESS == tmp_ret) {
    last_submit_cb_log_id_ = membership_log_id_;
  }
  CLOG_LOG(INFO,
      "submit_success_cb_task_",
      K_(partition_key),
      K_(membership_timestamp),
      K_(curr_member_list),
      K_(prev_member_list),
      K_(ms_type));
}

void ObLogMembershipMgr::submit_success_cb_task(const ObLogType& log_type, const uint64_t log_id, const char* log_buf,
    const int64_t log_buf_len, const common::ObProposalID& proposal_id)
{
  int tmp_ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    tmp_ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key), K(tmp_ret));
  } else if (OB_ISNULL(state_mgr_) || OB_ISNULL(cb_engine_)) {
    tmp_ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arugment", K_(partition_key), K(tmp_ret));
  } else if (OB_LOG_MEMBERSHIP == log_type || OB_LOG_START_MEMBERSHIP == log_type) {
    ObMembershipLog ms_log;
    if (OB_SUCCESS != (tmp_ret = ms_log.deserialize(log_buf, log_buf_len, pos))) {
      CLOG_LOG(ERROR, "deserialize failed", K_(partition_key), K(tmp_ret), K(ms_log));
    } else if (need_skip_apply_ms_log_(ms_log) ||
               (OB_INVALID_ID != last_submit_cb_log_id_ && last_submit_cb_log_id_ >= log_id)) {
      CLOG_LOG(INFO, "need skip this ms log", K_(partition_key), K(last_submit_cb_log_id_), K(ms_log));
    } else {
      while (OB_SUCCESS != (tmp_ret = cb_engine_->submit_member_change_success_cb_task(partition_key_,
                                log_type,
                                log_id,
                                ms_log.get_timestamp(),
                                ms_log.get_replica_num(),
                                ms_log.get_prev_member_list(),
                                ms_log.get_member_list(),
                                proposal_id))) {
        // observer no memory, try again
        CLOG_LOG(WARN, "submit_member_change_success_cb_task failed, try again", K_(partition_key), K(tmp_ret));
        usleep(1000);
      }
      if (OB_SUCCESS == tmp_ret) {
        last_submit_cb_log_id_ = log_id;
      }
      CLOG_LOG(INFO,
          "submit_member_change_success_cb_task_",
          K_(partition_key),
          "membership_timestamp",
          ms_log.get_timestamp(),
          "curr_member_list",
          ms_log.get_member_list(),
          K(log_id),
          "prev_member_list",
          ms_log.get_prev_member_list(),
          "ms_type",
          ms_log.get_type());
    }
  } else if (OB_LOG_RENEW_MEMBERSHIP == log_type) {
    ObRenewMembershipLog ms_log;
    const int64_t self_cluster_id = state_mgr_->get_self_cluster_id();
    if (OB_SUCCESS != (tmp_ret = ms_log.deserialize(log_buf, log_buf_len, pos))) {
      CLOG_LOG(ERROR, "deserialize failed", K_(partition_key), K(tmp_ret), K(ms_log));
    } else if (ms_log.get_cluster_id() != self_cluster_id) {
      CLOG_LOG(INFO, "receive diff cluster renew_ms_log, ignore", K_(partition_key), K(log_id), K(ms_log));
    } else {
      while (OB_SUCCESS != (tmp_ret = cb_engine_->submit_member_change_success_cb_task(partition_key_,
                                log_type,
                                log_id,
                                ms_log.get_timestamp(),
                                ms_log.get_replica_num(),
                                ms_log.get_prev_member_list(),
                                ms_log.get_member_list(),
                                proposal_id))) {
        // observer no memory, try again
        CLOG_LOG(WARN, "submit_member_change_success_cb_task failed, try again", K_(partition_key), K(tmp_ret));
        usleep(1000);
      }
      CLOG_LOG(INFO, "submit_member_change_success_cb_task_", K_(partition_key), K(ms_log));
    }
  } else {
    // do nothing
  }
  return;
}

bool ObLogMembershipMgr::check_follower_pending_entry_()
{
  bool bool_ret = false;
  if (IS_NOT_INIT) {
    bool_ret = false;
  } else if (OB_ISNULL(state_mgr_)) {
    bool_ret = false;
  } else {
    bool_ret = ((state_mgr_->get_role() == FOLLOWER) && has_follower_pending_entry_ &&
                check_barrier_condition_(follower_pending_entry_.get_header().get_log_id()));
  }
  return bool_ret;
}

int ObLogMembershipMgr::handle_follower_pending_entry_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key), K(ret));
  } else if (check_follower_pending_entry_()) {
    ret = receive_log_without_lock_(follower_pending_entry_,
        follower_pending_entry_server_,
        follower_pending_entry_server_cluster_id_,
        follower_pending_entry_type_);
    reset_follower_pending_entry();
  }
  CLOG_LOG(INFO, "handle_follower_pending_entry_", K_(partition_key), K(ret));
  return ret;
}

void ObLogMembershipMgr::reset_follower_pending_entry()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key), K(ret));
  } else if (OB_ISNULL(alloc_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arugment", K_(partition_key), KP(alloc_mgr_));
  } else {
    has_follower_pending_entry_ = false;
    char* prev_buff = const_cast<char*>(follower_pending_entry_.get_buf());
    if (prev_buff != NULL) {
      TMA_MGR_INSTANCE.free_log_entry_buf(prev_buff);
      prev_buff = NULL;
    }
    follower_pending_entry_.reset();
    follower_pending_entry_server_.reset();
    follower_pending_entry_server_cluster_id_ = OB_INVALID_CLUSTER_ID;
    follower_pending_entry_type_ = RL_TYPE_UNKNOWN;
  }
}

void ObLogMembershipMgr::reconfirm_update_status(const ObLogEntry& log_entry)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMembershipLog ms_log;
  bool need_skip_apply = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key), K(ret));
  } else if (OB_SUCCESS !=
             (ret = ms_log.deserialize(log_entry.get_buf(), log_entry.get_header().get_data_len(), pos))) {
    CLOG_LOG(
        ERROR, "deserialize failed", K_(partition_key), K(ret), K(ms_log), "log_entry_header", log_entry.get_header());
  } else if (need_skip_apply_ms_log_(ms_log)) {
    need_skip_apply = true;
  } else {
    update_status_(log_entry, ms_log);
  }
  CLOG_LOG(INFO,
      "reconfirm_update_status",
      K_(partition_key),
      K(ret),
      K(need_skip_apply),
      K(ms_log),
      "log_entry_header",
      log_entry.get_header());
  return;
}

void ObLogMembershipMgr::reset_status_without_lock_()
{
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K_(partition_key));
  } else {
    ATOMIC_STORE(&ms_state_, MS_INIT);
    reset_pending_status_();
    ATOMIC_STORE(&on_success_called_, false);

    reset_follower_pending_entry();
  }
}

int ObLogMembershipMgr::set_replica_type(const enum ObReplicaType replica_type)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  replica_type_ = replica_type;
  return ret;
}

int ObLogMembershipMgr::update_ms_proposal_id(const common::ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    WLockGuard guard(lock_);
    if (ms_proposal_id > ms_proposal_id_) {
      ms_proposal_id_ = ms_proposal_id;
    }
  }
  return ret;
}

int ObLogMembershipMgr::change_quorum(const common::ObMemberList& curr_member_list, const int64_t curr_quorum,
    const int64_t new_quorum, obrpc::ObMCLogInfo& log_info)
{
  WLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  bool is_standby_op = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K(ret), K(partition_key_));
  } else if (!curr_member_list.is_valid() || curr_quorum <= 0 || curr_quorum > OB_MAX_MEMBER_NUMBER ||
             new_quorum <= 0 || new_quorum > OB_MAX_MEMBER_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K(ret), K(partition_key_), K(curr_member_list), K(curr_quorum), K(new_quorum));
    // There is currently a member change task being executed.
  } else if (MS_INIT != ATOMIC_LOAD(&ms_state_)) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "now can't change_quorum, state not match", K(ret), K(partition_key_), K(ms_state_));
    // member_list argument is not match with current member_list
  } else if (!curr_member_list_.member_addr_equal(curr_member_list)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments, member_list not match",
        K(ret),
        K(partition_key_),
        "curr_member_list",
        curr_member_list_,
        "arg_member_list",
        curr_member_list);
    // replica_num_ is not equal to curr_quorum
  } else if (replica_num_ != curr_quorum) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments, quorum not match",
        K(ret),
        K(partition_key_),
        K(replica_num_),
        "arg_curr_quorum",
        curr_quorum);
    // new_quorum is not equal to replica_num+1 or replica_num-1
  } else if (replica_num_ - 1 != new_quorum && replica_num_ + 1 != new_quorum) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments, quorum not continuous",
        K(ret),
        K(partition_key_),
        K(replica_num_),
        "arg_new_quorum",
        new_quorum);
    // The current member list exceeds new_quorum or the current member list does not satisfy
    // the majority of new_quorum
  } else if (curr_member_list_.get_member_number() > new_quorum ||
             curr_member_list_.get_member_number() < (new_quorum / 2 + 1)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(
        WARN, "invalid arguments, quorum not valid", K(ret), K(partition_key_), K(curr_member_list_), K(new_quorum));
  } else if (!state_mgr_->can_submit_log()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "can_submit_log return false", K_(partition_key), K(ret));
  } else {
    is_standby_op = (STANDBY_LEADER == state_mgr_->get_role());
    if (!is_standby_op) {
      const int64_t base_timestamp = 0;
      if (OB_FAIL(sw_->alloc_log_id(base_timestamp, pending_ms_log_id_, pending_ms_timestamp_))) {
        CLOG_LOG(WARN, "alloc_log_id failed", K(ret), K(partition_key_));
      }
    } else {
      // standby_leader
      pending_op_type_ = MS_OP_STANDBY_LEADER;
      const uint64_t max_log_id = sw_->get_max_log_id();
      // set pending_ms_log_id_ to max_log_id+1, which is convenient for barrier check
      pending_ms_log_id_ = max_log_id + 1;
      pending_ms_timestamp_ = ObTimeUtility::current_time();
      if (OB_INVALID_TIMESTAMP != membership_timestamp_ && membership_timestamp_ >= pending_ms_timestamp_) {
        pending_ms_timestamp_ = membership_timestamp_ + 1;
      }
    }

    if (OB_SUCC(ret)) {
      log_info.log_id_ = pending_ms_log_id_;
      log_info.timestamp_ = pending_ms_timestamp_;
      pending_member_for_add_.reset();
      pending_member_for_remove_.reset();
      pending_ms_type_ = MS_TYPE_CHANGE_QUORUM;
      pending_replica_num_ = new_quorum;
      if (check_barrier_condition_(pending_ms_log_id_) && (OB_SUCCESS == (ret = to_changing_()))) {
      } else {
        ATOMIC_STORE(&ms_state_, MS_PENDING);
      }
    }
    if (MS_PENDING != ATOMIC_LOAD(&ms_state_)) {
      reset_pending_status_();
    }
  }
  CLOG_LOG(INFO, "change_quorum", K(ret), K(partition_key_), K(curr_member_list), K(curr_quorum), K(new_quorum));
  return ret;
}

int ObLogMembershipMgr::change_member_list_to_self(const int64_t new_membership_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogMembershipMgr is not inited", K(ret), K_(partition_key));
  } else if (new_membership_version <= membership_timestamp_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN,
        "new_membership_version is smaller than current, unexpect",
        K(ret),
        K_(partition_key),
        K(new_membership_version),
        K(membership_timestamp_));
  } else {
    ObMemberList tmp_member_list;
    if (OB_FAIL(tmp_member_list.add_server(self_))) {
      CLOG_LOG(WARN, "add_server failed", K(ret), K_(partition_key));
    } else if (OB_FAIL(prev_member_list_.deep_copy(curr_member_list_))) {
      CLOG_LOG(WARN, "prev_member_list_ deep_copy failed", K_(partition_key), K(ret));
    } else if (OB_FAIL(curr_member_list_.deep_copy(tmp_member_list))) {
      CLOG_LOG(WARN, "curr_member_list_ deep_copy failed", K_(partition_key), K(ret));
    } else {
      membership_timestamp_ = new_membership_version;
      replica_num_ = curr_member_list_.get_member_number();
      if (ObReplicaTypeCheck::is_paxos_replica_V2(replica_type_)) {
        set_election_candidate_();
      }
    }
  }
  return ret;
}

// non-private table in standby cluster checks if need update election's member_list
int ObLogMembershipMgr::try_check_cluster_state_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!need_check_cluster_type_) {
    // no need check, skip
    CLOG_LOG(WARN, "no need check cluster state", K_(partition_key), K_(ms_proposal_id), K_(need_check_cluster_type));
  } else if (common::INVALID_CLUSTER_TYPE == GCTX.get_cluster_type()) {
    // cluster type is invalid, skip
    CLOG_LOG(WARN, "cluster type is invalid", K_(partition_key), K_(ms_proposal_id), K_(need_check_cluster_type));
  } else {
    if (GCTX.is_primary_cluster() || ms_proposal_id_.is_valid() || curr_member_list_.contains(self_)) {
      set_election_candidate_();
    }
    // cluster type is valid, reset flag
    need_check_cluster_type_ = false;
    CLOG_LOG(INFO,
        "try_check_cluster_state_ success",
        K_(partition_key),
        K_(ms_proposal_id),
        K_(curr_member_list),
        K_(need_check_cluster_type));
  }
  return ret;
}

}  // namespace clog
}  // namespace oceanbase
