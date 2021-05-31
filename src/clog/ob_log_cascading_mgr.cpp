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
#include "election/ob_election_mgr.h"
#include "storage/ob_partition_service.h"
#include "ob_log_callback_engine.h"
#include "ob_log_entry.h"
#include "ob_log_sliding_window.h"
#include "ob_log_state_mgr.h"
#include "share/ob_i_ps_cb.h"
#include "ob_i_log_engine.h"
#include "observer/ob_server_struct.h"
#include "share/ob_server_blacklist.h"

namespace oceanbase {
using namespace common;
using namespace share;

namespace clog {
ObLogCascadingMgr::ObLogCascadingMgr()
    : is_inited_(false),
      lock_(ObLatchIds::CLOG_CASCADING_INFO_LOCK),
      self_(),
      partition_key_(),
      parent_(),
      prev_parent_(),
      sw_(NULL),
      state_mgr_(NULL),
      mm_(NULL),
      log_engine_(NULL),
      ps_cb_(NULL),
      children_list_(),
      async_standby_children_(),
      sync_standby_child_(),
      get_parent_candidate_list_log_time_(OB_INVALID_TIMESTAMP),
      last_check_parent_time_(OB_INVALID_TIMESTAMP),
      last_check_child_state_time_(OB_INVALID_TIMESTAMP),
      parent_invalid_start_ts_(OB_INVALID_TIMESTAMP),
      reset_parent_warn_time_(OB_INVALID_TIMESTAMP),
      update_parent_warn_time_(OB_INVALID_TIMESTAMP),
      receive_reject_msg_warn_time_(OB_INVALID_TIMESTAMP),
      check_parent_invalid_warn_time_(OB_INVALID_TIMESTAMP),
      parent_invalid_warn_time_(OB_INVALID_TIMESTAMP),
      last_check_log_ts_(OB_INVALID_TIMESTAMP),
      last_replace_child_ts_(OB_INVALID_TIMESTAMP),
      reregister_begin_ts_(OB_INVALID_TIMESTAMP),
      last_renew_standby_loc_time_(OB_INVALID_TIMESTAMP),
      last_request_candidate_(),
      candidate_server_list_()
{}

ObLogCascadingMgr::~ObLogCascadingMgr()
{
  destroy();
}

int ObLogCascadingMgr::init(const common::ObAddr& self, const common::ObPartitionKey& partition_key,
    ObILogSWForCasMgr* sw, ObILogStateMgrForCasMgr* state_mgr, ObILogMembershipMgr* mm, ObILogEngine* log_engine,
    share::ObIPSCb* partition_service_cb)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (!self.is_valid() || !partition_key.is_valid() || NULL == sw || NULL == state_mgr || NULL == mm ||
             NULL == log_engine || NULL == partition_service_cb) {
    CLOG_LOG(
        ERROR, "invalid arguments", K(partition_key), K(self), KP(sw), KP(state_mgr), KP(mm), KP(partition_service_cb));
    ret = OB_INVALID_ARGUMENT;
  } else {
    self_ = self;
    partition_key_ = partition_key;
    parent_.reset();
    sw_ = sw;
    state_mgr_ = state_mgr;
    mm_ = mm;
    log_engine_ = log_engine;
    ps_cb_ = partition_service_cb;
    get_parent_candidate_list_log_time_ = OB_INVALID_TIMESTAMP;
    last_check_parent_time_ = OB_INVALID_TIMESTAMP;
    last_check_child_state_time_ = OB_INVALID_TIMESTAMP;
    check_parent_invalid_warn_time_ = OB_INVALID_TIMESTAMP;
    parent_invalid_warn_time_ = OB_INVALID_TIMESTAMP;
    last_check_log_ts_ = OB_INVALID_TIMESTAMP;
    last_replace_child_ts_ = OB_INVALID_TIMESTAMP;
    reregister_begin_ts_ = OB_INVALID_TIMESTAMP;
    last_renew_standby_loc_time_ = OB_INVALID_TIMESTAMP;
    last_request_candidate_.reset();
    candidate_server_list_.reset();
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogCascadingMgr init success", K_(partition_key));
  }
  return ret;
}

void ObLogCascadingMgr::destroy()
{
  if (IS_NOT_INIT) {
  } else {
    WLockGuard guard(lock_);
    is_inited_ = false;
    self_.reset();
    partition_key_.reset();
    parent_.reset();
    sw_ = NULL;
    state_mgr_ = NULL;
    mm_ = NULL;
    log_engine_ = NULL;
    ps_cb_ = NULL;
    children_list_.reset();
    async_standby_children_.reset();
    sync_standby_child_.reset();
    get_parent_candidate_list_log_time_ = OB_INVALID_TIMESTAMP;
    last_check_parent_time_ = OB_INVALID_TIMESTAMP;
    last_check_child_state_time_ = OB_INVALID_TIMESTAMP;
    parent_invalid_start_ts_ = OB_INVALID_TIMESTAMP;
    check_parent_invalid_warn_time_ = OB_INVALID_TIMESTAMP;
    parent_invalid_warn_time_ = OB_INVALID_TIMESTAMP;
    last_check_log_ts_ = OB_INVALID_TIMESTAMP;
    last_replace_child_ts_ = OB_INVALID_TIMESTAMP;
    reregister_begin_ts_ = OB_INVALID_TIMESTAMP;
    last_renew_standby_loc_time_ = OB_INVALID_TIMESTAMP;
    last_request_candidate_.reset();
    candidate_server_list_.reset();
    CLOG_LOG(INFO, "ObLogCascadingMgr::destroy finished", K_(partition_key));
  }
}

int ObLogCascadingMgr::get_children_list(ObCascadMemberList& list) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (OB_FAIL(list.deep_copy(children_list_))) {
    LOG_WARN("deep_copy failed", K(ret));
  }
  return ret;
}

int ObLogCascadingMgr::get_async_standby_children(ObCascadMemberList& list) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (OB_FAIL(list.deep_copy(async_standby_children_))) {
    LOG_WARN("deep_copy failed", K(ret));
  }
  return ret;
}

bool ObLogCascadingMgr::is_valid_child(const common::ObAddr& server) const
{
  RLockGuard guard(lock_);
  return is_valid_child_unlock_(server);
}

bool ObLogCascadingMgr::is_valid_child_unlock_(const common::ObAddr& server) const
{
  bool bool_ret = false;
  bool_ret = (children_list_.contains(server) || async_standby_children_.contains(server) ||
              server == sync_standby_child_.get_server());
  return bool_ret;
}

bool ObLogCascadingMgr::has_valid_child() const
{
  RLockGuard guard(lock_);
  return (children_list_.get_member_number() > 0);
}

bool ObLogCascadingMgr::has_valid_async_standby_child() const
{
  RLockGuard guard(lock_);
  return (async_standby_children_.get_member_number() > 0);
}

bool ObLogCascadingMgr::has_valid_sync_standby_child() const
{
  RLockGuard guard(lock_);
  return sync_standby_child_.is_valid();
}

int ObLogCascadingMgr::set_parent(const ObCascadMember& parent)
{
  return set_parent(parent.get_server(), parent.get_cluster_id());
}

int ObLogCascadingMgr::set_parent(const common::ObAddr& new_parent, const int64_t cluster_id)
{
  WLockGuard guard(lock_);
  return set_parent_(new_parent, cluster_id);
}

int ObLogCascadingMgr::set_parent_(const common::ObAddr& new_parent_addr, const int64_t cluster_id)
{
  // caller holds write lock
  int ret = OB_SUCCESS;
  const ObAddr leader = state_mgr_->get_leader();
  const ObAddr primary_leader = state_mgr_->get_primary_leader_addr();
  ObCascadMember new_parent(new_parent_addr, cluster_id);
  ObCascadMember cur_parent = parent_;
  if (!new_parent.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(new_parent));
  } else if (state_mgr_->is_cluster_allow_vote() && ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) &&
             FOLLOWER == state_mgr_->get_role() && leader.is_valid() && new_parent_addr != leader) {
    CLOG_LOG(WARN,
        "self is paxos member, but new_parent is not leader",
        K_(partition_key),
        K(new_parent),
        K(cur_parent),
        K(leader));
  } else if (STANDBY_LEADER == state_mgr_->get_role() && GCTX.is_sync_level_on_standby() && primary_leader.is_valid() &&
             new_parent_addr != primary_leader) {
    // in max protection mode, the parent of standby leader must be primary leader
    CLOG_LOG(WARN,
        "standby_leader is in max_protection mode, but new_parent is not primary_leader",
        K_(partition_key),
        K(new_parent),
        K(cur_parent),
        K(primary_leader));
  } else if (self_ == new_parent_addr) {
    // ignore self
  } else if (cur_parent == new_parent) {
    // no need update
  } else if (is_valid_child_unlock_(new_parent_addr) && OB_FAIL(try_remove_child_unlock_(new_parent_addr))) {
    // new_parent maybe my child, so remove it firstly
    CLOG_LOG(WARN, "try_remove_child_unlock_ failed", K(ret), K_(partition_key), K(new_parent_addr));
  } else {
    state_mgr_->reset_need_rebuild();
    state_mgr_->reset_fetch_state();
    prev_parent_ = parent_;
    parent_ = new_parent;
    if (partition_reach_time_interval(60 * 1000 * 1000, update_parent_warn_time_)) {
      CLOG_LOG(
          INFO, "update parent", K_(partition_key), K(cur_parent), K(new_parent), K(prev_parent_), K_(children_list));
    }
  }
  if (parent_.get_server().is_valid()) {
    // parent is valid, reset parent_invalid_start_ts_
    parent_invalid_start_ts_ = OB_INVALID_TIMESTAMP;
  }
  return ret;
}

void ObLogCascadingMgr::reset_parent(const uint32_t reset_type)
{
  WLockGuard guard(lock_);
  reset_parent_(reset_type);
}

void ObLogCascadingMgr::reset_parent_(const uint32_t reset_type)
{
  // caller holds write lock
  state_mgr_->reset_need_rebuild();
  state_mgr_->reset_fetch_state();
  if (parent_.is_valid()) {
    // record the time that parent becomes invalid
    prev_parent_ = parent_;
    parent_invalid_start_ts_ = ObTimeUtility::current_time();
  }
  // following situations will trigger reset parent_invalid_start_ts_
  // 1. self is elected to be leader
  // 2. self is set offline
  if (SELF_ELECTED == reset_type || SELF_OFFLINE == reset_type || STANDBY_UPDATE == reset_type) {
    parent_invalid_start_ts_ = OB_INVALID_TIMESTAMP;
  }
  if (partition_reach_time_interval(60 * 1000 * 1000, reset_parent_warn_time_)) {
    CLOG_LOG(INFO, "reset parent", K_(partition_key), K(reset_type), K_(parent), K_(prev_parent));
  }
  parent_.reset();
}

int ObLogCascadingMgr::update_sync_standby_child(const common::ObAddr& server, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObCascadMember new_sync_child(server, cluster_id);
  if (!server.is_valid() || OB_INVALID_CLUSTER_ID == cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", K_(partition_key), K(server), K(cluster_id));
  } else if (cluster_id == state_mgr_->get_self_cluster_id()) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN,
        "cluster_id is same with self, cannot update sync child",
        K(ret),
        K_(partition_key),
        K(server),
        K(cluster_id));
  } else {
    WLockGuard guard(lock_);
    // check if server is in async_standby_children_
    if (async_standby_children_.contains(server) && OB_FAIL(async_standby_children_.remove_server(server))) {
      CLOG_LOG(WARN, "async_standby_children_ remove server failed", K_(partition_key), K(ret), K(server));
    } else {
      sync_standby_child_ = new_sync_child;
      CLOG_LOG(INFO, "update_sync_standby_child success", K_(partition_key), K(ret), K_(sync_standby_child));
    }
  }
  return ret;
}

int ObLogCascadingMgr::notify_change_parent(const share::ObCascadMemberList& member_list)
{
  // notify non-paxos child to change parent
  int ret = OB_SUCCESS;
  const int64_t child_cnt = member_list.get_member_number();
  if (!member_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (child_cnt > 0) {
    ObReplicaMsgType msg_type = OB_REPLICA_MSG_TYPE_NOT_PARENT;
    for (int64_t i = 0; i < child_cnt && OB_SUCC(ret); ++i) {
      ObCascadMember cur_member;
      if (OB_FAIL(member_list.get_member_by_index(i, cur_member))) {
        CLOG_LOG(WARN, "member_list.get_server_by_index failed", K(ret), K_(partition_key), K(i));
      } else if (OB_FAIL(reject_server(cur_member.get_server(), cur_member.get_cluster_id(), msg_type))) {
        CLOG_LOG(WARN, "reject_server failed", K(ret), K_(partition_key), K(cur_member), K(msg_type));
      } else {
        // do nothing
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObLogCascadingMgr::process_reregister_msg(const ObCascadMember& new_leader)
{
  int ret = OB_SUCCESS;
  ObRegion self_region = DEFAULT_REGION_NAME;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogCascadingMgr is not inited", K(ret), K_(partition_key));
  } else if (!new_leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K_(partition_key), K(new_leader));
  } else if (OB_FAIL(get_server_region_(self_, self_region))) {
    CLOG_LOG(WARN, "get_server_region failed", K(ret), K_(partition_key));
  } else {
    WLockGuard guard(lock_);
    if (OB_FAIL(fetch_register_server(
            new_leader.get_server(), new_leader.get_cluster_id(), self_region, state_mgr_->get_idc(), true, false))) {
      CLOG_LOG(WARN, "fetch_register_server failed", K_(partition_key), K(ret));
    } else {
      reset_last_request_state_unlock_();
      reregister_begin_ts_ = ObTimeUtility::current_time();
    }
  }
  CLOG_LOG(INFO, "process_reregister_msg finished", K(ret), K_(partition_key), K(new_leader), K_(reregister_begin_ts));
  return ret;
}

int ObLogCascadingMgr::reject_server(
    const common::ObAddr& server, const int64_t dst_cluster_id, const int32_t msg_type) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_REPLICA_MSG_TYPE_UNKNOWN == msg_type) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(server), K(msg_type));
  } else if (OB_FAIL(log_engine_->reject_server(server, dst_cluster_id, partition_key_, msg_type))) {
    CLOG_LOG(WARN, "reject_server failed", K_(partition_key), K(ret), K(server), K(msg_type));
  } else {
    // do nothing
  }
  return ret;
}

int ObLogCascadingMgr::process_reject_msg(const common::ObAddr& server, const int32_t msg_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || OB_REPLICA_MSG_TYPE_UNKNOWN == msg_type) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    WLockGuard guard(lock_);
    if (OB_REPLICA_MSG_TYPE_NOT_PARENT == msg_type) {
      // server is not my parent
      if (server == parent_.get_server()) {
        if (state_mgr_->is_cluster_allow_vote() && ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) &&
            server == state_mgr_->get_leader()) {
          // paxos replica that colocates with strong leader won't reset parent,
          // because maybe leader is not elected yet
          if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
            CLOG_LOG(WARN,
                "self is paxos replica in same cluster with leader, ignore leader's reject msg",
                K_(partition_key),
                K(ret),
                K(server),
                K(msg_type));
          }
        } else {
          const uint32_t reset_type = ResetPType::REJECT_MSG;
          reset_parent_(reset_type);
        }
      }
    } else if (OB_REPLICA_MSG_TYPE_NOT_CHILD == msg_type) {
      // server is not my child, need remove it
      if (OB_FAIL(try_remove_child_unlock_(server))) {
        CLOG_LOG(WARN, "try_remove_child_unlock_ failed", K_(partition_key), K(ret), K(server));
      }
    } else if (OB_REPLICA_MSG_TYPE_NOT_MASTER == msg_type) {
      // server is not leader, wait local leader updates
      if (partition_reach_time_interval(60 * 1000 * 1000, receive_reject_msg_warn_time_)) {
        CLOG_LOG(INFO,
            "receive reject msg: sender is not leader",
            K_(partition_key),
            K(server),
            "leader",
            state_mgr_->get_leader());
      }
    } else if (OB_REPLICA_MSG_TYPE_NOT_EXIST == msg_type) {
      // server not exist, remove it
      if (OB_FAIL(try_remove_child_unlock_(server))) {
        CLOG_LOG(WARN, "try_remove_child_unlock_ failed", K_(partition_key), K(ret), K(server));
      }
    } else if (OB_REPLICA_MSG_TYPE_DISABLED_STATE == msg_type) {
      // server is in disabled state, it cannot become my parent.
      if (partition_reach_time_interval(30 * 1000 * 1000, receive_reject_msg_warn_time_)) {
        CLOG_LOG(INFO,
            "receive reject msg: sender is in disabled state",
            K_(partition_key),
            K(server),
            "leader",
            state_mgr_->get_leader());
      }
    } else {
      // do nothing
    }
    if (REACH_TIME_INTERVAL(500 * 1000)) {
      CLOG_LOG(INFO, "process_reject_msg", K_(partition_key), K(server), K(msg_type));
    }
  }
  return ret;
}

int ObLogCascadingMgr::process_region_change(const common::ObRegion& new_region)
{
  // called when self region changed
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (new_region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int tmp_ret = OB_SUCCESS;
    if (LEADER != state_mgr_->get_role()) {
      WLockGuard guard(lock_);
      // when follower/standby_leader's region changed, it need notify different region children, and
      // 1) if self is non-paxos replica, then reset parent and send request to leader
      // 2) if self is paxos replica, no need notify leader
      ObCascadMember cascad_leader;
      (void)state_mgr_->get_cascad_leader(cascad_leader);
      const ObAddr leader = cascad_leader.get_server();
      const int64_t leader_cluster_id = cascad_leader.get_cluster_id();
      if (leader.is_valid() && !state_mgr_->is_offline()) {
        if (!ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) ||
            STANDBY_LEADER == state_mgr_->get_role()) {
          const uint32_t reset_type = ResetPType::REGION_CHANGE;
          reset_parent_(reset_type);
        }
        if (OB_SUCCESS != (tmp_ret = fetch_register_server(
                               leader, leader_cluster_id, new_region, state_mgr_->get_idc(), true, false))) {
          CLOG_LOG(WARN, "fetch_register_server failed", K_(partition_key), K(tmp_ret));
        } else {
          reset_last_request_state_unlock_();
          CLOG_LOG(INFO, "self region changed, fetch_register_server success", K_(partition_key), K(new_region));
        }
      }
      // notify diff region children to change parent
      ObReplicaMsgType msg_type = OB_REPLICA_MSG_TYPE_NOT_PARENT;
      const int64_t child_cnt = children_list_.get_member_number();
      const bool is_follower = (FOLLOWER == state_mgr_->get_role());
      if (is_follower) {
        // when follower's region changed, it need reset children_list directly
        for (int64_t i = 0; i < child_cnt; ++i) {
          ObCascadMember cur_member;
          if (OB_SUCCESS != (tmp_ret = children_list_.get_member_by_index(i, cur_member))) {
            CLOG_LOG(WARN, "get_server_by_index failed", K(tmp_ret), K_(partition_key), K(cur_member));
          } else if (!cur_member.is_valid()) {
            CLOG_LOG(WARN, "invalid member", K(ret), K_(partition_key), K(cur_member));
          } else if (OB_SUCCESS !=
                     (tmp_ret = reject_server(cur_member.get_server(), cur_member.get_cluster_id(), msg_type))) {
            CLOG_LOG(WARN, "reject_server failed", K_(partition_key), K(tmp_ret), K(cur_member), K(msg_type));
          } else {
            // do nothing
          }
        }
      }
      // when region changed, non strong leader replica need reset other cluster children
      const int64_t async_child_cnt = async_standby_children_.get_member_number();
      for (int64_t i = 0; i < async_child_cnt; ++i) {
        ObCascadMember cur_member;
        if (OB_SUCCESS != (tmp_ret = async_standby_children_.get_member_by_index(i, cur_member))) {
          CLOG_LOG(WARN, "get_server_by_index failed", K(tmp_ret), K_(partition_key), K(cur_member));
        } else if (!cur_member.is_valid()) {
          CLOG_LOG(WARN, "invalid member", K(ret), K_(partition_key), K(cur_member));
        } else if (OB_SUCCESS !=
                   (tmp_ret = reject_server(cur_member.get_server(), cur_member.get_cluster_id(), msg_type))) {
          CLOG_LOG(WARN, "reject_server failed", K_(partition_key), K(tmp_ret), K(cur_member), K(msg_type));
        } else {
          // do nothing
        }
      }
    }
  }

  return ret;
}

bool ObLogCascadingMgr::has_async_standby_child_(const int64_t cluster_id, common::ObAddr& server) const
{
  bool bool_ret = false;
  if (IS_NOT_INIT) {
  } else {
    int tmp_ret = OB_SUCCESS;
    ObCascadMember tmp_member;
    const int64_t child_cnt = async_standby_children_.get_member_number();
    for (int64_t i = 0; !bool_ret && i < child_cnt && OB_SUCCESS == tmp_ret; ++i) {
      tmp_member.reset();
      if (OB_SUCCESS != (tmp_ret = children_list_.get_member_by_index(i, tmp_member))) {
        CLOG_LOG(WARN, "get_member_by_index failed", K_(partition_key), K(tmp_ret), K(i));
      } else if (!tmp_member.is_valid()) {
        CLOG_LOG(WARN, "tmp_member is invalid", K_(partition_key));
      } else if (tmp_member.get_cluster_id() == cluster_id) {
        server = tmp_member.get_server();
        bool_ret = true;
      } else {
        // do nothing
      }
    }
  }
  return bool_ret;
}

int ObLogCascadingMgr::primary_process_protect_mode_switch()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (LEADER != state_mgr_->get_role()) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(WARN, "self is not leader", K_(partition_key), K(ret));
  } else if (!GCTX.need_sync_to_standby()) {
    // changed to async mode(max performance/max availability), need change sync child to async child
    ObCascadMember old_sync_child = sync_standby_child_;
    if (old_sync_child.is_valid() && OB_FAIL(try_add_child_unlock_(old_sync_child))) {
      CLOG_LOG(WARN, "try_add_child_unlock_ failed", K_(partition_key), K(ret), K(old_sync_child));
    }
    sync_standby_child_.reset();
  } else if (GCTX.need_sync_to_standby()) {
    // changed to sync mode(max protection/max availability), try to update sync child
    int64_t sync_cluster_id = OB_INVALID_CLUSTER_ID;
    ObAddr dst_standby_child;
    if (OB_FAIL(get_sync_standby_cluster_id(sync_cluster_id)) || OB_INVALID_CLUSTER_ID == sync_cluster_id) {
      CLOG_LOG(WARN, "get_sync_standby_cluster_id failed", K_(partition_key), K(ret));
    } else if (has_async_standby_child_(sync_cluster_id, dst_standby_child)) {
      // if it is aysnc child, change it to sync child
      if (OB_FAIL(async_standby_children_.remove_server(dst_standby_child))) {
        CLOG_LOG(WARN, "async_standby_children_ remove server failed", K_(partition_key), K(ret), K(dst_standby_child));
      } else {
        // update sync_child
        ObCascadMember new_sync_child(dst_standby_child, sync_cluster_id);
        sync_standby_child_ = new_sync_child;
        CLOG_LOG(INFO, "update sync_standby_child_ success", K_(partition_key), K_(sync_standby_child));
      }
    } else {
      // need get sync stadnby_leader from location cache, it may returns -4023, just ignore
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = leader_try_update_sync_standby_child_unlock_(true))) {
        CLOG_LOG(WARN, "leader_try_update_sync_standby_child_unlock_ failed", K_(partition_key), K(tmp_ret));
      }
    }
  } else {
  }

  return ret;
}

int ObLogCascadingMgr::leader_try_update_sync_standby_child(const bool need_renew_loc)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  ret = leader_try_update_sync_standby_child_unlock_(need_renew_loc);
  return ret;
}

int ObLogCascadingMgr::leader_try_update_sync_standby_child_unlock_(const bool need_renew_loc)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (LEADER != state_mgr_->get_role()) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(WARN, "self is not leader", K_(partition_key), K(ret));
  } else if (!GCTX.need_sync_to_standby() ||
             ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    // not sync mode or private talbe, skip
  } else {
    int64_t sync_cluster_id = OB_INVALID_CLUSTER_ID;
    if (OB_FAIL(get_sync_standby_cluster_id(sync_cluster_id)) || OB_INVALID_CLUSTER_ID == sync_cluster_id) {
      CLOG_LOG(WARN, "get_sync_standby_cluster_id failed", K_(partition_key), K(ret));
    } else {
      // retrieve sync standby_leader from lcoation cache with renew
      bool need_renew = need_renew_loc;
      const int64_t now = ObTimeUtility::current_time();
      if (need_renew && (OB_INVALID_TIMESTAMP == last_renew_standby_loc_time_ ||
                            now - last_renew_standby_loc_time_ >= PRIMARY_RENEW_LOCATION_TIME_INTERVAL)) {
        // control interval when need_renew is true
        last_renew_standby_loc_time_ = now;
      } else {
        // not reach renew interval, cannot renew
        need_renew = false;
      }
      ObAddr dst_leader;
      (void)state_mgr_->async_get_dst_cluster_leader_from_loc_cache(need_renew, sync_cluster_id, dst_leader);
      if (!dst_leader.is_valid()) {
        // cannot get valid standby_leader, return -4023
        ret = OB_EAGAIN;
      } else {
        ObCascadMember new_sync_child(dst_leader, sync_cluster_id);
        // check if sync_child is already in async_children
        if (async_standby_children_.contains(dst_leader) &&
            OB_FAIL(async_standby_children_.remove_server(dst_leader))) {
          CLOG_LOG(WARN, "async_standby_children_ remove server failed", K_(partition_key), K(ret), K(dst_leader));
        } else {
          // update sync_child
          sync_standby_child_ = new_sync_child;
          CLOG_LOG(INFO, "update sync_standby_child_ success", K_(partition_key), K_(sync_standby_child));
        }
      }
    }
  }

  return ret;
}

int ObLogCascadingMgr::get_sync_standby_cluster_id(int64_t& sync_cluster_id)
{
  // The caller ensures that only LEADER calls this interface
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!GCTX.need_sync_to_standby() ||
             ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "not in max protection mode", K_(partition_key), K(ret));
  } else {
    int64_t tmp_cluster_id = OB_INVALID_CLUSTER_ID;
    if (OB_FAIL(GCTX.get_sync_standby_cluster_id(tmp_cluster_id))) {
      CLOG_LOG(WARN, "GCTX.get_sync_standby_cluster_id failed", K_(partition_key), K(ret));
    } else {
      sync_cluster_id = tmp_cluster_id;
    }
  }
  return ret;
}

/* assign parent for server */
int ObLogCascadingMgr::process_fetch_reg_server_req(const common::ObAddr& server,
    const common::ObReplicaType replica_type, const bool is_self_lag_behind, const bool is_request_leader,
    const bool is_need_force_register, const common::ObRegion& region, const int64_t cluster_id, const ObIDC& idc,
    const common::ObRegion& self_region)
{
  UNUSED(idc);
  int ret = OB_SUCCESS;
  bool is_need_response = true;
  int64_t self_cluster_id = common::INVALID_CLUSTER_ID;
  const ObCascadMember member(server, cluster_id);

  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (server == self_) {
    // ignore self
    is_need_response = false;
  } else if (state_mgr_->is_offline()) {
    // self is offline, reject server
    ObReplicaMsgType msg_type = OB_REPLICA_MSG_TYPE_NOT_PARENT;
    (void)reject_server(server, cluster_id, msg_type);
    is_need_response = false;
  } else if (is_valid_cluster_id(cluster_id) && cluster_id != state_mgr_->get_self_cluster_id() &&
             GCTX.is_in_disabled_state()) {
    // server is from diff cluster and self is in disabled state
    // cannot assign parent
    ObReplicaMsgType msg_type = OB_REPLICA_MSG_TYPE_DISABLED_STATE;
    (void)reject_server(server, cluster_id, msg_type);
    is_need_response = false;
    CLOG_LOG(WARN,
        "server is from diff cluster and self is in disabled state, cannot assign parent",
        K_(partition_key),
        K(server),
        K(cluster_id),
        "self_cluster_id",
        state_mgr_->get_self_cluster_id());
  } else {
    bool is_from_diff_cluster = false;
    self_cluster_id = state_mgr_->get_self_cluster_id();
    int tmp_ret = OB_SUCCESS;
    // record request server's cluster_id, region
    if (is_valid_cluster_id(cluster_id) && cluster_id != self_cluster_id) {
      is_from_diff_cluster = true;
      if (OB_SUCCESS != (tmp_ret = ps_cb_->record_server_region(server, region))) {
        CLOG_LOG(WARN,
            "record_server_region failed",
            K_(partition_key),
            K(tmp_ret),
            K(server),
            K(cluster_id),
            K(self_cluster_id),
            K(region));
      }
    }
    ObCascadMemberList candidate_member_list;
    bool is_need_assign_parent = true;
    bool is_assign_parent_succeed = false;
    ObRegRespMsgType resp_msg = OB_REG_RESP_MSG_UNKNOWN;
    common::ObAddr assigned_parent;
    int64_t assigned_parent_cluster_id = self_cluster_id;

    // rules that assigning parent for non-paxos replicas:
    // if self is a follwer, try to become its parent Preferentially
    // if self is leader, try to allocate candidates from children
    // 0. is_need_force_register = true, leader need force add it to children
    if (LEADER == state_mgr_->get_role() || STANDBY_LEADER == state_mgr_->get_role()) {
      if (cluster_id == self_cluster_id && ObReplicaTypeCheck::is_paxos_replica_V2(replica_type) &&
          !mm_->get_curr_member_list().contains(server)) {
        // for rpelica that not in same cluster with leader, no need assing parent
        is_need_assign_parent = false;
        is_need_response = false;
      } else if (mm_->get_curr_member_list().contains(server)) {
        // from version 2.20, leader need assign parent for paxos replica
        is_assign_parent_succeed = true;
      } else if (is_from_diff_cluster) {
        if (LEADER == state_mgr_->get_role()) {
          bool is_sync_standby_member = false;
          int64_t sync_cluster_id = OB_INVALID_CLUSTER_ID;
          if (GCTX.need_sync_to_standby() &&
              !ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id())) {
            if (OB_FAIL(get_sync_standby_cluster_id(sync_cluster_id))) {
              CLOG_LOG(WARN, "get_sync_standby_cluster_id failed", K_(partition_key), K(ret));
            } else if (sync_cluster_id == cluster_id) {
              is_sync_standby_member = true;
            } else {
            }
          }
          if (is_sync_standby_member) {
            // sync standby replica must be leader's child
            sync_standby_child_ = member;
            is_assign_parent_succeed = true;
            CLOG_LOG(INFO, "update sync_standby_child_ success", K_(partition_key), K(member), K_(sync_standby_child));
          } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2270) {
            // for versions before 2.2.70, standby replica must be leader's child
            if (OB_FAIL(try_add_child_unlock_(member))) {
              CLOG_LOG(WARN, "try_add_child_unlock_ failed", K_(partition_key), K(ret), K(member));
            } else {
              is_assign_parent_succeed = true;
            }
          } else {
            if (REACH_TIME_INTERVAL(100 * 1000)) {
              CLOG_LOG(INFO,
                  "self is leader, receive non-max-protect standby request",
                  K_(partition_key),
                  K(ret),
                  K(member),
                  K(cluster_id));
            }
          }
        } else {
          // when standby_leader receives other cluster's server request,
          // if server's region is same with self, then it will become my child directly.
          CLOG_LOG(INFO,
              "self is standby_leader, receive diff cluster register req",
              K_(partition_key),
              K(ret),
              K(member),
              K(self_cluster_id),
              "role",
              state_mgr_->get_role());
          if (self_region != DEFAULT_REGION_NAME && region != self_region) {
            // if server's region is not same with self, return err msg
            resp_msg = OB_REG_RESP_MSG_REGION_DIFF;
          } else if (is_from_diff_cluster && is_async_standby_children_full_unlock_()) {
            // if own standby_children list is already full, notify request server
            resp_msg = OB_REG_RESP_MSG_CHILD_LIST_FULL;
          } else if (OB_FAIL(try_add_child_unlock_(member))) {
            CLOG_LOG(WARN, "try_add_child_unlock_ failed", K_(partition_key), K(ret), K(member));
            ret = OB_SUCCESS;
          } else {
            assigned_parent = self_;
            assigned_parent_cluster_id = self_cluster_id;
            is_assign_parent_succeed = true;
          }
        }
      } else if (is_request_leader && is_need_force_register) {
        if (OB_FAIL(leader_force_add_child_unlock_(member))) {
          CLOG_LOG(WARN, "leader_force_add_child_unlock_ failed", K_(partition_key), K(ret), K(member));
        } else {
          is_assign_parent_succeed = true;
        }
      }
      if (is_assign_parent_succeed) {
        assigned_parent = self_;
        assigned_parent_cluster_id = self_cluster_id;
      }
    } else {
      // 1. self is follwer, if own children list is not full, it will become its parent directly.
      if (is_request_leader) {
        // If the requester thinks I am the leader, I will not respond
        ret = OB_NOT_MASTER;
        is_need_response = false;
      } else if (self_region != DEFAULT_REGION_NAME && region != self_region) {
        // The requester's region is not same with own, an error code is returned
        resp_msg = OB_REG_RESP_MSG_REGION_DIFF;
      } else if (is_self_lag_behind) {
        // If self is sick(unsync or lag behind requester), it need notify requester.
        resp_msg = OB_REG_RESP_MSG_SICK;
        candidate_member_list.reset();
        if (!ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) && parent_.is_valid()) {
          // If self is non-paxos replica, it need notify requester to replace self in my parent's children_list.
          if (OB_FAIL(candidate_member_list.add_member(parent_))) {
            CLOG_LOG(WARN, "candidate_member_list.add_server failed", K_(partition_key), K(ret));
          }
        }
      } else if (!is_from_diff_cluster && is_children_list_full_unlock_()) {
        // children list is full, notify requester
        resp_msg = OB_REG_RESP_MSG_CHILD_LIST_FULL;
      } else if (is_from_diff_cluster && is_async_standby_children_full_unlock_()) {
        // follower receives other cluster's reuqest, and own children list is full, notify requester
        resp_msg = OB_REG_RESP_MSG_CHILD_LIST_FULL;
      } else if (OB_FAIL(try_add_child_unlock_(member))) {
        CLOG_LOG(WARN, "try_add_child_unlock_ failed", K_(partition_key), K(ret), K(member));
        ret = OB_SUCCESS;
      } else {
        assigned_parent = self_;
        assigned_parent_cluster_id = self_cluster_id;
        is_assign_parent_succeed = true;
      }
    }
    // 2. If I have not become parent and I'm not sick, try to assign candidates
    bool is_standby_switching_leader = false;
    if (is_self_lag_behind && LEADER == state_mgr_->get_role() && GCTX.is_in_standby_switching_state()) {
      // In switching state during switchover, leader won't update next_log_ts,
      // but assigning logic is still running normally.
      is_standby_switching_leader = true;
    }
    if (OB_SUCC(ret) && !is_assign_parent_succeed && is_need_assign_parent &&
        (false == is_self_lag_behind ||
            STANDBY_LEADER == state_mgr_->get_role()  // standby_leader need assign parent even unsync
            || is_standby_switching_leader)) {
      bool is_become_my_child = false;
      candidate_member_list.reset();
      if (OB_FAIL(get_parent_candidate_list_(
              server, replica_type, region, cluster_id, self_region, candidate_member_list, is_become_my_child))) {
        CLOG_LOG(WARN, "get_parent_candidate_list_ failed", K_(partition_key), K(ret));
      }
      if (is_become_my_child) {
        assigned_parent = self_;
        assigned_parent_cluster_id = self_cluster_id;
        is_assign_parent_succeed = true;
      } else if (candidate_member_list.get_member_number() <= 0) {
        CLOG_LOG(WARN, "candidate_member_list is null", K_(partition_key), K(server));
      } else {
        // do nothing
      }
    }

    if (is_assign_parent_succeed) {
      candidate_member_list.reset();
      if (OB_FAIL(candidate_member_list.add_member(ObCascadMember(assigned_parent, assigned_parent_cluster_id)))) {
        CLOG_LOG(WARN,
            "candidate_member_list.add_server failed",
            K_(partition_key),
            K(ret),
            K(candidate_member_list),
            K(assigned_parent),
            K(assigned_parent_cluster_id));
      }
    }
    if (is_need_response) {
      if (OB_FAIL(log_engine_->response_register_server(
              server, cluster_id, partition_key_, is_assign_parent_succeed, candidate_member_list, resp_msg))) {
        CLOG_LOG(WARN,
            "response_register_server failed",
            K_(partition_key),
            K(ret),
            K_(self),
            K(server),
            K(resp_msg),
            K(cluster_id));
      }
    }
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(INFO,
          "finish process_fetch_reg_server_req",
          K_(partition_key),
          K(ret),
          K(server),
          K(region),
          K(cluster_id),
          K(replica_type),
          K(is_assign_parent_succeed),
          K(resp_msg),
          K(candidate_member_list));
    }
  }

  return ret;
}

/* assgin parent response */
int ObLogCascadingMgr::process_fetch_reg_server_resp(const common::ObAddr& sender, const bool is_assign_parent_succeed,
    const ObCascadMemberList& candidate_list, const int32_t msg_type, const common::ObRegion& self_region)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!sender.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(is_assign_parent_succeed), K(candidate_list));
  } else if (parent_.is_valid() && !is_in_reregister_period()) {
    // my parent is valid and I'm not in reregister period, ignore msg
    reset_last_request_state_unlock_();
    reregister_begin_ts_ = OB_INVALID_TIMESTAMP;
  } else if (last_request_candidate_.is_valid() && sender != last_request_candidate_) {
    // sender is not last_request_candidate_
    CLOG_LOG(WARN, "sender is not last_request_candidate_", K_(partition_key), K(sender), K(last_request_candidate_));
  } else if (is_assign_parent_succeed) {
    // assign parent succeussfully
    ObCascadMember new_parent;
    if (OB_FAIL(candidate_list.get_member_by_index(0, new_parent))) {
      CLOG_LOG(WARN, "candidate_list.get_server_by_index failed", K_(partition_key), K(ret), K(candidate_list));
    } else if (OB_FAIL(set_parent_(new_parent.get_server(), new_parent.get_cluster_id()))) {
      CLOG_LOG(WARN, "set_parent_ failed", K_(partition_key), K(ret), K(new_parent));
    } else {
      // assign finished, reset candidate state
      reset_last_request_state_unlock_();
      // reset reregister state
      reregister_begin_ts_ = OB_INVALID_TIMESTAMP;
    }
  } else {
    bool need_request_next_candidate = false;
    bool need_request_next_level = true;
    if (OB_REG_RESP_MSG_SICK == msg_type) {
      if (candidate_list.get_member_number() > 0) {
        // sender is sick and non-paxos type, try to notify its parent to replace it
        ObCascadMember its_parent;
        if (OB_FAIL(candidate_list.get_member_by_index(0, its_parent))) {
          CLOG_LOG(WARN, "get_server_by_index failed", K_(partition_key), K(ret), K(candidate_list));
        } else if (OB_FAIL(log_engine_->request_replace_sick_child(
                       its_parent.get_server(), its_parent.get_cluster_id(), partition_key_, sender))) {
          CLOG_LOG(WARN, "request_replace_sick_child failed", K(ret), K_(partition_key), K(its_parent), K(sender));
        } else {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            CLOG_LOG(INFO, "detect sick candidate, try to replcace it", K_(partition_key), K(its_parent), K(sender));
          }
          last_request_candidate_ = its_parent.get_server();
          need_request_next_level = false;
        }
      } else {
        // server is sick but cannot be replaced, need request next candidate
        need_request_next_candidate = true;
      }
    } else if (OB_REG_RESP_MSG_CHILD_LIST_FULL == msg_type) {
      // server's children list has been full, need request next candidate
      need_request_next_candidate = true;
    }
    if (need_request_next_candidate) {
      if (candidate_server_list_.get_member_number() <= 0) {
        // candidate list is empty, need request next level
        need_request_next_level = true;
      } else if (OB_FAIL(try_request_next_candidate_(self_region))) {
        CLOG_LOG(WARN, "try_request_next_candidate_ t failed", K_(partition_key), K(ret));
      } else {
        need_request_next_level = false;
      }
    }
    if (need_request_next_level) {
      if (OB_FAIL(candidate_server_list_.deep_copy(candidate_list))) {
        CLOG_LOG(WARN, "deep_copy candidate_list failed", K_(partition_key), K(ret), K(candidate_list));
      } else if (OB_FAIL(try_request_next_candidate_(self_region))) {
        // request failed, wait next check cycle to trigger request again
        CLOG_LOG(WARN, "try_request_next_candidate_ failed", K_(partition_key), K(ret), K(candidate_list));
      } else {
        // do nothing
      }
    }
  }
  if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
    CLOG_LOG(INFO,
        "finish process_fetch_reg_server_resp",
        K(ret),
        K_(partition_key),
        K(sender),
        K(parent_),
        K(is_assign_parent_succeed),
        K(candidate_list),
        K(msg_type));
  }

  return ret;
}

int ObLogCascadingMgr::try_replace_sick_child(
    const common::ObAddr& sender, const int64_t cluster_id, const common::ObAddr& sick_child)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!sender.is_valid() || !sick_child.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(sender), K(sick_child));
  } else {
    WLockGuard guard(lock_);
    const int64_t now = ObTimeUtility::current_time();
    ObCascadMemberList candidate_member_list;
    ObRegRespMsgType resp_msg = OB_REG_RESP_MSG_UNKNOWN;
    const ObCascadMember new_child(sender, cluster_id);
    if (now - last_replace_child_ts_ < 10 * 1000 * 1000 || mm_->get_curr_member_list().contains(sick_child)) {
      // Only one replacement is allowed within 10s
      // paxos member cannot be replaced
      ret = OB_OP_NOT_ALLOW;
    } else if (!is_valid_child_unlock_(sick_child)) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(
          WARN, "sick server is not my child", K_(partition_key), K(ret), K(sender), K(sick_child), K(children_list_));
    } else if (OB_FAIL(try_remove_child_unlock_(sick_child))) {
      CLOG_LOG(WARN, "try_remove_child_unlock_ failed", K_(partition_key), K(sick_child), K(children_list_));
    } else if (OB_FAIL(try_add_child_unlock_(new_child))) {
      CLOG_LOG(WARN, "try_add_child_unlock_ failed", K_(partition_key), K(ret), K(new_child), K(children_list_));
    } else {
      // replace successfully, become requester's parent and send notification
      last_replace_child_ts_ = now;
      const int64_t self_cluster_id = state_mgr_->get_self_cluster_id();
      ObCascadMember member(self_, self_cluster_id);
      if (OB_FAIL(candidate_member_list.add_member(member))) {
        CLOG_LOG(WARN, "candidate_member_list add_member failed", K_(partition_key), K(ret), K(member));
      } else if (OB_FAIL(log_engine_->response_register_server(
                     sender, cluster_id, partition_key_, true, candidate_member_list, resp_msg))) {
        CLOG_LOG(WARN,
            "response_register_server failed",
            K_(partition_key),
            K(ret),
            K_(self),
            K(sender),
            K(cluster_id),
            K(resp_msg));
      } else {
        CLOG_LOG(INFO,
            "finish try_replace_sick_child",
            K_(partition_key),
            K(ret),
            K(sender),
            K(sick_child),
            K(candidate_member_list),
            K(children_list_));
      }
    }
  }

  return ret;
}

bool ObLogCascadingMgr::is_region_recorded_(const RegionArray& region_list, const common::ObRegion region) const
{
  bool bool_ret = false;

  if (region_list.count() <= 0) {
    // empty list, skip
  } else {
    const int64_t list_cnt = region_list.count();
    for (int64_t i = 0; i < list_cnt && !bool_ret; ++i) {
      if (region == region_list.at(i)) {
        bool_ret = true;
      }
    }
  }

  return bool_ret;
}

int ObLogCascadingMgr::leader_get_dup_region_child_list_(ObCascadMemberList& dup_region_list) const
{
  // The leader periodically checks whether there are same region children
  // if true, try to remove duplicat-region child
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (LEADER != state_mgr_->get_role()) {
    // only leader need exec this func
  } else {
    RLockGuard guard(lock_);
    int tmp_ret = OB_SUCCESS;
    RegionArray region_list;
    common::ObRegion cur_region;
    const int64_t child_cnt = children_list_.get_member_number();
    for (int64_t i = 0; i < (child_cnt - 1) && OB_SUCC(ret); ++i) {
      ObCascadMember cur_member;
      cur_region = DEFAULT_REGION_NAME;
      if (OB_FAIL(children_list_.get_member_by_index(i, cur_member))) {
        CLOG_LOG(WARN, "get_server_by_index failed", K_(partition_key), K(ret), K(i), K_(children_list));
      } else if (!cur_member.is_valid()) {
        CLOG_LOG(WARN, "cur_member is invalid", K_(partition_key));
      } else if (OB_SUCCESS != (tmp_ret = get_server_region_(cur_member.get_server(), cur_region))) {
        CLOG_LOG(WARN, "get_server_region_ failed", K_(partition_key), K(tmp_ret), K(cur_member));
      } else if (dup_region_list.get_member_number() > 0 && is_region_recorded_(region_list, cur_region)) {
        // Only one dup child is collected in each round of the region.
      } else {
        bool is_dup_child = false;
        for (int64_t j = (i + 1); j < child_cnt && OB_SUCC(ret) && !is_dup_child; ++j) {
          ObCascadMember tmp_member;
          common::ObRegion tmp_region = DEFAULT_REGION_NAME;
          if (OB_FAIL(children_list_.get_member_by_index(j, tmp_member))) {
            CLOG_LOG(WARN, "get_server_by_index failed", K_(partition_key), K(ret), K(j), K_(children_list));
          } else if (!tmp_member.is_valid()) {
            CLOG_LOG(WARN, "tmp_member is invalid", K_(partition_key));
          } else if (OB_SUCCESS != (tmp_ret = get_server_region_(tmp_member.get_server(), tmp_region))) {
            CLOG_LOG(WARN, "get_server_region_ failed", K_(partition_key), K(tmp_ret), K(tmp_member));
          } else if (cur_region == tmp_region) {
            // find same region child
            is_dup_child = true;
          } else {
            // do nothing
          }
        }
        if (is_dup_child) {
          if (OB_FAIL(dup_region_list.add_member(cur_member))) {
            CLOG_LOG(WARN, "add_member failed", K_(partition_key), K(ret), K(cur_member));
          } else if (OB_FAIL(region_list.push_back(cur_region))) {
            CLOG_LOG(WARN, "push_back failed", K_(partition_key), K(ret), K(cur_region));
          } else {
            // do nothing
          }
        }
      }
    }
  }
  return ret;
}

/* check cascading state periodically */
int ObLogCascadingMgr::check_cascading_state()
{
  int ret = OB_SUCCESS;
  ObRegion self_region = DEFAULT_REGION_NAME;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_server_region_(self_, self_region))) {
    CLOG_LOG(WARN, "get_server_region_ failed", K_(partition_key), K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    const int64_t now = ObTimeUtility::current_time();
    // parent check child state periodically
    if (now - last_check_child_state_time_ > CLOG_CASCADING_CHECK_CHILD_INTERVAL) {
      if (GCTX.is_in_disabled_state()) {
        // slef is in disabled state, need clear all diff cluster children
        (void)reset_async_standby_children();
        (void)reset_sync_standby_child();
      }
      bool is_elect_leader = false;
      if (is_leader_by_election(state_mgr_->get_role())) {
        is_elect_leader = true;
      }
      ObCascadMemberList dead_child_list;                // children that in server blacklist
      ObCascadMemberList dead_async_standby_child_list;  // async standby children that in server blacklist
      ObCascadMemberList diff_region_child_list;         // children that need change parent
      do {
        // need hold rdlock
        RLockGuard guard(lock_);
        last_check_child_state_time_ = now;
        // clear non-paxos children in blacklist
        const int64_t child_cnt = children_list_.get_member_number();
        for (int64_t i = 0; i < child_cnt && OB_SUCC(ret); ++i) {
          ObCascadMember cur_member;
          common::ObRegion cur_region = DEFAULT_REGION_NAME;
          if (OB_SUCCESS != (tmp_ret = children_list_.get_member_by_index(i, cur_member))) {
            CLOG_LOG(WARN, "get_server_by_index failed", K_(partition_key), K(tmp_ret), K(i), K(children_list_));
          } else if (OB_UNLIKELY(share::ObServerBlacklist::get_instance().is_in_blacklist(cur_member))) {
            // remove non-paxos child in blacklist
            if (OB_SUCCESS != (tmp_ret = dead_child_list.add_member(cur_member))) {
              CLOG_LOG(WARN, "add_server failed", K_(partition_key), K(tmp_ret));
            }
          } else if (!is_elect_leader) {
            // follower need find diff region child
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = get_server_region_(cur_member.get_server(), cur_region))) {
              CLOG_LOG(WARN, "get_server_region_ failed", K_(partition_key), K(tmp_ret));
            } else if (self_region == cur_region) {
              // skip same region child
            } else if (OB_SUCCESS != (tmp_ret = diff_region_child_list.add_member(cur_member))) {
            } else {
              CLOG_LOG(INFO, "find diff region child", K_(partition_key), K(self_region), K(cur_region), K(cur_member));
            }
          } else {
          }
        }
        // clear dead async_standby_children_ according to server blacklist
        const int64_t standby_child_cnt = async_standby_children_.get_member_number();
        for (int64_t i = 0; i < standby_child_cnt && OB_SUCC(ret); ++i) {
          ObCascadMember cur_member;
          common::ObRegion cur_region = DEFAULT_REGION_NAME;
          if (OB_SUCCESS != (tmp_ret = async_standby_children_.get_member_by_index(i, cur_member))) {
            CLOG_LOG(
                WARN, "get_server_by_index failed", K_(partition_key), K(tmp_ret), K(i), K(async_standby_children_));
          } else if (OB_UNLIKELY(share::ObServerBlacklist::get_instance().is_in_blacklist(cur_member))) {
            // remove non-paxos child in blacklist
            if (OB_SUCCESS != (tmp_ret = dead_async_standby_child_list.add_member(cur_member))) {
              CLOG_LOG(WARN, "add_server failed", K_(partition_key), K(tmp_ret));
            }
          } else if (!is_elect_leader) {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = get_server_region_(cur_member.get_server(), cur_region))) {
              CLOG_LOG(WARN, "get_server_region_ failed", K_(partition_key), K(tmp_ret));
            } else if (self_region == cur_region) {
              // skip same region child
            } else if (OB_SUCCESS != (tmp_ret = diff_region_child_list.add_member(cur_member))) {
            } else {
              CLOG_LOG(INFO, "find diff region child", K_(partition_key), K(self_region), K(cur_region), K(cur_member));
            }
          } else {
          }
        }
      } while (0);
      // remove dead children
      int64_t dead_child_cnt = dead_child_list.get_member_number();
      for (int64_t i = 0; i < dead_child_cnt; ++i) {
        ObCascadMember cur_member;
        if (OB_SUCCESS != (tmp_ret = dead_child_list.get_member_by_index(i, cur_member))) {
          CLOG_LOG(WARN, "get_server_by_index failed", K_(partition_key), K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = try_remove_child(cur_member.get_server()))) {
          CLOG_LOG(WARN, "try_remove_child failed", K(tmp_ret), K_(partition_key), K(cur_member));
        } else {
          CLOG_LOG(INFO, "remove dead child success", K_(partition_key), K(cur_member));
        }
      }
      // remove dead async_standby_children_
      dead_child_cnt = dead_async_standby_child_list.get_member_number();
      for (int64_t i = 0; i < dead_child_cnt; ++i) {
        ObCascadMember cur_member;
        if (OB_SUCCESS != (tmp_ret = dead_async_standby_child_list.get_member_by_index(i, cur_member))) {
          CLOG_LOG(WARN, "get_server_by_index failed", K_(partition_key), K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = try_remove_child(cur_member.get_server()))) {
          CLOG_LOG(WARN, "try_remove_child failed", K(tmp_ret), K_(partition_key), K(cur_member));
        } else {
          CLOG_LOG(INFO, "remove dead async standby child because of blacklist", K_(partition_key), K(cur_member));
        }
      }
      // leader check whether sync_standby_child_ is in blacklist
      if (sync_standby_child_.is_valid() &&
          share::ObServerBlacklist::get_instance().is_in_blacklist(sync_standby_child_)) {
        if (OB_SUCCESS != (tmp_ret = try_remove_child(sync_standby_child_.get_server()))) {
          CLOG_LOG(WARN, "try_remove_child failed", K(tmp_ret), K_(partition_key));
        } else {
          CLOG_LOG(INFO, "remove sync_standby_child_ because of blacklist", K_(partition_key));
        }
      }
      // notify children to change parent
      if (!is_elect_leader) {
        const int64_t diff_region_cnt = diff_region_child_list.get_member_number();
        const ObCascadMember leader = state_mgr_->get_leader_member();
        if (!leader.is_valid()) {
          // leader is invalid, skip
        } else if (diff_region_cnt <= 0) {
          // no diff region child, skip
        } else {
          for (int64_t i = 0; i < diff_region_cnt; ++i) {
            ObCascadMember cur_member;
            if (OB_SUCCESS != (tmp_ret = diff_region_child_list.get_member_by_index(i, cur_member))) {
              CLOG_LOG(WARN, "get_member_by_index failed", K(tmp_ret), K_(partition_key), K(i));
            } else if (OB_SUCCESS !=
                       (tmp_ret = log_engine_->notify_reregister(
                            cur_member.get_server(), cur_member.get_cluster_id(), partition_key_, leader))) {
              CLOG_LOG(WARN, "notify_reregister failed", K(tmp_ret), K_(partition_key), K(cur_member), K(leader));
            } else {
              // do nothing
            }
          }
          CLOG_LOG(INFO,
              "follower finish notify diff region child reregister",
              K_(partition_key),
              K(leader),
              K(diff_region_child_list));
        }
      } else if (is_elect_leader) {
        // leader need clear dup region children
        ObCascadMemberList dup_region_list;
        if (OB_SUCCESS != (tmp_ret = leader_get_dup_region_child_list_(dup_region_list))) {
          CLOG_LOG(WARN, "leader_get_dup_region_child_list_ failed", K(tmp_ret), K_(partition_key));
        } else if (0 == dup_region_list.get_member_number()) {
          // no such child, skip
        } else {
          const int64_t self_cluster_id = state_mgr_->get_self_cluster_id();
          ObCascadMember leader(self_, self_cluster_id);
          const int64_t dup_region_cnt = dup_region_list.get_member_number();
          for (int64_t i = 0; i < dup_region_cnt; ++i) {
            ObCascadMember cur_member;
            if (OB_SUCCESS != (tmp_ret = dup_region_list.get_member_by_index(i, cur_member))) {
              CLOG_LOG(WARN, "get_member_by_index failed", K(tmp_ret), K_(partition_key), K(i));
            } else if (OB_SUCCESS !=
                       (tmp_ret = log_engine_->notify_reregister(
                            cur_member.get_server(), cur_member.get_cluster_id(), partition_key_, leader))) {
              CLOG_LOG(WARN, "notify_reregister failed", K(tmp_ret), K_(partition_key), K(cur_member));
            } else {
              // do nothing
            }
          }
          CLOG_LOG(INFO,
              "leader finish notify dup region child reregister",
              K_(partition_key),
              K(self_),
              K(leader),
              K(dup_region_list));
        }
      }
    }
  }

  return ret;
}

/* check child legal when handle_fetch_log */
int ObLogCascadingMgr::check_child_legal(
    const ObAddr& server, const int64_t cluster_id, const common::ObReplicaType replica_type) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    const bool is_in_children_list = is_valid_child(server);
    const bool is_in_member_list = mm_->get_curr_member_list().contains(server);
    const int64_t self_cluster_id = state_mgr_->get_self_cluster_id();
    if (!is_in_children_list) {
      bool need_reject = false;
      if (LEADER == state_mgr_->get_role() || STANDBY_LEADER == state_mgr_->get_role()) {
        // The leader allows paxos replicas of the same cluster to fetch logs
        if (is_in_member_list) {
        } else if (cluster_id == self_cluster_id && ObReplicaTypeCheck::is_paxos_replica_V2(replica_type)) {
        } else {
          // other cases, need reject
          need_reject = true;
        }
      } else {
        // follower rejects non-child server
        need_reject = true;
      }
      if (need_reject) {
        ObReplicaMsgType msg_type = OB_REPLICA_MSG_TYPE_NOT_PARENT;
        if (OB_FAIL(reject_server(server, cluster_id, msg_type))) {
          CLOG_LOG(WARN, "reject_server failed", K(ret), K_(partition_key), K(server), K(msg_type));
        }
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "server is not my child, reject it", K(server), K_(partition_key));
        }
      }
    }
  }
  return ret;
}

/* request next candidate to assign parent */
int ObLogCascadingMgr::try_request_next_candidate_(const common::ObRegion& self_region)
{
  // caller need hold lock
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (candidate_server_list_.get_member_number() <= 0) {
    // candidate_list is already empty
    if (last_request_candidate_.is_valid()) {
      // after request all candidates, but still failed, it need request leader to force becaming its child.
      ObCascadMember cascad_leader;
      (void)state_mgr_->get_cascad_leader(cascad_leader);

      if (cascad_leader.is_valid()) {
        if (OB_FAIL(fetch_register_server(cascad_leader.get_server(),
                cascad_leader.get_cluster_id(),
                self_region,
                state_mgr_->get_idc(),
                true,
                true))) {
          CLOG_LOG(WARN, "fetch_register_server failed", K_(partition_key), K(ret), K(cascad_leader));
        }
      }
      reset_last_request_state_unlock_();
    } else {
      ret = OB_EAGAIN;
    }
  } else {
    // random choose one candidate
    ObCascadMember candidate_member;
    int64_t candidate_num = candidate_server_list_.get_member_number();
    if (candidate_num == 1) {
      if (OB_FAIL(candidate_server_list_.get_member_by_index(0, candidate_member))) {
        CLOG_LOG(WARN, "candidate_server_list_ get_server_by_index failed", K_(partition_key), K(ret));
      }
    } else if (candidate_num > 1) {
      const common::ObAddr old_parent = parent_.get_server();
      if (old_parent.is_valid() && candidate_server_list_.contains(old_parent)) {
        if (OB_FAIL(candidate_server_list_.remove_server(old_parent))) {
          CLOG_LOG(WARN, "candidate_server_list_ remove_server failed", K_(partition_key), K(ret), K(old_parent));
        }
      }
      candidate_num = candidate_server_list_.get_member_number();
      int64_t random_idx = ObRandom::rand(0, candidate_num - 1);
      if (OB_FAIL(candidate_server_list_.get_member_by_index(random_idx, candidate_member))) {
        CLOG_LOG(WARN, "candidate_server_list_ get_server_by_index failed", K_(partition_key), K(ret), K(random_idx));
      }
    } else {
      // candidate_num < 1, impossible
      ret = OB_EAGAIN;
    }
    if (!candidate_member.is_valid()) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "candidate_member is invalid", K_(partition_key), K(ret), K(candidate_member));
    } else if (OB_FAIL(candidate_server_list_.remove_server(candidate_member.get_server()))) {
      CLOG_LOG(WARN, "candidate_server_list_ remove_server failed", K_(partition_key), K(ret), K(candidate_member));
    } else if (state_mgr_->is_offline()) {
      CLOG_LOG(WARN, "self is offline, abort fetch parent", K_(partition_key), "is_offline", state_mgr_->is_offline());
    } else {
      common::ObAddr candidate_server = candidate_member.get_server();
      const int64_t dst_cluster_id = candidate_member.get_cluster_id();
      if (is_valid_child_unlock_(candidate_server)) {
        // Be careful not to form a ring: a->b->...->a.
        // So need check whether candidate is my child every time.
        if (OB_FAIL(try_remove_child_unlock_(candidate_server))) {
          CLOG_LOG(WARN, "try_remove_child_unlock_ failed", K_(partition_key), K(ret), "server", candidate_server);
        }
      }
      if (OB_FAIL(fetch_register_server(
              candidate_server, dst_cluster_id, self_region, state_mgr_->get_idc(), false, false))) {
        CLOG_LOG(WARN, "fetch_register_server failed", K_(partition_key), K(ret));
      } else {
        last_request_candidate_ = candidate_server;
      }
    }
  }
  return ret;
}

int ObLogCascadingMgr::fetch_register_server(const common::ObAddr& dst_server, const int64_t dst_cluster_id,
    const common::ObRegion& region, const common::ObIDC& idc, const bool is_request_leader,
    const bool is_need_force_register) const
{
  int ret = OB_SUCCESS;
  int64_t next_replay_log_ts = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!dst_server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!ObMultiClusterUtil::is_cluster_private_table(partition_key_.get_table_id()) &&
             GCTX.is_in_disabled_state() && dst_cluster_id != state_mgr_->get_self_cluster_id()) {
    ret = OB_STATE_NOT_MATCH;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN,
          "non-private table in disabled state, cannot fetch_regitster_server from diff cluster server",
          K(ret),
          K_(partition_key));
    }
  } else if (OB_FAIL(sw_->get_next_replay_log_timestamp(next_replay_log_ts))) {
    CLOG_LOG(WARN, "sw get_next_replay_log_timestamp failed", K(ret), K_(partition_key));
  } else if (OB_FAIL(log_engine_->fetch_register_server(dst_server,
                 dst_cluster_id,
                 partition_key_,
                 region,
                 idc,
                 mm_->get_replica_type(),
                 next_replay_log_ts,
                 is_request_leader,
                 is_need_force_register))) {
    CLOG_LOG(WARN, "fetch_register_server failed", K(ret), K_(partition_key), K(dst_server));
  } else {
    CLOG_LOG(DEBUG,
        "fetch_register_server success",
        K(ret),
        K_(partition_key),
        K(dst_server),
        K(is_request_leader),
        K(is_need_force_register));
  }
  return ret;
}

int ObLogCascadingMgr::reset_last_request_state()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    WLockGuard guard(lock_);
    reset_last_request_state_unlock_();
  }
  return ret;
}

void ObLogCascadingMgr::reset_last_request_state_unlock_()
{
  last_request_candidate_.reset();
  candidate_server_list_.reset();
}

int ObLogCascadingMgr::check_parent_state(const int64_t now, const common::ObRegion& self_region)
{
  // caller guarantees self is in active, online state
  int ret = OB_SUCCESS;
  bool is_need_update_parent = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (state_mgr_->is_offline()) {
    // self is offline, skip
  } else {
    ObCascadMember cascad_leader;
    (void)state_mgr_->get_cascad_leader(cascad_leader);
    do {
      RLockGuard guard(lock_);
      if (!parent_.is_valid()) {
        // parent is invalid, check if need print error log
        if (parent_invalid_start_ts_ != OB_INVALID_TIMESTAMP &&
            now - parent_invalid_start_ts_ > CLOG_PARENT_INVALID_ALERT_TIME) {
          if (partition_reach_time_interval(5 * 60 * 1000 * 1000, parent_invalid_warn_time_)) {
            int tmp_ret = OB_SUCCESS;
            bool is_exist = true;
            if (OB_SUCCESS != ps_cb_->check_partition_exist(partition_key_, is_exist)) {
              CLOG_LOG(WARN, "check_partition_exist failed", K_(partition_key), K(tmp_ret));
            }
            if (is_exist && cascad_leader.is_valid()) {
              // partition exists and leader is valid, if parent is invalid too long, print error
              CLOG_LOG(ERROR,
                  "parent invalid last too long, network maybe broken",
                  K_(partition_key),
                  K(last_request_candidate_),
                  K(parent_invalid_start_ts_),
                  K(last_check_parent_time_),
                  K(cascad_leader));
            } else {
              CLOG_LOG(WARN,
                  "parent invalid last too long, leader is invalid",
                  K_(partition_key),
                  K(is_exist),
                  K(last_request_candidate_),
                  K(parent_invalid_start_ts_),
                  K(last_check_parent_time_));
            }
          }
        }
        if ((now - last_check_parent_time_ > CLOG_CASCADING_PARENT_INVALID_DETECT_THRESHOLD)) {
          const ObCascadMember leader_member = state_mgr_->get_leader_member();
          if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) && FOLLOWER == state_mgr_->get_role()) {
            // paxos follower treats leader as parent
            if (leader_member.is_valid() && self_ != leader_member.get_server()) {
              if (OB_FAIL(set_parent_(leader_member.get_server(), leader_member.get_cluster_id()))) {
                CLOG_LOG(WARN, "set_parent_ failed", K_(partition_key), K(ret), K(leader_member));
              } else {
                last_check_parent_time_ = now;
              }
            }
          } else {
            // non-paxos OR non-FOLLOWER (STANDBY_LEADER/RESTORE_LEADER) replica
            // parent is invalid
            if (STANDBY_LEADER == state_mgr_->get_role() || GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_3100) {
              // only standby_leader need renew location cache to get primary_leader
              // follower replicas (including readonly replicas) rely on gc thread to trigger renew
              (void)state_mgr_->try_update_leader_from_loc_cache();
            }
            (void)state_mgr_->get_cascad_leader(cascad_leader);
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = try_request_next_candidate_(self_region))) {
              // request next candidate failed, need request leader
              if (cascad_leader.is_valid()) {
                if (OB_FAIL(fetch_register_server(cascad_leader.get_server(),
                        cascad_leader.get_cluster_id(),
                        self_region,
                        state_mgr_->get_idc(),
                        true,
                        false))) {
                  CLOG_LOG(WARN, "fetch_register_server failed", K(ret), K(partition_key_));
                } else {
                  // update check time
                  last_check_parent_time_ = now;
                  reset_last_request_state_unlock_();
                  CLOG_LOG(
                      INFO, "parent is invalid, begin new register round", K(ret), K(partition_key_), K(cascad_leader));
                }
              }
            } else {
              // request next cadidate succeed, update check time
              last_check_parent_time_ = now;
            }
          }
        }
      } else {
        if (now - last_check_parent_time_ < CLOG_CHECK_PARENT_INTERVAL) {
          // not reach interval, skip
        } else {
          // parent is valid, check next_log_ts
          const uint64_t next_ilog_id = sw_->get_next_index_log_id();
          const uint64_t sw_start_id = sw_->get_start_id();
          int64_t next_replay_log_ts = OB_INVALID_TIMESTAMP;
          uint64_t next_replay_log_id = OB_INVALID_ID;
          sw_->get_next_replay_log_id_info(next_replay_log_id, next_replay_log_ts);
          if (now - next_replay_log_ts < MAX_SYNC_FALLBACK_INTERVAL) {
            // self sync
            last_check_log_ts_ = next_replay_log_ts;
            last_check_parent_time_ = now;
          } else if (next_ilog_id > sw_start_id) {
            // there is log in sw, maybe relay is blocked for some reason, no need change parent
            // attention: do not use next_replay_log_id to replace sw_start_id, because next_replay_log_id may fallback
            // in restore_replayed_log situation, but sw_start_id won't.
            if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
              CLOG_LOG(WARN,
                  "sw has confirmed log, no need to change parent",
                  K(next_ilog_id),
                  K(sw_start_id),
                  K_(partition_key));
            }
            last_check_log_ts_ = next_replay_log_ts;
            last_check_parent_time_ = now;
          } else if (state_mgr_->is_need_rebuild()) {
            if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
              CLOG_LOG(INFO, "wait rebuild", K_(partition_key));
            }
            last_check_log_ts_ = next_replay_log_ts;
            last_check_parent_time_ = now;
          } else if (last_check_log_ts_ == next_replay_log_ts && need_update_parent_()) {
            // self is unsync && reach time interval, need update parent
            if (partition_reach_time_interval(5 * 60 * 1000 * 1000, check_parent_invalid_warn_time_)) {
              CLOG_LOG(INFO, "detect invalid parent", K_(partition_key), K(parent_));
            }

            if (ObReplicaTypeCheck::is_paxos_replica_V2(mm_->get_replica_type()) &&
                FOLLOWER == state_mgr_->get_role()) {
              // paxos follower
              const ObCascadMember leader_member = state_mgr_->get_leader_member();
              if (leader_member.is_valid()) {
                if (leader_member.get_server() == parent_.get_server()) {
                  if (REACH_TIME_INTERVAL(100 * 1000)) {
                    CLOG_LOG(WARN,
                        "paxos follower's parent is current leader, but next_log_ts is delayed",
                        K_(partition_key),
                        K(parent_));
                  }
                } else {
                  if (OB_FAIL(set_parent_(leader_member.get_server(), leader_member.get_cluster_id()))) {
                    CLOG_LOG(WARN, "set_parent_ failed", K_(partition_key), K(ret), K(leader_member));
                  }
                }
              }
            } else {
              // non-paxos follower, need change parent
              is_need_update_parent = true;
            }
            last_check_parent_time_ = now;
            if (STANDBY_LEADER == state_mgr_->get_role() || GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_3100) {
              // standby_leader need renew location cache to get primary_leader
              // follower replicas (including readonly replicas) rely on gc thread to trigger renew
              (void)state_mgr_->try_update_leader_from_loc_cache();
            }
          } else if (last_check_log_ts_ != next_replay_log_ts) {
            // next_replay_log_ts is changed, update
            last_check_log_ts_ = next_replay_log_ts;
            last_check_parent_time_ = now;
          } else {
            // do nothing
          }
        }
      }
    } while (0);

    if (is_need_update_parent) {
      WLockGuard guard(lock_);
      const uint32_t reset_type = ResetPType::CHECK_STATE;
      reset_parent_(reset_type);
      if (cascad_leader.is_valid() && OB_FAIL(fetch_register_server(cascad_leader.get_server(),
                                          cascad_leader.get_cluster_id(),
                                          self_region,
                                          state_mgr_->get_idc(),
                                          true,
                                          false))) {
        CLOG_LOG(WARN, "fetch_register_server failed", K(ret), K(partition_key_));
      } else {
        reset_last_request_state_unlock_();
      }
    }
  }
  return ret;
}

/* assign candidate_list for requester */
int ObLogCascadingMgr::get_parent_candidate_list_(const common::ObAddr& server,
    const common::ObReplicaType replica_type, const ObRegion& region, const int64_t cluster_id,
    const common::ObRegion& self_region, ObCascadMemberList& candidate_member_list, bool& is_become_my_child)
{
  // caller need hold lock
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  candidate_member_list.reset();
  is_become_my_child = false;
  int64_t self_cluster_id = OB_INVALID_CLUSTER_ID;
  const ObCascadMember member(server, cluster_id);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || self_region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    self_cluster_id = state_mgr_->get_self_cluster_id();
    const bool is_from_same_cluster = (self_cluster_id == cluster_id);
    // find same region candidates
    ObCascadMemberList same_region_candidate_list;
    common::ObAddr cur_server;
    common::ObRegion cur_region;
    ObCascadMember cur_member;
    if (LEADER == state_mgr_->get_role() || STANDBY_LEADER == state_mgr_->get_role()) {
      // self is leader, find same region candidates in member_list firstly
      const common::ObMemberList curr_member_list = mm_->get_curr_member_list();
      const int64_t member_cnt = curr_member_list.get_member_number();
      for (int64_t i = 0; i < member_cnt && OB_SUCC(ret); ++i) {
        cur_member.reset();
        cur_server.reset();
        cur_region.reset();
        (void)cur_member.set_cluster_id(self_cluster_id);  // server in member_list has same cluster_id with self
        if (OB_FAIL(curr_member_list.get_server_by_index(i, cur_server))) {
          CLOG_LOG(WARN, "get server failed", K_(partition_key), K(ret), K(i), K(curr_member_list));
        } else if (cur_server == server) {
          // skip arg server
        } else if (cur_server == self_) {
          // skip self
        } else if (OB_SUCCESS != (tmp_ret = cur_member.set_server(cur_server))) {
          CLOG_LOG(WARN, "set_server failed", K(tmp_ret), K_(partition_key), K(cur_server));
        } else if (share::ObServerBlacklist::get_instance().is_in_blacklist(cur_member)) {
          // server in blacklist cannot be candidate
        } else if (OB_SUCCESS != (tmp_ret = get_server_region_(cur_server, cur_region))) {
          CLOG_LOG(WARN, "get_server_region_ failed", K(tmp_ret), K_(partition_key), K(cur_server));
        } else if (region != cur_region) {
          // skip diff region server
        } else {
          (void)same_region_candidate_list.add_member(cur_member);
        }
      }
    }
    // find same region candidates in children list
    const int64_t child_cnt = children_list_.get_member_number();
    for (int64_t i = 0; i < child_cnt && OB_SUCC(ret); ++i) {
      cur_member.reset();
      cur_server.reset();
      cur_region.reset();
      if (OB_FAIL(children_list_.get_member_by_index(i, cur_member))) {
        CLOG_LOG(WARN, "children_list_ get member failed", K_(partition_key), K(ret), K(i), K_(children_list));
      } else {
        cur_server = cur_member.get_server();
        if (cur_member.get_server() == server) {
          // skip arg server
        } else if (share::ObServerBlacklist::get_instance().is_in_blacklist(cur_member)) {
          // server in blacklist cannot be candidate
        } else if (OB_SUCCESS != (tmp_ret = get_server_region_(cur_server, cur_region))) {
          CLOG_LOG(WARN, "get_server_region_ failed", K(tmp_ret), K_(partition_key), K(cur_server));
        } else if (region != cur_region) {
          // skip diff region server
        } else {
          (void)same_region_candidate_list.add_member(cur_member);
        }
      }
    }
    // find same region candidates in async_children
    // attention: only other cluster's requesters will enter this logic
    const int64_t async_child_cnt = async_standby_children_.get_member_number();
    for (int64_t i = 0; !is_from_same_cluster && i < async_child_cnt && OB_SUCC(ret); ++i) {
      cur_member.reset();
      cur_server.reset();
      cur_region.reset();
      if (same_region_candidate_list.get_member_number() >= OB_MAX_CHILD_MEMBER_NUMBER) {
        // already full, terminate
        break;
      } else if (OB_FAIL(async_standby_children_.get_member_by_index(i, cur_member))) {
        CLOG_LOG(WARN,
            "async_standby_children_ get member failed",
            K_(partition_key),
            K(ret),
            K(i),
            K_(async_standby_children));
      } else {
        cur_server = cur_member.get_server();
        if (cur_member.get_server() == server) {
          // skip arg server
        } else if (share::ObServerBlacklist::get_instance().is_in_blacklist(cur_member)) {
          // server in blacklist cannot be candidate
        } else if (OB_SUCCESS != (tmp_ret = get_server_region_(cur_server, cur_region))) {
          CLOG_LOG(WARN, "get_server_region_ failed", K(tmp_ret), K_(partition_key), K(cur_server));
        } else if (region != cur_region) {
          // skip diff region server
        } else {
          (void)same_region_candidate_list.add_member(cur_member);
        }
      }
    }

    if (same_region_candidate_list.get_member_number() > 0) {
      // return same region candidate
      if (OB_FAIL(candidate_member_list.deep_copy(same_region_candidate_list))) {
        CLOG_LOG(WARN,
            "candidate_member_list.deep_copy failed",
            K(ret),
            K_(partition_key),
            K(same_region_candidate_list),
            K(candidate_member_list));
      }
    } else {
      // do nothing
    }

    if (OB_SUCC(ret)) {
      if (candidate_member_list.get_member_number() <= 0) {
        // candidate list is empty
        // 1. if there is space in children list, try insert, but need check:
        // - self is leader, requester can be my child to avoid assign failure
        // - self is follower, we must guarantee requester's region is same with self
        if (LEADER == state_mgr_->get_role()
            // standby_leader only process same cluster requesters
            || (STANDBY_LEADER == state_mgr_->get_role() && is_from_same_cluster) || region == self_region) {
          if (OB_SUCC(try_add_child_unlock_(member))) {
            is_become_my_child = true;
          }
        }
        // 2. children list is full, leader will force add requester to children
        if (is_become_my_child) {
          // do nothing
        } else if (LEADER == state_mgr_->get_role() ||
                   (STANDBY_LEADER == state_mgr_->get_role() && is_from_same_cluster)) {
          // standby_leader only process same cluster requesters
          if (OB_SUCC(leader_force_add_child_unlock_(member))) {
            is_become_my_child = true;
          }
        } else {
          ret = OB_EAGAIN;
        }
      }
    }
    if (partition_reach_time_interval(5 * 60 * 1000 * 1000, get_parent_candidate_list_log_time_)) {
      CLOG_LOG(INFO,
          "get_parent_candidate_list_ finished",
          K_(partition_key),
          K(ret),
          K(server),
          K(region),
          K(replica_type),
          K(is_become_my_child),
          "role",
          state_mgr_->get_role(),
          K(self_region),
          K(candidate_member_list));
    }
  }
  return ret;
}

void ObLogCascadingMgr::reset_children_list()
{
  WLockGuard guard(lock_);
  children_list_.reset();
  async_standby_children_.reset();
  if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
    CLOG_LOG(INFO, "reset_children_list finished", K_(partition_key));
  }
}

void ObLogCascadingMgr::reset_async_standby_children()
{
  WLockGuard guard(lock_);
  async_standby_children_.reset();
  if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
    CLOG_LOG(INFO, "reset_async_standby_children finished", K_(partition_key));
  }
}

void ObLogCascadingMgr::reset_sync_standby_child()
{
  WLockGuard guard(lock_);
  sync_standby_child_.reset();
  if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
    CLOG_LOG(INFO, "reset_sync_standby_child finished", K_(partition_key));
  }
}

int ObLogCascadingMgr::leader_force_add_child_unlock_(const ObCascadMember& member)
{
  int ret = OB_SUCCESS;

  if (!member.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(member));
  } else if (member.get_server() == self_) {
    // ignore self
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "arg member is self, unexpect case", K_(partition_key), K(ret), K(member));
  } else if (is_valid_child_unlock_(member.get_server())) {
    // already exist, do nothing
  } else if (LEADER != state_mgr_->get_role() && STANDBY_LEADER != state_mgr_->get_role()) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(WARN, "self is not leader", K_(partition_key), K(ret), K(member));
  } else {
    if (OB_FAIL(children_list_.add_member(member))) {
      CLOG_LOG(WARN, "children_list_ add member failed", K_(partition_key), K(ret), K(member), K(children_list_));
    } else {
      CLOG_LOG(INFO, "leader_force_add_child_unlock_ success", K_(partition_key), K(ret), K(member), K_(children_list));
    }
  }
  return ret;
}

int ObLogCascadingMgr::try_add_child_unlock_(const ObCascadMember& member)
{
  int ret = OB_SUCCESS;
  int64_t self_cluster_id = state_mgr_->get_self_cluster_id();
  if (!member.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(member));
  } else if (member.get_server() == self_ || member.get_server() == parent_.get_server()) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN,
        "arg member is self or my parent, unexpected",
        K_(partition_key),
        K(ret),
        K(member),
        K_(parent),
        K_(children_list));
  } else if (self_cluster_id == member.get_cluster_id()) {
    // same cluster member
    if (children_list_.contains(member)) {
      // already exist, do nothing
    } else if (is_children_list_full_unlock_()) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "children_list_ already full", K_(partition_key), K(ret), K(member), K(children_list_));
    } else if (OB_FAIL(children_list_.add_member(member))) {
      CLOG_LOG(WARN, "children_list_ add member failed", K_(partition_key), K(ret), K(member), K(children_list_));
    } else {
      CLOG_LOG(INFO, "add child success", K_(partition_key), K(ret), K(member), K_(children_list), K_(parent));
    }
  } else {
    // diff cluster member
    if (async_standby_children_.contains(member)) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(INFO,
            "already in async_standby_children",
            K_(partition_key),
            K(ret),
            K(member),
            K_(async_standby_children));
      }
    } else if (OB_FAIL(remove_dup_cluster_standby_child_(member))) {
      CLOG_LOG(WARN,
          "remove_dup_cluster_standby_child_ failed",
          K_(partition_key),
          K(ret),
          K(member),
          K(async_standby_children_));
    } else if (is_async_standby_children_full_unlock_()) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN,
          "async_standby_children_ already full",
          K_(partition_key),
          K(ret),
          K(member),
          K(async_standby_children_));
    } else if (OB_FAIL(async_standby_children_.add_member(member))) {
      CLOG_LOG(WARN,
          "async_standby_children_ add member failed",
          K_(partition_key),
          K(ret),
          K(member),
          K(async_standby_children_));
    } else {
      CLOG_LOG(INFO,
          "add standby child success",
          K_(partition_key),
          K(ret),
          K(member),
          K_(async_standby_children),
          K_(parent));
    }
  }
  return ret;
}

int ObLogCascadingMgr::remove_dup_cluster_standby_child_(const ObCascadMember& member)
{
  int ret = OB_SUCCESS;
  if (!member.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(member));
  } else {
    int64_t arg_cluster_id = member.get_cluster_id();
    ObCascadMember dst_member;
    bool dst_found = false;
    for (int64_t i = 0; i < async_standby_children_.get_member_number() && !dst_found; ++i) {
      ObCascadMember cur_member;
      if (OB_FAIL(async_standby_children_.get_member_by_index(i, cur_member))) {
        LOG_WARN("fail to get member by index", K(ret), K(i));
      } else if (arg_cluster_id != cur_member.get_cluster_id()) {
        // skip diff cluster child
      } else {
        dst_member = cur_member;
        dst_found = true;
      }
    }
    if (dst_found) {
      if (OB_FAIL(async_standby_children_.remove_member(dst_member))) {
        CLOG_LOG(WARN, "async_standby_children_ remove member failed", K_(partition_key), K(ret), K(dst_member));
      } else {
        CLOG_LOG(INFO,
            "async_standby_children_ remove dup cluster member success",
            K_(partition_key),
            K(ret),
            K(dst_member),
            K(member),
            K_(async_standby_children));
      }
    }
    if (sync_standby_child_.get_cluster_id() == member.get_cluster_id()) {
      CLOG_LOG(INFO,
          "reset sync_standby_child_ because of dup cluster member",
          K_(partition_key),
          K(member),
          K_(sync_standby_child));
      sync_standby_child_.reset();
    }
  }
  return ret;
}

int ObLogCascadingMgr::try_remove_child(const common::ObAddr& server)
{
  WLockGuard guard(lock_);
  return try_remove_child_unlock_(server);
}

int ObLogCascadingMgr::try_remove_child_unlock_(const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(server));
  } else if (children_list_.contains(server)) {
    if (OB_FAIL(children_list_.remove_server(server))) {
      CLOG_LOG(WARN, "children_list_ remove server failed", K_(partition_key), K(ret), K(server));
    }
  } else if (async_standby_children_.contains(server)) {
    if (OB_FAIL(async_standby_children_.remove_server(server))) {
      CLOG_LOG(WARN, "async_standby_children_ remove server failed", K_(partition_key), K(ret), K(server));
    }
  } else if (server == sync_standby_child_.get_server()) {
    sync_standby_child_.reset();
  } else {
  }
  if (REACH_TIME_INTERVAL(100 * 1000)) {
    CLOG_LOG(INFO,
        "after try_remove_child_unlock_",
        K(ret),
        K_(partition_key),
        K(server),
        K_(children_list),
        K_(async_standby_children),
        K_(sync_standby_child));
  }
  return ret;
}

bool ObLogCascadingMgr::is_async_standby_children_full_unlock_()
{
  // the number of async standby children is 15 at most
  return (async_standby_children_.get_member_number() >= common::OB_MAX_CHILD_MEMBER_NUMBER);
}

bool ObLogCascadingMgr::is_children_list_full_unlock_()
{
  // the number of children is 5 at most
  return (children_list_.get_member_number() >= MAX_CHILD_NUM);
}

int ObLogCascadingMgr::get_server_region_(const common::ObAddr& server, common::ObRegion& region) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K_(partition_key), K(ret), K(server));
  } else if (OB_FAIL(ps_cb_->get_server_region(server, region))) {
    region = DEFAULT_REGION_NAME;
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      CLOG_LOG(WARN, "get_server_region_ failed", K_(partition_key), K(ret), K(server));
    }
  } else {
    // do nothing
  }
  return ret;
}

/* interface for location_cache to get children replicas in local cluster */
int ObLogCascadingMgr::get_lower_level_replica_list(ObChildReplicaList& list) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    RLockGuard guard(lock_);
    const int64_t self_cluster_id = state_mgr_->get_self_cluster_id();
    ObReplicaType type = REPLICA_TYPE_MAX;
    int64_t child_cnt = children_list_.get_member_number();
    for (int64_t i = 0; i < child_cnt && OB_SUCC(ret); i++) {
      ObCascadMember cur_member;
      if (OB_FAIL(children_list_.get_member_by_index(i, cur_member))) {
        LOG_WARN("fail to get server by index", K(ret), K(i));
      } else if (self_cluster_id != cur_member.get_cluster_id()) {
        // skip diff cluster child
      } else {
        ObReplicaMember replica(cur_member.get_server(), 0, type);
        if (OB_FAIL(list.push_back(replica))) {
          LOG_WARN("fail to push back replica", K(ret), K(replica));
        }
      }
    }
    if (LEADER == state_mgr_->get_role()) {
      ObAddr server;
      const common::ObMemberList curr_member_list = mm_->get_curr_member_list();
      const int64_t member_cnt = curr_member_list.get_member_number();
      for (int64_t i = 0; i < member_cnt && OB_SUCC(ret); i++) {
        server.reset();
        if (OB_FAIL(curr_member_list.get_server_by_index(i, server))) {
          LOG_WARN("fail to get server by index", K(i));
        } else if (server == self_) {
          // skip self
        } else {
          ObReplicaMember replica(server, 0, type);
          if (OB_FAIL(list.push_back(replica))) {
            LOG_WARN("fail to push back replica", K(ret), K(replica));
          }
        }
      }
    }
  }
  return ret;
}

bool ObLogCascadingMgr::need_update_parent_() const
{
  bool bool_ret = false;

  const int64_t now = ObClockGenerator::getClock();
  if (now - last_check_parent_time_ >=
      std::max(2 * CLOG_CHECK_PARENT_INTERVAL, state_mgr_->get_fetch_log_interval() + CLOG_CHECK_PARENT_INTERVAL)) {
    bool_ret = true;
  } else if (share::ObServerBlacklist::get_instance().is_in_blacklist(parent_)) {
    bool_ret = true;
  }

  return bool_ret;
}
}  // namespace clog
}  // namespace oceanbase
