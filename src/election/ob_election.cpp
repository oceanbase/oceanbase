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

#include "ob_election.h"
#include "common/ob_clock_generator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/oblog/ob_trace_log.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "share/ob_cluster_version.h"
#include "share/ob_multi_cluster_util.h"
#include "share/ob_server_blacklist.h"
#include "storage/ob_partition_service.h"
#include "ob_election_mgr.h"
#include "ob_election_group.h"
#include "ob_election_group_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_service.h"

namespace oceanbase {
using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;

namespace election {
/**********should removed after a barrier version bigger than 3.1**********/
bool ObElection::IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1 = true;
/**************************************************************************************************/
const char* const ObElection::REVOKE_REASON_STR[REVOKE_TYPE_MAX] = {"leader lease expired",
    "self is not candidate",
    "disk error",
    "clog reconfirm timeout",
    "clog sliding_window timeout",
    "clog role change timeout",
    "replica_type disallowed",
    "member_list disallowed",
    "partiton_service internal_leader_active failed",
    "cluster role switch revoke all",
    "transaction log cb error",
    "standby replica restore failed",
    "memory alloc failed when submit_log",
    "clog disk full",
    "clog disk hang"};

void ObElection::reset()
{
  is_inited_ = false;
  is_need_query_ = false;
  is_reappoint_period_ = false;
  is_force_leader_ = false;
  is_in_eg_ = false;
  unconfirmed_leader_.reset();
  unconfirmed_leader_lease_.first = 0;
  unconfirmed_leader_lease_.second = 0;
  leader_revoke_timestamp_ = 0;
  candidate_index_ = 0;
  msg_pool_.reset();
  rpc_ = NULL;
  timer_.reset();
  election_cb_ = NULL;
  eg_mgr_ = NULL;
  election_group_ = NULL;
  eg_part_array_idx_ = -1;
  valid_candidates_.reset();
  tmp_valid_candidates_.reset();
  local_priority_.reset();
  ObElectionInfo::reset();
  max_leader_epoch_ever_seen_ = OB_INVALID_TIMESTAMP;
  reappoint_count_ = 0;
  need_print_trace_log_ = false;
  ignore_log_ = false;
  // lease setted to 9.0s after version 1.3.0
  // no need to consider compatibility on version 2.0
  vote_period_ = T_CENTRALIZED_VOTE_EXTENDS_V2;
  lease_time_ = OB_ELECTION_130_LEASE_TIME;
  last_gts_ = 0;
  move_out_timestamp_ = 0;
  cached_lease_end_ = 0;
}

int ObElection::init(const ObPartitionKey& partition, const ObAddr& self, ObIElectionRpc* rpc, ObTimeWheel* tw,
    const int64_t replica_num, ObIElectionCallback* election_cb, ObIElectionGroupMgr* eg_mgr,
    ObElectionEventHistoryArray* event_history_array)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    FORCE_ELECT_LOG(WARN, "ObElection inited twice", K(ret));
  } else if (!partition.is_valid() || !self.is_valid() || NULL == rpc || NULL == tw || replica_num <= 0 ||
             OB_MAX_MEMBER_NUMBER < replica_num || NULL == election_cb || NULL == eg_mgr) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN,
        "invalid argument",
        K(ret),
        K(partition),
        K(self),
        KP(rpc),
        KP(tw),
        K(replica_num),
        KP(election_cb),
        KP(eg_mgr));
  } else {
    // init ObElection member
    partition_ = partition;
    self_ = self;
    msg_pool_.init(self_, this);
    replica_num_ = replica_num;
    const int64_t cur_ts = get_current_ts();
    active_timestamp_ = cur_ts + ACTIVE_TIME_DELTA;
    leader_revoke_timestamp_ = cur_ts;
    rpc_ = rpc;
    election_cb_ = election_cb;
    eg_mgr_ = eg_mgr;
    event_history_array_ = event_history_array;
    if (OB_FAIL(timer_.init(this, tw, rpc))) {
      FORCE_ELECT_LOG(WARN, "election timer inited error", K(ret), K(partition));
    } else {
      is_inited_ = true;
      ELECT_ASYNC_LOG(INFO, "ObElection inited success", K(partition), K(self));
    }
    /**********should removed after a barrier version bigger than 3.1**********/
    // if (OB_SYS_TENANT_ID == extract_tenant_id(partition_.get_table_id())) {
    //   election_time_offset_ = (partition_.hash() % OB_ELECTION_HASH_TABLE_NUM) * OB_ELECTION_HASH_TIME_US;
    // } else {
    //   election_time_offset_ = (partition_.hash() % OB_ELECTION_HASH_TABLE_NUM_NEW) * (OB_ELECTION_HASH_TIME_US_NEW -
    //   OB_ELECTION_HASH_TIME_US) + OB_ELECTION_HASH_TIME_US;
    // }
    election_time_offset_ = (partition_.hash() % OB_ELECTION_HASH_TABLE_NUM) * OB_ELECTION_HASH_TIME_US;
    old_election_time_offset_ = (partition_.hash() % OB_ELECTION_HASH_TABLE_NUM) * OB_ELECTION_HASH_TIME_US;
    new_election_time_offset_ = (partition_.hash() % OB_ELECTION_HASH_TABLE_NUM_NEW) * OB_ELECTION_HASH_TIME_US +
                                OB_ELECTION_HASH_TABLE_NUM * OB_ELECTION_HASH_TIME_US;
    // if partition belongs to system, use old parameters, in [0,1s] interval
    if (OB_SYS_TENANT_ID == extract_tenant_id(partition_.get_table_id())) {
      physical_condition_ = PhysicalCondition::DEAD;
      election_time_offset_ = old_election_time_offset_;
      temp_election_time_offset_ = election_time_offset_;
      ELECT_ASYNC_LOG(INFO, "system tenant, use old_election_time_offset_", "election", *this);
    } else {  // if partition belongs to user, set parameters according to min_cluster_version
      if (ATOMIC_LOAD(&IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1) == true &&
          GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100) {
        ATOMIC_SET(&IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1, false);
        ELECT_ASYNC_LOG(INFO, "cluster version is not less than 3.1", K_(new_election_time_offset), "election", *this);
      }
      if (ATOMIC_LOAD(&IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1) == false) {  // version>=3.1
        election_time_offset_ = new_election_time_offset_;
        ELECT_ASYNC_LOG(INFO, "election_time_offset_ update to new_election_time_offset_", "election", *this);
        physical_condition_ = PhysicalCondition::DEAD;
      } else {  // version<3.1
        election_time_offset_ = old_election_time_offset_;
        ELECT_ASYNC_LOG(INFO, "election_time_offset_ init with old_election_time_offset_", "election", *this);
        physical_condition_ = PhysicalCondition::HEALTHY;
      }
      temp_election_time_offset_ = election_time_offset_;
    }
    /****************************************************************************************************/
  }

  return ret;
}

void ObElection::destroy()
{
  int ret = OB_EAGAIN;
  const int64_t SLEEP_US = 10 * 1000;

  while (OB_EAGAIN == ret) {
    ret = OB_SUCCESS;
    do {
      WLockGuard guard(lock_);
      if (is_inited_) {
        if (is_running_) {
          if (OB_SUCCESS == (ret = try_stop_())) {
            ELECT_ASYNC_LOG(INFO, "election try_stop success", "election", *this);
          } else if (OB_EAGAIN == ret) {
            // need retry
          } else {
            FORCE_ELECT_LOG(ERROR, "election stop error", K(ret), K_(partition), K_(self));
          }
        }
        if (OB_SUCC(ret)) {
          timer_.destroy();
          ObElectionInfo::destroy();
          eg_mgr_ = NULL;
          is_inited_ = false;
          FORCE_ELECT_LOG(INFO, "ObElection destroyed success", K_(partition), K_(self), KP(this));
        } else if (OB_EAGAIN == ret) {
          // retry
        } else {
          FORCE_ELECT_LOG(WARN, "ObElection destroyed error", K(ret), K_(partition), K_(self));
        }
      }
    } while (0);

    if (OB_EAGAIN == ret) {
      usleep(SLEEP_US);
    }
  }
}

int ObElection::start()
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_US = 10 * 1000;
  bool need_retry = true;

  for (int64_t retry = 0; need_retry && OB_SUCC(ret); ++retry) {
    do {
      WLockGuard guard(lock_);
      if (!is_inited_) {
        need_retry = false;
        ret = OB_NOT_INIT;
        FORCE_ELECT_LOG(WARN, "ObElection not inited", K(ret));
      } else if (is_running_) {
        // already running, skip
        need_retry = false;
        FORCE_ELECT_LOG(WARN, "election already been started", K(ret), K_(partition), K_(self));
      } else {
        // start timestamp
        const int64_t cur_ts = get_current_ts();
        int64_t start = (cur_ts - election_time_offset_ + T_ELECT2 + RESERVED_TIME_US) / T_ELECT2 * T_ELECT2 +
                        election_time_offset_;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = timer_.start(start))) {
          FORCE_ELECT_LOG(ERROR, "election timer start error", K(tmp_ret), K(retry), K_(partition), K_(self));
        } else {
          is_running_ = true;
          need_retry = false;
          start_timestamp_ = cur_ts;
          ELECT_ASYNC_LOG(INFO, "election timer start success", K_(partition), "election", *this);
        }
      }
    } while (0);

    if (need_retry) {
      usleep(SLEEP_US);
    }
  }

  return ret;
}

int ObElection::stop()
{
  int ret = OB_EAGAIN;
  const int64_t SLEEP_US = 10 * 1000;

  while (OB_EAGAIN == ret) {
    do {
      ret = OB_SUCCESS;
      WLockGuard guard(lock_);
      if (!is_inited_) {
        ret = OB_NOT_INIT;
        FORCE_ELECT_LOG(WARN, "ObElection not inited", K(ret));
      } else if (!is_running_) {
        // do nothing, return OB_SUCCESS
      } else if (OB_SUCCESS == (ret = try_stop_())) {
        ELECT_ASYNC_LOG(INFO, "election try_stop success", K_(partition), K_(self));
      } else if (OB_EAGAIN == ret) {
        // need retry
      } else {
        ret = OB_ERR_UNEXPECTED;
        FORCE_ELECT_LOG(ERROR, "election stop error", K(ret), K_(partition), K_(self));
      }
    } while (0);

    if (OB_EAGAIN == ret) {
      usleep(SLEEP_US);
    }
  }

  if (OB_FAIL(ret)) {
    FORCE_ELECT_LOG(WARN, "election stop error", K(ret), K_(partition), K_(self));
  } else {
    ELECT_ASYNC_LOG(INFO, "election stop success", "election", *this);
  }

  return ret;
}

int ObElection::process_devote_prepare_(const ObElectionMsgDEPrepare& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K(*this), K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K(*this), K(msg));
  } else {
    const int64_t msg_real_t1 = get_msg_real_T1_(msg);
    StateHelper state_helper(state_);

    if (!IN_T1_RANGE(cur_ts, msg_real_t1)) {
      ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
      ELECT_ASYNC_LOG_(WARN,
          "message not in time",
          K(ret),
          K(*this),
          K(msg),
          K(cur_ts),
          "msg fly cost",
          cur_ts - msg.get_send_timestamp(),
          "delta over msg_real_t1",
          cur_ts - msg_real_t1);
    } else if (OB_FAIL(state_helper.switch_state(Ops::D_PREPARE))) {
      FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this), K(msg));
    } else if (OB_FAIL(msg_pool_.store(msg))) {
      FORCE_ELECT_LOG(WARN, "store deprepare message error", K(ret), K(*this), K(msg));
    } else {
      // do nothing
    }
  }
  ELECT_ASYNC_LOG(DEBUG, "process_devote_prepare finished", K(ret), K(*this), K(msg));

  return ret;
}

int ObElection::local_handle_devote_prepare_(const ObElectionMsgDEPrepare& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K(*this));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K(*this), K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K(*this), K(msg));
  } else if (current_leader_.is_valid() && OB_SUCCESS == verify_leader_valid_(cur_ts)) {
    // leader is valid, ignore
  } else if (NULL != election_group_) {
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG_(WARN, "already in election_group, ignore msg", K(ret), K(*this), K(msg));
  } else if (OB_FAIL(process_devote_prepare_(msg))) {
    ELECT_ASYNC_LOG_(WARN, "process_devote_prepare failed", K(ret), K_(partition), K(msg));
  } else {
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  ELECT_ASYNC_LOG(DEBUG, "local_handle_devote_prepare finished", K(ret), K(*this), K(msg));

  return ret;
}

int ObElection::handle_devote_prepare(const ObElectionMsgDEPrepare& msg, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();

  WLockGuard guard(lock_);
  /**********should removed after a barrier version bigger than 3.1**********/
  update_condition_(msg, msg.get_T1_timestamp());
  /*************************************************************************************/
  ELECT_ASYNC_LOG(DEBUG, "handle devote prepare", "election", *this, K(msg));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), "election", *this, K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), "election", *this, K(msg));
  } else if (current_leader_.is_valid() && OB_SUCCESS == verify_leader_valid_(cur_ts)) {
    // if current leader is valid, some one ask for decentralized voting, meaning the requester don't know the leader
    // notify him who is leader
    const ObAddr& sender = msg.get_sender();
    if (current_leader_ != sender && self_ != sender) {
      if (OB_FAIL(notify_leader_(sender))) {
        ELECT_ASYNC_LOG_(WARN, "notify leader error", K(ret), "election", *this, K(msg));
      }
    }
  } else if (current_leader_.is_valid()) {
    // leader lease has been expired
    // need revoke first
    ELECT_ASYNC_LOG_(INFO, "leader lease is expired, need revoke firstly", K(ret), "election", *this, K(msg));
    leader_revoke_();
  } else if (NULL != election_group_) {
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG_(WARN, "already in election_group, ignore msg", K(ret), "election", *this, K(msg));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_devote_prepare_(msg))) {
      ELECT_ASYNC_LOG_(WARN, "process_devote_prepare failed", K(ret), K_(partition), K(msg));
    }
  }

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObElection::process_devote_vote_(const ObElectionMsgDEVote& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K(*this));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K(*this), K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K(*this), K(msg));
  } else {
    const int64_t cur_ts = get_current_ts();
    const int64_t msg_real_t1 = get_msg_real_T1_(msg);

    if (!IN_T2_RANGE(cur_ts, msg_real_t1)) {
      ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
      ELECT_ASYNC_LOG_(WARN,
          "message not in time",
          K(ret),
          K(*this),
          K(msg),
          K(cur_ts),
          "msg fly cost",
          cur_ts - msg.get_send_timestamp(),
          "delta over msg_real_t1",
          cur_ts - msg_real_t1);
    } else if (OB_FAIL(msg_pool_.store(msg))) {
      ELECT_ASYNC_LOG_(WARN, "store devote message error", K(ret), K(*this), K(msg));
    } else {
    }
  }
  ELECT_ASYNC_LOG(DEBUG, "process_devote_vote finished", K(ret), K(*this), K(msg));

  return ret;
}

int ObElection::local_handle_devote_vote_(const ObElectionMsgDEVote& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K(*this));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K(*this), K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K(*this), K(msg));
  } else if (!is_candidate_(msg.get_sender())) {
    ret = OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER;
  } else if (NULL != election_group_) {
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG_(WARN, "already in election_group, ignore msg", K(ret), K(*this), K(msg));
  } else if (OB_FAIL(process_devote_vote_(msg))) {
  } else {
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  ELECT_ASYNC_LOG(DEBUG, "local_handle_devote_vote finished", K(ret), K(*this), K(msg));

  return ret;
}

int ObElection::handle_devote_vote(const ObElectionMsgDEVote& msg, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  /**********should removed after a barrier version bigger than 3.1**********/
  update_condition_(msg, msg.get_T1_timestamp());
  /*************************************************************************************/
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), "election", *this, K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), "election", *this, K(msg));
  } else if (!is_candidate_(msg.get_sender())) {
    ret = OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER;
  } else if (NULL != election_group_) {
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG_(WARN, "already in election_group, ignore msg", K(ret), "election", *this, K(msg));
  } else if (OB_FAIL(process_devote_vote_(msg))) {
  } else {
  }

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  ELECT_ASYNC_LOG(DEBUG, "handle devote vote", K(ret), "election", *this, K(msg));

  return ret;
}

int ObElection::process_devote_success_(const ObElectionMsgDESuccess& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K(*this), K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else {
    const int64_t cur_ts = get_current_ts();
    const ObAddr& msg_leader = msg.get_leader();
    const int64_t msg_t1 = msg.get_T1_timestamp();
    const int64_t msg_real_t1 = get_msg_real_T1_(msg);
    const int64_t lease_time = msg.get_lease_time();
    const int64_t now_lease_time = (lease_time == 0) ? (T_CENTRALIZED_VOTE_EXTENDS * T_ELECT2) : lease_time;
    StateHelper state_helper(state_);

    ObAddr cur_leader;
    int tmp_ret = OB_SUCCESS;
    if (!IN_T3_RANGE(cur_ts, msg_real_t1)) {
      ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
      ELECT_ASYNC_LOG_(WARN,
          "message not in time",
          K(ret),
          K(*this),
          K(msg),
          K(cur_ts),
          "msg fly cost",
          cur_ts - msg.get_send_timestamp(),
          "delta over msg_real_t1",
          cur_ts - msg_real_t1);
    } else if (OB_SUCCESS == (tmp_ret = get_leader_(cur_leader))) {
      if (cur_leader != msg_leader) {
        // current leader is valid, but not match with message, maybe double leader error
        if (NULL != election_group_) {
          (void)update_info_from_eg_();
        }
        ret = OB_ELECTION_WARN_PROTOCOL_ERROR;
        FORCE_ELECT_LOG(WARN, "leader available but recevive devote_success message", K(ret), K(*this), K(msg));
      }
    } else if (NULL != election_group_) {
      // current leader is invalid, need move out from election group
      ELECT_ASYNC_LOG(INFO, "leader is already invalid, need move out from eg", K(*this));
      if (OB_FAIL(move_out_election_group_unlock_())) {
        FORCE_ELECT_LOG(ERROR, "move_out_election_group_unlock_ failed", K(ret), K(*this));
      }
    } else {
      // do nothing
    }

    if (OB_SUCC(ret) && NULL != election_group_) {
      // self is in a lease valid group, don't handle devote prepare msg anymore
      ret = OB_STATE_NOT_MATCH;
    }

    if (OB_SUCC(ret)) {
      is_need_query_ = false;
      if (OB_FAIL(state_helper.switch_state(Ops::D_SUCCESS))) {
        FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this));
      } else {
        if (OB_FAIL(leader_takeover_(msg_leader, msg_t1, msg_t1, now_lease_time, false))) {
          ELECT_ASYNC_LOG_(WARN, "decentralized voting, leader takeover error", K(ret), K(*this), K(msg));
        } else {
          event_history_array_->add_event_history_item(partition_, current_leader_, EventType::L_TAKEOVER_DEVOTE);
          ELECT_ASYNC_LOG_(INFO, "decentralized voting, leader takeover success", K(*this), K(msg), K_(local_priority));
        }
      }
    }
  }

  ELECT_ASYNC_LOG(DEBUG, "process_devote_success finished", K(ret), K(*this), K(msg));
  return ret;
}

int ObElection::local_handle_devote_success_(const ObElectionMsgDESuccess& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K(*this));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K(*this), K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else if (OB_FAIL(process_devote_success_(msg))) {
    ELECT_ASYNC_LOG_(WARN, "process_devote_success_ faield", K(ret), K_(partition), K(msg));
  } else {
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  ELECT_ASYNC_LOG(DEBUG, "local_handle_devote_success finished", K(ret), K(*this), K(msg));

  return ret;
}

int ObElection::handle_devote_success(const ObElectionMsgDESuccess& msg, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  /**********should removed after a barrier version bigger than 3.1**********/
  update_condition_(msg, msg.get_T1_timestamp());
  /*************************************************************************************/
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), "election", *this, K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else if (OB_FAIL(process_devote_success_(msg))) {
    ELECT_ASYNC_LOG_(WARN, "process_devote_success_ faield", K(ret), K_(partition), K(msg));
  } else {
  }

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  ELECT_ASYNC_LOG(DEBUG, "handle_devote_success finished", K(ret), "election", *this, K(msg));

  return ret;
}

int ObElection::process_vote_prepare_(const ObElectionMsgPrepare& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K(*this), K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else {
    const int64_t cur_ts = get_current_ts();
    const bool is_reappoint_msg = msg.get_cur_leader() == msg.get_new_leader() ? true : false;
    int64_t msg_real_t1 = get_msg_real_T1_(msg);
    const int64_t lease_time = msg.get_lease_time();
    const int64_t now_lease_time = (lease_time == 0) ? (OB_ELECTION_130_LEASE_TIME) : lease_time;
    StateHelper state_helper(state_);

    /**********should removed after a barrier version bigger than 3.1**********/
    if (msg.get_cur_leader() == msg.get_new_leader() && temp_election_time_offset_ != election_time_offset_) {
      msg_real_t1 = msg.get_T1_timestamp() + temp_election_time_offset_;
    } else {
    }
    /***************************************************************/
    if (!IN_T0_RANGE(cur_ts, msg_real_t1)) {
      ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
      if (is_reappoint_msg) {
        ELECT_ASYNC_LOG_(WARN,
            "reappoint message not in time",
            K(ret),
            K(*this),
            K(msg),
            K(cur_ts),
            "msg fly cost",
            cur_ts - msg.get_send_timestamp(),
            "delta over msg_real_t1",
            cur_ts - msg_real_t1,
            K(is_reappoint_msg));
      } else {
        ELECT_ASYNC_LOG_(WARN,
            "change leader message not in time",
            K(ret),
            K(*this),
            K(msg),
            K(cur_ts),
            "msg fly cost",
            cur_ts - msg.get_send_timestamp(),
            "delta over msg_real_t1",
            cur_ts - msg_real_t1,
            K(is_reappoint_msg));
      }
    }

    if (OB_SUCC(ret)) {
      if (is_changing_leader_) {
        // only leader can reach here
        if (OB_FAIL(state_helper.switch_state(Ops::CHANGE_LEADER))) {
          FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this));
        }
      } else {
        if (OB_FAIL(state_helper.switch_state(Ops::V_PREPARE))) {
          FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (NULL != election_group_ && OB_FAIL(move_out_election_group_unlock_())) {
        // move out from group before vote, expect move out operation always success
        FORCE_ELECT_LOG(ERROR, "move_out_election_group_unlock_ failed", K(ret), K_(partition));
      } else if (OB_FAIL(msg_pool_.store(msg))) {
        ELECT_ASYNC_LOG_(WARN, "store prepare message error", K(ret), K(*this), K(msg));
      } else {
        // T1_timestamp_ = msg.get_T1_timestamp();
        if (OB_FAIL(try_centralized_voting_(now_lease_time, msg.get_T1_timestamp()))) {
          ELECT_ASYNC_LOG_(WARN, "try centralized voting error", K(ret), K(*this), K(msg), K(now_lease_time));
        }
      }
    }
  }

  ELECT_ASYNC_LOG(DEBUG, "process_vote_prepare finished", K(ret), K(*this), K(msg));
  return ret;
}

int ObElection::local_handle_vote_prepare_(const ObElectionMsgPrepare& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K(*this));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K(*this), K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else if (OB_FAIL(process_vote_prepare_(msg))) {
    ELECT_ASYNC_LOG_(WARN, "process_vote_prepare_ failed", K(ret), K_(partition), K(msg));
  } else {
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  ELECT_ASYNC_LOG(DEBUG, "local_handle_vote_prepare finished", K(ret), K(*this), K(msg));

  return ret;
}

int ObElection::handle_vote_prepare(const ObElectionMsgPrepare& msg, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  /**********should removed after a barrier version bigger than 3.1**********/
  bool is_run_in_new_parameter = update_condition_(msg, msg.get_T1_timestamp());
  temp_election_time_offset_ = is_run_in_new_parameter ? new_election_time_offset_ : old_election_time_offset_;
  /*************************************************************************************/
  ELECT_ASYNC_LOG(DEBUG, "handle_vote prepare", "election", *this, K(msg));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), "election", *this, K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else if (OB_FAIL(process_vote_prepare_(msg))) {
    ELECT_ASYNC_LOG_(WARN, "process_vote_prepare_ failed", K(ret), K_(partition), K(msg));
  } else {
  }

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  /**********should removed after a barrier version bigger than 3.1**********/
  temp_election_time_offset_ = election_time_offset_;
  /*************************************************************************************/
  return ret;
}

int ObElection::process_vote_vote_(const ObElectionMsgVote& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K_(partition), K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_ELECTION_WARN_INVALID_MESSAGE;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K(*this), K(msg));
  } else {
    const int64_t cur_ts = get_current_ts();
    const bool is_reappoint_msg = msg.get_cur_leader() == msg.get_new_leader() ? true : false;
    const int64_t msg_real_t1 = get_msg_real_T1_(msg);
    StateHelper state_helper(state_);

    if (!IN_T0_RANGE(cur_ts, msg_real_t1)) {
      ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
      if (is_reappoint_msg) {
        ELECT_ASYNC_LOG_(WARN,
            "reappoint message not in time",
            K(ret),
            K(*this),
            K(msg),
            K(cur_ts),
            "msg fly cost",
            cur_ts - msg.get_send_timestamp(),
            "delta over msg_real_t1",
            cur_ts - msg_real_t1,
            K(is_reappoint_msg));
      } else {
        ELECT_ASYNC_LOG_(WARN,
            "change leader message not in time",
            K(ret),
            K(*this),
            K(msg),
            K(cur_ts),
            "msg fly cost",
            cur_ts - msg.get_send_timestamp(),
            "delta over msg_real_t1",
            cur_ts - msg_real_t1,
            K(is_reappoint_msg));
      }
    }

    if (OB_SUCC(ret)) {
      if (role_ != ROLE_LEADER) {
        ret = OB_NOT_MASTER;
      } else if (OB_FAIL(state_helper.switch_state(Ops::V_VOTE))) {
        FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this));
      } else if (OB_FAIL(collect_valid_candidates_(msg))) {
        ELECT_ASYNC_LOG_(WARN, "collect_valid_candidates error", K(ret), K_(partition), K(msg));
      } else if (OB_FAIL(msg_pool_.store(msg))) {
        ELECT_ASYNC_LOG_(WARN, "store vote message error", K(ret), K(*this), K(msg));
      } else if (OB_FAIL(check_centralized_majority_())) {
        ELECT_ASYNC_LOG_(WARN, "check_centralized_majority error", K(ret), K(*this), K(msg));
      } else {
        // do nothing
      }
    }
  }

  ELECT_ASYNC_LOG(DEBUG, "process_vote_vote finished", K(ret), K(*this), K(msg));
  return ret;
}

int ObElection::local_handle_vote_vote_(const ObElectionMsgVote& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K_(partition), K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_ELECTION_WARN_INVALID_MESSAGE;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K(*this), K(msg));
  } else if (!is_candidate_(msg.get_sender())) {
    ret = OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER;
    ELECT_ASYNC_LOG_(WARN, "message sender is not candidate", K(ret), K_(partition), K(msg));
  } else if (NULL != election_group_) {
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG_(WARN, "already in election_group, ignore msg", K(ret), K(*this), K(msg));
  } else if (OB_FAIL(process_vote_vote_(msg))) {
    ELECT_ASYNC_LOG_(WARN, "process_vote_vote_ failed", K(ret), K(*this), K(msg));
  } else {
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  ELECT_ASYNC_LOG(DEBUG, "local_handle_vote_vote finished", K(ret), K(*this), K(msg));

  return ret;
}

int ObElection::handle_vote_vote(const ObElectionMsgVote& msg, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  /**********should removed after a barrier version bigger than 3.1**********/
  update_condition_(msg, msg.get_T1_timestamp());
  /*************************************************************************************/
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K_(partition), K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_ELECTION_WARN_INVALID_MESSAGE;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), "election", *this, K(msg));
  } else if (!is_candidate_(msg.get_sender())) {
    ret = OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER;
    ELECT_ASYNC_LOG_(WARN, "message sender is not candidate", K(ret), K_(partition), K(msg));
  } else if (NULL != election_group_) {
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG_(WARN, "already in election_group, ignore msg", K(ret), "election", *this, K(msg));
  } else if (OB_FAIL(process_vote_vote_(msg))) {
    ELECT_ASYNC_LOG_(WARN, "process_vote_vote_ failed", K(ret), "election", *this, K(msg));
  } else {
  }

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  ELECT_ASYNC_LOG(DEBUG, "handle_vote_vote finished", K(ret), "election", *this, K(msg));

  return ret;
}

int ObElection::process_vote_success_(const ObElectionMsgSuccess& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;

  if (msg.get_last_leader_epoch() != OB_INVALID_TIMESTAMP &&
      (max_leader_epoch_ever_seen_ == OB_INVALID_TIMESTAMP ||  // avoid OB_INVALID_TIMESTAMP definded too large
          msg.get_last_leader_epoch() > max_leader_epoch_ever_seen_)) {
    max_leader_epoch_ever_seen_ = msg.get_last_leader_epoch();
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K_(partition), K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else {
    const int64_t cur_ts = get_current_ts();
    const bool is_reappoint_msg = msg.get_cur_leader() == msg.get_new_leader() ? true : false;
    const int64_t msg_t1 = msg.get_T1_timestamp();
    const int64_t msg_real_t1 = get_msg_real_T1_(msg);
    const int64_t lease_time = msg.get_lease_time();
    const int64_t now_lease_time = (lease_time == 0) ? (OB_ELECTION_130_LEASE_TIME) : lease_time;

    const ObAddr& msg_new_leader = msg.get_new_leader();
    const ObAddr& msg_cur_leader = msg.get_cur_leader();
    ObAddr cur_leader;

    if (!IN_T0_RANGE(cur_ts, msg_real_t1)) {
      ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
      if (is_reappoint_msg) {
        ELECT_ASYNC_LOG_(WARN,
            "reappoint message not in time",
            K(ret),
            K(*this),
            K(msg),
            K(cur_ts),
            "msg fly cost",
            cur_ts - msg.get_send_timestamp(),
            "delta over msg_real_t1",
            cur_ts - msg_real_t1,
            K(is_reappoint_msg));
      } else {
        ELECT_ASYNC_LOG_(WARN,
            "change leader message not in time",
            K(ret),
            K(*this),
            K(msg),
            K(cur_ts),
            "msg fly cost",
            cur_ts - msg.get_send_timestamp(),
            "delta over msg_real_t1",
            cur_ts - msg_real_t1,
            K(is_reappoint_msg));
      }
    }

    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS == (tmp_ret = get_leader_(cur_leader))) {
        if (cur_leader != msg_cur_leader) {
          // current leader is valid but nat match with msg leader, maybe double leader error
          if (NULL != election_group_) {
            (void)update_info_from_eg_();
          }
          ret = OB_ELECTION_WARN_PROTOCOL_ERROR;
          ELECT_ASYNC_LOG_(WARN,
              "lease available but cur_leader != msg_cur_leader, maybe leader is changed",
              K(ret),
              K(*this),
              K(msg));
        }
      } else if (NULL != election_group_) {
        // current leader is invalid, need move out from group first
        ELECT_ASYNC_LOG(INFO, "leader is already invalid, need move out from eg", K_(partition));
        if (OB_FAIL(move_out_election_group_unlock_())) {
          FORCE_ELECT_LOG(ERROR, "move_out_election_group_unlock_ failed", K(ret), K_(partition));
        }
      } else {
        // do nothing
      }
    }

    if (OB_SUCC(ret) && NULL != election_group_) {
      // in group already, don't handle vote success msg anymore
      ret = OB_STATE_NOT_MATCH;
    }

    if (OB_SUCC(ret)) {
      is_need_query_ = false;
      StateHelper state_helper(state_);

      if (cur_leader == msg_new_leader) {
        inc_update(&last_gts_, msg.get_last_gts());
        if (OB_FAIL(state_helper.switch_state(Ops::V_SUCCESS))) {
          FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this));
        } else if (OB_FAIL(leader_reappoint_(msg_new_leader, msg_t1, now_lease_time))) {
          ELECT_ASYNC_LOG_(WARN, "centralized voting, leader reappoint error", K(ret), K(*this), K(msg));
        } else {
          ELECT_ASYNC_LOG_(DEBUG, "centralized voting, leader reappoint success", K(*this), K(msg));
        }
      } else {  // for change leader, need double check priority before takeover
        inc_update(&last_gts_, msg.get_last_gts());
        const bool is_change_leader = cur_leader.is_valid();
        if (is_change_leader) {
          if (OB_FAIL(state_helper.switch_state(Ops::CHANGE_LEADER))) {
            FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this));
          }
        } else {
          if (OB_FAIL(state_helper.switch_state(Ops::V_SUCCESS))) {
            FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this));
          }
        }

        if (OB_SUCC(ret)) {
          if (is_self_(msg_new_leader) && OB_FAIL(priority_double_check_())) {
            ELECT_ASYNC_LOG_(WARN, "priority double check error", K(ret), K(*this));
          } else if (!can_elect_new_leader_()) {
            ret = OB_STATE_NOT_MATCH;
            FORCE_ELECT_LOG(WARN, "cluster state do not allow new leader takeover, give up", K(ret), K(*this));
          } else if (OB_FAIL(leader_takeover_(msg_new_leader, msg_t1, msg_t1, now_lease_time, is_change_leader))) {
            ELECT_ASYNC_LOG_(WARN, "change leader, leader takeover error", K(ret), K(*this), K(msg));
          } else if (is_change_leader) {  // change leader
            EVENT_ADD(ELECTION_CHANGE_LEAER_COUNT, 1);
            event_history_array_->add_event_history_item(partition_, current_leader_, EventType::L_TAKEOVER_CHANGE);
            ELECT_ASYNC_LOG(INFO,
                "change leader, leader takeover success",
                K_(partition),
                K_(self),
                K_(current_leader),
                K(msg_cur_leader),
                K(msg_new_leader),
                K(msg_t1),
                K_(leader_epoch));
          } else {
            event_history_array_->add_event_history_item(partition_, current_leader_, EventType::L_TAKEOVER_REAPPOINT);
          }
        }
      }
    }
  }

  ELECT_ASYNC_LOG(DEBUG, "process_vote_success finished", K(ret), K(*this), K(msg));
  return ret;
}

int ObElection::local_handle_vote_success_(const ObElectionMsgSuccess& msg)
{
  // requires caller to lock and check
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K_(partition), K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else if (OB_FAIL(process_vote_success_(msg))) {
    ELECT_ASYNC_LOG_(WARN, "process_vote_success_ failed", K(ret), K_(partition), K(msg));
  } else {
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  ELECT_ASYNC_LOG(DEBUG, "local_handle_vote_success finished", K(ret), K(*this), K(msg));

  return ret;
}

int ObElection::handle_vote_success(const ObElectionMsgSuccess& msg, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  /**********should removed after a barrier version bigger than 3.1**********/
  update_condition_(msg, msg.get_T1_timestamp());
  /*************************************************************************************/
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K_(partition), K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else if (OB_FAIL(process_vote_success_(msg))) {
    ELECT_ASYNC_LOG_(WARN, "process_vote_success_ failed", K(ret), K_(partition), K(msg));
  } else {
  }

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  ELECT_ASYNC_LOG(DEBUG, "handle_vote_success finished", K(ret), "election", *this, K(msg));

  return ret;
}

int ObElection::handle_query_leader(const ObElectionQueryLeader& msg, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  /**********should removed after a barrier version bigger than 3.1**********/
  update_condition_(msg, OB_INVALID_TIMESTAMP);
  /*************************************************************************************/
  ELECT_ASYNC_LOG(DEBUG, "handle_query_leader", "election", *this, K(msg));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K_(partition), K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argumenet", K(ret), K_(partition), K(msg));
  } else {
    const int64_t cur_ts = get_current_ts();
    const ObAddr& sender = msg.get_sender();
    // defense
    if (OB_SUCC(verify_leader_valid_(cur_ts))  // leader must be valid
        && current_leader_ != sender           // sender can't be leader
        && self_ != sender) {                  // sender can't be self
      if (OB_FAIL(notify_leader_(sender))) {
        ELECT_ASYNC_LOG_(WARN, "notify leader error", K(ret), "election", *this);
      }
    }
  }

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  return ret;
}

int ObElection::handle_query_leader_response(const ObElectionQueryLeaderResponse& msg, ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  /**********should removed after a barrier version bigger than 3.1**********/
  update_condition_(msg, msg.get_t1());
  /*************************************************************************************/
  ELECT_ASYNC_LOG(DEBUG, "handle_query_leader_response", "election", *this, K(msg));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), K_(partition), K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argumenet", K(ret), K_(partition), K(msg));
  } else if (NULL != election_group_) {
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG_(WARN, "already in election_group, ignore msg", K(ret), "election", *this, K(msg));
  } else {
    const ObAddr& msg_leader = msg.get_leader();
    const int64_t t1 = msg.get_t1();
    const int64_t epoch = msg.get_epoch();
    const int64_t lease_time = msg.get_lease_time();
    const int64_t now_lease_time =
        lease_time - election_time_offset_;  // reduce election_time_offset because will added in leader_elected process
    StateHelper state_helper(state_);

    if (self_ == msg_leader || now_lease_time <= 0) {
      ret = OB_ERR_UNEXPECTED;
      if (self_ == msg_leader) {
        ELECT_ASYNC_LOG_(WARN, "leader dont't know self is leader", K(ret), "election", *this, K(msg));
      } else {
        ELECT_ASYNC_LOG_(WARN, "now_lease_time is not greater than 0", K(ret), "election", *this, K(msg));
      }
    } else if (current_leader_ != msg_leader) {
      if (is_real_leader_(current_leader_)) {
        ELECT_ASYNC_LOG(INFO, "current leader is valid, ignore msg_leader", K(msg), "election", *this);
      } else if (t1 + election_time_offset_ + lease_time + T_SLAVE_LEASE_MORE <
                 leader_lease_.second) {  // make sure lease not go back
        ELECT_ASYNC_LOG_(WARN, "receive older leader lease", K(ret), "election", *this, K(msg));
      } else if (OB_FAIL(leader_takeover_(msg_leader, epoch, t1, now_lease_time, false))) {
        ELECT_ASYNC_LOG_(WARN, "leader takeover error", K(ret), "election", *this, K(msg));
      } else {
        event_history_array_->add_event_history_item(partition_, current_leader_, EventType::L_TAKEOVER_QUERY);
        ELECT_ASYNC_LOG_(DEBUG, "leader takeover success", "election", *this, K(msg));
      }
    } else {
      if (t1 + election_time_offset_ + lease_time + T_SLAVE_LEASE_MORE <
          leader_lease_.second) {  // make sure lease not go back
        // avoid older lease cover new lease
        ELECT_ASYNC_LOG_(WARN, "receive older leader lease", K(ret), "election", *this, K(msg));
      } else if (OB_FAIL(leader_reappoint_(msg_leader, t1, now_lease_time))) {
        ELECT_ASYNC_LOG_(WARN, "leader reappoint error", K(ret), "election", *this, K(msg));
      } else {
        ELECT_ASYNC_LOG_(DEBUG, "leader reappoint success", "election", *this, K(msg));
      }
    }
    if (OB_SUCC(ret) && current_leader_.is_valid() && is_real_leader_(current_leader_)) {
      if (OB_FAIL(state_helper.switch_state(Ops::QUERY_RESPONSE))) {
        FORCE_ELECT_LOG(WARN, "switch state error", K(ret), "election", *this);
      } else {
        is_need_query_ = true;
      }
    }
  }

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  return ret;
}

int ObElection::set_candidate(
    const int64_t replica_num, const ObMemberList& curr_mlist, const int64_t membership_version)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (replica_num <= 0 || replica_num > OB_MAX_MEMBER_NUMBER || curr_mlist.get_member_number() <= 0 ||
             curr_mlist.get_member_number() > replica_num || !is_valid_membership_version(membership_version) ||
             curr_membership_version_ > membership_version) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN,
        "invalid argument",
        K(ret),
        K_(partition),
        K(replica_num),
        K(curr_mlist),
        K(membership_version),
        K_(curr_membership_version),
        K_(replica_num));
  } else {
    const bool is_member_list_equal = curr_candidates_.member_addr_equal(curr_mlist);
    if (!is_member_list_equal && OB_FAIL(curr_candidates_.deep_copy(curr_mlist))) {
      FORCE_ELECT_LOG(WARN, "set candidates error", K(ret), K_(partition), K(curr_mlist));
    } else {
      replica_num_ = replica_num;
      curr_membership_version_ = membership_version;
      if (!is_candidate_(self_) && is_real_leader_(self_)) {
        ELECT_ASYNC_LOG(INFO, "self is not candidate, leader revoke", K_(partition));
        leader_revoke_();
        event_history_array_->add_event_history_item(partition_, current_leader_, EventType::L_REVOKE_NOT_CANDIDATE);
      }
      if (!is_member_list_equal && NULL != election_group_) {
        // memberlist changed, need move out from election group
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = move_out_election_group_unlock_())) {
          FORCE_ELECT_LOG(ERROR, "move_out_election_group_unlock_ failed", K(tmp_ret), K_(partition));
        }
      }
    }
  }
  if (REACH_TIME_INTERVAL(100 * 1000)) {
    ELECT_ASYNC_LOG(INFO,
        "set candidates",
        K(ret),
        K_(partition),
        K_(curr_candidates),
        K_(curr_membership_version),
        K_(replica_num));
  }
  ELECT_ASYNC_LOG(TRACE,
      "set candidates",
      K(ret),
      K_(partition),
      K_(curr_candidates),
      K_(curr_membership_version),
      K_(replica_num));

  return ret;
}

int ObElection::force_leader_async()
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    FORCE_ELECT_LOG(WARN, "election is not running", K(ret), "election", *this);
  } else if (OB_FAIL(force_leader_async_())) {
    FORCE_ELECT_LOG(WARN, "force leader async error", K(ret), "election", *this);
  } else {
    is_force_leader_ = true;
    ELECT_ASYNC_LOG(INFO, "force leader async success", "election", *this);
  }

  return ret;
}

int ObElection::change_leader_async(const ObAddr& leader, ObTsWindows& changing_leader_windows)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    FORCE_ELECT_LOG(WARN, "election is not running", K(ret), "election", *this);
  } else if (!leader.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(leader));
  } else if (!can_elect_new_leader_()) {
    // cluster status not allowed change leader
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG(INFO, "cluster state do not allow do change leader", K_(partition), K(ret));
  } else if (OB_FAIL(change_leader_async_(leader, changing_leader_windows))) {
    FORCE_ELECT_LOG(WARN, "change leader async error", K(ret), "election", *this, K(leader));
  } else {
    ELECT_ASYNC_LOG(DEBUG, "change leader async success", "election", *this, K(leader));
  }

  return ret;
}

// This function can be used to trigger self revoke->takeover, and update leader_epoch.
int ObElection::change_leader_to_self_async()
{
  int ret = OB_SUCCESS;
  const ObAddr new_leader = self_;
  ObTsWindows unused_change_window;

  WLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    FORCE_ELECT_LOG(WARN, "election is not running", K(ret), "election", *this);
  } else if (!can_elect_new_leader_()) {
    // cluster status not allowed change leader
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG(INFO, "cluster state do not allow do change leader", K_(partition), K(ret));
  } else if (OB_FAIL(change_leader_async_(new_leader, unused_change_window))) {
    FORCE_ELECT_LOG(WARN, "change leader async error", K(ret), "election", *this, K(new_leader));
  } else {
    ELECT_ASYNC_LOG(DEBUG, "change leader to self async success", "election", *this, K(new_leader));
  }

  return ret;
}

int ObElection::get_curr_candidate(ObMemberList& mlist) const
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (OB_FAIL(mlist.deep_copy(curr_candidates_))) {
    FORCE_ELECT_LOG(WARN, "get curr candidate error", K(ret), "election", *this);
  } else {
    // do nothing
  }

  return ret;
}

// only leader could call this interface
// return OB_NOT_MASTER if caller is follower
int ObElection::get_valid_candidate(ObMemberList& mlist) const
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    FORCE_ELECT_LOG(WARN, "election is not running", K(ret), "election", *this);
  } else if (!is_real_leader_(self_)) {
    ELECT_ASYNC_LOG(DEBUG, "election is not real leader", "election", *this);
    ret = OB_NOT_MASTER;
  } else if (valid_candidates_.get_member_number() < curr_candidates_.get_member_number()) {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG(INFO, "valid candidates less than current candidates", "election", *this, K_(valid_candidates));
    }
    if (valid_candidates_.get_member_number() == 0) {
      ret = OB_EAGAIN;
    }
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mlist.deep_copy(valid_candidates_))) {
      FORCE_ELECT_LOG(WARN, "deep copy candidate error", K(ret), "election", *this);
    }
  }

  return ret;
}

int ObElection::get_leader(ObAddr& leader, int64_t& leader_epoch, bool& is_elected_by_changing_leader,
    ObTsWindows& changing_leader_windows) const
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
  } else {
    const int64_t cur_ts = get_current_ts();
    if (OB_FAIL(verify_leader_valid_(cur_ts))) {
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        ELECT_ASYNC_LIMIT_LOG(WARN, "current leader is invalid", K(ret), "election", *this, K(cur_ts));
      }
    } else {
      leader = current_leader_;
      leader_epoch = leader_epoch_;
      is_elected_by_changing_leader = is_elected_by_changing_leader_;
      if (proposal_leader_.is_valid()) {
        get_change_leader_window_(change_leader_timestamp_, changing_leader_windows);
      }
    }
  }

  return ret;
}

int ObElection::get_leader(
    ObAddr& leader, ObAddr& previous_leader, int64_t& leader_epoch, bool& is_elected_by_changing_leader) const
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
  } else {
    const int64_t cur_ts = get_current_ts();
    if (OB_FAIL(verify_leader_valid_(cur_ts))) {
      if (REACH_TIME_INTERVAL(3 * 1000 * 1000)) {
        ELECT_ASYNC_LIMIT_LOG(WARN, "current leader is invalid", K(ret), "election", *this, K(cur_ts));
      }
    } else {
      leader = current_leader_;
      previous_leader = previous_leader_;
      leader_epoch = leader_epoch_;
      is_elected_by_changing_leader = is_elected_by_changing_leader_;
    }
  }

  return ret;
}

int ObElection::get_current_leader(ObAddr& leader) const
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
  } else {
    leader = current_leader_;
  }

  return ret;
}

int ObElection::get_election_info(ObElectionInfo& election_info) const
{
  RLockGuard guard(lock_);
  election_info = *this;
  return OB_SUCCESS;
}

uint64_t ObElection::hash() const
{
  const uint64_t hash_val = get_partition().hash();
  return hash_val;
}

int64_t ObElection::get_current_ts() const
{
  return ObClockGenerator::getRealClock();
}

int ObElection::priority_double_check_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(refresh_local_priority_())) {
    // do nothing
  } else if (!local_priority_.is_candidate()) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(WARN, "priority is not candidate", K(ret), K_(local_priority));
  } else {
    // do nothing
  }

  return ret;
}

int ObElection::refresh_local_priority_()
{
  int ret = OB_SUCCESS;
  ObElectionPriority priority;

  // thread safe. no need lock
  if (OB_ISNULL(election_cb_)) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(WARN, "election callback is null", K(ret), KP_(election_cb));
  } else if (OB_FAIL(election_cb_->on_get_election_priority(priority))) {
    // do nothing
  } else if (!priority.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(WARN, "election priority is invalid", K(ret), K_(partition), K(priority));
  } else {
    local_priority_ = priority;
  }

  return ret;
}

int ObElection::cal_valid_candidates_()
{
  int ret = OB_SUCCESS;

  if (tmp_valid_candidates_.get_member_number() > 0) {
    ret = valid_candidates_.deep_copy(tmp_valid_candidates_);
    tmp_valid_candidates_.reset();
  } else {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      FORCE_ELECT_LOG(WARN, "all candidates invalid", "election", *this);
    }
    valid_candidates_.reset();
  }

  return ret;
}

// check if the corresponding lease is valid
bool ObElection::verify_lease_start_valid_(const ObAddr& leader, const int64_t logic_lease_start_t1) const
{
  bool bool_ret = false;
  if (!leader.is_valid() || logic_lease_start_t1 <= 0) {
    // invalid argument, return false
  } else {
    int64_t lease_end = real_ts_(logic_lease_start_t1) + OB_ELECTION_130_LEASE_TIME;
    if (self_ == leader) {
      lease_end += T_LEADER_LEASE_MORE;
    } else {
      lease_end += T_SLAVE_LEASE_MORE;
    }
    const int64_t cur_ts = get_current_ts();
    bool_ret = (cur_ts <= lease_end);
  }
  return bool_ret;
}

// call this interface to assign a leader
// called when partition is created
int ObElection::leader_takeover(const ObAddr& leader, const int64_t lease_start, int64_t& leader_epoch)
{
  int ret = OB_SUCCESS;
  const int64_t logic_t1 = ((lease_start - get_election_time_offset()) / T_ELECT2) * T_ELECT2;

  WLockGuard guard(lock_);
  StateHelper state_helper(state_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!leader.is_valid() || lease_start <= 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), "election", *this, K(leader), K(lease_start));
  } else if (!verify_lease_start_valid_(leader, logic_t1)) {
    FORCE_ELECT_LOG(
        WARN, "invalid lease_start, ignore initial leader", K(ret), "election", *this, K(leader), K(lease_start));
  } else if (OB_FAIL(state_helper.switch_state(Ops::V_SUCCESS))) {
    FORCE_ELECT_LOG(WARN, "switch state error", K(ret), "election", *this);
  } else {
    if (OB_FAIL(leader_takeover_(leader, logic_t1, logic_t1, lease_time_, false))) {
      FORCE_ELECT_LOG(WARN, "leader takeover error", K(ret), "election", *this, K(leader), K(logic_t1));
    } else {
      leader_epoch = leader_epoch_;
      event_history_array_->add_event_history_item(partition_, current_leader_, EventType::L_TAKEOVER_ASSIGN);
      ELECT_ASYNC_LOG(INFO, "leader takeover success", "election", *this, K(leader), K(logic_t1), K_(local_priority));
    }
    // replica type is considered in prioeiry, no need more judge here
    // wrong replica type will be handled by caller
  }
  return ret;
}

int ObElection::try_stop_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(timer_.try_stop())) {
    FORCE_ELECT_LOG(WARN, "timer try stop error", K(ret), "election", *this);
  } else {
    is_running_ = false;
    ELECT_ASYNC_LOG(INFO, "timer try stop success", "election", *this);
    // move out from election group when stop
    if (NULL != election_group_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = move_out_election_group_unlock_())) {
        FORCE_ELECT_LOG(ERROR, "move_out_election_group_unlock_ failed", K_(partition), K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObElection::force_leader_async_()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObElection::change_leader_async_(const ObAddr& server, ObTsWindows& changing_leader_windows)
{
  int ret = OB_SUCCESS;
  int64_t delay = 0;

  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(server));
  } else if (!is_candidate_(server)) {
    ret = OB_ELECTION_WARN_NOT_CANDIDATE;
  } else if (!is_real_leader_(self_)) {
    ret = OB_ELECTION_WARN_CURRENT_SERVER_NOT_LEADER;
  } else if (proposal_leader_.is_valid()) {
    ret = OB_ELECTION_WARN_LAST_OPERATION_NOT_DONE;
  } else if (server != current_leader_) {
    // leader need move out from election group when received a change leader request
    if (NULL != election_group_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = move_out_election_group_unlock_())) {
        FORCE_ELECT_LOG(ERROR, "move_out_election_group_unlock_ failed", K_(partition), K(tmp_ret));
      }
    }
    need_change_leader_ = true;
    proposal_leader_ = server;
    const int64_t cur_ts = get_current_ts();
    // change leader task will be executed at T1 since version 2.1
    change_leader_timestamp_ = ((cur_ts / T_ELECT2 + 1) * T_ELECT2);

    // reserve a short time, make sure most transations could done before change leader
    // if change leader timestamp is too close, register to next valid time stamp
    if (cur_ts >= change_leader_timestamp_ - ObServerConfig::get_instance().trx_force_kill_threshold) {
      change_leader_timestamp_ += T_ELECT2;
    }

    if (OB_FAIL(timer_.register_change_leader_once(change_leader_timestamp_, delay))) {
      ELECT_ASYNC_LIMIT_LOG(WARN,
          "fail to register change leader task",
          K(ret),
          "election",
          *this,
          K_(change_leader_timestamp),
          K(delay));
      // register timer task failed, clear state
      change_leader_clear_();
    } else {
      get_change_leader_window_(change_leader_timestamp_, changing_leader_windows);
    }
    ELECT_ASYNC_LOG(
        INFO, "need change leader", K(ret), "election", *this, K(server), K_(change_leader_timestamp), K(delay));
  } else {
    // change leader to self
    changing_leader_windows.reset();
    const int64_t old_leader_epoch = leader_epoch_;
    need_change_leader_ = true;
    proposal_leader_ = server;
    // execute change leader to self, inc leader_epoch_
    leader_epoch_ = old_leader_epoch + 1;
    // change finished
    change_leader_clear_();
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = election_cb_->on_election_role_change())) {
      FORCE_ELECT_LOG(WARN, "on_election_role_change failed", K(tmp_ret), K_(partition));
    }
    ELECT_ASYNC_LOG(INFO, "change leader to self finished", K(ret), "election", *this, K(old_leader_epoch));
  }

  return ret;
}

int ObElection::update_info_from_eg_()
{
  // update info from election group
  // leader_epoch changed when leader changed only, so no need update from group
  int ret = OB_SUCCESS;

  int64_t eg_lease_end = 0;
  int64_t eg_takeover_t1_ts = 0;
  const int64_t cur_ts = get_current_ts();
  if (OB_FAIL(election_group_->get_leader_lease_info(
          partition_, eg_part_array_idx_, cur_ts, eg_lease_end, eg_takeover_t1_ts))) {
    FORCE_ELECT_LOG(WARN, "get_leader_lease_info failed", K(ret), K_(partition), "eleciton", *this);
  } else if (eg_lease_end < 0) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(ERROR, "eg_lease_end is invalid", K(ret), K(eg_lease_end), "election", *this);
  } else {
    // get group lease.end
    leader_lease_.second = eg_lease_end > leader_lease_.second ? eg_lease_end : leader_lease_.second;
    cached_lease_end_ = leader_lease_.second;
    takeover_t1_timestamp_ = (eg_takeover_t1_ts - election_time_offset_) / T_ELECT2 * T_ELECT2;
    if (is_in_eg_) {
      // update T1_timestamp_, avoid t1 behind too much
      // T1 must be integer multiple of T_ELECT2, or ObElectionVoteMsg::is_valid() will return error
      int64_t expected_T1 = (get_current_ts() - election_time_offset_) / T_ELECT2 * T_ELECT2;
      T1_timestamp_ = T1_timestamp_ > expected_T1 ? T1_timestamp_ : expected_T1;
    }
    common::ObAddr eg_unconfirmed_leader;
    lease_t eg_unconfirmed_leader_lease;
    if (OB_FAIL(election_group_->get_unconfirmed_leader_info(eg_unconfirmed_leader, eg_unconfirmed_leader_lease))) {
      FORCE_ELECT_LOG(ERROR, "get_unconfirmed_leader_info failed", K(ret), K_(partition));
    } else {
      // update group's valid lease to election's state
      unconfirmed_leader_ = eg_unconfirmed_leader;
      unconfirmed_leader_lease_.second = eg_unconfirmed_leader_lease.second;
    }
  }
  return ret;
}

void ObElection::change_leader_clear_()
{
  proposal_leader_.reset();
  need_change_leader_ = false;
  is_changing_leader_ = false;
  change_leader_timestamp_ = 0;
}

// check if server is leader, check role and lease also
bool ObElection::is_real_leader_(const ObAddr& server) const
{
  bool bool_ret = false;
  const int64_t cur_ts = get_current_ts();

  if (!server.is_valid()) {
    FORCE_ELECT_LOG(WARN, "invalid argument", K(server));
    bool_ret = false;
  } else {
    bool is_lease_expired = leader_lease_is_expired_(cur_ts);
    if (self_ == server) {
      bool_ret = ((current_leader_ == server) && (ROLE_LEADER == role_) && !is_lease_expired);
    } else {
      bool_ret = ((current_leader_ == server) && !is_lease_expired);
    }
  }

  return bool_ret;
}

bool ObElection::check_if_allowed_to_move_into_eg_()
{
  // check if allowed to join the group
  bool bool_ret = false;
  const int64_t cur_ts = get_current_ts();
  if (!unconfirmed_leader_.is_valid() || unconfirmed_leader_ == current_leader_ ||
      unconfirmed_leader_lease_is_expired_(cur_ts)) {
    bool_ret = true;
  }
  return bool_ret;
}

int ObElection::leader_elected_(const ObAddr& leader, const int64_t logic_T1_timestamp, const int64_t lease_time,
    const bool is_elected_by_changing_leader)
{
  int ret = OB_SUCCESS;

  if (!leader.is_valid() || logic_T1_timestamp <= 0 || logic_T1_timestamp % T_ELECT2 != 0 || lease_time <= 0) {
    FORCE_ELECT_LOG(WARN, "invalid argument", K(leader), K(logic_T1_timestamp), K(lease_time));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // this interface will be called in change leader ande reappoint process
    // update previous_leader_ if and only if leader and current_leader_ matched
    if (current_leader_ != leader) {
      previous_leader_ = current_leader_;
    }
    // set current leader
    current_leader_ = leader;
    is_reappoint_period_ = true;
    takeover_t1_timestamp_ = logic_T1_timestamp;
    // set lease
    leader_lease_.first = real_ts_(logic_T1_timestamp);
    vote_period_ = T_CENTRALIZED_VOTE_EXTENDS_V2;
    lease_time_ = OB_ELECTION_130_LEASE_TIME;
    is_elected_by_changing_leader_ = is_elected_by_changing_leader;
    if (current_leader_ == self_) {
      role_ = ROLE_LEADER;
      leader_lease_.second = real_ts_(logic_T1_timestamp) + (lease_time + T_LEADER_LEASE_MORE);
      if (GCONF.enable_election_group && T1_timestamp_ > 0 &&
          !is_elected_by_changing_leader
          /**********should removed after a barrier version bigger than 3.1**********/
          && (physical_condition_ == PhysicalCondition::HEALTHY || physical_condition_ == PhysicalCondition::DEAD)
          /***********************************************************************************************************************/
      ) {
        // leader try join group if group is enabled
        // this logic is not executed when changing leader,
        // to avoid affecting the processing speed of change leader
        int tmp_ret = OB_SUCCESS;
        ObElectionGroupId eg_id;
        if (NULL == eg_mgr_) {
          FORCE_ELECT_LOG(WARN, "eg_mgr_ is NULL");
        } else if (!is_real_leader_(self_)) {
          // self is not valid leader, skip
        } else if (false == check_if_allowed_to_move_into_eg_()) {
          // unconfirmed_leader_ not allow to move
        } else if (NULL != election_group_ && current_leader_ != election_group_->get_curr_leader()) {
          // change lease, self is new leader, move out from group
          if (OB_SUCCESS != (tmp_ret = move_out_election_group_unlock_())) {
            FORCE_ELECT_LOG(ERROR, "move_out_election_group_unlock_ failed", K_(partition), K(tmp_ret));
          }
        } else if (proposal_leader_.is_valid()) {
          // changing leader, move in group not allowed
        } else if (OB_SUCCESS != (tmp_ret = eg_mgr_->assign_election_group(
                                      partition_, current_leader_, replica_num_, curr_candidates_, eg_id))) {
          FORCE_ELECT_LOG(WARN, "assign_election_group failed", K(tmp_ret), K_(partition));
        } else if (OB_SUCCESS != (tmp_ret = move_into_election_group_unlock_(eg_id))) {
          // self is leader move in group
          FORCE_ELECT_LOG(WARN, "move_into_election_group failed", K(tmp_ret), K_(partition), K(eg_id));
        } else {
          // do nothing
        }
      }
      ELECT_ASYNC_LOG(DEBUG,
          "leader elected",
          K_(partition),
          "first",
          leader_lease_.first,
          "second",
          leader_lease_.second,
          K_(vote_period),
          K_(lease_time));
    } else {
      role_ = ROLE_SLAVE;
      leader_lease_.second = real_ts_(logic_T1_timestamp) + (lease_time + T_SLAVE_LEASE_MORE);
    }
  }
  flush_trace_buf_();
  ignore_log_ = false;

  return ret;
}

int ObElection::leader_takeover_(const ObAddr& leader, const int64_t epoch, const int64_t logic_t1,
    const int64_t lease_time, const bool is_elected_by_changing_leader)
{
  int ret = OB_SUCCESS;

  if (!leader.is_valid() || logic_t1 <= 0 || lease_time <= 0 || (self_ == leader && epoch <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K_(self), K(leader), K(epoch), K(logic_t1), K(lease_time));
  } else if (OB_FAIL(leader_elected_(leader, logic_t1, lease_time, is_elected_by_changing_leader))) {
    // do nothing
  } else {
    if (leader == self_) {
      // leader_epoch updated on leader only
      if (max_leader_epoch_ever_seen_ != OB_INVALID_TIMESTAMP && epoch <= max_leader_epoch_ever_seen_) {
        leader_epoch_ = max_leader_epoch_ever_seen_ + 1;
      } else {
        leader_epoch_ = epoch;
      }
      max_leader_epoch_ever_seen_ = leader_epoch_;
    } else {
      // leader_epoch is invalid on follower
      leader_epoch_ = OB_INVALID_TIMESTAMP;
    }
    if (OB_ISNULL(election_cb_)) {
      ret = OB_ERR_UNEXPECTED;
      FORCE_ELECT_LOG(WARN, "election callback is null", K(ret), KP_(election_cb));
    } else if (OB_FAIL(election_cb_->on_election_role_change())) {
      FORCE_ELECT_LOG(WARN, "on_election_role_change failed", K(ret), K_(partition));
    } else {
      ELECT_ASYNC_LOG(TRACE, "on_election_role_change success", K(ret), K_(partition));
    }
  }

  return ret;
}

int ObElection::leader_reappoint_(const ObAddr& leader, const int64_t logic_t1, const int64_t lease_time)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();

  if (!leader.is_valid() || logic_t1 <= 0 || lease_time <= 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(leader), K(logic_t1), K(lease_time));
  } else if (ObElectionRole::ROLE_LEADER == role_ && OB_FAIL(verify_leader_valid_(cur_ts))) {
    FORCE_ELECT_LOG(WARN, "lease expired, give up reappointing", KR(ret), K(logic_t1), K(lease_time), K(*this));
  } else if (!is_real_leader_(current_leader_)) {
    ret = OB_ELECTION_WARN_INVALID_LEADER;
    FORCE_ELECT_LOG(WARN, "current leader is invalid", K(ret));
  } else if (OB_FAIL(leader_elected_(leader, logic_t1, lease_time, false))) {
    // do nothing
  } else {
    // do nothing
  }

  return ret;
}

int ObElection::verify_leader_valid_(const int64_t cur_ts) const
{
  int ret = OB_SUCCESS;

  if (!current_leader_.is_valid()) {
    ret = OB_ELECTION_WARN_INVALID_LEADER;
  } else if (leader_lease_is_expired_(cur_ts)) {
    ret = OB_ELECTION_WARN_LEADER_LEASE_EXPIRED;
  } else {
    // do nothing
  }

  return ret;
}

int ObElection::get_leader_(ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();

  if (OB_SUCC(verify_leader_valid_(cur_ts))) {
    leader = current_leader_;
  }

  return ret;
}

bool ObElection::leader_lease_is_expired_(const int64_t cur_ts) const
{
  bool bool_ret = false;
  if (!is_single_replica_tbl_()) {
    // non-ofs-single-zone-mode
    if (NULL != election_group_) {
      bool_ret = (cur_ts > cached_lease_end_);
      if (bool_ret) {
        // if cached lease expired, check eg real lease
        bool_ret =
            election_group_->part_leader_lease_is_expired(cur_ts, partition_, eg_part_array_idx_, cached_lease_end_);
      }
    } else {
      bool_ret = (cur_ts > leader_lease_.second);
    }
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

int ObElection::set_unconfirmed_leader_lease_(const int64_t start, const int64_t end)
{
  int ret = OB_SUCCESS;

  if (start <= 0 || end <= 0 || start >= end) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K_(partition), K(start), K(end));
  } else {
    unconfirmed_leader_lease_.first = start;
    unconfirmed_leader_lease_.second = end;
  }

  return ret;
}

bool ObElection::unconfirmed_leader_lease_is_expired_(const int64_t cur_ts)
{
  return cur_ts > unconfirmed_leader_lease_.second;
}

void ObElection::leader_revoke_()
{
  int tmp_ret = OB_SUCCESS;

  StateHelper state_helper(state_);
  if (OB_SUCCESS != (tmp_ret = state_helper.switch_state(Ops::LEADER_REVOKE))) {
    FORCE_ELECT_LOG(WARN, "switch state error", "ret", tmp_ret, "election", *this);
  } else if (current_leader_.is_valid()) {
    ELECT_ASYNC_LOG(INFO, "leader revoke", "election", *this);
    if (NULL != election_group_) {
      // move out when leader revoke
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = move_out_election_group_unlock_())) {
        FORCE_ELECT_LOG(ERROR, "move_out_election_group_unlock_ failed", K_(partition), K(tmp_ret));
      }
    }
    current_leader_.reset();
    valid_candidates_.reset();
    tmp_valid_candidates_.reset();
    leader_lease_.first = 0;
    leader_lease_.second = 0;
    cached_lease_end_ = 0;
    role_ = ROLE_SLAVE;
    // clear unfinished changing leader
    change_leader_clear_();
    is_elected_by_changing_leader_ = false;
    if (is_self_(current_leader_)) {
      EVENT_ADD(ELECTION_LEADER_REVOKE_COUNT, 1);
    }
    if (OB_ISNULL(election_cb_)) {
      FORCE_ELECT_LOG(WARN, "election callback is null", KP_(election_cb));
    } else if (OB_SUCCESS != (tmp_ret = election_cb_->on_election_role_change())) {
      FORCE_ELECT_LOG(WARN, "on_election_role_change failed", K(tmp_ret), K_(partition));
    } else {
      ELECT_ASYNC_LOG(TRACE, "on_election_role_change success", K(tmp_ret), K_(partition));
    }
  } else {
    // do nothing
  }
}

void ObElection::leader_revoke(const uint32_t revoke_type)
{
  int tmp_ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  if (!is_inited_) {
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited");
  } else if (!is_running_) {
    ELECT_ASYNC_LOG_(WARN, "election is not running", "election", *this);
  } else if (revoke_type >= REVOKE_TYPE_MAX) {
    tmp_ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid revoke_type", K(tmp_ret), K_(partition), K(revoke_type));
  } else {
    ELECT_ASYNC_LOG(
        INFO, "take the initiative leader revoke", K_(partition), "revoke reason", REVOKE_REASON_STR[revoke_type]);
    if (current_leader_ == self_) {
      ATOMIC_STORE(&last_leader_revoke_time_, ObTimeUtility::current_time());
      if (RevokeTypeChecker::is_unexpected_revoke_type(revoke_type)) {
        FORCE_ELECT_LOG(ERROR,
            "leader_revoke, please attention!",
            "revoke reason",
            REVOKE_REASON_STR[revoke_type],
            "election",
            *this);
      } else {
        ELECT_ASYNC_LOG(INFO,
            "leader_revoke, please attention!",
            "revoke reason",
            REVOKE_REASON_STR[revoke_type],
            "election",
            *this);
      }
    }
    leader_revoke_();
  }
}

void ObElection::set_stage_(const ObElectionStage stage)
{
  if (STAGE_UNKNOWN > stage || STAGE_COUNTING < stage) {
    FORCE_ELECT_LOG(WARN, "invalid argument", K_(partition), K(stage));
  } else {
    stage_ = stage;
  }
}

void ObElection::set_type_(const ObElectionMemberType type)
{
  if (MEMBER_TYPE_UNKNOWN > type || MEMBER_TYPE_CANDIDATE < type) {
    FORCE_ELECT_LOG(WARN, "invalid argument", K_(partition), K(type));
  } else {
    type_ = type;
  }
}

bool ObElection::lease_expired_too_long_(const int64_t cur_ts) const
{
  return (cur_ts - leader_revoke_timestamp_ > EXPIRED_TIME_FOR_WARNING);
}

void ObElection::set_unconfirmed_leader_(const ObAddr& unconfirmed_leader)
{
  unconfirmed_leader_ = unconfirmed_leader;
}

int ObElection::move_out_election_group(const ObElectionGroupId& eg_id)
{
  // called from election group
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  if (NULL == election_group_) {
    // already move out, return success
    ret = OB_SUCCESS;
  } else if (eg_id != eg_id_) {
    // eg_id not match, partition is in new group now, ignore
    ret = OB_STATE_NOT_MATCH;
    FORCE_ELECT_LOG(
        WARN, "eg_id not match when move out", K(ret), K(partition_), "arg eg_id", eg_id, "current eg_id", eg_id_);
  } else {
    ret = move_out_election_group_unlock_();
  }

  return ret;
}

int ObElection::move_out_election_group_unlock_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    FORCE_ELECT_LOG(WARN, "ObElection not inited", K(ret), K(partition_));
  } else if (NULL == election_group_) {
    ret = OB_STATE_NOT_MATCH;
    FORCE_ELECT_LOG(WARN, "election_group_ is already NULL", K(ret), K(partition_));
  } else {
    if (OB_FAIL(update_info_from_eg_())) {
      FORCE_ELECT_LOG(WARN, "update_info_from_eg_ failed", K(ret), K(partition_));
    } else if (OB_FAIL(election_group_->move_out_partition(partition_, eg_part_array_idx_))) {
      FORCE_ELECT_LOG(ERROR, "move_out_partition failed", K(ret), K(partition_), K(eg_part_array_idx_));
    } else if (OB_FAIL(eg_mgr_->revert_election_group(election_group_))) {
      FORCE_ELECT_LOG(WARN, "revert_election_group failed", K(ret), K(partition_));
    }
    move_out_timestamp_ = get_current_ts();
    election_group_ = NULL;
    eg_id_.reset();
    eg_part_array_idx_ = -1;
    cached_lease_end_ = 0;
    is_in_eg_ = false;
    if (is_running_) {
      // re-register gt1 task
      int tmp_ret = OB_SUCCESS;
      // register on real T1 time point
      int64_t next_expect_ts = T1_timestamp_ + T_ELECT2 + election_time_offset_;
      int64_t delay = 0;
      while (OB_SUCCESS != (tmp_ret = timer_.register_gt1_once(next_expect_ts, delay))) {
        ELECT_ASYNC_LIMIT_LOG(WARN, "fail to register gt1", K(tmp_ret), "election", *this, K(next_expect_ts), K(delay));
        if (OB_NOT_RUNNING == tmp_ret) {
          // not retry if time wheel has been stop
          break;
        }
        if (delay < 0) {
          T1_timestamp_ = next_expect_ts;
          next_expect_ts -= (delay - T_ELECT2 + 1) / T_ELECT2 * T_ELECT2;
        }
      }
    }
  }
  if (REACH_TIME_INTERVAL(1000 * 1000)) {
    ELECT_ASYNC_LOG(INFO, "finish move_out_from_election_group", K(ret), "election", *this);
  }

  return ret;
}

int ObElection::move_into_election_group(const ObElectionGroupId& eg_id)
{
  WLockGuard guard(lock_);
  return move_into_election_group_unlock_(eg_id);
}

int ObElection::move_into_election_group_unlock_(const ObElectionGroupId& eg_id)
{
  int ret = OB_SUCCESS;
  ObIElectionGroup* election_group = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    FORCE_ELECT_LOG(WARN, "ObElection not inited", K(ret), K(partition_));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election is not running", K(ret), "election", *this);
    }
  } else if (!eg_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(partition_));
  } else if (NULL != election_group_) {
    // return success if already belong to the election group
    // return error if not so
    if (eg_id != eg_id_) {
      ret = OB_STATE_NOT_MATCH;
      FORCE_ELECT_LOG(WARN, "already in other election_group", K(ret), K(partition_), K(eg_id), K(eg_id_));
    }
  } else if (!current_leader_.is_valid() || current_leader_ != eg_id.get_server() ||
             !is_real_leader_(current_leader_)) {
    // allow follower moive in only if leader is valid and match group's leader
    ret = OB_STATE_NOT_MATCH;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      FORCE_ELECT_LOG(WARN, "current_leader_ not match with eg_id", K(ret), K(eg_id), "election", *this);
    }
  } else if (!check_if_allowed_to_move_into_eg_()) {
    ret = OB_STATE_NOT_MATCH;
    FORCE_ELECT_LOG(WARN,
        "check_if_allowed_to_move_into_eg_ return false",
        K(ret),
        K(partition_),
        K(current_leader_),
        K(unconfirmed_leader_));
  } else if (NULL == (election_group = eg_mgr_->get_election_group(eg_id))) {
    ret = OB_ELECTION_GROUP_NOT_EXIST;
    FORCE_ELECT_LOG(WARN, "election_group not exist", K(ret), K(partition_), K(eg_id));
  } else {
    int64_t ret_idx = -1;
    int64_t converted_takeover_t1_timestamp_ = (takeover_t1_timestamp_ + election_time_offset_) / T_ELECT2 * T_ELECT2;
    if (OB_FAIL(election_group->move_in_partition(
            partition_, get_leader_lease(), converted_takeover_t1_timestamp_, ret_idx))) {
      FORCE_ELECT_LOG(WARN, "move_in_partition() failed", K(ret), K(partition_));
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = eg_mgr_->revert_election_group(election_group))) {
        FORCE_ELECT_LOG(WARN, "revert_election_group failed", K(ret), K(partition_));
      }
      election_group_ = NULL;
      eg_id_.reset();
      eg_part_array_idx_ = -1;
    } else {
      election_group_ = election_group;
      eg_id_ = eg_id;
      eg_part_array_idx_ = ret_idx;
      is_in_eg_ = true;
    }
  }
  if (REACH_TIME_INTERVAL(1000 * 1000)) {
    ELECT_ASYNC_LOG(INFO, "finish move_into_election_group", K(ret), K_(self), K(partition_), K(eg_id_));
  }

  return ret;
}

int ObElection::set_replica_num(const int64_t replica_num)
{
  int ret = OB_SUCCESS;
  ELECT_ASYNC_LOG(
      INFO, "begin set_replica_num", K_(partition), "curr_replica_num", replica_num_, "new_replica_num", replica_num);

  WLockGuard guard(lock_);
  if (!is_inited_) {
    FORCE_ELECT_LOG(WARN, "ObElection not inited");
    ret = OB_NOT_INIT;
  } else if (replica_num == replica_num_) {
    ELECT_ASYNC_LOG(INFO, "in set_replica_num, no need change", K(replica_num), "election", *this);
  } else if (replica_num > OB_MAX_MEMBER_NUMBER || replica_num < curr_candidates_.get_member_number()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(ERROR, "invalid argument, set_replica_num not allowed", K(replica_num), "election", *this);
  } else {
    replica_num_ = replica_num;
    ELECT_ASYNC_LOG(INFO, "set_replica_num success", K(replica_num), "election", *this);
  }

  return ret;
}

int ObElection::inc_replica_num()
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (OB_MAX_MEMBER_NUMBER <= replica_num_) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(ERROR, "increase election replica number error", K(ret), "election", *this);
  } else {
    ++replica_num_;
    ELECT_ASYNC_LOG(INFO, "increase election replica number success", "election", *this);
  }

  return ret;
}

int ObElection::dec_replica_num()
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (replica_num_ <= 1) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(ERROR, "decrease election replica number error", K(ret), "election", *this);
  } else {
    --replica_num_;
    ELECT_ASYNC_LOG(INFO, "decrease election replica number success", "election", *this);
  }

  return ret;
}

bool ObElection::is_network_error_()
{
  // requires caller to lock and check
  bool bool_ret = false;
  int64_t net_broken_cnt = 0;
  ObSEArray<ObAddr, 8> error_svr_list;
  int tmp_ret = OB_SUCCESS;
  const int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  for (int64_t i = 0; i < curr_candidates_.get_member_number(); ++i) {
    ObAddr cur_svr;
    if (OB_SUCCESS != (tmp_ret = curr_candidates_.get_server_by_index(i, cur_svr))) {
      FORCE_ELECT_LOG(WARN, "curr_candidates_.get_server_by_index failed", K(tmp_ret));
    } else if (cur_svr == self_) {
      // skip self
    } else if (share::ObServerBlacklist::get_instance().is_in_blacklist(share::ObCascadMember(cur_svr, cluster_id))) {
      net_broken_cnt++;
      if (OB_SUCCESS != (tmp_ret = error_svr_list.push_back(cur_svr))) {
        FORCE_ELECT_LOG(WARN, "error_svr_list.push_back failed", K(tmp_ret));
      }
    } else {
      // do nothing
    }
  }
  if (replica_num_ > 0 && net_broken_cnt >= (replica_num_ + 1) / 2) {
    bool_ret = true;
    FORCE_ELECT_LOG(WARN,
        "self network is broken with majority, attention!!!",
        K_(partition),
        K(net_broken_cnt),
        K_(replica_num),
        K(error_svr_list));
  }
  return bool_ret;
}

bool ObElection::is_clockdiff_error_()
{
  // requires caller to lock and check
  bool bool_ret = false;
  int64_t clockdiff_error_cnt = 0;
  ObSEArray<ObAddr, 8> error_svr_list;
  int tmp_ret = OB_SUCCESS;
  const int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  for (int64_t i = 0; i < curr_candidates_.get_member_number(); ++i) {
    ObAddr cur_svr;
    if (OB_SUCCESS != (tmp_ret = curr_candidates_.get_server_by_index(i, cur_svr))) {
      FORCE_ELECT_LOG(WARN, "curr_candidates_.get_server_by_index failed", K(tmp_ret));
    } else if (cur_svr == self_) {
      // skip self
    } else if (share::ObServerBlacklist::get_instance().is_clockdiff_error(
                   share::ObCascadMember(cur_svr, cluster_id))) {
      clockdiff_error_cnt++;
      if (OB_SUCCESS != (tmp_ret = error_svr_list.push_back(cur_svr))) {
        FORCE_ELECT_LOG(WARN, "error_svr_list.push_back failed", K(tmp_ret));
      }
    } else {
      // do nothing
    }
  }
  if (replica_num_ > 0 && clockdiff_error_cnt >= (replica_num_ + 1) / 2) {
    bool_ret = true;
    FORCE_ELECT_LOG(WARN,
        "self clock is skew with majority, attention!!!",
        K_(partition),
        K(clockdiff_error_cnt),
        K_(replica_num),
        K(error_svr_list));
  }
  return bool_ret;
}

bool ObElection::is_single_replica_tbl_() const
{
  return false;
}

bool ObElection::can_do_reappoint_() const
{
  bool bool_ret = true;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200) {
    if (GCTX.is_in_phy_fb_mode()) {
      // flashback mode not allow be leader
      bool_ret = false;
    } else {
      bool_ret = true;
    }
  }
  if (bool_ret) {
    bool_ret = !is_single_replica_tbl_();
  }
  return bool_ret;
}

bool ObElection::can_elect_new_leader_() const
{
  // check if cluster can elect leader or change leader
  bool bool_ret = true;
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200) {
    common::ObClusterType cluster_type = INVALID_CLUSTER_TYPE;
    share::ServerServiceStatus server_status = share::OBSERVER_INVALID_STATUS;
    GCTX.get_cluster_type_and_status(cluster_type, server_status);
    if (GCTX.is_in_phy_fb_mode()) {
      // flashback mode not allow elect leader
      bool_ret = false;
    } else if (common::PRIMARY_CLUSTER == cluster_type ||
               ObMultiClusterUtil::is_cluster_private_table(partition_.get_table_id())) {
      // main datebase or private table allow elect
      bool_ret = true;
    } else if (share::OBSERVER_INVALID_STATUS == server_status) {
      // server status unknown, non-private table not allow to elect
      bool_ret = false;
    } else if (share::OBSERVER_SWITCHING == server_status) {
      // server status is in switching, now allow to elect
      bool_ret = false;
    } else {
      // otherwise, allowed(leader or standby_leader)
      bool_ret = true;
    }
  }
  if (bool_ret) {
    bool_ret = !is_single_replica_tbl_();
  }
  return bool_ret;
}

void ObElection::run_gt1_task(const int64_t real_T1_timestamp)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool expired = false;
  int64_t t2 = 0;
  int64_t delay = 0;
  ObElectionPriority priority;

  WLockGuard guard(lock_);
  /**********should removed after a barrier version bigger than 3.1**********/
  if ((physical_condition_ == PhysicalCondition::SICK) &&
      (real_T1_timestamp - new_election_time_offset_ >= T1_timestamp_)) {
    physical_condition_ = PhysicalCondition::DEAD;
    election_time_offset_ = new_election_time_offset_;
    temp_election_time_offset_ = election_time_offset_;
    takeover_t1_timestamp_ = 0;
    ELECT_ASYNC_LOG(INFO, "be dead, parameters conversion is compeleted", "election", *this);
  } else {
  }
  /*********************************************************************************************/
  int64_t logic_T1_timestamp = logic_ts_(real_T1_timestamp);
  const int64_t cur_ts = get_current_ts();
  StateHelper state_helper(state_);
  ELECT_ASYNC_LOG(DEBUG, "GT1Task", "election", *this, K(real_T1_timestamp), K(cur_ts));
  if (real_T1_timestamp <= 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(real_T1_timestamp), "election", *this);
  } else if (run_time_out_of_range_(cur_ts, real_T1_timestamp)) {
    ret = OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE;
    ELECT_ASYNC_LIMIT_LOG(WARN,
        "run time out of range",
        K(ret),
        "election",
        *this,
        K(cur_ts),
        K(real_T1_timestamp),
        "delta",
        cur_ts - real_T1_timestamp);
  } else if (is_single_replica_tbl_()) {
    T1_timestamp_ = logic_T1_timestamp;
    if (!leader_lease_is_expired_(cur_ts)) {
      if (!current_leader_.is_valid()) {
        leader_epoch_ = T1_timestamp_;
      }
    } else {
      RevokeType revoke_type = LEASE_EXPIRED;
      ELECT_ASYNC_LOG_(WARN,
          "[ofs single zone mode]leader lease is expired",
          "revoke reason",
          REVOKE_REASON_STR[revoke_type],
          "election",
          *this);
    }
  } else if (is_in_eg_) {
    // timer task is stopped, if in eg
    if (NULL == election_group_ || !eg_id_.is_valid()) {
      // report error if election_group_ invalid, bug #21748396
      FORCE_ELECT_LOG(ERROR,
          "is_in_eg_ = true, but election_group is not valid",
          K(real_T1_timestamp),
          K(logic_T1_timestamp),
          "election",
          *this);
    } else {
      ELECT_ASYNC_LOG(INFO, "is_in_eg_ = true", K(real_T1_timestamp), K(logic_T1_timestamp), "election", *this);
    }
  } else {
    T1_timestamp_ = logic_T1_timestamp;
    expired = leader_lease_is_expired_(cur_ts);
    if (!expired) {
    } else {
      const ObAddr prev_leader = current_leader_;
      if (prev_leader.is_valid()) {
        is_need_query_ = true;
        leader_revoke_timestamp_ = cur_ts;
      }
      RevokeType revoke_type = LEASE_EXPIRED;
      leader_revoke_();

      if (prev_leader.is_valid()) {
        // record event only when prev_leader is valid
        event_history_array_->add_event_history_item(partition_, prev_leader, EventType::L_REVOKE_LEASE_EXPIRE);
      }

      // avoid printing ERROR log on starting
      if (cur_ts > start_timestamp_ + (vote_period_ + T_CYCLE_EXTEND_MORE) * T_ELECT2) {
        // only leader can print ERROR when lease expired
        if (prev_leader == self_) {
          if (!local_priority_.is_candidate()) {
            revoke_type = NOT_CANDIDATE;
          }
          bool is_exist = true;
          if (OB_SUCCESS !=
              (tmp_ret = storage::ObPartitionService::get_instance().check_partition_exist(partition_, is_exist))) {
            ELECT_ASYNC_LOG(WARN, "check_partition_exist failed", K_(partition), K(tmp_ret));
          }
          ATOMIC_STORE(&last_leader_revoke_time_, ObTimeUtility::current_time());
          if (is_exist) {
            FORCE_ELECT_LOG(ERROR,
                "leader_revoke, please attention!",
                "revoke reason",
                REVOKE_REASON_STR[revoke_type],
                "election",
                *this);
          } else {
            FORCE_ELECT_LOG(WARN,
                "leader_revoke, please attention!",
                "revoke reason",
                REVOKE_REASON_STR[revoke_type],
                "election",
                *this);
          }
        } else {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            ELECT_ASYNC_LOG_(WARN,
                "leader lease is expired",
                K(prev_leader),
                "revoke reason",
                REVOKE_REASON_STR[revoke_type],
                "election",
                *this);
          }
        }
      }
      need_print_trace_log_ = true;
      flush_trace_buf_();
      if (lease_expired_too_long_(cur_ts)) {
        ignore_log_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (expired) {
        if (0 == logic_T1_timestamp % T_CYCLE_V1) {  // decentralized voting
          if (!is_active_(cur_ts)) {
            ELECT_ASYNC_LOG(DEBUG, "election is not active", "election", *this);
          } else if (is_candidate_(self_)) {  // self is in memberlist, can elect
            if (OB_FAIL(state_helper.switch_state(Ops::D_ELECT))) {
              FORCE_ELECT_LOG(WARN, "switch state error", K(ret), "election", *this);
            } else {
              bool is_send_devote_prepare = true;
              if (is_force_leader_) {
                if (is_need_query_) {
                  FORCE_ELECT_LOG(WARN, "in query, cannot force leader", "election", *this);
                } else {
                  if (OB_FAIL(priority.init(true, INT64_MAX, UINT64_MAX, 0))) {
                    FORCE_ELECT_LOG(WARN, "priority init error", K(ret));
                  }
                }
                is_force_leader_ = false;
              } else {
                if (OB_FAIL(refresh_local_priority_())) {
                  FORCE_ELECT_LOG(WARN, "refresh_local_priority_ failed", K(ret), "election", *this);
                } else if (!local_priority_.is_candidate()) {
                  // self can't elct, need ask who is leader
                  is_need_query_ = true;
                  is_send_devote_prepare = false;
                  ELECT_ASYNC_LIMIT_LOG(
                      WARN, "priority candidate is false, need query leader", "election", *this, K_(local_priority));
                } else {
                  priority = local_priority_;
                }
                if (is_network_error_() || is_clockdiff_error_()) {
                  // network ot time error, give up electing
                  is_send_devote_prepare = false;
                  FORCE_ELECT_LOG(WARN,
                      "network/clockdiff error occur with majority candidates, not send devote_prepare",
                      "election",
                      *this);
                }
              }
              if (!can_elect_new_leader_()) {
                // not allowed to elect
                common::ObClusterType cluster_type = INVALID_CLUSTER_TYPE;
                share::ServerServiceStatus server_status = share::OBSERVER_INVALID_STATUS;
                GCTX.get_cluster_type_and_status(cluster_type, server_status);
                is_send_devote_prepare = false;
                ELECT_ASYNC_LOG(INFO,
                    "do not allow do leader_takeover, give up devote",
                    K_(partition),
                    K(cluster_type),
                    K(server_status));
              }
              if (OB_SUCC(ret) && is_send_devote_prepare) {
                ObElectionMsgDEPrepare msg(priority, T1_timestamp_, get_current_ts(), self_, lease_time_);
                if (OB_FAIL(local_handle_devote_prepare_(msg))) {
                  FORCE_ELECT_LOG(WARN, "local_handle_devote_prepare failed", K(ret), "election", *this, K(msg));
                } else if (OB_FAIL(send_devote_prepare_(msg))) {
                  FORCE_ELECT_LOG(WARN, "send devote prepare error", K(ret), "election", *this, K(msg));
                } else {
                }
              }
            }
          } else {  // self not in memberlist
            is_need_query_ = true;
          }
          // register gt2 whatever, could receive vote prepare if self not in memberlist
          t2 = T2_TIMESTAMP(real_T1_timestamp);
          if (OB_SUCCESS != (tmp_ret = timer_.register_gt2_once(t2, delay))) {
            ELECT_ASYNC_LIMIT_LOG(WARN, "fail to register gt2", K(tmp_ret), "election", *this, K(delay));
          }
        }
        // lease is not expired
      } else {  // centralized voting
        // reappoint could retry in last 4 period
        // only leader need this process
        ELECT_ASYNC_LOG(DEBUG,
            "in_reappoint_period",
            K_(partition),
            "diff",
            logic_T1_timestamp - takeover_t1_timestamp_,
            K_(vote_period),
            K_(lease_time));
        if (is_real_leader_(self_) &&
            (need_change_leader_ || is_changing_leader_ || in_reappoint_period_(logic_T1_timestamp))) {
          reappoint_count_++;
          if (!is_active_(cur_ts)) {
            ELECT_ASYNC_LOG(DEBUG, "election is not active", "election", *this);
          } else if (!is_candidate_(self_)) {
            FORCE_ELECT_LOG(WARN, "self is not candidate", "election", *this);
          } else if (OB_FAIL(refresh_local_priority_())) {
            FORCE_ELECT_LOG(WARN, "refresh_local_priority_ failed", K(ret), "election", *this);
          } else {
            if (need_change_leader_) {
              // if self is changing leader, give up reappoint(change_leader_timestamp_ is a real timestamp)
              if (is_changing_leader_ && (real_T1_timestamp - change_leader_timestamp_) >= T_ELECT2) {
                // change leader task executed, but failed
                change_leader_timestamp_ = (get_current_ts() + T_ELECT2) / T_ELECT2 * T_ELECT2;
                while (OB_SUCCESS != (tmp_ret = timer_.register_change_leader_once(change_leader_timestamp_, delay))) {
                  ELECT_ASYNC_LIMIT_LOG(WARN,
                      "fail to change leader task",
                      K(real_T1_timestamp),
                      K(tmp_ret),
                      "election",
                      *this,
                      K_(change_leader_timestamp),
                      K(delay));
                  if (OB_NOT_RUNNING == tmp_ret) {
                    break;
                  }
                  if (delay < 0) {
                    change_leader_timestamp_ += (T_ELECT2 - delay) / T_ELECT2 * T_ELECT2;
                  }
                }
                if (OB_SUCCESS == tmp_ret) {
                  ELECT_ASYNC_LOG(INFO,
                      "success to register change leader task",
                      "election",
                      *this,
                      K_(change_leader_timestamp),
                      K(delay));

                  ObTsWindows changing_leader_windows;
                  get_change_leader_window_(change_leader_timestamp_, changing_leader_windows);
                  if (OB_SUCCESS !=
                      (tmp_ret = election_cb_->on_change_leader_retry(proposal_leader_, changing_leader_windows))) {
                    FORCE_ELECT_LOG(WARN,
                        "change leader fail",
                        "election",
                        *this,
                        K_(change_leader_timestamp),
                        K(delay),
                        K(changing_leader_windows),
                        K_(proposal_leader));
                  } else {
                    ELECT_ASYNC_LOG(INFO,
                        "on change leader retry success when election retry",
                        "election",
                        *this,
                        K_(change_leader_timestamp),
                        K(delay),
                        K(changing_leader_windows),
                        K_(proposal_leader));
                  }
                }

                if (OB_SUCCESS != tmp_ret) {
                  ret = tmp_ret;
                }
              }
            } else {
              if (!can_do_reappoint_()) {
                // not allow to elect
                common::ObClusterType cluster_type = INVALID_CLUSTER_TYPE;
                share::ServerServiceStatus server_status = share::OBSERVER_INVALID_STATUS;
                GCTX.get_cluster_type_and_status(cluster_type, server_status);
                FORCE_ELECT_LOG(
                    WARN, "do not allow do reappointment", K_(partition), K(cluster_type), K(server_status));
              } else if (!local_priority_.is_candidate()) {
                ELECT_ASYNC_LIMIT_LOG(WARN,
                    "current leader is not candidate, give up reappointment",
                    K(ret),
                    "election",
                    *this,
                    K_(local_priority));
              } else if (OB_FAIL(state_helper.switch_state(Ops::V_ELECT))) {
                FORCE_ELECT_LOG(WARN, "switch state error", K(ret), "election", *this);
              } else {
                /**********should removed after a barrier version bigger than 3.1**********/
                // only leader be here
                if (physical_condition_ == PhysicalCondition::INFECTED) {
                  physical_condition_ = PhysicalCondition::SICK;
                  ELECT_ASYNC_LOG_(INFO, "be sick", "election", *this);
                } else {
                }
                /************************************************************************************************************************/
                ObElectionMsgPrepare msg(
                    current_leader_, current_leader_, T1_timestamp_, get_current_ts(), self_, lease_time_);
                if (OB_FAIL(local_handle_vote_prepare_(msg))) {
                  ELECT_ASYNC_LOG_(WARN, "local_handle_vote_prepare_ failed", K(ret), "election", *this, K(msg));
                } else if (OB_FAIL(send_vote_prepare_(msg))) {
                  ELECT_ASYNC_LOG_(WARN, "send vote prepare error", K(ret), "election", *this, K(msg));
                } else {
                }
              }
            }
          }
        }
        is_reappoint_period_ = false;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      FORCE_ELECT_LOG(WARN, "run gt1 task error", K(ret), "election", *this);
    }
    // query leader if needed
    if (is_need_query_ && OB_SUCCESS != (tmp_ret = send_query_leader_())) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        FORCE_ELECT_LOG(WARN, "send_query_leader_ failed", K(tmp_ret), "election", *this);
      }
    }
  }
  // register next timer task
  if (!is_in_eg_) {
    // register gt4. clear status
    if (need_change_leader_) {
      // not register gt4 if self is changing leader, avoid clear state
    } else {
      const int64_t t4 = T4_TIMESTAMP(real_T1_timestamp);
      if (OB_SUCCESS != (tmp_ret = timer_.register_gt4_once(t4, delay))) {
        ELECT_ASYNC_LIMIT_LOG(WARN, "fail to register gt4", K(tmp_ret), "election", *this, K(delay));
      }
    }
    // register gt1. if register error, we must retry otherwise election halt
    int64_t next_expect_ts = OB_INVALID_TIMESTAMP;
    /**********should removed after a barrier version bigger than 3.1**********/
    if (physical_condition_ == PhysicalCondition::SICK) {
      next_expect_ts = logic_T1_timestamp + T_ELECT2 + new_election_time_offset_;  // prevent from t1 going back
      ELECT_ASYNC_LOG_(INFO, "calculate new t1", K(next_expect_ts), "election", *this);
    } else {
      next_expect_ts = real_T1_timestamp + T_ELECT2;
    }
    /*********************************************************************************************/
    while (OB_SUCCESS != (tmp_ret = timer_.register_gt1_once(next_expect_ts, delay))) {
      ELECT_ASYNC_LIMIT_LOG(WARN, "fail to register gt1", K(tmp_ret), "election", *this, K(next_expect_ts), K(delay));
      if (OB_NOT_RUNNING == tmp_ret) {
        break;
      }
      if (delay < 0) {
        next_expect_ts -= (delay - T_ELECT2 + 1) / T_ELECT2 * T_ELECT2;
      }
    }
  }
  // msg_pool's record_T1 should not greater than election's T1, check this periodically
  msg_pool_.correct_recordT1_if_necessary(T1_timestamp_);
  /**********should removed after a barrier version bigger than 3.1**********/
  if (ATOMIC_LOAD(&IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1) == true && REACH_TIME_INTERVAL(1 * 1000 * 1000) &&
      GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3100) {
    ATOMIC_SET(&IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1, false);
    ELECT_ASYNC_LOG(INFO, "find cluster min version not less than 3.1 anymore", "election", *this);
  } else {
  }
  if (physical_condition_ == PhysicalCondition::HEALTHY &&
      ATOMIC_LOAD(&IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1) == false) {
    physical_condition_ = PhysicalCondition::INFECTED;
    ELECT_ASYNC_LOG(INFO, "be infected, virus from machine", "election", *this);
  } else {
  }
  /*********************************************************************************************/
}

int ObElection::send_devote_prepare_(const ObElectionMsgDEPrepare& msg)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else {
    if (OB_FAIL(post_election_msg_(curr_candidates_, partition_, msg))) {
      FORCE_ELECT_LOG(WARN, "send devote prepare message error", K(ret), K_(partition), K(msg));
    } else {
      ELECT_ASYNC_LOG(DEBUG, "send devote prepare message success", K_(partition), K_(curr_candidates), K(msg));
    }
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

// add lease time, make sure follower won't vote for others
int ObElection::send_vote_prepare_(const ObElectionMsgPrepare& msg)
{
  int ret = OB_SUCCESS;
  ObAddr new_leader;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else if (OB_FAIL(post_election_msg_(curr_candidates_, partition_, msg))) {
    FORCE_ELECT_LOG(WARN, "send vote prepare message error", K(ret), K_(partition), K(msg));
  } else {
    ELECT_ASYNC_LOG(DEBUG, "send vote prepare message success", "election", *this, K(msg));
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObElection::send_change_leader_vote_prepare_(const ObElectionMsgPrepare& msg)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition), K(msg));
  } else if (OB_FAIL(post_election_msg_(curr_candidates_, partition_, msg))) {
    FORCE_ELECT_LOG(WARN, "send vote prepare message error", K(ret), K(msg));
  } else {
    ELECT_ASYNC_LOG(DEBUG, "send_change_leader_vote_prepare success", K(*this), K(msg));
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObElection::send_query_leader_()
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();
  ObAddr candidate;
  int64_t mcount = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else if (current_leader_.is_valid()) {
    candidate = current_leader_;
  } else if (0 >= (mcount = curr_candidates_.get_member_number())) {
    ret = OB_STATE_NOT_MATCH;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "curr_candidates_ is empty, skip", K(ret), K(*this));
    }
  } else if (OB_FAIL(curr_candidates_.get_server_by_index(candidate_index_++ % mcount, candidate))) {
    // don't send query to self
  } else if (candidate == self_ && mcount > 1) {
    if (OB_FAIL(curr_candidates_.get_server_by_index(candidate_index_++ % mcount, candidate))) {
      // do nothing
    } else if (candidate == self_) {
      ret = OB_ERR_UNEXPECTED;
      FORCE_ELECT_LOG(WARN, "get candidate error", K(ret), K(candidate));
    } else {
      // do nothing
    }
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    ObElectionQueryLeader msg(cur_ts, self_);
    if (candidate == self_) {
      // skip self
    } else if (OB_FAIL(post_election_msg_(candidate, partition_, msg))) {
      FORCE_ELECT_LOG(WARN, "send query leader message error", K(ret), K(candidate), K(msg));
    } else {
      ELECT_ASYNC_LOG(DEBUG, "send query leader message success", K(candidate), "election", *this, K(msg));
    }
  }

  return ret;
}

bool ObElection::devote_run_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const
{
  return (cur_ts - expect_ts) > T_DEVOTE_TIMER_DIFF;
}

bool ObElection::vote_run_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const
{
  return (cur_ts - expect_ts) > T_VOTE_TIMER_DIFF;
}

bool ObElection::change_leader_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const
{
  return (cur_ts - expect_ts) > T_CHANGE_LEADER_DIFF;
}

bool ObElection::run_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const
{
  bool bool_ret = false;
  if (State::is_decentralized_state(state_)) {
    bool_ret = devote_run_time_out_of_range_(cur_ts, expect_ts);
  } else if (State::is_centralized_state(state_)) {
    if (is_changing_leader_) {
      bool_ret = change_leader_time_out_of_range_(cur_ts, expect_ts);
    } else {
      bool_ret = vote_run_time_out_of_range_(cur_ts, expect_ts);
    }
  } else {
    // do nothing
  }

  return bool_ret;
}

void ObElection::run_gt2_task(const int64_t real_T2_timestamp)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();
  int64_t delay = 0;

  WLockGuard guard(lock_);
  ELECT_ASYNC_LOG(DEBUG, "GT2Task", "election", *this, K(real_T2_timestamp), K(cur_ts));

  const int64_t t3 = T3_TIMESTAMP(real_ts_(T1_timestamp_));
  if (real_T2_timestamp <= 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(real_T2_timestamp), "election", *this);
  } else if (run_time_out_of_range_(cur_ts, real_T2_timestamp)) {
    ret = OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE;
    ELECT_ASYNC_LIMIT_LOG(WARN,
        "run time out of range",
        K(ret),
        "election",
        *this,
        K(cur_ts),
        K(real_T2_timestamp),
        "delta",
        cur_ts - real_T2_timestamp);
  } else {
    StateHelper state_helper(state_);
    if (OB_SUCC(state_helper.switch_state(Ops::D_VOTE))) {
      if (!leader_lease_is_expired_(cur_ts)) {
        ret = OB_ELECTION_WARN_PROTOCOL_ERROR;
        FORCE_ELECT_LOG(
            WARN, "in decentralized stat but leader lease is not expired", K(ret), "election", *this, K(cur_ts));
        // leader lease is expired
      } else {
        ObAddr server;
        ObElectionPriority priority;
        int64_t lease_time = 0;
        bool is_send_vote = true;

        if (OB_FAIL(get_decentralized_candidate_(server, priority, lease_time))) {
        } else if (server != self_) {
          if (OB_FAIL(refresh_local_priority_())) {
            FORCE_ELECT_LOG(WARN, "get priority error", K(ret), "election", *this);
          } else if (local_priority_.get_membership_version() > priority.get_membership_version()) {
            is_send_vote = false;
            FORCE_ELECT_LOG(WARN,
                "membership_version is bigger than candidate, not send vote",
                K(partition_),
                K_(local_priority),
                K(priority));
          } else {
            // do nothing
          }
        } else {
          // do nothing
        }
        // just voter and candidate can send devote message
        if (OB_SUCC(ret)) {
          if (is_send_vote && is_active_(cur_ts) &&
              (unconfirmed_leader_lease_is_expired_(cur_ts) || unconfirmed_leader_ == server)) {
            ObElectionMsgDEVote msg(server, T1_timestamp_, get_current_ts(), self_);
            if (self_ == server) {
              if (OB_FAIL(local_handle_devote_vote_(msg))) {
                ELECT_ASYNC_LOG_(WARN, "local_handle_devote_vote failed", K(ret), "election", *this, K(msg));
              }
            } else if (OB_FAIL(send_devote_vote_(server, msg))) {
              ELECT_ASYNC_LOG_(WARN, "send devote vote error", K(ret), "election", *this, K(server));
            } else {
              ELECT_ASYNC_LOG(DEBUG, "send devote vote success", "election", *this, K(server));
            }
            set_unconfirmed_leader_(server);
            set_unconfirmed_leader_lease_(
                real_ts_(T1_timestamp_), real_ts_(T1_timestamp_) + lease_time + T_SLAVE_LEASE_MORE);
          } else {
            FORCE_ELECT_LOG(
                WARN, "not send devote vote", K(ret), "election", *this, K(is_send_vote), K(server), K(cur_ts));
          }
        }
      }  // leader lease is expired
    } else {
      ret = OB_STATE_NOT_MATCH;
      FORCE_ELECT_LOG(WARN, "election state not match", K(ret), "election", *this);
    }
  }

  // decentralized voting, self in memberlist or,
  // centralized voting, self is leader
  // need register gt3
  if ((State::is_decentralized_state(state_) && is_candidate_(self_)) ||
      (State::is_centralized_state(state_) && is_real_leader_(self_))) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = timer_.register_gt3_once(t3, delay))) {
      ELECT_ASYNC_LIMIT_LOG(WARN, "fail to register gt3", K(tmp_ret), "election", *this, K(delay));
    }
  }
}

int ObElection::get_decentralized_candidate_(ObAddr& server, ObElectionPriority& priority, int64_t& lease_time)
{
  int ret = msg_pool_.get_decentralized_candidate(server, priority, replica_num_, T1_timestamp_, lease_time);
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  return ret;
}

int ObElection::get_centralized_candidate_(ObAddr& cur_leader, ObAddr& new_leader, int64_t msg_t1)
{
  int ret = msg_pool_.get_centralized_candidate(cur_leader, new_leader, msg_t1);
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  return ret;
}

int ObElection::send_devote_vote_(const ObAddr& server, const ObElectionMsgDEVote& msg)
{
  int ret = OB_SUCCESS;

  if (!server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K_(partition), K(server), K(msg));
  } else {
    if (OB_FAIL(post_election_msg_(server, partition_, msg))) {
      FORCE_ELECT_LOG(WARN, "send devote vote message error", K(ret), K_(partition), K(msg));
    } else {
      ELECT_ASYNC_LOG(DEBUG, "send devote vote message success", "election", *this, K(server), K(msg));
    }
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObElection::try_centralized_voting_(const int64_t lease_time, int64_t msg_t1)
{
  int ret = OB_SUCCESS;
  ObAddr msg_cur_leader;
  ObAddr msg_new_leader;
  const int64_t cur_ts = get_current_ts();
  set_stage_(STAGE_VOTING);

  if (OB_FAIL(get_centralized_candidate_(msg_cur_leader, msg_new_leader, msg_t1))) {
    ELECT_ASYNC_LOG_(WARN, "get_centralized_candidate error", K(ret), "election", *this);
  } else {
    ELECT_ASYNC_LOG(DEBUG,
        "get_centralized_candidate success",
        "election",
        *this,
        K(msg_cur_leader),
        K(msg_new_leader),
        K_(current_leader));
    if (current_leader_ != msg_cur_leader && !leader_lease_is_expired_(cur_ts)) {
      FORCE_ELECT_LOG(WARN,
          "lease available but cur_leader != msg_cur_leader",
          "election",
          *this,
          K_(current_leader),
          K(msg_cur_leader));
    } else if (OB_FAIL(refresh_local_priority_())) {
      FORCE_ELECT_LOG(WARN, "refresh_local_priority_ failed", K(ret), "election", *this);
      // just voter or candidate can send vote message
    } else if (is_active_(cur_ts) &&
               (unconfirmed_leader_lease_is_expired_(cur_ts) || unconfirmed_leader_ == msg_cur_leader ||
                   unconfirmed_leader_ == msg_new_leader)) {
      if (OB_SUCC(ret)) {
        ObElectionMsgVote msg(local_priority_, msg_cur_leader, msg_new_leader, msg_t1, get_current_ts(), self_);
        if (self_ == msg_cur_leader) {
          if (OB_FAIL(local_handle_vote_vote_(msg))) {
            ELECT_ASYNC_LOG_(WARN, "local_handle_vote_vote_ failed", K(ret), "election", *this, K(msg));
          }
        } else if (OB_FAIL(send_vote_vote_(msg_cur_leader, msg))) {
          ELECT_ASYNC_LOG_(
              WARN, "send vote vote error", K(ret), "election", *this, K(msg_cur_leader), K(msg_new_leader));
        }
        set_unconfirmed_leader_(msg_new_leader);
        /**********should removed after a barrier version bigger than 3.1**********/
        set_unconfirmed_leader_lease_(
            msg_t1 + temp_election_time_offset_, msg_t1 + temp_election_time_offset_ + lease_time + T_SLAVE_LEASE_MORE);
        /*********************************************************************************************/
        // set_unconfirmed_leader_lease_(msg_t1 + election_time_offset_, msg_t1 + election_time_offset_ + lease_time +
        // T_SLAVE_LEASE_MORE);
      }
    } else {
      FORCE_ELECT_LOG(WARN,
          "not send vote vote",
          K(cur_ts),
          "election",
          *this,
          K_(unconfirmed_leader),
          K(msg_cur_leader),
          K(msg_new_leader));
    }
  }

  return ret;
}

// receive new message
// 1.judge if there are enough messages
//   if self is in reappointing, need majority's message
//   if self is in changing leader, need majority's message and new leader's message
// 2.if there are enough messages, send vote success message
int ObElection::check_centralized_majority_()
{
  int ret = OB_SUCCESS;
  ObAddr cur_leader;
  ObAddr new_leader;
  ObElectionPriority new_leader_priority;

  if (OB_FAIL(msg_pool_.check_centralized_majority(
          cur_leader, new_leader, new_leader_priority, replica_num_, T1_timestamp_))) {
    // no enough messages or waiting for leader's message
    if (OB_ELECTION_WARN_NOT_REACH_MAJORITY == ret || OB_ELECTION_WAIT_LEADER_MESSAGE == ret) {
      // rewrite ret
      ret = OB_SUCCESS;
    } else {
      ELECT_ASYNC_LOG_(WARN, "check_centralized_majority error", K(ret));
    }
  } else {
    StateHelper state_helper(state_);
    if (state_ == State::V_SUCCESS) {
      // do nothing
    } else if (OB_FAIL(state_helper.switch_state(Ops::V_COUNT))) {
      FORCE_ELECT_LOG(WARN, "switch state error", K(ret), "election", *this);
    } else if (is_candidate_(self_) && is_real_leader_(self_) && cur_leader == current_leader_) {
      bool change_leader = false;
      if (cur_leader != new_leader) {  // change leader
        // need new leader's message and its prioeity
        if (!new_leader_priority.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          FORCE_ELECT_LOG(WARN, "change leader but new leader priority is invalid", K(ret), K(new_leader_priority));
        } else {
          // need new leader's ticket to compare new leader's priority and current leader's priority in changing leader
          // process no compare needed in reappointing process
          if (!new_leader_priority.is_candidate()) {
            FORCE_ELECT_LOG(
                WARN, "new leader is not candidate", "election", *this, K(new_leader), K(new_leader_priority));
          } else if (OB_FAIL(refresh_local_priority_())) {
            FORCE_ELECT_LOG(WARN, "get election priority error", K(ret), "election", *this);
          } else if (new_leader_priority.compare_without_logid(local_priority_) < 0) {
            FORCE_ELECT_LOG(WARN,
                "new leader priority small than current leader priority",
                K_(partition),
                K(new_leader_priority),
                K_(local_priority));
          } else {
            leader_revoke_();
            change_leader = true;
            ELECT_ASYNC_LOG(INFO, "change leader vote success, self leader revoke", "election", *this, K(new_leader));
            event_history_array_->add_event_history_item(partition_, self_, EventType::L_REVOKE_CHANGE);
          }
        }
      }
      if (cur_leader == new_leader || change_leader) {
        ObElectionMsgSuccess msg(
            cur_leader, new_leader, T1_timestamp_, get_current_ts(), self_, lease_time_, last_gts_, leader_epoch_);
        if (OB_FAIL(local_handle_vote_success_(msg))) {
          ELECT_ASYNC_LOG_(WARN, "local_handle_vote_success_ failed", K(ret), "election", *this, K(msg));
        } else if (OB_FAIL(send_vote_success_(msg))) {
          ELECT_ASYNC_LOG_(WARN, "send vote success error", K(ret), "election", *this, K(cur_leader), K(new_leader));
        } else {
          ELECT_ASYNC_LOG(DEBUG, "send vote success", "election", *this, K(cur_leader), K(new_leader));
        }
      } else {
        FORCE_ELECT_LOG(WARN, "not send vote success", K(ret), "election", *this, K(cur_leader), K(new_leader));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      FORCE_ELECT_LOG(WARN, "check centralized majority error", K(ret), "election", *this, K(cur_leader));
    }
  }

  return ret;
}

int ObElection::send_vote_vote_(const ObAddr& server, const ObElectionMsgVote& msg)
{
  int ret = OB_SUCCESS;

  if (!server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K_(partition), K(server), K(msg));
  } else if (OB_FAIL(post_election_msg_(server, partition_, msg))) {
    FORCE_ELECT_LOG(WARN, "send vote vote message error", K(ret), K_(partition), K(msg));
  } else {
    ELECT_ASYNC_LOG(DEBUG, "send vote vote message success", "election", *this, K(msg));
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObElection::notify_leader_(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  int64_t lease_time = 0;

  if (NULL != election_group_) {
    if (OB_FAIL(update_info_from_eg_())) {
      FORCE_ELECT_LOG(WARN, "update_info_from_eg_ failed", K(ret), K(partition_));
    } else {
    }
  } else {
  }

  lease_time = leader_lease_.second - takeover_t1_timestamp_;
  // peer will add LEASE_MORE in leader_elected_(), so here need reduce LEASE_MORE
  lease_time -= (current_leader_ == self_ ? T_LEADER_LEASE_MORE : T_SLAVE_LEASE_MORE);
  int64_t cur_ts = get_current_ts();
  if (lease_time < 0 || cur_ts >= leader_lease_.second) {
    ret = OB_LEASE_NOT_ENOUGH;
    FORCE_ELECT_LOG(WARN,
        "lease not valid, give up notify leader",
        K(ret),
        K(cur_ts),
        K(lease_time),
        K_(takeover_t1_timestamp),
        "election",
        *this);
  } else {
    ObElectionQueryLeaderResponse resp(
        get_current_ts(), self_, current_leader_, leader_epoch_, takeover_t1_timestamp_, lease_time);
    if (OB_SUCC(ret)) {
      if (!server.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(server));
      } else if (OB_FAIL(post_election_msg_(server, partition_, resp))) {
        FORCE_ELECT_LOG(WARN, "send query leader response error", K(ret), K(server), "msg", resp);
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

void ObElection::run_gt3_task(const int64_t real_T3_timestamp)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();
  int64_t ticket = 0;
  ObAddr cur_leader;
  ObAddr new_leader;
  ObElectionPriority new_leader_priority;

  WLockGuard guard(lock_);
  ELECT_ASYNC_LOG(DEBUG, "GT3Task", "election", *this, K(real_T3_timestamp), K(cur_ts));

  if (real_T3_timestamp <= 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(real_T3_timestamp), "election", *this);
  } else if (run_time_out_of_range_(cur_ts, real_T3_timestamp)) {
    ret = OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE;
    ELECT_ASYNC_LIMIT_LOG(WARN,
        "run time out of range",
        K(ret),
        "election",
        *this,
        K(cur_ts),
        K(real_T3_timestamp),
        "delta",
        cur_ts - real_T3_timestamp);
  } else {
    StateHelper state_helper(state_);
    if (OB_SUCC(state_helper.switch_state(Ops::D_COUNT))) {
      if (!leader_lease_is_expired_(cur_ts)) {
        FORCE_ELECT_LOG(WARN, "protocol error. lease not expired", "election", *this, K(cur_ts));
      } else if (OB_FAIL(check_decentralized_majority_(new_leader, ticket))) {
        ELECT_ASYNC_LOG_(
            WARN, "check_decentralized_majority error", K(ret), "election", *this, K(new_leader), K(ticket));
      } else if (!is_candidate_(new_leader)) {
        FORCE_ELECT_LOG(WARN, "devote leader is not candidate", "election", *this, K(new_leader));
      } else {
        ELECT_ASYNC_LOG(DEBUG, "check_decentralized_majority succ", "election", *this, K(new_leader), K(ticket));
        if (new_leader == self_ && is_candidate_(self_)) {
          ObElectionMsgDESuccess msg(new_leader, T1_timestamp_, get_current_ts(), self_, lease_time_);
          if (OB_FAIL(local_handle_devote_success_(msg))) {
            ELECT_ASYNC_LOG_(WARN, "local_handle_devote_success_ failed", K(ret), "election", *this, K(msg));
          } else if (OB_FAIL(send_devote_success_(msg))) {
            ELECT_ASYNC_LOG_(WARN, "send devote success error", K(ret), "election", *this, K(msg));
          } else {
          }
        }
      }
    } else {
      ret = OB_STATE_NOT_MATCH;
      FORCE_ELECT_LOG(WARN, "election state not match", K(ret), "election", *this);
    }
  }
}

int ObElection::check_decentralized_majority_(ObAddr& new_leader, int64_t& ticket)
{
  int ret = msg_pool_.check_decentralized_majority(new_leader, ticket, replica_num_, T1_timestamp_);
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  return ret;
}

// return OB_SUCCESS, avoid no leader
int ObElection::collect_valid_candidates_(const ObElectionMsgVote& msg)
{
  int tmp_ret = OB_SUCCESS;

  // no need to count valid candidate while changing leader, cause they are on old leader
  if (!is_changing_leader_) {
    if ((!msg.get_priority().is_candidate()) || (0 > msg.get_priority().compare_with_buffered_logid(local_priority_))) {
      if (msg.get_priority().get_locality() >= local_priority_.get_locality()) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          ELECT_ASYNC_LIMIT_LOG(INFO, "follower priority less than leader", K_(partition), K(msg), K_(local_priority));
        }
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = tmp_valid_candidates_.add_server(msg.get_sender())) && OB_ENTRY_EXIST != tmp_ret) {
        ELECT_ASYNC_LIMIT_LOG(
            WARN, "add server to candidates failed", "ret", tmp_ret, K_(partition), K(msg), K_(tmp_valid_candidates));
      }
      if (msg.get_sender() != self_ && msg.get_priority().compare_with_buffered_logid(local_priority_) > 0) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          ELECT_ASYNC_LIMIT_LOG(
              WARN, "follower priority larger than leader", K_(partition), K(msg), K_(local_priority));
        }
      }
    }
  }

  return OB_SUCCESS;
}

int ObElection::send_devote_success_(const ObElectionMsgDESuccess& msg)
{
  int ret = OB_SUCCESS;

  if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else if (OB_FAIL(post_election_msg_(curr_candidates_, partition_, msg))) {
    FORCE_ELECT_LOG(WARN, "send devote success message error", K(ret), K_(partition), K(msg));
  } else {
    ELECT_ASYNC_LOG(DEBUG, "send devote success message success", "election", *this, K(msg));
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObElection::send_vote_success_(const ObElectionMsgSuccess& msg)
{
  int ret = OB_SUCCESS;

  if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K_(partition), K(msg));
  } else if (OB_FAIL(post_election_msg_(curr_candidates_, partition_, msg))) {
    FORCE_ELECT_LOG(WARN, "send vote success message error", K(ret), K_(partition), K(msg));
  } else {
    ELECT_ASYNC_LOG(DEBUG, "send vote success message success", "election", *this, K(msg));
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

void ObElection::run_gt4_task(const int64_t real_T4_timestamp)
{
  int tmp_ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();
  WLockGuard guard(lock_);
  ELECT_ASYNC_LOG(DEBUG, "GT4Task", "election", *this, K(real_T4_timestamp), K(cur_ts));

  StateHelper state_helper(state_);
  if (real_T4_timestamp <= 0) {
    FORCE_ELECT_LOG(WARN, "invalid argument", K(real_T4_timestamp), "election", *this);
  } else if (run_time_out_of_range_(cur_ts, real_T4_timestamp)) {
    ELECT_ASYNC_LIMIT_LOG(WARN,
        "run time out of range",
        "election",
        *this,
        K(cur_ts),
        K(real_T4_timestamp),
        "delta",
        cur_ts - real_T4_timestamp);
  } else if (is_changing_leader_) {
    ELECT_ASYNC_LIMIT_LOG(
        INFO, "now is_changing_leader, skip run_gt4", "election", *this, K(cur_ts), K(real_T4_timestamp));
  } else if (OB_SUCCESS != (tmp_ret = state_helper.switch_state(Ops::RESET))) {
    FORCE_ELECT_LOG(WARN, "switch state error", "ret", tmp_ret, "election", *this);
  } else {
    if (!is_single_replica_tbl_() && is_real_leader_(self_) && is_reappoint_period_) {
      if (OB_SUCCESS != (tmp_ret = cal_valid_candidates_())) {
        FORCE_ELECT_LOG(WARN, "calculate valid candidate error", K(tmp_ret), "election", *this);
      }
    }
  }
}

// change leader task ts is integer multiple of T_ELECT2
void ObElection::run_change_leader_task(const int64_t real_T1_timestamp)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();
  // need push T1
  // 1. make sure leader_epoch pushed
  // 2. avoid gt1 run in changing leader, and pushed T1_timestamp further more
  const int64_t next_logic_T1_timestamp = ((real_T1_timestamp - election_time_offset_ - 1) / T_ELECT2 + 1) * T_ELECT2;

  WLockGuard guard(lock_);

  StateHelper state_helper(state_);
  is_changing_leader_ = true;
  if (next_logic_T1_timestamp > T1_timestamp_) {
    // change leader task need push T1 after version 2.1
    T1_timestamp_ = next_logic_T1_timestamp;
  }
  ELECT_ASYNC_LOG(INFO,
      "run ChangeLeader Task",
      K_(partition),
      K_(proposal_leader),
      K_(change_leader_timestamp),
      K_(T1_timestamp),
      K(real_T1_timestamp),
      K(cur_ts),
      "delta",
      cur_ts - real_T1_timestamp);

  if (real_T1_timestamp <= 0) {
    FORCE_ELECT_LOG(WARN, "invalid argument", K(real_T1_timestamp), K(*this));
  } else if (run_time_out_of_range_(cur_ts, real_T1_timestamp)) {
    ELECT_ASYNC_LIMIT_LOG(
        WARN, "run time out of range", K(*this), K(cur_ts), K(real_T1_timestamp), "delta", cur_ts - real_T1_timestamp);
  } else if (!is_real_leader_(self_)) {
    ELECT_ASYNC_LOG(DEBUG, "election is not real leader", K(*this));
  } else if (!is_active_(cur_ts)) {
    ELECT_ASYNC_LOG(DEBUG, "election is not active", K(*this));
  } else if (!is_candidate_(self_)) {
    FORCE_ELECT_LOG(WARN, "self is not candidate", K(*this));
  } else if (!proposal_leader_.is_valid()) {
    ret = OB_ELECTION_WARN_INVALID_SERVER;
    FORCE_ELECT_LOG(WARN, "new leader is invalid", K(ret), K_(partition));
  } else if (OB_FAIL(refresh_local_priority_())) {
    FORCE_ELECT_LOG(WARN, "current leader get priority error", K(ret), K(*this));
  } else if (OB_FAIL(state_helper.switch_state(Ops::CHANGE_LEADER))) {
    FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this));
  } else {
    ELECT_ASYNC_LOG(
        INFO, "need change leader", K_(partition), K_(curr_candidates), K_(current_leader), K_(proposal_leader));
    ObElectionMsgPrepare msg(current_leader_, proposal_leader_, T1_timestamp_, get_current_ts(), self_, lease_time_);
    if (OB_FAIL(local_handle_vote_prepare_(msg))) {
      ELECT_ASYNC_LOG_(WARN, "local_handle_vote_prepare_ failed", K(ret), K(*this), K(msg));
    } else if (OB_FAIL(send_change_leader_vote_prepare_(msg))) {
      FORCE_ELECT_LOG(WARN, "send change leader vote prepare error", K(ret), K(*this), K(msg));
    } else {
      ELECT_ASYNC_LOG(DEBUG, "send change leader vote prepare success", K(ret), K(*this));
    }
  }
}

int ObElection::post_election_msg_(
    const common::ObMemberList& mlist, const common::ObPartitionKey& partition, const ObElectionMsg& msg)
{
  int ret = common::OB_SUCCESS;
  common::ObAddr server;

  /**********should removed after a barrier version bigger than 3.1**********/
  insert_physical_condition_into_msg_(msg);
  /*************************************************************************************/
  if (mlist.get_member_number() <= 0 || !partition.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(mlist), K(partition), K(msg));
  } else {
    const int64_t dst_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    for (int64_t i = 0; i < mlist.get_member_number() && common::OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(mlist.get_server_by_index(i, server))) {
        FORCE_ELECT_LOG(WARN, "get member error", K(ret), "index", i);
      } else if (!server.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        FORCE_ELECT_LOG(WARN, "server is invalid", K(ret), K(server));
      } else if (self_ == server) {
        // no need send msg to self
      } else if (OB_FAIL(rpc_->post_election_msg(server, dst_cluster_id, partition, msg))) {
        // do nothing
      } else {
        ELECT_ASYNC_LOG(DEBUG, "send election message success", K(msg), K(server));
      }
    }
  }

  return ret;
}

int ObElection::post_election_msg_(
    const common::ObAddr& server, const common::ObPartitionKey& partition, const ObElectionMsg& msg)
{
  int ret = common::OB_SUCCESS;
  const int64_t dst_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;

  /**********should removed after a barrier version bigger than 3.1**********/
  insert_physical_condition_into_msg_(msg);
  /*************************************************************************************/
  if (!server.is_valid() || !partition.is_valid() || !msg.is_valid() || self_ == server) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K_(self), K(server), K(partition), K(msg));
  } else if (OB_FAIL(rpc_->post_election_msg(server, dst_cluster_id, partition, msg))) {
    // do nothing
  } else {
    ELECT_ASYNC_LOG(DEBUG, "send election message success", K(server), K(msg));
  }

  return ret;
}

void ObElection::flush_trace_buf_()
{
  need_print_trace_log_ = false;
  reappoint_count_ = 0;
}

// only called from leader
// make sure takeover_t1_timestamp_ updated after send vote_prepare
bool ObElection::in_reappoint_period_(const int64_t ts)
{
  bool bool_ret = true;

  if (is_real_leader_(self_)) {
    bool_ret = (ts - takeover_t1_timestamp_) >= (vote_period_ * T_ELECT2);
  } else {
    bool_ret = false;
  }

  return bool_ret;
}

void ObElection::get_change_leader_window_(const int64_t ts, ObTsWindows& windows) const
{
  const int64_t T_SWITCH_LEADER_RESERVED = ObServerConfig::get_instance().trx_force_kill_threshold;
  if (!GCONF.enable_smooth_leader_switch) {
    windows.set(ts - T_SWITCH_LEADER_RESERVED, ts, ts + T_SWITCH_LEADER_RESERVED / 2);
  } else {
    windows.set(ts - (T_SWITCH_LEADER_RESERVED * 3 / 2), ts, ts + 2 * T_SWITCH_LEADER_RESERVED);
  }
}

int ObElection::get_priority(ObElectionPriority& priority) const
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else {
    priority = local_priority_;
  }

  return ret;
}

int ObElection::get_timestamp(int64_t& gts, ObAddr& leader) const
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "ObElection not inited", K(ret), K_(partition));
  } else {
    int64_t tmp_gts = 0;
    int64_t cur_ts = ObClockGenerator::getRealClock();
    const int64_t last_gts = ATOMIC_LOAD(&last_gts_);
    if (cur_ts > last_gts) {
      if (ATOMIC_BCAS(&last_gts_, last_gts, cur_ts)) {
        tmp_gts = cur_ts;
      } else {
        tmp_gts = ATOMIC_AAF(&last_gts_, 1);
      }
    } else {
      tmp_gts = ATOMIC_AAF(&last_gts_, 1);
    }
    cur_ts = get_current_ts();
    if (leader_lease_is_expired_(cur_ts)) {
      leader.reset();
      ret = OB_NOT_MASTER;
    } else {
      leader = current_leader_;
      if (current_leader_ != self_) {
        ret = OB_NOT_MASTER;
      } else {
        gts = tmp_gts;
      }
    }
  }
  return ret;
}

int64_t ObElection::get_msg_real_T1_(const ObElectionVoteMsg& msg) const
{
  int64_t result_ts = OB_INVALID_TIMESTAMP;
  // is msg is about change leader, T1 is strictly integer multiples of T_ELECT2
  // otherwise, just reduce election_time_offset_
  if ((msg.get_msg_type() == ObElectionMsgType::OB_ELECTION_VOTE_PREPARE &&
          static_cast<const ObElectionMsgPrepare&>(msg).get_cur_leader() !=
              static_cast<const ObElectionMsgPrepare&>(msg).get_new_leader()) ||
      (msg.get_msg_type() == ObElectionMsgType::OB_ELECTION_VOTE_VOTE &&
          static_cast<const ObElectionMsgVote&>(msg).get_cur_leader() !=
              static_cast<const ObElectionMsgVote&>(msg).get_new_leader()) ||
      (msg.get_msg_type() == ObElectionMsgType::OB_ELECTION_VOTE_SUCCESS &&
          static_cast<const ObElectionMsgSuccess&>(msg).get_cur_leader() !=
              static_cast<const ObElectionMsgSuccess&>(msg).get_new_leader())) {
    result_ts = (msg.get_T1_timestamp() + election_time_offset_) / T_ELECT2 * T_ELECT2;
  } else {
    result_ts = real_ts_(msg.get_T1_timestamp());
  }
  return result_ts;
}

int64_t ObElection::real_ts_(int64_t logic_ts) const
{
  return logic_ts + election_time_offset_;
}

int64_t ObElection::logic_ts_(int64_t real_ts) const
{
  return real_ts - election_time_offset_;
}

/**********should removed after a barrier version bigger than 3.1**********/
void ObElection::insert_physical_condition_into_msg_(const ObElectionMsg& msg)
{
  if (OB_SYS_TENANT_ID != extract_tenant_id(partition_.get_table_id())) {
    int64_t send_timestamp = msg.get_send_timestamp();
    int64_t cond = 0;
    switch (physical_condition_) {
      case PhysicalCondition::HEALTHY:
        cond = 0;
        break;
      case PhysicalCondition::INFECTED:
        cond = 1;
        break;
      case PhysicalCondition::SICK:
        cond = 2;
        break;
      case PhysicalCondition::DEAD:
        cond = 3;
        break;
      default:
        break;
    }
    cond <<= 61;
    cond &= 0x6000000000000000;
    send_timestamp |= cond;
    const_cast<ObElectionMsg&>(msg).set_send_timestamp(send_timestamp);
  } else {
  }
}

PhysicalCondition ObElection::fetch_others_condition_(const ObElectionMsg& msg)
{
  int64_t send_timestamp = msg.get_send_timestamp();
  int64_t condition = send_timestamp & 0x6000000000000000;
  condition >>= 61;
  condition &= 0x0000000000000003;
  PhysicalCondition ret;
  switch (condition) {
    case 0:
      ret = PhysicalCondition::HEALTHY;
      break;
    case 1:
      ret = PhysicalCondition::INFECTED;
      break;
    case 2:
      ret = PhysicalCondition::SICK;
      break;
    case 3:
      ret = PhysicalCondition::DEAD;
      break;
    default:
      FORCE_ELECT_LOG(ERROR, "coding error", "election", *this);
      break;
  }
  send_timestamp &= 0x9fffffffffffffff;
  const_cast<ObElectionMsg&>(msg).set_send_timestamp(send_timestamp);
  return ret;
}

void ObElection::register_gt1_with_neccessary_retry_(int64_t next_t1_timestamp)
{
  int64_t delay = 0;
  int temp_ret = OB_SUCCESS;
  do {
    delay = 0;
    if (OB_SUCCESS != (temp_ret = timer_.register_gt1_once(next_t1_timestamp, delay))) {
      FORCE_ELECT_LOG(WARN, "register gt1 failed", K(temp_ret), K(next_t1_timestamp), "election", *this);
      next_t1_timestamp += T_ELECT2;
    } else {
    }
  } while (OB_INVALID_ARGUMENT == temp_ret && delay < 0);
}

bool ObElection::update_condition_(const ObElectionMsg& msg, const int64_t msg_t1)
{
  PhysicalCondition cond = fetch_others_condition_(msg);
  if (physical_condition_ < cond) {
    switch (cond) {
      case PhysicalCondition::DEAD:
        if (msg_t1 != OB_INVALID_TIMESTAMP) {
          physical_condition_ = PhysicalCondition::DEAD;
          election_time_offset_ = new_election_time_offset_;
          temp_election_time_offset_ = election_time_offset_;
          takeover_t1_timestamp_ = 0;
          ATOMIC_SET(&IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1, false);
          int64_t bigger_T1 = T1_timestamp_ > msg_t1 ? T1_timestamp_ : msg_t1;
          int64_t next_t1_timestamp = bigger_T1 + T_ELECT2 + new_election_time_offset_;
          register_gt1_with_neccessary_retry_(next_t1_timestamp);
          ELECT_ASYNC_LOG(INFO,
              "be dead, virus from msg",
              K_(new_election_time_offset),
              K(next_t1_timestamp),
              "election",
              *this,
              K(msg));
        } else {
        }
        break;
      case PhysicalCondition::SICK:
        if (msg_t1 != OB_INVALID_TIMESTAMP) {
          physical_condition_ = PhysicalCondition::SICK;
          int64_t bigger_T1 = T1_timestamp_ > msg_t1 ? T1_timestamp_ : msg_t1;
          int64_t next_t1_timestamp = bigger_T1 + T_ELECT2 + new_election_time_offset_;
          ATOMIC_SET(&IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1, false);
          ELECT_ASYNC_LOG(INFO, "be sick", K(next_t1_timestamp), "election", *this);
          register_gt1_with_neccessary_retry_(next_t1_timestamp);
        } else {
        }
        break;
      case PhysicalCondition::INFECTED:
        physical_condition_ = PhysicalCondition::INFECTED;
        ATOMIC_SET(&IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1, false);
        ELECT_ASYNC_LOG(INFO, "be infected, virus from msg", "election", *this);
        break;
      case PhysicalCondition::HEALTHY:
        break;
      default:
        break;
    }
  } else {
  }
  return (cond == PhysicalCondition::DEAD ? true : false);
}
/***************************************************************************************************************************************************/
}  // namespace election
}  // namespace oceanbase
