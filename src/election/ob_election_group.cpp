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

#include "ob_election_group.h"
#include <stdint.h>
#include "lib/net/ob_addr.h"
#include "common/ob_clock_generator.h"
#include "lib/list/ob_list.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/utility/utility.h"
#include "common/ob_member_list.h"
#include "ob_election.h"
#include "ob_election_info.h"
#include "ob_election_group_id.h"
#include "ob_election_base.h"
#include "ob_election_rpc.h"
#include "ob_election_msg_pool.h"
#include "ob_election_mgr.h"
#include "ob_election_timer.h"
#include "ob_election_time_def.h"
#include "ob_election_group_cache.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace obrpc;
using namespace common;

namespace election {

int ObElectionGroup::init(const ObElectionGroupId& eg_id, const ObAddr& self, const uint64_t tenant_id,
    const common::ObMemberList& sorted_member_list, const int64_t replica_num, ObIElectionRpc* rpc, ObTimeWheel* tw,
    ObIElectionMgr* election_mgr, ObElectionGroupCache* election_group_cache, ObIElectionGroupPriorityGetter* eg_cb,
    ObIElectionGroupMgr* eg_mgr)
{
  // init for leader election group
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    FORCE_ELECT_LOG(WARN, "election_group inited twice", K(ret), "election_group", *this);
  } else if (!eg_id.is_valid() || !self.is_valid() || 0 == tenant_id || !sorted_member_list.is_valid() ||
             replica_num <= 0 || NULL == rpc || NULL == tw || NULL == election_mgr || NULL == eg_cb || NULL == eg_mgr) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN,
        "invalid argument",
        K(ret),
        K(eg_id),
        K(self),
        K(tenant_id),
        K(sorted_member_list),
        K(replica_num),
        KP(election_mgr),
        KP(rpc),
        KP(tw),
        KP(eg_cb),
        KP(eg_mgr));
  } else if (self != eg_id.get_server()) {
    ret = OB_STATE_NOT_MATCH;
    FORCE_ELECT_LOG(WARN, "self is not match with eg_id's server, unexpect", K(ret), K(eg_id), K(self), K(tenant_id));
  } else {
    eg_id_ = eg_id;
    self_ = self;
    tenant_id_ = tenant_id;
    sorted_member_list_ = sorted_member_list;
    replica_num_ = replica_num;
    rpc_ = rpc;
    election_mgr_ = election_mgr;
    election_group_cache_ = election_group_cache;
    eg_cb_ = eg_cb;
    eg_mgr_ = eg_mgr;
    int64_t cur_ts = get_current_ts();
    // set initial lease with cur_ts
    if (OB_FAIL(leader_elected_(eg_id.get_server(), cur_ts, lease_time_))) {
      FORCE_ELECT_LOG(WARN, "leader_elected_ failed", K(ret), K(eg_id_));
    }
    tlog_.reset();
    REC_TRACE_EXT(tlog_, election_group_init, Y(eg_id));
    if (OB_FAIL(timer_.init(this, tw, rpc))) {
      FORCE_ELECT_LOG(WARN, "election_group timer inited error", K(ret), K(eg_id_));
    } else {
      active_timestamp_ = cur_ts;
      is_inited_ = true;
      ELECT_ASYNC_LOG(INFO, "election_group init success", K(ret), "election_group", *this);
    }
  }
  return ret;
}

int ObElectionGroup::init(const ObElectionGroupId& eg_id, const ObAddr& self, const uint64_t tenant_id,
    ObIElectionRpc* rpc, ObTimeWheel* tw, ObIElectionMgr* election_mgr, ObElectionGroupCache* election_group_cache,
    ObIElectionGroupPriorityGetter* eg_cb, ObIElectionGroupMgr* eg_mgr)
{
  // init for follower elction group
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    FORCE_ELECT_LOG(WARN, "election_group inited twice", K(ret), "election_group", *this);
  } else if (!eg_id.is_valid() || !self.is_valid() || 0 == tenant_id || NULL == rpc || NULL == tw ||
             NULL == election_mgr || NULL == eg_cb || NULL == eg_mgr) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN,
        "invalid argument",
        K(ret),
        K(eg_id),
        K(self),
        K(tenant_id),
        KP(election_mgr),
        KP(rpc),
        KP(tw),
        KP(eg_cb),
        KP(eg_mgr));
  } else if (self == eg_id.get_server()) {
    ret = OB_STATE_NOT_MATCH;
    FORCE_ELECT_LOG(WARN, "self is leader, unexpect", K(ret), K(eg_id), K(self), K(tenant_id));
  } else {
    eg_id_ = eg_id;
    self_ = self;
    tenant_id_ = tenant_id;
    // set member_list, replica_num invalid for follower election group
    sorted_member_list_.reset();
    replica_num_ = 0;
    rpc_ = rpc;
    election_mgr_ = election_mgr;
    election_group_cache_ = election_group_cache;
    eg_cb_ = eg_cb;
    eg_mgr_ = eg_mgr;
    int64_t cur_ts = get_current_ts();
    // set initial lease with cur_ts
    if (OB_FAIL(leader_elected_(eg_id.get_server(), cur_ts, lease_time_))) {
      FORCE_ELECT_LOG(WARN, "leader_elected_ failed", K(ret), K(eg_id_));
    }
    tlog_.reset();
    REC_TRACE_EXT(tlog_, election_group_init, Y(eg_id));
    if (OB_FAIL(timer_.init(this, tw, rpc))) {
      FORCE_ELECT_LOG(WARN, "election_group timer inited error", K(ret), K(eg_id_));
    } else {
      active_timestamp_ = cur_ts;
      is_inited_ = true;
      ELECT_ASYNC_LOG(INFO, "election_group init success", K(ret), "election_group", *this);
    }
  }
  return ret;
}

void ObElectionGroup::destroy()
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
            ELECT_ASYNC_LOG(INFO, "election_group stop success", K_(eg_id), K_(self));
            is_running_ = false;
          } else if (OB_EAGAIN == ret) {
            // need retry
          } else {
            FORCE_ELECT_LOG(ERROR, "election_group stop error", K(ret), K_(eg_id), K_(self));
          }
        }
        if (OB_SUCC(ret)) {
          timer_.destroy();
          is_inited_ = false;
          ELECT_ASYNC_LOG(INFO, "election_group destroyed success", K_(eg_id), K_(self));
        } else if (OB_EAGAIN == ret) {
          // retry
        } else {
          FORCE_ELECT_LOG(WARN, "election_group destroyed error", K(ret), K_(eg_id), K_(self));
        }
      }
    } while (0);

    if (OB_EAGAIN == ret) {
      usleep(SLEEP_US);
    }
  }
}

void ObElectionGroup::reset()
{
  is_inited_ = false;
  is_running_ = false;
  msg_pool_.reset();
  rpc_ = NULL;
  timer_.reset();
  election_mgr_ = NULL;
  election_group_cache_ = NULL;
  eg_cb_ = NULL;
  eg_mgr_ = NULL;
  eg_id_.reset();
  eg_version_ = 0;
  last_seen_max_eg_version_ = 0;
  is_all_part_merged_in_ = true;
  partition_array_.reset();
  part_state_array_.reset();
  self_.reset();
  tenant_id_ = 0;
  self_priority_.reset();
  curr_leader_.reset();
  sorted_member_list_.reset();
  replica_num_ = 0;
  takeover_t1_timestamp_ = 0;
  T1_timestamp_ = 0;
  leader_lease_.first = 0;
  leader_lease_.second = 0;
  ATOMIC_STORE(&role_, ROLE_UNKNOWN);
  state_ = ObElectionInfo::State::V_IDLE;  // state setted V_IDLE in initial
  start_timestamp_ = 0;
  active_timestamp_ = 0;
  unconfirmed_leader_.reset();
  unconfirmed_leader_lease_.first = 0;
  unconfirmed_leader_lease_.second = 0;
  vote_period_ = T_CENTRALIZED_VOTE_PERIOD_EG;
  lease_time_ = OB_ELECTION_130_LEASE_TIME;
  need_print_trace_log_ = false;
  ignore_log_ = false;
  ATOMIC_STORE(&pre_destroy_state_, false);
  first_gt4_ts_ = OB_INVALID_TIMESTAMP;
  need_update_array_buf_ = false;
}

int ObElectionGroup::start()
{
  WLockGuard guard(lock_);
  return try_start_();
}

int ObElectionGroup::try_start_()
{
  int ret = OB_SUCCESS;
  const int64_t SLEEP_US = 10 * 1000;
  bool need_retry = true;

  for (int64_t retry = 0; need_retry && OB_SUCC(ret); ++retry) {
    do {
      if (!is_inited_) {
        need_retry = false;
        ret = OB_NOT_INIT;
        FORCE_ELECT_LOG(WARN, "election_group not inited", K(ret), "election_group", *this);
      } else if (is_running_) {
        need_retry = false;
        ret = OB_ELECTION_WARN_IS_RUNNING;
        FORCE_ELECT_LOG(WARN, "election_group is already running", K(ret), "election_group", *this);
      } else {
        // start timestamp
        const int64_t cur_ts = get_current_ts();
        int64_t start = ((cur_ts + T_ELECT2 + RESERVED_TIME_US) / T_ELECT2) * T_ELECT2;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = timer_.start(start))) {
          FORCE_ELECT_LOG(ERROR, "election_group timer start error", K(tmp_ret), K(retry), K_(eg_id), K_(self));
        } else {
          is_running_ = true;
          need_retry = false;
          start_timestamp_ = cur_ts;
          ELECT_ASYNC_LOG(INFO, "election_group timer start success", K(start), K(cur_ts), "self", *this);
        }
      }
    } while (0);

    if (need_retry) {
      usleep(SLEEP_US);
    }
  }

  return ret;
}

int ObElectionGroup::stop()
{
  int ret = OB_EAGAIN;
  const int64_t SLEEP_US = 10 * 1000;

  while (OB_EAGAIN == ret) {
    do {
      ret = OB_SUCCESS;
      WLockGuard guard(lock_);
      if (!is_inited_) {
        ret = OB_NOT_INIT;
        FORCE_ELECT_LOG(WARN, "election_group not inited", K(ret), "election_group", *this);
      } else if (!is_running_) {
        // do nothing, return OB_SUCCESS
      } else if (OB_SUCCESS == (ret = try_stop_())) {
        is_running_ = false;
      } else if (OB_EAGAIN == ret) {
        // need retry
      } else {
        ret = OB_ERR_UNEXPECTED;
        FORCE_ELECT_LOG(ERROR, "election_group stop error", K(ret), K_(eg_id));
      }
    } while (0);

    if (OB_EAGAIN == ret) {
      usleep(SLEEP_US);
    }
  }

  if (OB_FAIL(ret)) {
    FORCE_ELECT_LOG(WARN, "election_group stop error", K(ret), K_(eg_id), K_(self));
  } else {
    ELECT_ASYNC_LOG(INFO, "election_group stop success", K_(eg_id), K_(self));
  }
  return ret;
}

int ObElectionGroup::try_stop_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(timer_.try_stop())) {
    FORCE_ELECT_LOG(WARN, "timer stop error", K(ret), "election_group", *this);
  } else {
    ELECT_ASYNC_LOG(INFO, "timer stop success", "election_group", *this);
  }

  return ret;
}

int ObElectionGroup::try_update_eg_version(const int64_t msg_eg_version, const ObPartitionArray& partition_array)
{
  // update eg_version
  int ret = OB_SUCCESS;
  if (ATOMIC_LOAD(&eg_version_) == msg_eg_version) {
    // version equal, skip
  } else {
    WLockGuard guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      FORCE_ELECT_LOG(WARN, "election_group not inited", K(ret), K_(eg_id));
    } else if (ROLE_SLAVE == role_) {
      if (eg_version_ != msg_eg_version && partition_array.count() == partition_array_.count() &&
          is_part_array_equal_(partition_array)) {
        ATOMIC_STORE(&eg_version_, msg_eg_version);
      }
    }
  }
  return ret;
}

bool ObElectionGroup::is_need_check_eg_version(const int64_t msg_eg_version) const
{
  return (ROLE_LEADER != ATOMIC_LOAD(&role_) && (ATOMIC_LOAD(&eg_version_) != msg_eg_version));
}

int ObElectionGroup::check_eg_version(const int64_t msg_eg_version, const ObPartitionArray& partition_array,
    ObPartitionArray& pending_move_out_array, ObPartitionArray& pending_move_in_array)
{
  // check version before follower group process messsages
  // is_running may be false for follower group
  int ret = OB_SUCCESS;

  if (ROLE_LEADER == ATOMIC_LOAD(&role_)) {
    // self is leader, no need check version
  } else if (ATOMIC_LOAD(&eg_version_) == msg_eg_version) {
    // version equal, return success
  } else if (msg_eg_version < ATOMIC_LOAD(&last_seen_max_eg_version_)) {
    // drop lower version message
    ret = OB_UNEXPECT_EG_VERSION;
    FORCE_ELECT_LOG(WARN,
        "receive smaller msg_eg_version, ignore",
        K(ret),
        K(msg_eg_version),
        K(last_seen_max_eg_version_),
        "election_group",
        *this);
  } else {
    WLockGuard guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      FORCE_ELECT_LOG(WARN, "election_group not inited", K(ret), K_(eg_id));
    } else if (ROLE_LEADER == role_) {
      // only eg-follower need check eg_version
    } else if (msg_eg_version < ATOMIC_LOAD(&last_seen_max_eg_version_)) {
      // drop lower version message
      ret = OB_UNEXPECT_EG_VERSION;
      FORCE_ELECT_LOG(WARN,
          "receive smaller msg_eg_version, ignore",
          K(ret),
          K(msg_eg_version),
          K(last_seen_max_eg_version_),
          "election_group",
          *this);
    } else if (eg_version_ == msg_eg_version) {
      // version match, return OB_SUCCESS
    } else {
      // push last_seen_max_eg_version_
      inc_update(&last_seen_max_eg_version_, msg_eg_version);
      if (OB_FAIL(gen_pending_move_part_array_(
              msg_eg_version, partition_array, pending_move_in_array, pending_move_out_array))) {
        FORCE_ELECT_LOG(WARN,
            "gen_pending_move_part_array_ failed",
            K(ret),
            K(msg_eg_version),
            "part cnt",
            partition_array.count(),
            K_(eg_id));
      }
    }
  }

  if (REACH_TIME_INTERVAL(1000 * 1000)) {
    ELECT_ASYNC_LOG(INFO, "check_eg_version finished", K(ret), K(msg_eg_version), "election_group", *this);
  }

  return ret;
}

bool ObElectionGroup::is_part_array_equal_(const ObPartitionArray& partition_array) const
{
  bool bool_ret = false;
  if (partition_array_.count() != partition_array.count()) {
    bool_ret = false;
  } else {
    bool item_not_hit = false;
    const int64_t array_cnt = partition_array.count();
    int64_t tmp_idx = -1;
    for (int64_t i = 0; !item_not_hit && i < array_cnt; ++i) {
      if (!is_pkey_in_array_(partition_array_.at(i), partition_array, tmp_idx)) {
        item_not_hit = true;
      }
    }
    bool_ret = (false == item_not_hit);
  }
  return bool_ret;
}

int ObElectionGroup::gen_pending_move_part_array_(const int64_t msg_eg_version, const ObPartitionArray& partition_array,
    ObPartitionArray& pending_move_in_array, ObPartitionArray& pending_move_out_array)
{
  // follower group calculate move in/out list according to leader group's message
  int ret = OB_SUCCESS;
  if (msg_eg_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(msg_eg_version), "array cnt", partition_array.count());
  } else if (msg_eg_version <= eg_version_) {
    // smaller or equal version(leader must reach here), skip
  } else {
    pending_move_in_array.reset();
    pending_move_out_array.reset();
    // calculate move out partitions
    const int64_t self_part_count = partition_array_.count();
    int64_t tmp_idx = -1;
    for (int64_t i = 0; i < self_part_count; ++i) {
      const ObPartitionKey& tmp_arg_pkey = partition_array_.at(i);
      if (!is_pkey_in_array_(tmp_arg_pkey, partition_array, tmp_idx)) {
        if (OB_FAIL(pending_move_out_array.push_back(tmp_arg_pkey))) {
          FORCE_ELECT_LOG(WARN, "array push_back failed", K(ret), K(tmp_arg_pkey));
        }
      }
    }
    // calculate move in partitions
    const int64_t arg_part_count = partition_array.count();
    for (int64_t i = 0; i < arg_part_count; ++i) {
      const ObPartitionKey& tmp_arg_pkey = partition_array.at(i);
      if (!is_partition_exist_unlock_(tmp_arg_pkey, tmp_idx)) {
        if (OB_FAIL(pending_move_in_array.push_back(tmp_arg_pkey))) {
          FORCE_ELECT_LOG(WARN, "array push_back failed", K(ret), K(tmp_arg_pkey));
        }
      }
    }
  }
  return ret;
}

int ObElectionGroup::batch_move_out_partition_(const ObPartitionArray& pending_move_out_array)
{
  // handle with move out partitions,
  // no lock protected, call interface of ObElection
  int ret = OB_SUCCESS;

  int64_t fail_cnt = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    FORCE_ELECT_LOG(WARN, "election_group not inited", K(ret));
  } else if (pending_move_out_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "pending_move_out_array is empty", K(ret));
  } else if (is_empty()) {
    // group is empty, return OB_SUCCESS
  } else {
    int tmp_ret = OB_SUCCESS;
    ObPartitionKey tmp_pkey;
    int64_t array_count = pending_move_out_array.count();
    for (int64_t i = 0; i < array_count; ++i) {
      tmp_pkey.reset();
      tmp_pkey = pending_move_out_array.at(i);
      if (!is_pkey_exist_(tmp_pkey)) {
        // check if tmp_pkey is still in group(with lock protected), skip if not in group anymore
      } else if (OB_SUCCESS != (tmp_ret = election_mgr_->move_out_election_group(tmp_pkey, eg_id_))) {
        fail_cnt++;
        ELECT_ASYNC_LIMIT_LOG(WARN, "move_out_election_group failed", K(tmp_ret), K(tmp_pkey));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (fail_cnt > 0) {
      ret = OB_PARTIAL_FAILED;
    } else if (is_empty()) {
      // set is_all_part_merged_in_ to true if group is empty
      is_all_part_merged_in_ = true;
    }
  }
  return ret;
}

int ObElectionGroup::push_lease_to_part_stat_()
{
  ObPartitionKey tmp_pkey;
  return push_lease_to_part_skip_pkey_(tmp_pkey);
}

int ObElectionGroup::push_lease_to_part_skip_pkey_(const ObPartitionKey& skip_pkey)
{
  // push group's lease to every partition's state, except skip_pkey
  int ret = OB_SUCCESS;

  if (partition_array_.count() != part_state_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(ERROR, "two array count not match", K(ret), "election_group", *this);
  } else {
    int64_t part_cnt = part_state_array_.count();
    for (int64_t i = 0; i < part_cnt; ++i) {
      if (skip_pkey.is_valid() && partition_array_[i] == skip_pkey) {
        // skip
      } else if (leader_lease_.second > part_state_array_.at(i).lease_end_) {
        part_state_array_.at(i).takeover_t1_timestamp_ = takeover_t1_timestamp_;
        part_state_array_.at(i).lease_end_ = leader_lease_.second;
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int ObElectionGroup::move_in_partition(
    const ObPartitionKey& pkey, const lease_t part_lease, const int64_t takeover_t1_timestamp, int64_t& ret_idx)
{
  // move in partition, called by ObElection
  // do:add partitions;update group's lease_end.
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    FORCE_ELECT_LOG(WARN, "election_group not inited", K(ret), K(eg_id_));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    FORCE_ELECT_LOG(WARN, "election_group not running", K(ret), K(eg_id_));
  } else if (!pkey.is_valid() || part_lease.first == 0 || part_lease.second == 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN,
        "invalid argument",
        K(ret),
        K(pkey),
        K(eg_id_),
        "lease.start",
        part_lease.first,
        "end",
        part_lease.second);
  } else if (is_pre_destroy_state()) {
    // not permitted if group is pre-destory
    ret = OB_STATE_NOT_MATCH;
    FORCE_ELECT_LOG(WARN, "in pre_destroy_state_, cannot move in", K(ret), K(eg_id_), K_(pre_destroy_state));
  } else if (is_full_()) {
    ret = OB_EAGAIN;
    FORCE_ELECT_LOG(WARN, "eg already full", K(ret), K(eg_id_));
  } else if (is_partition_exist_unlock_(pkey, ret_idx)) {
    ret = OB_ENTRY_EXIST;
    FORCE_ELECT_LOG(WARN, "partition already in election_group", K(ret), K(eg_id_));
  } else if (partition_array_.count() != part_state_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(ERROR, "two array count not match", K(ret), "election_group", *this);
  } else {
    PartState part_stat(part_lease.second, takeover_t1_timestamp);
    if (OB_FAIL(partition_array_.push_back(pkey))) {
      FORCE_ELECT_LOG(WARN, "array push_back failed", K(ret), K(pkey), K(partition_array_));
    } else if (OB_FAIL(part_state_array_.push_back(part_stat))) {
      FORCE_ELECT_LOG(WARN, "array push_back failed", K(ret), K(pkey), K(part_stat), K(part_state_array_));
      if (partition_array_.count() > part_state_array_.count()) {
        partition_array_.pop_back();
      }
    } else {
      ATOMIC_STORE(&need_update_array_buf_, true);
      ret_idx = partition_array_.count() - 1;
      if (curr_leader_ == self_) {
        // need update version if leader goup move in partition
        (void)ATOMIC_FAA(&eg_version_, 1);
      }
      // set is_all_part_merged_in_ to false
      if (is_all_part_merged_in_) {
        // if is_all_part_merged_in_ was true, need push lease to every partition's state
        push_lease_to_part_skip_pkey_(pkey);
      }
      is_all_part_merged_in_ = false;
    }
  }

  if (REACH_TIME_INTERVAL(1000 * 1000)) {
    ELECT_ASYNC_LOG(INFO, "finish move_in_partition", K(ret), K(pkey), "election_group", *this);
  }
  return ret;
}

int ObElectionGroup::move_out_partition(const ObPartitionKey& pkey, const int64_t target_idx)
{
  // move out partition, called by ObElection
  int ret = OB_SUCCESS;
  bool was_full = false;
  ObElectionRole role = ObElectionRole::ROLE_UNKNOWN;
  ObElectionGroupKey key_outside;

  do {
    WLockGuard guard(lock_);
    if (is_full_()) {
      was_full = true;
      ObElectionGroupKey key_inside(tenant_id_, curr_leader_, replica_num_, sorted_member_list_);
      key_outside = key_inside;
      role = role_;
    } else {
    }
    bool is_part_exist = true;
    int64_t real_idx = -1;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      FORCE_ELECT_LOG(WARN, "election_group not inited", K(ret), K(eg_id_));
    } else if (!pkey.is_valid() || target_idx < 0) {
      ret = OB_INVALID_ARGUMENT;
      FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(pkey), K(target_idx), K(eg_id_));
    } else if (partition_array_.count() != part_state_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      FORCE_ELECT_LOG(ERROR, "two array count not match", K(ret), "election_group", *this);
    } else if (target_idx >= partition_array_.count() ||
               (target_idx < partition_array_.count() && partition_array_[target_idx] != pkey)) {
      is_part_exist = is_partition_exist_unlock_(pkey, real_idx);
    } else {
      real_idx = target_idx;
      is_part_exist = true;
    }

    if (OB_SUCC(ret)) {
      if (!is_part_exist) {
        ret = OB_ENTRY_NOT_EXIST;
        FORCE_ELECT_LOG(ERROR, "partition not exist in election_group", K(ret), K(pkey), K(eg_id_));
      } else if (OB_FAIL(partition_array_.remove(real_idx))) {
        FORCE_ELECT_LOG(WARN, "remove partition failed", K(ret), K(pkey), K(eg_id_));
      } else if (OB_FAIL(part_state_array_.remove(real_idx))) {
        FORCE_ELECT_LOG(ERROR, "remove part_stat failed", K(ret), K(pkey), K(eg_id_));
      } else {
        ATOMIC_STORE(&need_update_array_buf_, true);
        if (curr_leader_ == self_) {
          // need update version_ after leader group move out election
          (void)ATOMIC_FAA(&eg_version_, 1);
          if (is_empty_()) {
            // set is_all_part_merged_in_ to true if group is empty
            is_all_part_merged_in_ = true;
            // group is empty, leader set pre-destory state and notify follower
            ATOMIC_STORE(&pre_destroy_state_, true);
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = notify_follower_destroy_())) {
              FORCE_ELECT_LOG(WARN, "notify_follower_destroy_ failed", K(tmp_ret), K(eg_id_));
            } else {
              ELECT_ASYNC_LOG(INFO, "notify_follower_destroy_ success", K(tmp_ret), K(eg_id_), K(pre_destroy_state_));
            }
          }
        } else {
          // every time follower group move out partition, set eg_version to 0(indicate it's invalid)
          ATOMIC_STORE(&eg_version_, 0);
        }
      }
    }
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      ELECT_ASYNC_LOG(INFO, "finish move_out_partition", K(ret), K(pkey), "self", *this);
    }
  } while (0);

  // need add this group's eg_id to cache if this group not full
  if (ObElectionRole::ROLE_LEADER == role && OB_SUCCESS == ret && was_full && nullptr != election_group_cache_) {
    if (OB_FAIL(election_group_cache_->put_eg_id(key_outside, eg_id_))) {
      FORCE_ELECT_LOG(ERROR, "cache eg id failed", K(eg_id_), K(ret));
    } else {
      // do noting
    }
  }

  return ret;
}

bool ObElectionGroup::is_full() const
{
  RLockGuard guard(lock_);
  return is_full_();
}

bool ObElectionGroup::is_full_() const
{
  return (partition_array_.count() >= MAX_EG_PARTITION_NUM);
}

bool ObElectionGroup::is_empty() const
{
  RLockGuard guard(lock_);
  return is_empty_();
}

bool ObElectionGroup::is_empty_() const
{
  return (partition_array_.count() == 0);
}

bool ObElectionGroup::is_pkey_exist_(const ObPartitionKey& pkey) const
{
  // read lock added here
  int64_t dummy_idx = -1;
  RLockGuard guard(lock_);
  return is_partition_exist_unlock_(pkey, dummy_idx);
}

bool ObElectionGroup::is_partition_exist_unlock_(const ObPartitionKey& pkey, int64_t& idx) const
{
  return is_pkey_in_array_(pkey, partition_array_, idx);
}

bool ObElectionGroup::is_pkey_in_array_(
    const ObPartitionKey& pkey, const ObPartitionArray& partition_array, int64_t& idx) const
{
  bool bool_ret = false;
  idx = -1;
  int64_t count = partition_array.count();
  for (int64_t i = 0; !bool_ret && i < count; ++i) {
    if (pkey == partition_array.at(i)) {
      idx = i;
      bool_ret = true;
    }
  }
  return bool_ret;
}

int64_t ObElectionGroup::get_current_ts() const
{
  return ObTimeUtility::current_time();
}

int ObElectionGroup::process_vote_prepare_(const ObElectionMsgEGPrepare& msg, const int64_t msg_eg_version)
{
  // caller need check and add read lock
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "election_group not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election_group is not running", K(ret), "self", *this, K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), "self", *this, K(msg));
  } else if (msg_eg_version < eg_version_ && ROLE_SLAVE == role_) {
    ret = OB_UNEXPECT_EG_VERSION;
    ELECT_ASYNC_LOG_(WARN, "receive smaller eg_version", K(ret), K(msg), K(msg_eg_version), K(eg_version_));
  } else {
    const int64_t cur_ts = get_current_ts();
    const int64_t msg_t1 = msg.get_T1_timestamp();
    const int64_t lease_time = msg.get_lease_time();
    const int64_t now_lease_time = (lease_time == 0) ? (OB_ELECTION_130_LEASE_TIME) : lease_time;
    ObElectionInfo::StateHelper state_helper(state_);

    if (!IN_T0_RANGE(cur_ts, msg_t1)) {
      ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
      ELECT_ASYNC_LOG_(WARN,
          "message not in time",
          K(ret),
          "self",
          *this,
          K(msg),
          K(cur_ts),
          "msg fly cost",
          cur_ts - msg.get_send_timestamp(),
          "delta over msg_t1",
          cur_ts - msg_t1);
    } else if (OB_FAIL(state_helper.switch_state(ObElectionInfo::Ops::V_PREPARE))) {
      FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this), K(msg));
    } else {
      // do nothing
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(msg_pool_.store(msg))) {
        ELECT_ASYNC_LOG_(WARN, "store prepare message error", K(ret), "election_group", *this, K(msg));
      } else {
        T1_timestamp_ = msg_t1;
        if (OB_FAIL(try_centralized_voting_(now_lease_time))) {
          ELECT_ASYNC_LOG_(WARN, "try centralized voting error", K(ret), "self", *this, K(msg), K(now_lease_time));
        } else {
          if (msg_eg_version != eg_version_ && ROLE_SLAVE == role_) {
            ELECT_ASYNC_LOG_(
                INFO, "eg_version not equal with leader when send_vote_vote", K(msg), K(msg_eg_version), "self", *this);
          }
        }
        REC_TRACE_EXT(tlog_, try_centralized_voting, Y(ret), Y_(vote_period), Y(lease_time));
      }
    }
  }

  ELECT_ASYNC_LOG(DEBUG, "process_vote_prepare finished", K(ret), K(*this), K(msg));

  return ret;
}

int ObElectionGroup::local_handle_vote_prepare_(const ObElectionMsgEGPrepare& msg)
{
  // caller need add read lock
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "election_group not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election_group is not running", K(ret), "self", *this, K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), "self", *this, K(msg));
  } else if (ROLE_SLAVE == role_) {
    ret = OB_UNEXPECT_EG_VERSION;
    ELECT_ASYNC_LOG_(WARN, "receive smaller eg_version", K(ret), K(msg), K(eg_version_));
  } else if (OB_FAIL(process_vote_prepare_(msg, eg_version_))) {
    ELECT_ASYNC_LOG_(WARN, "process_vote_prepare failed", K(ret), K(*this), K(msg), K(eg_version_));
  } else {
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  REC_TRACE_EXT(tlog_, handle_vote_prepare, Y(ret), Y_(state), Y(msg));
  ELECT_ASYNC_LOG(DEBUG, "finish local_handle_vote_prepare", K(ret), K(*this), K(msg));

  return ret;
}

int ObElectionGroup::handle_vote_prepare(
    const ObElectionMsgEGPrepare& msg, const int64_t msg_eg_version, obrpc::ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "election_group not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election_group is not running", K(ret), "self", *this, K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), "self", *this, K(msg));
  } else if (msg_eg_version < eg_version_ && ROLE_SLAVE == role_) {
    ret = OB_UNEXPECT_EG_VERSION;
    ELECT_ASYNC_LOG_(WARN, "receive smaller eg_version", K(ret), K(msg), K(msg_eg_version), K(eg_version_));
  } else if (OB_FAIL(process_vote_prepare_(msg, msg_eg_version))) {
    ELECT_ASYNC_LOG_(WARN, "process_vote_prepare_ failed", K(ret), K(*this), K(msg), K(msg_eg_version), K(eg_version_));
  } else {
  }

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  REC_TRACE_EXT(tlog_, handle_vote_prepare, Y(ret), Y_(state), Y(msg));
  ELECT_ASYNC_LOG(DEBUG, "finish handle_vote_prepare", K(ret), K(*this), K(msg));

  return ret;
}

int ObElectionGroup::try_centralized_voting_(const int64_t lease_time)
{
  int ret = OB_SUCCESS;
  ObAddr msg_cur_leader;
  ObAddr msg_new_leader;
  const int64_t cur_ts = get_current_ts();

  if (OB_FAIL(get_eg_centralized_candidate_(msg_cur_leader, msg_new_leader))) {
    ELECT_ASYNC_LOG_(WARN, "get_eg_centralized_candidate error", K(ret), "self", *this);
  } else {
    bool unused_bool = false;
    if (curr_leader_ != msg_cur_leader && !eg_lease_is_completely_expired_(cur_ts, unused_bool)) {
      FORCE_ELECT_LOG(
          WARN, "lease available but cur_leader != msg_cur_leader", "self", *this, K_(curr_leader), K(msg_cur_leader));
    } else if (OB_FAIL(get_priority_(self_priority_))) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        FORCE_ELECT_LOG(WARN, "get priority failed", K(ret), "self", *this);
      }
    } else if (is_active_(cur_ts) &&
               (unconfirmed_leader_lease_is_expired_(cur_ts) || unconfirmed_leader_ == msg_cur_leader ||
                   unconfirmed_leader_ == msg_new_leader)) {
      ObElectionMsgEGVote msg(self_priority_, msg_cur_leader, msg_new_leader, T1_timestamp_, get_current_ts(), self_);
      if (self_ == msg_cur_leader) {
        // if voter is myself, process it directly
        if (OB_FAIL(local_handle_vote_vote_(msg))) {
          ELECT_ASYNC_LOG_(WARN, "local_handle_vote_vote failed", K(ret), K(*this), K(msg));
        }
      } else if (OB_FAIL(send_vote_vote_(msg))) {
        ELECT_ASYNC_LOG_(WARN, "send vote vote error", K(ret), K(*this), K(msg));
      } else {
      }
      set_unconfirmed_leader_(msg_new_leader);
      set_unconfirmed_leader_lease_(T1_timestamp_, T1_timestamp_ + lease_time + T_SLAVE_LEASE_MORE);
    } else {
      FORCE_ELECT_LOG(WARN,
          "not send vote vote",
          K(cur_ts),
          "self",
          *this,
          K_(unconfirmed_leader),
          K(msg_cur_leader),
          K(msg_new_leader));
    }
  }

  return ret;
}

int ObElectionGroup::local_handle_vote_vote_(const ObElectionMsgEGVote& msg)
{
  // caller need add read lock
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "election_group not inited", K(ret), K(*this));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election_group is not running", K(ret), K(*this), K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_ELECTION_WARN_INVALID_MESSAGE;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), "self", *this, K(msg));
  } else if (ROLE_SLAVE == role_) {
    ret = OB_NOT_MASTER;
    ELECT_ASYNC_LOG_(WARN, "follower receive vote_vote msg", K(ret), K(*this), K(msg));
  } else if (!is_candidate_(msg.get_sender())) {
    ret = OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER;
    ELECT_ASYNC_LOG_(WARN, "message sender is not candidate", K(ret), K(*this), K(msg));
  } else {
    const int64_t cur_ts = get_current_ts();
    const int64_t msg_t1 = msg.get_T1_timestamp();
    const ObElectionGroupPriority msg_priority = msg.get_priority();
    ObElectionInfo::StateHelper state_helper(state_);

    ObElectionMsgEGVote4Store msg_vote_to_store(msg);
    msg_vote_to_store.set_eg_version(eg_version_);
    msg_vote_to_store.set_vote_part_array(partition_array_);

    if (!IN_T0_RANGE(cur_ts, msg_t1)) {
      ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
      ELECT_ASYNC_LOG_(WARN,
          "message not in time",
          K(ret),
          K(*this),
          K(msg),
          K(cur_ts),
          "msg fly cost",
          cur_ts - msg.get_send_timestamp(),
          "delta over msg_t1",
          cur_ts - msg_t1);
    } else if (role_ != ROLE_LEADER) {
      ret = OB_NOT_MASTER;
      ELECT_ASYNC_LOG(TRACE, "self is not leader", K(ret), K(*this), K(msg), K(cur_ts));
    } else if (OB_FAIL(state_helper.switch_state(ObElectionInfo::Ops::V_VOTE))) {
      FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this), K(msg), K(cur_ts));
    } else if (OB_FAIL(msg_pool_.store(msg_vote_to_store))) {
      ELECT_ASYNC_LOG_(WARN, "store vote message error", K(ret), K(*this), K(msg_vote_to_store));
    } else {
    }
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  REC_TRACE_EXT(tlog_, handle_vote_vote, Y(ret), Y_(state), Y(msg));
  ELECT_ASYNC_LOG(DEBUG, "finish local_handle_vote_vote", K(ret), K(*this));

  return ret;
}

int ObElectionGroup::handle_vote_vote(const ObElectionMsgEGVote& msg, const int64_t msg_eg_version,
    const bool is_array_deserialized, const char* buf, const int64_t buf_len, const int64_t array_start_pos,
    ObPartitionArray& msg_part_array, obrpc::ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "election_group not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    ELECT_ASYNC_LOG_(WARN, "election_group is not running", K(ret), "self", *this, K(msg));
  } else if (!msg.is_valid()) {
    ret = OB_ELECTION_WARN_INVALID_MESSAGE;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), "self", *this, K(msg));
  } else if (ROLE_SLAVE == role_) {
    ret = OB_NOT_MASTER;
    ELECT_ASYNC_LOG_(WARN, "follower receive vote_vote msg", K(ret), K(msg), K(role_));
  } else if (!is_candidate_(msg.get_sender())) {
    ret = OB_ELECTION_WARN_NOT_CANDIDATE_OR_VOTER;
    ELECT_ASYNC_LOG_(WARN, "message sender is not candidate", K(ret), "self", *this, K(msg));
  } else {
    bool is_use_self_array = false;
    if (!is_array_deserialized) {
      int64_t start_pos = array_start_pos;
      if (msg_eg_version == ATOMIC_LOAD(&eg_version_)) {
        // if version is equal, could use self's partition_array
        is_use_self_array = true;
      } else if (OB_FAIL(decode_partition_array_buf(buf, buf_len, start_pos, msg_part_array))) {
        FORCE_ELECT_LOG(WARN, "decode_partition_array_buf error", K(ret));
      } else {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          ELECT_ASYNC_LOG_(WARN,
              "msg_eg_version is not equal with self",
              K(eg_id_),
              K(msg),
              K(msg_eg_version),
              K(eg_version_),
              "part_cnt",
              partition_array_.count(),
              "msg_part_cnt",
              msg_part_array.count(),
              K(msg_part_array));
        }
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t cur_ts = get_current_ts();
      const int64_t msg_t1 = msg.get_T1_timestamp();
      const ObElectionGroupPriority msg_priority = msg.get_priority();
      ObElectionInfo::StateHelper state_helper(state_);

      ObElectionMsgEGVote4Store msg_vote_to_store(msg);
      msg_vote_to_store.set_eg_version(msg_eg_version);
      if (is_use_self_array) {
        msg_vote_to_store.set_vote_part_array(partition_array_);
      } else {
        msg_vote_to_store.set_vote_part_array(msg_part_array);
      }

      if (!IN_T0_RANGE(cur_ts, msg_t1)) {
        ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
        ELECT_ASYNC_LOG_(WARN,
            "message not in time",
            K(ret),
            "self",
            *this,
            K(msg),
            K(cur_ts),
            "msg fly cost",
            cur_ts - msg.get_send_timestamp(),
            "delta over msg_t1",
            cur_ts - msg_t1);
      } else if (role_ != ROLE_LEADER) {
        ret = OB_NOT_MASTER;
        ELECT_ASYNC_LOG(TRACE, "self is not leader", K(ret), "self", *this, K(msg), K(cur_ts));
      } else if (OB_FAIL(state_helper.switch_state(ObElectionInfo::Ops::V_VOTE))) {
        FORCE_ELECT_LOG(WARN, "switch state error", K(ret), "self", *this, K(msg), K(cur_ts));
      } else if (OB_FAIL(msg_pool_.store(msg_vote_to_store))) {
        ELECT_ASYNC_LOG_(WARN, "store vote message error", K(ret), "self", *this, K(msg_vote_to_store));
      } else {
        if (self_ != msg.get_sender() && msg_priority.compare(self_priority_) > 0) {
          ELECT_ASYNC_LOG_(WARN, "msg_priority is larger than self", K(ret), K(msg), K(self_priority_), "self", *this);
        }
      }
    }
  }
  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  REC_TRACE_EXT(tlog_, handle_vote_vote, Y(ret), Y_(state), Y(msg));
  ELECT_ASYNC_LOG(DEBUG, "finish handle_vote_vote", K(ret), "election_group", *this);

  return ret;
}

int ObElectionGroup::send_vote_vote_(const ObElectionMsgEGVote& msg)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();
  if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(msg));
  } else if (OB_FAIL(post_election_msg_(msg.get_cur_leader(), msg))) {
    FORCE_ELECT_LOG(WARN, "eg send vote vote message error", K(ret), K(msg));
  } else {
    ELECT_ASYNC_LOG(DEBUG, "eg send vote vote success", "self", *this, K(msg));
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  return ret;
}

int ObElectionGroup::get_majority_part_idx_array_(ObPartIdxArray& majority_part_idx_array)
{
  // get the list of partitions who received majority votes
  int ret = OB_SUCCESS;

  if (partition_array_.count() != part_state_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(ERROR, "two array count not match", K(ret), "election_group", *this);
  } else {
    majority_part_idx_array.reset();
    int64_t part_cnt = part_state_array_.count();
    PartState tmp_part_stat;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; ++i) {
      tmp_part_stat = part_state_array_.at(i);
      if (tmp_part_stat.vote_cnt_ > replica_num_ / 2) {
        if (OB_FAIL(majority_part_idx_array.push_back(i))) {
          FORCE_ELECT_LOG(WARN, "array push_back failed", K(ret));
        }
      }
    }
  }

  return ret;
}

void ObElectionGroup::clear_part_stat_vote_cnt_()
{
  // clean every partition's ticket infos
  int64_t part_cnt = part_state_array_.count();
  for (int64_t i = 0; i < part_cnt; ++i) {
    part_state_array_.at(i).vote_cnt_ = 0;
  }
}

// receive new message
// 1. check if received enough messages
// 2. if so, send vote success message
int ObElectionGroup::check_centralized_majority_()
{
  int ret = OB_SUCCESS;
  ObAddr cur_leader;
  ObAddr new_leader;
  bool is_eg_majority = false;  // if all group merged in
  const int64_t cur_ts = get_current_ts();

  if (OB_FAIL(msg_pool_.check_eg_centralized_majority(cur_leader,
          new_leader,
          is_all_part_merged_in_,
          is_eg_majority,
          part_state_array_,  // for counting every singal partition's tick
          partition_array_,
          eg_version_,
          replica_num_,
          T1_timestamp_,
          hash(),
          cur_ts))) {
    if (OB_ELECTION_WARN_NOT_REACH_MAJORITY == ret || OB_ELECTION_WAIT_LEADER_MESSAGE == ret) {
      // no enough message or waiting for leader's message
    } else {
      ELECT_ASYNC_LOG_(WARN, "check_centralized_majority_ error", K(ret), K_(eg_id));
    }
  } else {
    bool arg_all_part_merged_in = is_all_part_merged_in_;
    ObPartIdxArray majority_part_idx_array;
    if (is_eg_majority) {
      // the entire group renew lease success, say group all merged-in
      // need update lease first, then set is_all_part_merged_in_ status
      arg_all_part_merged_in = true;
    } else if (partition_array_.count() > 0) {
      // get the list of partitions who win majority's vote
      get_majority_part_idx_array_(majority_part_idx_array);
      if (is_all_part_merged_in_) {
        // need push lease to every partition's state,
        // if is_all_part_merged_in_ turned to false from true
        push_lease_to_part_stat_();
      }
      is_all_part_merged_in_ = false;
      arg_all_part_merged_in = false;
    }

    // switch status, send vote_success message
    ObElectionInfo::StateHelper state_helper(state_);
    if (OB_FAIL(state_helper.switch_state(ObElectionInfo::Ops::V_COUNT))) {
      FORCE_ELECT_LOG(WARN, "switch state error", K(ret), "self", *this);
    } else if (is_real_leader_(self_) && is_candidate_(self_) && cur_leader == curr_leader_) {
      if (cur_leader == new_leader) {
        ObElectionMsgEGSuccess msg(cur_leader,
            new_leader,
            T1_timestamp_,
            arg_all_part_merged_in,
            majority_part_idx_array,
            get_current_ts(),
            self_,
            lease_time_);
        if (is_empty_()) {
          FORCE_ELECT_LOG(WARN, "self is empty, not send_vote_success", K(ret), K(*this));
        } else if (OB_FAIL(local_handle_vote_success_(msg))) {
          FORCE_ELECT_LOG(WARN, "local_handle_vote_success failed", K(ret), K(*this), K(msg));
        } else if (OB_FAIL(send_vote_success_(msg))) {
          FORCE_ELECT_LOG(WARN, "send vote success error", K(ret), K(*this), K(msg));
        }
      }
    } else {
      ret = OB_STATE_NOT_MATCH;
      FORCE_ELECT_LOG(WARN, "check_centralized_majority_ error", K(ret), K(*this), K(cur_leader));
    }
  }
  clear_part_stat_vote_cnt_();

  return ret;
}

bool ObElectionGroup::is_pkey_in_majority_array_(const ObPartitionKey& tmp_pkey, const ObPartitionArray& msg_part_array,
    const ObPartIdxArray& msg_majority_part_idx_array) const
{
  bool bool_ret = false;
  const int64_t msg_majority_array_cnt = msg_majority_part_idx_array.count();
  const int64_t msg_array_cnt = msg_part_array.count();

  for (int64_t i = 0; !bool_ret && i < msg_majority_array_cnt; ++i) {
    int64_t cur_idx = msg_majority_part_idx_array.at(i);
    if (cur_idx >= 0 && cur_idx < msg_array_cnt) {
      if (msg_part_array.at(cur_idx) == tmp_pkey) {
        bool_ret = true;
      }
    }
  }

  return bool_ret;
}

int ObElectionGroup::parse_majority_part_idx_array_(const bool msg_all_part_merged_in,
    const ObPartIdxArray& msg_majority_part_idx_array, const ObPartitionArray& msg_part_array,
    ObPartIdxArray& majority_idx_array) const
{
  int ret = OB_SUCCESS;
  const int64_t msg_array_cnt = msg_part_array.count();

  // get the list of partitions' index who successfully renew lease
  int64_t tmp_idx = -1;
  majority_idx_array.reset();
  if (partition_array_.count() != part_state_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(ERROR, "two array count not match", K(ret), "election_group", *this);
  } else if (msg_all_part_merged_in) {
    for (int64_t i = 0; OB_SUCC(ret) && i < msg_array_cnt; ++i) {
      const ObPartitionKey& tmp_pkey = msg_part_array.at(i);
      if (is_partition_exist_unlock_(tmp_pkey, tmp_idx) && OB_FAIL(majority_idx_array.push_back(tmp_idx))) {
        FORCE_ELECT_LOG(WARN, "array push_back failed", K(ret));
      }
    }
  } else {
    const int64_t msg_majority_array_cnt = msg_majority_part_idx_array.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < msg_majority_array_cnt; ++i) {
      int64_t cur_idx = msg_majority_part_idx_array.at(i);
      if (cur_idx >= 0 && cur_idx < msg_array_cnt) {
        const ObPartitionKey& tmp_pkey = msg_part_array.at(cur_idx);
        if (is_partition_exist_unlock_(tmp_pkey, tmp_idx) && OB_FAIL(majority_idx_array.push_back(tmp_idx))) {
          FORCE_ELECT_LOG(WARN, "array push_back failed", K(ret));
        }
      }
    }
  }

  return ret;
}

// leader process vote_success message locally
int ObElectionGroup::local_handle_vote_success_(const ObElectionMsgEGSuccess& msg)
{
  // caller need add lock
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "election_group not inited", K(ret), K(*this));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election_group is not running", K(ret), K(*this), K(msg));
    }
  } else if (is_pre_destroy_state()) {
    ret = OB_STATE_NOT_MATCH;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election_group is in pre_destroy state, ignore msg", K(ret), K(*this), K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), K(*this), K(msg));
  } else if (ROLE_SLAVE == role_) {
    ret = OB_ERR_UNEXPECTED;
    ELECT_ASYNC_LOG_(WARN, "self is not leader", K(ret), K(*this), K(msg));
  } else if (curr_leader_ != msg.get_new_leader()) {
    if (is_pre_destroy_state()) {
      ELECT_ASYNC_LOG_(WARN, "curr_leader_ not same with msg_new_leader, ignore msg", K(ret), K(msg), K(*this));
    } else {
      ret = OB_ERR_UNEXPECTED;
      ELECT_ASYNC_LOG_(ERROR, "curr_leader_ not same with msg_new_leader", K(ret), K(msg), K(*this));
    }
  } else {
    const int64_t cur_ts = get_current_ts();
    const int64_t msg_t1 = msg.get_T1_timestamp();
    const int64_t lease_time = msg.get_lease_time();
    const int64_t now_lease_time = (lease_time == 0) ? (OB_ELECTION_130_LEASE_TIME) : lease_time;

    if (!IN_T0_RANGE(cur_ts, msg_t1)) {
      ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
      ELECT_ASYNC_LOG_(WARN,
          "message not in time",
          K(ret),
          K(*this),
          K(msg),
          K(cur_ts),
          "msg fly cost",
          cur_ts - msg.get_send_timestamp(),
          "delta over msg_t1",
          cur_ts - msg_t1);
    } else {
      ObElectionInfo::StateHelper state_helper(state_);
      const ObAddr& msg_new_leader = msg.get_new_leader();
      const bool msg_all_part_merged_in = msg.is_all_part_merged_in();
      // indicate should push election's entire lease or not
      bool need_update_eg_lease = msg_all_part_merged_in;

      ObPartIdxArray majority_idx_array;
      if (!need_update_eg_lease) {
        // group can not push entire lease, need analyze successfully renew lease partitions
        bool is_use_self_array = true;  // can use self's partition_array directly
        // get all successfully renre lease partitions' index
        if (OB_SUCC(ret)) {
          if (is_use_self_array) {
            (void)parse_majority_part_idx_array_(
                msg_all_part_merged_in, msg.get_majority_part_idx_array(), partition_array_, majority_idx_array);
          }
        }
      }

      // update leader lease
      if (OB_FAIL(ret)) {
        // abort
      } else if (OB_FAIL(state_helper.switch_state(ObElectionInfo::Ops::V_SUCCESS))) {
        FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this), K(msg));
      } else if (OB_FAIL(leader_reappoint_(
                     need_update_eg_lease, majority_idx_array, msg_new_leader, msg_t1, now_lease_time))) {
        ELECT_ASYNC_LOG_(WARN, "centralized voting, leader reappoint error", K(ret), K(*this), K(msg));
      } else {
        // set is_all_part_merged_in_ after push lease
        is_all_part_merged_in_ = msg_all_part_merged_in;
      }
    }
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  REC_TRACE_EXT(tlog_, handle_vote_success, Y(ret), Y_(state), Y(msg));
  ELECT_ASYNC_LOG(DEBUG, "finish local_handle_vote_success", K(ret), K(msg), K(*this));

  return ret;
}

int ObElectionGroup::handle_vote_success(const ObElectionMsgEGSuccess& msg, const int64_t msg_eg_version,
    const bool is_array_deserialized, const char* buf, const int64_t buf_len, const int64_t array_start_pos,
    ObPartitionArray& msg_part_array, ObPartitionArray& move_in_failed_array, obrpc::ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  WLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG_(WARN, "election_group not inited", K(ret));
  } else if (!is_running_) {
    ret = OB_ELECTION_WARN_NOT_RUNNING;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election_group is not running", K(ret), "self", *this, K(msg));
    }
  } else if (is_pre_destroy_state()) {
    ret = OB_STATE_NOT_MATCH;
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      ELECT_ASYNC_LOG_(WARN, "election_group is in pre_destroy state, ignore msg", K(ret), "self", *this, K(msg));
    }
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), "self", *this, K(msg));
  } else if (msg_eg_version < eg_version_ && ROLE_SLAVE == role_) {
    ret = OB_UNEXPECT_EG_VERSION;
    ELECT_ASYNC_LOG_(WARN, "receive smaller eg_version", K(ret), K(msg), K(msg_eg_version), K(eg_version_));
  } else if (!curr_leader_.is_valid()) {
    ret = OB_STATE_NOT_MATCH;
    ELECT_ASYNC_LOG_(WARN, "curr_leader_ is invalid, cannot handle msg", K(ret), K(msg), K(msg_eg_version), K(*this));
  } else if (curr_leader_ != msg.get_new_leader()) {
    if (is_pre_destroy_state()) {
      ELECT_ASYNC_LOG_(WARN, "curr_leader_ not same with msg_new_leader, ignore msg", K(ret), K(msg), "self", *this);
    } else {
      ret = OB_ERR_UNEXPECTED;
      ELECT_ASYNC_LOG_(ERROR, "curr_leader_ not same with msg_new_leader", K(ret), K(msg), "self", *this);
    }
  } else {
    const int64_t cur_ts = get_current_ts();
    const int64_t msg_t1 = msg.get_T1_timestamp();
    const int64_t lease_time = msg.get_lease_time();
    const int64_t now_lease_time = (lease_time == 0) ? (OB_ELECTION_130_LEASE_TIME) : lease_time;

    if (!IN_T0_RANGE(cur_ts, msg_t1)) {
      ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
      ELECT_ASYNC_LOG_(WARN,
          "message not in time",
          K(ret),
          "election_group",
          *this,
          K(msg),
          K(cur_ts),
          "msg fly cost",
          cur_ts - msg.get_send_timestamp(),
          "delta over msg_t1",
          cur_ts - msg_t1);
    }
    if (OB_SUCC(ret)) {
      ObElectionInfo::StateHelper state_helper(state_);
      const ObAddr& msg_new_leader = msg.get_new_leader();
      const bool msg_all_part_merged_in = msg.is_all_part_merged_in();
      bool need_update_merged_in_val = false;

      // indicate should push entire group lease
      bool need_update_eg_lease = msg_all_part_merged_in;
      if (msg_new_leader == self_) {
        if (msg_eg_version == eg_version_) {
          // if message equal, use message's eg version
          need_update_merged_in_val = true;
        } else {
          // if message not equal, means some leader moved in after send_vote_success,
          // need push lease to every partition
          need_update_eg_lease = false;
        }
      } else {
        // follower group follows the message info
        // it's safe because move in may failed, but move out won't fail.
        // even though follower move in failed, it's partitions must be a part of leader's
        // so if leader's vote_success message says all partitions merged-in, then all followers must merged-in
        if (true == is_all_part_merged_in_ && false == msg_all_part_merged_in) {
          // push lease to every partition's state when is_all_part_merged_in_ turned to false
          push_lease_to_part_stat_();
        }
        need_update_merged_in_val = true;
      }

      ObPartIdxArray majority_idx_array;
      if (!need_update_eg_lease) {
        // group can not push entire lease, need analyze successfully renew lease partitions
        bool is_use_self_array = false;
        if (!is_array_deserialized) {
          int64_t start_pos = array_start_pos;
          if (msg_eg_version == ATOMIC_LOAD(&eg_version_)) {
            // if version equal, could use self's partition_array
            is_use_self_array = true;
          } else if (OB_FAIL(decode_partition_array_buf(buf, buf_len, start_pos, msg_part_array))) {
            FORCE_ELECT_LOG(WARN, "decode_partition_array_buf error", K(ret));
          } else {
            // do nothing
          }
        }
        // get all successfully renre lease partitions' index
        if (OB_SUCC(ret)) {
          if (is_use_self_array) {
            (void)parse_majority_part_idx_array_(
                msg_all_part_merged_in, msg.get_majority_part_idx_array(), partition_array_, majority_idx_array);
          } else {
            (void)parse_majority_part_idx_array_(
                msg_all_part_merged_in, msg.get_majority_part_idx_array(), msg_part_array, majority_idx_array);
          }
        }
      }

      // count all move in failed but successfully renew lease partitions
      if (move_in_failed_array.count() > 0) {
        // move in failed means version not equal, so move in failed must has been deserialized
        int tmp_ret = OB_SUCCESS;
        ObPartitionArray vote_succ_array;
        const int64_t fail_cnt = move_in_failed_array.count();
        for (int64_t i = 0; i < fail_cnt; ++i) {
          const ObPartitionKey tmp_pkey = move_in_failed_array.at(i);
          int64_t unused_idx = -1;
          if (msg_all_part_merged_in) {
            if (is_pkey_in_array_(tmp_pkey, msg_part_array, unused_idx)) {
              (void)vote_succ_array.push_back(tmp_pkey);
            }
          } else {
            if (is_pkey_in_majority_array_(tmp_pkey, msg_part_array, msg.get_majority_part_idx_array())) {
              (void)vote_succ_array.push_back(tmp_pkey);
            }
          }
        }
        // update input arg, returned as output arg
        move_in_failed_array.reset();
        if (vote_succ_array.count() > 0 && OB_SUCCESS != (tmp_ret = move_in_failed_array.assign(vote_succ_array))) {
          FORCE_ELECT_LOG(WARN, "assign array failed", K(tmp_ret), "election_group", *this);
        }
      }

      // update leader lease
      if (OB_FAIL(ret)) {
        // abort
      } else if (OB_FAIL(state_helper.switch_state(ObElectionInfo::Ops::V_SUCCESS))) {
        FORCE_ELECT_LOG(WARN, "switch state error", K(ret), K(*this), K(msg));
      } else if (OB_FAIL(leader_reappoint_(
                     need_update_eg_lease, majority_idx_array, msg_new_leader, msg_t1, now_lease_time))) {
        ELECT_ASYNC_LOG_(WARN, "centralized voting, leader reappoint error", K(ret), "election_group", *this, K(msg));
      } else {
        // set is_all_part_merged_in_ after lease updated
        if (need_update_merged_in_val) {
          is_all_part_merged_in_ = msg_all_part_merged_in;
        }
      }
      if (OB_SUCC(ret) && ROLE_SLAVE == role_) {
        // if follower's gt4 can't be executed before next vote_prepare arrived, change state to V_IDLE
        int tmp_ret = OB_SUCCESS;
        if (OB_INVALID_TIMESTAMP != first_gt4_ts_ && first_gt4_ts_ < msg_t1 + T_ELECT2) {
          // gt4 should be executed in time, just skip
        } else if (OB_SUCCESS != (tmp_ret = state_helper.switch_state(ObElectionInfo::Ops::RESET))) {
          FORCE_ELECT_LOG(WARN, "switch state error", K(tmp_ret), K(*this), K(msg));
        }
      }
    }
  }

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  REC_TRACE_EXT(tlog_, handle_vote_success, Y(ret), Y_(state), Y(msg));
  ELECT_ASYNC_LOG(DEBUG, "finish handle_vote_success", K(ret), "election_group", *this);

  return ret;
}

int ObElectionGroup::leader_reappoint_(const bool need_update_eg_lease, const ObPartIdxArray& majority_part_idx_array,
    const ObAddr& leader, const int64_t t1, const int64_t lease_time)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();

  if (!leader.is_valid() || t1 <= 0 || lease_time <= 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(leader), K(t1), K(lease_time), K(eg_id_));
  } else {
    if (need_update_eg_lease) {
      // push lease for entire group
      if (true == is_all_part_merged_in_ &&  // need check lease only if group is not merged-in
                                             // lease could be expired if group not merged-in, cause not maintained
          ObElectionRole::ROLE_LEADER == role_ &&  // need check lease only if I'm leader
          cur_ts > leader_lease_.second) {         // check if lease has been expired
        // maybe lease expired event has been exposed to others, leader epoch should pushed
        // give up renew lease, group will be destoryed
        ret = OB_ELECTION_WARN_LEADER_LEASE_EXPIRED;
        FORCE_ELECT_LOG(WARN,
            "group leader lease expired, give up reappointing",
            KR(ret),
            K(need_update_eg_lease),
            K(t1),
            K(lease_time),
            K(cur_ts),
            K(*this));
      } else if (OB_FAIL(leader_elected_(leader, t1, lease_time))) {
        FORCE_ELECT_LOG(WARN, "leader_elected_ failed", K(ret), K(leader), K(t1), K(lease_time), K(eg_id_));
      }
    } else {
      // update lease for every partition
      // lease has been updated if group turned to not merge-in
      int64_t lease_more = 0;
      if (leader == self_) {
        lease_more = T_LEADER_LEASE_MORE;
      } else {
        lease_more = T_SLAVE_LEASE_MORE;
      }
      int64_t tmp_idx = -1;
      // update every partitions' lease
      for (int64_t i = 0; i < majority_part_idx_array.count(); ++i) {
        tmp_idx = majority_part_idx_array.at(i);
        if (tmp_idx >= 0 && tmp_idx < part_state_array_.count()) {
          if (ObElectionRole::ROLE_LEADER == role_ && cur_ts > part_state_array_.at(tmp_idx).lease_end_) {
            // do nothing
          } else {
            part_state_array_.at(tmp_idx).takeover_t1_timestamp_ = t1;
            part_state_array_.at(tmp_idx).lease_end_ = t1 + (lease_time + lease_more);
          }
        }
      }
    }
  }

  return ret;
}

int ObElectionGroup::leader_elected_(const ObAddr& leader, const int64_t t1, const int64_t lease_time)
{
  int ret = OB_SUCCESS;

  if (!leader.is_valid() || t1 <= 0 || lease_time <= 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(leader), K(t1), K(lease_time));
  } else {
    // set current leader
    curr_leader_ = leader;
    takeover_t1_timestamp_ = t1;
    vote_period_ = T_CENTRALIZED_VOTE_PERIOD_EG;
    lease_time_ = OB_ELECTION_130_LEASE_TIME;
    // election group don't maintain lease.start anymore
    // leader_lease_.first = t1;
    if (curr_leader_ == self_) {
      ATOMIC_STORE(&role_, ROLE_LEADER);
      leader_lease_.second = t1 + (lease_time + T_LEADER_LEASE_MORE);
    } else {
      ATOMIC_STORE(&role_, ROLE_SLAVE);
      leader_lease_.second = t1 + (lease_time + T_SLAVE_LEASE_MORE);
    }
  }
  flush_trace_buf_();
  ignore_log_ = false;

  return ret;
}

void ObElectionGroup::flush_trace_buf_()
{
  if (!ignore_log_ && need_print_trace_log_) {
    PRINT_ASYNC_TRACE_BUF(tlog_);
  } else {
    tlog_.reset();
  }
  REC_TRACE_EXT(tlog_, new_period, Y_(eg_id), Y_(state));
  need_print_trace_log_ = false;
}

bool ObElectionGroup::lease_expired_too_long_(const int64_t cur_ts) const
{
  return (cur_ts - takeover_t1_timestamp_ > EXPIRED_TIME_FOR_WARNING);
}

int ObElectionGroup::handle_prepare_destroy_msg(
    const ObElectionMsg& msg, const int64_t msg_eg_version, obrpc::ObElectionRpcResult& result)
{
  int ret = OB_SUCCESS;

  do {
    WLockGuard guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      ELECT_ASYNC_LOG_(WARN, "election_group not inited", K(ret), K(eg_id_));
    } else if (!is_running_) {
      ret = OB_ELECTION_WARN_NOT_RUNNING;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        ELECT_ASYNC_LOG_(WARN, "election_group is not running", K(ret), "self", *this, K(msg));
      }
    } else if (msg_eg_version < eg_version_) {
      ret = OB_UNEXPECT_EG_VERSION;
      ELECT_ASYNC_LOG_(WARN, "receive smaller eg_version", K(ret), K(msg), K(msg_eg_version), K(eg_version_));
    } else if (!msg.is_valid() || (curr_leader_.is_valid() && curr_leader_ != msg.get_sender())) {
      ret = OB_INVALID_ARGUMENT;
      ELECT_ASYNC_LOG_(WARN, "invalid argument", K(ret), "self", *this, K(msg));
    } else if (ROLE_SLAVE != role_) {
      ret = OB_STATE_NOT_MATCH;
      ELECT_ASYNC_LOG_(WARN, "self is not follower", K(ret), "self", *this, K(msg));
    } else {
      ATOMIC_STORE(&pre_destroy_state_, true);
      ELECT_ASYNC_LOG_(INFO, "handle_prepare_destroy_msg success", K(ret), "self", *this, K(msg_eg_version), K(msg));
    }
  } while (0);

  result.reset();
  result.set_status(ret);
  result.set_timestamp(msg.get_send_timestamp());

  return ret;
}

int ObElectionGroup::notify_follower_destroy_()
{
  // leader group should notify follower group destory if in pre-destory state
  int ret = OB_SUCCESS;

  if (curr_leader_.is_valid() && curr_leader_ != self_) {
    // skip eg follower
  } else {
    const int64_t cur_ts = get_current_ts();
    ObElectionMsg msg(OB_ELECTION_EG_DESTROY, cur_ts, self_);
    if (OB_FAIL(post_election_msg_(sorted_member_list_, msg))) {
      FORCE_ELECT_LOG(WARN, "send destroy message error", K(ret), K(msg), K(eg_id_));
    } else {
      ELECT_ASYNC_LOG(INFO, "send destroy message success", K(ret), K(msg), K(eg_id_));
    }
  }

  return ret;
}

int ObElectionGroup::prepare_destroy()
{
  // called before destory, set pre_destroy_state, move out all election
  int ret = OB_SUCCESS;
  ObPartitionArray pending_move_out_array;

  do {
    // leader group notify follower group before destory
    WLockGuard guard(lock_);
    // stop move in
    ATOMIC_STORE(&pre_destroy_state_, true);
    // copy current partition lise, no partition added under pre_destroy state
    if (partition_array_.count() > 0 && OB_FAIL(pending_move_out_array.assign(partition_array_))) {
      FORCE_ELECT_LOG(WARN, "assign array failed", K(ret), "election_group", *this);
    }
  } while (0);

  // try move out all partitions
  if (pending_move_out_array.count() > 0 && OB_FAIL(batch_move_out_partition_(pending_move_out_array))) {
    FORCE_ELECT_LOG(WARN, "batch_move_out_partition_ failed", K(ret), "election_group", *this);
  }

  return ret;
}

int ObElectionGroup::get_priority_(ObElectionGroupPriority& priority) const
{
  // only leader group need call this to get priority for now
  int ret = OB_SUCCESS;

  if (OB_ISNULL(eg_cb_)) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(ERROR, "eg_cb_ is NULL", K(ret), KP_(eg_cb), K_(eg_id));
  } else if (OB_FAIL(eg_cb_->get_election_group_priority(tenant_id_, priority))) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      FORCE_ELECT_LOG(WARN, "get_election_group_priority failed", K(ret), K_(eg_id));
    }
  } else {
    // do nothing
  }

  return ret;
}

void ObElectionGroup::run_gt1_task(const int64_t expect_ts)
{
  int ret = OB_SUCCESS;
  bool expired = false;
  int64_t delay = 0;
  ObPartitionArray pending_move_out_array;

  do {
    // <need lock>if lease expired, need disband group
    WLockGuard guard(lock_);
    const int64_t cur_ts = get_current_ts();
    ObElectionInfo::StateHelper state_helper(state_);
    ELECT_ASYNC_LOG(DEBUG, "GT1Task", "election_group", *this, K(expect_ts), K(cur_ts));
    if (expect_ts <= 0) {
      ret = OB_INVALID_ARGUMENT;
      FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(expect_ts), "election_group", *this);
    } else if (run_time_out_of_range_(cur_ts, expect_ts)) {
      ret = OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE;
      ELECT_ASYNC_LIMIT_LOG(WARN,
          "run time out of range",
          K(ret),
          "election_group",
          *this,
          K(cur_ts),
          K(expect_ts),
          "delta",
          cur_ts - expect_ts);
    } else {
      T1_timestamp_ = expect_ts;
      int tmp_ret = OB_SUCCESS;

      if (!GCONF.enable_election_group && curr_leader_ == self_) {
        // disabled group config, leader group should be disbanded
        if (OB_SUCCESS != (tmp_ret = pending_move_out_array.assign(partition_array_))) {
          FORCE_ELECT_LOG(WARN, "assign array failed", K(tmp_ret), "election_group", *this);
        }
        ATOMIC_STORE(&pre_destroy_state_, true);
        if (!partition_array_.empty()) {
          ELECT_ASYNC_LOG(
              INFO, "GCONF.enable_election_group is false, need move out all partition", "election_group", *this);
        }
      } else if (is_pre_destroy_state()) {
        // if group is pre-destoryed, move out all partitions and disbanded
        if (OB_SUCCESS != (tmp_ret = pending_move_out_array.assign(partition_array_))) {
          FORCE_ELECT_LOG(WARN, "assign array failed", K(tmp_ret), "election_group", *this);
        }
        ELECT_ASYNC_LOG(INFO, "self is in pre_destroy state, need move out all partition", "election_group", *this);
      } else {
        bool is_all_dropped = false;
        expired = eg_lease_is_completely_expired_(cur_ts, is_all_dropped);
        if (!expired) {
          if (!is_all_part_merged_in_) {
            // check every partition's lease, move out those lease expired partitions
            bool is_all_expired = false;
            if (OB_SUCCESS !=
                (tmp_ret = check_all_part_leader_lease_(cur_ts, is_all_expired, pending_move_out_array))) {
              FORCE_ELECT_LOG(WARN, "check_all_part_leader_lease_ failed", K(tmp_ret), "election_group", *this);
            } else if (pending_move_out_array.count() > 0) {
              FORCE_ELECT_LOG(
                  WARN, "some partitions' lease is expired, need move out them", K(eg_id_), K(pending_move_out_array));
            } else {
              // do nothing
            }
          }
        } else {
          // lease expired:execute leader_revoke, destory self
          const ObAddr old_leader = curr_leader_;
          leader_revoke_();
          // need move out all partitions
          if (OB_SUCCESS != (tmp_ret = pending_move_out_array.assign(partition_array_))) {
            FORCE_ELECT_LOG(WARN, "assign array failed", K(tmp_ret), "election_group", *this);
          }
          // avoid print error log while election run at start
          if (cur_ts > start_timestamp_ + (vote_period_ + T_CYCLE_EXTEND_MORE) * T_ELECT2) {
            // print error log only if leader group's lease expired
            if (old_leader == self_ && partition_array_.count() > 0 && !is_all_dropped) {
              ELECT_ASYNC_LOG_(ERROR, "eg leader lease is expired", K(*this), K(old_leader), K(is_all_dropped));
            } else {
              ELECT_ASYNC_LOG_(WARN, "eg leader lease is expired", K(*this), K(old_leader), K(is_all_dropped));
            }
          }
          REC_TRACE_EXT(tlog_, lease_expired, OB_ID(election_group), *this);
          need_print_trace_log_ = true;
          flush_trace_buf_();
          if (lease_expired_too_long_(cur_ts)) {
            ignore_log_ = true;
            // lease expired too long, set pre_destroy_state_, avoid follower not received leader's notify
            ATOMIC_STORE(&pre_destroy_state_, true);
          }
        }
        if (OB_SUCC(ret)) {
          if (expired) {
            // disband group
          } else {
            // leader reappoint from (vote_period_*1.4s)
            if (is_real_leader_(self_) && in_reappoint_period_(expect_ts)) {
              if (!is_active_(cur_ts)) {
                ELECT_ASYNC_LOG(DEBUG, "election is not active", "self", *this);
              } else if (!is_candidate_(self_)) {
                FORCE_ELECT_LOG(WARN, "self is not candidate", "self", *this);
              } else if (is_pre_destroy_state()) {
                // must not do reappoint if group if pre-destroyed
                ELECT_ASYNC_LOG(INFO, "in pre_destroy_state, give up reappoint", "self", *this);
              } else if (is_empty_()) {
                FORCE_ELECT_LOG(WARN, "self is empty, no need to reappoint", K(ret), "self", *this);
              } else if (OB_FAIL(get_priority_(self_priority_))) {
                if (REACH_TIME_INTERVAL(1000 * 1000)) {
                  FORCE_ELECT_LOG(WARN, "get priority failed", K(ret), "self", *this);
                }
              } else if (!self_priority_.is_candidate()) {
                // priority is not allow self be leader, disband self
                if (OB_SUCCESS != (tmp_ret = pending_move_out_array.assign(partition_array_))) {
                  FORCE_ELECT_LOG(WARN, "assign array failed", K(tmp_ret), "election_group", *this);
                }
                ATOMIC_STORE(&pre_destroy_state_, true);
                FORCE_ELECT_LOG(
                    WARN, "priority is not candidate, give up reappointment", K(ret), K(self_priority_), "self", *this);
              } else if (OB_FAIL(state_helper.switch_state(ObElectionInfo::Ops::V_ELECT))) {
                FORCE_ELECT_LOG(WARN, "switch state error", K(ret), "self", *this);
              } else {
                ObAddr new_leader = curr_leader_;
                ObElectionMsgEGPrepare msg(
                    curr_leader_, new_leader, T1_timestamp_, get_current_ts(), self_, lease_time_);
                if (OB_FAIL(local_handle_vote_prepare_(msg))) {
                  ELECT_ASYNC_LOG_(WARN, "local_handle_vote_prepare_ failed", K(ret), K(*this), K(msg));
                } else if (OB_FAIL(send_vote_prepare_(msg))) {
                  ELECT_ASYNC_LOG_(WARN, "send vote prepare error", K(ret), "self", *this, K(msg));
                } else {
                  // leader need gt3 to count tickets
                  const int64_t t3 = T3_TIMESTAMP(T1_timestamp_);
                  int tmp_ret = OB_SUCCESS;
                  if (OB_SUCCESS != (tmp_ret = timer_.register_gt3_once(t3, delay))) {
                    ELECT_ASYNC_LIMIT_LOG(WARN, "fail to register gt3", K(tmp_ret), "self", *this, K(delay));
                  } else {
                    ELECT_ASYNC_LIMIT_LOG(DEBUG, "register gt3 success", K(T1_timestamp_), K(t3), K(delay));
                  }
                }
              }
            }
            REC_TRACE_EXT(tlog_, run_gt1_task, Y(expect_ts), Y_(state), Y_(takeover_t1_timestamp));
          }
        } else {
          FORCE_ELECT_LOG(WARN, "run gt1 task error", K(ret), "self", *this);
        }
      }
    }
    // register gt4. clear status
    int tmp_ret = OB_SUCCESS;
    const int64_t t4 = T4_TIMESTAMP(expect_ts);
    if (OB_SUCCESS != (tmp_ret = timer_.register_gt4_once(t4, delay))) {
      ELECT_ASYNC_LIMIT_LOG(WARN, "fail to register gt4", K(tmp_ret), "self", *this, K(delay));
    } else if (OB_INVALID_TIMESTAMP == first_gt4_ts_) {
      first_gt4_ts_ = t4;
    }
    // register gt1. if register error, we must retry otherwise election halt
    int64_t next_expect_ts = expect_ts + T_ELECT2;
    while (OB_SUCCESS != (tmp_ret = timer_.register_gt1_once(next_expect_ts, delay))) {
      ELECT_ASYNC_LIMIT_LOG(WARN, "fail to register gt1", K(tmp_ret), "self", *this, K(next_expect_ts), K(delay));
      if (OB_NOT_RUNNING == tmp_ret) {
        // don't need retry if time_wheel has been stop
        break;
      }
      if (delay < 0) {
        next_expect_ts -= (delay - T_ELECT2 + 1) / T_ELECT2 * T_ELECT2;
      }
    }
  } while (0);
  // <no lock needed>move out partition
  if (pending_move_out_array.count() > 0) {
    (void)batch_move_out_partition_(pending_move_out_array);
  }
}

void ObElectionGroup::run_gt3_task(const int64_t expect_ts)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();
  ObAddr cur_leader;
  ObAddr new_leader;

  WLockGuard guard(lock_);

  if (expect_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(expect_ts), "election_group", *this);
  } else if (run_time_out_of_range_(cur_ts, expect_ts)) {
    ret = OB_ELECTION_WARN_RUNTIME_OUT_OF_RANGE;
    ELECT_ASYNC_LIMIT_LOG(
        WARN, "run time out of range", K(ret), "self", *this, K(cur_ts), K(expect_ts), "delta", cur_ts - expect_ts);
  } else {
    if (OB_FAIL(check_centralized_majority_())) {
      ELECT_ASYNC_LOG_(WARN, "check_centralized_majority_ error", K(ret), "self", *this);
    }
    REC_TRACE_EXT(tlog_, check_centralized_majority, Y(ret));
  }
  ELECT_ASYNC_LOG(DEBUG, "GT3Task", K(ret), "self", *this, K(expect_ts), K(cur_ts));
}

void ObElectionGroup::run_gt4_task(const int64_t expect_ts)
{
  int tmp_ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();

  WLockGuard guard(lock_);
  ObElectionInfo::StateHelper state_helper(state_);
  if (expect_ts <= 0) {
    FORCE_ELECT_LOG(WARN, "invalid argument", K(expect_ts), "self", *this);
  } else if (run_time_out_of_range_(cur_ts, expect_ts)) {
    ELECT_ASYNC_LIMIT_LOG(
        WARN, "run time out of range", "self", *this, K(cur_ts), K(expect_ts), "delta", cur_ts - expect_ts);
  } else if (OB_SUCCESS != (tmp_ret = state_helper.switch_state(ObElectionInfo::Ops::RESET))) {
    FORCE_ELECT_LOG(WARN, "switch state error", "ret", tmp_ret, "self", *this);
  } else {
    msg_pool_.reset();
  }
  ELECT_ASYNC_LIMIT_LOG(DEBUG, "GT4Task", "self", *this, K(expect_ts), K(cur_ts));
}

void ObElectionGroup::leader_revoke_()
{
  int tmp_ret = OB_SUCCESS;
  ObElectionInfo::StateHelper state_helper(state_);
  if (OB_SUCCESS != (tmp_ret = state_helper.switch_state(ObElectionInfo::Ops::LEADER_REVOKE))) {
    FORCE_ELECT_LOG(WARN, "switch state error", "ret", tmp_ret, "self", *this);
  } else if (curr_leader_.is_valid()) {
    // lease.end need copy to part_stat before setted to 0,avoid reading a invalid lease.end
    if (is_all_part_merged_in_) {
      push_lease_to_part_stat_();
    }
    if (self_ == curr_leader_) {
      // eg leader need set pre_destroy_state_, avoid moving in new partition
      ATOMIC_STORE(&pre_destroy_state_, true);
    }
    ELECT_ASYNC_LOG(INFO, "leader revoke", "self", *this);
  } else {
    // do nothing
  }
}

bool ObElectionGroup::run_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const
{
  bool bool_ret = false;
  if (ObElectionInfo::State::is_centralized_state(state_)) {
    bool_ret = vote_run_time_out_of_range_(cur_ts, expect_ts);
  }
  return bool_ret;
}

bool ObElectionGroup::vote_run_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const
{
  return (cur_ts - expect_ts) > T_VOTE_TIMER_DIFF;
}

int ObElectionGroup::send_vote_prepare_(const ObElectionMsgEGPrepare& msg)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    FORCE_ELECT_LOG(WARN, "election_group not inited", K(ret), K(*this));
  } else if (!msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(*this), K(msg));
  } else {
    if (OB_FAIL(post_election_msg_(sorted_member_list_, msg))) {
      FORCE_ELECT_LOG(WARN, "send vote prepare message error", K(ret), K(msg));
    } else {
      ELECT_ASYNC_LOG(DEBUG, "send vote prepare message success", "self", *this, K(msg));
    }
    REC_TRACE_EXT(tlog_, send_vote_prepare, Y(ret), Y_(state), Y(msg));
  }

  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }

  return ret;
}

int ObElectionGroup::send_vote_success_(const ObElectionMsgEGSuccess& msg)
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = get_current_ts();

  if (!msg.is_valid()) {
  } else if (OB_FAIL(post_election_msg_(sorted_member_list_, msg))) {
    FORCE_ELECT_LOG(WARN, "send vote success message error", K(ret), K(msg));
  } else {
    ELECT_ASYNC_LOG(DEBUG, "send vote success message success", K(*this), K(msg));
  }
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  REC_TRACE_EXT(tlog_, send_vote_success, Y(ret), Y_(state), Y(msg));

  return ret;
}

int ObElectionGroup::get_eg_centralized_candidate_(ObAddr& cur_leader, ObAddr& new_leader)
{
  int ret = msg_pool_.get_eg_centralized_candidate(cur_leader, new_leader, T1_timestamp_);
  if (OB_FAIL(ret)) {
    need_print_trace_log_ = true;
  }
  REC_TRACE_EXT(tlog_, get_centralized_candidate, Y(ret), Y(cur_leader), Y(new_leader));
  return ret;
}

int ObElectionGroup::get_unconfirmed_leader_info(ObAddr& unconfirmed_leader, lease_t& unconfirmed_leader_lease) const
{
  int ret = OB_SUCCESS;

  RLockGuard guard(lock_);
  unconfirmed_leader = unconfirmed_leader_;
  unconfirmed_leader_lease = unconfirmed_leader_lease_;

  return ret;
}

int ObElectionGroup::get_leader_lease_info(const ObPartitionKey& pkey, const int64_t target_idx,
    const int64_t part_cur_ts, int64_t& eg_lease_end, int64_t& eg_takeover_t1_ts) const
{
  // called from election
  int ret = OB_SUCCESS;
  int64_t real_idx = target_idx;

  RLockGuard guard(lock_);
  if (!pkey.is_valid() || target_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(pkey), K_(eg_id));
  } else if (partition_array_.count() != part_state_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(ERROR, "two array count not match", K(ret), "election_group", *this);
  } else {
    const bool is_lease_expired = part_leader_lease_is_expired_unlock_(part_cur_ts, pkey, target_idx, eg_lease_end);
    UNUSED(is_lease_expired);

    if (is_all_part_merged_in_) {
      // eg_lease_end = is_lease_expired ? 0 : leader_lease_.second;
      eg_takeover_t1_ts = takeover_t1_timestamp_;
    } else {
      if (target_idx >= partition_array_.count() ||
          (target_idx < partition_array_.count() && partition_array_[target_idx] != pkey)) {
        // target_idx invalid(some partitions moved out, element behind them will shift left), retry
        if (!is_partition_exist_unlock_(pkey, real_idx)) {
          ret = OB_PARTITION_NOT_EXIST;
        }
      }
      if (OB_SUCC(ret)) {
        if (real_idx >= part_state_array_.count()) {
          FORCE_ELECT_LOG(
              WARN, "invalid real_idx", K(real_idx), K(target_idx), K(pkey), K(partition_array_), K(part_state_array_));
        } else {
          // eg_lease_end = is_lease_expired ? 0 : part_state_array_[real_idx].lease_end_;
          eg_takeover_t1_ts = part_state_array_[real_idx].takeover_t1_timestamp_;
        }
      }
    }
  }

  return ret;
}

bool ObElectionGroup::part_leader_lease_is_expired(
    const int64_t part_cur_ts, const ObPartitionKey& pkey, const int64_t target_idx, int64_t& out_lease_end) const
{
  // called from election, checking if lease has been expired
  RLockGuard guard(lock_);
  return part_leader_lease_is_expired_unlock_(part_cur_ts, pkey, target_idx, out_lease_end);
}

bool ObElectionGroup::part_leader_lease_is_expired_unlock_(
    const int64_t part_cur_ts, const ObPartitionKey& pkey, const int64_t target_idx, int64_t& out_lease_end) const
{
  bool bool_ret = true;
  const int64_t cur_ts = get_current_ts();
  if (is_all_part_merged_in_) {
    bool_ret = (cur_ts > leader_lease_.second);
    out_lease_end = bool_ret ? 0 : leader_lease_.second;
  } else {
    if (!pkey.is_valid() || target_idx < 0) {
      FORCE_ELECT_LOG(WARN, "invalid argument", K(pkey), K(target_idx), "election_group", *this);
    } else if (partition_array_.count() != part_state_array_.count()) {
      FORCE_ELECT_LOG(ERROR, "two array count not match", "election_group", *this);
    } else {
      bool is_part_exist = false;
      int64_t real_idx = -1;
      if (target_idx >= partition_array_.count() ||
          (target_idx < partition_array_.count() && partition_array_[target_idx] != pkey)) {
        is_part_exist = is_partition_exist_unlock_(pkey, real_idx);
      } else {
        // partition_array_[target_idx] == pkey
        real_idx = target_idx;
        is_part_exist = true;
      }
      if (!is_part_exist) {
        FORCE_ELECT_LOG(ERROR,
            "partition not exist",
            K(real_idx),
            K(pkey),
            K(target_idx),
            K_(eg_id),
            K(partition_array_),
            K(part_state_array_));
      } else {
        bool_ret = (part_cur_ts > part_state_array_[real_idx].lease_end_);
        out_lease_end = bool_ret ? 0 : part_state_array_[real_idx].lease_end_;
      }
    }
  }

  return bool_ret;
}

int ObElectionGroup::check_all_part_leader_lease_(
    const int64_t cur_ts, bool& is_all_expired, ObPartitionArray& expired_part_array) const
{
  // check all partitions' lease in group
  // will be called when is_all_part_merged_in_=false
  int ret = OB_SUCCESS;
  is_all_expired = true;

  expired_part_array.reset();
  if (is_all_part_merged_in_) {
    ret = OB_STATE_NOT_MATCH;
    FORCE_ELECT_LOG(WARN, "is_all_part_merged_in_ is true", K(ret), "election_group", *this);
  } else if (partition_array_.count() != part_state_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    FORCE_ELECT_LOG(ERROR, "two array count not match", K(ret), "election_group", *this);
  } else if (is_empty_()) {
    // group is empty, returned true
  } else {
    // group is not empty, check every partition's lease
    const int64_t part_array_cnt = partition_array_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < part_array_cnt; ++i) {
      const ObPartitionKey& tmp_pkey = partition_array_.at(i);
      const int64_t tmp_lease_end = part_state_array_.at(i).lease_end_;
      if (cur_ts <= tmp_lease_end) {
        // not expired
        is_all_expired = false;
      } else if (OB_FAIL(expired_part_array.push_back(tmp_pkey))) {
        FORCE_ELECT_LOG(WARN, "array push_back failed", K(ret), K_(eg_id));
      }
    }
  }

  return ret;
}

bool ObElectionGroup::eg_lease_is_completely_expired_(const int64_t cur_ts, bool& is_all_dropped) const
{
  bool bool_ret = true;

  if (is_all_part_merged_in_) {
    bool_ret = (cur_ts > leader_lease_.second);
  } else {
    int tmp_ret = OB_SUCCESS;
    bool is_all_expired = false;
    ObPartitionArray expired_part_array;
    if (OB_SUCCESS != (tmp_ret = check_all_part_leader_lease_(cur_ts, is_all_expired, expired_part_array))) {
      FORCE_ELECT_LOG(WARN, "check_all_part_leader_lease_ failed", K(tmp_ret));
    } else if (false == is_all_expired) {
      bool_ret = false;
    } else {
      bool_ret = true;
      // check if all partitions have been dropped
      int tmp_ret = OB_SUCCESS;
      is_all_dropped = true;
      const int64_t part_array_cnt = expired_part_array.count();
      for (int64_t i = 0; is_all_dropped && i < part_array_cnt; ++i) {
        const ObPartitionKey& tmp_pkey = expired_part_array.at(i);
        bool is_exist = true;
        if (OB_SUCCESS !=
            (tmp_ret = storage::ObPartitionService::get_instance().check_partition_exist(tmp_pkey, is_exist))) {
          FORCE_ELECT_LOG(WARN, "check_partition_exist failed", K(tmp_pkey), K(tmp_ret));
        } else if (is_exist) {
          is_all_dropped = false;
        } else {
        }
      }
      if (is_all_dropped) {
        // eg is empty or all partitions have been dropped
        ELECT_ASYNC_LOG(INFO, "self is empty or all partitions are offline", K(is_all_dropped), K(*this));
      }
    }
  }

  return bool_ret;
}

int ObElectionGroup::set_unconfirmed_leader_lease_(const int64_t start, const int64_t end)
{
  int ret = OB_SUCCESS;

  if (start <= 0 || end <= 0 || start >= end) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(start), K(end));
  } else {
    //  unconfirmed_leader_lease_.first = start;
    unconfirmed_leader_lease_.second = end;
  }

  return ret;
}

bool ObElectionGroup::unconfirmed_leader_lease_is_expired_(const int64_t cur_ts) const
{
  // check lease.end only
  return (cur_ts > unconfirmed_leader_lease_.second);
}

void ObElectionGroup::set_unconfirmed_leader_(const ObAddr& unconfirmed_leader)
{
  unconfirmed_leader_ = unconfirmed_leader;
}

int ObElectionGroup::post_election_msg_(const ObMemberList& mlist, const ObElectionMsg& msg)
{
  int ret = OB_SUCCESS;
  ObAddr server;

  if (mlist.get_member_number() <= 0 || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(mlist), K(msg), K(eg_id_));
  } else {
    for (int64_t i = 0; i < mlist.get_member_number() && OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(mlist.get_server_by_index(i, server))) {
        FORCE_ELECT_LOG(WARN, "get member error", K(ret), "index", i);
      } else if (!server.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        FORCE_ELECT_LOG(WARN, "server is invalid", K(server));
      } else if (server == self_) {
        // skip self
      } else if (OB_FAIL(post_election_msg_(server, msg))) {
        // do nothing
      } else {
        ELECT_ASYNC_LOG(DEBUG, "send election message success", K(msg), K(server));
      }
    }
  }

  return ret;
}

int ObElectionGroup::check_array_buf_()
{
  int ret = OB_SUCCESS;
  if (array_serialized_buf_.get_eg_version() == eg_version_ && false == ATOMIC_LOAD(&need_update_array_buf_)) {
    // version match, ignore
  } else if (OB_FAIL(array_serialized_buf_.update_content(partition_array_))) {
    FORCE_ELECT_LOG(WARN, "array_serialized_buf_.update_content failed", K(ret));
  } else {
    array_serialized_buf_.set_eg_version(eg_version_);
    ATOMIC_STORE(&need_update_array_buf_, false);
  }
  return ret;
}

int ObElectionGroup::post_election_msg_(const ObAddr& server, const ObElectionMsg& msg)
{
  int ret = OB_SUCCESS;

  const int64_t dst_cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  if (!server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(ret), K(server), K(msg), K(eg_id_));
  } else if (OB_FAIL(check_array_buf_())) {
    FORCE_ELECT_LOG(WARN, "check_array_buf_ failed", K(ret), K(server), K(msg), K(eg_id_));
  } else if (OB_FAIL(rpc_->post_election_group_msg(server, dst_cluster_id, eg_id_, array_serialized_buf_, msg))) {
  } else {
    ELECT_ASYNC_LOG(DEBUG, "send election message success", K(server), K(msg));
  }

  return ret;
}

bool ObElectionGroup::is_candidate_(const ObAddr& server) const
{
  return sorted_member_list_.contains(server);
}

bool ObElectionGroup::is_real_leader_(const ObAddr& server) const
{
  bool bool_ret = false;
  const int64_t cur_ts = get_current_ts();
  bool unused_bool = false;

  if (!server.is_valid()) {
    bool_ret = false;
    FORCE_ELECT_LOG(WARN, "invalid argument", K(server));
  } else if (self_ == server) {
    bool_ret =
        ((curr_leader_ == server) && (ROLE_LEADER == role_) && (!eg_lease_is_completely_expired_(cur_ts, unused_bool)));
  } else {
    bool_ret = ((curr_leader_ == server) && (!eg_lease_is_completely_expired_(cur_ts, unused_bool)));
  }

  return bool_ret;
}

bool ObElectionGroup::in_reappoint_period_(const int64_t ts)
{
  bool bool_ret = true;
  if (is_real_leader_(self_)) {
    bool_ret = (ts - takeover_t1_timestamp_) >= (static_cast<int64_t>(vote_period_) * T_ELECT2);
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

int ObElectionGroup::get_election_group_info(ObElectionGroupInfo& eg_info) const
{
  RLockGuard guard(lock_);
  eg_info.set_is_running(is_running_);
  eg_info.set_eg_id(eg_id_);
  eg_info.set_eg_version(eg_version_);
  eg_info.set_eg_part_cnt(partition_array_.count());
  eg_info.set_is_all_part_merged_in(is_all_part_merged_in_);
  eg_info.set_is_priority_allow_reappoint(true);
  eg_info.set_self_addr(self_);
  eg_info.set_tenant_id(tenant_id_);
  eg_info.set_priority(self_priority_);
  eg_info.set_current_leader(curr_leader_);
  eg_info.set_member_list(sorted_member_list_);
  eg_info.set_replica_num(replica_num_);
  eg_info.set_takeover_t1_timestamp(takeover_t1_timestamp_);
  eg_info.set_T1_timestamp(T1_timestamp_);
  eg_info.set_leader_lease(leader_lease_);
  eg_info.set_role(role_);
  eg_info.set_state(state_);
  eg_info.set_pre_destroy_state(pre_destroy_state_);
  return OB_SUCCESS;
}

}  // namespace election
}  // namespace oceanbase
