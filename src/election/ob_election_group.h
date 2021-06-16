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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_GROUP_
#define OCEANBASE_ELECTION_OB_ELECTION_GROUP_

#include <stdint.h>
#include "lib/net/ob_addr.h"
#include "lib/list/ob_list.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/utility/utility.h"
#include "common/ob_member_list.h"
#include "ob_election.h"
#include "ob_election_group_id.h"
#include "ob_election_group_priority.h"
#include "ob_election_base.h"
#include "ob_election_rpc.h"
#include "ob_election_msg.h"
#include "ob_election_group_msg_pool.h"
#include "ob_election_timer.h"
#include "ob_election_part_array_buf.h"

namespace oceanbase {
namespace obrpc {
class ObElectionRpcProxy;
class ObElectionRpcResult;
}  // namespace obrpc

namespace common {
class ObTimeWheel;
class SpinRLockGuard;
class SpinWLockGuard;
}  // namespace common

namespace election {
class ObIElectionMgr;
class ObIElectionGroupMgr;
class ObElectionGroupInfo;
class ObElectionGroupCache;

class ObIElectionGroup : public common::LinkHashValue<election::ObElectionGroupId> {
public:
  ObIElectionGroup()
  {}
  virtual ~ObIElectionGroup()
  {}
  virtual int init(const ObElectionGroupId& eg_id, const common::ObAddr& self, const uint64_t tenant_id,
      const common::ObMemberList& sorted_member_list, const int64_t replica_num, ObIElectionRpc* rpc,
      common::ObTimeWheel* tw, ObIElectionMgr* election_mgr, ObElectionGroupCache* election_group_cache,
      ObIElectionGroupPriorityGetter* eg_cb, ObIElectionGroupMgr* eg_mgr) = 0;
  virtual int init(const ObElectionGroupId& eg_id, const common::ObAddr& self, const uint64_t tenant_id,
      ObIElectionRpc* rpc, common::ObTimeWheel* tw, ObIElectionMgr* election_mgr,
      ObElectionGroupCache* election_group_cache, ObIElectionGroupPriorityGetter* eg_cb,
      ObIElectionGroupMgr* eg_mgr) = 0;
  virtual void destroy() = 0;
  virtual int start() = 0;
  virtual int stop() = 0;

public:
  virtual bool is_need_check_eg_version(const int64_t msg_eg_version) const = 0;
  virtual int check_eg_version(const int64_t msg_eg_version, const common::ObPartitionArray& partition_array,
      common::ObPartitionArray& pending_move_out_array, common::ObPartitionArray& pending_move_in_array) = 0;
  virtual int try_update_eg_version(const int64_t msg_eg_version, const common::ObPartitionArray& partition_array) = 0;
  virtual int move_in_partition(const common::ObPartitionKey& pkey, const lease_t part_lease,
      const int64_t takeover_t1_timestamp, int64_t& ret_idx) = 0;
  virtual bool is_pre_destroy_state() = 0;
  virtual int move_out_partition(const common::ObPartitionKey& pkey, const int64_t target_idx) = 0;
  virtual int handle_vote_prepare(
      const ObElectionMsgEGPrepare& msg, const int64_t msg_eg_version, obrpc::ObElectionRpcResult& result) = 0;
  virtual int handle_vote_vote(const ObElectionMsgEGVote& msg, const int64_t msg_eg_version,
      const bool is_array_deserialized, const char* buf, const int64_t buf_len, const int64_t array_start_pos,
      common::ObPartitionArray& msg_part_array, obrpc::ObElectionRpcResult& result) = 0;
  virtual int handle_vote_success(const ObElectionMsgEGSuccess& msg, const int64_t msg_eg_version,
      const bool is_array_deserialized, const char* buf, const int64_t buf_len, const int64_t array_start_pos,
      common::ObPartitionArray& msg_part_array, common::ObPartitionArray& move_in_failed_array,
      obrpc::ObElectionRpcResult& result) = 0;
  virtual int handle_prepare_destroy_msg(
      const ObElectionMsg& msg, const int64_t msg_eg_version, obrpc::ObElectionRpcResult& result) = 0;

public:
  virtual const common::ObAddr& get_curr_leader() const = 0;
  virtual const ObElectionRole& get_role() const = 0;
  virtual int64_t get_replica_num() const = 0;
  virtual const ObElectionGroupId& get_eg_id() const = 0;
  virtual int64_t get_eg_version() const = 0;
  virtual int get_unconfirmed_leader_info(
      common::ObAddr& unconfirmed_leader, lease_t& unconfirmed_leader_lease) const = 0;
  virtual bool is_full() const = 0;
  virtual bool is_empty() const = 0;
  virtual int64_t get_partition_count() const = 0;
  virtual bool part_leader_lease_is_expired(const int64_t part_cur_ts, const common::ObPartitionKey& pkey,
      const int64_t target_idx, int64_t& out_lease_end) const = 0;
  virtual int get_leader_lease_info(const common::ObPartitionKey& pkey, const int64_t target_idx,
      const int64_t part_cur_ts, int64_t& eg_lease_end, int64_t& eg_takeover_t1_ts) const = 0;
  virtual int64_t get_T1_timestamp() const = 0;
  virtual uint64_t hash() const = 0;
  virtual int prepare_destroy() = 0;
  virtual const common::ObMemberList& get_member_list() const = 0;
  virtual uint64_t get_tenant_id() const = 0;
  virtual int get_election_group_info(ObElectionGroupInfo& eg_info) const = 0;
};

class ObElectionGroup : public ObIElectionGroup, public ObIElectionTimerP {
public:
  ObElectionGroup()
      : lock_(common::ObLatchIds::ELECTION_GROUP_LOCK),
        tlog_(true, common::ObLatchIds::ELECTION_GROUP_TRACE_RECORDER_LOCK)
  {
    reset();
  }
  ~ObElectionGroup()
  {
    destroy();
  }
  int init(const ObElectionGroupId& eg_id, const common::ObAddr& self, const uint64_t tenant_id,
      const common::ObMemberList& sorted_member_list, const int64_t replica_num, ObIElectionRpc* rpc,
      common::ObTimeWheel* tw, ObIElectionMgr* election_mgr, ObElectionGroupCache* election_group_cache,
      ObIElectionGroupPriorityGetter* eg_cb, ObIElectionGroupMgr* eg_mgr);
  int init(const ObElectionGroupId& eg_id, const common::ObAddr& self, const uint64_t tenant_id, ObIElectionRpc* rpc,
      common::ObTimeWheel* tw, ObIElectionMgr* election_mgr, ObElectionGroupCache* election_group_cache,
      ObIElectionGroupPriorityGetter* eg_cb, ObIElectionGroupMgr* eg_mgr);
  void destroy();
  void reset();
  int start();
  int stop();
  int prepare_destroy();  // move out all partitions and set pre-destory state
  const common::ObMemberList& get_member_list() const
  {
    return sorted_member_list_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int get_election_group_info(ObElectionGroupInfo& eg_info) const;
  int64_t get_partition_count() const
  {
    return partition_array_.count();
  }
  bool is_pre_destroy_state()
  {
    return (true == ATOMIC_LOAD(&pre_destroy_state_));
  }
  bool is_need_check_eg_version(const int64_t msg_eg_version) const;
  int check_eg_version(const int64_t msg_eg_version, const common::ObPartitionArray& partition_array,
      common::ObPartitionArray& pending_move_out_array, common::ObPartitionArray& pending_move_in_array);
  int try_update_eg_version(const int64_t msg_eg_version, const common::ObPartitionArray& partition_array);
  int move_in_partition(const common::ObPartitionKey& pkey, const lease_t part_lease,
      const int64_t takeover_t1_timestamp, int64_t& ret_idx);
  int move_out_partition(const common::ObPartitionKey& pkey, const int64_t target_idx);
  int handle_vote_prepare(
      const ObElectionMsgEGPrepare& msg, const int64_t msg_eg_version, obrpc::ObElectionRpcResult& result);
  int handle_vote_vote(const ObElectionMsgEGVote& msg, const int64_t msg_eg_version, const bool is_array_deserialized,
      const char* buf, const int64_t buf_len, const int64_t array_start_pos, common::ObPartitionArray& msg_part_array,
      obrpc::ObElectionRpcResult& result);
  int handle_vote_success(const ObElectionMsgEGSuccess& msg, const int64_t msg_eg_version,
      const bool is_array_deserialized, const char* buf, const int64_t buf_len, const int64_t array_start_pos,
      common::ObPartitionArray& msg_part_array, common::ObPartitionArray& move_in_failed_array,
      obrpc::ObElectionRpcResult& result);
  int handle_prepare_destroy_msg(
      const ObElectionMsg& msg, const int64_t msg_eg_version, obrpc::ObElectionRpcResult& result);
  int64_t get_replica_num() const
  {
    return replica_num_;
  }
  const common::ObAddr& get_curr_leader() const
  {
    return curr_leader_;
  }
  const ObElectionRole& get_role() const
  {
    return role_;
  }
  bool part_leader_lease_is_expired(const int64_t part_cur_ts, const common::ObPartitionKey& pkey,
      const int64_t target_idx, int64_t& out_lease_end) const;
  int get_leader_lease_info(const common::ObPartitionKey& pkey, const int64_t target_idx, const int64_t part_cur_ts,
      int64_t& eg_lease_end, int64_t& eg_takeover_t1_ts) const;
  int64_t get_T1_timestamp() const
  {
    return T1_timestamp_;
  }

public:
  uint64_t hash() const
  {
    return eg_id_.hash();
  }
  int64_t get_current_ts() const;
  void run_gt1_task(const int64_t expect_ts);
  // gt2 not used
  void run_gt2_task(const int64_t expect_ts)
  {
    UNUSED(expect_ts);
  }
  void run_gt3_task(const int64_t expect_ts);  // count tickets
  void run_gt4_task(const int64_t expect_ts);
  void run_change_leader_task(const int64_t expect_ts)
  {
    UNUSED(expect_ts);
  }
  void run_change_leader_task_V2(const int64_t expect_ts)
  {
    UNUSED(expect_ts);
  }
  const ObElectionGroupId& get_eg_id() const
  {
    return eg_id_;
  }
  int64_t get_eg_version() const
  {
    return eg_version_;
  }
  int get_unconfirmed_leader_info(common::ObAddr& unconfirmed_leader, lease_t& unconfirmed_leader_lease) const;
  bool is_full() const;
  bool is_empty() const;

  TO_STRING_AND_YSON(Y_(is_running), OB_ID(eg_part_cnt), partition_array_.count(), Y_(eg_id), Y_(eg_version),
      Y_(pre_destroy_state), Y_(is_all_part_merged_in), Y_(self), OB_ID(cur_leader), curr_leader_,
      Y_(sorted_member_list), Y_(tenant_id), OB_ID(lease_start), leader_lease_.first, OB_ID(lease_end),
      leader_lease_.second, Y_(active_timestamp), Y_(T1_timestamp), Y_(state), Y_(role), Y_(replica_num),
      Y_(unconfirmed_leader), Y_(takeover_t1_timestamp), Y_(vote_period), Y_(lease_time));

private:
  void flush_trace_buf_();
  bool lease_expired_too_long_(const int64_t cur_ts) const;
  int try_start_();
  bool is_full_() const;
  bool is_empty_() const;
  bool is_part_array_equal_(const common::ObPartitionArray& partition_array) const;
  bool is_pkey_exist_(const common::ObPartitionKey& pkey) const;
  bool is_partition_exist_unlock_(const common::ObPartitionKey& pkey, int64_t& idx) const;
  bool is_pkey_in_array_(
      const common::ObPartitionKey& pkey, const common::ObPartitionArray& partition_array, int64_t& idx) const;
  int leader_check_partition_array_(const int64_t follower_version, const common::ObPartitionArray& partition_array,
      common::ObPartitionArray& notify_part_array);
  int gen_pending_move_part_array_(const int64_t msg_eg_version, const common::ObPartitionArray& partition_array,
      common::ObPartitionArray& pending_move_in_array, common::ObPartitionArray& pending_move_out_array);
  int batch_move_out_partition_(const common::ObPartitionArray& pending_move_out_array);
  bool is_pre_destroy_state_();
  int notify_partition_vote_vote_(const ObElectionMsgEGVote& msg, const common::ObPartitionArray& notify_part_array);
  int leader_reappoint_(const bool need_update_eg_lease, const ObPartIdxArray& majority_part_idx_array,
      const common::ObAddr& leader, const int64_t t1, const int64_t lease_time);
  int leader_elected_(const common::ObAddr& leader, const int64_t t1, const int64_t lease_time);
  void leader_revoke_();
  bool in_reappoint_period_(const int64_t ts);
  bool is_candidate_(const common::ObAddr& server) const;
  bool is_real_leader_(const common::ObAddr& server) const;
  int try_stop_();
  bool is_active_(const int64_t cur_ts) const
  {
    return cur_ts >= active_timestamp_;
  }
  bool run_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const;
  bool vote_run_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const;
  int get_eg_centralized_candidate_(common::ObAddr& cur_leader, common::ObAddr& new_leader);
  int try_centralized_voting_(const int64_t lease_time);
  int check_centralized_majority_();
  void set_unconfirmed_leader_(const common::ObAddr& unconfirmed_leader);
  int set_unconfirmed_leader_lease_(const int64_t start, const int64_t end);
  bool unconfirmed_leader_lease_is_expired_(const int64_t cur_ts) const;
  bool eg_leader_lease_is_expired_(const int64_t cur_ts) const;
  int check_array_buf_();
  int post_election_msg_(const common::ObMemberList& mlist, const ObElectionMsg& msg);
  int post_election_msg_(const common::ObAddr& server, const ObElectionMsg& msg);
  int push_lease_to_part_stat_();
  int push_lease_to_part_skip_pkey_(const common::ObPartitionKey& skip_pkey);
  int get_majority_part_idx_array_(ObPartIdxArray& majority_part_idx_array);
  bool is_pkey_in_majority_array_(const common::ObPartitionKey& tmp_pkey,
      const common::ObPartitionArray& msg_part_array, const ObPartIdxArray& msg_majority_part_idx_array) const;
  int parse_majority_part_idx_array_(const bool msg_all_part_merged_in,
      const ObPartIdxArray& msg_majority_part_idx_array, const common::ObPartitionArray& msg_part_array,
      ObPartIdxArray& majority_idx_array) const;
  void clear_part_stat_vote_cnt_();
  int leader_get_expired_part_array_(const int64_t cur_ts, common::ObPartitionArray& pending_move_out_array);
  int notify_follower_destroy_();
  int check_all_part_leader_lease_(
      const int64_t cur_ts, bool& is_all_expired, common::ObPartitionArray& expired_part_array) const;
  bool eg_lease_is_completely_expired_(const int64_t cur_ts, bool& is_all_dropped) const;
  int get_priority_(ObElectionGroupPriority& priority) const;
  int send_vote_prepare_(const ObElectionMsgEGPrepare& msg);
  int send_vote_vote_(const ObElectionMsgEGVote& msg);
  int send_vote_success_(const ObElectionMsgEGSuccess& msg);
  int process_vote_prepare_(const ObElectionMsgEGPrepare& msg, const int64_t msg_eg_version);
  int local_handle_vote_prepare_(const ObElectionMsgEGPrepare& msg);
  int local_handle_vote_vote_(const ObElectionMsgEGVote& msg);
  int local_handle_vote_success_(const ObElectionMsgEGSuccess& msg);
  bool part_leader_lease_is_expired_unlock_(const int64_t part_cur_ts, const common::ObPartitionKey& pkey,
      const int64_t target_idx, int64_t& out_lease_end) const;

private:
#if defined(__x86_64__)
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;
  mutable common::SpinRWLock lock_;
#elif defined(__aarch64__)
  typedef common::RWLock::RLockGuard RLockGuard;
  typedef common::RWLock::WLockGuard WLockGuard;
  mutable common::RWLock lock_;
#endif
private:
  common::ObTraceEventRecorder tlog_;
  bool is_inited_;
  bool is_running_;
  ObEGVoteMsgPool msg_pool_;
  ObIElectionRpc* rpc_;
  ObElectionTimer timer_;
  ObIElectionMgr* election_mgr_;
  ObElectionGroupCache* election_group_cache_;
  ObIElectionGroupPriorityGetter* eg_cb_;
  ObIElectionGroupMgr* eg_mgr_;
  // election group related
  ObElectionGroupId eg_id_;
  int64_t eg_version_;
  int64_t last_seen_max_eg_version_;  // the max version has been processed
  bool is_all_part_merged_in_;
  common::ObPartitionArray partition_array_;  // partition key list
  ObPartStateArray part_state_array_;         // partition state list
  common::ObAddr self_;
  uint64_t tenant_id_;
  ObElectionGroupPriority self_priority_;
  common::ObAddr curr_leader_;
  common::ObMemberList sorted_member_list_;  // valid only on leader
  int64_t replica_num_;                      // valid only on leader
  int64_t takeover_t1_timestamp_;
  int64_t T1_timestamp_;
  lease_t leader_lease_;
  ObElectionRole role_;
  int32_t state_;
  int64_t start_timestamp_;
  int64_t active_timestamp_;
  common::ObAddr unconfirmed_leader_;
  lease_t unconfirmed_leader_lease_;
  int32_t vote_period_;
  int64_t lease_time_;
  bool need_print_trace_log_;
  bool ignore_log_;
  bool pre_destroy_state_;
  int64_t first_gt4_ts_;
  ObPartArrayBuffer array_serialized_buf_;
  bool need_update_array_buf_;
};

}  // namespace election
}  // namespace oceanbase
#endif  // OCEANBASE_ELECTION_OB_ELECTION_GROUP_
