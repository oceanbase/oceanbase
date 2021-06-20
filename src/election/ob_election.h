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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_
#define OCEANBASE_ELECTION_OB_ELECTION_

#include <stdint.h>
#include "lib/net/ob_addr.h"
#include "lib/list/ob_list.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/utility/utility.h"
#include "common/ob_member_list.h"
#include "ob_election_cb.h"
#include "ob_election_base.h"
#include "ob_election_event_history.h"
#include "ob_election_group_id.h"
#include "ob_election_info.h"
#include "ob_election_msg.h"
#include "ob_election_msg_pool.h"
#include "ob_election_rpc.h"
#include "ob_election_timer.h"
#include "ob_election_time_def.h"

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
class ObElectionPriority;
class ObElectionRpc;
class ObIElectionGroupMgr;
class ObIElectionGroup;

/**********should removed after a barrier version bigger than 3.1**********/
enum class PhysicalCondition { HEALTHY = 0, INFECTED = 1, SICK = 2, DEAD = 3 };
/**************************************************************************************************/

class ObIElection : public common::LinkHashValue<common::ObPartitionKey> {
public:
  ObIElection()
  {}
  virtual ~ObIElection()
  {}
  virtual int init(const common::ObPartitionKey& partition, const common::ObAddr& self, ObIElectionRpc* rpc,
      common::ObTimeWheel* tw, const int64_t replica_num, ObIElectionCallback* election_cb, ObIElectionGroupMgr* eg_mgr,
      ObElectionEventHistoryArray* event_history_array) = 0;
  virtual void destroy() = 0;
  virtual int start() = 0;
  virtual int stop() = 0;

public:
  virtual int handle_devote_prepare(const ObElectionMsgDEPrepare& msg, obrpc::ObElectionRpcResult& result) = 0;
  virtual int handle_devote_vote(const ObElectionMsgDEVote& msg, obrpc::ObElectionRpcResult& result) = 0;
  virtual int handle_devote_success(const ObElectionMsgDESuccess& msg, obrpc::ObElectionRpcResult& result) = 0;
  virtual int handle_vote_prepare(const ObElectionMsgPrepare& msg, obrpc::ObElectionRpcResult& result) = 0;
  virtual int handle_vote_vote(const ObElectionMsgVote& msg, obrpc::ObElectionRpcResult& result) = 0;
  virtual int handle_vote_success(const ObElectionMsgSuccess& msg, obrpc::ObElectionRpcResult& result) = 0;
  virtual int handle_query_leader(const ObElectionQueryLeader& msg, obrpc::ObElectionRpcResult& result) = 0;
  virtual int handle_query_leader_response(
      const ObElectionQueryLeaderResponse& msg, obrpc::ObElectionRpcResult& result) = 0;

public:
  virtual int set_candidate(
      const int64_t replica_num, const common::ObMemberList& curr_mlist, const int64_t membership_version) = 0;
  virtual int change_leader_async(const common::ObAddr& leader, common::ObTsWindows& changing_leader_windows) = 0;
  virtual int change_leader_to_self_async() = 0;
  virtual int force_leader_async() = 0;
  virtual int get_curr_candidate(common::ObMemberList& mlist) const = 0;
  virtual int get_valid_candidate(common::ObMemberList& mlist) const = 0;
  virtual int get_leader(common::ObAddr& leader, common::ObAddr& previous_leader, int64_t& leader_epoch,
      bool& is_elected_by_changing_leader) const = 0;
  virtual int get_leader(common::ObAddr& leader, int64_t& leader_epoch, bool& is_elected_by_changing_leader,
      common::ObTsWindows& changing_leader_windows) const = 0;
  virtual int get_current_leader(common::ObAddr& leader) const = 0;
  virtual int get_election_info(ObElectionInfo& election_info) const = 0;
  virtual void leader_revoke(const uint32_t revoke_type) = 0;
  virtual int inc_replica_num() = 0;
  virtual int dec_replica_num() = 0;
  virtual int set_replica_num(const int64_t replica_num) = 0;
  virtual int64_t get_election_time_offset() const = 0;

public:
  virtual int leader_takeover(const common::ObAddr& leader, const int64_t lease_start, int64_t& leader_epoch) = 0;
  virtual int64_t get_current_ts() const = 0;
  virtual int get_priority(ObElectionPriority& priority) const = 0;
  virtual int get_timestamp(int64_t& gts, common::ObAddr& leader) const = 0;
  virtual int64_t get_last_leader_revoke_time() const = 0;

public:
  // for election group
  virtual int move_out_election_group(const ObElectionGroupId& eg_id) = 0;
  virtual int move_into_election_group(const ObElectionGroupId& eg_id) = 0;
  virtual const lease_t& get_leader_lease() const = 0;
  virtual int64_t get_replica_num() const = 0;
  virtual uint64_t get_eg_id_hash() const = 0;
};

class ObIElectionTimerP {
public:
  ObIElectionTimerP()
  {}
  virtual ~ObIElectionTimerP()
  {}

public:
  virtual uint64_t hash() const = 0;
  virtual int64_t get_current_ts() const = 0;
  virtual void run_gt1_task(const int64_t expect_ts) = 0;
  virtual void run_gt2_task(const int64_t expect_ts) = 0;
  virtual void run_gt3_task(const int64_t expect_ts) = 0;
  virtual void run_gt4_task(const int64_t expect_ts) = 0;
  virtual void run_change_leader_task(const int64_t expect_ts) = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
};

class ObElection : public ObIElection, public ObIElectionTimerP, public ObElectionInfo {
  typedef ObElectionEventHistory::EventType EventType;

public:
  ObElection() : lock_(common::ObLatchIds::ELECTION_LOCK)
  {
    reset();
  }
  ~ObElection()
  {
    destroy();
  }
  int init(const common::ObPartitionKey& partition, const common::ObAddr& self, ObIElectionRpc* rpc,
      common::ObTimeWheel* tw, const int64_t replica_num, ObIElectionCallback* election_cb, ObIElectionGroupMgr* eg_mgr,
      ObElectionEventHistoryArray* event_history_array) override;
  void destroy() override;
  void reset();
  int start() override;
  int stop() override;

public:
  int handle_devote_prepare(const ObElectionMsgDEPrepare& msg, obrpc::ObElectionRpcResult& result) override;
  int handle_devote_vote(const ObElectionMsgDEVote& msg, obrpc::ObElectionRpcResult& result) override;
  int handle_devote_success(const ObElectionMsgDESuccess& msg, obrpc::ObElectionRpcResult& result) override;
  int handle_vote_prepare(const ObElectionMsgPrepare& msg, obrpc::ObElectionRpcResult& result) override;
  int handle_vote_vote(const ObElectionMsgVote& msg, obrpc::ObElectionRpcResult& result) override;
  int handle_vote_success(const ObElectionMsgSuccess& msg, obrpc::ObElectionRpcResult& result) override;
  int handle_query_leader(const ObElectionQueryLeader& msg, obrpc::ObElectionRpcResult& result) override;
  int handle_query_leader_response(
      const ObElectionQueryLeaderResponse& msg, obrpc::ObElectionRpcResult& result) override;

public:
  int set_candidate(
      const int64_t replica_num, const common::ObMemberList& curr_mlist, const int64_t membership_version) override;
  int change_leader_async(const common::ObAddr& leader, common::ObTsWindows& changing_leader_windows) override;
  int change_leader_to_self_async() override;
  int force_leader_async() override;
  int get_curr_candidate(common::ObMemberList& mlist) const override;
  int get_valid_candidate(common::ObMemberList& mlist) const override;
  int get_leader(common::ObAddr& leader, common::ObAddr& previous_leader, int64_t& leader_epoch,
      bool& is_elected_by_changing_leader) const override;
  int get_leader(common::ObAddr& leader, int64_t& leader_epoch, bool& is_elected_by_changing_leader,
      common::ObTsWindows& changing_leader_windows) const override;
  int get_current_leader(common::ObAddr& leader) const override;
  int get_election_info(ObElectionInfo& election_info) const override;
  void leader_revoke(const uint32_t revoke_type) override;
  int inc_replica_num() override;
  int dec_replica_num() override;
  int set_replica_num(const int64_t replica_num) override;
  int64_t get_election_time_offset() const override
  {
    return get_election_time_offset_();
  }
  static bool is_valid_leader_epoch(const int64_t leader_epoch)
  {
    return leader_epoch > 0;
  }

public:
  int leader_takeover(const common::ObAddr& leader, const int64_t lease_start, int64_t& leader_epoch) override;
  int64_t get_current_ts() const override;
  int get_priority(ObElectionPriority& priority) const override;
  int get_timestamp(int64_t& gts, common::ObAddr& leader) const override;
  int move_out_election_group(const ObElectionGroupId& eg_id) override;
  int move_into_election_group(const ObElectionGroupId& eg_id) override;
  int64_t get_replica_num() const override
  {
    return replica_num_;
  }
  uint64_t get_eg_id_hash() const override
  {
    return eg_id_.hash();
  }
  const lease_t& get_leader_lease() const override
  {
    return leader_lease_;
  }
  uint64_t hash() const override;
  int64_t get_last_leader_revoke_time() const override
  {
    return ATOMIC_LOAD(&last_leader_revoke_time_);
  }

public:
  TO_STRING_AND_YSON(Y_(partition), Y_(is_running), Y_(is_changing_leader), Y_(self), Y_(proposal_leader),
      OB_ID(cur_leader), current_leader_, Y_(curr_candidates), Y_(curr_membership_version), Y_(leader_lease),
      Y_(election_time_offset), Y_(active_timestamp), Y_(T1_timestamp), Y_(leader_epoch), Y_(state), Y_(role),
      Y_(stage), Y_(type), Y_(replica_num), Y_(unconfirmed_leader), Y_(unconfirmed_leader_lease),
      Y_(takeover_t1_timestamp), Y_(is_need_query), Y_(valid_candidates), Y_(change_leader_timestamp), Y_(ignore_log),
      Y_(leader_revoke_timestamp), Y_(vote_period), Y_(lease_time), Y_(move_out_timestamp), OB_ID(eg_part_array_idx),
      eg_part_array_idx_, OB_ID(eg_id_hash), eg_id_.hash());

public:
  void run_gt1_task(const int64_t expect_ts) override;
  void run_gt2_task(const int64_t expect_ts) override;
  void run_gt3_task(const int64_t expect_ts) override;
  void run_gt4_task(const int64_t expect_ts) override;
  void run_change_leader_task(const int64_t expect_ts) override;

public:
  /**********should removed after a barrier version bigger than 3.1**********/
  static bool IS_CLUSTER_MIN_VSERDION_LESS_THAN_3_1;
  /**************************************************************************************************/
  enum RevokeType {
    LEASE_EXPIRED = 0,
    NOT_CANDIDATE,
    DISK_ERROR,
    RECONFIRM_TIMEOUT,                // CLOG reconfirm timeout
    CLOG_SW_TIMEOUT,                  // CLOG sliding window timeout
    ROLE_CHANGE_TIMEOUT,              // CLOG role change timeout
    REPLICA_TYPE_DISALLOW,            // replica_type now allow to be leader
    MEMBER_LIST_DISALLOW,             // member_list not contain self
    PS_LEADER_ACTIVE_FAIL,            // partition_service leader_active fail
    CLUSTER_ROLE_SWITCH,              // all leader revoke when cluster role switching
    OFS_MIGRATE_REVOKE_SRC,           // src server's leader revoke in OFS mode
    TRANS_CB_ERROR,                   // leader need revoke when CLOG call transation's callback failed
    STANDBY_RESTORE_FAIL,             // standby restore fail
    SUBMIT_LOG_MEMORY_ALLOC_FAIL,     // submit_log memory alloc fail
    RESTORE_LEADER_SUBMIT_TASK_FAIL,  // standby restore fail`
    CLOG_DISK_FULL,                   // CLOG disk full
    CLOG_DISK_HANG,                   // CLOG DISK HANG
    SHARED_STORAGE_LEASE_EXPIRED,     // lease expired in shared storage mode
    REVOKE_TYPE_MAX
  };
  static const char* const REVOKE_REASON_STR[RevokeType::REVOKE_TYPE_MAX];
  class RevokeTypeChecker {
  public:
    static bool is_unexpected_revoke_type(const uint32_t revoke_type)
    {
      return (CLUSTER_ROLE_SWITCH != revoke_type && STANDBY_RESTORE_FAIL != revoke_type &&
              RECONFIRM_TIMEOUT != revoke_type);
    }
  };

private:
  int process_devote_prepare_(const ObElectionMsgDEPrepare& msg);
  int process_devote_vote_(const ObElectionMsgDEVote& msg);
  int process_devote_success_(const ObElectionMsgDESuccess& msg);
  int process_vote_prepare_(const ObElectionMsgPrepare& msg);
  int process_vote_vote_(const ObElectionMsgVote& msg);
  int process_vote_success_(const ObElectionMsgSuccess& msg);
  int local_handle_devote_prepare_(const ObElectionMsgDEPrepare& msg);
  int local_handle_devote_vote_(const ObElectionMsgDEVote& msg);
  int local_handle_devote_success_(const ObElectionMsgDESuccess& msg);
  int local_handle_vote_prepare_(const ObElectionMsgPrepare& msg);
  int local_handle_vote_vote_(const ObElectionMsgVote& msg);
  int local_handle_vote_success_(const ObElectionMsgSuccess& msg);
  int notify_leader_(const common::ObAddr& server);
  bool run_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const;
  bool devote_run_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const;
  bool vote_run_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const;
  bool change_leader_time_out_of_range_(const int64_t cur_ts, const int64_t expect_ts) const;
  int get_decentralized_candidate_(common::ObAddr& server, ObElectionPriority& priority, int64_t& lease_time);
  int get_centralized_candidate_(common::ObAddr& cur_leader, common::ObAddr& new_leader, int64_t msg_t1);
  int try_centralized_voting_(const int64_t lease_time, int64_t msg_t1);
  int check_centralized_majority_();
  int check_decentralized_majority_(common::ObAddr& new_leader, int64_t& ticket);
  int collect_valid_candidates_(const ObElectionMsgVote& msg);
  int send_devote_prepare_(const ObElectionMsgDEPrepare& msg);
  int send_vote_prepare_(const ObElectionMsgPrepare& msg);
  int send_change_leader_vote_prepare_(const ObElectionMsgPrepare& msg);
  int send_query_leader_();
  int send_devote_vote_(const common::ObAddr& server, const ObElectionMsgDEVote& msg);
  int send_vote_vote_(const common::ObAddr& server, const ObElectionMsgVote& msg);
  int send_devote_success_(const ObElectionMsgDESuccess& msg);
  int send_vote_success_(const ObElectionMsgSuccess& msg);

private:
  int try_stop_();
  int change_leader_async_(const common::ObAddr& server, common::ObTsWindows& changing_leader_windows);
  int force_leader_async_();
  void change_leader_clear_();
  bool is_real_leader_(const common::ObAddr& server) const;
  // called by leader_takeover_ and leader_reappoint_
  int leader_elected_(const common::ObAddr& leader, const int64_t t1, const int64_t lease_time,
      const bool is_elected_by_changing_leader);
  // leader takeover
  int leader_takeover_(const common::ObAddr& leader, const int64_t epoch, const int64_t t1, const int64_t lease_time,
      const bool is_elected_by_changing_leader);
  // leader reappoint
  int leader_reappoint_(const common::ObAddr& leader, const int64_t t1, const int64_t lease_time);
  int verify_leader_valid_(const int64_t cur_ts) const;
  int get_leader_(common::ObAddr& leader) const;
  bool is_candidate_(const common::ObAddr& server) const
  {
    return curr_candidates_.contains(server);
  }
  int64_t get_election_time_offset_() const
  {
    return election_time_offset_;
  }
  bool is_active_(const int64_t cur_ts) const
  {
    return cur_ts >= active_timestamp_;
  }
  bool leader_lease_is_expired_(const int64_t cur_ts) const;
  void leader_revoke_();
  bool lease_expired_too_long_(const int64_t cur_ts) const;
  void set_stage_(const ObElectionStage stage);
  void set_type_(const ObElectionMemberType type);
  int set_unconfirmed_leader_lease_(const int64_t start, const int64_t end);
  bool unconfirmed_leader_lease_is_expired_(const int64_t cur_ts);
  void set_unconfirmed_leader_(const common::ObAddr& unconfirmed_leader);
  int refresh_local_priority_();
  int cal_valid_candidates_();
  bool is_local_message_(const common::ObAddr& server, const ObElectionMsgType msg_type) const;
  // for election_group
  int update_info_from_eg_();
  bool check_if_allowed_to_move_into_eg_();
  int move_into_election_group_unlock_(const ObElectionGroupId& eg_id);
  int move_out_election_group_unlock_();
  void reset_state_();
  bool is_network_error_();
  bool is_clockdiff_error_();
  bool verify_lease_start_valid_(const common::ObAddr& leader, const int64_t lease_start_t1) const;

private:
  int post_election_msg_(
      const common::ObMemberList& mlist, const common::ObPartitionKey& partition, const ObElectionMsg& msg);
  int post_election_msg_(
      const common::ObAddr& server, const common::ObPartitionKey& partition, const ObElectionMsg& msg);
  int priority_double_check_();
  void flush_trace_buf_();
  void add_change_leader_event_(const common::ObAddr& old_leader, const common::ObAddr& new_leader);
  bool in_reappoint_period_(const int64_t ts);
  void get_change_leader_window_(const int64_t ts, common::ObTsWindows& changing_leader_windows) const;
  bool can_do_reappoint_() const;
  bool can_launch_devote_() const;
  bool is_single_replica_tbl_() const;
  bool can_elect_new_leader_() const;
  int64_t get_msg_real_T1_(const ObElectionVoteMsg& msg) const;
  int64_t real_ts_(int64_t logic_ts) const;
  int64_t logic_ts_(int64_t real_ts) const;

private:
  static const int64_t ACTIVE_TIME_DELTA = 0;
  static const int64_t MAX_WATCHER = 16;
  /**********should removed after a barrier version bigger than 3.1**********/
  static const int64_t OB_ELECTION_HASH_TABLE_NUM = 100;      // mod 100
  static const int64_t OB_ELECTION_HASH_TIME_US = 10000;      // 10ms
  static const int64_t OB_ELECTION_HASH_TABLE_NUM_NEW = 580;  // mod 580
  /**************************************************************************************************/
  static const int64_t OB_ELECTION_MAX_CENTRIALIZED_TIME = 100000;                       // 100ms
  static const int64_t OB_ELECTION_LEADER_EXPIRED_PRINT_INTERVAL_US = 10 * 1000 * 1000;  // 10S
  // RWLock performs better on ARM platform, but worse on x86 platform
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
  bool is_inited_;
  bool is_need_query_;
  bool is_reappoint_period_;
  bool is_force_leader_;
  bool is_in_eg_;
  ObIElectionGroup* election_group_;
  int64_t eg_part_array_idx_;  // partition index of grop list
  // The unconfirmed leader
  common::ObAddr unconfirmed_leader_;
  // unconfirmed leader's lease
  lease_t unconfirmed_leader_lease_;
  // when leader revoke
  int64_t leader_revoke_timestamp_;
  int64_t candidate_index_;
  // election message recv from other voter
  ObElectionVoteMsgPool msg_pool_;
  ObIElectionRpc* rpc_;
  ObElectionTimer timer_;
  ObIElectionCallback* election_cb_;
  ObIElectionGroupMgr* eg_mgr_;
  ObElectionEventHistoryArray* event_history_array_;
  common::ObMemberList valid_candidates_;
  common::ObMemberList tmp_valid_candidates_;
  ObElectionPriority local_priority_;
  int reappoint_count_;
  bool need_print_trace_log_;
  bool ignore_log_;
  int64_t vote_period_;
  int64_t lease_time_;
  mutable int64_t last_gts_;
  int64_t max_leader_epoch_ever_seen_;
  int64_t move_out_timestamp_;
  mutable int64_t cached_lease_end_;
  /**********should removed after a barrier version bigger than 3.1**********/
  int64_t new_election_time_offset_;
  int64_t old_election_time_offset_;
  PhysicalCondition physical_condition_;
  int64_t temp_election_time_offset_;
  void register_gt1_with_neccessary_retry_(int64_t next_expect_ts);
  void insert_physical_condition_into_msg_(const ObElectionMsg&);
  PhysicalCondition fetch_others_condition_(const ObElectionMsg&);
  bool update_condition_(const ObElectionMsg&, const int64_t);
  /***************************************************************/
};

}  // namespace election
}  // namespace oceanbase
#endif  // OCEANBASE_ELECTION_OB_ELECTION_
