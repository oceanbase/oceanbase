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

#ifndef OCEANBASE_LOGSERVICE_LOG_CONFIG_MGR_
#define OCEANBASE_LOGSERVICE_LOG_CONFIG_MGR_

#include "common/ob_member_list.h"              // ObMemberList
#include "lib/lock/ob_spin_lock.h"              // SpinRWLock
#include "lib/random/ob_random.h"               // ObRandom
#include "lib/hash/ob_array_hash_map.h"         // ObArrayHashMap
#include "lib/function/ob_function.h"           // ObFunction
#include "log_define.h"                         // utils
#include "log_meta_info.h"                      // LogMembershipMeta
#include "log_req.h"                            // LogLearnerReqType
#include "log_simple_member_list.h"             // LogSimpleMemberList
#include "log_state_mgr.h"                      // LogStateMgr
#include "palf_callback.h"                      // PalfLocationCacheCb

namespace oceanbase
{
namespace common
{
class ObILogAllocator;
class ObAddr;
}

namespace palf
{
class LogSlidingWindow;
class LogStateMgr;
class LogConfigInfo;
class LogConfigMeta;
class LSN;
class LogEngine;
class LogConfigVersion;
class LogLearner;
class LogModeMgr;
namespace election
{
class Election;
}

enum LogConfigChangeType
{
  INVALID_LOG_CONFIG_CHANGE_TYPE = 0,
  CHANGE_REPLICA_NUM,
  ADD_MEMBER,
  ADD_ARB_MEMBER,
  REMOVE_MEMBER,
  REMOVE_ARB_MEMBER,
  ADD_MEMBER_AND_NUM,
  REMOVE_MEMBER_AND_NUM,
  ADD_ARB_MEMBER_AND_NUM,
  REMOVE_ARB_MEMBER_AND_NUM,
  ADD_LEARNER,
  REMOVE_LEARNER,
  SWITCH_LEARNER_TO_ACCEPTOR,
  SWITCH_ACCEPTOR_TO_LEARNER,
  DEGRADE_ACCEPTOR_TO_LEARNER,
  UPGRADE_LEARNER_TO_ACCEPTOR,
  STARTWORKING,
};

typedef common::ObArrayHashMap<common::ObAddr, common::ObRegion> LogMemberRegionMap;

inline bool is_add_log_sync_member_list(const LogConfigChangeType type)
{
  return ADD_MEMBER == type || ADD_MEMBER_AND_NUM == type ||
         SWITCH_LEARNER_TO_ACCEPTOR == type || UPGRADE_LEARNER_TO_ACCEPTOR == type;
}

inline bool is_remove_log_sync_member_list(const LogConfigChangeType type)
{
  return REMOVE_MEMBER == type || REMOVE_MEMBER_AND_NUM == type ||
         SWITCH_ACCEPTOR_TO_LEARNER == type || DEGRADE_ACCEPTOR_TO_LEARNER == type;
}

inline bool is_add_member_list(const LogConfigChangeType type)
{
  return is_add_log_sync_member_list(type) || ADD_ARB_MEMBER == type || ADD_ARB_MEMBER_AND_NUM == type;
}

inline bool is_remove_member_list(const LogConfigChangeType type)
{
  return is_remove_log_sync_member_list(type) || REMOVE_ARB_MEMBER == type || REMOVE_ARB_MEMBER_AND_NUM == type;
}

inline bool is_add_learner_list(const LogConfigChangeType type)
{
  return ADD_LEARNER == type || SWITCH_ACCEPTOR_TO_LEARNER == type || DEGRADE_ACCEPTOR_TO_LEARNER == type;
}

inline bool is_remove_learner_list(const LogConfigChangeType type)
{
  return REMOVE_LEARNER == type || SWITCH_LEARNER_TO_ACCEPTOR == type || UPGRADE_LEARNER_TO_ACCEPTOR == type;
}

inline bool is_upgrade_or_degrade(const LogConfigChangeType type)
{
  return UPGRADE_LEARNER_TO_ACCEPTOR == type || DEGRADE_ACCEPTOR_TO_LEARNER == type;
}

inline bool is_use_replica_num_args(const LogConfigChangeType type)
{
  return ADD_MEMBER == type || REMOVE_MEMBER == type || ADD_ARB_MEMBER == type ||
      REMOVE_ARB_MEMBER == type || CHANGE_REPLICA_NUM == type;
}

inline bool is_change_replica_num(const LogConfigChangeType type)
{
  return is_add_member_list(type) || is_remove_member_list(type) || CHANGE_REPLICA_NUM == type;
}

struct LogConfigChangeArgs
{
public:
  LogConfigChangeArgs():
      server_(),
      curr_member_list_(),
      curr_replica_num_(0),
      new_replica_num_(0),
      config_version_(),
      ref_ts_ns_(OB_INVALID_TIMESTAMP),
      type_(INVALID_LOG_CONFIG_CHANGE_TYPE) { }

  LogConfigChangeArgs(const LogConfigVersion &config_version,
                      const int64_t ref_ts_ns,
                      const LogConfigChangeType type)
    : server_(), curr_member_list_(), curr_replica_num_(0), new_replica_num_(0),
      config_version_(config_version), ref_ts_ns_(ref_ts_ns), type_(type) { }

  LogConfigChangeArgs(const common::ObMember &server,
                      const int64_t new_replica_num,
                      const LogConfigChangeType type)
    : server_(server), curr_member_list_(), curr_replica_num_(0), new_replica_num_(new_replica_num),
      config_version_(), ref_ts_ns_(OB_INVALID_TIMESTAMP), type_(type) { }

  LogConfigChangeArgs(const common::ObMemberList &member_list,
                      const int64_t curr_replica_num,
                      const int64_t new_replica_num,
                      const LogConfigChangeType type)
    : server_(), curr_member_list_(member_list), curr_replica_num_(curr_replica_num), new_replica_num_(new_replica_num),
      config_version_(), ref_ts_ns_(OB_INVALID_TIMESTAMP), type_(type) { }

  ~LogConfigChangeArgs()
  {
    reset();
  }
  bool is_valid() const;
  void reset();
  const char *Type2Str(const LogConfigChangeType state) const
  {
    #define CHECK_LOG_CONFIG_TYPE_STR(x) case(LogConfigChangeType::x): return #x
    switch(state)
    {
      CHECK_LOG_CONFIG_TYPE_STR(CHANGE_REPLICA_NUM);
      CHECK_LOG_CONFIG_TYPE_STR(ADD_MEMBER);
      CHECK_LOG_CONFIG_TYPE_STR(ADD_ARB_MEMBER);
      CHECK_LOG_CONFIG_TYPE_STR(REMOVE_MEMBER);
      CHECK_LOG_CONFIG_TYPE_STR(REMOVE_ARB_MEMBER);
      CHECK_LOG_CONFIG_TYPE_STR(ADD_MEMBER_AND_NUM);
      CHECK_LOG_CONFIG_TYPE_STR(REMOVE_MEMBER_AND_NUM);
      CHECK_LOG_CONFIG_TYPE_STR(ADD_ARB_MEMBER_AND_NUM);
      CHECK_LOG_CONFIG_TYPE_STR(REMOVE_ARB_MEMBER_AND_NUM);
      CHECK_LOG_CONFIG_TYPE_STR(ADD_LEARNER);
      CHECK_LOG_CONFIG_TYPE_STR(REMOVE_LEARNER);
      CHECK_LOG_CONFIG_TYPE_STR(SWITCH_LEARNER_TO_ACCEPTOR);
      CHECK_LOG_CONFIG_TYPE_STR(SWITCH_ACCEPTOR_TO_LEARNER);
      CHECK_LOG_CONFIG_TYPE_STR(DEGRADE_ACCEPTOR_TO_LEARNER);
      CHECK_LOG_CONFIG_TYPE_STR(UPGRADE_LEARNER_TO_ACCEPTOR);
      CHECK_LOG_CONFIG_TYPE_STR(STARTWORKING);
      default:
        return "Invalid";
    }
    #undef CHECK_LOG_CONFIG_TYPE_STR
  }
  TO_STRING_KV(K_(server), K_(curr_member_list), K_(curr_replica_num), K_(new_replica_num),
      K_(config_version), K_(ref_ts_ns), "type", Type2Str(type_));
  common::ObMember server_;
  common::ObMemberList curr_member_list_;
  int64_t curr_replica_num_;
  int64_t new_replica_num_;
  LogConfigVersion config_version_;
  int64_t ref_ts_ns_;
  LogConfigChangeType type_;
};

class LogConfigMgr
{
public:
  LogConfigMgr();
  virtual ~LogConfigMgr();
public:
  virtual int init(const int64_t palf_id,
                   const common::ObAddr &self,
                   const LogConfigMeta &log_ms_meta,
                   LogEngine *log_engine,
                   LogSlidingWindow *sw,
                   LogStateMgr *state_mgr,
                   election::Election *election,
                   LogModeMgr *mode_mgr);
  virtual void destroy();

  // require caller holds WLock in PalfHandleImpl
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const int64_t replica_num,
                                      const int64_t proposal_id,
                                      LogConfigVersion &config_version);
  // require caller holds WLock in PalfHandleImpl
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const common::ObMember &arb_replica,
                                      const int64_t replica_num,
                                      const int64_t proposal_id,
                                      LogConfigVersion &config_version);
  // set region for self
  int set_region(const common::ObRegion &region);
  // set region hash_map for Paxos members
  int set_paxos_member_region_map(const LogMemberRegionMap &region_map);
  // following get ops need caller holds RLock in PalfHandleImpl
  virtual int64_t get_accept_proposal_id() const;
  int get_global_learner_list(common::GlobalLearnerList &learner_list) const;
  virtual int get_degraded_learner_list(common::GlobalLearnerList &degraded_learner_list) const;
  virtual int get_curr_member_list(common::ObMemberList &member_list) const;
  virtual int get_paxos_log_sync_list(ObMemberList &member_list) const;
  virtual int get_prev_member_list(common::ObMemberList &member_list) const;
  virtual int get_children_list(LogLearnerList &children) const;
  virtual int get_config_version(LogConfigVersion &config_version) const;
  virtual int get_replica_num(int64_t &replica_num) const;
  virtual int get_paxos_log_sync_replica_num(int64_t &replica_num) const;
  const common::ObAddr &get_parent() const;
  virtual int leader_do_loop_work();
  // ================= Config Change =================

  int check_args_and_generate_config(const int64_t curr_proposal_id,
                                     const LogConfigChangeArgs &args,
                                     bool &is_already_finished,
                                     LogConfigInfo &new_config_info) const;
  int pre_sync_config_log(const common::ObMember &server, const int64_t proposal_id);
  bool is_leader_for_config_change(const LogConfigChangeType &type) const;
  int is_state_changed(bool &need_rlock, bool &need_wlock) const;
  int change_config(const LogConfigChangeArgs &args);
  void after_config_change_timeout();

  // for reconfirm
  virtual int confirm_start_working_log();
  // for PalfHandleImpl::receive_config_log
  virtual bool can_receive_ms_log(const common::ObAddr &leader, const LogConfigMeta &meta) const;
  virtual int after_flush_config_log(const LogConfigVersion &config_version);

  // follower接收到成员变更日志需要进行前向校验
  virtual int receive_config_log(const common::ObAddr &leader, const LogConfigMeta &meta);

  // for PalfHandleImpl::ack_config_log
  virtual int ack_config_log(const common::ObAddr &sender,
                     const int64_t proposal_id,
                     const LogConfigVersion &config_version);
  int wait_config_log_persistence(const LogConfigVersion &config_version) const;
  // broadcast leader info to global learners, only called in leader active
  virtual int submit_broadcast_leader_info(const int64_t proposal_id) const;
  virtual void reset_status();
  // ================ Config Change ==================
  // ==================== Child ========================
  virtual int register_parent();
  virtual int handle_register_parent_resp(const LogLearner &server,
                                  const LogCandidateList &candidate_list,
                                  const RegisterReturn reg_ret);
  // NB: no handle_retire_parent and retire_children
  virtual int handle_retire_child(const LogLearner &parent);
  virtual int handle_learner_keepalive_req(const LogLearner &parent);
  // failure detector
  int check_parent_health();
  // ==================== Child ========================
  // ==================== Parent ========================
  int handle_register_parent_req(const LogLearner &child, const bool is_to_leader);
  int handle_retire_parent(const LogLearner &child);
  int handle_learner_keepalive_resp(const LogLearner &child);
  int check_children_health();
  // ==================== Parent ========================
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    SpinLockGuard guard(lock_);
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(palf_id), K_(self), K_(log_ms_meta), K_(prev_log_proposal_id), K_(prev_lsn),      \
      K_(prev_mode_pid), K_(state), K_(persistent_config_version),                            \
      K_(ms_ack_list), K_(resend_config_version), K_(resend_log_list),
      K_(last_submit_config_log_ts_ns), K_(region), K_(paxos_member_region_map),
      K_(register_ts_ns), K_(parent), K_(parent_keepalive_ts_ns),
      K_(last_submit_register_req_ts_ns), K_(children), K_(last_submit_keepalive_ts_ns), KP(this));
    J_OBJ_END();
    return pos;
  }
private:
  enum ConfigChangeState
  {
    INIT = 0,
    CHANGING,
  };
  typedef common::ObSpinLockGuard SpinLockGuard;
  typedef common::ObFunction<bool(const LogLearner &)> LogLearnerCond;
  typedef common::ObFunction<int(const LogLearner &)> LogLearnerAction;
private:
  int set_initial_config_info_(const LogConfigInfo &config_info,
                               const int64_t proposal_id,
                               LogConfigVersion &init_config_version);
  bool can_memberlist_majority_(const int64_t new_member_list_len, const int64_t new_replica_num) const;
  int check_config_change_args_(const LogConfigChangeArgs &args, bool &is_already_finished) const;
  int generate_new_config_info_(const int64_t proposal_id,
                                const LogConfigChangeArgs &args,
                                LogConfigInfo &new_config_info) const;
  int update_complete_config_info_(const LogConfigInfo &config_info);
  int update_match_lsn_map_(const LogConfigChangeArgs &args);
  int update_election_meta_(const LogConfigInfo &info);
  int update_election_meta_(const ObMemberList &member_list,
                            const LogConfigVersion &config_version,
                            const int64_t new_replica_num);
  int is_state_changed_(bool &need_rlock, bool &need_wlock) const;
  int renew_config_change_barrier_();
  bool is_reach_majority_() const;
  bool need_resend_config_log_() const;
  bool is_leader_for_config_change_(const LogConfigChangeType &type) const;
  int change_config_(const LogConfigChangeArgs &args);
  int apply_config_meta_(const int64_t curr_proposal_id,
                         const LogConfigChangeArgs &args,
                         bool &is_already_finished);
  int set_resend_log_info_();
  int submit_config_log_(const int64_t proposal_id,
                         const int64_t prev_log_proposal_id,
                         const LSN &prev_lsn,
                         const int64_t prev_mode_pid,
                         const LogConfigMeta &config_meta);
  int check_barrier_condition_(const int64_t &prev_log_proposal_id,
                               const LSN &prev_lsn,
                               const int64_t prev_mode_pid) const;
  bool check_need_update_memberlist_without_lock_(const LogConfigVersion &config_version) const;
  int update_election_memberlist_(const LogConfigMeta &log_ms_meta);
  // int check_ms_log_committed_(const int64_t proposal_id, const LogConfigVersion &config_version);
  int check_ms_log_sync_state_() const;
  int try_resend_config_log_(const int64_t proposal_id);
  // broadcast leader info to global learners, only called in leader active
  int submit_broadcast_leader_info_(const int64_t proposal_id) const;
private:
  // log_ms_meta_ is protected by RWLock in PalfHandleImpl,
  // any read/write ops to log_ms_meta_ should acquire RLock/WLock in PalfHandleImpl.
  // ================= Config Change =================
  // ==================== Child ========================
  bool is_registering_() const;
  void reset_registering_state_();
  int register_parent_();
  int after_register_done_(); // enable fetch_log
  int after_region_changed_(const common::ObRegion &old_region, const common::ObRegion &new_region);
  int get_register_leader_(common::ObAddr &leader) const;
  int retire_parent_();
  void reset_parent_info_();
  // ==================== Child ========================
  // ==================== Parent ========================
  int generate_candidate_list_(const LogLearner &child, LogCandidateList &candidate_list);
  int generate_candidate_list_from_member_(const LogLearner &child, LogCandidateList &candidate_list);
  int generate_candidate_list_from_children_(const LogLearner &child, LogCandidateList &candidate_list);
  int remove_timeout_child_(LogLearnerList &dead_children);
  int remove_diff_region_child_(LogLearnerList &diff_region_children);
  int remove_duplicate_region_child_(LogLearnerList &dup_region_children);
  int remove_child_is_not_learner_(LogLearnerList &removed_children);
  int remove_children_(LogLearnerList &this_children, const LogLearnerList &removed_children);
  int get_member_regions_(common::ObArrayHashMap<ObRegion, int> &region_map) const;
  int submit_retire_children_req_(const LogLearnerList &retired_children);
  int children_if_cond_then_action_(const LogLearnerCond &cond, const LogLearnerAction &action);
  // ==================== Parent ========================
private:
  // log_ms_meta_, region_ and paxos_member_region_map_ is protected by RWLock in PalfHandleImpl,
  // any read/write ops should acquire RLock/WLock in PalfHandleImpl.
  LogConfigMeta log_ms_meta_;
  // list of all paxos members, including arbitration replica
  // NB: different from log_ms_meta_.curr_.log_sync_memberlist_, which don't include arbitration replica
  common::ObMemberList paxos_memberlist_;
  // number of all paxos members, including arbitration replica
  // NB: different from log_ms_meta_.curr_.log_sync_replica_num_, which don't include arbitration replica
  int64_t paxos_replica_num_;
  // list of all learners, including learners which has been degraded from acceptors
  GlobalLearnerList all_learnerlist_;
  common::ObRegion region_;
  LogMemberRegionMap paxos_member_region_map_;
  // this lock protects all states related to config change, except for log_ms_meta_
  mutable common::ObSpinLock lock_;
  int64_t palf_id_;
  common::ObAddr self_;
  // ================= Config Change =================
  // previous log proposal_id for barrier
  int64_t prev_log_proposal_id_;
  // previous lsn for barrier
  LSN prev_lsn_;
  // previous mode proposal_id for barrier
  int64_t prev_mode_pid_;
  ConfigChangeState state_;
  int64_t last_submit_config_log_ts_ns_;
  // record ack to membership log
  LogSimpleMemberList ms_ack_list_;
  // need change_config with background thread
  bool need_change_config_bkgd_;
  // In our current implement, leader won't send config change log to followers and learners
  // after config change log has committed. Considering following scenario:
  // Paxos group (A, B, C), their config version are all 1, user switches leader from A to B.
  // B sends startworking log (config version 2) and this log is writed in B and C, not in A.
  // In this time user wants to switch leader from B to A, but A's config version is smaller than
  // majority's, so A can't be leader.
  // To solve the above problem, leader should guarantee config change log should be writed in all
  // paxos members and all learners.
  // for resend config log
  LogConfigVersion resend_config_version_;
  ResendConfigLogList resend_log_list_;
  int64_t last_broadcast_leader_info_ts_ns_;
  LogConfigVersion persistent_config_version_;
  mutable int64_t barrier_print_log_time_;
  mutable int64_t last_check_state_ts_us_;
  mutable int64_t check_config_print_time_;
  // ================= Config Change =================
  // ==================== Child ========================
  mutable common::ObSpinLock parent_lock_;
  int64_t register_ts_ns_;
  common::ObAddr parent_;
  int64_t parent_keepalive_ts_ns_;
  // registering state
  int64_t last_submit_register_req_ts_ns_;
  // control register req frequency
  int64_t last_first_register_ts_ns_;
  // ==================== Child ========================
  // ==================== Parent ========================
  mutable common::ObSpinLock child_lock_;
  LogLearnerList children_;
  int64_t last_submit_keepalive_ts_ns_;
  // ==================== Parent ========================
  LogEngine *log_engine_;
  LogSlidingWindow *sw_;
  LogStateMgr *state_mgr_;
  election::Election* election_;
  LogModeMgr *mode_mgr_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(LogConfigMgr);
};
} // namespace palf
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_CONFIG_MGR_
