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
#include "share/scn.h"                                //SCN
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
class LogConfigInfoV2;
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
  ADD_LEARNER,
  REMOVE_LEARNER,
  SWITCH_LEARNER_TO_ACCEPTOR,
  SWITCH_ACCEPTOR_TO_LEARNER,
  DEGRADE_ACCEPTOR_TO_LEARNER,
  UPGRADE_LEARNER_TO_ACCEPTOR,
  STARTWORKING,
  FORCE_SINGLE_MEMBER,
  TRY_LOCK_CONFIG_CHANGE,
  UNLOCK_CONFIG_CHANGE,
  REPLACE_LEARNERS,
  SWITCH_LEARNER_TO_ACCEPTOR_AND_NUM,
};

inline const char *LogConfigChangeType2Str(const LogConfigChangeType state)
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
    CHECK_LOG_CONFIG_TYPE_STR(ADD_LEARNER);
    CHECK_LOG_CONFIG_TYPE_STR(REMOVE_LEARNER);
    CHECK_LOG_CONFIG_TYPE_STR(SWITCH_LEARNER_TO_ACCEPTOR);
    CHECK_LOG_CONFIG_TYPE_STR(SWITCH_ACCEPTOR_TO_LEARNER);
    CHECK_LOG_CONFIG_TYPE_STR(DEGRADE_ACCEPTOR_TO_LEARNER);
    CHECK_LOG_CONFIG_TYPE_STR(UPGRADE_LEARNER_TO_ACCEPTOR);
    CHECK_LOG_CONFIG_TYPE_STR(STARTWORKING);
    CHECK_LOG_CONFIG_TYPE_STR(FORCE_SINGLE_MEMBER);
    CHECK_LOG_CONFIG_TYPE_STR(TRY_LOCK_CONFIG_CHANGE);
    CHECK_LOG_CONFIG_TYPE_STR(UNLOCK_CONFIG_CHANGE);
    CHECK_LOG_CONFIG_TYPE_STR(REPLACE_LEARNERS);
    CHECK_LOG_CONFIG_TYPE_STR(SWITCH_LEARNER_TO_ACCEPTOR_AND_NUM);
    default:
      return "Invalid";
  }
  #undef CHECK_LOG_CONFIG_TYPE_STR
}

typedef common::ObArrayHashMap<common::ObAddr, common::ObRegion> LogMemberRegionMap;

// Note: We need to check if the cluster has been upgraded to version 4.2.
//       If not, invalid config_version is allowed because OBServer v4.1
//       may send a LogConfigChangeCmd (with invalid config_version) to
//       the leader v4.2, we need to allow the reconfiguration.
inline bool need_check_config_version(const LogConfigChangeType type)
{
  const bool is_cluster_already_4200 = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0;
  return (is_cluster_already_4200) &&
         (ADD_MEMBER == type || ADD_MEMBER_AND_NUM == type ||
          SWITCH_LEARNER_TO_ACCEPTOR == type || SWITCH_LEARNER_TO_ACCEPTOR_AND_NUM == type);
}

inline bool is_add_log_sync_member_list(const LogConfigChangeType type)
{
  return ADD_MEMBER == type || ADD_MEMBER_AND_NUM == type ||
         SWITCH_LEARNER_TO_ACCEPTOR == type || UPGRADE_LEARNER_TO_ACCEPTOR == type ||
         SWITCH_LEARNER_TO_ACCEPTOR_AND_NUM == type;
}

inline bool is_remove_log_sync_member_list(const LogConfigChangeType type)
{
  return REMOVE_MEMBER == type || REMOVE_MEMBER_AND_NUM == type ||
         SWITCH_ACCEPTOR_TO_LEARNER == type || DEGRADE_ACCEPTOR_TO_LEARNER == type;
}

inline bool is_add_member_list(const LogConfigChangeType type)
{
  return is_add_log_sync_member_list(type) || ADD_ARB_MEMBER == type;
}

inline bool is_remove_member_list(const LogConfigChangeType type)
{
  return is_remove_log_sync_member_list(type) || REMOVE_ARB_MEMBER == type;
}

inline bool is_arb_member_change_type(const LogConfigChangeType type)
{
  return ADD_ARB_MEMBER == type || REMOVE_ARB_MEMBER == type;
}

inline bool is_add_learner_list(const LogConfigChangeType type)
{
  return ADD_LEARNER == type || SWITCH_ACCEPTOR_TO_LEARNER == type ||
      DEGRADE_ACCEPTOR_TO_LEARNER == type || REPLACE_LEARNERS == type;
}

inline bool is_remove_learner_list(const LogConfigChangeType type)
{
  return REMOVE_LEARNER == type || SWITCH_LEARNER_TO_ACCEPTOR == type ||
         UPGRADE_LEARNER_TO_ACCEPTOR == type || SWITCH_LEARNER_TO_ACCEPTOR_AND_NUM == type ||
         REPLACE_LEARNERS == type;
}

inline bool is_upgrade_or_degrade(const LogConfigChangeType type)
{
  return UPGRADE_LEARNER_TO_ACCEPTOR == type || DEGRADE_ACCEPTOR_TO_LEARNER == type;
}

inline bool is_use_replica_num_args(const LogConfigChangeType type)
{
  return ADD_MEMBER == type || REMOVE_MEMBER == type || CHANGE_REPLICA_NUM == type ||
      FORCE_SINGLE_MEMBER == type || SWITCH_LEARNER_TO_ACCEPTOR == type || SWITCH_ACCEPTOR_TO_LEARNER == type;
}

inline bool need_exec_on_leader_(const LogConfigChangeType type)
{
  return (FORCE_SINGLE_MEMBER != type);
}

inline bool is_may_change_replica_num(const LogConfigChangeType type)
{
  return is_add_member_list(type) || is_remove_member_list(type) || CHANGE_REPLICA_NUM == type || FORCE_SINGLE_MEMBER == type;
}

inline bool is_paxos_member_list_change(const LogConfigChangeType type)
{
  return (ADD_MEMBER == type || REMOVE_MEMBER == type
      || ADD_MEMBER_AND_NUM == type || REMOVE_MEMBER_AND_NUM == type
      || SWITCH_LEARNER_TO_ACCEPTOR == type || SWITCH_ACCEPTOR_TO_LEARNER == type
      || CHANGE_REPLICA_NUM == type);
}

inline bool is_try_lock_config_change(const LogConfigChangeType type)
{
  return TRY_LOCK_CONFIG_CHANGE == type;
}

inline bool is_unlock_config_change(const LogConfigChangeType type)
{
  return UNLOCK_CONFIG_CHANGE == type;
}

inline bool is_use_added_list(const LogConfigChangeType type)
{
  return REPLACE_LEARNERS == type;
}

inline bool is_use_removed_list(const LogConfigChangeType type)
{
  return REPLACE_LEARNERS == type;
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
      ref_scn_(),
      lock_owner_(OB_INVALID_CONFIG_CHANGE_LOCK_OWNER),
      lock_type_(ConfigChangeLockType::LOCK_NOTHING),
      type_(INVALID_LOG_CONFIG_CHANGE_TYPE),
      added_list_(),
      removed_list_() { }

  LogConfigChangeArgs(const common::ObMember &server,
                      const int64_t new_replica_num,
                      const LogConfigVersion &config_version,
                      const LogConfigChangeType type)
    : server_(server), curr_member_list_(), curr_replica_num_(0), new_replica_num_(new_replica_num),
      config_version_(config_version), ref_scn_(), lock_owner_(OB_INVALID_CONFIG_CHANGE_LOCK_OWNER),
      lock_type_(ConfigChangeLockType::LOCK_NOTHING), type_(type),
      added_list_(), removed_list_() { }

  LogConfigChangeArgs(const common::ObMember &server,
                      const int64_t new_replica_num,
                      const LogConfigChangeType type)
    : server_(server), curr_member_list_(), curr_replica_num_(0), new_replica_num_(new_replica_num),
      config_version_(), ref_scn_(), lock_owner_(OB_INVALID_CONFIG_CHANGE_LOCK_OWNER),
      lock_type_(ConfigChangeLockType::LOCK_NOTHING), type_(type), added_list_(), removed_list_() { }

  LogConfigChangeArgs(const common::ObMemberList &member_list,
                      const int64_t curr_replica_num,
                      const int64_t new_replica_num,
                      const LogConfigChangeType type)
    : server_(), curr_member_list_(member_list), curr_replica_num_(curr_replica_num), new_replica_num_(new_replica_num),
      config_version_(), ref_scn_(), lock_owner_(OB_INVALID_CONFIG_CHANGE_LOCK_OWNER),
      lock_type_(ConfigChangeLockType::LOCK_NOTHING), type_(type), added_list_(), removed_list_() { }

  LogConfigChangeArgs(const int64_t lock_owner,
                      const int64_t lock_type,
                      const LogConfigChangeType type)
    : server_(), curr_member_list_(), curr_replica_num_(0), new_replica_num_(),
      config_version_(), ref_scn_(), lock_owner_(lock_owner),
      lock_type_(lock_type), type_(type), added_list_(), removed_list_() { }

  LogConfigChangeArgs(const common::ObMemberList &added_list,
                      const common::ObMemberList &removed_list,
                      const LogConfigChangeType type)
    : server_(), curr_member_list_(), curr_replica_num_(0),
      new_replica_num_(0), config_version_(), ref_scn_(),
      lock_owner_(OB_INVALID_CONFIG_CHANGE_LOCK_OWNER),
      lock_type_(ConfigChangeLockType::LOCK_NOTHING), type_(type),
      added_list_(added_list), removed_list_(removed_list) { }

  ~LogConfigChangeArgs()
  {
    reset();
  }
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(server), K_(curr_member_list), K_(curr_replica_num), K_(new_replica_num),
      K_(config_version), K_(ref_scn), K_(lock_owner), K_(lock_type), K_(added_list),
      K_(removed_list), "type", LogConfigChangeType2Str(type_));
  common::ObMember server_;
  common::ObMemberList curr_member_list_;
  int64_t curr_replica_num_;
  int64_t new_replica_num_;
  LogConfigVersion config_version_;
  share::SCN ref_scn_;
  int64_t lock_owner_;
  int64_t lock_type_;
  LogConfigChangeType type_;
  common::ObMemberList added_list_;
  common::ObMemberList removed_list_;
};

struct LogReconfigBarrier
{
  LogReconfigBarrier()
    : prev_log_proposal_id_(INVALID_PROPOSAL_ID),
      prev_lsn_(PALF_INITIAL_LSN_VAL),
      prev_end_lsn_(PALF_INITIAL_LSN_VAL),
      prev_mode_pid_(INVALID_PROPOSAL_ID) { }
  LogReconfigBarrier(const int64_t prev_log_pid,
                     const LSN &prev_lsn,
                     const LSN &prev_end_lsn,
                     const int64_t prev_mode_pid)
    : prev_log_proposal_id_(prev_log_pid),
      prev_lsn_(prev_lsn),
      prev_end_lsn_(prev_end_lsn),
      prev_mode_pid_(prev_mode_pid) { }
  ~LogReconfigBarrier() { reset(); }

  void reset()
  {
    prev_log_proposal_id_ = INVALID_PROPOSAL_ID;
    prev_lsn_.reset();
    prev_end_lsn_.reset();
    prev_mode_pid_ = INVALID_PROPOSAL_ID;
  }
  TO_STRING_KV(K_(prev_log_proposal_id), K_(prev_lsn), K_(prev_end_lsn), K_(prev_mode_pid));
  // previous log proposal_id for barrier
  int64_t prev_log_proposal_id_;
  // previous lsn for barrier
  LSN prev_lsn_;
  LSN prev_end_lsn_;
  // previous mode proposal_id for barrier
  int64_t prev_mode_pid_;
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
                   LogModeMgr *mode_mgr,
                   LogReconfirm *reconfirm,
                   LogPlugins *plugins);
  virtual void destroy();

  // require caller holds WLock in PalfHandleImpl
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const int64_t replica_num,
                                      const common::GlobalLearnerList &learner_list,
                                      const int64_t proposal_id,
                                      LogConfigVersion &config_version);
  // require caller holds WLock in PalfHandleImpl
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const common::ObMember &arb_member,
                                      const int64_t replica_num,
                                      const common::GlobalLearnerList &learner_list,
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
  // @brief get the expected paxos member list without arbitraion member,
  // including degraded members.
  // This interface is generally used by outer modules, such as reporting replica logic.
  // @param[in/out] ObMemberList, the output member list
  // @param[in/out] int64_t, the output replica_num
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  virtual int get_curr_member_list(common::ObMemberList &member_list,
      int64_t &replica_num) const;
  // @brief get the paxos member list which are all alive, including arbitraion member,
  // and excluding degraded members.
  // This list can be used for config change/paxos prepare phase.
  // This interface is only used by palf.
  // @param[in/out] ObMemberList, the output member list
  // @param[in/out] int64_t, the output replica_num
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  virtual int get_alive_member_list_with_arb(common::ObMemberList &member_list,
      int64_t &replica_num) const;
  // @brief get the paxos member list which is responsible for sync group log, excluding arbitraion member,
  // and excluding degraded paxos members.
  // This interface is only used by palf.
  // @param[in/out] ObMemberList, the output member list
  // @param[in/out] int64_t, the output replica_num
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  virtual int get_log_sync_member_list(common::ObMemberList &member_list,
      int64_t &replica_num) const;
  // @brief get the paxos member list which is responsible for generating
  //        committed_end_lsn in the leader, excluding arbitraion member
  //        and excluding degraded paxos members.
  // This interface is only used by palf.
  // @param[in/out] ObMemberList, the output member list
  // @param[in/out] int64_t, the output replica_num
  // @param[in/out] bool, whehter the current committed_end_lsn is smaller than
  //        log barrier of a reconfiguration. If the current committed_end_lsn is
  //        smaller(larger) than barrier, the output member_list and replica_num will be
  //        the configuration before(after) the reconfiguration.
  // @param[in/out] barrier_lsn, log barrier of a reconfiguration (last_submit_end_lsn)
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  virtual int get_log_sync_member_list_for_generate_committed_lsn(
      ObMemberList &prev_member_list,
      int64_t &prev_replica_num,
      ObMemberList &curr_member_list,
      int64_t &curr_replica_num,
      bool &is_before_barrier,
      LSN &barrier_lsn) const;
  virtual int get_arbitration_member(common::ObMember &arb_member) const;
  virtual int get_prev_member_list(common::ObMemberList &member_list) const;
  virtual int get_children_list(LogLearnerList &children) const;
  virtual int get_config_version(LogConfigVersion &config_version) const;
  // @brief get replica_num of expected paxos member list, excluding arbitraion member,
  // and including degraded members.
  // @param[in/out] int64_t, the output replica_num
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  virtual int get_replica_num(int64_t &replica_num) const;
  const common::ObAddr &get_parent() const;
  int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked);
  virtual int leader_do_loop_work(bool &need_change_config);
  virtual int switch_state();
  virtual int wait_log_barrier(const LogConfigChangeArgs &args,
                               const LogConfigInfoV2 &new_config_info) const;
  virtual int renew_config_change_barrier();
  // ================= Config Change =================

  int check_args_and_generate_config(const LogConfigChangeArgs &args,
                                     const int64_t proposal_id,
                                     const int64_t election_epoch,
                                     bool &is_already_finished,
                                     LogConfigInfoV2 &new_config_info) const;
  int pre_sync_config_log_and_mode_meta(const common::ObMember &server, const int64_t proposal_id);
  int start_change_config(int64_t &proposal_id,
                          int64_t &election_epoch,
                          const LogConfigChangeType &type);
  int end_degrade();
  int is_state_changed(bool &need_rlock, bool &need_wlock) const;
  int change_config(const LogConfigChangeArgs &args,
                    const int64_t proposal_id,
                    const int64_t election_epoch,
                    LogConfigVersion &config_version);
  void after_config_change_timeout(const LogConfigVersion &config_version);

  // for reconfirm
  virtual int confirm_start_working_log(const int64_t proposal_id,
                                        const int64_t election_epoch,
                                        LogConfigVersion &config_version);
  // for PalfHandleImpl::receive_config_log
  virtual bool can_receive_config_log(const common::ObAddr &leader, const LogConfigMeta &meta) const;
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
  int check_follower_sync_status(const LogConfigChangeArgs &args,
                                 const LogConfigInfoV2 &new_config_info,
                                 bool &added_member_has_new_version) const;
  int wait_log_barrier_(const LogConfigChangeArgs &args,
                        const LogConfigInfoV2 &new_config_info) const;
  int wait_log_barrier_before_start_working_(const LogConfigChangeArgs &args);
  int sync_meta_for_arb_election_leader();
  void set_sync_to_degraded_learners();
  bool is_sync_to_degraded_learners() const;
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
    J_KV(K_(palf_id), K_(self), K_(alive_paxos_memberlist), K_(alive_paxos_replica_num),              \
      K_(log_ms_meta), K_(running_args), K_(state), K_(checking_barrier), K_(reconfig_barrier),       \
      K_(persistent_config_version), K_(ms_ack_list), K_(resend_config_version), K_(resend_log_list), \
      K_(last_submit_config_log_time_us), K_(need_change_config_bkgd), K_(bkgd_config_version),       \
      K_(region), K_(paxos_member_region_map),                                                        \
      K_(register_time_us), K_(parent), K_(parent_keepalive_time_us),                                 \
      K_(last_submit_register_req_time_us), K_(children), K_(last_submit_keepalive_time_us), KP(this));
    J_OBJ_END();
    return pos;
  }
private:
  enum ConfigChangeState
  {
    INIT = 0,
    CHANGING = 1,
  };
  typedef common::ObSpinLockGuard SpinLockGuard;
  typedef common::ObFunction<bool(const LogLearner &)> LogLearnerCond;
  typedef common::ObFunction<int(const LogLearner &)> LogLearnerAction;
  static constexpr int64_t MAX_WAIT_BARRIER_TIME_US_FOR_RECONFIGURATION = 2 * 1000 * 1000;
  static constexpr int64_t MAX_WAIT_BARRIER_TIME_US_FOR_STABLE_LOG = 1 * 1000 * 1000;
private:
  int set_initial_config_info_(const LogConfigInfoV2 &config_info,
                               const int64_t proposal_id,
                               LogConfigVersion &init_config_version);
  bool can_memberlist_majority_(const int64_t new_member_list_len, const int64_t new_replica_num) const;
  int check_config_change_args_(const LogConfigChangeArgs &args, bool &is_already_finished) const;
  int check_config_change_args_by_type_(const LogConfigChangeArgs &args, bool &is_already_finished) const;
  int check_config_version_matches_state_(const LogConfigChangeType &type, const LogConfigVersion &config_version) const;
  int generate_new_config_info_(const int64_t proposal_id,
                                const LogConfigChangeArgs &args,
                                LogConfigInfoV2 &new_config_info) const;


  int append_config_info_(const LogConfigInfoV2 &config_info);
  int apply_config_info_(const LogConfigInfoV2 &config_info);
  int update_match_lsn_map_(const LogConfigChangeArgs &args, const LogConfigInfoV2 &new_config_info);
  int update_election_meta_(const LogConfigInfoV2 &info);
  int update_election_meta_(const ObMemberList &member_list,
                            const LogConfigVersion &config_version,
                            const int64_t new_replica_num);
  int is_state_changed_(bool &need_rlock, bool &need_wlock) const;
  int renew_config_change_barrier_();
  bool is_reach_majority_() const;
  bool need_resend_config_log_() const;
  bool need_recheck_init_state_() const;
  bool is_leader_for_config_change_(const LogConfigChangeType &type,
                                    const int64_t proposal_id,
                                    const int64_t election_epoch) const;
  int change_config_(const LogConfigChangeArgs &args,
                     const int64_t proposal_id,
                     const int64_t election_epoch,
                     LogConfigVersion &config_version);
  int append_config_meta_(const int64_t curr_proposal_id,
                         const LogConfigChangeArgs &args,
                         bool &is_already_finished);
  int set_resend_log_info_();
  int submit_config_log_(const common::ObMemberList &paxos_member_list,
                         const int64_t proposal_id,
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
  int try_resend_config_log_(const int64_t proposal_id);
  // broadcast leader info to global learners, only called in leader active
  int submit_broadcast_leader_info_(const int64_t proposal_id) const;
  int check_servers_lsn_and_version_(const common::ObAddr &server,
                                     const LogConfigVersion &config_version,
                                     const int64_t conn_timeout_us,
                                     const bool force_remote_check,
                                     const bool need_purge_throttling,
                                     LSN &max_flushed_end_lsn,
                                     bool &has_same_version,
                                     int64_t &last_slide_log_id) const;
  int sync_get_committed_end_lsn_(const LogConfigChangeArgs &args,
                                  const LogConfigInfoV2 &new_config_info,
                                  const bool need_purge_throttling,
                                  const bool need_remote_check,
                                  const int64_t conn_timeout_us,
                                  LSN &committed_end_lsn,
                                  bool &added_member_has_new_version,
                                  LSN &added_member_flushed_end_lsn,
                                  int64_t &added_member_last_slide_log_id) const;
  int check_follower_sync_status_(const LogConfigChangeArgs &args,
                                  const LogConfigInfoV2 &new_config_info,
                                  bool &added_member_has_new_version) const;
  int pre_sync_config_log_and_mode_meta_(const common::ObMember &server,
                                         const int64_t proposal_id,
                                         const bool is_arb_replica);

private:
  // inner_config_meta_ is protected by RWLock in PalfHandleImpl,
  // any read/write ops to inner_config_meta_ should acquire RLock/WLock in PalfHandleImpl.
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
  // inner_config_meta_, region_ and paxos_member_region_map_ is protected by RWLock in PalfHandleImpl,
  // any read/write ops should acquire RLock/WLock in PalfHandleImpl.
  // inner_config_meta_, inner_alive_paxos_memberlist_, inner_alive_paxos_replica_num_,
  // and inner_all_learnerlist_ take effect as long as they are accepted by the replica,
  // they are used within LogConfigMgr
  LogConfigMeta log_ms_meta_;
  // list of all alive paxos members, including arbitration member
  common::ObMemberList alive_paxos_memberlist_;
  // number of all alive paxos members, including arbitration member
  int64_t alive_paxos_replica_num_;
  // list of all learners, including learners which has been degraded from acceptors
  GlobalLearnerList all_learnerlist_;
  LogConfigChangeArgs running_args_;
  common::ObRegion region_;
  LogMemberRegionMap paxos_member_region_map_;
  // this lock protects all states related to config change, except for inner_config_meta_
  mutable common::ObSpinLock lock_;
  int64_t palf_id_;
  common::ObAddr self_;
  // ================= Config Change =================
  // barrier for reconfiguration
  LogReconfigBarrier reconfig_barrier_;
  // barrier for checking log before reconfiguration
  LogReconfigBarrier checking_barrier_;
  ConfigChangeState state_;
  int64_t last_submit_config_log_time_us_;
  // record ack to membership log
  LogSimpleMemberList ms_ack_list_;
  // need change_config with background thread
  bool need_change_config_bkgd_;
  LogConfigVersion bkgd_config_version_;
  bool is_sw_interrupted_by_degrade_;
  bool will_upgrade_;
  int64_t last_start_upgrade_time_us_;
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
  // the epoch of leader who is executing config changing request
  int64_t election_leader_epoch_;
  int64_t last_broadcast_leader_info_time_us_;
  LogConfigVersion persistent_config_version_;
  mutable int64_t barrier_print_log_time_;
  mutable int64_t last_check_init_state_time_us_;
  mutable int64_t check_config_print_time_;
  mutable int64_t start_wait_barrier_time_us_;
  mutable int64_t last_wait_barrier_time_us_;
  mutable LSN last_wait_committed_end_lsn_;
  int64_t last_sync_meta_for_arb_election_leader_time_us_;
  // ================= Config Change =================
  // ==================== Child ========================
  mutable common::ObSpinLock parent_lock_;
  int64_t register_time_us_;
  common::ObAddr parent_;
  int64_t parent_keepalive_time_us_;
  // registering state
  int64_t last_submit_register_req_time_us_;
  // control register req frequency
  int64_t last_first_register_time_us_;
  // ==================== Child ========================
  // ==================== Parent ========================
  mutable common::ObSpinLock child_lock_;
  LogLearnerList children_;
  int64_t last_submit_keepalive_time_us_;
  // ==================== Parent ========================
  LogEngine *log_engine_;
  LogSlidingWindow *sw_;
  LogStateMgr *state_mgr_;
  election::Election* election_;
  LogModeMgr *mode_mgr_;
  LogReconfirm *reconfirm_;
  LogPlugins *plugins_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(LogConfigMgr);
};
} // namespace palf
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_CONFIG_MGR_
