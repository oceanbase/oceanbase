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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_HANDLER_
#define OCEANBASE_LOGSERVICE_OB_LOG_HANDLER_
#include <cstdint>
#include "lib/utility/ob_macro_utils.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "common/ob_role.h"
#include "common/ob_member_list.h"
#include "share/ob_delegate.h"
#include "palf/palf_handle.h"
#include "palf/palf_base_info.h"
#include "logrpc/ob_log_rpc_proxy.h"
#include "logrpc/ob_log_request_handler.h"
#include "ob_log_handler_base.h"

#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
#include "logservice/ob_log_compression.h"
#endif


namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace share
{
class SCN;
}
namespace transaction
{
class ObTsMgr;
}
namespace palf
{
class PalfEnv;
class LSN;
}
namespace logservice
{
class ObLogApplyService;
class ObApplyStatus;
class ObLogReplayService;
class AppendCb;
struct LogHandlerDiagnoseInfo {
  LogHandlerDiagnoseInfo() { reset(); }
  ~LogHandlerDiagnoseInfo() { reset(); }
  common::ObRole log_handler_role_;
  int64_t log_handler_proposal_id_;
  void reset() {
    log_handler_role_ = FOLLOWER;
    log_handler_proposal_id_ = palf::INVALID_PROPOSAL_ID;
  }
  TO_STRING_KV(K(log_handler_role_),
               K(log_handler_proposal_id_));
};

class ObRoleChangeService;
class ObILogHandler
{
public:
  virtual ~ObILogHandler() {}
  virtual bool is_valid() const = 0;
  virtual int append(const void *buffer,
                     const int64_t nbytes,
                     const share::SCN &ref_scn,
                     const bool need_nonblock,
                     const bool allow_compress,
                     AppendCb *cb,
                     palf::LSN &lsn,
                     share::SCN &scn) = 0;

  virtual int append_big_log(const void *buffer,
                             const int64_t nbytes,
                             const share::SCN &ref_scn,
                             const bool need_nonblock,
                             const bool allow_compress,
                             AppendCb *cb,
                             palf::LSN &lsn,
                             share::SCN &scn) = 0;

  virtual int get_role(common::ObRole &role, int64_t &proposal_id) const = 0;

  virtual int change_access_mode(const int64_t mode_version,
                                 const palf::AccessMode &access_mode,
                                 const share::SCN &ref_scn) = 0;
  virtual int get_access_mode(int64_t &mode_version, palf::AccessMode &access_mode) const = 0;
  virtual int get_append_mode_initial_scn(SCN &ref_scn) const = 0;
  virtual int seek(const palf::LSN &lsn, palf::PalfBufferIterator &iter) = 0;
  virtual int seek(const palf::LSN &lsn, palf::PalfGroupBufferIterator &iter) = 0;
  virtual int seek(const share::SCN &scn, palf::PalfGroupBufferIterator &iter) = 0;
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const int64_t paxos_replica_num,
                                      const common::GlobalLearnerList &learner_list) = 0;
#ifdef OB_BUILD_ARBITRATION
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const common::ObMember &arb_member,
                                      const int64_t paxos_replica_num,
                                      const common::GlobalLearnerList &learner_list) = 0;
#endif
  virtual int set_election_priority(palf::election::ElectionPriority *priority) = 0;
  virtual int reset_election_priority() = 0;

  virtual int locate_by_scn_coarsely(const share::SCN &scn, palf::LSN &result_lsn) = 0;
  virtual int locate_by_lsn_coarsely(const palf::LSN &lsn, share::SCN &result_scn) = 0;
  virtual int get_max_decided_scn_as_leader(share::SCN &scn) const = 0;
  virtual int advance_base_lsn(const palf::LSN &lsn) = 0;
  virtual int get_begin_lsn(palf::LSN &lsn) const = 0;
  virtual int get_end_lsn(palf::LSN &lsn) const = 0;
  virtual int get_max_lsn(palf::LSN &lsn) const = 0;

  virtual int get_max_scn(share::SCN &scn) const = 0;
  virtual int get_end_scn(share::SCN &scn) const = 0;
  virtual int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num) const = 0;
  virtual int get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                                     int64_t &paxos_replica_num,
                                                     common::GlobalLearnerList &learner_list) const = 0;
  virtual int get_global_learner_list(common::GlobalLearnerList &learner_list) const = 0;
  virtual int get_leader_config_version(palf::LogConfigVersion &config_version) const = 0;
  //  get leader from election, used only for non_palf_leader rebuilding.
  virtual int get_election_leader(common::ObAddr &addr) const = 0;
  virtual int get_parent(common::ObAddr &parent) const = 0;
  virtual int change_replica_num(const common::ObMemberList &member_list,
                                 const int64_t curr_replica_num,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_us) = 0;
  virtual int force_set_as_single_replica() = 0;
  virtual int add_member(const common::ObMember &member,
                         const int64_t paxos_replica_num,
                         const palf::LogConfigVersion &config_version,
                         const int64_t timeout_us) = 0;
  virtual int remove_member(const common::ObMember &member,
                            const int64_t paxos_replica_num,
                            const int64_t timeout_us) = 0;
  virtual int replace_member(const common::ObMember &added_member,
                             const common::ObMember &removed_member,
                             const palf::LogConfigVersion &config_version,
                             const int64_t timeout_us) = 0;
  virtual int add_learner(const common::ObMember &added_learner, const int64_t timeout_us) = 0;
  virtual int remove_learner(const common::ObMember &removed_learner, const int64_t timeout_us) = 0;
  virtual int replace_learners(const common::ObMemberList &added_learners,
                               const common::ObMemberList &removed_learners,
                               const int64_t timeout_us) = 0;
  virtual int replace_member_with_learner(const common::ObMember &added_member,
                                          const common::ObMember &removed_member,
                                          const palf::LogConfigVersion &config_version,
                                          const int64_t timeout_us) = 0;
  virtual int switch_learner_to_acceptor(const common::ObMember &learner,
                                         const int64_t paxos_replica_num,
                                         const palf::LogConfigVersion &config_version,
                                         const int64_t timeout_us) = 0;
  virtual int switch_acceptor_to_learner(const common::ObMember &member,
                                         const int64_t paxos_replica_num,
                                         const int64_t timeout_us) = 0;
#ifdef OB_BUILD_ARBITRATION
  virtual int add_arbitration_member(const common::ObMember &added_member,
                                     const int64_t timeout_us) = 0;
  virtual int remove_arbitration_member(const common::ObMember &removed_member,
                                        const int64_t timeout_us) = 0;
  virtual int degrade_acceptor_to_learner(const palf::LogMemberAckInfoList &degrade_servers,
                                          const int64_t timeout_us) = 0;
  virtual int upgrade_learner_to_acceptor(const palf::LogMemberAckInfoList &upgrade_servers,
                                          const int64_t timeout_us) = 0;
#endif
  virtual int try_lock_config_change(const int64_t lock_owner, const int64_t timeout_us) = 0;
  virtual int unlock_config_change(const int64_t lock_owner, const int64_t timeout_us) = 0;
  virtual int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked) = 0;
  virtual int get_palf_base_info(const palf::LSN &base_lsn, palf::PalfBaseInfo &palf_base_info) = 0;
  virtual int is_in_sync(bool &is_log_sync, bool &is_need_rebuild) const = 0;
  virtual int enable_sync() = 0;
  virtual int disable_sync() = 0;
  virtual bool is_sync_enabled() const = 0;
  virtual int advance_base_info(const palf::PalfBaseInfo &palf_base_info, const bool is_rebuild) = 0;
  virtual int get_member_gc_stat(const common::ObAddr &addr, bool &is_valid_member, obrpc::LogMemberGCStat &stat) const = 0;
  virtual void wait_append_sync() = 0;
  virtual int enable_replay(const palf::LSN &initial_lsn, const share::SCN &initial_scn) = 0;
  virtual int disable_replay() = 0;
  virtual int get_max_decided_scn(share::SCN &scn) = 0;
  virtual int pend_submit_replay_log() = 0;
  virtual int restore_submit_replay_log() = 0;
  virtual bool is_replay_enabled() const = 0;
  virtual int disable_vote(const bool need_check_log_missing) = 0;
  virtual int enable_vote() = 0;
  virtual int register_rebuild_cb(palf::PalfRebuildCb *rebuild_cb) = 0;
  virtual int unregister_rebuild_cb() = 0;
  virtual int offline() = 0;
  virtual int online(const palf::LSN &lsn, const share::SCN &scn) = 0;
  virtual bool is_offline() const = 0;
  virtual int is_replay_fatal_error(bool &has_fatal_error) = 0;
};

class ObLogHandler : public ObILogHandler, public ObLogHandlerBase
{
public:
  ObLogHandler();
  virtual ~ObLogHandler();

  int init(const int64_t id,
           const common::ObAddr &self,
           ObLogApplyService *apply_service,
           ObLogReplayService *replay_service,
           ObRoleChangeService *rc_service,
           palf::PalfEnv *palf_env,
           palf::PalfLocationCacheCb *lc_cb,
           obrpc::ObLogServiceRpcProxy *rpc_proxy,
           common::ObILogAllocator *alloc_mgr);
  bool is_valid() const;
  int stop();
  void destroy();
  // @breif, wait cb append onto apply service all be called
  // is reentrant and should be called before destroy(),
  // protect cb will not be used when log handler destroyed
  int safe_to_destroy(bool &is_safe_destroy);
  // @brief append count bytes from the buffer starting at buf to the palf handle, return the LSN and timestamp
  // @param[in] const void *, the data buffer.
  // @param[in] const uint64_t, the length of data buffer.
  // @param[in] const int64_t, the base timestamp(ns), palf will ensure that the return tiemstamp will greater
  //            or equal than this field.
  // @param[in] const bool, decide this append option whether need block thread.
  // @param[in] const bool, decide this append option whether compress buffer.
  // @param[int] AppendCb*, the callback of this append option, log handler will ensure that cb will be called after log has been committed
  // @param[out] LSN&, the append position.
  // @param[out] int64_t&, the append timestamp.
  // @retval
  //    OB_SUCCESS
  //    OB_NOT_MASTER, the prospoal_id of ObLogHandler is not same with PalfHandle.
  // NB: only support for primary(AccessMode::APPEND)
  int append(const void *buffer,
             const int64_t nbytes,
             const share::SCN &ref_scn,
             const bool need_nonblock,
             const bool allow_compress,
             AppendCb *cb,
             palf::LSN &lsn,
             share::SCN &scn) override final;

  // @brief append count bytes(which is bigger than MAX_NORMAL_LOG_BODY_SIZE) from the buffer starting at buf to the palf handle, return the LSN and timestamp
  // @param[in] const void *, the data buffer.
  // @param[in] const uint64_t, the length of data buffer.
  // @param[in] const int64_t, the base timestamp(ns), palf will ensure that the return tiemstamp will greater
  //            or equal than this field.
  // @param[in] const bool, decide this append option whether need block thread.
  // @param[in] const bool, decide this append option whether compress buffer.
  // @param[int] AppendCb*, the callback of this append option, log handler will ensure that cb will be called after log has been committed
  // @param[out] LSN&, the append position.
  // @param[out] int64_t&, the append timestamp.
  // @retval
  //    OB_SUCCESS
  //    OB_NOT_MASTER, the prospoal_id of ObLogHandler is not same with PalfHandle.
  // NB: only support for primary(AccessMode::APPEND)
  int append_big_log(const void *buffer,
                     const int64_t nbytes,
                     const share::SCN &ref_scn,
                     const bool need_nonblock,
                     const bool allow_compress,
                     AppendCb *cb,
                     palf::LSN &lsn,
                     share::SCN &scn) override final;

  // @brief switch log_handle role, to LEADER or FOLLOWER
  // @param[in], role, LEADER or FOLLOWER
  // @param[in], proposal_id, global monotonically increasing id
  virtual void switch_role(const common::ObRole &role, const int64_t proposal_id) override;
  // @brief query role and proposal_id from ObLogHandler.
  // @param[out], role:
  //    LEADER, if 'role_' of ObLogHandler is LEADER and 'proposal_id' is same with PalfHandle.
  //    FOLLOWER, otherwise.
  // @param[out], proposal_id, global monotonically increasing.
  // @retval
  //   OB_SUCCESS
  // NB: for standby, is always FOLLOWER
  int get_role(common::ObRole &role, int64_t &proposal_id) const override final;
  // @brief: query the access_mode of palf and it's corresponding mode_version
  // @param[out] palf::AccessMode &access_mode: current access_mode
  // @param[out] int64_t &mode_version: mode_version corresponding to AccessMode
  // @retval
  //   OB_SUCCESS
  int get_access_mode(int64_t &mode_version, palf::AccessMode &access_mode) const override final;
  // @description: get ref_scn of APPEND mode
  // @return
  // - OB_SUCCESS
  // - OB_STATE_NOT_MATCH: current access mode is not APPEND
  int get_append_mode_initial_scn(share::SCN &initial_scn) const override final;
  // @brief change AccessMode of palf.
  // @param[in] const int64_t &mode_version: mode_version corresponding to AccessMode,
  // can be gotted by get_access_mode
  // @param[in] const palf::AccessMode access_mode: access_mode will be changed to
  // @param[in] const int64_t ref_scn: scn of all submitted logs after changing access mode
  // are bigger than ref_scn
  // NB: ref_scn will take effect only when:
  //     a. ref_scn is bigger than/equal to max_ts(get_max_scn())
  //     b. AccessMode is set to APPEND
  // @retval
  //   OB_SUCCESS
  //   OB_NOT_MASTER: self is not active leader
  //   OB_EAGAIN: another change_acess_mode is running, try again later
  //   OB_STATE_NOT_MATCH: the param 'mode_version' don't match with current mode_version
  // NB: 1. if return OB_EAGAIN, caller need execute 'change_access_mode' again.
  //     2. before execute 'change_access_mode', caller need execute 'get_access_mode' to
  //      get 'mode_version' and pass it to 'change_access_mode'
  int change_access_mode(const int64_t mode_version,
                         const palf::AccessMode &access_mode,
                         const share::SCN &ref_scn) override final;

  // @brief alloc PalfBufferIterator, this iterator will get something append by caller.
  // @param[in] const LSN &, the start lsn of iterator.
  // @param[out] PalfBufferIterator &.
  int seek(const palf::LSN &lsn,
           palf::PalfBufferIterator &iter) override final;
  // @brief alloc PalfGroupBufferIterator, this iterator will get get group entry generated by palf.
  // @param[in] const LSN &, the start lsn of iterator.
  // @param[out] PalfGroupBufferIterator &.
  int seek(const palf::LSN &lsn,
           palf::PalfGroupBufferIterator &iter) override final;
  // @brief alloc PalfBufferIterator, this iterator will get something append by caller.
  // @param[in] const int64_t, the start ts of iterator.
  // @param[out] PalfBufferIterator &, scn of first log in this iterator must be higher than/equal to start_scn.
  int seek(const share::SCN &scn,
           palf::PalfGroupBufferIterator &iter) override final;
  // @brief set the initial member list of paxos group
  // @param[in] ObMemberList, the initial member list
  // @param[in] int64_t, the paxos relica num
  // @param[in] GlobalLearnerList, the initial learner list
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list) override final;
#ifdef OB_BUILD_ARBITRATION
  // @brief set the initial member list of paxos group which contains arbitration replica
  // @param[in] ObMemberList, the initial member list, do not include arbitration replica
  // @param[in] ObMember, the arbitration replica
  // @param[in] int64_t, the paxos relica num(including arbitration replica)
  // @param[in] GlobalLearnerList, the initial learner list
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const common::ObMember &arb_member,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list) override final;
#endif
  int set_election_priority(palf::election::ElectionPriority *priority) override final;
  int reset_election_priority() override final;
  // @desc: query coarse lsn by ts(ns), that means there is a LogGroupEntry in disk,
  // its lsn and scn are result_lsn and result_scn, and result_scn <= scn.
  // Note that this function may be time-consuming
  // Note that result_lsn always points to head of log file
  // @params [in] scn: timestamp(nano second)
  // @params [out] result_lsn: the lower bound lsn which includes scn
  // @return
  // - OB_SUCCESS: locate_by_scn_coarsely success
  // - OB_INVALID_ARGUMENT
  // - OB_ENTRY_NOT_EXIST: there is no log in disk
  // - OB_ERR_OUT_OF_LOWER_BOUND: scn is too old, log files may have been recycled
  // - OB_NEED_RETRY: the block is being flashback, need retry.
  // - others: bug
  int locate_by_scn_coarsely(const share::SCN &scn, palf::LSN &result_lsn) override final;

  // @desc: query coarse ts by lsn, that means there is a log in disk,
  // its lsn and scn are result_lsn and result_scn, and result_lsn <= lsn.
  // Note that this function may be time-consuming
  // @params [in] lsn: lsn
  // @params [out] result_scn: the lower bound timestamp which includes lsn
  // - OB_SUCCESS; locate_by_lsn_coarsely success
  // - OB_INVALID_ARGUMENT
  // - OB_ERR_OUT_OF_LOWER_BOUND: lsn is too small, log files may have been recycled
  // - OB_NEED_RETRY: the block is being flashback, need retry.
  // - others: bug
  int locate_by_lsn_coarsely(const palf::LSN &lsn, share::SCN &result_scn) override final;
  // @brief, get max committed scn from applyservice, which is the max scn of log committed by oneself as leader;
  // Example:
  // At time T1, the replica of the log stream is the leader and the maximum SCN of the logs
  // confirmed by this replica is 100. At time T2, the replica switches to being a follower and the
  // maximum SCN of the logs synchronized and replayed is 200. At time T3, the log stream replica
  // switches back to being the leader and writes logs with SCNs ranging from 201 to 300, all of
  // which are not confirmed. In this case, the returned value of the interface would be 100.
  // @param[out] max scn of logs confirmed by this replica as being leader
  // @return
  //  OB_NOT_INIT : ls is not inited
  //  OB_ERR_UNEXPECTED: unexpected error such as apply_service_ is NULL
  //  OB_SUCCESS
  int get_max_decided_scn_as_leader(share::SCN &scn) const override final;
  // @brief, set the recycable lsn, palf will ensure that the data before recycable lsn readable.
  // @param[in] const LSN&, recycable lsn.
  int advance_base_lsn(const palf::LSN &lsn) override final;
  // @brief, get begin lsn
  // @param[out] LSN&, begin lsn
  int get_begin_lsn(palf::LSN &lsn) const override final;
  int get_end_lsn(palf::LSN &lsn) const override final;
  // @brief, get max lsn.
  // @param[out] LSN&, max lsn.
  int get_max_lsn(palf::LSN &lsn) const override final;
  // @brief, get max log ts.
  // @param[out] int64_t&, max log ts.
  int get_max_scn(share::SCN &scn) const override final;
  // @brief, get timestamp of end lsn.
  // @param[out] int64_t, timestamp.
  int get_end_scn(share::SCN &scn) const override final;
  // @brief, get paxos member list of this paxos group
  // @param[out] common::ObMemberList&
  // @param[out] int64_t&
  int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num) const override final;
  // @brief, get paxos member list and global list of this paxos group atomically
  // @param[out] common::ObMemberList&
  // @param[out] int64_t&
  // @param[out] common::GlobalLearnerList&
  int get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                             int64_t &paxos_replica_num,
                                             common::GlobalLearnerList &learner_list) const override final;
  // @brief, get global learner list of this paxos group
  // @param[out] common::GlobalLearnerList&
  int get_global_learner_list(common::GlobalLearnerList &learner_list) const override final;
  // @brief, get leader from election, used only for non_palf_leader rebuilding
  // @param[out] addr: address of leader
  // retval:
  //   OB_SUCCESS
  //   OB_NOT_INIT
  //   OB_LEADER_NOT_EXIST
  int get_election_leader(common::ObAddr &addr) const override final;
  // @brief, get parent
  // @param[out] addr: address of parent
  // retval:
  //   OB_SUCCESS
  //   OB_NOT_INIT
  //   OB_ENTRY_NOT_EXIST: parent is invalid
  int get_parent(common::ObAddr &parent) const override final;
  // PalfBaseInfo include the 'base_lsn' and the 'prev_log_info' of sliding window.
  // @param[in] const LSN&, base_lsn of ls.
  // @param[out] PalfBaseInfo&, palf_base_info
  // retval:
  //   OB_SUCCESS
  //   OB_ERR_OUT_OF_LOWER_BOUND, the block of 'base_lsn' has been recycled
  int get_palf_base_info(const palf::LSN &base_lsn, palf::PalfBaseInfo &palf_base_info) override final;
  // @brief, advance base_log_info for migrate/rebuild scenes.
  // @param[in] const PalfBaseInfo&, palf_base_info.
  // NB: caller should call disable_sync() before calling advance_base_info. after calling advance_base_info, replay_status will be disabled.
  // So if advance_base_info returns OB_SUCCESS, that means log sync and replay_status have been disabled.
  int advance_base_info(const palf::PalfBaseInfo &palf_base_info, const bool is_rebuild) override final;
  // check if palf is in sync state or need rebuild
  // @param [out] is_log_sync: if the log of this replica is sync with leader's,
  //    - false: leader's max_scn  - local max_scn > 3s + keepalive_interval
  //    - true:  leader's max_scn  - local max_scn < 3s + keepalive_interval
  // @param [out] is_need_rebuild, if the log of this replica is far behind than leader's
  // and need do rebuild
  int is_in_sync(bool &is_log_sync, bool &is_need_rebuild) const override final;

  // @brief, enable log sync
  int enable_sync() override final;
  // @brief, disable log sync
  int disable_sync() override final;
  // @brief, check if log sync is enabled
  bool is_sync_enabled() const override final;

  // @brief: get config_version
  // @return
  // - OB_SUCCESS: get config_version successfully
  // - OB_NOT_INIT: is not inited
  // - OB_NOT_RUNNING: is in stopped state
  // - OB_NOT_MASTER: this replica is not master
  // - other: bug
  int get_leader_config_version(palf::LogConfigVersion &config_version) const;

  // @brief: a special config change interface, change replica number of paxos group
  // @param[in] common::ObMemberList: current memberlist, for pre-check
  // @param[in] const int64_t curr_replica_num: current replica num, for pre-check
  // @param[in] const int64_t new_replica_num: new replica num
  // @param[in] const int64_t timeout_us: timeout, us
  // @return
  // - OB_SUCCESS: change_replica_num successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: change_replica_num timeout
  // - other: bug
  int change_replica_num(const common::ObMemberList &member_list,
                         const int64_t curr_replica_num,
                         const int64_t new_replica_num,
                         const int64_t timeout_us) override final;

  // @brief: force set self as single replica.
  // @return
  // - OB_SUCCESS: change_replica_num successfully
  // - OB_TIMEOUT: change_replica_num timeout
  // - other: bug
  virtual int force_set_as_single_replica() override final;
  // @brief, add a member to paxos group, can be called in any member
  // @param[in] common::ObMember &member: member which will be added
  // @param[in] const int64_t paxos_replica_num: replica number of paxos group after adding 'member'
  // @param[in] const palf::LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us: add member timeout, us
  // @return
  // - OB_SUCCESS: add member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_NOT_MASTER: not master
  // - OB_STATE_NOT_MATCH: leader has switched
  // - other: bug
  int add_member(const common::ObMember &member,
                 const int64_t paxos_replica_num,
                 const palf::LogConfigVersion &config_version,
                 const int64_t timeout_us) override final;

  // @brief, remove a member from paxos group, can be called in any member
  // @param[in] common::ObMember &member: member which will be removed
  // @param[in] const int64_t paxos_replica_num: replica number of paxos group after removing 'member'
  // @param[in] const int64_t timeout_us: remove member timeout, us
  // @return
  // - OB_SUCCESS: remove member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: remove member timeout
  // - other: bug
  int remove_member(const common::ObMember &member,
                    const int64_t paxos_replica_num,
                    const int64_t timeout_us) override final;

  // @brief, replace old_member with new_member, can be called in any member
  // @param[in] const common::ObMember &added_member: member will be added
  // @param[in] const common::ObMember &removed_member: member will be removed
  // @param[in] const palf::LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace member timeout
  // - other: bug
  int replace_member(const common::ObMember &added_member,
                     const common::ObMember &removed_member,
                     const palf::LogConfigVersion &config_version,
                     const int64_t timeout_us) override final;

  // @brief: add a learner(read only replica) in this cluster
  // @param[in] const common::ObMember &added_learner: learner will be added
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: add_learner timeout
  int add_learner(const common::ObMember &added_learner,
                  const int64_t timeout_us) override final;

  // @brief, replace removed_learners with added_learners, can be called in any member
  // @param[in] const common::ObMemberList &added_learners: learners will be added
  // @param[in] const common::ObMemberList &removed_learners: learners will be removed
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace learner successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace learner timeout
  // - other: bug
  int replace_learners(const common::ObMemberList &added_learners,
                       const common::ObMemberList &removed_learners,
                       const int64_t timeout_us) override final;

  // @brief, replace removed_member with learner, can be called in any member
  // @param[in] const common::ObMember &added_member: member will be added
  // @param[in] const common::ObMember &removed_member: member will be removed
  // @param[in] const palf::LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace member timeout
  // - other: bug
  int replace_member_with_learner(const common::ObMember &added_member,
                                  const common::ObMember &removed_member,
                                  const palf::LogConfigVersion &config_version,
                                  const int64_t timeout_us) override final;

  // @brief: remove a learner(read only replica) in this cluster
  // @param[in] const common::ObMember &removed_learner: learner will be removed
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: remove_learner timeout
  int remove_learner(const common::ObMember &removed_learner,
                     const int64_t timeout_us) override final;

  // @brief: switch a learner(read only replica) to acceptor(full replica) in this cluster
  // @param[in] const common::ObMember &learner: learner will be switched to acceptor
  // @param[in] const int64_t new_replica_num: replica number of paxos group after switching
  //            learner to acceptor (similar to add_member)
  // @param[in] const palf::LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: switch_learner_to_acceptor timeout
  int switch_learner_to_acceptor(const common::ObMember &learner,
                                 const int64_t new_replica_num,
                                 const palf::LogConfigVersion &config_version,
                                 const int64_t timeout_us) override final;

  // @brief: switch an acceptor(full replica) to learner(read only replica) in this cluster
  // @param[in] const common::ObMember &member: acceptor will be switched to learner
  // @param[in] const int64_t new_replica_num: replica number of paxos group after switching
  //            acceptor to learner (similar to remove_member)
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: switch_acceptor_to_learner timeout
  int switch_acceptor_to_learner(const common::ObMember &member,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_us) override final;

#ifdef OB_BUILD_ARBITRATION
  // @brief, add an arbitration member to paxos group
  // @param[in] common::ObMember &member: arbitration member which will be added
  // @param[in] const int64_t timeout_us: add member timeout, us
  // @return
  // - OB_SUCCESS: add arbitration member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: add arbitration member timeout
  // - other: bug
  int add_arbitration_member(const common::ObMember &added_member,
                             const int64_t timeout_us) override final;

  // @brief, remove an arbitration member from paxos group
  // @param[in] common::ObMember &member: arbitration member which will be removed
  // @param[in] const int64_t timeout_us: remove member timeout, us
  // @return
  // - OB_SUCCESS: remove arbitration member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: remove arbitration member timeout
  // - other: bug
  int remove_arbitration_member(const common::ObMember &arb_member,
                                const int64_t timeout_us) override final;
  // @brief: degrade an acceptor(full replica) to learner(special read only replica) in this cluster
  // @param[in] const common::ObMemberList &member_list: acceptors will be degraded to learner
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: timeout
  // - OB_NOT_MASTER: not leader
  // - OB_EAGAIN: need retry
  // - OB_OP_NOT_ALLOW: can not do degrade because of pre check fails
  //      if last_ack_ts of any server in LogMemberAckInfoList has changed, can not degrade
  int degrade_acceptor_to_learner(const palf::LogMemberAckInfoList &degrade_servers, const int64_t timeout_us) override final;

  // @brief: upgrade a learner(special read only replica) to acceptor(full replica) in this cluster
  // @param[in] const common::ObMemberList &learner_list: learners will be upgraded to acceptors
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: timeout
  // - OB_NOT_MASTER: not leader
  // - OB_EAGAIN: need retry
  // - OB_OB_NOT_ALLOW: can not do upgrade because of pre check fails
  //      if lsn of any server in LogMemberAckInfoList is less than match_lsn in palf, can not upgrade
  int upgrade_learner_to_acceptor(const palf::LogMemberAckInfoList &upgrade_servers, const int64_t timeout_us) override final;
#endif

  //---------config change lock related--------//
  //@brief: try lock config change which will forbidden changing on memberlist
  // @param[in] const int64_ lock_owner: owner of locking_config_change operation
  // @param[in] const int64_t timeout_us
  //@return
  // -- OB_NOT_INIT           not_init
  // -- OB_SUCCESS            successfull lock
  // -- OB_TRY_LOCK_CONFIG_CHANGE_CONFLICT   failed to lock because of locked by others
  // -- OB_TIMEOUT              timeout, may lock successfully or not
  // -- OB_EAGAIN               other config change operation is going on,need retry later
  // -- OB_NOT_MASTER           this replica is not leader, not refresh location and retry with actual leader
  // -- OB_STATE_NOT_MATCH        lock_owner is smaller than previous lock_owner
  int try_lock_config_change(const int64_t lock_owner, const int64_t timeout_us);

  //@brief: unlock config change which will allow changing on memberlist
  // @param[in] const int64_ lock_owner: expected owner of config_change
  // @param[in] const int64_t timeout_us
  //@return
  // -- OB_NOT_INIT           not_init
  // -- OB_SUCCESS            successfull unlock
  // -- OB_TIMEOUT            timeout, may unlock successfully or not
  // -- OB_EAGAIN             other config change operation is going on,need retry later
  // -- OB_NOT_MASTER         this replica is not leader, need refresh location and retry with actual leader
  // -- OB_STATE_NOT_MATCH    lock_owner is smaller than previous lock_owner,or lock_owner is bigger than previous lock_owner
  int unlock_config_change(const int64_t lock_owner, const int64_t timeout_us);

  //@brief: get config change lock stat
  //@return
  //ret:
  // -- OB_NOT_INIT           not_init
  // -- OB_SUCCESS            success
  // -- OB_NOT_MASTER         this replica is not leader, not refresh location and retry with actual leader
  // -- OB_EAGAIN             is_locking or unlocking
  // lock_owner: owner of config_change_lock
  // is_locked:  whether config_change_lock is locked
  int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked);

  // @breif, check request server is in self member list
  // @param[in] const common::ObAddr, request server.
  // @param[out] is_valid_member&, whether in member list or  learner list.
  // @param[out] obrpc::LogMemberGCStat&, gc stat like learner in migrating.
  int get_member_gc_stat(const common::ObAddr &addr, bool &is_valid_member, obrpc::LogMemberGCStat &stat) const override final;
  // @breif, wait cb append onto apply service Qsync
  // protect submit log and push cb in Qsync guard
  void wait_append_sync() override final;
  // @brief, enable replay status with specific start point.
  // @param[in] const palf::LSN &initial_lsn: replay new start lsn.
  // @param[in] const int64_t &initial_scn: replay new start ts.
  int enable_replay(const palf::LSN &initial_lsn,
                    const share::SCN &initial_scn) override final;
  // @brief, disable replay for current ls.
  int disable_replay() override final;
  // @brief, pending sumbit replay log
  int pend_submit_replay_log() override final;
  // @brief, restore sumbit replay log
  int restore_submit_replay_log() override final;

  // @brief, check if replay is enabled.
  bool is_replay_enabled() const override final;
  // @brief, get max decided scn considering both apply and replay.
  // @param[out] int64_t&, max decided scn.
  // @return
  // OB_NOT_INIT: not inited
  // OB_STATE_NOT_MATCH: ls is offline or stopped
  // OB_SUCCESS
  int get_max_decided_scn(share::SCN &scn) override final;
  // @brief: store a persistent flag which means this paxos replica  can not reply ack when receiving logs.
  // By default, paxos replica can reply ack.
  // @param[in] need_check_log_missing: for rebuildinng caused by log missing, need check whether log
  // is actually missing
  // @return:
  // OB_NOT_INIT: not inited
  // OB_NOT_RUNNING: in stop state
  // OB_OP_NOT_ALLOW: no need to rebuilds, log is available. rebuilding should be abandoned.
  // OB_LEADER_NOT_EXIST: no leader when double checking. rebuilding should retry.
  int disable_vote(const bool need_check_log_missing) override final;
  // @brief: store a persistent flag which means this paxos replica
  // can reply ack when receiving logs.
  // By default, paxos replica can reply ack.
  int enable_vote() override final;
  int register_rebuild_cb(palf::PalfRebuildCb *rebuild_cb) override final;
  int unregister_rebuild_cb() override final;
  int diagnose(LogHandlerDiagnoseInfo &diagnose_info) const;
  int diagnose_palf(palf::PalfDiagnoseInfo &diagnose_info) const;

  TO_STRING_KV(K_(role), K_(proposal_id), KP(palf_env_), K(is_in_stop_state_), K(is_inited_), K(id_));
  int offline() override final;
  int online(const palf::LSN &lsn, const share::SCN &scn) override final;
  bool is_offline() const override final;
  // @brief: check there's a fatal error in replay service.
  // @param[out] has_fatal_error.
  // @return:
  // OB_NOT_INIT: not inited
  // OB_NOT_RUNNING: in stop state
  // OB_EAGAIN: try lock failed, need retry.
  int is_replay_fatal_error(bool &has_fatal_error);
private:
  static constexpr int64_t MIN_CONN_TIMEOUT_US = 5 * 1000 * 1000;     // 5s
  const int64_t MAX_APPEND_RETRY_INTERNAL = 500 * 1000L;
  typedef common::TCRWLock::RLockGuardWithTimeout RLockGuardWithTimeout;
  typedef common::TCRWLock::RLockGuard RLockGuard;
  typedef common::TCRWLock::WLockGuardWithTimeout WLockGuardWithTimeout;
private:
  int submit_config_change_cmd_(const LogConfigChangeCmd &req);
  int submit_config_change_cmd_(const LogConfigChangeCmd &req,
                                LogConfigChangeCmdResp &resp);

  int append_(const void *buffer,
              const int64_t nbytes,
              const share::SCN &ref_scn,
              const bool need_nonblock,
              const bool allow_compress,
              AppendCb *cb,
              palf::LSN &lsn,
              share::SCN &scn);

#ifdef OB_BUILD_ARBITRATION
  int create_arb_member_(const common::ObMember &arb_member, const int64_t timeout_us);
  int delete_arb_member_(const common::ObMember &arb_member, const int64_t timeout_us);
#endif
  DISALLOW_COPY_AND_ASSIGN(ObLogHandler);
private:
  common::ObAddr self_;
  //log_handler会高频调用apply_status, 减少通过applyservice哈希的开销
  ObApplyStatus *apply_status_;
  ObLogApplyService *apply_service_;
  ObLogReplayService *replay_service_;
  ObRoleChangeService *rc_service_;
  // Note: using TCRWLock for using WLockGuardWithTimeout
  common::TCRWLock deps_lock_;
  mutable palf::PalfLocationCacheCb *lc_cb_;
  mutable obrpc::ObLogServiceRpcProxy *rpc_proxy_;
  common::ObQSync ls_qs_;
  ObMiniStat::ObStatItem append_cost_stat_;
  bool is_offline_;
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
  ObLogCompressorWrapper compressor_wrapper_;
#endif
  mutable int64_t get_max_decided_scn_debug_time_;
};

struct ObLogStat
{
public:
  // @desc: role of ls
  // if ((log_handler is leader or restore_handler is leader) && palf is leader)
  //    role_ = LEADER;
  // else
  //    role_ = FOLLOWER
  common::ObRole role_;
  // @desc: proposal_id of ls(log_handler)
  int64_t proposal_id_;
  palf::PalfStat palf_stat_;
  bool in_sync_;
  TO_STRING_KV(K_(palf_stat), K_(in_sync))
};
} // end namespace logservice
} // end namespace oceanbase
#endif
