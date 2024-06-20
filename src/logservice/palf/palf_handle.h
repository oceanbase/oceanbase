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

#ifndef OCEANBASE_LOGSERVICE_PALF_HANDLE_
#define OCEANBASE_LOGSERVICE_PALF_HANDLE_
#include "common/ob_member_list.h"
#include "common/ob_role.h"
#include "election/interface/election_priority.h"
#include "lsn.h"
#include "palf_handle_impl.h"
#include "palf_handle_impl_guard.h"
#include "palf_iterator.h"
namespace oceanbase
{
namespace share
{
class SCN;
}
namespace palf
{
class PalfAppendOptions;
class PalfFSCb;
class PalfRoleChangeCb;
class PalfLocationCacheCb;
class PalfHandle
{
public:
  friend class PalfEnv;
  friend class PalfEnvImpl;
  friend class PalfHandleGuard;
  PalfHandle();
  ~PalfHandle();
  bool is_valid() const;

  // @brief copy-assignment operator
  // NB: we wouldn't destroy 'this', therefor, if 'this' is valid,
  // after operator=, PalfHandleImpl and Callback have leaked.
  PalfHandle& operator=(const PalfHandle &rhs);
  // @brief move-assignment operator
  PalfHandle& operator=(PalfHandle &&rhs);
  bool operator==(const PalfHandle &rhs) const;

  // 在创建日志流成功后，设置初始成员列表信息，只允许执行一次
  //
  // @param [in] member_list, 日志流的成员列表
  // @param [in] paxos_replica_num, 日志流paxos成员组中的副本数
  //
  // @return :TODO
  // @brief set the initial member list of paxos group after creating
  // palf successfully, it can only be called once
  // @param[in] ObMemberList, the initial member list, do not include arbitration replica
  // @param[in] int64_t, the paxos relica num
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list);
#ifdef OB_BUILD_ARBITRATION
  // @brief set the initial member list of paxos group which contains
  // arbitration replica after creating palf successfully,
  // it can only be called once
  // @param[in] ObMemberList, the initial member list, do not include arbitration replica
  // @param[in] ObMember, the arbitration replica
  // @param[in] int64_t, the paxos relica num(including arbitration replica)
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const common::ObMember &arb_member,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list);
#endif
  //================ 文件访问相关接口 =======================
  int append(const PalfAppendOptions &opts,
             const void *buffer,
             const int64_t nbytes,
             const share::SCN &ref_scn,
             LSN &lsn,
             share::SCN &scn);

  int raw_write(const PalfAppendOptions &opts,
                const LSN &lsn,
                const void *buffer,
                const int64_t nbytes);

  // @brief: read up to 'nbytes' from palf at offset of 'lsn' into the 'read_buf', and
  //         there are alignment restrictions on the length and address of user-space buffers
  //         and the file offset.
  //
  // @param[in] lsn, the start offset to be read, must be aligned with LOG_DIO_ALIGN_SIZE
  // @param[in] buffer, the start of 'buffer', must be aligned with LOG_DIO_ALIGN_SIZE.
  // @param[in] nbytes, the read size, must aligned with LOG_DIO_ALIGN_SIZE
  // @param[out] read_size, the number of bytes read return.
  //
  // @return value
  // OB_SUCCESS.
  // OB_INVALID_ARGUMENT.
  // OB_ERR_OUT_OF_LOWER_BOUND, the lsn is out of lower bound.
  // OB_ERR_OUT_OF_UPPER_BOUND, the lsn is out of upper bound.
  // OB_NEED_RETRY, there is a flashback operation during raw_read.
  // others.
  //
  // 1. use oceanbase::share::mtl_malloc_align or oceanbase::common::ob_malloc_align
  //    with LOG_DIO_ALIGN_SIZE to allocate aligned buffer.
  // 2. use oceanbase::common::lower_align or oceanbase::common::upper_align with
  //    LOG_DIO_ALIGN_SIZE to get aligned lsn or nbytes.
  int raw_read(const palf::LSN &lsn,
               void *buffer,
               const int64_t nbytes,
               int64_t &read_size);

  // iter->next返回的是append调用写入的值，不会在返回的buf中携带Palf增加的header信息
  //           返回的值不包含未确认日志
  //
  // 在指定start_lsn构造Iterator时，iter会自动根据PalfHandle::accepted_end_lsn
  // 确定迭代的结束位置，此结束位置会自动更新（即返回OB_ITER_END后再次
  // 调用iter->next()有返回有效值的可能）
  //
  // PalfBufferIterator的生命周期由调用者管理
  // 调用者需要确保在iter关联的PalfHandle close后不再访问
  // 这个Iterator会在内部缓存一个大的Buffer
  int seek(const LSN &lsn, PalfBufferIterator &iter);

  int seek(const LSN &lsn, PalfGroupBufferIterator &iter);

  // @desc: seek a group buffer iterator by scn, the first log A in iterator must meet
  // one of the following conditions:
  // 1. scn of log A equals to scn
  // 2. scn of log A is higher than scn and A is the first log which scn is higher
  // than scn in all committed logs
  // Note that this function may be time-consuming
  // @params [in] scn:
  //  @params [out] iter: group buffer iterator in which all logs's scn are higher than/equal to
  // scn
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT
  // - OB_ENTRY_NOT_EXIST: there is no log's scn is higher than scn
  // - OB_ERR_OUT_OF_LOWER_BOUND: scn is too old, log files may have been recycled
  // - others: bug
  int seek(const share::SCN &scn, PalfGroupBufferIterator &iter);

  // @desc: query coarse lsn by scn, that means there is a LogGroupEntry in disk,
  // its lsn and scn are result_lsn and result_scn, and result_scn <= scn.
  // Note that this function may be time-consuming
  // Note that result_lsn always points to head of log file
  // @params [in] scn:
  // @params [out] result_lsn: the lower bound lsn which includes scn
  // @return
  // - OB_SUCCESS: locate_by_scn_coarsely success
  // - OB_INVALID_ARGUMENT
  // - OB_ENTRY_NOT_EXIST: there is no log in disk
  // - OB_ERR_OUT_OF_LOWER_BOUND: scn is too small, log files may have been recycled
  // - others: bug
  int locate_by_scn_coarsely(const share::SCN &scn, LSN &result_lsn);

  // @desc: query coarse scn by lsn, that means there is a log in disk,
  // its lsn and scn are result_lsn and result_scn, and result_lsn <= lsn.
  // Note that this function may be time-consuming
  // @params [in] lsn: lsn
  // @params [out] result_scn: the lower bound scn which includes lsn
  // - OB_SUCCESS; locate_by_lsn_coarsely success
  // - OB_INVALID_ARGUMENT
  // - OB_ERR_OUT_OF_LOWER_BOUND: lsn is too small, log files may have been recycled
  // - others: bug
  int locate_by_lsn_coarsely(const LSN &lsn, share::SCN &result_scn);

  // 开启日志同步
  int enable_sync();
  // 关闭日志同步
  int disable_sync();
  bool is_sync_enabled() const;
  // 推进文件的可回收点
  int advance_base_lsn(const LSN &lsn);
  // 迁移/rebuild场景推进base_lsn
  int advance_base_info(const palf::PalfBaseInfo &palf_base_info, const bool is_rebuild);
  int flashback(const int64_t mode_version, const share::SCN &flashback_scn, const int64_t timeout_us);

  // 返回文件中可读的最早日志的位置信息
  int get_begin_lsn(LSN &lsn) const;
  int get_begin_scn(share::SCN &scn) const;

  // return the max recyclable point of Palf
  int get_base_lsn(LSN &lsn) const;

  // PalfBaseInfo include the 'base_lsn' and the 'prev_log_info' of sliding window.
  // @param[in] const LSN&, base_lsn of ls.
  // @param[out] PalfBaseInfo&, palf_base_info
  int get_base_info(const LSN &lsn,
                    PalfBaseInfo &palf_base_info);

  // 返回最后一条已确认日志的下一位置
  // 在没有新的写入的场景下，返回的end_lsn不可读
  int get_end_lsn(LSN &lsn) const;
  int get_end_scn(share::SCN &scn) const;
  int get_max_lsn(LSN &lsn) const;
  int get_max_scn(share::SCN &scn) const;
  int get_last_rebuild_lsn(LSN &last_rebuild_lsn) const;

  //================= 分布式相关接口 =========================

  // 返回当前副本的角色，只存在Leader和Follower两种角色
 	//
 	// @param [out] role, 当前副本的角色
 	// @param [out] leader_epoch，表示一轮leader任期, 保证在切主和重启场景下的单调递增性
 	// @param [out] is_pending_state，表示当前副本是否处于pending状态
 	//
 	// @return :TODO
  int get_role(common::ObRole &role, int64_t &proposal_id, bool &is_pending_state) const;
  int get_palf_id(int64_t &palf_id) const;

  int get_global_learner_list(common::GlobalLearnerList &learner_list) const;
  int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num) const;
  int get_config_version(LogConfigVersion &config_version) const;
  int get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                             int64_t &paxos_replica_num,
                                             GlobalLearnerList &learner_list) const;
  int get_election_leader(common::ObAddr &addr) const;
  int get_parent(common::ObAddr &parent) const;

  // @brief: a special config change interface, change replica number of paxos group
  // @param[in] common::ObMemberList: current memberlist, for pre-check
  // @param[in] const int64_t curr_replica_num: current replica num, for pre-check
  // @param[in] const int64_t new_replica_num: new replica num
  // @param[in] const int64_t timeout_us: timeout, us
  // @return
  // - OB_SUCCESS: change_replica_num successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: change_replica_num timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  int change_replica_num(const common::ObMemberList &member_list,
                         const int64_t curr_replica_num,
                         const int64_t new_replica_num,
                         const int64_t timeout_us);
  // @brief: force set self as single member
  int force_set_as_single_replica();

  int get_ack_info_array(LogMemberAckInfoList &ack_info_array,
                         common::GlobalLearnerList &degraded_list) const;
  // @brief, add a member to paxos group, can be called only in leader
  // @param[in] common::ObMember &member: member which will be added
  // @param[in] const int64_t new_replica_num: replica number of paxos group after adding 'member'
  // @param[in] const LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us: add member timeout, us
  // @return
  // - OB_SUCCESS: add member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: add member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - OB_STATE_NOT_MATCH: not the same leader
  // - other: bug
  int add_member(const common::ObMember &member,
                 const int64_t new_replica_num,
                 const LogConfigVersion &config_version,
                 const int64_t timeout_us);

  // @brief, remove a member from paxos group, can be called only in leader
  // @param[in] common::ObMember &member: member which will be removed
  // @param[in] const int64_t new_replica_num: replica number of paxos group after removing 'member'
  // @param[in] const int64_t timeout_us: remove member timeout, us
  // @return
  // - OB_SUCCESS: remove member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: remove member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  int remove_member(const common::ObMember &member,
                    const int64_t new_replica_num,
                    const int64_t timeout_us);

  // @brief, replace old_member with new_member, can be called only in leader
  // @param[in] const common::ObMember &added_member: member will be added
  // @param[in] const common::ObMember &removed_member: member will be removed
  // @param[in] const LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  int replace_member(const common::ObMember &added_member,
                     const common::ObMember &removed_member,
                     const LogConfigVersion &config_version,
                     const int64_t timeout_us);

  // @brief: add a learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &added_learner: learner will be added
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: add_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  int add_learner(const common::ObMember &added_learner,
                  const int64_t timeout_us);

  // @brief: remove a learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &removed_learner: learner will be removed
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: remove_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  int remove_learner(const common::ObMember &removed_learner,
                     const int64_t timeout_us);

  // @brief: switch a learner(read only replica) to acceptor(full replica) in this clsuter
  // @param[in] const common::ObMember &learner: learner will be switched to acceptor
  // @param[in] const int64_t new_replica_num: replica number of paxos group after switching
  //            learner to acceptor (similar to add_member)
  // @param[in] const LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: switch_learner_to_acceptor timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  int switch_learner_to_acceptor(const common::ObMember &learner,
                                 const int64_t new_replica_num,
                                 const LogConfigVersion &config_version,
                                 const int64_t timeout_us);

  // @brief: switch an acceptor(full replica) to learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &member: acceptor will be switched to learner
  // @param[in] const int64_t new_replica_num: replica number of paxos group after switching
  //            acceptor to learner (similar to remove_member)
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: switch_acceptor_to_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  int switch_acceptor_to_learner(const common::ObMember &member,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_us);

  // @brief, replace removed_learners with added_learners
  // @param[in] const common::ObMemberList &added_learners: learners will be added
  // @param[in] const common::ObMemberList &removed_learners: learners will be removed
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace learner successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  int replace_learners(const common::ObMemberList &added_learners,
                       const common::ObMemberList &removed_learners,
                       const int64_t timeout_us);

  // @brief, replace removed_member with learner
  // @param[in] const common::ObMember &added_member: member will be added
  // @param[in] const common::ObMember &removed_member: member will be removed
  // @param[in] const LogConfigVersion &config_version: config_version for leader checking
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  int replace_member_with_learner(const common::ObMember &added_member,
                                  const common::ObMember &removed_member,
                                  const LogConfigVersion &config_version,
                                  const int64_t timeout_us);
#ifdef OB_BUILD_ARBITRATION

  // @brief, add an arbitration member to paxos group
  // @param[in] common::ObMember &member: arbitration member which will be added
  // @param[in] const int64_t timeout_us: add member timeout, us
  // @return
  // - OB_SUCCESS: add arbitration member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: add arbitration member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  int add_arb_member(const common::ObMember &added_member,
                     const int64_t timeout_us);

  // @brief, remove an arbitration member from paxos group
  // @param[in] common::ObMember &member: arbitration member which will be removed
  // @param[in] const int64_t timeout_us: remove member timeout, us
  // @return
  // - OB_SUCCESS: remove arbitration member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: remove arbitration member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  int remove_arb_member(const common::ObMember &arb_member,
                        const int64_t timeout_us);
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
  int degrade_acceptor_to_learner(const LogMemberAckInfoList &degrade_servers, const int64_t timeout_us);

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
  int upgrade_learner_to_acceptor(const LogMemberAckInfoList &upgrade_servers, const int64_t timeout_us);
  // @brief: get arbitration member info from arbitration service by sync rpc.
  // @param[in] ArbMemberInfo &arb_member_info: arbitration member info
  // @return
  // - OB_SUCCESS
  // - OB_NOT_MASTER: self is not leader
  // - OB_STATE_NOT_MATCH: palf does not have an arbitration member
  // - OB_TIMEOUT: rpc timeout
  // - OB_RPC_POST_ERROR: arbitration server is in easy's black_list
  int get_remote_arb_member_info(ArbMemberInfo &arb_member_info) const;
  // @brief: get arbitration member
  // @param[in/out] common::ObMember &arb_member
  // @return
  // - OB_SUCCESS
  // - OB_NOT_INIT
  int get_arbitration_member(common::ObMember &arb_member) const;
#endif
  int advance_election_epoch_and_downgrade_priority(const int64_t proposal_id,
                                                    const int64_t downgrade_priority_time_us,
                                                    const char *reason);
  int change_leader_to(const common::ObAddr &dst_addr);
  // @brief: change AccessMode of palf.
  // @param[in] const int64_t &proposal_id: current proposal_id of leader
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
  // NB: 1. if return OB_EAGAIN, caller need execute 'change_access_mode' again.
  //     2. before execute 'change_access_mode', caller need execute 'get_access_mode' to
  //      get 'mode_version' and pass it to 'change_access_mode'
  int change_access_mode(const int64_t proposal_id,
                         const int64_t mode_version,
                         const AccessMode &access_mode,
                         const share::SCN &ref_scn);
  // @brief: query the access_mode of palf and it's corresponding mode_version
  // @param[out] palf::AccessMode &access_mode: current access_mode
  // @param[out] int64_t &mode_version: mode_version corresponding to AccessMode
  // @retval
  //   OB_SUCCESS
  int get_access_mode(int64_t &mode_version, AccessMode &access_mode) const;
  int get_access_mode(AccessMode &access_mode) const;
  int get_access_mode_ref_scn(int64_t &mode_version,
                              AccessMode &access_mode,
                              SCN &ref_scn) const;

  // @brief: check whether the palf instance is allowed to vote for logs
  // By default, return true;
  // After calling disable_vote(), return false.
  bool is_vote_enabled() const;
  // @brief: store a persistent flag which means this paxos replica
  // can not reply ack when receiving logs.
  // @param[in] need_check_log_missing: for rebuildinng caused by log missing, need check whether log
  // By default, paxos replica can reply ack.
  // @return:
  int disable_vote(const bool need_check_log_missing);
  // @brief: store a persistent flag which means this paxos replica
  // can reply ack when receiving logs.
  // By default, paxos replica can reply ack.
  // @return:
  int enable_vote();

	//================= 回调函数注册 ===========================
  // @brief: register a callback to PalfHandleImpl, and do something in
  // this callback when file size has changed.
  // NB: not thread safe
  int register_file_size_cb(PalfFSCb *fs_cb);

  // @brief: unregister a callback from PalfHandleImpl
  // NB: not thread safe
  int unregister_file_size_cb();

  // @brief: register a callback to PalfHandleImpl, and do something in
  // this callback when role has changed.
  // NB: not thread safe
  int register_role_change_cb(PalfRoleChangeCb *rc_cb);

  // @brief: unregister a callback from PalfHandleImpl
  // NB: not thread safe
  int unregister_role_change_cb();

  // @brief: register a callback to PalfHandleImpl, and do something in
  // this callback when there is a rebuild operation.
  // NB: not thread safe
  int register_rebuild_cb(PalfRebuildCb *rebuild_cb);

  // @brief: unregister a callback from PalfHandleImpl
  // NB: not thread safe
  int unregister_rebuild_cb();

	//================= 依赖功能注册 ===========================
  int set_location_cache_cb(PalfLocationCacheCb *lc_cb);
  int reset_location_cache_cb();
  int set_election_priority(election::ElectionPriority *priority);
  int reset_election_priority();
  int set_locality_cb(palf::PalfLocalityInfoCb *locality_cb);
  int reset_locality_cb();
  int stat(PalfStat &palf_stat) const;

  //---------config change lock related--------//
  //@return
  // -- OB_NOT_INIT           not_init
  // -- OB_SUCCESS            successfull lock
  // -- OB_TRY_LOCK_CONFIG_CHANGE_CONFLICT   failed to lock because of locked by others
  // -- OB_TIMEOUT              timeout, may lock successfully or not
  // -- OB_EAGAIN               other config change operation is going on,need retry later
  // -- OB_NOT_MASTER           this replica is not leader, not refresh location and retry with actual leader
  // -- OB_STATE_NOT_MATCH        lock_owner is smaller than previous lock_owner
  int try_lock_config_change(int64_t lock_owner, int64_t timeout_us);
  //@return
  // -- OB_NOT_INIT           not_init
  // -- OB_SUCCESS            successfull unlock
  // -- OB_TIMEOUT            timeout, may unlock successfully or not
  // -- OB_EAGAIN             other config change operation is going on,need retry later
  // -- OB_NOT_MASTER         this replica is not leader, need refresh location and retry with actual leader
  // -- OB_STATE_NOT_MATCH    lock_owner is smaller than previous lock_owner,or lock_owner is bigger than previous lock_owner
  int unlock_config_change(int64_t lock_owner, int64_t timeout_us);
  //@return
  // -- OB_NOT_INIT           not_init
  // -- OB_SUCCESS            success
  // -- OB_NOT_MASTER         this replica is not leader, not refresh location and retry with actual leader
  // -- OB_EAGAIN             is_locking or unlocking
  int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked);

	// @param [out] diagnose info, current diagnose info of palf
  int diagnose(PalfDiagnoseInfo &diagnose_info) const;

  TO_STRING_KV(KP(palf_handle_impl_), KP(rc_cb_), KP(fs_cb_));
private:
  palf::IPalfHandleImpl *palf_handle_impl_;
  palf::PalfRoleChangeCbNode *rc_cb_;
  palf::PalfFSCbNode *fs_cb_;
  palf::PalfRebuildCbNode *rebuild_cb_;
};
} // end namespace oceanbase
} // end namespace palf
#endif
