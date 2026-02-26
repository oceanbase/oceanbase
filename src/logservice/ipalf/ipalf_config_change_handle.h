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

 #ifndef OCEANBASE_LOGSERVICE_IPALF_CONFIG_CHANGE_HANDLER_HANDLE_
 #define OCEANBASE_LOGSERVICE_IPALF_CONFIG_CHANGE_HANDLER_HANDLE_

#include "common/ob_member_list.h"
#include "common/ob_learner_list.h"

namespace oceanbase
{
namespace palf
{
class LogConfigVersion;
}
namespace ipalf
{
class IPalfConfigChangeHandler
{
public:
  IPalfConfigChangeHandler() {}
  virtual ~IPalfConfigChangeHandler() {}
public:
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
  virtual int set_initial_member_list(const common::ObMemberList &member_list,
                                      const int64_t paxos_replica_num,
                                      const common::GlobalLearnerList &learner_list) = 0;

  virtual int get_global_learner_list(common::GlobalLearnerList &learner_list) const = 0;
  virtual int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num, const bool &filter_logonly_replica = false) const = 0;
  virtual int get_config_version(palf::LogConfigVersion &config_version) const = 0;
  virtual int get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                                     int64_t &paxos_replica_num,
                                                     GlobalLearnerList &learner_list, const bool &filter_logonly_replica = false) const = 0;
  virtual int get_stable_membership(palf::LogConfigVersion &config_version,
                                    common::ObMemberList &member_list,
                                    int64_t &paxos_replica_num,
                                    common::GlobalLearnerList &learner_list,
                                    const bool &filter_logonly_replica = false) const = 0;

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
  virtual int change_replica_num(const common::ObMemberList &member_list,
                                 const int64_t curr_replica_num,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_us) = 0;
  // @brief: force set self as single member
  virtual int force_set_as_single_replica() = 0;
  // @brief: force set member list.
  // @param[in] const common::ObMemberList &new_member_list: members which will be added
  // @param[in] const int64_t new_replica_num: replica number of paxos group after forcing to set member list
  // @return
  // - OB_SUCCESS: force_set_member_list successfully
  // - OB_TIMEOUT: force_set_member_list timeout
  // - OB_NOT_RUNNING: log stream is stopped
  // - OB_INVALID_ARGUMENT: invalid argument
  // - other: bug
  virtual int force_set_member_list(const common::ObMemberList &new_member_list, const int64_t new_replica_num) = 0;

  // increment member info config version
  // update proposer_id if proposer_id in member info is not latest, otherwise inc config_seq in member_info
  virtual int inc_config_version(int64_t timeout_us) = 0;

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
  virtual int add_member(const common::ObMember &member,
                         const int64_t new_replica_num,
                         const palf::LogConfigVersion &config_version,
                         const int64_t timeout_us) = 0;

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
  virtual int remove_member(const common::ObMember &member,
                            const int64_t new_replica_num,
                            const int64_t timeout_us) = 0;

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
  virtual int replace_member(const common::ObMember &added_member,
                             const common::ObMember &removed_member,
                             const palf::LogConfigVersion &config_version,
                             const int64_t timeout_us) = 0;

  // @brief: add a learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &added_learner: learner will be added
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: add_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  virtual int add_learner(const common::ObMember &added_learner,
                          const int64_t timeout_us) = 0;

  // @brief: remove a learner(read only replica) in this clsuter
  // @param[in] const common::ObMember &removed_learner: learner will be removed
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT: invalid argument
  // - OB_TIMEOUT: remove_learner timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  virtual int remove_learner(const common::ObMember &removed_learner,
                             const int64_t timeout_us) = 0;

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
  virtual int switch_learner_to_acceptor(const common::ObMember &learner,
                                         const int64_t new_replica_num,
                                         const palf::LogConfigVersion &config_version,
                                         const int64_t timeout_us) = 0;

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
  virtual int switch_acceptor_to_learner(const common::ObMember &member,
                                         const int64_t new_replica_num,
                                         const int64_t timeout_us) = 0;

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
  virtual int replace_learners(const common::ObMemberList &added_learners,
                               const common::ObMemberList &removed_learners,
                               const int64_t timeout_us) = 0;

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
  virtual int replace_member_with_learner(const common::ObMember &added_member,
                                          const common::ObMember &removed_member,
                                          const palf::LogConfigVersion &config_version,
                                          const int64_t timeout_us) = 0;
  //---------config change lock related--------//
  //@return
  // -- OB_NOT_INIT           not_init
  // -- OB_SUCCESS            successfull lock
  // -- OB_TRY_LOCK_CONFIG_CHANGE_CONFLICT   failed to lock because of locked by others
  // -- OB_TIMEOUT              timeout, may lock successfully or not
  // -- OB_EAGAIN               other config change operation is going on,need retry later
  // -- OB_NOT_MASTER           this replica is not leader, not refresh location and retry with actual leader
  // -- OB_STATE_NOT_MATCH        lock_owner is smaller than previous lock_owner
  virtual int try_lock_config_change(int64_t lock_owner, int64_t timeout_us) = 0;
  //@return
  // -- OB_NOT_INIT           not_init
  // -- OB_SUCCESS            successfull unlock
  // -- OB_TIMEOUT            timeout, may unlock successfully or not
  // -- OB_EAGAIN             other config change operation is going on,need retry later
  // -- OB_NOT_MASTER         this replica is not leader, need refresh location and retry with actual leader
  // -- OB_STATE_NOT_MATCH    lock_owner is smaller than previous lock_owner,or lock_owner is bigger than previous lock_owner
  virtual int unlock_config_change(int64_t lock_owner, int64_t timeout_us) = 0;
  //@return
  // -- OB_NOT_INIT           not_init
  // -- OB_SUCCESS            success
  // -- OB_NOT_MASTER         this replica is not leader, not refresh location and retry with actual leader
  // -- OB_EAGAIN             is_locking or unlocking
  virtual int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked) = 0;

};
}
}

 #endif
