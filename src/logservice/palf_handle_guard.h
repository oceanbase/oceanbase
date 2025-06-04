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

#ifndef OCEANBASE_LOGSERVICE_PALF_HANDLE_GUARD_
#define OCEANBASE_LOGSERVICE_PALF_HANDLE_GUARD_
#include "share/ob_delegate.h"
#include "ipalf/ipalf_handle.h"
#include "ipalf/ipalf_env.h"
#include "palf/palf_handle.h"
#include "palf/palf_env.h"
namespace oceanbase
{
namespace palf
{
class PalfHandleGuard
{
public:
  PalfHandleGuard() : palf_handle_(nullptr), palf_env_(nullptr)
  {
  }
  ~PalfHandleGuard()
  {
    reset();
  }
  void reset()
  {
    if (OB_NOT_NULL(palf_env_) && OB_NOT_NULL(palf_handle_))
    {
      palf_env_->close(palf_handle_);
    }
    palf_env_ = nullptr;
    palf_handle_ = nullptr;
  }

  bool is_valid() const
  {
    return OB_NOT_NULL(palf_handle_) && palf_handle_->is_valid();
  }

  ipalf::IPalfHandle *get_palf_handle() { return palf_handle_; }

  void set(ipalf::IPalfHandle *palf_handle, ipalf::IPalfEnv *palf_env)
  {
    palf_handle_ = palf_handle;
    palf_env_ = palf_env;
  }
  // @brief set the initial member list of paxos group
  // @param[in] ObMemberList, the initial member list
  // @param[in] int64_t, the paxos relica num
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  // int set_initial_member_list(const common::ObMemberList &member_list,
  //                             const int64_t paxos_replica_num);
  // DELEGATE_PTR_WITH_RET(palf_handle_, set_initial_member_list, int);
  // @brief append count bytes from the buffer starting at buf to the palf handle, return the LSN and timestamp
  // @param[in] cost PalfAppendOptions&, decide this append option whether need block thread.
  // @param[in] const void *, the data buffer.
  // @param[in] const uint64_t, the length of data buffer.
  // @param[in] const int64_t, the base timestamp(ns), palf will ensure that the return tiemstamp will greater
  //            or equal than this field.
  // @param[out] LSN&, the append position.
  // @param[out] int64_t&, the append timestamp.
  // int append(const PalfAppendOptions &options,
  //            const void *buffer,
  //            const int64_t nbytes,
  //            const int64_t ref_scn,
  //            LSN &lsn,
  //            int64_t &scn);
  DELEGATE_PTR_WITH_RET(palf_handle_, append, int);
  DELEGATE_PTR_WITH_RET(palf_handle_, raw_write, int);
  DELEGATE_PTR_WITH_RET(palf_handle_, raw_read, int);

  // @breif, query lsn by timestamp, note that this function may be time-consuming
  // @param[in] const int64_t, specified timestamp(ns).
  // @param[out] LSN&, the lower bound lsn which include timestamp.
  // @breif, query lsn by timestamp, note that this function may be time-consuming
  // @param[in] const int64_t, specified timestamp(ns).
  // @param[out] LSN&, the lower bound lsn which include timestamp.
  // int locate_by_scn_coarsely(const int64_t scn, LSN &lsn, int64_t &ts);
  DELEGATE_PTR_WITH_RET(palf_handle_, locate_by_scn_coarsely, int);

  DELEGATE_PTR_WITH_RET(palf_handle_, locate_by_lsn_coarsely, int);
  // @brief, set the recycable lsn, palf will ensure that the data before recycable lsn readable.
  // @param[in] const LSN&, recycable lsn.
  // int advance_base_lsn(const LSN &lsn);
  DELEGATE_PTR_WITH_RET(palf_handle_, advance_base_lsn, int);

  CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_base_lsn, int);
  DELEGATE_PTR_WITH_RET(palf_handle_, change_leader_to, int);
  // @breif, get begin lsn, begin lsn maybe smaller than recycable lsn, because palf will not delete data before
  //         recycable lsn immediately.
  // @param[out] int64_t&, begin lsn.
  // int get_base_scn(int64_t &ts) const;
  CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_begin_scn, int);
  // int get_begin_lsn(palf::LSN &lsn) const;
  CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_begin_lsn, int);
  // @brief, get timestamp of begin lsn.
  // @param[out] int64_t&, timestmap.
  // int get_begin_scn(int64_t &ts) const;
  // CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_begin_scn, int);
  // @brief, get end lsn.
  // @param[out] LSN&, end lsn.
  // int get_end_lsn(LSN &lsn) const;
  CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_end_lsn, int);
  // @brief, get timestamp of end lsn.
  // @param[out] int64_t, timestamp.
  // int get_end_scn(int64_t &ts) const;
  CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_end_scn, int);
  // @brief, get max timestamp.
  // @param[out] int64_t, timestamp.
  // int get_max_scn(int64_t &ts) const;
  CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_max_scn, int);
  // @brief, get max lsn.
  // @param[out] int64_t, LSN.
  // int get_max_lsn(LSN &lsn) const;
  CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_max_lsn, int);
  // @brief get readable end lsn for this replica, all logs before it can be readable.
  // @param[out] lsn, readable end lsn.
  // -- OB_NOT_INIT           not_init
  // -- OB_SUCCESS
  // CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_readable_end_lsn, int);

  // @brief, get role of this replica
  // @param[out] common::ObRole&
  // @param[out] int64_t&, the proposal_id of current leader, palf will ensure increasing this field monotonically.
  // @param[out] bool&, whether it's in pending state
  // int get_role(common::ObRole &role, int64_t &proposal_id) const;
  int get_role(common::ObRole &role, int64_t &proposal_id)
  {
    bool unused_state;
    return palf_handle_->get_role(role, proposal_id, unused_state);
  }

  int get_role(common::ObRole &role, int64_t &proposal_id, bool &is_pending_state)
  {
    return palf_handle_->get_role(role, proposal_id, is_pending_state);
  }
  // @brief, get paxos member list of this paxos group
  // @param[out] common::ObMemberList&
  // int get_paxos_member_list(common::ObMemberList &member_list) const override final;
  // CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_paxos_member_list, int);
  // @brief, get paxos member list and learner_list of this paxos group
  // @param[out] common::ObMemberList&
  // // int get_paxos_member_list(common::ObMemberList &member_list) const override final;
  // CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_paxos_member_list_and_learner_list, int);

#ifdef OB_BUILD_ARBITRATION
  // @brief: get arbitration member of log stram
  // @params[out] ObMember arbitration_member
  CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_arbitration_member, int);
#endif

  // @brief: a special config change interface, change replica number of paxos group
  // @param[in] common::ObMemberList: current memberlist, for pre-check
  // @param[in] const int64_t curr_replica_num: current replica num, for pre-check
  // @param[in] const int64_t new_replica_num: new replica num
  // @param[in] const int64_t timeout_us: timeout, ns
  // @return
  // - OB_SUCCESS: change_replica_num successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: change_replica_num timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  // DELEGATE_PTR_WITH_RET(palf_handle_, change_replica_num, int);

// @brief, add a member to paxos group, can be called only in leader
  // @param[in] common::ObMember &member: member which will be added
  // @param[in] const int64_t paxos_replica_num: replica number of paxos group after adding 'member'
  // @param[in] const int64_t timeout_us: add member timeout, ns
  // @param[in] const palf::LogConfigVersion &config_version: config_version for checking leader's
  // config_version
  // @return
  // - OB_SUCCESS: add member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: add member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - OB_STATE_NOT_MATCH: leader has switched
  // - other: bug
  // DELEGATE_PTR_WITH_RET(palf_handle_, add_member, int);

  // @brief, get config_version
  // @return
  // - OB_SUCCESS: get_config_version successfully
  // - OB_NOT_INIT
  // DELEGATE_PTR_WITH_RET(palf_handle_, get_config_version, int);

  // @brief, remove a member from paxos group, can be called only in leader
  // @param[in] common::ObMember &member: member which will be removed
  // @param[in] const int64_t paxos_replica_num: replica number of paxos group after removing 'member'
  // @param[in] const int64_t timeout_us: remove member timeout, ns
  // @return
  // - OB_SUCCESS: remove member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: remove member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  // int remove_member(const common::ObMember &member,
  //                const int64_t paxos_replica_num,
  //                const int64_t timeout_us)
  // DELEGATE_PTR_WITH_RET(palf_handle_, remove_member, int);

  // @brief, replace old_member with new_member, can be called only in leader
  // @param[in] const common::ObMember &removed_member: member will be removed
  // @param[in] const common::ObMember &added_member: member will be added
  // @param[in] const int64_t timeout_us
  // @return
  // - OB_SUCCESS: replace member successfully
  // - OB_INVALID_ARGUMENT: invalid argumemt or not supported config change
  // - OB_TIMEOUT: replace member timeout
  // - OB_NOT_MASTER: not leader or rolechange during membership changing
  // - other: bug
  // DELEGATE_PTR_WITH_RET(palf_handle_, replace_member, int);
  // DELEGATE_PTR_WITH_RET(palf_handle_, add_learner, int);
  // DELEGATE_PTR_WITH_RET(palf_handle_, remove_learner, int);
  // DELEGATE_PTR_WITH_RET(palf_handle_, switch_learner_to_acceptor, int);
  // DELEGATE_PTR_WITH_RET(palf_handle_, switch_acceptor_to_learner, int);
  // DELEGATE_PTR_WITH_RET(palf_handle_, set_location_cache_cb, int);
  DELEGATE_PTR_WITH_RET(palf_handle_, change_access_mode, int);
  DELEGATE_PTR_WITH_RET(palf_handle_, get_access_mode, int);
  DELEGATE_PTR_WITH_RET(palf_handle_, flashback, int);
  // CONST_DELEGATE_PTR_WITH_RET(palf_handle_, stat, int);
  // DELEGATE_PTR_WITH_RET(palf_handle_, try_lock_config_change, int);
  // DELEGATE_PTR_WITH_RET(palf_handle_, unlock_config_change, int);
  // DELEGATE_PTR_WITH_RET(palf_handle_, get_config_change_lock_stat, int);
  DELEGATE_PTR_WITH_RET(palf_handle_, disable_sync, int);
  DELEGATE_PTR_WITH_RET(palf_handle_, enable_sync, int);
  // DELEGATE_PTR_WITH_RET(palf_handle_, advance_base_info, int);
  // DELEGATE_PTR_WITH_RET(palf_handle_, is_vote_enabled, bool);
  DELEGATE_PTR_WITH_RET(palf_handle_, get_election_leader, int);
  DELEGATE_PTR_WITH_RET(palf_handle_, is_sync_enabled, bool);
  DELEGATE_PTR_WITH_RET(palf_handle_, get_access_mode_version, int);
  CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_election_leader, int);
  CONST_DELEGATE_PTR_WITH_RET(palf_handle_, get_palf_epoch, int);
  TO_STRING_KV(K(palf_handle_));
private:
  ipalf::IPalfHandle *palf_handle_;
  ipalf::IPalfEnv *palf_env_;
};
}
}
#endif
