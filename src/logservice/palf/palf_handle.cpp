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

#include "palf_handle.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_errno.h"
#include "palf_callback.h"
#include "palf_callback_wrapper.h"
#include "palf_iterator.h"
#include "palf_handle_impl.h"

namespace oceanbase
{
using namespace share;
namespace palf
{
#define CHECK_VALID if (NULL == palf_handle_impl_) { return OB_NOT_INIT; }

PalfHandle::PalfHandle() : palf_handle_impl_(NULL),
                           rc_cb_(NULL),
                           fs_cb_(NULL),
                           rebuild_cb_(NULL)
{
}

PalfHandle::~PalfHandle()
{
  palf_handle_impl_ = NULL;
  rc_cb_ = NULL;
  fs_cb_ = NULL;
  rebuild_cb_ = NULL;
}

bool PalfHandle::is_valid() const
{
  return NULL != palf_handle_impl_;
}

PalfHandle& PalfHandle::operator=(const PalfHandle &rhs)
{
  if (this == &rhs) {
    return *this;
  }
  palf_handle_impl_ = rhs.palf_handle_impl_;
  rc_cb_ = rhs.rc_cb_;
  fs_cb_ = rhs.fs_cb_;
  rebuild_cb_ = rhs.rebuild_cb_;
  return *this;
}

PalfHandle& PalfHandle::operator=(PalfHandle &&rhs)
{
  if (this == &rhs) {
    return *this;
  }
  palf_handle_impl_ = rhs.palf_handle_impl_;
  rc_cb_ = rhs.rc_cb_;
  fs_cb_ = rhs.fs_cb_;
  rebuild_cb_ = rhs.rebuild_cb_;
  rhs.palf_handle_impl_ = NULL;
  rhs.rc_cb_ = NULL;
  rhs.fs_cb_ = NULL;
  rhs.rebuild_cb_ = NULL;
  return *this;
}

bool  PalfHandle::operator==(const PalfHandle &rhs) const
{
  return palf_handle_impl_ == rhs.palf_handle_impl_;
}

int PalfHandle::set_initial_member_list(const common::ObMemberList &member_list,
                                        const int64_t paxos_replica_num,
                                        const common::GlobalLearnerList &learner_list)
{
  CHECK_VALID;
  return palf_handle_impl_->set_initial_member_list(member_list, paxos_replica_num, learner_list);
}

#ifdef OB_BUILD_ARBITRATION
int PalfHandle::set_initial_member_list(const common::ObMemberList &member_list,
                                        const common::ObMember &arb_member,
                                        const int64_t paxos_replica_num,
                                        const common::GlobalLearnerList &learner_list)
{
  CHECK_VALID;
  return palf_handle_impl_->set_initial_member_list(member_list, arb_member, paxos_replica_num, learner_list);
}

int PalfHandle::get_remote_arb_member_info(ArbMemberInfo &arb_member_info) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_remote_arb_member_info(arb_member_info);
}

int PalfHandle::get_arbitration_member(common::ObMember &arb_member) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_arbitration_member(arb_member);
}
#endif

int PalfHandle::append(const PalfAppendOptions &opts,
                       const void *buffer,
                       const int64_t nbytes,
                       const SCN &ref_scn,
                       LSN &lsn,
                       SCN &scn)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  ret = palf_handle_impl_->submit_log(opts, static_cast<const char*>(buffer), nbytes, ref_scn, lsn, scn);
  return ret;
}

int PalfHandle::raw_write(const PalfAppendOptions &opts,
                          const LSN &lsn,
                          const void *buffer,
                          const int64_t nbytes)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  ret = palf_handle_impl_->submit_group_log(opts, lsn, static_cast<const char*>(buffer), nbytes);
  return ret;
}

int PalfHandle::seek(const LSN &lsn, PalfBufferIterator &iter)
{
  CHECK_VALID;
  int ret = OB_SUCCESS;
  if (!lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid lsn to seek iterator", KR(ret), K(lsn));
  } else if (true == iter.is_inited()) {
    ret = iter.reuse(lsn);
  } else {
    ret = palf_handle_impl_->alloc_palf_buffer_iterator(lsn, iter);
  }
  return ret;
}

int PalfHandle::seek(const LSN &lsn, PalfGroupBufferIterator &iter)
{
  CHECK_VALID;
  int ret = OB_SUCCESS;
  if (!lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid lsn to seek iterator", KR(ret), K(lsn));
  } else if (true == iter.is_inited()) {
    ret = iter.reuse(lsn);
  } else {
    ret = palf_handle_impl_->alloc_palf_group_buffer_iterator(lsn, iter);
  }
  return ret;
}

int PalfHandle::seek(const SCN &scn, PalfGroupBufferIterator &iter)
{
  CHECK_VALID;
  return palf_handle_impl_->alloc_palf_group_buffer_iterator(scn, iter);
}

int PalfHandle::locate_by_scn_coarsely(const SCN &scn, LSN &result_lsn)
{
  CHECK_VALID;
  return palf_handle_impl_->locate_by_scn_coarsely(scn, result_lsn);
}

int PalfHandle::locate_by_lsn_coarsely(const LSN &lsn, SCN &result_scn)
{
  CHECK_VALID;
  return palf_handle_impl_->locate_by_lsn_coarsely(lsn, result_scn);
}

int PalfHandle::enable_sync()
{
  CHECK_VALID;
  return palf_handle_impl_->enable_sync();
}

int PalfHandle::disable_sync()
{
  CHECK_VALID;
  return palf_handle_impl_->disable_sync();
}

bool PalfHandle::is_sync_enabled() const
{
  CHECK_VALID;
  return palf_handle_impl_->is_sync_enabled();
}

int PalfHandle::advance_base_lsn(const LSN &lsn)
{
  CHECK_VALID;
  return palf_handle_impl_->set_base_lsn(lsn);
}

int PalfHandle::advance_base_info(const palf::PalfBaseInfo &palf_base_info, const bool is_rebuild)
{
  CHECK_VALID;
  return palf_handle_impl_->advance_base_info(palf_base_info, is_rebuild);
}

int PalfHandle::flashback(const int64_t mode_version, const SCN &flashback_scn, const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->flashback(mode_version, flashback_scn, timeout_us);
}

int PalfHandle::get_begin_lsn(LSN &lsn) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_begin_lsn(lsn);
}

int PalfHandle::get_begin_scn(SCN &scn) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_begin_scn(scn);
}

int PalfHandle::get_base_lsn(LSN &lsn) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_base_lsn(lsn);
}

int PalfHandle::get_base_info(const LSN &lsn,
                              PalfBaseInfo &palf_base_info)
{
  CHECK_VALID;
  return palf_handle_impl_->get_base_info(lsn, palf_base_info);
}


int PalfHandle::get_end_lsn(LSN &lsn) const
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  lsn = palf_handle_impl_->get_end_lsn();
  return ret;
}

int PalfHandle::get_last_rebuild_lsn(LSN &last_rebuild_lsn) const
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  ret = palf_handle_impl_->get_last_rebuild_lsn(last_rebuild_lsn);
  return ret;
}

int PalfHandle::get_max_lsn(LSN &lsn) const
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  lsn = palf_handle_impl_->get_max_lsn();
  return ret;
}

int PalfHandle::get_max_scn(SCN &scn) const
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  scn = palf_handle_impl_->get_max_scn();
  return ret;
}

int PalfHandle::get_config_version(LogConfigVersion &config_version) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_config_version(config_version);
}

int PalfHandle::get_role(common::ObRole &role, int64_t &proposal_id, bool &is_pending_state) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_role(role, proposal_id, is_pending_state);
}

int PalfHandle::get_palf_id(int64_t &palf_id) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_palf_id(palf_id);
}

int PalfHandle::get_end_scn(SCN &scn) const
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  scn = palf_handle_impl_->get_end_scn();
  return ret;
}

int PalfHandle::get_global_learner_list(common::GlobalLearnerList &learner_list) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_global_learner_list(learner_list);
}

int PalfHandle::get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_paxos_member_list(member_list, paxos_replica_num);
}

int PalfHandle::get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                                       int64_t &paxos_replica_num,
                                                       GlobalLearnerList &learner_list) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_paxos_member_list_and_learner_list(member_list, paxos_replica_num, learner_list);
}

int PalfHandle::get_election_leader(common::ObAddr &addr) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_election_leader(addr);
}

int PalfHandle::get_parent(common::ObAddr &parent) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_parent(parent);
}

int PalfHandle::change_replica_num(const common::ObMemberList &member_list,
                                   const int64_t curr_replica_num,
                                   const int64_t new_replica_num,
                                   const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->change_replica_num(member_list, curr_replica_num, new_replica_num, timeout_us);
}
int PalfHandle::force_set_as_single_replica()
{
  CHECK_VALID;
  return palf_handle_impl_->force_set_as_single_replica();
}
int PalfHandle::get_ack_info_array(LogMemberAckInfoList &ack_info_array,
                                   common::GlobalLearnerList &degraded_list) const
{
  CHECK_VALID;
  return palf_handle_impl_->get_ack_info_array(ack_info_array, degraded_list);
}

int PalfHandle::add_member(const common::ObMember &member,
                           const int64_t new_replica_num,
                           const LogConfigVersion &config_version,
                           const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->add_member(member, new_replica_num, config_version, timeout_us);
}

int PalfHandle::remove_member(const common::ObMember &member,
                              const int64_t new_replica_num,
                              const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->remove_member(member, new_replica_num, timeout_us);
}

int PalfHandle::replace_member(const common::ObMember &added_member,
                               const common::ObMember &removed_member,
                               const LogConfigVersion &config_version,
                               const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->replace_member(added_member, removed_member, config_version, timeout_us);
}

int PalfHandle::add_learner(const common::ObMember &added_learner, const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->add_learner(added_learner, timeout_us);
}

int PalfHandle::remove_learner(const common::ObMember &removed_learner, const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->remove_learner(removed_learner, timeout_us);
}

int PalfHandle::switch_learner_to_acceptor(const common::ObMember &learner,
                                           const int64_t new_replica_num,
                                           const LogConfigVersion &config_version,
                                           const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->switch_learner_to_acceptor(learner, new_replica_num, config_version, timeout_us);
}

int PalfHandle::switch_acceptor_to_learner(const common::ObMember &member,
                                           const int64_t new_replica_num,
                                           const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->switch_acceptor_to_learner(member, new_replica_num, timeout_us);
}

int PalfHandle::replace_learners(const common::ObMemberList &added_learners,
                                 const common::ObMemberList &removed_learners,
                                 const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->replace_learners(added_learners, removed_learners, timeout_us);
}

int PalfHandle::replace_member_with_learner(const common::ObMember &added_member,
                                            const common::ObMember &removed_member,
                                            const LogConfigVersion &config_version,
                                            const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->replace_member_with_learner(added_member, removed_member, config_version, timeout_us);
}

#ifdef OB_BUILD_ARBITRATION
int PalfHandle::add_arb_member(const common::ObMember &member,
                               const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->add_arb_member(member, timeout_us);
}

int PalfHandle::remove_arb_member(const common::ObMember &member,
                                  const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->remove_arb_member(member, timeout_us);
}

int PalfHandle::degrade_acceptor_to_learner(const LogMemberAckInfoList &degrade_servers,
                                            const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->degrade_acceptor_to_learner(degrade_servers, timeout_us);
}

int PalfHandle::upgrade_learner_to_acceptor(const LogMemberAckInfoList &upgrade_servers,
                                            const int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->upgrade_learner_to_acceptor(upgrade_servers, timeout_us);
}
#endif

int PalfHandle::change_leader_to(const common::ObAddr &dst_addr)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  ret = palf_handle_impl_->change_leader_to(dst_addr);
  return ret;
}

int PalfHandle::change_access_mode(const int64_t proposal_id,
                                   const int64_t mode_version,
                                   const AccessMode &access_mode,
                                   const SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  ret = palf_handle_impl_->change_access_mode(proposal_id, mode_version, access_mode, ref_scn);
  return ret;
}

int PalfHandle::get_access_mode(int64_t &mode_version, AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  ret = palf_handle_impl_->get_access_mode(mode_version, access_mode);
  return ret;
}

int PalfHandle::get_access_mode(AccessMode &access_mode) const
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  ret = palf_handle_impl_->get_access_mode(access_mode);
  return ret;
}

int PalfHandle::get_access_mode_ref_scn(int64_t &mode_version,
                                        AccessMode &access_mode,
                                        SCN &ref_scn) const
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  ret = palf_handle_impl_->get_access_mode_ref_scn(mode_version, access_mode, ref_scn);
  return ret;
}

int PalfHandle::disable_vote(const bool need_check_log_missing)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  ret = palf_handle_impl_->disable_vote(need_check_log_missing);
  return ret;
}

bool PalfHandle::is_vote_enabled() const
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  CHECK_VALID;
  bool_ret = palf_handle_impl_->is_vote_enabled();
  return bool_ret;
}

int PalfHandle::enable_vote()
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  ret = palf_handle_impl_->enable_vote();
  return ret;
}

int PalfHandle::register_file_size_cb(PalfFSCb *fs_cb)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (NULL == fs_cb) {
    PALF_LOG(TRACE, "no need register_file_size_cb", K(ret));
  } else if (NULL != fs_cb_) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "PalfHandle has register_file_size_cb, not support regist repeatedly", K(ret), K(fs_cb_), K(fs_cb));
  } else {
    PalfFSCbNode *fs_cb_node = MTL_NEW(PalfFSCbNode, "PalfFSCbNode", fs_cb);
    if (NULL == fs_cb_node) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(palf_handle_impl_->register_file_size_cb(fs_cb_node))) {
      PALF_LOG(WARN, "register_file_size_cb failed", K(ret));
    } else {
      fs_cb_ = fs_cb_node;
    }
    if (OB_FAIL(ret)) {
      MTL_DELETE(PalfFSCbNode, "PalfFSCbNode", fs_cb_);
      fs_cb_ = NULL;
    }
  }
  return ret;
}

int PalfHandle::unregister_file_size_cb()
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (NULL == fs_cb_) {
    PALF_LOG(TRACE, "no need unregister_file_size_cb", K(fs_cb_));
  } else if (OB_FAIL(palf_handle_impl_->unregister_file_size_cb(fs_cb_))) {
    PALF_LOG(WARN, "unregister_role_change_cb failed", K(ret));
  } else {
    MTL_DELETE(PalfFSCbNode, "PalfFSCbNode", fs_cb_);
    fs_cb_ = NULL;
  }
  return ret;
}

int PalfHandle::register_role_change_cb(PalfRoleChangeCb *rc_cb)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (NULL == rc_cb) {
    PALF_LOG(TRACE, "no need register_role_change_cb", K(ret));
  } else if (NULL != rc_cb_) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "PalfHandle has register_role_change_cb, not support regist repeatedly", K(ret), K(rc_cb), K(rc_cb_));
  } else {
    PalfRoleChangeCbNode *rc_cb_node = MTL_NEW(PalfRoleChangeCbNode, "PalfRCCbNode", rc_cb);
    if (NULL == rc_cb_node) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(palf_handle_impl_->register_role_change_cb(rc_cb_node))) {
      PALF_LOG(WARN, "register_role_change_cb failed", K(ret));
    } else {
      rc_cb_ = rc_cb_node;
    }
    if (OB_FAIL(ret)) {
      MTL_DELETE(PalfRoleChangeCbNode, "PalfRCCbNode", rc_cb_node);
      rc_cb_ = NULL;
    }
  }
  return ret;
}

int PalfHandle::unregister_role_change_cb()
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (NULL == rc_cb_) {
    PALF_LOG(TRACE, "no need unregister_role_change_cb", K(rc_cb_));
  } else if (OB_FAIL(palf_handle_impl_->unregister_role_change_cb(rc_cb_))) {
    PALF_LOG(WARN, "unregister_role_change_cb failed", K(ret));
  } else {
    MTL_DELETE(PalfRoleChangeCbNode, "PalfRCCbNode", rc_cb_);
    rc_cb_ = NULL;
  }
  return ret;
}

int PalfHandle::register_rebuild_cb(PalfRebuildCb *rebuild_cb)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (NULL == rebuild_cb) {
    PALF_LOG(TRACE, "no need rebuild_cb", K(ret));
  } else if (NULL != rebuild_cb_) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "PalfHandle has register_rebuild_cb, not support regist repeatedly",
        K(ret), K(rebuild_cb), K(rebuild_cb_));
  } else {
    PalfRebuildCbNode *rebuild_cb_node = MTL_NEW(PalfRebuildCbNode, "RebuildCbNode", rebuild_cb);
    if (NULL == rebuild_cb_node) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(palf_handle_impl_->register_rebuild_cb(rebuild_cb_node))) {
      PALF_LOG(WARN, "register_rebuild_cb failed", K(ret));
    } else {
      rebuild_cb_ = rebuild_cb_node;
    }
    if (OB_FAIL(ret)) {
      MTL_DELETE(PalfRebuildCbNode, "RebuildCbNode", rebuild_cb_node);
      rebuild_cb_ = NULL;
    }
  }
  return ret;
}

int PalfHandle::unregister_rebuild_cb()
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (NULL == rebuild_cb_) {
    PALF_LOG(TRACE, "no need unregister_rebuild_cb", K(ret));
  } else if (OB_FAIL(palf_handle_impl_->unregister_rebuild_cb(rebuild_cb_))) {
    PALF_LOG(WARN, "unregister_rebuild_cb failed", K(ret));
  } else {
    MTL_DELETE(PalfRebuildCbNode, "RebuildCbNode", rebuild_cb_);
    rebuild_cb_ = NULL;
    PALF_LOG(INFO, "unregister_rebuild_cb success", K(ret), KPC(this));
  }
	return ret;
}

int PalfHandle::set_location_cache_cb(PalfLocationCacheCb *lc_cb)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (OB_ISNULL(lc_cb)) {
    PALF_LOG(INFO, "no need set_location_cache_cb", KR(ret));
  } else if (OB_FAIL(palf_handle_impl_->set_location_cache_cb(lc_cb))) {
    PALF_LOG(WARN, "set_location_cache_cb failed", KR(ret));
  } else {
  }
  return ret;
}

int PalfHandle::reset_location_cache_cb()
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (OB_FAIL(palf_handle_impl_->reset_location_cache_cb())) {
    PALF_LOG(WARN, "reset_location_cache_cb failed", KR(ret));
  } else {
    PALF_LOG(INFO, "reset_location_cache_cb success", KR(ret));
  }
  return ret;
}

int PalfHandle::set_election_priority(election::ElectionPriority *priority)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (OB_ISNULL(priority)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "priority is nullptr", KR(ret), KP(priority));
  } else if (OB_FAIL(palf_handle_impl_->set_election_priority(priority))) {
    PALF_LOG(WARN, "set election priority to palf failed", KR(ret));
  }
  return ret;
}

int PalfHandle::reset_election_priority()
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (OB_FAIL(palf_handle_impl_->reset_election_priority())) {
    PALF_LOG(WARN, "set election priority to palf failed", KR(ret));
  }
  return ret;
}

int PalfHandle::advance_election_epoch_and_downgrade_priority(const int64_t proposal_id,
                                                              const int64_t downgrade_priority_time_us,
                                                              const char *reason)
{
  CHECK_VALID;
  return palf_handle_impl_->advance_election_epoch_and_downgrade_priority(proposal_id,
                                                                          downgrade_priority_time_us,
                                                                          reason);
}

int PalfHandle::set_locality_cb(PalfLocalityInfoCb *locality_cb)
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (OB_ISNULL(locality_cb)) {
    PALF_LOG(INFO, "no need set_locality_cb", KR(ret), KP(locality_cb));
  } else if (OB_FAIL(palf_handle_impl_->set_locality_cb(locality_cb))) {
    PALF_LOG(WARN, "set_locality_cb failed", KR(ret));
  } else {
  }
  return ret;
}

int PalfHandle::reset_locality_cb()
{
  int ret = OB_SUCCESS;
  CHECK_VALID;
  if (OB_FAIL(palf_handle_impl_->reset_locality_cb())) {
    PALF_LOG(WARN, "reset_locality_cb failed", KR(ret));
  } else {
    PALF_LOG(INFO, "reset_locality_cb success", KR(ret));
  }
  return ret;
}

int PalfHandle::stat(PalfStat &palf_stat) const
{
  CHECK_VALID;
  return palf_handle_impl_->stat(palf_stat);
}

int PalfHandle::try_lock_config_change(int64_t lock_owner,
                                       int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->try_lock_config_change(lock_owner, timeout_us);
}

int PalfHandle::unlock_config_change(int64_t lock_owner, int64_t timeout_us)
{
  CHECK_VALID;
  return palf_handle_impl_->unlock_config_change(lock_owner, timeout_us);
}

int PalfHandle::get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked)
{
  CHECK_VALID;
  return palf_handle_impl_->get_config_change_lock_stat(lock_owner, is_locked);
}

int PalfHandle::diagnose(PalfDiagnoseInfo &diagnose_info) const
{
  CHECK_VALID;
  return palf_handle_impl_->diagnose(diagnose_info);
}

int PalfHandle::raw_read(const palf::LSN &lsn,
                         void *buffer,
                         const int64_t nbytes,
                         int64_t &read_size)
{
  CHECK_VALID;
  return palf_handle_impl_->raw_read(lsn, reinterpret_cast<char*>(buffer), nbytes, read_size);
}

} // end namespace palf
} // end namespace oceanbase
