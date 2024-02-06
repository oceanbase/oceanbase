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

#include "logservice/ob_log_handler.h"
#include "logservice/palf/palf_handle.h"
#include "logservice/ob_log_service.h"
#include "logservice/palf/palf_env.h"
#include "share/scn.h"
#include "logservice/ob_log_base_type.h"

#ifndef MOCK_OB_LOG_HANDLER_H_
#define MOCK_OB_LOG_HANDLER_H_

namespace oceanbase
{
using namespace palf;

namespace storage
{

class MockObLogHandler : public logservice::ObILogHandler
{
public:
  MockObLogHandler(){};
  virtual bool is_valid() const { return true; }
  virtual int append(const void *buffer,
                     const int64_t nbytes,
                     const share::SCN &ref_scn,
                     const bool need_nonblock,
                     logservice::AppendCb *cb,
                     palf::LSN &lsn,
                     share::SCN &scn)
  {
    UNUSED(need_nonblock);
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(ref_scn);
    UNUSED(cb);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }

  virtual int append(const void *buffer,
                     const int64_t nbytes,
                     const int64_t ref_ts_ns,
                     const bool need_nonblock,
                     logservice::AppendCb *cb,
                     palf::LSN &lsn,
                     int64_t &ts_ns)
  {
    UNUSED(need_nonblock);
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(ref_ts_ns);
    UNUSED(cb);
    UNUSED(lsn);
    UNUSED(ts_ns);
    return OB_SUCCESS;
  }
  virtual int set_election_priority(palf::election::ElectionPriority *) { return OB_SUCCESS; }
  virtual int reset_election_priority() { return OB_SUCCESS; }
  virtual int get_role(common::ObRole &role, int64_t &proposal_id) const
  {
    UNUSED(role);
    UNUSED(proposal_id);
    return OB_SUCCESS;
  }
  virtual int get_access_mode(int64_t &mode_version, AccessMode &access_mode) const
  {
    UNUSED(access_mode);
    UNUSED(mode_version);
    return OB_SUCCESS;
  }

  virtual int get_append_mode_initial_scn(SCN &initial_scn) const
  {
    UNUSED(initial_scn);
    return OB_SUCCESS;
  }

  virtual int change_access_mode(const int64_t mode_version,
                                 const AccessMode &access_mode,
                                 const share::SCN &ref_scn)
  {
    UNUSED(mode_version);
    UNUSED(access_mode);
    UNUSED(ref_scn);
    return OB_SUCCESS;
  }
  virtual int change_access_mode(const int64_t mode_version,
                                 const AccessMode &access_mode,
                                 const int64_t ref_ts_ns)
  {
    UNUSED(mode_version);
    UNUSED(access_mode);
    UNUSED(ref_ts_ns);
    return OB_SUCCESS;
  }
  virtual int get_pending_end_lsn(palf::LSN &pending_end_lsn) const
  {
    UNUSED(pending_end_lsn);
    return OB_SUCCESS;
  }
  int seek(const palf::LSN &start_lsn, palf::PalfBufferIterator &iter)
  {
    UNUSED(start_lsn);
    UNUSED(iter);
    return OB_SUCCESS;
  };
  int seek(const palf::LSN &start_lsn,
           palf::PalfGroupBufferIterator &iter)
  {
    UNUSED(start_lsn);
    UNUSED(iter);
    return OB_SUCCESS;
  };
  int seek(const share::SCN &start_scn,
           palf::PalfGroupBufferIterator &iter)
  {
    UNUSED(start_scn);
    UNUSED(iter);
    return OB_SUCCESS;
  }
  int seek(const int64_t start_ts_ns,
           palf::PalfGroupBufferIterator &iter)
  {
    UNUSED(start_ts_ns);
    UNUSED(iter);
    return OB_SUCCESS;
  }
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list)
  {
    UNUSED(member_list);
    UNUSED(paxos_replica_num);
    UNUSED(learner_list);
    return OB_SUCCESS;
  }
  int set_initial_member_list(const common::ObMemberList &member_list,
                              const common::ObMember &arb_replica,
                              const int64_t paxos_replica_num,
                              const common::GlobalLearnerList &learner_list)
  {
    UNUSEDx(member_list, arb_replica, paxos_replica_num);
    UNUSED(learner_list);
    return OB_SUCCESS;
  }
  int get_end_scn(share::SCN &scn) const
  {
    UNUSED(scn);
    return OB_SUCCESS;
  }
  int get_end_ts_ns(int64_t &ts) const
  {
    UNUSED(ts);
    return OB_SUCCESS;
  }
  int get_global_learner_list(common::GlobalLearnerList &learner_list) const
  {
    UNUSED(learner_list);
    return OB_SUCCESS;
  }
  int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num) const
  {
    UNUSED(member_list);
    UNUSED(paxos_replica_num);
    return OB_SUCCESS;
  }
  int get_paxos_member_list_and_learner_list(common::ObMemberList &member_list,
                                             int64_t &paxos_replica_num,
                                             common::GlobalLearnerList &learner_list) const
  {
    UNUSEDx(member_list, paxos_replica_num, learner_list);
    return OB_SUCCESS;
  }
  int get_max_lsn(palf::LSN &lsn) const
  {
    UNUSED(lsn);
    return OB_SUCCESS;
  }
  int get_max_scn(share::SCN &scn) const
  {
    UNUSED(scn);
    return OB_SUCCESS;
  }
  int get_max_ts_ns(int64_t &ts_ns) const
  {
    UNUSED(ts_ns);
    return OB_SUCCESS;
  }
  int locate_by_scn_coarsely(const share::SCN &scn, LSN &result_lsn)
  {
    LSN tmp(scn.get_val_for_inner_table_field());
    result_lsn = tmp;
    return OB_SUCCESS;
  }
  int locate_by_ts_ns_coarsely(const int64_t ts_ns, LSN &result_lsn)
  {
    LSN tmp(ts_ns);
    result_lsn = tmp;
    return OB_SUCCESS;
  }

  int advance_base_lsn(const LSN &lsn)
  {
    base_lsn_ = lsn;
    return OB_SUCCESS;
  }

  int locate_by_lsn_coarsely(const palf::LSN &lsn, share::SCN &result_scn)
  {
    result_scn = result_scn_;
    return OB_SUCCESS;
  }

  int locate_by_lsn_coarsely(const palf::LSN &lsn, int64_t &result_ts_ns)
  {
    result_ts_ns = result_ts_ns_;
    return OB_SUCCESS;
  }

  int get_begin_lsn(LSN &lsn) const
  {
    UNUSED(lsn);
    return OB_SUCCESS;
  }

  int get_end_lsn(LSN &lsn) const
  {
    UNUSED(lsn);
    return OB_SUCCESS;
  }

  int get_palf_base_info(const palf::LSN &base_lsn, palf::PalfBaseInfo &palf_base_info)
  {
    UNUSED(base_lsn);
    UNUSED(palf_base_info);
    return OB_SUCCESS;
  }
  int is_in_sync(bool &is_log_sync, bool &is_need_rebuild) const
  {
    UNUSED(is_log_sync);
    UNUSED(is_need_rebuild);
    return OB_SUCCESS;
  }
  int advance_base_info(const palf::PalfBaseInfo &palf_base_info, const bool is_rebuild)
  {
    UNUSED(palf_base_info);
    UNUSED(is_rebuild);
    return OB_SUCCESS;
  }
  bool is_sync_enabled() const
  {
    return true;
  }
  bool is_replay_enabled() const
  {
    return true;
  }
  int enable_sync()
  {
    return OB_SUCCESS;
  }
  int disable_sync()
  {
    return OB_SUCCESS;
  }
  int get_leader_config_version(palf::LogConfigVersion &config_version) const
  {
    UNUSED(config_version);
    return OB_SUCCESS;
  }
  int change_replica_num(const common::ObMemberList &member_list,
                         const int64_t curr_replica_num,
                         const int64_t new_replica_num,
                         const int64_t timeout_us)
  {
    UNUSEDx(member_list, curr_replica_num, new_replica_num, timeout_us);
    return OB_SUCCESS;
  }
  int force_set_as_single_replica()
  {
    return OB_SUCCESS;
  }
  int add_member(const common::ObMember &member,
                 const int64_t paxos_replica_num,
                 const palf::LogConfigVersion &config_version,
                 const int64_t timeout_ns)
  {
    UNUSEDx(member, paxos_replica_num, config_version, timeout_ns);
    return OB_SUCCESS;
  }
  int remove_member(const common::ObMember &member,
                    const int64_t paxos_replica_num,
                    const int64_t timeout_ns)
  {
    UNUSED(member);
    UNUSED(paxos_replica_num);
    UNUSED(timeout_ns);
    return OB_SUCCESS;
  }
  int replace_member(const common::ObMember &added_member,
                     const common::ObMember &removed_member,
                     const palf::LogConfigVersion &config_version,
                     const int64_t timeout_ns)
  {
    UNUSED(added_member);
    UNUSED(removed_member);
    UNUSED(config_version);
    UNUSED(timeout_ns);
    return OB_SUCCESS;
  }
  int add_learner(const common::ObMember &added_learner,
                                const int64_t timeout_us)
  {
    int ret = OB_SUCCESS;
    UNUSED(added_learner);
    UNUSED(timeout_us);
    return ret;
  }

  int remove_learner(const common::ObMember &removed_learner,
                                  const int64_t timeout_us)
  {
    int ret = OB_SUCCESS;
    UNUSED(removed_learner);
    UNUSED(timeout_us);
    return ret;
  }

  int replace_learners(const common::ObMemberList &added_learners,
                       const common::ObMemberList &removed_learners,
                       const int64_t timeout_us)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(added_learners, removed_learners, timeout_us);
    return ret;
  }

  int replace_member_with_learner(const common::ObMember &added_member,
                                  const common::ObMember &removed_member,
                                  const palf::LogConfigVersion &config_version,
                                  const int64_t timeout_us)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(added_member, removed_member, timeout_us);
    return ret;
  }


  int switch_learner_to_acceptor(const common::ObMember &learner,
                                 const int64_t new_replica_num,
                                 const palf::LogConfigVersion &config_version,
                                 const int64_t timeout_us)
  {
    int ret = OB_SUCCESS;
    UNUSED(config_version);
    UNUSEDx(learner, new_replica_num, timeout_us);
    return ret;
  }
  int switch_acceptor_to_learner(const common::ObMember &member,
                                 const int64_t new_replica_num,
                                 const int64_t timeout_us)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(member, new_replica_num, timeout_us);
    return ret;
  }
  int add_arbitration_member(const common::ObMember &added_member, const int64_t timeout_us)
  {
    UNUSEDx(added_member, timeout_us);
    return OB_SUCCESS;
  }
  int remove_arbitration_member(const common::ObMember &removed_member, const int64_t timeout_us)
  {
    UNUSEDx(removed_member, timeout_us);
    return OB_SUCCESS;
  }
  int degrade_acceptor_to_learner(const palf::LogMemberAckInfoList &degrade_servers,
                                  const int64_t timeout_us)
  {
    UNUSEDx(degrade_servers, timeout_us);
    return OB_SUCCESS;
  }
  int upgrade_learner_to_acceptor(const palf::LogMemberAckInfoList &upgrade_servers,
                                  const int64_t timeout_us)
  {
    UNUSEDx(upgrade_servers, timeout_us);
    return OB_SUCCESS;
  }
  int get_member_gc_stat(const common::ObAddr &addr, bool &is_valid_member, obrpc::LogMemberGCStat &stat) const
  {
    UNUSEDx(addr, is_valid_member, stat);
    return OB_SUCCESS;
  }
  int set_region(const common::ObRegion &region)
  {
    UNUSED(region);
    return OB_SUCCESS;
  }
  void wait_append_sync()
  {
    return;
  }

  int pend_submit_replay_log()
  {
    return OB_SUCCESS;
  }

  int restore_submit_replay_log()
  {
    return OB_SUCCESS;
  }

  int try_lock_config_change(const int64_t lock_owner, const int64_t timeout_us)
  {
    UNUSED(lock_owner);
    UNUSED(timeout_us);
    return OB_SUCCESS;
  }
  int unlock_config_change(const int64_t lock_owner, const int64_t timeout_us)
  {
    UNUSED(lock_owner);
    UNUSED(timeout_us);
    return OB_SUCCESS;
  }
  int get_config_change_lock_stat(int64_t &lock_owner, bool &is_locked)
  {
    lock_owner = palf::OB_INVALID_CONFIG_CHANGE_LOCK_OWNER;
    is_locked = false;
    return OB_SUCCESS;
  }

  LSN base_lsn_;
  int64_t result_ts_ns_;
  share::SCN result_scn_;
  int enable_replay(const palf::LSN &initial_lsn,
                    const share::SCN &initial_scn)
  {
    UNUSED(initial_lsn);
    UNUSED(initial_scn);
    return OB_SUCCESS;
  }
  int enable_replay(const palf::LSN &initial_lsn,
                    const int64_t &initial_log_ts)
  {
    UNUSED(initial_lsn);
    UNUSED(initial_log_ts);
    return OB_SUCCESS;
  }
  int disable_replay()
  {
    return OB_SUCCESS;
  }
  int get_max_decided_scn(share::SCN &scn)
  {
    scn.set_max();
    return OB_SUCCESS;
  }
  int get_max_decided_log_ts_ns(int64_t &log_ts)
  {
    log_ts = INT64_MAX;
    return OB_SUCCESS;
  }
  int enable_vote() { return OB_SUCCESS; }
  int disable_vote(const bool need_check_log_missing)
  {
    UNUSED(need_check_log_missing);
    return OB_SUCCESS;
  }
  int get_election_leader(common::ObAddr &addr) const
  {
    UNUSED(addr);
    return OB_SUCCESS;
  }
  int register_rebuild_cb(palf::PalfRebuildCb *rebuild_cb)
  {
    UNUSED(rebuild_cb);
    return OB_SUCCESS;
  }
  int unregister_rebuild_cb() { return OB_SUCCESS; }
  bool is_offline() const {return false;};
  int offline() {return OB_SUCCESS;};
  int online(const LSN &lsn, const share::SCN &scn) { UNUSED(lsn); UNUSED(scn); return OB_SUCCESS;};
};

}  // namespace storage
}  // namespace oceanbase


#endif

