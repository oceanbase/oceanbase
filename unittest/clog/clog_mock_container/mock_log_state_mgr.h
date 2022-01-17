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

#ifndef OCEANBASE_CLOG_MOCK_OB_LOG_STATE_MGR_H_
#define OCEANBASE_CLOG_MOCK_OB_LOG_STATE_MGR_H_
#include "clog/ob_log_state_mgr.h"

namespace oceanbase {
namespace election {
class ObIElectionMgr;
}
namespace share {
class ObIPSCb;
}

namespace clog {
class ObILogSWForStateMgr;
class ObILogReconfirm;
class ObILogEngine;
class ObILogMembershipMgr;
class ObILogReplayEngineWrapper;
class ObILogCallbackEngine;
class ObILogAllocator;

class MockObLogStateMgr : public ObLogStateMgr {
public:
  MockObLogStateMgr()
  {}
  ~MockObLogStateMgr()
  {
    destroy();
  }

public:
  int init(ObILogSWForStateMgr* sw, ObILogReconfirm* reconfirm, ObILogEngine* log_engine, ObILogMembershipMgr* mm,
      election::ObIElectionMgr* election_mgr, ObILogReplayEngineWrapper* replay_engine, share::ObIPSCb* ps_cb,
      ObILogAllocator* alloc_mgr, const common::ObAddr& self, const common::ObProposalID& proposal_id,
      const common::ObVersion& freeze_version, const common::ObPartitionKey& partition_key)
  {
    UNUSED(sw);
    UNUSED(reconfirm);
    UNUSED(log_engine);
    UNUSED(mm);
    UNUSED(election_mgr);
    UNUSED(replay_engine);
    UNUSED(ps_cb);
    UNUSED(alloc_mgr);
    UNUSED(self);
    UNUSED(proposal_id);
    UNUSED(freeze_version);
    UNUSED(partition_key);
    return OB_SUCCESS;
  }
  bool can_submit_log() const
  {
    return true;
  }
  bool can_append_disk_log() const
  {
    return true;
  }
  bool can_majority_cb(const common::ObProposalID& proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  bool can_receive_log_ack(const common::ObProposalID& proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  bool can_send_log_ack(const common::ObProposalID& proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  bool can_receive_max_log_id(const common::ObProposalID& proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  bool can_get_log() const
  {
    return true;
  }
  bool can_get_log(const common::ObProposalID& proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  bool can_receive_log(const common::ObProposalID& proposal_id, const common::ObProposalID& proposal_id_in_log) const
  {
    UNUSED(proposal_id);
    UNUSED(proposal_id_in_log);
    return true;
  }
  bool can_change_member() const
  {
    return true;
  }
  bool can_get_base_storage_info() const
  {
    return true;
  }
  bool can_change_leader() const
  {
    return true;
  }
  bool can_get_leader_curr_member_list() const
  {
    return true;
  }
  bool can_handle_prepare_rqst(const common::ObProposalID& proposal_id) const
  {
    UNUSED(proposal_id);
    return true;
  }
  bool can_slide_sw() const
  {
    return true;
  }
  bool can_receive_confirmed_info() const
  {
    return true;
  }
  bool need_quicker_polling() const
  {
    return true;
  }

  common::ObProposalID get_proposal_id() const
  {
    common::ObProposalID proposal_id;
    return proposal_id;
  }
  int switch_state()
  {
    return OB_SUCCESS;
  }
  bool is_state_changed()
  {
    return true;
  }
  int set_scan_disk_log_finished()
  {
    return OB_SUCCESS;
  }
  ObLogState get_state() const
  {
    ObLogState state = UNKNOWN;
    return state;
  }
  common::ObRole get_role() const
  {
    return INVALID_ROLE;
  }
  common::ObAddr get_leader() const
  {
    common::ObAddr leader;
    return leader;
  }
  common::ObVersion get_freeze_version() const
  {
    common::ObVersion freeze_version;
    return freeze_version;
  }
  int update_freeze_version(const common::ObVersion& freeze_version)
  {
    UNUSED(freeze_version);
    return OB_SUCCESS;
  }
  int handle_prepare_rqst(const common::ObProposalID& proposal_id, const common::ObAddr& new_leader)
  {
    UNUSED(proposal_id);
    UNUSED(new_leader);
    return OB_SUCCESS;
  }
  int update_proposal_id(const common::ObProposalID& proposal_id)
  {
    UNUSED(proposal_id);
    return OB_SUCCESS;
  }
  int stop_election()
  {
    return OB_SUCCESS;
  }
  int set_election_leader(const common::ObAddr& leader, const int64_t lease_start)
  {
    UNUSED(leader);
    UNUSED(lease_start);
    return OB_SUCCESS;
  }
  void set_removed()
  {}
  void set_temporary_replica()
  {}
  void reset_temporary_replica()
  {}
  bool is_temporary_replica() const
  {
    return true;
  }
  bool is_changing_leader() const
  {
    return true;
  }
  int change_leader_async(const common::ObPartitionKey& partition_key, const ObAddr& leader)
  {
    UNUSED(partition_key);
    UNUSED(leader);
    return OB_SUCCESS;
  }
  bool is_inited() const
  {
    return true;
  }
  void destroy()
  {}

  DISALLOW_COPY_AND_ASSIGN(MockObLogStateMgr);
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_STATE_MGR_V2_H_
