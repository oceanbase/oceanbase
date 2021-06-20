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

#ifndef OCEANBASE_UNITTEST_CLOG_MOCK_LOG_MEMBERSHIPMGR
#define OCEANBASE_UNITTEST_CLOG_MOCK_LOG_MEMBERSHIPMGR
#include "clog/ob_log_membership_mgr_V2.h"
#include "clog/ob_i_submit_log_cb.h"
#include "clog/ob_log_define.h"
#include "clog/ob_log_type.h"
#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace election {
class ObIElectionMgr;
}

namespace clog {
class ObILogSWForMS;
class ObILogStateMgrForMS;
class ObILogCallbackEngine;
class ObLogEntry;
class ObLogEntryHeader;

class MockObLogMembershipMgr : public ObILogMembershipMgr, public ObISubmitLogCb {
public:
  MockObLogMembershipMgr()
  {}
  virtual ~MockObLogMembershipMgr()
  {}

public:
  int init(const common::ObMemberList& member_list, const int64_t membership_timestamp,
      const uint64_t membership_log_id, const int64_t replica_num, const common::ObAddr& self,
      const common::ObPartitionKey& partition_key, ObILogSWForMS* sw, ObILogStateMgrForMS* state_mgr,
      ObILogCallbackEngine* cb_engine, election::ObIElectionMgr* election_mgr)
  {
    UNUSED(member_list);
    UNUSED(membership_timestamp);
    UNUSED(membership_log_id);
    UNUSED(replica_num);
    UNUSED(self);
    UNUSED(partition_key);
    UNUSED(sw);
    UNUSED(state_mgr);
    UNUSED(cb_engine);
    UNUSED(election_mgr);
    return OB_SUCCESS;
  }
  bool is_state_changed()
  {
    return true;
  }
  int switch_state()
  {
    return OB_SUCCESS;
  }
  int receive_recovery_log(
      const ObLogEntry& log_entry, const bool is_confirmed, const int64_t accum_checksum, const bool is_batch_committed)
  {
    UNUSED(log_entry);
    UNUSED(is_confirmed);
    UNUSED(accum_checksum);
    UNUSED(is_batch_committed);
    return OB_SUCCESS;
  }
  int add_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
  {
    UNUSED(member);
    UNUSED(quorum);
    UNUSED(log_info);
    return OB_SUCCESS;
  }
  int remove_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
  {
    UNUSED(member);
    UNUSED(quorum);
    UNUSED(log_info);
    return OB_SUCCESS;
  }
  int64_t get_replica_num() const
  {
    return 3;
  }
  common::ObReplicaType get_replica_type() const
  {
    return REPLICA_TYPE_MAX;
  }
  common::ObReplicaProperty get_replica_property() const
  {
    return ObReplicaProperty();
  }
  const common::ObMemberList& get_curr_member_list() const
  {
    return member_list_;
  }
  virtual int get_lower_level_replica_list(common::ObChildReplicaList& list) const
  {
    UNUSED(list);
    return 0;
  }
  virtual int check_paxos_children() const
  {
    return 0;
  }
  virtual void reset_children_list()
  {
    return;
  }
  virtual int set_children_list_to_curr_member_list()
  {
    return 0;
  }
  virtual int try_add_member_to_children_list(
      const ObMember& member, const common::ObRegion& region, const common::ObReplicaType replica_type)
  {
    UNUSED(member);
    UNUSED(region);
    UNUSED(replica_type);
    return 0;
  }
  virtual int try_replace_add_to_children_list(const ObMember& member, const common::ObRegion& region,
      const common::ObReplicaType replica_type, const int64_t next_ilog_ts)
  {
    UNUSED(member);
    UNUSED(region);
    UNUSED(replica_type);
    UNUSED(next_ilog_ts);
    return 0;
  }
  virtual int try_remove_server_from_children_list(const common::ObAddr& server)
  {
    UNUSED(server);
    return 0;
  }
  virtual bool is_children_list_full()
  {
    return true;
  }
  virtual int receive_log(
      const ObLogEntry& log_entry, const common::ObAddr& server, const int64_t cluster_id, const ReceiveLogType rl_type)
  {
    UNUSED(log_entry);
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(rl_type);
    return 0;
  }
  virtual int append_disk_log(const ObLogEntry& log_entry, const ObLogCursor& log_cursor, const int64_t accum_checksum,
      const bool batch_committed)
  {
    UNUSED(log_entry);
    UNUSED(log_cursor);
    UNUSED(accum_checksum);
    UNUSED(batch_committed);
    return 0;
  }
  virtual int64_t get_timestamp() const
  {
    return 0;
  }
  virtual uint64_t get_log_id() const
  {
    return 0;
  }
  virtual void reset_status()
  {
    return;
  }
  virtual int write_start_membership(const ObLogType& log_type)
  {
    UNUSED(log_type);
    return 0;
  }
  virtual bool is_state_init() const
  {
    return true;
  }
  virtual void reset_follower_pending_entry()
  {
    return;
  }
  virtual void submit_success_cb_task(const ObLogType& log_type, const uint64_t log_id, const char* log_buf,
      const int64_t log_buf_len, const common::ObProposalID& proposal_id)
  {
    UNUSED(log_type);
    UNUSED(log_id);
    UNUSED(log_buf);
    UNUSED(log_buf_len);
    UNUSED(proposal_id);
    return;
  }
  virtual void reconfirm_update_status(const ObLogEntry& log_entry)
  {
    UNUSED(log_entry);
    return;
  }
  virtual int force_set_as_single_replica()
  {
    return 0;
  }
  virtual int force_set_replica_num(const int64_t replica_num)
  {
    UNUSED(replica_num);
    return 0;
  }
  virtual int set_replica_type(const enum ObReplicaType replica_type)
  {
    UNUSED(replica_type);
    return 0;
  }
  virtual int change_member_list_to_self(const int64_t new_membership_version)
  {
    UNUSED(new_membership_version);
    return OB_SUCCESS;
  }
  virtual int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type,
      const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed)
  {
    UNUSED(partition_key);
    UNUSED(log_type);
    UNUSED(log_id);
    UNUSED(version);
    UNUSED(batch_committed);
    UNUSED(batch_last_succeed);
    return 0;
  }
  virtual int inc_replica_num(const int64_t replica_num)
  {
    UNUSED(replica_num);
    return OB_NOT_SUPPORTED;
  }
  virtual int dec_replica_num(const int64_t replica_num)
  {
    UNUSED(replica_num);
    return OB_NOT_SUPPORTED;
  }
  virtual int change_quorum(const common::ObMemberList& curr_member_list, const int64_t curr_quorum,
      const int64_t new_quorum, obrpc::ObMCLogInfo& log_info)
  {
    UNUSED(curr_member_list);
    UNUSED(curr_quorum);
    UNUSED(new_quorum);
    UNUSED(log_info);
    return 0;
  }
  virtual common::ObProposalID get_ms_proposal_id() const
  {
    return ms_proposal_id_;
  }
  virtual int update_ms_proposal_id(const common::ObProposalID& ms_proposal_id)
  {
    UNUSED(ms_proposal_id);
    return OB_SUCCESS;
  }

  virtual bool has_elect_mlist() const
  {
    return false;
  }
  virtual int get_curr_ms_log_body(ObLogEntryHeader& log_entry_header, char* buffer, const int64_t buf_len) const
  {
    UNUSED(log_entry_header);
    UNUSED(buffer);
    UNUSED(buf_len);
    return OB_SUCCESS;
  }
  virtual int reset_renew_ms_log_task()
  {
    return OB_SUCCESS;
  }
  virtual bool is_renew_ms_log_majority_success() const
  {
    return true;
  }
  virtual int check_renew_ms_log_sync_state() const
  {
    return OB_SUCCESS;
  }

private:
  common::ObMemberList member_list_;
  common::ObProposalID ms_proposal_id_;
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_CLOG_MOCK_LOG_MEMBERSHIPMGR
