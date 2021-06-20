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

#ifndef OCEANBASE_CLOG_OB_LOG_MEMBERSHIP_MGR_
#define OCEANBASE_CLOG_OB_LOG_MEMBERSHIP_MGR_

#include "lib/lock/ob_spin_lock.h"
#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"
#include "share/ob_rpc_struct.h"
#include "ob_i_submit_log_cb.h"
#include "ob_log_define.h"
#include "ob_log_entry.h"
#include "ob_log_type.h"
#include "ob_log_req.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace common {
class ObILogAllocator;
}

namespace clog {
class ObILogSWForMS;
class ObLogMembershipTaskMgr;
class ObILogStateMgrForMS;
class ObLogCascadingMgr;
class ObILogCallbackEngine;
class ObLogEntry;
class ObLogEntryHeader;

static const int64_t OB_DEFAULT_MC_LOG_INFO_COUNT = 16;
typedef common::ObSEArray<obrpc::ObMCLogInfo, OB_DEFAULT_MC_LOG_INFO_COUNT> ObMCLogInfoArray;

class ObILogMembershipMgr {
public:
  ObILogMembershipMgr()
  {}
  virtual ~ObILogMembershipMgr()
  {}

public:
  virtual bool is_state_changed() = 0;
  virtual int switch_state() = 0;
  virtual int add_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info) = 0;
  virtual int remove_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info) = 0;
  virtual int change_quorum(const common::ObMemberList& curr_member_list, const int64_t curr_quorum,
      const int64_t new_quorum, obrpc::ObMCLogInfo& log_info) = 0;
  virtual int64_t get_replica_num() const = 0;
  virtual common::ObReplicaType get_replica_type() const = 0;
  virtual common::ObReplicaProperty get_replica_property() const = 0;
  virtual const common::ObMemberList& get_curr_member_list() const = 0;
  virtual int receive_log(const ObLogEntry& log_entry, const common::ObAddr& server, const int64_t cluster_id,
      const ReceiveLogType rl_type) = 0;
  virtual int receive_recovery_log(const ObLogEntry& log_entry, const bool is_confirmed, const int64_t accum_checksum,
      const bool is_batch_committed) = 0;
  virtual int append_disk_log(const ObLogEntry& log_entry, const ObLogCursor& log_cursor, const int64_t accum_checksum,
      const bool batch_committed) = 0;
  virtual int64_t get_timestamp() const = 0;
  virtual uint64_t get_log_id() const = 0;
  virtual void reset_status() = 0;
  virtual int write_start_membership(const ObLogType& log_type) = 0;
  virtual bool is_state_init() const = 0;
  virtual void reset_follower_pending_entry() = 0;
  virtual void submit_success_cb_task(const ObLogType& log_type, const uint64_t log_id, const char* log_buf,
      const int64_t log_buf_len, const common::ObProposalID& proposal_id) = 0;
  // When a member change log is encountered in the reconfirm phase, try to update the member group and other
  // information
  virtual void reconfirm_update_status(const ObLogEntry& log_entry) = 0;
  virtual int force_set_as_single_replica() = 0;
  virtual int force_set_replica_num(const int64_t replica_num) = 0;
  virtual int set_replica_type(const common::ObReplicaType replica_type) = 0;
  virtual int change_member_list_to_self(const int64_t new_membership_version) = 0;
  virtual common::ObProposalID get_ms_proposal_id() const = 0;
  virtual int update_ms_proposal_id(const common::ObProposalID& ms_proposal_id) = 0;
  virtual int get_curr_ms_log_body(ObLogEntryHeader& log_entry_header, char* buffer, const int64_t buf_len) const = 0;
  virtual int reset_renew_ms_log_task() = 0;
  virtual bool is_renew_ms_log_majority_success() const = 0;
  virtual int check_renew_ms_log_sync_state() const = 0;
};

class ObLogMembershipMgr : public ObILogMembershipMgr, public ObISubmitLogCb {
public:
  ObLogMembershipMgr();
  virtual ~ObLogMembershipMgr();

public:
  // When creating Partition,membership_timestamp is 0,membership_log_id is 0
  // When observer is restarted, the value is obtained from BaseStorageInfo
  int init(const common::ObMemberList& member_list, const int64_t membership_timestamp,
      const uint64_t membership_log_id, const common::ObProposalID& ms_proposal_id,
      const common::ObReplicaType replica_type, const common::ObReplicaProperty replica_property,
      const int64_t replica_num, const bool need_skip_mlist_check, const common::ObAddr& self,
      const common::ObPartitionKey& partition_key, ObILogSWForMS* sw, ObLogMembershipTaskMgr* ms_task_mgr,
      ObILogStateMgrForMS* state_mgr, ObLogCascadingMgr* cascading_mgr, ObILogCallbackEngine* cb_engine,
      storage::ObPartitionService* partition_service, common::ObILogAllocator* alloc_mgr);
  virtual bool is_state_changed() override;
  virtual int switch_state() override;
  virtual int add_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info) override;
  virtual int remove_member(
      const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info) override;
  virtual int change_quorum(const common::ObMemberList& curr_member_list, const int64_t curr_quorum,
      const int64_t new_quorum, obrpc::ObMCLogInfo& log_info) override;
  virtual int64_t get_replica_num() const override;
  virtual common::ObReplicaType get_replica_type() const override;
  virtual common::ObReplicaProperty get_replica_property() const override;
  virtual const common::ObMemberList& get_curr_member_list() const override;
  virtual int receive_log(const ObLogEntry& log_entry, const common::ObAddr& server, const int64_t cluster_id,
      const ReceiveLogType rl_type) override;
  virtual int receive_recovery_log(const ObLogEntry& log_entry, const bool is_confirmed, const int64_t accum_checksum,
      const bool is_batch_committed) override;
  virtual int append_disk_log(const ObLogEntry& log_entry, const ObLogCursor& log_cursor, const int64_t accum_checksum,
      const bool batch_committed) override;
  virtual int force_set_member_list(const common::ObMemberList& member_list, const int64_t replica_num);
  virtual int set_membership_info(const common::ObMemberList& member_list, const int64_t membership_timestamp,
      const uint64_t membership_log_id, const int64_t replica_num, const common::ObProposalID& ms_proposal_id);
  virtual int64_t get_timestamp() const override;
  virtual uint64_t get_log_id() const override
  {
    return membership_log_id_;
  }
  virtual void reset_status() override;
  virtual int write_start_membership(const ObLogType& log_type) override;
  virtual int on_success(const common::ObPartitionKey& partition_key, const clog::ObLogType log_type,
      const uint64_t log_id, const int64_t version, const bool batch_committed, const bool batch_last_succeed) override;
  bool is_inited() const
  {
    return is_inited_;
  }
  virtual bool is_state_init() const override;
  virtual void reset_follower_pending_entry() override;
  virtual void submit_success_cb_task(const ObLogType& log_type, const uint64_t log_id, const char* log_buf,
      const int64_t log_buf_len, const common::ObProposalID& proposal_id) override;
  virtual void reconfirm_update_status(const ObLogEntry& log_entry) override;
  virtual int force_set_as_single_replica() override;
  virtual int force_set_replica_num(const int64_t replica_num) override;
  // Only keep members existing in server_list in member_list
  // Used to force a member list
  virtual int force_set_server_list(const obrpc::ObServerList& server_list, const int64_t replica_num);
  void destroy();
  virtual int set_replica_type(const enum ObReplicaType replica_type) override;
  virtual int change_member_list_to_self(const int64_t new_membership_version) override;
  common::ObProposalID get_ms_proposal_id() const override
  {
    return ms_proposal_id_;
  }
  int update_ms_proposal_id(const common::ObProposalID& ms_proposal_id) override;
  int check_follower_has_pending_entry(bool& has_pending);
  int get_curr_ms_log_body(ObLogEntryHeader& log_entry_header, char* buffer, const int64_t buf_len) const override;
  int reset_renew_ms_log_task() override;
  bool is_renew_ms_log_majority_success() const override;
  int check_renew_ms_log_sync_state() const override;

private:
  enum ObMsState {
    MS_INIT,
    MS_PENDING,
    MS_CHANGING,
  };
  enum ObPendingOpType {
    MS_OP_LEADER,
    MS_OP_STANDBY_LEADER,
  };
  static const int64_t MS_TYPE_UNKNOWN = 0;
  static const int64_t MS_TYPE_ADD_MEMBER = 1;
  static const int64_t MS_TYPE_REMOVE_MEMBER = 3;
  static const int64_t MS_TYPE_START_MEMBERSHIP = 4;
  static const int64_t MS_TYPE_FORCE_MEMBERSHIP = 5;
  static const int64_t MS_TYPE_CHANGE_QUORUM = 6;
  static const int64_t MS_TYPE_RENEW_MEMBERSHIP = 7;
  bool check_barrier_condition_(const uint64_t log_id) const;
  bool check_on_success_called_();
  int pending_to_changing_();
  int to_changing_();
  int changing_to_init_();

  int generate_log_(ObLogEntryHeader& log_entry_header, char* buffer, const int64_t buf_len, const ObLogType log_type,
      const uint64_t membership_log_id, const int64_t replica_num, const int64_t membership_timestamp,
      const int64_t ms_type);
  int generate_start_log_(
      const ObLogType log_type, char* buffer, const int64_t buf_len, int64_t& membership_timestamp, int64_t& log_size);
  int standby_generate_log_(ObLogEntryHeader& log_entry_header, ObRenewMembershipLog& ms_log, char* buffer,
      const int64_t buf_len, const ObLogType log_type, const uint64_t membership_log_id, const int64_t replica_num,
      const int64_t membership_timestamp, const int64_t ms_type);
  int standby_generate_start_log_(const ObLogType log_type, ObRenewMembershipLog& ms_log, char* buffer,
      const int64_t buf_len, int64_t& membership_timestamp, int64_t& log_size);
  void generate_curr_member_list_();
  void reset_pending_status_();
  void update_status_(const ObLogEntry& log_entry, const ObMembershipLog& ms_log);
  void update_status_(const ObLogEntry& log_entry, const ObRenewMembershipLog& ms_log);
  void set_election_candidate_(const bool is_force_set = false);
  bool check_follower_pending_entry_();
  int handle_follower_pending_entry_();
  int receive_log_without_lock_(const ObLogEntry& log_entry, const common::ObAddr& server, const int64_t cluster_id,
      const ReceiveLogType rl_type);
  void submit_success_cb_task_(const ObLogType log_type);
  void reset_status_without_lock_();
  bool need_skip_apply_ms_log_(const ObMembershipLog& ms_log) const;
  int try_check_cluster_state_();

private:
  bool is_inited_;
  bool need_check_cluster_type_;

  ObILogSWForMS* sw_;
  ObLogMembershipTaskMgr* ms_task_mgr_;
  ObILogStateMgrForMS* state_mgr_;
  ObLogCascadingMgr* cascading_mgr_;
  ObILogCallbackEngine* cb_engine_;
  storage::ObPartitionService* partition_service_;
  common::ObILogAllocator* alloc_mgr_;

  mutable common::SpinRWLock lock_;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;
  ObMsState ms_state_;
  common::ObMemberList curr_member_list_;
  common::ObMemberList prev_member_list_;
  int64_t membership_timestamp_;
  uint64_t membership_log_id_;
  uint64_t last_submit_cb_log_id_;  // The log_id of the last successful submission of success_cb
  int64_t ms_type_;
  common::ObProposalID ms_proposal_id_;

  common::ObMember pending_member_for_add_;
  common::ObMember pending_member_for_remove_;
  int64_t pending_ms_timestamp_;
  uint64_t pending_ms_log_id_;
  int64_t pending_ms_type_;
  int64_t pending_op_type_;
  int64_t pending_replica_num_;
  bool on_success_called_;

  bool is_single_replica_mode_;
  bool has_follower_pending_entry_;
  common::ObReplicaType replica_type_;
  common::ObReplicaProperty replica_property_;
  int64_t replica_num_;
  common::ObAddr self_;
  common::ObPartitionKey partition_key_;

  ObLogEntry follower_pending_entry_;
  common::ObAddr follower_pending_entry_server_;
  int64_t follower_pending_entry_server_cluster_id_;
  ReceiveLogType follower_pending_entry_type_;

  DISALLOW_COPY_AND_ASSIGN(ObLogMembershipMgr);
};
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_MEMBERSHIP_MGR_H_
