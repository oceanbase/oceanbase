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

#ifndef OCEANBASE_CLOG_OB_PARTITION_LOG_SERVICE_
#define OCEANBASE_CLOG_OB_PARTITION_LOG_SERVICE_

#include "lib/lock/ob_tc_rwlock.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/utility/utility.h"
#include "storage/ob_saved_storage_info.h"
#include "storage/ob_slog_writer_queue_thread.h"
#include "election/ob_election_cb.h"
#include "election/ob_election.h"
#include "ob_log_checksum_V2.h"
#include "ob_log_common.h"
#include "ob_log_define.h"
#include "ob_log_flush_task.h"
#include "ob_log_membership_mgr_V2.h"
#include "ob_log_reconfirm.h"
#include "ob_log_sliding_window.h"
#include "ob_log_state_mgr.h"
#include "ob_log_virtual_stat.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "ob_log_broadcast_info_mgr.h"
#include "ob_log_cascading_mgr.h"
#include "ob_log_membership_task_mgr.h"
#include "ob_log_restore_mgr.h"

namespace oceanbase {
namespace common {
class ObILogAllocator;
}
namespace election {
class ObElectionPriority;
class ObIElectionMgr;
}  // namespace election
namespace transaction {
class ObTransID;
class ObTransTraceLog;
}  // namespace transaction
namespace archive {
class ObArchiveMgr;
class ObArchiveRestoreEngine;
}  // namespace archive
namespace clog {
struct ObReadBuf;
class ObIFetchLogEngine;
class ObILogEngine;
class ObLogReplayEngineWrapper;
class ObLogEntry;
class ObISubmitLogCb;
class ObLogEventScheduler;
class ObRemoteLogQueryEngine;

class ObCandidateInfo {
public:
  ObCandidateInfo()
  {
    reset();
  }
  ~ObCandidateInfo()
  {}

public:
  void reset();
  common::ObRole get_role() const
  {
    return role_;
  }
  void set_role(const common::ObRole& role)
  {
    role_ = role;
  }
  bool is_tenant_active() const
  {
    return is_tenant_active_;
  }
  void set_tenant_active(const bool is_tenant_active)
  {
    is_tenant_active_ = is_tenant_active;
  }
  bool is_dest_splitting() const
  {
    return is_dest_splitting_;
  }
  void set_dest_splitting(const bool is_dest_splitting)
  {
    is_dest_splitting_ = is_dest_splitting;
  }
  bool on_revoke_blacklist() const
  {
    return on_revoke_blacklist_;
  }
  void set_on_revoke_blacklist(const bool on_revoke_blacklist)
  {
    on_revoke_blacklist_ = on_revoke_blacklist;
  }
  bool on_loop_blacklist() const
  {
    return on_loop_blacklist_;
  }
  void set_on_loop_blacklist(const bool on_loop_blacklist)
  {
    on_loop_blacklist_ = on_loop_blacklist;
  }
  common::ObReplicaType get_replica_type() const
  {
    return replica_type_;
  }
  void set_replica_type(const common::ObReplicaType& replica_type)
  {
    replica_type_ = replica_type;
  }
  share::RSServerStatus get_server_status() const
  {
    return server_status_;
  }
  void set_server_status(const share::RSServerStatus& server_status)
  {
    server_status_ = server_status;
  }
  bool is_clog_disk_full() const
  {
    return is_clog_disk_full_;
  }
  void set_clog_disk_full(const bool is_clog_disk_full)
  {
    is_clog_disk_full_ = is_clog_disk_full;
  }
  bool is_offline() const
  {
    return is_offline_;
  }
  void set_offline(const bool is_offline)
  {
    is_offline_ = is_offline;
  }
  bool is_need_rebuild() const
  {
    return is_need_rebuild_;
  }
  void set_need_rebuild(const bool is_need_rebuild)
  {
    is_need_rebuild_ = is_need_rebuild;
  }
  bool is_partition_candidate() const
  {
    return is_partition_candidate_;
  }
  void set_is_partition_candidate(const bool is_partition_candidate)
  {
    is_partition_candidate_ = is_partition_candidate;
  }
  bool is_disk_error() const
  {
    return is_disk_error_;
  }
  void set_is_disk_error(const bool is_disk_error)
  {
    is_disk_error_ = is_disk_error;
  }
  int64_t get_memstore_percent() const
  {
    return memstore_percent_;
  }
  void set_memstore_percent(const int64_t memstore_percent)
  {
    memstore_percent_ = memstore_percent;
  }

private:
  common::ObRole role_;
  bool is_tenant_active_;
  bool is_dest_splitting_;
  bool on_revoke_blacklist_;
  bool on_loop_blacklist_;
  common::ObReplicaType replica_type_;
  share::RSServerStatus server_status_;
  bool is_clog_disk_full_;
  bool is_offline_;
  bool is_need_rebuild_;
  bool is_partition_candidate_;
  bool is_disk_error_;
  int64_t memstore_percent_;
};

class ObCursorArrayCache {
public:
  ObCursorArrayCache()
  {}
  ~ObCursorArrayCache()
  {}

public:
  int init();

public:
  static const int64_t CURSOR_ARRAY_SIZE = 20;
  uint64_t cursor_start_log_id_[SCAN_THREAD_CNT];
  uint64_t cursor_end_log_id_[SCAN_THREAD_CNT];
  ObLogCursorExt cursor_array_[SCAN_THREAD_CNT][CURSOR_ARRAY_SIZE];

private:
  DISALLOW_COPY_AND_ASSIGN(ObCursorArrayCache);
};

class ObIPartitionLogService : public ObILogFlushCb, public election::ObIElectionCallback {
public:
  ObIPartitionLogService()
  {}
  virtual ~ObIPartitionLogService()
  {}

public:
  virtual int init(ObILogEngine* log_engine, ObLogReplayEngineWrapper* replay_engine,
      ObIFetchLogEngine* fetch_log_engine, election::ObIElectionMgr* election_mgr,
      storage::ObPartitionService* partition_service, ObILogCallbackEngine* cb_engine,
      common::ObILogAllocator* alloc_mgr, ObLogEventScheduler* event_scheduler, const common::ObAddr& self,
      const common::ObVersion& data_version, const common::ObPartitionKey& parition_key,
      const common::ObReplicaType replica_type, const common::ObReplicaProperty replia_property,
      const common::ObBaseStorageInfo& base_storage_info, const int16_t archive_restore_state,
      const bool need_skip_mlist_check, ObRemoteLogQueryEngine* remote_log_query_engine,
      archive::ObArchiveMgr* archive_mgr, archive::ObArchiveRestoreEngine* archive_restore_engine,
      const enum ObCreatePlsType& create_pls_type) = 0;
  virtual int remove_election() = 0;
  virtual int stop_election() = 0;
  virtual int standby_update_protection_level() = 0;
  virtual int get_standby_leader_protection_level(uint32_t& protection_level) = 0;
  virtual int primary_process_protect_mode_switch() = 0;
  virtual int get_log_id_timestamp(const int64_t base_timestamp, ObLogMeta& log_meta) = 0;
  virtual int submit_aggre_log(ObAggreBuffer* buffer, const int64_t base_timestamp) = 0;
  virtual int submit_log(const char* buff, const int64_t size, const int64_t base_timestamp, ObISubmitLogCb* cb,
      const bool is_trans_log, uint64_t& log_id, int64_t& log_timestamp) = 0;
  virtual int add_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info) = 0;
  virtual int remove_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info) = 0;
  virtual int change_quorum(const common::ObMemberList& curr_member_list, const int64_t curr_quorum,
      const int64_t new_quorum, obrpc::ObMCLogInfo& log_info) = 0;
  virtual int is_member_change_done(const obrpc::ObMCLogInfo& log_info) = 0;
  virtual int get_base_storage_info(common::ObBaseStorageInfo& base_storage_info, uint64_t& sw_last_replay_log_id) = 0;
  virtual common::ObPartitionKey get_partition_key() const = 0;
  virtual int get_saved_base_storage_info(common::ObBaseStorageInfo& base_storage_info) const = 0;
  virtual int get_leader(common::ObAddr& leader) const = 0;
  virtual int get_clog_parent(common::ObAddr& parent, int64_t& cluster_id) const = 0;
  virtual int change_leader(const common::ObAddr& leader, common::ObTsWindows& changing_leader_windows) = 0;
  virtual int change_restore_leader(const common::ObAddr& leader) = 0;
  virtual int check_and_set_restore_progress() = 0;
  virtual int set_restore_fetch_log_finished(ObArchiveFetchLogResult fetch_log_result) = 0;
  virtual int try_update_next_replay_log_ts_in_restore(const int64_t new_ts) = 0;
  virtual int get_role(common::ObRole& role) const = 0;
  virtual int get_role_for_partition_table(common::ObRole& role) const = 0;
  virtual int get_role_unsafe(int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) const = 0;
  virtual int get_role_unlock(int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) const = 0;
  virtual int get_role_and_last_leader_active_time(common::ObRole& role, int64_t& timestamp) const = 0;
  virtual int get_role_and_leader_epoch(common::ObRole& role, int64_t& leader_epoch) = 0;
  virtual int get_role_and_leader_epoch(common::ObRole& role, int64_t& leader_epoch, int64_t& takeover_time) = 0;
  virtual int set_election_leader(const common::ObAddr& leader, const int64_t lease_start) = 0;
  virtual int get_leader_curr_member_list(common::ObMemberList& member_list) const = 0;
  virtual int get_curr_member_list(common::ObMemberList& member_list) const = 0;
  virtual int get_curr_member_list_for_report(common::ObMemberList& member_list) const = 0;
  virtual int try_get_curr_member_list(common::ObMemberList& member_list) const = 0;
  virtual int get_replica_num(int64_t& replica_num) const = 0;
  virtual int try_get_replica_num(int64_t& replica_num) const = 0;
  virtual int receive_log(const ObLogEntry& log_entry, const common::ObAddr& server, const int64_t cluster_id,
      const common::ObProposalID& proposal_id, const ObPushLogMode push_mode, const ReceiveLogType type) = 0;
  virtual int receive_renew_ms_log(const ObLogEntry& log_entry, const ObAddr& server, const int64_t cluster_id,
      const ObProposalID& proposal_id, const ReceiveLogType type) = 0;
  virtual int receive_archive_log(const ObLogEntry& log_entry, const bool is_batch_committed) = 0;
  virtual int ack_log(const common::ObAddr& server, const uint64_t log_id, const common::ObProposalID& proposal_id) = 0;
  virtual int standby_ack_log(
      const ObAddr& server, const int64_t cluster_id, const uint64_t log_id, const ObProposalID& proposal_id) = 0;
  virtual int fake_ack_log(const ObAddr& server, const uint64_t log_id, const ObProposalID& proposal_id) = 0;
  virtual int ack_renew_ms_log(const ObAddr& server, const uint64_t log_id, const int64_t submit_timestamp,
      const ObProposalID& ms_proposal_id) = 0;
  virtual int fake_receive_log(const ObAddr& server, const uint64_t log_id, const ObProposalID& proposal_id) = 0;
  virtual int get_log(const common::ObAddr& server, const uint64_t log_id, const int64_t log_num,
      const ObFetchLogType fetch_type, const common::ObProposalID& proposal_id, const int64_t cluster_id,
      const common::ObReplicaType replica_type, const int64_t network_limit, const uint64_t max_confirmed_log_id) = 0;
  virtual int async_get_log(const common::ObAddr& server, const int64_t cluster_id, const uint64_t start_log_id,
      const uint64_t end_log_id, const ObFetchLogType fetch_type, const common::ObProposalID& proposal_id,
      const int64_t network_limit) = 0;
  virtual int get_max_log_id(
      const common::ObAddr& server, const int64_t cluster_id, const common::ObProposalID& proposal_id) = 0;
  virtual int handle_standby_prepare_req(
      const ObAddr& server, const int64_t cluster_id, const ObProposalID& proposal_id) = 0;
  virtual int handle_standby_prepare_resp(const ObAddr& server, const ObProposalID& proposal_id,
      const uint64_t ms_log_id, const int64_t membership_version, const ObMemberList& member_list) = 0;
  virtual int handle_query_sync_start_id_req(const ObAddr& server, const int64_t cluster_id, const int64_t send_ts) = 0;
  virtual int handle_sync_start_id_resp(
      const ObAddr& server, const int64_t cluster_id, const int64_t original_send_ts, const uint64_t sync_start_id) = 0;
  virtual int receive_max_log_id(const common::ObAddr& server, const uint64_t max_log_id,
      const ObProposalID& proposal_id, const int64_t max_log_ts) = 0;
  virtual int receive_confirmed_info(const common::ObAddr& server, const int64_t src_cluster_id, const uint64_t log_id,
      const ObConfirmedInfo& confirmed_info, const bool batch_committed) = 0;
  virtual int receive_renew_ms_log_confirmed_info(const common::ObAddr& server, const uint64_t log_id,
      const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info) = 0;
  virtual int append_disk_log(const ObLogEntry& log_entry, const ObLogCursor& log_cursor, const int64_t accum_checksum,
      const bool batch_committed) = 0;
  virtual int is_offline(bool& is_offline) = 0;
  virtual int set_offline() = 0;
  virtual int set_online(
      const common::ObBaseStorageInfo& base_storage_info, const common::ObVersion& memstore_version) = 0;
  virtual int on_election_role_change() = 0;
  virtual int set_scan_disk_log_finished() = 0;
  virtual int switch_state() = 0;
  virtual int switch_state(bool& need_retry) = 0;
  virtual int check_mc_and_sliding_window_state() = 0;
  virtual int leader_send_max_log_info() = 0;
  virtual int leader_keepalive(const int64_t keepalive_interval) = 0;
  virtual int sync_log_archive_progress() = 0;
  virtual int get_log_archive_status(ObPGLogArchiveStatus& status) = 0;
  virtual int check_cascading_state() = 0;
  virtual int archive_checkpoint(const int64_t interval) = 0;
  virtual int set_next_index_log_id(const uint64_t log_id, const int64_t accum_checksum) = 0;
  virtual int is_in_sync(bool& is_sync) const = 0;
  virtual int is_log_sync_with_leader(bool& is_sync) const = 0;
  virtual int is_log_sync_with_primary(const int64_t switchover_epoch, bool& is_sync) const = 0;
  virtual int is_need_rebuild(bool& need_rebuild) const = 0;
  virtual bool is_leader_active() const = 0;
  virtual int get_next_replay_log_info(uint64_t& next_replay_log_id, int64_t& next_replay_log_timestamp) = 0;
  virtual int get_follower_log_delay(int64_t& log_delay) = 0;
  virtual int process_keepalive_msg(const common::ObAddr& server, const int64_t cluster_id, const uint64_t next_log_id,
      const int64_t next_log_ts_lb, const uint64_t deliver_cnt) = 0;
  virtual int process_archive_checkpoint(const uint64_t next_log_id, const int64_t next_log_ts) = 0;
  virtual int process_restore_takeover_msg(const int64_t send_ts) = 0;
  virtual int process_leader_max_log_msg(const common::ObAddr& server, const int64_t switchover_epoch,
      const uint64_t leader_max_log_id, const int64_t leader_next_log_ts) = 0;
  virtual int process_sync_log_archive_progress_msg(
      const common::ObAddr& server, const int64_t cluster_id, const ObPGLogArchiveStatus& status) = 0;
  virtual int notify_log_missing(const common::ObAddr& src_server, const uint64_t start_log_id,
      const bool is_in_member_list, const int32_t msg_type) = 0;
  virtual int fetch_register_server(const common::ObAddr& server, const common::ObReplicaType replica_type,
      const int64_t next_replay_log_ts, const bool is_request_leader, const bool is_need_force_register,
      const common::ObRegion& region, const int64_t cluster_id, const common::ObIDC& idc) = 0;
  virtual int fetch_register_server_resp_v2(const common::ObAddr& sender, const bool is_assign_parent_succeed,
      const share::ObCascadMemberList& candidate_list, const int32_t msg_type) = 0;
  virtual int replace_sick_child(
      const common::ObAddr& sender, const int64_t cluster_id, const common::ObAddr& sick_child) = 0;
  virtual int process_reject_msg(const common::ObAddr& server, const int32_t msg_type, const int64_t timestamp) = 0;
  virtual int process_reregister_msg(
      const common::ObAddr& src_server, const share::ObCascadMember& new_leader, const int64_t send_ts) = 0;
  virtual int process_restore_alive_msg(const common::ObAddr& server, const uint64_t start_log_id) = 0;
  virtual int process_restore_alive_req(
      const common::ObAddr& server, const int64_t dst_cluster_id, const int64_t send_ts) = 0;
  virtual int process_restore_alive_resp(const common::ObAddr& server, const int64_t send_ts) = 0;
  virtual int process_restore_log_finish_msg(const common::ObAddr& src_server, const uint64_t log_id) = 0;
  virtual int get_curr_leader_and_memberlist(common::ObAddr& leader, common::ObRole& role,
      common::ObMemberList& curr_member_list, common::ObChildReplicaList& children_list) const = 0;
  virtual int migrate_set_base_storage_info(const common::ObBaseStorageInfo& base_storage_info) = 0;
  virtual int restore_replayed_log(const common::ObBaseStorageInfo& base_storage_info) = 0;
  virtual int get_restore_leader_info(bool& is_restore_leader, int64_t& leader_takeover_ts) = 0;
  virtual uint64_t get_last_replay_log_id() const = 0;
  virtual void get_last_replay_log(uint64_t& log_id, int64_t& ts) = 0;
  virtual int64_t get_last_submit_timestamp() const = 0;
  virtual int64_t get_membership_timestamp() const = 0;
  virtual int try_replay(const bool need_async, bool& is_replayed, bool& is_replay_failed) = 0;
  virtual bool is_svr_in_member_list(const ObAddr& server) const = 0;
  virtual int get_dst_leader_candidate(common::ObMemberList& member_list) const = 0;
  virtual int get_max_data_version(ObVersion& max_data_version) const = 0;
  virtual int set_follower_active() = 0;
  virtual uint64_t get_last_slide_log_id() = 0;
  virtual uint64_t get_start_log_id() = 0;
  virtual uint64_t get_start_log_id_unsafe() = 0;
  virtual int get_log_id_range(uint64_t& start_log_id, int64_t& start_ts, uint64_t& end_log_id, int64_t& end_ts) = 0;
  virtual int get_sw_max_log_id_info(uint64_t& sw_max_log_id, int64_t& sw_max_log_ts) = 0;
  virtual int get_sw_max_log_id(uint64_t& sw_max_log_id) = 0;
  virtual ObClogVirtualStat* alloc_clog_virtual_stat() = 0;
  virtual int revert_clog_virtual_stat(ObClogVirtualStat* virtual_stat) = 0;
  virtual int force_set_as_single_replica() = 0;
  virtual int force_set_replica_num(const int64_t replica_num) = 0;
  virtual int force_set_parent(const common::ObAddr& new_parent) = 0;
  virtual int force_reset_parent() = 0;
  virtual int force_set_server_list(const obrpc::ObServerList& server_list, const int64_t replica_num) = 0;
  virtual int get_next_timestamp(const uint64_t last_log_id, int64_t& res_ts) = 0;
  virtual int get_next_served_log_info_for_leader(uint64_t& next_served_log_id, int64_t& next_served_log_ts) = 0;
  virtual uint64_t get_next_index_log_id() const = 0;
  virtual int get_pls_epoch(int64_t& pls_epoch) const = 0;
  virtual int64_t get_scan_confirmed_log_cnt() const = 0;
  virtual int allow_gc(bool& allowed) = 0;
  virtual int query_max_flushed_ilog_id(uint64_t& ret_max_ilog_id) = 0;
  virtual void set_zone_priority(const uint64_t zone_priority) = 0;
  virtual int set_region(const common::ObRegion& region) = 0;
  virtual int set_idc(const common::ObIDC& idc) = 0;
  virtual enum ObReplicaType get_replica_type() = 0;
  virtual int set_replica_type(const enum ObReplicaType replica_type) = 0;
  virtual int check_is_normal_partition(bool& is_normal_partition) const = 0;
  //==================== batch change member begin ====================
  virtual int pre_change_member(const int64_t quorum, const bool is_add_member, int64_t& mc_timestamp,
      ObMemberList& member_list, ObProposalID& proposal_id) = 0;
  virtual int set_member_changing() = 0;
  virtual int reset_member_changing() = 0;
  virtual int wait_log_confirmed(const int64_t begin_time, const int64_t max_wait_time) = 0;
  virtual int batch_change_member(const common::ObMember& member, const int64_t quorum, const int64_t mc_timestamp,
      const common::ObProposalID& proposal_id, const bool is_add_member, obrpc::ObMCLogInfo& mc_log_info) = 0;
  virtual int get_partition_max_log_id(uint64_t& partition_max_log_id) = 0;
  //==================== batch change member end ====================
  virtual bool need_skip_when_check_start_service() const = 0;
  virtual int broadcast_info() = 0;
  virtual int update_broadcast_info(
      const common::ObAddr& server, const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id) = 0;
  virtual int get_recyclable_log_id(uint64_t& log_id) const = 0;
  virtual int get_candidate_info(ObCandidateInfo& info) = 0;
  virtual int is_valid_member(const common::ObAddr& addr, bool& is_valid) const = 0;

  //=================== one phase commit begin ================

  // Specify log_id to query the trans_id corresponding to the confirmed log.
  // Used to query the transaction state in the one-phase commit optimization of the transaction, and restore the use of
  // the transaction state machine
  //
  // ret code:
  // OB_SUCCESS
  // OB_NOT_MASTER               not leader
  // OB_EAGAIN                   The specified log has not been confirmed, or the ilog_storage is being loaded, or other
  // servers need to be accessed. Retry from the outside OB_ENTRY_NOT_EXIST          log does not exist, corresponding
  // to prepare no
  //
  // other error code:
  // OB_NOT_INIT
  // OB_INVALID_ARGUMENT
  virtual int query_confirmed_log(
      const uint64_t log_id, transaction::ObTransID& trans_id, int64_t& submit_timestamp) = 0;
  virtual int backfill_log(
      const ObLogInfo& log_info, const ObLogCursor& log_cursor, const bool is_leader, ObISubmitLogCb* submit_cb) = 0;
  virtual int backfill_confirmed(const ObLogInfo& log_info, const bool batch_first_participant) = 0;
  virtual int resubmit_log(const ObLogInfo& log_info, ObISubmitLogCb* submit_cb) = 0;
  // When calling get_log_id_timestamp for batch submission, due to the limitation of the length of the sliding window,
  // deadlock may occur. The scenario is as follows: Transactions T1 and T2 submit logs on P1 and P2 respectively
  // Suppose T1 calls get_log_id_timestamp in the order of P1\P2, and T2 calls get_log_id_timestamp in the order of
  // P2\P1 The length of the sliding window is 10000, it may appear T1 allocates log_id = 1 on P1, tries to allocate
  // log_id = 10001 on P2, but keeps returning OB_EAGAIN T2 allocates log_id = 1 on P2, tries to allocate log_id = 10002
  // on P2, but keeps returning OB_EAGAIN
  //
  // Since the batch submission of logs requires that the logs are synchronized successfully before they can be
  // confirmed, the above two transactions will deadlock until a certain partition times out and switches to the master
  //
  // So in this scenario, get_log_id_timestamp returns the special error code OB_TOO_LARGE_LOG_ID,
  // The transaction needs to call this function to call back the nop log to the partition that has been assigned
  // log_id, and then follow the regular two-phase commit
  //
  // Another solution is to require each batch submission to call get_log_id_timestamp in the order of the partition,
  // and ensure that get_log_id_timestamp will succeed in the end through the order It is impossible to analyze which of
  // the two schemes is better from an analytical point of view, you can consider implementing both and then verify
  //
  // Cause of this problem:
  // 1 The length of the sliding window needs to be limited due to memory limitations;
  // 2 One-phase optimization introduces a dependency relationship between different participants;
  virtual int backfill_nop_log(const ObLogMeta& log_meta) = 0;
  //=================== one phase commit end   ================
  virtual bool need_add_event_task() = 0;
  virtual void reset_has_event_task() = 0;
  virtual int check_can_receive_batch_log(const uint64_t log_id) = 0;
  virtual int update_max_flushed_ilog_id(const uint64_t log_id) = 0;
  virtual int get_cursor_with_cache(
      const int64_t scan_thread_index, const uint64_t log_id, ObLogCursorExt& log_cursor) = 0;
  virtual int reset_has_pop_task() = 0;
  virtual int set_member_list(const ObMemberList& member_list, const int64_t replica_num,
      const common::ObAddr& assigned_leader, const int64_t lease_start) = 0;
  virtual int try_freeze_aggre_buffer() = 0;
  virtual bool has_valid_next_replay_log_ts() const = 0;
  virtual int check_if_start_log_task_empty(bool& is_empty) = 0;
  virtual bool has_valid_member_list() const = 0;
  virtual int get_last_archived_log_id(
      const int64_t incarnartion, const int64_t archive_round, uint64_t& last_archived_log_id) = 0;
  virtual int process_check_rebuild_req(
      const common::ObAddr& server, const uint64_t start_log_id, const int64_t cluster_id) = 0;
  virtual void get_max_majority_log(uint64_t& log_id, int64_t& log_ts) const = 0;
  virtual int set_archive_restore_state(const int16_t archive_restore_state) = 0;
  virtual uint64_t get_max_confirmed_log_id() const = 0;
  virtual bool is_archive_restoring() const = 0;
  virtual common::ObAddr get_restore_leader() const = 0;
  virtual int restore_leader_try_confirm_log() = 0;
  virtual bool is_standby_restore_state() const = 0;
  virtual int check_and_try_leader_revoke(const election::ObElection::RevokeType& revoke_type) = 0;
  virtual int renew_ms_log_flush_cb(const storage::ObMsInfoTask& task) = 0;
  virtual int try_update_leader_from_loc_cache() = 0;
};

class ObPartitionLogService : public ObIPartitionLogService {
public:
  ObPartitionLogService();
  virtual ~ObPartitionLogService()
  {
    destroy();
  }

public:
  virtual int init(ObILogEngine* log_engine, ObLogReplayEngineWrapper* replay_engine,
      ObIFetchLogEngine* fetch_log_engine, election::ObIElectionMgr* election_mgr,
      storage::ObPartitionService* partition_service, ObILogCallbackEngine* cb_engine,
      common::ObILogAllocator* alloc_mgr, ObLogEventScheduler* event_scheduler, const common::ObAddr& self_addr,
      const common::ObVersion& freeze_version, const common::ObPartitionKey& parition_key,
      const common::ObReplicaType replica_type, const common::ObReplicaProperty replica_property,
      const common::ObBaseStorageInfo& base_storage_info, const int16_t archive_restore_state,
      const bool need_skip_mlist_check, ObRemoteLogQueryEngine* remote_log_query_engine,
      archive::ObArchiveMgr* archive_mgr, archive::ObArchiveRestoreEngine* archive_restore_engine,
      const enum ObCreatePlsType& create_pls_type) override;

  virtual int get_log_id_timestamp(const int64_t base_timestamp, ObLogMeta& log_meta) override;
  virtual int submit_aggre_log(ObAggreBuffer* buffer, const int64_t base_timestamp) override;
  virtual int submit_log(const char* buff, const int64_t size, const int64_t base_timestamp, ObISubmitLogCb* cb,
      const bool is_trans_log, uint64_t& log_id, int64_t& log_timestamp) override;
  virtual int add_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info) override;
  virtual int remove_member(
      const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info) override;
  virtual int change_quorum(const common::ObMemberList& curr_member_list, const int64_t curr_quorum,
      const int64_t new_quorum, obrpc::ObMCLogInfo& log_info) override;
  virtual int is_member_change_done(const obrpc::ObMCLogInfo& log_info) override;
  virtual int get_base_storage_info(
      common::ObBaseStorageInfo& base_storage_info, uint64_t& sw_last_replay_log_id) override;
  virtual common::ObPartitionKey get_partition_key() const override;
  virtual int get_saved_base_storage_info(common::ObBaseStorageInfo& base_storage_info) const override;
  virtual int get_leader(common::ObAddr& addr) const override;
  virtual int get_clog_parent(common::ObAddr& parent, int64_t& cluster_id) const override;
  virtual int change_leader(const common::ObAddr& leader, common::ObTsWindows& changing_leader_windows) override;
  virtual int change_restore_leader(const common::ObAddr& leader) override;
  virtual int check_and_set_restore_progress() override;
  virtual int set_restore_fetch_log_finished(ObArchiveFetchLogResult fetch_log_result) override;
  virtual int process_restore_takeover_msg(const int64_t send_ts) override;
  virtual int try_update_next_replay_log_ts_in_restore(const int64_t new_ts) override;
  virtual int get_role(common::ObRole& role) const override;
  virtual int get_role_for_partition_table(common::ObRole& role) const override;
  virtual int get_role_unsafe(int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) const override;
  virtual int get_role_unlock(int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) const override;
  virtual int get_role_for_partition_table_unlock(
      int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) const;
  virtual int get_role_and_last_leader_active_time(common::ObRole& role, int64_t& timestamp) const override;
  virtual int get_role_and_leader_epoch(common::ObRole& role, int64_t& leader_epoch) override;
  virtual int get_role_and_leader_epoch(common::ObRole& role, int64_t& leader_epoch, int64_t& takeover_time) override;
  virtual int set_election_leader(const common::ObAddr& leader, const int64_t lease_start) override;
  virtual int get_leader_curr_member_list(common::ObMemberList& member_list) const override;
  virtual int get_curr_member_list(common::ObMemberList& member_list) const override;
  virtual int get_curr_member_list_for_report(common::ObMemberList& member_list) const override;
  virtual int try_get_curr_member_list(common::ObMemberList& member_list) const override;
  virtual int get_replica_num(int64_t& replica_num) const override;
  virtual int try_get_replica_num(int64_t& replica_num) const override;
  virtual int receive_log(const ObLogEntry& log_entry, const common::ObAddr& server, const int64_t cluster_id,
      const common::ObProposalID& proposal_id, const ObPushLogMode push_mode, const ReceiveLogType type) override;
  virtual int receive_renew_ms_log(const ObLogEntry& log_entry, const ObAddr& server, const int64_t cluster_id,
      const ObProposalID& proposal_id, const ReceiveLogType type) override;
  virtual int receive_archive_log(const ObLogEntry& log_entry, const bool is_batch_committed) override;
  virtual int ack_log(
      const common::ObAddr& server, const uint64_t log_id, const common::ObProposalID& proposal_id) override;
  virtual int standby_ack_log(
      const ObAddr& server, const int64_t cluster_id, const uint64_t log_id, const ObProposalID& proposal_id) override;
  virtual int fake_ack_log(const ObAddr& server, const uint64_t log_id, const ObProposalID& proposal_id) override;
  virtual int ack_renew_ms_log(const ObAddr& server, const uint64_t log_id, const int64_t submit_timestamp,
      const ObProposalID& ms_proposal_id) override;
  virtual int fake_receive_log(const ObAddr& server, const uint64_t log_id, const ObProposalID& proposal_id) override;
  virtual int get_log(const common::ObAddr& server, const uint64_t log_id, const int64_t log_num,
      const ObFetchLogType fetch_type, const common::ObProposalID& proposal_id, const int64_t cluster_id,
      const common::ObReplicaType replica_type, const int64_t network_limit,
      const uint64_t max_confirmed_log_id) override;
  virtual int async_get_log(const common::ObAddr& server, const int64_t cluster_id, const uint64_t start_log_id,
      const uint64_t end_log_id, const ObFetchLogType fetch_type, const common::ObProposalID& proposal_id,
      const int64_t network_limit) override;
  virtual int get_max_log_id(
      const common::ObAddr& server, const int64_t cluster_id, const common::ObProposalID& proposal_id) override;
  virtual int handle_standby_prepare_req(
      const ObAddr& server, const int64_t cluster_id, const ObProposalID& proposal_id) override;
  virtual int handle_standby_prepare_resp(const ObAddr& server, const ObProposalID& proposal_id,
      const uint64_t ms_log_id, const int64_t membership_version, const ObMemberList& member_list) override;
  virtual int handle_query_sync_start_id_req(
      const ObAddr& server, const int64_t cluster_id, const int64_t send_ts) override;
  virtual int handle_sync_start_id_resp(const ObAddr& server, const int64_t cluster_id, const int64_t original_send_ts,
      const uint64_t sync_start_id) override;
  virtual int receive_max_log_id(const common::ObAddr& server, const uint64_t max_log_id,
      const ObProposalID& proposal_id, const int64_t max_log_ts) override;
  virtual int receive_confirmed_info(const common::ObAddr& server, const int64_t src_cluster_id, const uint64_t log_id,
      const ObConfirmedInfo& confirmed_info, const bool batch_committed) override;
  virtual int receive_renew_ms_log_confirmed_info(const common::ObAddr& server, const uint64_t log_id,
      const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info) override;
  virtual int append_disk_log(const ObLogEntry& log, const ObLogCursor& log_cursor, const int64_t accum_checksum,
      const bool batch_committed) override;
  virtual int is_offline(bool& is_offline) override;
  virtual int stop_election() override;
  virtual int set_offline() override;
  virtual int set_online(
      const common::ObBaseStorageInfo& base_storage_info, const common::ObVersion& memstore_version) override;
  virtual int on_election_role_change() override;
  virtual int set_scan_disk_log_finished() override;
  virtual int switch_state() override;
  virtual int switch_state(bool& need_retry) override;
  virtual int check_mc_and_sliding_window_state() override;
  virtual int leader_send_max_log_info() override;
  virtual int leader_keepalive(const int64_t keepalive_interval) override;
  virtual int sync_log_archive_progress() override;
  virtual int get_log_archive_status(ObPGLogArchiveStatus& status) override;
  virtual int check_cascading_state() override;
  virtual int archive_checkpoint(const int64_t interval) override;
  virtual int set_next_index_log_id(const uint64_t log_id, const int64_t accum_checksum) override;
  virtual int is_in_sync(bool& is_sync) const override;
  virtual int is_log_sync_with_leader(bool& is_sync) const override;
  virtual int is_log_sync_with_primary(const int64_t switchover_epoch, bool& is_sync) const override;
  virtual int is_need_rebuild(bool& need_rebuild) const override;
  virtual bool is_leader_active() const override;
  virtual int check_self_is_election_leader_() const;
  virtual int get_next_replay_log_info(uint64_t& next_replay_log_id, int64_t& next_replay_log_timestamp) override;
  virtual int get_follower_log_delay(int64_t& log_delay) override;
  virtual int process_keepalive_msg(const common::ObAddr& server, const int64_t cluster_id, const uint64_t next_log_id,
      const int64_t next_log_ts_lb, const uint64_t deliver_cnt) override;
  virtual int process_archive_checkpoint(const uint64_t next_log_id, const int64_t next_log_ts) override;
  virtual int get_restore_leader_info(bool& is_restore_leader, int64_t& leader_takeover_ts) override;
  virtual int process_leader_max_log_msg(const common::ObAddr& server, const int64_t switchover_epoch,
      const uint64_t leader_max_log_id, const int64_t leader_next_log_ts) override;
  virtual int process_sync_log_archive_progress_msg(
      const common::ObAddr& server, const int64_t cluster_id, const ObPGLogArchiveStatus& status) override;
  virtual int notify_log_missing(const common::ObAddr& src_server, const uint64_t start_log_id,
      const bool is_in_member_list, const int32_t msg_type) override;
  virtual int fetch_register_server(const common::ObAddr& server, const common::ObReplicaType replica_type,
      const int64_t next_replay_log_ts, const bool is_request_leader, const bool is_need_force_register,
      const common::ObRegion& region, const int64_t cluster_id, const common::ObIDC& idc) override;
  virtual int fetch_register_server_resp_v2(const common::ObAddr& sender, const bool is_assign_parent_succeed,
      const share::ObCascadMemberList& candidate_list, const int32_t msg_type) override;
  virtual int replace_sick_child(
      const common::ObAddr& sender, const int64_t cluster_id, const common::ObAddr& sick_child) override;
  virtual int get_curr_leader_and_memberlist(common::ObAddr& leader, common::ObRole& role,
      common::ObMemberList& curr_member_list, common::ObChildReplicaList& children_list) const override;
  virtual int migrate_set_base_storage_info(const common::ObBaseStorageInfo& base_storage_info) override;
  virtual int restore_replayed_log(const common::ObBaseStorageInfo& base_storage_info) override;
  inline virtual uint64_t get_last_replay_log_id() const override
  {
    return saved_base_storage_info_.get_last_replay_log_id();
  }
  virtual void get_last_replay_log(uint64_t& log_id, int64_t& ts) override;
  virtual int64_t get_last_submit_timestamp() const override;
  virtual int remove_election() override;
  virtual int standby_update_protection_level() override;
  virtual int get_standby_leader_protection_level(uint32_t& protection_level) override;
  virtual int primary_process_protect_mode_switch() override;
  virtual int64_t get_membership_timestamp() const override;
  virtual int try_replay(const bool need_async, bool& is_replayed, bool& is_replay_failed) override;
  virtual bool is_svr_in_member_list(const ObAddr& server) const override;
  virtual int get_dst_leader_candidate(common::ObMemberList& member_list) const override;
  virtual int get_max_data_version(ObVersion& max_data_version) const override;
  virtual int set_follower_active() override;
  virtual uint64_t get_last_slide_log_id() override;
  virtual uint64_t get_start_log_id() override;
  virtual uint64_t get_start_log_id_unsafe() override;
  virtual int get_log_id_range(
      uint64_t& start_log_id, int64_t& start_ts, uint64_t& end_log_id, int64_t& end_ts) override;
  virtual int get_sw_max_log_id_info(uint64_t& sw_max_log_id, int64_t& sw_max_log_ts) override;
  int get_sw_max_log_id(uint64_t& sw_max_log_id) override;
  virtual ObClogVirtualStat* alloc_clog_virtual_stat() override;
  virtual int revert_clog_virtual_stat(ObClogVirtualStat* virtual_stat) override;
  virtual int force_set_as_single_replica() override;
  virtual int force_set_replica_num(const int64_t replica_num) override;
  virtual int force_set_parent(const common::ObAddr& new_parent) override;
  virtual int force_reset_parent() override;
  virtual int force_set_server_list(const obrpc::ObServerList& server_list, const int64_t replica_num) override;
  virtual int renew_ms_log_flush_cb(const storage::ObMsInfoTask& task) override;
  virtual int try_update_leader_from_loc_cache() override;
  virtual int flush_cb(const ObLogFlushCbArg& arg) override;
  virtual int on_get_election_priority(election::ObElectionPriority& priority) override;
  virtual int on_change_leader_retry(const common::ObAddr& server, ObTsWindows& changing_leader_windows) override;
  virtual int get_next_timestamp(const uint64_t last_log_id, int64_t& res_ts) override;
  virtual int get_next_served_log_info_for_leader(uint64_t& next_served_log_id, int64_t& next_served_log_ts) override;
  virtual uint64_t get_next_index_log_id() const override
  {
    return sw_.get_next_index_log_id();
  }

  virtual int64_t get_scan_confirmed_log_cnt() const override
  {
    return ATOMIC_LOAD(&scan_confirmed_log_cnt_);
  }
  virtual int get_pls_epoch(int64_t& pls_epoch) const override;
  virtual int allow_gc(bool& allowed) override;
  virtual int query_max_flushed_ilog_id(uint64_t& ret_max_ilog_id) override;
  virtual void set_zone_priority(const uint64_t zone_priority) override;
  virtual int set_region(const common::ObRegion& region) override;
  virtual int set_idc(const common::ObIDC& idc) override;
  virtual const common::ObRegion& get_region() const;
  virtual const common::ObIDC& get_idc() const;
  virtual enum ObReplicaType get_replica_type() override;
  virtual int set_replica_type(const enum ObReplicaType replica_type) override;
  virtual int check_is_normal_partition(bool& is_normal_partition) const override;

public:
  virtual bool need_skip_when_check_start_service() const override;
  virtual int broadcast_info() override;
  virtual int update_broadcast_info(const common::ObAddr& server, const common::ObReplicaType& replica_type,
      const uint64_t max_confirmed_log_id) override;
  virtual int get_recyclable_log_id(uint64_t& log_id) const override;
  virtual int is_valid_member(const common::ObAddr& addr, bool& is_valid) const override;
  virtual bool has_valid_next_replay_log_ts() const override;
  virtual bool has_valid_member_list() const override;
  virtual int process_check_rebuild_req(
      const common::ObAddr& server, const uint64_t start_log_id, const int64_t cluster_id) override;
  virtual void get_max_majority_log(uint64_t& log_id, int64_t& log_ts) const override;
  virtual uint64_t get_max_confirmed_log_id() const override;

public:
  //==================== batch change member begin ====================
  virtual int pre_change_member(const int64_t quorum, const bool is_add_member, int64_t& mc_timestamp,
      ObMemberList& member_list, ObProposalID& proposal_id) override;
  virtual int set_member_changing() override;
  virtual int reset_member_changing() override;
  virtual int wait_log_confirmed(const int64_t begin_time, const int64_t max_wait_time) override;
  virtual int batch_change_member(const common::ObMember& member, const int64_t quorum, const int64_t mc_timestamp,
      const common::ObProposalID& proposal_id, const bool is_add_member, obrpc::ObMCLogInfo& mc_log_info) override;
  virtual int get_partition_max_log_id(uint64_t& partition_max_log_id) override;
  //==================== batch change member end ====================

  //=================== one phase commit begin ================
  virtual int get_candidate_info(ObCandidateInfo& info) override;

  virtual int query_confirmed_log(
      const uint64_t log_id, transaction::ObTransID& trans_id, int64_t& submit_timestamp) override;
  virtual int backfill_log(const ObLogInfo& log_info, const ObLogCursor& log_cursor, const bool is_leader,
      ObISubmitLogCb* submit_cb) override;
  virtual int backfill_confirmed(const ObLogInfo& log_info, const bool batch_first_participant) override;
  virtual int resubmit_log(const ObLogInfo& log_info, ObISubmitLogCb* submit_cb) override;
  virtual int backfill_nop_log(const ObLogMeta& log_meta) override;
  //=================== one phase commit end ================
  virtual bool need_add_event_task() override;
  virtual void reset_has_event_task() override;
  virtual int check_can_receive_batch_log(const uint64_t log_id) override;
  virtual int update_max_flushed_ilog_id(const uint64_t log_id) override;
  virtual int get_cursor_with_cache(
      const int64_t scan_thread_index, const uint64_t log_id, ObLogCursorExt& log_cursor) override;
  virtual int reset_has_pop_task() override;
  virtual int set_member_list(const ObMemberList& member_list, const int64_t replica_num,
      const common::ObAddr& assigned_leader, const int64_t lease_start) override;
  int get_last_archived_log_id(
      const int64_t incarnartion, const int64_t archive_round, uint64_t& last_archived_log_id) override;
  virtual int try_freeze_aggre_buffer() override;
  int set_archive_restore_state(const int16_t archive_restore_state) override;
  /*return values:
   * OB_SUCCESS
   * OB_EAGAIN: start log has slide out, need retry
   * other code
   * */

  virtual int check_if_start_log_task_empty(bool& is_empty) override;
  bool is_archive_restoring() const override;
  common::ObAddr get_restore_leader() const override;
  virtual int restore_leader_try_confirm_log() override;
  virtual bool is_standby_restore_state() const override;
  virtual int check_and_try_leader_revoke(const election::ObElection::RevokeType& revoke_type) override;

private:
  enum { DEFAULT_TIMEOUT = 10 * 1000 * 1000 };
  virtual void destroy();
  int majority_cb_(const uint64_t log_id);
  int standby_majority_cb_(const common::ObAddr& server, const int64_t cluster_id,
      const common::ObProposalID& proposal_id, const uint64_t log_id);
  int renew_ms_log_majority_cb_(
      const uint64_t log_id, const int64_t submit_timestamp, const common::ObProposalID& proposal_id);
  int get_log_follower_active_(const common::ObAddr& server, const int64_t cluster_id, const uint64_t start_log_id,
      const uint64_t end_log_id, const ObFetchLogType& fetch_type, const uint32_t log_attr,
      const bool need_send_confirm_info, const int64_t network_limit);
  int get_log_leader_reconfirm_(const common::ObAddr& server, const uint64_t start_log_id, const uint64_t end_log_id,
      const uint32_t log_attr, const bool need_send_confirm_info, const common::ObProposalID& proposal_id,
      const int64_t network_limit);
  int get_log_with_cursor_(const common::ObAddr& server, const int64_t cluster_id, const ObFetchLogType& fetch_type,
      const common::ObProposalID& proposal_id, const bool need_send_confirm_info, const bool log_confirmed,
      const ObLogCursor& log_cursor, const int64_t accum_checksum, const bool batch_committed,
      const int64_t network_limit);
  int send_log_(const common::ObAddr& server, const int64_t cluster_id, const ObLogEntry& log_entry,
      const common::ObProposalID& proposal_id, const int64_t network_limit);
  int send_null_log_(const common::ObAddr& server, const int64_t cluster_id, const uint64_t log_id,
      const common::ObProposalID& proposal_id, const int64_t network_limit);
  int send_confirm_info_(const common::ObAddr& server, const int64_t cluster_id, const ObLogEntry& log_entry,
      const int64_t accum_checksum, const bool batch_committed);
  bool check_election_leader_(int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) const;
  bool is_freeze_log_(const char* buff, const int64_t size) const;
  bool try_update_freeze_version_(const ObLogEntry& log_entry);
  bool check_remote_mc_ts_(const ObMemberList& member_list, const ObAddr& addr_, const int64_t timestamp);
  bool check_remote_mc_ts_sync_(const ObMemberList& member_list, const ObAddr& addr_, const int64_t timestamp);
  bool internal_check_remote_mc_ts_(
      const ObMemberList& member_list, const ObAddr& addr_, const int64_t timestamp, const uint64_t follower_max_gap);
  int wait_log_confirmed_();
  int truncate_first_stage_(const common::ObBaseStorageInfo& base_storage_info);
  int truncate_second_stage_(const common::ObBaseStorageInfo& base_storage_info);
  int change_member_(
      const common::ObMember& member, const bool is_add_member, const int64_t quorum, obrpc::ObMCLogInfo& log_info);
  int fill_base_storage_info(common::ObBaseStorageInfo& base_storage_info, uint64_t& sw_last_replay_log_id);
  int64_t get_zone_priority() const;
  int response_sliding_window_info_(const common::ObAddr& server, const bool is_leader);
  int process_replica_type_change_();
  int process_reject_msg(const common::ObAddr& server, const int32_t msg_type, const int64_t timestamp) override;
  int process_reregister_msg(
      const common::ObAddr& src_server, const share::ObCascadMember& new_leader, const int64_t send_ts) override;
  int process_restore_alive_msg(const common::ObAddr& server, const uint64_t start_log_id) override;
  int process_restore_alive_req(
      const common::ObAddr& server, const int64_t dst_cluster_id, const int64_t send_ts) override;
  int process_restore_alive_resp(const common::ObAddr& server, const int64_t send_ts) override;
  int process_restore_log_finish_msg(const common::ObAddr& src_server, const uint64_t log_id) override;
  int resp_sw_info_when_get_log_(const common::ObAddr& server, const int64_t cluster_id);
  int internal_fetch_log_(const uint64_t start_log_id, const uint64_t end_log_id, const ObFetchLogType& fetch_type,
      const ObProposalID& proposal_id, const common::ObAddr& server, const int64_t cluster_id, const uint32_t log_attr,
      const bool need_send_confirm_info, const int64_t network_limit);
  int fetch_log_from_sw_(const uint64_t log_id, const ObFetchLogType& fetch_type, const ObProposalID& proposal_id,
      const common::ObAddr& server, const int64_t cluster_id, const uint32_t log_attr,
      const bool need_send_confirm_info, const int64_t network_limit);
  int fetch_log_from_ilog_storage_(const uint64_t log_id, const ObFetchLogType& fetch_type,
      const ObProposalID& proposal_id, const common::ObAddr& server, const int64_t cluster_id,
      const bool need_send_confirm_info, const int64_t network_limit, ObGetCursorResult& result,
      uint64_t& cursor_start_log_id);
  int send_keepalive_msg_to_children_(
      const uint64_t next_log_id, const int64_t next_log_ts_lb, const uint64_t deliver_cnt) const;
  int send_keepalive_msg_to_mlist_(
      const uint64_t next_log_id, const int64_t next_log_ts_lb, const uint64_t deliver_cnt) const;
  int send_archive_progress_msg_to_children_(const ObPGLogArchiveStatus& status) const;
  int send_max_log_msg_to_children_(
      const int64_t switchover_epoch, const uint64_t leader_max_log_id, const int64_t leader_next_log_ts) const;
  int send_max_log_msg_to_mlist_(
      const int64_t switchover_epoch, const uint64_t leader_max_log_id, const int64_t leader_next_log_ts) const;
  bool is_paxos_offline_replica_() const;
  bool is_no_update_next_replay_log_id_info_too_long_() const;
  int check_state_();
  int get_storage_last_replay_log_id_(uint64_t& last_replay_log_id) const;
  int get_confirmed_log_from_sw_(const uint64_t log_id, ObReadBuf& buf, ObLogEntry& log_entry);
  int get_log_from_ilog_storage_(const uint64_t log_id, const common::ObMemberList& member_list,
      transaction::ObTransID& trans_id, int64_t& submit_timestamp);
  int add_role_change_task_();
  bool is_self_lag_behind_(const int64_t next_replay_log_ts);
  int leader_update_next_replay_log_ts_(const int64_t new_ts);
  int leader_update_next_replay_log_ts_under_lock();
  bool is_in_partition_service_() const;
  bool cluster_version_before_2200_() const
  {
    return GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2200;
  }
  bool cluster_version_before_2250_() const
  {
    return GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2250;
  }
  int enable_replay_();
  int disable_replay_();
  bool check_if_need_rebuild_(const uint64_t start_log_id, const ObFetchLogType fetch_type) const;
  int get_next_replay_info_for_data_replica_(uint64_t& next_replay_log_id, int64_t& next_replay_log_ts);
  int block_partition_split_();
  int unblock_partition_split_();
  int follower_update_log_archive_progress_(const ObPGLogArchiveStatus& status);
  int try_send_sync_start_id_request_();
  int try_exec_standby_restore_(const share::ObCascadMember& src_member);
  int sync_local_archive_progress_(bool& need_sync);
  int set_restore_flag_after_restore_log_();
  int alloc_cursor_array_cache_(const common::ObPartitionKey& partition_key);
  void free_cursor_array_cache_();
  int alloc_broadcast_info_mgr_(const common::ObPartitionKey& partition_key);
  void free_broadcast_info_mgr_();
  int check_majority_replica_clog_disk_full_(bool& majority_is_clog_disk_full);
  bool is_primary_need_sync_to_standby_() const;
  bool is_tenant_out_of_memory_() const;
  int get_role_and_leader_epoch_unlock_(common::ObRole& role, int64_t& leader_epoch, int64_t& takeover_time);

private:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  typedef RWLock::WLockGuardWithTimeout WLockGuardWithTimeout;
  typedef RWLock::WLockGuardWithRetry WLockGuardWithRetry;
  typedef common::ObSEArray<common::ObAddr, 16> ServerArray;
  static const int64_t REBUILD_REPLICA_INTERVAL = 2l * 60l * 1000l * 1000l;
  static const int64_t STANDBY_CHECK_RESTORE_STATE_INTERVAL = 15l * 1000l * 1000l;
  static const int64_t STANDBY_CHECK_MS_INTERVAL = 60l * 1000l * 1000l;
  static const int64_t WRLOCK_TIMEOUT_US = 100 * 1000;  // 100ms

  bool is_inited_;
  mutable RWLock lock_;
  ObLogSlidingWindow sw_;
  ObLogMembershipTaskMgr ms_task_mgr_;
  ObLogStateMgr state_mgr_;
  ObLogReconfirm reconfirm_;
  ObILogEngine* log_engine_;
  ObLogReplayEngineWrapper* replay_engine_;
  election::ObIElection* election_;
  election::ObIElectionMgr* election_mgr_;
  storage::ObPartitionService* partition_service_;
  ObPartitionKey partition_key_;
  ObLogMembershipMgr mm_;
  ObLogCascadingMgr cascading_mgr_;
  ObLogRestoreMgr restore_mgr_;
  ObLogBroadcastInfoMgr* broadcast_info_mgr_;
  ObIFetchLogEngine* fetch_log_engine_;
  common::ObILogAllocator* alloc_mgr_;
  ObLogEventScheduler* event_scheduler_;
  common::ObAddr self_;
  ObLogChecksum checksum_;
  common::ObBaseStorageInfo saved_base_storage_info_;
  uint64_t zone_priority_;
  bool is_candidate_;
  int64_t last_rebuild_time_;
  int64_t last_check_restore_state_time_;
  int64_t last_check_standby_ms_time_;
  mutable int64_t ack_log_time_;
  mutable int64_t recv_child_next_ilog_ts_time_;
  mutable int64_t submit_log_mc_time_;
  mutable int64_t in_sync_check_time_;
  mutable int64_t update_next_replay_log_time_;
  bool need_rebuild_;
  mutable int64_t send_reject_msg_warn_time_;
  mutable int64_t leader_max_log_interval_;
  mutable int64_t cannot_slide_sw_interval_;
  ObRemoteLogQueryEngine* remote_log_query_engine_;

  // log archive related
  archive::ObArchiveMgr* archive_mgr_;
  SpinRWLock log_archive_status_rwlock_;  // protect log_archive_status
  ObPGLogArchiveStatus log_archive_status_;

  bool is_in_leader_member_list_;
  int64_t last_leader_keepalive_time_;
  int64_t last_check_state_time_;

  mutable common::ObSpinLock event_task_lock_;
  // whether there is submitted ObLogEventScheduler
  bool has_event_task_;
  uint64_t max_flushed_ilog_id_;
  int64_t scan_confirmed_log_cnt_;

  // The flag is used to determine whether the election_ object has been removed from the election_mgr
  bool election_has_removed_;

  ObCursorArrayCache* cursor_array_cache_;

  DISALLOW_COPY_AND_ASSIGN(ObPartitionLogService);
};
}  // namespace clog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_PARTITION_LOG_SERVICE_
