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

#ifndef OCEANBASE_MOCK_OB_PARTITION_LOG_SERVICE_H_
#define OCEANBASE_MOCK_OB_PARTITION_LOG_SERVICE_H_

#include "clog/ob_partition_log_service.h"
#include "clog/ob_log_define.h"
#include "election/ob_election_cb.h"
#include "storage/ob_base_storage_info.h"
#include "storage/ob_storage_log_type.h"

namespace oceanbase {
namespace common {
class ObILogAllocator;
}
namespace clog {
class MockPartitionLogService : public ObIPartitionLogService {
public:
  virtual int init(ObILogEngine* log_engine, ObLogReplayEngineWrapper* replay_engine,
      ObIFetchLogEngine* fetch_log_engine, election::ObIElectionMgr* election_mgr,
      storage::ObPartitionService* partition_mgr, ObILogCallbackEngine* cb_engine, common::ObILogAllocator* alloc_mgr,
      ObLogEventScheduler* event_scheduler, const common::ObAddr& self_addr, const common::ObVersion& version,
      const common::ObPartitionKey& parition_key, const common::ObReplicaType replica_type,
      const common::ObReplicaProperty replica_property, const common::ObBaseStorageInfo& base_storage_info,
      const int16_t archive_restore_state, const bool can_repeat_create_standby,
      ObRemoteLogQueryEngine* remote_log_query_engine, archive::ObArchiveMgr* archive_mgr,
      archive::ObArchiveRestoreEngine* archive_restore_engine, const enum ObCreatePlsType& create_pls_type)
  {
    UNUSED(log_engine);
    UNUSED(replay_engine);
    UNUSED(fetch_log_engine);
    UNUSED(election_mgr);
    UNUSED(partition_mgr);
    UNUSED(cb_engine);
    UNUSED(alloc_mgr);
    UNUSED(event_scheduler);
    UNUSED(self_addr);
    UNUSED(version);
    UNUSED(parition_key);
    UNUSED(replica_type);
    UNUSED(replica_property);
    UNUSED(base_storage_info);
    UNUSED(archive_restore_state);
    UNUSED(can_repeat_create_standby);
    UNUSED(remote_log_query_engine);
    UNUSED(archive_mgr);
    UNUSED(archive_restore_engine);
    UNUSED(create_pls_type);
    return common::OB_SUCCESS;
  }

  virtual int force_set_server_list(const obrpc::ObServerList& server_list, const int64_t replica_num)
  {
    UNUSED(server_list);
    UNUSED(replica_num);
    return common::OB_SUCCESS;
  }

  virtual int get_leader_curr_member_list(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
  {
    UNUSED(pkey);
    UNUSED(member_list);
    return OB_SUCCESS;
  }
  virtual int get_base_storage_info(common::ObBaseStorageInfo& base_storage_info, uint64_t& sw_last_replay_log_id)
  {
    UNUSED(base_storage_info);
    UNUSED(sw_last_replay_log_id);
    return OB_SUCCESS;
  }
  virtual int get_role_unsafe(int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) const
  {
    UNUSED(leader_epoch);
    UNUSED(changing_leader_windows);
    return OB_SUCCESS;
  }
  // virtual int get_curr_member_list(common::ObMemberList &member_list) const;
  virtual int get_curr_member_list(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
  {
    UNUSED(pkey);
    UNUSED(member_list);
    return OB_SUCCESS;
  }
  virtual int try_get_replica_num(int64_t& replica_num) const
  {
    UNUSED(replica_num);
    return OB_SUCCESS;
  }
  virtual int receive_confirmed_info(const common::ObAddr& server, const int64_t src_cluster_id, const uint64_t log_id,
      const ObConfirmedInfo& confirmed_info, const bool batch_committed)
  {
    UNUSED(server);
    UNUSED(src_cluster_id);
    UNUSED(log_id);
    UNUSED(confirmed_info);
    UNUSED(batch_committed);
    return OB_SUCCESS;
  }
  virtual int submit_aggre_log(ObAggreBuffer* buffer, const int64_t base_timestamp)
  {
    UNUSED(buffer);
    UNUSED(base_timestamp);
    return OB_SUCCESS;
  }
  virtual int append_disk_log(
      const ObLogEntry& log, const ObLogCursor& log_cursor, const int64_t accum_checksum, const bool batch_committed)
  {
    UNUSED(log);
    UNUSED(log_cursor);
    UNUSED(accum_checksum);
    UNUSED(batch_committed);
    return OB_SUCCESS;
  }
  virtual int set_online(const common::ObBaseStorageInfo& base_storage_info, const common::ObVersion& memstore_version)
  {
    UNUSED(base_storage_info);
    UNUSED(memstore_version);
    return OB_SUCCESS;
  }
  virtual int leader_keepalive(const int64_t keepalive_interval)
  {
    UNUSED(keepalive_interval);
    return OB_SUCCESS;
  }
  virtual int sync_log_archive_progress()
  {
    return OB_SUCCESS;
  }
  virtual int process_archive_checkpoint(const uint64_t next_log_id, const int64_t next_log_ts)
  {
    UNUSED(next_log_id);
    UNUSED(next_log_ts);
    return OB_SUCCESS;
  }
  virtual int get_restore_leader_info(bool& is_restore_leader, int64_t& leader_takeover_ts)
  {
    UNUSED(is_restore_leader);
    UNUSED(leader_takeover_ts);
    return OB_SUCCESS;
  }
  virtual int receive_archive_log(const ObLogEntry& log_entry, const bool is_batch_committed)
  {
    UNUSED(log_entry);
    UNUSED(is_batch_committed);
    return OB_SUCCESS;
  }
  int get_last_archived_log_id(
      const int64_t incarnation, const int64_t log_archive_round, uint64_t& last_archive_log_id)
  {
    UNUSED(incarnation);
    UNUSED(log_archive_round);
    UNUSED(last_archive_log_id);
    return OB_SUCCESS;
  }

  virtual int archive_checkpoint(const int64_t interval)
  {
    UNUSED(interval);
    return OB_SUCCESS;
  }
  virtual int check_cascading_state()
  {
    return OB_SUCCESS;
  }
  virtual int fetch_register_server(const common::ObAddr& server, const common::ObReplicaType replica_type,
      const int64_t next_replay_log_ts, const bool is_request_leader, const bool is_need_force_register,
      const common::ObRegion& region, const int64_t cluster_id, const common::ObIDC& idc)
  {
    UNUSED(server);
    UNUSED(replica_type);
    UNUSED(next_replay_log_ts);
    UNUSED(is_request_leader);
    UNUSED(is_need_force_register);
    UNUSED(region);
    UNUSED(cluster_id);
    UNUSED(idc);
    return OB_SUCCESS;
  }
  virtual int fetch_register_server_resp_v2(const common::ObAddr& sender, const bool is_assign_parent_succeed,
      const share::ObCascadMemberList& candidate_list, const int32_t msg_type)
  {
    UNUSED(sender);
    UNUSED(is_assign_parent_succeed);
    UNUSED(candidate_list);
    UNUSED(msg_type);
    return OB_SUCCESS;
  }
  virtual int replace_sick_child(
      const common::ObAddr& sender, const int64_t cluster_id, const common::ObAddr& sick_child)
  {
    UNUSED(sender);
    UNUSED(cluster_id);
    UNUSED(sick_child);
    return OB_SUCCESS;
  }
  int process_reject_msg(const common::ObAddr& server, const int32_t msg_type, const int64_t timestamp)
  {
    UNUSED(server);
    UNUSED(msg_type);
    UNUSED(timestamp);
    return OB_SUCCESS;
  }
  virtual int process_restore_alive_msg(const common::ObAddr& server, const uint64_t start_log_id)
  {
    UNUSED(server);
    UNUSED(start_log_id);
    return OB_SUCCESS;
  }
  virtual int process_restore_alive_req(
      const common::ObAddr& server, const int64_t dst_cluster_id, const int64_t send_ts)
  {
    UNUSED(server);
    UNUSED(dst_cluster_id);
    UNUSED(send_ts);
    return OB_SUCCESS;
  }
  virtual int process_restore_alive_resp(const common::ObAddr& server, const int64_t send_ts)
  {
    UNUSED(server);
    UNUSED(send_ts);
    return OB_SUCCESS;
  }
  virtual int process_restore_log_finish_msg(const common::ObAddr& src_server, const uint64_t log_id)
  {
    UNUSED(src_server);
    UNUSED(log_id);
    return OB_SUCCESS;
  }
  virtual int process_reregister_msg(
      const common::ObAddr& src_server, const share::ObCascadMember& new_leader, const int64_t send_ts)
  {
    UNUSED(src_server);
    UNUSED(new_leader);
    UNUSED(send_ts);
    return OB_SUCCESS;
  }
  virtual int get_log_archive_status(ObPGLogArchiveStatus& status)
  {
    UNUSED(status);
    return OB_SUCCESS;
  }
  virtual int process_sync_log_archive_progress_msg(
      const common::ObAddr& server, const int64_t cluster_id, const ObPGLogArchiveStatus& status)
  {
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(status);
    return OB_SUCCESS;
  }
  virtual int get_role_unlock(int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows) const
  {
    UNUSED(leader_epoch);
    UNUSED(changing_leader_windows);
    return OB_SUCCESS;
  }
  virtual int try_get_curr_member_list(common::ObMemberList& member_list) const
  {
    UNUSED(member_list);
    return OB_SUCCESS;
  }
  virtual bool is_svr_in_member_list(const ObAddr& server) const
  {
    UNUSED(server);
    return OB_SUCCESS;
  }
  virtual uint64_t get_start_log_id()
  {
    return OB_SUCCESS;
  }
  virtual uint64_t get_start_log_id_unsafe()
  {
    return OB_SUCCESS;
  }
  virtual int get_sw_max_log_id_info(uint64_t& sw_max_log_id, int64_t& sw_max_log_ts)
  {
    UNUSED(sw_max_log_id);
    UNUSED(sw_max_log_ts);
    return OB_SUCCESS;
  }
  virtual int query_max_flushed_ilog_id(uint64_t& last_flushed_ilog_id)
  {
    UNUSED(last_flushed_ilog_id);
    return OB_SUCCESS;
  }
  virtual int wait_log_confirmed(const int64_t begin_time, const int64_t max_wait_time)
  {
    UNUSED(begin_time);
    UNUSED(max_wait_time);
    return OB_SUCCESS;
  }
  virtual int is_valid_member(const common::ObAddr& addr, bool& is_valid)
  {
    UNUSED(addr);
    UNUSED(is_valid);
    return OB_SUCCESS;
  }
  virtual int backfill_confirmed(const ObLogInfo& log_info, const bool batch_last_succeed)
  {
    UNUSED(log_info);
    UNUSED(batch_last_succeed);
    return OB_SUCCESS;
  }
  virtual int query_confirmed_log(const uint64_t log_id, transaction::ObTransID& trans_id, int64_t& submit_timestamp)
  {
    UNUSED(log_id);
    UNUSED(trans_id);
    UNUSED(submit_timestamp);
    return OB_SUCCESS;
  }
  virtual int backfill_log(
      const ObLogInfo& log_info, const ObLogCursor& log_cursor, const bool is_leader, ObISubmitLogCb* submit_cb)
  {
    UNUSED(log_info);
    UNUSED(log_cursor);
    UNUSED(is_leader);
    UNUSED(submit_cb);
    return OB_SUCCESS;
  }
  virtual int resubmit_log(const ObLogInfo& log_info, ObISubmitLogCb* submit_cb)
  {
    UNUSED(submit_cb);
    UNUSED(log_info);
    return OB_SUCCESS;
  }
  virtual int backfill_nop_log(const ObLogMeta& log_meta)
  {
    UNUSED(log_meta);
    return OB_SUCCESS;
  }
  virtual int is_valid_member(const common::ObAddr& addr, bool& is_valid) const
  {
    UNUSED(addr);
    UNUSED(is_valid);
    return OB_SUCCESS;
  }

  virtual void destroy()
  {
    return;
  }
  virtual int stop_election()
  {
    return common::OB_SUCCESS;
  }
  virtual int remove_election()
  {
    return common::OB_SUCCESS;
  }

  virtual int get_log_id_timestamp(const int64_t prepare_version, ObLogMeta& log_meta)
  {
    UNUSED(prepare_version);
    UNUSED(log_meta);
    return common::OB_SUCCESS;
  }

  virtual int get_pls_epoch(int64_t& pls_epoch) const
  {
    UNUSED(pls_epoch);
    return common::OB_SUCCESS;
  }

  virtual int try_update_keepalive()
  {
    return common::OB_SUCCESS;
  }
  virtual int submit_log(const char* buff, const int64_t size, const uint64_t log_id, const int64_t timestamp,
      const storage::ObStorageLogType log_type, const common::ObVersion& version, ObISubmitLogCb* cb)
  {
    UNUSED(buff);
    UNUSED(size);
    UNUSED(log_id);
    UNUSED(timestamp);
    UNUSED(log_type);
    UNUSED(cb);
    UNUSED(version);
    return common::OB_SUCCESS;
  }
  virtual int submit_log(const char* buff, const int64_t size, const storage::ObStorageLogType log_type,
      const common::ObVersion& version, ObISubmitLogCb* cb, uint64_t& log_id, int64_t& log_timestamp)
  {
    UNUSED(buff);
    UNUSED(size);
    UNUSED(log_type);
    UNUSED(cb);
    UNUSED(version);
    UNUSED(log_id);
    UNUSED(log_timestamp);
    return common::OB_SUCCESS;
  }
  virtual int submit_log(const char* buff, const int64_t size, const int64_t base_timestamp, ObISubmitLogCb* cb,
      const bool need_aggre_commit, uint64_t& log_id, int64_t& log_timestamp)
  {
    UNUSED(buff);
    UNUSED(size);
    UNUSED(base_timestamp);
    UNUSED(cb);
    UNUSED(need_aggre_commit);
    UNUSED(log_id);
    UNUSED(log_timestamp);
    return common::OB_SUCCESS;
  }
  virtual int force_set_replica_num(const int64_t replica_num)
  {
    UNUSED(replica_num);
    return 0;
  }
  virtual int force_set_as_single_replica()
  {
    return 0;
  }
  virtual int force_reset_parent()
  {
    return 0;
  }
  virtual int force_set_parent(const common::ObAddr& new_parent)
  {
    UNUSED(new_parent);
    return 0;
  }
  virtual int add_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
  {
    UNUSED(member);
    UNUSED(quorum);
    UNUSED(log_info);
    return common::OB_SUCCESS;
  }
  virtual int remove_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
  {
    UNUSED(member);
    UNUSED(quorum);
    UNUSED(log_info);
    return common::OB_SUCCESS;
  }
  virtual int is_member_change_done(const obrpc::ObMCLogInfo& log_info)
  {
    UNUSED(log_info);
    return common::OB_SUCCESS;
  }
  virtual int get_minor_base_storage_info(common::ObBaseStorageInfo& base_storage_info)
  {
    UNUSED(base_storage_info);
    return common::OB_SUCCESS;
  }
  virtual common::ObPartitionKey get_partition_key() const
  {
    return p_k_;
  }
  virtual int get_saved_base_storage_info(common::ObBaseStorageInfo& base_storage_info) const
  {
    UNUSED(base_storage_info);
    return common::OB_SUCCESS;
  }
  virtual int get_leader(common::ObAddr& leader)
  {
    UNUSED(leader);
    return common::OB_SUCCESS;
  }
  virtual int change_leader(const common::ObAddr& leader, common::ObTsWindows& changing_leader_windows)
  {
    UNUSED(leader);
    UNUSED(changing_leader_windows);
    return common::OB_SUCCESS;
  }
  virtual int get_role(common::ObRole& role) const
  {
    UNUSED(role);
    return common::OB_SUCCESS;
  }
  virtual int get_role_for_partition_table(common::ObRole& role) const
  {
    UNUSED(role);
    return common::OB_SUCCESS;
  }
  virtual int set_archive_restore_state(const int16_t archive_restore_state)
  {
    UNUSED(archive_restore_state);
    return common::OB_SUCCESS;
  }
  virtual bool is_archive_restoring() const
  {
    return false;
  }
  virtual common::ObAddr get_restore_leader() const
  {
    common::ObAddr fake_addr;
    return fake_addr;
  }
  virtual int try_release_aggre_buffer()
  {
    return common::OB_SUCCESS;
  }
  virtual int get_role_unsafe(bool& in_changing_leader_windows, int64_t& leader_epoch) const
  {
    UNUSED(in_changing_leader_windows);
    UNUSED(leader_epoch);
    return common::OB_SUCCESS;
  }
  virtual int get_role_unlock(bool& in_changing_leader_windows, int64_t& leader_epoch) const
  {
    UNUSED(in_changing_leader_windows);
    UNUSED(leader_epoch);
    return common::OB_SUCCESS;
  }
  virtual int get_role_for_partition_table_unlock(bool& in_changing_leader_windows, int64_t& leader_epoch) const
  {
    UNUSED(in_changing_leader_windows);
    UNUSED(leader_epoch);
    return common::OB_SUCCESS;
  }
  virtual int get_leader_curr_member_list(common::ObMemberList& member_list) const
  {
    UNUSED(member_list);
    return common::OB_SUCCESS;
  }
  virtual int get_leader(common::ObAddr& leader) const
  {
    UNUSED(leader);
    return common::OB_SUCCESS;
  }
  virtual int get_clog_parent(common::ObAddr& parent, int64_t& cluster_id) const
  {
    UNUSED(parent);
    UNUSED(cluster_id);
    return common::OB_SUCCESS;
  }
  virtual int get_curr_member_list(common::ObMemberList& member_list) const
  {
    UNUSED(member_list);
    return common::OB_SUCCESS;
  }
  virtual int get_curr_member_list_for_report(common::ObMemberList& member_list) const
  {
    UNUSED(member_list);
    return common::OB_SUCCESS;
  }
  virtual int update_min_file_id(uint64_t log_id)
  {
    UNUSED(log_id);
    return common::OB_SUCCESS;
  }
  virtual uint64_t get_min_file_id() const
  {
    return 0;
  }
  virtual int update_log_range(const file_id_t file_id, const uint64_t log_id)
  {
    UNUSED(file_id);
    UNUSED(log_id);
    return common::OB_SUCCESS;
  }
  virtual int check_if_start_log_task_empty(bool& is_empty)
  {
    UNUSED(is_empty);
    return common::OB_SUCCESS;
  }
  virtual int receive_log(const ObLogEntry& log_entry, const common::ObAddr& server, const int64_t cluster_id,
      const common::ObProposalID& proposal_id, const ObPushLogMode push_mode, const ReceiveLogType type)
  {
    UNUSED(log_entry);
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(proposal_id);
    UNUSED(push_mode);
    UNUSED(type);
    return common::OB_SUCCESS;
  }
  virtual int receive_local_log(const ObLogEntry& log_entry)
  {
    UNUSED(log_entry);
    return common::OB_SUCCESS;
  }
  virtual int ack_log(const common::ObAddr& server, const uint64_t log_id, const common::ObProposalID& proposal_id)
  {
    UNUSED(server);
    UNUSED(log_id);
    UNUSED(proposal_id);
    return common::OB_SUCCESS;
  }
  virtual int standby_ack_log(const common::ObAddr& server, const int64_t cluster_id, const uint64_t log_id,
      const common::ObProposalID& proposal_id)
  {
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(log_id);
    UNUSED(proposal_id);
    return common::OB_SUCCESS;
  }
  virtual int fake_ack_log(const ObAddr& server, const uint64_t log_id, const ObProposalID& proposal_id)
  {
    UNUSED(server);
    UNUSED(log_id);
    UNUSED(proposal_id);
    return common::OB_SUCCESS;
  }
  virtual int fake_receive_log(const ObAddr& server, const uint64_t log_id, const ObProposalID& proposal_id)
  {
    UNUSED(server);
    UNUSED(log_id);
    UNUSED(proposal_id);
    return common::OB_SUCCESS;
  }
  virtual int get_log(const common::ObAddr& server, const uint64_t log_id, const int64_t log_num,
      const ObFetchLogType fetch_type, const common::ObProposalID& proposal_id, const int64_t cluster_id,
      const common::ObReplicaType replica_type, const int64_t network_limit, const uint64_t max_confirmed_log_id)
  {
    UNUSED(server);
    UNUSED(log_id);
    UNUSED(log_num);
    UNUSED(fetch_type);
    UNUSED(proposal_id);
    UNUSED(cluster_id);
    UNUSED(replica_type);
    UNUSED(network_limit);
    UNUSED(max_confirmed_log_id);
    return common::OB_SUCCESS;
  }

  virtual int get_membership_log(char* buff, const int64_t buff_len, int64_t& used_len)
  {
    UNUSED(buff);
    UNUSED(buff_len);
    UNUSED(used_len);
    return common::OB_SUCCESS;
  }
  virtual int get_max_log_id(
      const common::ObAddr& server, const int64_t cluster_id, const common::ObProposalID& proposal_id)
  {
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(proposal_id);
    return common::OB_SUCCESS;
  }
  virtual int receive_max_log_id(const common::ObAddr& server, const uint64_t max_log_id,
      const ObProposalID& proposal_id, const int64_t max_log_ts)
  {
    UNUSED(server);
    UNUSED(max_log_id);
    UNUSED(proposal_id);
    UNUSED(max_log_ts);
    return common::OB_SUCCESS;
  }
  virtual int append_disk_log(const ObLogEntry& log, const ObLogCursor& log_cursor, const int64_t accum_checksum)
  {
    UNUSED(log);
    UNUSED(log_cursor);
    UNUSED(accum_checksum);
    return common::OB_SUCCESS;
  }
  virtual int is_offline(bool& is_temp)
  {
    is_temp = false;

    return common::OB_SUCCESS;
  }
  virtual int set_offline()
  {
    return common::OB_SUCCESS;
  }
  virtual int set_scan_disk_log_finished()
  {
    return common::OB_SUCCESS;
  }
  virtual int switch_log()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual uint64_t get_curr_file_min_log_id() const
  {
    return 0;
  }
  virtual uint64_t get_curr_file_max_log_id() const
  {
    return 0;
  }

  virtual int on_election_role_change()
  {
    return common::OB_SUCCESS;
  }
  virtual int on_change_leader_retry(const common::ObAddr&, common::ObTsWindows&)
  {
    return common::OB_SUCCESS;
  }
  virtual int switch_state()
  {
    return common::OB_SUCCESS;
  }
  virtual int switch_state(bool& need_retry)
  {
    UNUSED(need_retry);
    return common::OB_SUCCESS;
  }
  virtual int check_mc_and_sliding_window_state()
  {
    return common::OB_SUCCESS;
  }
  virtual int check_lower_level_member_state()
  {
    return common::OB_SUCCESS;
  }
  virtual bool is_heartbeat_timeout(const common::ObAddr& server) const
  {
    UNUSED(server);
    return false;
  }
  virtual int check_replica_state()
  {
    return common::OB_SUCCESS;
  }
  virtual int send_heartbeat_to_leader()
  {
    return common::OB_SUCCESS;
  }
  virtual int check_mc_timeout()
  {
    return common::OB_SUCCESS;
  }
  virtual int check_stale_member(common::ObMember& stale_server)
  {
    UNUSED(stale_server);
    return common::OB_SUCCESS;
  }

  virtual void check_memory_usage()
  {
    return;
  }
  virtual int renew_ms_log_flush_cb(const storage::ObMsInfoTask& arg)
  {
    UNUSED(arg);
    return common::OB_SUCCESS;
  }
  virtual int flush_cb(const oceanbase::clog::ObLogFlushCbArg& arg)
  {
    UNUSED(arg);
    return common::OB_SUCCESS;
  }
  virtual int set_election_leader(const common::ObAddr& leader, const int64_t lease_start)
  {
    UNUSED(leader);
    UNUSED(lease_start);
    return common::OB_SUCCESS;
  }
  virtual bool need_quicker_polling() const
  {
    return true;
  }
  virtual int set_next_index_log_id(const uint64_t log_id, const int64_t accum_checksum)
  {
    UNUSED(log_id);
    UNUSED(accum_checksum);
    return common::OB_SUCCESS;
  }
  virtual int receive_confirmed_info(
      const common::ObAddr& server, const uint64_t log_id, const ObConfirmedInfo& confirm_info)
  {
    UNUSED(server);
    UNUSED(log_id);
    UNUSED(confirm_info);
    return common::OB_SUCCESS;
  }
  virtual int is_in_sync(bool& is_sync) const
  {
    UNUSED(is_sync);
    return common::OB_SUCCESS;
  }
  virtual int is_log_sync_with_leader(bool& is_sync) const
  {
    UNUSED(is_sync);
    return common::OB_SUCCESS;
  }
  virtual int is_log_sync_with_primary(const int64_t switchover_epoch, bool& is_sync) const
  {
    UNUSED(switchover_epoch);
    UNUSED(is_sync);
    return common::OB_SUCCESS;
  }
  virtual bool vt_is_in_sync() const
  {
    return true;
  }
  virtual int update_max_confirmed_log_id(const common::ObAddr& server, const uint64_t log_id)
  {
    UNUSED(server);
    UNUSED(log_id);
    return common::OB_SUCCESS;
  }
  virtual int migrate_set_base_storage_info(const common::ObBaseStorageInfo& base_storage_info)
  {
    UNUSED(base_storage_info);
    return common::OB_SUCCESS;
  }
  virtual int restore_replayed_log(const common::ObBaseStorageInfo& base_storage_info)
  {
    UNUSED(base_storage_info);
    return common::OB_SUCCESS;
  }
  virtual uint64_t get_last_replay_log_id() const
  {
    return OB_INVALID_ID;
  }
  virtual int64_t get_last_submit_timestamp() const
  {
    return OB_INVALID_ID;
  }
  virtual void get_last_replay_log(uint64_t& log_id, int64_t& ts)
  {
    UNUSED(log_id);
    UNUSED(ts);
  }
  virtual uint64_t get_last_slide_log_id()
  {
    return OB_INVALID_ID;
  }
  virtual int report_task_stat()
  {
    return common::OB_SUCCESS;
  }
  virtual int64_t get_membership_timestamp() const
  {
    return 0;
  }
  virtual int get_role_and_last_leader_active_time(oceanbase::common::ObRole& role, int64_t& timestamp) const
  {
    UNUSED(role);
    UNUSED(timestamp);
    return common::OB_SUCCESS;
  }
  virtual int try_replay(const bool need_async, bool& is_replayed, bool& unused)
  {
    UNUSED(need_async);
    UNUSED(is_replayed);
    UNUSED(unused);
    return common::OB_SUCCESS;
  }
  virtual int follower_report_max_confirmed_log_id()
  {
    return common::OB_SUCCESS;
  }
  virtual int set_online(const storage::ObSavedStorageInfo& base_storage_info)
  {
    UNUSED(base_storage_info);
    return common::OB_SUCCESS;
  }
  virtual int get_dst_leader_candidate(common::ObMemberList& member_list) const
  {
    UNUSED(member_list);
    return common::OB_SUCCESS;
  }
  virtual int enable_replay()
  {
    return common::OB_SUCCESS;
  }
  virtual int disable_replay()
  {
    return common::OB_SUCCESS;
  }
  virtual int get_curr_leader_and_memberlist(common::ObAddr& leader, common::ObRole& role,
      common::ObMemberList& curr_member_list, common::ObChildReplicaList& children_list) const
  {
    UNUSED(leader);
    UNUSED(role);
    UNUSED(curr_member_list);
    UNUSED(children_list);
    return common::OB_SUCCESS;
  }
  virtual int get_max_data_version(ObVersion& max_data_version) const
  {
    UNUSED(max_data_version);
    return common::OB_SUCCESS;
  }
  virtual int remove_notify()
  {
    return common::OB_SUCCESS;
  }
  virtual int continue_notify()
  {
    return common::OB_SUCCESS;
  }
  virtual int on_get_election_priority(election::ObElectionPriority& priority)
  {
    UNUSED(priority);
    return common::OB_SUCCESS;
  }
  virtual int set_follower_active()
  {
    return common::OB_SUCCESS;
  }
  virtual int get_proposal_id(common::ObProposalID& proposal_id) const
  {
    UNUSED(proposal_id);
    return common::OB_SUCCESS;
  }

  int get_role_and_leader_epoch(ObRole& role, int64_t& leader_epoch)
  {
    UNUSED(role);
    UNUSED(leader_epoch);
    return common::OB_SUCCESS;
  }
  int get_role_and_leader_epoch(common::ObRole& role, int64_t& leader_epoch, int64_t& takeover_time)
  {
    UNUSED(role);
    UNUSED(leader_epoch);
    UNUSED(takeover_time);
    return common::OB_SUCCESS;
  }
  int get_cursor_from_log_index(uint64_t log_id, ObLogCursorExt& csr)
  {
    UNUSED(log_id);
    UNUSED(csr);
    return OB_SUCCESS;
  }
  int get_cursor_from_log_index(const uint64_t log_id, ObLogCursor& cursor)
  {
    int ret = OB_SUCCESS;
    UNUSED(log_id);
    UNUSED(cursor);
    return ret;
  }
  int get_follower_log_delay(int64_t& log_delay)
  {
    UNUSED(log_delay);
    return common::OB_SUCCESS;
  }
  int get_next_replay_log_info(uint64_t& next_replay_log_id, int64_t& next_replay_log_timestamp)
  {
    UNUSED(next_replay_log_id);
    UNUSED(next_replay_log_timestamp);
    return common::OB_SUCCESS;
  }
  int process_keepalive_msg(const common::ObAddr& server, const int64_t cluster_id, const uint64_t next_log_id,
      const int64_t next_log_ts_lb, const uint64_t deliver_cnt)
  {
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(next_log_id);
    UNUSED(next_log_ts_lb);
    UNUSED(deliver_cnt);
    return common::OB_SUCCESS;
  }
  virtual int process_restore_leader_alive_msg(const common::ObAddr& server, const int64_t resp_time)
  {
    UNUSED(server);
    UNUSED(resp_time);
    return common::OB_SUCCESS;
  }
  virtual int process_leader_max_log_msg(const common::ObAddr& server, const int64_t switchover_epoch,
      const uint64_t leader_max_log_id, const int64_t leader_next_log_ts)
  {
    UNUSED(server);
    UNUSED(switchover_epoch);
    UNUSED(leader_max_log_id);
    UNUSED(leader_next_log_ts);
    return common::OB_SUCCESS;
  }
  virtual int notify_log_missing(
      const common::ObAddr& leader, const uint64_t start_log_id, const bool is_in_member_list, const int32_t msg_type)
  {
    UNUSED(leader);
    UNUSED(start_log_id);
    UNUSED(is_in_member_list);
    UNUSED(msg_type);
    return common::OB_SUCCESS;
  }
  virtual int receive_replica_heartbeat(const common::ObAddr& server, const common::ObRegion& region,
      const int32_t replica_type, const uint64_t max_log_id, const common::ObAddr& register_leader)
  {
    UNUSED(server);
    UNUSED(region);
    UNUSED(replica_type);
    UNUSED(max_log_id);
    UNUSED(register_leader);
    return common::OB_SUCCESS;
  }
  virtual int reject_server(const common::ObAddr& server, const int32_t msg_type)
  {
    UNUSED(server);
    UNUSED(msg_type);
    return common::OB_SUCCESS;
  }
  virtual int process_reject_msg(const common::ObAddr& server, const int32_t msg_type)
  {
    UNUSED(server);
    UNUSED(msg_type);
    return common::OB_SUCCESS;
  }
  virtual int get_log_id_range(uint64_t& start_log_id, int64_t& start_ts, uint64_t& end_log_id, int64_t& end_ts)
  {
    start_log_id = OB_INVALID_ID;
    start_ts = OB_INVALID_TIMESTAMP;
    end_log_id = OB_INVALID_ID;
    end_ts = OB_INVALID_TIMESTAMP;
    return OB_SUCCESS;
  }

  virtual int truncate_log_index(const uint64_t new_start_id)
  {
    UNUSED(new_start_id);
    return OB_SUCCESS;
  }

  virtual ObClogVirtualStat* alloc_clog_virtual_stat()
  {
    return static_cast<ObClogVirtualStat*>(NULL);
  }
  virtual int revert_clog_virtual_stat(ObClogVirtualStat* virtual_stat)
  {
    UNUSED(virtual_stat);
    return OB_SUCCESS;
  }
  virtual int64_t get_sync_timestamp()
  {
    return OB_INVALID_TIMESTAMP;
  }
  virtual int update_log_sync_state(const uint64_t log_id_, const int64_t sync_timestamp)
  {
    UNUSED(log_id_);
    UNUSED(sync_timestamp);
    return common::OB_SUCCESS;
  }
  virtual int reduce_active_used()
  {
    return common::OB_SUCCESS;
  }
  virtual int async_get_log(const common::ObAddr& server, const int64_t cluster_id, const uint64_t start_log_id,
      const uint64_t end_log_id, const ObFetchLogType fetch_type, const common::ObProposalID& proposal_id,
      const int64_t network_limit)
  {
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(start_log_id);
    UNUSED(end_log_id);
    UNUSED(fetch_type);
    UNUSED(proposal_id);
    UNUSED(network_limit);
    return common::OB_SUCCESS;
  }
  virtual int get_next_timestamp(const uint64_t last_log_id, int64_t& res_ts)
  {
    UNUSED(last_log_id);
    res_ts = OB_INVALID_TIMESTAMP;
    return common::OB_SUCCESS;
  }
  virtual int get_next_served_log_info_for_leader(uint64_t& next_served_log_id, int64_t& next_served_log_ts)
  {
    next_served_log_id = OB_INVALID_ID;
    next_served_log_ts = OB_INVALID_TIMESTAMP;
    return common::OB_SUCCESS;
  }
  virtual int try_truncate(const common::ObBaseStorageInfo& base_storage_info)
  {
    UNUSED(base_storage_info);
    return common::OB_SUCCESS;
  }
  virtual int update_leader_max_confirmed_log_id(const common::ObAddr& server, uint64_t log_id)
  {
    UNUSED(server);
    UNUSED(log_id);
    return common::OB_SUCCESS;
  }
  virtual int pre_prepare_freeze()
  {
    return common::OB_SUCCESS;
  }
  virtual int commit_abort_freeze()
  {
    return common::OB_SUCCESS;
  }
  virtual int get_cur_ilog_progress(uint64_t& log_id, int64_t& ts, file_id_t& file_id)
  {
    UNUSED(log_id);
    UNUSED(ts);
    UNUSED(file_id);
    return common::OB_SUCCESS;
  }

  virtual int index_flush_cb(const uint64_t log_id, const int64_t ts)
  {
    UNUSED(log_id);
    UNUSED(ts);
    return common::OB_SUCCESS;
  }
  virtual int is_ilog_flushed(uint64_t log_id, bool& is_flushed)
  {
    UNUSED(log_id);
    UNUSED(is_flushed);
    return common::OB_SUCCESS;
  }
  virtual int get_sw_max_log_id(uint64_t& sw_max_log_id)
  {
    UNUSED(sw_max_log_id);
    return common::OB_SUCCESS;
  }
  virtual uint64_t get_next_index_log_id() const
  {
    return 0;
  }
  virtual int64_t get_scan_confirmed_log_cnt() const
  {
    return 0;
  }
  virtual int allow_gc(bool& allow)
  {
    UNUSED(allow);
    return common::OB_SUCCESS;
  }
  virtual int try_truncate_log_index(const uint64_t new_start_id, const bool new_merged)
  {
    UNUSED(new_merged);
    UNUSED(new_start_id);
    return common::OB_SUCCESS;
  }
  virtual int wait_log_callback_done()
  {
    return common::OB_SUCCESS;
  }
  virtual void set_zone_priority(const uint64_t zone_priority)
  {
    UNUSED(zone_priority);
  }
  virtual int set_region(const common::ObRegion& region)
  {
    UNUSED(region);
    return common::OB_SUCCESS;
  }
  virtual int set_idc(const common::ObIDC& idc)
  {
    UNUSED(idc);
    return common::OB_SUCCESS;
  }
  virtual enum ObReplicaType get_replica_type()
  {
    return ObReplicaType::REPLICA_TYPE_FULL;
  }
  virtual int set_replica_type(const enum ObReplicaType replica_type)
  {
    UNUSED(replica_type);
    return common::OB_SUCCESS;
  }
  virtual int check_is_normal_partition(bool& is_normal_partition) const
  {
    UNUSED(is_normal_partition);
    return common::OB_SUCCESS;
  }
  virtual int get_election_priority(election::ObElectionPriority& priority) const
  {
    UNUSED(priority);
    return common::OB_SUCCESS;
  }
  virtual int get_candidate_info(ObCandidateInfo& info)
  {
    UNUSED(info);
    return common::OB_SUCCESS;
  }
  virtual int pre_change_member(const int64_t quorum, const bool is_add_member, int64_t& mc_timestamp,
      ObMemberList& member_list, ObProposalID& proposal_id)
  {
    int ret = OB_SUCCESS;
    UNUSED(quorum);
    UNUSED(is_add_member);
    UNUSED(mc_timestamp);
    UNUSED(member_list);
    UNUSED(proposal_id);
    return ret;
  }
  virtual int set_member_changing()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual int reset_member_changing()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual int wait_log_confirmed()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  virtual int batch_change_member(const common::ObMember& member, const int64_t quorum, const int64_t mc_timestamp,
      const common::ObProposalID& proposal_id, const bool is_add_member, obrpc::ObMCLogInfo& mc_log_info)
  {
    int ret = OB_SUCCESS;
    UNUSED(member);
    UNUSED(quorum);
    UNUSED(mc_timestamp);
    UNUSED(proposal_id);
    UNUSED(is_add_member);
    UNUSED(mc_log_info);
    return ret;
  }
  virtual bool need_skip_when_check_start_service() const
  {
    bool bool_ret = false;
    return bool_ret;
  }
  virtual int broadcast_info()
  {
    return 0;
  }
  virtual int update_broadcast_info(
      const common::ObAddr& server, const common::ObReplicaType& replica_type, const uint64_t max_confirmed_log_id)
  {
    UNUSED(server);
    UNUSED(replica_type);
    UNUSED(max_confirmed_log_id);
    return 0;
  }
  virtual int get_recyclable_log_id(uint64_t& log_id) const
  {
    UNUSED(log_id);
    return 0;
  }
  virtual int get_partition_max_log_id(uint64_t& partition_max_log_id)
  {
    int ret = OB_SUCCESS;
    UNUSED(partition_max_log_id);
    return ret;
  }
  virtual uint64_t get_max_confirmed_log_id() const
  {
    return 0;
  }
  virtual int is_need_rebuild(bool& need_rebuild) const
  {
    UNUSED(need_rebuild);
    return OB_NOT_SUPPORTED;
  }
  virtual bool is_leader_active() const
  {
    return false;
  }
  virtual int query_confirmed_log(const uint64_t log_id, ObLogEntry& log_entry)
  {
    UNUSED(log_id);
    UNUSED(log_entry);
    return OB_NOT_SUPPORTED;
  }
  virtual int change_quorum(const common::ObMemberList& curr_member_list, const int64_t curr_quorum,
      const int64_t new_quorum, obrpc::ObMCLogInfo& log_info)
  {
    int ret = OB_SUCCESS;
    UNUSED(curr_member_list);
    UNUSED(curr_quorum);
    UNUSED(new_quorum);
    UNUSED(log_info);
    return ret;
  }
  virtual int get_replica_num(int64_t& replica_num) const
  {
    int ret = OB_SUCCESS;
    UNUSED(replica_num);
    return ret;
  }
  virtual bool need_add_event_task()
  {
    return true;
  }
  virtual void reset_has_event_task()
  {
    return;
  }
  virtual int check_can_receive_batch_log(const uint64_t log_id)
  {
    UNUSED(log_id);
    return common::OB_SUCCESS;
  }
  virtual int update_max_flushed_ilog_id(const uint64_t log_id)
  {
    UNUSED(log_id);
    return common::OB_SUCCESS;
  }
  virtual int reset_has_pop_task()
  {
    return common::OB_SUCCESS;
  }
  virtual int get_cursor_with_cache(const int64_t scan_thread_index, const uint64_t log_id, ObLogCursorExt& log_cursor)
  {
    int ret = OB_SUCCESS;
    UNUSED(scan_thread_index);
    UNUSED(log_id);
    UNUSED(log_cursor);
    return ret;
  }
  virtual int set_member_list(const ObMemberList& member_list, const int64_t replica_num,
      const common::ObAddr& assigned_leader, const int64_t lease_start)
  {
    UNUSED(member_list);
    UNUSED(replica_num);
    UNUSED(assigned_leader);
    UNUSED(lease_start);
    return OB_SUCCESS;
  }
  virtual int try_freeze_aggre_buffer()
  {
    return OB_SUCCESS;
  }
  virtual int leader_send_max_log_info()
  {
    return OB_SUCCESS;
  }
  virtual bool has_valid_next_replay_log_ts() const
  {
    return false;
  }
  virtual int process_check_rebuild_req(
      const common::ObAddr& server, const uint64_t start_log_id, const int64_t cluster_id)
  {
    UNUSED(server);
    UNUSED(start_log_id);
    UNUSED(cluster_id);
    return OB_SUCCESS;
  }
  virtual void get_max_majority_log(uint64_t& log_id, int64_t& log_ts) const
  {
    log_id = 0;
    log_ts = 0;
  }
  virtual int change_restore_leader(const common::ObAddr& leader)
  {
    UNUSED(leader);
    return OB_SUCCESS;
  }
  virtual int process_restore_takeover_msg(const int64_t send_ts)
  {
    UNUSED(send_ts);
    return OB_SUCCESS;
  }
  virtual int set_restore_fetch_log_finished(ObArchiveFetchLogResult result)
  {
    UNUSED(result);
    return OB_SUCCESS;
  }
  virtual int try_update_next_replay_log_ts_in_restore(const int64_t ts)
  {
    UNUSED(ts);
    return OB_SUCCESS;
  }

  virtual int handle_standby_prepare_req(
      const ObAddr& server, const int64_t cluster_id, const ObProposalID& proposal_id)
  {
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(proposal_id);
    return OB_SUCCESS;
  }
  virtual int handle_standby_prepare_resp(const ObAddr& server, const ObProposalID& proposal_id,
      const uint64_t ms_log_id, const int64_t membership_version, const ObMemberList& member_list)
  {
    UNUSED(server);
    UNUSED(proposal_id);
    UNUSED(ms_log_id);
    UNUSED(membership_version);
    UNUSED(member_list);
    return OB_SUCCESS;
  }

  virtual int ack_renew_ms_log(
      const ObAddr& server, const uint64_t log_id, const int64_t submit_timestamp, const ObProposalID& ms_proposal_id)
  {
    UNUSED(server);
    UNUSED(log_id);
    UNUSED(submit_timestamp);
    UNUSED(ms_proposal_id);
    return OB_SUCCESS;
  }
  virtual int receive_renew_ms_log_confirmed_info(const common::ObAddr& server, const uint64_t log_id,
      const common::ObProposalID& ms_proposal_id, const ObConfirmedInfo& confirmed_info)
  {
    UNUSED(server);
    UNUSED(log_id);
    UNUSED(ms_proposal_id);
    UNUSED(confirmed_info);
    return OB_SUCCESS;
  }

  virtual int check_and_set_restore_progress()
  {
    return OB_SUCCESS;
  }

  virtual int restore_leader_try_confirm_log()
  {
    return OB_SUCCESS;
  }
  virtual int standby_add_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
  {
    UNUSED(member);
    UNUSED(quorum);
    UNUSED(log_info);
    return OB_SUCCESS;
  }
  virtual int standby_remove_member(const common::ObMember& member, const int64_t quorum, obrpc::ObMCLogInfo& log_info)
  {
    UNUSED(member);
    UNUSED(quorum);
    UNUSED(log_info);
    return OB_SUCCESS;
  }
  virtual int receive_renew_ms_log(const ObLogEntry& log_entry, const ObAddr& server, const int64_t cluster_id,
      const ObProposalID& proposal_id, const ReceiveLogType type)
  {
    UNUSED(log_entry);
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(proposal_id);
    UNUSED(type);
    return OB_SUCCESS;
  }
  virtual int standby_pre_change_member(const int64_t quorum, const bool is_add_member, int64_t& mc_timestamp,
      uint64_t max_log_id, ObMemberList& member_list, ObProposalID& proposal_id)
  {
    UNUSED(quorum);
    UNUSED(is_add_member);
    UNUSED(mc_timestamp);
    UNUSED(max_log_id);
    UNUSED(member_list);
    UNUSED(proposal_id);
    return OB_SUCCESS;
  }
  virtual int standby_wait_log_confirmed(
      const int64_t begin_time, const int64_t max_wait_time, const uint64_t max_log_id)
  {
    UNUSED(begin_time);
    UNUSED(max_wait_time);
    UNUSED(max_log_id);
    return OB_SUCCESS;
  }
  virtual int standby_batch_change_member(const common::ObMember& member, const int64_t quorum,
      const int64_t mc_timestamp, const uint64_t max_log_id, const common::ObProposalID& proposal_id,
      const bool is_add_member, obrpc::ObMCLogInfo& mc_log_info)
  {
    UNUSED(member);
    UNUSED(quorum);
    UNUSED(mc_timestamp);
    UNUSED(max_log_id);
    UNUSED(proposal_id);
    UNUSED(is_add_member);
    UNUSED(mc_log_info);
    return OB_SUCCESS;
  }

  virtual int standby_update_protection_level()
  {
    return OB_SUCCESS;
  }
  virtual int get_standby_leader_protection_level(uint32_t& protection_level)
  {
    UNUSED(protection_level);
    return OB_SUCCESS;
  }
  virtual int primary_process_protect_mode_switch()
  {
    return OB_SUCCESS;
  }
  virtual int handle_query_sync_start_id_req(const ObAddr& server, const int64_t cluster_id, const int64_t send_ts)
  {
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(send_ts);
    return OB_SUCCESS;
  }
  virtual int handle_sync_start_id_resp(
      const ObAddr& server, const int64_t cluster_id, const int64_t original_send_ts, const uint64_t sync_start_id)
  {
    UNUSED(server);
    UNUSED(cluster_id);
    UNUSED(original_send_ts);
    UNUSED(sync_start_id);
    return OB_SUCCESS;
  }
  virtual bool has_valid_member_list() const
  {
    return true;
  }
  virtual bool is_standby_restore_state() const
  {
    return true;
  }
  virtual int check_and_try_leader_revoke(const election::ObElection::RevokeType& revoke_type)
  {
    UNUSED(revoke_type);
    return OB_SUCCESS;
  }
  int try_update_leader_from_loc_cache()
  {
    return OB_SUCCESS;
  }

private:
  common::ObPartitionKey p_k_;
};

}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_MOCK_OB_PARTITION_LOG_SERVICE_H_
