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

#include "storage/ob_i_partition_group.h"
#include "storage/ob_replay_status.h"
#include "storage/ob_saved_storage_info.h"
#include "storage/ob_pg_all_meta_checkpoint_writer.h"
#include "storage/ob_i_partition_report.h"
#include "gmock/gmock.h"
#include "common/ob_range.h"
#include "share/ob_partition_modify.h"
#include "share/stat/ob_table_stat.h"
#include "clog/ob_log_define.h"

#ifndef OCEANBASE_STORAGE_MOCK_OB_PARTITION_H_
#define OCEANBASE_STORAGE_MOCK_OB_PARTITION_H_

namespace oceanbase {
namespace storage {
class MockObIPartitionGroup : public ObIPartitionGroup {
public:
  MOCK_METHOD8(init, int(const common::ObPartitionKey& key, ObIPartitionComponentFactory* cp_fty,
                         share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs,
                         replayengine::ObILogReplayEngine* rp_eg, ObPartitionService* ps,
                         ObPartitionGroupIndex* pg_index, ObPGPartitionMap* pg_partition_map));
  MOCK_METHOD0(destroy, void());
  // get partition log service
  MOCK_CONST_METHOD0(get_log_service, const clog::ObIPartitionLogService*());
  MOCK_METHOD0(get_log_service, clog::ObIPartitionLogService*());
  MOCK_METHOD1(is_replica_need_gc, int(bool& is_offline));

  MOCK_METHOD0(get_partition_service, ObPartitionService*());
  MOCK_METHOD0(get_pg_index, ObPartitionGroupIndex*());
  MOCK_METHOD0(get_pg_partition_map, ObPGPartitionMap*());
  MOCK_METHOD0(get_trans_service, transaction::ObTransService*());
  // get partition storage
  MOCK_CONST_METHOD0(get_partition_key, const common::ObPartitionKey&());

  MOCK_METHOD2(table_scan, int(ObTableScanParam& param, common::ObNewRowIterator*& result));
  MOCK_METHOD2(table_scan, int(ObTableScanParam& param, common::ObNewIterIterator*& result));
  MOCK_METHOD4(join_mv_scan, int(ObTableScanParam& left_param, ObTableScanParam& right_param,
                                 ObIPartitionGroup& right_partition, common::ObNewRowIterator*& result));
  MOCK_METHOD1(revert_scan_iter, int(common::ObNewRowIterator* iter));
  MOCK_METHOD1(revert_scan_iter, int(common::ObNewIterIterator* iter));
  MOCK_METHOD5(delete_rows,
      int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& column_ids,
          common::ObNewRowIterator* row_iter, int64_t& affected_rows));
  MOCK_METHOD4(delete_row, int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
                               const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row));
  MOCK_METHOD5(put_rows,
      int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& column_ids,
          common::ObNewRowIterator* row_iter, int64_t& affected_rows));
  MOCK_METHOD5(insert_rows,
      int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& column_ids,
          common::ObNewRowIterator* row_iter, int64_t& affected_rows));
  MOCK_METHOD8(insert_row,
      int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& column_ids,
          const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row, const ObInsertFlag flag,
          int64_t& affected_rows, common::ObNewRowIterator*& duplicated_rows));
  MOCK_METHOD4(insert_row, int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
                               const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row));
  MOCK_METHOD6(fetch_conflict_rows,
      int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& in_column_ids,
          const common::ObIArray<uint64_t>& out_column_ids, common::ObNewRowIterator& check_row_iter,
          common::ObIArray<common::ObNewRowIterator*>& dup_row_iters));
  MOCK_METHOD2(revert_insert_iter, int(const common::ObPartitionKey& pkey, common::ObNewRowIterator* iter));
  MOCK_METHOD6(update_rows,
      int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& column_ids,
          const common::ObIArray<uint64_t>& updated_column_ids, common::ObNewRowIterator* row_iter,
          int64_t& affected_rows));
  MOCK_METHOD6(update_row,
      int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& column_ids,
          const common::ObIArray<uint64_t>& updated_column_ids, const common::ObNewRow& old_row,
          const common::ObNewRow& new_row));
  MOCK_METHOD6(lock_rows, int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
                              common::ObNewRowIterator* row_iter, const ObLockFlag lock_flag, int64_t& affected_rows));
  MOCK_METHOD5(lock_rows, int(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
                              const common::ObNewRow& row, ObLockFlag lock_flag));

  MOCK_METHOD0(set_valid, int());
  MOCK_CONST_METHOD0(is_valid, bool());
  MOCK_METHOD2(get_migrating_store, int(const common::ObVersion& migrate_version, ObTablesHandle& handle));

  // leader or follower
  MOCK_CONST_METHOD1(get_role, int(common::ObRole& role));
  MOCK_CONST_METHOD1(get_role_for_partition_table, int(common::ObRole& role));
  MOCK_CONST_METHOD1(get_role_unsafe, int(common::ObRole& role));
  MOCK_CONST_METHOD1(get_leader_curr_member_list, int(common::ObMemberList& member_list));
  MOCK_CONST_METHOD1(get_leader, int(common::ObAddr& addr));
  MOCK_CONST_METHOD1(get_curr_member_list, int(common::ObMemberList& member_list));
  MOCK_CONST_METHOD1(get_curr_member_list_for_report, int(common::ObMemberList& member_list));
  MOCK_CONST_METHOD2(get_curr_member_list, int(const common::ObPartitionKey& pkey, common::ObMemberList& member_list));
  MOCK_CONST_METHOD4(get_curr_leader_and_memberlist,
      int(common::ObAddr& leader, common::ObRole& role, common::ObMemberList& curr_member_list,
          common::ObChildReplicaList& children_list));
  MOCK_CONST_METHOD1(get_dst_leader_candidate, int(common::ObMemberList& member_list));
  MOCK_CONST_METHOD1(get_log_archive_status, int(clog::ObPGLogArchiveStatus& status));
  MOCK_METHOD1(change_leader, int(const common::ObAddr& leader));
  MOCK_METHOD0(leader_takeover, int());
  MOCK_METHOD0(leader_revoke, int());
  MOCK_METHOD0(leader_active, int());
  MOCK_METHOD0(get_replay_status, ObReplayStatus*());
  MOCK_METHOD0(adjust_freeze_status, int());
  MOCK_METHOD1(retire_warmup_store, int(const bool is_disk_full));
  MOCK_METHOD1(enable_write_log, int(const bool replay_old));
  MOCK_METHOD2(get_pg_partition, int(const common::ObPartitionKey& pkey, ObPGPartitionGuard& guard));

  // write ssstore objects @version tree to data file , used by write_check_point
  MOCK_CONST_METHOD3(serialize, int(char* buf, const int64_t buf_len, int64_t& pos));
  MOCK_METHOD3(serialize, int(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_len));
  // read ssstore objects from data file to construct partition storage's version tree.
  MOCK_METHOD3(deserialize, int(const char* buf, const int64_t buf_len, int64_t& pos));
  MOCK_METHOD3(deserialize_v1, int(const char* buf, const int64_t buf_len, int64_t& pos));

  MOCK_CONST_METHOD0(get_serialize_size, int64_t());

  MOCK_METHOD1(get_create_ts, int(int64_t& create_ts));
  MOCK_CONST_METHOD0(get_cur_min_log_service_ts, int64_t());
  MOCK_CONST_METHOD0(get_cur_min_trans_service_ts, int64_t());
  MOCK_CONST_METHOD0(get_cur_min_replay_engine_ts, int64_t());
  MOCK_METHOD1(set_migrating_flag, void(const bool flag));
  MOCK_CONST_METHOD0(get_migrating_flag, bool());
  MOCK_METHOD0(replay_status_revoke, void());
  MOCK_METHOD0(report_clog_history_online, int());
  MOCK_METHOD0(report_clog_history_offline, int());

  MOCK_METHOD1(offline_itself, int(const bool));
  MOCK_CONST_METHOD0(is_removed, bool());
  MOCK_CONST_METHOD1(check_is_in_member_list, int(bool& is_in_member_list));
  MOCK_METHOD4(add_partition_to_pg, int(const int64_t log_type, const obrpc::ObCreatePartitionArg& arg,
                                        ObPartitionService* ps, ObAddPartitionToPGLogCb*& out_cb));

  MOCK_METHOD6(retry_raise_memstore_until_timeout,
      int(const ObFrozenStatus& frozen_status, const bool switch_state, const int64_t wait_freeze_interval,
          const int64_t expired_time, transaction::ObTransService* txs, ObFreezeLogCb* freeze_log_cb));
  MOCK_METHOD5(
      raise_memstore, int(const ObFrozenStatus& frozen_status, const bool switch_state, const int64_t expired_time,
                          transaction::ObTransService* txs, ObFreezeLogCb* freeze_log_cb));
  MOCK_METHOD3(raise_memstore,
      int(const ObFrozenStatus& frozen_status, const ObSavedStorageInfo& info, transaction::ObTransService* txs));
  MOCK_METHOD3(submit_freeze_log,
      int(const ObStorageLogType log_type, const ObSavedStorageInfo& info, ObFreezeLogCb* freeze_log_cb));
  MOCK_METHOD4(decode_freeze_log, int(const char* log, const int64_t size, int64_t& pos, ObSavedStorageInfo& info));
  MOCK_METHOD4(replay_partition_meta_log,
      int(const ObStorageLogType log_type, const int64_t log_id, const char* buf, const int64_t size));
  MOCK_METHOD0(set_wait_split, int());
  MOCK_METHOD1(save_split_state, int(const bool write_slog));
  MOCK_METHOD1(restore_split_state, int(const int state));
  MOCK_METHOD1(restore_split_info, int(const ObPartitionSplitInfo& split_info));
  MOCK_METHOD3(
      replay_split_source_log, int(const ObPartitionSplitSourceLog& log, const uint64_t log_id, const int64_t log_ts));
  MOCK_METHOD1(replay_split_dest_log, int(const ObPartitionSplitDestLog& log));
  MOCK_METHOD2(sync_split_source_log_success, int(const int64_t log_id, const int64_t log_ts));
  MOCK_METHOD0(sync_split_dest_log_success, int());
  MOCK_METHOD3(prepare_splitting,
      int(const ObPartitionSplitInfo& split_info, const common::ObMemberList& mlist, const common::ObAddr& leader));
  MOCK_METHOD3(split_source_partition, int(const int64_t schema_version, const share::ObSplitPartitionPair& info,
                                           enum share::ObSplitProgress& partition_progress));
  MOCK_METHOD2(
      split_dest_partition, int(const ObPartitionSplitInfo& split_info, enum share::ObSplitProgress& progress));
  MOCK_METHOD1(replay_split_state_slog, int(const ObSplitPartitionStateLogEntry& log_entry));
  MOCK_METHOD1(replay_split_info_slog, int(const ObSplitPartitionInfoLogEntry& log_entry));
  MOCK_METHOD3(set_dest_partition_split_progress,
      int(const int64_t schema_version, const common::ObPartitionKey& pkey, const int progress));
  MOCK_METHOD2(get_all_table_ids, int(const ObPartitionKey& pkey, common::ObIArray<uint64_t>& index_tables));
  MOCK_METHOD3(get_reference_tables, int(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle));
  MOCK_METHOD1(get_reference_memtables, int(ObTablesHandle& handle));
  MOCK_METHOD3(set_reference_tables, int(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle));
  MOCK_METHOD1(set_split_version, int(const int64_t split_version));
  MOCK_METHOD0(set_source_tables_refered, int());
  MOCK_METHOD5(get_split_table_ids, int(const common::ObVersion& merge_version, bool is_minor_split, bool& need_split,
                                        bool& need_minor_split, common::ObIArray<uint64_t>& index_tables));
  MOCK_METHOD1(check_can_migrate, int(bool& can_migrate));
  MOCK_CONST_METHOD0(is_splitting, bool());
  MOCK_CONST_METHOD0(is_split_source_partition, bool());
  MOCK_CONST_METHOD0(is_in_dest_split, bool());
  MOCK_CONST_METHOD0(is_dest_logical_split_finish, bool());
  MOCK_CONST_METHOD0(check_split_state, int());
  MOCK_METHOD2(get_split_progress, int(const int64_t schema_version, int& progress));
  MOCK_METHOD2(set_split_progress, int(const common::ObAddr& replica, const int progress));
  MOCK_METHOD1(check_physical_split, int(bool& finished));
  MOCK_METHOD0(block_partition_split_by_mc, int());
  MOCK_METHOD0(unblock_partition_split_by_mc, int());
  MOCK_METHOD1(need_replay_redo, bool(const int64_t log_id));
  MOCK_CONST_METHOD0(get_freeze_snapshot_ts, int64_t());

  MOCK_METHOD1(get_frozen_version, int(common::ObVersion& frozen_version));
  MOCK_CONST_METHOD1(get_merged_version, int(common::ObVersion& merged_version));
  MOCK_CONST_METHOD1(get_active_version, int(common::ObVersion& active_version));
  MOCK_METHOD1(set_frozen_version, int(const common::ObVersion& frozen_version));
  MOCK_METHOD1(effect_new_active_memstore, int(common::ObVersion& version));
  MOCK_METHOD0(revert_new_active_memstore, int());
  MOCK_CONST_METHOD0(get_partition_state, ObPartitionState());
  MOCK_METHOD1(switch_partition_state, int(const ObPartitionState state));
  MOCK_METHOD1(try_switch_partition_state, int(const ObPartitionState state));
  MOCK_METHOD1(get_safe_publish_version, int(int64_t& publish_version));
  MOCK_METHOD1(get_ssstore_max_version, int(oceanbase::common::ObVersion&));
  MOCK_METHOD1(get_base_ssstore_max_version, int(oceanbase::common::ObVersion&));
  MOCK_METHOD1(do_warm_up_request, int(const oceanbase::storage::ObIWarmUpRequest*));
  MOCK_METHOD2(check_can_do_merge, int(bool& can_merge, bool& need_merge));
  MOCK_METHOD0(get_cur_min_log_service_ts, int64_t());
  MOCK_METHOD0(get_cur_min_trans_service_ts, int64_t());
  MOCK_METHOD0(get_cur_min_replay_engine_ts, int64_t());
  MOCK_METHOD3(
      check_can_do_minor_merge, int(ObIPartitionStorage* storage, const int64_t frozen_version, bool& can_merge));
  MOCK_METHOD1(raise_minor_memstore, int(const ObFrozenStatus& frozen_status));
  MOCK_METHOD2(set_replica_type, int(const common::ObReplicaType& replica_type, const bool write_redo_log));
  MOCK_CONST_METHOD0(get_replica_type, common::ObReplicaType());
  MOCK_CONST_METHOD0(get_replica_property, common::ObReplicaProperty());
  MOCK_CONST_METHOD0(is_replica_using_remote_memstore, bool());
  MOCK_CONST_METHOD0(is_share_major_in_zone, bool());
  MOCK_METHOD3(append_local_sort_data,
      int(const common::ObPartitionKey& pkey, const share::ObBuildIndexAppendLocalDataParam& param,
          common::ObNewRowIterator& iter));
  MOCK_METHOD3(append_sstable, int(const common::ObPartitionKey& pkey,
                                   const share::ObBuildIndexAppendSSTableParam& param, common::ObNewRowIterator& iter));
  MOCK_METHOD3(get_latest_schema_version, int(share::schema::ObMultiVersionSchemaService* schema_service,
                                              const common::ObPartitionKey& pkey, int64_t& latest_schema_version));
  MOCK_METHOD3(freeze, int(const bool emergency, const bool force, int64_t& freeze_snapshot));
  MOCK_METHOD1(mark_dirty_trans, int(bool& cleared));
  MOCK_METHOD4(get_curr_storage_info_for_migrate,
      int(const bool use_slave_safe_read_ts, const common::ObReplicaType replica_type, const int64_t src_cluster_id,
          ObSavedStorageInfoV2& info));

  MOCK_CONST_METHOD1(check_is_from_restore, int(bool& is_from_restore));
  MOCK_CONST_METHOD1(get_all_saved_info, int(ObSavedStorageInfoV2& info));
  MOCK_CONST_METHOD1(get_saved_clog_info, int(common::ObBaseStorageInfo& clog_info));
  MOCK_CONST_METHOD1(get_saved_data_info, int(ObDataStorageInfo& data_info));
  MOCK_CONST_METHOD2(get_saved_last_log_info, int(uint64_t& log_id, int64_t& submit_timestamp));

  MOCK_METHOD0(get_split_info, ObPartitionSplitInfo&());
  MOCK_METHOD1(check_cur_partition_split, int(bool& is_split_partition));
  MOCK_METHOD1(get_trans_split_info, int(transaction::ObTransSplitInfo& split_info));
  MOCK_METHOD2(
      check_single_replica_major_sstable_exist, int(const common::ObPartitionKey& pkey, const uint64_t index_table_id));
  MOCK_CONST_METHOD1(get_max_decided_trans_version, int(int64_t& max_decided_trans_version));

  MOCK_METHOD3(
      create_memtable, int(const bool in_slog_trans, const bool is_replay, const bool ignore_memstore_percent));
  MOCK_METHOD2(get_table_stat, int(const common::ObPartitionKey& pkey, common::ObTableStat& stat));
  MOCK_METHOD1(get_checkpoint, int(int64_t& checkpoint));
  MOCK_METHOD1(update_last_checkpoint, int(const int64_t checkpoint));
  MOCK_METHOD1(set_replay_checkpoint, int(const int64_t checkpoint));
  //  MOCK_METHOD1(get_replay_checkpoint, int(int64_t &checkpoint));
  int get_replay_checkpoint(int64_t& checkpoint)
  {
    checkpoint = 100;
    return common::OB_SUCCESS;
  }
  MOCK_METHOD1(allow_gc, int(bool& allow_gc));
  MOCK_METHOD3(gc_check_valid_member, int(const bool is_valid, const int64_t gc_seq, bool& need_gc));
  MOCK_METHOD1(check_pg_partition_offline, bool(const ObPartitionKey& pkey));
  MOCK_CONST_METHOD4(check_offline_log_archived,
      int(const ObPartitionKey& pkey, const int64_t incarnation, const int64_t archive_round, bool& has_archived));
  MOCK_CONST_METHOD1(get_leader_epoch, int(int64_t& leader_epoch));
  MOCK_METHOD0(clear_pg_non_reused_stores, int());
  MOCK_METHOD0(clear_sstables_after_relay_point, int());
  MOCK_METHOD1(get_replica_status, int(share::ObReplicaStatus& status));
  MOCK_METHOD1(set_storage_info, int(const ObSavedStorageInfoV2& info));
  MOCK_METHOD1(fill_replica, int(share::ObPartitionReplica& replica));
  MOCK_METHOD3(fill_pg_partition_replica,
      int(const ObPartitionKey& pkey, share::ObReplicaStatus& replica_status, ObReportStatus& report_status));
  MOCK_METHOD0(get_gc_schema_drop_ts, int64_t());
  MOCK_CONST_METHOD1(get_merge_priority_info, int(memtable::ObMergePriorityInfo& info));
  MOCK_METHOD0(set_need_rebuild, void());
  MOCK_CONST_METHOD0(is_need_rebuild, bool());
  void set_need_standby_restore()
  {
    return;
  }
  bool is_need_standby_restore() const
  {
    return false;
  }
  void reset_migrate_retry_flag()
  {
    return;
  }
  void set_need_gc()
  {
    return;
  }
  bool is_need_gc() const
  {
    return false;
  }
  uint64_t get_offline_log_id() const
  {
    return 0;
  }
  int set_offline_log_id(const uint64_t log_id)
  {
    UNUSED(log_id);
    return common::OB_SUCCESS;
  }
  MOCK_METHOD0(get_min_replayed_log_id, uint64_t());
  MOCK_METHOD2(get_min_replayed_log, void(uint64_t& min_replay_log_id, int64_t& min_replay_log_ts));
  MOCK_METHOD2(get_min_replayed_log_with_keepalive, int(uint64_t& min_replay_log_id, int64_t& min_replay_log_ts));
  MOCK_CONST_METHOD1(get_table_store_cnt, int(int64_t& table_cnt));
  MOCK_METHOD4(
      check_dirty_txn, int(const int64_t min_log_ts, const int64_t max_log_ts, int64_t& freeze_ts, bool& is_dirty));
  MOCK_METHOD1(create_partition_group, int(const ObCreatePGParam& param));
  MOCK_METHOD8(create_pg_partition,
      int(const common::ObPartitionKey& pkey, const int64_t multi_version_start, const uint64_t data_table_id,
          const obrpc::ObCreatePartitionArg& arg, const bool in_slog_trans, const bool is_replay, const uint64_t log_id,
          ObTablesHandle& sstables_handle));
  MOCK_METHOD8(create_pg_partition,
      int(const common::ObPartitionKey& pkey, const int64_t multi_version_start, const uint64_t data_table_id,
          const ObCreatePartitionParam& arg, const bool in_slog_trans, const bool is_replay, const uint64_t log_id,
          ObTablesHandle& sstables_handle));
  MOCK_METHOD2(replay_pg_partition, int(const common::ObPartitionKey& pkey, const uint64_t log_id));
  MOCK_METHOD0(get_pg_storage, ObPGStorage&());
  bool is_pg() const
  {
    return false;
  }
  MOCK_METHOD2(get_all_pg_partition_keys, int(ObPartitionArray& pkeys, const bool include_trans_table));
  MOCK_METHOD5(
      submit_add_partition_to_pg_log, int(const obrpc::ObCreatePartitionArg& arg, ObPartitionService* ps,
                                          uint64_t& log_id, int64_t& log_ts, ObAddPartitionToPGLogCb*& out_cb));
  MOCK_METHOD1(submit_remove_partition_from_pg_log, int(const ObPartitionKey& pkey));
  MOCK_METHOD4(remove_partition_from_pg,
      int(const bool for_replay, const ObPartitionKey& pkey, const bool write_slog_trans, const uint64_t log_id));
  MOCK_METHOD7(submit_partition_schema_change_log,
      int(const common::ObPartitionKey& pkey, const int64_t schema_version, const uint64_t index_id,
          ObPartitionService* ps, uint64_t& log_id, int64_t& log_ts, ObSchemaChangeClogCb*& out_cb));
  MOCK_METHOD5(add_sstable_for_merge,
      int(const ObPartitionKey& pkey, storage::ObSSTable* sstable, const int64_t max_kept_major_version_number,
          ObIPartitionReport& report, ObSSTable* complement_minor_sstable));
  MOCK_METHOD1(check_replica_ready_for_bounded_staleness_read, int(const int64_t snapshot_version));
  MOCK_METHOD1(feedback_scan_access_stat, int(const ObTableScanParam& param));

  MOCK_METHOD0(pause, int());
  MOCK_METHOD0(stop, int());
  MOCK_METHOD0(offline, int());
  MOCK_METHOD3(create_sstables, int(const common::ObIArray<storage::ObPGCreateSSTableParam>& create_sstable_params,
                                    ObTablesHandle& tables_handle, const bool in_slog_trans));
  MOCK_METHOD3(create_sstable,
      int(const storage::ObPGCreateSSTableParam& param, ObTableHandle& table_handle, const bool in_slog_trans));
  MOCK_METHOD2(get_checkpoint_info, int(common::ObArenaAllocator& allocator, ObPGCheckpointInfo& pg_checkpoint_info));
  MOCK_METHOD2(acquire_sstable, int(const ObITable::TableKey& table_key, ObTableHandle& table_handle));
  MOCK_METHOD1(set_storage_file, int(ObStorageFileHandle& file));
  MOCK_METHOD1(set_meta_block_list, int(const common::ObIArray<blocksstable::MacroBlockId>& meta_block_list));
  MOCK_METHOD3(schema_drop, int(const bool, const uint64_t, const bool));
  MOCK_METHOD1(check_source_partition_status, int(bool& can_offline));
  MOCK_CONST_METHOD0(need_replay_redo, bool());
  MOCK_METHOD0(try_clear_split_info, int());
  MOCK_METHOD1(check_complete, int(bool& is_complete));
  MOCK_METHOD5(
      try_update_clog_member_list, int(const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
                                       const ObMemberList& mlist, const common::ObProposalID& ms_proposal_id));
  MOCK_METHOD1(physical_flashback, int(const int64_t flashback_scn));
  MOCK_METHOD1(check_can_physical_flashback, int(const int64_t flashback_scn));
  MOCK_METHOD2(register_txs_change_leader, int(const common::ObAddr& server, ObTsWindows& changing_leader_windows));
  MOCK_METHOD4(try_update_clog_member_list,
      int(const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num, const ObMemberList& mlist));
  MOCK_METHOD3(
      check_physical_flashback_succ, int(const obrpc::ObCheckPhysicalFlashbackArg& arg, const int64_t max_version,
                                         obrpc::ObPhysicalFlashbackResultArg& result));
  MOCK_METHOD1(save_split_info, int(const ObPartitionSplitInfo& split_info));
  MOCK_METHOD1(save_split_state, int(const int64_t split_state));
  MOCK_METHOD3(
      shutdown, int(const int64_t snapshot_version, const uint64_t replay_log_id, const int64_t schema_version));
  MOCK_METHOD0(get_partition_loop_worker, ObPartitionLoopWorker*());
  MOCK_METHOD2(push_reference_tables,
      int(const common::ObIArray<common::ObPartitionKey>& dest_array, const int64_t split_version));
  MOCK_CONST_METHOD1(get_meta_block_list, int(common::ObIArray<blocksstable::MacroBlockId>& meta_block_list));
  MOCK_METHOD1(get_all_tables, int(ObTablesHandle& tables_handle));
  MOCK_METHOD2(recycle_unused_sstables, int(const int64_t max_recycle_cnt, int64_t& recycled_cnt));
  MOCK_METHOD1(check_can_free, int(bool& can_free));
  MOCK_METHOD0(clear, void());
  MOCK_METHOD1(get_merge_log_ts, int(int64_t& merge_log_ts));
  MOCK_METHOD2(check_ready_for_split, int(const ObPartitionArray& src_pkeys, bool& is_ready));
  MOCK_METHOD1(clear_trans_after_restore_log, int(const uint64_t last_restore_log_id));
  MOCK_METHOD0(reset_for_replay, int());
  MOCK_METHOD2(inc_pending_batch_commit_count, int(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts));
  MOCK_METHOD2(inc_pending_elr_count, int(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts));
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_MOCK_OB_PARTITION_H_
