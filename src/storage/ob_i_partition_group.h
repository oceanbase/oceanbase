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

#ifndef OCEANBASE_STORAGE_OB_I_PARTITION_GROUP_
#define OCEANBASE_STORAGE_OB_I_PARTITION_GROUP_

#include "lib/lock/ob_tc_ref.h"
#include "lib/queue/ob_link.h"
#include "common/ob_role.h"
#include "common/ob_range.h"
#include "common/ob_partition_key.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_build_index_struct.h"
#include "share/partition_table/ob_partition_info.h"
#include "storage/replayengine/ob_i_log_replay_engine.h"
#include "storage/transaction/ob_trans_define.h"
#include "storage/ob_saved_storage_info.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_storage_log_type.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/ob_partition_split.h"
#include "storage/ob_pg_index.h"
#include "storage/ob_pg_partition.h"
#include "storage/blocksstable/ob_store_file_system.h"

namespace oceanbase {
namespace common {
class ObTableStat;
}
namespace clog {
class ObIPartitionLogService;
class ObISubmitLogCb;
struct ObPGLogArchiveStatus;
}  // namespace clog
namespace share {
class ObSplitPartitionPair;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share

namespace blocksstable {
class ObStorageFileHandle;
}

namespace memtable {
class ObIMemtable;
}

namespace transaction {
class ObTransService;
class ObTransSplitInfo;
}  // namespace transaction
namespace obrpc {
class ObCreatePartitionArg;
}

namespace storage {
class ObCreatePartitionParam;
class ObPGStorage;
class ObPGCreateSSTableParam;
class ObAddPartitionToPGLogCb;
class ObSchemaChangeClogCb;
class ObPGCheckpointInfo;
class ObFlashBackPartitionCb;
enum ObPartitionState {
  INIT = 0,
  F_WORKING,      // 1
  F_MINOR,        // 2
  L_TAKEOVER,     // 3
  L_TAKEOVERED,   // 4
  L_CANCELED,     // 5
  L_WORKING,      // 6
  L_FROZEN,       // 7
  L_MINOR,        // 8
  L_REVOKE,       // 9
  OFFLINING,      // 10
  OFFLINE,        // 11
  REMOVE,         // 12
  INVALID_STATE,  // 13
};

inline bool is_leader_state(const ObPartitionState& state)
{
  return (L_TAKEOVER == state || L_TAKEOVERED == state || L_WORKING == state || L_FROZEN == state || L_MINOR == state);
}

inline bool is_leader_working(const ObPartitionState& state)
{
  return (L_WORKING == state);
}

inline bool is_follower_state(const ObPartitionState& state)
{
  return (F_WORKING == state || L_REVOKE == state || F_MINOR == state);
}

inline bool is_working_state(const ObPartitionState& state)
{
  return (state >= F_WORKING && state <= L_REVOKE);
}

inline bool is_d_replica(
    const common::ObReplicaType& replica_type, const ObReplicaProperty& replica_property, const ObPartitionState& state)
{
  return 0 == replica_property.get_memstore_percent() && is_follower_state(state) &&
         ObReplicaTypeCheck::is_replica_with_ssstore(replica_type);
}

enum ObPartitionReplicaState {
  OB_NOT_EXIST_REPLICA = 0,
  OB_PERMANENT_OFFLINE_REPLICA = 1,
  OB_NORMAL_REPLICA = 2,
  OB_RESTORE_REPLICA = 3,
  OB_UNKNOWN_REPLICA,
};

inline const char* partition_replica_state_to_str(const ObPartitionReplicaState& state)
{
  const char* str = "OB_UNKOWN_REPLICA";
  switch (state) {
    case OB_NOT_EXIST_REPLICA:
      str = "OB_NOT_EXIST_REPLICA";
      break;
    case OB_PERMANENT_OFFLINE_REPLICA:
      str = "OB_PERMANENT_OFFLINE_REPLICA";
      break;
    case OB_NORMAL_REPLICA:
      str = "OB_NORMAL_REPLICA";
      break;
    case OB_RESTORE_REPLICA:
      str = "OB_RESTORE_REPLICA";
      break;
    default:
      str = "OB_UNKOWN_REPLICA";
      break;
  }
  return str;
}

class ObIPartitionComponentFactory;
class ObPartitionStorage;
class ObPartitionService;
class ObReplayStatus;
class ObOfflinePartitionCb;
class ObFreezeLogCb;
struct ObFrozenStatus;
class ObPartitionSplitWorker;
class ObPartitionLoopWorker;
class ObSplitPartitionStateLogEntry;
class ObSplitPartitionInfoLogEntry;
class ObPGPartitionGuard;

class ObIPartitionGroup : public common::ObLink {
public:
  ObIPartitionGroup() : ref_cnt_(0)
  {}
  virtual ~ObIPartitionGroup()
  {}
  inline void inc_ref()
  {
    ATOMIC_INC(&ref_cnt_);
  }
  inline int32_t dec_ref()
  {
    return ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */);
  }
  inline int32_t get_ref()
  {
    return ATOMIC_LOAD(&ref_cnt_);
  }

  inline int64_t get_file_id() const
  {
    const blocksstable::ObStorageFile* file = get_storage_file();
    return nullptr == file ? OB_INVALID_DATA_FILE_ID : file->get_file_id();
  }

  virtual int init(const common::ObPartitionKey& key, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs,
      replayengine::ObILogReplayEngine* rp_eg, ObPartitionService* ps,
      ObPartitionGroupIndex* pg_index = nullptr /*assign from replay process*/,
      ObPGPartitionMap* pg_partition_map = nullptr /*assign from replay process*/) = 0;
  virtual void destroy() = 0;
  virtual void clear() = 0;
  virtual int remove_election_from_mgr() = 0;

  // get partition log service
  virtual const clog::ObIPartitionLogService* get_log_service() const = 0;
  virtual clog::ObIPartitionLogService* get_log_service() = 0;

  virtual transaction::ObTransService* get_trans_service() = 0;
  virtual ObPartitionService* get_partition_service() = 0;
  virtual ObPartitionGroupIndex* get_pg_index() = 0;
  virtual ObPGPartitionMap* get_pg_partition_map() = 0;

  // get partition storage
  virtual ObPGStorage& get_pg_storage() = 0;
  virtual const common::ObPartitionKey& get_partition_key() const = 0;
  virtual int get_pg_partition(const common::ObPartitionKey& pkey, ObPGPartitionGuard& guard) = 0;

  virtual int table_scan(ObTableScanParam& param, common::ObNewRowIterator*& result) = 0;
  virtual int table_scan(ObTableScanParam& param, common::ObNewIterIterator*& result) = 0;
  virtual int join_mv_scan(ObTableScanParam& left_param, ObTableScanParam& right_param,
      ObIPartitionGroup& right_partition, common::ObNewRowIterator*& result) = 0;
  virtual int delete_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows) = 0;
  virtual int delete_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row) = 0;
  virtual int put_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows) = 0;
  virtual int insert_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows) = 0;
  virtual int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row) = 0;
  virtual int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& duplicated_column_ids,
      const common::ObNewRow& row, const ObInsertFlag flag, int64_t& affected_rows,
      common::ObNewRowIterator*& duplicated_rows) = 0;
  // Used to check whether the specified row conflicts in the storage
  // @in_column_ids is used to specify the column of the row to be checked.
  // Rowkey must be first and local unique index must be included.
  // @out_column_ids is used to specify the column to be returned of the conflicting row.
  // @check_row_iter is the iterator of row to be checked.
  // @dup_row_iters is the iterator array of each row conflict. The number of iterators equals
  // to the number of rows to be checked
  virtual int fetch_conflict_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& in_column_ids, const common::ObIArray<uint64_t>& out_column_ids,
      common::ObNewRowIterator& check_row_iter, common::ObIArray<common::ObNewRowIterator*>& dup_row_iters) = 0;
  virtual int revert_insert_iter(const common::ObPartitionKey& pkey, common::ObNewRowIterator* iter) = 0;
  virtual int update_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& updated_column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows) = 0;
  virtual int update_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& updated_column_ids,
      const common::ObNewRow& old_row, const common::ObNewRow& new_row) = 0;
  virtual int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      common::ObNewRowIterator* row_iter, const ObLockFlag lock_flag, int64_t& affected_rows) = 0;
  virtual int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      const common::ObNewRow& row, ObLockFlag lock_flag) = 0;

  virtual int set_valid() = 0;
  virtual void set_invalid()
  {}
  virtual bool is_valid() const = 0;
  virtual bool is_pg() const = 0;

  // leader or follower
  virtual int get_role(common::ObRole& role) const = 0;
  virtual int get_role_for_partition_table(common::ObRole& role) const = 0;
  virtual int get_role_unsafe(common::ObRole& role) const = 0;
  virtual int get_leader_curr_member_list(common::ObMemberList& member_list) const = 0;
  virtual int get_leader(common::ObAddr& leader) const = 0;
  virtual int get_curr_member_list(common::ObMemberList& member_list) const = 0;
  virtual int get_curr_member_list_for_report(common::ObMemberList& member_list) const = 0;
  virtual int get_curr_leader_and_memberlist(common::ObAddr& leader, common::ObRole& role,
      common::ObMemberList& curr_member_list, common::ObChildReplicaList& children_list) const = 0;
  virtual int get_dst_leader_candidate(common::ObMemberList& member_list) const = 0;
  virtual int get_log_archive_status(clog::ObPGLogArchiveStatus& status) const = 0;
  virtual int change_leader(const common::ObAddr& leader) = 0;

  virtual int leader_takeover() = 0;
  virtual int leader_revoke() = 0;
  virtual int leader_active() = 0;

  virtual ObReplayStatus* get_replay_status() = 0;

  // write ssstore objects @version tree to data file , used by write_check_point
  virtual int serialize(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size) = 0;
  // read ssstore objects from data file to construct partition storage's version tree.
  virtual int deserialize(const char* buf, const int64_t buf_len, int64_t& pos) = 0;

  // get create timestamp
  virtual int get_create_ts(int64_t& create_ts) = 0;

  // role change status
  virtual void replay_status_revoke() = 0;

  virtual int report_clog_history_online() = 0;
  virtual int report_clog_history_offline() = 0;

  virtual int pause() = 0;
  virtual int stop() = 0;
  virtual int offline() = 0;
  virtual int schema_drop(const bool for_replay, const uint64_t log_id, const bool is_physical_drop) = 0;

  // replica state
  virtual int get_replica_state(ObPartitionReplicaState& state)
  {
    state = OB_NORMAL_REPLICA;
    return common::OB_SUCCESS;
  }

  virtual bool is_removed() const = 0;
  virtual int check_is_in_member_list(bool& is_in_member_list) const = 0;
  virtual int offline_itself(const bool is_physical_drop) = 0;
  virtual int submit_add_partition_to_pg_log(const obrpc::ObCreatePartitionArg& arg, ObPartitionService* ps,
      uint64_t& log_id, int64_t& log_timestamp, ObAddPartitionToPGLogCb*& out_cb) = 0;
  virtual int submit_remove_partition_from_pg_log(const ObPartitionKey& pkey) = 0;
  virtual int submit_partition_schema_change_log(const common::ObPartitionKey& pkey, const int64_t schema_version,
      const uint64_t index_id, ObPartitionService* ps, uint64_t& log_id, int64_t& log_ts,
      ObSchemaChangeClogCb*& out_cb) = 0;
  virtual int remove_partition_from_pg(
      const bool for_replay, const ObPartitionKey& pkey, const bool write_slog_trans, const uint64_t log_id) = 0;

  virtual int replay_partition_meta_log(
      const ObStorageLogType log_type, const int64_t log_id, const char* buf, const int64_t size) = 0;

  virtual int set_wait_split() = 0;
  virtual int save_split_state(const bool write_slog) = 0;
  virtual int restore_split_state(const int state) = 0;
  virtual int restore_split_info(const ObPartitionSplitInfo& split_info) = 0;
  virtual int replay_split_source_log(
      const ObPartitionSplitSourceLog& log, const uint64_t log_id, const int64_t log_ts) = 0;
  virtual int replay_split_dest_log(const ObPartitionSplitDestLog& log) = 0;
  virtual int sync_split_source_log_success(const int64_t log_id, const int64_t log_ts) = 0;
  virtual int sync_split_dest_log_success() = 0;
  virtual int prepare_splitting(
      const ObPartitionSplitInfo& split_info, const common::ObMemberList& mlist, const common::ObAddr& leader) = 0;
  virtual int split_source_partition(const int64_t schema_version, const share::ObSplitPartitionPair& info,
      enum share::ObSplitProgress& partition_progress) = 0;
  virtual int split_dest_partition(const ObPartitionSplitInfo& split_info, enum share::ObSplitProgress& progress) = 0;
  virtual int push_reference_tables(
      const common::ObIArray<common::ObPartitionKey>& dest_array, const int64_t split_version) = 0;
  virtual int replay_split_state_slog(const ObSplitPartitionStateLogEntry& log_entry) = 0;
  virtual int replay_split_info_slog(const ObSplitPartitionInfoLogEntry& log_entry) = 0;
  virtual int set_dest_partition_split_progress(
      const int64_t schema_version, const common::ObPartitionKey& pkey, const int progress) = 0;

  virtual int get_all_table_ids(const ObPartitionKey& pkey, common::ObIArray<uint64_t>& index_tables) = 0;
  virtual int get_reference_tables(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle) = 0;
  virtual int get_reference_memtables(ObTablesHandle& handle) = 0;
  virtual int set_reference_tables(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle) = 0;
  virtual int set_split_version(const int64_t split_version) = 0;
  virtual int check_can_migrate(bool& can_migrate) = 0;
  virtual bool is_splitting() const = 0;
  virtual bool is_split_source_partition() const = 0;
  virtual bool is_in_dest_split() const = 0;
  virtual bool is_dest_logical_split_finish() const = 0;
  virtual int check_split_state() const = 0;
  virtual int get_split_progress(const int64_t schema_version, int& progress) = 0;
  virtual int set_split_progress(const common::ObAddr& replica, const int progress) = 0;
  virtual int block_partition_split_by_mc() = 0;
  virtual int unblock_partition_split_by_mc() = 0;

  virtual int64_t get_freeze_snapshot_ts() const = 0;
  virtual ObPartitionState get_partition_state() const = 0;
  virtual int switch_partition_state(const ObPartitionState state) = 0;
  virtual int try_switch_partition_state(const ObPartitionState state) = 0;
  virtual int set_replica_type(const common::ObReplicaType& replica_type, const bool write_redo_log) = 0;
  virtual common::ObReplicaType get_replica_type() const = 0;
  virtual common::ObReplicaProperty get_replica_property() const = 0;
  virtual int get_latest_schema_version(share::schema::ObMultiVersionSchemaService* schema_service,
      const common::ObPartitionKey& pkey, int64_t& latest_schema_version) = 0;
  virtual int check_schema_version(share::schema::ObMultiVersionSchemaService* schema_service)
  {
    UNUSED(schema_service);
    return common::OB_SUCCESS;
  }
  virtual int set_base_schema_version(int64_t base_schema_version)
  {
    UNUSED(base_schema_version);
    return common::OB_SUCCESS;
  }

  virtual int do_warm_up_request(const ObIWarmUpRequest* request) = 0;
  virtual int check_can_do_merge(bool& can_merge, bool& need_merge) = 0;
  virtual int64_t get_cur_min_log_service_ts() = 0;
  virtual int64_t get_cur_min_trans_service_ts() = 0;
  virtual int64_t get_cur_min_replay_engine_ts() = 0;
  virtual void set_migrating_flag(const bool flag) = 0;
  virtual bool get_migrating_flag() const = 0;

  virtual int get_weak_read_timestamp(int64_t& timestamp)
  {
    UNUSED(timestamp);
    return common::OB_SUCCESS;
  }
  virtual int generate_weak_read_timestamp(const int64_t max_stale_time, int64_t& timestamp)
  {
    UNUSED(max_stale_time);
    UNUSED(timestamp);
    return common::OB_SUCCESS;
  }
  // Check if weak read is enabled.
  virtual bool can_weak_read() const
  {
    return true;
  }
  virtual int do_partition_loop_work()
  {
    return common::OB_SUCCESS;
  }
  virtual int need_minor_freeze(const uint64_t& log_id, bool& need_freeze)
  {
    UNUSED(log_id);
    need_freeze = false;
    return common::OB_SUCCESS;
  }
  virtual int set_emergency_release()
  {
    return common::OB_ERR_SYS;
  }

  // The interface for partition freeze. The freeze_snapshot point will
  // be decided internal.
  virtual int freeze(const bool emergency, const bool force, int64_t& freeze_snapshot) = 0;
  // Mark dirty of transactions on the terminated memtable
  virtual int mark_dirty_trans(bool& cleared) = 0;

  // WARNING: sub-second level
  virtual int get_curr_storage_info_for_migrate(const bool use_slave_safe_read_ts,
      const common::ObReplicaType replica_type, const int64_t src_cluster_id, ObSavedStorageInfoV2& info) = 0;
  virtual int check_is_from_restore(bool& is_from_restore) const = 0;
  virtual int get_all_saved_info(ObSavedStorageInfoV2& info) const = 0;
  virtual int get_saved_clog_info(common::ObBaseStorageInfo& clog_info) const = 0;
  virtual int get_saved_data_info(ObDataStorageInfo& data_info) const = 0;
  virtual int get_saved_last_log_info(uint64_t& log_id, int64_t& submit_timestamp) const = 0;

  virtual int append_local_sort_data(const common::ObPartitionKey& pkey,
      const share::ObBuildIndexAppendLocalDataParam& param, common::ObNewRowIterator& iter) = 0;
  virtual int append_sstable(const common::ObPartitionKey& pkey, const share::ObBuildIndexAppendSSTableParam& param,
      common::ObNewRowIterator& iter) = 0;
  virtual const ObPartitionSplitInfo& get_split_info() = 0;
  virtual int check_cur_partition_split(bool& is_split_partition) = 0;
  virtual int get_trans_split_info(transaction::ObTransSplitInfo& split_info) = 0;
  virtual int check_single_replica_major_sstable_exist(const ObPartitionKey& pkey, const uint64_t index_table_id) = 0;
  virtual int get_max_decided_trans_version(int64_t& max_decided_trans_version) const = 0;

  virtual int create_memtable(
      const bool in_slog_trans = false, const bool is_replay = false, const bool ignore_memstore_percent = false) = 0;
  virtual int get_table_stat(const common::ObPartitionKey& pkey, common::ObTableStat& stat) = 0;
  virtual int update_last_checkpoint(const int64_t checkpoint) = 0;
  virtual int set_replay_checkpoint(const int64_t checkpoint) = 0;
  virtual int get_replay_checkpoint(int64_t& checkpoint) = 0;
  virtual int allow_gc(bool& allow_gc) = 0;
  virtual int gc_check_valid_member(const bool is_valid, const int64_t gc_seq, bool& need_gc) = 0;
  virtual bool check_pg_partition_offline(const ObPartitionKey& pkey) = 0;
  virtual int check_offline_log_archived(
      const ObPartitionKey& pkey, const int64_t incarnation, const int64_t archive_round, bool& has_archived) const = 0;
  virtual int get_leader_epoch(int64_t& leader_epoch) const = 0;
  virtual int get_replica_status(share::ObReplicaStatus& status) = 0;
  virtual int is_replica_need_gc(bool& is_offline) = 0;
  virtual int set_storage_info(const ObSavedStorageInfoV2& info) = 0;
  virtual int fill_pg_partition_replica(
      const ObPartitionKey& pkey, share::ObReplicaStatus& replica_status, ObReportStatus& report_status) = 0;
  virtual int fill_replica(share::ObPartitionReplica& replica) = 0;
  virtual int get_merge_priority_info(memtable::ObMergePriorityInfo& info) const = 0;
  virtual int64_t get_gc_schema_drop_ts() = 0;
  virtual void set_need_rebuild() = 0;
  virtual bool is_need_rebuild() const = 0;
  virtual void set_need_standby_restore() = 0;
  virtual bool is_need_standby_restore() const = 0;
  virtual void reset_migrate_retry_flag() = 0;
  virtual void set_need_gc() = 0;
  virtual bool is_need_gc() const = 0;
  virtual uint64_t get_offline_log_id() const = 0;
  virtual int set_offline_log_id(const uint64_t log_id) = 0;
  virtual int retire_warmup_store(const bool is_disk_full) = 0;
  virtual int enable_write_log(const bool is_replay_old) = 0;
  virtual uint64_t get_min_replayed_log_id() = 0;  // Get the minimum log id that has been replayed continuously.
  virtual void get_min_replayed_log(uint64_t& min_replay_log_id, int64_t& min_replay_log_ts) = 0;
  virtual int get_min_replayed_log_with_keepalive(uint64_t& min_replay_log_id, int64_t& min_replay_log_ts) = 0;
  virtual int create_partition_group(const ObCreatePGParam& param) = 0;
  virtual int create_pg_partition(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
      const uint64_t data_table_id, const obrpc::ObCreatePartitionArg& arg, const bool in_slog_trans,
      const bool is_replay, const uint64_t log_id, ObTablesHandle& sstables_handle) = 0;
  virtual int create_pg_partition(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
      const uint64_t data_table_id, const ObCreatePartitionParam& arg, const bool in_slog_trans, const bool is_replay,
      const uint64_t log_id, ObTablesHandle& sstables_handle) = 0;
  virtual int replay_pg_partition(const common::ObPartitionKey& pkey, const uint64_t log_id) = 0;
  virtual int get_all_pg_partition_keys(ObPartitionArray& pkeys, const bool include_trans_table = false) = 0;
  virtual int add_sstable_for_merge(const ObPartitionKey& pkey, storage::ObSSTable* sstable,
      const int64_t max_kept_major_version_number, ObIPartitionReport& report,
      ObSSTable* complement_minor_sstable = nullptr) = 0;
  virtual int check_replica_ready_for_bounded_staleness_read(const int64_t snapshot_version) = 0;
  virtual bool is_replica_using_remote_memstore() const = 0;
  virtual bool is_share_major_in_zone() const = 0;
  virtual int feedback_scan_access_stat(const ObTableScanParam& param) = 0;
  virtual blocksstable::ObStorageFile* get_storage_file() = 0;
  virtual const blocksstable::ObStorageFile* get_storage_file() const = 0;
  virtual blocksstable::ObStorageFileHandle& get_storage_file_handle() = 0;
  virtual int create_sstables(const common::ObIArray<storage::ObPGCreateSSTableParam>& create_sstable_params,
      ObTablesHandle& tables_handle, const bool in_slog_trans = false) = 0;
  virtual int create_sstable(
      const storage::ObPGCreateSSTableParam& param, ObTableHandle& table_handle, const bool in_slog_trans = false) = 0;
  virtual int get_checkpoint_info(common::ObArenaAllocator& allocator, ObPGCheckpointInfo& pg_checkpoint_info) = 0;
  virtual int acquire_sstable(const ObITable::TableKey& table_key, ObTableHandle& table_handle) = 0;
  virtual int set_storage_file(blocksstable::ObStorageFileHandle& file_handle) = 0;
  virtual bool need_replay_redo() const = 0;
  virtual int check_dirty_txn(
      const int64_t min_log_ts, const int64_t max_log_ts, int64_t& freeze_ts, bool& is_dirty) = 0;
  virtual int try_clear_split_info() = 0;
  virtual int check_complete(bool& is_complete) = 0;
  virtual int try_update_clog_member_list(const uint64_t ms_log_id, const int64_t mc_timestamp,
      const int64_t replica_num, const ObMemberList& mlist, const common::ObProposalID& ms_proposal_id) = 0;
  virtual int check_physical_flashback_succ(const obrpc::ObCheckPhysicalFlashbackArg& arg, const int64_t max_version,
      obrpc::ObPhysicalFlashbackResultArg& result) = 0;
  virtual int save_split_info(const ObPartitionSplitInfo& split_info) = 0;
  virtual int save_split_state(const int64_t split_state) = 0;
  virtual int shutdown(const int64_t snapshot_version, const uint64_t replay_log_id, const int64_t schema_version) = 0;
  virtual ObPartitionLoopWorker* get_partition_loop_worker() = 0;
  virtual int physical_flashback(const int64_t flashback_scn) = 0;
  virtual int set_meta_block_list(const common::ObIArray<blocksstable::MacroBlockId>& meta_block_list) = 0;
  virtual int get_meta_block_list(common::ObIArray<blocksstable::MacroBlockId>& meta_block_list) const = 0;
  virtual int get_all_tables(ObTablesHandle& tables_handle) = 0;
  virtual int recycle_unused_sstables(const int64_t max_recycle_cnt, int64_t& recycled_cnt) = 0;
  virtual int check_can_free(bool& can_free) = 0;
  virtual int get_merge_log_ts(int64_t& freeze_ts) = 0;
  virtual int get_table_store_cnt(int64_t& table_cnt) const = 0;
  virtual int check_can_physical_flashback(const int64_t flashback_scn) = 0;
  virtual int register_txs_change_leader(const common::ObAddr& server, ObTsWindows& changing_leader_windows) = 0;
  virtual int check_physical_split(bool& finished) = 0;
  virtual int clear_trans_after_restore_log(const uint64_t last_restore_log_id) = 0;
  virtual int reset_for_replay() = 0;
  virtual int inc_pending_batch_commit_count(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts) = 0;
  virtual int inc_pending_elr_count(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts) = 0;
  TO_STRING_KV(K_(ref_cnt));

protected:
  // resource usage statistics
  int32_t ref_cnt_;
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_I_PARTITION_GROUP_
