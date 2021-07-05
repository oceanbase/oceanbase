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

#ifndef OCEANBASE_STORAGE_OB_PG_STORAGE
#define OCEANBASE_STORAGE_OB_PG_STORAGE

#include "common/ob_partition_key.h"
#include "common/storage/ob_freeze_define.h"
#include "share/ob_freeze_info_proxy.h"
#include "lib/container/ob_mask_set2.h"
#include "lib/hash/ob_dchash.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/metrics/ob_meter.h"
#include "ob_warm_up_request.h"
#include "share/ob_partition_modify.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_freeze_record.h"
#include "storage/ob_partition_schema_recorder.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_replay_status.h"
#include "storage/ob_partition_loop_worker.h"
#include "storage/ob_pg_partition.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_pg_memtable_mgr.h"
#include "storage/ob_pg_sstable_mgr.h"
#include "storage/ob_tenant_file_struct.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/ob_reserved_data_mgr.h"
#include "storage/compaction/ob_partition_merge_util.h"

namespace oceanbase {
namespace share {
class ObSplitPartitionPair;
}
namespace obrpc {
class ObCreatePartitionArg;
}

namespace storage {
class ObIPartitionComponentFactory;
class ObPartitionMigrateCtx;

struct ObPGCreateSSTableParam final {
public:
  ObPGCreateSSTableParam()
      : with_partition_param_(nullptr),
        with_table_param_(nullptr),
        table_key_(nullptr),
        meta_(nullptr),
        data_blocks_(),
        lob_blocks_(),
        bloomfilter_block_(nullptr),
        sstable_merge_info_(nullptr),
        in_slog_trans_(false)
  {}
  ~ObPGCreateSSTableParam() = default;
  bool is_valid() const;
  int assign(const ObPGCreateSSTableParam& other);
  TO_STRING_KV(KP_(with_partition_param), KP_(with_table_param), KP_(table_key), KP_(meta), K_(data_blocks),
      K_(lob_blocks), KP_(bloomfilter_block), KP_(sstable_merge_info));
  ObCreateSSTableParamWithPartition* with_partition_param_;
  ObCreateSSTableParamWithTable* with_table_param_;
  ObITable::TableKey* table_key_;
  blocksstable::ObSSTableBaseMeta* meta_;
  common::ObArray<blocksstable::ObMacroBlocksWriteCtx*> data_blocks_;
  common::ObArray<blocksstable::ObMacroBlocksWriteCtx*> lob_blocks_;
  blocksstable::ObMacroBlocksWriteCtx* bloomfilter_block_;
  const ObSSTableMergeInfo* sstable_merge_info_;
  bool in_slog_trans_;
};

class ObPGStorage {
  friend class ObPGPartitionArrayGuard;

public:
  ObPGStorage();
  ~ObPGStorage();
  int init(const ObPartitionKey& key, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs,
      clog::ObIPartitionLogService* pls, ObIPartitionGroup* pg);
  void destroy();
  void clear();
  int serialize(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size);
  int deserialize(
      const char* buf, const int64_t buf_len, int64_t& pos, int64_t& state, ObPartitionSplitInfo& split_info);

  // DML
  int table_scan(ObTableScanParam& param, ObNewRowIterator*& result);
  int table_scan(ObTableScanParam& param, ObNewIterIterator*& result);

  int join_mv_scan(ObTableScanParam& left_param, ObTableScanParam& right_param, ObIPartitionGroup& right_partition,
      common::ObNewRowIterator*& result);

  int delete_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      ObNewRowIterator* row_iter, int64_t& affected_rows);

  int delete_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      const ObNewRow& row);

  int put_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      ObNewRowIterator* row_iter, int64_t& affected_rows);

  int insert_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      ObNewRowIterator* row_iter, int64_t& affected_rows);

  int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& column_ids,
      const common::ObNewRow& row);

  int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row, const ObInsertFlag flag,
      int64_t& affected_rows, common::ObNewRowIterator*& duplicated_rows);

  int fetch_conflict_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const ObIArray<uint64_t>& in_column_ids, const ObIArray<uint64_t>& out_column_ids,
      ObNewRowIterator& check_row_iter, ObIArray<ObNewRowIterator*>& dup_row_iters);

  int revert_insert_iter(const common::ObPartitionKey& pkey, ObNewRowIterator* iter);

  int update_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      const ObIArray<uint64_t>& updated_column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows);

  int update_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      const ObIArray<uint64_t>& updated_column_ids, const ObNewRow& old_row, const ObNewRow& new_row);

  int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      ObNewRowIterator* row_iter, const ObLockFlag lock_flag, int64_t& affected_rows);

  int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      const ObNewRow& row, const ObLockFlag lock_flag);

  // memstore
  int create_memtable(const bool in_slog_trans, const bool is_replay, const bool ignore_memstore_percent = false);
  int check_and_new_active_memstore(
      const ObBaseStorageInfo& clog_info, const bool force, bool& changed, int64_t& active_protection_clock);
  bool has_active_memtable();
  bool has_memstore();
  bool is_replica_with_remote_memstore() const;
  bool is_share_major_in_zone() const;
  int check_pg_partition_offline(const ObPartitionKey& pkey, bool& offline);
  int new_active_memstore(int64_t& active_protection_clock);
  int effect_new_active_memstore(const ObSavedStorageInfoV2& info, const bool emergency);
  int complete_active_memstore(const ObSavedStorageInfoV2& info);
  int clean_new_active_memstore();
  int get_active_memtable_base_version(int64_t& base_version);
  int get_active_memtable_start_log_ts(int64_t& base_version);
  int check_active_mt_hotspot_row_exist(bool& need_fast_freeze, const int64_t fast_freeze_interval);
  int64_t get_last_freeze_ts() const
  {
    return last_freeze_ts_;
  }
  void set_freeze_ts(const int64_t freeze_ts)
  {
    last_freeze_ts_ = freeze_ts;
  }
  int get_active_protection_clock(int64_t& active_protection_clock);
  int remove_pg_partition_from_pg(const ObPartitionKey& pkey, const bool write_slog_trans, const uint64_t log_id);
  int clear_non_reused_stores(const ObPartitionKey& pkey, bool& cleared_memstore);
  int set_emergency_release();
  int get_active_memtable(ObTableHandle& handle);

  // PG, pg partition
  int get_pg_partition(const common::ObPartitionKey& pkey, ObPGPartitionGuard& guard);
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }

  int replay(const ObStoreCtx& ctx, const char* data, const int64_t data_len, bool& replayed);
  int replay_partition_schema_version_change_log(const int64_t schema_version);
  int replay_pg_partition(const common::ObPartitionKey& pkey, const uint64_t log_id);
  int check_can_create_pg_partition(bool& can_create);
  int create_pg_partition(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
      const uint64_t data_table_id, const obrpc::ObCreatePartitionArg& arg, const bool in_slog_trans,
      const bool is_replay, const uint64_t log_id, ObTablesHandle& sstables_handle);
  int create_pg_partition(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
      const uint64_t data_table_id, const ObCreatePartitionParam& create_param, const bool in_slog_trans,
      const bool is_replay, const uint64_t log_id, ObTablesHandle& sstables_handle);
  int get_weak_read_timestamp(int64_t& timestamp);

  int set_pg_removed();
  int write_update_pg_meta_trans(const ObPartitionGroupMeta& meta, const LogCommand& cmd);
  int get_pg_meta(ObPartitionGroupMeta& pg_meta);
  int get_pg_create_ts(int64_t& create_ts);
  int get_major_version(ObVersion& major_version);

  // minor freeze
  int check_need_minor_freeze(const uint64_t log_id, bool& need_freeze);
  int check_log_or_data_changed(const ObBaseStorageInfo& clog_info, bool& log_changed, bool& need_freeze_data);
  int get_all_saved_info(ObSavedStorageInfoV2& info) const;
  int get_saved_clog_info(common::ObBaseStorageInfo& clog_info) const;
  int get_saved_data_info(ObDataStorageInfo& data_info) const;
  int set_pg_storage_info(const ObSavedStorageInfoV2& info);
  int set_pg_clog_info(const ObBaseStorageInfo& clog_info, const bool replica_with_data);
  // build index
  int append_local_sort_data(
      const ObPartitionKey& pkey, const share::ObBuildIndexAppendLocalDataParam& param, ObNewRowIterator& iter);
  int append_sstable(
      const ObPartitionKey& pkey, const share::ObBuildIndexAppendSSTableParam& param, common::ObNewRowIterator& iter);
  int check_single_replica_major_sstable_exist(const ObPartitionKey& pkey, const uint64_t index_table_id);
  int get_table_stat(const common::ObPartitionKey& pkey, ObTableStat& stat);
  int get_all_table_ids(const ObPartitionKey& pkey, ObIArray<uint64_t>& index_tables);

  // split
  int set_publish_version_after_create(const int64_t publish_version);
  int save_split_state(const int64_t state, const bool write_slog);
  int save_split_info(const ObPartitionSplitInfo& split_info, const bool write_slog);
  int clear_split_info();
  int check_physical_split(bool& finished);
  int get_reference_tables(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle);
  int set_reference_tables(const ObPartitionKey& pkey, const int64_t index_id, ObTablesHandle& handle);

  int get_replica_state(const bool disable_replay_log, ObPartitionReplicaState& state);
  int get_replica_type(common::ObReplicaType& replica_type) const;
  int get_replica_property(common::ObReplicaProperty& replica_property) const;
  int check_can_migrate(bool& can_migrate);
  int set_pg_replica_type(const ObReplicaType& replica_type, const bool write_redo_log);

  int do_warm_up_request(const ObIWarmUpRequest* request);
  int fill_replica(share::ObPartitionReplica& replica);
  int fill_pg_partition_replica(const ObPartitionKey& pkey, ObReportStatus& report_status);
  int get_merge_priority_info(memtable::ObMergePriorityInfo& merge_priority_info);

  // schema log
  int replay_schema_log(const char* buf, const int64_t size, const int64_t log_id);

  int retire_warmup_store(const bool is_disk_full);
  int enable_write_log(const bool is_replay_old);
  int get_all_pg_partition_keys(common::ObPartitionArray& pkeys, const bool include_trans_table = false)
  {
    return get_all_pg_partition_keys_(pkeys, include_trans_table);
  }
  bool is_empty_pg();
  int get_based_schema_version(int64_t& schema_version);
  int check_release_memtable(ObIPartitionReport& report);
  bool is_restore();
  bool is_pg_meta_valid();
  share::ObReplicaRestoreStatus get_restore_status();
  int64_t get_create_frozen_version();
  bool is_restoring_base_data();
  bool is_restoring_standby();
  int16_t get_restore_state() const;
  int get_all_pg_partitions(ObPGPartitionArrayGuard& guard);
  int check_can_replay_add_partition_to_pg_log(
      const common::ObPartitionKey& pkey, const uint64_t log_id, const int64_t log_ts, bool& can_replay);
  int check_can_replay_remove_partition_from_pg_log(
      const common::ObPartitionKey& pkey, const uint64_t log_id, bool& can_replay);
  int try_update_report_status(
      ObIPartitionReport& report, const common::ObVersion& version, bool& is_finished, bool& need_report);

  // migrate
  int get_pg_migrate_status(ObMigrateStatus& migrate_status);
  int get_migrate_status(ObMigrateStatus& migrate_status, int64_t& timestamp);
  int set_pg_migrate_status(const ObMigrateStatus migrate_status, const int64_t timestamp);
  int get_pg_partition_store_meta(const ObPartitionKey& pkey, ObPGPartitionStoreMeta& meta);
  int get_migrate_table_ids(const ObPartitionKey& pkey, ObIArray<uint64_t>& table_ids);
  int get_partition_tables(
      const ObPartitionKey& pkey, const uint64_t table_id, ObTablesHandle& tables_handle, bool& is_ready_for_read);
  int get_partition_gc_tables(const ObPartitionKey& pkey, const uint64_t table_id, ObTablesHandle& gc_tables_handle);
  int update_multi_version_start(const ObPartitionKey& pkey, const int64_t multi_version_start);
  int get_last_all_major_sstable(ObTablesHandle& handle);
  int get_create_schema_version(int64_t& create_schema_verison);
  int get_create_schema_version(const ObPartitionKey& partition_key, int64_t& create_schema_verison);

  int replay_partition_group_meta(const ObPartitionGroupMeta& meta);
  int create_partition_group(const ObCreatePGParam& param);
  int replay_pg_partition_store(ObPGPartitionStoreMeta& meta);
  int replay_pg_partition_meta(ObPGPartitionStoreMeta& meta);
  int add_sstable_for_merge(const ObPartitionKey& pkey, storage::ObSSTable* table,
      const int64_t max_kept_major_version_number, ObSSTable* complement_minor_sstable = nullptr);
  int set_restore_flag(const int16_t restore_flag, const int64_t restore_snapshot_version);
  int set_last_restore_log_id(const int64_t last_restore_log_id);
  int get_restore_replay_info(uint64_t& last_restore_log_id, int64_t& restore_snapshot_version);
  int set_partition_removed(const ObPartitionKey& pkey);
  int get_all_pg_partition_keys_with_lock(common::ObPartitionArray& pkeys, const bool include_trans_table = false);
  int clear_all_memtables();

  // table store modification
  int create_index_table_store(const ObPartitionKey& pkey, const uint64_t table_id, const int64_t schema_version);
  int add_sstable(const ObPartitionKey& pkey, storage::ObSSTable* table, const int64_t max_kept_major_version_number,
      const bool in_slog_trans);
  int halt_prewarm();
  int replay_modify_table_store(const ObModifyTableStoreLogEntry& log_entry);

  int replay_drop_index(const ObPartitionKey& pkey, const uint64_t table_id);
  int try_drop_unneed_index(const int64_t latest_schema_version = common::OB_INVALID_VERSION);
  int get_partition_store_info(const ObPartitionKey& pkey, ObPartitionStoreInfo& info);
  int64_t get_publish_version() const;

  int get_reference_memtables(ObTablesHandle& memtables);
  int set_reference_memtables(const ObTablesHandle& memtables);
  int remove_uncontinues_inc_tables(const ObPartitionKey& pkey, const uint64_t table_id);

  int update_readable_info(const ObPartitionReadableInfo& readable_info);
  const ObPartitionReadableInfo& get_readable_info() const
  {
    return pg_memtable_mgr_.get_readable_info();
  }
  int get_pkey_for_table(const int64_t table_id, ObPartitionKey& pkey);
  int update_split_state_after_merge(int64_t& split_state);
  int remove_all_pg_index();
  int update_split_table_store(
      const common::ObPartitionKey& pkey, int64_t table_id, bool is_major_split, ObTablesHandle& handle);
  int get_partition_access_stat(const ObPartitionKey& pkey, ObPartitionPrefixAccessStat& stat);
  int feedback_scan_access_stat(const ObTableScanParam& param);
  OB_INLINE blocksstable::ObStorageFile* get_storage_file()
  {
    return file_handle_.get_storage_file();
  }
  OB_INLINE blocksstable::ObStorageFileHandle& get_storage_file_handle()
  {
    return file_handle_;
  }
  int set_storage_file(blocksstable::ObStorageFileHandle& file_handle);
  int create_sstables(const common::ObIArray<ObPGCreateSSTableParam>& create_sstable_params, const bool in_slog_trans,
      ObTablesHandle& tables_handle);
  int create_sstable(const ObPGCreateSSTableParam& param, const bool in_slog_trans, ObTableHandle& table_handle);
  int get_checkpoint_info(common::ObArenaAllocator& allocator, ObPGCheckpointInfo& pg_checkpoint_info);
  int replay_add_sstable(ObSSTable& replay_sstable);
  int replay_remove_sstable(const ObITable::TableKey& table_key);
  int acquire_sstable(const ObITable::TableKey& table_key, ObTableHandle& table_handle);
  int recycle_unused_sstables(const int64_t max_recycle_cnt, int64_t& recycled_cnt);
  int check_can_free(bool& can_free);

  int get_min_frozen_memtable_base_version(int64_t& min_base_version);
  int get_first_frozen_memtable(ObTableHandle& handle);
  int64_t get_partition_cnt();
  int check_need_merge_trans_table(
      bool& need_merge, int64_t& merge_log_ts, int64_t& trans_table_seq, int64_t& end_log_ts, int64_t& timestamp);

  int check_complete(bool& is_complete);
  int try_update_member_list(const uint64_t ms_log_id, const int64_t mc_timestamp, const int64_t replica_num,
      const common::ObMemberList& mlist, const common::ObProposalID& ms_proposal_id);
  bool need_create_memtable();
  int get_max_major_sstable_snapshot(int64_t& sstable_ts);
  int get_min_max_major_version(int64_t& min_version, int64_t& max_version);
  int update_upper_trans_version_and_gc_sstable(
      transaction::ObTransService& trans_service, const int64_t frozen_version);
  int restore_mem_trans_table(ObSSTable& trans_sstable);
  int restore_mem_trans_table();
  int get_max_cleanout_log_ts(int64_t& max_cleanout_log_ts);
  int clear_unused_trans_status();
  int physical_flashback(const int64_t flashback_scn);
  int set_meta_block_list(const common::ObIArray<blocksstable::MacroBlockId>& meta_block_list);
  int get_meta_block_list(common::ObIArray<blocksstable::MacroBlockId>& meta_block_list) const;
  int get_latest_frozen_memtable(ObTableHandle& handle);
  int get_all_sstables(ObTablesHandle& handle);
  int get_table_store_cnt(int64_t& table_store_cnt) const;
  int get_trans_table_end_log_ts_and_timestamp(int64_t& end_log_ts, int64_t& timestamp);
  int check_can_physical_flashback(const int64_t flashback_scn);
  int allow_gc(bool& allow_gc);
  int get_min_schema_version(int64_t& min_schema_version);
  int get_latest_table_count(const ObPartitionKey& pkey, const int64_t index_id, int64_t& table_count);
  int create_trans_table_partition_(const bool in_slog_trans, const bool is_replay);
  int get_trans_table_status(ObTransTableStatus& status);
  int batch_replace_store_map(const common::ObIArray<ObPartitionMigrateCtx>& part_ctx_array,
      const int64_t schema_version, const bool is_restore, const int64_t old_trans_table_seq);

  void pause();
  void online();
  bool is_paused();
  int add_trans_sstable(const int64_t old_trans_table_seq, ObSSTable* sstable);
  int64_t get_trans_table_seq();
  int get_partition_migrate_tables(
      const ObPartitionKey& pkey, const uint64_t table_id, ObTablesHandle& tables_handle, bool& is_ready_for_read);
  int check_trans_table_merged(bool& is_merged);
  int update_backup_meta_data(const ObPartitionGroupMeta& meta);
  int replay_add_recovery_point_data(const ObRecoveryPointType point_type, const ObRecoveryPointData& point_data);
  int replay_remove_recovery_point_data(const ObRecoveryPointType point_type, const int64_t snapshot_version);
  ObRecoveryDataMgr& get_recovery_data_mgr()
  {
    return recovery_point_data_mgr_;
  }

  int inc_pending_batch_commit_count(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts);
  int inc_pending_elr_count(memtable::ObMemtableCtx& mt_ctx, const int64_t log_ts);

  // backup data mgr
  int update_restore_points(
      const ObIArray<int64_t>& restore_points, const ObIArray<int64_t>& schema_versions, const int64_t snapshot_gc_ts);
  int update_backup_points(
      const ObIArray<int64_t>& backup_points, const ObIArray<int64_t>& schema_versions, const int64_t snapshot_gc_ts);
  int get_backup_meta_data(const int64_t snapshot_version, ObPartitionGroupMeta& pg_meta, ObTablesHandle& handle);
  int get_backup_pg_meta_data(const int64_t snapshot_version, ObPartitionGroupMeta& pg_meta);
  int get_backup_partition_meta_data(const ObPartitionKey& pkey, const int64_t snapshot_version,
      ObPGPartitionStoreMeta& partition_store_meta, ObTablesHandle& handle);

  TO_STRING_KV(KP(this), K_(pkey), KP_(meta), K_(log_seq_num), K_(last_freeze_ts));

private:
  class GetPGPartitionCountFunctor {
  public:
    GetPGPartitionCountFunctor(const bool include_trans_table, int64_t& count)
        : include_trans_table_(include_trans_table), count_(count)
    {}
    ~GetPGPartitionCountFunctor()
    {}
    int operator()(const common::ObPartitionKey& pkey)
    {
      int ret = OB_SUCCESS;
      if (!pkey.is_valid()) {
        STORAGE_LOG(WARN, "invalid pkey", K(ret), K(pkey));
      } else if (!pkey.is_trans_table()) {
        count_++;
      } else if (include_trans_table_) {
        count_++;
      } else {
        // do nothing
      }
      return ret;
    }

  private:
    bool include_trans_table_;
    int64_t& count_;
  };
  class GetAllPGPartitionKeyFunctor {
  public:
    explicit GetAllPGPartitionKeyFunctor(ObPartitionArray& arr) : pkeys_(arr)
    {}
    ~GetAllPGPartitionKeyFunctor()
    {}
    int operator()(const common::ObPartitionKey& pkey)
    {
      int ret = OB_SUCCESS;
      if (!pkey.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid pkey", K(ret), K(pkey));
      } else if (!pkey.is_trans_table() && OB_FAIL(pkeys_.push_back(pkey))) {
        STORAGE_LOG(WARN, "pkey push back error", K(ret), K(pkey));
      }
      return ret;
    }

  private:
    ObPartitionArray& pkeys_;
  };
  class RemovePGIndexFunctor {
  public:
    explicit RemovePGIndexFunctor(ObPartitionGroupIndex& pg_index) : pg_index_(pg_index)
    {}
    ~RemovePGIndexFunctor()
    {}
    int operator()(const common::ObPartitionKey& pkey)
    {
      int ret = OB_SUCCESS;
      if (!pkey.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(ERROR, "invalid pkey", K(pkey));
      } else if (OB_FAIL(pg_index_.remove_partition(pkey))) {
        STORAGE_LOG(ERROR, "failed to remove pg index", K(ret), K(pkey));
      }
      return ret;
    }

  private:
    ObPartitionGroupIndex& pg_index_;
  };

  class GetAllPGPartitionFunctor {
  public:
    explicit GetAllPGPartitionFunctor(ObPGPartitionArrayGuard& guard) : guard_(guard)
    {}
    int operator()(const common::ObPartitionKey& pkey)
    {
      int ret = OB_SUCCESS;
      ObPGPartition* pg_partition = NULL;
      if (!pkey.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid pkey", K(pkey));
      } else if (OB_FAIL(guard_.get_partition_map().get(pkey, pg_partition))) {
        STORAGE_LOG(WARN, "can not found partition in partition_map_ ", K(pkey), K(ret));
      } else if (OB_ISNULL(pg_partition)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "get pg_partition return NULL", K(pkey));
      } else if (!pkey.is_trans_table() && OB_FAIL(guard_.push_back(pg_partition))) {
        STORAGE_LOG(WARN, "pg partition push back error", K(ret), K(pkey));
      }

      if (pkey.is_trans_table()) {
        guard_.get_partition_map().revert(pg_partition);
      }
      return ret;
    }

  private:
    ObPGPartitionArrayGuard& guard_;
  };
  class RemovePGPartitionFunctor {
  public:
    RemovePGPartitionFunctor(ObPGPartitionMap& map) : map_(map)
    {}
    void operator()(const common::ObPartitionKey& pkey)
    {
      if (!pkey.is_valid()) {
        STORAGE_LOG(WARN, "invalid pkey", K(pkey));
      } else {
        map_.del(pkey);
      }
    }

  private:
    ObPGPartitionMap& map_;
  };
  struct SerializePair {
    SerializePair(char* buf, int64_t size) : buf_(buf), size_(size)
    {}
    SerializePair() : buf_(nullptr), size_(0)
    {}
    TO_STRING_KV(KP_(buf), K_(size));
    char* buf_;
    int64_t size_;
  };

private:
  int register_pg_partition_(const common::ObPartitionKey& pkey, ObPGPartition* pg_info);
  int create_pg_memtable_(ObPGPartition* pg_partition);
  int get_all_pg_partition_keys_(common::ObPartitionArray& pkeys, const bool include_trans_table = false);
  int check_need_report_(const common::ObVersion& version, bool& need_report);
  int update_report_status_(ObPGReportStatus& pg_report_status, const int64_t default_data_version = 0,
      const int64_t default_snapshot_version = 0);
  int update_report_status_(const bool write_slog);
  int update_report_status_and_schema_version_(ObPGReportStatus& status, const int64_t schema_version,
      const int64_t default_data_version, const int64_t default_snapshot_version, const bool write_slog);
  int check_can_release_pg_memtable_(ObTablesHandle& memtable_merged, ObTablesHandle& memtable_to_release);
  int write_report_status_(const ObPGReportStatus& status, const bool write_slog);
  int write_report_status_and_schema_version_(
      const ObPGReportStatus& status, const int64_t schema_version, const bool write_slog);
  int64_t get_meta_serialize_size_();
  int set_pg_meta_(const ObPartitionGroupMeta& pg_meta, const bool is_replay);
  int init_pg_meta_(const ObCreatePGParam& param, ObPartitionGroupMeta& pg_meta);
  int deserialize_(
      const char* buf, const int64_t buf_len, int64_t& pos, int64_t& state, ObPartitionSplitInfo& split_info);
  int deserialize_2270_(
      const char* buf, const int64_t buf_len, int64_t& pos, int64_t& state, ObPartitionSplitInfo& split_info);
  int deserialize_before_2270_(
      const char* buf, const int64_t buf_len, int64_t& pos, int64_t& state, ObPartitionSplitInfo& split_info);
  int deserialize_old_partition_(
      const char* buf, const int64_t buf_len, int64_t& pos, int64_t& state, ObPartitionSplitInfo& split_info);
  int insert_pg_partition_arr_(ObPartitionArray* arr, const ObPartitionKey& pkey);
  int del_pg_partition_arr_(ObPartitionArray* arr, const ObPartitionKey& pkey);
  int64_t get_bucket_idx_(const ObPartitionKey& pkey) const;
  int write_set_replica_type_log_(const ObReplicaType replica_type);
  int clear_pg_partition_non_reused_stores_(const ObPartitionKey& pkey, const bool need_remove_flag);
  int check_table_store_empty(bool& is_empty);
  int update_dest_split_state_after_merge_(int64_t& split_state);
  int write_create_partition_slog(const common::ObPartitionKey& pkey, const uint64_t log_id);
  int write_remove_pg_partition_trans_(
      const common::ObPartitionKey& pkey, const uint64_t log_id, const ObPartitionGroupMeta& next_meta);
  int write_create_partition_slog(const common::ObPartitionKey& pkey);
  int write_remove_pg_partition_trans(const common::ObPartitionKey& pkey);
  int set_partition_removed_(const ObPartitionKey& pkey);
  int create_sstable_impl(const common::ObIArray<blocksstable::ObMacroBlocksWriteCtx*>& data_blocks,
      const common::ObIArray<blocksstable::ObMacroBlocksWriteCtx*>& lob_blocks,
      blocksstable::ObMacroBlocksWriteCtx* bloomfilter_block, const ObSSTableMergeInfo* sstable_merge_info,
      ObTableHandle& handle);
  int serialize_impl(
      ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size, ObTablesHandle* tables_handle);
  int set_replay_sstables(const bool is_replay_old, ObPartitionStore& store);
  int append_sstable(const share::ObBuildIndexAppendSSTableParam& param, common::ObNewRowIterator& iter,
      ObTableHandle& sstable_handle);
  int get_min_sstable_version_(int64_t& min_sstable_snapshot_version);
  int check_update_split_state_();
  int remove_old_table_(const ObPartitionKey& pkey, const int64_t frozen_version);
  int update_upper_trans_version_(const ObPartitionKey& pkey, transaction::ObTransService& trans_service,
      const int64_t last_replay_log_ts, ObTablesHandle& updated_tables);
  int remove_mem_ctx_for_trans_ctx_(memtable::ObMemtable* mt);
  int check_flashback_partition_need_remove(
      const int64_t flashback_scn, ObPGPartition* pg_partition, bool& need_remove);
  int alloc_file_for_old_replay();
  int transform_and_add_old_sstable_for_partition(ObPartitionStore& store);
  int transform_and_add_old_sstable();
  int check_set_member_list_for_restore(ObIPartitionReport& report);
  int check_for_restore_(ObIPartitionReport& report);
  int check_restore_flag(const int16_t old_flag, const int16_t new_flag) const;
  int get_trans_table_end_log_ts_and_timestamp_(int64_t& end_log_ts, int64_t& timestamp);
  int clear_all_complement_minor_sstable_();
  int check_pg_partition_exist(const common::ObPartitionKey& pkey, bool& exist);
  int create_trans_sstable(
      const ObCreatePartitionParam& create_partition_param, const bool in_slog_trans, ObTablesHandle& sstables_handle);
  int prepare_partition_store_map_(const ObPartitionMigrateCtx& ctx, ObPartitionStore::TableStoreMap*& new_store_map);
  int do_replace_store_map_(
      const common::ObIArray<ObPartitionMigrateCtx>& part_ctx_array, ObPartitionStore::TableStoreMap** store_maps);
  int get_freeze_info_(const common::ObVersion& version, ObFreezeInfoSnapshotMgr::FreezeInfo& freeze_info);
  int create_pg_partition_(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
      const uint64_t data_table_id, const ObCreatePartitionParam& create_param, const bool in_slog_trans,
      const bool is_replay, const uint64_t log_id, ObTablesHandle& sstables_handle);
  int create_pg_partition_(const common::ObPartitionKey& pkey, const int64_t multi_version_start,
      const uint64_t data_table_id, const obrpc::ObCreatePartitionArg& arg, const bool in_slog_trans,
      const bool is_replay, const uint64_t log_id, ObTablesHandle& sstables_handle);
  int create_pg_partition_if_need_(
      const ObIArray<ObPartitionMigrateCtx>& part_ctx_array, const int64_t schema_version, const bool is_restore);
  int check_split_dest_merged_(bool& is_merged);
  ObReplicaType get_replica_type_() const;
  int compat_fill_log_ts(ObArray<ObSSTable*>& replay_tables);

  int replay_for_compat_(ObSSTable* sstable, int64_t& fill_log_ts);
  int get_restore_point_tables_(const int64_t snapshot_version, const int64_t schema_version,
      ObPartitionGroupMeta& pg_meta, ObIArray<ObPGPartitionStoreMeta>& partition_metas, ObTablesHandle& handle,
      bool& is_ready, bool& is_need);
  int alloc_meta_(ObPartitionGroupMeta*& meta);
  void free_meta_(ObPartitionGroupMeta*& meta);
  void switch_meta_(ObPartitionGroupMeta*& next_meta);
  int get_restore_point_max_schema_version_(
      const int64_t publish_version, const ObPartitionArray& partitions, int64_t& schema_version);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPGStorage);

private:
  static const int64_t MAGIC_NUM_BEFORE_2_2_70 = -0xBCDE;
  static const int64_t MAGIC_NUM_2_2_70 = -0xBCDF;
  static const int64_t MAGIC_NUM = -0xBCE0;
  static const int64_t BUCKET_LOCK_BUCKET_CNT = 16;
  static const int64_t META_BLOCK_HANDLE_CNT = 2;
  static const int64_t FETCH_LAST_REPLAY_LOG_CHECKSUM_TIMEOUT = 5L * 1000L * 1000L;
  static const int64_t PG_LOG_INFO_QUERY_TIMEOUT = 5L * 1000L * 1000L;
  bool is_inited_;
  bool is_removed_;
  common::ObPartitionKey pkey_;
  ObIPartitionComponentFactory* cp_fty_;
  transaction::ObTransService* txs_;
  clog::ObIPartitionLogService* pls_;
  ObIPartitionGroup* pg_;
  ObPGPartition* pg_partition_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObPartitionKeyList partition_list_;

  // pg meta and memstore
  common::ObBucketLock bucket_lock_;
  common::TCRWLock lock_;
  ObPartitionGroupMeta* meta_;
  int64_t log_seq_num_;  // abandoned in 3.0, keep it because of serialization compatibility
  ObPGMemtableMgr pg_memtable_mgr_;
  bool need_clear_pg_index_;
  blocksstable::ObStorageFileHandle file_handle_;
  ObPGSSTableMgr sstable_mgr_;
  ObMetaBlockListHandle meta_block_handle_;
  ObBaseFileMgr* file_mgr_;
  bool is_paused_;
  int64_t trans_table_seq_;
  int64_t last_freeze_ts_;
  // for performance optimization
  ObReplicaType cached_replica_type_;
  ObRecoveryDataMgr recovery_point_data_mgr_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PG_STORAGE
