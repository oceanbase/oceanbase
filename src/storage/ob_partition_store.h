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

#ifndef SRC_STORAGE_OB_PARTITION_STORE_H_
#define SRC_STORAGE_OB_PARTITION_STORE_H_

#include "ob_i_table.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "ob_storage_struct.h"
#include "memtable/ob_memtable.h"
#include "ob_multi_version_table_store.h"
#include "ob_saved_storage_info_v2.h"
#include "blocksstable/slog/ob_storage_log_struct.h"
#include "ob_partition_log.h"
//#include "ob_pg_memtable_mgr.h"

namespace oceanbase {
namespace share {
class ObSSTableChecksumItem;
}
namespace storage {
class ObPGMemtableMgr;

struct ObPartitionStoreInfo {
  int64_t is_restore_;
  int64_t migrate_status_;
  int64_t migrate_timestamp_;
  int64_t replica_type_;
  int64_t split_state_;
  int64_t multi_version_start_;
  int64_t report_version_;
  int64_t report_row_count_;
  int64_t report_data_checksum_;
  int64_t report_data_size_;
  int64_t report_required_size_;
  int64_t readable_ts_;

  ObPartitionStoreInfo()
  {
    reset();
  }
  void reset();
};

struct TableStoreStat {
  TableStoreStat();
  uint64_t index_id_;
  bool has_dropped_flag_;
  TO_STRING_KV(K_(index_id), K_(has_dropped_flag));
};

class ObPGStorage;
class ObPartitionStorage;

class ObPartitionStore {
  friend class ObPGStorage;
  friend class ObPartitionStorage;

public:
  typedef common::hash::ObCuckooHashMap<uint64_t, ObMultiVersionTableStore*> TableStoreMap;
  typedef TableStoreMap* TableStoreMapPtr;
  static const int64_t TABLE_STORE_BUCKET_NUM = 2;
  static const int64_t MAX_KETP_VERSION_SHIFT_NUM = 6L;
  static const int64_t MAX_KETP_VERSION_NUM = 1LL << MAX_KETP_VERSION_SHIFT_NUM;
  static const int64_t MAGIC_NUM_2 = -0xABCF;
  static const int64_t PRINT_READABLE_INFO_DURATION_US = 1 * 60 * 1000 * 1000LL;  // 1m
  ObPartitionStore();
  virtual ~ObPartitionStore();
  bool is_inited() const
  {
    return is_inited_;
  }

  void destroy();

  common::ObReplicaType get_replica_type() const;
  //  common::ObReplicaProperty get_replica_property() const;
  int get_read_tables(const uint64_t table_id, const int64_t snapshot_version, ObTablesHandle& handle,
      const bool allow_not_ready = false, const bool need_safety_check = false, const bool reset_handle = true,
      const bool print_dropped_alert = true);
  template <class T1, class T2>
  int get_read_tables(const T1 table_id, const T2 snapshot_version, ObTablesHandle& handle,
      ObTablesHandle* main_table_major_sstable_handle = NULL) = delete;
  int get_read_frozen_tables(const uint64_t table_id, const common::ObVersion& frozen_version, ObTablesHandle& handle,
      const bool reset_handle = true);
  int get_sample_read_tables(const common::SampleInfo& sample_info, const uint64_t table_id, ObTablesHandle& handle,
      const bool reset_handle = true);
  int get_effective_tables(const uint64_t table_id, ObTablesHandle& handle, bool& is_ready_for_read);
  int get_gc_sstables(const uint64_t table_id, ObTablesHandle& handle);

  int get_all_tables(ObTablesHandle& handle);
  int get_active_memtable(ObTableHandle& handle);
  int get_memtables(ObTablesHandle& handle, const bool reset_handle = true);
  bool has_memtable();
  bool has_active_memtable();
  int has_major_sstable(const uint64_t index_table_id, bool& has_major);
  bool is_freezing() const
  {
    return ATOMIC_LOAD(&is_freezing_);
  }

  int fill_checksum(const uint64_t index_id, const int sstable_type, share::ObSSTableChecksumItem& checksum);

  int get_all_table_ids(common::ObIArray<uint64_t>& index_tables);
  int get_all_table_stats(common::ObIArray<TableStoreStat>& index_tables);
  int get_reference_tables(int64_t table_id, ObTablesHandle& handle);
  int get_merge_table_ids(const ObMergeType merge_type, const bool using_remote_memstore, const bool is_in_dest_split,
      const int64_t trans_table_end_log_ts, const int64_t trans_table_timestamp, common::ObVersion& merge_version,
      common::ObIArray<uint64_t>& index_tables, bool& need_merge);
  int is_physical_split_finished(bool& is_finish);
  int get_physical_split_info(ObVirtualPartitionSplitInfo& split_info);
  int check_need_split(
      const int64_t& index_id, common::ObVersion& split_version, bool& need_split, bool& need_minor_split);
  int check_can_migrate(bool& can_migrate);
  int get_split_table_ids(
      common::ObVersion& split_version, bool is_minor_split, common::ObIArray<uint64_t>& index_tables);

  int get_merge_tables(const ObGetMergeTablesParam& param, ObGetMergeTablesResult& result);
  int get_split_tables(const bool is_major_split, const uint64_t index_id, ObTablesHandle& handle);

  // for merge/backup/migrate
  int get_major_sstable(const uint64_t index_id, const common::ObVersion& merge_version, ObTablesHandle& handle);
  int get_last_major_sstable(const uint64_t index_id, ObTableHandle& handle);
  int get_last_all_major_sstable(ObTablesHandle& handle);
  int get_data_table_latest_sstables(ObTablesHandle& handle);

  int get_migrate_table_ids(common::ObIArray<uint64_t>& table_ids);
  OB_INLINE common::ObPartitionKey get_partition_key() const;
  int get_meta(ObPGPartitionStoreMeta& meta);

  int check_ready_for_read(const uint64_t table_id, bool& is_raedy);
  int check_major_merge_finished(const common::ObVersion& version, bool& finished);
  int check_need_report(const common::ObVersion& version, bool& need_report);
  int check_all_merged(
      memtable::ObMemtable& memtable, const int64_t schema_version, bool& is_all_merged, bool& can_release);
  int64_t get_multi_version_start();

  int get_replay_tables(const uint64_t table_id, ObIArray<ObITable::TableKey>& replay_tables);
  int set_replay_sstables(const uint64_t table_id, const bool is_replay_old, ObIArray<ObSSTable*>& sstables);

  int get_first_frozen_memtable(ObTableHandle& handle);
  int check_table_store_exist(const uint64_t index_id, bool& exist);
  int get_major_frozen_versions(const ObVersion& data_version, int64_t& publish_version, int64_t& schema_version);
  int get_create_schema_version(int64_t& create_schema_version);
  int get_min_sstable_version(int64_t& min_sstable_snapshot_version);
  int get_min_max_major_version(int64_t& min_version, int64_t& max_version);
  int get_latest_minor_sstables(const int64_t index_id, ObTablesHandle& handle);
  int get_all_latest_minor_sstables(ObTablesHandle& handle);
  int scan_trans_table(const ObTableIterParam& iter_param, ObTableAccessContext& access_context,
      const ObExtStoreRange& whole_range, ObStoreRowIterator*& row_iter);
  int get_max_major_sstable_snapshot(int64_t& max_snapshot_version);
  int physical_flashback(const int64_t flashback_scn, ObVersion& data_version);
  int check_store_complete(bool& is_complete);
  int get_mark_deletion_tables(
      const uint64_t index_id, const int64_t end_log_ts, const int64_t snapshot_version, ObTablesHandle& handle);
  int get_trans_table_end_log_ts_and_timestamp(int64_t& end_log_ts, int64_t& timestamp);
  int clear_complement_minor_sstable();
  int64_t get_table_store_cnt() const;
  int get_physical_flashback_publish_version(const int64_t flashback_scn, int64_t& publish_version);
  int get_min_schema_version(int64_t& min_schema_version);
  int get_restore_point_tables(const int64_t snapshot_version, const int64_t publish_version,
      ObRecoveryPointSchemaFilter& schema_filter, ObTablesHandle& handle, bool& is_ready);
  int get_latest_table_count(const int64_t table_id, int64_t& table_count);

  int get_table_schema_version(const uint64_t table_id, int64_t& schema_version);
  int get_trans_table_status(ObTransTableStatus& status);
  int get_migrate_tables(const uint64_t table_id, ObTablesHandle& handle, bool& is_ready_for_read);
  int set_drop_schema_info(const int64_t index_id, const int64_t schema_version);
  int get_drop_schema_info(const int64_t index_id, int64_t& drop_schema_version, int64_t& drop_schema_refreshed_ts);

  TO_STRING_KV(K_(is_inited), K_(log_seq_num), "meta", *meta_);

private:
  int create_partition_store(const ObPGPartitionStoreMeta& meta, const bool write_slog, ObIPartitionGroup* pg,
      ObFreezeInfoSnapshotMgr& freeze_info_mgr, ObPGMemtableMgr* pg_memtable_mgr);
  int init(const ObPGPartitionStoreMeta& meta, ObIPartitionGroup* pg, ObFreezeInfoSnapshotMgr& freeze_info_mgr);
  int create_multi_version_store(const uint64_t table_id, ObMultiVersionTableStore*& table_store);
  int create_multi_version_store(
      const uint64_t table_id, const int64_t schema_version, ObMultiVersionTableStore*& table_store);
  int create_multi_version_store_(
      const uint64_t table_id, const int64_t schema_version, ObMultiVersionTableStore*& table_store);
  int get_all_tables_unlock(ObTablesHandle& handle);
  int get_major_merge_table_ids(const int64_t save_slave_read_version, common::ObVersion& merge_version,
      common::ObIArray<uint64_t>& index_tables, bool& need_merge);
  int get_minor_merge_table_ids(
      const bool using_remote_memstore, const ObMergeType merge_type, common::ObIArray<uint64_t>& index_tables);
  int get_mini_merge_table_ids(const int64_t save_slave_read_version, const ObMergeType merge_type,
      const int64_t trans_table_end_log_ts, const int64_t trans_table_timestamp,
      common::ObIArray<uint64_t>& index_tables, bool& need_merge);
  int check_need_mini_merge(ObMultiVersionTableStore* table_store, const ObMergeType merge_type,
      const int64_t save_slave_read_version, const int64_t trans_table_end_log_ts, const int64_t trans_table_timestamp,
      bool& need_merge, bool& can_merge);
  int check_need_minor_merge(const uint64_t table_id, bool& need_merge);
  int check_need_major_merge(ObMultiVersionTableStore* table_store, const int64_t save_slave_read_version,
      common::ObVersion& merge_version, bool& need_merge, bool& can_merge);
  int push_frozen_memtable_per_table_(memtable::ObMemtable* frozen_memtable);
  int check_all_tables_merged(const memtable::ObMemtable& memtable,
      const common::ObIArray<share::schema::ObIndexTableStat>& index_status,
      const common::ObIArray<uint64_t>& deleted_index_ids, bool& all_merged, bool& can_release);
  int check_skip_check_index_merge_status(const share::schema::ObIndexTableStat& index_stat,
      const common::ObIArray<uint64_t>& deleted_index_ids, bool& need_skip);
  int do_add_sstable(
      AddTableParam& param, const ObMigrateStatus& migarte_status, const bool is_in_source_split = false);

  int begin_transaction(bool in_slog_trans, bool write_slog);
  int end_transaction(bool in_slog_trans, bool write_slog, bool is_success);

  int write_create_partition_store_log(const ObPGPartitionStoreMeta& meta);
  int write_update_partition_meta_trans(const ObPGPartitionStoreMeta& meta, const LogCommand& cmd);
  int write_modify_table_store_log(ObTableStore& table_store, const int64_t multi_version_start);
  int write_drop_index_trans(const common::ObPartitionKey& pkey, const uint64_t index_id);
  int check_new_table_store(
      ObTableStore& new_table_store, const ObMigrateStatus migrate_status, const bool is_in_source_split = false);
  ObPGPartitionStoreMeta& get_next_meta();
  void switch_meta();
  int64_t get_serialize_size();
  int has_major_sstable_nolock(const uint64_t index_table_id, bool& has_major);
  int write_report_status(const ObReportStatus& status, const uint64_t data_table_id, const bool write_slog);
  int remove_old_table_(const common::ObVersion& kept_min_version, const int64_t multi_version_start,
      const int64_t kept_major_num, const int64_t backup_snapshot_version, ObMultiVersionTableStore& table_store);
  int get_index_status(const int64_t schema_version, common::ObIArray<share::schema::ObIndexTableStat>& index_status,
      common::ObIArray<uint64_t>& deleted_index_ids);
  int release_head_memtable(memtable::ObMemtable* memtable);
  int check_table_store_exist_nolock(const uint64_t index_id, bool& exist, ObMultiVersionTableStore*& got_table_store);
  int check_table_store_exist_with_lock(
      const uint64_t index_id, bool& exist, ObMultiVersionTableStore*& got_table_store);
  int set_table_store(const ObTableStore& table_store);
  void clear_sstores_no_lock();
  static ObMultiVersionTableStore* alloc_multi_version_table_store(const uint64_t tenant_id);
  static void free_multi_version_table_store(ObMultiVersionTableStore*& table_store);
  OB_INLINE ObMultiVersionTableStore* get_data_table_store();
  int create_table_store_if_need(const uint64_t table_id, bool& is_created);
  int get_kept_multi_version_start(
      const bool is_in_dest_split, int64_t& multi_version_start, int64_t& backup_snapshot_version);
  int inner_remove_uncontinues_inc_tables(ObMultiVersionTableStore* multi_table_store);
  int set_removed();
  void set_replica_type(const common::ObReplicaType& replica_type);
  int create_index_table_store(const uint64_t table_id, const int64_t schema_version);
  int add_sstable(storage::ObSSTable* table, const int64_t max_kept_major_version_number, const bool in_slog_trans,
      const ObMigrateStatus& migrate_status, const bool is_in_dest_split, const int64_t schema_version = 0);
  int add_sstable_for_merge(storage::ObSSTable* table, const int64_t max_kept_major_version_number,
      const ObMigrateStatus& migrate_status, const bool is_in_restore, const bool is_in_source_split,
      const bool is_in_dest_split, storage::ObSSTable* complement_minor_sstable);
  int set_reference_tables(const uint64_t table_id, ObTablesHandle& handle);
  int halt_prewarm();
  int retire_prewarm_store(const bool is_disk_full);
  int get_active_protection_clock(int64_t& active_protection_clock);
  int new_active_memstore(int64_t& protection_clock);
  int effect_new_active_memstore(const ObSavedStorageInfoV2& info, const bool emergency);
  int clean_new_active_memstore();
  int complete_active_memstore(const ObSavedStorageInfoV2& info);
  int drop_index(const uint64_t table_id, const bool write_slog);
  int set_dropped_flag(const uint64_t table_id);
  int remove_old_table(const int64_t frozen_version);

  int serialize(common::ObIAllocator& allocator, char*& new_buf, int64_t& serialize_size);
  int deserialize(ObIPartitionGroup* pg, ObPGMemtableMgr* pg_memtable_mgr, const ObReplicaType replica_type,
      const char* buf, const int64_t buf_len, int64_t& pos, bool& is_old_meta, ObPartitionStoreMeta& old_meta);

  int replay_partition_meta(const ObPGPartitionStoreMeta& meta);
  int replay_modify_table_store(const ObModifyTableStoreLogEntry& log_entry);
  int update_multi_version_start(const int64_t multi_version_start);
  int try_update_report_status(const uint64_t data_table_id, const bool write_slog = true);
  int push_frozen_memtable_per_table(memtable::ObMemtable* frozen_memtable);
  int calc_report_status(ObReportStatus& status);
  int finish_replay();
  int get_partition_store_info(ObPartitionStoreInfo& info);
  void clear_ssstores();
  int get_report_status(ObReportStatus& status);
  bool is_removed() const;
  int64_t get_readable_ts_() const;
  int remove_uncontinues_inc_tables(const uint64_t table_id);
  int update_split_table_store(int64_t table_id, bool is_major_split, ObTablesHandle& handle);
  int check_store_complete_(bool& is_complete);
  int get_min_merged_version_(int64_t& min_merged_version);
  int inner_physical_flashback(
      const bool is_data_table, const int64_t flashback_scn, ObMultiVersionTableStore* multi_table_store);

  void replace_store_map(TableStoreMap& store_map);
  int prepare_new_store_map(const ObTablesHandle& sstables, const int64_t max_kept_major_version_number,
      const bool need_reuse_local_minor, TableStoreMap*& new_store_map);
  int prepare_new_table_store_(ObTablesHandle& index_handle, const int64_t max_kept_major_version_number,
      const bool need_reuse_local_minor, ObMultiVersionTableStore*& multi_version_table_store);
  int get_remote_table_info_(const ObTablesHandle& index_handle, const bool need_reuse_local_minor,
      ObMigrateRemoteTableInfo& remote_table_info);
  int create_new_store_map_(TableStoreMap*& new_store_map);
  static void destroy_store_map(TableStoreMap* store_map);
  int filter_read_sstables(
      const ObTablesHandle& tmp_handle, const int64_t snapshot_version, ObTablesHandle& handle, bool& is_ready);
  int get_restore_point_trans_tables_(const int64_t snapshot_version, ObTablesHandle& handle, bool& is_ready);
  int get_restore_point_normal_tables_(const int64_t snapshot_version, const int64_t publish_version,
      ObRecoveryPointSchemaFilter& schema_filter, ObTablesHandle& handle, bool& is_ready);

private:
  bool is_inited_;
  bool is_removed_;
  ObPGPartitionStoreMeta meta_buf_[2];
  ObPGPartitionStoreMeta* meta_;
  int64_t log_seq_num_;       // abandoned in 3.0, keep it for serialization compatibility
  TableStoreMap* store_map_;  // main data table and index tables
  bool is_freezing_;
  ObPGMemtableMgr* pg_memtable_mgr_;
  mutable common::TCRWLock lock_;  // protect modification of in-memory structures
  ObFreezeInfoSnapshotMgr* freeze_info_mgr_;
  common::ObPartitionKey pkey_;
  ObIPartitionGroup* pg_;
  ObMultiVersionTableStore* cached_data_table_store_;
  ObLfFIFOAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionStore);
};

OB_INLINE common::ObPartitionKey ObPartitionStore::get_partition_key() const
{
  TCRLockGuard guard(lock_);
  return meta_->pkey_;
}

OB_INLINE ObMultiVersionTableStore* ObPartitionStore::get_data_table_store()
{
  ObMultiVersionTableStore* data_table_store = NULL;

  if (!is_inited_) {
    STORAGE_LOG(ERROR, "not inited");
  } else {
    int hash_ret = store_map_->get(meta_->pkey_.get_table_id(), data_table_store);
    if (OB_HASH_NOT_EXIST != hash_ret && OB_SUCCESS != hash_ret) {
      STORAGE_LOG(ERROR, "failed to get data table store", K(hash_ret), K(*meta_));
    }
  }
  return data_table_store;
}

}  // namespace storage
}  // namespace oceanbase

#endif /* SRC_STORAGE_OB_PARTITION_STORE_H_ */
