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

#ifndef SRC_STORAGE_OB_MULTI_VERSION_TABLE_STORE_H_
#define SRC_STORAGE_OB_MULTI_VERSION_TABLE_STORE_H_
#include "ob_table_store.h"
#include "ob_i_table.h"
#include "share/config/ob_server_config.h"
namespace oceanbase {
namespace storage {

class ObPGMemtableMgr;

class ObMultiVersionTableStore {
public:
  typedef common::hash::ObHashSet<int64_t, common::hash::NoPthreadDefendMode> TableSet;
  static const int64_t MAX_KETP_VERSION_SHIFT_NUM = 4L;
  static const int64_t MAX_KETP_VERSION_NUM = 1LL << MAX_KETP_VERSION_SHIFT_NUM;
  static const int64_t MAX_DEFERRED_GC_MINOR_SSTABLE_COUNT = MAX_SSTABLE_CNT_IN_STORAGE;

  ObMultiVersionTableStore();
  virtual ~ObMultiVersionTableStore();

  int init(const common::ObPartitionKey& pkey, const uint64_t table_id, ObFreezeInfoSnapshotMgr* freeze_info_mgr,
      ObPGMemtableMgr* pg_memtable_mgr, const int64_t schema_version = 0);
  uint64_t get_table_id() const
  {
    return table_id_;
  }

  int set_reference_tables(
      ObTablesHandle& handle, ObTableStore*& new_table_store, const int64_t memtable_base_version, bool& need_update);
  int prepare_update_split_table_store(
      const bool is_major_split, ObTablesHandle& handle, bool& need_update, ObTableStore*& new_table_store);
  int check_need_split(common::ObVersion& split_version, bool& need_split, bool& need_minor_split);
  int check_can_split(bool& can_split);
  int check_can_migrate(bool& can_migrate);
  int check_complete(bool& is_complete);
  int is_physical_split_finished(bool& is_physical_split_finish);
  int get_physical_split_info(ObVirtualPartitionSplitInfo& split_info);
  int prepare_add_sstable(AddTableParam& param, ObTableStore*& new_table_store, bool& need_update);
  int prepare_remove_sstable(
      AddTableParam& param, ObTablesHandle& handle, ObTableStore*& new_table_store, bool& is_equal);
  int alloc_table_store(ObTableStore*& new_table_store);
  void free_table_store(ObTableStore*& new_table_store);
  int enable_table_store(const bool need_prewarm, ObTableStore& new_table_store);

  int get_read_tables(const int64_t snapshot_version, ObTablesHandle& handle, const bool allow_not_ready,
      const bool print_dropped_alert);
  int get_reference_tables(ObTablesHandle& handle);
  int get_sample_read_tables(const common::SampleInfo& sample_info, ObTablesHandle& handle);
  int get_major_sstable(const common::ObVersion& version, ObTablesHandle& handle);

  int get_all_tables(TableSet& table_set, ObTablesHandle& handle);
  int check_memtable_merged(const memtable::ObMemtable& memtable, bool& all_merged, bool& can_release);
  int get_latest_major_sstable(ObTableHandle& handle);
  int get_sstable_schema_version(int64_t& schema_version);
  int get_merge_tables(
      const ObGetMergeTablesParam& param, const int64_t multi_version_start, ObGetMergeTablesResult& result);

  int get_split_tables(const bool is_major_split, ObTablesHandle& handle);
  int retire_prewarm_store(const int64_t duration, const int64_t minor_deferred_gc_time);
  int get_latest_table_count(int64_t& latest_table_count);
  int check_latest_table_count_safe(bool& is_safe);
  int latest_has_major_sstable(bool& has_major);
  int get_effective_tables(const bool include_active_memtable, ObTablesHandle& handle);

  int finish_replay(const int64_t multi_version_start);
  int check_ready_for_read(bool& is_ready);
  int need_remove_old_table(const common::ObVersion& kept_min_version, const int64_t multi_version_start,
      const int64_t backup_snapshot_verison, int64_t& real_kept_major_num, bool& need_remove);
  int set_table_store(const ObTableStore& table_store);
  OB_INLINE ObTableStore* get_latest_store();
  int get_replay_tables(ObIArray<ObITable::TableKey>& replay_tables);
  int set_replay_sstables(const bool is_replay_old, const common::ObIArray<ObSSTable*>& sstables);
  int check_need_minor_merge(const bool is_follower_data_rep, const ObMergeType merge_type, bool& need_merge);
  int get_oldest_read_tables(const int64_t snapshot_version, ObTablesHandle& handle);
  int get_gc_sstables(ObTablesHandle& handle);
  int get_multi_version_start(int64_t& multi_version_start);
  int get_max_major_sstable_snapshot(int64_t& max_snapshot_version);
  int get_min_max_major_version(int64_t& min_version, int64_t& max_version);
  int get_latest_minor_sstables(ObTablesHandle& handle);
  int get_latest_continue_tables(ObTablesHandle& handle);
  int get_flashback_major_tables(const int64_t flashback_scn, ObTablesHandle& handle);
  int get_mark_deletion_tables(const int64_t end_log_ts, const int64_t snapshot_version, ObTablesHandle& handle);
  int clear_complement_minor_sstable();
  bool is_data_table()
  {
    return table_id_ == pkey_.get_table_id();
  }
  int get_min_schema_version(int64_t& min_schema_version);
  int get_schema_version(int64_t& schema_version);
  void set_is_dropped_schema()
  {
    is_dropped_schema_ = true;
  }
  bool is_dropped_schema() const
  {
    return is_dropped_schema_;
  }
  int get_migrate_tables(ObTablesHandle& handle, bool& is_ready_for_read);
  int get_needed_local_tables_for_migrate(const ObMigrateRemoteTableInfo& remote_table_info, ObTablesHandle& handle);
  int set_drop_schema_info(const int64_t drop_schema_version);
  int get_drop_schema_info(int64_t& drop_schema_version, int64_t& drop_schema_refreshed_ts);
  int get_recovery_point_tables(const int64_t snapshot_version, ObTablesHandle& handle);

  DECLARE_VIRTUAL_TO_STRING;

private:
  struct ObGCSSTableInfo final {
    ObSSTable* sstable_;
    int64_t retired_ts_;

    ObGCSSTableInfo();
    ~ObGCSSTableInfo();
    void reset();
    DECLARE_TO_STRING;
  };
  OB_INLINE int64_t get_idx(const int64_t pos) const;
  OB_INLINE ObTableStore* get_store(const int64_t pos);
  OB_INLINE ObTableStore* get_read_store();
  int retire_head_store();
  OB_INLINE ObTableStore* get_oldest_store();
  int record_gc_minor_sstable(ObTableStore& new_table_store);
  int add_gc_minor_sstable(const int64_t gc_ts, ObSSTable* table, const int64_t max_deferred_gc_count);
  int add_complement_minor_sstable_if_needed_(AddTableParam& param, ObTableStore& store);

private:
  bool is_inited_;
  common::ObPartitionKey pkey_;
  uint64_t table_id_;
  ObTableStore* table_stores_[MAX_KETP_VERSION_NUM];
  int64_t head_;
  int64_t tail_;
  common::ObRandom rand_;
  ObFreezeInfoSnapshotMgr* freeze_info_mgr_;
  ObPGMemtableMgr* pg_memtable_mgr_;
  // Currently, only mini sstables are recorded, sorted by release time, and those released earlier are later
  ObGCSSTableInfo gc_sstable_infos_[MAX_DEFERRED_GC_MINOR_SSTABLE_COUNT];
  int64_t gc_sstable_count_;
  int64_t create_schema_version_;
  bool is_dropped_schema_;
  // Record the version number of the drop in the index table,
  // which is expected to be greater than the version number when it is actually deleted
  int64_t drop_schema_version_;
  // Record the timestamp corresponding to the version number of the index table drop,
  // which is a local timestamp, which may be greater than the actual drop time of the index table
  int64_t drop_schema_refreshed_ts_;

  DISALLOW_COPY_AND_ASSIGN(ObMultiVersionTableStore);
};

OB_INLINE int64_t ObMultiVersionTableStore::get_idx(const int64_t pos) const
{
  return pos & ((1L << MAX_KETP_VERSION_SHIFT_NUM) - 1L);
}

OB_INLINE ObTableStore* ObMultiVersionTableStore::get_store(const int64_t pos)
{
  return table_stores_[get_idx(pos)];
}

OB_INLINE ObTableStore* ObMultiVersionTableStore::get_latest_store()
{
  ObTableStore* table_store = NULL;
  if (tail_ > head_) {
    table_store = table_stores_[get_idx(tail_ - 1L)];
  }
  return table_store;
}
OB_INLINE ObTableStore* ObMultiVersionTableStore::get_read_store()
{
  int64_t index = get_idx(head_);
  const int64_t duration = GCONF.minor_warm_up_duration_time;
  if (duration > 0 && tail_ - head_ > 1L) {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t rand = rand_.get(0L, duration);

    for (int64_t i = tail_ - 1L; i > head_; --i) {
      const int64_t start_time = table_stores_[get_idx(i)]->get_uptime();
      // The function is transformed into a quadratic function,
      // and the flow rate increases slowly during warm-up and then increases rapidly
      const double x = static_cast<int>(now - start_time) / static_cast<double>(duration);
      const double y = x * x;

      if (rand < y * static_cast<int>(duration)) {
        index = get_idx(i);
        break;  // stop searching
      }
    }
  }

  return table_stores_[index];
}

OB_INLINE ObTableStore* ObMultiVersionTableStore::get_oldest_store()
{
  ObTableStore* table_store = NULL;
  if (head_ < tail_) {
    table_store = table_stores_[get_idx(head_)];
  }
  return table_store;
}

}  // namespace storage
}  // namespace oceanbase

#endif /* SRC_STORAGE_OB_MULTI_VERSION_TABLE_STORE_H_ */
