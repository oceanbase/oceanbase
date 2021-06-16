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

#ifndef OCEANBASE_STORAGE_OB_TABLE_STORE_H_
#define OCEANBASE_STORAGE_OB_TABLE_STORE_H_

#include "ob_sstable.h"
#include "ob_storage_struct.h"
#include "storage/memtable/ob_memtable.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "ob_freeze_info_snapshot_mgr.h"
#include "ob_i_partition_base_data_reader.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace storage {

class ObPGMemtableMgr;

struct ObVirtualPartitionSplitInfo {
  ObVirtualPartitionSplitInfo() : is_major_split_(false), table_id_(0), partition_id_(0), merge_version_(0)
  {}
  TO_STRING_KV(K_(is_major_split), K_(table_id), K_(partition_id), K_(merge_version));
  bool is_major_split_;
  int64_t table_id_;
  int64_t partition_id_;
  int64_t merge_version_;
};

class ObTableStore {
public:
  friend class ObPrintableTableSchema;
  static const int64_t OB_TABLE_STORE_VERSION = 1;
  struct ObITableIDCompare {
    explicit ObITableIDCompare(int& sort_ret) : result_code_(sort_ret)
    {}
    bool operator()(const ObITable* ltable, const ObITable* rtable) const;

    int& result_code_;
  };
  struct ObITableLogTsRangeCompare {
    explicit ObITableLogTsRangeCompare(int& sort_ret) : result_code_(sort_ret)
    {}
    bool operator()(const ObITable* ltable, const ObITable* rtable) const;

    int& result_code_;
  };
  struct ObITableSnapshotVersionCompare {
    explicit ObITableSnapshotVersionCompare(int& sort_ret) : result_code_(sort_ret)
    {}
    bool operator()(const ObITable* lstore, const ObITable* rstore) const;

    int& result_code_;
  };

  ObTableStore();
  virtual ~ObTableStore();

  int init(const common::ObPartitionKey& pkey, const uint64_t table_id, ObFreezeInfoSnapshotMgr* freeze_info_mgr,
      ObPGMemtableMgr* pg_memtable_mgr);
  int init(const ObTableStore& table_store, ObFreezeInfoSnapshotMgr* freeze_info_mgr, ObPGMemtableMgr* pg_memtable_mgr);
  void reset();
  OB_INLINE bool is_valid() const;
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  const ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }

  int64_t get_uptime() const
  {
    return uptime_;
  }

  // Split Section
  int set_reference_tables(
      ObTablesHandle& handle, ObTablesHandle& old_handle, const int64_t memtable_base_version, bool& need_update);
  int check_need_split(
      const ObTablesHandle& old_handle, common::ObVersion& split_version, bool& need_split, bool& need_minor_split);
  int is_physical_split_finished(bool& is_physical_split_finish);
  int is_physical_split_finished(ObTablesHandle& old_handle, bool& is_physical_split_finish);
  int get_physical_split_info(ObVirtualPartitionSplitInfo& split_info);
  int get_reference_tables(ObTablesHandle& handle);
  int get_major_split_tables(ObTablesHandle& handle);
  int get_minor_split_tables(ObTablesHandle& handle);
  int check_can_split(bool& can_split);
  int check_can_split(const ObTablesHandle& old_handle, bool& can_split);
  int build_major_split_store(const ObTablesHandle& old_handle, ObTablesHandle& handle, bool& need_update);
  int build_minor_split_store(const ObTablesHandle& old_handle, ObTablesHandle& handle, bool& need_update);

  // Common Section
  int get_all_tables(
      const bool include_active_memtable, const bool include_complement_minor_sstable, ObTablesHandle& handle);
  int get_read_tables(const int64_t snapshot_version, ObTablesHandle& handle, const bool allow_not_ready = false);
  // get major sstable of specified version
  int get_major_sstable(const common::ObVersion& version, ObTablesHandle& handle);
  int get_latest_major_sstable(ObTableHandle& handle);
  int get_sample_read_tables(const common::SampleInfo& sample_info, ObTablesHandle& handle);
  int build_new_merge_store(const AddTableParam& param, ObTablesHandle& old_handle);
  int get_major_merge_tables(const ObGetMergeTablesParam& param, ObGetMergeTablesResult& result);
  int get_mini_minor_merge_tables(
      const ObGetMergeTablesParam& param, const int64_t multi_version_start, ObGetMergeTablesResult& result);
  int get_hist_minor_range(const common::ObIArray<ObFreezeInfoSnapshotMgr::FreezeInfoLite>& freeze_infos,
      const bool is_optimize, int64_t& min_snapshot_version, int64_t& max_snapshot_version);
  int get_hist_minor_merge_tables(
      const ObGetMergeTablesParam& param, const int64_t multi_version_start, ObGetMergeTablesResult& result);
  int get_mini_merge_tables(
      const ObGetMergeTablesParam& param, const int64_t demand_multi_version_start, ObGetMergeTablesResult& result);
  int get_table_count(int64_t& table_count) const;
  int check_table_count_safe(bool& is_safe) const;
  int has_major_sstable(bool& has_major);

  int check_can_migrate(bool& can_migrate);
  bool is_ready_for_read() const
  {
    return is_ready_for_read_;
  }
  int need_remove_old_table(const common::ObVersion& kept_min_version, const int64_t multi_version_start,
      const int64_t backup_snapshot_verison, int64_t& real_kept_major_num, bool& need_remove);
  int equals(const ObTableStore& other, bool& is_equal);

  int finish_replay(const int64_t multi_version_start);
  int get_replay_tables(ObIArray<ObITable::TableKey>& replay_tables);
  int set_replay_sstables(const bool is_replay_old, const common::ObIArray<ObSSTable*>& sstables);
  int get_all_sstables(const bool need_complemnt, ObTablesHandle& handle);
  int get_continue_tables(ObTablesHandle& handle);
  int check_need_mini_minor_merge(const bool using_remote_memstore, bool& need_merge);
  int64_t cal_hist_minor_merge_threshold() const;
  int check_need_hist_minor_merge(bool& need_merge);
  int get_multi_version_start(int64_t& multi_version_start);
  int get_min_max_major_version(int64_t& min_version, int64_t& max_version);
  int check_complete(bool& is_complete);
  int get_minor_sstables(ObTablesHandle& handle);
  ObSSTable* get_complement_minor_sstable()
  {
    return complement_minor_sstable_;
  }
  int get_flashback_major_sstable(const int64_t flashback_scn, ObTablesHandle& handle);
  int get_mark_deletion_tables(const int64_t end_log_ts, const int64_t snapshot_version, ObTablesHandle& handle);
  int clear_complement_minor_sstable();
  int get_min_schema_version(int64_t& min_schema_version);
  int get_schema_version(int64_t& schema_version) const;
  int get_needed_local_tables_for_migrate(const ObMigrateRemoteTableInfo& remote_table_info, ObTablesHandle& handle);
  int is_memtable_need_merge(const memtable::ObMemtable& memtable, bool& need_merge);
  int get_recovery_point_tables(const int64_t snapshot_version, ObTablesHandle& handle);
  DECLARE_VIRTUAL_TO_STRING;

private:
  // Common Section
  bool is_multi_version_break(const ObVersionRange& new_version_range, const int64_t last_snapshot_vesion);
  int classify_tables(const ObTablesHandle& old_handle, common::ObArray<ObITable*>& major_tables,
      common::ObArray<ObITable*>& inc_tables);
  int add_trans_sstable(ObSSTable* new_table, common::ObIArray<ObITable*>& trans_tables);
  int add_major_sstable(ObSSTable* new_table, common::ObArray<ObITable*>& major_tables);
  int add_minor_sstable(
      const bool need_safe_check, common::ObIArray<ObITable*>& inc_tables, storage::ObSSTable* new_table);
  bool check_include_by_log_ts_range(ObITable& a, ObITable& b);
  bool check_intersect_by_log_ts_range(ObITable& a, ObITable& b);
  int check_need_add_minor_sstable(
      common::ObIArray<ObITable*>& inc_tables, ObITable* new_table, ObIArray<ObITable*>& tmp_tables);
  int build_tables(const int64_t kept_major_version_num, const int64_t multi_version_start,
      ObIArray<ObITable*>& major_tables, ObIArray<ObITable*>& inc_tables, const int64_t backup_snapshot_version);
  int cal_major_copy_start_pos(const ObIArray<ObITable*>& major_tables, const int64_t kept_major_version_num,
      const int64_t backup_snapshot_version, const int64_t need_major_pos, int64_t& copy_start_pos);
  int find_need_major_sstable(const common::ObIArray<ObITable*>& major_tables, const int64_t multi_version_start,
      int64_t& major_pos, ObITable*& major_table);
  int find_need_backup_major_sstable(
      const common::ObIArray<ObITable*>& major_tables, const int64_t backup_snapshot_version, int64_t& major_pos);
  int add_backup_table(
      common::ObIArray<ObITable*>& major_tables, const int64_t backup_major_pos, const int64_t copy_start_pos);
  int refine_mini_minor_merge_result(ObGetMergeTablesResult& result);
  int refine_mini_merge_result(ObGetMergeTablesResult& result);
  int refine_mini_merge_result_in_reboot_phase(ObITable& last_table, ObGetMergeTablesResult& result);
  int add_complement_to_mini_merge_result(ObGetMergeTablesResult& result);
  int refine_mini_merge_log_ts_range(const ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite& freeze_info,
      const int64_t demand_multi_version_start, ObGetMergeTablesResult& result);
  int find_mini_minor_merge_tables(const ObGetMergeTablesParam& param, const int64_t min_snapshot_version,
      const int64_t max_snapshot_version, const int64_t expect_multi_version, ObGetMergeTablesResult& result);
  int find_mini_merge_tables(const ObGetMergeTablesParam& param,
      const ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite& freeze_info, ObGetMergeTablesResult& result);
  int check_continues_and_get_schema_version(const ObMergeType& merge_type, ObGetMergeTablesResult& result);
  int add_minor_merge_result(ObITable* table, ObGetMergeTablesResult& result);
  int cal_minor_merge_inc_base_version(int64_t& inc_base_version);
  int cal_mini_merge_inc_base_version(int64_t& inc_base_version);
  int cal_multi_version_start(const int64_t demand_multi_version_start, ObVersionRange& version_range);
  int inner_add_table(common::ObIArray<ObITable*>& tables, const int64_t start);
  int check_ready_for_read();
  int get_next_major_merge_info(const int merge_version, int64_t& latest_major_table_pos, int64_t& next_major_version,
      int64_t& next_major_freeze_ts, int64_t& base_schema_version, int64_t& schema_version);
  int get_read_base_tables(const int64_t snapshot_version, ObTablesHandle& handle, bool& contain_snapshot_version);
  int get_hist_major_table(const int64_t snapshot_version, ObTablesHandle& handle);
  int get_inc_read_tables(const int64_t snapshot_version, ObTablesHandle& handle, bool& contain_snapshot_version);
  int update_replay_tables();
  int update_multi_version_start();
  int get_memtables(const bool include_active_memtable, ObTablesHandle& handle);
  int get_all_memtables(const bool include_active_memtable, ObTablesHandle& handle);
  int sort_minor_tables(ObArray<ObITable*>& tables);
  int sort_major_tables(ObArray<ObITable*>& tables);
  int get_boundary_major_sstable(const bool last, ObITable*& major_table);
  int get_boundary_minor_sstable(const bool last, ObITable*& minor_table);
  bool is_own_table(const ObITable* table);
  virtual int get_minor_schema_version(int64_t& schema_version);  // virtual is for test
  int get_all_tables_from_start(ObTablesHandle& handle);
  bool check_complete_(const ObTablesHandle& handle);
  int find_major_merge_inc_tables(
      const ObSSTable& base_table, const int64_t next_major_freeze_ts, ObTablesHandle& handle);
  int judge_need_mini_minor_merge(
      const bool using_remote_memstore, const int64_t minor_check_snapshot_version, bool& need_merge);
  int deal_with_mini_minor_result(
      const ObMergeType& merge_type, const int64_t multi_version_start, ObGetMergeTablesResult& result);
  int deal_with_minor_result(
      const ObMergeType& merge_type, const int64_t demand_multi_version_start, ObGetMergeTablesResult& result);
  int get_neighbour_freeze_info(
      const int64_t snapshot_version, ObFreezeInfoSnapshotMgr::NeighbourFreezeInfoLite& freeze_info);

  // Split Section
  bool is_spliting();
  int check_need_reference_tables(
      ObTablesHandle& old_handle, const int64_t memtable_base_version, bool& need_reference);
  int get_minor_split_table_pos(
      const ObTablesHandle& old_handle, int64_t& first_reference_pos, int64_t& last_reference_pos);
  int get_major_split_table_pos(const ObTablesHandle& old_handle, int64_t& pos);

protected:
  static const int64_t INVAID_TABLE_POS = -1;
  static const int64_t DEFAULT_SSTABLE_CNT = 6;
  static const int64_t OB_HIST_MINOR_FACTOR = 3;
  // No Need Persistence
  bool is_inited_;
  ObFreezeInfoSnapshotMgr* freeze_info_mgr_;
  // tables_: [Major SSTable | Minor SSTable | Memtable]
  ObITable* tables_[MAX_SSTABLE_CNT_IN_STORAGE];
  int64_t table_count_;
  bool is_ready_for_read_;
  ObPGMemtableMgr* pg_memtable_mgr_;
  ObSSTable* complement_minor_sstable_;

  // Need Persistence
  common::ObPartitionKey pkey_;
  uint64_t table_id_;
  int64_t uptime_;
  // The first table position actually used, tables before start_pos_ are reserved for multiple major sstables
  int64_t start_pos_;
  int64_t inc_pos_;
  common::ObSEArray<ObITable::TableKey, DEFAULT_SSTABLE_CNT> replay_tables_;
  int64_t multi_version_start_;
  OB_UNIS_VERSION_V(OB_TABLE_STORE_VERSION);
  DISALLOW_COPY_AND_ASSIGN(ObTableStore);
};

OB_INLINE bool ObTableStore::is_valid() const
{
  return is_inited_ && table_count_ >= 0 && table_count_ <= MAX_SSTABLE_CNT_IN_STORAGE && start_pos_ >= 0 &&
         inc_pos_ >= start_pos_ && inc_pos_ <= table_count_;
}

class ObLogTsCompater {
public:
  ObLogTsCompater() : version_(0), log_ts_range_(nullptr), is_base_(false)
  {}
  ~ObLogTsCompater()
  {}
  explicit ObLogTsCompater(const int64_t version, common::ObLogTsRange& log_ts_range, bool is_base)
      : version_(version), log_ts_range_(&log_ts_range), is_base_(is_base)
  {}
  int set_log_ts(const int64_t log_ts);
  bool operator<(const ObLogTsCompater& rtable) const
  {
    return version_ < rtable.version_;
  }
  bool operator==(const ObLogTsCompater& rtable) const
  {
    return version_ == rtable.version_;
  }
  bool operator>(const ObLogTsCompater& rtable) const
  {
    return version_ > rtable.version_;
  }
  TO_STRING_KV(K_(version), K_(log_ts_range), K_(is_base));
  int64_t version_;
  ObLogTsRange* log_ts_range_;
  bool is_base_;
};

class ObTableCompater {
public:
  static const int64_t OB_MAX_COMPAT_LOG_TS = MAX_SSTABLE_CNT_IN_STORAGE * MAX_SSTABLE_CNT_IN_STORAGE;
  ObTableCompater() : compaters_()
  {}
  ~ObTableCompater()
  {}
  OB_INLINE void reset()
  {
    compaters_.reset();
  }
  int add_tables(ObIArray<ObSSTable*>& sstables);
  int add_tables(ObIArray<ObMigrateTableInfo::SSTableInfo>& sstables);
  int fill_log_ts();
  TO_STRING_KV(K_(compaters));

private:
  ObSEArray<ObLogTsCompater, 8> compaters_;
};

class ObPrintableTableStore final : public ObTableStore {
public:
  virtual int64_t to_string(char* buf, const int64_t buf_len) const override;

private:
  void table_to_string(ObITable* table, const char* table_type, char* buf, const int64_t buf_len, int64_t& pos) const;
  ObPrintableTableStore() = delete;
};

#define PRETTY_TS_P(x) (reinterpret_cast<const ObPrintableTableStore*>(x))
#define PRETTY_TS(x) (reinterpret_cast<const ObPrintableTableStore&>(x))

}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_TABLE_STORE_H_ */
