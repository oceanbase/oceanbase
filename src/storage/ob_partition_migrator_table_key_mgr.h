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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_MIGRATOR_TABLE_KEY_MGR_
#define OCEANBASE_STORAGE_OB_PARTITION_MIGRATOR_TABLE_KEY_MGR_

#include "common/ob_partition_key.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_i_store.h"
#include "storage/ob_i_partition_base_data_reader.h"
#include "storage/ob_freeze_info_snapshot_mgr.h"

namespace oceanbase {
namespace storage {

// common util class which src and dest all needed
class ObTableKeyMgrUtil {
  ObTableKeyMgrUtil()
  {}
  virtual ~ObTableKeyMgrUtil()
  {}

public:
  static int convert_minor_table_key(
      const storage::ObITable::TableKey& minor_table_key, storage::ObITable::TableKey& tmp_table_key);
  static int classify_mgirate_tables(const uint64_t table_id, const common::ObIArray<ObITable::TableKey>& all_tables,
      common::ObIArray<ObITable::TableKey>& major_sstables, common::ObIArray<ObITable::TableKey>& inc_sstables);
  static int convert_src_table_keys(const int64_t log_id, const int64_t table_id, const bool is_only_major_sstable,
      const ObTablesHandle& handle, ObIArray<storage::ObITable::TableKey>& table_keys);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableKeyMgrUtil);
};

// src table key manager
class ObSrcTableKeyManager {
public:
  ObSrcTableKeyManager();
  virtual ~ObSrcTableKeyManager();
  int init(const int64_t snapshot_version, const int64_t table_id, const ObPartitionKey& pkey,
      const bool is_only_major_sstable);
  int add_table_key(const ObITable::TableKey& table_key);
  int get_table_keys(common::ObIArray<ObITable::TableKey>& table_keys);

private:
  int add_major_table(const ObITable::TableKey& table_key);
  int add_minor_table(const ObITable::TableKey& table_key);
  int add_mem_table(const ObITable::TableKey& mem_table_key);
  int conver_mem_table_key(
      const ObITable::TableKey& mem_table_key, common::ObIArray<storage::ObITable::TableKey>& tmp_key_list);
  int check_and_cut_memtable_range(ObITable::TableKey& table_key);

private:
  static const int64_t RETRY_INTERVAL = 1000 * 1000L;
  bool is_inited_;
  int64_t snapshot_version_;
  int64_t max_memtable_base_version_;
  int64_t table_id_;
  ObPartitionKey pkey_;
  ObArray<storage::ObITable::TableKey> major_table_keys_;
  ObArray<storage::ObITable::TableKey> inc_table_keys_;
  bool is_only_major_sstable_;
  DISALLOW_COPY_AND_ASSIGN(ObSrcTableKeyManager);
};

class ObDestTableKeyManager {
public:
  ObDestTableKeyManager()
  {}
  virtual ~ObDestTableKeyManager()
  {}
  static int convert_needed_inc_table_key(const common::ObIArray<ObITable::TableKey>& local_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables, const bool is_copy_cover_minor,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  static int convert_needed_inc_table_key_v2(const int64_t need_reserve_major_snapshot,
      const common::ObIArray<ObITable::TableKey>& local_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables, const bool is_copy_cover_minor,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  static int convert_sstable_info_to_table_key(
      const ObMigrateTableInfo::SSTableInfo& sstable_info, ObITable::TableKey& table_key);

private:
  static int check_table_continues(const common::ObIArray<ObITable::TableKey>& tables, bool& is_continues);
  static int get_all_needed_table(const common::ObIArray<ObITable::TableKey>& local_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables, const bool is_copy_cover_minor,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  static int get_all_needed_table_with_cover_range(const common::ObIArray<ObITable::TableKey>& local_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  static int get_all_needed_table_without_cover_range(const common::ObIArray<ObITable::TableKey>& local_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables, bool& is_succ);
  static int get_all_needed_table_v2(const int64_t need_reserve_major_snapshot,
      const common::ObIArray<ObITable::TableKey>& local_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables, const bool is_copy_cover_minor,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  static int get_all_needed_table_without_cover_range_v2(const common::ObIArray<ObITable::TableKey>& local_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_gc_inc_tables,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables, bool& is_succ);
  static int get_all_needed_table_with_cover_range_v2(const int64_t need_reserve_major_snapshot,
      const common::ObIArray<ObITable::TableKey>& local_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_inc_tables,
      common::ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables);
  static int check_can_use_remote_to_cover_local(const common::ObIArray<ObITable::TableKey>& local_inc_tables,
      const common::ObIArray<ObITable::TableKey>& remote_inc_tables, const int64_t remote_start_copy_pos,
      const int64_t need_reserve_major_snapshot, bool& can_use_remote_to_cover_local);

  DISALLOW_COPY_AND_ASSIGN(ObDestTableKeyManager);
};

struct ObTableKeyVersionRangeCompare {
  bool operator()(const ObITable::TableKey& left, const ObITable::TableKey& right);
};

struct ObTableKeyCompare {
  bool operator()(const ObITable::TableKey& left, const ObITable::TableKey& right);
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_PARTITION_MIGRATOR_TABLE_KEY_MGR_ */
