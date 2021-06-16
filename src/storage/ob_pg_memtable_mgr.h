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

#ifndef OCEANBASE_STORAGE_OB_PG_MEMTABLE_MGR
#define OCEANBASE_STORAGE_OB_PG_MEMTABLE_MGR

#include "common/ob_partition_key.h"
#include "common/storage/ob_freeze_define.h"
#include "lib/hash/ob_dchash.h"
#include "lib/lock/ob_mutex.h"
#include "lib/metrics/ob_meter.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_pg_partition.h"
//#include "storage/ob_i_table.h"

namespace oceanbase {

namespace storage {
class ObIPartitionComponentFactory;
class ObTableHandle;

class ObPGMemtableMgr {
public:
  ObPGMemtableMgr();
  ~ObPGMemtableMgr()
  {
    destroy();
  }
  void reset();
  void destroy();

  // create the first memtable when partition is creating;
  int create_memtable(const ObDataStorageInfo& data_info, const common::ObPartitionKey& pkey);

  int new_active_memstore(const common::ObPartitionKey& pkey, int64_t& protection_clock);
  int effect_new_active_memstore(const ObSavedStorageInfoV2& info, const common::ObPartitionKey& pkey,
      const common::ObReplicaType& replica_type, const bool emergency);
  int clean_new_active_memstore();
  // partition split related
  int complete_active_memstore(const ObSavedStorageInfoV2& info);

  bool has_active_memtable();
  int get_active_memtable(ObTableHandle& handle);
  int64_t get_memtable_count() const;
  int get_memtables(ObTablesHandle& handle, const bool reset_handle = true, const int64_t start_point = -1,
      const bool include_active_memtable = true);
  int get_memtables_v2(ObTablesHandle& handle, const int64_t start_log_ts, const int64_t start_snapshot_version,
      const bool reset_handle = true, const bool include_active_memtable = true);
  int get_all_memtables(ObTablesHandle& handle);
  int get_memtables_nolock(ObTablesHandle& handle);
  bool has_memtable();
  void clean_memtables();
  int release_head_memtable(memtable::ObMemtable* memtable);
  int get_first_frozen_memtable(ObTableHandle& handle);

  // partition split related
  int push_reference_tables(const ObTablesHandle& ref_memtables);
  int update_readable_info(const ObPartitionReadableInfo& readable_info);
  const ObPartitionReadableInfo& get_readable_info() const
  {
    return cur_readable_info_;
  }
  int64_t get_readable_ts() const
  {
    return cur_readable_info_.max_readable_ts_;
  }
  void set_pkey(const common::ObPartitionKey& pkey)
  {
    pkey_ = pkey;
  }
  const ObPartitionKey& get_pkey() const
  {
    return pkey_;
  }
  int update_memtable_schema_version(const int64_t schema_version);

  DECLARE_VIRTUAL_TO_STRING;

private:
  // minor freeze
  int save_frozen_base_storage_info_(const ObSavedStorageInfoV2& info);
  int wait_old_memtable_release_();
  int freeze_and_add_memtable_(
      const common::ObReplicaType& replica_type, memtable::ObMemtable* memtable, const bool emergency);
  int freeze_active_memtable_(int64_t freeze_log_ts, int64_t snapshot_version, const bool emergency);
  int add_memtable_(memtable::ObMemtable* memtable);

  int64_t get_memtable_idx_(const int64_t pos) const;
  int64_t get_memtable_count_() const;
  int64_t get_unmerged_memtable_count_() const;
  memtable::ObMemtable* get_active_memtable_();
  int get_memtables_(ObTablesHandle& handle, const int64_t start_point, const bool include_active_memtable);
  int add_tables_(const int64_t start_pos, const bool include_active_memtable, ObTablesHandle& handle);
  memtable::ObMemtable* get_memtable_(const int64_t pos) const;
  int find_start_pos_(const int64_t start_point, int64_t& start_pos);
  int find_start_pos_(const int64_t start_log_ts, const int64_t start_snapshot_version, int64_t& start_pos);
  void invalidate_readable_info_();
  memtable::ObMemtable* get_first_frozen_memtable_();

  DISALLOW_COPY_AND_ASSIGN(ObPGMemtableMgr);

private:
  static const int64_t PRINT_READABLE_INFO_DURATION_US = 1000 * 1000 * 60 * 10L;  // 10min
  static const int64_t MAX_MEMSTORE_CNT = 16;

private:
  int64_t memtable_head_;
  int64_t memtable_tail_;
  memtable::ObMemtable* memtables_[MAX_MEMSTORE_CNT];
  ObTableHandle new_active_memstore_handle_;
  mutable common::TCRWLock lock_;

  common::ObPartitionKey pkey_;
  ObPartitionReadableInfo last_readable_info_;
  ObPartitionReadableInfo tmp_readable_info_;
  ObPartitionReadableInfo cur_readable_info_;
  int64_t last_print_readable_info_ts_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PG_STORAGE
