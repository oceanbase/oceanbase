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

#ifndef OB_PARTITION_MERGE_POLICY_H_
#define OB_PARTITION_MERGE_POLICY_H_

#include "storage/memtable/ob_memtable.h"
#include "ob_tenant_freeze_info_mgr.h"

namespace oceanbase
{
namespace storage
{
class ObITable;
class ObGetMergeTablesParam;
class ObSSTableArray;
class ObTablet;
class ObTabletTableStore;
class ObGetMergeTablesResult;
class ObTablesHandleArray;
class ObStorageSchema;
enum ObMergeType;
}
using namespace storage;

namespace compaction
{
class ObPartitionMergePolicy
{
public:
  static int get_mini_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      const int64_t multi_version_start,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_mini_minor_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      const int64_t multi_version_start,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_hist_minor_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      const int64_t multi_version_start,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_buf_minor_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      const int64_t multi_version_start,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_major_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      const int64_t multi_version_start,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int check_need_mini_merge(
      const storage::ObTablet &tablet,
      bool &need_merge);

  static int check_need_mini_minor_merge(
      const storage::ObTablet &tablet,
      bool &need_merge);

  static int check_need_hist_minor_merge(
      const storage::ObTablet &tablet,
      bool &need_merge);

  static int check_need_buf_minor_merge(
      const storage::ObTablet &tablet,
      bool &need_merge);

  static int check_need_major_merge(
      const storage::ObTablet &tablet,
      int64_t &merge_version,
      bool &need_merge,
      bool &can_merge,
      bool &need_frorce_freeze);

  static int diagnose_table_count_unsafe(
      const storage::ObMergeType &merge_type,
      const storage::ObTablet &tablet);
private:
  static int find_mini_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      const storage::ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info,
      const storage::ObTablet &tablet,
      ObIArray<storage::ObTableHandleV2> &memtable_handles,
      storage::ObGetMergeTablesResult &result);

  static int find_mini_minor_merge_tables(
      const ObGetMergeTablesParam &param,
      const int64_t min_snapshot_version,
      const int64_t max_snapshot_version,
      const int64_t expect_multi_version_start,
      const ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int find_buf_minor_merge_tables(
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult *result = nullptr);
  static int find_buf_minor_base_table(
      storage::ObITable *last_major_table,
      storage::ObITable *&buf_minor_base_table);

  static int add_buf_minor_merge_result(storage::ObITable *table, storage::ObGetMergeTablesResult &result);

  static int refine_mini_merge_result(
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);
  static int refine_mini_minor_merge_result(storage::ObGetMergeTablesResult &result);

  static int deal_with_minor_result(
      const storage::ObMergeType &merge_type,
      const int64_t expect_multi_version_start,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_boundary_snapshot_version(
      const ObTablet &tablet,
      int64_t &min_snapshot,
      int64_t &max_snapshot);

  static storage::ObITable *get_latest_sstable(const storage::ObTabletTableStore &table_store);

  static int get_neighbour_freeze_info(
      const int64_t snapshot_version,
      const storage::ObITable *last_major_table,
      storage::ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info);

  static int64_t cal_hist_minor_merge_threshold();

  static int deal_hist_minor_merge(
      const ObTablet &tablet,
      int64_t &max_snapshot_version);

  static bool check_table_count_safe(const storage::ObTabletTableStore &table_store);
  // diagnose part
  static int diagnose_minor_dag(
      storage::ObMergeType merge_type,
      const share::ObLSID ls_id,
      const ObTabletID tablet_id,
      char *buf,
      const int64_t buf_len);
public:
  static const int64_t OB_HIST_MINOR_FACTOR = 3;
  static const int64_t OB_UNSAFE_TABLE_CNT = 32;
  static const int64_t OB_EMERGENCY_TABLE_CNT = 56;
  static const int64_t DEFAULT_MINOR_COMPACT_TRIGGER = 2;

  typedef int (*GetMergeTables)(const storage::ObGetMergeTablesParam&,
                                const int64_t,
                                const storage::ObTablet &,
                                storage::ObGetMergeTablesResult&);
  static GetMergeTables get_merge_tables[storage::ObMergeType::MERGE_TYPE_MAX];

  typedef int (*CheckNeedMerge)(const storage::ObTablet&, bool&);
  static CheckNeedMerge check_need_minor_merge[storage::ObMergeType::MERGE_TYPE_MAX];
};

} /* namespace compaction */
} /* namespace oceanbase */
#endif // OB_PARTITION_MERGE_POLICY_H_
