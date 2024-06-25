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

#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/compaction/ob_compaction_util.h"
#include "share/ob_table_range.h"
#include "share/scheduler/ob_diagnose_config.h"
namespace oceanbase
{
namespace storage
{
class ObITable;
class ObGetMergeTablesParam;
class ObTablet;
class ObGetMergeTablesResult;
class ObTablesHandleArray;
struct ObTabletStatAnalyzer;
struct ObTableHandleV2;
struct ObStorageMetaHandle;
class ObLS;
}

namespace blocksstable
{
class ObSSTable;
}

namespace compaction
{
struct ObMinorExecuteRangeMgr;

class ObPartitionMergePolicy
{
public:
  static int get_mini_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_minor_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_hist_minor_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_medium_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_mds_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int not_support_merge_type(
      const storage::ObGetMergeTablesParam &param,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result)
  {
    return OB_NOT_SUPPORTED;
  }
   static int check_need_medium_merge(
      storage::ObLS &ls,
      storage::ObTablet &tablet,
      const int64_t medium_snapshot,
      bool &need_merge,
      bool &can_merge,
      bool &need_force_freeze);
  static int generate_parallel_minor_interval(
      const ObMergeType merge_type,
      const int64_t minor_compact_trigger,
      const storage::ObGetMergeTablesResult &input_result,
      ObMinorExecuteRangeMgr &minor_range_mgr,
      ObIArray<storage::ObGetMergeTablesResult> &parallel_result);

  static int get_boundary_snapshot_version(
      const storage::ObTablet &tablet,
      int64_t &min_snapshot,
      int64_t &max_snapshot,
      const bool check_table_cnt = false,
      const bool is_multi_version_merge = false);

  static int diagnose_table_count_unsafe(
      const compaction::ObMergeType merge_type,
      const share::ObDiagnoseTabletType diagnose_type,
      const storage::ObTablet &tablet);

  static int get_multi_version_start(
      const compaction::ObMergeType merge_type,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      ObVersionRange &result_version_range,
      ObStorageSnapshotInfo &snapshot_info);

  static int add_table_with_check(storage::ObGetMergeTablesResult &result, storage::ObTableHandleV2 &table_handle);
  static int get_result_by_snapshot(
    storage::ObTablet &tablet,
    const int64_t snapshot,
    storage::ObGetMergeTablesResult &result);
  static bool is_sstable_count_not_safe(const int64_t minor_table_cnt);

private:
  static int find_mini_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      const storage::ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      common::ObIArray<ObTableHandleV2> &memtable_handles,
      storage::ObGetMergeTablesResult &result);

  static int find_minor_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      const int64_t min_snapshot_version,
      const int64_t max_snapshot_version,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int refine_minor_merge_tables(
      const storage::ObTablet &tablet,
      const storage::ObTablesHandleArray &merge_tables,
      int64_t &left_border,
      int64_t &right_border);

private:
  static int refine_mini_merge_result(
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result,
      bool &need_check_tablet);
  static int refine_minor_merge_result(
      const ObMergeType merge_type,
      const int64_t minor_compact_trigger,
      storage::ObGetMergeTablesResult &result);

  static int deal_with_minor_result(
      const compaction::ObMergeType &merge_type,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_neighbour_freeze_info(
      const int64_t snapshot_version,
      const int64_t last_major_snapshot_version,
      storage::ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info,
      const bool is_multi_version_merge);

  static int64_t cal_hist_minor_merge_threshold();
  static int generate_input_result_array(
      const storage::ObGetMergeTablesResult &input_result,
      ObMinorExecuteRangeMgr &minor_range_mgr,
      int64_t &fixed_input_table_cnt,
      common::ObIArray<storage::ObGetMergeTablesResult> &input_result_array);

  static int split_parallel_minor_range(
      const int64_t table_count_threshold,
      const storage::ObGetMergeTablesResult &input_result,
      common::ObIArray<storage::ObGetMergeTablesResult> &parallel_result);

  static int deal_hist_minor_merge(
      const storage::ObTablet &tablet,
      int64_t &max_snapshot_version);

  // diagnose part
  static int diagnose_minor_dag(
      compaction::ObMergeType merge_type,
      const share::ObLSID ls_id,
      const common::ObTabletID tablet_id,
      char *buf,
      const int64_t buf_len);
public:
  static const int64_t OB_HIST_MINOR_FACTOR = 3;
  static const int64_t OB_UNSAFE_TABLE_CNT = 32;
  static const int64_t OB_EMERGENCY_TABLE_CNT = 56;
  static const int64_t DEFAULT_MINOR_COMPACT_TRIGGER = 2;
  static const int64_t OB_DEFAULT_COMPACTION_AMPLIFICATION_FACTOR = 25;
  static const int64_t OB_MINOR_PARALLEL_SSTABLE_CNT_TRIGGER = 20;
  static const int64_t OB_MINOR_PARALLEL_SSTABLE_CNT_IN_DAG = 10;
  static const int64_t OB_MINOR_PARALLEL_INFO_ARRAY_SIZE = MAX_SSTABLE_CNT_IN_STORAGE / OB_MINOR_PARALLEL_SSTABLE_CNT_IN_DAG;
  static const int64_t OB_LARGE_MINOR_SSTABLE_ROW_COUNT = 2000000;

  typedef int (*GetMergeTables)(const storage::ObGetMergeTablesParam&,
                                storage::ObLS &ls,
                                const storage::ObTablet &,
                                storage::ObGetMergeTablesResult&);
  static GetMergeTables get_merge_tables[compaction::ObMergeType::MERGE_TYPE_MAX];
};

struct ObMinorExecuteRangeMgr
{
  ObMinorExecuteRangeMgr()
    : exe_range_array_()
  {}
  ~ObMinorExecuteRangeMgr()
  {
    reset();
  }
  void reset()
  {
    exe_range_array_.reset();
  }

  int get_merge_ranges(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  bool in_execute_range(const storage::ObITable *table) const;
  int sort_ranges();

  ObSEArray<share::ObScnRange, ObPartitionMergePolicy::OB_MINOR_PARALLEL_INFO_ARRAY_SIZE> exe_range_array_;
};


class ObAdaptiveMergePolicy
{
public:
  enum AdaptiveMergeReason : uint8_t {
    NONE = 0,
    LOAD_DATA_SCENE = 1,
    TOMBSTONE_SCENE = 2,
    INEFFICIENT_QUERY = 3,
    FREQUENT_WRITE = 4,
    TENANT_MAJOR = 5,
    USER_REQUEST = 6,
    REBUILD_COLUMN_GROUP = 7,
    CRAZY_MEDIUM_FOR_TEST = 8,
    INVALID_REASON
  };

  enum AdaptiveCompactionPolicy : uint8 {
    NORMAL = 0,   // only medium
    ADVANCED = 1, // medium with meta
    EXTREME = 2,  // only meta
    INVALID_POLICY
  };

  static const char *merge_reason_to_str(const int64_t merge_reason);
  static bool is_valid_merge_reason(const AdaptiveMergeReason &reason);
  static bool is_valid_compaction_policy(const AdaptiveCompactionPolicy &policy);
  static bool is_schedule_medium(const share::schema::ObTableModeFlag &mode);
  static bool is_schedule_meta(const share::schema::ObTableModeFlag &mode);
  static bool take_normal_policy(const share::schema::ObTableModeFlag &mode);
  static bool take_advanced_policy(const share::schema::ObTableModeFlag &mode);
  static bool take_extrem_policy(const share::schema::ObTableModeFlag &mode);

  static int get_meta_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);

  static int get_adaptive_merge_reason(
      const storage::ObTablet &tablet,
      AdaptiveMergeReason &reason);
  static int check_tombstone_reason(
      const storage::ObTablet &tablet,
      AdaptiveMergeReason &reason);
private:
  static int find_adaptive_merge_tables(
        const ObMergeType &merge_type,
        const storage::ObTablet &tablet,
        storage::ObGetMergeTablesResult &result);
  static int add_meta_merge_result(
      storage::ObITable *table,
      const storage::ObStorageMetaHandle &table_meta_handle,
      storage::ObGetMergeTablesResult &result,
      const bool update_snapshot_flag);
private:
  static int check_load_data_situation(
      const storage::ObTabletStatAnalyzer &analyzer,
      AdaptiveMergeReason &merge_reason);
  static int check_tombstone_situation(
      const storage::ObTabletStatAnalyzer &analyzer,
      AdaptiveMergeReason &merge_reason);
  static int check_ineffecient_read(
      const storage::ObTabletStatAnalyzer &analyzer,
      AdaptiveMergeReason &merge_reason);
  static int check_inc_sstable_row_cnt_percentage(
      const storage::ObTablet &tablet,
      AdaptiveMergeReason &merge_reason);

public:
  static constexpr int64_t INC_ROW_COUNT_THRESHOLD = 100L * 1000L; // 10w
  static constexpr int64_t TOMBSTONE_ROW_COUNT_THRESHOLD = 250L * 1000L; // 25w, the same as ObFastFreezeChecker::TOMBSTONE_DEFAULT_ROW_COUNT
  static constexpr int64_t BASE_ROW_COUNT_THRESHOLD = 10L * 1000L; // 1w
  static constexpr int64_t LOAD_DATA_SCENE_THRESHOLD = 70;
  static constexpr int64_t TOMBSTONE_SCENE_THRESHOLD = 50;
  static constexpr float INC_ROW_COUNT_PERCENTAGE_THRESHOLD = 0.5;
  static constexpr int64_t TRANS_STATE_DETERM_ROW_CNT_THRESHOLD = 10000L; // 10k
  static constexpr int64_t MEDIUM_COOLING_TIME_THRESHOLD_NS = 600_s * 1000; // 1000: set precision from us to ns
};

class ObCOMajorMergePolicy
{
public:
  enum ObCOMajorMergeType : uint8_t
  {
    INVALID_CO_MAJOR_MERGE_TYPE = 0,
    BUILD_COLUMN_STORE_MERGE = 1,
    BUILD_ROW_STORE_MERGE = 2,
    REBUILD_COLUMN_STORE_MERGE = 3,
    MAX_CO_MAJOR_MERGE_TYPE = 4
  };
  static const char *co_major_merge_type_to_str(const ObCOMajorMergeType co_merge_type);
  static inline bool is_valid_major_merge_type(const ObCOMajorMergeType &major_merge_type)
  {
    return major_merge_type > INVALID_CO_MAJOR_MERGE_TYPE && major_merge_type < MAX_CO_MAJOR_MERGE_TYPE;
  }
  static inline bool is_build_column_store_merge(const ObCOMajorMergeType &major_merge_type)
  {
    return BUILD_COLUMN_STORE_MERGE == major_merge_type;
  }
  static inline bool is_build_row_store_merge(const ObCOMajorMergeType &major_merge_type)
  {
    return BUILD_ROW_STORE_MERGE == major_merge_type;
  }
  static inline bool is_rebuild_column_store_merge(const ObCOMajorMergeType &major_merge_type)
  {
    return REBUILD_COLUMN_STORE_MERGE == major_merge_type;
  }
  static int decide_co_major_sstable_status(
      const ObCOSSTableV2 &co_sstable,
      const ObStorageSchema &storage_schema,
      ObCOMajorSSTableStatus &major_sstable_status);
  static int estimate_row_cnt_for_major_merge(
      const uint64_t table_id,
      const ObIArray<ObITable *> &tables,
      const ObStorageSchema &storage_schema,
      const ObTabletHandle &tablet_handle,
      int64_t &estimate_row_cnt);
  static bool whether_to_build_row_store(
      const int64_t &estimate_row_cnt,
      const int64_t &column_cnt);
  static bool whether_to_rebuild_column_store(
      const ObCOMajorSSTableStatus &major_sstable_status,
      const int64_t &estimate_row_cnt,
      const int64_t &column_cnt);
  static int decide_co_major_merge_type(
      const ObCOSSTableV2 &co_sstable,
      const ObIArray<ObITable *> &tables,
      const ObStorageSchema &storage_schema,
      const ObTabletHandle &tablet_handle,
      ObCOMajorMergeType &major_merge_type);

private:
  // Thresholds for swtiching major sstable
  // build row store
  static const int64_t ROW_CNT_THRESHOLD_BUILD_ROW_STORE = 10000;
  static const int64_t COL_CNT_THRESHOLD_BUILD_ROW_STORE = 3;
  // rebuild column store
  static const int64_t ROW_CNT_THRESHOLD_REBUILD_COLUMN_STORE = 20000;
  static const int64_t ROW_CNT_THRESHOLD_REBUILD_ROWKEY_STORE = 16000;
  static const int64_t COL_CNT_THRESHOLD_REBUILD_COLUMN_STORE = 5;
};


} /* namespace compaction */
} /* namespace oceanbase */
#endif // OB_PARTITION_MERGE_POLICY_H_
