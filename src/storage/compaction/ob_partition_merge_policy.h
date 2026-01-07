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
struct ObMediumCompactionInfo;

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
#ifdef OB_BUILD_SHARED_STORAGE
  static int get_ss_minor_merge_tables(
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);
#endif
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
  static int get_inc_major_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);
  static int get_mds_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);
  static int get_convert_co_major_merge_tables(
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
  static int get_co_major_minor_merge_tables(
    const ObStorageSchema *storage_schema,
    const int64_t merge_version,
    const int64_t start_pos,
    const ObTablesHandleArray &input_tables,
    ObIArray<ObTableHandleV2> &output_tables);

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
      const ObMergeType merge_type,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      ObVersionRange &result_version_range,
      ObStorageSnapshotInfo &snapshot_info);

  static int add_table_with_check(storage::ObGetMergeTablesResult &result, storage::ObTableHandleV2 &table_handle);
  static int get_result_by_snapshot(
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      const int64_t snapshot,
      storage::ObGetMergeTablesResult &result,
      const bool need_check_tablet);
  static bool is_sstable_count_not_safe(const int64_t minor_table_cnt);

private:
  static int find_mini_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      const int64_t max_snapshot_version,
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
  static int refine_mini_merge_result(
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result,
      bool &need_check_tablet);
  static int refine_and_get_minor_merge_result(
      const ObGetMergeTablesParam &param,
      const ObTablet &tablet,
      const int64_t minor_compact_trigger,
      ObTablesHandleArray &tables,
      ObGetMergeTablesResult &result);
  static int refine_minor_merge_result(
      const compaction::ObMergeType merge_type,
      const int64_t minor_compact_trigger,
      const bool is_tablet_referenced_by_collect_mv,
      storage::ObGetMergeTablesResult &result);
  static int deal_with_minor_result(
      const compaction::ObMergeType &merge_type,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);
  static int64_t cal_hist_minor_merge_threshold(const bool is_tablet_referenced_by_collect_mv = false);
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
#ifdef OB_BUILD_SHARED_STORAGE
  static int get_ss_minor_boundary_snapshot_version(
      ObLS &ls,
      const ObTablet &tablet,
      int64_t &min_snapshot,
      int64_t &max_snapshot);
  static int deal_with_ss_minor_result(
      ObLS &ls,
      const ObTablet &tablet,
      ObGetMergeTablesResult &result);
#endif
  // diagnose part
  static int diagnose_minor_dag(
      compaction::ObMergeType merge_type,
      const share::ObLSID ls_id,
      const common::ObTabletID tablet_id,
      char *buf,
      const int64_t buf_len);

  static int schedule_co_major_minor_errsim(
    const ObTablesHandleArray &input_tables,
    const int64_t start_pos,
    ObIArray<ObTableHandleV2> &output_tables);
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
  static const int64_t SCHEDULE_CO_MAJOR_MINOR_CG_CNT_THREASHOLD = 20;
  static const int64_t SCHEDULE_CO_MAJOR_MINOR_TRIGGER = 3;
  static const int64_t SCHEDULE_CO_MAJOR_MINOR_ROW_CNT_THREASHOLD = 100 * 1000L;

  typedef int (*GetMergeTables)(const storage::ObGetMergeTablesParam&,
                                storage::ObLS &ls,
                                const storage::ObTablet &,
                                storage::ObGetMergeTablesResult&);
  static GetMergeTables get_merge_tables[];
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
    REBUILD_COLUMN_GROUP = 7, // use row_store to rebuild column_store(when column store have error)
    CRAZY_MEDIUM_FOR_TEST = 8,
    // no incremental data(MEMTABLE/MINI/MINOR) after last major
    NO_INC_DATA = 9,
    // no major sstable / table schema is hidden or invalid index
    DURING_DDL = 10,
    RECYCLE_TRUNCATE_INFO = 11,
    TOO_MANY_INC_MAJOR = 12,
    INVALID_REASON
  };

  enum AdaptiveCompactionPolicy : uint8 {
    NORMAL = 0,   // only medium
    ADVANCED = 1, // medium with meta
    EXTREME = 2,  // only meta
    INVALID_POLICY
  };

  enum AdaptiveCompactionEvent : uint8_t {
    SCHEDULE_MEDIUM = 0,
    SCHEDULE_META = 1,
    SCHEDULE_AFTER_MINI = 2,
    INVALID_EVENT
  };

  static const char *merge_reason_to_str(const int64_t merge_reason);
  static bool is_valid_merge_reason(const AdaptiveMergeReason &reason);
  static bool is_user_request_merge_reason(const AdaptiveMergeReason &reason);
  static bool is_skip_merge_reason(const AdaptiveMergeReason &reason);
  static bool is_recycle_truncate_info_merge_reason(const AdaptiveMergeReason &reason);
  static bool is_valid_compaction_policy(const AdaptiveCompactionPolicy &policy);
  static bool is_schedule_medium(const share::schema::ObTableModeFlag &mode);
  static bool is_schedule_meta(const share::schema::ObTableModeFlag &mode);
  static bool take_normal_policy(const share::schema::ObTableModeFlag &mode);
  static bool take_advanced_policy(const share::schema::ObTableModeFlag &mode);
  static bool take_extrem_policy(const share::schema::ObTableModeFlag &mode);
  static bool need_schedule_meta(const AdaptiveCompactionEvent& event);
  static bool need_schedule_medium(const AdaptiveCompactionEvent& event);

  static int get_meta_merge_tables(
      const storage::ObGetMergeTablesParam &param,
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);
  static int get_adaptive_merge_reason(
      storage::ObTablet &tablet,
      AdaptiveMergeReason &reason,
      int64_t &least_medium_snapshot);
  static int check_adaptive_merge_reason(
      const storage::ObTablet &tablet,
      const ObTabletStatAnalyzer &tablet_analyzer,
      AdaptiveMergeReason &reason);
  static int check_adaptive_merge_reason_for_event(
      const storage::ObLS &ls,
      const storage::ObTablet &tablet,
      const AdaptiveCompactionEvent &event,
      const int64_t update_row_cnt,
      const int64_t delete_row_cnt,
      ObTableModeFlag &mode,
      AdaptiveMergeReason &reason);
  static int check_tombstone_reason(
      const storage::ObTablet &tablet,
      AdaptiveMergeReason &reason);
  static int find_adaptive_merge_tables(
      const ObMergeType &merge_type,
      const storage::ObTablet &tablet,
      storage::ObGetMergeTablesResult &result);
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
  static int check_incremental_table(
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
  static const int64_t RECYCLE_TRUNCATE_INFO_THRESHOLD = 5;
};

/*
  SCHEMA_TYPE
  1) ALL+EACH
  ALL+EACH --(BUILD_ROW_STORE_MERGE)--> ALL --(USE_RS_BUILD_SCHEMA_MATCH_MERGE)--> ALL+EACH
  ALL+EACH --(BUILD_COLUMN_STORE_MERGE)--> ALL+EACH
  EACH --(BUILD_REDUNDANT_ROW_STORE_MERGE)--> ALL+EACH
  2) EACH
  EACH --(BUILD_ROW_STORE_MERGE)--> ALL --(USE_RS_BUILD_SCHEMA_MATCH_MERGE)--> EACH
  EACH --(BUILD_COLUMN_STORE_MERGE)--> EACH

  BUILD_COLUMN_STORE_MERGE vs. USE_RS_BUILD_SCHEMA_MATCH_MERGE
  SAME : output a major sstable match schema
  DIFF : BUILD_COLUMN_STORE_MERGE could reuse CG macro from old major, USE_RS_BUILD_SCHEMA_MATCH_MERGE can't reuse macro
*/

class ObCOMajorMergePolicy
{
public:
  enum ObCOMajorMergeType : uint8_t
  {
    INVALID_CO_MAJOR_MERGE_TYPE = 0,
    BUILD_COLUMN_STORE_MERGE = 1,
    BUILD_ROW_STORE_MERGE = 2,
    USE_RS_BUILD_SCHEMA_MATCH_MERGE = 3,
    BUILD_REDUNDANT_ROW_STORE_MERGE = 4, // only for cs replica now
    MAX_CO_MAJOR_MERGE_TYPE = 5
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
  static inline bool is_use_rs_build_schema_match_merge(const ObCOMajorMergeType &major_merge_type)
  {
    return USE_RS_BUILD_SCHEMA_MATCH_MERGE == major_merge_type;
  }
  static inline bool is_build_redundent_row_store_merge(const ObCOMajorMergeType &major_merge_type)
  {
    return BUILD_REDUNDANT_ROW_STORE_MERGE == major_merge_type;
  }
  static int decide_co_major_sstable_status(
      const ObCOSSTableV2 &co_sstable,
      const ObStorageSchema &storage_schema,
      ObCOMajorSSTableStatus &major_sstable_status);
  static bool whether_to_build_row_store(
      const int64_t &estimate_row_cnt,
      const int64_t &column_cnt);
  static bool whether_to_rebuild_column_store(
      const ObCOMajorSSTableStatus &major_sstable_status,
      const int64_t &estimate_row_cnt,
      const int64_t &column_cnt);
  static int accumulate_physical_row_cnt(
      const ObIArray<ObITable *> &tables,
      int64_t &physical_row_cnt);
  static int decide_co_major_merge_type(
      const ObCOSSTableV2 &co_sstable,
      const ObIArray<ObITable *> &tables,
      const ObStorageSchema &storage_schema,
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


// Describes the strategy for this major merge
struct ObCOMajorMergeStrategy
{
  OB_UNIS_VERSION(1);
public:
  ObCOMajorMergeStrategy() { reset(); }
  void reset() {
    MEMSET(this, 0, sizeof(*this));
  }
  // Set merge strategy
  // @param build_all_cg_only: true = build ALL CG ONLY, false = build schema match merge
  // @param only_use_row: true = only use row store data to build column store
  void set(bool build_all_cg_only, bool only_use_row) {
    is_valid_ = true;
    build_all_cg_only_ = build_all_cg_only;
    only_use_row_store_ = only_use_row;
  }
  inline bool is_valid() const { return is_valid_; }
  inline bool is_build_all_cg_only() const { return build_all_cg_only_; }
  inline bool only_use_row_store() const { return only_use_row_store_; }
  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const {}
  int64_t to_string(char *buf, const int64_t buf_len) const { return 0; }
  ObCOMajorMergeStrategy &operator=(const ObCOMajorMergeStrategy &other) {
    is_valid_ = other.is_valid_;
    build_all_cg_only_ = other.build_all_cg_only_;
    only_use_row_store_ = other.only_use_row_store_;
    return *this;
  }
  bool is_valid_;
  bool build_all_cg_only_;
  bool only_use_row_store_;
};

class ObIncMajorTxHelper final
{
public:
  static int get_inc_major_commit_version(
    ObLS &ls,
    const blocksstable::ObSSTable &inc_major_table,
    const share::SCN &read_tx_scn,
    int64_t &trans_state,
    int64_t &commit_version);
  static int check_inc_major_trans_can_read(
      ObLS *ls,
      const transaction::ObTransID &trans_id,
      const transaction::ObTxSEQ &seq_no,
      const share::SCN &read_scn,
      int64_t &trans_state,
      bool &can_read,
      share::SCN &trans_version);

  static int get_trans_id_and_seq_no_from_sstable(
      const blocksstable::ObSSTable *sstable,
      transaction::ObTransID &trans_id,
      transaction::ObTxSEQ &seq_no);

  static int check_can_access(ObTableAccessContext &context,
                              const transaction::ObTransID &trans_id,
                              const transaction::ObTxSEQ &seq_no,
                              const share::SCN &max_scn,
                              bool &can_access);
  static int check_can_access(ObTableAccessContext &context,
                              const ObUncommitTxDesc &tx_desc,
                              const share::SCN &max_scn,
                              bool &can_access);
  static int check_can_access(ObTableAccessContext &context,
                              const blocksstable::ObSSTable &sstable,
                              bool &can_access);

  static int check_inc_major_table_status(
      const compaction::ObMediumCompactionInfo &medium_info,
      const ObMergeType merge_type,
      const int64_t &merge_snapshot_version,
      const ObTablesHandleArray &candidates,
      const bool is_cs_replica);
  static int find_inc_major_sstable(
    const ObSSTableArray &inc_major_array,
    const blocksstable::ObSSTable *inc_major_ddl_sstable,
    bool &found);
  static int find_inc_major_sstable(
      const common::ObIArray<ObITable *> &inc_major_sstables,
      const blocksstable::ObSSTable *inc_major_ddl_sstable,
      bool &found);
  static int check_inc_major_exist(
      const ObTabletHandle &tablet_handle,
      const transaction::ObTransID &trans_id,
      const transaction::ObTxSEQ &seq_no,
      bool &is_exist);
  static int check_inc_major_exist(
      const common::ObIArray<ObITable *> &inc_major_sstables,
      const transaction::ObTransID &trans_id,
      const transaction::ObTxSEQ &seq_no,
      bool &is_exist);
  static int get_ls(const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static int check_need_gc_ddl_dump(
      const ObTablet &tablet,
      const blocksstable::ObSSTable &ddl_dump,
      bool &need_gc);
  static int check_inc_major_included_by_major(
      ObLS &ls,
      const int64_t major_version,
      const blocksstable::ObSSTable &sstable,
      bool &is_included);
  static int check_all_inc_major_included_by_major(
      ObLS &ls,
      const ObTablet &tablet,
      bool &all_included);
private:
  static void dump_inc_major_error_info(
      const int64_t merge_snapshot_version,
      const ObMergeType merge_type,
      const ObIArray<ObITable *> &sstables,
      const ObMediumCompactionInfo &medium_info);
};



} /* namespace compaction */
} /* namespace oceanbase */
#endif // OB_PARTITION_MERGE_POLICY_H_
