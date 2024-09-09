//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_FUNC_H_
#define OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_FUNC_H_

#include "storage/ls/ob_ls.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "share/tablet/ob_tablet_filter.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "share/ob_tablet_replica_checksum_operator.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_tenant_medium_checker.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_ckm_error_tablet_info.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction/ob_ls_compaction_list.h"
#endif

namespace oceanbase
{
namespace compaction
{
struct ObTabletSchedulePair;

class ObMediumCompactionScheduleFunc
{
public:
  ObMediumCompactionScheduleFunc(
    ObLS &ls,
    ObTabletHandle &tablet_handle,
    const SCN &weak_read_ts,
    const ObMediumCompactionInfoList &medium_info_list,
    ObScheduleStatistics *schedule_stat,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::NONE)
    : allocator_("MediumSchedule"),
      ls_(ls),
      tablet_handle_(tablet_handle),
      weak_read_ts_(weak_read_ts.get_val_for_tx()),
      medium_info_list_(&medium_info_list),
      schedule_stat_(schedule_stat),
      merge_reason_(merge_reason)
  {}
  ~ObMediumCompactionScheduleFunc() {}

  static int schedule_tablet_medium_merge(
      ObLS &ls,
      ObTablet &tablet,
      ObTabletSchedulePair &schedule_pair,
      bool &create_dag_flag,
      const int64_t major_frozen_scn,
      const bool scheduler_called,
      const ObExecMode exec_mode);
  /*
   * see
   * standby tenant should catch up broadcast scn when freeze info is recycled
   */
  static int decide_standy_tenant_schedule(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const ObMediumCompactionInfo::ObCompactionType &compaction_type,
      const int64_t schedule_scn,
      const int64_t major_frozen_snapshot,
      const ObMediumCompactionInfoList &medium_list,
      bool &schedule_flag);
  static int read_medium_info_from_list(
      const ObMediumCompactionInfoList &medium_list,
      const int64_t major_frozen_snapshot,
      const int64_t last_major_snapshot,
      ObMediumCompactionInfo::ObCompactionType &compaction_type,
      int64_t &schedule_scn);
  static int is_election_leader(const share::ObLSID &ls_id, bool &ls_election_leader);
  static int get_max_sync_medium_scn(
    const ObTablet &tablet,
    const ObMediumCompactionInfoList &medium_list,
    int64_t &max_sync_medium_scn);
  static int get_table_schema_to_merge(
    ObMultiVersionSchemaService &schema_service,
    const ObTablet &tablet,
    const int64_t schema_version,
    const int64_t medium_compat_version,
    ObIAllocator &allocator,
    storage::ObStorageSchema &storage_schema,
    bool &is_skip_merge_index);
  static int batch_check_medium_finish(
    const hash::ObHashMap<ObLSID, share::ObLSInfo> &ls_info_map,
    ObIArray<ObTabletCheckInfo> &finish_tablet_ls_infos,
    const ObIArray<ObTabletCheckInfo> &tablet_ls_infos,
    ObCompactionTimeGuard &time_guard,
    const share::ObLSColumnReplicaCache &ls_cs_replica_cache);
  static int check_replica_checksum_items(
      const ObReplicaCkmArray &checksum_items,
      const ObLSColumnReplicaCache &ls_cs_replica_cache,
      const bool is_medium_checker);
  static int check_need_merge_and_schedule(
      ObLS &ls,
      ObTablet &tablet,
      const int64_t schedule_scn,
      const ObExecMode exec_mode,
      bool &tablet_need_freeze_flag,
      bool &create_dag_flag);
  int schedule_next_medium_for_leader(
    const int64_t major_snapshot,
    const bool is_tombstone,
    bool &medium_clog_submitted);
#ifdef OB_BUILD_SHARED_STORAGE
  // medium compaction is not considered
  int prepare_ls_major_merge_info(
    const int64_t merge_version,
    ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
    bool &submit_clog_flag);
  int check_tablet_inc_data(
    ObTablet &tablet,
    ObMediumCompactionInfo &medium_info,
    bool &no_inc_data);
  int check_progressive_merge(
    ObTablet &tablet,
    const storage::ObStorageSchema &storage_schema,
    bool &is_progressive_merge);
#endif

  int64_t to_string(char* buf, const int64_t buf_len) const;
protected:
  int decide_medium_snapshot(bool &medium_clog_submitted);
  static int get_status_from_inner_table(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      share::ObTabletCompactionScnInfo &ret_info);
  int prepare_medium_info(
    const ObGetMergeTablesResult &result,
    const int64_t schema_version,
    ObMediumCompactionInfo &medium_info);
  int init_parallel_range_and_schema_changed_and_co_merge_type(
      const ObGetMergeTablesResult &result,
      ObMediumCompactionInfo &medium_info);
  int check_if_schema_changed(ObMediumCompactionInfo &medium_info);
  int init_co_major_merge_type(
      const ObGetMergeTablesResult &result,
      ObMediumCompactionInfo &medium_info);
  static int get_result_for_major(
      ObTablet &tablet,
      const ObMediumCompactionInfo &medium_info,
      ObGetMergeTablesResult &result);
  int prepare_iter(
      const ObGetMergeTablesResult &result,
      ObTableStoreIterator &table_iter);
  int submit_medium_clog(ObMediumCompactionInfo &medium_info);
  static int batch_check_medium_meta_table(
      const ObIArray<ObTabletCheckInfo> &tablet_ls_infos,
      const hash::ObHashMap<ObLSID, share::ObLSInfo> &ls_info_map,
      ObIArray<ObTabletCheckInfo> &finish_tablet_ls,
      ObCompactionTimeGuard &time_guard);
  static int check_medium_meta_table(
      const int64_t medium_snapshot,
      const ObTabletInfo &tablet_info,
      const share::ObTabletReplicaFilterHolder &filters,
      const hash::ObHashMap<ObLSID, share::ObLSInfo> &ls_info_map,
      bool &merge_finish);
  static int init_tablet_filters(share::ObTabletReplicaFilterHolder &filters);
  static int check_tablet_checksum(
      const share::ObReplicaCkmArray &checksum_items,
      const ObLSColumnReplicaCache &ls_cs_replica_cache,
      const int64_t start_idx,
      const int64_t end_idx,
      const bool is_medium_checker,
      ObIArray<ObCkmErrorTabletLSInfo> &error_pairs,
      int &check_ret);
  int choose_medium_snapshot(
      const int64_t max_sync_medium_scn,
      ObMediumCompactionInfo &medium_info,
      ObGetMergeTablesResult &result,
      int64_t &schema_version);
  int choose_major_snapshot(
      const int64_t max_sync_medium_scn,
      ObMediumCompactionInfo &medium_info,
      ObGetMergeTablesResult &result,
      int64_t &schema_version);
  int find_valid_freeze_info(
      ObMediumCompactionInfo &medium_info,
      share::ObFreezeInfo &freeze_info,
      bool &force_schedule_medium_merge);
  int switch_to_choose_medium_snapshot(
    const int64_t freeze_version,
    ObMediumCompactionInfo &medium_info,
    int64_t &schema_version);

  int schedule_next_medium_primary_cluster(
    const int64_t major_snapshot,
    const bool is_tombstone,
    bool &medium_clog_submitted);

  int choose_new_medium_snapshot(
    const int64_t max_reserved_snapshot,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result,
    int64_t &schema_version);
  int get_max_reserved_snapshot(int64_t &max_reserved_snapshot);
  static int get_table_id(
      ObMultiVersionSchemaService &schema_service,
      const ObTabletID &tablet_id,
      const int64_t schema_version,
      uint64_t &table_id);

  int check_frequency(
    const int64_t max_reserved_snapshot,
    const int64_t medium_snapshot);
  int choose_scn_for_user_request(
    const int64_t max_sync_medium_scn,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result,
    int64_t &schema_version);
  int get_adaptive_reason(const int64_t schedule_major_snapshot);
  static const int64_t DEFAULT_SCHEDULE_MEDIUM_INTERVAL = 60L * 1000L * 1000L; // 60s
  static constexpr double SCHEDULE_RANGE_INC_ROW_COUNT_PERCENRAGE_THRESHOLD = 0.2;
  static const int64_t SCHEDULE_RANGE_ROW_COUNT_THRESHOLD = 1000 * 1000L; // 100w
  static bool is_user_request(const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason)
  {
    return ObAdaptiveMergePolicy::USER_REQUEST == merge_reason
      || ObAdaptiveMergePolicy::REBUILD_COLUMN_GROUP == merge_reason
      || ObAdaptiveMergePolicy::CRAZY_MEDIUM_FOR_TEST == merge_reason;
  }
#ifdef ERRSIM
  int errsim_choose_medium_snapshot(
    const int64_t max_sync_medium_scn,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result);
#endif
private:
  ObArenaAllocator allocator_;
  ObLS &ls_;
  ObTabletHandle tablet_handle_;
  const int64_t weak_read_ts_; // weak_read_ts_ should get before tablet
  const ObMediumCompactionInfoList *medium_info_list_;
  ObScheduleStatistics *schedule_stat_;
  ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason_;
};

} //namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_FUNC_H_
