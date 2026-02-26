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
    const SCN &weak_read_ts,
    const ObMediumCompactionInfoList &medium_info_list,
    ObScheduleTabletCnt *schedule_tablet_cnt,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::NONE,
    const int64_t least_medium_snapshot = 0)
    : allocator_("MediumSchedule"),
      ls_(ls),
      weak_read_ts_(weak_read_ts.get_val_for_tx()),
      medium_info_list_(&medium_info_list),
      schedule_tablet_cnt_(schedule_tablet_cnt),
      merge_reason_(merge_reason),
      least_medium_snapshot_(least_medium_snapshot)
  {}
  ~ObMediumCompactionScheduleFunc() {}
  int init_tablet_handle(ObTabletHandle &tablet_handle)
  {
    return tablet_handle_.assign(tablet_handle);
  }
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
  static int is_election_leader(const share::ObLSID &ls_id, bool &ls_election_leader);
  static int get_max_sync_medium_scn(
    const ObTablet &tablet,
    const ObMediumCompactionInfoList &medium_list,
    int64_t &max_sync_medium_scn);
  static int get_table_schema_to_merge(
    ObMultiVersionSchemaService &schema_service,
    const ObTablet &tablet,
    const int64_t schema_version,
    const int64_t data_version,
    ObIAllocator &allocator,
    storage::ObStorageSchema &storage_schema);
  static int batch_check_medium_finish(
    const hash::ObHashMap<ObLSID, share::ObLSInfo> &ls_info_map,
    ObIArray<ObTabletCheckInfo> &finish_tablet_ls_infos,
    const ObIArray<ObTabletCheckInfo> &tablet_ls_infos,
    ObCompactionTimeGuard &time_guard);
  static int check_replica_checksum_items(
      const ObReplicaCkmArray &checksum_items,
      const bool is_medium_checker);
  int schedule_next_medium_for_leader(
    const int64_t major_snapshot,
    bool &medium_clog_submitted);
  static int get_table_id(
      ObMultiVersionSchemaService &schema_service,
      const ObTabletID &tablet_id,
      const int64_t schema_version,
      uint64_t &table_id);
  static int check_if_schema_changed(
    const ObTablet &tablet,
    const ObStorageSchema &storage_schema,
    const uint64_t data_version,
    bool &is_schema_changed);
  static int check_if_table_schema_changed(
    const ObSSTable &major_sstable,
    const int64_t major_column_count,
    const common::ObRowStoreType major_row_store_type,
    const common::ObCompressorType major_compressor_type,
    const ObStorageSchema &storage_schema,
    const uint64_t data_version,
    bool &is_schema_changed);
#ifdef OB_BUILD_SHARED_STORAGE
  // medium compaction is not considered
  int try_skip_merge_for_ss(
    const int64_t merge_version,
    share::ObFreezeInfo &freeze_info,
    ObMediumCompactionInfo &medium_info,
    bool &skip);
  int check_tablet_inc_data(
    ObTablet &tablet,
    ObMediumCompactionInfo &medium_info,
    bool &no_inc_data);
  int check_progressive_merge(
    const storage::ObTabletTableStore &table_store,
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
  int choose_encoding_limit(ObMediumCompactionInfo &medium_info);
  int init_parallel_range_and_schema_changed_and_co_merge_type(
      const ObGetMergeTablesResult &result,
      ObMediumCompactionInfo &medium_info);
  int check_if_schema_changed(ObMediumCompactionInfo &medium_info);
  int init_co_major_merge_type(
      const ObGetMergeTablesResult &result,
      ObMediumCompactionInfo &medium_info);
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
      const int64_t start_idx,
      const int64_t end_idx,
      const bool is_medium_checker,
      ObTabletDataChecksumChecker &data_checksum_checker,
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
    bool &medium_clog_submitted);

  int choose_new_medium_snapshot(
    const int64_t max_reserved_snapshot,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result,
    int64_t &schema_version);
  int get_max_reserved_snapshot(int64_t &max_reserved_snapshot);
  int check_frequency(
    const int64_t max_reserved_snapshot,
    const int64_t medium_snapshot);
  int choose_scn_for_user_request(
    const int64_t max_sync_medium_scn,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result,
    int64_t &schema_version);
  int get_adaptive_reason(const int64_t schedule_major_snapshot);
  int fill_mds_filter_info(ObMediumCompactionInfo &medium_info);
  static const int64_t DEFAULT_SCHEDULE_MEDIUM_INTERVAL = 60_s;
  static constexpr double SCHEDULE_RANGE_INC_ROW_COUNT_PERCENRAGE_THRESHOLD = 0.2;
  static const int64_t SCHEDULE_RANGE_ROW_COUNT_THRESHOLD = 1000 * 1000L; // 100w
  static const int64_t RECYCLE_TRUNCATE_INFO_INTERVAL = 2 * 60 * 1000L * 1000L * 1000L;
#ifdef ERRSIM
  int errsim_choose_medium_snapshot(
    const int64_t max_sync_medium_scn,
    int64_t &schema_version,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result);
#endif
private:
  ObArenaAllocator allocator_;
  ObLS &ls_;
  ObTabletHandle tablet_handle_;
  const int64_t weak_read_ts_; // weak_read_ts_ should get before tablet
  const ObMediumCompactionInfoList *medium_info_list_;
  ObScheduleTabletCnt *schedule_tablet_cnt_;
  ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason_;
  int64_t least_medium_snapshot_; // choosen medium snapshot should >= least_medium_snapshot_
};

} //namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_FUNC_H_
