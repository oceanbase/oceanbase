// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_STORAGE_COMPACTION_SCHEDULE_TABLET_FUNC_H_
#define OB_STORAGE_COMPACTION_SCHEDULE_TABLET_FUNC_H_
#include "storage/compaction/ob_basic_schedule_tablet_func.h"
#include "storage/compaction_ttl/ob_mlog_purge_info_helper.h"
namespace oceanbase
{
namespace storage
{
class ObLS;
class ObTablet;
class ObTabletHandle;
}
namespace compaction
{
struct ObWindowCompactionDecisionLogInfo;
struct ObTenantMlogPurgeScnMapCache final
{
  static constexpr int64_t MIN_REFRESH_INTERVAL_US = 30 * 1000 * 1000L; // 30s
  static constexpr int64_t MIN_NEED_REFRESH_MAP_SIZE = 10000;
  ObTenantMlogPurgeScnMapCache()
    : map_(),
      read_snapshot_(-1)
  {}
  DISABLE_COPY_ASSIGN(ObTenantMlogPurgeScnMapCache);
  int refresh_or_init(const int64_t read_snapshot);
  const MlogPurgeScnMap &get_map() const { return map_; }
  int64_t get_read_snapshot() const { return read_snapshot_; }
  bool is_initialized() const { return map_.created(); }
  bool is_empty() const { return map_.empty(); }
  MlogPurgeScnMap map_;
  int64_t read_snapshot_;
};
struct ObScheduleTabletFunc : public ObBasicScheduleTabletFunc
{
  ObScheduleTabletFunc(
    const int64_t merge_version,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::NONE,
    const int64_t loop_cnt = 0,
    const ObCompactionScheduleMode schedule_mode = COMPACTION_NORMAL_MODE);
  virtual ~ObScheduleTabletFunc() {}
  int iterate_switch_tablet(
    storage::ObTabletHandle &tablet_handle,
    bool &can_merge,
    bool &need_schedule); // should combine with destroy_tablet_status()
  int schedule_tablet(
    storage::ObTabletHandle &tablet_handle,
    bool &tablet_merge_finish);
  int switch_and_schedule_tablet(
    ObTabletHandle &tablet_handle,
    bool &tablet_merge_finish);
  int request_schedule_new_round(
    storage::ObTabletHandle &tablet_handle,
    const bool user_request,
    const bool need_load_tablet_status);
  int refresh_mlog_purge_scn_cache(const int64_t &merge_version);
  const ObTabletStatusCache &get_tablet_status() const { return tablet_status_; }
  virtual const ObCompactionTimeGuard &get_time_guard() const override { return time_guard_; }
  virtual const ObWindowCompactionDecisionLogInfo *get_window_decision_log_info() const { return nullptr; }
  int diagnose_switch_tablet(storage::ObLS &ls, storage::ObTablet &tablet);
  OB_INLINE bool is_window_compaction_func() const { return schedule_mode_ == COMPACTION_WINDOW_MODE; }
  OB_INLINE bool is_window_compaction_active() const { return is_window_compaction_active_; }
  OB_INLINE void destroy_tablet_status() { tablet_status_.destroy(); }
  INHERIT_TO_STRING_KV("ObScheduleTabletFunc", ObBasicScheduleTabletFunc,
    K_(merge_reason), K_(schedule_mode), K_(is_window_compaction_active), K_(tablet_status), K_(time_guard));
protected:
  virtual int check_with_schedule_scn(
    const storage::ObTablet &tablet,
    const int64_t schedule_scn,
    const ObTabletStatusCache &tablet_status,
    bool &can_merge,
    const ObCOMajorMergeStrategy &co_major_merge_strategy,
    const int64_t need_freeze_snapshot) override;
  virtual int schedule_merge_dag(
    const ObLSID &ls_id,
    const ObTablet &tablet,
    const ObMergeType merge_type,
    const int64_t schedule_scn,
    const ObCOMajorMergeStrategy &co_major_merge_strategy,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason) override;
  int try_schedule_tablet_new_round(storage::ObTabletHandle &tablet_handle);
  int schedule_tablet_new_round(
    storage::ObTabletHandle &tablet_handle,
    const bool user_request);
  int set_merge_reason(const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason);
private:
  virtual void schedule_freeze_dag(const bool force) override;
  int schedule_tablet_execute(
    storage::ObTablet &tablet);
  int get_schedule_execute_info(
    storage::ObTablet &tablet,
    int64_t &schedule_scn,
    ObCOMajorMergeStrategy &co_major_merge_strategy,
    ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
    int64_t &need_freeze_snapshot);
  int get_mlog_latest_purge_scn(
    const storage::ObTablet &tablet,
    int64_t &latest_mlog_purge_scn);

protected:
  ObTabletStatusCache tablet_status_;
private:
  ObCompactionScheduleTimeGuard time_guard_;
  ObSEArray<ObTabletID, 64> clear_stat_tablets_;
  ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason_;
  ObCompactionScheduleMode schedule_mode_;
  ObTenantMlogPurgeScnMapCache mlog_purge_scn_cache_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_SCHEDULE_TABLET_FUNC_H_
