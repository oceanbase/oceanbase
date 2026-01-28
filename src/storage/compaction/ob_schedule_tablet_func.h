//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_SCHEDULE_TABLET_FUNC_H_
#define OB_STORAGE_COMPACTION_SCHEDULE_TABLET_FUNC_H_
#include "storage/compaction/ob_basic_schedule_tablet_func.h"
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
struct ObScheduleTabletFunc : public ObBasicScheduleTabletFunc
{
  ObScheduleTabletFunc(
    const int64_t merge_version,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::NONE,
    const int64_t loop_cnt = 0,
    const ObCompactionScheduleMode schedule_mode = COMPACTION_NORMAL_MODE);
  virtual ~ObScheduleTabletFunc() {}
  int iterate_switch_tablet(storage::ObTabletHandle &tablet_handle, bool &can_merge); // should combine with destroy_tablet_status()
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
    const ObCOMajorMergeStrategy &co_major_merge_strategy) override;
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
    ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason);
protected:
  ObTabletStatusCache tablet_status_;
private:
  ObCompactionScheduleTimeGuard time_guard_;
  ObSEArray<ObTabletID, 64> clear_stat_tablets_;
  ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason_;
  ObCompactionScheduleMode schedule_mode_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_SCHEDULE_TABLET_FUNC_H_
