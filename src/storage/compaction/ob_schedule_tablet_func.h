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
struct ObScheduleTabletFunc : public ObBasicScheduleTabletFunc
{
  ObScheduleTabletFunc(
    const int64_t merge_version,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason = ObAdaptiveMergePolicy::NONE,
    const int64_t loop_cnt = 0
    );
  virtual ~ObScheduleTabletFunc() {}
  int schedule_tablet(
    storage::ObTabletHandle &tablet_handle,
    bool &tablet_merge_finish);
  int request_schedule_new_round(
    storage::ObTabletHandle &tablet_handle,
    const bool user_request);
  const ObTabletStatusCache &get_tablet_status() const { return tablet_status_; }
  virtual const ObCompactionTimeGuard &get_time_guard() const override { return time_guard_; }
  int diagnose_switch_tablet(storage::ObLS &ls, const storage::ObTablet &tablet);
  int64_t get_merged_version() const { return merge_version_; }
  INHERIT_TO_STRING_KV("ObScheduleTabletFunc", ObBasicScheduleTabletFunc,
    K_(merge_reason), K_(tablet_status), K_(time_guard));
protected:
int check_with_schedule_scn(
    const storage::ObTablet &tablet,
    const int64_t schedule_scn,
    const ObTabletStatusCache &tablet_status,
    bool &can_merge,
    const ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type = ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE) override;
int schedule_merge_dag(
    const ObLSID &ls_id,
    const ObTablet &tablet,
    const ObMergeType merge_type,
    const int64_t schedule_scn,
    const ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason) override;
private:
  virtual void schedule_freeze_dag(const bool force) override;
  int schedule_tablet_new_round(
    storage::ObTabletHandle &tablet_handle,
    const bool user_request);
  int schedule_tablet_execute(
    storage::ObTablet &tablet);
  int get_schedule_execute_info(
    storage::ObTablet &tablet,
    int64_t &schedule_scn,
    ObCOMajorMergePolicy::ObCOMajorMergeType &co_major_merge_type,
    ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason);
private:
  ObTabletStatusCache tablet_status_;
  ObCompactionScheduleTimeGuard time_guard_;
  ObSEArray<ObTabletID, 64> clear_stat_tablets_;
  ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_SCHEDULE_TABLET_FUNC_H_
