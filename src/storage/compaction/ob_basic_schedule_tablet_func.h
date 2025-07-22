//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_BASIC_SCHEDULE_TABLET_FUNC_H_
#define OB_STORAGE_COMPACTION_BASIC_SCHEDULE_TABLET_FUNC_H_
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/compaction/ob_batch_freeze_tablets_dag.h"
#include "storage/compaction/ob_schedule_status_cache.h"
namespace oceanbase
{
namespace storage
{
class ObLS;
class ObTablet;
class ObTabletHandle;
class ObLSHandle;
}
namespace compaction
{
struct ObBasicScheduleTabletFunc
{
  ObBasicScheduleTabletFunc(const int64_t merge_version, const int64_t loop_cnt = 0);
  virtual ~ObBasicScheduleTabletFunc() { destroy(); }
  int switch_ls(storage::ObLSHandle &ls_handle);
  void destroy();
  const ObLSStatusCache &get_ls_status() const { return ls_status_; }
  ObScheduleTabletCnt &get_schedule_tablet_cnt() { return tablet_cnt_; }
  const ObScheduleTabletCnt &get_schedule_tablet_cnt() const { return tablet_cnt_; }
  virtual const ObCompactionTimeGuard &get_time_guard() const = 0;
  int64_t get_loop_cnt() const { return loop_cnt_; }
  VIRTUAL_TO_STRING_KV(K_(merge_version), K_(ls_status),
    K_(ls_could_schedule_new_round), K_(ls_could_schedule_merge), K_(is_skip_merge_tenant),
    K_(tablet_cnt), K_(loop_cnt));
  /*
   * diagnose section
  */
  int diagnose_switch_ls(storage::ObLSHandle &ls_handle);
protected:
  void update_tenant_cached_status();
  virtual void schedule_freeze_dag(const bool force);
  int check_with_schedule_scn(
    const storage::ObTablet &tablet,
    const int64_t schedule_scn,
    const ObTabletStatusCache &tablet_status,
    bool &can_merge,
    const ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type = ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE);
  int check_need_force_freeze(
    const storage::ObTablet &tablet,
    const int64_t schedule_scn,
    bool &need_force_freeze);
protected:
  static const int64_t PRINT_LOG_INVERVAL = 2 * 60 * 1000 * 1000L; // 2m
  static const int64_t SCHEDULE_DAG_THREHOLD = 1000;
  int64_t merge_version_;
  ObLSStatusCache ls_status_;
  ObScheduleTabletCnt tablet_cnt_;
  ObBatchFreezeTabletsParam freeze_param_;
  bool ls_could_schedule_new_round_;
  bool ls_could_schedule_merge_;  // suspend merge OR during restore inner_table
  bool is_skip_merge_tenant_; // remote tenant OR during restore tenant with(Standby role)
  int64_t loop_cnt_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_BASIC_SCHEDULE_TABLET_FUNC_H_
