//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_MEDIUM_LOOP_H_
#define OB_STORAGE_COMPACTION_MEDIUM_LOOP_H_
#include "storage/compaction/ob_compaction_schedule_iterator.h"
#include "storage/compaction/ob_compaction_schedule_util.h"
namespace oceanbase
{
namespace compaction
{
struct ObScheduleTabletFunc;
class ObTabletCheckInfo;

struct ObMediumLoop
{
  ObMediumLoop()
    : merge_version_(ObBasicMergeScheduler::INIT_COMPACTION_SCN),
      loop_version_(ObBasicMergeScheduler::INIT_COMPACTION_SCN),
      loop_cnt_(0),
      ls_tablet_iter_(true/*is_major*/),
      lock_()
  {}
  ~ObMediumLoop() {}
  int start_merge(const int64_t merge_version);
  int init(const int64_t batch_size);
  int loop();
  void clear();
  OB_INLINE bool schedule_ignore_error(const int ret)
  {
    return OB_ITER_END == ret
      || OB_STATE_NOT_MATCH == ret
      || OB_LS_NOT_EXIST == ret;
  }
private:
  int loop_in_ls(
    storage::ObLSHandle &ls_handle,
    ObScheduleTabletFunc &tablet_schedule_func);
  int update_report_scn_as_ls_leader(
    storage::ObLS &ls,
    const ObScheduleTabletFunc &func);
  void add_event_and_diagnose(const ObScheduleTabletFunc &func);
private:
  static const int64_t ADD_LOOP_EVENT_INTERVAL = 120 * 1000 * 1000L; // 120s

  int64_t merge_version_;
  int64_t loop_version_;
  int64_t loop_cnt_;
  ObScheduleStatistics schedule_stats_;
  ObCompactionScheduleIterator ls_tablet_iter_;
  lib::ObMutex lock_;
};

struct ObScheduleNewMediumLoop
{
  ObScheduleNewMediumLoop(
    ObArray<ObTabletCheckInfo> &tablet_ls_infos)
    : tablet_ls_infos_(tablet_ls_infos)
  {}
  ~ObScheduleNewMediumLoop() {}
  int loop();
  int sort_tablet_ls_info();
private:
  ObArray<ObTabletCheckInfo> &tablet_ls_infos_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MEDIUM_LOOP_H_
