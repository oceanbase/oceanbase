/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_window_schedule_tablet_func.h"
#include "storage/compaction/ob_window_compaction_utils.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/tablet/ob_tablet.h" // for complete type of ObTablet

namespace oceanbase
{
namespace compaction
{

int ObWindowScheduleTabletFunc::refresh_window_tablet(const ObTabletCompactionScore &candidate)
{
  int ret = OB_SUCCESS;
  ObAdaptiveMergePolicy::AdaptiveMergeReason new_merge_reason = ObAdaptiveMergePolicy::AdaptiveMergeReason::INVALID_REASON;
  window_decision_log_info_.reset();
  if (OB_UNLIKELY(!candidate.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(candidate));
  } else if (candidate.decision_info_.dynamic_info_.need_recycle_mds_) {
    new_merge_reason = ObAdaptiveMergePolicy::AdaptiveMergeReason::RECYCLE_TRUNCATE_INFO;
  } else {
    new_merge_reason = ObAdaptiveMergePolicy::AdaptiveMergeReason::WINDOW_COMPACTION;
  }
  if (FAILEDx(set_merge_reason(new_merge_reason))) {
    LOG_WARN("failed to set merge reason", K(ret), K(new_merge_reason));
  } else {
    // sync decision info via medium clog for diagnosis in tablet compaction history
    (void) window_decision_log_info_.init(candidate); // ignore ret, don't block schedule tablet
  }
  return ret;
}

// Both process_ready_candidate and process_log_submitted_candidate should be called with inited tablet_status_.
// In one round, only one of them will schedule tablet, so don't need to reload tablet status again.
int ObWindowScheduleTabletFunc::process_ready_candidate(
    ObTabletCompactionScore &candidate,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  int64_t max_sync_medium_scn = 0;
  bool need_schedule_tablet = false;
  bool unused_tablet_merge_finish = false;
  if (OB_UNLIKELY(!candidate.is_ready_status() || !candidate.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(candidate), K(tablet_handle));
  } else if (OB_ISNULL(tablet_status_.medium_list())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium info list is nullptr", K(ret), K_(tablet_status));
  } else if (OB_FAIL(tablet_status_.medium_list()->get_max_sync_medium_scn(max_sync_medium_scn))) {
    LOG_WARN("failed to get max sync medium scn", K(ret), KPC(tablet));
  } else if (max_sync_medium_scn > candidate.get_major_snapshot_version()) {
    candidate.set_log_submitted_status();
    LOG_INFO("[WIN-COMPACTION] Medium clog is submitted, skip schedule tablet", K(candidate), K(max_sync_medium_scn));
  } else if (OB_FAIL(schedule_tablet(tablet_handle, unused_tablet_merge_finish))) {
    LOG_WARN("failed to schedule tablet", K(ret), KPC(tablet));
  }
  return ret;
}

int ObWindowScheduleTabletFunc::process_log_submitted_candidate(
    ObTabletCompactionScore &candidate,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  bool unused_tablet_merge_finish = false;
  if (OB_UNLIKELY(!candidate.is_log_submitted_status() || !candidate.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(candidate), K(tablet_handle));
  } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (tablet->get_last_major_snapshot_version() > candidate.get_major_snapshot_version()) {
    candidate.set_finished_status();
    LOG_INFO("[WIN-COMPACTION] Tablet is merged, skip schedule tablet", K(candidate), "last_major_snapshot_version", tablet->get_last_major_snapshot_version());
  } else if (OB_FAIL(schedule_tablet(tablet_handle, unused_tablet_merge_finish))) {
    LOG_WARN("failed to schedule tablet", K(ret), KPC(tablet));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase