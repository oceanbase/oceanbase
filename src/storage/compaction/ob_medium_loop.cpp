//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_medium_loop.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_schedule_tablet_func.h"
#include "storage/compaction/ob_server_compaction_event_history.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace compaction
{
/********************************************ObMediumLoop impl******************************************/
int ObMediumLoop::start_merge(const int64_t merge_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merge_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(merge_version));
  } else {
    merge_version_ = merge_version;
    schedule_stats_.start_merge();

    const int64_t last_merged_version = ObBasicMergeScheduler::get_merge_scheduler()->get_merged_version();
    ADD_COMPACTION_EVENT(
        merge_version,
        ObServerCompactionEvent::RECEIVE_BROADCAST_SCN,
        schedule_stats_.start_timestamp_,
        K(last_merged_version));
  }
  return ret;
}

int ObMediumLoop::init(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merge_version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge_version", KR(ret), K_(merge_version));
  } else if (OB_FAIL(ls_tablet_iter_.build_iter(batch_size))) {
    LOG_WARN("failed to init ls iterator", K(ret));
  }
  return ret;
}

int ObMediumLoop::loop()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObScheduleTabletFunc func(merge_version_);
  ObLSID ls_id;
  schedule_stats_.all_ls_weak_read_ts_ready_ = true;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ls_tablet_iter_.get_next_ls(ls_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get ls", K(ret), K_(ls_tablet_iter));
      }
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls));
    } else if (FALSE_IT(ls_id = ls->get_ls_id())) {
    } else if (OB_TMP_FAIL(loop_in_ls(ls_handle, func))) {
      LOG_TRACE("failed to loop ls", KR(ret), KPC(ls), K(func));
      ls_tablet_iter_.skip_cur_ls(); // for any errno, skip cur ls
      ls_tablet_iter_.update_merge_finish(false);
      if (OB_SIZE_OVERFLOW == tmp_ret) {
        break;
      } else if (!schedule_ignore_error(tmp_ret)) {
        LOG_WARN("failed to schedule ls merge", K(tmp_ret), K(ls_id));
      }
    }
    if (OB_SUCC(ret) && ls_tablet_iter_.need_report_scn()) {
      // loop tablet_meta table to update smaller report_scn because of migration
      tmp_ret = update_report_scn_as_ls_leader(*ls, func);
#ifndef ERRSIM
      LOG_INFO("try to update report scn as ls leader", K(tmp_ret), K(ls_id)); // low printing frequency
#endif
    }
  } // while
  add_event_and_diagnose(func);
  LOG_TRACE("finish schedule ls medium merge", K(tmp_ret), K(ret), K_(ls_tablet_iter), K(ls_id));
  return ret;
}

int ObMediumLoop::loop_in_ls(
  ObLSHandle &ls_handle,
  ObScheduleTabletFunc &func)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
  if (OB_FAIL(func.switch_ls(ls_handle))) {
    if (OB_STATE_NOT_MATCH != ret) {
      LOG_WARN("failed to switch ls", KR(ret), K(ls_id), K(func));
    } else {
      ls_tablet_iter_.update_merge_finish(false);
      schedule_stats_.all_ls_weak_read_ts_ready_ = false;
    }
  } else {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObTabletID tablet_id;
    bool tablet_merge_finish = false;
    while (OB_SUCC(ret)) { // loop all tablet in ls
      if (OB_FAIL(ls_tablet_iter_.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid()
        || nullptr == (tablet = tablet_handle.get_obj()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet handle is invalid", KR(ret), K(ls_id), K(tablet_handle));
      } else if (FALSE_IT(tablet_id = tablet->get_tablet_id())) {
      } else if (tablet_id.is_ls_inner_tablet()) {
        // do nothing
      } else if (OB_TMP_FAIL(func.schedule_tablet(tablet_handle, tablet_merge_finish))) {
        if (OB_STATE_NOT_MATCH != tmp_ret) {
          LOG_WARN("failed to schedule tablet", KR(tmp_ret), K(ls_id), K(tablet_id));
        }
        ls_tablet_iter_.update_merge_finish(false);
      } else {
        ls_tablet_iter_.update_merge_finish(tablet_merge_finish);
      }
    } // while
  }
  return ret;
}

void ObMediumLoop::add_event_and_diagnose(const ObScheduleTabletFunc &func)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!ls_tablet_iter_.tenant_merge_finish() && merge_version_ > ObBasicMergeScheduler::INIT_COMPACTION_SCN) {
    // not finish cur merge_version
    if (schedule_stats_.all_ls_weak_read_ts_ready_) { // check schedule Timer Task
      if (schedule_stats_.add_weak_read_ts_event_flag_ && ls_tablet_iter_.is_scan_finish()) { // all ls scan finish
        schedule_stats_.add_weak_read_ts_event_flag_ = false;
        ADD_COMPACTION_EVENT(
            merge_version_,
            ObServerCompactionEvent::WEAK_READ_TS_READY,
            ObTimeUtility::fast_current_time(),
            "check_weak_read_ts_cnt", schedule_stats_.check_weak_read_ts_cnt_ + 1);
      }
    } else {
      schedule_stats_.check_weak_read_ts_cnt_++;
    }

    if (ls_tablet_iter_.is_scan_finish()) {
      loop_cnt_++;
      if (REACH_THREAD_TIME_INTERVAL(ADD_LOOP_EVENT_INTERVAL)) {
        ADD_COMPACTION_EVENT(
          merge_version_,
          ObServerCompactionEvent::SCHEDULER_LOOP,
          ObTimeUtility::fast_current_time(),
          "schedule_stats", schedule_stats_,
          "schedule_tablet_cnt", func.get_schedule_tablet_cnt());
      }
    }
  }

  const int64_t merged_version = ObBasicMergeScheduler::get_merge_scheduler()->get_merged_version();
  if (ls_tablet_iter_.tenant_merge_finish() && merge_version_ > merged_version) {
    ObBasicMergeScheduler::get_merge_scheduler()->update_merged_version(merge_version_);
    LOG_INFO("all tablet major merge finish", K(merged_version), K_(loop_cnt));

    DEL_SUSPECT_INFO(MEDIUM_MERGE, UNKNOW_LS_ID, UNKNOW_TABLET_ID, share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE);
    if (OB_TMP_FAIL(MTL(ObTenantCompactionProgressMgr *)->finish_progress(merge_version_))) {
      LOG_WARN("failed to finish progress", K(tmp_ret), K_(merge_version));
    }

    const int64_t current_time = ObTimeUtility::fast_current_time();
    ADD_COMPACTION_EVENT(
          merge_version_,
          ObServerCompactionEvent::TABLET_COMPACTION_FINISHED,
          current_time,
          "cost_time",
          current_time - schedule_stats_.start_timestamp_);
  }

  LOG_INFO("finish schedule all tablet merge", K(merge_version_), K(schedule_stats_), K_(loop_cnt),
      "tenant_merge_finish", ls_tablet_iter_.tenant_merge_finish(),
      "is_scan_all_tablet_finish", ls_tablet_iter_.is_scan_finish(),
      "schedule_tablet_cnt", func.get_schedule_tablet_cnt(),
      "time_guard", func.get_time_guard());
}

void ObMediumLoop::clear()
{
  loop_cnt_ = 0;
  schedule_stats_.reset();
}

int ObMediumLoop::update_report_scn_as_ls_leader(ObLS &ls, const ObScheduleTabletFunc &func)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  const int64_t inner_table_merged_scn = ObBasicMergeScheduler::get_merge_scheduler()->get_inner_table_merged_scn();
  const ObLSStatusCache &ls_status = func.get_ls_status();
  if (ls_status.can_merge() && ls_status.is_leader_) {
    ObSEArray<ObTabletID, 200> tablet_id_array;
    if (OB_FAIL(ls.get_tablet_svr()->get_all_tablet_ids(true/*except_ls_inner_tablet*/, tablet_id_array))) {
      LOG_WARN("failed to get tablet id", K(ret), K(ls_id));
    } else if (inner_table_merged_scn > ObBasicMergeScheduler::INIT_COMPACTION_SCN
        && OB_FAIL(ObTabletMetaTableCompactionOperator::batch_update_unequal_report_scn_tablet(
          MTL_ID(), ls_id, inner_table_merged_scn, tablet_id_array))) {
      LOG_WARN("failed to get unequal report scn", K(ret), K(ls_id), K(inner_table_merged_scn));
    }
  } else {
    ret = OB_LS_LOCATION_LEADER_NOT_EXIST;
  }
  return ret;
}

/********************************************ObScheduleNewMediumLoop impl******************************************/
int ObScheduleNewMediumLoop::loop()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  const int64_t frozen_version = ObBasicMergeScheduler::get_merge_scheduler()->get_frozen_version();
  ObScheduleTabletFunc func(frozen_version);
  // sort tablet ls info
  if (OB_FAIL(sort_tablet_ls_info())) {
    LOG_WARN("failed to sort", KR(ret));
  }
  for (int64_t i = 0, idx = 0; i < tablet_ls_infos_.count(); ++i) { // ignore OB_FAIL
    const ObLSID &ls_id = tablet_ls_infos_.at(i).get_ls_id();
    const ObTabletID &tablet_id = tablet_ls_infos_.at(i).get_tablet_id();
    ObTabletHandle tablet_handle;
    if (func.get_ls_status().ls_id_ == ls_id) {
      // do nothing, use old ls_handle
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::COMPACT_MODE))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_FAIL(func.switch_ls(ls_handle))) {
      if (OB_STATE_NOT_MATCH != ret) {
        LOG_WARN("failed to switch ls", KR(ret), K(ls_id), K(ls_id));
      } else {
        LOG_WARN("not support schedule medium for ls", K(ret), K(ls_id), K(tablet_id), K(func));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!func.get_ls_status().is_leader_) {
      // not leader, can't schedule
      LOG_TRACE("not ls leader, can't schedule medium", K(ret), K(ls_id), K(tablet_id), K(func));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
                 tablet_id, tablet_handle, 0 /*timeout_us*/))) {
      LOG_WARN("get tablet failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(func.request_schedule_new_round(tablet_handle, false/*user_request*/))) {
      LOG_WARN("get tablet failed", K(ret), K(ls_id), K(tablet_id));
    }
  } // end of for
  ret = OB_SUCCESS;
  LOG_INFO("end of ObScheduleNewMediumLoop", KR(ret), K(func));
  return ret;
}

struct ObTabletLSInfoComparator final {
public:
  ObTabletLSInfoComparator(int &sort_ret)
    : result_code_(sort_ret)
  {}
  bool operator()(const ObTabletCheckInfo &lhs, const ObTabletCheckInfo &rhs)
  {
    return lhs.get_ls_id() < rhs.get_ls_id();
  }
  int &result_code_;
};

int ObScheduleNewMediumLoop::sort_tablet_ls_info()
{
  int ret = OB_SUCCESS;
  if (tablet_ls_infos_.count() > 2) {
    ObTabletLSInfoComparator cmp(ret);
    ob_sort(tablet_ls_infos_.begin(), tablet_ls_infos_.end(), cmp);
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase