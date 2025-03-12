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
#include "storage/compaction/ob_basic_schedule_tablet_func.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_schedule_dag_func.h"
namespace oceanbase
{
using namespace storage;
namespace compaction
{
/********************************************ObBasicScheduleTabletFunc impl******************************************/

ObBasicScheduleTabletFunc::ObBasicScheduleTabletFunc(
  const int64_t merge_version)
  : merge_version_(merge_version),
    ls_status_(),
    tablet_cnt_(),
    freeze_param_(),
    ls_could_schedule_new_round_(false),
    ls_could_schedule_merge_(false),
    is_skip_merge_tenant_(false)
{
}

void ObBasicScheduleTabletFunc::destroy()
{
  schedule_freeze_dag(true/*force*/); // schedule dag before destroy
}

int ObBasicScheduleTabletFunc::switch_ls(ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
  schedule_freeze_dag(true/*force*/); // schedule dag before switch to next ls

  if (OB_FAIL(ls_status_.init_for_major(merge_version_, ls_handle))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("failed to init ls status", KR(ret), K_(merge_version), K(ls_id));
    }
  } else if (OB_UNLIKELY(merge_version_ > ObBasicMergeScheduler::INIT_COMPACTION_SCN
      && !ls_status_.can_merge())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("could not to merge now", K(ret), K(ls_id), K(ls_status_));
  } else {
    freeze_param_.ls_id_ = ls_id;
    freeze_param_.compaction_scn_ = merge_version_;
  }
  if (OB_SUCC(ret)) {
    update_tenant_cached_status();
  }
  return ret;
}

void ObBasicScheduleTabletFunc::update_tenant_cached_status()
{
  const ObBasicMergeScheduler * scheduler = ObBasicMergeScheduler::get_merge_scheduler();
  if (OB_NOT_NULL(scheduler)) {
    is_skip_merge_tenant_ = scheduler->get_tenant_status().is_skip_merge_tenant();
    ls_could_schedule_merge_ = scheduler->could_major_merge_start() && ls_status_.can_merge();
    // can only schedule new round on ls leader
    ls_could_schedule_new_round_ = ls_could_schedule_merge_ && ls_status_.is_leader_;

    if (!ls_status_.can_merge() && REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INVERVAL)) {
      LOG_INFO("should not schedule major merge for ls", K_(ls_status),
        "tenant_status", scheduler->get_tenant_status());
      ADD_COMMON_SUSPECT_INFO(
                  MEDIUM_MERGE, share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                  ObSuspectInfoType::SUSPECT_SUSPEND_MERGE,
                  merge_version_);
    }
  }
}

void ObBasicScheduleTabletFunc::schedule_freeze_dag(const bool force)
{
  int tmp_ret = OB_SUCCESS;
  if (freeze_param_.tablet_info_array_.empty()) {
  } else if (!force && freeze_param_.tablet_info_array_.count() < SCHEDULE_DAG_THREHOLD) {
  } else {
    if (OB_TMP_FAIL(ObScheduleDagFunc::schedule_batch_freeze_dag(freeze_param_))) {
      LOG_WARN_RET(tmp_ret, "failed to schedule batch force freeze tablets dag", K(freeze_param_));
      // most tablets will clear failed since the capacity of ObTenantTabletStatMgr is limited
    } else {
      LOG_INFO("success to schedule batch freeze dag", KR(tmp_ret), K_(freeze_param));
    }
    freeze_param_.clear_array();
  }
}

/*
 * diagnose section
 */
int ObBasicScheduleTabletFunc::diagnose_switch_ls(
  ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_status_.init_for_major(merge_version_, ls_handle))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("failed to init ls status", KR(ret), K_(merge_version), K(ls_handle));
    }
  } else {
    update_tenant_cached_status();
  }
  return ret;
}

/*
 * tablet_status is inited with frozen_version, may not suitable for new schedule scn
*/
int ObBasicScheduleTabletFunc::check_with_schedule_scn(
  const ObTablet &tablet,
  const int64_t schedule_scn,
  const ObTabletStatusCache &tablet_status,
  bool &can_merge,
  const ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type)
{
  const ObLSID &ls_id = ls_status_.ls_id_;
  can_merge = false;
  int ret = OB_SUCCESS;
  bool need_force_freeze = false;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  const bool need_merge = tablet.get_last_major_snapshot_version() < schedule_scn;
  const bool weak_read_ts_ready = ls_status_.weak_read_ts_.get_val_for_tx() >= schedule_scn;

  if (need_merge) {
    can_merge = (tablet_status.can_merge() &&
                weak_read_ts_ready &&
                tablet.get_snapshot_version() >= schedule_scn);
    if (!can_merge && OB_FAIL(check_need_force_freeze(tablet, schedule_scn, need_force_freeze))) {
      LOG_WARN("failed to check need force freeze", KR(ret), K(tablet_id), K(schedule_scn));
    } else if (need_force_freeze) {
      tablet_cnt_.force_freeze_cnt_++;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(freeze_param_.tablet_info_array_.push_back(ObTabletSchedulePair(tablet_id, schedule_scn, co_major_merge_type)))) {
        LOG_WARN("failed to push back tablet_id for batch_freeze", KR(tmp_ret), K(tablet_id));
      }
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_DIAGNOSE_CANNOT_MAJOR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_DIAGNOSE_CANNOT_MAJOR", K(ret));
      can_merge = false;
      ret = OB_SUCCESS;
    }
  }
#endif
  if (OB_SUCC(ret) && need_merge && !can_merge && REACH_THREAD_TIME_INTERVAL(60L * 1000L * 1000L)) {
    LOG_INFO("tablet can't schedule dag", K(ret), K(tablet_id),
             K(need_merge), K(can_merge), K(schedule_scn), K(need_force_freeze),
             K(weak_read_ts_ready), K_(ls_status), K(tablet_status));
    ADD_SUSPECT_INFO(MEDIUM_MERGE, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                    ls_id,
                    tablet_id,
                    ObSuspectInfoType::SUSPECT_CANT_MAJOR_MERGE,
                    schedule_scn,
                    tablet.get_snapshot_version(),
                    static_cast<int64_t>(need_force_freeze),
                    static_cast<int64_t>(weak_read_ts_ready),
                    tablet.get_tablet_meta().max_serialized_medium_scn_);
  }
  return ret;
}

int ObBasicScheduleTabletFunc::check_need_force_freeze(
    const ObTablet &tablet,
    const int64_t schedule_scn,
    bool &need_force_freeze)
{
  need_force_freeze = false;
  int ret = OB_SUCCESS;

  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObTableHandleV2 memtable_handle;
  ObProtectedMemtableMgrHandle *protected_handle = nullptr;
  ObITabletMemtable *last_frozen_memtable = nullptr;

  if (OB_FAIL(tablet.get_protected_memtable_mgr_handle(protected_handle))) {
    LOG_WARN("failed to get_protected_memtable_mgr_handle", K(ret), K(tablet));
  } else if (OB_FAIL(protected_handle->get_last_frozen_memtable(memtable_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) { // no frozen memtable, need force freeze
      need_force_freeze = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get last frozen memtable", K(ret), K(tablet));
    }
  } else if (OB_FAIL(memtable_handle.get_tablet_memtable(last_frozen_memtable))) {
    LOG_WARN("failed to get last frozen memtable", K(ret));
  } else {
    need_force_freeze = last_frozen_memtable->get_snapshot_version() < schedule_scn;
    if (!need_force_freeze) {
      LOG_INFO("tablet no need force freeze", K(ret), K(tablet_id),
               K(schedule_scn), KPC(last_frozen_memtable));
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
