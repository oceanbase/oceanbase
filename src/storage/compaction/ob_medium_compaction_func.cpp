//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "share/tablet/ob_tablet_info.h"
#include "share/tablet/ob_tablet_table_operator.h"
#include "share/ob_tablet_replica_checksum_operator.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "logservice/ob_log_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/ob_partition_range_spliter.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "src/storage/column_store/ob_column_oriented_sstable.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
using namespace common;

namespace compaction
{

int64_t ObMediumCompactionScheduleFunc::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV("ls_id", ls_.get_ls_id());
    J_COMMA();
    if (tablet_handle_.is_valid()) {
      J_KV("tablet_id", tablet_handle_.get_obj()->get_tablet_meta().tablet_id_);
    } else {
      J_KV("tablet_handle", tablet_handle_);
    }
    J_OBJ_END();
  }
  return pos;
}

int ObMediumCompactionScheduleFunc::choose_medium_snapshot(
    const int64_t max_sync_medium_scn,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObGetMergeTablesParam param;
  param.merge_type_ = MEDIUM_MERGE;
  int64_t max_reserved_snapshot = 0;
  ObTablet &tablet = *tablet_handle_.get_obj();

  if (OB_FAIL(ObAdaptiveMergePolicy::get_meta_merge_tables(param, ls_, tablet, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get meta merge tables", K(ret), K(param));
    }
  } else if (FALSE_IT(medium_info.medium_snapshot_ = result.version_range_.snapshot_version_)) {
  } else if (OB_FAIL(get_max_reserved_snapshot(max_reserved_snapshot))) {
    LOG_WARN("failed to get reserved snapshot", K(ret), KPC(this));
  } else if (medium_info.medium_snapshot_ < max_reserved_snapshot
      || medium_info.medium_snapshot_ > tablet.get_snapshot_version()) {
    // chosen medium snapshot is far too old
    if (OB_FAIL(choose_new_medium_snapshot(max_reserved_snapshot, medium_info, result, schema_version))) {
      LOG_WARN("failed to choose new medium snapshot", KR(ret), K(medium_info), K(max_reserved_snapshot));
    }
  } else if (OB_FAIL(tablet.get_schema_version_from_storage_schema(schema_version))) {
    LOG_WARN("failed to get schema version from tablet", KR(ret), K(tablet));
  }
  if (OB_FAIL(ret)) {
  } else if (medium_info.medium_snapshot_ <= max_sync_medium_scn) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_FAIL(check_frequency(max_reserved_snapshot, medium_info.medium_snapshot_))) { // check schedule interval
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to check medium scn valid", K(ret), KPC(this));
    }
  } else {
    medium_info.set_basic_info(
      ObMediumCompactionInfo::MEDIUM_COMPACTION,
      merge_reason_,
      medium_info.medium_snapshot_);
    LOG_TRACE("choose_medium_snapshot", K(ret), KPC(this), K(result), K(medium_info));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::find_valid_freeze_info(
  ObMediumCompactionInfo &medium_info,
  share::ObFreezeInfo &freeze_info,
  bool &force_schedule_medium_merge)
{
  int ret = OB_SUCCESS;
  force_schedule_medium_merge = false;
  ObTablet &tablet = *tablet_handle_.get_obj();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  int64_t schedule_snapshot = 0;
  bool schedule_with_newer_info = false;
  const int64_t scheduler_frozen_version = MTL(ObTenantTabletScheduler*)->get_frozen_version();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTable *last_major = nullptr;
  int64_t last_sstable_schema_version = 0;
  ObMultiVersionSchemaService *schema_service = nullptr;
  if (OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service from MTL", K(ret));
  } else if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), K(tablet));
  } else {
    last_major = static_cast<ObSSTable *>(table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/));
    if (OB_ISNULL(last_major)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("major sstable is unexpected null", K(ret), K(tablet_id), KPC(last_major));
    } else if (OB_FAIL(last_major->get_frozen_schema_version(last_sstable_schema_version))) {
      LOG_WARN("failed to get frozen schema version", KR(ret), KPC(last_major));
    } else {
      schedule_snapshot = last_major->get_snapshot_version();
    }
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(get_freeze_info_behind_snapshot_version, schedule_snapshot, freeze_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get freeze info", K(ret), K(tablet_id), K(schedule_snapshot));
      } else {
        ret = OB_NO_NEED_MERGE;
      }
    } else if (OB_UNLIKELY(freeze_info.schema_version_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema version is invalid", K(ret), K(freeze_info));
    } else if (OB_UNLIKELY(freeze_info.schema_version_ < last_sstable_schema_version)) {
      medium_info.is_skip_tenant_major_ = true;
      force_schedule_medium_merge = true;
      FLOG_INFO("schema version in freeze info is too small, try to schedule medium compaction instead", K(ret),
                K(tablet_id), K(last_sstable_schema_version), K(freeze_info));
      break;
    } else if (OB_FAIL(get_table_schema_to_merge(
        *schema_service, tablet, freeze_info.schema_version_, medium_info.medium_compat_version_, allocator_, medium_info.storage_schema_))) {
      if (OB_TABLE_IS_DELETED == ret) {
        // do nothing, end loop
      } else if (OB_ERR_SCHEMA_HISTORY_EMPTY == ret) {
        if (freeze_info.frozen_scn_.get_val_for_tx() <= scheduler_frozen_version) {
          FLOG_INFO("table schema may recycled, use newer freeze info instead", K(ret), KPC(last_major), K(freeze_info),
            K(scheduler_frozen_version));
          schedule_snapshot = freeze_info.frozen_scn_.get_val_for_tx();
          schedule_with_newer_info = true;
          medium_info.is_skip_tenant_major_ = true;
          ret = OB_SUCCESS;
          FLOG_INFO("schedule with newer freeze info", K(ret), K(freeze_info));
          continue;
        }
      } else {
        LOG_WARN("failed to get table schema", K(ret),  K(medium_info));
      }
    } else {
      break;
    }
  } // end of while
  return ret;
}

int ObMediumCompactionScheduleFunc::choose_major_snapshot(
    const int64_t max_sync_medium_scn,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObTablet &tablet = *tablet_handle_.get_obj();
  share::ObFreezeInfo freeze_info;
  bool force_schedule_medium_merge = false;

  if (OB_FAIL(find_valid_freeze_info(medium_info, freeze_info, force_schedule_medium_merge))) {
    LOG_WARN("failed to find valid freeze info", KR(ret));
  } else if (force_schedule_medium_merge) {
    if (OB_FAIL(switch_to_choose_medium_snapshot(freeze_info.frozen_scn_.get_val_for_tx(), medium_info, schema_version))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to switch to choose medium snapshot", K(ret), KPC(this));
      }
    }
  } else {
    medium_info.set_basic_info(
      ObMediumCompactionInfo::MAJOR_COMPACTION,
      ObAdaptiveMergePolicy::AdaptiveMergeReason::TENANT_MAJOR,
      freeze_info.frozen_scn_.get_val_for_tx());
    schema_version = freeze_info.schema_version_;
  }

  if (FAILEDx(ObPartitionMergePolicy::get_result_by_snapshot(tablet, medium_info.medium_snapshot_, result))) {
    LOG_WARN("failed get result for major", K(ret), K(medium_info));
  } else {
    LOG_TRACE("choose_major_snapshot", K(ret), KPC(this), K(medium_info), K(freeze_info), K(result),
      K(medium_info), K(schema_version));
#ifdef ERRSIM
    if (tablet.get_tablet_meta().tablet_id_.id() == 1) {
      ret = OB_E(EventTable::EN_SPECIAL_TABLE_HAVE_LARGER_SCN) ret;
      if (OB_FAIL(ret)) {
        ret = OB_SUCCESS;
        medium_info.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
        medium_info.medium_snapshot_ += 100;
        FLOG_INFO("ERRSIM EN_SPECIAL_TABLE_HAVE_LARGER_SCN", KPC(this),K(medium_info));
      }
    }
#endif
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::switch_to_choose_medium_snapshot(
    const int64_t freeze_version,
    ObMediumCompactionInfo &medium_info,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  int64_t medium_snapshot = 0;

  if (weak_read_ts_ < freeze_version + 1) {
    ret = OB_EAGAIN;
    LOG_WARN("weak read ts is smaller than new medium snapshot, try later", K(ret), KPC(this), K(freeze_version));
  } else if (FALSE_IT(medium_snapshot = MAX(weak_read_ts_, freeze_version + 1))) {
  } else if (OB_FAIL(tablet_handle_.get_obj()->get_newest_schema_version(schema_version))) {
    LOG_WARN("fail to choose medium schema version", K(ret), KPC(this));
  } else {
    medium_info.set_basic_info(
      ObMediumCompactionInfo::MEDIUM_COMPACTION,
      ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE,
      medium_snapshot);
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::get_status_from_inner_table(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObTabletCompactionScnInfo &ret_info)
{
  int ret = OB_SUCCESS;
  ret_info.reset();

  ObTabletCompactionScnInfo snapshot_info(
      MTL_ID(),
      ls_id,
      tablet_id,
      ObTabletReplica::SCN_STATUS_IDLE);
  if (OB_FAIL(ObTabletMetaTableCompactionOperator::get_status(snapshot_info, ret_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // first schedule medium snapshot
      ret_info.status_ = ObTabletReplica::SCN_STATUS_IDLE;
    } else {
      LOG_WARN("failed to get cur medium snapshot", K(ret), K(ret_info));
    }
  }
  return ret;
}

// cal this func with PLAF LEADER ROLE && last_medium_scn_ = 0
int ObMediumCompactionScheduleFunc::schedule_next_medium_for_leader(
    const int64_t major_snapshot,
    const bool is_tombstone,
    bool &medium_clog_submitted)
{
  int ret = OB_SUCCESS;
  ObRole role = INVALID_ROLE;
  if (OB_FAIL(ls_.get_ls_role(role))) {
    LOG_WARN("failed to get ls role", K(ret), KPC(this));
  } else if (LEADER == role) {
    // only log_handler_leader can schedule
#ifdef ERRSIM
    ret = OB_E(EventTable::EN_SKIP_INDEX_MAJOR) ret;
    // skip schedule major for user index table
    ObTablet *tablet = nullptr;
    if (OB_FAIL(ret) && tablet_handle_.is_valid() && OB_NOT_NULL(tablet = tablet_handle_.get_obj())) {
      if (tablet->get_tablet_meta().tablet_id_.id() > ObTabletID::MIN_USER_TABLET_ID
        && tablet->get_tablet_meta().tablet_id_ != tablet->get_tablet_meta().data_tablet_id_) {
        LOG_INFO("ERRSIM EN_SKIP_INDEX_MAJOR", K(ret), KPC(tablet));
        return ret;
      } else {
        ret = OB_SUCCESS;
      }
    }
#endif
    ret = schedule_next_medium_primary_cluster(major_snapshot, is_tombstone, medium_clog_submitted);
  } else {
    LOG_TRACE("not leader", K(ret), K(role), K(ls_.get_ls_id()));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::get_adaptive_reason(
  const int64_t schedule_major_snapshot)
{
  int ret = OB_SUCCESS;
  int64_t max_sync_medium_scn = 0;
  ObTablet *tablet = tablet_handle_.get_obj();
  if (OB_FAIL(ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
      *tablet, *medium_info_list_, max_sync_medium_scn))) {
    LOG_WARN("failed to get max received medium scn", KR(ret), KPC(this));
  } else if (schedule_major_snapshot > max_sync_medium_scn) {
    merge_reason_ = ObAdaptiveMergePolicy::AdaptiveMergeReason::TENANT_MAJOR;
  } else if (OB_FAIL(ObAdaptiveMergePolicy::get_adaptive_merge_reason(*tablet, merge_reason_))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get meta merge priority", K(ret), KPC(this));
    } else {
      ret = OB_SUCCESS;
    }
  }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      if (tablet->get_tablet_meta().tablet_id_.id() > ObTabletID::MIN_USER_TABLET_ID) {
        ret = OB_E(EventTable::EN_SCHEDULE_MEDIUM_COMPACTION) ret;
        LOG_INFO("errsim", K(ret), KPC(this));
        if (OB_FAIL(ret)) {
          FLOG_INFO("errsim EN_SCHEDULE_MEDIUM_COMPACTION", KPC(this));
          ret = OB_SUCCESS;
          merge_reason_ = ObAdaptiveMergePolicy::AdaptiveMergeReason::LOAD_DATA_SCENE;
        }
      }
    }
#endif
  return ret;
}

int ObMediumCompactionScheduleFunc::schedule_next_medium_primary_cluster(
    const int64_t schedule_major_snapshot,
    const bool is_tombstone,
    bool &medium_clog_submitted)
{
  int ret = OB_SUCCESS;
  ObTabletCompactionScnInfo ret_info;
  // check last medium type, select inner table for last major
  bool schedule_medium_flag = false;
  int64_t max_sync_medium_scn = 0;
  int64_t last_major_snapshot_version = 0;
  ObTablet *tablet = nullptr;

  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else if (FALSE_IT(last_major_snapshot_version = tablet->get_last_major_snapshot_version())) {
  } else if (0 >= last_major_snapshot_version) {
    // no major, do nothing
  } else if (OB_ISNULL(medium_info_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium info list is unexpected null", K(ret), KPC(this), KPC_(medium_info_list));
  } else if (!medium_info_list_->could_schedule_next_round(last_major_snapshot_version)) { // check serialized list
    // do nothing
  } else if (!ObAdaptiveMergePolicy::is_valid_merge_reason(merge_reason_)
      && OB_FAIL(get_adaptive_reason(schedule_major_snapshot))) {
    LOG_WARN("failed to get adaptive reason", KR(ret), K(schedule_major_snapshot));
  } else if (ObAdaptiveMergePolicy::is_valid_merge_reason(merge_reason_)) {
    schedule_medium_flag = true;
  } else if (is_tombstone && ObAdaptiveMergePolicy::NONE == merge_reason_) {
    merge_reason_ = ObAdaptiveMergePolicy::TOMBSTONE_SCENE;
    schedule_medium_flag = true;
  }
  LOG_TRACE("schedule next medium in primary cluster", K(ret), KPC(this), K(schedule_medium_flag),
      K(schedule_major_snapshot), K(merge_reason_), K(last_major_snapshot_version), KPC_(medium_info_list), K(max_sync_medium_scn));
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      if (tablet->get_tablet_meta().tablet_id_.id() > ObTabletID::MIN_USER_TABLET_ID) {
        ret = OB_E(EventTable::EN_SCHEDULE_MEDIUM_COMPACTION) ret;
        if (OB_FAIL(ret)) {
          FLOG_INFO("ERRSIM EN_SCHEDULE_MEDIUM_COMPACTION", KPC(this));
          ret = OB_SUCCESS;
          schedule_medium_flag = true;
        }
      }
    }
#endif
  bool schedule_flag = false;
  if (OB_FAIL(ret) || !schedule_medium_flag) {
  } else if (MTL(ObTenantTabletScheduler*)->get_inner_table_merged_scn() >= last_major_snapshot_version) {
    schedule_flag = true;
  } else if (ObMediumCompactionInfo::MAJOR_COMPACTION == medium_info_list_->get_last_compaction_type()) {
    // for normal medium, checksum error happened, wait_check_medium_scn_ will never = 0
    // for major, need select inner_table to check RS status
    if (OB_FAIL(get_status_from_inner_table(ls_.get_ls_id(), tablet->get_tablet_meta().tablet_id_, ret_info))) {
      LOG_WARN("failed to get status from inner tablet", K(ret), KPC(this));
    } else if (ret_info.could_schedule_next_round(medium_info_list_->get_last_compaction_scn())) {
      LOG_INFO("success to check RS major checksum validation finished", K(ret), KPC(this), K(ret_info));
      schedule_flag = true;
    } else if (OB_NOT_NULL(schedule_stat_)) {
      ++schedule_stat_->wait_rs_validate_cnt_;
      LOG_TRACE("cannot schedule next round merge now", K(ret), K(ret_info));
    }
  } else {
    schedule_flag = true;
  }
  if (OB_SUCC(ret) && schedule_flag) {
    ret = decide_medium_snapshot(medium_clog_submitted);
  }

  return ret;
}

int ObMediumCompactionScheduleFunc::choose_scn_for_user_request(
    const int64_t max_sync_medium_scn,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  // check exist not finish freeze info
  schema_version = 0;
  int64_t max_reserved_snapshot = 0;
  const int64_t latest_frozen_version = MTL(ObTenantFreezeInfoMgr*)->get_latest_frozen_version();
  const int64_t last_major_snapshot_version = tablet_handle_.get_obj()->get_last_major_snapshot_version();
  ObTablet *tablet = nullptr;

  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
    LOG_WARN("major sstable should not be empty", K(ret), KPC(this));
  } else if (latest_frozen_version > last_major_snapshot_version) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("unfinished freeze info exist, can't schedule another medium", K(ret));
  } else if (OB_FAIL(get_max_reserved_snapshot(max_reserved_snapshot))) {
    LOG_WARN("failed to get reserved snapshot", K(ret), KPC(this));
  } else if (FALSE_IT(medium_info.medium_snapshot_ = MAX(max_reserved_snapshot, weak_read_ts_))) {
  } else if (medium_info.medium_snapshot_ < max_sync_medium_scn) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("chosen medium snapshot is synced before", K(ret), K(medium_info), K(max_sync_medium_scn));
  } else {
    medium_info.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
    medium_info.medium_merge_reason_ = merge_reason_;
    if (OB_FAIL(ObPartitionMergePolicy::get_result_by_snapshot(*tablet, medium_info.medium_snapshot_, result))) {
      LOG_WARN("failed to get result for major", K(ret), K(last_major_snapshot_version), K(medium_info));
    } else if (OB_FAIL(tablet->get_newest_schema_version(schema_version))) {
      LOG_WARN("failed to get schema version from tablet", K(ret), KPC(tablet));
    } else {
      LOG_INFO("choose medium_scn for user request", K(ret), K(result), K(schema_version), K(medium_info),
        K(max_sync_medium_scn), K(max_reserved_snapshot));
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::check_frequency(
  const int64_t max_reserved_snapshot,
  const int64_t medium_snapshot)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle_.get_obj();
  const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
  const int64_t current_time = ObTimeUtility::current_time_ns();
  if (max_reserved_snapshot < current_time) {
    const int64_t time_interval = (current_time - max_reserved_snapshot) / 2;
    const int64_t last_major_snapshot_version = tablet->get_last_major_snapshot_version();
    if (0 >= last_major_snapshot_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("major sstable should not be empty", K(ret), K(last_major_snapshot_version));
    } else if (last_major_snapshot_version + time_interval > medium_snapshot) {
      // TODO(chengkong): for better performance, here should take meta major merge in the future.
      ObTableQueuingModeCfg queuing_cfg;
      if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->get_queuing_cfg(ls_.get_ls_id(), tablet_id, queuing_cfg))) {
        LOG_WARN_RET(tmp_ret, "failed to get table queuing mode, treat it as normal table", "ls_id", ls_.get_ls_id(), K(tablet_id));
        ret = OB_NO_NEED_MERGE;
        LOG_TRACE("schedule medium frequently", K(ret), K(last_major_snapshot_version), K(medium_snapshot), K(time_interval));
      } else if (queuing_cfg.is_queuing_mode()) {
        const int64_t cooling_down_interval = ObAdaptiveMergePolicy::MEDIUM_COOLING_TIME_THRESHOLD_NS * queuing_cfg.queuing_factor_;
        const bool max_reserved_cooling_down = last_major_snapshot_version + time_interval * queuing_cfg.queuing_factor_ > medium_snapshot;
        const bool medium_is_cooling_down = last_major_snapshot_version  + cooling_down_interval > ObTimeUtility::current_time_ns();
        if (max_reserved_cooling_down && medium_is_cooling_down) {
          ret = OB_NO_NEED_MERGE;
          LOG_DEBUG("schedule queuing medium frequently", K(ret), KPC(tablet), K(medium_snapshot), K(time_interval), K(queuing_cfg),
                    K(cooling_down_interval), K(max_reserved_cooling_down), K(medium_is_cooling_down));
        }
      } else {
        ret = OB_NO_NEED_MERGE;
        LOG_TRACE("schedule medium frequently", K(ret), K(last_major_snapshot_version), K(medium_snapshot), K(time_interval));
      }
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::get_max_reserved_snapshot(int64_t &max_reserved_snapshot)
{
  int ret = OB_SUCCESS;
  max_reserved_snapshot = INT64_MAX;

  ObStorageSnapshotInfo snapshot_info;
  int64_t last_major_snapshot_version = 0;
  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else if (FALSE_IT(last_major_snapshot_version = tablet->get_last_major_snapshot_version())) {
  } else if (0 >= last_major_snapshot_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major sstable should not be empty", K(ret), K(last_major_snapshot_version));
  } else if (0 == ls_.get_min_reserved_snapshot()) {
    ret = OB_NO_NEED_MERGE;
    // not sync reserved snapshot yet, should not schedule now
  } else if (OB_FAIL(MTL(ObTenantFreezeInfoMgr*)->get_min_reserved_snapshot(
      tablet->get_tablet_meta().tablet_id_, last_major_snapshot_version, snapshot_info))) {
    LOG_WARN("failed to get reserved snapshot from freeze info mgr", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_);
  } else {
    max_reserved_snapshot = MAX(ls_.get_min_reserved_snapshot(), snapshot_info.snapshot_);
    LOG_TRACE("get max reserved snapshot", KR(ret), K(max_reserved_snapshot), K(snapshot_info));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::choose_new_medium_snapshot(
  const int64_t max_reserved_snapshot,
  ObMediumCompactionInfo &medium_info,
  ObGetMergeTablesResult &result,
  int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle_.get_obj();
  int64_t snapshot_gc_ts = 0;
  if (medium_info.medium_snapshot_ == tablet->get_snapshot_version() //  no uncommitted sstable
      && weak_read_ts_ + DEFAULT_SCHEDULE_MEDIUM_INTERVAL < ObTimeUtility::current_time_ns()) {
    snapshot_gc_ts = MTL(ObTenantFreezeInfoMgr *)->get_snapshot_gc_ts();
    // data before weak_read_ts & latest storage schema on memtable is match for schedule medium
    medium_info.medium_snapshot_ = MAX(max_reserved_snapshot, MIN(weak_read_ts_, snapshot_gc_ts));
  }
  if (medium_info.medium_snapshot_ < max_reserved_snapshot) {
    // may not rewrite medium_snapshot above
    ret = OB_NO_NEED_MERGE;
  } else {
    LOG_INFO("use weak_read_ts to schedule medium", K(ret), KPC(this),
      K(medium_info), K(max_reserved_snapshot), K_(weak_read_ts), K(snapshot_gc_ts));
  }
  // update schema version for cur medium scn
  if (FAILEDx(tablet->get_newest_schema_version(schema_version))) {
    LOG_WARN("failed to get schema version from tablet", K(ret), KPC(tablet));
  } else {
    LOG_INFO("chosen new medium snapshot", K(ret), KPC(this),
      K(medium_info), K(max_reserved_snapshot), K(result), K(schema_version),
             K(medium_info), K(max_reserved_snapshot), K_(weak_read_ts), K(snapshot_gc_ts));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::decide_medium_snapshot(bool &medium_clog_submitted)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t max_sync_medium_scn = 0;
  uint64_t compat_version = 0;
  ObTablet *tablet = nullptr;
  medium_clog_submitted = false;
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (OB_FAIL(MTL(ObTenantTabletScheduler*)->get_min_data_version(compat_version))) {
    LOG_WARN("failed to get min data version", KR(ret));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_1_0_0)) {
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
      *tablet, *medium_info_list_, max_sync_medium_scn))) {
      LOG_WARN("failed to get max sync medium scn", KR(ret), KPC(this));
  } else if (OB_FAIL(ls_.add_dependent_medium_tablet(tablet->get_tablet_meta().tablet_id_))) { // add dependent_id in ObLSReservedSnapshotMgr
    LOG_WARN("failed to add dependent tablet", K(ret), KPC(this));
  } else {
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    LOG_TRACE("decide_medium_snapshot", K(ret), KPC(this), K(compat_version), K(tablet_id), K(max_sync_medium_scn), K_(merge_reason));
    int64_t schema_version = 0;
    ObGetMergeTablesResult result;
    ObMediumCompactionInfo medium_info;

    if (OB_FAIL(medium_info.init_data_version(compat_version))) {
      LOG_WARN("fail to set data version", K(ret), K(tablet_id), K(compat_version));
    } else if (is_user_request(merge_reason_)) {
      if (OB_FAIL(choose_scn_for_user_request(max_sync_medium_scn, medium_info, result, schema_version))) {
        LOG_WARN("failed to choose medium scn for user request", K(ret), KPC(this));
      }
    } else if (ObAdaptiveMergePolicy::TENANT_MAJOR == merge_reason_) {
      if (OB_FAIL(choose_major_snapshot(max_sync_medium_scn, medium_info, result, schema_version))) {
        LOG_WARN("failed to choose medium scn for major", K(ret), KPC(this));
      }
    } else if (OB_FAIL(choose_medium_snapshot(max_sync_medium_scn, medium_info, result, schema_version))) {
      LOG_WARN("failed to choose medium scn for medium", K(ret), KPC(this));
    }
    if (OB_FAIL(ret)) {
    } else if (medium_info.medium_snapshot_ <= max_sync_medium_scn) {
      ret = OB_NO_NEED_MERGE;
    }
#ifdef ERRSIM
    if (OB_SUCC(ret) || OB_NO_NEED_MERGE == ret) {
      ret = errsim_choose_medium_snapshot(max_sync_medium_scn, medium_info, result);
    }
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_SCHEDULE_MEDIUM_FAILED) ret;
      if (OB_FAIL(ret)) {
        LOG_INFO("ERRSIM EN_SCHEDULE_MEDIUM_FAILED", K(ret));
      }
    }
#endif
    if (FAILEDx(prepare_medium_info(result, schema_version, medium_info))) {
      if (OB_TABLE_IS_DELETED == ret || OB_NO_NEED_MERGE == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to prepare medium info", K(ret), K(result));
      }
    } else if (OB_FAIL(submit_medium_clog(medium_info))) {
      LOG_WARN("failed to submit medium clog and update inner table", K(ret), KPC(this));
    } else {
      medium_clog_submitted = true;
      if (OB_NOT_NULL(schedule_stat_)) {
        ++schedule_stat_->submit_clog_cnt_;
      }
    }
    // delete tablet_id in ObLSReservedSnapshotMgr even if submit clog or update inner table failed
    if (OB_TMP_FAIL(ls_.del_dependent_medium_tablet(tablet_id))) {
      LOG_ERROR("failed to delete dependent medium tablet", K(tmp_ret), KPC(this));
      ob_abort();
    }
    ret = OB_NO_NEED_MERGE == ret ? OB_SUCCESS : ret;
    if (OB_FAIL(ret)) {
        // add schedule suspect info
        if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MEDIUM_MERGE, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                ls_.get_ls_id(), tablet_id,
                ObSuspectInfoType::SUSPECT_SCHEDULE_MEDIUM_FAILED,
                medium_info.medium_snapshot_,
                medium_info.storage_schema_.store_column_cnt_,
                static_cast<int64_t>(ret)))) {
        LOG_WARN("failed to add suspect info", K(tmp_ret));
      }
    }
  }
  return ret;
}

#ifdef ERRSIM
int ObMediumCompactionScheduleFunc::errsim_choose_medium_snapshot(
  const int64_t max_sync_medium_scn,
  ObMediumCompactionInfo &medium_info,
  ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObTablet &tablet = *tablet_handle_.get_obj();
  if (tablet.get_tablet_meta().tablet_id_.id() > ObTabletID::MIN_USER_TABLET_ID) {
    ret = OB_E(EventTable::EN_SCHEDULE_MEDIUM_COMPACTION) ret;
  }
  if (OB_FAIL(ret)) {
    LOG_INFO("ERRSIM EN_SCHEDULE_MEDIUM_COMPACTION", K(ret), KPC(this));
    const int64_t snapshot_gc_ts =
        MTL(ObTenantFreezeInfoMgr *)->get_snapshot_gc_ts();
    medium_info.medium_snapshot_ = MIN(weak_read_ts_, snapshot_gc_ts);
    medium_info.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
    int64_t max_reserved_snapshot = 0;
    (void) result.reset();
    if (OB_FAIL(get_max_reserved_snapshot(max_reserved_snapshot))) {
      LOG_WARN("failed to get reserved snapshot", K(ret), KPC(this));
    } else if (medium_info.medium_snapshot_ <= max_sync_medium_scn
        || medium_info.medium_snapshot_ < max_reserved_snapshot) {
      ret = OB_NO_NEED_MERGE;
    } else if (OB_FAIL(ObPartitionMergePolicy::get_result_by_snapshot(tablet, medium_info.medium_snapshot_, result))) {
      LOG_WARN("failed to get result by snapshot", K(ret), K(medium_info), KPC(this));
    } else {
      FLOG_INFO("ERRSIM EN_SCHEDULE_MEDIUM_COMPACTION", KPC(this));
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
#endif

int ObMediumCompactionScheduleFunc::init_schema_changed(
  ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  int64_t full_stored_col_cnt = 0;
  const ObStorageSchema &schema = medium_info.storage_schema_;
  if (OB_UNLIKELY(!schema.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is not inited", KR(ret), K(schema));
  } else if (OB_FAIL(schema.get_stored_column_count_in_sstable(full_stored_col_cnt))) {
    LOG_WARN("failed to get stored column count in sstable", K(ret), K(schema));
  } else if (OB_UNLIKELY(tablet_handle_.get_obj()->get_last_major_column_count() > full_stored_col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stored col cnt in curr schema is less than old major sstable", K(ret),
      "col_cnt_in_sstable", tablet_handle_.get_obj()->get_last_major_column_count(),
      "col_cnt_in_schema", full_stored_col_cnt, KPC(this));
  } else if (tablet_handle_.get_obj()->get_last_major_column_count() != full_stored_col_cnt
    || tablet_handle_.get_obj()->get_last_major_compressor_type() != schema.get_compressor_type()
    || (ObRowStoreType::DUMMY_ROW_STORE != tablet_handle_.get_obj()->get_last_major_latest_row_store_type()
      && tablet_handle_.get_obj()->get_last_major_latest_row_store_type() != schema.row_store_type_)) {
    medium_info.is_schema_changed_ = true;
    LOG_INFO("schema changed", K(schema),
      "col_cnt_in_sstable", tablet_handle_.get_obj()->get_last_major_column_count(),
      "compressor_type_in_sstable", tablet_handle_.get_obj()->get_last_major_compressor_type(),
      "latest_row_store_type_in_sstable", tablet_handle_.get_obj()->get_last_major_latest_row_store_type());
  } else {
    medium_info.is_schema_changed_ = false;
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::init_parallel_range_and_schema_changed_and_co_merge_type(
    const ObGetMergeTablesResult &result,
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  int64_t expected_task_count = 0;
  const int64_t tablet_size = medium_info.storage_schema_.get_tablet_size();
  const ObSSTable *first_sstable = static_cast<const ObSSTable *>(result.handle_.get_table(0));

  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else if (OB_ISNULL(first_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable is unexpected null", K(ret), K(result));
  } else {
    const int64_t macro_block_cnt = first_sstable->get_data_macro_block_count();
    int64_t inc_row_cnt = 0;
    int64_t inc_macro_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
      inc_row_cnt += static_cast<const ObSSTable*>(result.handle_.get_table(i))->get_row_count();
      inc_macro_cnt += static_cast<const ObSSTable*>(result.handle_.get_table(i))->get_data_macro_block_count();
    }

    if (OB_FAIL(ret)) {
    } else if ((0 == macro_block_cnt && inc_row_cnt > SCHEDULE_RANGE_ROW_COUNT_THRESHOLD)
        || (first_sstable->get_row_count() >= SCHEDULE_RANGE_ROW_COUNT_THRESHOLD
            && inc_row_cnt >= first_sstable->get_row_count() * SCHEDULE_RANGE_INC_ROW_COUNT_PERCENRAGE_THRESHOLD)) {
      const int64_t estimate_macro_cnt = macro_block_cnt + inc_macro_cnt / 5;
      if (OB_FAIL(ObParallelMergeCtx::get_concurrent_cnt(tablet_size, estimate_macro_cnt, expected_task_count))) {
        STORAGE_LOG(WARN, "failed to get concurrent cnt", K(ret), K(tablet_size), K(expected_task_count),
          KPC(first_sstable));
      }
    }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_MEDIUM_INIT_PARALLEL_RANGE) ret;
    if (OB_FAIL(ret)) {
      expected_task_count = 2;
      LOG_INFO("ERRSIM EN_COMPACTION_MEDIUM_INIT_PARALLEL_RANGE", KPC(this), K(expected_task_count));
      ret = OB_SUCCESS;
    } else {
      ret = OB_E(EventTable::EN_COMPACTION_MEDIUM_INIT_LARGE_PARALLEL_RANGE) ret;
      if (OB_FAIL(ret)) {
        expected_task_count = 64;
        LOG_INFO("ERRSIM EN_COMPACTION_MEDIUM_INIT_LARGE_PARALLEL_RANGE", KPC(this), K(expected_task_count));
        ret = OB_SUCCESS;
      }
    }
  }
#endif
    // init is_schema_changed
    if (FAILEDx(init_schema_changed(medium_info))) {
      STORAGE_LOG(WARN, "failed to init schema changed", KR(ret), K(first_sstable));
    } else if (!tablet->is_row_store() && OB_FAIL(init_co_major_merge_type(result, medium_info))) {
      STORAGE_LOG(WARN, "failed to init co major merge type");
    }

    if (OB_FAIL(ret)) {
    } else if (expected_task_count <= 1) {
      medium_info.clear_parallel_range();
    } else {
      ObTableStoreIterator table_iter;
      ObArrayArray<ObStoreRange> range_array;
      ObPartitionMultiRangeSpliter range_spliter;
      ObSEArray<ObStoreRange, 1> input_range_array;
      ObStoreRange range;
      range.set_start_key(ObStoreRowkey::MIN_STORE_ROWKEY);
      range.set_end_key(ObStoreRowkey::MAX_STORE_ROWKEY);
      const bool is_major = medium_info.is_major_compaction();
      lib::CompatModeGuard g(tablet->get_tablet_meta().compat_mode_);
      if (OB_FAIL(prepare_iter(result, table_iter))) {
        LOG_WARN("failed to get table iter", K(ret), K(range_array));
      } else if (OB_FAIL(input_range_array.push_back(range))) {
        LOG_WARN("failed to push back range", K(ret), K(range));
      } else {
        bool recalc_count_flag = false;
        do {
          if (OB_FAIL(range_spliter.get_split_multi_ranges(
                  input_range_array,
                  expected_task_count,
                  tablet->get_rowkey_read_info(),
                  table_iter,
                  allocator_,
                  range_array))) {
            LOG_WARN("failed to get split multi range", K(ret), K(range_array));
          } else if (OB_FAIL(medium_info.gene_parallel_info(allocator_, range_array))) {
            LOG_WARN("failed to get parallel ranges", K(ret), K(range_array));
          } else {
            int64_t buf_len = ObTabletMediumCompactionInfoRecorder::cal_buf_len(tablet->get_tablet_meta().tablet_id_, medium_info, nullptr/*log_header*/);
#ifdef ERRSIM
            ret = OB_E(EventTable::EN_COMPACTION_MEDIUM_INIT_LARGE_PARALLEL_RANGE) ret;
            if (OB_FAIL(ret)) {
              ret = OB_SUCCESS;
              if (!recalc_count_flag) {
                buf_len = common::OB_MAX_LOG_ALLOWED_SIZE;
              }
            }
#endif
            if (buf_len < common::OB_MAX_LOG_ALLOWED_SIZE) {
              LOG_TRACE("success to split ranges", KR(ret), K(buf_len), K(medium_info.parallel_merge_info_), K(range_array), K(medium_info.parallel_merge_info_.get_serialize_size()));
              break;
            } else if (recalc_count_flag) {
              expected_task_count -= MAX(1, expected_task_count / 5);
            } else {
              recalc_count_flag = true;
              // get parallel info serialize size
              const int64_t parallel_size = medium_info.parallel_merge_info_.get_serialize_size();
              const double avg_range_size = (parallel_size + 0.0) / range_array.count();
              const int64_t rest_info_size = buf_len - parallel_size;
              expected_task_count = MAX(1, (common::OB_MAX_LOG_ALLOWED_SIZE - 1 - rest_info_size) / avg_range_size);
              expected_task_count = MIN(expected_task_count, MAX_MERGE_THREAD);
              LOG_INFO("success to recalc ranges", KR(ret), K(buf_len), K(expected_task_count), K(avg_range_size), K(rest_info_size));
            }
            medium_info.clear_parallel_range();
            table_iter.resume();
            range_array.reuse();
          }
        } while (OB_SUCC(ret) && !medium_info.contain_parallel_range_ && expected_task_count > 1);
      }
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::init_co_major_merge_type(
    const ObGetMergeTablesResult &result,
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  ObSSTable *first_sstable = static_cast<ObSSTable *>(result.handle_.get_table(0));
  ObCOSSTableV2 *co_sstable = nullptr;
  ObCOMajorMergePolicy::ObCOMajorMergeType major_merge_type = ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE;
  ObTabletTableIterator iter;
  ObSEArray<ObITable*, OB_DEFAULT_SE_ARRAY_COUNT> tables;
  if (OB_ISNULL(first_sstable) || OB_UNLIKELY(!first_sstable->is_co_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("first sstable in tables handle is null or not co sstable", K(ret), K(result.handle_));
  } else if (FALSE_IT(co_sstable = static_cast<ObCOSSTableV2 *>(first_sstable))) {
  } else if (OB_FAIL(iter.set_tablet_handle(tablet_handle_))) {
    LOG_WARN("failed to set tablet handle", K(ret), K(iter), K(tablet_handle_));
  } else if (OB_FAIL(iter.get_read_tables_from_tablet(medium_info.medium_snapshot_, false/*allow_no_ready_read*/, false/*major_sstable_only*/, tables))) {
    LOG_WARN("failed to get read tables for estimate row cnt", K(ret), K(medium_info), K(iter));
  } else if (OB_FAIL(ObCOMajorMergePolicy::decide_co_major_merge_type(
          *co_sstable,
          tables,
          medium_info.storage_schema_,
          tablet_handle_,
          major_merge_type))) {
    LOG_WARN("failed to decide co major merge type", K(ret));
  } else {
    medium_info.co_major_merge_type_ = major_merge_type;
    LOG_DEBUG("chengkong debug: success to get ",
      "major_merge_type", ObCOMajorMergePolicy::co_major_merge_type_to_str(major_merge_type));
  }

  return ret;
}

int ObMediumCompactionScheduleFunc::get_result_for_major(
      ObTablet &tablet,
      const ObMediumCompactionInfo &medium_info,
      ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(medium_info), KPC(table_store_wrapper.get_member()));
  } else if (OB_ISNULL(base_table = static_cast<ObSSTable*>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("major sstable not exist", K(ret), KPC(table_store_wrapper.get_member()));
  } else if (base_table->get_snapshot_version() >= medium_info.medium_snapshot_) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_FAIL(result.handle_.add_sstable(base_table, table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("failed to add table into iterator", K(ret), KP(base_table));
  } else {
    const ObSSTableArray &minor_tables = table_store_wrapper.get_member()->get_minor_sstables();
    bool start_add_table_flag = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
      if (OB_ISNULL(minor_tables[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must not null", K(ret), K(i), K(minor_tables));
      } else if (!start_add_table_flag
        && minor_tables[i]->get_upper_trans_version() >= base_table->get_snapshot_version()) {
        start_add_table_flag = true;
      }
      if (OB_SUCC(ret) && start_add_table_flag) {
        if (OB_FAIL(result.handle_.add_sstable(minor_tables[i], table_store_wrapper.get_meta_handle()))) {
          LOG_WARN("failed to add table", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::prepare_iter(
    const ObGetMergeTablesResult &result,
    ObTableStoreIterator &table_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result.handle_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle is invalid", K(ret), K(result));
  }
  for (int i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
    if (OB_FAIL(table_iter.add_table(result.handle_.get_table(i)))) {
      LOG_WARN("failed to add table into table_iter", K(ret), K(i));
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::prepare_medium_info(
    const ObGetMergeTablesResult &result,
    const int64_t schema_version,
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  medium_info.cluster_id_ = GCONF.cluster_id;
  medium_info.tenant_id_ = MTL_ID();
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (OB_UNLIKELY(result.handle_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table handle in result is empty", KR(ret), K(result));
  } else if (0 == schema_version) { // not formal schema version
    ret = OB_NO_NEED_MERGE;
    LOG_TRACE("not formal schema version", KR(ret), KPC(this), K(schema_version));
  } else if (medium_info.is_medium_compaction()) {
    ObMultiVersionSchemaService *schema_service = nullptr;
    ObTablet *tablet = tablet_handle_.get_obj();
    if (OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get schema service from MTL", K(ret));
    } else if (FALSE_IT(medium_info.storage_schema_.reset())) {
    } else if (OB_FAIL(get_table_schema_to_merge(*schema_service, *tablet, schema_version,
          medium_info.medium_compat_version_, allocator_, medium_info.storage_schema_))) {
      // for major compaction, storage schema is inited in choose_major_snapshot
      if (OB_TABLE_IS_DELETED != ret) {
        LOG_WARN("failed to get table schema", KR(ret), KPC(this), K(medium_info));
      }
    }
  }
  if (FAILEDx(init_parallel_range_and_schema_changed_and_co_merge_type(result, medium_info))) {
    LOG_WARN("failed to init parallel range", K(ret), K(medium_info));
  } else {
    medium_info.last_medium_snapshot_ = result.handle_.get_table(0)->get_snapshot_version();
    LOG_TRACE("success to prepare medium info", K(ret), K(medium_info));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::get_table_id(
    ObMultiVersionSchemaService &schema_service,
    const ObTabletID &tablet_id,
    const int64_t schema_version,
    uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;

  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<uint64_t, 1> table_ids;
  if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    LOG_WARN("failed to add tablet id", K(ret));
  } else if (OB_FAIL(schema_service.get_tablet_to_table_history(MTL_ID(), tablet_ids, schema_version, table_ids))) {
    LOG_WARN("failed to get table id according to tablet id", K(ret), K(schema_version));
  } else if (OB_UNLIKELY(table_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty table id", K(ret), K(table_ids));
  } else if (table_ids.at(0) == OB_INVALID_ID){
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("table is deleted", K(ret), K(tablet_id), K(schema_version));
  } else {
    table_id = table_ids.at(0);
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::get_table_schema_to_merge(
    ObMultiVersionSchemaService &schema_service,
    const ObTablet &tablet,
    const int64_t schema_version,
    const int64_t medium_compat_version,
    ObIAllocator &allocator,
    ObStorageSchema &storage_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  uint64_t table_id = OB_INVALID_ID;
  schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  int64_t save_schema_version = schema_version;
  if (OB_FAIL(get_table_id(schema_service, tablet_id, schema_version, table_id))) {
    if (OB_TABLE_IS_DELETED != ret) {
      LOG_WARN("failed to get table id", K(ret), K(tablet_id));
    }
  } else if (OB_FAIL(schema_service.retry_get_schema_guard(tenant_id,
                                                            schema_version,
                                                            table_id,
                                                            schema_guard,
                                                            save_schema_version))) {
    if (OB_TABLE_IS_DELETED == ret) {
      LOG_WARN("table is deleted", K(ret), K(table_id));
    } else if (OB_ERR_SCHEMA_HISTORY_EMPTY == ret) {
      LOG_WARN("schema history may recycle", K(ret));
    } else {
      LOG_WARN("Fail to get schema", K(ret), K(tenant_id), K(schema_version), K(table_id));
    }
  } else if (OB_UNLIKELY(save_schema_version < schema_version)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("can not use older schema version", K(ret), K(schema_version), K(save_schema_version), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("Fail to get table schema", K(ret), K(table_id));
  } else if (NULL == table_schema) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("table is deleted", K(ret), K(table_id));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    static bool have_set_errno = false;
    static ObTabletID errno_tablet_id;
    ret = OB_E(EventTable::EN_SCHEDULE_MAJOR_GET_TABLE_SCHEMA) ret;
    if (OB_FAIL(ret)) {
      if (tablet_id.id() > ObTabletID::MIN_USER_TABLET_ID
        && tablet_id != tablet.get_tablet_meta().data_tablet_id_
        && ATOMIC_BCAS(&have_set_errno, false, true)) {
        LOG_INFO("ERRSIM EN_SCHEDULE_MAJOR_GET_TABLE_SCHEMA", K(ret), K(table_id), K(tablet_id), K(storage_schema));
        errno_tablet_id = tablet_id;
        return ret;
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
#endif

  int64_t storage_schema_version =  ObStorageSchema::STORAGE_SCHEMA_VERSION_V3;

  if (medium_compat_version < ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2) {
    storage_schema_version = ObStorageSchema::STORAGE_SCHEMA_VERSION;
  } else if (medium_compat_version < ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V3) {
    storage_schema_version = ObStorageSchema::STORAGE_SCHEMA_VERSION_V2;
  }
  // for old version medium info, need generate old version schema
  if (FAILEDx(storage_schema.init(
          allocator, *table_schema, tablet.get_tablet_meta().compat_mode_, false/*skip_column_info*/,
          storage_schema_version))) {
    LOG_WARN("failed to init storage schema", K(ret), K(schema_version));
  } else {
    LOG_INFO("get schema to merge", K(tablet_id), K(table_id), K(schema_version), K(save_schema_version),
              K(storage_schema), K(*reinterpret_cast<const ObPrintableTableSchema*>(table_schema)));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::submit_medium_clog(
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_MEDIUM_COMPACTION_SUBMIT_CLOG_FAILED) ret;
  if (OB_FAIL(ret)) {
    LOG_INFO("ERRSIM EN_MEDIUM_COMPACTION_SUBMIT_CLOG_FAILED", KPC(this));
    return ret;
  }
#endif
  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else if (OB_FAIL(tablet->submit_medium_compaction_clog(medium_info, allocator_))) {
    LOG_WARN("failed to submit medium compaction clog", K(ret), K(medium_info));
  } else {
    LOG_INFO("success to submit medium compaction clog", K(ret), KPC(this), K(medium_info));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::batch_check_medium_meta_table(
    const ObIArray<ObTabletCheckInfo> &tablet_ls_infos,
    const hash::ObHashMap<ObLSID, share::ObLSInfo> &ls_info_map,
    ObIArray<ObTabletCheckInfo> &finish_tablet_ls,
    ObCompactionTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (tablet_ls_infos.empty()) {
  } else {
    share::ObTabletReplicaFilterHolder filters;
    ObArray<ObTabletInfo> tablet_infos;
    tablet_infos.set_attr(ObMemAttr(MTL_ID(), "TabletInfos"));
    if (OB_FAIL(tablet_infos.reserve(tablet_ls_infos.count()))) {
      LOG_WARN("failed to reserve array", KR(ret), "array_cnt", tablet_ls_infos.count());
    } else if (OB_FAIL(init_tablet_filters(filters))) {
      LOG_WARN("failed to init tablet filters", K(ret));
    } else if (OB_FAIL(ObTabletTableOperator::batch_get_tablet_info(GCTX.sql_proxy_, MTL_ID(), tablet_ls_infos, share::OBCG_STORAGE /*group_list*/, tablet_infos))) {
      LOG_WARN("failed to get tablet info", K(ret), K(tablet_ls_infos));
    } else {
      time_guard.click(ObCompactionScheduleTimeGuard::SEARCH_META_TABLE);
      for (int64_t i = 0, idx = 0; OB_SUCC(ret) && i < tablet_infos.count() && idx < tablet_ls_infos.count(); ++idx) {
        bool merge_finish = false;
        const ObTabletCheckInfo &tablet_ls_info = tablet_ls_infos.at(idx);
        const ObTabletInfo &tablet_info = tablet_infos.at(i);
        const ObLSID &ls_id = tablet_info.get_ls_id();
        const ObTabletID &tablet_id = tablet_info.get_tablet_id();
        const int64_t check_medium_scn = tablet_ls_info.get_medium_scn();
        if (tablet_ls_info.get_ls_id() != ls_id
            || tablet_ls_info.get_tablet_id() != tablet_id) {
          LOG_INFO("tablet_ls_info has been deleted", K(tablet_ls_info), K(tablet_info));
        } else {
          if (OB_TMP_FAIL(check_medium_meta_table(check_medium_scn, tablet_info, filters, ls_info_map, merge_finish))) {
            LOG_WARN("failed to check medium meta table", K(tmp_ret), K(check_medium_scn), K(tablet_info));
          } else if (merge_finish &&
              OB_TMP_FAIL(finish_tablet_ls.push_back(ObTabletCheckInfo(tablet_id, ls_id, check_medium_scn)))) {
            LOG_WARN("fail to push back tablet_ls_infos", K(tmp_ret), K(tablet_info));
          }
          ++i;
        }
      }
      time_guard.click(ObCompactionScheduleTimeGuard::CHECK_META_TABLE);
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::check_medium_meta_table(
    const int64_t check_medium_snapshot,
    const ObTabletInfo &tablet_info,
    const share::ObTabletReplicaFilterHolder &filters,
    const hash::ObHashMap<ObLSID, share::ObLSInfo> &ls_info_map,
    bool &merge_finish)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  merge_finish = false;
  const ObLSID &ls_id = tablet_info.get_ls_id();

  if (OB_UNLIKELY(check_medium_snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(check_medium_snapshot), K(tablet_info));
  } else if (OB_UNLIKELY(!tablet_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tabled_id is invalid", K(ret), K(tablet_info));
  } else {
    const ObArray<ObTabletReplica> &replica_array = tablet_info.get_replicas();
    int64_t unfinish_cnt = 0;
    int64_t filter_cnt = 0;
    bool pass = true;
    const ObLSReplica *ls_replica = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < replica_array.count(); ++i) {
      const ObTabletReplica &replica = replica_array.at(i);
      if (OB_UNLIKELY(!replica.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica info is invalid", K(ret), K(tablet_info), K(replica));
      } else if (OB_FAIL(filters.check(replica, pass))) {
        LOG_WARN("filter replica failed", K(ret), K(replica), K(filters));
      } else if (!pass) {
        // do nothing
        filter_cnt++;
      } else if (replica.get_snapshot_version() >= check_medium_snapshot) {
        // replica may have check_medium_snapshot = 2, but have received medium info of 3,
        // when this replica is elected as leader, this will happened
      } else {
        share::ObLSInfo ls_info;
        const ObLSReplica *ls_replica = nullptr;
        if (OB_TMP_FAIL(ls_info_map.get_refactored(ls_id, ls_info))) {
          LOG_WARN("failed to get map", K(tmp_ret), K(ls_id));
          unfinish_cnt++;
        } else if (OB_ENTRY_NOT_EXIST ==
            (tmp_ret = ls_info.find(replica.get_server(), ls_replica))) {
            filter_cnt++;
            LOG_TRACE("filter by ls locality", K(tmp_ret), K(replica), K(ls_info));
        } else {
          LOG_TRACE("tablet unfinish", K(tmp_ret), K(ls_info), K(replica));
          unfinish_cnt++;
        }
      }
    } // end of for
    LOG_INFO("check_medium_compaction_finish", K(ret), K(tablet_info), K(check_medium_snapshot),
        K(unfinish_cnt), K(filter_cnt), "total_cnt", replica_array.count());

    if (0 == unfinish_cnt) { // merge finish
      merge_finish = true;
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::init_tablet_filters(share::ObTabletReplicaFilterHolder &filters)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(filters.set_filter_not_exist_server(ObAllServerTracer::get_instance()))) {
    LOG_WARN("fail to set not exist server filter", KR(ret));
  } else if (OB_FAIL(filters.set_filter_permanent_offline(ObAllServerTracer::get_instance()))) {
    LOG_WARN("fail to set filter", KR(ret));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::check_medium_checksum(
    const ObIArray<ObTabletReplicaChecksumItem> &checksum_items,
    ObIArray<ObTabletLSPair> &error_pairs,
    int64_t &item_idx,
    int &check_ret)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  check_ret = OB_SUCCESS;
  int64_t items_cnt = checksum_items.count();
  ObTabletReplicaChecksumItem prev_item;
  ObTabletReplicaChecksumItem curr_item;
  while (OB_SUCC(ret) && item_idx < items_cnt) {
    curr_item.reset();
    if (OB_FAIL(curr_item.assign(checksum_items.at(item_idx)))) {
      LOG_WARN("fail to assign tablet replica checksum item", KR(ret), K(item_idx), "item", checksum_items.at(item_idx));
    } else {
      if (prev_item.is_key_valid()) {
        if (curr_item.is_same_tablet(prev_item)) { // same tablet
          if (OB_TMP_FAIL(curr_item.verify_checksum(prev_item))) {
            LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "checksum error in tablet replica checksum", KR(tmp_ret),
              K(curr_item), K(prev_item));
            if (OB_SUCCESS == check_ret) {
              if (OB_TMP_FAIL(error_pairs.push_back(ObTabletLSPair(curr_item.tablet_id_, curr_item.ls_id_)))) {
                LOG_WARN("fail to push back error pair", K(tmp_ret), "tablet_id", curr_item.tablet_id_, "ls_id", curr_item.ls_id_);
              }
              check_ret = OB_CHECKSUM_ERROR;
            }
          }
#ifdef ERRSIM
          if (OB_SUCC(ret)) {
            ret = OB_E(EventTable::EN_MEDIUM_REPLICA_CHECKSUM_ERROR) OB_SUCCESS;
            if (OB_FAIL(ret)) {
              STORAGE_LOG(INFO, "ERRSIM EN_MEDIUM_REPLICA_CHECKSUM_ERROR", K(ret),
                  "tablet_id", curr_item.tablet_id_, "ls_id", curr_item.ls_id_);
              if (OB_TMP_FAIL(error_pairs.push_back(ObTabletLSPair(curr_item.tablet_id_, curr_item.ls_id_)))) {
                LOG_WARN("fail to push back error pair", K(tmp_ret), "tablet_id", curr_item.tablet_id_, "ls_id", curr_item.ls_id_);
              }
              check_ret = OB_CHECKSUM_ERROR;
            }
          }
#endif
        } else {
          break;
        }
      } else if (OB_FAIL(prev_item.assign(curr_item))) {
        LOG_WARN("fail to assign tablet replica checksum item", KR(ret), K(item_idx), K(curr_item));
      }
      ++item_idx;
    }
  } // end for while
  return ret;
}

int ObMediumCompactionScheduleFunc::batch_check_medium_checksum(
    const ObIArray<ObTabletReplicaChecksumItem> &checksum_items)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int check_ret = OB_SUCCESS;
  int64_t pair_idx = 0;
  int64_t item_idx = 0;
  int64_t items_cnt = checksum_items.count();
  int64_t affected_rows = 0;
  ObSEArray<ObTabletLSPair, 64> error_pairs;
  while (OB_SUCC(ret) && item_idx < items_cnt) {
    const ObTabletReplicaChecksumItem &tmp_item = checksum_items.at(item_idx);
    const ObTabletID &tablet_id = tmp_item.tablet_id_;
    const ObLSID &ls_id = tmp_item.ls_id_;
    if (OB_FAIL(check_medium_checksum(checksum_items, error_pairs, item_idx, check_ret))) {
      LOG_WARN("failed to check medium checksum", K(ret), K(item_idx));
    } else if (OB_SUCCESS == check_ret) {
      ObLSHandle ls_handle;
      ObTabletHandle unused_handle;
      if (OB_TMP_FAIL((MTL(storage::ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::COMPACT_MODE)))) {
        if (OB_LS_NOT_EXIST == tmp_ret) {
          LOG_TRACE("ls not exist", K(tmp_ret), K(ls_id));
        } else {
          LOG_WARN("failed to get ls", K(tmp_ret), K(ls_id));
        }
      } else if (OB_TMP_FAIL(ls_handle.get_ls()->update_medium_compaction_info(tablet_id, unused_handle))) {
        LOG_WARN("failed to update medium compaction info", K(tmp_ret), K(ls_id), K(tablet_id));
      } else {
        FLOG_INFO("finish check medium compaction info", K(tmp_ret), K(ls_id), K(tablet_id));
      }
    }
    ++pair_idx;
    check_ret = OB_SUCCESS;
  } // end for while
  if (!error_pairs.empty()) {
    if (OB_TMP_FAIL(ObTabletMetaTableCompactionOperator::batch_set_info_status(MTL_ID(), error_pairs, affected_rows))) {
      LOG_WARN("fail to batch set info status", KR(tmp_ret));
    } else {
      LOG_INFO("succ to batch set info status", K(ret), K(affected_rows), K(error_pairs));
    }
  }
  if (affected_rows > 0) {
    MTL(ObTenantTabletScheduler*)->update_error_tablet_cnt(affected_rows);
  }
  return ret;
}

// for Leader, clean wait_check_medium_scn
int ObMediumCompactionScheduleFunc::batch_check_medium_finish(
    const hash::ObHashMap<ObLSID, share::ObLSInfo> &ls_info_map,
    ObIArray<ObTabletCheckInfo> &finish_tablet_ls_infos,
    const ObIArray<ObTabletCheckInfo> &tablet_ls_infos,
    ObCompactionTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  if (tablet_ls_infos.empty()) {
  } else {
    // different ObTabletCheckInfo have different medium_check_scn
    ObArray<ObTabletReplicaChecksumItem> checksum_items;
    checksum_items.set_attr(ObMemAttr(MTL_ID(), "CkmItems"));
    if (OB_FAIL(batch_check_medium_meta_table(tablet_ls_infos, ls_info_map, finish_tablet_ls_infos, time_guard))) {
      LOG_WARN("failed to check inner table", K(ret), K(tablet_ls_infos));
    } else if (!finish_tablet_ls_infos.empty()) {
      if (OB_FAIL(checksum_items.reserve(finish_tablet_ls_infos.count()))) {
        LOG_WARN("failed to reserve array", KR(ret), "array_cnt", finish_tablet_ls_infos.count());
      } else if (OB_FAIL(ObTabletReplicaChecksumOperator::get_tablets_replica_checksum(
          MTL_ID(), finish_tablet_ls_infos, checksum_items))) {
        LOG_WARN("failed to get tablet checksum", K(ret));
      } else if (FALSE_IT(time_guard.click(ObCompactionScheduleTimeGuard::SEARCH_CHECKSUM))) {
      } else if (OB_FAIL(batch_check_medium_checksum(checksum_items))) {
        LOG_WARN("failed to check medium tablets checksum", K(ret));
      } else if (FALSE_IT(time_guard.click(ObCompactionScheduleTimeGuard::CHECK_CHECKSUM))) {
      }
    }
    // TODO, sort tablet ls pair first
  }
  return ret;
}

// scheduler_called = false : called in compaction, DO NOT visit inner table in this func
// scheduler_called = true : in scheduler loop, could visit inner table
int ObMediumCompactionScheduleFunc::schedule_tablet_medium_merge(
    ObLS &ls,
    ObTablet &tablet,
    ObTabletSchedulePair &schedule_pair,
    bool &create_dag_flag,
    const int64_t input_major_snapshot,
    const bool scheduler_called)
{
  int ret = OB_SUCCESS;
  create_dag_flag = false;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_MEDIUM_CREATE_DAG) ret;
  if (OB_FAIL(ret)) {
    LOG_INFO("ERRSIM EN_MEDIUM_CREATE_DAG", K(ret));
    return ret;
  }
#endif
  const int64_t last_major_snapshot = tablet.get_last_major_snapshot_version(); // current compaction scn
  if (MTL(ObTenantTabletScheduler *)->could_major_merge_start() && last_major_snapshot > 0) {
    bool is_standy_tenant = !MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID();
    if (is_standy_tenant && !scheduler_called) {
      // standy tenant should not visit inner table if it is not scheduler_called, wait for scheduler loop
    } else {
      const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
      const ObLSID &ls_id = ls.get_ls_id();
      ObArenaAllocator temp_allocator("GetMediumInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()); // for load medium info
      const ObMediumCompactionInfoList *medium_list = nullptr;
      bool schedule_flag = false;
      const int64_t major_frozen_snapshot = 0 == input_major_snapshot ? MTL(ObTenantTabletScheduler *)->get_frozen_version() : input_major_snapshot; // broadcast scn
      ObMediumCompactionInfo::ObCompactionType compaction_type = ObMediumCompactionInfo::COMPACTION_TYPE_MAX;
      int64_t schedule_scn = 0; // medium_snapshot in medium info
      bool tablet_need_freeze_flag = false;

      if (OB_FAIL(tablet.read_medium_info_list(temp_allocator, medium_list))) {
        LOG_WARN("failed to load medium info list", K(ret), K(tablet));
      } else if (OB_FAIL(read_medium_info_from_list(*medium_list, last_major_snapshot, major_frozen_snapshot, compaction_type, schedule_scn))) {
        LOG_WARN("failed to read medium info from list", K(ret), K(ls_id), K(tablet_id), KPC(medium_list), K(last_major_snapshot));
      } else if (is_standy_tenant) { // for STANDBY/RESTORE TENANT
        if (OB_FAIL(decide_standy_tenant_schedule(ls_id, tablet_id, compaction_type, schedule_scn, major_frozen_snapshot, *medium_list, schedule_flag))) {
          LOG_WARN("failed to decide whehter to schedule standy schedule", K(ret), K(ls_id), K(tablet_id), K(compaction_type), K(schedule_scn), K(major_frozen_snapshot), K(medium_list));
        }
      } else {
        schedule_flag = true;
      }
      if (OB_FAIL(ret) || !schedule_flag) {
      } else if (schedule_scn > 0 && OB_FAIL(check_need_merge_and_schedule(ls, tablet, schedule_scn, tablet_need_freeze_flag, create_dag_flag))) {
        LOG_WARN("failed to check medium merge", K(ret), K(ls_id), K(tablet_id), K(schedule_scn));
      }

      if (OB_SUCC(ret) && tablet_need_freeze_flag) {
        schedule_pair.tablet_id_ = tablet_id;
        schedule_pair.schedule_merge_scn_ = schedule_scn;
      } else {
        schedule_pair.reset();
      }
    }
  }

  return ret;
}

int ObMediumCompactionScheduleFunc::decide_standy_tenant_schedule(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const ObMediumCompactionInfo::ObCompactionType &compaction_type,
      const int64_t schedule_scn,
      const int64_t major_frozen_snapshot,
      const ObMediumCompactionInfoList &medium_list,
      bool &schedule_flag)
{
    int ret = OB_SUCCESS;
    schedule_flag = false;
    if (ObMediumCompactionInfo::MAJOR_COMPACTION == compaction_type) {
      if (schedule_scn > major_frozen_snapshot) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schedule_scn of current major is bigger than broadcast scn, wait for next round loop", K(ret), K(schedule_scn), K(major_frozen_snapshot));
      } else {
        schedule_flag = true;
      }
    } else if (ObMediumCompactionInfo::MEDIUM_COMPACTION == compaction_type) {
      if (schedule_scn > major_frozen_snapshot && ObMediumCompactionInfo::MAJOR_COMPACTION == medium_list.get_last_compaction_type()) {
        ObTabletCompactionScnInfo ret_info;
        if (OB_FAIL(get_status_from_inner_table(ls_id, tablet_id, ret_info))) {
          LOG_WARN("failed to get status from inner tablet", K(ret), K(ls_id), K(tablet_id));
        } else if (ret_info.could_schedule_next_round(medium_list.get_last_compaction_scn())) {
          LOG_INFO("success to check RS major checksum validation finished", K(ret), K(ls_id), K(tablet_id));
          schedule_flag = true;
        }
      } else {
        schedule_flag = true;
      }
    } else {
      // does not read valid medium info, wait for next round scheduler loop
    }
    return ret;
}

int ObMediumCompactionScheduleFunc::read_medium_info_from_list(
  const ObMediumCompactionInfoList &medium_list,
  const int64_t last_major_snapshot,
  const int64_t major_frozen_snapshot,
  ObMediumCompactionInfo::ObCompactionType &compaction_type,
  int64_t &schedule_scn)
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH_X(info, medium_list.get_list(), OB_SUCC(ret)) {
    if (info->medium_snapshot_ <= last_major_snapshot) {
      // finished, this medium info could recycle
    } else {
      if (info->is_medium_compaction()
          || info->medium_snapshot_ <= major_frozen_snapshot) {
        schedule_scn = info->medium_snapshot_;
        compaction_type = (ObMediumCompactionInfo::ObCompactionType)info->compaction_type_;
      }
      break; // found one unfinish medium info, loop end
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::is_election_leader(const ObLSID &ls_id, bool &is_election_leader)
{
  int ret = OB_SUCCESS;
  ObRole role = INVALID_ROLE;
  int64_t unused_proposal_id = 0;
  palf::PalfHandleGuard palf_handle_guard;
  if (OB_FAIL(MTL(logservice::ObLogService*)->open_palf(ls_id, palf_handle_guard))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("failed to open palf", K(ret), K(ls_id));
    }
  } else if (OB_FAIL(palf_handle_guard.get_role(role, unused_proposal_id))) {
    LOG_WARN("failed to get palf handle role", K(ret), K(ls_id));
  } else {
    is_election_leader = is_leader_by_election(role);
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::check_need_merge_and_schedule(
    ObLS &ls,
    ObTablet &tablet,
    const int64_t schedule_scn,
    bool &tablet_need_freeze_flag,
    bool &create_dag_flag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_merge = false;
  bool can_merge = false;
  bool need_force_freeze = false;
  const ObLSID &ls_id = ls.get_ls_id();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  create_dag_flag = false;

  if (OB_UNLIKELY(0 == schedule_scn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schedule_scn));
  } else if (OB_FAIL(ObPartitionMergePolicy::check_need_medium_merge(
          ls,
          tablet,
          schedule_scn,
          need_merge,
          can_merge,
          need_force_freeze))) { // check merge finish
    LOG_WARN("failed to check medium merge", K(ret), K(ls_id), K(tablet_id));
  } else if (need_merge && can_merge) {
    if (OB_TMP_FAIL(ObTenantTabletScheduler::schedule_merge_dag(
            ls.get_ls_id(),
            tablet,
            MEDIUM_MERGE,
            schedule_scn))) {
      if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to schedule medium merge dag", K(ret), K(ls_id), K(tablet_id));
      }
    } else {
      create_dag_flag = true;
      LOG_DEBUG("success to schedule medium merge dag", K(ret), K(schedule_scn));
    }
  } else if (need_force_freeze) {
    tablet_need_freeze_flag = true;
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
    const ObTablet &tablet,
    const ObMediumCompactionInfoList &medium_list,
    int64_t &max_sync_medium_scn)
{
  int ret = OB_SUCCESS;
  max_sync_medium_scn = 0;
  int64_t max_sync_medium_scn_on_tablet = 0;
  int64_t max_sync_medium_scn_from_list = 0;
  if (OB_FAIL(tablet.get_max_sync_medium_scn(max_sync_medium_scn_on_tablet))) {
    LOG_WARN("failed to get max sync medium scn from tablet", KR(ret), K(tablet));
  } else if (OB_FAIL(medium_list.get_max_sync_medium_scn(max_sync_medium_scn_from_list))) {
    LOG_WARN("failed to get max sync medium scn from medium_list", KR(ret), K(medium_list));
  } else {
    max_sync_medium_scn = MAX(max_sync_medium_scn_on_tablet, max_sync_medium_scn_from_list);
    LOG_TRACE("get max sync medium scn", KR(ret), K(max_sync_medium_scn), K(max_sync_medium_scn_on_tablet),
      K(max_sync_medium_scn_from_list));
  }
  return ret;
}

} //namespace compaction
} // namespace oceanbase
