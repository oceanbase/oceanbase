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
#include "storage/tablet/ob_tablet_medium_info_reader.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
using namespace common;

namespace compaction
{

ObMediumCompactionScheduleFunc::ChooseMediumScn ObMediumCompactionScheduleFunc::choose_medium_scn[MEDIUM_FUNC_CNT]
  = { ObMediumCompactionScheduleFunc::choose_medium_snapshot,
      ObMediumCompactionScheduleFunc::choose_major_snapshot,
    };

int64_t ObMediumCompactionScheduleFunc::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV("ls_id", ls_.get_ls_id());
    J_COMMA();
    if (tablet_handle_.is_valid() && OB_NOT_NULL(tablet_handle_.get_obj())) {
      J_KV("tablet_id", tablet_handle_.get_obj()->get_tablet_meta().tablet_id_);
    } else {
      J_KV("tablet_handle", tablet_handle_);
    }
    J_OBJ_END();
  }
  return pos;
}

int ObMediumCompactionScheduleFunc::choose_medium_snapshot(
    const ObMediumCompactionScheduleFunc &func,
    ObLS &ls,
    ObTablet &tablet,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
    ObArenaAllocator &allocator,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result,
    int64_t &schema_version)
{
  UNUSEDx(func, allocator, schema_version);
  int ret = OB_SUCCESS;
  ObGetMergeTablesParam param;
  param.merge_type_ = META_MAJOR_MERGE;
  if (OB_FAIL(ObAdaptiveMergePolicy::get_meta_merge_tables(
          param,
          ls,
          tablet,
          result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get meta merge tables", K(ret), K(param));
    }
  } else {
    medium_info.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
    medium_info.medium_merge_reason_ = merge_reason;
    medium_info.medium_snapshot_ = result.version_range_.snapshot_version_;
    LOG_TRACE("choose_medium_snapshot", K(ret), "ls_id", ls.get_ls_id(),
        "tablet_id", tablet.get_tablet_meta().tablet_id_, K(result), K(medium_info));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::choose_major_snapshot(
    const ObMediumCompactionScheduleFunc &func,
    ObLS &ls,
    ObTablet &tablet,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
    ObArenaAllocator &allocator,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result,
    int64_t &schema_version)
{
  UNUSED(merge_reason);
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObTenantFreezeInfoMgr::FreezeInfo freeze_info;
  int64_t schedule_snapshot = 0;
  const int64_t scheduler_frozen_version = MTL(ObTenantTabletScheduler*)->get_frozen_version();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTable *last_major = nullptr;
  int64_t last_sstable_schema_version = 0;
  bool schedule_with_newer_info = false;
  ObMultiVersionSchemaService *schema_service = nullptr;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("load medium info list fail", K(ret), K(tablet));
  } else {
    last_major = static_cast<ObSSTable *>(table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/));
    if (OB_ISNULL(last_major)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("major sstable is unexpected null", K(ret), KPC(last_major));
    } else if (OB_FAIL(last_major->get_frozen_schema_version(last_sstable_schema_version))) {
      LOG_WARN("failed to get frozen schema version", KR(ret), KPC(last_major));
    } else {
      schedule_snapshot = last_major->get_snapshot_version();
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service from MTL", K(ret));
  }

  bool schedule_medium_merge = false;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(get_freeze_info_behind_snapshot_version, schedule_snapshot, freeze_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get freeze info", K(ret), K(schedule_snapshot), K(ls_id), K(tablet_id));
      } else {
        ret = OB_NO_NEED_MERGE;
      }
    } else if (OB_UNLIKELY(freeze_info.schema_version <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema version is invalid", K(ret), K(ls_id), K(tablet_id), K(freeze_info));
    } else if (OB_UNLIKELY(freeze_info.schema_version < last_sstable_schema_version)) {
      schedule_medium_merge = true;
      FLOG_INFO("schema version in freeze info is too small, try to schedule medium compaction instead", K(ret),
                K(ls_id), K(tablet_id), K(last_sstable_schema_version), K(freeze_info));
      break;
    } else if (OB_FAIL(get_table_schema_to_merge(
        *schema_service, tablet, freeze_info.schema_version, allocator, medium_info))) {
      if (OB_TABLE_IS_DELETED == ret) {
        // do nothing, end loop
      } else if (OB_ERR_SCHEMA_HISTORY_EMPTY == ret) {
        if (freeze_info.freeze_version <= scheduler_frozen_version) {
          FLOG_INFO("table schema may recycled, use newer freeze info instead", K(ret), KPC(last_major), K(freeze_info),
            K(scheduler_frozen_version));
          schedule_snapshot = freeze_info.freeze_version;
          schedule_with_newer_info = true;
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("failed to get table schema", K(ret), K(ls_id), K(tablet_id), K(medium_info));
      }
    }
    if (OB_SUCC(ret)) { // success to get table schema
      if (schedule_with_newer_info) {
        FLOG_INFO("schedule with newer freeze info", K(ret), K(ls_id), K(tablet_id), K(freeze_info));
      }
      break;
    }
  } // end of while

  if (OB_FAIL(ret)) {
  } else if (schedule_medium_merge) {
    if (OB_FAIL(switch_to_choose_medium_snapshot(func, allocator, ls, tablet, freeze_info.freeze_version, medium_info, schema_version))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("failed to switch to choose medium snapshot", K(ret), K(tablet));
      }
    }
  } else {
    medium_info.compaction_type_ = ObMediumCompactionInfo::MAJOR_COMPACTION;
    medium_info.medium_merge_reason_ = ObAdaptiveMergePolicy::AdaptiveMergeReason::TENANT_MAJOR;
    medium_info.medium_snapshot_ = freeze_info.freeze_version;
    schema_version = freeze_info.schema_version;
  }

  if (FAILEDx(get_result_for_major(tablet, medium_info, result))) {
    LOG_WARN("failed get result for major", K(ret), K(medium_info));
  } else {
    LOG_TRACE("choose_major_snapshot", K(ret), "ls_id", ls.get_ls_id(),
      "tablet_id", tablet.get_tablet_meta().tablet_id_, K(medium_info), K(freeze_info), K(result),
      K(last_sstable_schema_version));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::switch_to_choose_medium_snapshot(
    const ObMediumCompactionScheduleFunc &func,
    ObArenaAllocator &allocator,
    ObLS &ls,
    ObTablet &tablet,
    const int64_t freeze_version,
    ObMediumCompactionInfo &medium_info,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  int64_t medium_snapshot = 0;

  if (func.weak_read_ts_ < freeze_version + 1) {
    ret = OB_EAGAIN;
    LOG_WARN("weak read ts is smaller than new medium snapshot, try later", K(ret), K(tablet));
  } else if (FALSE_IT(medium_snapshot = MAX(func.weak_read_ts_, freeze_version + 1))) {
  } else if (OB_FAIL(choose_medium_schema_version(allocator, medium_snapshot, tablet, schema_version))) {
    LOG_WARN("fail to choose medium schema version", K(ret), K(tablet));
  } else {
    medium_info.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
    medium_info.medium_merge_reason_ = ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE;
    medium_info.medium_snapshot_ = medium_snapshot;
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
      ObTenantTabletScheduler::ObScheduleStatistics &schedule_stat)
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
        return ret;
      } else {
        ret = OB_SUCCESS;
      }
    }
#endif
    ret = schedule_next_medium_primary_cluster(major_snapshot, schedule_stat);
  } else {
    LOG_TRACE("not leader", K(ret), K(role), K(ls_.get_ls_id()));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::get_adaptive_reason(
  const int64_t schedule_major_snapshot,
  ObAdaptiveMergePolicy::AdaptiveMergeReason &adaptive_merge_reason)
{
  int ret = OB_SUCCESS;
  int64_t max_sync_medium_scn = 0;
  ObTablet *tablet = tablet_handle_.get_obj();
  if (OB_FAIL(ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
      *tablet, *medium_info_list_, max_sync_medium_scn))) {
    LOG_WARN("failed to get max received medium scn", KR(ret), KPC(this));
  } else if (schedule_major_snapshot > max_sync_medium_scn) {
    adaptive_merge_reason = ObAdaptiveMergePolicy::AdaptiveMergeReason::TENANT_MAJOR;
  } else if (OB_FAIL(ObAdaptiveMergePolicy::get_adaptive_merge_reason(*tablet, adaptive_merge_reason))) {
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
          adaptive_merge_reason = ObAdaptiveMergePolicy::AdaptiveMergeReason::LOAD_DATA_SCENE;
        }
      }
    }
#endif
  return ret;
}

int ObMediumCompactionScheduleFunc::schedule_next_medium_primary_cluster(
      const int64_t schedule_major_snapshot,
      ObTenantTabletScheduler::ObScheduleStatistics &schedule_stat)
{
  int ret = OB_SUCCESS;
  ObAdaptiveMergePolicy::AdaptiveMergeReason adaptive_merge_reason = ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE;
  ObTabletCompactionScnInfo ret_info;
  // check last medium type, select inner table for last major
  bool schedule_medium_flag = false;
  ObITable *last_major = nullptr;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_ISNULL(last_major =
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/))) {
    // no major, do nothing
  } else if (nullptr == medium_info_list_
      && OB_FAIL(tablet->read_medium_info_list(allocator_, medium_info_list_))) {
    LOG_WARN("failed to read medium info list", K(ret), KPC(tablet));
  } else if (OB_ISNULL(medium_info_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium info list is unexpected null", K(ret), KPC(this), KPC_(medium_info_list));
  } else if (!medium_info_list_->could_schedule_next_round(last_major->get_snapshot_version())) {
    // check medium list int mds_table
  } else if (OB_FAIL(get_adaptive_reason(schedule_major_snapshot, adaptive_merge_reason))) {
    LOG_WARN("failed to get adaptive reason", KR(ret), K(schedule_major_snapshot));
  } else if (adaptive_merge_reason > ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE) {
    schedule_medium_flag = true;
  }
  LOG_TRACE("schedule next medium in primary cluster", K(ret), KPC(this), K(schedule_medium_flag),
      K(schedule_major_snapshot), K(adaptive_merge_reason), KPC(last_major), KPC_(medium_info_list));

  if (OB_FAIL(ret) || !schedule_medium_flag) {
  } else if (ObMediumCompactionInfo::MAJOR_COMPACTION == medium_info_list_->get_last_compaction_type()) {
    // for normal medium, checksum error happened, wait_check_medium_scn_ will never = 0
    // for major, need select inner_table to check RS status
    if (OB_FAIL(get_status_from_inner_table(ls_.get_ls_id(), tablet->get_tablet_meta().tablet_id_, ret_info))) {
      LOG_WARN("failed to get status from inner tablet", K(ret), KPC(this));
    } else if (ret_info.could_schedule_next_round(medium_info_list_->get_last_compaction_scn())) {
      LOG_INFO("success to check RS major checksum validation finished", K(ret), KPC(this), K(ret_info));
      ret = decide_medium_snapshot(adaptive_merge_reason);
    } else {
      ++schedule_stat.wait_rs_validate_cnt_;
      LOG_TRACE("cannot schedule next round merge now", K(ret), K(ret_info), KPC_(medium_info_list), KPC(tablet), K(adaptive_merge_reason));
    }
  } else {
    ret = decide_medium_snapshot(adaptive_merge_reason);
  }

  return ret;
}

int ObMediumCompactionScheduleFunc::get_max_reserved_snapshot(int64_t &max_reserved_snapshot)
{
  int ret = OB_SUCCESS;
  max_reserved_snapshot = INT64_MAX;

  int64_t max_merged_snapshot = 0;
  int64_t min_reserved_snapshot = INT64_MAX;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else if (OB_FAIL(tablet->fetch_table_store(wrapper))) {
    LOG_WARN("fetch table store fail", K(ret), KPC(tablet));
  } else if (0 == wrapper.get_member()->get_major_sstables().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major sstable should not be empty", K(ret), KPC(tablet));
  } else if (0 == ls_.get_min_reserved_snapshot()) {
    ret = OB_NO_NEED_MERGE;
    // not sync reserved snapshot yet, should not schedule now
  } else if (FALSE_IT(max_merged_snapshot = wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)->get_snapshot_version())) {
  } else if (OB_FAIL(MTL(ObTenantFreezeInfoMgr*)->get_min_reserved_snapshot(
      tablet->get_tablet_meta().tablet_id_, max_merged_snapshot, min_reserved_snapshot))) {
    LOG_WARN("failed to get multi version from freeze info mgr", K(ret), K(tablet->get_tablet_meta().tablet_id_));
  } else {
    max_reserved_snapshot = MAX(ls_.get_min_reserved_snapshot(), min_reserved_snapshot);
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::choose_new_medium_snapshot(
  const int64_t max_reserved_snapshot,
  ObMediumCompactionInfo &medium_info,
  ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle_.get_obj();
  int64_t snapshot_gc_ts = 0;
  if (medium_info.medium_snapshot_ == tablet->get_snapshot_version() //  no uncommitted sstable
      && weak_read_ts_ + DEFAULT_SCHEDULE_MEDIUM_INTERVAL < ObTimeUtility::current_time_ns()) {
    snapshot_gc_ts = MTL(ObTenantFreezeInfoMgr *)->get_snapshot_gc_ts();
    // data before weak_read_ts & latest storage schema on memtable is match for schedule medium
    medium_info.medium_snapshot_ = MIN(weak_read_ts_, snapshot_gc_ts);
  }
  if (medium_info.medium_snapshot_ < max_reserved_snapshot) {
    ret = OB_NO_NEED_MERGE;
  } else {
    LOG_INFO("use weak_read_ts to schedule medium", K(ret), KPC(this),
             K(medium_info), K(max_reserved_snapshot), K_(weak_read_ts), K(snapshot_gc_ts));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::choose_medium_schema_version(
    common::ObArenaAllocator &allocator,
    const int64_t medium_snapshot,
    ObTablet &tablet,
    int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_snapshot_version = tablet.get_snapshot_version();
  const ObStorageSchema *schema_on_tablet = nullptr;
  int64_t store_column_cnt_in_schema = 0;
  ObSEArray<storage::ObITable *, MAX_MEMSTORE_CNT> memtables;

  if (OB_FAIL(tablet.load_storage_schema(allocator, schema_on_tablet))) {
    LOG_WARN("fail to load storage schema", K(ret));
  } else if (FALSE_IT(schema_version = schema_on_tablet->schema_version_)) {
  } else if (medium_snapshot <= tablet_snapshot_version) {
    // do nothing, use schema version on tablet
  } else if (OB_FAIL(schema_on_tablet->get_store_column_count(store_column_cnt_in_schema, true/*full_col*/))) {
    LOG_WARN("failed to get store column count", K(ret), K(store_column_cnt_in_schema));
  } else if (OB_FAIL(tablet.get_memtables(memtables, true/*need_active*/))) {
    LOG_WARN("failed to get memtables", KR(ret), K(tablet));
  }

  int64_t max_schema_version_on_memtable = 0;
  int64_t max_column_cnt_on_memtable = 0; // placeholder
  for (int64_t idx = 0; OB_SUCC(ret) && idx < memtables.count(); ++idx) {
    memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(memtables.at(idx));
    if (memtable->get_snapshot_version() <= tablet_snapshot_version) {
      // continue
    } else if (OB_FAIL(memtable->get_schema_info(
      store_column_cnt_in_schema, max_schema_version_on_memtable, max_column_cnt_on_memtable))) {
      LOG_WARN("failed to get schema info from memtable", KR(ret), KPC(memtable));
    } else {
      schema_version = MAX(schema_version, max_schema_version_on_memtable);
      break;
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::decide_medium_snapshot(
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  const bool is_major = (merge_reason == ObAdaptiveMergePolicy::TENANT_MAJOR);
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else {
    tablet = tablet_handle_.get_obj();
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    int64_t max_sync_medium_scn = 0;
    ObMediumCompactionInfo medium_info;
    LOG_TRACE("decide_medium_snapshot", K(ret), KPC(this), K(tablet_id), K(merge_reason));
    if (OB_FAIL(ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
      *tablet, *medium_info_list_, max_sync_medium_scn))) {
      LOG_WARN("failed to get max sync medium scn", KR(ret), KPC(this));
    } else if (OB_FAIL(ls_.add_dependent_medium_tablet(tablet_id))) { // add dependent_id in ObLSReservedSnapshotMgr
      LOG_WARN("failed to add dependent tablet", K(ret), KPC(this));
    } else if (OB_FAIL(medium_info.init_data_version())) {
      LOG_WARN("fail to set data version", K(ret));
    } else {
      int64_t max_reserved_snapshot = 0;
      ObGetMergeTablesResult result;
      int64_t schema_version = 0;
      if (OB_FAIL(choose_medium_scn[is_major](*this, ls_, *tablet, merge_reason, allocator_, medium_info, result, schema_version))) {
        if (OB_NO_NEED_MERGE != ret) {
          LOG_WARN("failed to choose medium snapshot", K(ret), KPC(this));
        }
      } else if (medium_info.medium_snapshot_ <= max_sync_medium_scn) {
        // for standby -> primary cluster, new chosen medium_snapshot should larger than max_sync_medium_scn
        ret = OB_NO_NEED_MERGE;
      } else if (is_major) {
        // do nothing
      } else if (OB_FAIL(get_max_reserved_snapshot(max_reserved_snapshot))) {
        LOG_WARN("failed to get multi_version_start", K(ret), KPC(this));
      } else if (medium_info.medium_snapshot_ < max_reserved_snapshot
          && OB_FAIL(choose_new_medium_snapshot(max_reserved_snapshot, medium_info, result))) {
        // chosen medium snapshot is far too old
        LOG_WARN("failed to choose new medium snapshot", KR(ret), K(max_reserved_snapshot), K(medium_info));
      } else if (OB_FAIL(choose_medium_schema_version(allocator_, medium_info.medium_snapshot_, *tablet, schema_version))) {
        LOG_WARN("failed to choose medium schema version", K(ret), K(tablet));
      }

      if (OB_SUCC(ret) && !is_major) {
        const int64_t current_time = ObTimeUtility::current_time_ns();
        if (max_reserved_snapshot < current_time) {
          const int64_t time_interval = (current_time - max_reserved_snapshot) / 2;
          ObSSTable *table = nullptr;
          ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
          if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
            LOG_WARN("fail to fetch table store", K(ret));
          } else if (OB_ISNULL(table = static_cast<ObSSTable *>(table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true /*last*/)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table is unexpected null", K(ret), KP(table));
          } else if (table->get_snapshot_version() + time_interval > medium_info.medium_snapshot_) {
            ret = OB_NO_NEED_MERGE;
            LOG_DEBUG("schedule medium frequently", K(ret), KPC(table), K(medium_info), K(time_interval));
          }
        }
      }
  #ifdef ERRSIM
    if (OB_SUCC(ret) || OB_NO_NEED_MERGE == ret) {
      if (tablet->get_tablet_meta().tablet_id_.id() > ObTabletID::MIN_USER_TABLET_ID) {
        ret = OB_E(EventTable::EN_SCHEDULE_MEDIUM_COMPACTION) ret;
        LOG_INFO("errsim", K(ret), KPC(this));
        if (OB_FAIL(ret)) {
          const int64_t snapshot_gc_ts = MTL(ObTenantFreezeInfoMgr*)->get_snapshot_gc_ts();
          medium_info.medium_snapshot_ = MIN(weak_read_ts_, snapshot_gc_ts);
          if (medium_info.medium_snapshot_ > max_sync_medium_scn
            && medium_info.medium_snapshot_ >= max_reserved_snapshot) {
            FLOG_INFO("set schedule medium with errsim", KPC(this));
            ret = OB_SUCCESS;
          }
        }
      }
    }
  #endif
      if (FAILEDx(prepare_medium_info(result, schema_version, medium_info))) {
        if (OB_TABLE_IS_DELETED == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to prepare medium info", K(ret), K(result), K(schema_version));
        }
      } else if (OB_FAIL(submit_medium_clog(medium_info))) {
        LOG_WARN("failed to submit medium clog and update inner table", K(ret), KPC(this));
      }
      // delete tablet_id in ObLSReservedSnapshotMgr even if submit clog or update inner table failed
      if (OB_TMP_FAIL(ls_.del_dependent_medium_tablet(tablet_id))) {
        LOG_ERROR("failed to delete dependent medium tablet", K(tmp_ret), KPC(this));
        ob_abort();
      }
      ret = OB_NO_NEED_MERGE == ret ? OB_SUCCESS : ret;
      if (OB_FAIL(ret)) {
        // add schedule suspect info
        if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MEDIUM_MERGE, ls_.get_ls_id(), tablet_id,
                        ObSuspectInfoType::SUSPECT_SCHEDULE_MEDIUM_FAILED,
                        medium_info.medium_snapshot_,
                        medium_info.storage_schema_.store_column_cnt_,
                        static_cast<int64_t>(ret)))) {
          LOG_WARN("failed to add suspect info", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::init_schema_changed(
  const ObSSTableMeta &sstable_meta,
  ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  int64_t full_stored_col_cnt = 0;
  const ObSSTableBasicMeta &basic_meta = sstable_meta.get_basic_meta();
  const ObStorageSchema &schema = medium_info.storage_schema_;
  if (OB_UNLIKELY(!schema.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is not inited", KR(ret), K(schema));
  } else if (OB_FAIL(schema.get_stored_column_count_in_sstable(full_stored_col_cnt))) {
    LOG_WARN("failed to get stored column count in sstable", K(ret), K(schema));
  } else if (OB_UNLIKELY(sstable_meta.get_column_count() > full_stored_col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stored col cnt in curr schema is less than old major sstable", K(ret),
      "col_cnt_in_sstable", sstable_meta.get_column_count(),
      "col_cnt_in_schema", full_stored_col_cnt, KPC(this));
  } else if (sstable_meta.get_column_count() != full_stored_col_cnt
    || basic_meta.compressor_type_ != schema.get_compressor_type()
    || (ObRowStoreType::DUMMY_ROW_STORE != basic_meta.latest_row_store_type_
      && basic_meta.latest_row_store_type_ != schema.row_store_type_)) {
    medium_info.is_schema_changed_ = true;
    LOG_INFO("schema changed", K(sstable_meta), K(schema));
  } else {
    medium_info.is_schema_changed_ = false;
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::init_parallel_range_and_schema_changed(
    const ObGetMergeTablesResult &result,
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
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
  } else if (OB_FAIL(first_sstable->get_meta(meta_handle))) {
    LOG_WARN("first sstable get meta fail", K(ret), KPC(first_sstable));
  } else {
    const int64_t macro_block_cnt = first_sstable->get_data_macro_block_count();
    int64_t inc_row_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
      ObSSTableMetaHandle inc_handle;
      if (OB_FAIL(static_cast<const ObSSTable*>(result.handle_.get_table(i))->get_meta(inc_handle))) {
        LOG_WARN("sstable get meta fail", K(ret));
      } else {
        inc_row_cnt += inc_handle.get_sstable_meta().get_row_count();
      }
    }

    if (OB_FAIL(ret)) {
    } else if ((0 == macro_block_cnt && inc_row_cnt > SCHEDULE_RANGE_ROW_COUNT_THRESHOLD)
        || (meta_handle.get_sstable_meta().get_row_count() >= SCHEDULE_RANGE_ROW_COUNT_THRESHOLD
            && inc_row_cnt >= meta_handle.get_sstable_meta().get_row_count() * SCHEDULE_RANGE_INC_ROW_COUNT_PERCENRAGE_THRESHOLD)) {
      if (OB_FAIL(ObParallelMergeCtx::get_concurrent_cnt(tablet_size, macro_block_cnt, expected_task_count))) {
        STORAGE_LOG(WARN, "failed to get concurrent cnt", K(ret), K(tablet_size), K(expected_task_count),
          KPC(first_sstable), K(meta_handle));
      }
    }
    // init is_schema_changed
    if (FAILEDx(init_schema_changed(meta_handle.get_sstable_meta(), medium_info))) {
      STORAGE_LOG(WARN, "failed to init schema changed", KR(ret), "sstable_meta", meta_handle.get_sstable_meta());
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
      } else if (OB_FAIL(range_spliter.get_split_multi_ranges(
              input_range_array,
              expected_task_count,
              tablet->get_rowkey_read_info(),
              table_iter,
              allocator_,
              range_array))) {
        LOG_WARN("failed to get split multi range", K(ret), K(range_array));
      } else if (OB_FAIL(medium_info.gene_parallel_info(allocator_, range_array))) {
        LOG_WARN("failed to get parallel ranges", K(ret), K(range_array));
      }
    }
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
  const ObStorageSchema *storage_schema = nullptr;
  ObTablet *tablet = nullptr;
  ObTableStoreIterator table_iter;
  medium_info.cluster_id_ = GCONF.cluster_id;
  medium_info.tenant_id_ = MTL_ID();
  // get table schema
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else if (0 == schema_version) { // not formal schema version
    ret = OB_NO_NEED_MERGE;
  } else if (medium_info.is_medium_compaction()) {
    const uint64_t tenant_id = MTL_ID();
    ObMultiVersionSchemaService *schema_service = nullptr;
    if (OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get schema service from MTL", K(ret));
    } else if (FALSE_IT(medium_info.storage_schema_.reset())) {
    } else if (OB_FAIL(get_table_schema_to_merge(*schema_service, *tablet, schema_version, allocator_, medium_info))) {
      // for major compaction, storage schema is inited in choose_major_snapshot
      if (OB_TABLE_IS_DELETED != ret) {
        LOG_WARN("failed to get table schema", KR(ret), KPC(this), K(medium_info));
      }
    }
  }
  if (FAILEDx(init_parallel_range_and_schema_changed(result, medium_info))) {
    LOG_WARN("failed to init parallel range", K(ret), K(medium_info));
  } else if (OB_UNLIKELY(result.handle_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table handle in result is empty", KR(ret), K(result));
  } else {
    medium_info.last_medium_snapshot_ = result.handle_.get_table(0)->get_snapshot_version();
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("success to prepare medium info", K(ret), K(medium_info));
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
    ObIAllocator &allocator,
    ObMediumCompactionInfo &medium_info)
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
  } else if (save_schema_version < schema_version) {
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
        LOG_INFO("set get table schema failed with errsim", K(ret), K(table_id), K(tablet_id));
        errno_tablet_id = tablet_id;
        return ret;
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
#endif
  // for old version medium info, need generate old version schema
  if (FAILEDx(medium_info.storage_schema_.init(
          allocator, *table_schema, tablet.get_tablet_meta().compat_mode_, false/*skip_column_info*/,
          ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2 == medium_info.medium_compat_version_
              ? ObStorageSchema::STORAGE_SCHEMA_VERSION_V2
              : ObStorageSchema::STORAGE_SCHEMA_VERSION))) {
    LOG_WARN("failed to init storage schema", K(ret), K(schema_version));
  } else {
    LOG_TRACE("get schema to merge", K(table_id), K(schema_version), K(save_schema_version),
      K(*reinterpret_cast<const ObPrintableTableSchema*>(table_schema)));
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
    LOG_INFO("set update medium clog failed with errsim", KPC(this));
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

int ObMediumCompactionScheduleFunc::check_medium_meta_table(
    const int64_t check_medium_snapshot,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObLSLocality &ls_locality,
    bool &merge_finish)
{
  int ret = OB_SUCCESS;
  merge_finish = false;
  ObTabletInfo tablet_info;

  if (OB_UNLIKELY(check_medium_snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(check_medium_snapshot), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(init_tablet_filters())) {
    LOG_WARN("failed to init tablet filters", K(ret));
  } else if (OB_FAIL(ObTabletTableOperator::get_tablet_info(GCTX.sql_proxy_, MTL_ID(), tablet_id, ls_id, tablet_info))) {
    LOG_WARN("failed to get tablet info", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(!tablet_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tabled_id is invalid", K(ret), K(ls_id), K(tablet_id));
  } else {
    const ObArray<ObTabletReplica> &replica_array = tablet_info.get_replicas();
    int64_t unfinish_cnt = 0;
    int64_t filter_cnt = 0;
    bool pass = true;
    for (int i = 0; OB_SUCC(ret) && i < replica_array.count(); ++i) {
      const ObTabletReplica &replica = replica_array.at(i);
      if (OB_UNLIKELY(!replica.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica info is invalid", K(ret), K(ls_id), K(tablet_id), K(replica));
      } else if (OB_FAIL(filters_.check(replica, pass))) {
        LOG_WARN("filter replica failed", K(ret), K(replica), K(filters_));
      } else if (!pass) {
        // do nothing
        filter_cnt++;
      } else if (replica.get_snapshot_version() >= check_medium_snapshot) {
        // replica may have check_medium_snapshot = 2, but have received medium info of 3,
        // when this replica is elected as leader, this will happened
      } else if (ls_locality.is_valid()) {
        if (ls_locality.check_exist(replica.get_server())) {
          unfinish_cnt++;
        } else {
          filter_cnt++;
          LOG_TRACE("filter by ls locality", K(ret), K(replica));
        }
      } else {
        unfinish_cnt++;
      }
    } // end of for
    LOG_INFO("check_medium_compaction_finish", K(ret), K(ls_id), K(tablet_id), K(check_medium_snapshot),
        K(unfinish_cnt), K(filter_cnt), "total_cnt", replica_array.count());

    if (0 == unfinish_cnt) { // merge finish
      merge_finish = true;
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::init_tablet_filters()
{
  int ret = OB_SUCCESS;
  if (!filters_inited_) {
    if (OB_FAIL(filters_.set_filter_not_exist_server(ObAllServerTracer::get_instance()))) {
      LOG_WARN("fail to set not exist server filter", KR(ret));
    } else if (OB_FAIL(filters_.set_filter_permanent_offline(ObAllServerTracer::get_instance()))) {
      LOG_WARN("fail to set filter", KR(ret));
    } else {
      filters_inited_ = true;
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::check_medium_checksum_table(
    const int64_t check_medium_snapshot,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletReplicaChecksumItem, 3> checksum_items;
  if (OB_FAIL(ObTabletReplicaChecksumOperator::get_specified_tablet_checksum(
      MTL_ID(), ls_id.id(), tablet_id.id(), check_medium_snapshot, checksum_items))) {
    LOG_WARN("failed to get tablet checksum", K(ret), K(ls_id), K(tablet_id), K(check_medium_snapshot));
  } else {
    for (int i = 1; OB_SUCC(ret) && i < checksum_items.count(); ++i) {
      if (OB_FAIL(checksum_items.at(0).verify_checksum(checksum_items.at(i)))) {
        if (ret == OB_CHECKSUM_ERROR) {
          LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "checksum verify failed", K(ret), K(checksum_items.at(0)), K(i), K(checksum_items.at(i)));
        } else {
          LOG_ERROR("checksum verify failed", K(ret), K(checksum_items.at(0)), K(i), K(checksum_items.at(i)));

        }
      }
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_MEDIUM_REPLICA_CHECKSUM_ERROR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(INFO, "ERRSIM EN_MEDIUM_REPLICA_CHECKSUM_ERROR", K(ret), K(ls_id), K(tablet_id));
      }
    }
#endif
    if (OB_CHECKSUM_ERROR == ret) {
      int tmp_ret = OB_SUCCESS;
      ObTabletCompactionScnInfo medium_snapshot_info(
          MTL_ID(),
          ls_id,
          tablet_id,
          ObTabletReplica::SCN_STATUS_ERROR);
      ObTabletCompactionScnInfo unused_ret_info;
      int64_t affected_rows = 0;
      // TODO(@lixia.yq) delete status when data_checksum_error is a inner_table
      if (OB_TMP_FAIL(ObTabletMetaTableCompactionOperator::set_info_status(
          medium_snapshot_info, unused_ret_info, affected_rows))) {
        LOG_WARN("failed to set info status", K(tmp_ret), K(medium_snapshot_info));
      } else {
        MTL(ObTenantTabletScheduler*)->update_error_tablet_cnt(affected_rows);
      }
    }
  }
  return ret;
}

// for Leader, clean wait_check_medium_scn
// may change tablet_ to the new tablet
int ObMediumCompactionScheduleFunc::check_medium_finish(const ObLSLocality &ls_locality)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  const ObLSID &ls_id = ls_.get_ls_id();
  bool merge_finish = false;

  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else if (nullptr == medium_info_list_
      && OB_FAIL(tablet->read_medium_info_list(allocator_, medium_info_list_))) {
    LOG_WARN("failed to read medium info list", K(ret), KPC(tablet));
  } else if (OB_ISNULL(medium_info_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium info list is unexpected null", K(ret), KPC(this), KPC_(medium_info_list));
  } else {
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    const int64_t wait_check_medium_scn = medium_info_list_->get_wait_check_medium_scn();
    if (0 == wait_check_medium_scn) {
      // do nothing
    } else if (OB_FAIL(check_medium_meta_table(wait_check_medium_scn, ls_id, tablet_id, ls_locality, merge_finish))) {
      LOG_WARN("failed to check inner table", K(ret), KPC(this));
    } else if (!merge_finish) {
      // do nothing
    } else if (OB_FAIL(check_medium_checksum_table(wait_check_medium_scn, ls_id, tablet_id))) { // check checksum
      LOG_WARN("failed to check checksum", K(ret), K(wait_check_medium_scn), KPC(this));
    } else {
      const ObMediumCompactionInfo::ObCompactionType compaction_type = medium_info_list_->get_last_compaction_type();
      LOG_INFO("check medium compaction info", K(ret), K(ls_id), K(tablet_id), K(compaction_type), K(wait_check_medium_scn));

      // clear wait_check_medium_scn on Tablet
      ObTabletHandle new_handle;
      if (OB_FAIL(ls_.update_medium_compaction_info(tablet_id, new_handle))) {
        LOG_WARN("failed to update medium compaction info", K(ret), K(ls_id), K(tablet_id));
      } else if (!new_handle.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(new_handle));
      } else {
        tablet_handle_ = new_handle;
      }
    }
  }
  return ret;
}

// scheduler_called = false : called in compaction, DO NOT visit inner table in this func
// scheduler_called = true : in scheduler loop, could visit inner table
int ObMediumCompactionScheduleFunc::schedule_tablet_medium_merge(
    ObLS &ls,
    ObTablet &tablet,
    const int64_t input_major_snapshot,
    const bool scheduler_called)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_MEDIUM_CREATE_DAG) ret;
  if (OB_FAIL(ret)) {
    LOG_INFO("set create medium dag failed with errsim", K(ret));
    return ret;
  }
#endif
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
      LOG_WARN("failed to fetch table store", K(ret), K(tablet));
    } else {
      ObITable *last_major = table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/);
      if (MTL(ObTenantTabletScheduler *)->could_major_merge_start() && OB_NOT_NULL(last_major)) {
        const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
        const ObLSID &ls_id = ls.get_ls_id();
        ObArenaAllocator temp_allocator("GetMediumInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()); // for load medium info
        const ObMediumCompactionInfoList *medium_list = nullptr;
        bool schedule_flag = false;
        const int64_t inner_table_merged_version = MTL(ObTenantTabletScheduler *)->get_inner_table_merged_scn();

        if (OB_FAIL(tablet.read_medium_info_list(temp_allocator, medium_list))) {
          LOG_WARN("failed to load medium info list", K(ret), K(tablet));
        } else if (ObMediumCompactionInfo::MAJOR_COMPACTION == medium_list->get_last_compaction_type()
            && inner_table_merged_version < medium_list->get_last_compaction_scn()
            && !MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) { // for STANDBY/RESTORE TENANT
          ObTabletCompactionScnInfo ret_info;
          // for standby/restore tenant, need select inner_table to check RS status before schedule new round
          if (!scheduler_called) { // should not visit inner table, wait for scheduler loop
          } else if (OB_FAIL(get_status_from_inner_table(ls_id, tablet_id, ret_info))) {
            LOG_WARN("failed to get status from inner tablet", K(ret), K(ls_id), K(tablet_id));
          } else if (ret_info.could_schedule_next_round(medium_list->get_last_compaction_scn())) {
            LOG_INFO("success to check RS major checksum validation finished", K(ret), K(ls_id), K(tablet_id));
            schedule_flag = true;
          }
        } else {
          schedule_flag = true;
        }
        if (OB_FAIL(ret) || !schedule_flag) {
        } else {
          const int64_t major_frozen_snapshot = 0 == input_major_snapshot ? MTL(ObTenantTabletScheduler *)->get_frozen_version() : input_major_snapshot;
          ObMediumCompactionInfo::ObCompactionType compaction_type = ObMediumCompactionInfo::COMPACTION_TYPE_MAX;
          int64_t schedule_scn = 0;
          if (OB_FAIL(read_medium_info_from_list(*medium_list, last_major->get_snapshot_version(),
              major_frozen_snapshot, compaction_type, schedule_scn))) {
          } else if (schedule_scn > 0 && OB_FAIL(check_need_merge_and_schedule(ls, tablet, schedule_scn, compaction_type))) {
            LOG_WARN("failed to check medium merge", K(ret), K(ls_id), K(tablet_id), K(schedule_scn));
          }
        }
      }
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

int ObMediumCompactionScheduleFunc::get_palf_role(const ObLSID &ls_id, ObRole &role)
{
  int ret = OB_SUCCESS;
  role = INVALID_ROLE;
  int64_t unused_proposal_id = 0;
  palf::PalfHandleGuard palf_handle_guard;
  if (OB_FAIL(MTL(logservice::ObLogService*)->open_palf(ls_id, palf_handle_guard))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("failed to open palf", K(ret), K(ls_id));
    }
  } else if (OB_FAIL(palf_handle_guard.get_role(role, unused_proposal_id))) {
    LOG_WARN("failed to get palf handle role", K(ret), K(ls_id));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::check_need_merge_and_schedule(
    ObLS &ls,
    ObTablet &tablet,
    const int64_t schedule_scn,
    const ObMediumCompactionInfo::ObCompactionType compaction_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_merge = false;
  bool can_merge = false;
  bool need_force_freeze = false;
  const ObLSID &ls_id = ls.get_ls_id();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;

  if (OB_UNLIKELY(0 == schedule_scn || !ObMediumCompactionInfo::is_valid_compaction_type(compaction_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schedule_scn), K(compaction_type));
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
            tablet_id,
            MEDIUM_MERGE,
            schedule_scn,
            ObMediumCompactionInfo::is_major_compaction(compaction_type)))) {
      if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to schedule medium merge dag", K(ret), K(ls_id), K(tablet_id));
      }
    } else {
      LOG_DEBUG("success to schedule medium merge dag", K(ret), K(schedule_scn));
    }
  } else if (need_force_freeze) {
    if (OB_TMP_FAIL(MTL(ObTenantFreezer *)->tablet_freeze(tablet_id, true/*force_freeze*/, true/*is_sync*/))) {
      LOG_WARN("failed to force freeze tablet", K(tmp_ret), K(ls_id), K(tablet_id));
    }
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
