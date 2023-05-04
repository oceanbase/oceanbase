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
    ObLS &ls,
    ObTablet &tablet,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
    ObIAllocator &allocator,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result)
{
  UNUSED(allocator);
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
    ObLS &ls,
    ObTablet &tablet,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
    ObIAllocator &allocator,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result)
{
  UNUSED(merge_reason);
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObTenantFreezeInfoMgr::FreezeInfo freeze_info;
  int64_t schedule_snapshot = 0;
  const int64_t scheduler_frozen_version = MTL(ObTenantTabletScheduler*)->get_frozen_version();
  const ObMediumCompactionInfoList &medium_list = tablet.get_medium_compaction_info_list();
  ObITable *last_major = tablet.get_table_store().get_major_sstables().get_boundary_table(true/*last*/);
  bool schedule_with_newer_info = false;

  if (OB_ISNULL(last_major)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major sstable is unexpected null", K(ret), KPC(last_major));
  } else {
    schedule_snapshot = last_major->get_snapshot_version();
  }
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
    } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_table_schema_to_merge(
        tablet, freeze_info.schema_version, allocator, medium_info))) {
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
    } else { // success to get table schema
      if (schedule_with_newer_info) {
        LOG_INFO("schedule with newer freeze info", K(ret), K(ls_id), K(tablet_id), K(freeze_info));
      }
      break;
    }
  } // end of while
  if (OB_SUCC(ret)) {
    medium_info.compaction_type_ = ObMediumCompactionInfo::MAJOR_COMPACTION;
    medium_info.medium_merge_reason_ = ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE;
    medium_info.medium_snapshot_ = freeze_info.freeze_version;
    result.schema_version_ = freeze_info.schema_version;
    if (OB_FAIL(get_result_for_major(tablet, medium_info, result))) {
      LOG_WARN("failed get result for major", K(ret), K(medium_info));
    } else {
      LOG_TRACE("choose_major_snapshot", K(ret), "ls_id", ls.get_ls_id(),
        "tablet_id", tablet.get_tablet_meta().tablet_id_, K(medium_info), K(freeze_info), K(result));
    }
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

// cal this func with PLAF LEADER ROLE && wait_check_medium_scn_ = 0
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
  }
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
  int64_t max_sync_medium_scn = 0;
  ObITable *last_major = nullptr;
  ObTablet *tablet = nullptr;
  const bool is_major = 0 != schedule_major_snapshot;
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else {
    const ObMediumCompactionInfoList &medium_list = tablet->get_medium_compaction_info_list();
    if (OB_ISNULL(last_major = tablet->get_table_store().get_major_sstables().get_boundary_table(true/*last*/))) {
      // no major, do nothing
    } else if (!medium_list.could_schedule_next_round()) { // check serialized list
      // do nothing
    } else if (OB_FAIL(tablet->get_max_sync_medium_scn(max_sync_medium_scn))) { // check info in memory
      LOG_WARN("failed to get max sync medium scn", K(ret), K(max_sync_medium_scn));
    } else if (is_major && schedule_major_snapshot > max_sync_medium_scn) {
      schedule_medium_flag = true;
    } else if (nullptr != last_major && last_major->get_snapshot_version() < max_sync_medium_scn) {
      // do nothing
    } else if (OB_FAIL(ObAdaptiveMergePolicy::get_adaptive_merge_reason(*tablet, adaptive_merge_reason))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get meta merge priority", K(ret), KPC(this));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (adaptive_merge_reason > ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE) {
      schedule_medium_flag = true;
    }
    LOG_TRACE("schedule next medium in primary cluster", K(ret), KPC(this), K(schedule_medium_flag),
        K(schedule_major_snapshot), K(adaptive_merge_reason), KPC(last_major), K(medium_list), K(max_sync_medium_scn));
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      if (tablet->get_tablet_meta().tablet_id_.id() > ObTabletID::MIN_USER_TABLET_ID) {
        ret = OB_E(EventTable::EN_SCHEDULE_MEDIUM_COMPACTION) ret;
        LOG_INFO("errsim", K(ret), KPC(this));
        if (OB_FAIL(ret)) {
          FLOG_INFO("set schedule medium with errsim", KPC(this));
          ret = OB_SUCCESS;
          schedule_medium_flag = true;
        }
      }
    }
#endif

    if (OB_FAIL(ret) || !schedule_medium_flag) {
    } else if (ObMediumCompactionInfo::MAJOR_COMPACTION == medium_list.get_last_compaction_type()) {
      // for normal medium, checksum error happened, wait_check_medium_scn_ will never = 0
      // for major, need select inner_table to check RS status
      if (OB_FAIL(get_status_from_inner_table(ls_.get_ls_id(), tablet->get_tablet_meta().tablet_id_, ret_info))) {
        LOG_WARN("failed to get status from inner tablet", K(ret), KPC(this));
      } else if (ret_info.could_schedule_next_round(medium_list.get_last_compaction_scn())) {
        LOG_INFO("success to check RS major checksum validation finished", K(ret), KPC(this), K(ret_info));
        ret = decide_medium_snapshot(is_major);
      } else {
        ++schedule_stat.wait_rs_validate_cnt_;
      }
    } else {
      ret = decide_medium_snapshot(is_major, adaptive_merge_reason);
    }
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
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else {
    const ObTabletTableStore &table_store = tablet->get_table_store();
    if (0 == table_store.get_major_sstables().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("major sstable should not be empty", K(ret), K(tablet));
    } else if (0 == ls_.get_min_reserved_snapshot()) {
      ret = OB_NO_NEED_MERGE;
      // not sync reserved snapshot yet, should not schedule now
    } else if (FALSE_IT(max_merged_snapshot = table_store.get_major_sstables().get_boundary_table(true/*last*/)->get_snapshot_version())) {
    } else if (OB_FAIL(MTL(ObTenantFreezeInfoMgr*)->get_min_reserved_snapshot(
        tablet->get_tablet_meta().tablet_id_, max_merged_snapshot, min_reserved_snapshot))) {
      LOG_WARN("failed to get multi version from freeze info mgr", K(ret), K(table_id));
    } else {
      max_reserved_snapshot = MAX(ls_.get_min_reserved_snapshot(), min_reserved_snapshot);
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::decide_medium_snapshot(
    const bool is_major,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else {
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    int64_t max_sync_medium_scn = 0;
    uint64_t compat_version = 0;
    LOG_TRACE("decide_medium_snapshot", K(ret), KPC(this), K(tablet_id));
    if (OB_FAIL(tablet->get_max_sync_medium_scn(max_sync_medium_scn))) {
      LOG_WARN("failed to get max sync medium scn", K(ret), KPC(this));
    } else if (OB_FAIL(ls_.add_dependent_medium_tablet(tablet_id))) { // add dependent_id in ObLSReservedSnapshotMgr
      LOG_WARN("failed to add dependent tablet", K(ret), KPC(this));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
      LOG_WARN("fail to get data version", K(ret));
    } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_1_0_0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data version to schedule medium compaction", K(ret), K(compat_version));
    } else {
      int64_t max_reserved_snapshot = 0;
      ObGetMergeTablesResult result;
      ObMediumCompactionInfo medium_info;
      medium_info.data_version_ = compat_version;

      if (OB_FAIL(choose_medium_scn[is_major](ls_, *tablet, merge_reason, allocator_, medium_info, result))) {
        if (OB_NO_NEED_MERGE != ret) {
          LOG_WARN("failed to choose medium snapshot", K(ret), KPC(this));
        }
      } else if (medium_info.medium_snapshot_ <= max_sync_medium_scn) {
        ret = OB_NO_NEED_MERGE;
      } else if (is_major) {
        // do nothing
      } else if (OB_FAIL(get_max_reserved_snapshot(max_reserved_snapshot))) {
        LOG_WARN("failed to get multi_version_start", K(ret), KPC(this));
      } else if (medium_info.medium_snapshot_ < max_reserved_snapshot) {
        // chosen medium snapshot is far too old
        LOG_INFO("chosen medium snapshot is invalid for multi_version_start", K(ret), KPC(this),
            K(medium_info), K(max_reserved_snapshot));
        const share::SCN &weak_read_ts = ls_.get_ls_wrs_handler()->get_ls_weak_read_ts();
        if (medium_info.medium_snapshot_ == tablet->get_snapshot_version() //  no uncommitted sstable
            && weak_read_ts.get_val_for_tx() <= max_reserved_snapshot
            && weak_read_ts.get_val_for_tx() + DEFAULT_SCHEDULE_MEDIUM_INTERVAL < ObTimeUtility::current_time_ns()) {
          const int64_t snapshot_gc_ts = MTL(ObTenantFreezeInfoMgr*)->get_snapshot_gc_ts();
          medium_info.medium_snapshot_ = MAX(max_reserved_snapshot, MIN(weak_read_ts.get_val_for_tx(), snapshot_gc_ts));
          LOG_INFO("use weak_read_ts to schedule medium", K(ret), KPC(this),
              K(medium_info), K(max_reserved_snapshot), K(weak_read_ts), K(snapshot_gc_ts));
        } else {
          ret = OB_NO_NEED_MERGE;
        }
      }
      if (OB_SUCC(ret) && !is_major) {
        const int64_t current_time = ObTimeUtility::current_time_ns();
        if (max_reserved_snapshot < current_time) {
          const int64_t time_interval = (current_time - max_reserved_snapshot) / 2;
          ObSSTable *table = static_cast<ObSSTable *>(tablet->get_table_store().get_major_sstables().get_boundary_table(true/*last*/));
          if (OB_ISNULL(table)) {
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
          const share::SCN &weak_read_ts = ls_.get_ls_wrs_handler()->get_ls_weak_read_ts();
          const int64_t snapshot_gc_ts = MTL(ObTenantFreezeInfoMgr*)->get_snapshot_gc_ts();
          medium_info.medium_snapshot_ = MAX(MAX(max_reserved_snapshot, MIN(weak_read_ts.get_val_for_tx(), snapshot_gc_ts));
          if (medium_info.medium_snapshot_ > max_sync_medium_scn) {
            FLOG_INFO("set schedule medium with errsim", KPC(this));
            ret = OB_SUCCESS;
          }
        }
      }
    }
  #endif
      if (FAILEDx(prepare_medium_info(result, medium_info))) {
        if (OB_TABLE_IS_DELETED == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to prepare medium info", K(ret), K(result), K(tablet->get_storage_schema()));
        }
      } else if (OB_FAIL(submit_medium_clog(medium_info))) {
        LOG_WARN("failed to submit medium clog and update inner table", K(ret), KPC(this));
      } else if (OB_TMP_FAIL(ls_.tablet_freeze(tablet_id, true/*is_sync*/))) {
        // need to freeze memtable with MediumCompactionInfo
        LOG_WARN("failed to freeze tablet", K(tmp_ret), KPC(this));
      }
      // delete tablet_id in ObLSReservedSnapshotMgr even if submit clog or update inner table failed
      if (OB_TMP_FAIL(ls_.del_dependent_medium_tablet(tablet_id))) {
        LOG_ERROR("failed to delete dependent medium tablet", K(tmp_ret), KPC(this));
        ob_abort();
      }
      ret = OB_NO_NEED_MERGE == ret ? OB_SUCCESS : ret;
      if (OB_FAIL(ret)) {
          // add schedule suspect info
          ADD_SUSPECT_INFO(MEDIUM_MERGE, ls_.get_ls_id(), tablet_id,
                          "schedule medium failed",
                          "compaction_scn", medium_info.medium_snapshot_,
                          "schema_version", medium_info.storage_schema_.schema_version_,
                          "error_no", ret);
      }
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::init_parallel_range(
    const ObGetMergeTablesResult &result,
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;

  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else {
    int64_t expected_task_count = 0;
    const int64_t tablet_size = medium_info.storage_schema_.get_tablet_size();
    const ObSSTable *first_sstable = static_cast<const ObSSTable *>(result.handle_.get_table(0));
    if (OB_ISNULL(first_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable is unexpected null", K(ret), K(tablet));
    } else {
      const int64_t macro_block_cnt = first_sstable->get_meta().get_macro_info().get_data_block_ids().count();
      int64_t inc_row_cnt = 0;
      for (int64_t i = 0; i < result.handle_.get_count(); ++i) {
        inc_row_cnt += static_cast<const ObSSTable*>(result.handle_.get_table(i))->get_meta().get_row_count();
      }
      if ((0 == macro_block_cnt && inc_row_cnt > SCHEDULE_RANGE_ROW_COUNT_THRESHOLD)
          || (first_sstable->get_meta().get_row_count() >= SCHEDULE_RANGE_ROW_COUNT_THRESHOLD
              && inc_row_cnt >= first_sstable->get_meta().get_row_count() * SCHEDULE_RANGE_INC_ROW_COUNT_PERCENRAGE_THRESHOLD)) {
        if (OB_FAIL(ObParallelMergeCtx::get_concurrent_cnt(tablet_size, macro_block_cnt, expected_task_count))) {
          STORAGE_LOG(WARN, "failed to get concurrent cnt", K(ret), K(tablet_size), K(expected_task_count),
            KPC(first_sstable));
        }
      }
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
      if (OB_FAIL(prepare_iter(result, table_iter))) {
        LOG_WARN("failed to get table iter", K(ret), K(range_array));
      } else if (OB_FAIL(input_range_array.push_back(range))) {
        LOG_WARN("failed to push back range", K(ret), K(range));
      } else if (OB_FAIL(range_spliter.get_split_multi_ranges(
              input_range_array,
              expected_task_count,
              tablet->get_index_read_info(),
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
  const ObTabletTableStore &table_store = tablet.get_table_store();

  if (OB_UNLIKELY(!medium_info.is_valid() || !table_store.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(medium_info), K(table_store));
  } else if (OB_ISNULL(base_table = static_cast<ObSSTable*>(table_store.get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("major sstable not exist", K(ret), K(table_store));
  } else if (base_table->get_snapshot_version() >= medium_info.medium_snapshot_) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_FAIL(result.handle_.add_table(base_table))) {
    LOG_WARN("failed to add table into iterator", K(ret), KP(base_table));
  } else {
    const ObSSTableArray &minor_tables = table_store.get_minor_sstables();
    bool start_add_table_flag = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count_; ++i) {
      if (OB_ISNULL(minor_tables[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must not null", K(ret), K(i), K(minor_tables));
      } else if (!start_add_table_flag
        && minor_tables[i]->get_upper_trans_version() >= base_table->get_snapshot_version()) {
        start_add_table_flag = true;
      }
      if (OB_SUCC(ret) && start_add_table_flag) {
        if (OB_FAIL(result.handle_.add_table(minor_tables[i]))) {
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
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else {
    ObTableStoreIterator table_iter;
    medium_info.cluster_id_ = GCONF.cluster_id; // set cluster id

    if (medium_info.is_medium_compaction()) {
      ObStorageSchema tmp_storage_schema;
      bool use_storage_schema_on_tablet = true;
      if (medium_info.medium_snapshot_ > tablet->get_snapshot_version()) {
        ObSEArray<ObITable*, MAX_MEMSTORE_CNT> memtables;
        if (OB_FAIL(tablet->get_memtables(memtables, true/*need_active*/))) {
          LOG_WARN("failed to get memtables", K(ret), KPC(this));
        } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_latest_storage_schema_from_memtable(
            allocator_, memtables, tmp_storage_schema))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS; // clear errno
          } else {
            LOG_WARN("failed to get storage schema from memtable", K(ret));
          }
        } else {
          use_storage_schema_on_tablet = false;
        }
      }

      if (FAILEDx(medium_info.save_storage_schema(
          allocator_,
          use_storage_schema_on_tablet ? tablet->get_storage_schema() : tmp_storage_schema))) {
        LOG_WARN("failed to save storage schema", K(ret), K(use_storage_schema_on_tablet), K(tmp_storage_schema));
      }
    }
    if (FAILEDx(init_parallel_range(result, medium_info))) {
      LOG_WARN("failed to init parallel range", K(ret), K(medium_info));
    } else {
      LOG_INFO("success to prepare medium info", K(ret), K(medium_info));
    }
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
    const ObTablet &tablet,
    const int64_t schema_version,
    ObIAllocator &allocator,
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  uint64_t table_id = OB_INVALID_ID;
  ObMultiVersionSchemaService *schema_service = nullptr;
  schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  int64_t save_schema_version = schema_version;
  if (OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service from MTL", K(ret));
  } else if (OB_FAIL(get_table_id(*schema_service, tablet_id, schema_version, table_id))) {
    if (OB_TABLE_IS_DELETED != ret) {
      LOG_WARN("failed to get table id", K(ret), K(tablet_id));
    }
  } else if (OB_FAIL(schema_service->retry_get_schema_guard(tenant_id,
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
  if (FAILEDx(medium_info.storage_schema_.init(
    allocator,
    *table_schema,
    tablet.get_tablet_meta().compat_mode_))) {
    LOG_WARN("failed to init storage schema", K(ret), K(schema_version));
  } else {
    FLOG_INFO("get schema to merge", K(table_id), K(schema_version), K(save_schema_version),
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
  if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet_handle", K(ret), K(tablet_handle_));
  } else if (FALSE_IT(tablet = tablet_handle_.get_obj())) {
  } else {
    const ObLSID &ls_id = ls_.get_ls_id();
    const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
    const int64_t wait_check_medium_scn = tablet->get_medium_compaction_info_list().get_wait_check_medium_scn();
    bool merge_finish = false;

    if (0 == wait_check_medium_scn) {
      // do nothing
    } else if (OB_FAIL(check_medium_meta_table(wait_check_medium_scn, ls_id, tablet_id, ls_locality, merge_finish))) {
      LOG_WARN("failed to check inner table", K(ret), KPC(this));
    } else if (!merge_finish) {
      // do nothing
    } else if (OB_FAIL(check_medium_checksum_table(wait_check_medium_scn, ls_id, tablet_id))) { // check checksum
      LOG_WARN("failed to check checksum", K(ret), K(wait_check_medium_scn), KPC(this));
    } else {
      const ObMediumCompactionInfo::ObCompactionType compaction_type = tablet->get_medium_compaction_info_list().get_last_compaction_type();
      FLOG_INFO("check medium compaction info", K(ret), K(ls_id), K(tablet_id), K(compaction_type));

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
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_MEDIUM_CREATE_DAG) ret;
  if (OB_FAIL(ret)) {
    LOG_INFO("set create medium dag failed with errsim", K(ret));
    return ret;
  }
#endif

  if (MTL(ObTenantTabletScheduler *)->could_major_merge_start()) {
    const ObMediumCompactionInfoList &medium_list = tablet.get_medium_compaction_info_list();
    const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
    const ObLSID &ls_id = ls.get_ls_id();
    ObITable *last_major = tablet.get_table_store().get_major_sstables().get_boundary_table(true/*last*/);

    bool schedule_flag = false;
    if (OB_ISNULL(last_major)) {
      // do nothing
    } else if (ObMediumCompactionInfo::MAJOR_COMPACTION == medium_list.get_last_compaction_type()
      && !MTL_IS_PRIMARY_TENANT()) { // for STANDBY/RESTORE TENANT
      ObTabletCompactionScnInfo ret_info;
      // for standby/restore tenant, need select inner_table to check RS status before schedule new round
      if (!scheduler_called) { // should not visit inner table, wait for scheduler loop
      } else if (OB_FAIL(get_status_from_inner_table(ls_id, tablet_id, ret_info))) {
        LOG_WARN("failed to get status from inner tablet", K(ret), K(ls_id), K(tablet_id));
      } else if (ret_info.could_schedule_next_round(medium_list.get_last_compaction_scn())) {
        LOG_INFO("success to check RS major checksum validation finished", K(ret), K(ls_id), K(tablet_id));
        schedule_flag = true;
      }
    } else {
      schedule_flag = true;
    }

    if (OB_SUCC(ret) && schedule_flag) {
      int64_t major_frozen_snapshot = 0 == input_major_snapshot ? MTL(ObTenantTabletScheduler *)->get_frozen_version() : input_major_snapshot;
      ObMediumCompactionInfo::ObCompactionType compaction_type = ObMediumCompactionInfo::COMPACTION_TYPE_MAX;
      int64_t schedule_scn = 0;
      bool need_merge = false;

      (void)tablet.get_medium_compaction_info_list().get_schedule_scn(major_frozen_snapshot, schedule_scn, compaction_type);

      LOG_DEBUG("schedule_tablet_medium_merge", K(schedule_scn), K(major_frozen_snapshot), K(scheduler_called), K(ls_id), K(tablet_id));
      if (0 == schedule_scn
        && 0 == tablet.get_medium_compaction_info_list().size() // serialized medium list is empty
        && scheduler_called
        && OB_FAIL(get_schedule_medium_from_memtable(tablet, major_frozen_snapshot, schedule_scn, compaction_type))) {
        LOG_WARN("failed to get schedule medium scn from memtables", K(ret));
      } else if (schedule_scn > 0) {
        if (OB_FAIL(check_need_merge_and_schedule(ls, tablet, schedule_scn, compaction_type))) {
          LOG_WARN("failed to check medium merge", K(ret), K(ls_id), K(tablet_id), K(schedule_scn));
        }
      }
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

int ObMediumCompactionScheduleFunc::get_schedule_medium_from_memtable(
  ObTablet &tablet,
  const int64_t major_frozen_snapshot,
  int64_t &schedule_medium_scn,
  ObMediumCompactionInfo::ObCompactionType &compaction_type)
{
  int ret = OB_SUCCESS;
  schedule_medium_scn = 0;
  compaction_type = ObMediumCompactionInfo::COMPACTION_TYPE_MAX;

  ObITable *last_major = tablet.get_table_store().get_major_sstables().get_boundary_table(true/*last*/);
  if (OB_NOT_NULL(last_major)) {
    ObArenaAllocator tmp_allocator;
    ObMediumCompactionInfoList tmp_medium_list;
    const int64_t last_major_snapshot = last_major->get_snapshot_version();
    ObSEArray<ObITable*, MAX_MEMSTORE_CNT> memtables;
    if (OB_FAIL(tablet.get_memtables(memtables, true/*need_active*/))) {
      LOG_WARN("failed to get memtables", K(ret), "tablet_id", tablet.get_tablet_meta().tablet_id_);
    } else if (memtables.empty()) {
      // do nothing
    } else if (OB_FAIL(get_medium_info_list_from_memtable(tmp_allocator, memtables, tmp_medium_list))) {
      LOG_WARN("failed to get medium info list from memtable", K(ret));
    } else if (!tmp_medium_list.is_empty()) {
      const ObMediumCompactionInfo *info_in_list = nullptr;
      DLIST_FOREACH_X(info, tmp_medium_list.get_list(), OB_SUCC(ret)) {
        if (OB_UNLIKELY(memtable::MultiSourceDataUnitType::MEDIUM_COMPACTION_INFO != info->type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("return info is invalid", K(ret), KPC(info));
        } else if (FALSE_IT(info_in_list = static_cast<const ObMediumCompactionInfo *>(info))) {
        } else if (info_in_list->medium_snapshot_ <= last_major_snapshot) {
          // finished, this medium info could recycle
        } else {
          if (info_in_list->is_medium_compaction() || info_in_list->medium_snapshot_ <= major_frozen_snapshot) {
            schedule_medium_scn = info_in_list->medium_snapshot_;
            compaction_type = (ObMediumCompactionInfo::ObCompactionType)info_in_list->compaction_type_;
          }
          break; // found one unfinish medium info, loop end
        }
      }
    }
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
    const ObMediumCompactionInfo *medium_info = nullptr;
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

int ObMediumCompactionScheduleFunc::get_latest_storage_schema_from_memtable(
  ObIAllocator &allocator,
  const ObIArray<ObITable *> &memtables,
  ObStorageSchema &storage_schema)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  memtable::ObMemtable * memtable = nullptr;
  bool found = false;
  for (int64_t i = memtables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_ISNULL(table = memtables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table in tables_handle is invalid", K(ret), KPC(table));
    } else if (OB_ISNULL(memtable = dynamic_cast<memtable::ObMemtable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table pointer does not point to a ObMemtable object", KPC(table));
    } else if (OB_FAIL(memtable->get_multi_source_data_unit(&storage_schema, &allocator))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // clear OB_ENTRY_NOT_EXIST
      } else {
        LOG_WARN("failed to get storage schema from memtable", K(ret), KPC(table));
      }
    } else {
      found = true;
      break;
    }
  } // end for
  if (OB_SUCC(ret) && !found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

// make sure only data tablet could schedule this func
int ObMediumCompactionScheduleFunc::get_medium_info_list_from_memtable(
    ObIAllocator &allocator,
    const ObIArray<ObITable *> &memtables,
    ObMediumCompactionInfoList &medium_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(medium_list.init(allocator))) {
    LOG_WARN("failed to init merge list", K(ret));
  }
  ObITable *table = nullptr;
  memtable::ObMemtable *memtable = nullptr;
  compaction::ObMediumCompactionInfo useless_medium_info;
  memtable::ObMultiSourceData::ObIMultiSourceDataUnitList dst_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < memtables.count(); ++i) {
    dst_list.reset();
    if (OB_ISNULL(table = memtables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table in tables_handle is invalid", K(ret), KPC(table));
    } else if (OB_ISNULL(memtable = dynamic_cast<memtable::ObMemtable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table pointer does not point to a ObMemtable object", KPC(table));
    } else if (OB_FAIL(memtable->get_multi_source_data_unit_list(&useless_medium_info, dst_list, &allocator))) {
      LOG_WARN("failed to get medium info from memtable", K(ret), KPC(table));
    } else {
      ObMediumCompactionInfo *info_in_list = nullptr;
      DLIST_FOREACH_X(info, dst_list, OB_SUCC(ret)) {
        if (OB_UNLIKELY(memtable::MultiSourceDataUnitType::MEDIUM_COMPACTION_INFO != info->type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("return info is invalid", K(ret), KPC(info));
        } else if (FALSE_IT(info_in_list = static_cast<ObMediumCompactionInfo *>(info))) {
        } else if (OB_FAIL(medium_list.add_medium_compaction_info(*info_in_list))) {
          LOG_WARN("failed to add medium compaction info", K(ret), KPC(info_in_list));
        }
      }
    }
  } // end of for
  return ret;
}

} //namespace compaction
} // namespace oceanbase
