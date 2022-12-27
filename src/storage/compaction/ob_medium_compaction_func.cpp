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

ObMediumCompactionScheduleFunc::PrepareTableIter ObMediumCompactionScheduleFunc::prepare_table_iter[MEDIUM_FUNC_CNT]
  = { ObMediumCompactionScheduleFunc::prepare_iter_for_medium,
      ObMediumCompactionScheduleFunc::prepare_iter_for_major,
    };

int ObMediumCompactionScheduleFunc::choose_medium_snapshot(
    ObLS &ls,
    ObTablet &tablet,
    const int64_t schedule_medium_snapshot,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result)
{
  UNUSED(schedule_medium_snapshot);
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
    const int64_t schedule_medium_snapshot,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason &merge_reason,
    ObMediumCompactionInfo &medium_info,
    ObGetMergeTablesResult &result)
{
  UNUSED(merge_reason);
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr::FreezeInfo freeze_info;
  if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(get_freeze_info_by_snapshot_version, schedule_medium_snapshot, freeze_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get freeze info", K(ret), K(schedule_medium_snapshot), "ls_id", ls.get_ls_id(),
          "tablet_id", tablet.get_tablet_meta().tablet_id_);
    } else {
      ret = OB_NO_NEED_MERGE;
    }
  } else {
    medium_info.compaction_type_ = ObMediumCompactionInfo::MAJOR_COMPACTION;
    medium_info.medium_merge_reason_ = ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE;
    medium_info.medium_snapshot_ = schedule_medium_snapshot;
    result.schema_version_ = freeze_info.schema_version;
    LOG_TRACE("choose_major_snapshot", K(ret), "ls_id", ls.get_ls_id(),
        "tablet_id", tablet.get_tablet_meta().tablet_id_, K(medium_info), K(freeze_info));
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::get_status_from_inner_table(
    ObTabletCompactionScnInfo &ret_info)
{
  int ret = OB_SUCCESS;
  ret_info.reset();

  ObTabletCompactionScnInfo snapshot_info(
      MTL_ID(),
      ls_.get_ls_id(),
      tablet_.get_tablet_meta().tablet_id_,
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
      const int64_t major_snapshot)
{
  int ret = OB_SUCCESS;
  ObRole role = INVALID_ROLE;
  if (OB_FAIL(ls_.get_ls_role(role))) {
    LOG_WARN("failed to get ls role", K(ret), KPC(this));
  } else if (LEADER == role) {
    // only log_handler_leader can schedule
    ret = schedule_next_medium_primary_cluster(major_snapshot);
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::schedule_next_medium_primary_cluster(
      const int64_t schedule_major_snapshot)
{
  int ret = OB_SUCCESS;
  ObAdaptiveMergePolicy::AdaptiveMergeReason adaptive_merge_reason = ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE;
  ObTabletCompactionScnInfo ret_info;
  // check last medium type, select inner table for last major
  bool schedule_medium_flag = false;
  int64_t max_sync_medium_scn = 0;
  ObITable *last_major = tablet_.get_table_store().get_major_sstables().get_boundary_table(true/*last*/);
  const ObMediumCompactionInfoList &medium_list = tablet_.get_medium_compaction_info_list();
  if (OB_ISNULL(last_major)) {
    // no major, do nothing
  } else if (!medium_list.could_schedule_next_round()) { // check serialized list
    // do nothing
  } else if (OB_FAIL(tablet_.get_max_sync_medium_scn(max_sync_medium_scn))) { // check info in memory
    LOG_WARN("failed to get max sync medium scn", K(ret), K(max_sync_medium_scn));
  } else if (0 != schedule_major_snapshot && schedule_major_snapshot > max_sync_medium_scn) {
    schedule_medium_flag = true;
  } else if (nullptr != last_major && last_major->get_snapshot_version() < max_sync_medium_scn) {
    // do nothing
  } else if (OB_FAIL(ObAdaptiveMergePolicy::get_adaptive_merge_reason(tablet_, adaptive_merge_reason))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get meta merge priority", K(ret), KPC(this));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (adaptive_merge_reason > ObAdaptiveMergePolicy::AdaptiveMergeReason::NONE) {
    schedule_medium_flag = true;
  }
  LOG_DEBUG("schedule next medium in primary cluster", K(ret), KPC(this), K(schedule_medium_flag),
      K(schedule_major_snapshot), K(adaptive_merge_reason), KPC(last_major), K(medium_list), K(max_sync_medium_scn));

  if (OB_FAIL(ret) || !schedule_medium_flag) {
  } else if (ObMediumCompactionInfo::MAJOR_COMPACTION == medium_list.get_last_compaction_type()) {
    // for normal medium, checksum error happened, wait_check_medium_scn_ will never = 0
    // for major, need select inner_table to check RS status
    if (OB_FAIL(get_status_from_inner_table(ret_info))) {
      LOG_WARN("failed to get status from inner tablet", K(ret), KPC(this));
    } else if (ret_info.could_schedule_next_round(last_major->get_snapshot_version())) {
      ret = decide_medium_snapshot(schedule_major_snapshot);
    }
  } else {
    ret = decide_medium_snapshot(schedule_major_snapshot, adaptive_merge_reason);
  }

  return ret;
}

int ObMediumCompactionScheduleFunc::decide_medium_snapshot(
    const int64_t schedule_medium_snapshot,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet_.get_tablet_meta().tablet_id_;
  int64_t max_sync_medium_scn = 0;
  LOG_DEBUG("decide_medium_snapshot", K(ret), KPC(this), K(schedule_medium_snapshot));
  if (OB_FAIL(tablet_.get_max_sync_medium_scn(max_sync_medium_scn))) {
    LOG_WARN("failed to get max sync medium scn", K(ret), KPC(this));
  } else if (OB_FAIL(ls_.add_dependent_medium_tablet(tablet_id))) { // add dependent_id in ObLSReservedSnapshotMgr
    LOG_WARN("failed to add dependent tablet", K(ret), KPC(this));
  } else {
    const bool is_major = (0 != schedule_medium_snapshot);
    int64_t multi_version_start = 0;
    ObGetMergeTablesResult result;
    ObMediumCompactionInfo medium_info;
    uint64_t compat_version = 0;
    if (OB_FAIL(choose_medium_scn[is_major](ls_, tablet_, schedule_medium_snapshot, merge_reason, medium_info, result))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("failed to choose medium snapshot", K(ret), KPC(this));
      }
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
      LOG_WARN("fail to get data version", K(ret));
    } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_1_0_0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data version to schedule medium compaction", K(ret), K(compat_version));
    } else if (FALSE_IT(medium_info.data_version_ = compat_version)) {
    } else if (is_major) {
      // do nothing
    } else if (medium_info.medium_snapshot_ <= max_sync_medium_scn) {
      ret = OB_NO_NEED_MERGE;
    } else if (OB_FAIL(ObTablet::get_kept_multi_version_start(ls_, tablet_, multi_version_start))) {
      LOG_WARN("failed to get multi_version_start", K(ret), KPC(this));
    } else if (medium_info.medium_snapshot_ < multi_version_start) {
      // chosen medium snapshot is far too old
      LOG_INFO("chosen medium snapshot is invalid for multi_version_start", K(ret), KPC(this),
          K(medium_info), K(multi_version_start));
      const share::SCN &weak_read_ts = ls_.get_ls_wrs_handler()->get_ls_weak_read_ts();
      if (medium_info.medium_snapshot_ == tablet_.get_snapshot_version() //  no uncommitted sstable
          && weak_read_ts.get_val_for_tx() <= multi_version_start
          && weak_read_ts.get_val_for_tx() + DEFAULT_SCHEDULE_MEDIUM_INTERVAL < ObTimeUtility::current_time_ns()) {
        medium_info.medium_snapshot_ = weak_read_ts.get_val_for_tx();
        LOG_INFO("use weak_read_ts to schedule medium", K(ret), KPC(this),
            K(medium_info), K(multi_version_start), K(weak_read_ts));
      } else {
        ret = OB_NO_NEED_MERGE;
      }
    }
    if (OB_SUCC(ret) && !is_major) {
      const int64_t current_time = ObTimeUtility::current_time_ns();
      if (multi_version_start < current_time) {
        const int64_t time_interval = (current_time - multi_version_start) / 2;
        ObSSTable *table = static_cast<ObSSTable *>(tablet_.get_table_store().get_major_sstables().get_boundary_table(true/*last*/));
        if (OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table is unexpected null", K(ret), KP(table));
        } else if (table->get_snapshot_version() + time_interval > medium_info.medium_snapshot_) {
          ret = OB_NO_NEED_MERGE;
          LOG_DEBUG("schedule medium frequently", K(ret), KPC(table), K(medium_info), K(time_interval));
        }
      }
    }
    if (FAILEDx(prepare_medium_info(result, medium_info))) {
      if (OB_TABLE_IS_DELETED == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to prepare medium info", K(ret), K(result), K(tablet_.get_storage_schema()));
      }
    } else if (OB_FAIL(submit_medium_clog(medium_info))) {
      LOG_WARN("failed to submit medium clog and update inner table", K(ret), KPC(this));
    } else if (OB_TMP_FAIL(MTL(ObTenantFreezer *)->tablet_freeze(tablet_id, false/*force_freeze*/))) {
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
  return ret;
}

int ObMediumCompactionScheduleFunc::init_parallel_range(
    const ObGetMergeTablesResult &result,
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;

  int64_t expected_task_count = 0;
  const int64_t tablet_size = medium_info.storage_schema_.get_tablet_size();
  const ObSSTable *first_sstable =
      static_cast<const ObSSTable*>(tablet_.get_table_store().get_major_sstables().get_boundary_table(true/*last*/));
  if (OB_ISNULL(first_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable is unexpected null", K(ret), K(tablet_));
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
    if (OB_FAIL(prepare_table_iter[is_major](tablet_, result, medium_info, table_iter))) {
      LOG_WARN("failed to get table iter", K(ret), K(range_array));
    } else if (OB_FAIL(input_range_array.push_back(range))) {
      LOG_WARN("failed to push back range", K(ret), K(range));
    } else if (OB_FAIL(range_spliter.get_split_multi_ranges(
            input_range_array,
            expected_task_count,
            tablet_.get_index_read_info(),
            table_iter,
            allocator_,
            range_array))) {
      LOG_WARN("failed to get split multi range", K(ret), K(range_array));
    } else if (OB_FAIL(medium_info.gene_parallel_info(allocator_, range_array))) {
      LOG_WARN("failed to get parallel ranges", K(ret), K(range_array));
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::prepare_iter_for_major(
    ObTablet &tablet,
    const ObGetMergeTablesResult &result,
    ObMediumCompactionInfo &medium_info,
    ObTableStoreIterator &table_iter)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  table_iter.reset();
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
  } else if (OB_FAIL(table_iter.add_table(base_table))) {
    LOG_WARN("failed to add table into iterator", K(ret), KP(base_table));
  } else {
    const ObSSTableArray &minor_tables = table_store.get_minor_sstables();
    int64_t start_idx = 0;
    bool start_add_table_flag = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count_; ++i) {
      if (OB_ISNULL(minor_tables[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must not null", K(ret), K(i), K(minor_tables));
      } else if (minor_tables[i]->get_upper_trans_version() >= base_table->get_snapshot_version()) {
        start_idx = i;
        break;
      }
    }
    if (FAILEDx(table_iter.add_tables(minor_tables.array_ + start_idx, minor_tables.count_ - start_idx))) {
      LOG_WARN("failed to add tables", K(ret), K(start_idx));
    }
  }
  return ret;
}
int ObMediumCompactionScheduleFunc::prepare_iter_for_medium(
    ObTablet &tablet,
    const ObGetMergeTablesResult &result,
    ObMediumCompactionInfo &medium_info,
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
  ObTableStoreIterator table_iter;
  medium_info.cluster_id_ = GCONF.cluster_id;
  if (medium_info.is_major_compaction()) {
    // get table schema
    if (OB_UNLIKELY(result.schema_version_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("schema version is invalid", K(ret), K(result), KPC(this), K(medium_info));
    } else if (OB_FAIL(get_table_schema_to_merge(result.schema_version_, medium_info))) {
      if (OB_TABLE_IS_DELETED != ret) {
        LOG_WARN("failed to get table schema", K(ret), KPC(this), K(medium_info));
      }
    }
  } else if (OB_FAIL(medium_info.save_storage_schema(allocator_, tablet_.get_storage_schema()))) {
    LOG_WARN("failed to save storage schema", K(ret), K(tablet_.get_storage_schema()));
  }

  if (FAILEDx(init_parallel_range(result, medium_info))) {
    LOG_WARN("failed to init parallel range", K(ret), K(medium_info));
  } else {
    LOG_INFO("success to init parallel range", K(ret), K(medium_info));
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
    const int64_t schema_version,
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const ObTabletID &tablet_id = tablet_.get_tablet_meta().tablet_id_;
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
    if (OB_TABLE_IS_DELETED != ret) {
      LOG_WARN("Fail to get schema", K(ret), K(tenant_id), K(schema_version), K(table_id));
    } else {
      LOG_WARN("table is deleted", K(ret), K(table_id));
    }
  } else if (save_schema_version < schema_version) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("can not use older schema version", K(ret), K(schema_version), K(save_schema_version), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("Fail to get table schema", K(ret), K(table_id));
  } else if (NULL == table_schema) {
    if (OB_FAIL(schema_service->get_tenant_full_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("Fail to get schema", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("Fail to get table schema", K(ret), K(table_id));
    } else if (NULL == table_schema) {
      ret = OB_TABLE_IS_DELETED;
      LOG_WARN("table is deleted", K(ret), K(table_id));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t max_schema_version = 0;
    if (OB_FAIL(tablet_.get_max_sync_storage_schema_version(max_schema_version))) {
      LOG_WARN("failed to get max sync storage schema version", K(ret), KPC(this));
    } else if (max_schema_version < table_schema->get_schema_version()) {
      // need sync schema clog
      if (OB_FAIL(tablet_.try_update_storage_schema(
          table_id,
          table_schema->get_schema_version(),
          allocator_,
          DEFAULT_SYNC_SCHEMA_CLOG_TIMEOUT))) {
        LOG_WARN("failed to sync schema clog", K(ret), KPC(this), KPC(table_schema));
      }
    }
    if (FAILEDx(medium_info.storage_schema_.init(
        allocator_,
        *table_schema,
        tablet_.get_tablet_meta().compat_mode_))) {
      LOG_WARN("failed to init storage schema", K(ret), K(schema_version));
    } else {
      FLOG_INFO("get schema to merge", K(table_id), K(schema_version), K(save_schema_version),
                K(*reinterpret_cast<const ObPrintableTableSchema*>(table_schema)));
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::submit_medium_clog(
    ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = E(EventTable::EN_MEDIUM_COMPACTION_SUBMIT_CLOG_FAILED) ret;
  if (OB_FAIL(ret)) {
    LOG_INFO("set update medium clog failed with errsim", KPC(this));
    return ret;
  }
#endif
  if (OB_FAIL(tablet_.submit_medium_compaction_clog(medium_info, allocator_))) {
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
      } else if (replica.get_snapshot_version() >= check_medium_snapshot) {
        // replica may have check_medium_snapshot = 2, but have received medium info of 3,
        // when this replica is elected as leader, this will happened
      } else {
        unfinish_cnt++;
      }
    } // end of for
    FLOG_INFO("check_medium_compaction_finish", K(ret), K(ls_id), K(tablet_id), K(check_medium_snapshot),
        K(unfinish_cnt), "total_cnt", replica_array.count());

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
        LOG_ERROR("checksum verify failed", K(ret), K(checksum_items.at(0)), K(i), K(checksum_items.at(i)));
      }
    }
#ifdef ERRSIM
    ret = E(EventTable::EN_MEDIUM_REPLICA_CHECKSUM_ERROR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_MEDIUM_REPLICA_CHECKSUM_ERROR", K(ret), K(ls_id), K(tablet_id));
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
      // TODO(@lixia.yq) delete status when data_checksum_error is a inner_table
      if (OB_TMP_FAIL(ObTabletMetaTableCompactionOperator::set_info_status(
          medium_snapshot_info, unused_ret_info))) {
        LOG_WARN("failed to set info status", K(tmp_ret), K(medium_snapshot_info));
      }
    }
  }
  return ret;
}

// for Leader, clean wait_check_medium_scn
int ObMediumCompactionScheduleFunc::check_medium_finish()
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_.get_ls_id();
  const ObTabletID &tablet_id = tablet_.get_tablet_meta().tablet_id_;
  const int64_t wait_check_medium_scn = tablet_.get_medium_compaction_info_list().get_wait_check_medium_scn();
  bool merge_finish = false;

  if (0 == wait_check_medium_scn) {
    // do nothing
  } else if (OB_FAIL(check_medium_meta_table(wait_check_medium_scn, ls_id, tablet_id, merge_finish))) {
    LOG_WARN("failed to check inner table", K(ret), KPC(this));
  } else if (!merge_finish) {
    // do nothing
  } else if (OB_FAIL(check_medium_checksum_table(wait_check_medium_scn, ls_id, tablet_id))) { // check checksum
    LOG_WARN("failed to check checksum", K(ret), K(wait_check_medium_scn), KPC(this));
  } else {
    const ObMediumCompactionInfo::ObCompactionType compaction_type = tablet_.get_medium_compaction_info_list().get_last_compaction_type();
    FLOG_INFO("check medium compaction info", K(ret), K(ls_id), K(tablet_id), K(compaction_type));

    // clear wait_check_medium_scn on Tablet
    ObTabletHandle unused_handle;
    if (OB_FAIL(ls_.update_medium_compaction_info(tablet_id, unused_handle))) {
      LOG_WARN("failed to update medium compaction info", K(ret), K(ls_id), K(tablet_id));
    }
  }

  return ret;
}

// DO NOT visit inner table in this func
int ObMediumCompactionScheduleFunc::schedule_tablet_medium_merge(
    ObLS &ls,
    ObTablet &tablet,
    const int64_t input_major_snapshot)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = E(EventTable::EN_MEDIUM_CREATE_DAG) ret;
  if (OB_FAIL(ret)) {
    LOG_INFO("set create medium dag failed with errsim", K(ret));
    return ret;
  }
#endif
  const ObMediumCompactionInfoList &medium_list = tablet.get_medium_compaction_info_list();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObLSID &ls_id = ls.get_ls_id();
  int64_t major_frozen_snapshot = 0 == input_major_snapshot ? MTL(ObTenantTabletScheduler *)->get_frozen_version() : input_major_snapshot;

  const int64_t schedule_scn = tablet.get_medium_compaction_info_list().get_schedule_scn(major_frozen_snapshot);
  bool need_merge = false;
  LOG_DEBUG("schedule_tablet_medium_merge", K(schedule_scn), K(ls_id), K(tablet_id));
  if (schedule_scn > 0) {
    if (OB_FAIL(check_need_merge_and_schedule(ls, tablet, schedule_scn, need_merge))) {
      LOG_WARN("failed to check medium merge", K(ret), K(ls_id), K(tablet_id), K(schedule_scn));
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

int ObMediumCompactionScheduleFunc::freeze_memtable_to_get_medium_info()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable*, MAX_MEMSTORE_CNT> memtables;
  if (OB_FAIL(tablet_.get_table_store().get_memtables(memtables, true/*need_active*/))) {
    LOG_WARN("failed to get memtables", K(ret), KPC(this));
  } else if (memtables.empty()) {
    // do nothing
  } else {
    memtable::ObMemtable *memtable = nullptr;
    bool receive_medium_info = false;
    bool has_medium_info = false;
    for (int i = 0; OB_SUCC(ret) && i < memtables.count(); ++i) {
      if (OB_ISNULL(memtable = static_cast<memtable::ObMemtable*>(memtables.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("memtable is null", K(ret), K(i), KPC(memtables.at(i)), K(memtable));
      } else if (memtable->has_multi_source_data_unit(memtable::MultiSourceDataUnitType::MEDIUM_COMPACTION_INFO)) {
        has_medium_info = true;
        if (memtable->is_active_memtable()) {
          receive_medium_info = true;
          break;
        }
      }
    } // end of for
    if (OB_FAIL(ret)) {
    } else if (receive_medium_info) {
      if (OB_FAIL(MTL(ObTenantFreezer *)->tablet_freeze(tablet_.get_tablet_meta().tablet_id_, false/*force_freeze*/))) {
        if (OB_TABLE_NOT_EXIST != ret) {
          LOG_WARN("failed to freeze tablet", K(ret), KPC(this));
        }
      }
    } else if (has_medium_info) {
      LOG_INFO("received medium info, the memtable is frozen, no need to freeze tablet again", K(ret), K(tablet_));
    }
  }
  return ret;
}

int ObMediumCompactionScheduleFunc::check_need_merge_and_schedule(
    ObLS &ls,
    ObTablet &tablet,
    const int64_t schedule_scn,
    bool &need_merge)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  need_merge = false;
  bool can_merge = false;

  if (OB_FAIL(ObPartitionMergePolicy::check_need_medium_merge(
          ls,
          tablet,
          schedule_scn,
          need_merge,
          can_merge))) { // check merge finish
    LOG_WARN("failed to check medium merge", K(ret), K(ls_id), "tablet_id", tablet.get_tablet_meta().tablet_id_);
  } else if (need_merge && can_merge) {
    const ObMediumCompactionInfo *medium_info = nullptr;
    if (OB_FAIL(tablet.get_medium_compaction_info_list().get_specified_scn_info(schedule_scn, medium_info))) {
      LOG_WARN("failed to get specified scn info", K(ret), K(schedule_scn));
    } else if (OB_TMP_FAIL(ObTenantTabletScheduler::schedule_merge_dag(
            ls.get_ls_id(),
            tablet.get_tablet_meta().tablet_id_,
            MEDIUM_MERGE,
            schedule_scn,
            medium_info->is_major_compaction()))) {
      if (OB_SIZE_OVERFLOW != tmp_ret && OB_EAGAIN != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to schedule medium merge dag", K(ret), K(ls_id),
            "tablet_id", tablet.get_tablet_meta().tablet_id_);
      }
    } else {
      LOG_DEBUG("success to schedule medium merge dag", K(ret), K(schedule_scn));
    }
  }
  return ret;
}

} //namespace compaction
} // namespace oceanbase
