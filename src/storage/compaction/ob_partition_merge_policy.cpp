/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#define PRINT_TS_WRAPPER(x) (ObPrintTableStore(*(x.get_member())))

#include "ob_partition_merge_policy.h"
#include "share/ob_debug_sync_point.h"
#include "share/ob_debug_sync.h"
#include "share/ob_force_print_log.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_schema.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/ob_storage_schema.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "share/scn.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/access/ob_table_estimator.h"
#include "storage/access/ob_index_sstable_estimator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;

namespace compaction
{
ERRSIM_POINT_DEF(EN_COMPACTION_DISABLE_ROW_COL_SWITCH);

// keep order with ObMergeType
ObPartitionMergePolicy::GetMergeTables ObPartitionMergePolicy::get_merge_tables[MERGE_TYPE_MAX]
  = { ObPartitionMergePolicy::get_minor_merge_tables,
      ObPartitionMergePolicy::get_hist_minor_merge_tables,
      ObAdaptiveMergePolicy::get_meta_merge_tables,
      ObPartitionMergePolicy::get_mini_merge_tables,
      ObPartitionMergePolicy::get_medium_merge_tables,
      ObPartitionMergePolicy::get_medium_merge_tables,
      ObPartitionMergePolicy::not_support_merge_type,
      ObPartitionMergePolicy::not_support_merge_type,
      ObPartitionMergePolicy::not_support_merge_type,
      ObPartitionMergePolicy::get_mds_merge_tables
    };


int ObPartitionMergePolicy::get_neighbour_freeze_info(
    const int64_t snapshot_version,
    const int64_t last_major_snapshot_version,
    ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info,
    const bool is_multi_version_merge)
{
  STATIC_ASSERT(static_cast<int64_t>(MERGE_TYPE_MAX) == ARRAYSIZEOF(get_merge_tables), "get merge table func cnt is mismatch");
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_neighbour_major_freeze(snapshot_version, freeze_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("Failed to get freeze info, use snapshot_gc_ts instead", K(ret), K(snapshot_version));
      ret = OB_SUCCESS;
      freeze_info.reset();
      freeze_info.next.frozen_scn_.set_max();
      if (last_major_snapshot_version > 0) {
        if (OB_FAIL(freeze_info.prev.frozen_scn_.convert_for_tx(last_major_snapshot_version))) {
          LOG_WARN("fail to convert for tx", K(ret), K(last_major_snapshot_version));
        }
      } else { // no major
        freeze_info.prev.frozen_scn_.set_min();
      }
    } else {
      LOG_WARN("Failed to get neighbour major freeze info", K(ret), K(snapshot_version), K(last_major_snapshot_version));
    }
  }
  if (OB_SUCC(ret) && freeze_info.next.frozen_scn_.is_max() && is_multi_version_merge) {
    freeze_info.next.frozen_scn_ = MTL(ObTenantFreezeInfoMgr*)->get_snapshot_gc_scn();
  }
  return ret;
}

int ObPartitionMergePolicy::get_medium_merge_tables(
  const ObGetMergeTablesParam &param,
  ObLS &ls,
  const ObTablet &tablet,
  ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  result.reset();
  result.merge_version_ = param.merge_version_;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  DEBUG_SYNC(BEFORE_GET_MAJOR_MGERGE_TABLES);
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(
      !table_store_wrapper.get_member()->is_valid()
      || !param.is_valid()
      || !is_major_merge_type(param.merge_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(table_store_wrapper.get_member()), K(param));
  } else if (OB_ISNULL(base_table = static_cast<ObSSTable*>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("major sstable not exist", K(ret), KPC(table_store_wrapper.get_member()));
  } else if (OB_FAIL(result.handle_.add_sstable(base_table, table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("failed to add base_table to result", K(ret));
  } else if (base_table->get_snapshot_version() >= param.merge_version_) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("medium merge already finished", K(ret), KPC(base_table), K(result));
  } else if (OB_UNLIKELY(tablet.get_snapshot_version() < param.merge_version_)) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("tablet is not ready to schedule medium merge", K(ret), K(tablet), K(param));
  } else if (OB_UNLIKELY(tablet.get_multi_version_start() > param.merge_version_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet haven't kept medium snapshot", K(ret), K(tablet), K(param));
  } else {
    const ObSSTableArray &minor_tables = table_store_wrapper.get_member()->get_minor_sstables();
    bool start_add_table_flag = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
      if (OB_ISNULL(minor_tables[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must not null", K(ret), K(i), K(minor_tables));
      // TODO: add right boundary for major
      } else if (!start_add_table_flag && minor_tables[i]->get_upper_trans_version() >= base_table->get_snapshot_version()) {
        start_add_table_flag = true;
      }
      if (OB_SUCC(ret) && start_add_table_flag) {
        if (OB_FAIL(result.handle_.add_sstable(minor_tables[i], table_store_wrapper.get_meta_handle()))) {
          LOG_WARN("failed to add table", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(result.handle_.check_continues(nullptr))) {
      LOG_WARN("failed to check continues for major merge", K(ret));
      SET_DIAGNOSE_LOCATION(result.error_location_);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(base_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null base table", K(ret), K(tablet));
  } else {
    result.version_range_.base_version_ = 0;
    result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
    result.version_range_.snapshot_version_ = param.merge_version_;
    if (OB_FAIL(get_multi_version_start(param.merge_type_, ls, tablet, result.version_range_, result.snapshot_info_))) {
      LOG_WARN("failed to get multi version_start", K(ret));
    }
  }
  return ret;
}

int ObPartitionMergePolicy::get_mds_merge_tables(
  const storage::ObGetMergeTablesParam &param,
  storage::ObLS &ls,
  const storage::ObTablet &tablet,
  storage::ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = param.merge_type_;
  result.reset();
  DEBUG_SYNC(BEFORE_GET_MINOR_MGERGE_TABLES);

  if (OB_UNLIKELY(!is_mds_minor_merge(merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), "merge_type", merge_type_to_str(merge_type));
  } else {
    ObTableStoreIterator mds_table_iter;
    if (OB_FAIL(tablet.get_mds_sstables(mds_table_iter))) {
      LOG_WARN("failed to get mini  minor sstables", K(ret));
    } else {
      ObTableHandleV2 cur_table_handle;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(mds_table_iter.get_next(cur_table_handle))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next table", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_UNLIKELY(result.handle_.get_count() > 0
            && result.scn_range_.end_scn_ < cur_table_handle.get_table()->get_start_scn())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("log ts not continues, reset previous minor merge tables", K(ret),
                  "last_end_log_ts", result.scn_range_.end_scn_, K(cur_table_handle));
        } else if (OB_FAIL(result.handle_.add_table(cur_table_handle))) {
          LOG_WARN("Failed to add table", K(ret), K(cur_table_handle));
        } else {
          if (1 == result.handle_.get_count()) {
            result.scn_range_.start_scn_ = cur_table_handle.get_table()->get_start_scn();
          }
          result.scn_range_.end_scn_ = cur_table_handle.get_table()->get_end_scn();
        }
      }
    }
  }
  int64_t minor_compact_trigger = DEFAULT_MINOR_COMPACT_TRIGGER;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      minor_compact_trigger = tenant_config->minor_compact_trigger;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (result.handle_.get_count() < MAX(minor_compact_trigger, DEFAULT_MINOR_COMPACT_TRIGGER)) {
    ret = OB_NO_NEED_MERGE;
    result.handle_.reset();
  } else {
    result.version_range_.snapshot_version_ = tablet.get_snapshot_version();
  }
  return ret;
}

int ObPartitionMergePolicy::get_result_by_snapshot(
      ObTablet &tablet,
      const int64_t snapshot,
      ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot));
  } else if (OB_ISNULL(base_table = static_cast<ObSSTable *>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("major sstable not exist", K(ret), KPC(table_store_wrapper.get_member()));
  } else if (base_table->get_snapshot_version() >= snapshot) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_FAIL(result.handle_.add_sstable(base_table, table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("failed to add table into iterator", K(ret), KP(base_table));
  } else {
    const ObSSTableArray &minor_tables = table_store_wrapper.get_member()->get_minor_sstables();
    int64_t start_idx = 0;
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

bool ObPartitionMergePolicy::is_sstable_count_not_safe(const int64_t minor_table_cnt)
{
  bool bret = minor_table_cnt + 1/*major*/ >= MAX_SSTABLE_CNT_IN_STORAGE;
#ifdef ERRSIM
  if (!bret) {
    int ret = OB_SUCCESS;
    ret = OB_E(EventTable::EN_COMPACTION_DIAGNOSE_TABLE_STORE_UNSAFE_FAILED) ret;
    if (OB_FAIL(ret)) {
      bret = true;
      LOG_INFO("ERRSIM EN_COMPACTION_DIAGNOSE_TABLE_STORE_UNSAFE_FAILED with errsim", K(ret), K(bret), K(minor_table_cnt));
    }
  }
#endif
  return bret;
}

int ObPartitionMergePolicy::get_mini_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;

  ObTenantFreezeInfoMgr::NeighbourFreezeInfo freeze_info;
  int64_t merge_inc_base_version = tablet.get_snapshot_version();
  const ObMergeType merge_type = param.merge_type_;
  ObSEArray<ObTableHandleV2, BASIC_MEMSTORE_CNT> memtable_handles;
  result.reset();

  if (OB_UNLIKELY(MINI_MERGE != merge_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), "merge_type", merge_type_to_str(merge_type));
  } else if (is_sstable_count_not_safe(tablet.get_minor_table_count())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_ERROR("Too many sstables, delay mini merge until sstable count falls below MAX_SSTABLE_CNT",
              K(ret), K(tablet));
    // add compaction diagnose info
    ObPartitionMergePolicy::diagnose_table_count_unsafe(MINI_MERGE, ObDiagnoseTabletType::TYPE_MINI_MERGE, tablet);
  } else if (OB_FAIL(tablet.get_all_memtables(memtable_handles))) {
    LOG_WARN("failed to get all memtable", K(ret), K(tablet));
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version,
                                               tablet.get_last_major_snapshot_version(),
                                               freeze_info,
                                               true/*is_multi_version_merge*/))) {
    LOG_WARN("failed to get next major freeze", K(ret), K(merge_inc_base_version), K(tablet));
  } else if (OB_FAIL(find_mini_merge_tables(param, freeze_info, ls, tablet, memtable_handles, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to find mini merge tables", K(ret), K(freeze_info));
    }
  } else if (result.update_tablet_directly_) {
    // do nothing
  } else if (OB_FAIL(deal_with_minor_result(merge_type, ls, tablet, result))) {
    LOG_WARN("failed to deal with minor merge result", K(ret));
  }

  return ret;
}

int ObPartitionMergePolicy::find_mini_merge_tables(
    const ObGetMergeTablesParam &param,
    const ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info,
    ObLS &ls,
    const storage::ObTablet &tablet,
    ObIArray<ObTableHandleV2> &memtable_handles,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  // TODO: @dengzhi.ldz, remove max_snapshot_version, merge all forzen memtables
  // Keep max_snapshot_version currently because major merge must be done step by step
  int64_t max_snapshot_version = freeze_info.next.frozen_scn_.get_val_for_tx();
  const SCN &clog_checkpoint_scn = tablet.get_clog_checkpoint_scn();

  // Freezing in the restart phase may not satisfy end >= last_max_sstable,
  // so the memtable cannot be filtered by scn
  // can only take out all frozen memtable
  ObITabletMemtable *memtable = nullptr;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  bool has_release_memtable = false;

  for (int64_t i = 0; OB_SUCC(ret) && i < memtable_handles.count(); ++i) {
    if (OB_ISNULL(memtable = static_cast<ObITabletMemtable *>(memtable_handles.at(i).get_table()))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable must not null", K(ret), K(tablet));
    } else if (memtable->is_direct_load_memtable()) {
      FLOG_INFO("mini merge only flush data memtables", K(i), K(memtable_handles), KP(memtable));
      break;
    } else if (OB_UNLIKELY(memtable->is_active_memtable())) {
      LOG_DEBUG("skip active memtable", K(i), KPC(memtable), K(memtable_handles));
      break;
    } else if (!memtable->can_be_minor_merged()) {
      FLOG_INFO("memtable cannot mini merge now", K(ret), K(i), KPC(memtable), K(max_snapshot_version), K(memtable_handles), K(param));
      break;
    } else if (memtable->get_end_scn() <= clog_checkpoint_scn) {
      has_release_memtable = true;
    } else if (result.handle_.get_count() > 0) {
      if (result.scn_range_.end_scn_ <= clog_checkpoint_scn) {
        // meet the first memtable should be merged, reset the result to remove the memtable should be released.
        result.reset();
      } else if (result.scn_range_.end_scn_ < memtable->get_start_scn()) {
        FLOG_INFO("scn range  not continues, reset previous minor merge tables",
                  "last_end_scn", result.scn_range_.end_scn_, KPC(memtable), K(tablet));
        // mini merge always use the oldest memtable to dump
        break;
      } else if (memtable->get_snapshot_version() > max_snapshot_version) {
        // This judgment is only to try to prevent cross-major mini merge,
        // but when the result is empty, it can still be added
        LOG_INFO("max_snapshot_version is reached, no need find more tables",
                 K(max_snapshot_version), KPC(memtable));
        break;
      }
    }

    if (FAILEDx(result.handle_.add_memtable(memtable))) {
      LOG_WARN("Failed to add memtable", K(ret), KPC(memtable));
    } else {
      // update end_scn/snapshot_version
      if (1 == result.handle_.get_count()) {
        result.scn_range_.start_scn_ = memtable->get_start_scn();
      }
      result.scn_range_.end_scn_ = memtable->get_end_scn();
      result.version_range_.snapshot_version_ = MAX(memtable->get_snapshot_version(), result.version_range_.snapshot_version_);
    }
  } // end for

  bool need_check_tablet = false;
  if (OB_SUCC(ret)) {
    result.version_range_.multi_version_start_ = tablet.get_multi_version_start();

    if (result.scn_range_.end_scn_ <= clog_checkpoint_scn) {
      if (has_release_memtable) {
        result.update_tablet_directly_ = true;
        result.version_range_.multi_version_start_ = 0; // set multi_version_start to pass tablet::init check
        FLOG_INFO("no memtable should be merged, but has memtable should be released", K(ret), K(result), K(memtable_handles));
      } else {
        ret = OB_NO_NEED_MERGE;
      }
    } else if (OB_FAIL(refine_mini_merge_result(tablet, result, need_check_tablet))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("failed to refine mini merge result", K(ret), K(tablet_id));
      }
    } else if (OB_UNLIKELY(need_check_tablet)) {
      ret = OB_EAGAIN;
      int tmp_ret = OB_SUCCESS;
      ObTabletHandle tmp_tablet_handle;
      if (OB_TMP_FAIL(ls.get_tablet(tablet_id, tmp_tablet_handle, 0, storage::ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("failed to get tablet", K(tmp_ret), K(tablet_id));
      } else if (OB_UNLIKELY(!tmp_tablet_handle.is_valid())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid tablet", K(tmp_ret), K(tablet_id));
      } else if (tmp_tablet_handle.get_obj()->get_clog_checkpoint_scn() != clog_checkpoint_scn) {
        // do nothing, just retry the mini compaction
      } else {
        LOG_ERROR("Unexpected uncontinuous scn_range in mini merge",
              K(ret), K(clog_checkpoint_scn), K(result), K(tablet), KPC(tmp_tablet_handle.get_obj()));
      }
    }

    if (OB_SUCC(ret) && result.version_range_.snapshot_version_ > max_snapshot_version) {
      result.schedule_major_ = true;
    }
  }
  return ret;
}

int ObPartitionMergePolicy::deal_with_minor_result(
    const ObMergeType &merge_type,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  if (result.handle_.empty()) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("no need to minor merge", K(ret), "merge_type", merge_type_to_str(merge_type), K(result));
  } else if (OB_UNLIKELY(!result.scn_range_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid argument to check result", K(ret), K(result));
  } else if (OB_FAIL(result.handle_.check_continues(&result.scn_range_))) {
    LOG_WARN("failed to check continues", K(ret), K(result));
  } else if (OB_FAIL(get_multi_version_start(merge_type, ls, tablet, result.version_range_, result.snapshot_info_))) {
    LOG_WARN("failed to get kept multi_version_start", K(ret), "merge_type", merge_type_to_str(merge_type), K(tablet));
  } else {
    result.version_range_.base_version_ = 0;
    if (OB_SUCC(ret) && !is_mini_merge(merge_type)) {
      if (OB_FAIL(tablet.get_recycle_version(result.version_range_.multi_version_start_, result.version_range_.base_version_))) {
        LOG_WARN("Fail to get table store recycle version", K(ret), K(result.version_range_), K(tablet));
      }
    }
  }
  return ret;
}

int ObPartitionMergePolicy::get_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  int64_t min_snapshot_version = 0;
  int64_t max_snapshot_version = 0;
  const ObMergeType merge_type = param.merge_type_;
  result.reset();
  DEBUG_SYNC(BEFORE_GET_MINOR_MGERGE_TABLES);

  // no need to distinguish data tablet and tx tablet, all minor tables included
  if (OB_UNLIKELY(!is_minor_merge(merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), "merge_type", merge_type_to_str(merge_type));
  } else if (tablet.is_ls_inner_tablet()) {
    min_snapshot_version = 0;
    max_snapshot_version = INT64_MAX;
  } else if (OB_FAIL(get_boundary_snapshot_version(
                 tablet, min_snapshot_version, max_snapshot_version,
                 true /*check_table_cnt*/, true /*is_multi_version_merge*/))) {
    LOG_WARN("fail to calculate boundary version", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(find_minor_merge_tables(param,
                                             min_snapshot_version,
                                             max_snapshot_version,
                                             ls,
                                             tablet,
                                             result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get minor merge tables", K(ret), K(max_snapshot_version));
    }
  }

  return ret;
}

int ObPartitionMergePolicy::get_boundary_snapshot_version(
    const ObTablet &tablet,
    int64_t &min_snapshot,
    int64_t &max_snapshot,
    const bool check_table_cnt,
    const bool is_multi_version_merge)
{
  int ret = OB_SUCCESS;
  int64_t merge_inc_base_version = tablet.get_snapshot_version();
  ObTenantFreezeInfoMgr::NeighbourFreezeInfo freeze_info;
  int64_t last_major_snapshot_version = 0;
  int64_t tablet_table_cnt = 0;

  if (OB_UNLIKELY(tablet.is_ls_inner_tablet())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for special tablet", K(ret), K(tablet));
  } else if (FALSE_IT(last_major_snapshot_version = tablet.get_last_major_snapshot_version())) {
  } else if (FALSE_IT(tablet_table_cnt = tablet.get_major_table_count() + tablet.get_minor_table_count())) {
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version,
                                               last_major_snapshot_version,
                                               freeze_info,
                                               is_multi_version_merge))) {
    LOG_WARN("failed to get freeze info", K(ret), K(merge_inc_base_version), K(tablet));
  } else if (check_table_cnt && tablet_table_cnt >= OB_UNSAFE_TABLE_CNT) {
    max_snapshot = INT64_MAX;
    if (tablet_table_cnt >= OB_EMERGENCY_TABLE_CNT) {
      min_snapshot = 0;
    } else if (last_major_snapshot_version > 0) {
      min_snapshot = last_major_snapshot_version;
    }
  } else {
    if (last_major_snapshot_version > 0) {
      min_snapshot = max(last_major_snapshot_version, freeze_info.prev.frozen_scn_.get_val_for_tx());
    } else {
      min_snapshot = freeze_info.prev.frozen_scn_.get_val_for_tx();
    }

    if (freeze_info.next.frozen_scn_.is_max() && is_multi_version_merge) {
      max_snapshot = MTL(ObTenantFreezeInfoMgr*)->get_snapshot_gc_ts();
    } else {
      max_snapshot = freeze_info.next.frozen_scn_.get_val_for_tx();
    }

    int64_t max_medium_scn = 0;
    ObArenaAllocator allocator("GetMediumList", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const compaction::ObMediumCompactionInfoList *medium_list = nullptr;
    if (OB_FAIL(tablet.read_medium_info_list(allocator, medium_list))) {
      LOG_WARN("failed to read medium info list", K(ret), KPC(medium_list));
    } else if (OB_FAIL(compaction::ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
        tablet, *medium_list, max_medium_scn))) {
      LOG_WARN("failed to get max medium snapshot", K(ret), K(tablet));
    } else {
      min_snapshot = max(min_snapshot, max_medium_scn);
    }
    LOG_DEBUG("get boundary snapshot", K(ret), "tablet_id", tablet.get_tablet_meta().tablet_id_, K(tablet),
              K(min_snapshot), K(max_snapshot), K(max_medium_scn), K(last_major_snapshot_version), K(freeze_info));
  }
  return ret;
}

int ObPartitionMergePolicy::find_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t min_snapshot_version,
    const int64_t max_snapshot_version,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  result.reset_handle_and_range();
  ObTableStoreIterator minor_table_iter;
  int64_t minor_compact_trigger = DEFAULT_MINOR_COMPACT_TRIGGER;

  if (OB_FAIL(tablet.get_mini_minor_sstables(minor_table_iter))) {
    LOG_WARN("failed to get mini  minor sstables", K(ret));
  } else {
    ObSSTable *table = nullptr;
    bool found_greater = false;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        minor_compact_trigger = tenant_config->minor_compact_trigger;
      }
    }

    ObTablesHandleArray minor_merge_candidates;
    while (OB_SUCC(ret)) {
      ObTableHandleV2 cur_table_handle;
      if (OB_FAIL(minor_table_iter.get_next(cur_table_handle))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next table", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(cur_table_handle.get_sstable(table))) {
        LOG_WARN("failed to get sstable from handle", K(ret), K(cur_table_handle));
      } else if (!found_greater && table->get_upper_trans_version() <= min_snapshot_version) {
        continue;
      } else {
        found_greater = true;
        if (0 == minor_merge_candidates.get_count()) {
        } else if (is_history_minor_merge(param.merge_type_) && table->get_upper_trans_version() > max_snapshot_version) {
          break;
        } else if (tablet.get_minor_table_count() + tablet.get_major_table_count() < OB_UNSAFE_TABLE_CNT &&
            table->get_max_merged_trans_version() > max_snapshot_version) {
          LOG_INFO("max_snapshot_version reached, stop find more tables", K(param), K(max_snapshot_version), KPC(table));
          break;
        }
        if (OB_FAIL(minor_merge_candidates.add_table(cur_table_handle))) {
          LOG_WARN("failed to add table", K(ret));
        }
      }
    }

    int64_t left_border = 0;
    int64_t right_border = minor_merge_candidates.get_count();
    if (OB_FAIL(ret)) {
    } else if (MINOR_MERGE != param.merge_type_) {
    } else if (OB_FAIL(refine_minor_merge_tables(tablet, minor_merge_candidates, left_border, right_border))) {
      LOG_WARN("failed to adjust mini minor merge tables", K(ret));
    }

    for (int64_t i = left_border; OB_SUCC(ret) && i < right_border; ++i) {
      ObTableHandleV2 tmp_table_handle;
      if (OB_FAIL(minor_merge_candidates.get_table(i, tmp_table_handle))) {
        LOG_WARN("failed to get table handle from array", K(ret), K(tmp_table_handle));
      } else if (result.handle_.get_count() > 0
          && result.scn_range_.end_scn_ < tmp_table_handle.get_table()->get_start_scn()) {
        LOG_INFO("log ts not continues, reset previous minor merge tables",
                "last_end_log_ts", result.scn_range_.end_scn_, K(tmp_table_handle));
        result.reset_handle_and_range();
      }
      if (FAILEDx(result.handle_.add_table(tmp_table_handle))) {
        LOG_WARN("Failed to add table", K(ret), KPC(table));
      } else {
        if (1 == result.handle_.get_count()) {
          result.scn_range_.start_scn_ = tmp_table_handle.get_table()->get_start_scn();
        }
        result.scn_range_.end_scn_ = tmp_table_handle.get_table()->get_end_scn();
      }
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_CAN_NOT_SCHEDULE_MINOR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_CAN_NOT_SCHEDULE_MINOR", K(ret));
    }
  }
  const int64_t diagnose_table_cnt = 2;
#else
  const int64_t diagnose_table_cnt = DIAGNOSE_TABLE_CNT_IN_STORAGE;
#endif
  if (OB_SUCC(ret)) {
    DEL_SUSPECT_INFO(MINOR_MERGE,
                    tablet.get_tablet_meta().ls_id_, tablet.get_tablet_meta().tablet_id_,
                    ObDiagnoseTabletType::TYPE_MINOR_MERGE);
    if (OB_FAIL(refine_minor_merge_result(param.merge_type_, minor_compact_trigger, result))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("failed to refine minor_merge result", K(ret));
      }
    } else if (FALSE_IT(result.version_range_.snapshot_version_ = tablet.get_snapshot_version())) {
    } else {
      if (OB_FAIL(deal_with_minor_result(param.merge_type_, ls, tablet, result))) {
        LOG_WARN("Failed to deal with minor merge result", K(ret), K(param), K(result));
      } else {
        LOG_TRACE("succeed to get minor merge tables", K(min_snapshot_version), K(max_snapshot_version),
            K(result), K(tablet));
      }
    }
  } else if (OB_NO_NEED_MERGE == ret && tablet.get_minor_table_count() >= diagnose_table_cnt) {
    if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MINOR_MERGE, ObDiagnoseTabletType::TYPE_MINOR_MERGE,
                    tablet.get_tablet_meta().ls_id_,
                    tablet.get_tablet_meta().tablet_id_,
                    ObSuspectInfoType::SUSPECT_CANT_SCHEDULE_MINOR_MERGE,
                    min_snapshot_version, max_snapshot_version, tablet.get_minor_table_count()))) {
      LOG_WARN("failed to add suspect info", K(tmp_ret));
    }
  }
  return ret;
}

int ObPartitionMergePolicy::refine_minor_merge_tables(
    const ObTablet &tablet,
    const ObTablesHandleArray &merge_tables,
    int64_t &left_border,
    int64_t &right_border)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObITable *meta_table = nullptr;
  int64_t covered_by_meta_table_cnt = 0;
  left_border = 0;
  right_border = merge_tables.get_count();

  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (FALSE_IT(meta_table = table_store_wrapper.get_member()->get_meta_major_sstable())) {
  } else if (tablet_id.is_special_merge_tablet()) {
  } else if (merge_tables.get_count() < 2 || nullptr == meta_table) {
    // do nothing
  } else {
    // no need meta merge, but exist meta_sstable
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_tables.get_count(); ++i) {
      if (OB_ISNULL(merge_tables.get_table(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret), K(i), K(merge_tables));
      } else if (merge_tables.get_table(i)->get_upper_trans_version() > meta_table->get_snapshot_version()) {
        break;
      } else {
        ++covered_by_meta_table_cnt;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (covered_by_meta_table_cnt * 2 >= merge_tables.get_count()) {
    right_border = covered_by_meta_table_cnt;
  } else {
    left_border = covered_by_meta_table_cnt;
  }
  return ret;
}

int ObPartitionMergePolicy::get_hist_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = param.merge_type_;
  int64_t max_snapshot_version = 0;
  result.reset();

  if (OB_UNLIKELY(!is_history_minor_merge(merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), "merge_type", merge_type_to_str(merge_type));
  } else if (OB_FAIL(deal_hist_minor_merge(tablet, max_snapshot_version))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to deal hist minor merge", K(ret));
    }
  } else if (OB_FAIL(find_minor_merge_tables(param, 0/*min_snapshot*/,
      max_snapshot_version, ls, tablet, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get minor tables for hist minor merge", K(ret));
    }
  }
  return ret;
}

int ObPartitionMergePolicy::deal_hist_minor_merge(
    const ObTablet &tablet,
    int64_t &max_snapshot_version)
{
  int ret = OB_SUCCESS;
  const int64_t hist_threshold = cal_hist_minor_merge_threshold();
  ObITable *first_major_table = nullptr;
  max_snapshot_version = 0;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected invalid table store", K(ret), K(table_store_wrapper));
  } else if (table_store_wrapper.get_member()->get_minor_sstables().count() < hist_threshold) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_ISNULL(first_major_table = table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false))) {
    // index table during building, need compat with continuous multi version
    if (0 == (max_snapshot_version = MTL(ObTenantFreezeInfoMgr*)->get_latest_frozen_version())) {
      // no freeze info found, wait normal mini minor to free sstable
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No freeze range to do hist minor merge for buiding index", K(ret), K(PRINT_TS_WRAPPER(table_store_wrapper)));
    }
  } else {
    ObSEArray<share::ObFreezeInfo, 8> freeze_infos;
    if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_freeze_info_behind_major_snapshot(
                first_major_table->get_snapshot_version(),
                freeze_infos))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_NO_NEED_MERGE;
      } else {
        LOG_WARN("Failed to get freeze infos behind major version", K(ret), KPC(first_major_table));
      }
    } else if (freeze_infos.count() <= 1) {
      // only one major freeze found, wait normal mini minor to reduce table count
      ret = OB_NO_NEED_MERGE;
      LOG_DEBUG("No enough freeze range to do hist minor merge", K(ret), K(freeze_infos));
    } else {
      int64_t table_cnt = 0;
      int64_t min_minor_version = 0;
      int64_t unused_max_minor_version = 0;
      if (OB_FAIL(get_boundary_snapshot_version(
              tablet, min_minor_version, unused_max_minor_version,
              true /*check_table_cnt*/, true /*is_multi_version_merge*/))) {
        LOG_WARN("fail to calculate boundary version", K(ret));
      } else {
        ObSSTable *table = nullptr;
        const ObSSTableArray &minor_tables = table_store_wrapper.get_member()->get_minor_sstables();
        for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
          if (OB_ISNULL(table = static_cast<ObSSTable*>(minor_tables[i]))) {
            ret = OB_ERR_SYS;
            LOG_ERROR("table must not null", K(ret), K(i), K(table_store_wrapper));
          } else if (table->get_upper_trans_version() <= min_minor_version) {
            table_cnt++;
          } else {
            break;
          }
        }

        if (OB_SUCC(ret)) {
          if (1 < table_cnt) {
            max_snapshot_version = min_minor_version;
          } else {
            ret = OB_NO_NEED_MERGE;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionMergePolicy::diagnose_minor_dag(
    ObMergeType merge_type,
    const ObLSID ls_id,
    const ObTabletID tablet_id,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObTabletMergeExecuteDag dag;
  ObDiagnoseTabletCompProgress progress;
  if (OB_FAIL(ObCompactionDiagnoseMgr::diagnose_dag(
          merge_type,
          ls_id,
          tablet_id,
          ObVersion::MIN_VERSION,
          dag,
          progress))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to init dag", K(ret), K(ls_id), K(tablet_id));
    } else {
      // no minor merge dag
      ret = OB_SUCCESS;
    }
  } else if (progress.is_valid()) { // dag exist
    ADD_COMPACTION_INFO_PARAM(buf, buf_len, "minor_merge_progress", progress);
  }
  return ret;
}

int ObPartitionMergePolicy::diagnose_table_count_unsafe(
    const ObMergeType merge_type,
    const share::ObDiagnoseTabletType diagnose_type,
    const storage::ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t minor_compact_trigger = DEFAULT_MINOR_COMPACT_TRIGGER;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    minor_compact_trigger = tenant_config->minor_compact_trigger;
  }

  // check min_reserved_snapshot
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  int64_t min_merged_snapshot = INT64_MAX;
  int64_t first_minor_start_scn = 0;
  ObStorageSnapshotInfo snapshot_info;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &major_tables = table_store_wrapper.get_member()->get_major_sstables();
    const ObSSTableArray &minor_tables = table_store_wrapper.get_member()->get_minor_sstables();
    // add sstable info
    const int64_t major_table_count = major_tables.count();
    const int64_t minor_table_count = minor_tables.count();
    const ObSSTable *major_sstable = static_cast<const ObSSTable *>(major_tables.get_boundary_table(false/*last*/));
    if (OB_ISNULL(major_sstable)) {
      const ObSSTable *minor_sstable = static_cast<const ObSSTable *>(minor_tables.get_boundary_table(false/*last*/));
      first_minor_start_scn = minor_sstable->get_start_scn().get_val_for_tx();
    } else if (FALSE_IT(min_merged_snapshot = major_sstable->get_snapshot_version())) {
    } else if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(get_min_reserved_snapshot,
        tablet_id,
        min_merged_snapshot,
        snapshot_info))) {
      LOG_WARN("failed to get min reserved snapshot", K(ret), K(tablet_id));
    }

    // check have minor merge DAG
    const int64_t buf_len = compaction::OB_DIAGNOSE_INFO_PARAM_STR_LENGTH;
    char dag_str[buf_len] = "\0";
    if (OB_TMP_FAIL(diagnose_minor_dag(MINOR_MERGE, ls_id, tablet_id, dag_str, buf_len))) {
      LOG_WARN("failed to diagnose minor dag", K(tmp_ret), K(ls_id), K(tablet_id), K(dag_str));
    }
    if (OB_TMP_FAIL(diagnose_minor_dag(HISTORY_MINOR_MERGE, ls_id, tablet_id, dag_str, buf_len))) {
      LOG_WARN("failed to diagnose history minor dag", K(tmp_ret), K(ls_id), K(tablet_id), K(dag_str));
    }

    if (OB_TMP_FAIL(ADD_SUSPECT_INFO(merge_type, diagnose_type,
        ls_id, tablet_id, ObSuspectInfoType::SUSPECT_SSTABLE_COUNT_NOT_SAFE,
        minor_compact_trigger,
        major_table_count, minor_table_count, first_minor_start_scn,
        "snapshot", snapshot_info, "dag", dag_str))) {
      LOG_WARN("failed to add suspect info", K(tmp_ret));
    }
  }

  return ret;
}

int ObPartitionMergePolicy::refine_mini_merge_result(
    const ObTablet &tablet,
    ObGetMergeTablesResult &result,
    bool &need_check_tablet)
{
  int ret = OB_SUCCESS;
  need_check_tablet = false;
  ObITable *last_table = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table store not valid", K(ret), K(table_store_wrapper));
  } else if (OB_ISNULL(last_table =
      table_store_wrapper.get_member()->get_minor_sstables().get_boundary_table(true/*last*/))) {
    // no minor sstable, skip to cut memtable's boundary
  } else if (result.scn_range_.start_scn_ > last_table->get_end_scn()) {
    need_check_tablet = true;
  } else if (result.scn_range_.start_scn_ < last_table->get_end_scn()
      && !tablet.get_tablet_meta().tablet_id_.is_special_merge_tablet()) {
    // fix start_scn to make scn_range continuous in migrate phase for issue 42832934
    if (result.scn_range_.end_scn_ <= last_table->get_end_scn()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("No need mini merge memtable which is covered by existing sstable",
               K(ret), K(result), KPC(last_table), K(PRINT_TS_WRAPPER(table_store_wrapper)), K(tablet));
    } else {
      result.scn_range_.start_scn_ = last_table->get_end_scn();
      FLOG_INFO("Fix mini merge result scn range", K(ret), K(result), KPC(last_table), K(PRINT_TS_WRAPPER(table_store_wrapper)), K(tablet));
    }
  }
  return ret;
}

int ObPartitionMergePolicy::refine_minor_merge_result(
    const ObMergeType merge_type,
    const int64_t minor_compact_trigger,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  if (result.handle_.get_count() <= MAX(minor_compact_trigger, 1)) {
    ret = OB_NO_NEED_MERGE;
    LOG_DEBUG("minor refine, no need to do minor merge", K(result));
    result.handle_.reset();
  } else if (OB_UNLIKELY(!is_minor_merge_type(merge_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected merge type to refine merge tables", K(result), K(ret));
  } else if (0 == minor_compact_trigger || result.handle_.get_count() >= OB_UNSAFE_TABLE_CNT) {
    // no refine
  } else {
    ObTablesHandleArray mini_tables;
    ObITable *table = NULL;
    ObSSTable *sstable = NULL;
    int64_t large_sstable_cnt = 0;
    int64_t large_sstable_row_cnt = 0;
    int64_t mini_sstable_row_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
      ObTableHandleV2 tmp_table_handle;
      if (OB_FAIL(result.handle_.get_table(i, tmp_table_handle))) {
        LOG_WARN("failed to get table from handles array", K(ret), K(i));
      } else if (OB_ISNULL(table = tmp_table_handle.get_table()) || !table->is_minor_sstable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("get unexpected table", KP(table), K(ret));
      } else if (FALSE_IT(sstable = reinterpret_cast<ObSSTable*>(table))) {
      } else {
        if (sstable->get_row_count() > OB_LARGE_MINOR_SSTABLE_ROW_COUNT) { // large sstable
          ++large_sstable_cnt;
          large_sstable_row_cnt += sstable->get_row_count();
          if (mini_tables.get_count() > minor_compact_trigger) {
            break;
          } else {
            mini_tables.reset();
            continue;
          }
        } else {
          mini_sstable_row_cnt += sstable->get_row_count();
        }
        if (OB_FAIL(mini_tables.add_table(tmp_table_handle))) { // mini_tables hold continues small sstables
          LOG_WARN("Failed to push mini minor table into array", K(ret));
        }
      }
    } // end of for

    int64_t size_amplification_factor = OB_DEFAULT_COMPACTION_AMPLIFICATION_FACTOR;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid() && int64_t(tenant_config->_minor_compaction_amplification_factor) > 0) {
        size_amplification_factor = tenant_config->_minor_compaction_amplification_factor;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (large_sstable_cnt > 1
        || mini_sstable_row_cnt > (large_sstable_row_cnt * size_amplification_factor / 100)) {
      // no refine, use current result to compaction
    } else if (mini_tables.get_count() <= minor_compact_trigger) {
      ret = OB_NO_NEED_MERGE;
      result.reset();
    } else if (mini_tables.get_count() != result.handle_.get_count()) {
      // reset the merge result, mini sstable merge into a new mini sstable
      result.reset_handle_and_range();
      for (int64_t i = 0; OB_SUCC(ret) && i < mini_tables.get_count(); i++) {
        ObTableHandleV2 tmp_table_handle;
        if (OB_FAIL(mini_tables.get_table(i, tmp_table_handle))) {
          LOG_WARN("failed to get table from handles array", K(ret), K(i));
        } else if (OB_UNLIKELY(0 != i
            && tmp_table_handle.get_table()->get_start_scn() != result.scn_range_.end_scn_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexepcted table array", K(ret), K(i), K(tmp_table_handle), K(mini_tables));
        } else if (OB_FAIL(result.handle_.add_table(tmp_table_handle))) {
          LOG_WARN("Failed to add table to minor merge result", K(tmp_table_handle), K(ret));
        } else {
          if (1 == result.handle_.get_count()) {
            result.scn_range_.start_scn_ = tmp_table_handle.get_table()->get_start_scn();
          }
          result.scn_range_.end_scn_ = tmp_table_handle.get_table()->get_end_scn();
        }
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("minor refine, mini minor merge sstable refine info", K(result));
      }
    }
  }
  return ret;
}

// call this func means have serialized medium compaction clog = medium_snapshot
int ObPartitionMergePolicy::check_need_medium_merge(
    ObLS &ls,
    storage::ObTablet &tablet,
    const int64_t medium_snapshot,
    bool &need_merge,
    bool &can_merge,
    bool &need_force_freeze)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  need_merge = false;
  can_merge = false;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const int64_t last_major_snapshot_version = tablet.get_last_major_snapshot_version();
  const bool is_tablet_data_status_complete = tablet.get_tablet_meta().ha_status_.is_data_status_complete();

  bool ls_weak_read_ts_ready = ObTenantTabletScheduler::check_weak_read_ts_ready(medium_snapshot, ls);
  ObProtectedMemtableMgrHandle *protected_handle = NULL;

  if (0 >= last_major_snapshot_version) {
    // no major, no medium
  } else {
    need_merge = last_major_snapshot_version < medium_snapshot;
    if (need_merge
        && is_tablet_data_status_complete
        && ls_weak_read_ts_ready) {
      can_merge = tablet.get_snapshot_version() >= medium_snapshot;
      if (!can_merge) {
        ObTableHandleV2 memtable_handle;
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
          need_force_freeze = last_frozen_memtable->get_snapshot_version() < medium_snapshot;
          if (!need_force_freeze) {
            LOG_INFO("tablet no need force freeze", K(ret), K(tablet_id), K(medium_snapshot), KPC(last_frozen_memtable));
          }
        }
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
  if (need_merge && !can_merge && REACH_TENANT_TIME_INTERVAL(60L * 1000L * 1000L)) {
    LOG_INFO("check_need_medium_merge", K(ret), K(ls_id), K(tablet_id),
             K(need_merge), K(can_merge), K(medium_snapshot),
             K(need_force_freeze), K(is_tablet_data_status_complete), K(ls_weak_read_ts_ready));
    if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MEDIUM_MERGE, ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
                    ls_id,
                    tablet_id,
                    ObSuspectInfoType::SUSPECT_CANT_MAJOR_MERGE,
                    medium_snapshot,
                    tablet.get_snapshot_version(),
                    static_cast<int64_t>(is_tablet_data_status_complete),
                    static_cast<int64_t>(ls_weak_read_ts_ready),
                    static_cast<int64_t>(need_force_freeze),
                    tablet.get_tablet_meta().max_serialized_medium_scn_))) {
      LOG_WARN("failed to add suspect info", K(tmp_ret));
    }
  }
  return ret;
}

int64_t ObPartitionMergePolicy::cal_hist_minor_merge_threshold()
{
  int64_t compact_trigger = DEFAULT_MINOR_COMPACT_TRIGGER;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    compact_trigger = tenant_config->minor_compact_trigger;
  }
  return MIN((1 + compact_trigger) * OB_HIST_MINOR_FACTOR, MAX_TABLE_CNT_IN_STORAGE / 2);
}

int ObPartitionMergePolicy::get_multi_version_start(
    const ObMergeType merge_type,
    ObLS &ls,
    const ObTablet &tablet,
    ObVersionRange &result_version_range,
    ObStorageSnapshotInfo &snapshot_info)
{
  int ret = OB_SUCCESS;
  snapshot_info.reset();
  if (tablet.is_ls_inner_tablet()) {
    result_version_range.multi_version_start_ = INT64_MAX;
  } else if (OB_FAIL(tablet.get_kept_snapshot_info(ls.get_min_reserved_snapshot(), snapshot_info))) {
    if (is_mini_merge(merge_type) || OB_TENANT_NOT_EXIST == ret) {
      snapshot_info.reset();
      snapshot_info.snapshot_type_ = ObStorageSnapshotInfo::SNAPSHOT_MULTI_VERSION_START_ON_TABLET;
      snapshot_info.snapshot_ = tablet.get_multi_version_start();
      FLOG_INFO("failed to get multi_version_start, use multi_version_start on tablet", K(ret),
          "merge_type", merge_type_to_str(merge_type), K(snapshot_info));
      ret = OB_SUCCESS; // clear errno
    } else {
      LOG_WARN("failed to get kept multi_version_start", K(ret),
          "tablet_id", tablet.get_tablet_meta().tablet_id_);
    }
  }
  if (OB_SUCC(ret) && !tablet.is_ls_inner_tablet()) {
    // update multi_version_start
    if (snapshot_info.snapshot_ < result_version_range.multi_version_start_) {
      LOG_WARN("cannot reserve multi_version_start", "multi_version_start", result_version_range.multi_version_start_,
               K(snapshot_info));
    } else if (snapshot_info.snapshot_ < result_version_range.snapshot_version_) {
      result_version_range.multi_version_start_ = snapshot_info.snapshot_;
      LOG_DEBUG("succ reserve multi_version_start", "multi_version_start", result_version_range.multi_version_start_,
                K(snapshot_info));
    } else {
      result_version_range.multi_version_start_ = result_version_range.snapshot_version_;
      LOG_DEBUG("no need keep multi version", K(snapshot_info), "multi_version_start", result_version_range.multi_version_start_);
    }
  }
  return ret;
}

int ObPartitionMergePolicy::add_table_with_check(ObGetMergeTablesResult &result, ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_handle));
  } else if (OB_UNLIKELY(!result.handle_.empty()
      && table_handle.get_table()->get_start_scn() > result.scn_range_.end_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log ts range is not continues", K(ret), K(result), K(table_handle));
  } else if (OB_FAIL(result.handle_.add_table(table_handle))) {
    LOG_WARN("failed to add table", K(ret), K(table_handle));
  } else {
    if (1 == result.handle_.get_count()) {
      result.scn_range_.start_scn_ = table_handle.get_table()->get_start_scn();
    }
    result.scn_range_.end_scn_ = table_handle.get_table()->get_end_scn();
  }
  return ret;
}

int ObPartitionMergePolicy::generate_input_result_array(
    const ObGetMergeTablesResult &input_result,
    ObMinorExecuteRangeMgr &minor_range_mgr,
    int64_t &fixed_input_table_cnt,
    ObIArray<ObGetMergeTablesResult> &input_result_array)
{
  int ret = OB_SUCCESS;
  fixed_input_table_cnt = 0;
  input_result_array.reset();
  ObGetMergeTablesResult tmp_result;

  if (minor_range_mgr.exe_range_array_.empty()) {
    if (OB_FAIL(input_result_array.push_back(input_result))) {
      LOG_WARN("failed to add input result", K(ret), K(input_result));
    } else {
      fixed_input_table_cnt += input_result.handle_.get_count();
    }
  } else if (OB_FAIL(tmp_result.copy_basic_info(input_result))) {
    LOG_WARN("failed to copy basic info", K(ret), K(input_result));
  } else {
    const ObTablesHandleArray &table_array = input_result.handle_;
    ObTableHandleV2 tmp_table_handle;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < table_array.get_count(); ++idx) {
      tmp_table_handle.reset();
      if (OB_FAIL(table_array.get_table(idx, tmp_table_handle))) {
        LOG_WARN("fail to get table handle from table handle array", K(ret), K(idx), K(table_array));
      } else if (minor_range_mgr.in_execute_range(tmp_table_handle.get_table())) {
        if (tmp_result.handle_.get_count() < 2) {
        } else if (OB_FAIL(input_result_array.push_back(tmp_result))) {
          LOG_WARN("failed to add tmp result", K(ret), K(tmp_result));
        } else {
          fixed_input_table_cnt += tmp_result.handle_.get_count();
        }
        tmp_result.handle_.reset();
        tmp_result.scn_range_.reset();
      } else if (OB_FAIL(add_table_with_check(tmp_result, tmp_table_handle))) {
        LOG_WARN("failed to add table into result", K(ret), K(tmp_result), K(tmp_table_handle));
      }
    } // end of for

    if (OB_FAIL(ret) || tmp_result.handle_.get_count() < 2) {
    } else if (OB_FAIL(input_result_array.push_back(tmp_result))) {
      LOG_WARN("failed to add tmp result", K(ret), K(tmp_result));
    } else {
      fixed_input_table_cnt += tmp_result.handle_.get_count();
    }
  }
  return ret;
}

int ObPartitionMergePolicy::split_parallel_minor_range(
    const int64_t table_count_threshold,
    const ObGetMergeTablesResult &input_result,
    ObIArray<ObGetMergeTablesResult> &parallel_result)
{
  int ret = OB_SUCCESS;
  const int64_t input_table_cnt = input_result.handle_.get_count();
  ObGetMergeTablesResult tmp_result;
  if (input_table_cnt < table_count_threshold) {
    // if there are no running minor dags, then the input_table_cnt must be greater than threshold.
  } else if (input_table_cnt < OB_MINOR_PARALLEL_SSTABLE_CNT_TRIGGER) {
    if (OB_FAIL(parallel_result.push_back(input_result))) {
      LOG_WARN("failed to add input result", K(ret), K(input_result));
    }
  } else if (OB_FAIL(tmp_result.copy_basic_info(input_result))) {
    LOG_WARN("failed to copy basic info", K(ret), K(input_result));
  } else {
    const int64_t parallel_dag_cnt = MAX(1, input_table_cnt / OB_MINOR_PARALLEL_SSTABLE_CNT_IN_DAG);
    const int64_t parallel_table_cnt = input_table_cnt / parallel_dag_cnt;
    const ObTablesHandleArray &table_array = input_result.handle_;
    ObTableHandleV2 tmp_table_handle;

    int64_t start = 0;
    int64_t end = 0;
    for (int64_t seq = 0; OB_SUCC(ret) && seq < parallel_dag_cnt; ++seq) {
      start = parallel_table_cnt * seq;
      end = (parallel_dag_cnt - 1 == seq) ? table_array.get_count() : end + parallel_table_cnt;
      for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
        tmp_table_handle.reset();
        if (OB_FAIL(table_array.get_table(i, tmp_table_handle))) {
          LOG_WARN("fail to get table handle from tables handel array", K(ret), K(i), K(table_array));
        } else if (OB_FAIL(add_table_with_check(tmp_result, tmp_table_handle))) {
          LOG_WARN("failed to add table into result", K(ret), K(tmp_result), K(tmp_table_handle));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(parallel_result.push_back(tmp_result))) {
        LOG_WARN("failed to add tmp result", K(ret), K(tmp_result));
      } else {
        LOG_DEBUG("success to push result", K(ret), K(tmp_result), K(parallel_result));
        tmp_result.handle_.reset();
        tmp_result.scn_range_.reset();
      }
    }
  }
  return ret;
}

int ObPartitionMergePolicy::generate_parallel_minor_interval(
    const ObMergeType merge_type,
    const int64_t minor_compact_trigger,
    const ObGetMergeTablesResult &input_result,
    ObMinorExecuteRangeMgr &minor_range_mgr,
    ObIArray<ObGetMergeTablesResult> &parallel_result)
{
  int ret = OB_SUCCESS;
  parallel_result.reset();
  ObSEArray<ObGetMergeTablesResult, 2> input_result_array;
  int64_t fixed_input_table_cnt = 0;

  if (!compaction::is_minor_merge_type(merge_type) && !compaction::is_mds_minor_merge(merge_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge type", K(ret), "merge_type", merge_type_to_str(merge_type));
  } else if (input_result.handle_.get_count() < minor_compact_trigger) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_FAIL(generate_input_result_array(input_result, minor_range_mgr, fixed_input_table_cnt, input_result_array))) {
    LOG_WARN("failed to generate input result into array", K(ret), K(input_result));
  } else if (fixed_input_table_cnt < minor_compact_trigger) {
    // the quantity of table that should be merged is smaller than trigger, wait for the existing minor tasks to finish.
    ret = OB_NO_NEED_MERGE;
  }

  /*
   * When existing minor dag, we should ensure that the quantity of tables per parallel dag is a reasonable value:
   * 1. If compact_trigger is small, minor merge should be easier to schedule, we should lower the threshold;
   * 2. If compact_trigger is big, we should upper the threshold to prevent the creation of dag frequently.
   */
  int64_t exist_dag_cnt = minor_range_mgr.exe_range_array_.count();
  int64_t table_count_threshold = (0 == exist_dag_cnt)
                                ? minor_compact_trigger
                                : OB_MINOR_PARALLEL_SSTABLE_CNT_IN_DAG + (OB_MINOR_PARALLEL_SSTABLE_CNT_IN_DAG / 2) * (exist_dag_cnt - 1);
  for (int64_t i = 0; OB_SUCC(ret) && i < input_result_array.count(); ++i) {
    if (OB_FAIL(split_parallel_minor_range(table_count_threshold, input_result_array.at(i), parallel_result))) {
      LOG_WARN("failed to split parallel minor range", K(ret), K(input_result_array.at(i)));
    }
  }
  return ret;
}


/*************************************** ObMinorExecuteRangeMgr ***************************************/
bool compareScnRange(share::ObScnRange &a, share::ObScnRange &b)
{
  return a.end_scn_ < b.end_scn_;
}

int ObMinorExecuteRangeMgr::get_merge_ranges(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  ObTabletMergeDagParam param;
  param.merge_type_ = MINOR_MERGE;
  param.merge_version_ = ObVersion::MIN_VERSION;
  param.ls_id_ = ls_id;
  param.tablet_id_ = tablet_id;
  param.skip_get_tablet_ = true;

  if (OB_FAIL(MTL(ObTenantDagScheduler*)->get_minor_exe_dag_info(param, exe_range_array_))) {
    LOG_WARN("failed to get minor exe dag info", K(ret));
  } else if (OB_FAIL(sort_ranges())) {
    LOG_WARN("failed to sort ranges", K(ret), K(param));
  }
  return ret;
}

int ObMinorExecuteRangeMgr::sort_ranges()
{
  int ret = OB_SUCCESS;
  lib::ob_sort(exe_range_array_.begin(), exe_range_array_.end(), compareScnRange);
  for (int i = 1; OB_SUCC(ret) && i < exe_range_array_.count(); ++i) {
    if (OB_UNLIKELY(!exe_range_array_.at(i).is_valid()
        || (exe_range_array_.at(i - 1).start_scn_.get_val_for_tx() > 0 // except meta major merge range
            && exe_range_array_.at(i).start_scn_.get_val_for_tx() > 0
            && exe_range_array_.at(i).start_scn_ < exe_range_array_.at(i - 1).end_scn_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected minor ranges", K(ret), K(i), K(exe_range_array_));
    }
  }
  return ret;
}

bool ObMinorExecuteRangeMgr::in_execute_range(const ObITable *table) const
{
  bool bret = false;
  if (exe_range_array_.count() > 0 && OB_NOT_NULL(table)) {
    for (int i = 0; i < exe_range_array_.count(); ++i) {
      if (table->get_end_scn() <= exe_range_array_.at(i).end_scn_
          && table->get_end_scn() > exe_range_array_.at(i).start_scn_) {
        bret = true;
        LOG_DEBUG("in execute range", KPC(table), K(i), K(exe_range_array_.at(i)));
        break;
      }
    }
  }
  return bret;
}


/*************************************** ObAdaptiveMergePolicy ***************************************/
const char * ObAdaptiveMergeReasonStr[] = {
  "NONE",
  "LOAD_DATA_SCENE",
  "TOMBSTONE_SCENE",
  "INEFFICIENT_QUERY",
  "FREQUENT_WRITE",
  "TENANT_MAJOR",
  "USER_REQUEST",
  "REBUILD_COLUMN_GROUP",
  "CRAZY_MEDIUM_FOR_TEST"
};

const char* ObAdaptiveMergePolicy::merge_reason_to_str(const int64_t merge_reason)
{
  STATIC_ASSERT(static_cast<int64_t>(INVALID_REASON) == ARRAYSIZEOF(ObAdaptiveMergeReasonStr),
                "adaptive merge reason str len is mismatch");
  const char *str = "";
  if (merge_reason >= INVALID_REASON || merge_reason < NONE) {
    str = "invalid_merge_reason";
  } else {
    str = ObAdaptiveMergeReasonStr[merge_reason];
  }
  return str;
}

bool ObAdaptiveMergePolicy::is_valid_merge_reason(const AdaptiveMergeReason &reason)
{
  return reason > AdaptiveMergeReason::NONE &&
         reason < AdaptiveMergeReason::INVALID_REASON;
}

bool ObAdaptiveMergePolicy::is_valid_compaction_policy(const AdaptiveCompactionPolicy &policy)
{
  return policy >= AdaptiveCompactionPolicy::NORMAL &&
         policy < AdaptiveCompactionPolicy::INVALID_POLICY;
}

#ifdef ERRSIM
  #define SHOULD_SCHEDULE_MERGE(tracepoint)            \
    int ret = OB_E(EventTable::tracepoint) OB_SUCCESS; \
    if (OB_FAIL(ret)) {                                \
      bret = true;                                     \
      LOG_INFO("ERRSIM should merge:" #tracepoint);    \
    }
#endif

bool ObAdaptiveMergePolicy::is_schedule_medium(const share::schema::ObTableModeFlag &mode)
{
  bool bret = take_advanced_policy(mode) || take_normal_policy(mode);
#ifdef ERRSIM
  SHOULD_SCHEDULE_MERGE(EN_COMPACTION_SCHEDULE_MEDIUM_MERGE_AFTER_MINI);
#endif
  return bret;
}
bool ObAdaptiveMergePolicy::is_schedule_meta(const share::schema::ObTableModeFlag &mode)
{
  bool bret = take_advanced_policy(mode) || take_extrem_policy(mode);
#ifdef ERRSIM
  SHOULD_SCHEDULE_MERGE(EN_COMPACTION_SCHEDULE_META_MERGE);
#endif
  return bret;
}

bool ObAdaptiveMergePolicy::take_normal_policy(const share::schema::ObTableModeFlag &mode)
{
  return share::schema::ObTableModeFlag::TABLE_MODE_NORMAL == mode
      || share::schema::ObTableModeFlag::TABLE_MODE_QUEUING == mode;
}
bool ObAdaptiveMergePolicy::take_advanced_policy(const share::schema::ObTableModeFlag &mode)
{
  return share::schema::ObTableModeFlag::TABLE_MODE_QUEUING_MODERATE == mode
      || share::schema::ObTableModeFlag::TABLE_MODE_QUEUING_SUPER == mode;
}
bool ObAdaptiveMergePolicy::take_extrem_policy(const share::schema::ObTableModeFlag &mode)
{
  return share::schema::ObTableModeFlag::TABLE_MODE_QUEUING_EXTREME == mode;
}

int ObAdaptiveMergePolicy::get_meta_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = param.merge_type_;
  result.reset();

  if (OB_UNLIKELY(META_MAJOR_MERGE != merge_type && MEDIUM_MERGE != merge_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), "merge_type", merge_type_to_str(merge_type));
  } else if (OB_FAIL(find_adaptive_merge_tables(merge_type, tablet, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("Failed to find minor merge tables", K(ret));
    }
  } else if (OB_FAIL(result.handle_.check_continues(nullptr))) {
    LOG_WARN("failed to check continues", K(ret), K(result));
  } else if (MEDIUM_MERGE == merge_type) {
    result.version_range_.snapshot_version_ = MIN(tablet.get_snapshot_version(), result.version_range_.snapshot_version_);
    if (OB_FAIL(ObPartitionMergePolicy::get_multi_version_start(
        merge_type, ls, tablet, result.version_range_, result.snapshot_info_))) {
      LOG_WARN("failed to get multi version_start", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("succeed to get meta major merge tables", K(merge_type), K(result), K(tablet));
  }
  return ret;
}

int ObAdaptiveMergePolicy::find_adaptive_merge_tables(
    const ObMergeType &merge_type,
    const storage::ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  int64_t min_snapshot = 0;
  int64_t max_snapshot = 0;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  ObSSTable *base_table = nullptr;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(NULL == (table_store = table_store_wrapper.get_member()) || !table_store->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletTableStore is not valid", K(ret), K(table_store_wrapper));
  } else if (table_store->get_minor_sstables().empty() || table_store->get_major_sstables().empty()) {
    ret = OB_NO_NEED_MERGE;
    LOG_DEBUG("no minor/major sstable to do meta major merge", K(ret), KPC(table_store));
  } else if (is_meta_major_merge(merge_type)) {
    base_table = table_store->get_meta_major_sstable();
    if (nullptr == base_table) {
      base_table = static_cast<ObSSTable*>(table_store->get_major_sstables().get_boundary_table(true/*last*/));
    }
    const ObSSTableArray &minor_tables = table_store->get_minor_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
      const int64_t cur_upper_trans_version = minor_tables[i]->get_upper_trans_version();
      if (cur_upper_trans_version <= base_table->get_snapshot_version()) {
        continue;
      } else if (cur_upper_trans_version > tablet.get_snapshot_version()) {
        ret = OB_NO_NEED_MERGE;
        LOG_WARN("first minor upper trans version is bigger than tablet snapshot version, no need to merge",
            K(ret), K(cur_upper_trans_version), "tablet_snapshot_version", tablet.get_snapshot_version());
      }
      break;
    }
  } else {
    base_table = static_cast<ObSSTable*>(table_store->get_major_sstables().get_boundary_table(true/*last*/));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(base_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null base table", K(ret), KPC(table_store), K(tablet));
  } else if (OB_FAIL(ObPartitionMergePolicy::get_boundary_snapshot_version(tablet, min_snapshot, max_snapshot))) {
    LOG_WARN("failed to get boundary snapshot version", K(ret), KPC(base_table), K(min_snapshot), K(max_snapshot));
  } else if (base_table->get_snapshot_version() < min_snapshot || max_snapshot != INT64_MAX /*exist next freeze info*/) {
    ret = OB_NO_NEED_MERGE;
    LOG_DEBUG("no need meta merge when the tablet is doing major merge", K(ret), K(min_snapshot), K(max_snapshot), KPC(base_table));
  } else if (OB_FAIL(add_meta_merge_result(base_table, table_store_wrapper.get_meta_handle(), result, true))) {
    LOG_WARN("failed to add base table to meta merge result", K(ret), KPC(base_table), K(result));
  } else {
    int64_t tx_determ_table_cnt = 1;
    int64_t inc_row_cnt = 0;
    const ObSSTableArray &minor_tables = table_store->get_minor_sstables();
    bool found_undeterm_table = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
      ObITable *table = minor_tables[i];
      if (OB_UNLIKELY(NULL == table || !table->is_multi_version_minor_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected table", K(ret), K(i), K(PRINT_TS_WRAPPER(table_store_wrapper)));
      } else if (result.handle_.get_count() <= 1 && table->get_upper_trans_version() <= base_table->get_snapshot_version()) {
        continue; // skip minor sstable which has been merged
      } else if (!table->is_trans_state_deterministic()) {
        found_undeterm_table = true;
      } else if (!found_undeterm_table) {
        ++tx_determ_table_cnt;
        inc_row_cnt += static_cast<ObSSTable *>(table)->get_row_count();
      }

      if (FAILEDx(add_meta_merge_result(table, table_store_wrapper.get_meta_handle(), result, !found_undeterm_table))) {
        LOG_WARN("failed to add minor table to meta merge result", K(ret));
      }
    } // end for

    const int64_t base_inc_row_cnt = base_table->get_row_count();
    bool scanty_tx_determ_table = tx_determ_table_cnt < 2;
    bool scanty_inc_row_cnt = inc_row_cnt < TRANS_STATE_DETERM_ROW_CNT_THRESHOLD
                                 || inc_row_cnt < INC_ROW_COUNT_PERCENTAGE_THRESHOLD * base_inc_row_cnt;

#ifdef ERRSIM
  #define META_POLICY_ERRSIM(tracepoint)                      \
    do {                                                      \
      if (OB_SUCC(ret)) {                                     \
        ret = OB_E((EventTable::tracepoint)) OB_SUCCESS;      \
        if (OB_FAIL(ret)) {                                   \
          ret = OB_SUCCESS;                                   \
          STORAGE_LOG(INFO, "ERRSIM " #tracepoint);           \
          scanty_tx_determ_table = false;                     \
          scanty_inc_row_cnt = false;                         \
        }                                                     \
      }                                                       \
    } while(0);
  META_POLICY_ERRSIM(EN_COMPACTION_SCHEDULE_META_MERGE);
  #undef META_POLICY_ERRSIM
#endif

    if (OB_FAIL(ret)) {
    } else if (scanty_tx_determ_table || scanty_inc_row_cnt) {
      int tmp_ret = OB_SUCCESS;
      ObTableQueuingModeCfg cfg;
      if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->get_queuing_cfg(tablet.get_ls_id(), tablet.get_tablet_id(), cfg))) {
        LOG_WARN_RET(tmp_ret, "failed to get table queuing mode, treat it as normal table");
      }
      if (cfg.is_queuing_mode() && scanty_inc_row_cnt && !scanty_tx_determ_table) {
      } else {
        ret = OB_NO_NEED_MERGE;
        if (REACH_TENANT_TIME_INTERVAL(30_s) || cfg.is_queuing_mode()) {
          LOG_INFO("no enough table or no enough rows for meta merge", K(ret),
              K(scanty_tx_determ_table), K(scanty_inc_row_cnt),  K(inc_row_cnt), K(base_inc_row_cnt), K(result), K(PRINT_TS_WRAPPER(table_store_wrapper)));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_meta_major_merge(merge_type)) {
      result.version_range_.snapshot_version_ = tablet.get_snapshot_version();
    }

    if (OB_FAIL(ret)) {
    } else if (result.version_range_.snapshot_version_ < tablet.get_multi_version_start()
            || result.version_range_.snapshot_version_ <= base_table->get_snapshot_version()) {
      ret = OB_NO_NEED_MERGE;
      if (REACH_TENANT_TIME_INTERVAL(30_s)) {
        LOG_INFO("chosen snapshot is abandoned", K(ret), K(result), K(tablet.get_multi_version_start()), KPC(base_table));
      }
    }

#ifdef ERRSIM
  if (OB_NO_NEED_MERGE == ret) {
    if (tablet.get_tablet_meta().tablet_id_.id() > ObTabletID::MIN_USER_TABLET_ID) {
      ret = OB_E(EventTable::EN_SCHEDULE_MEDIUM_COMPACTION) ret;
      LOG_INFO("errsim", K(ret), "tablet_id", tablet.get_tablet_meta().tablet_id_);
      if (OB_FAIL(ret)) {
        FLOG_INFO("set schedule medium with errsim", "tablet_id", tablet.get_tablet_meta().tablet_id_);
        ret = OB_SUCCESS;
      }
    }
  }
#endif

  } // else
  return ret;
}

int ObAdaptiveMergePolicy::add_meta_merge_result(
    ObITable *table,
    const ObStorageMetaHandle &table_meta_handle,
    ObGetMergeTablesResult &result,
    const bool update_snapshot_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(table));
  } else if (OB_FAIL(result.handle_.add_sstable(table, table_meta_handle))) {
    LOG_WARN("failed to add table", K(ret), KPC(table));
  } else if (table->is_major_sstable()) {
    result.version_range_.base_version_ = 0;
    result.version_range_.multi_version_start_ = table->get_snapshot_version();
    result.version_range_.snapshot_version_ = table->get_snapshot_version();
  } else if (update_snapshot_flag) {
    int64_t max_snapshot = MAX(table->get_max_merged_trans_version(), result.version_range_.snapshot_version_);
    result.version_range_.multi_version_start_ = max_snapshot;
    result.version_range_.snapshot_version_ = max_snapshot;
    result.scn_range_.end_scn_ = table->get_end_scn();
  }
  return ret;
}

int ObAdaptiveMergePolicy::get_adaptive_merge_reason(
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool crazy_medium_flag = false;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;

  reason = AdaptiveMergeReason::NONE;
  ObTabletStatAnalyzer tablet_analyzer;

#ifdef ENABLE_DEBUG_LOG
  crazy_medium_flag = GCONF.enable_crazy_medium_compaction;
#endif

  if (tablet_id.is_special_merge_tablet()) {
    // do nothing
  } else if (crazy_medium_flag) {
    reason = AdaptiveMergeReason::CRAZY_MEDIUM_FOR_TEST;
    LOG_DEBUG("check crazy medium situation", K(ret), K(ls_id), K(tablet_id), K(reason), K(crazy_medium_flag));
  } else if (OB_FAIL(MTL(ObTenantTabletStatMgr *)->get_tablet_analyzer(ls_id, tablet_id, tablet_analyzer))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get tablet analyzer stat", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_TMP_FAIL(check_inc_sstable_row_cnt_percentage(tablet, reason))) {
      LOG_WARN("failed to check sstable data situation", K(tmp_ret), K(ls_id), K(tablet_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    if (OB_TMP_FAIL(check_tombstone_situation(tablet_analyzer, reason))) {
      LOG_WARN("failed to check tombstone scene", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
    }
    if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_load_data_situation(tablet_analyzer, reason))) {
      LOG_WARN("failed to check load data scene", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
    }
    if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_inc_sstable_row_cnt_percentage(tablet, reason))) {
      LOG_WARN("failed to check sstable data situation", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
    }
    if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_ineffecient_read(tablet_analyzer, reason))) {
      LOG_WARN("failed to check ineffecient read", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret) && AdaptiveMergeReason::NONE == reason) {
    ret = OB_E(EventTable::EN_COMPACTION_SCHEDULE_MEDIUM_MERGE_AFTER_MINI) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS;
      reason = AdaptiveMergeReason::TOMBSTONE_SCENE;
      LOG_INFO("EN_COMPACTION_SCHEDULE_MEDIUM_MERGE_AFTER_MINI, set adaptive reason", K(reason));
    }
  }
#endif

  if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000 /*10s*/)) {
    LOG_INFO("Check tablet adaptive merge reason", K(ret), K(ls_id), K(tablet_id),  K(reason), K(tablet_analyzer), K(crazy_medium_flag));
  }
  return ret;
}

int ObAdaptiveMergePolicy::check_tombstone_reason(
    const storage::ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  reason = AdaptiveMergeReason::NONE;
  ObTabletStatAnalyzer tablet_analyzer;
  if (tablet_id.is_special_merge_tablet()) {
    // do nothing
  } else if (OB_FAIL(MTL(ObTenantTabletStatMgr *)->get_tablet_analyzer(ls_id, tablet_id, tablet_analyzer))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get tablet analyzer stat", K(ret), K(ls_id), K(tablet_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_TMP_FAIL(check_tombstone_situation(tablet_analyzer, reason))) {
    LOG_WARN("failed to check tombstone scene", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
  }
  return ret;
}

int ObAdaptiveMergePolicy::check_inc_sstable_row_cnt_percentage(
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObSSTable *last_major = nullptr;
  int64_t base_row_count = 0;
  int64_t inc_row_count = 0;
  ObSSTable *sstable = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (FALSE_IT(last_major = static_cast<ObSSTable *>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true)))) {
  } else {
    base_row_count = (nullptr == last_major) ? 0 : last_major->get_row_count();
    const ObSSTableArray &minor_sstables = table_store_wrapper.get_member()->get_minor_sstables();
    for (int i = 0; OB_SUCC(ret) && i < minor_sstables.count(); ++i) {
      if (OB_ISNULL(sstable = minor_sstables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable is null", K(ret), K(i));
      } else {
        inc_row_count += sstable->get_row_count();
      }
    }
  }
  if ((inc_row_count > INC_ROW_COUNT_THRESHOLD) ||
      (base_row_count > BASE_ROW_COUNT_THRESHOLD &&
      (inc_row_count * 100 / base_row_count) > LOAD_DATA_SCENE_THRESHOLD)) {
    reason = AdaptiveMergeReason::FREQUENT_WRITE;
  }
  LOG_DEBUG("check_sstable_data_situation", K(ret), K(ls_id), K(tablet_id), K(reason),
      K(base_row_count), K(inc_row_count));
  return ret;
}

int ObAdaptiveMergePolicy::check_load_data_situation(
    const storage::ObTabletStatAnalyzer &analyzer,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  reason = AdaptiveMergeReason::NONE;

  if (OB_UNLIKELY(!analyzer.tablet_stat_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(analyzer));
  } else if (analyzer.is_hot_tablet() && analyzer.is_insert_mostly()) {
    reason = AdaptiveMergeReason::LOAD_DATA_SCENE;
  }
  LOG_DEBUG("check_load_data_situation", K(ret), K(reason), K(analyzer));
  return ret;
}

int ObAdaptiveMergePolicy::check_tombstone_situation(
    const storage::ObTabletStatAnalyzer &analyzer,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  reason = AdaptiveMergeReason::NONE;

  if (OB_UNLIKELY(!analyzer.tablet_stat_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(analyzer));
  } else if ((analyzer.tablet_stat_.merge_cnt_ > 1 && analyzer.is_update_or_delete_mostly()) || analyzer.has_accumnulated_delete()) {
    reason = AdaptiveMergeReason::TOMBSTONE_SCENE;
  }
  LOG_DEBUG("check_tombstone_situation", K(ret), K(reason), K(analyzer));
  return ret;
}

int ObAdaptiveMergePolicy::check_ineffecient_read(
    const storage::ObTabletStatAnalyzer &analyzer,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  reason = AdaptiveMergeReason::NONE;

  if (OB_UNLIKELY(!analyzer.tablet_stat_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(analyzer));
  } else if (analyzer.is_hot_tablet() && analyzer.has_slow_query()) {
    reason = AdaptiveMergeReason::INEFFICIENT_QUERY;
  }
  LOG_DEBUG("check_ineffecient_read", K(ret), K(reason), K(analyzer));
  return ret;
}


/*************************************** ObCOMajorMergePolicy ***************************************/
const char * ObCOMajorMergeTypeStr[] = {
  "INVALID_CO_MAJOR_MERGE_TYPE",
  "BUILD_COLUMN_STORE_MERGE",
  "BUILD_ROW_STORE_MERGE",
  "REBUILD_COLUMN_STORE_MERGE",
};

const char* ObCOMajorMergePolicy::co_major_merge_type_to_str(const ObCOMajorMergeType co_merge_type)
{
  STATIC_ASSERT(static_cast<int64_t>(MAX_CO_MAJOR_MERGE_TYPE) == ARRAYSIZEOF(ObCOMajorMergeTypeStr),
                "co major merge type str len is mismatch");
  const char *str = "";
  if (OB_UNLIKELY(co_merge_type >= MAX_CO_MAJOR_MERGE_TYPE)) {
    str = "invalid_co_major_merge_type";
  } else {
    str = ObCOMajorMergeTypeStr[co_merge_type];
  }
  return str;
}

int ObCOMajorMergePolicy::decide_co_major_sstable_status(
    const ObCOSSTableV2 &co_sstable,
    const ObStorageSchema &storage_schema,
    ObCOMajorSSTableStatus &major_sstable_status)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema.get_column_groups();
  const int64_t cg_count = cg_schemas.count();
  major_sstable_status = ObCOMajorSSTableStatus::INVALID_CO_MAJOR_SSTABLE_STATUS;

  for (int64_t idx = 0; idx < cg_count; idx++) {
    if (cg_schemas.at(idx).is_all_column_group()) {
      if (co_sstable.is_row_store_only_co_table()) {
        major_sstable_status = ObCOMajorSSTableStatus::COL_ONLY_ALL;
      } else {
        major_sstable_status = ObCOMajorSSTableStatus::COL_WITH_ALL;
      }
      break;
    } else if (cg_schemas.at(idx).is_rowkey_column_group()) {
      if (co_sstable.is_row_store_only_co_table()) {
        major_sstable_status = ObCOMajorSSTableStatus::PURE_COL_ONLY_ALL;
      } else {
        major_sstable_status = ObCOMajorSSTableStatus::PURE_COL;
      }
      break;
    }
  }

  if (OB_UNLIKELY(!is_valid_co_major_sstable_status(major_sstable_status))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to decide co major sstable status", K(ret), K(major_sstable_status), K(cg_schemas), K(co_sstable));
  } else {
    LOG_DEBUG("success to decide co major sstable status", K(ret), K(major_sstable_status));
  }
  return ret;
}

int ObCOMajorMergePolicy::estimate_row_cnt_for_major_merge(
    const uint64_t table_id,
    const ObIArray<ObITable *> &tables,
    const ObStorageSchema &storage_schema,
    const ObTabletHandle &tablet_handle,
    int64_t &estimate_row_cnt)
{
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag(ObQueryFlag::Forward,
                          true,   /*is daily merge scan*/
                          true,   /*is read multiple macro block*/
                          false,  /*sys task scan, read one macro block in single io*/
                          false,  /*full row scan flag, obsoleted*/
                          false,  /*index back*/
                          false); /*query_stat*/
  estimate_row_cnt = 0;
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_COMPACTION_ESTIMATE_ROW_FAILED) ret;
  if (OB_FAIL(ret)) {
    LOG_INFO("ERRSIM EN_COMPACTION_ESTIMATE_ROW_FAILED", K(ret));
    return ret;
  }
#endif
  /*
  * 1. if tables.empty(), no writes, row count = 0
  * 2. else, do estimate, use logical row count instead of physical
  */
  if (!tables.empty()) {
    ObTableEstimateBaseInput base_input(query_flag, table_id, transaction::ObTransID(), tables, tablet_handle);
    ObDatumRange whole_range;
    whole_range.set_whole_range();
    ObSEArray<ObDatumRange, 1> ranges;
    if (OB_FAIL(ranges.push_back(whole_range))) {
      LOG_WARN("failed to add ranges", K(ret), K(ranges), K(whole_range));
    } else {
      ObPartitionEst part_estimate;
      ObSEArray<ObEstRowCountRecord, MAX_SSTABLE_CNT_IN_STORAGE> records;
      if (OB_FAIL(ObTableEstimator::estimate_row_count_for_scan(base_input, ranges, part_estimate, records))) {
        LOG_WARN("failed to estimate row counts", K(ret), K(part_estimate), K(records));
      } else {
        estimate_row_cnt = MAX(1, part_estimate.logical_row_count_);
        LOG_DEBUG("successfully estimate row cnt", K(ret), K(estimate_row_cnt));
      }
    }
  }

  return ret;
}

bool ObCOMajorMergePolicy::whether_to_build_row_store(
    const int64_t &estimate_row_cnt,
    const int64_t &column_cnt)
{
  return column_cnt < COL_CNT_THRESHOLD_BUILD_ROW_STORE || estimate_row_cnt < ROW_CNT_THRESHOLD_BUILD_ROW_STORE;
}

bool ObCOMajorMergePolicy::whether_to_rebuild_column_store(
    const ObCOMajorSSTableStatus &major_sstable_status,
    const int64_t &estimate_row_cnt,
    const int64_t &column_cnt)
{
  bool bret = false;
  if (is_redundant_row_store_major_sstable(major_sstable_status)) {
    bret = column_cnt > COL_CNT_THRESHOLD_REBUILD_COLUMN_STORE || estimate_row_cnt > ROW_CNT_THRESHOLD_REBUILD_COLUMN_STORE;
  } else {
    bret = column_cnt > COL_CNT_THRESHOLD_REBUILD_COLUMN_STORE || estimate_row_cnt > ROW_CNT_THRESHOLD_REBUILD_ROWKEY_STORE;
  }
  return bret;
}

int ObCOMajorMergePolicy::decide_co_major_merge_type(
    const ObCOSSTableV2 &co_sstable,
    const ObIArray<ObITable *> &tables,
    const ObStorageSchema &storage_schema,
    const ObTabletHandle &tablet_handle,
    ObCOMajorMergeType &major_merge_type)
{
  int ret = OB_SUCCESS;
  ObCOMajorSSTableStatus major_sstable_status = ObCOMajorSSTableStatus::INVALID_CO_MAJOR_SSTABLE_STATUS;
  int64_t estimate_row_cnt = 0;
  const ObTabletID tablet_id = co_sstable.get_key().tablet_id_;

  if (OB_FAIL(decide_co_major_sstable_status(co_sstable, storage_schema, major_sstable_status))) {
    LOG_WARN("failed to decide co major sstable status");
  } else if (OB_UNLIKELY(EN_COMPACTION_DISABLE_ROW_COL_SWITCH)) {
    major_merge_type = is_major_sstable_match_schema(major_sstable_status) ? BUILD_COLUMN_STORE_MERGE : REBUILD_COLUMN_STORE_MERGE;
    LOG_INFO("[RowColSwitch] disable row col switch, only allow co major merge", K(tablet_id), K(co_sstable), K(tables), K(major_sstable_status), K(major_merge_type));
  } else if (OB_FAIL(estimate_row_cnt_for_major_merge(tablet_id.id(), tables, storage_schema, tablet_handle, estimate_row_cnt))) {
    // if estimate row cnt failed, make major sstable match schema
    major_merge_type = is_major_sstable_match_schema(major_sstable_status) ? BUILD_COLUMN_STORE_MERGE : REBUILD_COLUMN_STORE_MERGE;
    LOG_WARN("failed to estimate row count for co major merge, build column store by default", "estimate_ret", ret, K(major_sstable_status), K(major_merge_type));
    ret = OB_SUCCESS;
  } else {
    int64_t column_cnt = storage_schema.get_column_count();
    bool build_row_store_flag = whether_to_build_row_store(estimate_row_cnt, column_cnt);
    if (is_major_sstable_match_schema(major_sstable_status)) {
      if (build_row_store_flag) {
        major_merge_type = BUILD_ROW_STORE_MERGE;
      } else {
        major_merge_type = BUILD_COLUMN_STORE_MERGE;
      }
    } else if (!build_row_store_flag && whether_to_rebuild_column_store(major_sstable_status, estimate_row_cnt, column_cnt)) {
      major_merge_type = REBUILD_COLUMN_STORE_MERGE;
    } else {
      major_merge_type = BUILD_ROW_STORE_MERGE;
    }
    LOG_INFO("[RowColSwitch] finish decide major merge type", K(tablet_id), K(co_sstable), K(tables), K(major_sstable_status), K(major_merge_type), K(estimate_row_cnt), K(column_cnt));
  }
  return ret;
}


} /* namespace compaction */
} /* namespace oceanbase */
