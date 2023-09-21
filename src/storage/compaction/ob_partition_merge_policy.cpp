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
#include "share/scn.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_medium_compaction_func.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;
using namespace blocksstable;

namespace oceanbase
{
namespace compaction
{

// keep order with ObMergeType
ObPartitionMergePolicy::GetMergeTables ObPartitionMergePolicy::get_merge_tables[MERGE_TYPE_MAX]
  = { ObPartitionMergePolicy::get_minor_merge_tables,
      ObPartitionMergePolicy::get_hist_minor_merge_tables,
      ObAdaptiveMergePolicy::get_meta_merge_tables,
      ObPartitionMergePolicy::get_mini_merge_tables,
      ObPartitionMergePolicy::get_medium_merge_tables,
      ObPartitionMergePolicy::get_medium_merge_tables,
    };


int ObPartitionMergePolicy::get_neighbour_freeze_info(
    const int64_t snapshot_version,
    const ObITable *last_major,
    ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_neighbour_major_freeze(snapshot_version, freeze_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WARN("Failed to get freeze info, use snapshot_gc_ts instead", K(ret), K(snapshot_version));
      ret = OB_SUCCESS;
      freeze_info.reset();
      freeze_info.next.freeze_version = INT64_MAX;
      if (OB_NOT_NULL(last_major)) {
        freeze_info.prev.freeze_version = last_major->get_snapshot_version();
      }
    } else {
      LOG_WARN("Failed to get neighbour major freeze info", K(ret), K(snapshot_version));
    }
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
  result.suggest_merge_type_ = param.merge_type_;
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
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(base_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null base table", K(ret), K(tablet));
  } else {
    result.version_range_.base_version_ = base_table->get_upper_trans_version();
    result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
    result.version_range_.snapshot_version_ = param.merge_version_;
    ObSSTableMetaHandle sstable_meta_hdl;
    if (OB_FAIL(get_multi_version_start(param.merge_type_, ls, tablet, result.version_range_))) {
      LOG_WARN("failed to get multi version_start", K(ret));
    } else if (OB_FAIL(base_table->get_meta(sstable_meta_hdl))) {
      LOG_WARN("failed to get base table meta", K(ret));
    } else {
      result.read_base_version_ = base_table->get_snapshot_version();
      result.create_snapshot_version_ = sstable_meta_hdl.get_sstable_meta().get_basic_meta().create_snapshot_version_;
    }
  }
  return ret;
}

bool ObPartitionMergePolicy::is_sstable_count_not_safe(const int64_t minor_table_cnt)
{
  return minor_table_cnt + 1 /*major_sstable*/ >= MAX_SSTABLE_CNT_IN_STORAGE;
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
  result.reset();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSEArray<ObTableHandleV2, MAX_MEMSTORE_CNT> memtable_handles;
  DEBUG_SYNC(BEFORE_GET_MINOR_MGERGE_TABLES);
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(MINI_MERGE != merge_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(merge_type));
  } else if (OB_UNLIKELY(nullptr == tablet.get_memtable_mgr() || !table_store_wrapper.get_member()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null memtable mgr from tablet or invalid table store", K(ret), K(tablet), K(table_store_wrapper));
  } else if (is_sstable_count_not_safe(table_store_wrapper.get_member()->get_minor_sstables().count())) {
    ret = OB_SIZE_OVERFLOW;
    LOG_ERROR("Too many sstables, delay mini merge until sstable count falls below MAX_SSTABLE_CNT",
              K(ret), K(PRINT_TS_WRAPPER(table_store_wrapper)), K(tablet));
    // add compaction diagnose info
    ObPartitionMergePolicy::diagnose_table_count_unsafe(MINI_MERGE, tablet);
  } else if (OB_FAIL(tablet.get_memtable_mgr()->get_all_memtables(memtable_handles))) {
    LOG_WARN("failed to get all memtables from memtable mgr", K(ret));
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version,
                                               table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true),
                                               freeze_info))) {
    LOG_WARN("failed to get next major freeze", K(ret), K(merge_inc_base_version), K(PRINT_TS_WRAPPER(table_store_wrapper)));
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
  int64_t max_snapshot_version = freeze_info.next.freeze_version;
  const SCN &clog_checkpoint_scn = tablet.get_clog_checkpoint_scn();

  // Freezing in the restart phase may not satisfy end >= last_max_sstable,
  // so the memtable cannot be filtered by scn
  // can only take out all frozen memtable
  ObIMemtable *memtable = nullptr;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  bool has_release_memtable = false;

  for (int64_t i = 0; OB_SUCC(ret) && i < memtable_handles.count(); ++i) {
    if (OB_ISNULL(memtable = static_cast<ObIMemtable *>(memtable_handles.at(i).get_table()))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable must not null", K(ret), K(tablet));
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

  if (OB_SUCC(ret)) {
    bool need_check_tablet = false;
    result.suggest_merge_type_ = param.merge_type_;
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
    LOG_INFO("no need to minor merge", K(ret), K(merge_type), K(result));
  } else if (OB_UNLIKELY(!result.scn_range_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid argument to check result", K(ret), K(result));
  } else if (OB_FAIL(result.handle_.check_continues(&result.scn_range_))) {
    LOG_WARN("failed to check continues", K(ret), K(result));
  } else if (OB_FAIL(get_multi_version_start(merge_type, ls, tablet, result.version_range_))) {
    LOG_WARN("failed to get kept multi_version_start", K(ret), K(merge_type), K(tablet));
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
    LOG_WARN("get invalid arguments", K(ret), K(merge_type));
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
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObITable *last_major_table = nullptr;

  if (OB_UNLIKELY(tablet.is_ls_inner_tablet())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for special tablet", K(ret), K(tablet));
  } else if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(table_store_wrapper));
  } else if (FALSE_IT(last_major_table = table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true))) {
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version,
                                               last_major_table,
                                               freeze_info))) {
    LOG_WARN("failed to get freeze info", K(ret), K(merge_inc_base_version), K(PRINT_TS_WRAPPER(table_store_wrapper)));
  } else if (check_table_cnt && table_store_wrapper.get_member()->get_table_count() >= OB_UNSAFE_TABLE_CNT) {
    max_snapshot = INT64_MAX;
    if (table_store_wrapper.get_member()->get_table_count() >= OB_EMERGENCY_TABLE_CNT) {
      min_snapshot = 0;
    } else if (OB_NOT_NULL(last_major_table)) {
      min_snapshot = last_major_table->get_snapshot_version();
    }
  } else {
    if (OB_NOT_NULL(last_major_table)) {
      min_snapshot = max(last_major_table->get_snapshot_version(), freeze_info.prev.freeze_version);
    } else {
      min_snapshot = freeze_info.prev.freeze_version;
    }
    if (INT64_MAX == freeze_info.next.freeze_version && is_multi_version_merge) {
      max_snapshot = MTL(ObTenantFreezeInfoMgr*)->get_snapshot_gc_ts();
    } else {
      max_snapshot = freeze_info.next.freeze_version;
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
    LOG_DEBUG("get boundary snapshot", K(ret), "tablet_id", tablet.get_tablet_meta().tablet_id_, K(table_store_wrapper),
              K(min_snapshot), K(max_snapshot), K(max_medium_scn), KPC(last_major_table), K(freeze_info));
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
  result.reset_handle_and_range();
  ObTableStoreIterator minor_table_iter;
  int64_t minor_compact_trigger = DEFAULT_MINOR_COMPACT_TRIGGER;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("unexpected table store", K(ret), K(table_store_wrapper));
  } else if (OB_FAIL(tablet.get_mini_minor_sstables(minor_table_iter))) {
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
        } else if (table_store_wrapper.get_member()->get_table_count() < OB_UNSAFE_TABLE_CNT &&
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
      if (OB_FAIL(result.handle_.add_table(tmp_table_handle))) {
        LOG_WARN("Failed to add table", K(ret), KPC(table));
      } else {
        if (1 == result.handle_.get_count()) {
          result.scn_range_.start_scn_ = tmp_table_handle.get_table()->get_start_scn();
        }
        result.scn_range_.end_scn_ = tmp_table_handle.get_table()->get_end_scn();
      }
    }
  }

  if (OB_SUCC(ret)) {
    result.suggest_merge_type_ = param.merge_type_;
    if (OB_FAIL(refine_minor_merge_result(minor_compact_trigger, result))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("failed to refine_minor_merge_result", K(ret));
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
  } else if (OB_NO_NEED_MERGE == ret
      && table_store_wrapper.get_member()->get_minor_sstables().count() >= DIAGNOSE_TABLE_CNT_IN_STORAGE) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MINOR_MERGE,
                    tablet.get_tablet_meta().ls_id_,
                    tablet.get_tablet_meta().tablet_id_,
                    ObSuspectInfoType::SUSPECT_CANT_SCHEDULE_MINOR_MERGE,
                    min_snapshot_version, max_snapshot_version, table_store_wrapper.get_member()->get_minor_sstables().count()))) {
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
    LOG_WARN("invalid args", K(ret), K(merge_type));
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
    ObTenantFreezeInfoMgr::NeighbourFreezeInfo freeze_info;
    ObSEArray<ObTenantFreezeInfoMgr::FreezeInfo, 8> freeze_infos;
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
    const storage::ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t buf_len = ObScheduleSuspectInfoMgr::EXTRA_INFO_LEN;
  char tmp_str[buf_len] = "\0";
  if (OB_TMP_FAIL(ObCompactionDiagnoseMgr::check_system_compaction_config(tmp_str, buf_len))) {
    LOG_WARN("failed to check system compaction config", K(tmp_ret), K(tmp_str));
  }

  // check min_reserved_snapshot
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  int64_t min_reserved_snapshot = 0;
  int64_t min_merged_snapshot = INT64_MAX;
  int64_t first_minor_start_scn = 0;
  ObString snapshot_from_str;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &major_tables = table_store_wrapper.get_member()->get_major_sstables();
    const ObSSTableArray &minor_tables = table_store_wrapper.get_member()->get_minor_sstables();
    // add sstable info
    const int64_t major_table_count = major_tables.count();
    const int64_t minor_table_count = minor_tables.count();
    ADD_COMPACTION_INFO_PARAM(tmp_str, buf_len, K(major_table_count), K(minor_table_count));
    const ObSSTable *major_sstable = static_cast<const ObSSTable *>(major_tables.get_boundary_table(false/*last*/));
    if (OB_ISNULL(major_sstable)) {
      const ObSSTable *minor_sstable = static_cast<const ObSSTable *>(minor_tables.get_boundary_table(false/*last*/));
      first_minor_start_scn = minor_sstable->get_start_scn().get_val_for_tx();
    } else if (FALSE_IT(min_merged_snapshot = major_sstable->get_snapshot_version())) {
    } else if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(diagnose_min_reserved_snapshot,
        tablet_id,
        min_merged_snapshot,
        min_reserved_snapshot,
        snapshot_from_str))) {
      LOG_WARN("failed to get min reserved snapshot", K(ret), K(tablet_id));
    } else if (snapshot_from_str.length() > 0) {
      ADD_COMPACTION_INFO_PARAM(tmp_str, buf_len, "snapstho_type", snapshot_from_str, K(min_reserved_snapshot));
    }

    // check have minor merge DAG
    if (OB_TMP_FAIL(diagnose_minor_dag(MINOR_MERGE, ls_id, tablet_id, tmp_str, buf_len))) {
      LOG_WARN("failed to diagnose minor dag", K(tmp_ret), K(ls_id), K(tablet_id), K(tmp_str));
    }
    if (OB_TMP_FAIL(diagnose_minor_dag(HISTORY_MINOR_MERGE, ls_id, tablet_id, tmp_str, buf_len))) {
      LOG_WARN("failed to diagnose history minor dag", K(tmp_ret), K(ls_id), K(tablet_id), K(tmp_str));
    }

    if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MINI_MERGE, ls_id, tablet_id, ObSuspectInfoType::SUSPECT_SSTABLE_COUNT_NOT_SAFE,
        major_table_count, minor_table_count, first_minor_start_scn, "extra_info", tmp_str))) {
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
    const int64_t minor_compact_trigger,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObMergeType &merge_type = result.suggest_merge_type_;
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
      ObSSTableMetaHandle sstable_meta_hdl;
      ObTableHandleV2 tmp_table_handle;
      if (OB_FAIL(result.handle_.get_table(i, tmp_table_handle))) {
        LOG_WARN("failed to get table from handles array", K(ret), K(i));
      } else if (OB_ISNULL(table = tmp_table_handle.get_table()) || !table->is_minor_sstable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("get unexpected table", KP(table), K(ret));
      } else if (FALSE_IT(sstable = reinterpret_cast<ObSSTable*>(table))) {
      } else if (OB_FAIL(sstable->get_meta(sstable_meta_hdl))) {
        LOG_WARN("failed to get sstable meta handle", K(ret));
      } else {
        if (sstable_meta_hdl.get_sstable_meta().get_basic_meta().row_count_ > OB_LARGE_MINOR_SSTABLE_ROW_COUNT) { // large sstable
          ++large_sstable_cnt;
          large_sstable_row_cnt += sstable_meta_hdl.get_sstable_meta().get_basic_meta().row_count_;
          if (mini_tables.get_count() > minor_compact_trigger) {
            break;
          } else {
            mini_tables.reset();
            continue;
          }
        } else {
          mini_sstable_row_cnt += sstable_meta_hdl.get_sstable_meta().get_basic_meta().row_count_;
        }
        if (OB_FAIL(mini_tables.add_table(tmp_table_handle))) {
          LOG_WARN("Failed to push mini minor table into array", K(ret));
        }
      }
    } // end of for

    int64_t size_amplification_factor = OB_DEFAULT_COMPACTION_AMPLIFICATION_FACTOR;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        size_amplification_factor = tenant_config->_minor_compaction_amplification_factor;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (large_sstable_cnt > 1
        || mini_tables.get_count() <= minor_compact_trigger
        || mini_sstable_row_cnt > (large_sstable_row_cnt * size_amplification_factor / 100)) {
      // no refine, use current result to compaction
    } else if (mini_tables.get_count() != result.handle_.get_count()) {
      // reset the merge result, mini sstable merge into a new mini sstable
      result.reset_handle_and_range();
      for (int64_t i = 0; OB_SUCC(ret) && i < mini_tables.get_count(); i++) {
        ObTableHandleV2 tmp_table_handle;
        if (OB_FAIL(result.handle_.get_table(i, tmp_table_handle))) {
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
  need_merge = false;
  can_merge = false;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObITable *last_major = nullptr;
  const bool is_tablet_data_status_complete = tablet.is_data_complete();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_ISNULL(last_major =
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/))) {
    // no major, no medium
  } else {
    need_merge = last_major->get_snapshot_version() < medium_snapshot;
    if (need_merge
        && is_tablet_data_status_complete
        && ObTenantTabletScheduler::check_weak_read_ts_ready(medium_snapshot, ls)) {
      can_merge = tablet.get_snapshot_version() >= medium_snapshot;
      if (!can_merge) {
        ObTabletMemtableMgr *memtable_mgr = nullptr;
        ObTableHandleV2 memtable_handle;
        memtable::ObMemtable *last_frozen_memtable = nullptr;
        if (OB_ISNULL(memtable_mgr = static_cast<ObTabletMemtableMgr *>(tablet.get_memtable_mgr()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable mgr is unexpected null", K(ret), K(tablet));
        } else if (OB_FAIL(memtable_mgr->get_last_frozen_memtable(memtable_handle))) {
          if (OB_ENTRY_NOT_EXIST == ret) { // no frozen memtable, need force freeze
            need_force_freeze = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get last frozen memtable", K(ret));
          }
        } else if (OB_FAIL(memtable_handle.get_data_memtable(last_frozen_memtable))) {
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

  if (need_merge && !can_merge && REACH_TENANT_TIME_INTERVAL(60L * 1000L * 1000L)) {
    LOG_INFO("check_need_medium_merge", K(ret), "ls_id", tablet.get_tablet_meta().ls_id_, K(tablet_id),
             K(need_merge), K(can_merge), K(medium_snapshot), K(is_tablet_data_status_complete));
   int tmp_ret = OB_SUCCESS;
   if (OB_TMP_FAIL(ADD_SUSPECT_INFO(MAJOR_MERGE,
                   tablet.get_tablet_meta().ls_id_,
                   tablet_id,
                   ObSuspectInfoType::SUSPECT_CANT_MAJOR_MERGE,
                   medium_snapshot, static_cast<int64_t>(is_tablet_data_status_complete),
                   static_cast<int64_t>(need_force_freeze)))) {
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
    ObVersionRange &result_version_range)
{
  int ret = OB_SUCCESS;
  int64_t expect_multi_version_start = 0;
  if (tablet.is_ls_inner_tablet()) {
    result_version_range.multi_version_start_ = INT64_MAX;
  } else if (OB_FAIL(ObTablet::get_kept_multi_version_start(ls, tablet, expect_multi_version_start))) {
    if (is_mini_merge(merge_type) || OB_TENANT_NOT_EXIST == ret) {
      expect_multi_version_start = tablet.get_multi_version_start();
      FLOG_INFO("failed to get multi_version_start, use multi_version_start on tablet", K(ret),
          K(merge_type), K(expect_multi_version_start));
      ret = OB_SUCCESS; // clear errno
    } else {
      LOG_WARN("failed to get kept multi_version_start", K(ret),
          "tablet_id", tablet.get_tablet_meta().tablet_id_);
    }
  }
  if (OB_SUCC(ret) && !tablet.is_ls_inner_tablet()) {
    // update multi_version_start
    if (expect_multi_version_start < result_version_range.multi_version_start_) {
      LOG_WARN("cannot reserve multi_version_start", "multi_version_start", result_version_range.multi_version_start_,
               K(expect_multi_version_start));
    } else if (expect_multi_version_start < result_version_range.snapshot_version_) {
      result_version_range.multi_version_start_ = expect_multi_version_start;
      LOG_DEBUG("succ reserve multi_version_start", "multi_version_start", result_version_range.multi_version_start_,
                K(expect_multi_version_start));
    } else {
      result_version_range.multi_version_start_ = result_version_range.snapshot_version_;
      LOG_DEBUG("no need keep multi version", "multi_version_start", result_version_range.multi_version_start_,
                K(expect_multi_version_start));
    }
  }
  return ret;
}


int add_table_with_check(ObGetMergeTablesResult &result, ObTableHandleV2 &table_handle)
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
    }

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
    const int64_t minor_compact_trigger,
    const ObGetMergeTablesResult &input_result,
    ObMinorExecuteRangeMgr &minor_range_mgr,
    ObIArray<ObGetMergeTablesResult> &parallel_result)
{
  int ret = OB_SUCCESS;
  parallel_result.reset();
  ObSEArray<ObGetMergeTablesResult, 2> input_result_array;
  int64_t fixed_input_table_cnt = 0;

  if (!storage::is_minor_merge(input_result.suggest_merge_type_)) {
    ret = OB_NO_NEED_MERGE;
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
  param.for_diagnose_ = true;

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
  std::sort(exe_range_array_.begin(), exe_range_array_.end(), compareScnRange);
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
  "TENANT_MAJOR"
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

int ObAdaptiveMergePolicy::get_meta_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = param.merge_type_;
  result.reset();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(table_store_wrapper));
  } else if (OB_UNLIKELY(META_MAJOR_MERGE != merge_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(merge_type));
  } else if (OB_FAIL(find_meta_major_tables(tablet, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("Failed to find minor merge tables", K(ret));
    }
  } else if (OB_FAIL(result.handle_.check_continues(nullptr))) {
    LOG_WARN("failed to check continues", K(ret), K(result));
  } else if (FALSE_IT(result.suggest_merge_type_ = META_MAJOR_MERGE)) {
  } else if (FALSE_IT(result.version_range_.snapshot_version_ =
      MIN(tablet.get_snapshot_version(), result.version_range_.snapshot_version_))) {
    // chosen version should less than tablet::snapshot
  } else if (OB_FAIL(ObPartitionMergePolicy::get_multi_version_start(
      param.merge_type_, ls, tablet, result.version_range_))) {
    LOG_WARN("failed to get multi version_start", K(ret));
  } else {
    FLOG_INFO("succeed to get meta major merge tables", K(result), K(PRINT_TS_WRAPPER(table_store_wrapper)));
  }
  return ret;
}

int ObAdaptiveMergePolicy::find_meta_major_tables(
    const storage::ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  int64_t min_snapshot = 0;
  int64_t max_snapshot = 0;
  int64_t base_row_cnt = 0;
  int64_t inc_row_cnt = 0;
  int64_t tx_determ_table_cnt = 0;
  ObSSTableMetaHandle meta_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(NULL == (table_store = table_store_wrapper.get_member()) || !table_store->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletTableStore is not valid", K(ret), K(table_store_wrapper));
  } else {
    ObITable *last_major = table_store->get_major_sstables().get_boundary_table(true);
    ObITable *base_table = nullptr;
    const ObSSTableArray &minor_tables = table_store->get_minor_sstables();
    if (minor_tables.empty() || nullptr == last_major) {
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("no minor/major sstable to do meta major merge", K(ret), K(minor_tables), KPC(last_major));
    } else if (FALSE_IT(base_table = nullptr == table_store->get_meta_major_sstable()
                                   ? last_major
                                   : table_store->get_meta_major_sstable())) {
    } else if (OB_FAIL(ObPartitionMergePolicy::get_boundary_snapshot_version(
          tablet, min_snapshot, max_snapshot, false/*check_table_cnt*/, false/*is_multi_version_merge*/))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("Failed to find meta merge base table", K(ret), KPC(last_major), KPC(base_table));
      }
    } else if (base_table->get_snapshot_version() < min_snapshot || max_snapshot != INT64_MAX) {
      // max_snapshot == INT64_MAX means there's no next freeze_info
      ret = OB_NO_NEED_MERGE;
      LOG_DEBUG("no need meta merge when the tablet is doing major merge", K(ret), K(min_snapshot), K(max_snapshot), KPC(base_table));
    } else if (OB_FAIL(add_meta_merge_result(base_table, table_store_wrapper.get_meta_handle(), result, true/*update_snapshot*/))) {
      LOG_WARN("failed to add base table to meta merge result", K(ret), KPC(base_table), K(result));
    } else if (OB_FAIL(static_cast<ObSSTable *>(base_table)->get_meta(meta_handle))) {
      LOG_WARN("failed to base table meta", K(ret), KPC(base_table));
    } else {
      base_row_cnt = meta_handle.get_sstable_meta().get_row_count();
      ++tx_determ_table_cnt; // inc for base_table
    }

    bool found_undeterm_table = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
      ObITable *table = minor_tables[i];
      if (OB_UNLIKELY(NULL == table || !table->is_multi_version_minor_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected table", K(ret), K(i), K(PRINT_TS_WRAPPER(table_store_wrapper)));
      } else if (result.handle_.get_count() <= 1 && table->get_upper_trans_version() <= base_table->get_snapshot_version()) {
        continue; // skip minor sstable which has been merged
      } else if (!found_undeterm_table && table->is_trans_state_deterministic()) {
        ++tx_determ_table_cnt;
        ObSSTableMetaHandle inc_handle;
        if (OB_FAIL(static_cast<ObSSTable *>(table)->get_meta(inc_handle))) {
          LOG_WARN("failed to inc table meta", K(ret), KPC(table));
        } else {
          inc_row_cnt += inc_handle.get_sstable_meta().get_row_count();
        }
      } else {
        found_undeterm_table = true;
      }

      if (FAILEDx(add_meta_merge_result(
          table, table_store_wrapper.get_meta_handle(), result, !found_undeterm_table))) {
        LOG_WARN("failed to add minor table to meta merge result", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (tx_determ_table_cnt < 2) {
      ret = OB_NO_NEED_MERGE;
      if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000/*60s*/)) {
        LOG_INFO("no enough table for meta merge", K(ret), K(result), K(PRINT_TS_WRAPPER(table_store_wrapper)));
      }
    } else if (inc_row_cnt < TRANS_STATE_DETERM_ROW_CNT_THRESHOLD
            || inc_row_cnt < INC_ROW_COUNT_PERCENTAGE_THRESHOLD * base_row_cnt) {
      ret = OB_NO_NEED_MERGE;
      if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000/*60s*/)) {
        LOG_INFO("found sstable could merge is not enough", K(ret), K(inc_row_cnt), K(base_row_cnt));
      }
    } else if (result.version_range_.snapshot_version_ < tablet.get_multi_version_start()) {
      ret = OB_NO_NEED_MERGE;
      if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000/*60s*/)) {
        LOG_INFO("chosen snapshot is abandoned", K(ret), K(result), K(tablet.get_multi_version_start()));
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

int ObAdaptiveMergePolicy::find_base_table_and_inc_version(
    ObITable *last_major_table,
    ObITable *last_minor_table,
    ObITable *&meta_base_table,
    int64_t &merge_inc_version)
{
  int ret = OB_SUCCESS;
  // find meta base table
  if (OB_NOT_NULL(last_major_table)) {
    if (OB_ISNULL(meta_base_table)) {
      meta_base_table = last_major_table;
    } else if (OB_UNLIKELY(meta_base_table->get_snapshot_version() <= last_major_table->get_snapshot_version())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta major table covered by major", K(ret), KPC(meta_base_table), KPC(last_major_table));
    }
  }

  // find meta merge inc version
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(last_major_table) && OB_NOT_NULL(last_minor_table)) {
    merge_inc_version = MAX(last_major_table->get_snapshot_version(), last_minor_table->get_max_merged_trans_version());
  } else if (OB_NOT_NULL(last_major_table)) {
    merge_inc_version = last_major_table->get_snapshot_version();
  } else if (OB_NOT_NULL(last_minor_table)){
    merge_inc_version = last_minor_table->get_max_merged_trans_version();
  }

  if (OB_SUCC(ret) && (NULL == meta_base_table || merge_inc_version <= 0)) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("cannot meta merge with null base table or inc version", K(ret), K(meta_base_table), K(merge_inc_version));
  }
  return ret;
}

int ObAdaptiveMergePolicy::add_meta_merge_result(
    ObITable *table,
    const ObStorageMetaHandle &table_meta_handle,
    ObGetMergeTablesResult &result,
    const bool update_snapshot_flag)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;

  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(table));
  } else if (OB_FAIL(result.handle_.add_sstable(table, table_meta_handle))) {
    LOG_WARN("failed to add table", K(ret), KPC(table));
  } else if (table->is_meta_major_sstable() || table->is_major_sstable()) {
    if (OB_FAIL(static_cast<ObSSTable *>(table)->get_meta(meta_handle))) {
      LOG_WARN("faile to get sstable meta", K(ret), KPC(table));
    } else {
      result.version_range_.base_version_ = 0;
      result.version_range_.multi_version_start_ = table->get_snapshot_version();
      result.version_range_.snapshot_version_ = table->get_snapshot_version();
      result.create_snapshot_version_ = meta_handle.get_sstable_meta().get_basic_meta().create_snapshot_version_;
    }
  } else if (update_snapshot_flag) {
    int64_t max_snapshot = MAX(result.version_range_.snapshot_version_, table->get_max_merged_trans_version());
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
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;

  reason = AdaptiveMergeReason::NONE;
  ObTabletStatAnalyzer tablet_analyzer;

  if (tablet_id.is_special_merge_tablet()) {
    // do nothing
  } else if (OB_FAIL(MTL(ObTenantTabletStatMgr *)->get_tablet_analyzer(ls_id, tablet_id, tablet_analyzer))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get tablet analyzer stat", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_TMP_FAIL(check_inc_sstable_row_cnt_percentage(tablet, reason))) {
      LOG_WARN("failed to check sstable data situation", K(tmp_ret), K(ls_id), K(tablet_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    if (OB_TMP_FAIL(check_tombstone_situation(tablet_analyzer, tablet, reason))) {
      LOG_WARN("failed to check tombstone scene", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
    }
    if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_load_data_situation(tablet_analyzer, tablet, reason))) {
      LOG_WARN("failed to check load data scene", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
    }
    if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_inc_sstable_row_cnt_percentage(tablet, reason))) {
      LOG_WARN("failed to check sstable data situation", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
    }
    if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_ineffecient_read(tablet_analyzer, tablet, reason))) {
      LOG_WARN("failed to check ineffecient read", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
    }
  }

  if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000 /*10s*/)) {
    LOG_INFO("Check tablet adaptive merge reason", K(ret), K(ls_id), K(tablet_id),  K(reason), K(tablet_analyzer));
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
  ObSSTableMetaHandle major_meta_hdl;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (FALSE_IT(last_major = static_cast<ObSSTable *>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true)))) {
  } else if (nullptr != last_major && OB_FAIL(last_major->get_meta(major_meta_hdl))) {
    LOG_WARN("fail to get major meta", K(ret), KPC(last_major));
  } else {
    base_row_count = major_meta_hdl.get_sstable_meta().get_row_count();
    const ObSSTableArray &minor_sstables = table_store_wrapper.get_member()->get_minor_sstables();
    for (int i = 0; OB_SUCC(ret) && i < minor_sstables.count(); ++i) {
      ObSSTableMetaHandle inc_meta_hdl;
      if (OB_ISNULL(sstable = minor_sstables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable is null", K(ret), K(i));
      } else if (OB_FAIL(sstable->get_meta(inc_meta_hdl))) {
        LOG_WARN("fail to get sstable meta", K(ret), KPC(sstable));
      } else {
        inc_row_count += inc_meta_hdl.get_sstable_meta().get_row_count();
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
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  reason = AdaptiveMergeReason::NONE;

  if (OB_UNLIKELY(!tablet.is_valid() || !analyzer.tablet_stat_.is_valid()
      || ls_id.id() != analyzer.tablet_stat_.ls_id_ || tablet_id.id() != analyzer.tablet_stat_.tablet_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(tablet), K(analyzer));
  } else if (analyzer.is_hot_tablet() && analyzer.is_insert_mostly()) {
    reason = AdaptiveMergeReason::LOAD_DATA_SCENE;
  }
  LOG_DEBUG("check_load_data_situation", K(ret), K(ls_id), K(tablet_id), K(reason), K(analyzer));
  return ret;
}

int ObAdaptiveMergePolicy::check_tombstone_situation(
    const storage::ObTabletStatAnalyzer &analyzer,
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  reason = AdaptiveMergeReason::NONE;

  if (OB_UNLIKELY(!tablet.is_valid() || !analyzer.tablet_stat_.is_valid()
      || ls_id.id() != analyzer.tablet_stat_.ls_id_ || tablet_id.id() != analyzer.tablet_stat_.tablet_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(analyzer), K(tablet));
  } else if (analyzer.tablet_stat_.merge_cnt_ > 1 && analyzer.is_update_or_delete_mostly()) {
    reason = AdaptiveMergeReason::TOMBSTONE_SCENE;
  }
  LOG_DEBUG("check_tombstone_situation", K(ret), K(ls_id), K(tablet_id), K(reason), K(analyzer));
  return ret;
}

int ObAdaptiveMergePolicy::check_ineffecient_read(
    const storage::ObTabletStatAnalyzer &analyzer,
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  reason = AdaptiveMergeReason::NONE;

  if (OB_UNLIKELY(!tablet.is_valid() || !analyzer.tablet_stat_.is_valid()
      || ls_id.id() != analyzer.tablet_stat_.ls_id_ || tablet_id.id() != analyzer.tablet_stat_.tablet_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(tablet), K(analyzer));
  } else if (analyzer.is_hot_tablet() && analyzer.has_slow_query()) {
    reason = AdaptiveMergeReason::INEFFICIENT_QUERY;
  }
  LOG_DEBUG("check_ineffecient_read", K(ret), K(ls_id), K(tablet_id), K(reason), K(analyzer));
  return ret;
}


} /* namespace compaction */
} /* namespace oceanbase */
