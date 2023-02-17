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
#include "ob_partition_merge_policy.h"
#include "share/ob_debug_sync_point.h"
#include "share/ob_debug_sync.h"
#include "share/ob_force_print_log.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/ob_storage_schema.h"
#include "storage/ob_storage_struct.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "observer/omt/ob_tenant_config_mgr.h"

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
  = { ObPartitionMergePolicy::get_mini_minor_merge_tables,
      ObPartitionMergePolicy::get_buf_minor_merge_tables,
      ObPartitionMergePolicy::get_hist_minor_merge_tables,
      ObPartitionMergePolicy::get_mini_merge_tables,
      ObPartitionMergePolicy::get_major_merge_tables,
      ObPartitionMergePolicy::get_mini_minor_merge_tables
    };

ObPartitionMergePolicy::CheckNeedMerge ObPartitionMergePolicy::check_need_minor_merge[MERGE_TYPE_MAX]
  = { ObPartitionMergePolicy::check_need_mini_minor_merge,
      ObPartitionMergePolicy::check_need_buf_minor_merge,
      ObPartitionMergePolicy::check_need_hist_minor_merge,
      ObPartitionMergePolicy::check_need_mini_merge};

int ObPartitionMergePolicy::get_neighbour_freeze_info(
    const int64_t snapshot_version,
    const ObITable *last_major,
    ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr *freeze_info_mgr = nullptr;
  if (OB_ISNULL(freeze_info_mgr = MTL(ObTenantFreezeInfoMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null freeze_info_mgr", K(ret));
  } else if (OB_SUCC(freeze_info_mgr->get_neighbour_major_freeze(snapshot_version, freeze_info))) {
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    LOG_WARN("Failed to get freeze info, use snapshot_gc_ts instead", K(ret), K(snapshot_version));
    ret = OB_SUCCESS;
    freeze_info.reset();
    freeze_info.next.freeze_ts = INT64_MAX;
    if (OB_NOT_NULL(last_major)) {
      freeze_info.prev.freeze_ts = last_major->get_snapshot_version();
    }
  } else {
    LOG_WARN("Failed to get neighbour major freeze info", K(ret), K(snapshot_version));
  }
  return ret;
}

int ObPartitionMergePolicy::get_mini_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t multi_version_start,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObTenantFreezeInfoMgr::NeighbourFreezeInfo freeze_info;
  int64_t merge_inc_base_version = tablet.get_snapshot_version();
  const ObMergeType merge_type = param.merge_type_;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObSEArray<ObTableHandleV2, MAX_MEMSTORE_CNT> memtable_handles;
  result.reset();
  DEBUG_SYNC(BEFORE_GET_MINOR_MGERGE_TABLES);

  if (MINI_MERGE != merge_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(merge_type));
  } else if (OB_UNLIKELY(nullptr == tablet.get_memtable_mgr() || !table_store.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null memtable mgr from tablet or invalid table store", K(ret), K(tablet), K(table_store));
  } else if (table_store.get_minor_sstables().count() >= MAX_SSTABLE_CNT_IN_STORAGE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_ERROR("Too many sstables, delay mini merge until sstable count falls below MAX_SSTABLE_CNT",
              K(ret), K(table_store), K(tablet));
    // add compaction diagnose info
    diagnose_table_count_unsafe(MINI_MERGE, tablet);
  } else if (OB_FAIL(tablet.get_memtable_mgr()->get_all_memtables(memtable_handles))) {
    LOG_WARN("failed to get all memtables from memtable mgr", K(ret));
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version,
                                               table_store.get_major_sstables().get_boundary_table(true),
                                               freeze_info))) {
    LOG_WARN("failed to get next major freeze", K(ret), K(merge_inc_base_version), K(table_store));
  } else if (OB_FAIL(find_mini_merge_tables(param, freeze_info, tablet, memtable_handles, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to find mini merge tables", K(ret), K(freeze_info));
    }
  } else if (!result.update_tablet_directly_
      && OB_FAIL(deal_with_minor_result(merge_type, multi_version_start, tablet, result))) {
    LOG_WARN("failed to deal with minor merge result", K(ret));
  }
  return ret;
}

int ObPartitionMergePolicy::find_mini_merge_tables(
    const ObGetMergeTablesParam &param,
    const ObTenantFreezeInfoMgr::NeighbourFreezeInfo &freeze_info,
    const storage::ObTablet &tablet,
    ObIArray<ObTableHandleV2> &memtable_handles,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  // TODO: @dengzhi.ldz, remove max_snapshot_version, merge all forzen memtables
  // Keep max_snapshot_version currently because major merge must be done step by step
  int64_t max_snapshot_version = freeze_info.next.freeze_ts;
  const int64_t clog_checkpoint_ts = tablet.get_clog_checkpoint_ts();

  // Freezing in the restart phase may not satisfy end >= last_max_sstable,
  // so the memtable cannot be filtered by log_ts
  // can only take out all frozen memtable
  ObIMemtable *memtable = nullptr;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  bool need_update_snapshot_version = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < memtable_handles.count(); ++i) {
    if (OB_ISNULL(memtable = static_cast<ObIMemtable *>(memtable_handles.at(i).get_table()))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("memtable must not null", K(ret), K(tablet));
    } else if (OB_UNLIKELY(memtable->is_active_memtable())) {
      LOG_INFO("skip active memtable", K(i), KPC(memtable), K(memtable_handles));
      break;
    } else if (!memtable->can_be_minor_merged()) {
      FLOG_INFO("memtable cannot mini merge now", K(ret), K(i), KPC(memtable), K(max_snapshot_version), K(memtable_handles), K(param));
      break;
    } else if (memtable->get_end_log_ts() <= clog_checkpoint_ts) {
      if (!tablet_id.is_special_merge_tablet() &&
          memtable->get_snapshot_version() > tablet.get_tablet_meta().snapshot_version_) {
        need_update_snapshot_version = true;
      } else {
        LOG_DEBUG("memtable wait to release", K(param), KPC(memtable));
        continue;
      }
    } else if (result.handle_.get_count() > 0) {
      if (result.log_ts_range_.end_log_ts_ < memtable->get_start_log_ts()) {
        FLOG_INFO("log ts range not continues, reset previous minor merge tables",
                  "last_end_log_ts", result.log_ts_range_.end_log_ts_, KPC(memtable), K(tablet));
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

    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.handle_.add_table(memtable))) {
        LOG_WARN("Failed to add memtable", KPC(memtable), K(ret));
      } else {
        // update end_log_ts/snapshot_version
        if (1 == result.handle_.get_count()) {
          result.log_ts_range_.start_log_ts_ = memtable->get_start_log_ts();
        }
        result.log_ts_range_.end_log_ts_ = memtable->get_end_log_ts();
        result.version_range_.snapshot_version_ = MAX(memtable->get_snapshot_version(), result.version_range_.snapshot_version_);
      }
    }
  } // end for
  if (OB_SUCC(ret)) {
    result.suggest_merge_type_ = param.merge_type_;
    result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
    if (result.handle_.empty()) {
      ret = OB_NO_NEED_MERGE;
    } else if (result.log_ts_range_.end_log_ts_ <= clog_checkpoint_ts) {
      if (need_update_snapshot_version) {
        result.update_tablet_directly_ = true;
        LOG_INFO("meet empty force freeze memtable, could update tablet directly", K(ret), K(result));
      } else {
        ret = OB_NO_NEED_MERGE;
      }
    } else if (OB_FAIL(refine_mini_merge_result(tablet, result))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("failed to refine mini merge result", K(ret), K(tablet_id));
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
    const int64_t expect_multi_version_start,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  if (result.handle_.empty()) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("no need to minor merge", K(ret), K(merge_type), K(result));
  } else if (OB_UNLIKELY(!result.log_ts_range_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid argument to check result", K(ret), K(result));
  } else if (OB_FAIL(result.handle_.check_continues(merge_type == BUF_MINOR_MERGE ? nullptr : &result.log_ts_range_))) {
    LOG_WARN("failed to check continues", K(ret), K(result));
  } else if (BUF_MINOR_MERGE == merge_type) {
  } else {
    // update multi_version_start
    if (expect_multi_version_start < result.version_range_.multi_version_start_) {
      LOG_WARN("cannot reserve multi_version_start", "multi_version_start", result.version_range_.multi_version_start_,
               K(expect_multi_version_start));
    } else if (expect_multi_version_start < result.version_range_.snapshot_version_) {
      result.version_range_.multi_version_start_ = expect_multi_version_start;
      LOG_TRACE("succ reserve multi_version_start", "multi_version_start", result.version_range_.multi_version_start_,
                K(expect_multi_version_start));
    } else {
      result.version_range_.multi_version_start_ = result.version_range_.snapshot_version_;
      LOG_TRACE("no need keep multi version", "multi_version_start", result.version_range_.multi_version_start_,
                K(expect_multi_version_start));
    }
  }

  if (OB_SUCC(ret)) {
    result.schema_version_ = tablet.get_storage_schema().schema_version_;
    if (MINI_MERGE == merge_type) {
      ObITable *table = NULL;
      result.base_schema_version_ = result.schema_version_;
      for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
        if (OB_ISNULL(table = result.handle_.get_table(i)) || !table->is_memtable()) {
          ret = OB_ERR_SYS;
          LOG_ERROR("get unexpected table", KPC(table), K(ret));
        } else {
          result.schema_version_ = MAX(result.schema_version_, reinterpret_cast<ObIMemtable *>(table)->get_max_schema_version());
        }
      }
    } else {
      if (OB_FAIL(result.handle_.get_table(0)->get_frozen_schema_version(result.base_schema_version_))) {
        LOG_WARN("failed to get frozen schema version", K(ret), K(result));
      }
    }
  }
  return ret;
}

int ObPartitionMergePolicy::get_mini_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t multi_version_start,
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
  if (MINI_MINOR_MERGE != merge_type && MINOR_MERGE != merge_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(merge_type));
  } else if (tablet.is_ls_tx_data_tablet()) {
    min_snapshot_version = 0;
    max_snapshot_version = INT64_MAX;
  } else if (OB_FAIL(get_boundary_snapshot_version(tablet, min_snapshot_version, max_snapshot_version))) {
    LOG_WARN("fail to calculate boundary version", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(find_mini_minor_merge_tables(param,
                                                  min_snapshot_version,
                                                  max_snapshot_version,
                                                  multi_version_start,
                                                  tablet,
                                                  result))) {
    LOG_WARN("failed to get minor merge tables for mini minor merge", K(ret), K(max_snapshot_version),
             K(multi_version_start));
  }
  return ret;
}

int ObPartitionMergePolicy::get_boundary_snapshot_version(
    const ObTablet &tablet,
    int64_t &min_snapshot,
    int64_t &max_snapshot)
{
  int ret = OB_SUCCESS;
  int64_t merge_inc_base_version = tablet.get_snapshot_version();
  ObTenantFreezeInfoMgr::NeighbourFreezeInfo freeze_info;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObITable *last_major_table = table_store.get_major_sstables().get_boundary_table(true);

  if (OB_UNLIKELY(!table_store.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("table store not valid", K(ret), K(table_store));
  } else if (OB_FAIL(get_neighbour_freeze_info(merge_inc_base_version,
                                               last_major_table,
                                               freeze_info))) {
    LOG_WARN("failed to get freeze info", K(ret), K(merge_inc_base_version), K(table_store));
  } else if (table_store.get_table_count() >= OB_UNSAFE_TABLE_CNT) {
    max_snapshot = INT64_MAX;
    if (table_store.get_table_count() >= OB_EMERGENCY_TABLE_CNT) {
      min_snapshot = 0;
    } else if (OB_NOT_NULL(last_major_table)) {
      min_snapshot = last_major_table->get_snapshot_version();
    }
  } else {
    min_snapshot = freeze_info.prev.freeze_ts;
    max_snapshot = freeze_info.next.freeze_ts;
  }
  return ret;
}

int ObPartitionMergePolicy::find_mini_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t min_snapshot_version,
    const int64_t max_snapshot_version,
    const int64_t expect_multi_version_start,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  result.reset_handle_and_range();
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObTablesHandleArray minor_tables;


  if (OB_UNLIKELY(!table_store.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("unexpected table store", K(ret), K(table_store));
  } else if (OB_FAIL(table_store.get_mini_minor_sstables(minor_tables))) {
    LOG_WARN("failed to get mini minor sstables", K(ret), K(table_store));
  } else {
    ObSSTable *table = nullptr;
    bool found_greater = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.get_count(); ++i) {
      if (OB_ISNULL(table = static_cast<ObSSTable*>(minor_tables.get_table(i)))) {
        ret = OB_ERR_SYS;
        LOG_ERROR("table must not null", K(ret), K(i), K(table_store));
      } else if (!found_greater && table->get_upper_trans_version() <= min_snapshot_version) {
        continue;
      } else {
        found_greater = true;
        if (result.handle_.get_count() > 0) {
          if (result.log_ts_range_.end_log_ts_ < table->get_start_log_ts()) {
            LOG_INFO("log ts not continues, reset previous minor merge tables",
                     K(i), "last_end_log_ts", result.log_ts_range_.end_log_ts_, KPC(table));
            result.reset_handle_and_range();
          } else if (HISTORY_MINI_MINOR_MERGE == param.merge_type_ && table->get_upper_trans_version() > max_snapshot_version) {
            break;
          } else if (table_store.get_table_count() < OB_UNSAFE_TABLE_CNT &&
                     table->get_max_merged_trans_version() > max_snapshot_version) {
            LOG_INFO("max_snapshot_version reached, stop find more tables", K(param), K(max_snapshot_version), KPC(table));
            break;
          }
        }
        if (OB_FAIL(result.handle_.add_table(table))) {
          LOG_WARN("Failed to add table", KPC(table), K(ret));
        } else {
          if (1 == result.handle_.get_count()) {
            result.log_ts_range_.start_log_ts_ = table->get_start_log_ts();
          }
          result.log_ts_range_.end_log_ts_ = table->get_end_log_ts();
        }
      }
    } // end of for
  }

  if (OB_SUCC(ret)) {
    result.suggest_merge_type_ = param.merge_type_;
    if (OB_FAIL(refine_mini_minor_merge_result(result))) {
      LOG_WARN("failed to refine_minor_merge_result", K(ret));
    } else {
      result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
      result.version_range_.snapshot_version_ = tablet.get_snapshot_version();
      if (OB_FAIL(deal_with_minor_result(param.merge_type_, expect_multi_version_start, tablet, result))) {
        LOG_WARN("Failed to deal with minor merge result", K(ret), K(param), K(result));
      } else {
        FLOG_INFO("succeed to get minor merge tables", K(min_snapshot_version), K(max_snapshot_version),
                  K(expect_multi_version_start), K(result), K(tablet));
      }
    }
  }
  return ret;
}

int ObPartitionMergePolicy::get_hist_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t multi_version_start,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const ObMergeType merge_type = param.merge_type_;
  int64_t max_snapshot_version = 0;
  result.reset();

  if (HISTORY_MINI_MINOR_MERGE != merge_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(merge_type));
  } else if (OB_FAIL(deal_hist_minor_merge(tablet, max_snapshot_version))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to deal hist minor merge", K(ret));
    }
  } else if (OB_FAIL(find_mini_minor_merge_tables(param, 0, max_snapshot_version, multi_version_start, tablet, result))) {
    LOG_WARN("failed to get minor tables for hist minor merge", K(ret));
  }
  return ret;
}

int ObPartitionMergePolicy::get_buf_minor_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t multi_version_start,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  UNUSEDx(param, multi_version_start, tablet, result);
  return ret;
}

int ObPartitionMergePolicy::find_buf_minor_merge_tables(
    const storage::ObTablet &tablet,
    ObGetMergeTablesResult *result)
{
  int ret = OB_SUCCESS;
  UNUSEDx(tablet, result);
  return ret;
}

int ObPartitionMergePolicy::find_buf_minor_base_table(ObITable *last_major_table, ObITable *&buf_minor_base_table)
{
  int ret = OB_SUCCESS;
  UNUSEDx(last_major_table, buf_minor_base_table);
  return ret;
}

int ObPartitionMergePolicy::add_buf_minor_merge_result(ObITable *table, ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  UNUSEDx(table, result);
  return ret;
}

int ObPartitionMergePolicy::get_major_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t multi_version_start,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObTenantFreezeInfoMgr::FreezeInfo freeze_info;
  result.reset();
  result.merge_version_ = param.merge_version_;
  result.suggest_merge_type_ = MAJOR_MERGE;
  DEBUG_SYNC(BEFORE_GET_MAJOR_MGERGE_TABLES);

  if (OB_UNLIKELY(!table_store.is_valid() || !param.is_major_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(table_store), K(param));
  } else if (OB_ISNULL(base_table = static_cast<ObSSTable*>(table_store.get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("major sstable not exist", K(ret), K(table_store));
  } else if (base_table->get_snapshot_version() >= result.merge_version_) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("major merge already finished", K(ret), KPC(base_table), K(result));
  } else if (OB_FAIL(base_table->get_frozen_schema_version(result.base_schema_version_))) {
    LOG_WARN("failed to get frozen schema version", K(ret));
  } else if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(get_freeze_info_behind_snapshot_version,
      base_table->get_snapshot_version(), freeze_info))) {
    LOG_WARN("failed to get freeze info", K(ret), K(base_table->get_snapshot_version()));
  } else if (OB_FAIL(result.handle_.add_table(base_table))) {
    LOG_WARN("failed to add base_table to result", K(ret));
  } else if (base_table->get_snapshot_version() >= freeze_info.freeze_ts) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected sstable with snapshot_version bigger than next freeze_scn",
             K(ret), K(freeze_info), KPC(base_table), K(tablet));
  } else {
    const ObSSTableArray &minor_tables = table_store.get_minor_sstables();
    bool start_add_table_flag = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count_; ++i) {
      if (OB_ISNULL(minor_tables[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must not null", K(ret), K(i), K(minor_tables));
      // TODO: add right boundary for major
      } else if (!start_add_table_flag && minor_tables[i]->get_upper_trans_version() > base_table->get_snapshot_version()) {
        start_add_table_flag = true;
      }
      if (OB_SUCC(ret) && start_add_table_flag) {
        if (OB_FAIL(result.handle_.add_table(minor_tables[i]))) {
          LOG_WARN("failed to add table", K(ret));
        } else {
          result.log_ts_range_.end_log_ts_ = minor_tables[i]->get_key().get_end_log_ts();
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (result.handle_.get_count() < 2) { // fix issue 42746719
        if (OB_UNLIKELY(NULL == result.handle_.get_table(0) || !result.handle_.get_table(0)->is_major_sstable())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the only table must be major sstable", K(ret), K(result), K(table_store));
        }
      } else if (OB_FAIL(result.handle_.check_continues(nullptr))) {
        LOG_WARN("failed to check continues for major merge", K(ret), K(result));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(base_table)) {
    if (OB_UNLIKELY(tablet.get_snapshot_version() < param.merge_version_)) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("tablet is not ready to schedule major merge", K(ret), K(tablet), K(param));
    } else if (OB_UNLIKELY(tablet.get_multi_version_start() > param.merge_version_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet haven't kept major snapshot", K(ret), K(tablet), K(param));
    } else {
      const int64_t major_snapshot = MAX(base_table->get_snapshot_version(), freeze_info.freeze_ts);
      result.read_base_version_ = base_table->get_snapshot_version();
      result.version_range_.snapshot_version_ = major_snapshot;
      result.create_snapshot_version_ = base_table->get_meta().get_basic_meta().create_snapshot_version_;
      result.schema_version_ = freeze_info.schema_version;
      result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
      if (multi_version_start < result.version_range_.multi_version_start_) {
        LOG_WARN("cannot reserve multi_version_start", "old multi_version_start", result.version_range_.multi_version_start_,
                K(multi_version_start));
      } else if (multi_version_start < result.version_range_.snapshot_version_) {
        result.version_range_.multi_version_start_ = multi_version_start;
        LOG_TRACE("succ reserve multi_version_start", K(result.version_range_));
      } else {
        result.version_range_.multi_version_start_ = result.version_range_.snapshot_version_;
        LOG_TRACE("no need keep multi version", K(result.version_range_));
      }
    }
  }
  return ret;
}

// may need rewrite for checkpoint_mgr
int ObPartitionMergePolicy::check_need_mini_merge(
    const storage::ObTablet &tablet,
    bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  bool can_merge = false;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  ObSEArray<ObITable *, MAX_MEMSTORE_CNT> memtables;
  if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("tablet is unexpectedly invalid", K(ret), K(tablet));
  } else if (OB_FAIL(tablet.get_memtables(memtables, false/*need_active*/))) {
    LOG_WARN("failed to get all memtables from table store", K(ret));
  } else if (!memtables.empty()) {
    ObSSTable *latest_sstable = static_cast<ObSSTable*>(get_latest_sstable(table_store));
    ObIMemtable *first_frozen_memtable = static_cast<ObIMemtable*>(memtables.at(0));
    ObIMemtable *last_frozen_memtable = static_cast<ObIMemtable*>(memtables.at(memtables.count() - 1));
    if (OB_NOT_NULL(first_frozen_memtable)) {
      need_merge = true;
      if (first_frozen_memtable->can_be_minor_merged()) {
        can_merge = true;
        if (OB_NOT_NULL(latest_sstable)
            && (latest_sstable->get_end_log_ts() >= last_frozen_memtable->get_end_log_ts()
                && tablet.get_snapshot_version() >= last_frozen_memtable->get_snapshot_version())) {
          need_merge = false;
          LOG_ERROR("unexpected sstable", K(ret), KPC(latest_sstable), KPC(last_frozen_memtable));
        }
      } else if (REACH_TENANT_TIME_INTERVAL(30 * 1000 * 1000)) {
        LOG_INFO("memtable can not minor merge",
                 "memtable end_log_ts", first_frozen_memtable->get_end_log_ts(),
                 "memtable timestamp", first_frozen_memtable->get_timestamp());

        const ObStorageSchema &storage_schema = tablet.get_storage_schema();
        ADD_SUSPECT_INFO(MINI_MERGE,
            ls_id, tablet_id,
            "memtable can not minor merge",
            "memtable end_log_ts",
            first_frozen_memtable->get_end_log_ts(),
            "memtable timestamp",
            first_frozen_memtable->get_timestamp());
      }
      if (need_merge && !check_table_count_safe(table_store)) { // check table_store count
        can_merge = false;
        LOG_ERROR("table count is not safe for mini merge", K(tablet_id));
        // add compaction diagnose info
        diagnose_table_count_unsafe(MINI_MERGE, tablet);
      }
#ifdef ERRSIM
      // TODO@hanhui: fix this errsim later
      ret = E(EventTable::EN_COMPACTION_DIAGNOSE_TABLE_STORE_UNSAFE_FAILED) ret;
      if (OB_FAIL(ret)) {
         ret = OB_SUCCESS;
         need_merge = false;
         diagnose_table_count_unsafe(MINI_MERGE, tablet); // ignore failure
         LOG_INFO("check table count with errsim", K(tablet_id));
      }
#endif
      if (need_merge && !can_merge) {
        need_merge = false;
        if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000)) {
          LOG_INFO("check_need_mini_merge which cannot merge", K(tablet_id), K(need_merge), K(can_merge),
              K(latest_sstable), K(first_frozen_memtable));
        }
      }
    }
  }

  if (OB_SUCC(ret) && need_merge) {
    LOG_DEBUG("check mini merge", K(ls_id), "tablet_id", tablet_id.id(), K(need_merge),
              K(can_merge), K(table_store));
  }
  return ret;
}

int ObPartitionMergePolicy::check_need_mini_minor_merge(
    const ObTablet &tablet,
    bool &need_merge)
{
  int ret = OB_SUCCESS;
  int64_t min_snapshot_version = 0;
  int64_t max_snapshot_version = 0;
  int64_t minor_sstable_count = 0;
  int64_t need_merge_mini_count = 0;
  need_merge = false;
  int64_t mini_minor_threshold = DEFAULT_MINOR_COMPACT_TRIGGER;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  int64_t delay_merge_schedule_interval = 0;
  ObTablesHandleArray minor_tables;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      mini_minor_threshold = tenant_config->minor_compact_trigger;
      delay_merge_schedule_interval = tenant_config->_minor_compaction_interval;
    }
  } // end of ObTenantConfigGuard
  if (table_store.get_minor_sstables().count_ <= mini_minor_threshold) {
    // total number of mini sstable is less than threshold + 1
  } else if (tablet.is_ls_tx_data_tablet()) {
    min_snapshot_version = 0;
    max_snapshot_version = INT64_MAX;
  } else if (OB_FAIL(get_boundary_snapshot_version(tablet, min_snapshot_version, max_snapshot_version))) {
    LOG_WARN("failed to calculate boundary version", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(table_store.get_mini_minor_sstables(minor_tables))) {
    LOG_WARN("failed to get mini minor sstables", K(ret), K(table_store));
  } else {
    int64_t minor_check_snapshot_version = 0;
    bool found_greater = false;

    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.get_count(); ++i) {
      ObSSTable *table = static_cast<ObSSTable *>(minor_tables.get_table(i));
      if (OB_UNLIKELY(table->is_buf_minor_sstable())) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Unexpected buf minor sstable", K(ret), K(table_store));
      } else if (!found_greater && table->get_upper_trans_version() <= min_snapshot_version) {
        continue;
      } else if (table->get_max_merged_trans_version() > max_snapshot_version) {
        break;
      }
      found_greater = true;
      minor_sstable_count++;
      if (table->is_mini_sstable()) {
        if (mini_minor_threshold == need_merge_mini_count++) {
          minor_check_snapshot_version = table->get_max_merged_trans_version();
        }
      } else if (need_merge_mini_count > 0 || minor_sstable_count - need_merge_mini_count > 1) {
        // chaos order with mini and minor sstable OR more than one minor sstable
        // need compaction except data replica
        need_merge_mini_count = mini_minor_threshold + 1;
        break;
      }
    } // end of for

    if (OB_SUCC(ret)) {
      // GCONF.minor_compact_trigger means the maximum number of the current L0 sstable,
      // the compaction will be scheduled when it be exceeded
      // If minor_compact_trigger = 0, it means that all L0 sstables should be merged into L1 as soon as possible
      if (minor_sstable_count <= 1) {
        // only one minor sstable exist, no need to do mini minor merge
      } else if (table_store.get_table_count() >= MAX_SSTABLE_CNT_IN_STORAGE - RESERVED_STORE_CNT_IN_STORAGE) {
        need_merge = true;
        LOG_INFO("table store has too many sstables, need to compaction", K(table_store));
      } else if (need_merge_mini_count <= mini_minor_threshold) {
        // no need merge
      } else {
        if (delay_merge_schedule_interval > 0 && minor_check_snapshot_version > 0) {
          // delays the compaction scheduling
          int64_t current_time = ObTimeUtility::current_time();
          if (minor_check_snapshot_version + delay_merge_schedule_interval < current_time) {
            // need merge
            need_merge = true;
          }
        } else {
          need_merge = true;
        }
      }
    }
  }
  if (OB_SUCC(ret) && need_merge) {
    LOG_DEBUG("check mini minor merge", "ls_id", tablet.get_tablet_meta().ls_id_,
             K(tablet_id), K(need_merge), K(table_store));
  }
  if (OB_SUCC(ret) && !need_merge && table_store.get_minor_sstables().count() >= DIAGNOSE_TABLE_CNT_IN_STORAGE) {
    ADD_SUSPECT_INFO(MINOR_MERGE,
                     tablet.get_tablet_meta().ls_id_, tablet_id,
                     "can't schedule minor merge",
                     K(min_snapshot_version), K(max_snapshot_version), K(need_merge_mini_count),
                     K(minor_sstable_count), "mini_sstable_cnt", table_store.get_minor_sstables().count());
  }
  return ret;
}

int ObPartitionMergePolicy::check_need_buf_minor_merge(
    const ObTablet &tablet,
    bool &need_merge)
{
  int ret = OB_NO_NEED_MERGE;
  need_merge = false;
  UNUSED(tablet);
  return ret;
}

int ObPartitionMergePolicy::check_need_hist_minor_merge(
    const storage::ObTablet &tablet,
    bool &need_merge)
{
  int ret = OB_SUCCESS;
  const int64_t hist_threashold = cal_hist_minor_merge_threshold();
  int64_t max_snapshot_version = 0;
  need_merge = false;
  if (OB_FAIL(deal_hist_minor_merge(tablet, max_snapshot_version))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to deal hist minor merge", K(ret));
    }
  } else {
    need_merge = true;
  }
  if (OB_SUCC(ret) && need_merge) {
    if (REACH_TENANT_TIME_INTERVAL(30 * 1000 * 1000)) {
      FLOG_INFO("Table store need to do hist minor merge to reduce sstables", K(need_merge), K(hist_threashold));
    }
  }
  return ret;
}

int ObPartitionMergePolicy::deal_hist_minor_merge(
    const ObTablet &tablet,
    int64_t &max_snapshot_version)
{
  int ret = OB_SUCCESS;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  const int64_t hist_threshold = cal_hist_minor_merge_threshold();
  ObITable *first_major_table = nullptr;
  ObTenantFreezeInfoMgr *freeze_info_mgr = nullptr;
  max_snapshot_version = 0;

  if (!table_store.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected invalid table store", K(ret), K(table_store));
  } else if (table_store.get_minor_sstables().count_ < hist_threshold) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_ISNULL(freeze_info_mgr = MTL(ObTenantFreezeInfoMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get freeze info mgr from MTL", K(ret));
  } else if (OB_ISNULL(first_major_table = table_store.get_major_sstables().get_boundary_table(false))) {
    // index table during building, need compat with continuous multi version
    if (0 == (max_snapshot_version = freeze_info_mgr->get_latest_frozen_timestamp())) {
      // no freeze info found, wait normal mini minor to free sstable
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No freeze range to do hist minor merge for buiding index", K(ret), K(table_store));
    }
  } else {
    ObTenantFreezeInfoMgr::NeighbourFreezeInfo freeze_info;
    ObSEArray<ObTenantFreezeInfoMgr::FreezeInfo, 8> freeze_infos;
    if (OB_FAIL(freeze_info_mgr->get_freeze_info_behind_major_snapshot(
                first_major_table->get_snapshot_version(),
                freeze_infos))) {
      LOG_WARN("Failed to get freeze infos behind major version", K(ret), KPC(first_major_table));
    } else if (freeze_infos.count() <= 1) {
      // only one major freeze found, wait normal mini minor to reduce table count
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("No enough freeze range to do hist minor merge", K(ret), K(freeze_infos));
    } else {
      int64_t table_cnt = 0;
      int64_t min_minor_version = 0;
      int64_t max_minor_version = 0;
      if (OB_FAIL(get_boundary_snapshot_version(tablet, min_minor_version, max_minor_version))) {
        LOG_WARN("fail to calculate boundary version", K(ret));
      } else {
        ObSSTable *table = nullptr;
        const ObSSTableArray &minor_tables = table_store.get_minor_sstables();
        for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count_; ++i) {
          if (OB_ISNULL(table = static_cast<ObSSTable*>(minor_tables[i]))) {
            ret = OB_ERR_SYS;
            LOG_ERROR("table must not null", K(ret), K(i), K(table_store));
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

int ObPartitionMergePolicy::check_need_major_merge(
    const storage::ObTablet &tablet,
    int64_t &merge_version,
    bool &need_merge,
    bool &can_merge,
    bool &need_force_freeze)
{
  int ret = OB_SUCCESS;
  need_merge = false;
  can_merge = false;
  need_force_freeze = false;
  ObTenantFreezeInfoMgr::FreezeInfo freeze_info;
  int64_t last_sstable_snapshot = tablet.get_snapshot_version();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  int64_t major_sstable_version = 0;
  bool is_tablet_data_status_complete = true;   //ha status

  if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("tablet is unexpectedly invalid", K(ret), K(tablet));
  } else {
    ObSSTable *latest_major_sstable = static_cast<ObSSTable*>(table_store.get_major_sstables().get_boundary_table(true/*last*/));
    if (OB_NOT_NULL(latest_major_sstable)) {
      major_sstable_version = latest_major_sstable->get_snapshot_version();
      if (major_sstable_version < merge_version) {
        need_merge = true;
      }
    }
    if (need_merge) {
      if (!tablet.get_tablet_meta().ha_status_.is_data_status_complete()) {
        can_merge = false;
        is_tablet_data_status_complete = false;
        LOG_INFO("tablet data status incomplete, can not merge", K(ret), K(tablet_id));
      } else if (OB_FAIL(MTL_CALL_FREEZE_INFO_MGR(get_freeze_info_behind_snapshot_version, major_sstable_version, freeze_info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to get freeze info", K(ret), K(merge_version), K(major_sstable_version));
        } else {
          can_merge = false;
          ret = OB_SUCCESS;
          LOG_INFO("can't get freeze info after snapshot", K(ret), K(merge_version), K(major_sstable_version));
        }
      } else {
        can_merge = last_sstable_snapshot >= freeze_info.freeze_ts;
        if (!can_merge) {
          LOG_TRACE("tablet need merge, but cannot merge now", K(tablet_id), K(merge_version), K(last_sstable_snapshot), K(freeze_info));
        }
      }

      if (OB_SUCC(ret) && !can_merge && is_tablet_data_status_complete) {
        ObTabletMemtableMgr *memtable_mgr = nullptr;
        ObTableHandleV2 last_frozen_memtable_handle;
        memtable::ObMemtable *last_frozen_memtable = nullptr;
        if (OB_ISNULL(memtable_mgr = static_cast<ObTabletMemtableMgr *>(tablet.get_memtable_mgr()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable mgr is unexpected null", K(ret), K(tablet));
        } else if (OB_FAIL(memtable_mgr->get_last_frozen_memtable(last_frozen_memtable_handle))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("fail to get last frozen memtable", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(last_frozen_memtable_handle.get_data_memtable(last_frozen_memtable))) {
          LOG_WARN("fail to get memtable", K(ret));
        }

        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(last_frozen_memtable)) {
          // no frozen memtable, need force freeze
          need_force_freeze = true;
        } else {
          need_force_freeze = last_frozen_memtable->get_snapshot_version() < freeze_info.freeze_ts;
          if (!need_force_freeze) {
            FLOG_INFO("tablet no need force freeze", K(ret), K(tablet_id), K(merge_version), K(freeze_info), KPC(last_frozen_memtable));
          }
        }
      }
    }
    if (need_merge && !can_merge && REACH_TENANT_TIME_INTERVAL(60L * 1000L * 1000L)) {
      LOG_INFO("check_need_major_merge", K(ret), "ls_id", tablet.get_tablet_meta().ls_id_, K(tablet_id),
               K(need_merge), K(can_merge), K(need_force_freeze), K(merge_version), K(freeze_info),
               K(is_tablet_data_status_complete));
      const ObStorageSchema &storage_schema = tablet.get_storage_schema();
      ADD_SUSPECT_INFO(MAJOR_MERGE, tablet.get_tablet_meta().ls_id_, tablet_id,
                       "need major merge but can't merge now",
                       K(merge_version), K(freeze_info), K(last_sstable_snapshot), K(need_force_freeze),
                       K(is_tablet_data_status_complete));
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
  ObTabletMinorMergeDag dag;
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
    const ObMergeType &merge_type,
    const storage::ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t buf_len = ObScheduleSuspectInfoMgr::EXTRA_INFO_LEN;
  char tmp_str[buf_len] = "\0";
  const ObStorageSchema &storage_schema = tablet.get_storage_schema();
  const ObTabletTableStore &table_store = tablet.get_table_store();
  const ObSSTableArray &major_tables = table_store.get_major_sstables();
  const ObSSTableArray &minor_tables = table_store.get_minor_sstables();

  // add sstable info
  const int64_t major_table_count = major_tables.count_;
  const int64_t minor_table_count = minor_tables.count_;
  ADD_COMPACTION_INFO_PARAM(tmp_str, buf_len, K(major_table_count), K(minor_table_count));

  if (OB_TMP_FAIL(ObCompactionDiagnoseMgr::check_system_compaction_config(tmp_str, buf_len))) {
    LOG_WARN("failed to check system compaction config", K(tmp_ret), K(tmp_str));
  }

  // check min_reserved_snapshot
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  int64_t min_reserved_snapshot = 0;
  int64_t min_merged_snapshot = INT64_MAX;
  ObString snapshot_from_str;
  const ObSSTable *major_sstable = static_cast<const ObSSTable *>(major_tables.get_boundary_table(false/*last*/));
  if (OB_ISNULL(major_sstable)) {
    const ObSSTable *minor_sstable = static_cast<const ObSSTable *>(minor_tables.get_boundary_table(false/*last*/));
    ADD_COMPACTION_INFO_PARAM(tmp_str, buf_len, "no major sstable. first_minor_start_log_ts = ", minor_sstable->get_start_log_ts());
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
  if (OB_TMP_FAIL(diagnose_minor_dag(MINI_MINOR_MERGE, ls_id, tablet_id, tmp_str, buf_len))) {
    LOG_WARN("failed to diagnose minor dag", K(tmp_ret), K(ls_id), K(tablet_id), K(tmp_str));
  }
  if (OB_TMP_FAIL(diagnose_minor_dag(HISTORY_MINI_MINOR_MERGE, ls_id, tablet_id, tmp_str, buf_len))) {
    LOG_WARN("failed to diagnose history minor dag", K(tmp_ret), K(ls_id), K(tablet_id), K(tmp_str));
  }

  // add suspect info
  ADD_SUSPECT_INFO(merge_type, ls_id, tablet_id,
      "sstable count is not safe", "extra_info", tmp_str);
  return ret;
}

bool ObPartitionMergePolicy::check_table_count_safe(const ObTabletTableStore &table_store)
{
  return table_store.get_table_count() < OB_EMERGENCY_TABLE_CNT;
}

int ObPartitionMergePolicy::refine_mini_merge_result(
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObITable *last_table = nullptr;
  const ObTabletTableStore &table_store = tablet.get_table_store();

  if (OB_UNLIKELY(!table_store.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table store not valid", K(ret), K(table_store));
  } else if (OB_ISNULL(last_table = table_store.get_minor_sstables().get_boundary_table(true/*last*/))) {
    // no minor sstable, skip to cut memtable's boundary
  } else if (result.log_ts_range_.start_log_ts_ > last_table->get_end_log_ts()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected uncontinuous log_ts_range in mini merge",
              K(ret), K(result), KPC(last_table), K(table_store), K(tablet));
  } else if (result.log_ts_range_.start_log_ts_ < last_table->get_end_log_ts()
      && !tablet.get_tablet_meta().tablet_id_.is_special_merge_tablet()) {
    // fix start_log_ts to make log_ts_range continuous in migrate phase for issue 42832934
    if (result.log_ts_range_.end_log_ts_ <= last_table->get_end_log_ts()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("No need mini merge memtable which is covered by existing sstable",
               K(ret), K(result), KPC(last_table), K(table_store), K(tablet));
    } else {
      result.log_ts_range_.start_log_ts_ = last_table->get_end_log_ts();
      FLOG_INFO("Fix mini merge result log ts range", K(ret), K(result), KPC(last_table), K(table_store), K(tablet));
    }
  }
  return ret;
}

// Used to adjust whether to do L0 Minor merge or L1 Minor merge
int ObPartitionMergePolicy::refine_mini_minor_merge_result(ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObMergeType &merge_type = result.suggest_merge_type_;

  if (result.handle_.empty()) {
  } else if (MINI_MINOR_MERGE != merge_type
             && HISTORY_MINI_MINOR_MERGE != merge_type
             && MINOR_MERGE != merge_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected merge type to refine merge tables", K(result), K(ret));
  } else {
    ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> mini_tables;
    ObITable *table = NULL;
    ObSSTable *sstable = NULL;
    int64_t mini_sstable_size = 1;
    int64_t minor_sstable_size = 1;
    int64_t minor_sstable_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
      if (OB_ISNULL(table = result.handle_.get_table(i)) || !table->is_minor_sstable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("get unexpected table", KP(table), K(ret));
      } else if (FALSE_IT(sstable = reinterpret_cast<ObSSTable *>(table))) {
      } else if (table->is_mini_sstable()) { // L0 table
        mini_sstable_size += sstable->get_meta().get_basic_meta().row_count_;
        if (OB_FAIL(mini_tables.push_back(table))) {
          LOG_WARN("Failed to push mini minor table into array", K(ret));
        }
      } else if (table->is_multi_version_minor_sstable()) { // not include buf minor sstable, L1 table
        if (mini_tables.count() > 0) {
          mini_tables.reset();
          LOG_INFO("minor refine, minor merge sstable refine to minor merge due to chaos table order",
                   K(result));
          break;
        } else {
          minor_sstable_size += sstable->get_meta().get_basic_meta().row_count_;
          ++minor_sstable_count;
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t minor_compact_trigger = DEFAULT_MINOR_COMPACT_TRIGGER;
      int64_t size_amplification_factor = OB_DEFAULT_COMPACTION_AMPLIFICATION_FACTOR;
      {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
        if (tenant_config.is_valid()) {
          minor_compact_trigger = tenant_config->minor_compact_trigger;
          if (tenant_config->_minor_compaction_amplification_factor != 0) {
            size_amplification_factor = tenant_config->_minor_compaction_amplification_factor;
          }
        }
      } // end of ObTenantConfigGuard
      if (1 == result.handle_.get_count()) {
        LOG_INFO("minor refine, only one sstable, no need to do mini minor merge", K(result));
        result.handle_.reset();
      } else if (HISTORY_MINI_MINOR_MERGE == merge_type) {
        // use minor merge to do history mini minor merge and skip other checks
      } else if (0 == minor_compact_trigger || mini_tables.count() <= 1 || minor_sstable_count > 1) {
        merge_type = MINOR_MERGE;
      } else if (minor_sstable_count == 0 && mini_sstable_size > OB_MIN_MINOR_SSTABLE_ROW_COUNT) {
        merge_type = MINOR_MERGE;
        LOG_INFO("minor refine, mini minor merge sstable refine to minor merge",
                 K(minor_sstable_size), K(mini_sstable_size), K(OB_MIN_MINOR_SSTABLE_ROW_COUNT), K(result));
      } else if (minor_sstable_count == 1
                 && mini_sstable_size > (minor_sstable_size * size_amplification_factor / 100)) {
        merge_type = MINOR_MERGE;
        LOG_INFO("minor refine, mini minor merge sstable refine to minor merge", K(minor_sstable_size),
                 K(mini_sstable_size), K(size_amplification_factor), K(result));
      } else {
        // reset the merge result, mini sstable merge into a new mini sstable
        result.reset_handle_and_range();
        for (int64_t i = 0; OB_SUCC(ret) && i < mini_tables.count(); i++) {
          ObITable *table = mini_tables.at(i);
          if (OB_FAIL(result.handle_.add_table(table))) {
            LOG_WARN("Failed to add table to minor merge result", KPC(table), K(ret));
          } else {
            if (1 == result.handle_.get_count()) {
              result.log_ts_range_.start_log_ts_ = table->get_start_log_ts();
            }
            result.log_ts_range_.end_log_ts_ = table->get_end_log_ts();
          }
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("minor refine, mini minor merge sstable refine info", K(minor_sstable_size),
                   K(mini_sstable_size), K(result));
        }
      }
    }
  }
  return ret;
}

ObITable *ObPartitionMergePolicy::get_latest_sstable(const ObTabletTableStore &table_store)
{
  ObITable *major_table = table_store.get_major_sstables().get_boundary_table(true/*last*/);
  ObITable *minor_table = table_store.get_minor_sstables().get_boundary_table(true/*last*/);
  ObITable *latest_sstable = nullptr;
  if (OB_NOT_NULL(major_table) && OB_NOT_NULL(minor_table)) {
    if (major_table->get_snapshot_version() < minor_table->get_max_merged_trans_version()) {
      latest_sstable = static_cast<ObITable*>(minor_table);
    } else {
      latest_sstable = static_cast<ObITable*>(major_table);
    }
  } else if (OB_NOT_NULL(major_table)) {
    latest_sstable = static_cast<ObITable*>(major_table);
  } else {
    latest_sstable = static_cast<ObITable*>(minor_table);
  }
  return latest_sstable;
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

} /* namespace compaction */
} /* namespace oceanbase */
