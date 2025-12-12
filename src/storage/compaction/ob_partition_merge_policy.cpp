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
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/direct_load/ob_inc_major_ddl_aggregate_sstable.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;
using namespace transaction;

namespace compaction
{
ERRSIM_POINT_DEF(EN_COMPACTION_DISABLE_ROW_COL_SWITCH);
ERRSIM_POINT_DEF(EN_COMPACTION_MINOR_ALL);
ERRSIM_POINT_DEF(EN_CO_MERGE_WITH_MINOR);

// keep order with ObMergeType
ObPartitionMergePolicy::GetMergeTables ObPartitionMergePolicy::get_merge_tables[]
  = {
      ObPartitionMergePolicy::not_support_merge_type,
      ObPartitionMergePolicy::get_minor_merge_tables,
      ObPartitionMergePolicy::get_hist_minor_merge_tables,
      ObAdaptiveMergePolicy::get_meta_merge_tables,
      ObPartitionMergePolicy::get_mini_merge_tables,
      ObPartitionMergePolicy::get_medium_merge_tables,
      ObPartitionMergePolicy::get_medium_merge_tables,
      ObPartitionMergePolicy::not_support_merge_type,
      ObPartitionMergePolicy::not_support_merge_type,
      ObPartitionMergePolicy::not_support_merge_type,
      ObPartitionMergePolicy::get_mds_merge_tables,
      ObPartitionMergePolicy::not_support_merge_type,
      ObPartitionMergePolicy::get_convert_co_major_merge_tables,
      ObPartitionMergePolicy::get_inc_major_merge_tables,
      ObPartitionMergePolicy::not_support_merge_type,
    };

int ObPartitionMergePolicy::get_medium_merge_tables(
  const ObGetMergeTablesParam &param,
  ObLS &ls,
  const ObTablet &tablet,
  ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  result.merge_version_ = param.merge_version_;
  DEBUG_SYNC(BEFORE_GET_MAJOR_MGERGE_TABLES);
  if (OB_UNLIKELY(!param.is_valid() || !is_major_merge_type(param.merge_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(param));
  } else if (OB_FAIL(get_result_by_snapshot(ls, tablet, param.merge_version_, result, true/*need_check_tablet*/))) {
    LOG_WARN("failed to get medium result by snapshot", K(ret), K(param));
  } else if (OB_FAIL(result.handle_.check_continues(nullptr))) {
    LOG_WARN("failed to check continues for major merge", K(ret));
    SET_DIAGNOSE_LOCATION(result.error_location_);
  }

  if (FAILEDx(tablet.get_private_transfer_epoch(result.private_transfer_epoch_))) {
    LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet.get_tablet_meta());
  } else {
    // major sstable's rec scn will always be 0
    result.rec_scn_.set_min();
    result.version_range_.base_version_ = 0;
    result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
    result.version_range_.snapshot_version_ = param.merge_version_;
    if (OB_FAIL(get_multi_version_start(param.merge_type_, ls, tablet, result.version_range_, result.snapshot_info_))) {
      LOG_WARN("failed to get multi version_start", K(ret));
    }
  }
  return ret;
}

int ObPartitionMergePolicy::get_inc_major_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const int64_t INC_MAJOR_COMPACT_TRIGGER = 3; // TODO(@DanLing) add tenant config
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  int64_t min_snapshot = 0;
  int64_t max_snapshot = 0;
  result.reset();

  if (OB_UNLIKELY(tablet.is_ls_inner_tablet())) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to get table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid()
         || !param.is_valid() || !is_inc_major_merge(param.merge_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(table_store_wrapper.get_member()), K(param));
  } else if (table_store_wrapper.get_member()->get_inc_major_sstables().count() <= INC_MAJOR_COMPACT_TRIGGER) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_FAIL(get_boundary_snapshot_version(tablet, min_snapshot, max_snapshot))) {
    LOG_WARN("failed to get boundary snapshot version", K(ret));
  } else if (INT64_MAX != max_snapshot) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("exist unmerged snapshot, no need to do inc major merge", K(ret), K(min_snapshot), K(max_snapshot));
  } else {
    const ObSSTableArray &inc_major_tables = table_store_wrapper.get_member()->get_inc_major_sstables();
    uint64_t prev_tx_id = 0;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < inc_major_tables.count(); ++idx) {
      ObSSTable *sstable = inc_major_tables[idx];
      int64_t trans_state = ObTxData::UNKOWN;
      int64_t commit_version = -1;
      ObSSTableMetaHandle meta_hdl;

      if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null sstable", K(ret), K(inc_major_tables));
      } else if (OB_FAIL(ObIncMajorTxHelper::get_inc_major_commit_version(ls, *sstable, SCN::max_scn(), trans_state, commit_version))) {
        LOG_WARN("failed to get commit version for inc major", K(ret), KPC(sstable));
      } else if (ObTxData::ABORT == trans_state || commit_version <= min_snapshot) {
        continue;
      } else if (ObTxData::RUNNING == trans_state) {
        break;
      } else if (OB_FAIL(sstable->get_meta(meta_hdl))) {
        LOG_WARN("failed to get sstable meta handle", K(ret), KPC(sstable));
      } else if (OB_UNLIKELY(meta_hdl.get_sstable_meta().get_uncommit_tx_info().is_empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected empty tx info for inc major", K(ret), KPC(sstable), K(meta_hdl));
      } else {
        const int64_t cur_tx_id = meta_hdl.get_sstable_meta().get_uncommit_tx_info().tx_infos_[0].tx_id_;
        if (prev_tx_id != cur_tx_id) {
          if (result.handle_.get_count() <= INC_MAJOR_COMPACT_TRIGGER) {
            result.reset();
          } else {
            break;
          }
        }

        if (OB_FAIL(result.handle_.add_sstable(sstable, table_store_wrapper.get_meta_handle()))) {
          LOG_WARN("failed to add table", K(ret));
        } else {
          result.version_range_.snapshot_version_ = sstable->get_snapshot_version();
        }
        prev_tx_id = cur_tx_id;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (result.handle_.get_count() <= INC_MAJOR_COMPACT_TRIGGER) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("no need to do inc major merge due to table count", K(ret), K(result)); // change trace log later
    } else if (OB_FAIL(tablet.get_private_transfer_epoch(result.private_transfer_epoch_))) {
      LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta",
        tablet.get_tablet_meta());
    } else {
      result.version_range_.base_version_ = 0;
      result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
      if (OB_FAIL(get_multi_version_start(param.merge_type_, ls, tablet, result.version_range_, result.snapshot_info_))) {
        LOG_WARN("failed to get multi version_start", K(ret));
      }
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
            // mds has no major sstable
            result.rec_scn_ = cur_table_handle.get_table()->get_rec_scn();
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
      minor_compact_trigger = tenant_config->mds_minor_compact_trigger;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (result.handle_.get_count() < MAX(minor_compact_trigger, DEFAULT_MINOR_COMPACT_TRIGGER)) {
    ret = OB_NO_NEED_MERGE;
    result.handle_.reset();
  } else if (OB_FAIL(tablet.get_private_transfer_epoch(result.private_transfer_epoch_))) {
    LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet.get_tablet_meta());
  } else {
    result.version_range_.snapshot_version_ = tablet.get_snapshot_version();
  }
  return ret;
}

int ObPartitionMergePolicy::get_convert_co_major_merge_tables(
    const storage::ObGetMergeTablesParam &param,
    storage::ObLS &ls,
    const storage::ObTablet &tablet,
    storage::ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  result.reset();
  result.merge_version_ = param.merge_version_;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid()
                      || !param.is_valid()
                      || !is_convert_co_major_merge(param.merge_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(table_store_wrapper.get_member()), K(param));
  } else if (OB_ISNULL(base_table = static_cast<ObSSTable*>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("major sstable not exist", K(ret), KPC(table_store_wrapper.get_member()));
  } else if (OB_FAIL(result.handle_.add_sstable(base_table, table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("failed to add base_table to result", K(ret));
  } else if (OB_UNLIKELY(base_table->get_snapshot_version() != param.merge_version_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("convert co major merge should not change major snapshot version", K(ret), KPC(base_table), K(param), K(tablet));
  } else if (OB_FAIL(tablet.get_private_transfer_epoch(result.private_transfer_epoch_))) {
    LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet.get_tablet_meta());
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

// Only during major merge, when the tablet has been frozen, we can check the tablet
int ObPartitionMergePolicy::get_result_by_snapshot(
    storage::ObLS &ls,
    const ObTablet &tablet,
    const int64_t snapshot,
    ObGetMergeTablesResult &result,
    const bool need_check_tablet)
{
  int ret = OB_SUCCESS;
  ObSSTable *base_table = nullptr;
  result.reset();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(table_store_wrapper.get_member()));
  } else if (OB_UNLIKELY(snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(table_store_wrapper.get_member()));
  } else if (OB_ISNULL(base_table = static_cast<ObSSTable*>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("major sstable not exist", K(ret), KPC(table_store_wrapper.get_member()));
  } else if (OB_FAIL(result.handle_.add_sstable(base_table, table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("failed to add base_table to result", K(ret));
  } else if (base_table->get_snapshot_version() >= snapshot) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("medium merge already finished", K(ret), KPC(base_table), K(result));
  } else if (need_check_tablet && OB_UNLIKELY(tablet.get_snapshot_version() < snapshot)) { // TODO(@DanLing) consider inc major snapshot here
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("tablet is not ready to schedule medium merge", K(ret), K(tablet), K(snapshot));
  } else if (need_check_tablet && OB_UNLIKELY(tablet.get_multi_version_start() > snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet haven't kept medium snapshot", K(ret), K(tablet), K(snapshot));
  } else {
    result.rec_scn_ = base_table->get_rec_scn();
  }

  // prepare inc major tables
  const ObSSTableArray &inc_major_tables = table_store_wrapper.get_member()->get_inc_major_sstables();
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_tables.count(); ++i) {
    ObSSTable *sstable = inc_major_tables.at(i);
    int64_t tx_state = ObTxData::UNKOWN;
    int64_t commit_version = -1;
    if (OB_UNLIKELY(nullptr == sstable || !sstable->is_inc_major_type_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected table", KR(ret), KPC(sstable));
    } else if (OB_FAIL(ObIncMajorTxHelper::get_inc_major_commit_version(ls,
                                                                        *sstable,
                                                                        SCN::max_scn(),
                                                                        tx_state,
                                                                        commit_version))) {
      LOG_WARN("fail to get inc major commit version", KR(ret));
    } else if (ObTxData::COMMIT == tx_state && INT64_MAX != commit_version) {
      if (commit_version > snapshot) {
        break;
      } else if (commit_version > base_table->get_snapshot_version()
              && OB_FAIL(result.handle_.add_sstable(sstable, table_store_wrapper.get_meta_handle()))) {
        LOG_WARN("failed to add table", KR(ret));
      }
    } else if (ObTxData::RUNNING != tx_state && ObTxData::ABORT != tx_state) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans state", KR(ret), K(tx_state), K(commit_version), KPC(sstable));
    }
  }

#ifdef ERRSIM
  int errsim_ret = OB_SUCCESS;
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> inc_major_tables_for_ds;
  if (OB_SUCCESS != (errsim_ret = result.handle_.get_inc_major_tables(inc_major_tables_for_ds))) {
    LOG_WARN("fail to get inc major tables for debug sync", K(errsim_ret));
  } else if (inc_major_tables_for_ds.count() > 0) {
    DEBUG_SYNC(AFTER_INC_MAJOR_MERGE_GET_SSTABLE);
  }
#endif

  // prepare minor tables
  const ObSSTableArray &minor_tables = table_store_wrapper.get_member()->get_minor_sstables();
  bool start_add_table_flag = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
    if (OB_ISNULL(minor_tables[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table must not null", K(ret), K(i), K(minor_tables));
    // TODO: add right boundary for major
    } else if (!start_add_table_flag && minor_tables[i]->get_upper_trans_version() > base_table->get_snapshot_version()) {
      start_add_table_flag = true;
    }
    if (OB_SUCC(ret) && start_add_table_flag) {
      if (OB_FAIL(result.handle_.add_sstable(minor_tables[i], table_store_wrapper.get_meta_handle()))) {
        LOG_WARN("failed to add table", K(ret));
      } else if (result.rec_scn_.is_min()) {
        result.rec_scn_ = minor_tables[i]->get_rec_scn();
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

ERRSIM_POINT_DEF(ERRSIM_DISABLE_MINI_MERGE);
int ObPartitionMergePolicy::get_mini_merge_tables(
    const ObGetMergeTablesParam &param,
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObFreezeInfo, 4> freeze_infos;
  ObFreezeInfo next_freeze_info;
  const int64_t merge_inc_base_version = tablet.get_snapshot_version();
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
  } else if (OB_FAIL(tablet.get_all_memtables_from_memtable_mgr(memtable_handles))) {
    LOG_WARN("failed to get all memtable", K(ret), K(tablet));
  } else if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_freeze_info_behind_major_snapshot(merge_inc_base_version, false/*include_equal*/, freeze_infos))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get freeze info behind snapshot version", K(ret), K(merge_inc_base_version));
    } else {
      ret = OB_SUCCESS;
      next_freeze_info.frozen_scn_ = MTL(ObTenantFreezeInfoMgr *)->get_snapshot_gc_scn();
    }
  } else {
    next_freeze_info = freeze_infos.at(0);
  }

  if (OB_FAIL(ret)) {
  } else if (ERRSIM_DISABLE_MINI_MERGE && !is_sys_tenant(MTL_ID())) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("Errsim: disable mini merge", K(ret), "tenant_id", MTL_ID(), "tablet_id", tablet.get_tablet_meta().tablet_id_);
  } else if (OB_FAIL(find_mini_merge_tables(param, next_freeze_info.frozen_scn_.get_val_for_tx(), ls, tablet, memtable_handles, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to find mini merge tables", K(ret), K(next_freeze_info));
    }
  } else if (OB_FAIL(tablet.get_private_transfer_epoch(result.private_transfer_epoch_))) {
    LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet.get_tablet_meta());
  } else if (result.update_tablet_directly_) {
    // do nothing else
  } else if (OB_FAIL(deal_with_minor_result(merge_type, ls, tablet, result))) {
    LOG_WARN("failed to deal with minor merge result", K(ret));
  }

  return ret;
}

int ObPartitionMergePolicy::find_mini_merge_tables(
    const ObGetMergeTablesParam &param,
    const int64_t max_snapshot_version,
    ObLS &ls,
    const storage::ObTablet &tablet,
    ObIArray<ObTableHandleV2> &memtable_handles,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  // TODO: @dengzhi.ldz, remove max_snapshot_version, merge all forzen memtables
  // Keep max_snapshot_version currently because major merge must be done step by step
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
        result.rec_scn_ = memtable->get_rec_scn();
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
    if (OB_FAIL(tablet.get_private_transfer_epoch(result.private_transfer_epoch_))) {
      LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet.get_tablet_meta());
    } else if (OB_FAIL(tablet.get_recycle_version(result.version_range_.multi_version_start_, result.version_range_.base_version_))) {
      LOG_WARN("Fail to get table store recycle version", K(ret), K(result.version_range_), K(tablet));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DISABLE_MINOR_MERGE);
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
  } else if (ERRSIM_DISABLE_MINOR_MERGE && !is_sys_tenant(MTL_ID())) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("Errsim: disable minor merge", K(ret), "tenant_id", MTL_ID(), "tablet_id", tablet.get_tablet_meta().tablet_id_);
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
  STATIC_ASSERT(static_cast<int64_t>(MERGE_TYPE_MAX + 1) == ARRAYSIZEOF(get_merge_tables), "get merge table func cnt is mismatch");

  int ret = OB_SUCCESS;
  const int64_t tablet_table_cnt = tablet.get_major_table_count() + tablet.get_minor_table_count();
  const int64_t last_major_snapshot_version = tablet.get_last_major_snapshot_version();

  if (OB_UNLIKELY(tablet.is_ls_inner_tablet())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for special tablet", K(ret), K(tablet));
  } else if (check_table_cnt && tablet_table_cnt >= OB_UNSAFE_TABLE_CNT) {
    min_snapshot = tablet_table_cnt >= OB_EMERGENCY_TABLE_CNT ? 0 : last_major_snapshot_version;
    max_snapshot = INT64_MAX;
  } else {
    const int64_t merge_inc_base_version = tablet.get_snapshot_version();

    ObTenantFreezeInfoMgr::NeighbourFreezeInfo freeze_info;
    if (OB_SUCC(MTL(ObTenantFreezeInfoMgr *)->get_neighbour_major_freeze(merge_inc_base_version, freeze_info))) {
      // do nothing
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get neighbour major freeze info", K(ret), K(merge_inc_base_version));
    } else {
      ret = OB_SUCCESS;
      freeze_info.prev.frozen_scn_.set_min();
      freeze_info.next.frozen_scn_.set_max();
    }

    if (OB_SUCC(ret)) {
      min_snapshot = max(last_major_snapshot_version, freeze_info.prev.frozen_scn_.get_val_for_tx());
      max_snapshot = (freeze_info.next.frozen_scn_.is_max() && is_multi_version_merge)
                   ? MTL(ObTenantFreezeInfoMgr *)->get_snapshot_gc_ts()
                   : freeze_info.next.frozen_scn_.get_val_for_tx();
    }

    int64_t max_medium_scn = 0;
    ObArenaAllocator allocator("GetMediumList", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    const compaction::ObMediumCompactionInfoList *medium_list = nullptr;
    if (FAILEDx(tablet.read_medium_info_list(allocator, medium_list))) {
      LOG_WARN("failed to read medium info list", K(ret), KPC(medium_list));
    } else if (OB_FAIL(compaction::ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
        tablet, *medium_list, max_medium_scn))) {
      LOG_WARN("failed to get max medium snapshot", K(ret), K(tablet));
    } else {
      min_snapshot = max(min_snapshot, max_medium_scn);
    }
    LOG_DEBUG("get boundary snapshot", K(ret), "tablet_id", tablet.get_tablet_meta().tablet_id_, K(tablet),
              K(min_snapshot), K(max_snapshot), K(merge_inc_base_version),
              K(max_medium_scn), K(last_major_snapshot_version), K(freeze_info));
  }
#ifdef ERRSIM
  if (OB_FAIL(ret)) {
  } else if (EN_COMPACTION_MINOR_ALL) {
    FLOG_INFO("ERRSIM EN_COMPACTION_MINOR_ALL", KR(ret));
    min_snapshot = 0;
    max_snapshot = INT64_MAX;
  }
#endif
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
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      minor_compact_trigger = tenant_config->minor_compact_trigger;
    }
  }

  if (OB_FAIL(tablet.get_mini_minor_sstables(minor_table_iter))) {
    LOG_WARN("failed to get mini  minor sstables", K(ret));
  } else if (minor_table_iter.count() <= MAX(minor_compact_trigger, 1)) {
    ret = OB_NO_NEED_MERGE;
  } else {
    ObSSTable *table = nullptr;
    bool found_greater = false;

    // Tables split into 3 parts, to avoid minors acrossed major table
    // |........    ..........|........     .........|.........
    //  (candidates1)      min_scn (candidates2)  max_scn  (candidates3)

    bool trans_version_not_filled_in_prev = false;
    bool contain_uncommitted_row_in_prev = false;
    bool contain_uncommitted_row_in_next = false;
    ObTablesHandleArray prev_major_table_candidates;
    ObTablesHandleArray next_major_table_candidates;
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
      } else if (!found_greater
                 && (table->get_upper_trans_version() <= min_snapshot_version ||
                     (1 < minor_compact_trigger && table->get_max_merged_trans_version() <= min_snapshot_version && table->get_max_merged_trans_version() != 0))) {
        /* 1. upper trans ver <= min snapshot, should do hist minor merge
         * 2. max merged trans ver <= min snapshot < upper trans ver:
         *   2.1. no uncommited contained, upper trans ver != MAX, table crosses the snapshot, cannot merge
         *   2.2. uncommited contained, upper trans ver == MAX:
         *     2.2.1. after filled, upper trans ver <= min snapshot, should do hist minor merge
         *     2.2.2. after filled, upper trans ver > min snapshot, cross the snapshot, cannot merge
         *     2.2.3. not filled, upper trans ver(INT64_MAX) > min snapshot, cross the snapshot, cannot merge
         */
        if (table->get_upper_trans_version() > min_snapshot_version || !prev_major_table_candidates.empty()) {
          if (OB_FAIL(prev_major_table_candidates.add_table(cur_table_handle))) {
            LOG_WARN("failed to add table", K(ret));
          } else {
            contain_uncommitted_row_in_prev = contain_uncommitted_row_in_prev || table->contain_uncommitted_row();
            trans_version_not_filled_in_prev = trans_version_not_filled_in_prev || (table->get_upper_trans_version() == INT64_MAX);
          }
        }
        continue;
      } else if (!found_greater && 1 < minor_compact_trigger && table->get_start_scn().get_val_for_tx() < min_snapshot_version) {
        // start scn < min snapshot < max merged trans ver < upper trans ver, maybe cross the snapshot, cannot merge
        if (OB_FAIL(prev_major_table_candidates.add_table(cur_table_handle))) {
          LOG_WARN("failed to add table", K(ret));
        } else {
          contain_uncommitted_row_in_prev = contain_uncommitted_row_in_prev || table->contain_uncommitted_row();
          trans_version_not_filled_in_prev = trans_version_not_filled_in_prev || (table->get_upper_trans_version() == INT64_MAX);
        }
        continue;
      } else {
        found_greater = true;
        if (0 == next_major_table_candidates.get_count()) {
        } else if (is_history_minor_merge(param.merge_type_) && table->get_upper_trans_version() > max_snapshot_version) {
          break;
        } else if (tablet.get_minor_table_count() + tablet.get_major_table_count() < OB_UNSAFE_TABLE_CNT &&
            table->get_max_merged_trans_version() > max_snapshot_version) {
          LOG_INFO("max_snapshot_version reached, stop find more tables", K(param), K(max_snapshot_version), KPC(table));
          break;
        }
        if (OB_FAIL(next_major_table_candidates.add_table(cur_table_handle))) {
          LOG_WARN("failed to add table", K(ret));
        } else {
          contain_uncommitted_row_in_next = contain_uncommitted_row_in_next || table->contain_uncommitted_row();
        }
      }
    }

    ObTablesHandleArray *minor_merge_candidates = &next_major_table_candidates;
    if (prev_major_table_candidates.get_count() > next_major_table_candidates.get_count()) {
      minor_merge_candidates = &prev_major_table_candidates;
    } else if (prev_major_table_candidates.get_count() == next_major_table_candidates.get_count()) {
      if (trans_version_not_filled_in_prev) {
        minor_merge_candidates = &prev_major_table_candidates;
      } else if (!contain_uncommitted_row_in_next && contain_uncommitted_row_in_prev) {
        minor_merge_candidates = &prev_major_table_candidates;
      }
    }

    if (FAILEDx(refine_and_get_minor_merge_result(param, tablet, minor_compact_trigger, *minor_merge_candidates, result))) {
      if (OB_NO_NEED_MERGE != ret) {
        LOG_WARN("refine and get minor merge result fail", K(ret));
      }
    }
  }
  int64_t diagnose_table_cnt = DIAGNOSE_TABLE_CNT_IN_STORAGE;
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_CAN_NOT_SCHEDULE_MINOR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_CAN_NOT_SCHEDULE_MINOR", K(ret));
      diagnose_table_cnt = 2;
    }
  }
#endif
  if (OB_SUCC(ret)) {
    DEL_SUSPECT_INFO(MINOR_MERGE,
                    tablet.get_tablet_meta().ls_id_, tablet.get_tablet_meta().tablet_id_,
                    ObDiagnoseTabletType::TYPE_MINOR_MERGE);
    if (OB_FAIL(deal_with_minor_result(param.merge_type_, ls, tablet, result))) {
      LOG_WARN("Failed to deal with minor merge result", K(ret), K(param), K(result));
    } else {
      LOG_TRACE("succeed to get minor merge tables", K(min_snapshot_version), K(max_snapshot_version),
                K(result), K(tablet));
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

int ObPartitionMergePolicy::get_co_major_minor_merge_tables(
    const ObStorageSchema *storage_schema,
    const int64_t merge_version,
    const int64_t minor_start_pos,
    const ObTablesHandleArray &input_tables,
    ObIArray<ObTableHandleV2> &output_tables)
{
  int ret = OB_SUCCESS;
  output_tables.reset();

  if (OB_UNLIKELY(nullptr == storage_schema || !storage_schema->is_valid() || input_tables.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(storage_schema), K(input_tables));
  } else if (storage_schema->get_column_group_count() > SCHEDULE_CO_MAJOR_MINOR_CG_CNT_THREASHOLD
          && input_tables.get_count() - minor_start_pos > SCHEDULE_CO_MAJOR_MINOR_TRIGGER) {
    int64_t big_minor_table_cnt = 0;

    for (int64_t idx = minor_start_pos; OB_SUCC(ret) && idx < input_tables.get_count(); ++idx) {
      ObTableHandleV2 cur_table_hdl;
      ObSSTable *sstable = nullptr;

      if (OB_FAIL(input_tables.get_table(idx, cur_table_hdl))) {
        LOG_WARN("failed to get table", K(ret), K(idx));
      } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(cur_table_hdl.get_table()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null sstable", K(ret), K(idx), K(cur_table_hdl));
      } else if (sstable->get_upper_trans_version() > merge_version) {
        break;
      } else if (OB_FAIL(output_tables.push_back(cur_table_hdl))) {
        LOG_WARN("failed to push back minor table", K(ret), K(idx), KPC(sstable));
      } else if (sstable->get_row_count() >= SCHEDULE_CO_MAJOR_MINOR_ROW_CNT_THREASHOLD) {
        ++big_minor_table_cnt;
      }
    }

    if (OB_FAIL(ret) || big_minor_table_cnt <= 1) {
      output_tables.reset();
    }
  }

  if (FAILEDx(schedule_co_major_minor_errsim(input_tables, minor_start_pos, output_tables))) {
    LOG_WARN("failed to schedule co major minor errsim", K(ret));
  }
  return ret;
}

int ObPartitionMergePolicy::schedule_co_major_minor_errsim(
    const ObTablesHandleArray &input_tables,
    const int64_t minor_start_pos,
    ObIArray<ObTableHandleV2> &output_tables)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  bool schedule_minor = false;
  #define SCHEDULE_MINOR_ERRSIM(tracepoint)                            \
    do {                                                               \
      if (OB_SUCC(ret)) {                                              \
        ret = OB_E((EventTable::tracepoint)) OB_SUCCESS;               \
        if (OB_FAIL(ret)) {                                            \
          ret = OB_SUCCESS;                                            \
          STORAGE_LOG(INFO, "ERRSIM " #tracepoint);                    \
          schedule_minor = input_tables.get_count() > minor_start_pos; \
        }                                                              \
      }                                                                \
    } while(0);

  SCHEDULE_MINOR_ERRSIM(EN_SWAP_TABLET_IN_COMPACTION);
  SCHEDULE_MINOR_ERRSIM(EN_COMPACTION_SCHEDULE_MINOR_FAIL);
  SCHEDULE_MINOR_ERRSIM(EN_COMPACTION_CO_MERGE_SCHEDULE_FAILED);

  if (EN_CO_MERGE_WITH_MINOR) {
    STORAGE_LOG(INFO, "ERRSIM EN_CO_MERGE_WITH_MINOR");
    SERVER_EVENT_SYNC_ADD("merge_errsim", "co_merge_with_minor", "ret_code", ret);
    schedule_minor = input_tables.get_count() > 1;
  }

  if (schedule_minor && output_tables.empty()) {
    for (int64_t i = minor_start_pos; OB_SUCC(ret) && i < input_tables.get_count(); ++i) {
      ObTableHandleV2 cur_table_hdl;
      if (OB_FAIL(input_tables.get_table(i, cur_table_hdl))) {
        LOG_WARN("failed to get table", K(ret), K(i));
      } else if (OB_FAIL(output_tables.push_back(cur_table_hdl))) {
        LOG_WARN("failed to push back minor table", K(ret), K(i), K(cur_table_hdl));
      }
    }
  }
#endif
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
  if (tablet_id.is_special_merge_tablet()) {
  } else if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (FALSE_IT(meta_table = table_store_wrapper.get_member()->get_meta_major_sstable())) {
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
  int64_t hist_threshold = 0;
  ObITable *first_major = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  ObSEArray<share::ObFreezeInfo, 4> freeze_infos;

  if (FALSE_IT(hist_threshold = cal_hist_minor_merge_threshold(tablet.is_tablet_referenced_by_collect_mv()))) {
  } else if (OB_FAIL(tablet.fetch_table_store(wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_UNLIKELY(!wrapper.get_member()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected invalid table store", K(ret), K(wrapper));
  } else if (wrapper.get_member()->get_minor_sstables().count() < hist_threshold) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_ISNULL(first_major = wrapper.get_member()->get_major_sstables().get_boundary_table(false))) {
    // index table during building, need compat with continuous multi version
    if (0 == (max_snapshot_version = MTL(ObTenantFreezeInfoMgr*)->get_latest_frozen_version())) {
      ret = OB_NO_NEED_MERGE; // no freeze info, need to do normal minor merge
      LOG_WARN("No freeze range to do hist minor merge for buiding index", K(ret), K(PRINT_TS_WRAPPER(wrapper)));
    }
  } else if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_freeze_info_behind_major_snapshot(first_major->get_snapshot_version(), true/*include_equal*/, freeze_infos))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_NO_NEED_MERGE;
    } else {
      LOG_WARN("Failed to get freeze infos behind major version", K(ret), KPC(first_major));
    }
  } else if (freeze_infos.count() <= 1 && 1 == wrapper.get_member()->get_major_sstables().count()) {
    ret = OB_NO_NEED_MERGE; // only one freeze info, need to do normal minor merge
    LOG_TRACE("No enough freeze info to do hist minor merge", K(ret), K(freeze_infos));
  } else {
    int64_t table_cnt = 0;
    int64_t min_snapshot = 0;
    int64_t placeholder = 0;
    if (OB_FAIL(get_boundary_snapshot_version(tablet, min_snapshot, placeholder, true/*check_table_cnt*/, true/*is_multi_version_merge*/))) {
      LOG_WARN("fail to calculate boundary version", K(ret));
    }

    const ObSSTableArray &minor_tables = wrapper.get_member()->get_minor_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
      if (OB_ISNULL(minor_tables[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("table must not null", K(ret), K(i), K(wrapper));
      } else if (minor_tables[i]->get_upper_trans_version() <= min_snapshot) {
        table_cnt++;
      } else {
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (table_cnt <= 1) {
      ret = OB_NO_NEED_MERGE;
    } else if (freeze_infos.count() <= 1) {
      if (ObTimeUtility::fast_current_time() - tablet.get_multi_version_start() / 1000 < 60_min) {
        if (table_cnt < hist_threshold * 2) {
          ret = OB_NO_NEED_MERGE;
        }
      } else if (table_cnt < hist_threshold) {
        ret = OB_NO_NEED_MERGE;
      }
    }
    if (OB_SUCC(ret)) {
      max_snapshot_version = min_snapshot;
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
             && ((GCTX.is_shared_storage_mode() && !tablet.get_tablet_id().is_only_mini_merge_tablet())
                 || !tablet.get_tablet_id().is_special_merge_tablet())) {
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
    const bool is_tablet_referenced_by_collect_mv,
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
  } else if (0 == minor_compact_trigger || result.handle_.get_count() >= OB_UNSAFE_TABLE_CNT || is_tablet_referenced_by_collect_mv) {
    // no refine
  } else {
    ObTablesHandleArray mini_tables;
    ObITable *table = NULL;
    ObSSTable *sstable = NULL;
    int64_t large_sstable_cnt = 0;
    int64_t large_sstable_row_cnt = 0;
    int64_t mini_sstable_row_cnt = 0;

    int64_t write_amplification_threshold = OB_LARGE_MINOR_SSTABLE_ROW_COUNT;
    int64_t size_amplification_factor = OB_DEFAULT_COMPACTION_AMPLIFICATION_FACTOR;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      if (tenant_config.is_valid()) {
        write_amplification_threshold = tenant_config->_minor_merge_write_amplification_threshold;
        if (int64_t(tenant_config->_minor_compaction_amplification_factor) > 0) {
          size_amplification_factor = tenant_config->_minor_compaction_amplification_factor;
        }
      }
    }
#ifdef ERRSIM
if (OB_SUCC(ret)) {
  int tmp_ret = OB_E(EventTable::EN_MV_LARGE_SSTABLE_THRESHOLD) OB_SUCCESS;
  if (tmp_ret) {
    write_amplification_threshold = 10;
  }
}
#endif

    for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
      ObTableHandleV2 tmp_table_handle;
      if (OB_FAIL(result.handle_.get_table(i, tmp_table_handle))) {
        LOG_WARN("failed to get table from handles array", K(ret), K(i));
      } else if (OB_ISNULL(table = tmp_table_handle.get_table()) || !table->is_minor_sstable()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("get unexpected table", KP(table), K(ret));
      } else if (FALSE_IT(sstable = reinterpret_cast<ObSSTable*>(table))) {
      } else {
        if (sstable->get_row_count() > write_amplification_threshold) { // large sstable
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

    if (OB_FAIL(ret)) {
    } else if (large_sstable_cnt > 1
        || mini_sstable_row_cnt > (large_sstable_row_cnt * size_amplification_factor / 100)) {
      // no refine, use current result to compaction
    } else if (mini_tables.get_count() <= minor_compact_trigger) {
      ret = OB_NO_NEED_MERGE;
      result.reset();
      LOG_TRACE("small mini count not reach compact trigger", K(ret), K(large_sstable_row_cnt), K(size_amplification_factor), K(write_amplification_threshold));
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
            result.rec_scn_ = tmp_table_handle.get_table()->get_rec_scn();
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

int64_t ObPartitionMergePolicy::cal_hist_minor_merge_threshold(const bool is_tablet_referenced_by_collect_mv)
{
  int64_t compact_trigger = DEFAULT_MINOR_COMPACT_TRIGGER;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    compact_trigger = tenant_config->minor_compact_trigger;
  }
  if (!is_tablet_referenced_by_collect_mv) {
    compact_trigger = MIN((1 + compact_trigger) * OB_HIST_MINOR_FACTOR, MAX_TABLE_CNT_IN_STORAGE / 2);
  }
  return compact_trigger;
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
    // Minor Merge need read medium list to decide boundary snapshot and multi version start.
    // Bug when ls is migrating, data is not complete and mds data can not be read.
    // So if the sstable cnt is unsafe, a emergency minor should be scheduled.
    const bool need_emergency_minor = OB_EAGAIN == ret && is_minor_merge_type(merge_type)
                                   && !tablet.get_tablet_meta().ha_status_.is_data_status_complete()
                                   && (tablet.get_major_table_count() + tablet.get_minor_table_count()) > OB_UNSAFE_TABLE_CNT;
    if (is_mini_merge(merge_type) || OB_TENANT_NOT_EXIST == ret || need_emergency_minor) {
      snapshot_info.reset();
      snapshot_info.snapshot_type_ = ObStorageSnapshotInfo::SNAPSHOT_MULTI_VERSION_START_ON_TABLET;
      snapshot_info.snapshot_ = tablet.get_multi_version_start();
      FLOG_INFO("failed to get multi_version_start, use multi_version_start on tablet", K(ret),
          "merge_type", merge_type_to_str(merge_type), K(snapshot_info), K(need_emergency_minor));
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
      result.rec_scn_ = table_handle.get_table()->get_rec_scn();
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
  "CRAZY_MEDIUM_FOR_TEST",
  "NO_INC_DATA",
  "DURING_DDL",
  "RECYCLE_TRUNCATE_INFO",
  "TOO_MANY_INC_MAJOR"
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

bool ObAdaptiveMergePolicy::is_user_request_merge_reason(const AdaptiveMergeReason &reason)
{
  return ObAdaptiveMergePolicy::REBUILD_COLUMN_GROUP == reason
    || ObAdaptiveMergePolicy::USER_REQUEST == reason
    || ObAdaptiveMergePolicy::CRAZY_MEDIUM_FOR_TEST == reason;
}

bool ObAdaptiveMergePolicy::is_valid_compaction_policy(const AdaptiveCompactionPolicy &policy)
{
  return policy >= AdaptiveCompactionPolicy::NORMAL &&
         policy < AdaptiveCompactionPolicy::INVALID_POLICY;
}

bool ObAdaptiveMergePolicy::is_skip_merge_reason(const AdaptiveMergeReason &reason)
{
  return reason == AdaptiveMergeReason::NO_INC_DATA ||
         reason == AdaptiveMergeReason::DURING_DDL;
}

bool ObAdaptiveMergePolicy::is_recycle_truncate_info_merge_reason(const AdaptiveMergeReason &reason)
{
  return AdaptiveMergeReason::RECYCLE_TRUNCATE_INFO == reason;
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

bool ObAdaptiveMergePolicy::need_schedule_meta(const AdaptiveCompactionEvent& event)
{
  return AdaptiveCompactionEvent::SCHEDULE_META == event
      || AdaptiveCompactionEvent::SCHEDULE_AFTER_MINI == event;
}

bool ObAdaptiveMergePolicy::need_schedule_medium(const AdaptiveCompactionEvent& event)
{
  return AdaptiveCompactionEvent::SCHEDULE_MEDIUM == event
      || AdaptiveCompactionEvent::SCHEDULE_AFTER_MINI == event;
}


// TODO(@DanLing) refactor this logic
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
  } else if (FALSE_IT(result.rec_scn_.set_min())) { // major sstable's rec scn is min
  } else if (OB_FAIL(result.handle_.check_continues(nullptr))) {
    LOG_WARN("failed to check continues", K(ret), K(result));
  } else if (!is_medium_merge(merge_type)) {
    // only medium merge needs to fix the snapshot version and multi version start
  } else if (FALSE_IT(result.version_range_.snapshot_version_ = MIN(tablet.get_snapshot_version(), result.version_range_.snapshot_version_))) {
  } else if (OB_FAIL(ObPartitionMergePolicy::get_multi_version_start(merge_type, ls, tablet, result.version_range_, result.snapshot_info_))) {
    LOG_WARN("failed to get multi version_start", K(ret));
  }

  if (FAILEDx(tablet.get_private_transfer_epoch(result.private_transfer_epoch_))) {
    LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet.get_tablet_meta());
  } else {
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
  } else if (OB_FAIL(result.handle_.add_sstable(base_table, table_store_wrapper.get_meta_handle()))) {
    LOG_WARN("failed to add table", K(ret), KPC(base_table));
  } else {
    int64_t result_snapshot_version = base_table->get_snapshot_version();
    int64_t base_inc_row_cnt = base_table->get_row_count();
    int64_t tx_determ_table_cnt = 1;
    int64_t inc_row_cnt = 0;
    const ObSSTableArray &inc_major_tables = table_store->get_inc_major_sstables();

    for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_tables.count(); ++i) {
      ObSSTable *table = inc_major_tables[i];
      if (OB_UNLIKELY(NULL == table || !table->is_inc_major_type_sstable() || INT64_MAX == table->get_upper_trans_version())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected table", K(ret), KPC(table), K(PRINT_TS_WRAPPER(table_store_wrapper)));
      } else if (table->get_upper_trans_version() <= base_table->get_snapshot_version()) {
        continue;
      } else if (is_meta_major_merge(merge_type) && table->get_upper_trans_version() > tablet.get_snapshot_version()) {
       // meta merge: the tablet snapshot which will be the snapshot of meta merge can be used to select inc major candidates.
        break;
      } else if (OB_FAIL(result.handle_.add_sstable(table, table_store_wrapper.get_meta_handle()))) {
        LOG_WARN("failed to add inc major table to meta merge result", K(ret));
      } else {
        result_snapshot_version = MAX(table->get_upper_trans_version(), result_snapshot_version);
        base_inc_row_cnt += table->get_row_count();
      }
    }

    const ObSSTableArray &minor_tables = table_store->get_minor_sstables();
    bool found_undeterm_table = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_tables.count(); ++i) {
      ObSSTable *table = minor_tables[i];
      if (OB_UNLIKELY(NULL == table || !table->is_multi_version_minor_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected table", K(ret), K(i), K(PRINT_TS_WRAPPER(table_store_wrapper)));
      } else if (result.handle_.get_count() <= 1 && table->get_upper_trans_version() <= base_table->get_snapshot_version()) {
        continue; // skip minor sstable which has been merged
      } else if (!table->is_trans_state_deterministic()) {
        found_undeterm_table = true;
      } else if (!found_undeterm_table) {
        ++tx_determ_table_cnt;
        inc_row_cnt += table->get_row_count();
      }

      if (FAILEDx(result.handle_.add_sstable(table, table_store_wrapper.get_meta_handle()))) {
        LOG_WARN("failed to add inc major table to meta merge result", K(ret));
      } else if (found_undeterm_table) {
        // do nothing
      } else {
        result_snapshot_version = MAX(table->get_max_merged_trans_version(), result_snapshot_version);
        result.scn_range_.end_scn_ = table->get_end_scn();
      }
    } // end for

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
        if (REACH_THREAD_TIME_INTERVAL(30_s) || cfg.is_queuing_mode()) {
          LOG_INFO("no enough table or no enough rows for meta merge", K(ret),
              K(scanty_tx_determ_table), K(scanty_inc_row_cnt),  K(inc_row_cnt), K(base_inc_row_cnt), K(result), K(PRINT_TS_WRAPPER(table_store_wrapper)));
        }
      }
    }

    if (OB_SUCC(ret)) {
      result.version_range_.base_version_ = 0;
      result.version_range_.multi_version_start_ = tablet.get_multi_version_start();
      result.version_range_.snapshot_version_ = is_meta_major_merge(merge_type) ? tablet.get_snapshot_version() : result_snapshot_version;

      if (result.version_range_.snapshot_version_ < tablet.get_multi_version_start() ||
          result.version_range_.snapshot_version_ <= base_table->get_snapshot_version()) {
        ret = OB_NO_NEED_MERGE;
        if (REACH_THREAD_TIME_INTERVAL(30_s)) {
          LOG_INFO("chosen snapshot is abandoned", K(ret), K(result), K(tablet.get_multi_version_start()), KPC(base_table));
        }
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

int ObAdaptiveMergePolicy::get_adaptive_merge_reason(
    ObTablet &tablet,
    AdaptiveMergeReason &reason,
    int64_t &least_medium_snapshot)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool crazy_medium_flag = false;
  int64_t truncate_info_count = 0;
  int64_t truncate_newest_commit_version = 0;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;

  reason = AdaptiveMergeReason::NONE;
  ObTabletStatAnalyzer tablet_analyzer;
  ObArenaAllocator tmp_allocator;
  ObStorageSchema *schema_on_tablet = nullptr;
  const ObRowkeyReadInfo *read_info = static_cast<const ObRowkeyReadInfo *>(&tablet.get_rowkey_read_info());
  bool read_truncate_info_flag = false;

  if (tablet_id.is_special_merge_tablet()) {
    // do nothing
  } else if (OB_FAIL(MTL(ObTenantTabletStatMgr *)->get_tablet_analyzer(ls_id, tablet_id, tablet_analyzer))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get tablet analyzer stat", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_TMP_FAIL(check_incremental_table(tablet, reason))) {
      LOG_WARN("failed to check sstable data situation", K(tmp_ret), K(ls_id), K(tablet_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_TMP_FAIL(check_adaptive_merge_reason(tablet, tablet_analyzer, reason))) {
    LOG_WARN("failed to check adaptive merge reason", K(tmp_ret), K(ls_id), K(tablet_id));
  }

  if (OB_FAIL(ret) || AdaptiveMergeReason::NONE != reason || nullptr == read_info) {
  } else if (read_info->is_global_index_valid()) {
    read_truncate_info_flag = read_info->is_global_index_table();
  } else if (OB_FAIL(tablet.load_storage_schema(tmp_allocator, schema_on_tablet))) {
    LOG_WARN("failed to load storage schema", K(ret), K(ls_id), K(tablet_id));
  } else {
    read_truncate_info_flag = schema_on_tablet->is_global_index_table();
  }
  if (OB_FAIL(ret) || !read_truncate_info_flag) {
  } else if (OB_TMP_FAIL(tablet.get_truncate_info_newest_version(truncate_newest_commit_version, truncate_info_count))) {
    LOG_WARN("failed to get truncate info range", KR(tmp_ret));
  } else if (truncate_info_count > 0) {
    reason = AdaptiveMergeReason::RECYCLE_TRUNCATE_INFO;
    least_medium_snapshot = truncate_newest_commit_version;
    LOG_INFO("[TRUNCATE INFO]success to get adaptive merge reason", KR(tmp_ret), K(ls_id), K(tablet_id), K(reason), K(least_medium_snapshot));
  }
  if (OB_NOT_NULL(schema_on_tablet)) {
    schema_on_tablet->~ObStorageSchema();
    tmp_allocator.free(schema_on_tablet);
    schema_on_tablet = nullptr;
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

  if (REACH_THREAD_TIME_INTERVAL(10 * 1000 * 1000 /*10s*/)) {
    LOG_INFO("Check tablet adaptive merge reason", K(ret), K(ls_id), K(tablet_id),  K(reason), K(tablet_analyzer));
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

#define ADD_INC_ROW_COUNT(sstable_array)                            \
  for (int i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) { \
    ObSSTable *sstable = nullptr;                                   \
    if (OB_ISNULL(sstable = sstable_array.at(i))) {                 \
      ret = OB_ERR_UNEXPECTED;                                      \
      LOG_WARN("sstable is null", K(ret), K(i));                    \
    } else {                                                        \
      inc_row_count += sstable->get_row_count();                    \
    }                                                               \
  }

int ObAdaptiveMergePolicy::check_incremental_table(
    const ObTablet &tablet,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  int64_t base_row_count = 0;
  int64_t inc_row_count = 0;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObTabletTableStore *table_store = table_store_wrapper.get_member();
    ObSSTable *last_major = static_cast<ObSSTable *>(table_store->get_major_sstables().get_boundary_table(true));
    base_row_count = (nullptr == last_major) ? 0 : last_major->get_row_count();

    ADD_INC_ROW_COUNT(table_store->get_inc_major_sstables());
    ADD_INC_ROW_COUNT(table_store->get_minor_sstables());

    if (OB_FAIL(ret)) {
    } else if ((inc_row_count > INC_ROW_COUNT_THRESHOLD)
            || (base_row_count > BASE_ROW_COUNT_THRESHOLD &&
               (inc_row_count * 100 / base_row_count) > LOAD_DATA_SCENE_THRESHOLD)) {
      reason = AdaptiveMergeReason::FREQUENT_WRITE;
    } else {
      const ObSSTableArray &inc_major_sstables = table_store->get_inc_major_sstables();
      int64_t unmerged_inc_major_count = 0;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < inc_major_sstables.count(); ++idx) {
        ObSSTable *inc_major = inc_major_sstables[idx];
        if (OB_ISNULL(inc_major) || !inc_major->is_inc_major_type_sstable()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid inc major sstable", K(ret), K(idx), KPC(inc_major));
        } else if (inc_major->get_upper_trans_version() > last_major->get_snapshot_version()) {
          ++unmerged_inc_major_count;
        }
      }
      if (unmerged_inc_major_count >= ObTabletTableStore::EMERGENCY_INC_MAJOR_TABLE_CNT) {
        reason = AdaptiveMergeReason::TOO_MANY_INC_MAJOR;
      }
    }
  }

  LOG_DEBUG("check_sstable_data_situation", K(ret), K(ls_id), K(tablet_id), K(reason), K(base_row_count), K(inc_row_count));
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
  } else if (analyzer.is_hot_tablet() && analyzer.has_frequent_slow_query()) {
    reason = AdaptiveMergeReason::INEFFICIENT_QUERY;
  }
  LOG_DEBUG("check_ineffecient_read", K(ret), K(reason), K(analyzer));
  return ret;
}

int ObAdaptiveMergePolicy::check_adaptive_merge_reason(
    const storage::ObTablet &tablet,
    const ObTabletStatAnalyzer &tablet_analyzer,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_ls_id();
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  if (OB_TMP_FAIL(check_tombstone_situation(tablet_analyzer, reason))) {
    LOG_WARN("failed to check tombstone scene", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
  }
  if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_load_data_situation(tablet_analyzer, reason))) {
    LOG_WARN("failed to check load data scene", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
  }
  if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_ineffecient_read(tablet_analyzer, reason))) {
    LOG_WARN("failed to check ineffecient read", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
  }
  if (AdaptiveMergeReason::NONE == reason && OB_TMP_FAIL(check_incremental_table(tablet, reason))) {
    LOG_WARN("failed to check sstable data situation", K(tmp_ret), K(ls_id), K(tablet_id), K(tablet_analyzer));
  }
  return ret;
}

int ObAdaptiveMergePolicy::check_adaptive_merge_reason_for_event(
    const storage::ObLS &ls,
    const storage::ObTablet &tablet,
    const AdaptiveCompactionEvent &event,
    const int64_t update_row_cnt,
    const int64_t delete_row_cnt,
    ObTableModeFlag &mode,
    AdaptiveMergeReason &reason)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  mode = ObTableModeFlag::TABLE_MODE_NORMAL;
  reason = AdaptiveMergeReason::NONE;
  ObTabletStatAnalyzer tablet_analyzer;

  if (OB_UNLIKELY(!need_schedule_meta(event))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compaction event", K(ret), K(event));
  } else if (tablet_id.is_special_merge_tablet()) {
    // do nothing
  } else if (SCHEDULE_META == event && (ls.is_cs_replica() || !tablet.is_row_store())) {
    // column store tablet not suppoert meta compaction
  } else if (OB_FAIL(MTL(ObTenantTabletStatMgr *)->get_tablet_analyzer(ls_id, tablet_id, tablet_analyzer))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get tablet analyzer stat", K(ret), K(ls_id), K(tablet_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (FALSE_IT(mode = tablet_analyzer.mode_)) {
  } else if (SCHEDULE_META == event && mode != TABLE_MODE_QUEUING_EXTREME) {
    // backgroud meta only scheduled for extreme table
  } else {
    const ObTableQueuingModeCfg &queuing_cfg = ObTableQueuingModeCfg::get_basic_config(mode);
    const int64_t adaptive_threshold = TOMBSTONE_ROW_COUNT_THRESHOLD * queuing_cfg.queuing_factor_;
    if ((update_row_cnt + delete_row_cnt) >= adaptive_threshold  || delete_row_cnt > queuing_cfg.total_delete_row_cnt_) {
      reason = AdaptiveMergeReason::TOMBSTONE_SCENE;
    } else if (queuing_cfg.is_queuing_mode()) {
      if (OB_TMP_FAIL(check_adaptive_merge_reason(tablet, tablet_analyzer, reason))) {
        LOG_WARN("failed to check adaptive merge reason", K(tmp_ret), K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}

/*************************************** ObCOMajorMergePolicy ***************************************/
const char * ObCOMajorMergeTypeStr[] = {
  "INVALID_CO_MAJOR_MERGE_TYPE",
  "BUILD_COLUMN_STORE_MERGE",
  "BUILD_ROW_STORE_MERGE",
  "USE_RS_BUILD_SCHEMA_MATCH_MERGE",
  "BUILD_REDUNDANT_ROW_STORE_MERGE",
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
  major_sstable_status = ObCOMajorSSTableStatus::INVALID_CO_MAJOR_SSTABLE_STATUS;

  if (storage_schema.has_all_column_group()) {
    if (co_sstable.is_row_store_only_co_table()) {
      major_sstable_status = ObCOMajorSSTableStatus::COL_ONLY_ALL;
    } else if (co_sstable.is_rowkey_cg_base()) {
      major_sstable_status = ObCOMajorSSTableStatus::PURE_COL_WITH_ALL;
    } else {
      major_sstable_status = ObCOMajorSSTableStatus::COL_WITH_ALL;
    }
  } else {
    if (co_sstable.is_row_store_only_co_table()) {
      major_sstable_status = ObCOMajorSSTableStatus::PURE_COL_ONLY_ALL;
    } else {
      major_sstable_status = ObCOMajorSSTableStatus::PURE_COL;
    }
  }

  if (OB_UNLIKELY(!is_valid_co_major_sstable_status(major_sstable_status))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to decide co major sstable status", K(ret), K(major_sstable_status),
      "cg_schemas", storage_schema.get_column_groups(), K(co_sstable));
  } else {
    LOG_DEBUG("success to decide co major sstable status", K(ret), K(major_sstable_status));
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

int ObCOMajorMergePolicy::accumulate_physical_row_cnt(
    const ObIArray<ObITable *> &tables,
    int64_t &physical_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t count = tables.count();
  physical_row_cnt = 0;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_COMPACTION_ESTIMATE_ROW_FAILED) ret;
  if (OB_FAIL(ret)) {
    LOG_INFO("ERRSIM EN_COMPACTION_ESTIMATE_ROW_FAILED", K(ret));
  }
#endif

  for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
    const ObITable *table = tables.at(idx);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table", K(ret), K(idx), K(tables));
    } else if (OB_UNLIKELY(table->is_empty())) {
    } else if (table->is_sstable()) {
      const ObSSTable *sstable = static_cast<const ObSSTable *>(table);
      physical_row_cnt += sstable->get_row_count();
    } else if (table->is_data_memtable()) {
      const memtable::ObMemtable *memtable = static_cast<const memtable::ObMemtable *>(table);
      // not auccurate, empty_mvcc_row_count_ only consider the latest 5000 rollback operations
      physical_row_cnt += MAX(1, memtable->get_physical_row_cnt() - memtable->get_mt_stat().empty_mvcc_row_count_);
    } else if (table->is_direct_load_memtable()) {
      const ObDDLKV *ddlkv = static_cast<const ObDDLKV *>(table);
      physical_row_cnt += ddlkv->get_row_count();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table type", K(ret), KPC(table));
    }
  }
  return ret;
}

// only use base major to decide co_major_merge_type,
// cause we can't change column store to row store or switch PURE_COL and COL_WITH_ALL now,
// if base major is column store,inc major is always match storage schema (except for the column replica)
int ObCOMajorMergePolicy::decide_co_major_merge_type(
    const ObCOSSTableV2 &co_sstable,
    const ObIArray<ObITable *> &tables,
    const ObStorageSchema &storage_schema,
    ObCOMajorMergeType &major_merge_type)
{
  int ret = OB_SUCCESS;
  ObCOMajorSSTableStatus major_sstable_status = ObCOMajorSSTableStatus::INVALID_CO_MAJOR_SSTABLE_STATUS;
  int64_t physical_row_cnt = 0;
  const ObTabletID &tablet_id = co_sstable.get_key().tablet_id_;

  if (OB_FAIL(decide_co_major_sstable_status(co_sstable, storage_schema, major_sstable_status))) {
    LOG_WARN("failed to decide co major sstable status");
  } else if (OB_UNLIKELY(EN_COMPACTION_DISABLE_ROW_COL_SWITCH)) {
    major_merge_type = is_major_sstable_match_schema(major_sstable_status) ? BUILD_COLUMN_STORE_MERGE : USE_RS_BUILD_SCHEMA_MATCH_MERGE;
    LOG_INFO("[RowColSwitch] disable row col switch, only allow co major merge", K(tablet_id), K(co_sstable), K(tables), K(major_sstable_status), K(major_merge_type));
    SERVER_EVENT_SYNC_ADD("row_col_switch", "disable_row_col_switch", "tablet_id", tablet_id);
  } else if (OB_FAIL(accumulate_physical_row_cnt(tables, physical_row_cnt))) {
    // if accumulate row cnt failed, make major sstable match schema
    major_merge_type = is_major_sstable_match_schema(major_sstable_status) ? BUILD_COLUMN_STORE_MERGE : USE_RS_BUILD_SCHEMA_MATCH_MERGE;
    LOG_WARN("failed to accumulate row count for co major merge, build column store by default", "accumulate_ret", ret, K(major_sstable_status), K(major_merge_type));
    ret = OB_SUCCESS;
  } else {
    int64_t column_cnt = storage_schema.get_column_count();
    bool build_row_store_flag = whether_to_build_row_store(physical_row_cnt, column_cnt);
    if (is_major_sstable_match_schema(major_sstable_status)) {
      if (build_row_store_flag) {
        major_merge_type = BUILD_ROW_STORE_MERGE;
      } else {
        major_merge_type = BUILD_COLUMN_STORE_MERGE;
      }
    } else if (!build_row_store_flag && whether_to_rebuild_column_store(major_sstable_status, physical_row_cnt, column_cnt)) {
      major_merge_type = USE_RS_BUILD_SCHEMA_MATCH_MERGE;
    } else {
      major_merge_type = BUILD_ROW_STORE_MERGE;
    }
    LOG_INFO("[RowColSwitch] finish decide major merge type", K(tablet_id), K(co_sstable), K(tables), K(major_sstable_status), K(major_merge_type), K(physical_row_cnt), K(column_cnt));
  }
  return ret;
}

int ObPartitionMergePolicy::refine_and_get_minor_merge_result(const ObGetMergeTablesParam &param,
                                                              const ObTablet &tablet,
                                                              const int64_t minor_compact_trigger,
                                                              ObTablesHandleArray &minor_merge_candidates,
                                                              ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;

  int64_t left_border = 0;
  int64_t right_border = minor_merge_candidates.get_count();
  if (MINOR_MERGE == param.merge_type_ &&
      OB_FAIL(refine_minor_merge_tables(tablet, minor_merge_candidates, left_border, right_border))) {
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
      LOG_WARN("Failed to add table", K(ret), K(tmp_table_handle));
    } else {
      if (1 == result.handle_.get_count()) {
        result.scn_range_.start_scn_ = tmp_table_handle.get_table()->get_start_scn();
        result.rec_scn_ = tmp_table_handle.get_table()->get_rec_scn();
      }
      result.scn_range_.end_scn_ = tmp_table_handle.get_table()->get_end_scn();
    }
  }
  if (FAILEDx(refine_minor_merge_result(param.merge_type_, minor_compact_trigger, tablet.is_tablet_referenced_by_collect_mv(), result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to refine minor_merge result", K(ret));
    }
  } else {
    result.version_range_.snapshot_version_ = tablet.get_snapshot_version();
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObPartitionMergePolicy::get_ss_minor_merge_tables(
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  result.reset();
  int64_t min_snapshot_version = 0;
  int64_t max_snapshot_version = 0;
  if (tablet.is_ls_inner_tablet()) {
    min_snapshot_version = 0;
    max_snapshot_version = INT64_MAX;
  } else if (OB_FAIL(get_ss_minor_boundary_snapshot_version(ls, tablet, min_snapshot_version, max_snapshot_version))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("fail to calculate boundary version", K(ret));
    }
  }
  // iterate sstables
  ObTablesHandleArray minor_merge_candidates;
  ObTableStoreIterator minor_table_iter;
  if (FAILEDx(tablet.get_mini_minor_sstables(minor_table_iter))) {
    LOG_WARN("failed to get mini  minor sstables", K(ret));
  } else {
    ObSSTable *table = nullptr;
    bool found_greater = false;
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
      } else if (!found_greater) {
        if (table->get_upper_trans_version() == INT64_MAX) {
          found_greater = true;
          share::SCN upper_trans_version = share::SCN::max_scn();
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(ls.get_upper_trans_version_before_given_scn(table->get_end_scn(), upper_trans_version))) {
            LOG_WARN("get upper trans version fail", K(tmp_ret), K(table->get_end_scn()));
          } else if (upper_trans_version.get_val_for_tx() <= min_snapshot_version) {
            found_greater = false;
            LOG_TRACE("skip by upper trans version 1", KPC(table), K(upper_trans_version), K(min_snapshot_version));
          }
        } else if (table->get_upper_trans_version() <= min_snapshot_version) {
          LOG_TRACE("skip by upper trans version 2", KPC(table), K(table->get_upper_trans_version()), K(min_snapshot_version));
        } else {
          found_greater = true;
        }
      }
      if (OB_SUCC(ret) && found_greater) {
        if (0 == minor_merge_candidates.get_count()) {
        } else if (tablet.get_minor_table_count() + tablet.get_major_table_count() < OB_UNSAFE_TABLE_CNT &&
            table->get_max_merged_trans_version() > max_snapshot_version) {
          LOG_INFO("max_snapshot_version reached, stop find more tables", K(max_snapshot_version), KPC(table));
          break;
        }
        if (OB_FAIL(minor_merge_candidates.add_table(cur_table_handle))) {
          LOG_WARN("failed to add table", K(ret));
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
  ObGetMergeTablesParam param;
  param.merge_type_ = MINOR_MERGE;
  if (OB_FAIL(ret)) {
  } else if (minor_merge_candidates.empty()) {
    ret = OB_NO_NEED_MERGE;
#ifdef ERRSIM
  } else if (ERRSIM_DISABLE_MINOR_MERGE && !is_sys_tenant(MTL_ID())) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("Errsim: disable ss minor merge", K(ret), "tenant_id", MTL_ID(), "tablet_id", tablet.get_tablet_meta().tablet_id_);
#endif
  } else if (OB_FAIL(refine_and_get_minor_merge_result(param, tablet, minor_compact_trigger, minor_merge_candidates, result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("refine and get minor merge result fail", K(ret));
    }
  } else if (OB_FAIL(deal_with_ss_minor_result(ls, tablet, result))) {
    LOG_WARN("Failed to deal with minor merge result", K(ret), K(result));
  } else {
    LOG_INFO("succeed to get ss minor merge tables", K(min_snapshot_version), K(max_snapshot_version), K(result), K(tablet));
  }
  return ret;
}

int ObPartitionMergePolicy::get_ss_minor_boundary_snapshot_version(
    ObLS &ls,
    const ObTablet &tablet,
    int64_t &min_snapshot,
    int64_t &max_snapshot)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  const int64_t tablet_table_cnt = tablet.get_major_table_count() + tablet.get_minor_table_count();
  const int64_t last_major_snapshot_version = tablet.get_last_major_snapshot_version();
  if (tablet_table_cnt >= OB_UNSAFE_TABLE_CNT) {
    min_snapshot = tablet_table_cnt >= OB_EMERGENCY_TABLE_CNT ? 0 : last_major_snapshot_version;
    max_snapshot = INT64_MAX;
  } else {
    // get the medium info from local node, if local tablet not exist, the result may incorrect
    // which caused the minor may cross major:
    // if tablet not replayed out, need wait
    // if tablet has been transfered out, just skip minor
    // if tablet has been deleted, just skip minor
    // hence should skip this round of minor merge
    ObTabletHandle local_tablet_handle;
    int64_t local_last_major_snapshot = 0;
    if (OB_FAIL(ls.get_tablet(tablet_id, local_tablet_handle, 10_s, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("local tablet not exist, skip minor merge", K(ret), K(tablet_id));
        ret = OB_NO_NEED_MERGE;
      } else {
        LOG_WARN("get local tablet fail", K(ret), K(tablet_id));
      }
    } else if (FALSE_IT(local_last_major_snapshot = local_tablet_handle.get_obj()->get_last_major_snapshot_version())) {
    } else if (local_last_major_snapshot < last_major_snapshot_version) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("local tablet state is fall behind ss tablet, skip minor merge", K(ret), K(tablet_id),
               "local_last_major_snapshot", local_last_major_snapshot,
               "ss_last_major_snapshot", last_major_snapshot_version);
    } else if (OB_FAIL(get_boundary_snapshot_version(*local_tablet_handle.get_obj(), min_snapshot, max_snapshot, true, true))) {
      LOG_WARN("get local tablet boundary snapshot version fail", K(ret));
    } else {
      LOG_INFO("get boundary snapshot version succ", K(min_snapshot), K(max_snapshot), K(tablet_id));
    }
  }
  return ret;
}

int ObPartitionMergePolicy::deal_with_ss_minor_result(
    ObLS &ls,
    const ObTablet &tablet,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_id();
  if (result.handle_.empty()) {
    ret = OB_NO_NEED_MERGE;
  } else if (OB_UNLIKELY(!result.scn_range_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid argument to check result", K(ret), K(result));
  } else if (OB_FAIL(result.handle_.check_continues(&result.scn_range_))) {
    LOG_WARN("failed to check continues", K(ret), K(result));
  } else if (FALSE_IT(result.snapshot_info_.reset())) {
  } else if (tablet.is_ls_inner_tablet()) {
    result.version_range_.multi_version_start_ = INT64_MAX;
  } else {
    // get multi version start depends on medium info and tablet status in mds table
    // due to the shared tablet can not provide read mds table
    // use local tablet instead, this may caused accurated result, but it is safe
    ObTabletHandle local_tablet_handle;
    if (OB_FAIL(ls.get_tablet(tablet_id, local_tablet_handle, 10_s, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("local tablet not exist, skip minor merge", K(ret), K(tablet_id));
        ret = OB_NO_NEED_MERGE;
      } else {
        LOG_WARN("get local tablet fail", K(ret), K(tablet_id));
      }
    } else if (OB_FAIL(local_tablet_handle.get_obj()->get_kept_snapshot_info(ls.get_min_reserved_snapshot(), result.snapshot_info_))) {
      LOG_WARN("failed to get kept multi_version_start", K(ret), K(tablet_id));
    } else {
      // update multi_version_start
      if (result.snapshot_info_.snapshot_ < result.version_range_.multi_version_start_) {
        LOG_WARN("cannot reserve multi_version_start", K(result.version_range_), K(result.snapshot_info_));
      } else if (result.snapshot_info_.snapshot_ < result.version_range_.snapshot_version_) {
        result.version_range_.multi_version_start_ = result.snapshot_info_.snapshot_;
        LOG_DEBUG("succ reserve multi_version_start", K(result.version_range_), K(result.snapshot_info_));
      } else {
        result.version_range_.multi_version_start_ = result.version_range_.snapshot_version_;
        LOG_DEBUG("no need keep multi version", K(result.snapshot_info_), K(result.version_range_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    result.version_range_.base_version_ = 0;
    if (OB_FAIL(tablet.get_private_transfer_epoch(result.private_transfer_epoch_))) {
      LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet.get_tablet_meta());
    } else if (OB_FAIL(tablet.get_recycle_version(result.version_range_.multi_version_start_, result.version_range_.base_version_))) {
      LOG_WARN("Fail to get table store recycle version", K(ret), K(result.version_range_), K(tablet));
    }
  }
  return ret;
}
#endif

int ObIncMajorTxHelper::get_inc_major_commit_version(
    ObLS &ls,
    const blocksstable::ObSSTable &inc_major_table,
    const share::SCN &read_scn,
    int64_t &trans_state,
    int64_t &commit_version)
{
  int ret = OB_SUCCESS;
  SCN trans_version;
  bool can_read = true;
  ObSSTableMetaHandle meta_hdl;
  const compaction::ObMetaUncommitTxInfo *tx_info = nullptr;

  if (!inc_major_table.is_inc_major_type_sstable() && !inc_major_table.is_inc_major_ddl_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(inc_major_table));
  } else {
    commit_version = inc_major_table.get_upper_trans_version();
  }

  if (OB_FAIL(ret)) {
  } else if (INT64_MAX != commit_version) {
    if (OB_UNLIKELY(inc_major_table.is_inc_major_ddl_dump_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("inc ddl dump sstable get unexpected upper trans version", K(ret), K(inc_major_table)); // tmp error log, convert to warn log later
    } else {
      trans_state = ObTxData::COMMIT;
    }
  } else if (OB_FAIL(inc_major_table.get_meta(meta_hdl))) {
    LOG_WARN("failed to get sstable meta handle", K(ret));
  } else if (FALSE_IT(tx_info = &meta_hdl.get_sstable_meta().get_uncommit_tx_info() )) {
  } else if (OB_UNLIKELY(1 != tx_info->uncommit_tx_desc_count_
                      || 0 == tx_info->tx_infos_[0].sql_seq_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inc major should only has one tx info!", K(ret), KPC(tx_info));
  } else if (OB_FAIL(check_inc_major_trans_can_read(&ls, ObTransID(tx_info->tx_infos_[0].tx_id_),
      ObTxSEQ::cast_from_int(tx_info->tx_infos_[0].sql_seq_), read_scn, trans_state, can_read, trans_version))) {
    LOG_WARN("failed to check inc major trans can read", KR(ret), K(ls), KPC(tx_info));
  } else if (can_read) {
    commit_version = trans_version.get_val_for_tx();
  }
  return ret;
}

int ObIncMajorTxHelper::check_inc_major_trans_can_read(
    ObLS *ls,
    const ObTransID &trans_id,
    const ObTxSEQ &seq_no,
    const share::SCN &read_scn,
    int64_t &trans_state,
    bool &can_read,
    SCN &trans_version)
{
  int ret = OB_SUCCESS;
  can_read = false;
  trans_version.set_max();

  if (OB_ISNULL(ls) || OB_UNLIKELY(!trans_id.is_valid() || !seq_no.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(ls), K(trans_id), K(seq_no));
  } else {
    ObTxTableGuard tx_table_guard;
    if (OB_FAIL(ls->get_tx_table_guard(tx_table_guard))) {
      LOG_WARN("get_tx_table_guard from log stream fail.", K(ret));
    } else if (OB_FAIL(tx_table_guard.get_tx_state_with_scn(
        trans_id, read_scn, trans_state, trans_version))) {
      LOG_WARN("fail to get tx state", K(ret), K(trans_id));
    } else if (ObTxData::UNKOWN == trans_state || ObTxData::ELR_COMMIT == trans_state) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected tx state", K(ret), K(trans_id), K(trans_state));
    } else if (trans_state == ObTxData::RUNNING) {
      LOG_INFO("inc major has not been committed yet", K(trans_state));
    } else if (trans_state == ObTxData::ABORT) {
      LOG_INFO("inc major has been aborted, should remove", K(trans_state));
    } else if (OB_FAIL(tx_table_guard.check_sql_sequence_can_read(trans_id, seq_no, can_read))) {
      LOG_WARN("failed to check sql seq can read", K(ret), K(trans_id), K(seq_no));
    } else if (OB_UNLIKELY(!can_read)) {
      trans_state = ObTxData::ABORT;
      LOG_INFO("inc major has been rollback, should remove", K(trans_id), K(seq_no), K(trans_state), K(can_read));
    }
  }
  return ret;
}

int ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
    const ObSSTable *sstable,
    ObTransID &trans_id,
    ObTxSEQ &seq_no)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle table_meta_handle;
  if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sstable", KR(ret), KP(sstable));
  } else if (OB_FAIL(sstable->get_meta(table_meta_handle))) {
    LOG_WARN("failed to get sstable meta handle", KR(ret), KPC(sstable));
  } else if (OB_UNLIKELY(!table_meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet meta handle", KR(ret), K(table_meta_handle));
  } else {
    const ObMetaUncommitTxInfo &uncommit_info = table_meta_handle.get_sstable_meta().get_uncommit_tx_info();
    if (uncommit_info.is_valid() && (uncommit_info.get_info_count() > 0)) {
      trans_id = uncommit_info.tx_infos_[0].tx_id_;
      seq_no = ObTxSEQ::cast_from_int(uncommit_info.tx_infos_[0].sql_seq_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid uncommit tx info of sstable", KR(ret), KPC(sstable));
    }
  }
  return ret;
}

int ObIncMajorTxHelper::check_can_access(
  ObTableAccessContext &context,
  const ObTransID &trans_id,
  const ObTxSEQ &seq_no,
  const SCN &max_scn,
  bool &can_access)
{
  int ret = OB_SUCCESS;
  can_access = false;
  if (OB_UNLIKELY(!trans_id.is_valid() || !seq_no.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(trans_id), K(seq_no));
  } else {
    memtable::ObMvccAccessCtx &mvcc_acc_ctx = context.store_ctx_->mvcc_acc_ctx_;
    storage::ObTxTableGuards &tx_table_guards = mvcc_acc_ctx.get_tx_table_guards();
    transaction::ObLockForReadArg lock_for_read_arg(mvcc_acc_ctx,
                                                    trans_id,
                                                    seq_no,
                                                    context.query_flag_.read_latest_,
                                                    context.query_flag_.iter_uncommitted_row_,
                                                    max_scn);
    SCN trans_version;
    if (OB_FAIL(tx_table_guards.lock_for_read(lock_for_read_arg, can_access, trans_version))) {
      LOG_WARN("fail to lock for read", KR(ret), K(lock_for_read_arg));
    }
  }
  return ret;
}

int ObIncMajorTxHelper::check_can_access(
  ObTableAccessContext &context,
  const ObUncommitTxDesc &tx_desc,
  const SCN &max_scn,
  bool &can_access)
{
  int ret = OB_SUCCESS;
  can_access = false;
  if (OB_UNLIKELY(!tx_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tx_desc));
  } else {
    ret = check_can_access(context, tx_desc.tx_id_, ObTxSEQ::cast_from_int(tx_desc.sql_seq_), max_scn, can_access);
  }
  return ret;
}

int ObIncMajorTxHelper::check_can_access(
  ObTableAccessContext &context,
  const ObSSTable &sstable,
  bool &can_access)
{
  int ret = OB_SUCCESS;
  can_access = true;
  if (sstable.is_inc_major_type_sstable() || sstable.is_inc_major_ddl_sstable()) {
    ObSSTableMetaHandle meta_handle;
    if (OB_FAIL(sstable.get_meta(meta_handle))) {
      LOG_WARN("fail to get sstable meta", K(ret), K(sstable));
    } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid meta handle", K(ret), K(meta_handle));
    } else {
      const ObMetaUncommitTxInfo &uncommit_info = meta_handle.get_sstable_meta().get_uncommit_tx_info();
      // TODO: zhanghuidong.zhd, collect commit version map from sstable meta for multi-trans inc major
      if (OB_UNLIKELY(!uncommit_info.is_valid() || 1 != uncommit_info.get_info_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid uncommit info", K(ret), K(uncommit_info));
      } else if (INT64_MAX != sstable.get_upper_trans_version()) {
        can_access = context.trans_version_range_.snapshot_version_ >= sstable.get_upper_trans_version();
      } else {
        ObUncommitTxDesc uncommit_tx_desc;
        if (OB_FAIL(uncommit_info.get_uncommit_tx_desc(uncommit_tx_desc, 0))) {
          LOG_WARN("fail to get uncommit tx desc", KR(ret));
        } else if (OB_FAIL(check_can_access(context, uncommit_tx_desc, sstable.get_end_scn(), can_access))) {
          LOG_WARN("fail to check can access", KR(ret));
        }
      }
    }
  } else if (sstable.is_inc_major_ddl_aggregate_co_sstable()) {
    const ObIncMajorDDLAggregateCOSSTable *co_sstable = static_cast<const ObIncMajorDDLAggregateCOSSTable *>(&sstable);
    if (OB_ISNULL(co_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null co sstable", KR(ret), K(sstable));
    } else if (OB_FAIL(co_sstable->check_can_access(context, can_access))) {
      LOG_WARN("fail to check can access", KR(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sstable", KR(ret), K(sstable));
  }
  return ret;
}

void ObIncMajorTxHelper::dump_inc_major_error_info(
    const int64_t merge_snapshot_version,
    const ObMergeType merge_type,
    const ObIArray<ObITable *> &sstables,
    const ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObIncMajorSSTableInfo::IncMajorInfo, 4> local_inc_major_infos;
  ObSEArray<ObIncMajorSSTableInfo::IncMajorInfo, 4> local_inc_major_infos_part_2;

  for (int64_t idx = 0; OB_SUCC(ret) && idx < sstables.count(); idx++) {
    ObSSTable *cur_table = static_cast<ObSSTable *>(sstables.at(idx));
    if (OB_ISNULL(cur_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), K(sstables));
    } else {
      const ObIncMajorSSTableInfo::IncMajorInfo cur_info(cur_table->get_start_scn().get_val_for_tx(),
                                                         cur_table->get_end_scn().get_val_for_tx(),
                                                         cur_table->get_data_checksum(),
                                                         cur_table->get_row_count());
      ObIArray<ObIncMajorSSTableInfo::IncMajorInfo> &write_array = idx < 32 ? local_inc_major_infos : local_inc_major_infos_part_2;
      if (OB_FAIL(write_array.push_back(cur_info))) {
        LOG_WARN("failed to push back inc major info", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t local_info_cnt = local_inc_major_infos.count() + local_inc_major_infos_part_2.count();
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "dump inc major error info", K(merge_snapshot_version), K(merge_type),
                    K(local_info_cnt), K(local_inc_major_infos), K(local_inc_major_infos_part_2), "inc_major_info_in_clog", medium_info.inc_major_info_);
  }
}

int ObIncMajorTxHelper::check_inc_major_table_status(
    const ObMediumCompactionInfo &medium_info,
    const ObMergeType merge_type,
    const int64_t &merge_snapshot_version,
    const ObTablesHandleArray &candidates,
    const bool is_cs_replica)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, 4> inc_major_tables;

  if (GCTX.is_shared_storage_mode()) {
    // do nothing
  } else if (!is_major_merge_type(merge_type) && !is_inc_major_merge(merge_type)) {
    // do nothing
  } else if (OB_UNLIKELY(!medium_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid medium info", K(ret), K(medium_info));
  } else if (OB_FAIL(candidates.get_inc_major_tables(inc_major_tables))) {
    LOG_WARN("failed to get inc major tables", K(ret), K(candidates));
  } else if (!medium_info.contain_inc_major_info_) {
    if (OB_UNLIKELY(!inc_major_tables.empty())) {
      ret = OB_ERR_UNEXPECTED;
      dump_inc_major_error_info(merge_snapshot_version, merge_type, inc_major_tables, medium_info);
    }
  } else if (inc_major_tables.count() != medium_info.inc_major_info_.list_size_) {
    ret = OB_ERR_UNEXPECTED;
    dump_inc_major_error_info(merge_snapshot_version, merge_type, inc_major_tables, medium_info);
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < inc_major_tables.count(); idx++) {
      ObSSTable *cur_table = static_cast<ObSSTable *>(inc_major_tables.at(idx));
      if (OB_ISNULL(cur_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret), K(inc_major_tables));
      } else {
        const ObIncMajorSSTableInfo::IncMajorInfo cur_info(cur_table->get_start_scn().get_val_for_tx(),
                                                           cur_table->get_end_scn().get_val_for_tx(),
                                                           cur_table->get_data_checksum(),
                                                           cur_table->get_row_count());
        const ObIncMajorSSTableInfo::IncMajorInfo &info_in_clog = medium_info.inc_major_info_.info_list_[idx];

        if (is_cs_replica) {
          if (OB_UNLIKELY(info_in_clog.row_count_ != cur_info.row_count_
                       || info_in_clog.start_scn_ != cur_info.start_scn_
                       || info_in_clog.end_scn_ != cur_info.end_scn_)) {
            ret = OB_ERR_UNEXPECTED;
          }
        } else if (OB_UNLIKELY(info_in_clog != cur_info)) {
          ret = OB_ERR_UNEXPECTED;
        }

        if (OB_FAIL(ret)) {
          LOG_ERROR("inc major status not matched", K(ret), K(info_in_clog), K(cur_info));
          dump_inc_major_error_info(merge_snapshot_version, merge_type, inc_major_tables, medium_info);
        }
      }
    }
  }
  return ret;
}

int ObIncMajorTxHelper::find_inc_major_sstable(
    const ObSSTableArray &inc_major_array,
    const ObSSTable *inc_major_ddl_sstable,
    bool &found)
{
  int ret = OB_SUCCESS;
  ObArray<ObITable *> inc_major_sstables;
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_array.count(); ++i) {
    if (OB_FAIL(inc_major_sstables.push_back(inc_major_array.at(i)))) {
      LOG_WARN("failed to push back", KR(ret), K(i), K(inc_major_array.at(i)));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(find_inc_major_sstable(inc_major_sstables, inc_major_ddl_sstable, found))){
    LOG_WARN("failed to find inc major sstable", KR(ret), K(inc_major_sstables), KPC(inc_major_ddl_sstable));
  }
  return ret;
}

int ObIncMajorTxHelper::find_inc_major_sstable(
    const ObIArray<ObITable *> &inc_major_sstables,
    const ObSSTable *inc_major_ddl_sstable,
    bool &found)
{
  int ret = OB_SUCCESS;
  ObTransID target_trans_id;
  ObTxSEQ target_seq_no;
  found = false;
  if (OB_FAIL(ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
      inc_major_ddl_sstable, target_trans_id, target_seq_no))) {
    LOG_WARN("failed to get trans id and seq no from inc major ddl sstable",
        KR(ret), KPC(inc_major_ddl_sstable));
  } else if (OB_FAIL(check_inc_major_exist(inc_major_sstables, target_trans_id, target_seq_no, found))) {
    LOG_WARN("fail to check inc major exist", KR(ret), K(target_trans_id), K(target_seq_no));
  }
  return ret;
}

int ObIncMajorTxHelper::check_inc_major_exist(
    const common::ObIArray<ObITable *> &inc_major_sstables,
    const transaction::ObTransID &trans_id,
    const transaction::ObTxSEQ &seq_no,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  transaction::ObTransID tmp_trans_id;
  transaction::ObTxSEQ tmp_seq_no;
  if (OB_UNLIKELY(!trans_id.is_valid() || !seq_no.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(trans_id), K(seq_no));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_sstables.count(); ++i) {
      const ObSSTable *inc_major = static_cast<const ObSSTable *>(inc_major_sstables.at(i));
      if (OB_ISNULL(inc_major)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc major is nullptr", KR(ret), K(i));
      } else if (OB_FAIL(ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
                         inc_major, tmp_trans_id, tmp_seq_no))) {
        LOG_WARN("fail to get trans id and seq no", KR(ret), KPC(inc_major));
      } else if (tmp_trans_id == trans_id && tmp_seq_no == seq_no) {
        is_exist = true;
        break;
      }
    }
  }
  return ret;
}

int ObIncMajorTxHelper::check_inc_major_exist(
    const ObTabletHandle &tablet_handle,
    const transaction::ObTransID &trans_id,
    const transaction::ObTxSEQ &seq_no,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  ObSSTableArray inc_major_sstable_array;
  ObArray<ObITable *> inc_major_sstables;
  if (OB_UNLIKELY(!tablet_handle.is_valid()
                  || !trans_id.is_valid()
                  || !seq_no.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_handle), K(trans_id), K(seq_no));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", KR(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("fail to get tablet table store", KR(ret));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is nullptr", KR(ret));
  } else {
    const ObSSTableArray &inc_major_array = table_store->get_inc_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_array.count(); ++i) {
      if (OB_FAIL(inc_major_sstables.push_back(inc_major_array.at(i)))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(check_inc_major_exist(inc_major_sstables, trans_id, seq_no, is_exist))) {
      LOG_WARN("fail to check inc major exist", KR(ret), K(trans_id), K(seq_no));
    }
  }
  return ret;
}

int ObIncMajorTxHelper::get_ls(const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is nullptr", KR(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls handle is invalid", KR(ret), K(ls_handle));
  }
  return ret;
}

int ObIncMajorTxHelper::check_need_gc_ddl_dump(
    const ObTablet &tablet,
    const ObSSTable &ddl_dump,
    bool &need_gc)
{
  int ret = OB_SUCCESS;
  need_gc = true;
  ObTransID trans_id;
  ObTxSEQ seq_no;
  ObDDLKvMgrHandle ddlkv_mgr_handle;
  ObArray<ObDDLKVHandle> ddlkv_handles;
  if (OB_UNLIKELY(!ddl_dump.is_inc_major_ddl_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ddl_dump));
  } else if (OB_FAIL(get_trans_id_and_seq_no_from_sstable(&ddl_dump, trans_id, seq_no))) {
    LOG_WARN("fail to get trans id and seq no", KR(ret), K(ddl_dump));
  } else if (OB_FAIL(tablet.get_ddl_kv_mgr(ddlkv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("ddl kv mgr is not exist", KR(ret), K(tablet));
    } else {
      LOG_WARN("fail to get ddl kv mgr", KR(ret), K(tablet));
    }
  } else if (OB_FAIL(ddlkv_mgr_handle.get_obj()->get_ddl_kvs(false/*frozen_only*/, ddlkv_handles))) {
    LOG_WARN("fail to get ddl kv", KR(ret), K(tablet));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ddlkv_handles.count(); ++i) {
      const ObDDLKV *cur_ddlkv = ddlkv_handles.at(i).get_obj();
      if (OB_ISNULL(cur_ddlkv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddlkv is nullptr", KR(ret), K(i));
      } else if (cur_ddlkv->get_trans_id() == trans_id && cur_ddlkv->get_seq_no() == seq_no) {
        need_gc = false;
        break;
      }
    }
  }
  return ret;
}

int ObIncMajorTxHelper::check_inc_major_included_by_major(
  ObLS &ls,
  const int64_t major_version,
  const blocksstable::ObSSTable &sstable,
  bool &is_included)
{
  int ret = OB_SUCCESS;
  is_included = false;
  int64_t trans_state = ObTxData::UNKOWN;
  int64_t commit_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(!sstable.is_inc_major_ddl_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sstable));
  } else if (OB_FAIL(get_inc_major_commit_version(ls, sstable, SCN::max_scn(), trans_state, commit_version))) {
    LOG_WARN("fail to get inc major commit version", KR(ret), K(sstable));
  } else if (ObTxData::COMMIT == trans_state) {
    is_included = major_version >= commit_version;
  }
  return ret;
}

} /* namespace compaction */
} /* namespace oceanbase */
