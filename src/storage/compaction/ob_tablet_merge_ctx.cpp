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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "observer/omt/ob_multi_tenant.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "src/storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/access/ob_table_estimator.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/compaction/ob_medium_list_checker.h"

namespace oceanbase
{
using namespace share;
namespace compaction
{
/*
 *  ----------------------------------------------ObTabletMergeCtx--------------------------------------------------
 */
int ObTabletMergeCtx::prepare_index_tree()
{
  return ObBasicTabletMergeCtx::build_index_tree(
    merge_info_,
    &get_tablet()->get_rowkey_read_info());
}

void ObTabletMergeCtx::update_and_analyze_progress()
{
  (void) info_collector_.finish(merge_info_);
}

int ObTabletMergeCtx::init_tablet_merge_info(const bool need_check)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(merge_info_.init(*this, need_check))) {
    LOG_WARN("failed to init merge info", KR(ret));
  }
  return ret;
}

int ObTabletMergeCtx::create_sstable(const ObSSTable *&new_sstable)
{
  new_sstable = nullptr;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merged_table_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("merged table handle is unexpected valid before create", KR(ret), K(merged_table_handle_));
  } else {
    LOG_INFO("create new merged sstable", "dag_param", get_dag_param(),
             "snapshot_version", get_snapshot(), "scn_range", static_param_.scn_range_);
    bool tmp_bool = false;
    mem_ctx_.mem_click();
    if (OB_FAIL(merge_info_.create_sstable(*this, merged_table_handle_, tmp_bool/*skip_to_create_empty_cg*/))) {
      LOG_WARN("fail to create sstable", KR(ret));
    } else if (OB_FAIL(merged_table_handle_.get_sstable(new_sstable))) {
      LOG_WARN("fail to get sstable", KR(ret), K_(merged_table_handle));
    } else {
      time_guard_click(ObStorageCompactionTimeGuard::CREATE_SSTABLE);
      mem_ctx_.mem_click();
    }
  }
  return ret;
}

int ObTabletMergeCtx::collect_running_info()
{
  int ret = OB_SUCCESS;
  time_guard_click(ObStorageCompactionTimeGuard::DAG_FINISH);
  const ObSSTable *new_table = nullptr;
  if (OB_FAIL(merged_table_handle_.get_sstable(new_table))) {
    LOG_WARN("failed to get sstable", KR(ret), "merged_table_handle", merged_table_handle_);
  } else if (nullptr != merge_dag_) {
    add_sstable_merge_info(merge_info_.get_sstable_merge_info(), merge_dag_->get_dag_id(),
                 merge_dag_->hash(), info_collector_.time_guard_, new_table,
                 &static_param_.snapshot_info_);
  }
  return ret;
}

/*
 *  ----------------------------------------------ObTabletMiniMergeCtx--------------------------------------------------
 */
int ObTabletMiniMergeCtx::prepare_schema()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_storage_schema())) {
    LOG_WARN("failed to get storage schema from tablet", KR(ret));
  } else if (get_tablet_id().is_ls_inner_tablet()) {
    // for ls_inner_tablet, schema on tablet is not changed
    if (OB_FAIL(pre_process_tx_data_table_merge())) {
      LOG_WARN("pre process tx data table for merge failed", KR(ret), "dag_param", get_dag_param());
    }
    static_param_.set_full_merge_and_level(true/*is_full_merge*/);
    static_param_.read_base_version_ = 0;
  } else if (OB_FAIL(update_storage_schema_by_memtable(*static_param_.schema_, get_tables_handle()))) {
    LOG_WARN("failed to update storage schema by memtable", KR(ret));
  }
  return ret;
}

int ObTabletMiniMergeCtx::pre_process_tx_data_table_merge()
{
  int ret = OB_SUCCESS;
  if (is_mini_merge(get_merge_type()) && LS_TX_DATA_TABLET == get_tablet_id()) {
    const ObTablesHandleArray &tables_handle = get_tables_handle();
    for (int i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); i++) {
      ObITable *table = tables_handle.get_table(i);
      if (OB_FAIL(static_cast<ObTxDataMemtable *>(table)->pre_process_for_merge())) {
        LOG_WARN("do pre process for tx data table merge failed.", KR(ret), "param", get_dag_param(),
                 KPC(table));
      }
    }
  }
  time_guard_click(ObStorageCompactionTimeGuard::PRE_PROCESS_TX_TABLE);
  return ret;
}

int ObTabletMiniMergeCtx::update_tablet(
  const blocksstable::ObSSTable &sstable,
  ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletMergeCtx::update_tablet(sstable, new_tablet_handle))) {
    LOG_WARN("failed to update tablet", KR(ret), "param", get_dag_param());
  } else if (FALSE_IT(time_guard_click(ObStorageCompactionTimeGuard::UPDATE_TABLET))) {
  } else if (OB_FAIL(new_tablet_handle.get_obj()->release_memtables(static_param_.scn_range_.end_scn_))) {
    LOG_WARN("failed to release memtable", KR(ret), "param", get_dag_param());
  } else if (FALSE_IT(time_guard_click(ObStorageCompactionTimeGuard::RELEASE_MEMTABLE))) {
  } else {
    // schedule after mini
    try_schedule_compaction_after_mini(new_tablet_handle);
  }
  return ret;
}

void ObTabletMiniMergeCtx::try_schedule_compaction_after_mini(ObTabletHandle &tablet_handle)
{
  int tmp_ret = OB_SUCCESS;
  bool is_restore = false;
  bool create_dag = false;
  // when restoring, some log stream may be not ready,
  // thus the inner sql in ObTenantFreezeInfoMgr::try_update_info may timeout
  if (!MTL(ObTenantTabletScheduler *)->is_restore()) {
    if (get_tablet_id().is_ls_inner_tablet() ||
        0 == get_merge_info().get_sstable_merge_info().macro_block_count_) {
      // do nothing
    } else if (OB_TMP_FAIL(try_schedule_adaptive_merge(tablet_handle, create_dag))) {
      LOG_WARN_RET(tmp_ret, "failed to schedule meta merge", K(get_dag_param()));
    }

    if (create_dag || 0 == get_merge_info().get_sstable_merge_info().macro_block_count_) {
      // no need to schedule minor merge
    } else if (OB_TMP_FAIL(ObTenantTabletScheduler::schedule_tablet_minor_merge<ObTabletMergeExecuteDag>(
        static_param_.ls_handle_, tablet_handle))) {
      if (OB_SIZE_OVERFLOW != tmp_ret) {
        LOG_WARN_RET(tmp_ret, "failed to schedule special tablet minor merge",
            "ls_id", get_ls_id(), "tablet_id", get_tablet_id());
      }
    }
  }

  time_guard_click(ObStorageCompactionTimeGuard::SCHEDULE_OTHER_COMPACTION);
}

int ObTabletMiniMergeCtx::try_schedule_adaptive_merge(
    ObTabletHandle &tablet_handle,
    bool &create_dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID &ls_id = get_ls_id();
  const ObTabletID &tablet_id = get_tablet_id();
  ObTablet *tablet = tablet_handle.get_obj();
  bool medium_is_cooling_down = tablet->get_last_major_snapshot_version() + ObAdaptiveMergePolicy::MEDIUM_COOLING_TIME_THRESHOLD_NS > ObTimeUtility::current_time_ns();
  create_dag = false;
  ObTableQueuingModeCfg queuing_cfg;
  (void) try_report_tablet_stat_after_mini(); // try report after mini every time for updating table mode for tablet.
  if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->get_queuing_cfg(ls_id, tablet_id, queuing_cfg))) {
    LOG_WARN_RET(tmp_ret, "failed to get table queuing mode, treat it as normal table", K(ls_id), K(tablet_id));
  }
  const int64_t adaptive_threshold = ObAdaptiveMergePolicy::TOMBSTONE_ROW_COUNT_THRESHOLD * queuing_cfg.queuing_factor_;
  const ObTransNodeDMLStat &tnode_stat = info_collector_.tnode_stat_;
  bool is_tombstone_scene = (tnode_stat.update_row_count_ + tnode_stat.delete_row_count_) >= adaptive_threshold
                     || tnode_stat.delete_row_count_ > queuing_cfg.total_delete_row_cnt_;
  if (!is_tombstone_scene && queuing_cfg.is_queuing_mode()) {
    ObAdaptiveMergePolicy::AdaptiveMergeReason adaptive_merge_reason = ObAdaptiveMergePolicy::NONE;
    if (OB_TMP_FAIL(ObAdaptiveMergePolicy::get_adaptive_merge_reason(*tablet, adaptive_merge_reason))) {
      LOG_WARN("failed to get adaptive reason", K(tmp_ret));
    } else if (ObAdaptiveMergePolicy::NONE != adaptive_merge_reason) {
      is_tombstone_scene = true;
    }
  }
#ifdef ERRSIM
  // ATTENTION !!!: 2 tracepoint can only hit one at once
  #define SCHEDULE_META_MEDIUM_ERRSIM(tracepoint, cooling_down)              \
    do {                                                                     \
      if (OB_SUCC(ret)) {                                                    \
        ret = OB_E((EventTable::tracepoint)) OB_SUCCESS;                     \
        if (OB_FAIL(ret)) {                                                  \
          ret = OB_SUCCESS;                                                  \
          STORAGE_LOG(INFO, "ERRSIM " #tracepoint);                          \
          is_tombstone_scene = true;                                         \
          medium_is_cooling_down = cooling_down;                             \
        }                                                                    \
      }                                                                      \
    } while(0);
  SCHEDULE_META_MEDIUM_ERRSIM(EN_COMPACTION_SCHEDULE_MEDIUM_MERGE_AFTER_MINI, false /*cooling_down*/);
  SCHEDULE_META_MEDIUM_ERRSIM(EN_COMPACTION_SCHEDULE_META_MERGE, true /*cooling_down*/);
  #undef SCHEDULE_META_MEDIUM_ERRSIM
  STORAGE_LOG(INFO, "try_schedule_adaptive_merge hit errsim", K(ret), K(is_tombstone_scene), K(medium_is_cooling_down));
#endif

  if (is_tombstone_scene) {
    if (ObAdaptiveMergePolicy::is_schedule_medium(queuing_cfg.mode_) && !medium_is_cooling_down) {
      if (OB_TMP_FAIL(ObTenantTabletScheduler::schedule_tablet_medium_merge(static_param_.ls_handle_, tablet_id, create_dag))) {
        LOG_WARN_RET(tmp_ret, "failed to schedule medium merge for tablet after mini", "param",get_dag_param(), K(tablet_id));
      }
    } else if (ObAdaptiveMergePolicy::is_schedule_meta(queuing_cfg.mode_)) {
      // TODO(chengkong): if ls offine or deleted, affect meta compaction?
      if (OB_TMP_FAIL(ObTenantTabletScheduler::schedule_tablet_meta_merge(static_param_.ls_handle_, tablet_handle, create_dag))) {
        LOG_WARN_RET(tmp_ret, "failed to schedule meta merge for tablet", "param", get_dag_param(), K(tablet_id));
      }
    }
  }

  LOG_INFO("[Buffer-Opt] Try to schedule tablet medium/meta after mini", K(tmp_ret), K(ls_id), K(tablet_id), K(is_tombstone_scene),
                "mode", table_mode_flag_to_str(queuing_cfg.mode_), K(medium_is_cooling_down),  K(tnode_stat), K(create_dag));
  return ret;
}

int ObTabletMiniMergeCtx::try_report_tablet_stat_after_mini()
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = get_ls_id();
  const ObTabletID &tablet_id = get_tablet_id();
  const ObTransNodeDMLStat &tnode_stat = info_collector_.tnode_stat_;
  bool report_succ = false;

  if (tablet_id.is_special_merge_tablet()) {
    // no need report
  } else if (ObTabletStat::MERGE_REPORT_MIN_ROW_CNT >= tnode_stat.get_dml_count()) {
    // insufficient data, skip to report
  } else {
    ObTabletStat report_stat;
    report_stat.ls_id_ = get_ls_id().id();
    report_stat.tablet_id_ = get_tablet_id().id();
    report_stat.merge_cnt_ = 1;
    report_stat.insert_row_cnt_ = tnode_stat.insert_row_count_;
    report_stat.update_row_cnt_ = tnode_stat.update_row_count_;
    report_stat.delete_row_cnt_ = tnode_stat.delete_row_count_;
    if (OB_FAIL(MTL(ObTenantTabletStatMgr *)->report_stat(report_stat, report_succ))) {
      LOG_WARN("failed to report tablet stat", KR(ret));
    }
  }
  FLOG_INFO("try report tablet stat", KR(ret), K(ls_id), K(tablet_id), K(tnode_stat), K(report_succ));
  return ret;
}

int ObTabletMiniMergeCtx::get_merge_tables(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBasicTabletMergeCtx::get_merge_tables(get_merge_table_result))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get merge tables", KR(ret), KPC(this), K(get_merge_table_result));
    } else { // OB_NO_NEED_MERGE
      int tmp_ret = OB_SUCCESS;
      // then release memtable
      if (OB_TMP_FAIL(get_tablet()->release_memtables(get_tablet()->get_tablet_meta().clog_checkpoint_scn_))) {
        LOG_WARN("failed to release memtable", K(tmp_ret), "param", get_dag_param(),
          "clog_checkpoint_scn", get_tablet()->get_tablet_meta().clog_checkpoint_scn_);
      }
    }
  }
  return ret;
}

int ObTabletMiniMergeCtx::update_tablet_directly(
  ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObUpdateTableStoreParam param;
  ObTabletHandle new_tablet_handle;
  if (OB_FAIL(static_param_.get_basic_info_from_result(get_merge_table_result))) {
    LOG_WARN("failed to get basic infor from result", KR(ret), K(get_merge_table_result));
  } else if (OB_FAIL(get_storage_schema())) {
    LOG_WARN("failed to get storage schema from tablet", KR(ret));
  } else if (FALSE_IT(build_update_table_store_param(nullptr/*sstable*/, param))) {
  } else if (OB_FAIL(get_ls()->update_tablet_table_store(
      get_tablet_id(), param, new_tablet_handle))) {
    LOG_WARN("failed to update tablet table store", KR(ret), K(param));
  } else if (OB_TMP_FAIL(new_tablet_handle.get_obj()->release_memtables(new_tablet_handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_))) {
    LOG_WARN("failed to release memtable", K(tmp_ret), K(param));
  }
  if (FAILEDx(merge_info_.init(*this, false/*need_check*/))) {
    LOG_WARN("failed to inie merge info", KR(ret));
  } else if (OB_FAIL(static_param_.tables_handle_.assign(get_merge_table_result.handle_))) { // assgin for generate_participant_table_info
    LOG_WARN("failed to assgin table handle", KR(ret));
  } else {
    add_sstable_merge_info(merge_info_.get_sstable_merge_info(),
      merge_dag_->get_dag_id(), merge_dag_->hash(),
      info_collector_.time_guard_, nullptr/*sstable*/,
      &static_param_.snapshot_info_);

    (void) try_schedule_compaction_after_mini(new_tablet_handle);
  }
  return ret;
}

/*
 *  ----------------------------------------------ObTabletExeMergeCtx--------------------------------------------------
 */
int ObTabletExeMergeCtx::get_merge_tables(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_tables_by_key(get_merge_table_result))) {
    LOG_WARN("failed to get tables by key", KR(ret), "param", get_dag_param(), KPC(merge_dag_));
  } else if (get_tablet_id().is_ls_inner_tablet()) {
    // init compaction filter for minor merge in TxDataTable
    if (OB_FAIL(prepare_compaction_filter())) {
      LOG_WARN("failed to prepare compaction filter", KR(ret), "param", get_dag_param());
    }
  }
  return ret;
}

int ObTabletExeMergeCtx::cal_merge_param()
{
  int ret = OB_SUCCESS;
  const bool has_compaction_filter = nullptr != info_collector_.compaction_filter_;
  if (OB_FAIL(static_param_.cal_minor_merge_param(has_compaction_filter))) {
    LOG_WARN("failed to cal_major_merge_param", KR(ret));
  } else if (is_minor_merge(static_param_.get_merge_type())) {
    if (OB_FAIL(init_static_param_tx_id())) {
      LOG_WARN("failed to init_static_param_tx_id", KR(ret));
    }
  }
  return ret;
}

int ObTabletExeMergeCtx::init_static_param_tx_id()
{
  int ret = OB_SUCCESS;
  ObTxTableGuard tx_table_guard;
  const storage::ObTablesHandleArray &tables_handle = static_param_.tables_handle_;
  const int64_t table_cnt = tables_handle.get_count();
  ObITable *table = nullptr;
  ObLS *ls = nullptr;
  ObSSTableMetaHandle meta_handle;
  int64_t &static_param_tx_id = static_param_.tx_id_;

  if (OB_ISNULL(ls = static_param_.ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret));
  } else if (OB_FAIL(ls->get_tx_table_guard(tx_table_guard))) {
    STORAGE_LOG(WARN, "get_tx_table_guard from log stream fail.", K(ret), K(*ls));
  }

  static_param_tx_id = 0;
  for (int64_t i = 0; i < table_cnt && OB_SUCC(ret); i++) {
    if (OB_ISNULL(table = tables_handle.get_table(i)) || OB_UNLIKELY(!table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null table", K(ret), K(i), KPC(table));
    } else if (!static_cast<ObSSTable *>(table)->contain_uncommitted_row()) {
      //continue
    } else if (OB_FAIL(static_cast<ObSSTable *>(table)->get_meta(meta_handle))) {
      STORAGE_LOG(WARN, "fail to get meta", K(ret), K(i), KPC(table));
    } else if (meta_handle.get_sstable_meta().get_tx_id_count() > 0) {
      const int64_t tx_id = meta_handle.get_sstable_meta().get_tx_ids(0);
      int64_t state = 0;
      share::SCN trans_version;
      if (OB_UNLIKELY(meta_handle.get_sstable_meta().get_tx_id_count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected tx id count", K(ret), K(i), KPC(table), KPC(meta_handle.meta_));
      } else if (tx_id == static_param_tx_id) {
        //continue
      } else if (OB_FAIL(tx_table_guard.get_tx_state_with_scn(transaction::ObTransID(tx_id),
        static_param_.merge_scn_, state, trans_version))) {
        STORAGE_LOG(WARN, "fail to get tx state", K(ret), K(tx_id), K(static_param_.merge_scn_));
      } else if (state == ObTxData::RUNNING) {
        if (OB_UNLIKELY(transaction::ObTransID(static_param_tx_id).is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected tx id", K(ret), K(static_param_tx_id), K(tx_id), K(tables_handle));
        } else {
          static_param_tx_id = tx_id;
        }
      }
    }
  }

  STORAGE_LOG(INFO, "finish tx id init", K(ret), K(static_param_tx_id));
  return ret;
}

int ObTabletExeMergeCtx::get_tables_by_key(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTabletMergeExecuteDag *exe_dag = nullptr;
  if (OB_ISNULL(exe_dag = static_cast<ObTabletMergeExecuteDag *>(merge_dag_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge dag is not a execute dag", KR(ret), KPC(merge_dag_));
  } else if (OB_UNLIKELY(exe_dag->get_table_key_array().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table key array is empty", KR(ret), KPC(exe_dag));
  } else if (OB_FAIL(get_tablet()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", KR(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.get_member()->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet store is invalid", KR(ret), KPC(table_store_wrapper.get_member()));
  } else if (OB_FAIL(get_merge_table_result.assign(exe_dag->get_result()))) {
    LOG_WARN("failed to assign result", KR(ret));
  } else {
    const ObIArray<ObITable::TableKey> &table_key_array = exe_dag->get_table_key_array();
    ObSSTableWrapper sstable_wrapper;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_key_array.count(); ++i) {
      const ObITable::TableKey &table_key = table_key_array.at(i);
      if (OB_FAIL(table_store_wrapper.get_member()->get_sstable(table_key, sstable_wrapper))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_NO_NEED_MERGE;
        } else {
          LOG_WARN("failed to get table from new table_store", KR(ret));
        }
      } else if (OB_FAIL(get_merge_table_result.handle_.add_sstable(sstable_wrapper.get_sstable(), table_store_wrapper.get_meta_handle()))) {
        LOG_WARN("failed to add sstable into result", KR(ret), K(sstable_wrapper));
      }
    } // end of for
    if (OB_SUCC(ret) && get_merge_table_result.handle_.get_count() != table_key_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected tables from current tablet", KR(ret), K(table_key_array), K(get_merge_table_result));
    }
  }
  return ret;
}

int ObTabletExeMergeCtx::prepare_compaction_filter()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(!get_tablet_id().is_ls_tx_data_tablet())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only tx data tablet can execute minor merge", KR(ret), "param", get_dag_param());
  } else if (OB_ISNULL(buf = mem_ctx_.alloc(sizeof(ObTransStatusFilter)))) {
    // trans status filter is not neccesary for tx minor
  } else {
    ObTransStatusFilter *compaction_filter = new(buf) ObTransStatusFilter();
    ObTxTableGuard guard;
    share::SCN recycle_scn = share::SCN::min_scn();
    const ObTabletMergeDagParam &param = get_dag_param();
    int tmp_ret = OB_SUCCESS;

    if (OB_TMP_FAIL(get_ls()->get_tx_table_guard(guard))) {
      LOG_WARN("failed to get tx table", K(tmp_ret), K(param));
    } else if (OB_UNLIKELY(!guard.is_valid())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx table guard is invalid", K(tmp_ret), K(param), K(guard));
    } else if (OB_TMP_FAIL(guard.get_recycle_scn(recycle_scn))) {
      LOG_WARN("failed to get recycle ts", K(tmp_ret), K(param));
    } else if (OB_TMP_FAIL(compaction_filter->init(recycle_scn, ObTxTable::get_filter_col_idx()))) {
      LOG_WARN("failed to get init compaction filter", K(tmp_ret), K(param), K(recycle_scn));
    } else {
      info_collector_.compaction_filter_ = compaction_filter;
      FLOG_INFO("success to init compaction filter", K(tmp_ret), K(recycle_scn));
    }

    if (OB_SUCCESS != tmp_ret && OB_NOT_NULL(buf)) {
      mem_ctx_.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

/*
 *  ----------------------------------------------ObTabletMajorMergeCtx--------------------------------------------------
 */
int ObTabletMajorMergeCtx::prepare_schema()
{
  int ret = OB_SUCCESS;

  if (is_meta_major_merge(static_param_.get_merge_type())) {
    if (OB_FAIL(get_meta_compaction_info())) {
      LOG_WARN("failed to get meta compaction info", K(ret), KPC(this));
    }
  } else if (!MTL(ObTenantTabletScheduler *)->could_major_merge_start()) {
    ret = OB_CANCELED;
    LOG_INFO("Merge has been paused", KR(ret), "param", get_dag_param());
  } else if (OB_FAIL(get_medium_compaction_info())) {
    // have checked medium info inside
    LOG_WARN("failed to get medium compaction info", KR(ret), KPC(this));
  }
  return ret;
}
} // namespace compaction
} // namespace oceanbase
