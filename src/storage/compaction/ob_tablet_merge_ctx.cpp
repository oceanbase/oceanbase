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
#include "ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_schedule_tablet_func.h"
#include "storage/compaction/filter/ob_tx_data_minor_filter.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/compaction/filter/ob_reorg_info_minor_filter.h"

namespace oceanbase
{
using namespace share;
using namespace blocksstable;
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

int ObTabletMergeCtx::init_tablet_merge_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_sstable_merge_history())) {
    LOG_WARN("failed to init merge history", KR(ret));
  } else if (OB_FAIL(merge_info_.init(static_history_))) {
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
    add_sstable_merge_info(merge_info_.get_merge_history(), merge_dag_->get_dag_id(),
                 merge_dag_->hash(), info_collector_.time_guard_, new_table,
                 &static_param_.snapshot_info_);
  }
  return ret;
}

int ObTabletMergeCtx::update_block_info(
    const ObMergeBlockInfo &block_info,
    const int64_t cost_time)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(merge_info_.get_merge_history().update_block_info(block_info, false/*without_row_cnt*/))) {
    LOG_WARN("failed to update block info", KR(ret), K(block_info));
  } else {
    merge_info_.get_merge_history().update_execute_time(cost_time);
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
    } else {
      static_param_.set_full_merge_and_level(true/*is_full_merge*/);
      static_param_.read_base_version_ = 0;
    }
  } else if (OB_FAIL(update_storage_schema_by_memtable(*static_param_.schema_, get_tables_handle()))) {
    LOG_WARN("failed to update storage schema by memtable", KR(ret));
  }
#ifdef OB_BUILD_SHARED_STORAGE
  // prepare for register mini sstable upload
  if (OB_SUCC(ret)
      && GCTX.is_shared_storage_mode()
      && OB_FAIL(get_ls()->prepare_register_sstable_upload(upload_register_handle_))) {
    LOG_WARN("prepare mini sstable upload register fail", K(ret));
  }
#endif
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
  ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTabletMergeCtx::update_tablet(new_tablet_handle))) {
    LOG_WARN("failed to update tablet", KR(ret), "param", get_dag_param());
  } else {
    time_guard_click(ObStorageCompactionTimeGuard::UPDATE_TABLET);
#ifdef OB_BUILD_SHARED_STORAGE
    // scheduler mini upload for shared-storage
    if (GCTX.is_shared_storage_mode()) {
      (void)register_upload_task_(new_tablet_handle);
    }
#endif
    if (OB_FAIL(new_tablet_handle.get_obj()->release_memtables(static_param_.scn_range_.end_scn_))) {
      LOG_WARN("failed to release memtable", KR(ret), "param", get_dag_param());
    } else if (FALSE_IT(time_guard_click(ObStorageCompactionTimeGuard::RELEASE_MEMTABLE))) {
    }
    // schedule after mini
    try_schedule_compaction_after_mini(new_tablet_handle);
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
#define PRINT_TS_WRAPPER(x) (ObPrintTableStore(*(x.get_member())))
void ObTabletMiniMergeCtx::register_upload_task_(ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = NULL;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTableWrapper sstable_wrapper;
  if (OB_FAIL(merged_table_handle_.get_sstable(sstable))) {
    LOG_ERROR("get sstable fail", KR(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid sstable", K(new_tablet_handle));
  } else if (OB_FAIL(new_tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    STORAGE_LOG(ERROR, "fetch table store failed", K(new_tablet_handle));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_sstable(sstable->get_key(), sstable_wrapper))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      FLOG_INFO("no need register upload. MINI SSTable may be coverd by checkpoint or major sstable",
                K(PRINT_TS_WRAPPER(table_store_wrapper)));
    } else {
      STORAGE_LOG(ERROR, "get sstable failed", K(new_tablet_handle));
    }
  } else {
    // if get snapshot version failed, use min_scn as snapshot version to avoid mini merge failed
    int tmp_ret = OB_SUCCESS;
    SCN snapshot_version(SCN::min_scn());
    if (OB_TMP_FAIL(new_tablet_handle.get_obj()->get_snapshot_version(snapshot_version))) {
      // ignore error and use min_scn as snapshot version to avoid register upload failed
      snapshot_version.set_min();
      LOG_WARN("get snapshot version failed", KR(ret), K(new_tablet_handle));
    }

    ASYNC_UPLOAD_INC_SSTABLE(
        SSIncSSTableType::MINI_SSTABLE, upload_register_handle_, sstable->get_key(), snapshot_version);
  }
}
#undef PRINT_TS_WRAPPER
#endif

void ObTabletMiniMergeCtx::try_schedule_compaction_after_mini(ObTabletHandle &tablet_handle)
{
  int tmp_ret = OB_SUCCESS;
  bool create_dag = false;
  bool during_restore = false;
  // when restoring, some log stream may be not ready,
  // thus the inner sql in ObTenantFreezeInfoMgr::try_update_info may timeout
  if (OB_SUCCESS == ObBasicMergeScheduler::get_merge_scheduler()->during_restore(during_restore) && !during_restore) {
    if (get_tablet_id().is_ls_inner_tablet() ||
        0 == get_merge_info().get_merge_history().get_macro_block_count()) {
      // do nothing
    } else if (nullptr != static_param_.schema_ && static_param_.schema_->is_mv_major_refresh_table()) {
      // do nothing
    } else if (FALSE_IT(try_report_tablet_stat_after_mini())) { // try report after mini every time for updating table mode for tablet.
    } else if (OB_TMP_FAIL(ObTenantTabletScheduler::try_schedule_adaptive_merge(
                              static_param_.ls_handle_,
                              tablet_handle,
                              ObAdaptiveMergePolicy::SCHEDULE_AFTER_MINI,
                              info_collector_.tnode_stat_.update_row_count_,
                              info_collector_.tnode_stat_.delete_row_count_,
                              create_dag))) {
      LOG_WARN_RET(tmp_ret, "failed to schedule meta merge", K(get_dag_param()), "tnode_stat", info_collector_.tnode_stat_);
    }
    if (GCTX.is_shared_storage_mode()) {
      // disable minor merge in shared storage mode
    } else if (create_dag || 0 == get_merge_info().get_merge_history().get_macro_block_count()) {
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

int ObTabletMiniMergeCtx::try_report_tablet_stat_after_mini()
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = get_ls_id();
  const ObTabletID &tablet_id = get_tablet_id();
  const ObTransNodeDMLStat &tnode_stat = info_collector_.tnode_stat_;
  bool report_succ = false;

  if (tablet_id.is_special_merge_tablet()) {
    // no need report
  } else if (ObTabletStat::MERGE_MIN_ROW_CNT >= tnode_stat.get_dml_count()) {
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

int ObTabletMiniMergeCtx::update_tablet_directly(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObUpdateTableStoreParam param;
  ObTabletHandle new_tablet_handle;
  if (OB_FAIL(static_param_.get_basic_info_from_result(get_merge_table_result))) {
    LOG_WARN("failed to get basic infor from result", KR(ret), K(get_merge_table_result));
  } else if (OB_FAIL(get_storage_schema())) {
    LOG_WARN("failed to get storage schema from tablet", KR(ret));
  } else if (OB_FAIL(build_update_table_store_param(nullptr/*sstable*/, param))) {
    LOG_WARN("failed to build update table store param", KR(ret), K(param));
  } else if (OB_FAIL(get_ls()->update_tablet_table_store(
      get_tablet_id(), param, new_tablet_handle))) {
    LOG_WARN("failed to update tablet table store", KR(ret), K(param));
  } else if (OB_TMP_FAIL(new_tablet_handle.get_obj()->release_memtables(new_tablet_handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_))) {
    LOG_WARN("failed to release memtable", K(tmp_ret), K(param));
  }
  if (FAILEDx(init_sstable_merge_history())) {
    LOG_WARN("failed to init merge history", KR(ret));
  } else if (OB_FAIL(merge_info_.init(static_history_))) {
    LOG_WARN("failed to inie merge info", KR(ret));
  } else if (OB_FAIL(static_param_.tables_handle_.assign(get_merge_table_result.handle_))) { // assgin for generate_participant_table_info
    LOG_WARN("failed to assgin table handle", KR(ret));
  } else {
    add_sstable_merge_info(merge_info_.get_merge_history(),
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
  }
  return ret;
}

int ObTabletExeMergeCtx::cal_merge_param()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(static_param_.cal_minor_merge_param(has_filter()))) {
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

  STORAGE_LOG(INFO, "finish tx id init", K(ret), K(static_param_tx_id), K(tx_table_guard));
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
  const ObTabletID &tablet_id = get_tablet_id();
  if (!get_tablet_id().is_ls_inner_tablet()) {
    // do nothing
  } else if (tablet_id.is_ls_tx_data_tablet()) {
    if (OB_FAIL(prepare_tx_table_compaction_filter_())) {
      LOG_WARN("failed to prepare tx table compaction filter", K(ret));
    }
  } else if (tablet_id.is_ls_reorg_info_tablet()) {
    if (OB_FAIL(prepare_reorg_info_table_compaction_filter_())) {
      LOG_WARN("failed to prepare reorg info table compaction filter", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id is unexpected, cannot prepare compaction filter", K(ret), K(tablet_id));
  }
  return ret;
}

int ObTabletExeMergeCtx::prepare_tx_table_compaction_filter_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  // init compaction filter for minor merge in TxDataTable
  if (OB_UNLIKELY(!get_tablet_id().is_ls_tx_data_tablet())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only tx data tablet can execute minor merge", KR(ret), "param", get_dag_param());
  } else if (GCTX.is_shared_storage_mode() && is_local_exec_mode(get_exec_mode())) {
    // shared-storage mode, local minor do not filter tx data
  } else if (!static_param_.scn_range_.start_scn_.is_base_scn()) {
    FLOG_INFO ("Skip filtering because this minor merge does not contain the oldest minor sstable",
      K(static_param_.scn_range_));
  } else if (OB_ISNULL(buf = mem_ctx_.alloc(sizeof(ObTxDataMinorFilter)))) {
    // trans status filter is not neccesary for tx minor
  } else {
    ObTxDataMinorFilter *compaction_filter = new(buf) ObTxDataMinorFilter();
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(init_tx_table_compaction_filter_(compaction_filter))) {
      LOG_INFO("this TxTable MinorMerge do not recycle TxData", KR(ret));
      if (OB_NOT_NULL(buf)) {
        mem_ctx_.free(buf);
        buf = nullptr;
      }
    }
  }
  return ret;
}

int ObTabletExeMergeCtx::init_tx_table_compaction_filter_(ObTxDataMinorFilter *compaction_filter)
{
  int ret = OB_SUCCESS;
  ObTxTableGuard guard;
  share::SCN recycle_scn = share::SCN::min_scn();
  const ObTabletMergeDagParam &param = get_dag_param();
  const bool is_local = is_local_exec_mode(get_exec_mode());

  if (OB_FAIL(get_ls()->get_tx_table_guard(guard))) {
    LOG_WARN("failed to get tx table", K(ret), K(param));
  } else if (OB_UNLIKELY(!guard.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx table guard is invalid", K(ret), K(param), K(guard));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (!is_local) {
    if (OB_FAIL(guard.get_ss_recycle_scn(recycle_scn))) {
      LOG_WARN("failed to get ss_recycle_scn", K(ret), K(param));
    }
#endif
  } else if (OB_FAIL(guard.get_recycle_scn(recycle_scn))) {
    LOG_WARN("failed to get ss_recycle_scn", K(ret), K(param));
  }

  if (OB_FAIL(ret)) {
  } else if (!guard.can_recycle_tx_data(recycle_scn, is_local)) {
    ret = OB_EAGAIN;
    LOG_INFO("skip recycle tx data", K(ret), K(recycle_scn), K(param));
  } else if (OB_FAIL(compaction_filter->init(recycle_scn, ObTxTable::get_filter_col_idx()))) {
    LOG_WARN("failed to get init compaction filter", K(ret), K(param), K(recycle_scn));
  } else {
    filter_ctx_.compaction_filter_ = compaction_filter;
    FLOG_INFO("success to init compaction filter", K(ret), K(recycle_scn));
  }

  return ret;
}

int ObTabletExeMergeCtx::prepare_reorg_info_table_compaction_filter_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(!get_tablet_id().is_ls_reorg_info_tablet())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is not reorg info table, unexpected", KR(ret), "param", get_dag_param());
  } else if (OB_ISNULL(buf = mem_ctx_.alloc(sizeof(ObReorgInfoMinorFilter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mem", K(ret));
  } else {
    ObReorgInfoMinorFilter *compaction_filter = new(buf) ObReorgInfoMinorFilter();
    share::SCN recycle_scn = share::SCN::min_scn();
    const ObTabletMergeDagParam &param = get_dag_param();
    int tmp_ret = OB_SUCCESS;
    ObTabletReorgInfoTable *reorg_info_table = nullptr;

    if (OB_ISNULL(reorg_info_table = get_ls()->get_reorg_info_table())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reorg info table should not be NULL", K(tmp_ret), K(param));
    } else if (OB_TMP_FAIL(reorg_info_table->get_can_recycle_scn(recycle_scn))) {
      LOG_WARN("failed to get recycle scn", K(tmp_ret), K(param));
    } else if (OB_TMP_FAIL(compaction_filter->init(recycle_scn))) {
      LOG_WARN("failed to get init compaction filter", K(tmp_ret), K(param), K(recycle_scn));
    } else {
      filter_ctx_.compaction_filter_ = compaction_filter;
      compaction_filter = nullptr;
      buf = nullptr;
      FLOG_INFO("success to init reorg info table filter", K(tmp_ret), K(recycle_scn));
    }

    if (OB_NOT_NULL(compaction_filter)) {
      compaction_filter->~ObReorgInfoMinorFilter();
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
  } else if (!MERGE_SCHEDULER_PTR->could_major_merge_start()) {
    ret = OB_CANCELED;
    LOG_INFO("Merge has been paused", KR(ret), "param", get_dag_param());
  } else {
    ObArenaAllocator allocator("GetMediumInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObMediumCompactionInfo *medium_info = nullptr;
    if (OB_FAIL(OB_FAIL(ObTabletMediumInfoReader::get_medium_info_with_merge_version(get_merge_version(), *get_tablet(), allocator, medium_info)))) {
    LOG_WARN("fail to get medium info with merge version", K(ret), KPC(this));
    } else if (OB_FAIL(prepare_from_medium_compaction_info(medium_info))) {
      // have checked medium info inside
      LOG_WARN("failed to get medium compaction info", KR(ret), KPC(this), KPC(medium_info));
    }
    ObTabletObjLoadHelper::free(allocator, medium_info);
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
