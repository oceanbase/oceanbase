/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/ddl/ob_inc_ddl_merge_helper.h"
#include "storage/ddl/ob_ddl_merge_task_utils.h"
#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/direct_load/ob_direct_load_auto_inc_seq_service.h"

using namespace oceanbase::observer;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;
using namespace oceanbase::compaction;

namespace oceanbase
{
namespace storage
{

int ObIncMinDDLMergeHelper::get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param)
{
  return OB_SUCCESS; /* do nothing */
}

int ObIncMinDDLMergeHelper::process_prepare_task(ObIDag *dag,
                                                 ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                                 ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices)
{
  int ret = OB_SUCCESS;

  cg_slices.reset();
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObWriteTabletParam           *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx    = nullptr;

  ObTabletHandle tablet_handle;
  ObDDLKV *ddl_kv = nullptr;

  bool need_check_tablet = false;
  share::SCN clog_checkpoint_scn;
  hash::ObHashSet<int64_t> slice_idxes;

  /* check param & prepare necessary param*/
  if (!dag_merge_param.is_valid() || nullptr == dag) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param), KPC(dag));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(ls_id, tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret), K(dag_merge_param));
  } else if (nullptr == tablet_param || nullptr == merge_ctx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet param & merge ctx should not be null", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(slice_idxes.create(DDL_SLICE_BUCKET_NUM, ObMemAttr(MTL_ID(), "slice_idx_set")))) {
    LOG_WARN("create slice index set failed", K(ret));
  } else {
    clog_checkpoint_scn = tablet_handle.get_obj()->get_clog_checkpoint_scn();
  }

  /* check ddl kv valid && prepare ddl kv */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLMergeTaskUtils::prepare_incremental_direct_load_ddl_kvs(*(tablet_handle.get_obj()), merge_ctx->ddl_kv_handles_))) {
    LOG_WARN("failed to prepare incremental direct load ddl kvs", K(ret));
  } else if (1 != merge_ctx->ddl_kv_handles_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected frozen ddl kv count", K(ret));
  } else if (OB_ISNULL(ddl_kv = merge_ctx->ddl_kv_handles_.at(0).get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl kv should not be null", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(ddl_kv->close())) {
    LOG_WARN("close ddl kv failed", K(ret), KPC(ddl_kv));
  }

  /* notice !!! in incremental direct load, scn range should be set according to the ddl kv state */
  if (OB_FAIL(ret)) {
  } else {
    dag_merge_param.table_key_.scn_range_.start_scn_ = ddl_kv->get_start_scn();
    dag_merge_param.table_key_.scn_range_.end_scn_   = ddl_kv->get_end_scn();
    dag_merge_param.ddl_task_param_.snapshot_version_ = ddl_kv->get_snapshot_version();
  }

  /* chekc table key is valid */
  if (OB_FAIL(ret)) {
  } else if (clog_checkpoint_scn >= ddl_kv->get_end_scn()) {
    // do nothing, just release ddl_kv
  } else if (OB_FAIL(ObDDLMergeTaskUtils::refine_incremental_direct_load_merge_param(*tablet_handle.get_obj(),
                                                                                     dag_merge_param.table_key_,
                                                                                     need_check_tablet))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("fail to refine incremental direct load merge param", K(ret), KPC(tablet_handle.get_obj()), K(dag_merge_param));
    } else {
      ret = OB_SUCCESS;
      // do nothing, just release ddl_kv
    }
  } else if (OB_UNLIKELY(need_check_tablet)) {
    ret = OB_EAGAIN;
    int tmp_ret = OB_SUCCESS;
    ObTabletHandle tmp_tablet_handle;
    if (OB_TMP_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tmp_tablet_handle))) {
      LOG_WARN("failed to get tablet handle", K(ret), K(ls_id), K(tablet_id));
    } else if (tmp_tablet_handle.get_obj()->get_clog_checkpoint_scn() != clog_checkpoint_scn) {
      // do nothing, just retry the merge task
    } else {
      LOG_ERROR("Unexpected uncontinuous scn_range in mini merge", K(ret), K(tablet_handle), KPC(ddl_kv));
    }
  } else if (OB_FAIL(cg_slices.push_back(ObTuple<int64_t, int64_t, int64_t>(0 /* cg_idx */,
                                                                            0 /* start slice idx */,
                                                                            0 /* end slice idx   */)))) {
    LOG_WARN("failed to push back cg slice", K(ret));
  } else if (OB_FAIL(slice_idxes.set_refactored(0))) {
    LOG_WARN("failed to set slice idx", K(ret));
  } else if (OB_FAIL(dag_merge_param.init_cg_sstable_array(slice_idxes))) {
    LOG_WARN("failed to init cg sstable array", K(ret));
  }

  return ret;
}

int ObIncMinDDLMergeHelper::merge_cg_slice(ObIDag *dag,
                                           ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                           const int64_t cg_idx,
                                           const int64_t start_slice_idx,
                                           const int64_t end_slice_idx)
{
  int ret = OB_SUCCESS;

  ObLSID ls_id;
  ObTabletID tablet_id;
  ObTabletHandle tablet_handle;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx    *merge_ctx    = nullptr;

  ObTabletDDLParam tablet_ddl_param;

  ObDDLKV *ddl_kv = nullptr;
  ObArray<ObSSTable*> ddl_sstables;
  ObTableHandleV2 sstable_handle;

  ObArray<ObDDLBlockMeta> sorted_metas;
  ObArenaAllocator arena(ObMemAttr(MTL_ID(), "merge_cg_slice"));

  /* prepare param */
  if (nullptr == dag || !dag_merge_param.is_valid() || cg_idx < 0 || start_slice_idx > end_slice_idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(dag), K(dag_merge_param), K(cg_idx), K(start_slice_idx), K(end_slice_idx));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(ls_id, tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (nullptr == merge_ctx || nullptr == tablet_param) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet param should not be null", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_ISNULL(ddl_kv = merge_ctx->ddl_kv_handles_.at(0).get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl kv should not be null", K(ret), K(dag_merge_param));
  }

  /* prepare ddl param */
  if (OB_FAIL(ret)) {
  } else {
    tablet_ddl_param.direct_load_type_    = dag_merge_param.direct_load_type_;
    tablet_ddl_param.ls_id_               = ls_id;
    tablet_ddl_param.start_scn_           = ddl_kv->get_ddl_start_scn();
    tablet_ddl_param.commit_scn_          = ddl_kv->get_ddl_start_scn();
    tablet_ddl_param.data_format_version_ = dag_merge_param.ddl_task_param_.tenant_data_version_;
    tablet_ddl_param.table_key_           = dag_merge_param.table_key_;
    tablet_ddl_param.snapshot_version_                = ddl_kv->get_snapshot_version();
    tablet_ddl_param.trans_id_                        = ddl_kv->get_trans_id();
    tablet_ddl_param.seq_no_                          = ddl_kv->get_seq_no();
    tablet_ddl_param.rec_scn_                         = ddl_kv->get_rec_scn();
  }

  /* update storage schema from ddl kv */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLMergeTaskUtils::update_storage_schema(*tablet_handle.get_obj(),
                                                                tablet_ddl_param,
                                                                merge_ctx->arena_,
                                                                tablet_param->storage_schema_,
                                                                merge_ctx->ddl_kv_handles_))) {
    LOG_WARN("failed to update storage schema", K(ret));
  }

  /* merge from ddl kv */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLMergeTaskUtils::get_ddl_tables_from_ddl_kvs(merge_ctx->ddl_kv_handles_,
                                                                      cg_idx,
                                                                      start_slice_idx,
                                                                      end_slice_idx,
                                                                      ddl_sstables))) {
    LOG_WARN("failed to get ddl tables from  ddl kvs", K(ret));
  } else if (OB_FAIL(ObDDLMergeTaskUtils::get_sorted_meta_array(*tablet_handle.get_obj(),
                                                                tablet_ddl_param,
                                                                tablet_param->storage_schema_,
                                                                ddl_sstables,
                                                                tablet_handle.get_obj()->get_rowkey_read_info(),
                                                                arena,
                                                                sorted_metas))) {
    LOG_WARN("failed to get sorted meta array", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(*(tablet_handle.get_obj()),
                                                           tablet_ddl_param,
                                                           sorted_metas,
                                                           ObArray<MacroBlockId>(),
                                                           nullptr,
                                                           tablet_param->storage_schema_,
                                                           &merge_ctx->mutex_,
                                                           merge_ctx->arena_,
                                                           sstable_handle))) {
    LOG_WARN("failed to create sstable", K(ret), K(cg_idx), K(tablet_ddl_param));
  } else if (OB_FAIL(dag_merge_param.set_cg_slice_sstable(start_slice_idx, cg_idx, sstable_handle))) {
    LOG_WARN("failed to set ddl sstable", K(ret), K(dag_merge_param));
  }
  return ret;
}

int ObIncMinDDLMergeHelper::assemble_sstable(ObDDLTabletMergeDagParamV2 &dag_merge_param)
{
  int ret = OB_SUCCESS;

  ObLSID ls_id;
  ObTabletID tablet_id;
  ObWriteTabletParam           *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx    = nullptr;

  ObLSService *ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;

  ObTabletHandle tablet_handle;
  blocksstable::ObSSTable *sstable = nullptr;
  ObArray<ObTableHandleV2> *sstable_handles = nullptr;

  /* check arg valid & prepare param*/
  if (!dag_merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(ls_id, tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet ctx", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (OB_ISNULL(merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx should not be null", K(ret));
  } else if (merge_ctx->slice_cg_sstables_.empty()) {
    // do nothing, just release ddl_kv
  } else if (OB_FAIL(merge_ctx->slice_cg_sstables_.get_refactored(0 /*slice_id*/, sstable_handles))) {
    LOG_WARN("failed to get refactor", K(ret), K(dag_merge_param));
  } else if (OB_UNLIKELY(1 != sstable_handles->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sstable count", KR(ret), K(sstable_handles));
  } else if (OB_ISNULL(sstable = static_cast<ObSSTable*>(sstable_handles->at(0).get_table()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be null", K(ret));
  }

  /* update table store */
  if (OB_FAIL(ret)) {
  } else if (nullptr != sstable) {
     ObUpdateTableStoreParam table_store_param(dag_merge_param.ddl_task_param_.snapshot_version_,
                                               tablet_handle.get_obj()->get_multi_version_start(),
                                               tablet_param->storage_schema_,
                                               ls_handle.get_ls()->get_rebuild_seq(),
                                               sstable);
    if (OB_FAIL(table_store_param.init_with_compaction_info(ObCompactionTableStoreParam(compaction::MINI_MERGE,
                                                                                        share::SCN::min_scn(),
                                                                                        false /* not need report */,
                                                                                        false /* has truncate info*/)))) {
      LOG_WARN("failed to init with compaction info", K(ret));
    } else {
      table_store_param.compaction_info_.clog_checkpoint_scn_ = sstable->get_end_scn();
      table_store_param.ha_info_.need_check_transfer_seq_ = true;
      table_store_param.ha_info_.transfer_seq_ = tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_;
      ObTabletHandle new_tablet_handle;
      if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(tablet_id, table_store_param, new_tablet_handle))) {
        LOG_WARN("failed to update tablet table store", K(ret), K(dag_merge_param), K(table_store_param));
      } else {
        FLOG_INFO("ddl update table store success", KPC(new_tablet_handle.get_obj()), K(table_store_param));
      }
    }
  }

  /* release ddl memtable */
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObTabletHandle new_tablet_handle;
    if (OB_TMP_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                              tablet_id,
                                              new_tablet_handle,
                                              ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("failed to get tablet", K(tmp_ret), K(dag_merge_param));
    } else if (OB_TMP_FAIL(new_tablet_handle.get_obj()->release_memtables(new_tablet_handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_))) {
      LOG_WARN("failed to release memtable", K(tmp_ret),
        "clog_checkpoint_scn", new_tablet_handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_);
    }
  }
  return ret;
}

ObIncMajorDDLMergeHelper::ObIncMajorDDLMergeHelper()
{

}

ObIncMajorDDLMergeHelper::~ObIncMajorDDLMergeHelper()
{

}

int ObIncMajorDDLMergeHelper::get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param)
{
  return OB_SUCCESS; /* do nothing */
}

int ObIncMajorDDLMergeHelper::check_need_merge(
    ObIDag *dag,
    ObDDLTabletMergeDagParamV2 &merge_param,
    bool &need_merge)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  ObTableStoreIterator ddl_table_iter;
  ObArray<ObDDLKVHandle> frozen_ddl_kvs;
  ObTransID oldest_trans_id;
  ObTxSEQ oldest_seq_no;
  bool no_need_merge_ddl_sstable = false;
  bool no_need_merge_ddl_kv = false;
  need_merge = false;

  // check param and HA status
  if (OB_UNLIKELY(!merge_param.is_valid() || nullptr == dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(merge_param), KPC(dag));
  } else if (OB_FAIL(merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (merge_param.for_major_) {
    // do not merge inc_major during migration because data could be incomplete
    const ObTabletHAStatus &ha_status = tablet_handle.get_obj()->get_tablet_meta().ha_status_;
    if (OB_UNLIKELY(!ha_status.is_data_status_complete())) {
      ret = OB_NO_NEED_MERGE;
      FLOG_INFO("tablet data is incomplete, no need to merge inc major", KR(ret),
          K(target_ls_id), K(target_tablet_id), K(ha_status), K(merge_param), KPC(dag));
    }
  }

  // check ddl kvs and ddl sstables for this merge
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
    LOG_WARN("failed to get ddl kv mgr", K(ret), K(tablet_handle));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_inc_major_ddl_sstables(
      merge_param.trans_id_, merge_param.seq_no_, tablet_handle.get_obj(), ddl_table_iter))) {
    LOG_WARN("failed to get inc major ddl sstables", KR(ret), K(merge_param), K(tablet_handle));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_frozen_inc_major_ddl_kvs(
      merge_param.trans_id_, merge_param.seq_no_, ddl_kv_mgr_handle, frozen_ddl_kvs))) {
    LOG_WARN("failed to get frozen inc major ddl kvs", KR(ret), K(merge_param), K(ddl_kv_mgr_handle));
  } else if (frozen_ddl_kvs.empty() && (0 == ddl_table_iter.count())) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("no need to merge", K(ret), K(merge_param), K(frozen_ddl_kvs.count()), K(ddl_table_iter.count()));
  } else {
    frozen_ddl_kvs.reset();
    ddl_table_iter.reset();
  }

  // check ddl kvs and ddl sstables prior to this merge
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_all_inc_major_ddl_sstables(tablet_handle.get_obj(), ddl_table_iter))) {
    LOG_WARN("failed to get all inc major ddl sstables", KR(ret), K(tablet_handle), K(merge_param));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_all_frozen_ddl_kvs(ddl_kv_mgr_handle, frozen_ddl_kvs))) {
    LOG_WARN("failed to get all frozen ddl kvs", KR(ret), K(ddl_kv_mgr_handle), K(merge_param));
  } else if (merge_param.for_major_) {
    ObITable *first_ddl_sstable = nullptr;
    if (ddl_table_iter.count() > 0) {
      if (OB_FAIL(ddl_table_iter.get_boundary_table(false/*is_last*/, first_ddl_sstable))) {
        LOG_WARN("failed to get boundary table", KR(ret));
      } else if (OB_ISNULL(first_ddl_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null first ddl sstable", KR(ret), KP(first_ddl_sstable));
      } else {
        const ObSSTable *sstable = static_cast<const ObSSTable *>(first_ddl_sstable);
        if (OB_FAIL(ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
            sstable, oldest_trans_id, oldest_seq_no))) {
          LOG_WARN("failed to get trans id and seq no from sstable", KR(ret), KPC(sstable));
        } else if (OB_UNLIKELY((oldest_trans_id != merge_param.trans_id_)
                            || (oldest_seq_no != merge_param.seq_no_))) {
          no_need_merge_ddl_sstable = true;
        }
      }
    } else {
      no_need_merge_ddl_sstable = true;
    }
    if (OB_SUCC(ret)) {
      if (frozen_ddl_kvs.count() > 0) {
        const ObDDLKV *ddl_kv = frozen_ddl_kvs.at(0).get_obj();
        if (ddl_kv->get_trans_id() != merge_param.trans_id_
            || ddl_kv->get_seq_no() != merge_param.seq_no_) {
          no_need_merge_ddl_kv = true;
        }
      } else {
        no_need_merge_ddl_kv = true;
      }
    }

    if (OB_SUCC(ret) && no_need_merge_ddl_sstable && no_need_merge_ddl_kv) {
      ret = OB_NO_NEED_MERGE;
    }
  } else {
    if (frozen_ddl_kvs.count() > 0) {
      const ObDDLKV *ddl_kv = frozen_ddl_kvs.at(0).get_obj();
      oldest_trans_id = ddl_kv->get_trans_id();
      oldest_seq_no = ddl_kv->get_seq_no();
      if (oldest_trans_id != merge_param.trans_id_
          || oldest_seq_no != merge_param.seq_no_) {
        ret = OB_NO_NEED_MERGE;
      }
    } else {
      ret = OB_NO_NEED_MERGE;
    }
  }

  if (OB_SUCC(ret)) {
    need_merge = true;
  } else if (OB_NO_NEED_MERGE == ret) {
    ret = OB_SUCCESS;
    need_merge = false;
  }

  FLOG_INFO("[INC_MAJOR_DDL_MERGE_TASK][CHECK_NEED_MERGE]", KR(ret),
      K(need_merge), K(merge_param), K(ddl_table_iter.count()), K(frozen_ddl_kvs.count()),
      K(oldest_trans_id), K(oldest_seq_no), K(no_need_merge_ddl_sstable), K(no_need_merge_ddl_kv));
  return ret;
}

int ObIncMajorDDLMergeHelper::calculate_rec_scn(
    const ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
    ObTableStoreIterator &ddl_table_iter,
    SCN &rec_scn)
{
  int ret = OB_SUCCESS;
  // for empty tablet, rec_scn should equal to start_scn
  SCN res_rec_scn = SCN::min_scn();

  if (OB_UNLIKELY(0 == ddl_table_iter.count() && frozen_ddl_kvs.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ddl sstables and frozen ddl kvs are both empty",
          KR(ret), K(ddl_table_iter.count()), K(frozen_ddl_kvs.count()));
  } else {
    // get rec scn from ddl kv
    for (int64_t i = 0; OB_SUCC(ret) && i < frozen_ddl_kvs.count(); ++i) {
      const ObDDLKVHandle &ddl_kv_handle = frozen_ddl_kvs.at(i);
      if (OB_UNLIKELY(!ddl_kv_handle.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ddl kv handle", KR(ret), K(i), K(ddl_kv_handle));
      } else {
        res_rec_scn = SCN::max(res_rec_scn, ddl_kv_handle.get_obj()->get_end_scn());
      }
    }

    // get rec scn from ddl sstable
    while(OB_SUCC(ret)) {
      ObITable *table = nullptr;
      if (OB_FAIL(ddl_table_iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next table", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", KR(ret), KPC(table));
      } else {
        res_rec_scn = SCN::max(res_rec_scn, table->get_rec_scn());
      }
    }
  } // end if

  if (OB_SUCC(ret)) {
    rec_scn = res_rec_scn;
  }
  return ret;
}

int ObIncMajorDDLMergeHelper::process_prepare_task(
    ObIDag *dag,
    ObDDLTabletMergeDagParamV2 &ddl_merge_param,
    ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices)
{
  int ret = OB_SUCCESS;

  int64_t merge_slice_idx = 0;
  bool for_major = false;

  cg_slices.reset();
  hash::ObHashSet<int64_t> slice_idxes;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArray<const ObSSTable*> ddl_memtables;

  ObArray<ObDDLKVHandle> frozen_ddl_kvs;
  ObStorageSchema *storage_schema = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;

  ObTableStoreIterator ddl_table_iter;
  ObDDLKVQueryParam ddl_kv_query_param;
  ddl_kv_query_param.ddl_kv_type_ = ObDDLKVType::DDL_KV_INC_MAJOR;
  ddl_kv_query_param.trans_id_ = ddl_merge_param.trans_id_;
  ddl_kv_query_param.seq_no_ = ddl_merge_param.seq_no_;

  if (!ddl_merge_param.is_valid() || nullptr == dag) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_merge_param), KPC(dag));
  } else if (OB_FAIL(ddl_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (FALSE_IT(for_major = ddl_merge_param.for_major_)) {
  } else if (OB_FAIL(slice_idxes.create(DDL_SLICE_BUCKET_NUM, ObMemAttr(MTL_ID(), "slice_idx_set")))) {
    LOG_WARN("create slice index set failed", K(ret));
  } else if (OB_FAIL(ddl_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (OB_ISNULL(merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx should not be null", K(ret), K(ddl_merge_param));
  }

  /* get frozen ddl kvs and ddl sstables */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*true*/))) {
    LOG_WARN("failed to get ddl kv mgr", K(ret), K(tablet_handle));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_inc_major_ddl_sstables(
      ddl_merge_param.trans_id_, ddl_merge_param.seq_no_, tablet_handle.get_obj(), ddl_table_iter))) {
    LOG_WARN("failed to get inc major ddl sstables", KR(ret), K(ddl_merge_param), K(tablet_handle));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_frozen_inc_major_ddl_kvs(
      ddl_merge_param.trans_id_, ddl_merge_param.seq_no_, ddl_kv_mgr_handle, frozen_ddl_kvs))) {
    LOG_WARN("failed to get frozen inc major ddl kvs", KR(ret), K(ddl_merge_param), K(ddl_kv_mgr_handle));
  } else if (frozen_ddl_kvs.empty() && (0 == ddl_table_iter.count())) {
    ret = OB_NO_NEED_MERGE;
    LOG_WARN("no need to merge", K(ret), K(ddl_merge_param));
  } else if (OB_FAIL(calculate_rec_scn(frozen_ddl_kvs, ddl_table_iter, ddl_merge_param.rec_scn_))) {
    LOG_WARN("failed to calculate rec scn", K(ret), K(frozen_ddl_kvs), K(ddl_table_iter));
  } else if (OB_FAIL(merge_ctx->ddl_kv_handles_.assign(frozen_ddl_kvs))) {
    LOG_WARN("failed to assign frozen ddl kvs", K(ret), K(frozen_ddl_kvs));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::close_ddl_kvs(frozen_ddl_kvs))) {
    LOG_WARN("failed to close frozen ddl kvs", K(ret), K(frozen_ddl_kvs));
  }

  if (OB_SUCC(ret)) {
    /* set slice range info */
    if (OB_FAIL(ObDDLMergeTaskUtils::get_ddl_memtables(frozen_ddl_kvs, ddl_memtables))) {
      LOG_WARN("get ddl memtables failed", K(ret), K(frozen_ddl_kvs));
    } else if (ddl_merge_param.need_merge_all_slice()
        || (ddl_memtables.empty() && !frozen_ddl_kvs.empty())) {
      if (OB_FAIL(slice_idxes.set_refactored(0))) {
        LOG_WARN("failed to set refactored", K(ret)); // should have at least one slice in slice idx
      } else {
        merge_slice_idx = 0; // merge all slice
      }
    } else {
      if (OB_FAIL(ObDDLMergeTaskUtils::get_merge_slice_idx(frozen_ddl_kvs, merge_slice_idx))) {
        LOG_WARN("failed to get merge slice idx", K(ret));
      } else if (OB_FAIL(ObDDLMergeTaskUtils::get_slice_indexes(ddl_memtables, slice_idxes))) { // get slice idx from ddl memtable only
        LOG_WARN("get slice indexes failed", K(ret), K(ddl_merge_param));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ddl_merge_param.init_cg_sstable_array(slice_idxes))) {
      LOG_WARN("failed to init cg sstable array", K(ret));
    }

    int64_t cg_count = ObITable::is_column_store_sstable(ddl_merge_param.table_key_.table_type_) ?
                           tablet_param->storage_schema_->get_column_group_count() : 1;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_count; idx++) {
      for (hash::ObHashSet<int64_t>::const_iterator iter = slice_idxes.begin();
          OB_SUCC(ret) && iter != slice_idxes.end();
          ++iter) {
        int64_t start_slice_idx = iter->first;
        int64_t end_slice_idx   = 0 == iter->first ? merge_slice_idx : iter->first;
        int64_t cg_idx = -1;
        if (OB_FAIL(tablet_param->storage_schema_->convert_iter_idx_to_column_group_idx(idx, cg_idx))) {
          LOG_WARN("failed to convert iter idx to cg idx", K(ret), K(idx), K(cg_count));
        } else if (OB_FAIL(cg_slices.push_back(ObTuple<int64_t, int64_t, int64_t>(cg_idx, start_slice_idx, end_slice_idx)))) {
          LOG_WARN("failed to push back val", K(ret), K(start_slice_idx), K(end_slice_idx));
        }
      }
    }
  } // end if

  FLOG_INFO("[INC_MAJOR_DDL_MERGE_TASK][PREPARE]", K(ret), K(target_ls_id), K(target_tablet_id),
      K(frozen_ddl_kvs.count()), K(ddl_memtables.count()), K(ddl_table_iter.count()), K(cg_slices.count()));
  return ret;
}

int ObIncMajorDDLMergeHelper::merge_cg_slice(ObIDag *dag,
                                             ObDDLTabletMergeDagParamV2 &merge_param,
                                             const int64_t cg_idx,
                                             const int64_t start_slice_idx,
                                             const int64_t end_slice_idx)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObArray<ObSSTable*> ddl_sstables;
  ObArray<ObDDLBlockMeta> sorted_metas;
  ObArray<ObDDLBlockMeta> tmp_metas;
  ObArray<ObStorageMetaHandle> meta_handles;
  ObDDLWriteStat write_stat;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;

  ObTabletDDLParam ddl_param;

  ObArenaAllocator arena(ObMemAttr(MTL_ID(), "merge_cg_slice"));
  ObTabletDDLCompleteMdsUserData ddl_data;


  if (OB_ISNULL(dag) || cg_idx < 0 || start_slice_idx < 0 || end_slice_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(dag), K(cg_idx), K(start_slice_idx), K(end_slice_idx));
  } else if (OB_FAIL(merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret), K(merge_param));
  } else  if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(merge_param));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid tablet handle", K(ret), K(tablet_handle));
  } else if (OB_FAIL(prepare_ddl_param(merge_param, cg_idx, start_slice_idx, end_slice_idx, ddl_param))) {
    LOG_WARN("failed to prepare ddl_param", K(ret));
  } else if (OB_FAIL(merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret), K(merge_param));
  } else if (OB_ISNULL(merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_ctx should not be null", K(ret));
  }

  /* !!! note !!!
   * following SNV2, sstable meta should follow first dump sstable if exist
  */
  SMART_VAR(ObTableStoreIterator, ddl_sstable_iter) {
    const ObITableReadInfo *cg_index_read_info = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_ddl_sstables(
        ddl_sstable_iter, merge_param.trans_id_, merge_param.seq_no_))) {
      LOG_WARN("failed to get inc major ddl sstable", K(ret), K(merge_param));
    } else if (OB_FAIL(ObDDLMergeTaskUtils::get_ddl_tables_from_dump_tables(
                                                      !ObITable::is_column_store_sstable(ddl_param.table_key_.table_type_),
                                                       ddl_sstable_iter,
                                                       cg_idx,
                                                       start_slice_idx,
                                                       merge_param.need_merge_all_slice() ? INT64_MAX : end_slice_idx,
                                                       ddl_sstables,
                                                       meta_handles))) {
      LOG_WARN("failed to get ddl tables from dump sstables", K(ret), K(merge_param), K(cg_idx), K(start_slice_idx), K(end_slice_idx));
    } else if (OB_FAIL(calculate_scn_range(merge_ctx->ddl_kv_handles_, ddl_sstables, merge_param.for_major_, ddl_param))) {
      LOG_WARN("failed to calculate scn range of ddl param", KR(ret),
          K(merge_ctx->ddl_kv_handles_), K(ddl_sstables), K(merge_param.for_major_), K(ddl_param));
    } else if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(cg_index_read_info))) {
      LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", K(ret));
    } else if (OB_FAIL(ObDDLMergeTaskUtils::get_ddl_tables_from_ddl_kvs(merge_ctx->ddl_kv_handles_,
                                                   cg_idx,
                                                   start_slice_idx,
                                                   merge_param.need_merge_all_slice() ? INT64_MAX : end_slice_idx,
                                                   ddl_sstables))) {
     LOG_WARN("failed to get ddl tables from  ddl kvs", K(ret));
    } else if (OB_FAIL(ObDDLMergeTaskUtils::get_sorted_meta_array(*tablet_handle.get_obj(),
                                                                  ddl_param,
                                                                  tablet_param->storage_schema_,
                                                                  ddl_sstables,
                                                                  (cg_idx == merge_param.table_key_.column_group_idx_ || cg_idx == HIDDEN_ROWKEY_COLUMN_GROUP_IDX) ?
                                                                      tablet_handle.get_obj()->get_rowkey_read_info()
                                                                      : *cg_index_read_info,
                                                                  arena, tmp_metas))) {
      LOG_WARN("failed to get sorted meta array", K(ret));
    } else if (OB_FAIL(ObDDLMergeTaskUtils::check_idempodency(tmp_metas, sorted_metas, &write_stat))) {
      LOG_WARN("failed to check idempotency", K(ret));
    } else if (merge_param.for_major_) {
      // ignore error before it's stable
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObIncDDLMergeTaskUtils::check_inc_major_write_stat(tablet_handle, merge_param, cg_idx, write_stat))) {
        LOG_WARN("failed to check inc major write stat", KR(tmp_ret), K(tablet_handle),
            K(target_ls_id), K(target_tablet_id), K(merge_param), K(cg_idx), K(write_stat));
      }
    }
  } // ddl_sstable_iter

  ObTableHandleV2 sstable_handle;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(*(tablet_handle.get_obj()),
                                                           ddl_param,
                                                           sorted_metas,
                                                           ObArray<MacroBlockId>(),
                                                           nullptr,
                                                           tablet_param->storage_schema_,
                                                           &merge_ctx->mutex_,
                                                           merge_ctx->arena_,
                                                           sstable_handle))) {
    LOG_WARN("failed to create sstable", K(ret), K(cg_idx), K(ddl_param));
  } else if (OB_FAIL(merge_param.set_cg_slice_sstable(start_slice_idx, cg_idx, sstable_handle))) {
    LOG_WARN("failed to set ddl sstable", K(ret), K(ddl_param), KPC(tablet_param->storage_schema_));
  }

  FLOG_INFO("[INC_MAJOR_DDL_MERGE_TASK][MERGE_CG_SLICE]", KR(ret), K(target_ls_id), K(target_tablet_id),
      K(cg_idx), K(start_slice_idx), K(end_slice_idx));
  return ret;
}

int ObIncMajorDDLMergeHelper::assemble_sstable(ObDDLTabletMergeDagParamV2 &merge_param)
{
  int ret = OB_SUCCESS;
  bool for_major = merge_param.for_major_;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;

  ObTabletHandle tablet_handle;
  ObSSTable *inc_major_sstable = nullptr;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObTablesHandleArray co_sstable_array;
  ObTableStoreIterator inc_major_iter;
  bool major_already_included = false;
  bool inc_major_already_exist = false;
  bool sstables_empty = false;

  /* check param and get ctx */
  if (!merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(merge_param));
  } else if (OB_FAIL(merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret), K(tablet_param));
  } else if (OB_FAIL(merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret), K(merge_param));
  } else if (OB_ISNULL(merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge ctx should not be null", K(ret), KP(merge_ctx), K(merge_param));
  }

  /* check inc major sstable exist and build sstable */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(target_ls_id), K(target_tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_sstables(
      inc_major_iter, merge_param.trans_id_, merge_param.seq_no_))) {
    LOG_WARN("failed to get inc major sstables", K(ret), K(merge_param));
  } else if (OB_UNLIKELY(inc_major_iter.count() > 0)) {
    inc_major_already_exist = true;
    FLOG_INFO("no need to build sstable because inc major sstable already exist",
        K(inc_major_iter), K(inc_major_already_exist), K(merge_param));
  } else if (OB_FAIL(ObDDLMergeTaskUtils::build_sstable(merge_param, co_sstable_array, inc_major_sstable))) {
    LOG_WARN("failed to build sstable", KR(ret), K(merge_param));
  } else if (for_major) {
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    const ObSSTable *first_major_sstable = nullptr;
    if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("fail to fetch table store", K(ret));
    } else if (OB_FALSE_IT(first_major_sstable = static_cast<ObSSTable *>(
        table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)))) {
    } else if (OB_NOT_NULL(first_major_sstable)
        && OB_UNLIKELY(first_major_sstable->get_snapshot_version() >= merge_param.inc_major_trans_version_)) {
      major_already_included = true;
      FLOG_INFO("snapshot version is already included in major sstable",
          K(major_already_included), KPC(first_major_sstable), K(merge_param));
    } else if (OB_FAIL(check_sstables_empty(merge_param, co_sstable_array, sstables_empty))) {
      LOG_WARN("failed to check sstables empty", KR(ret), K(co_sstable_array));
    }

    if (OB_FAIL(ret)) {
    } else if (major_already_included || sstables_empty) {
      inc_major_sstable = nullptr;
      FLOG_INFO("no need to record inc major sstable to tablet table store", K(target_ls_id), K(target_tablet_id),
          K(major_already_included), K(sstables_empty), K(co_sstable_array), KP(inc_major_sstable), K(merge_param));
    }
  }

  /* update tablet table store */
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(inc_major_sstable) && OB_FAIL(verify_inc_major_sstable(target_ls_id, *inc_major_sstable, tablet_handle))) {
    LOG_WARN("failed to verify inc major sstable", KR(ret), K(target_ls_id), K(target_tablet_id), KPC(inc_major_sstable));
  } else if (OB_FAIL(update_tablet_table_store(merge_param, co_sstable_array, inc_major_sstable))) {
    LOG_WARN("failed to update tablet table store", K(ret), K(merge_param));
  }

  /* release ddl kvs after merge */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("failed to get ddl kv mgr handle", K(ret));
  } else if (OB_UNLIKELY(merge_ctx->ddl_kv_handles_.empty())) {
    LOG_INFO("no need to release ddl kv because frozen ddl kvs are empty", K(merge_param));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(DDL_KV_INC_MAJOR, merge_param.rec_scn_))) {
    LOG_WARN("release all ddl kv failed", K(ret), K(merge_param));
  }

  char extra_info[512];
  snprintf(extra_info, sizeof(extra_info), "table_key:{table_type:%s, column_group_idx:%ld, slice_range:{start_slice_idx:%ld, end_slice_idx:%ld}, scn_range:{start_scn:%ld, end_scn:%ld}}, for_major:%s, sstables_empty:%s, start_scn:%ld, rec_scn:%ld",
      ObITable::get_table_type_name(merge_param.table_key_.table_type_),
      merge_param.table_key_.column_group_idx_,
      merge_param.table_key_.slice_range_.start_slice_idx_,
      merge_param.table_key_.slice_range_.end_slice_idx_,
      merge_param.table_key_.scn_range_.start_scn_.get_val_for_tx(),
      merge_param.table_key_.scn_range_.end_scn_.get_val_for_tx(),
      for_major ? "true" : "false",
      sstables_empty ? "true" : "false",
      merge_param.start_scn_.get_val_for_tx(),
      merge_param.rec_scn_.get_val_for_tx());
  SERVER_EVENT_ADD("direct_load", "ddl merge inc major sstable",
                   "tenant_id", MTL_ID(),
                   "ret", ret,
                   "trace_id", *ObCurTraceId::get_trace_id(),
                   "tablet_id", target_tablet_id,
                   "trans_id", merge_param.trans_id_,
                   "seq_no", merge_param.seq_no_,
                   extra_info);

  FLOG_INFO("[INC_MAJOR_DDL_MERGE_TASK][ASSEMBLE_SSTABLE]", KR(ret), K(target_ls_id), K(target_tablet_id),
      K(major_already_included), K(inc_major_already_exist), K(sstables_empty), K(merge_param), KP(inc_major_sstable));
  return ret;
}

bool ObIncMajorDDLMergeHelper::is_supported_direct_load_type(const ObDirectLoadType direct_load_type)
{
  return (ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR== direct_load_type);
}

int ObIncMajorDDLMergeHelper::calculate_scn_range(
    const ObIArray<ObDDLKVHandle> &ddl_kvs,
    const ObIArray<ObSSTable *> &ddl_sstables,
    const bool for_major,
    ObTabletDDLParam &ddl_param)
{
  int ret = OB_SUCCESS;
  SCN min_start_scn = SCN::min_scn();
  SCN max_end_scn = SCN::min_scn();
  SCN ddl_sstable_start_scn = SCN::max_scn();
  SCN ddl_sstable_end_scn = SCN::min_scn();
  SCN ddl_kv_start_scn = SCN::max_scn();
  SCN ddl_kv_end_scn = SCN::min_scn();

  if (OB_UNLIKELY(ddl_kvs.empty() && ddl_sstables.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty ddl kvs and ddl sstables", KR(ret), K(ddl_kvs), K(ddl_sstables));
  }

  if (OB_FAIL(ret)) {
  } else if (ddl_sstables.count() > 0) {
    // ddl sstables' scn ranges are not in order
    ObSSTable *ddl_sstable = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && (i < ddl_sstables.count()); ++i) {
      if (OB_ISNULL(ddl_sstable = ddl_sstables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ddl sstable", KR(ret), KP(ddl_sstable), K(i));
      } else {
        ddl_sstable_start_scn = SCN::min(ddl_sstable_start_scn, ddl_sstable->get_start_scn());
        ddl_sstable_end_scn = SCN::max(ddl_sstable_end_scn, ddl_sstable->get_end_scn());
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ddl_kvs.count() > 0) {
    // ddl kvs' scn ranges are in order
    const ObDDLKVHandle &first_kv_handle = ddl_kvs.at(0);
    const ObDDLKVHandle &last_kv_handle = ddl_kvs.at(ddl_kvs.count() - 1);
    if (!first_kv_handle.is_valid() || !last_kv_handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid ddl kv handle", KR(ret), K(first_kv_handle), K(last_kv_handle));
    } else {
      ddl_kv_start_scn = first_kv_handle.get_obj()->get_start_scn();
      ddl_kv_end_scn = last_kv_handle.get_obj()->get_end_scn();
    }
  }

  if (OB_SUCC(ret)) {
    min_start_scn = SCN::min(ddl_sstable_start_scn, ddl_kv_start_scn);
    max_end_scn = SCN::max(ddl_sstable_end_scn, ddl_kv_end_scn);
    if (for_major) {
      // 1. The start_scn of an inc_major_sstable is the commit_scn of prior incremental major direct load,
      //    which must be smaller than the start_log's start_scn of current direct load
      // 2. The end_scn of an inc_major_sstable is the commit_scn of current incremental major direct load
      if (OB_UNLIKELY((min_start_scn >= ddl_param.start_scn_)
                   || (max_end_scn != ddl_param.rec_scn_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected start_scn or end_scn for inc major merge",
            KR(ret), K(for_major), K(min_start_scn), K(max_end_scn), K(ddl_param));
      } else {
        ddl_param.table_key_.scn_range_.start_scn_ = ddl_param.start_scn_;
        ddl_param.table_key_.scn_range_.end_scn_ = ddl_param.rec_scn_;
      }
    } else {
      ddl_param.table_key_.scn_range_.start_scn_ = min_start_scn;
      ddl_param.table_key_.scn_range_.end_scn_ = max_end_scn;
    }
    FLOG_INFO("inc major calculate scn range", KR(ret), K(ddl_param),
        K(min_start_scn), K(max_end_scn), K(for_major),
        K(ddl_sstable_start_scn), K(ddl_sstable_end_scn),
        K(ddl_kv_start_scn), K(ddl_kv_end_scn));
  }
  return ret;
}

int ObIncMajorDDLMergeHelper::check_sstables_empty(
    const ObDDLTabletMergeDagParamV2 &merge_param,
    const ObTablesHandleArray &table_array,
    bool &is_empty)
{
  int ret = OB_SUCCESS;
  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  is_empty = true;

  if (OB_UNLIKELY(!merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge param", KR(ret), K(merge_param));
  } else if (OB_FAIL(merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", KR(ret), K(merge_param));
  } else if (ObITable::is_column_store_sstable(merge_param.table_key_.table_type_)) {
    for (int64_t i = 0; OB_SUCC(ret) && is_empty && (i < table_array.get_count()); ++i) {
      ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(table_array.get_table(i));
      if (OB_ISNULL(co_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null co sstable", KR(ret), KP(co_sstable));
      } else if (co_sstable->get_cs_meta().data_macro_block_cnt_ > 0) {
        is_empty = false;
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_empty && (i < table_array.get_count()); ++i) {
      ObSSTable *sstable = static_cast<ObSSTable *>(table_array.get_table(i));
      if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sstable", KR(ret), KP(sstable));
      } else {
        is_empty = sstable->is_empty();
      }
    }
  }
  return ret;
}

int ObIncMajorDDLMergeHelper::verify_inc_major_sstable(
    const ObLSID &ls_id,
    const ObSSTable &inc_major_sstable,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  ObTransID trans_id;
  ObTxSEQ seq_no;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "VerifyInc"));
  ObTabletDDLCompleteMdsUserData user_data;
  ObLSHandle ls_handle;
  ObMigrationStatus migration_status;
  bool migration_failed = false;

  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else if (OB_FAIL(ObIncMajorTxHelper::get_ls(ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid ls handle", KR(ret), K(ls_handle));
  } else if (OB_UNLIKELY(!inc_major_sstable.is_inc_major_type_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table type", KR(ret), K(inc_major_sstable));
  } else if (OB_FAIL(inc_major_sstable.get_meta(meta_handle))) {
    LOG_WARN("failed to get sstable meta handle", KR(ret), K(inc_major_sstable));
  } else if (OB_UNLIKELY(meta_handle.get_sstable_meta().get_data_macro_block_count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty inc major sstable", KR(ret), K(meta_handle.get_sstable_meta()));
  } else if (OB_FAIL(ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(&inc_major_sstable, trans_id, seq_no))) {
    LOG_WARN("failed to get trans id and seq no from sstable", KR(ret), K(inc_major_sstable));
  } else if (OB_FAIL(ls_handle.get_ls()->get_ls_meta().get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", KR(ret), K(ls_id));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL == migration_status) {
    migration_failed = true;
    FLOG_INFO("ls migration failed, skip verify inc major sstable", K(trans_id), K(seq_no),
        K(ls_id), K(tablet_handle.get_obj()->get_tablet_id()), K(migration_status));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_direct_load_info(
      SCN::max_scn(), allocator, ObTabletDDLCompleteMdsUserDataKey(trans_id), user_data))) {
    LOG_WARN("failed to get inc major direct load info", KR(ret), K(trans_id));
  } else if (OB_UNLIKELY(inc_major_sstable.get_key().get_start_scn() != user_data.start_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid start scn", K(ret), K(inc_major_sstable.get_key()), K(user_data));
  } else if (OB_UNLIKELY(inc_major_sstable.get_key().get_end_scn() != user_data.inc_major_commit_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid end scn", K(ret), K(inc_major_sstable.get_key()), K(user_data));
  }

  const char *event_name = migration_failed ? "skip ddl merge verify inc major sstable due to the failure of ls migration"
                                            : "ddl merge verify inc major sstable";
  SERVER_EVENT_ADD("direct_load", event_name,
                   "tenant_id", MTL_ID(),
                   "ret", ret,
                   "trace_id", *ObCurTraceId::get_trace_id(),
                   "tablet_id", tablet_handle.get_obj()->get_tablet_id(),
                   "trans_id", trans_id,
                   "seq_no", seq_no);
  return ret;
}

int ObIncMajorDDLMergeHelper::update_tablet_table_store(
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    ObTablesHandleArray &co_sstable_array,
    ObSSTable *major_sstable)
{
  int ret = OB_SUCCESS;

  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  bool for_major = dag_merge_param.for_major_;

  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObArenaAllocator arena("IncMajorUpdTS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObStorageSchema *storage_schema = nullptr;

  int64_t rebuild_seq = -1;
  int64_t snapshot_version = 0;
  int64_t multi_version_start = 0;

  if (!dag_merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param), K(major_sstable), K(co_sstable_array));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(target_ls_id, target_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(target_ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(target_ls_id));
  } else if (!ls_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls handle should be valid", K(ret), K(ls_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(arena, storage_schema))) {
    LOG_WARN("failed to load storage schema", KR(ret), K(tablet_handle));
  } else if (OB_ISNULL(storage_schema) || OB_UNLIKELY(!storage_schema->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid storage schema", KR(ret), KPC(storage_schema));
  } else {
    rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    snapshot_version = tablet_handle.get_obj()->get_snapshot_version();
  }

  if (OB_FAIL(ret)) {
  } else {
    UpdateUpperTransParam upper_trans_param;
    ObSEArray<int64_t, 4> gc_inc_major_ddl_scns;
    gc_inc_major_ddl_scns.set_attr(ObMemAttr(MTL_ID(), "GCIncDDLScn"));

    ObUpdateTableStoreParam table_store_param(snapshot_version, multi_version_start, storage_schema, rebuild_seq, major_sstable, true);
    ObMergeType merge_type = compaction::MERGE_TYPE_MAX;
    if (for_major) {
      if (OB_NOT_NULL(major_sstable)) {
        merge_type = compaction::MEDIUM_MERGE;
      }
    } else {
      merge_type = compaction::MINI_MERGE;
    }

    if (OB_FAIL(table_store_param.init_with_compaction_info(ObCompactionTableStoreParam(merge_type,
                                                                                        share::SCN::min_scn(),
                                                                                        false /* need_report*/,
                                                                                        false /* has truncate info*/)))) {
      LOG_WARN("init with compaction info failed", K(ret));
    } else if (for_major && OB_ISNULL(major_sstable)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < co_sstable_array.get_count(); i++) {
        const ObITable *cur_table = co_sstable_array.get_table(i);
        if (OB_ISNULL(cur_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null table", K(ret), K(i), KP(cur_table));
        } else if (OB_FAIL(gc_inc_major_ddl_scns.push_back(cur_table->get_end_scn().get_val_for_tx()))) {
          LOG_WARN("failed to push back gc inc major ddl scn", K(ret), K(i), KPC(cur_table));
        }
      }

      if (OB_SUCC(ret) && !gc_inc_major_ddl_scns.empty()) {
        upper_trans_param.gc_inc_major_ddl_scns_ = &gc_inc_major_ddl_scns;
        table_store_param.set_upper_trans_param(upper_trans_param);
      }
    }

    if (OB_SUCC(ret)) {
      table_store_param.ddl_info_.keep_old_ddl_sstable_ = !for_major;
      table_store_param.ddl_info_.ddl_checkpoint_scn_ = dag_merge_param.rec_scn_;

      for (int64_t i = 0; !for_major && OB_SUCC(ret) && i < co_sstable_array.get_count(); i++ ) {
        const ObSSTable *cur_slice_sstable = static_cast<ObSSTable *>(co_sstable_array.get_table(i));
        if (OB_ISNULL(cur_slice_sstable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("slice sstable is null", K(ret), K(i), KP(cur_slice_sstable));
        } else if (OB_FAIL(table_store_param.ddl_info_.slice_sstables_.push_back(cur_slice_sstable))) {
          LOG_WARN("push back slice ddl sstable failed", K(ret), K(i), KPC(cur_slice_sstable));
        }
      }
      DEBUG_SYNC(BEFORE_INC_MAJOR_DDL_MERGE_UPDATE_TABLE_STORE);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(target_tablet_id, table_store_param, new_tablet_handle))) {
        LOG_WARN("failed to update tablet table store", K(ret), K(target_tablet_id), K(table_store_param));
      }
    }
  }
  ObTabletObjLoadHelper::free(arena, storage_schema);
  return ret;
}


} // namespace  storage
} // namespace oceanbase
