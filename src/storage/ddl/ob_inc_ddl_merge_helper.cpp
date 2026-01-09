/**
 * Copyright (c) 2025 OceanBase
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
#include "storage/ddl/ob_inc_ddl_merge_helper.h"
#include "storage/ddl/ob_ddl_merge_task_utils.h"
#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/direct_load/ob_direct_load_auto_inc_seq_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#endif

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
#ifdef OB_BUILD_SHARED_STORAGE
      if (OB_SUCC(ret) && GCTX.is_shared_storage_mode()) {
        ObSSTableUploadRegHandle upload_register_handle;
        if (OB_FAIL(ls_handle.get_ls()->prepare_register_sstable_upload(upload_register_handle))) {
          LOG_WARN("fail to prepare register sstable upload", KR(ret));
        } else {
          SCN snapshot_version(SCN::min_scn());
          if (OB_FAIL(new_tablet_handle.get_obj()->get_snapshot_version(snapshot_version))) {
            LOG_WARN("get snapshot version failed", K(new_tablet_handle));
          } else {
            ASYNC_UPLOAD_INC_SSTABLE(SSIncSSTableType::MINI_SSTABLE,
                                     upload_register_handle,
                                     sstable->get_key(),
                                     snapshot_version);
          }
        }
      }
#endif
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
    for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx < cg_count; cg_idx++) {
      for (hash::ObHashSet<int64_t>::const_iterator iter = slice_idxes.begin();
          OB_SUCC(ret) && iter != slice_idxes.end();
          ++iter) {
        int64_t start_slice_idx = iter->first;
        int64_t end_slice_idx   = 0 == iter->first ? merge_slice_idx : iter->first;
        if (OB_FAIL(cg_slices.push_back(ObTuple<int64_t, int64_t, int64_t>(cg_idx, start_slice_idx, end_slice_idx)))) {
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
                                                                  cg_idx == merge_param.table_key_.column_group_idx_ ?
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
  } else if (OB_NOT_NULL(inc_major_sstable) && OB_FAIL(verify_inc_major_sstable(*inc_major_sstable, tablet_handle))) {
    LOG_WARN("failed to verify inc major sstable", KR(ret), KPC(inc_major_sstable), K(tablet_handle));
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
    const ObSSTable &inc_major_sstable,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  ObTransID trans_id;
  ObTxSEQ seq_no;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "VerifyInc"));
  ObTabletDDLCompleteMdsUserData user_data;

  if (OB_UNLIKELY(!inc_major_sstable.is_inc_major_type_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table type", KR(ret), K(inc_major_sstable));
  } else if (OB_FAIL(inc_major_sstable.get_meta(meta_handle))) {
    LOG_WARN("failed to get sstable meta handle", KR(ret), K(inc_major_sstable));
  } else if (OB_UNLIKELY(meta_handle.get_sstable_meta().get_data_macro_block_count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty inc major sstable", KR(ret), K(meta_handle.get_sstable_meta()));
  } else if (OB_FAIL(ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(&inc_major_sstable, trans_id, seq_no))) {
    LOG_WARN("failed to get trans id and seq no from sstable", KR(ret), K(inc_major_sstable));
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

  SERVER_EVENT_ADD("direct_load", "ddl merge verify inc major sstable",
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

#ifdef OB_BUILD_SHARED_STORAGE
ObSSIncMajorDDLMergeHelper::ObSSIncMajorDDLMergeHelper()
{
}

ObSSIncMajorDDLMergeHelper::~ObSSIncMajorDDLMergeHelper()
{
}

int ObSSIncMajorDDLMergeHelper::prepare_ss_merge_param(
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    ObLSID &ls_id,
    ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle,
    ObStorageSchema *&storage_schema,
    ObWriteTabletParam *&tablet_param,
    ObDDLTabletContext::MergeCtx *&merge_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dag_merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(ls_id, tablet_id, tablet_param))) {
    LOG_WARN("failed to get merge param", K(ret));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (OB_UNLIKELY(nullptr == tablet_param || nullptr == merge_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_ctx is nullptr", K(ret), K(dag_merge_param), K(tablet_param), K(merge_ctx));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_ISNULL(storage_schema = tablet_param->storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema is nullptr", K(ret), K(dag_merge_param));
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::init_cg_sstable_array(
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    hash::ObHashSet<int64_t> &slice_idxes)
{
  int ret = OB_SUCCESS;
  /* prepare cg sstable array in context
   * for ss mode, slice count should always be 1
   */
  if (OB_FAIL(slice_idxes.create(1007, ObMemAttr(MTL_ID(), "slice_idx_set")))) {
    LOG_WARN("create slice index set failed", K(ret));
  } else if (OB_FAIL(slice_idxes.set_refactored(0))) {
    LOG_WARN("failed to set refactored", K(ret)); // should have at least one slice in slice idx
  } else if (OB_FAIL(dag_merge_param.init_cg_sstable_array(slice_idxes))) { /* for both major and dump only slice exist*/
    LOG_WARN("failed to set cg slice sstable", K(ret), K(dag_merge_param));
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::process_prepare_task(
    ObIDag *dag,
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    common::ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices)
{
  int ret = OB_SUCCESS;
  cg_slices.reset();

  ObLSID ls_id;
  ObTabletID tablet_id;
  ObTabletHandle tablet_handle;
  ObStorageSchema *storage_schema = nullptr;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;

  hash::ObHashSet<int64_t> slice_idxes;

  if (OB_UNLIKELY(nullptr == dag || !dag_merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(dag), K(dag_merge_param));
  } else if (OB_UNLIKELY(!is_supported_direct_load_type(dag_merge_param.direct_load_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported direct load type", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(prepare_ss_merge_param(dag_merge_param,
                                            ls_id,
                                            tablet_id,
                                            tablet_handle,
                                            storage_schema,
                                            tablet_param,
                                            merge_ctx))) {
    LOG_WARN("fail to prepare ss merge param", KR(ret), K(dag_merge_param));
  } else if (OB_FAIL(init_cg_sstable_array(dag_merge_param, slice_idxes))) {
    LOG_WARN("fail to init cg sstable array", KR(ret));
  } else if (ObITable::is_inc_major_ddl_dump_sstable(dag_merge_param.table_key_.table_type_)) {
    if (OB_FAIL(prepare_for_dump_sstable(tablet_handle,
                                         dag_merge_param,
                                         merge_ctx,
                                         cg_slices))) {
      LOG_WARN("failed to prepare for dump sstable", K(ret));
    }
  } else if (ObITable::is_inc_major_type_sstable(dag_merge_param.table_key_.table_type_)) {
    if (OB_FAIL(calculate_inc_major_end_scn(dag_merge_param, tablet_handle))) {
      LOG_WARN("fail to calculate inc major end scn", KR(ret));
    }
    /* for ss mode, inc major build from cg meta file, follower not need build inc major*/
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_param->storage_schema_->get_column_group_count(); ++i) {
      /* when build major only one slice is needed */
      if (OB_FAIL(cg_slices.push_back(ObTuple<int64_t, int64_t, int64_t>(i /* cg_idx */, 0 /* start_slice */, 0 /* end_slice */)))) {
        LOG_WARN("failed to get slice", K(ret));
      }
    }
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::calculate_inc_major_end_scn(
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArray<ObDDLKVHandle> ddl_kvs;
  ObTableStoreIterator sstable_iter;
  SCN start_scn = SCN::max_scn(); // unused
  if (OB_UNLIKELY(!dag_merge_param.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dag_merge_param), K(tablet_handle));
  } else if (OB_FAIL(get_ddl_kvs_and_ddl_dump_sstables(tablet_handle,
                                                       dag_merge_param,
                                                       false/*frozen_only*/,
                                                       ddl_kvs,
                                                       sstable_iter))) {
    LOG_WARN("fail to get ddl kvs and ddl dump sstables", KR(ret), K(dag_merge_param));
  } else if (OB_FAIL(get_min_max_scn(ddl_kvs, sstable_iter, start_scn, end_scn_))) {
    LOG_WARN("fail to get min max scn", KR(ret));
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::merge_cg_slice(
    ObIDag* dag,
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    const int64_t cg_idx,
    const int64_t start_slice,
    const int64_t end_slice)
{
  int ret = OB_SUCCESS;
  if (nullptr == dag || !dag_merge_param.is_valid()
                     || cg_idx < 0
                     || start_slice > end_slice) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(dag), K(dag_merge_param),
                                 K(cg_idx), K(start_slice), K(end_slice));
  } else if (dag_merge_param.for_major_ &&
             OB_FAIL(merge_cg_sstable(dag, dag_merge_param, cg_idx))) {
    LOG_WARN("failed to merge cg sstable", K(ret));
  } else if (!dag_merge_param.for_major_ &&
             OB_FAIL(merge_dump_sstable(dag_merge_param))) {
    LOG_WARN("failed to dump sstable", K(ret), K(dag_merge_param));
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::get_rec_scn(ObDDLTabletMergeDagParamV2 &merge_param)
{
  return ObIDDLMergeHelper::get_rec_scn_from_ddl_kvs(merge_param);
}

int ObSSIncMajorDDLMergeHelper::assemble_sstable(ObDDLTabletMergeDagParamV2 &dag_merge_param)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObWriteTabletParam  *tablet_param = nullptr;
  ObTabletHandle tablet_handle;
  ObSSTable *inc_major_sstable = nullptr;
  ObTablesHandleArray co_sstable_array;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;

  if (OB_UNLIKELY(!dag_merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(ls_id, tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is invalid", KR(ret), K(tablet_handle));
  } else if (OB_FAIL(ObSSDDLMergeHelper::build_sstable(dag_merge_param, co_sstable_array, inc_major_sstable))) {
    LOG_WARN("failed to build major sstable", K(ret), KPC(inc_major_sstable));
  } else if (OB_FAIL(update_tablet_table_store(dag_merge_param, co_sstable_array, inc_major_sstable))) {
    LOG_WARN("failed to update tablet table store", K(ret), KPC(inc_major_sstable));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("ddl kv mgr not exist", KR(ret), K(ls_id), K(tablet_id));
      ret = OB_TASK_EXPIRED;
    } else {
      LOG_WARN("fail to get ddl kv mgr", KR(ret));
    }
  } else if (!dag_merge_param.for_major_) {
    if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(ObDDLKVType::DDL_KV_INC_MAJOR,
                                                             dag_merge_param.rec_scn_))) {
      LOG_WARN("fail to release ddl kv", KR(ret), K(ls_id), K(tablet_id), K(dag_merge_param.rec_scn_));
    }
  } else {
    if (OB_FAIL(release_ddl_kv_for_major(dag_merge_param, ddl_kv_mgr_handle))) {
      LOG_WARN("fail to try release ddl kv", KR(ret));
    }
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::release_ddl_kv_for_major(
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObArray<ObDDLKVHandle> frozen_ddl_kvs;
  if (OB_UNLIKELY(!dag_merge_param.is_valid() || !ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dag_merge_param), K(ddl_kv_mgr_handle));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(SCN::min_scn()/*start_scn*/,
                                                                0/*snapshot_version*/,
                                                                0/*data_format_version*/,
                                                                SCN::min_scn()/*freeze_scn*/,
                                                                ObDDLKVType::DDL_KV_INC_MAJOR,
                                                                dag_merge_param.trans_id_,
                                                                dag_merge_param.seq_no_))) {
    LOG_WARN("fail to freeze ddl kv", KR(ret), K(dag_merge_param));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_frozen_inc_major_ddl_kvs(dag_merge_param.trans_id_,
                                                                          dag_merge_param.seq_no_,
                                                                          ddl_kv_mgr_handle,
                                                                          frozen_ddl_kvs))) {
    LOG_WARN("fail to get ddl kvs", KR(ret), K(dag_merge_param));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::close_ddl_kvs(frozen_ddl_kvs))) {
    LOG_WARN("fail to close ddl kvs", KR(ret));
  }
  // TODO @ganxin : Release all ddl kv after adding wait task.
  else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->release_ddl_kvs(ObDDLKVType::DDL_KV_INC_MAJOR, SCN::max_scn()))) {
    LOG_WARN("fail to release ddl kv", KR(ret));
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::merge_dump_sstable(ObDDLTabletMergeDagParamV2 &dag_merge_param)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObTabletHandle tablet_handle;
  ObArenaAllocator *allocator = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;

  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObTableStoreIterator sstable_iter;
  ObIArray<ObDDLKVHandle> *frozen_ddl_kvs = nullptr;

  ObTableHandleV2 ro_sstable_handle;
  ObArray<MacroBlockId> macro_id_array;
  hash::ObHashSet<MacroBlockId, hash::NoPthreadDefendMode> macro_id_set;

  ObTabletDDLParam ddl_param;
  /* check arg invalid */
  if (OB_UNLIKELY(!dag_merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dag_merge_param));
  } else if (!is_supported_direct_load_type(dag_merge_param.direct_load_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported direct load type", KR(ret), K(dag_merge_param));
  } else if (OB_FAIL(prepare_ss_merge_param(dag_merge_param,
                                            ls_id,
                                            tablet_id,
                                            tablet_handle,
                                            storage_schema,
                                            tablet_param,
                                            merge_ctx))) {
    LOG_WARN("fail to prepare ss merge param", KR(ret));
  } else if (FALSE_IT(allocator = &merge_ctx->arena_)) {
  } else if (OB_FAIL(macro_id_set.create(1023, ObMemAttr(MTL_ID(), "SS_INC_MAJOR_ID")))) {
    LOG_WARN("create set of macro block id failed", KR(ret));
  }

  ObArray<ObDDLBlockMeta> empty_meta_array;
  /* prepare ddl kv mgr and get frozne ddl kv */
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_ddl_sstables(sstable_iter,
                                                                         dag_merge_param.trans_id_,
                                                                         dag_merge_param.seq_no_))) {
    LOG_WARN("failed to get inc major sstable iter", KR(ret), K(dag_merge_param));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("ddl kv mgr not exist", KR(ret), K(ls_id), K(tablet_id));
      ret = OB_TASK_EXPIRED;
    } else {
      LOG_WARN("failed to get ddl kv mgr handle", K(ret));
    }
  } else if (OB_ISNULL(frozen_ddl_kvs = &merge_ctx->ddl_kv_handles_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frozen ddl kvs should not be null", KR(ret));
  } else if (OB_UNLIKELY(sstable_iter.count() == 0 && frozen_ddl_kvs->count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sstable_iter.count()), K(frozen_ddl_kvs->count()));
  } else if (OB_FAIL(get_macro_id_from_sstable(sstable_iter, macro_id_set))) {
    LOG_WARN("failed to get macro id from sstable", KR(ret));
  } else if (OB_FAIL(get_macro_id_from_ddl_kv(*frozen_ddl_kvs, macro_id_set))) {
    LOG_WARN("failed to get macro id from ddl kv", KR(ret));
  } else if (OB_FAIL(persist_macro_id(macro_id_set, macro_id_array))) {
    LOG_WARN("failed to persist macro id", KR(ret));
  } else if (OB_FAIL(prepare_ddl_param(dag_merge_param,
                                       0 /* cg_idx */,
                                       0 /* start_slice_idx */,
                                       0 /* end_slice_idx */,
                                       ddl_param))) {
    LOG_WARN("failed to prepare ddl param", KR(ret));
  } else if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(*(tablet_handle.get_obj()),
                                                         ddl_param,
                                                         empty_meta_array,
                                                         macro_id_array,
                                                         nullptr/*first ddl sstable*/,
                                                         storage_schema,
                                                         &merge_ctx->mutex_,
                                                         *allocator,
                                                         ro_sstable_handle))) {
    LOG_WARN("create sstable failed", KR(ret), K(ddl_param));
  } else if (OB_FAIL(dag_merge_param.set_cg_slice_sstable(0, 0, ro_sstable_handle))) {
    LOG_WARN("failed to set cg slice sstable", KR(ret));
  }
  return ret;
}

ERRSIM_POINT_DEF(FAIL_SS_DIRECT_LOAD_INC_MAJOR);
int ObSSIncMajorDDLMergeHelper::merge_cg_sstable(ObIDag* dag,
                                                 ObDDLTabletMergeDagParamV2 &dag_merge_param,
                                                 int64_t cg_idx)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObTabletHandle tablet_handle;
  ObArenaAllocator *allocator = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;

  ObITable::TableKey cur_cg_table_key;
  ObTableHandleV2 cg_sstable_handle;
  ObArray<ObMacroMetaStoreManager::StoreItem> sorted_meta_stores;

  int64_t create_schema_version_on_tablet = 0;
  ObDDLIndependentDag *independent_dag = nullptr;
  ObTabletDDLParam ddl_param;
  ObDDLWriteStat write_stat;
  if (OB_UNLIKELY(FAIL_SS_DIRECT_LOAD_INC_MAJOR)) {
    ret = FAIL_SS_DIRECT_LOAD_INC_MAJOR;
    LOG_WARN("set ret = FAIL_SS_DIRECT_LOAD_INC_MAJOR");
  } else if (OB_UNLIKELY(nullptr == dag || !dag_merge_param.is_valid()
                                 || !dag_merge_param.for_major_
                                 || cg_idx < 0
                                 || !dag->is_independent())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(dag), K(dag_merge_param), K(cg_idx));
  } else if (OB_ISNULL(independent_dag = static_cast<ObDDLIndependentDag *>(dag))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("independent dag is nullptr", KR(ret), KPC(dag));
  } else if (OB_FAIL(prepare_ss_merge_param(dag_merge_param,
                                            ls_id,
                                            tablet_id,
                                            tablet_handle,
                                            storage_schema,
                                            tablet_param,
                                            merge_ctx))) {
    LOG_WARN("fail to prepare ss merge param", KR(ret), K(dag_merge_param));
  } else if (OB_ISNULL(allocator = &merge_ctx->arena_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", KR(ret));
  } else if (OB_FAIL(prepare_cg_table_key(dag_merge_param,
                                          cg_idx,
                                          storage_schema,
                                          tablet_handle,
                                          cur_cg_table_key))) {
    LOG_WARN("failed to prepare cg table key", KR(ret), K(dag_merge_param),
                                               K(cg_idx), K(tablet_handle),
                                               K(cur_cg_table_key));
  } else if (OB_FAIL(ObSSDDLMergeHelper::get_meta_store_store(dag_merge_param,
                                                              tablet_id,
                                                              cg_idx,
                                                              sorted_meta_stores))) {
    LOG_WARN("failed to get store mgr", KR(ret), K(tablet_id), K(cg_idx));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", KR(ret));
  } else {
    ObMacroSeqParam macro_seq_param;
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    storage::ObDDLIncRedoLogWriterCallback clustered_index_flush_callback;
    ObDDLRedoLogWriterCallbackInitParam init_param;
    ObSSTableMergeRes res;
    const ObStorageColumnGroupSchema &cg_schema = storage_schema->get_column_groups().at(cg_idx);
    ObMacroDataSeq tmp_seq;
    ObDirectLoadAutoIncSeqData inc_start_seq;
    if (OB_FAIL(ObDirectLoadAutoIncSeqService::get_start_seq(ls_id, tablet_id, compaction::MACRO_STEP_SIZE, inc_start_seq))) {
      LOG_WARN("fail to get start seq", KR(ret), K(ls_id), K(tablet_id), K(inc_start_seq));
    } else {
      tmp_seq.macro_data_seq_ = inc_start_seq.get_seq_val();
      macro_seq_param.start_ = inc_start_seq.get_seq_val();
      init_param.ls_id_ = ls_id;
      init_param.tablet_id_ = tablet_id;
      init_param.direct_load_type_ = dag_merge_param.direct_load_type_;
      init_param.block_type_ = DDL_MB_INDEX_TYPE;
      init_param.table_key_ = cur_cg_table_key;
      init_param.start_scn_ = dag_merge_param.start_scn_;
      init_param.task_id_ = dag_merge_param.ddl_task_param_.ddl_task_id_;
      init_param.data_format_version_ = dag_merge_param.ddl_task_param_.tenant_data_version_;
      init_param.parallel_cnt_ = max(dag_merge_param.get_tablet_ctx()->slice_count_, 1);
      init_param.cg_cnt_ = storage_schema->get_column_group_count();
      init_param.row_id_offset_ = 0;
      init_param.need_delay_ = false;
      init_param.tx_desc_ = independent_dag->get_tx_info().tx_desc_;
      init_param.trans_id_ = dag_merge_param.trans_id_;
      init_param.seq_no_ = dag_merge_param.seq_no_;
    }

    ObWholeDataStoreDesc data_desc;
    ObSSTableIndexBuilder sstable_index_builder(true);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(clustered_index_flush_callback.init(init_param))) {
      LOG_WARN("fail to init sstable index builder", KR(ret), K(init_param));
    } else if (OB_FAIL(data_desc.init(true/*is_ddl*/,
                                      *storage_schema,
                                      ls_id,
                                      tablet_id,
                                      compaction::ObMergeType::MAJOR_MERGE,
                                      dag_merge_param.table_key_.get_snapshot_version(),
                                      dag_merge_param.ddl_task_param_.tenant_data_version_,
                                      tablet_param->is_micro_index_clustered_,
                                      tablet_param->tablet_transfer_seq_,
                                      0, /* concurrent_cnt */
                                      tablet_handle.get_obj()->get_reorganization_scn(),
                                      SCN::min_scn(),
                                      &cg_schema,
                                      cg_idx,
                                      compaction::ObExecMode::EXEC_MODE_OUTPUT,
                                      true/*need_submit_io*/,
                                      true/*is_inc_major*/))) {
      LOG_WARN("fail to init data desc", KR(ret));
    } else if (FALSE_IT(data_desc.get_static_desc().schema_version_ = storage_schema->get_schema_version())) {
    } else if (OB_FAIL(sstable_index_builder.init(data_desc.get_desc(),
                                                  ObSSTableIndexBuilder::DISABLE/*small SSTable op*/))) {
      LOG_WARN("fail to init sstable index builder", KR(ret), K(data_desc));
    } else if (OB_FAIL(rebuild_index(sstable_index_builder, ls_id, tablet_id, dag_merge_param,
                                     tablet_param, macro_seq_param, clustered_index_flush_callback,
                                     sorted_meta_stores, tmp_seq, init_param, res, write_stat) )) {
      LOG_WARN("failed to rebuild index", KR(ret), K(ls_id), K(tablet_id), K(dag_merge_param),
                                                  K(tablet_param), K(macro_seq_param),
                                                  K(sorted_meta_stores), K(tmp_seq), K(res));
    } else if (FALSE_IT(cur_cg_table_key.scn_range_.end_scn_ = end_scn_)) {
    } else if (OB_FAIL(create_sstable(dag_merge_param, *allocator, merge_ctx,
                                      res, cur_cg_table_key, storage_schema,
                                      create_schema_version_on_tablet,
                                      cg_sstable_handle))) {
      LOG_WARN("failed to create sstable", KR(ret), K(dag_merge_param), KP(merge_ctx), K(res),
                                           K(cur_cg_table_key), K(storage_schema),
                                           K(create_schema_version_on_tablet));
    } else if (OB_FAIL(dag_merge_param.set_cg_slice_sstable(0, cg_idx, cg_sstable_handle))) {
      LOG_WARN("failed to set cg slice sstable", K(ret), K(dag_merge_param));
    }
  }

  if (OB_SUCC(ret)) {
    // ignore error before it's stable
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObIncDDLMergeTaskUtils::check_inc_major_write_stat(tablet_handle, dag_merge_param, cg_idx, write_stat))) {
      LOG_WARN("failed to check inc major write stat", KR(tmp_ret), K(tablet_handle),
          K(ls_id), K(tablet_id), K(dag_merge_param), K(cg_idx), K(write_stat));
    }
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::get_macro_id_from_sstable(
    ObTableStoreIterator &sstable_iter,
    MacroBlockIdSet &macro_id_set)
{
  int ret = OB_SUCCESS;
  sstable_iter.resume();
  ObSSTableMetaHandle meta_handle;
  ObMacroIdIterator macro_id_iter;
  while (OB_SUCC(ret)) {
    ObITable *table = nullptr;
    ObSSTable *sstable = nullptr;
    meta_handle.reset();
    macro_id_iter.reset();
    if (OB_FAIL(sstable_iter.get_next(table))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next table failed", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_ISNULL(table)
               || !table->is_sstable()
               || OB_ISNULL(sstable = static_cast<ObSSTable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, table is nullptr", KR(ret), KPC(table), KPC(sstable));
    } else if (OB_FAIL(sstable->get_meta(meta_handle))) {
      LOG_WARN("get sstable meta handle failed", KR(ret));
    } else if (OB_FAIL(meta_handle.get_sstable_meta()
                                  .get_macro_info().get_other_block_iter(macro_id_iter))) {
      LOG_WARN("get other block iterator failed", KR(ret));
    } else {
      MacroBlockId macro_id;
      while (OB_SUCC(ret)) {
        macro_id.reset();
        if (OB_FAIL(macro_id_iter.get_next_macro_id(macro_id))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next macro id failed", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(macro_id_set.set_refactored(macro_id))) {
          LOG_WARN("set macro id failed", KR(ret), K(macro_id));
        }
      }
    }
  }
  return ret;
}

bool ObSSIncMajorDDLMergeHelper::is_supported_direct_load_type(
     const ObDirectLoadType direct_load_type)
{
  return direct_load_type == DIRECT_LOAD_INCREMENTAL_MAJOR;
}

// TODO @zhuoran.zzr Reuse this function.
int ObSSIncMajorDDLMergeHelper::get_macro_id_from_ddl_kv(
    ObIArray<ObDDLKVHandle> &frozen_ddl_kvs,
    MacroBlockIdSet &macro_id_set)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < frozen_ddl_kvs.count(); ++i) {
    ObDDLKV *cur_kv = frozen_ddl_kvs.at(i).get_obj();
    ObDDLMemtable *ddl_memtable = nullptr;
    ObArray<MacroBlockId> macro_id_array;
    if (OB_ISNULL(cur_kv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl kv is nullptr", KR(ret));
    } else if (cur_kv->get_ddl_memtables().empty()) {
      // do nothing
    } else if (OB_ISNULL(ddl_memtable = cur_kv->get_ddl_memtables().at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl_mem_table is nullptr", KR(ret));
    } else if (OB_FAIL(ddl_memtable->get_block_meta_tree()->get_macro_id_array(macro_id_array))) {
      LOG_WARN("get macro id array failed", KR(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < macro_id_array.count(); ++j) {
        if (OB_FAIL(macro_id_set.set_refactored(macro_id_array.at(j)))) {
          LOG_WARN("set macro id failed", KR(ret), K(macro_id_array.at(j)));
        }
      }
    }
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::persist_macro_id(
    const MacroBlockIdSet &macro_id_set,
    ObIArray<MacroBlockId> &macro_id_array)
{
  int ret = OB_SUCCESS;
  macro_id_array.reset();
  MacroBlockIdSet::const_iterator iter = macro_id_set.begin();
  if (OB_FAIL(macro_id_array.reserve(macro_id_set.size()))) {
    LOG_WARN("reserve macro id array failed", KR(ret));
  }
  for (; OB_SUCC(ret) && iter != macro_id_set.end(); ++iter) {
    if (OB_FAIL(macro_id_array.push_back(iter->first))) {
      LOG_WARN("push back macro id failed", KR(ret), K(iter->first));
    }
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::prepare_for_dump_sstable(
    const ObTabletHandle &tablet_handle,
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    ObDDLTabletContext::MergeCtx *merge_ctx,
    common::ObIArray<ObTuple<int64_t, int64_t, int64_t>> &cg_slices)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArray<ObDDLKVHandle> frozen_ddl_kvs;
  ObTableStoreIterator sstable_iter;
  ObDDLKVQueryParam ddl_kv_query_param;
  ddl_kv_query_param.ddl_kv_type_ = ObDDLKVType::DDL_KV_INC_MAJOR;
  ddl_kv_query_param.trans_id_ = dag_merge_param.trans_id_;
  ddl_kv_query_param.seq_no_ = dag_merge_param.seq_no_;

  /* freeze & get frozen ddl kvs */
  if (OB_UNLIKELY(!tablet_handle.is_valid() || !dag_merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_handle), K(dag_merge_param));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::freeze_inc_major_ddl_kv(tablet_handle,
                                                                     SCN::min_scn()/*freeze_scn*/,
                                                                     dag_merge_param.trans_id_,
                                                                     dag_merge_param.seq_no_,
                                                                     dag_merge_param.ddl_task_param_.snapshot_version_,
                                                                     dag_merge_param.ddl_task_param_.tenant_data_version_,
                                                                     false/*is_replay*/))) {
    LOG_WARN("fail to freeze inc major ddl kv", KR(ret), K(dag_merge_param));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr not exist", KR(ret), K(dag_merge_param));
    } else {
      LOG_WARN("get ddl kv mgr failed", KR(ret), K(dag_merge_param));
    }
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(true/*frozen_only*/,
                                                              frozen_ddl_kvs,
                                                              ddl_kv_query_param))) {
    LOG_WARN("get freezed ddl kv failed", KR(ret), K(dag_merge_param), K(ddl_kv_query_param));
  } else if (OB_FAIL(merge_ctx->ddl_kv_handles_.assign(frozen_ddl_kvs))) {
    LOG_WARN("failed to frozen ddl kv", KR(ret));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::close_ddl_kvs(frozen_ddl_kvs))) {
    LOG_WARN("failed to close ddl kvs", KR(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_ddl_sstables(sstable_iter,
                                                                         dag_merge_param.trans_id_,
                                                                         dag_merge_param.seq_no_))) {
    LOG_WARN("fail to get inc major ddl sstables", KR(ret), K(dag_merge_param));
  } else if (OB_FAIL(get_min_max_scn(frozen_ddl_kvs,
                                     sstable_iter,
                                     dag_merge_param.table_key_.scn_range_.start_scn_,
                                     dag_merge_param.table_key_.scn_range_.end_scn_))) {
    LOG_WARN("fail to get min max scn", KR(ret));
  } else if (OB_FAIL(cg_slices.push_back(ObTuple<int64_t, int64_t, int64_t>(0, 0, 0)))) {
    LOG_WARN("failed to push back values", K(ret));
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::prepare_cg_table_key(
    const ObDDLTabletMergeDagParamV2 &dag_merge_param,
    const int64_t cg_idx,
    const ObStorageSchema *storage_schema,
    const ObTabletHandle &tablet_handle,
    ObITable::TableKey &cur_cg_table_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dag_merge_param.is_valid()
                  || cg_idx < 0
                  || nullptr == storage_schema
                  || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param), K(cg_idx),
                                 KPC(storage_schema), K(tablet_handle));
  } else if (FALSE_IT(cur_cg_table_key = dag_merge_param.table_key_)) {/* init table key */
  } else if (!storage_schema->is_row_store()) {
    if (cg_idx == dag_merge_param.table_key_.column_group_idx_) {
      /* update for cg when target is column store */
      cur_cg_table_key.table_type_ =  ObITable::TableType::INC_COLUMN_ORIENTED_SSTABLE;
    } else {
      cur_cg_table_key.table_type_ =  ObITable::TableType::INC_NORMAL_COLUMN_GROUP_SSTABLE;
      cur_cg_table_key.column_group_idx_ = cg_idx;
    }
  }

  if (OB_SUCC(ret)) {
    ObTabletDDLCompleteMdsUserData user_data;
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SS_INC_MAJOR"));
    if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_direct_load_info(
                                                share::SCN::max_scn(),
                                                allocator,
                                                ObTabletDDLCompleteMdsUserDataKey(dag_merge_param.trans_id_),
                                                user_data))) {
      LOG_WARN("fail to get inc major direct load info", KR(ret), K(dag_merge_param));
    } else if (OB_UNLIKELY(!user_data.start_scn_.is_valid_and_not_min())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected inc major start scn", KR(ret), K(user_data));
    } else {
      cur_cg_table_key.scn_range_.start_scn_ = user_data.start_scn_;
    }
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::rebuild_index(
    ObSSTableIndexBuilder &sstable_index_builder,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    ObWriteTabletParam *tablet_param,
    ObMacroSeqParam &macro_seq_param,
    ObDDLIncRedoLogWriterCallback &clustered_index_flush_callback,
    ObIArray<ObMacroMetaStoreManager::StoreItem> &sorted_meta_stores,
    ObMacroDataSeq &tmp_seq,
    ObDDLRedoLogWriterCallbackInitParam &init_param,
    ObSSTableMergeRes &res,
    ObDDLWriteStat &write_stat)
{
  int ret = OB_SUCCESS;
  ObIndexBlockRebuilder index_block_rebuilder;
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || !tablet_id.is_valid()
                  || !dag_merge_param.is_valid())) {
    LOG_WARN("invalid argument", K(ls_id), K(tablet_id), K(dag_merge_param));
  } else if (OB_FAIL(index_block_rebuilder.init(sstable_index_builder,
                                         macro_seq_param,
                                         0/*task_idx*/,
                                         dag_merge_param.table_key_,
                                         tablet_param->is_micro_index_clustered_ ? &clustered_index_flush_callback : nullptr))) {
    LOG_WARN("fail to alloc index builder", K(ret));
  } else if (OB_FAIL(append_macro_rows_to_index_rebuilder(sorted_meta_stores,
                                                          index_block_rebuilder,
                                                          write_stat))) {
    LOG_WARN("failed to append macro rows to index rebuilder", KR(ret));
  } else if (OB_FAIL(close_sstable_index_builder(ls_id, tablet_id, init_param,
                                                 index_block_rebuilder,
                                                 sstable_index_builder, tmp_seq, res))) {
    LOG_WARN("failed to close sstable index builder", KR(ret), K(ls_id), K(tablet_id),
                                                      K(init_param), K(res));
  } else if (OB_FAIL(record_root_seq(ls_id, tablet_id, res))) {
    LOG_WARN("failed to record root seq", KR(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::append_macro_rows_to_index_rebuilder(
    ObIArray<ObMacroMetaStoreManager::StoreItem> &sorted_meta_stores,
    ObIndexBlockRebuilder &index_block_rebuilder,
    ObDDLWriteStat &write_stat)
{
  int ret = OB_SUCCESS;
  write_stat.reset();
  SMART_VARS_3((ObMacroMetaTempStoreIter, macro_meta_iter),
                (ObDataMacroBlockMeta, macro_meta),
                (ObMicroBlockData, leaf_index_block)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < sorted_meta_stores.count(); ++i) {
      ObMacroMetaTempStore *cur_macro_meta_store = sorted_meta_stores.at(i).macro_meta_store_;
      macro_meta_iter.reset();
      macro_meta.reset();
      leaf_index_block.reset();
      if (OB_ISNULL(cur_macro_meta_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current macro meta store is null", K(ret));
      } else if (OB_FAIL(macro_meta_iter.init(*cur_macro_meta_store))) {
        LOG_WARN("init macro meta store iterator failed", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(macro_meta_iter.get_next(macro_meta, leaf_index_block))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next macro meta failed", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(index_block_rebuilder.append_macro_row(macro_meta, leaf_index_block.get_buf(), leaf_index_block.get_buf_size()))) {
            LOG_WARN("append macro row failed", K(ret), K(macro_meta), K(leaf_index_block));
          } else {
            write_stat.row_count_ += macro_meta.val_.row_count_;
            LOG_TRACE("rebuilder append macro row", K(ret), K(macro_meta));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::create_sstable(
    const ObDDLTabletMergeDagParamV2 &dag_merge_param,
    ObArenaAllocator &allocator,
    ObDDLTabletContext::MergeCtx *merge_ctx,
    ObSSTableMergeRes &res,
    const ObITable::TableKey &cur_cg_table_key,
    const ObStorageSchema *storage_schema,
    const int64_t create_schema_version_on_tablet,
    ObTableHandleV2 &cg_sstable_handle)
{
  int ret = OB_SUCCESS;
  ObTabletCreateSSTableParam param;
  if (OB_UNLIKELY(!dag_merge_param.is_valid()
                  || nullptr == merge_ctx
                  || !res.is_valid()
                  || !cur_cg_table_key.is_valid()
                  || nullptr == storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dag_merge_param), KP(merge_ctx), K(res),
                                 K(cur_cg_table_key), KP(storage_schema));
  } else if (OB_FAIL(param.init_for_ss_ddl(res,
                                    cur_cg_table_key,
                                    *storage_schema,
                                    create_schema_version_on_tablet,
                                    dag_merge_param.direct_load_type_,
                                    dag_merge_param.trans_id_,
                                    dag_merge_param.seq_no_))) {
    LOG_WARN("fail to init ddl param for ddl", KR(ret),
                                               K(create_schema_version_on_tablet),
                                               K(cur_cg_table_key), KPC(storage_schema),
                                               K(dag_merge_param.trans_id_),
                                               K(dag_merge_param.seq_no_));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", KR(ret), K(param.is_valid()), K(param));
  } else {
    ObMutexGuard guard(merge_ctx->mutex_);
    if (cur_cg_table_key.is_co_sstable()) {
      if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param, allocator, cg_sstable_handle))) {
        LOG_WARN("create co sstable failed", KR(ret), K(param));
      }
    } else {
      if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(param, allocator, cg_sstable_handle))) {
        LOG_WARN("create ro sstable failed", KR(ret), K(param));
      }
    }
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::close_sstable_index_builder(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObDDLRedoLogWriterCallbackInitParam &init_param,
    ObIndexBlockRebuilder &index_block_rebuilder,
    ObSSTableIndexBuilder &sstable_index_builder,
    ObMacroDataSeq &tmp_seq,
    ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  tmp_seq.set_index_block();
  share::ObPreWarmerParam pre_warm_param;
  storage::ObDDLIncRedoLogWriterCallback flush_callback;
  init_param.direct_load_type_ = DIRECT_LOAD_INCREMENTAL_MAJOR;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !init_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(init_param));
  } else if (OB_FAIL(index_block_rebuilder.close())) {
    LOG_WARN("close index block rebuilder failed", KR(ret));
  } else if (OB_FAIL(pre_warm_param.init(ls_id, tablet_id))) {
    LOG_WARN("failed to init pre warm param", KR(ret));
  } else if (OB_FAIL(flush_callback.init(init_param))) {
    // The row_id_offset of cg index_macro_block has no effect, so set 0 to pass the defensive check.
    LOG_WARN("fail to init redo log writer callback", KR(ret), K(init_param));
  } else if (OB_FAIL(sstable_index_builder.close_with_macro_seq(
                     res,
                     tmp_seq.macro_data_seq_,
                     OB_DEFAULT_MACRO_BLOCK_SIZE /*nested_size*/,
                     0 /*nested_offset*/,
                     pre_warm_param, &flush_callback))) {
    LOG_WARN("fail to close", KR(ret), K(sstable_index_builder), K(tmp_seq));
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::record_root_seq(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  ObDirectLoadAutoIncSeqData inc_start_seq;
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || !tablet_id.is_valid()
                  || !res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(res));
  } else if (OB_FAIL(ObDirectLoadAutoIncSeqService::get_start_seq(ls_id, tablet_id, 1, inc_start_seq))) {
    LOG_WARN("failed to get inc start seq", KR(ret));
  } else {
    res.root_macro_seq_ = inc_start_seq.get_seq_val();
    LOG_INFO("[SS INC MAJOR]record root seq",
             K(ret), K(ls_id), K(tablet_id), K(res.root_macro_seq_), K(inc_start_seq));
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::update_tablet_table_store(
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    ObTablesHandleArray &table_array,
    ObSSTable *inc_major_sstable)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObTabletHandle tablet_handle;
  ObWriteTabletParam *tablet_param = nullptr;

  ObLSHandle ls_handle;
  ObTabletHandle new_tablet_handle;
  ObArenaAllocator arena("IncMajorUpdTS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObStorageSchema *storage_schema = nullptr;

  int64_t rebuild_seq = -1;
  int64_t snapshot_version = 0;
  const bool for_major = dag_merge_param.for_major_;
  ObLSService *ls_service = MTL(ObLSService*);

  if (OB_UNLIKELY(nullptr == ls_service || !dag_merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls_service), K(dag_merge_param));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(ls_id,
                                                      tablet_id,
                                                      tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (!ls_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls handle", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id,
                                                            tablet_id,
                                                            tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(arena, storage_schema))) {
    LOG_WARN("failed to load storage schema", KR(ret), K(tablet_handle));
  } else if (OB_ISNULL(storage_schema) || OB_UNLIKELY(!storage_schema->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid storage schema", KR(ret), KPC(storage_schema));
  } else {
    rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    snapshot_version = tablet_handle.get_obj()->get_snapshot_version();
  }

  if (OB_SUCC(ret)) {
    ObUpdateTableStoreParam table_store_param(snapshot_version,
                                              ObVersionRange::MIN_VERSION/*multi_version_start*/,
                                              storage_schema,
                                              rebuild_seq,
                                              inc_major_sstable);
    if (OB_FAIL(table_store_param.init_with_compaction_info(
                                  ObCompactionTableStoreParam(for_major ? compaction::MEDIUM_MERGE
                                                                        : compaction::MINOR_MERGE,
                                  share::SCN::min_scn(),
                                  false /* need_report*/,
                                  false /* has truncate info*/)))) {
      LOG_WARN("init with compaction info failed", K(ret));
    } else {
      table_store_param.ddl_info_.keep_old_ddl_sstable_ = !dag_merge_param.table_key_.is_inc_major_type_sstable();
      table_store_param.ddl_info_.ddl_checkpoint_scn_ = dag_merge_param.rec_scn_;

      for (int64_t i = 0; !for_major && OB_SUCC(ret) && i < table_array.get_count(); i++ ) {
        const ObSSTable *cur_slice_sstable = static_cast<ObSSTable *>(table_array.get_table(i));
        if (OB_ISNULL(cur_slice_sstable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("slice sstable is null", K(ret), K(i), KP(cur_slice_sstable));
        } else if (OB_FAIL(table_store_param.ddl_info_.slice_sstables_.push_back(cur_slice_sstable))) {
          LOG_WARN("push back slice ddl sstable failed", K(ret), K(i), KPC(cur_slice_sstable));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!for_major) {
        /* update local merge param for dump sstable */
        if (OB_FAIL(update_tablet_table_store_for_dump_sstable(dag_merge_param,
                                                               ls_id,
                                                               ls_handle,
                                                               tablet_id,
                                                               table_store_param,
                                                               new_tablet_handle))) {
          LOG_WARN("fail to update tablet table store for dump sstable", KR(ret));
        }
      } else {
        if (OB_FAIL(update_tablet_table_store_for_inc_major(dag_merge_param, inc_major_sstable))) {
          LOG_WARN("fail to update tablet table store for inc major", KR(ret));
        }
      }
    }
  }
  ObTabletObjLoadHelper::free(arena, storage_schema);
  return ret;
}

int ObSSIncMajorDDLMergeHelper::update_tablet_table_store_for_dump_sstable(
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    const ObLSID &ls_id,
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const ObUpdateTableStoreParam &update_table_store_param,
    ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!dag_merge_param.is_valid()
                  || !ls_id.is_valid()
                  || !ls_handle.is_valid()
                  || !tablet_id.is_valid()
                  || !update_table_store_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param), K(ls_id), K(ls_handle),
                                 K(tablet_id), K(update_table_store_param),
                                 K(new_tablet_handle));
  } else if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(tablet_id,
                                                                   update_table_store_param,
                                                                   new_tablet_handle))) {
    LOG_WARN("failed to update tablet table store", K(ret), K(tablet_id),
                                                    K(update_table_store_param));
  } else if (OB_FAIL(MTL(ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id))) {
    LOG_WARN("fail to submit tablet update task", K(ret), K(ls_id), K(tablet_id));
  } else {
    LOG_INFO("update tablet table store for inc major dump sstable success",
             K(ls_id), K(tablet_id), K(update_table_store_param));
    if (GCTX.is_shared_storage_mode()) {
      ObSSTableUploadRegHandle upload_register_handle;
      if (OB_FAIL(ls_handle.get_ls()->prepare_register_sstable_upload(upload_register_handle))) {
        LOG_WARN("fail to prepare register sstable upload", KR(ret));
      } else {
        ASYNC_UPLOAD_INC_SSTABLE(SSIncSSTableType::INC_MAJOR_DDL_SSTABLE,
                                upload_register_handle,
                                dag_merge_param.table_key_,
                                SCN::min_scn(),
                                dag_merge_param.trans_id_,
                                dag_merge_param.seq_no_);
        LOG_INFO("[SS INC MAJOR]async upload inc major ddl dump sstable",
                 KR(ret), K(dag_merge_param.trans_id_), K(dag_merge_param.seq_no_));
      }
    }
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::update_tablet_table_store_for_inc_major(
    ObDDLTabletMergeDagParamV2 &dag_merge_param,
    const ObSSTable *inc_major_sstable)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObLSHandle ls_handle;
  ObTabletID tablet_id;
  ObTabletHandle tablet_handle;
  ObArenaAllocator *allocator = nullptr;
  ObWriteTabletParam *tablet_param = nullptr;
  ObDDLTabletContext::MergeCtx *merge_ctx = nullptr;

  int64_t rebuild_seq = -1;
  int64_t snapshot_version = 0;

  ObLSService *ls_service = MTL(ObLSService*);
  share::SCN transfer_scn;
  ObITable::TableKey table_key;
  ObArray<ObDDLKVHandle> ddl_kvs;
  ObTableStoreIterator ddl_dump_sstables;
  SCN min_scn = SCN::max_scn(); // unused

  if (OB_UNLIKELY(!dag_merge_param.is_valid()
                  || OB_ISNULL(inc_major_sstable)
                  || OB_ISNULL(ls_service))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dag_merge_param),
                                 KP(inc_major_sstable), KP(ls_service));
  } else if (OB_FAIL(dag_merge_param.get_tablet_param(ls_id,
                                                      tablet_id,
                                                      tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(dag_merge_param.get_merge_ctx(merge_ctx))) {
    LOG_WARN("failed to get merge ctx", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (!ls_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls handle", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id,
                                                            tablet_id,
                                                            tablet_handle))) {
    LOG_WARN("failed to get tablet handle", K(ret));
  } else if (OB_FAIL(get_ddl_kvs_and_ddl_dump_sstables(tablet_handle,
                                                       dag_merge_param,
                                                       true/*frozen_only*/,
                                                       ddl_kvs,
                                                       ddl_dump_sstables))) {
    LOG_WARN("failed to get ddl kvs and ddl dump sstables", KR(ret), K(tablet_handle), K(dag_merge_param));
  } else if (OB_FAIL(get_min_max_scn(ddl_kvs, ddl_dump_sstables, min_scn, dag_merge_param.rec_scn_))) {
    LOG_WARN("failed to get min max scn", KR(ret), K(dag_merge_param));
  } else {
    rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    snapshot_version = tablet_handle.get_obj()->get_snapshot_version();
    transfer_scn = tablet_handle.get_obj()->get_reorganization_scn();
    table_key = dag_merge_param.table_key_;
    allocator = &merge_ctx->arena_;
  }

  if (OB_FAIL(ret)) {
  } else if (inc_major_sstable->is_empty()) {
    LOG_INFO("inc major sstable is empty", K(dag_merge_param), KPC(inc_major_sstable));
  } else if (OB_FAIL(ObSSDDLUtil::update_shared_tablet_table_store(ls_handle,
                                                              *inc_major_sstable,
                                                              *tablet_param->storage_schema_,
                                                              dag_merge_param.ddl_task_param_.tenant_data_version_,
                                                              transfer_scn))) {
    LOG_WARN("failed to update shared tablet", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id))) {
    LOG_WARN("fail to submit tablet update task", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(share::SSCompactHelper::link_inc_major(ls_id,
                                                    tablet_id,
                                                    snapshot_version,
                                                    dag_merge_param.trans_id_,
                                                    dag_merge_param.seq_no_))) {
    LOG_WARN("fail to link inc major", KR(ret));
  } else {
    ObUpdateTableStoreParam table_store_param(snapshot_version,
                                              ObVersionRange::MIN_VERSION/*multi_version_start*/,
                                              tablet_param->storage_schema_,
                                              rebuild_seq);
    table_store_param.ddl_info_.ddl_checkpoint_scn_ = dag_merge_param.rec_scn_;
    ObTabletHandle new_tablet_handle; // unused
    if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(tablet_id,
                                                              table_store_param,
                                                              new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", KR(ret), K(tablet_id),
                                                      K(table_store_param), K(dag_merge_param));
    }
  }
  FLOG_INFO("[SS INC MAJOR]update tablet table store for inc major", KR(ret), K(tablet_id), K(dag_merge_param));
  return ret;
}

int ObSSIncMajorDDLMergeHelper::get_ddl_kvs_and_ddl_dump_sstables(
    const ObTabletHandle &tablet_handle,
    const ObDDLTabletMergeDagParamV2 &dag_merge_param,
    const bool frozen_only,
    ObIArray<ObDDLKVHandle> &ddl_kvs,
    ObTableStoreIterator &ddl_dump_sstables)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObDDLKVQueryParam query_param;
  query_param.ddl_kv_type_ = ObDDLKVType::DDL_KV_INC_MAJOR;
  query_param.trans_id_ = dag_merge_param.trans_id_;
  query_param.seq_no_ = dag_merge_param.seq_no_;
  if (OB_UNLIKELY(!tablet_handle.is_valid() || !dag_merge_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_handle), K(dag_merge_param));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("ddl kv mgr not exist", KR(ret), K(dag_merge_param));
      ret = OB_TASK_EXPIRED;
    } else {
      LOG_WARN("fail to get ddl kv mgr", KR(ret));
    }
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(frozen_only, ddl_kvs, query_param))) {
    LOG_WARN("failed to get ddl kvs", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_ddl_sstables(ddl_dump_sstables,
                                                                         dag_merge_param.trans_id_,
                                                                         dag_merge_param.seq_no_))) {
    LOG_WARN("failed to get inc major ddl sstables", K(ret));
  }
  return ret;
}

int ObSSIncMajorDDLMergeHelper::get_min_max_scn(
    const ObIArray<ObDDLKVHandle> &ddl_kvs,
    ObTableStoreIterator &sstable_iter,
    SCN &min_scn,
    SCN &max_scn)
{
  int ret = OB_SUCCESS;
  min_scn = SCN::max_scn();
  max_scn = SCN::min_scn();
  for (int64_t i = 0; OB_SUCC(ret) && i < ddl_kvs.count(); ++i) {
    const ObDDLKVHandle &ddl_kv_handle = ddl_kvs.at(i);
    if (OB_UNLIKELY(!ddl_kv_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl kv handle is invalid", KR(ret), K(ddl_kv_handle));
    } else {
      min_scn = SCN::min(min_scn, ddl_kv_handle.get_obj()->get_start_scn());
      max_scn = SCN::max(max_scn, SCN::max(ddl_kv_handle.get_obj()->get_end_scn(),
                                           ddl_kv_handle.get_obj()->get_max_scn()));
    }
  }

  ObITable *table = nullptr;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_iter.get_next(table))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is nullptr", KR(ret));
    } else {
      min_scn = SCN::min(min_scn, table->get_start_scn());
      max_scn = SCN::max(max_scn, table->get_end_scn());
    }
  }
  return ret;
}
#endif

} // namespace  storage
} // namespace oceanbase
