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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_iter_utils.h"

namespace oceanbase
{
namespace sql
{
/***************** PUBLIC BEGIN *****************/
void ObDASIterUtils::init_scan_iter_param(ObDASScanIterParam &param, const ObDASScanCtDef *scan_ctdef, ObDASBaseRtDef *scan_rtdef)
{
  param.scan_ctdef_ = scan_ctdef;
  param.max_size_ = scan_rtdef->eval_ctx_->is_vectorized() ? scan_rtdef->eval_ctx_->max_batch_size_ : 1;
  param.eval_ctx_ = scan_rtdef->eval_ctx_;
  param.exec_ctx_ = &scan_rtdef->eval_ctx_->exec_ctx_;
  param.output_ = &scan_ctdef->result_output_;
}

void ObDASIterUtils::init_spatial_scan_iter_param(ObDASSpatialScanIterParam &param, const ObDASScanCtDef *scan_ctdef, ObDASScanRtDef *scan_rtdef)
{
  param.scan_ctdef_ = scan_ctdef;
  param.max_size_ = scan_rtdef->eval_ctx_->is_vectorized() ? scan_rtdef->eval_ctx_->max_batch_size_ : 1;
  param.eval_ctx_ = scan_rtdef->eval_ctx_;
  param.exec_ctx_ = &scan_rtdef->eval_ctx_->exec_ctx_;
  param.output_ = &scan_ctdef->result_output_;
  param.scan_rtdef_ = scan_rtdef;
}

int ObDASIterUtils::create_das_spatial_scan_iter(ObIAllocator &alloc, ObDASSpatialScanIterParam &param, ObDASSpatialScanIter *&result)
{
  int ret = OB_SUCCESS;
  ObDASSpatialScanIter *iter = nullptr;

  void *buf = alloc.alloc(sizeof(ObDASSpatialScanIter));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObDASSpatialScanIter buf");
  } else {
    iter= new(buf) ObDASSpatialScanIter(alloc);
    if (OB_FAIL(iter->init(param))) {
      LOG_WARN("failed to init ObDASSpatialScanIter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    result = iter;
  } else {
    if (OB_NOT_NULL(iter)) {
      iter->release();
      alloc.free(iter);
      iter = nullptr;
    }
  }
  return ret;
}

int ObDASIterUtils::create_das_scan_iter_tree(ObDASIterTreeType tree_type,
                                              storage::ObTableScanParam &scan_param,
                                              const ObDASScanCtDef *scan_ctdef,
                                              ObDASScanRtDef *scan_rtdef,
                                              const ObDASScanCtDef *lookup_ctdef,
                                              ObDASScanRtDef *lookup_rtdef,
                                              const ObDASBaseCtDef *attach_ctdef,
                                              ObDASBaseRtDef *attach_rtdef,
                                              const ObDASRelatedTabletID &related_tablet_ids,
                                              transaction::ObTxDesc *trans_desc,
                                              transaction::ObTxReadSnapshot *snapshot,
                                              common::ObIAllocator &alloc,
                                              ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  switch (tree_type) {
    case ITER_TREE_PARTITION_SCAN: {
      ret = create_partition_scan_tree(scan_param, alloc, scan_ctdef, scan_rtdef, attach_ctdef, attach_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
      break;
    }
    case ITER_TREE_LOCAL_LOOKUP: {
      ret = create_local_lookup_tree(scan_param, alloc, scan_ctdef, scan_rtdef, lookup_ctdef, lookup_rtdef, attach_ctdef, attach_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
      break;
    }
    case ITER_TREE_DOMAIN_LOOKUP: {
      // ret = create_domain_lookup_tree(scan_param, alloc, attach_ctdef, attach_rtdef, iter_tree);
      break;
    }
    case ITER_TREE_TEXT_RETRIEVAL: {
      ret = create_text_retrieval_tree(scan_param, alloc, attach_ctdef, attach_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
      break;
    }
    case ITER_TREE_INDEX_MERGE: {
      ret = create_index_merge_iter_tree(scan_param, alloc, attach_ctdef, attach_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
      break;
    }
    case ITER_TREE_FUNC_LOOKUP: {
      ret = create_function_lookup_tree(scan_param, alloc, attach_ctdef, attach_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
      break;
    }
    case ITER_TREE_MVI_LOOKUP: {
      ret = create_mvi_lookup_tree(scan_param, alloc, attach_ctdef, attach_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
      break;
    }
    case ITER_TREE_GIS_LOOKUP: {
      ret = create_gis_lookup_tree(scan_param, alloc, attach_ctdef, attach_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to create das scan iter tree", K(ret));
  }

  LOG_TRACE("create das scan iter tree", K(tree_type), K(ret));
  return ret;
}

int ObDASIterUtils::create_tsc_iter_tree(ObDASIterTreeType tree_type,
                                         const ObTableScanCtDef &tsc_ctdef,
                                         ObTableScanRtDef &tsc_rtdef,
                                         ObEvalCtx &eval_ctx,
                                         ObExecContext &exec_ctx,
                                         ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                         const ObTableScanSpec &spec,
                                         bool can_retry,
                                         ObDASMergeIter *&scan_iter,
                                         ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  switch (tree_type) {
    case ITER_TREE_TABLE_SCAN: {
      ret = create_table_scan_iter_tree(tsc_ctdef, eval_ctx, exec_ctx, eval_infos, spec, scan_iter, iter_tree);
      break;
    }
    case ITER_TREE_GLOBAL_LOOKUP: {
      ret = create_global_lookup_iter_tree(tsc_ctdef, tsc_rtdef, eval_ctx, exec_ctx, eval_infos, spec, can_retry, scan_iter, iter_tree);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to create table scan iter tree", K(ret));
  }

  LOG_DEBUG("create table scan iter tree", K(tree_type), K(ret));
  return ret;
}

int ObDASIterUtils::create_group_fold_iter(const ObTableScanCtDef &tsc_ctdef,
                                           ObTableScanRtDef &tsc_rtdef,
                                           ObEvalCtx &eval_ctx,
                                           ObExecContext &exec_ctx,
                                           ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                           const ObTableScanSpec &spec,
                                           ObDASIter *iter_tree,
                                           ObDASGroupFoldIter *&fold_iter)
{
  int ret = OB_SUCCESS;
  ObDASGroupFoldIter *iter = nullptr;
  const ObDASScanCtDef *scan_ctdef = &tsc_ctdef.scan_ctdef_;
  ObDASScanRtDef *scan_rtdef = &tsc_rtdef.scan_rtdef_;
  ObDASGroupFoldIterParam param;
  param.max_size_ = eval_ctx.is_vectorized() ? eval_ctx.max_batch_size_ : 1;
  param.eval_ctx_ = &eval_ctx;
  param.exec_ctx_ = &exec_ctx;
  param.output_ = &tsc_ctdef.get_das_output_exprs();
  param.group_id_expr_ = tsc_ctdef.scan_ctdef_.group_id_expr_;
  param.need_check_output_datum_ = scan_rtdef->need_check_output_datum_;
  param.iter_tree_ = iter_tree;
  if (OB_FAIL(create_das_iter(exec_ctx.get_allocator(), param, iter))) {
    LOG_WARN("failed to create das group fold iter", K(ret));
  } else if (OB_FAIL(create_iter_children_array(1, exec_ctx.get_allocator(), iter))) {
    LOG_WARN("failed to create iter children array", K(ret));
  } else {
    iter->get_children()[0] = iter_tree;
  }

  if (OB_SUCC(ret)) {
    fold_iter = iter;
  }
  return ret;
}

int ObDASIterUtils::set_text_retrieval_related_ids(const ObDASBaseCtDef *attach_ctdef,
                                                  const ObDASRelatedTabletID &related_tablet_ids,
                                                  const ObLSID &ls_id,
                                                  ObDASIter *root_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(root_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(attach_ctdef), KP(root_iter));
  } else {
    bool need_set_child = false;
    const ObDASIterType &iter_type = root_iter->get_type();
    switch (attach_ctdef->op_type_) {
    case ObDASOpType::DAS_OP_TABLE_LOOKUP: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_LOCAL_LOOKUP)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
      } else {
        ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter *>(root_iter);
        lookup_iter->set_ls_id(ls_id);
        lookup_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
        need_set_child = true;
      }
      break;
    }
    case ObDASOpType::DAS_OP_IR_AUX_LOOKUP: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_LOCAL_LOOKUP)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
      } else {
        ObDASLocalLookupIter *aux_lookup_iter = static_cast<ObDASLocalLookupIter *>(root_iter);
        aux_lookup_iter->set_ls_id(ls_id);
        aux_lookup_iter->set_tablet_id(related_tablet_ids.aux_lookup_tablet_id_);
        need_set_child = true;
      }
      break;
    }
    case ObDASOpType::DAS_OP_SORT: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_SORT)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
      } else {
        need_set_child = true;
      }
      break;
    }
    case ObDASOpType::DAS_OP_IR_SCAN: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_TEXT_RETRIEVAL_MERGE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
      } else {
        ObDASTextRetrievalMergeIter *tr_merge_iter = static_cast<ObDASTextRetrievalMergeIter *>(root_iter);
        need_set_child = false;
        ObDASFTSTabletID fts_tablet_ids;
        fts_tablet_ids.inv_idx_tablet_id_ = related_tablet_ids.inv_idx_tablet_id_;
        fts_tablet_ids.fwd_idx_tablet_id_ = related_tablet_ids.fwd_idx_tablet_id_;
        fts_tablet_ids.doc_id_idx_tablet_id_ = related_tablet_ids.doc_id_idx_tablet_id_;
        if (OB_FAIL(tr_merge_iter->set_related_tablet_ids(ls_id, fts_tablet_ids))) {
          LOG_WARN("failed to set related tablet ids", K(ret));
        }
      }
      break;
    }
    default: {
      need_set_child = false;
      break;
    }
    }

    if (OB_FAIL(ret) || !need_set_child) {
    } else if (OB_UNLIKELY(attach_ctdef->children_cnt_ != root_iter->get_children_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter children count not equal to ctdef children count",
          K(ret), K(attach_ctdef->children_cnt_), K(root_iter->get_children_cnt()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < attach_ctdef->children_cnt_; ++i) {
        if (OB_FAIL(set_text_retrieval_related_ids(
            attach_ctdef->children_[i],
            related_tablet_ids,
            ls_id,
            root_iter->get_children()[i]))) {
          LOG_WARN("failed to set text retrieval related ids", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASIterUtils::set_index_merge_related_ids(const ObDASBaseCtDef *attach_ctdef,
                                                const ObDASRelatedTabletID &related_tablet_ids,
                                                const ObLSID &ls_id,
                                                ObDASIter *root_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(root_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(attach_ctdef), KP(root_iter));
  } else {
    bool need_set_child = false;
    const ObDASIterType &iter_type = root_iter->get_type();
    switch (attach_ctdef->op_type_) {
      case ObDASOpType::DAS_OP_TABLE_LOOKUP: {
        if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_LOCAL_LOOKUP)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
        } else {
          ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter *>(root_iter);
          lookup_iter->set_ls_id(ls_id);
          lookup_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
          need_set_child = true;
        }
        break;
      }
      case ObDASOpType::DAS_OP_INDEX_MERGE: {
        if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_INDEX_MERGE)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
        } else {
          ObDASIndexMergeIter *merge_iter = static_cast<ObDASIndexMergeIter *>(root_iter);
          if (OB_FAIL(merge_iter->set_ls_tablet_ids(ls_id, related_tablet_ids))) {
            LOG_WARN("failed to set related tablet ids", K(ret));
          }
          need_set_child = true;
        }
        break;
      }
      default: {
        need_set_child = false;
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!need_set_child) {
    } else if (OB_UNLIKELY(attach_ctdef->children_cnt_ != root_iter->get_children_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter children count not equal to ctdef children count",
          K(attach_ctdef->children_cnt_), K(root_iter->get_children_cnt()), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < attach_ctdef->children_cnt_; ++i) {
        if (OB_FAIL(set_index_merge_related_ids(attach_ctdef->children_[i],
                                                related_tablet_ids,
                                                ls_id,
                                                root_iter->get_children()[i]))) {
          LOG_WARN("failed to set index merge related ids", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASIterUtils::set_func_lookup_iter_related_ids(const ObDASBaseCtDef *attach_ctdef,
                                                     const ObDASRelatedTabletID &related_tablet_ids,
                                                     const ObLSID &ls_id,
                                                     int64_t flag,
                                                     ObDASIter *root_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(root_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(attach_ctdef), KP(root_iter));
  } else {
    const ObDASIterType &iter_type = root_iter->get_type();
    bool need_set_child = false;
    switch (attach_ctdef->op_type_) {
    case ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_LOCAL_LOOKUP)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match", K(ret), K(iter_type));
      } else {
        ObDASLocalLookupIter *local_lookup_iter = static_cast<ObDASLocalLookupIter *>(root_iter);
        local_lookup_iter->set_tablet_id(related_tablet_ids.rowkey_doc_tablet_id_);
        local_lookup_iter->set_ls_id(ls_id);
        need_set_child = true;
      }
      break;
    }
    case ObDASOpType::DAS_OP_FUNC_LOOKUP: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_FUNC_LOOKUP)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
      } else {
        const ObDASFuncLookupCtDef *func_lookup_ctdef = static_cast<const ObDASFuncLookupCtDef *>(attach_ctdef);
        const int64_t func_lookup_cnt = func_lookup_ctdef->func_lookup_cnt_;
        ObDASFuncLookupIter *func_lookup_iter = static_cast<ObDASFuncLookupIter *>(root_iter);
        ObDASFuncDataIter *merge_iter = static_cast<ObDASFuncDataIter *>(root_iter->get_children()[1]);
        if (func_lookup_ctdef->has_main_table_lookup()) {
          merge_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
          merge_iter->set_ls_id(ls_id);
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < func_lookup_cnt; ++i) {
          if (OB_FAIL(set_func_lookup_iter_related_ids(
              func_lookup_ctdef->get_func_lookup_scan_ctdef(i),
              related_tablet_ids,
              ls_id,
              i,
              merge_iter->get_children()[i]))) {
            LOG_WARN("failed to set text retrieval related ids", K(ret));
          }
        }
        need_set_child = false;
      }
      break;
    }
    case ObDASOpType::DAS_OP_IR_AUX_LOOKUP: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_LOCAL_LOOKUP)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
      } else {
        ObDASLocalLookupIter *aux_lookup_iter = static_cast<ObDASLocalLookupIter *>(root_iter);
        aux_lookup_iter->set_ls_id(ls_id);
        aux_lookup_iter->set_tablet_id(related_tablet_ids.aux_lookup_tablet_id_);
        need_set_child = true;
      }
      break;
    }
    case ObDASOpType::DAS_OP_SORT: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_SORT)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
      } else {
        need_set_child = true;
      }
      break;
    }
    case ObDASOpType::DAS_OP_IR_SCAN: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_TEXT_RETRIEVAL_MERGE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
      } else {
        ObDASTextRetrievalMergeIter *tr_merge_iter = static_cast<ObDASTextRetrievalMergeIter *>(root_iter);
        ObDASFTSTabletID fts_tablet_ids;
        if (flag >= 0) {
          fts_tablet_ids.inv_idx_tablet_id_ = related_tablet_ids.fts_tablet_ids_[flag].inv_idx_tablet_id_;
          fts_tablet_ids.fwd_idx_tablet_id_ = related_tablet_ids.fts_tablet_ids_[flag].fwd_idx_tablet_id_;
          fts_tablet_ids.doc_id_idx_tablet_id_ = related_tablet_ids.fts_tablet_ids_[flag].doc_id_idx_tablet_id_;
        } else {
          fts_tablet_ids.inv_idx_tablet_id_ = related_tablet_ids.inv_idx_tablet_id_;
          fts_tablet_ids.fwd_idx_tablet_id_ = related_tablet_ids.fwd_idx_tablet_id_;
          fts_tablet_ids.doc_id_idx_tablet_id_ = related_tablet_ids.doc_id_idx_tablet_id_;
        }
        if (OB_FAIL(tr_merge_iter->set_related_tablet_ids(ls_id, fts_tablet_ids))) {
          LOG_WARN("failed to set related tablet ids", K(ret));
        }
        need_set_child = false;
      }
      break;
    }
    case ObDASOpType::DAS_OP_TABLE_SCAN: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_SCAN)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
      } else {
        need_set_child = false;
      }
      break;
    }
    default: {
      need_set_child = false;
      break;
    }
    }

    if (OB_FAIL(ret) || !need_set_child) {
    } else if (OB_UNLIKELY(attach_ctdef->children_cnt_ != root_iter->get_children_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter children count not equal to ctdef children count",
          K(ret), K(attach_ctdef->children_cnt_), K(root_iter->get_children_cnt()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < attach_ctdef->children_cnt_; ++i) {
        if (OB_FAIL(set_func_lookup_iter_related_ids(
            attach_ctdef->children_[i],
            related_tablet_ids,
            ls_id,
            -1,
            root_iter->get_children()[i]))) {
          LOG_WARN("failed to set text retrieval related ids", K(ret));
        }
      }
    }
  }
  return ret;
}

/***************** PUBLIC END *****************/

int ObDASIterUtils::create_partition_scan_tree(storage::ObTableScanParam &scan_param,
                                               common::ObIAllocator &alloc,
                                               const ObDASScanCtDef *scan_ctdef,
                                               ObDASScanRtDef *scan_rtdef,
                                               const ObDASBaseCtDef *attach_ctdef,
                                               ObDASBaseRtDef *attach_rtdef,
                                               const ObDASRelatedTabletID &related_tablet_ids,
                                               transaction::ObTxDesc *trans_desc,
                                               transaction::ObTxReadSnapshot *snapshot,
                                               ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  ObDASScanIterParam param;
  init_scan_iter_param(param, scan_ctdef, scan_rtdef);
  ObDASScanIter *scan_iter = nullptr;
  if (OB_FAIL(create_das_iter(alloc, param, scan_iter))) {
    LOG_WARN("failed to create das scan iter", K(ret));
  } else {
    scan_iter->set_scan_param(scan_param);
    iter_tree = scan_iter;
    if (OB_NOT_NULL(attach_ctdef)) {
      if (OB_UNLIKELY(ObDASOpType::DAS_OP_DOC_ID_MERGE != attach_ctdef->op_type_
                   && ObDASOpType::DAS_OP_VID_MERGE != attach_ctdef->op_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, attach op type isn't doc id merge", K(ret), K(attach_ctdef->op_type_), KPC(attach_ctdef));
      } else if (ObDASOpType::DAS_OP_DOC_ID_MERGE == attach_ctdef->op_type_) {
        if (OB_FAIL(create_doc_id_scan_sub_tree(scan_param, alloc, static_cast<const ObDASDocIdMergeCtDef *>(attach_ctdef),
              static_cast<ObDASDocIdMergeRtDef *>(attach_rtdef), related_tablet_ids, trans_desc, snapshot, iter_tree))) {
          LOG_WARN("fail to create doc id scan sub tree", K(ret), K(scan_param), KPC(attach_ctdef), KPC(attach_rtdef));
        }
      } else if (DAS_OP_VID_MERGE == attach_ctdef->op_type_) {
        if (OB_FAIL(create_vid_scan_sub_tree(scan_param, alloc, static_cast<const ObDASVIdMergeCtDef *>(attach_ctdef),
              static_cast<ObDASVIdMergeRtDef *>(attach_rtdef), related_tablet_ids, trans_desc, snapshot, iter_tree))) {
          LOG_WARN("fail to create vec vid scan sub tree", K(ret), K(scan_param), KPC(attach_ctdef), KPC(attach_rtdef));
        }
      }
    }
  }
  return ret;
}

int ObDASIterUtils::create_local_lookup_tree(ObTableScanParam &scan_param,
                                             common::ObIAllocator &alloc,
                                             const ObDASScanCtDef *scan_ctdef,
                                             ObDASScanRtDef *scan_rtdef,
                                             const ObDASScanCtDef *lookup_ctdef,
                                             ObDASScanRtDef *lookup_rtdef,
                                             const ObDASBaseCtDef *attach_ctdef,
                                             ObDASBaseRtDef *attach_rtdef,
                                             const ObDASRelatedTabletID &related_tablet_ids,
                                             transaction::ObTxDesc *trans_desc,
                                             transaction::ObTxReadSnapshot *snapshot,
                                             ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  ObDASScanIter *index_table_iter = nullptr;
  ObDASScanIter *data_table_iter = nullptr;
  ObDASLocalLookupIter *lookup_iter = nullptr;
  ObDASScanIterParam index_table_param;
  init_scan_iter_param(index_table_param, scan_ctdef, scan_rtdef);
  ObDASScanIterParam data_table_param;
  init_scan_iter_param(data_table_param, lookup_ctdef, lookup_rtdef);
  if (OB_FAIL(create_das_iter(alloc, index_table_param, index_table_iter))) {
    LOG_WARN("failed to create index table iter", K(ret));
  } else if (OB_FAIL(create_das_iter(alloc, data_table_param, data_table_iter))) {
    LOG_WARN("failed to create data table iter", K(ret));
  } else {
    iter_tree = static_cast<ObDASIter *>(data_table_iter);
    if (OB_ISNULL(attach_ctdef)) {
      // nothing to do
    } else if (OB_ISNULL(attach_rtdef)
            || OB_UNLIKELY(ObDASOpType::DAS_OP_TABLE_LOOKUP != attach_ctdef->op_type_ || attach_ctdef->children_cnt_ < 2)
            || OB_UNLIKELY(ObDASOpType::DAS_OP_TABLE_LOOKUP != attach_rtdef->op_type_ || attach_rtdef->children_cnt_ < 2)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments, attach op type isn't table lookup", K(ret), KPC(attach_ctdef), KPC(attach_rtdef));
    } else {
      ObDASBaseCtDef *ctdef = attach_ctdef->children_[1];
      ObDASBaseRtDef *rtdef = attach_rtdef->children_[1];
      if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpeted error, ctdef or rtdef is nullptr", K(ret), KPC(ctdef), KPC(rtdef));
      } else if (ObDASOpType::DAS_OP_TABLE_SCAN == ctdef->op_type_) {
        // nothing to do
      } else if (OB_UNLIKELY(ObDASOpType::DAS_OP_VID_MERGE == ctdef->op_type_)) {
        if (OB_FAIL(create_vid_scan_sub_tree(scan_param, alloc, static_cast<const ObDASVIdMergeCtDef *>(ctdef),
              static_cast<ObDASVIdMergeRtDef *>(rtdef), related_tablet_ids, trans_desc, snapshot, iter_tree))) {
          LOG_WARN("fail to create vec vid scan sub tree", K(ret), K(scan_param), KPC(ctdef), KPC(rtdef));
        }
      } else if (OB_UNLIKELY(ObDASOpType::DAS_OP_DOC_ID_MERGE != ctdef->op_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, attach op type isn't doc id merge", K(ret), K(ctdef->op_type_), KPC(ctdef));
      } else if (OB_FAIL(create_doc_id_scan_sub_tree(scan_param, alloc, static_cast<const ObDASDocIdMergeCtDef *>(ctdef),
              static_cast<ObDASDocIdMergeRtDef *>(rtdef), related_tablet_ids, trans_desc, snapshot, iter_tree))) {
        LOG_WARN("fail to create doc id scan sub tree", K(ret), K(scan_param), KPC(ctdef), KPC(rtdef));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObDASLocalLookupIterParam lookup_param;
    lookup_param.max_size_ = lookup_rtdef->eval_ctx_->is_vectorized() ? lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
    lookup_param.eval_ctx_ = lookup_rtdef->eval_ctx_;
    lookup_param.exec_ctx_ = &lookup_rtdef->eval_ctx_->exec_ctx_;
    lookup_param.output_ = &lookup_ctdef->result_output_;
    lookup_param.index_ctdef_ = scan_ctdef;
    lookup_param.index_rtdef_ = scan_rtdef;
    lookup_param.lookup_ctdef_ = lookup_ctdef;
    lookup_param.lookup_rtdef_ = lookup_rtdef;
    lookup_param.index_table_iter_ = index_table_iter;
    lookup_param.data_table_iter_ = iter_tree;
    lookup_param.trans_desc_ = trans_desc;
    lookup_param.snapshot_ = snapshot;
    lookup_param.rowkey_exprs_ = &lookup_ctdef->rowkey_exprs_;
    if (OB_FAIL(create_das_iter(alloc, lookup_param, lookup_iter))) {
      LOG_WARN("failed to create local lookup iter", K(ret));
    } else if (OB_FAIL(create_iter_children_array(2, alloc, lookup_iter))) {
      LOG_WARN("failed to create iter children array", K(ret));
    } else {
      lookup_iter->get_children()[0] = index_table_iter;
      lookup_iter->get_children()[1] = iter_tree;
      index_table_iter->set_scan_param(scan_param);
      data_table_iter->set_scan_param(lookup_iter->get_lookup_param());
      lookup_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
      lookup_iter->set_ls_id(scan_param.ls_id_);
      iter_tree = lookup_iter;
    }
  }
  return ret;
}

int ObDASIterUtils::create_text_retrieval_tree(ObTableScanParam &scan_param,
                                               common::ObIAllocator &alloc,
                                               const ObDASBaseCtDef *attach_ctdef,
                                               ObDASBaseRtDef *attach_rtdef,
                                               const ObDASRelatedTabletID &related_tablet_ids,
                                               transaction::ObTxDesc *trans_desc,
                                               transaction::ObTxReadSnapshot *snapshot,
                                               ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  const ObDASIRScanCtDef *ir_scan_ctdef = nullptr;
  ObDASIRScanRtDef *ir_scan_rtdef = nullptr;
  const ObDASTableLookupCtDef *table_lookup_ctdef = nullptr;
  ObDASTableLookupRtDef *table_lookup_rtdef = nullptr;
  const ObDASSortCtDef *sort_ctdef = nullptr;
  ObDASSortRtDef *sort_rtdef = nullptr;
  ObDASIter *text_retrieval_result = nullptr;
  ObDASIter *sort_result = nullptr;
  ObDASIter *root_iter = nullptr;
  const bool has_lookup = ObDASOpType::DAS_OP_TABLE_LOOKUP == attach_ctdef->op_type_;
  int64_t token_cnt = 0;
  bool taat_mode = false;
  ObDASFTSTabletID fts_tablet_ids;
  fts_tablet_ids.inv_idx_tablet_id_ = related_tablet_ids.inv_idx_tablet_id_;
  fts_tablet_ids.fwd_idx_tablet_id_ = related_tablet_ids.fwd_idx_tablet_id_;
  fts_tablet_ids.doc_id_idx_tablet_id_ = related_tablet_ids.doc_id_idx_tablet_id_;
  if (OB_UNLIKELY(attach_ctdef->op_type_ != ObDASOpType::DAS_OP_IR_SCAN
      && attach_ctdef->op_type_ != ObDASOpType::DAS_OP_TABLE_LOOKUP
      && attach_ctdef->op_type_ != ObDASOpType::DAS_OP_SORT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected text retrieval root attach def type", K(ret), KPC(attach_ctdef));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(
      attach_ctdef,
      attach_rtdef,
      ObDASOpType::DAS_OP_IR_SCAN,
      ir_scan_ctdef,
      ir_scan_rtdef))) {
    LOG_WARN("fail to find ir scan definition", K(ret));
  } else if (OB_FAIL(create_text_retrieval_sub_tree(
      scan_param.ls_id_,
      alloc,
      ir_scan_ctdef,
      ir_scan_rtdef,
      fts_tablet_ids,
      trans_desc,
      snapshot,
      text_retrieval_result))) {
    LOG_WARN("failed to create text retrieval sub tree", K(ret));
  } else {
    root_iter = text_retrieval_result;
    if (has_lookup) {
      table_lookup_ctdef = static_cast<const ObDASTableLookupCtDef *>(attach_ctdef);
      table_lookup_rtdef = static_cast<ObDASTableLookupRtDef *>(attach_rtdef);
      if (table_lookup_ctdef->get_rowkey_scan_ctdef()->op_type_ == ObDASOpType::DAS_OP_IR_AUX_LOOKUP) {
        const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = static_cast<const ObDASIRAuxLookupCtDef *>(
            table_lookup_ctdef->get_rowkey_scan_ctdef());
        ObDASIRAuxLookupRtDef *aux_lookup_rtdef = static_cast<ObDASIRAuxLookupRtDef *>(
            table_lookup_rtdef->get_rowkey_scan_rtdef());
        if (aux_lookup_ctdef->get_doc_id_scan_ctdef()->op_type_ == ObDASOpType::DAS_OP_SORT) {
          sort_ctdef = static_cast<const ObDASSortCtDef *>(aux_lookup_ctdef->get_doc_id_scan_ctdef());
          sort_rtdef = static_cast<ObDASSortRtDef *>(aux_lookup_rtdef->get_doc_id_scan_rtdef());
        }
      }
    } else {
      if (attach_ctdef->op_type_ == ObDASOpType::DAS_OP_SORT) {
        sort_ctdef = static_cast<const ObDASSortCtDef *>(attach_ctdef);
        sort_rtdef = static_cast<ObDASSortRtDef *>(attach_rtdef);
      }
    }
    const bool has_sort = nullptr != sort_ctdef;
    const bool need_rewind = true;
    if (!has_sort) {
      // skip
    } else if (OB_FAIL(create_sort_sub_tree(alloc, sort_ctdef, sort_rtdef, need_rewind, false/*need_distinct*/,text_retrieval_result, sort_result))) {
      LOG_WARN("failed to create sort sub tree", K(ret));
    } else {
      root_iter = sort_result;
    }
  }

  if (OB_SUCC(ret) && has_lookup) {
    ObDASIter *domain_lookup_result = nullptr;
    bool doc_id_lookup_keep_order = false;
    bool main_lookup_keep_order = false;
    const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
    ObDASTextRetrievalMergeIter *tr_merge_iter = static_cast<ObDASTextRetrievalMergeIter *>(text_retrieval_result);
    token_cnt = tr_merge_iter->get_query_tokens().count();
    taat_mode = tr_merge_iter->is_taat_mode();
    if (OB_ISNULL(table_lookup_ctdef) || OB_UNLIKELY(table_lookup_ctdef->get_rowkey_scan_ctdef()->op_type_ != ObDASOpType::DAS_OP_IR_AUX_LOOKUP)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey scan is not an aux lookup", K(ret));
    } else {
      aux_lookup_ctdef = static_cast<const ObDASIRAuxLookupCtDef *>(table_lookup_ctdef->get_rowkey_scan_ctdef());
      // below cases need keep order when look up the table
      // case: Taat or need sort by relevance
      if (nullptr != ir_scan_ctdef && ir_scan_ctdef->need_calc_relevance()) {
        if (taat_mode || DAS_OP_SORT == aux_lookup_ctdef->get_doc_id_scan_ctdef()->op_type_) {
          doc_id_lookup_keep_order = true;
        }
        main_lookup_keep_order = true;
      }
    }

    if (FAILEDx(create_domain_lookup_sub_tree(
        scan_param,
        scan_param.ls_id_,
        alloc,
        table_lookup_ctdef,
        table_lookup_rtdef,
        related_tablet_ids,
        doc_id_lookup_keep_order,
        main_lookup_keep_order,
        trans_desc,
        snapshot,
        root_iter,
        domain_lookup_result))) {
      LOG_WARN("failed to create domain index lookup iters", K(ret));
    } else {
      root_iter = domain_lookup_result;
    }
  }

  if (OB_SUCC(ret)) {
    iter_tree = root_iter;
  }
  return ret;
}

int ObDASIterUtils::create_functional_text_retrieval_sub_tree(const ObLSID &ls_id,
                                                              common::ObIAllocator &alloc,
                                                              const ObDASIRScanCtDef *ir_scan_ctdef,
                                                              ObDASIRScanRtDef *ir_scan_rtdef,
                                                              const ObDASFTSTabletID &related_tablet_ids,
                                                              transaction::ObTxDesc *trans_desc,
                                                              transaction::ObTxReadSnapshot *snapshot,
                                                              ObDASIter *&retrieval_result)
          {
  int ret = OB_SUCCESS;
  ObDASTextRetrievalMergeIterParam merge_iter_param;
  ObDASTextRetrievalMergeIter *tr_merge_iter = nullptr;
  ObDASScanIterParam doc_cnt_agg_param;
  ObDASScanIter *doc_cnt_agg_iter = nullptr;
  bool taat_mode = false;
  bool need_inv_idx_agg_reset = false;

  merge_iter_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
  merge_iter_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
  merge_iter_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
  merge_iter_param.output_ = &ir_scan_ctdef->result_output_;
  merge_iter_param.ir_ctdef_ = ir_scan_ctdef;
  merge_iter_param.ir_rtdef_ = ir_scan_rtdef;
  merge_iter_param.tx_desc_ = trans_desc;
  merge_iter_param.snapshot_ = snapshot;
  merge_iter_param.force_return_docid_ = true;

  if (0 != merge_iter_param.query_tokens_.count()) {
    merge_iter_param.query_tokens_.reuse();
  }

  if (OB_FAIL(ObDASTextRetrievalMergeIter::build_query_tokens(ir_scan_ctdef, ir_scan_rtdef, alloc, merge_iter_param.query_tokens_))) {
    LOG_WARN("failed to get query tokens for text retrieval", K(ret));
  } else if (!ir_scan_ctdef->need_proj_relevance_score()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("functional lookup without relevance score not supported", K(ret));
  } else if (merge_iter_param.query_tokens_.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT) {
    need_inv_idx_agg_reset = true;
    if (!ir_scan_ctdef->need_estimate_total_doc_cnt()) {
      doc_cnt_agg_param.scan_ctdef_ = ir_scan_ctdef->get_doc_id_idx_agg_ctdef();
      doc_cnt_agg_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
      doc_cnt_agg_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
      doc_cnt_agg_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
      doc_cnt_agg_param.output_ = &ir_scan_ctdef->get_doc_id_idx_agg_ctdef()->result_output_;
      if (OB_FAIL(create_das_iter(alloc, doc_cnt_agg_param, doc_cnt_agg_iter))) {
        LOG_WARN("failed to create doc cnt agg scan iter", K(ret));
      } else {
        merge_iter_param.doc_cnt_iter_ = doc_cnt_agg_iter;
      }
    }
    ObDASTRTaatLookupIter *fts_merge_iter = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_das_iter(alloc, merge_iter_param, fts_merge_iter))) {
      LOG_WARN("failed to create text retrieval merge iter", K(ret));
    } else {
      tr_merge_iter = fts_merge_iter;
      taat_mode = true;
    }
  } else {
    if (ir_scan_ctdef->need_calc_relevance() && !ir_scan_ctdef->need_estimate_total_doc_cnt()) {
      doc_cnt_agg_param.scan_ctdef_ = ir_scan_ctdef->get_doc_id_idx_agg_ctdef();
      doc_cnt_agg_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
      doc_cnt_agg_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
      doc_cnt_agg_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
      doc_cnt_agg_param.output_ = &ir_scan_ctdef->get_doc_id_idx_agg_ctdef()->result_output_;
      if (OB_FAIL(create_das_iter(alloc, doc_cnt_agg_param, doc_cnt_agg_iter))) {
        LOG_WARN("failed to create doc cnt agg scan iter", K(ret));
      } else {
        merge_iter_param.doc_cnt_iter_ = doc_cnt_agg_iter;
      }
    }
    ObDASTRDaatLookupIter *fts_merge_iter = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_das_iter(alloc, merge_iter_param, fts_merge_iter))) {
      LOG_WARN("failed to create text retrieval merge iter", K(ret));
    } else {
      tr_merge_iter = fts_merge_iter;
      taat_mode = false;
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    ObSEArray<ObDASIter *, 16> iters;
    const ObIArray<ObString> &query_tokens = tr_merge_iter->get_query_tokens();
    int64_t size = taat_mode && query_tokens.count() != 0 ? 1 : query_tokens.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      ObDASTextRetrievalIterParam retrieval_param;
      ObDASTextRetrievalIter *retrieval_iter = nullptr;
      retrieval_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
      retrieval_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
      retrieval_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
      retrieval_param.output_ = &ir_scan_ctdef->result_output_;
      retrieval_param.ir_ctdef_ = ir_scan_ctdef;
      retrieval_param.ir_rtdef_ = ir_scan_rtdef;
      retrieval_param.tx_desc_ = trans_desc;
      retrieval_param.snapshot_ = snapshot;
      retrieval_param.need_inv_idx_agg_reset_ = need_inv_idx_agg_reset;

      ObDASScanIterParam inv_idx_scan_iter_param;
      ObDASScanIter *inv_idx_scan_iter = nullptr;
      inv_idx_scan_iter_param.scan_ctdef_ = ir_scan_ctdef->get_inv_idx_scan_ctdef();
      inv_idx_scan_iter_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
      inv_idx_scan_iter_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
      inv_idx_scan_iter_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
      inv_idx_scan_iter_param.output_ = &ir_scan_ctdef->get_inv_idx_scan_ctdef()->result_output_;
      ObDASScanIterParam inv_idx_agg_iter_param;
      ObDASScanIter *inv_idx_agg_iter = nullptr;
      if (ir_scan_ctdef->need_inv_idx_agg()) {
        inv_idx_agg_iter_param.scan_ctdef_ = ir_scan_ctdef->get_inv_idx_agg_ctdef();
        inv_idx_agg_iter_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
        inv_idx_agg_iter_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
        inv_idx_agg_iter_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
        inv_idx_agg_iter_param.output_ = &ir_scan_ctdef->get_inv_idx_agg_ctdef()->result_output_;
      }
      ObDASScanIterParam fwd_idx_iter_param;
      ObDASScanIter *fwd_idx_iter = nullptr;
      if (ir_scan_ctdef->need_fwd_idx_agg()) {
        fwd_idx_iter_param.scan_ctdef_ = ir_scan_ctdef->get_fwd_idx_agg_ctdef();
        fwd_idx_iter_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
        fwd_idx_iter_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
        fwd_idx_iter_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
        fwd_idx_iter_param.output_ = &ir_scan_ctdef->get_fwd_idx_agg_ctdef()->result_output_;
      }
      if (OB_FAIL(create_das_iter(alloc, inv_idx_scan_iter_param, inv_idx_scan_iter))) {
        LOG_WARN("failed to create inv idx iter", K(ret));
      } else if (ir_scan_ctdef->need_inv_idx_agg()
          && OB_FAIL(create_das_iter(alloc, inv_idx_agg_iter_param, inv_idx_agg_iter))) {
        LOG_WARN("failed to create inv idx agg iter", K(ret));
      } else if (ir_scan_ctdef->need_fwd_idx_agg()
          && OB_FAIL(create_das_iter(alloc, fwd_idx_iter_param, fwd_idx_iter))) {
        LOG_WARN("failed to create fwd idx iter", K(ret));
      } else {
        retrieval_param.inv_idx_scan_iter_ = inv_idx_scan_iter;
        retrieval_param.inv_idx_agg_iter_ = inv_idx_agg_iter;
        retrieval_param.fwd_idx_iter_ = fwd_idx_iter;
        const int64_t inv_idx_iter_cnt = ir_scan_ctdef->need_inv_idx_agg() ? 2 : 1;
        const int64_t fwd_idx_iter_cnt = ir_scan_ctdef->need_fwd_idx_agg() ? 1 : 0;
        const int64_t tr_children_cnt = inv_idx_iter_cnt + fwd_idx_iter_cnt;
        if (taat_mode) {
          if (OB_FAIL(create_das_iter(alloc, retrieval_param, retrieval_iter))) {
            LOG_WARN("failed to create text retrieval iter", K(ret));
          }
        } else {
          ObDASTRCacheIter *tr_iter = nullptr;
          if (OB_FAIL(create_das_iter(alloc, retrieval_param, tr_iter))) {
            LOG_WARN("failed to create text retrieval iter", K(ret));
          } else {
            retrieval_iter = tr_iter;
          }
        }
        if (OB_FAIL(ret)) {
        // set query_token and range in do_table_scan
        } else if (OB_FAIL(create_iter_children_array(tr_children_cnt, alloc, retrieval_iter))) {
          LOG_WARN("failed to create iter children array", K(ret));
        } else {
          retrieval_iter->get_children()[0] = inv_idx_scan_iter;
          if (ir_scan_ctdef->need_inv_idx_agg()) {
            retrieval_iter->get_children()[1] = inv_idx_agg_iter;
          }
          if (ir_scan_ctdef->need_fwd_idx_agg()) {
            retrieval_iter->get_children()[2] = fwd_idx_iter;
          }
          retrieval_iter->set_ls_tablet_ids(
              ls_id,
              related_tablet_ids.inv_idx_tablet_id_,
              related_tablet_ids.fwd_idx_tablet_id_);
          if (OB_FAIL(iters.push_back(retrieval_iter))) {
            LOG_WARN("failed append retrieval iter to array", K(ret));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tr_merge_iter->set_merge_iters(iters))) {
      LOG_WARN("failed to set merge iters for text retrieval", K(ret));
    } else if (OB_FAIL(tr_merge_iter->set_related_tablet_ids(ls_id, related_tablet_ids))) {
      LOG_WARN("failed to set related tabelt ids", K(ret));
    } else {
      ObDASIter **&tr_merge_children = tr_merge_iter->get_children();
      const bool need_do_total_doc_cnt = (ir_scan_ctdef->need_calc_relevance()) && !ir_scan_ctdef->need_estimate_total_doc_cnt();
      const int64_t tr_merge_children_cnt = need_do_total_doc_cnt ? iters.count() + 1 : iters.count();
      if (0 != tr_merge_children_cnt
          && OB_FAIL(create_iter_children_array(tr_merge_children_cnt, alloc, tr_merge_iter))) {
        LOG_WARN("failed to alloc text retrieval merge iter children", K(ret), K(tr_merge_children_cnt));
      } else {
        for (int64_t i = 0; i < iters.count(); ++i) {
          tr_merge_children[i] = iters.at(i);
        }
        if (need_do_total_doc_cnt) {
          tr_merge_children[iters.count()] = doc_cnt_agg_iter;
        }
        tr_merge_iter->set_doc_id_idx_tablet_id(related_tablet_ids.doc_id_idx_tablet_id_);
        tr_merge_iter->set_ls_id(ls_id);
        retrieval_result = tr_merge_iter;
      }
    }
  }
  return ret;
}

int ObDASIterUtils::create_text_retrieval_sub_tree(const ObLSID &ls_id,
                                                  common::ObIAllocator &alloc,
                                                  const ObDASIRScanCtDef *ir_scan_ctdef,
                                                  ObDASIRScanRtDef *ir_scan_rtdef,
                                                  const ObDASFTSTabletID &related_tablet_ids,
                                                  transaction::ObTxDesc *trans_desc,
                                                  transaction::ObTxReadSnapshot *snapshot,
                                                  ObDASIter *&retrieval_result)
          {
  int ret = OB_SUCCESS;
  ObDASTextRetrievalMergeIterParam merge_iter_param;
  ObDASTextRetrievalMergeIter *tr_merge_iter = nullptr;
  ObDASScanIterParam doc_cnt_agg_param;
  ObDASScanIter *doc_cnt_agg_iter = nullptr;
  bool taat_mode = false;

  merge_iter_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
  merge_iter_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
  merge_iter_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
  merge_iter_param.output_ = &ir_scan_ctdef->result_output_;
  merge_iter_param.ir_ctdef_ = ir_scan_ctdef;
  merge_iter_param.ir_rtdef_ = ir_scan_rtdef;
  merge_iter_param.tx_desc_ = trans_desc;
  merge_iter_param.snapshot_ = snapshot;
  if (0 != merge_iter_param.query_tokens_.count()) {
    merge_iter_param.query_tokens_.reuse();
  }

  if (OB_FAIL(ObDASTextRetrievalMergeIter::build_query_tokens(ir_scan_ctdef, ir_scan_rtdef, alloc, merge_iter_param.query_tokens_))) {
    LOG_WARN("failed to get query tokens for text retrieval", K(ret));
  } else if (merge_iter_param.query_tokens_.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT
    || !ir_scan_ctdef->need_proj_relevance_score()) {
    if (!ir_scan_ctdef->need_estimate_total_doc_cnt()) {
      init_scan_iter_param(doc_cnt_agg_param, ir_scan_ctdef->get_doc_id_idx_agg_ctdef(), ir_scan_rtdef);
      doc_cnt_agg_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
      if (OB_FAIL(create_das_iter(alloc, doc_cnt_agg_param, doc_cnt_agg_iter))) {
        LOG_WARN("failed to create doc cnt agg scan iter", K(ret));
      } else {
        merge_iter_param.doc_cnt_iter_ = doc_cnt_agg_iter;
      }
    }
    ObDASTRTaatIter *fts_merge_iter = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_das_iter(alloc, merge_iter_param, fts_merge_iter))) {
      LOG_WARN("failed to create text retrieval merge iter", K(ret));
    } else {
      tr_merge_iter = fts_merge_iter;
      taat_mode = true;
    }
  } else {
    if (ir_scan_ctdef->need_calc_relevance() && !ir_scan_ctdef->need_estimate_total_doc_cnt()) {
      init_scan_iter_param(doc_cnt_agg_param, ir_scan_ctdef->get_doc_id_idx_agg_ctdef(), ir_scan_rtdef);
      doc_cnt_agg_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
      if (OB_FAIL(create_das_iter(alloc, doc_cnt_agg_param, doc_cnt_agg_iter))) {
        LOG_WARN("failed to create doc cnt agg scan iter", K(ret));
      } else {
        merge_iter_param.doc_cnt_iter_ = doc_cnt_agg_iter;
      }
    }
    ObDASTRDaatIter *fts_merge_iter = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_das_iter(alloc, merge_iter_param, fts_merge_iter))) {
      LOG_WARN("failed to create text retrieval merge iter", K(ret));
    } else {
      tr_merge_iter = fts_merge_iter;
      taat_mode = false;
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    ObSEArray<ObDASIter *, 16> iters;
    const ObIArray<ObString> &query_tokens = tr_merge_iter->get_query_tokens();
    int64_t size = taat_mode && query_tokens.count() != 0 ? 1 : query_tokens.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      ObDASTextRetrievalIterParam retrieval_param;
      ObDASTextRetrievalIter *retrieval_iter = nullptr;
      retrieval_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
      retrieval_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
      retrieval_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
      retrieval_param.output_ = &ir_scan_ctdef->result_output_;
      retrieval_param.ir_ctdef_ = ir_scan_ctdef;
      retrieval_param.ir_rtdef_ = ir_scan_rtdef;
      retrieval_param.tx_desc_ = trans_desc;
      retrieval_param.snapshot_ = snapshot;
      retrieval_param.need_inv_idx_agg_reset_ = true;

      ObDASScanIterParam inv_idx_scan_iter_param;
      ObDASScanIter *inv_idx_scan_iter = nullptr;
      init_scan_iter_param(inv_idx_scan_iter_param, ir_scan_ctdef->get_inv_idx_scan_ctdef(), ir_scan_rtdef);
      inv_idx_scan_iter_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
      ObDASScanIterParam inv_idx_agg_iter_param;
      ObDASScanIter *inv_idx_agg_iter = nullptr;
       if (ir_scan_ctdef->need_inv_idx_agg()) {
        init_scan_iter_param(inv_idx_agg_iter_param, ir_scan_ctdef->get_inv_idx_agg_ctdef(), ir_scan_rtdef);
      }
      ObDASScanIterParam fwd_idx_iter_param;
      ObDASScanIter *fwd_idx_iter = nullptr;
      if (ir_scan_ctdef->need_fwd_idx_agg()) {
        init_scan_iter_param(fwd_idx_iter_param, ir_scan_ctdef->get_fwd_idx_agg_ctdef(), ir_scan_rtdef);
      }
      if (OB_FAIL(create_das_iter(alloc, inv_idx_scan_iter_param, inv_idx_scan_iter))) {
        LOG_WARN("failed to create inv idx iter", K(ret));
      } else if (ir_scan_ctdef->need_inv_idx_agg()
          && OB_FAIL(create_das_iter(alloc, inv_idx_agg_iter_param, inv_idx_agg_iter))) {
        LOG_WARN("failed to create inv idx agg iter", K(ret));
      } else if (ir_scan_ctdef->need_fwd_idx_agg()
          && OB_FAIL(create_das_iter(alloc, fwd_idx_iter_param, fwd_idx_iter))) {
        LOG_WARN("failed to create fwd idx iter", K(ret));
      } else {
        retrieval_param.inv_idx_scan_iter_ = inv_idx_scan_iter;
        retrieval_param.inv_idx_agg_iter_ = inv_idx_agg_iter;
        retrieval_param.fwd_idx_iter_ = fwd_idx_iter;
        const int64_t inv_idx_iter_cnt = ir_scan_ctdef->need_inv_idx_agg() ? 2 : 1;
        const int64_t fwd_idx_iter_cnt = ir_scan_ctdef->need_fwd_idx_agg() ? 1 : 0;
        const int64_t tr_children_cnt = inv_idx_iter_cnt + fwd_idx_iter_cnt;
        if (taat_mode) {
          if (OB_FAIL(create_das_iter(alloc, retrieval_param, retrieval_iter))) {
            LOG_WARN("failed to create text retrieval iter", K(ret));
          }
        } else {
          ObDASTRCacheIter *tr_iter = nullptr;
          if (OB_FAIL(create_das_iter(alloc, retrieval_param, tr_iter))) {
            LOG_WARN("failed to create text retrieval iter", K(ret));
          } else {
            retrieval_iter = tr_iter;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(retrieval_iter->set_query_token(query_tokens.at(i)))) {
          LOG_WARN("failed to set query token for text retrieval iter", K(ret));
        } else if (OB_FAIL(create_iter_children_array(tr_children_cnt, alloc, retrieval_iter))) {
          LOG_WARN("failed to create iter children array", K(ret));
        } else {
          retrieval_iter->get_children()[0] = inv_idx_scan_iter;
          if (ir_scan_ctdef->need_inv_idx_agg()) {
            retrieval_iter->get_children()[1] = inv_idx_agg_iter;
          }
          if (ir_scan_ctdef->need_fwd_idx_agg()) {
            retrieval_iter->get_children()[2] = fwd_idx_iter;
          }
          retrieval_iter->set_ls_tablet_ids(
              ls_id,
              related_tablet_ids.inv_idx_tablet_id_,
              related_tablet_ids.fwd_idx_tablet_id_);
          if (OB_FAIL(iters.push_back(retrieval_iter))) {
            LOG_WARN("failed append retrieval iter to array", K(ret));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tr_merge_iter->set_merge_iters(iters))) {
      LOG_WARN("failed to set merge iters for text retrieval", K(ret));
    } else if (OB_FAIL(tr_merge_iter->set_related_tablet_ids(ls_id, related_tablet_ids))) {
      LOG_WARN("failed to set related tabelt ids", K(ret));
    } else {
      ObDASIter **&tr_merge_children = tr_merge_iter->get_children();
      const bool need_do_total_doc_cnt = (taat_mode || ir_scan_ctdef->need_calc_relevance()) && !ir_scan_ctdef->need_estimate_total_doc_cnt();
      const int64_t tr_merge_children_cnt = need_do_total_doc_cnt ? iters.count() + 1 : iters.count();
      if (0 != tr_merge_children_cnt
          && OB_FAIL(create_iter_children_array(tr_merge_children_cnt, alloc, tr_merge_iter))) {
        LOG_WARN("failed to alloc text retrieval merge iter children", K(ret), K(tr_merge_children_cnt));
      } else {
        for (int64_t i = 0; i < iters.count(); ++i) {
          tr_merge_children[i] = iters.at(i);
        }
        if (need_do_total_doc_cnt) {
          tr_merge_children[iters.count()] = doc_cnt_agg_iter;
        }
        tr_merge_iter->set_doc_id_idx_tablet_id(related_tablet_ids.doc_id_idx_tablet_id_);
        tr_merge_iter->set_ls_id(ls_id);
        retrieval_result = tr_merge_iter;
      }
    }
  }

  return ret;
}

int ObDASIterUtils::create_doc_id_scan_sub_tree(
    ObTableScanParam &scan_param,
    common::ObIAllocator &alloc,
    const ObDASDocIdMergeCtDef *merge_ctdef,
    ObDASDocIdMergeRtDef *merge_rtdef,
    const ObDASRelatedTabletID &related_tablet_ids,
    transaction::ObTxDesc *trans_desc,
    transaction::ObTxReadSnapshot *snapshot,
    ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_ctdef) || OB_ISNULL(merge_rtdef) || OB_UNLIKELY(2 != merge_ctdef->children_cnt_)
      || OB_ISNULL(iter_tree) || OB_UNLIKELY(ObDASIterType::DAS_ITER_SCAN != iter_tree->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(merge_ctdef), KPC(merge_rtdef), KPC(iter_tree));
  } else {
    ObDASDocIdMergeIterParam doc_id_merge_param;
    ObDASDocIdMergeIter *doc_id_merge_iter = nullptr;
    ObDASScanIterParam rowkey_doc_param;
    ObDASScanIter *rowkey_doc_iter = nullptr;
    init_scan_iter_param(rowkey_doc_param, static_cast<ObDASScanCtDef *>(merge_ctdef->children_[1]),merge_rtdef );
    if (OB_FAIL(create_das_iter(alloc, rowkey_doc_param, rowkey_doc_iter))) {
      LOG_WARN("fail to create das scan iter", K(ret), K(rowkey_doc_param));
    } else {
      doc_id_merge_param.rowkey_doc_tablet_id_ = related_tablet_ids.rowkey_doc_tablet_id_;
      doc_id_merge_param.rowkey_doc_ls_id_     = scan_param.ls_id_;
      doc_id_merge_param.data_table_ctdef_     = static_cast<ObDASScanCtDef *>(merge_ctdef->children_[0]);
      doc_id_merge_param.rowkey_doc_ctdef_     = static_cast<ObDASScanCtDef *>(merge_ctdef->children_[1]);
      doc_id_merge_param.data_table_rtdef_     = static_cast<ObDASScanRtDef *>(merge_rtdef->children_[0]);
      doc_id_merge_param.rowkey_doc_rtdef_     = static_cast<ObDASScanRtDef *>(merge_rtdef->children_[1]);
      doc_id_merge_param.data_table_iter_      = static_cast<ObDASScanIter *>(iter_tree);
      doc_id_merge_param.rowkey_doc_iter_      = rowkey_doc_iter;
      doc_id_merge_param.trans_desc_           = trans_desc;
      doc_id_merge_param.snapshot_             = snapshot;
      if (OB_FAIL(create_das_iter(alloc, doc_id_merge_param, doc_id_merge_iter))) {
        LOG_WARN("fail to create doc id merge iter", K(ret), K(doc_id_merge_param));
      } else if (OB_FAIL(create_iter_children_array(2, alloc, doc_id_merge_iter))) {
        LOG_WARN("fail to create doc id merge iter children array", K(ret));
      } else {
        doc_id_merge_iter->get_children()[0] = iter_tree;
        doc_id_merge_iter->get_children()[1] = rowkey_doc_iter;
        rowkey_doc_iter->set_scan_param(doc_id_merge_iter->get_rowkey_doc_scan_param());
        iter_tree = doc_id_merge_iter;
      }
    }
  }
  return ret;
}

int ObDASIterUtils::create_vid_scan_sub_tree(
    ObTableScanParam &scan_param,
    common::ObIAllocator &alloc,
    const ObDASVIdMergeCtDef *merge_ctdef,
    ObDASVIdMergeRtDef *merge_rtdef,
    const ObDASRelatedTabletID &related_tablet_ids,
    transaction::ObTxDesc *trans_desc,
    transaction::ObTxReadSnapshot *snapshot,
    ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_ctdef) || OB_ISNULL(merge_rtdef) || OB_UNLIKELY(2 != merge_ctdef->children_cnt_)
      || OB_ISNULL(iter_tree) || OB_UNLIKELY(ObDASIterType::DAS_ITER_SCAN != iter_tree->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(merge_ctdef), KPC(merge_rtdef), KPC(iter_tree));
  } else {
    ObDASVIdMergeIterParam vid_merge_param;
    ObDASVIdMergeIter *vid_merge_iter = nullptr;
    ObDASScanIterParam rowkey_vid_param;
    ObDASScanIter *rowkey_vid_iter = nullptr;
    init_scan_iter_param(rowkey_vid_param, static_cast<ObDASScanCtDef *>(merge_ctdef->children_[1]), merge_rtdef);
    if (OB_FAIL(create_das_iter(alloc, rowkey_vid_param, rowkey_vid_iter))) {
      LOG_WARN("fail to create das scan iter", K(ret), K(rowkey_vid_param));
    } else {
      vid_merge_param.rowkey_vid_tablet_id_ = related_tablet_ids.rowkey_vid_tablet_id_;
      vid_merge_param.rowkey_vid_ls_id_     = scan_param.ls_id_;
      vid_merge_param.data_table_ctdef_     = static_cast<ObDASScanCtDef *>(merge_ctdef->children_[0]);
      vid_merge_param.rowkey_vid_ctdef_     = static_cast<ObDASScanCtDef *>(merge_ctdef->children_[1]);
      vid_merge_param.data_table_rtdef_     = static_cast<ObDASScanRtDef *>(merge_rtdef->children_[0]);
      vid_merge_param.rowkey_vid_rtdef_     = static_cast<ObDASScanRtDef *>(merge_rtdef->children_[1]);
      vid_merge_param.data_table_iter_      = static_cast<ObDASScanIter *>(iter_tree);
      vid_merge_param.rowkey_vid_iter_      = rowkey_vid_iter;
      vid_merge_param.trans_desc_           = trans_desc;
      vid_merge_param.snapshot_             = snapshot;
      if (OB_FAIL(create_das_iter(alloc, vid_merge_param, vid_merge_iter))) {
        LOG_WARN("fail to create vid id merge iter", K(ret), K(vid_merge_param));
      } else if (OB_FAIL(create_iter_children_array(2, alloc, vid_merge_iter))) {
        LOG_WARN("fail to create vid id merge iter children array", K(ret));
      } else {
        vid_merge_iter->get_children()[0] = iter_tree;
        vid_merge_iter->get_children()[1] = rowkey_vid_iter;
        rowkey_vid_iter->set_scan_param(vid_merge_iter->get_rowkey_vid_scan_param());
        iter_tree = vid_merge_iter;
      }
    }
  }
  return ret;
}

int ObDASIterUtils::create_domain_lookup_sub_tree(ObTableScanParam &scan_param,
                                                  const ObLSID &ls_id,
                                                  common::ObIAllocator &alloc,
                                                  const ObDASTableLookupCtDef *table_lookup_ctdef,
                                                  ObDASTableLookupRtDef *table_lookup_rtdef,
                                                  const ObDASRelatedTabletID &related_tablet_ids,
                                                  const bool &doc_id_lookup_keep_order,
                                                  const bool &main_lookup_keep_order,
                                                  transaction::ObTxDesc *trans_desc,
                                                  transaction::ObTxReadSnapshot *snapshot,
                                                  ObDASIter *doc_id_iter,
                                                  ObDASIter *&domain_lookup_result)
{
  int ret = OB_SUCCESS;
  const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
  ObDASIRAuxLookupRtDef *aux_lookup_rtdef = nullptr;
  ObDASLocalLookupIter *doc_id_lookup_iter = nullptr;
  ObDASLocalLookupIterParam doc_id_lookup_param;
  ObDASLocalLookupIter *main_lookup_iter = nullptr;
  ObDASLocalLookupIterParam main_lookup_param;
  if (OB_UNLIKELY(table_lookup_ctdef->get_rowkey_scan_ctdef()->op_type_ != ObDASOpType::DAS_OP_IR_AUX_LOOKUP)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey scan is not an aux lookup", K(ret));
  } else {
    aux_lookup_ctdef = static_cast<const ObDASIRAuxLookupCtDef *>(table_lookup_ctdef->get_rowkey_scan_ctdef());
    aux_lookup_rtdef = static_cast<ObDASIRAuxLookupRtDef *>(table_lookup_rtdef->get_rowkey_scan_rtdef());
    ObDASScanIter *doc_id_table_iter = nullptr;
    ObDASScanIterParam doc_id_table_param;
    doc_id_table_param.scan_ctdef_ = aux_lookup_ctdef->get_lookup_scan_ctdef();
    doc_id_table_param.max_size_ = 1;
    doc_id_table_param.eval_ctx_ = aux_lookup_rtdef->eval_ctx_;
    doc_id_table_param.exec_ctx_ = &aux_lookup_rtdef->eval_ctx_->exec_ctx_;
    doc_id_table_param.output_ = &aux_lookup_ctdef->result_output_;
    if (OB_FAIL(create_das_iter(alloc, doc_id_table_param, doc_id_table_iter))) {
      LOG_WARN("failed to create doc id table iter", K(ret));
    } else {
      doc_id_lookup_param.max_size_ = aux_lookup_rtdef->eval_ctx_->is_vectorized()
           ? aux_lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
      doc_id_lookup_param.eval_ctx_ = aux_lookup_rtdef->eval_ctx_;
      doc_id_lookup_param.exec_ctx_ = &aux_lookup_rtdef->eval_ctx_->exec_ctx_;
      doc_id_lookup_param.output_ = &aux_lookup_ctdef->result_output_;
      doc_id_lookup_param.default_batch_row_count_ = doc_id_lookup_param.max_size_;
      doc_id_lookup_param.index_ctdef_ = aux_lookup_ctdef->get_doc_id_scan_ctdef();
      doc_id_lookup_param.index_rtdef_ = aux_lookup_rtdef->get_doc_id_scan_rtdef();
      doc_id_lookup_param.lookup_ctdef_ = aux_lookup_ctdef->get_lookup_scan_ctdef();
      doc_id_lookup_param.lookup_rtdef_ = aux_lookup_rtdef->get_lookup_scan_rtdef();
      doc_id_lookup_param.index_table_iter_ = doc_id_iter;
      doc_id_lookup_param.data_table_iter_ = doc_id_table_iter;
      doc_id_lookup_param.trans_desc_ = trans_desc;
      doc_id_lookup_param.snapshot_ = snapshot;
      doc_id_lookup_param.rowkey_exprs_ = &aux_lookup_ctdef->get_lookup_scan_ctdef()->rowkey_exprs_;
      if (doc_id_lookup_keep_order) {
        doc_id_lookup_param.lookup_rtdef_->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
      }
      if (OB_FAIL(create_das_iter(alloc, doc_id_lookup_param, doc_id_lookup_iter))) {
        LOG_WARN("failed to create doc id lookup iter", K(ret));
      } else if (OB_FAIL(create_iter_children_array(2, alloc, doc_id_lookup_iter))) {
        LOG_WARN("failed to create iter children array", K(ret));
      } else {
        doc_id_lookup_iter->get_children()[0] = doc_id_iter;
        doc_id_lookup_iter->get_children()[1] = doc_id_table_iter;
        doc_id_table_iter->set_scan_param(doc_id_lookup_iter->get_lookup_param());
        doc_id_lookup_iter->set_tablet_id(related_tablet_ids.doc_id_idx_tablet_id_);
        doc_id_lookup_iter->set_ls_id(ls_id);
      }
    }
  }

  ObDASScanIter *data_table_iter = nullptr;
  ObDASIter *iter_tree = nullptr;
  if (OB_SUCC(ret)) {
    ObDASScanIterParam data_table_param;
    data_table_param.scan_ctdef_ = table_lookup_ctdef->get_lookup_scan_ctdef();
    data_table_param.max_size_ = 1;
    data_table_param.eval_ctx_ = table_lookup_rtdef->eval_ctx_;
    data_table_param.exec_ctx_ = &table_lookup_rtdef->eval_ctx_->exec_ctx_;
    data_table_param.output_ = &table_lookup_ctdef->result_output_;
    if (OB_FAIL(create_das_iter(alloc, data_table_param, data_table_iter))) {
      LOG_WARN("failed to create data table lookup scan iter", K(ret));
    } else {
      iter_tree = static_cast<ObDASIter *>(data_table_iter);
      ObDASBaseCtDef *ctdef = table_lookup_ctdef->children_[1];
      ObDASBaseRtDef *rtdef = table_lookup_rtdef->children_[1];
      if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpeted error, ctdef or rtdef is nullptr", K(ret), KPC(ctdef), KPC(rtdef));
      } else if (ObDASOpType::DAS_OP_TABLE_SCAN == ctdef->op_type_) {
        // nothing to do
      } else if (OB_UNLIKELY(ObDASOpType::DAS_OP_VID_MERGE == ctdef->op_type_)) {
        if (OB_FAIL(create_vid_scan_sub_tree(scan_param, alloc, static_cast<const ObDASVIdMergeCtDef *>(ctdef),
              static_cast<ObDASVIdMergeRtDef *>(rtdef), related_tablet_ids, trans_desc, snapshot, iter_tree))) {
          LOG_WARN("fail to create doc id scan sub tree", K(ret), K(scan_param), KPC(ctdef), KPC(rtdef));
        }
      } else if (OB_UNLIKELY(ObDASOpType::DAS_OP_DOC_ID_MERGE != ctdef->op_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, attach op type isn't doc id merge", K(ret), K(ctdef->op_type_), KPC(ctdef));
      } else if (OB_FAIL(create_doc_id_scan_sub_tree(scan_param, alloc, static_cast<const ObDASDocIdMergeCtDef *>(ctdef),
              static_cast<ObDASDocIdMergeRtDef *>(rtdef), related_tablet_ids, trans_desc, snapshot, iter_tree))) {
        LOG_WARN("fail to create doc id scan sub tree", K(ret), K(scan_param), KPC(ctdef), KPC(rtdef));
      }
    }
  }
  if (OB_SUCC(ret)) {
    main_lookup_param.max_size_ = table_lookup_rtdef->eval_ctx_->is_vectorized()
        ? aux_lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
    main_lookup_param.eval_ctx_ = table_lookup_rtdef->eval_ctx_;
    main_lookup_param.exec_ctx_ = &table_lookup_rtdef->eval_ctx_->exec_ctx_;
    main_lookup_param.output_ = &table_lookup_ctdef->result_output_;
    main_lookup_param.default_batch_row_count_ = main_lookup_param.max_size_;
    main_lookup_param.index_ctdef_ = table_lookup_ctdef->get_rowkey_scan_ctdef();
    main_lookup_param.index_rtdef_ = table_lookup_rtdef->get_rowkey_scan_rtdef();
    main_lookup_param.lookup_ctdef_ = table_lookup_ctdef->get_lookup_scan_ctdef();
    main_lookup_param.lookup_rtdef_ = table_lookup_rtdef->get_lookup_scan_rtdef();
    main_lookup_param.index_table_iter_ = doc_id_lookup_iter;
    main_lookup_param.data_table_iter_ = iter_tree;
    main_lookup_param.trans_desc_ = trans_desc;
    main_lookup_param.snapshot_ = snapshot;
    main_lookup_param.rowkey_exprs_ = &table_lookup_ctdef->get_lookup_scan_ctdef()->rowkey_exprs_;
    if (main_lookup_keep_order) {
      main_lookup_param.lookup_rtdef_->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    }
    if (OB_FAIL(create_das_iter(alloc, main_lookup_param, main_lookup_iter))) {
      LOG_WARN("failed to create das iter", K(ret));
    } else if (OB_FAIL(create_iter_children_array(2, alloc, main_lookup_iter))) {
      LOG_WARN("failed to create iter children array", K(ret));
    } else {
      main_lookup_iter->get_children()[0] = doc_id_lookup_iter;
      main_lookup_iter->get_children()[1] = iter_tree;
      data_table_iter->set_scan_param(main_lookup_iter->get_lookup_param());
      main_lookup_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
      main_lookup_iter->set_ls_id(ls_id);
    }
  }

  if (OB_SUCC(ret)) {
    domain_lookup_result = main_lookup_iter;
  }

  return ret;
}

int ObDASIterUtils::create_function_lookup_tree(ObTableScanParam &scan_param,
                                               common::ObIAllocator &alloc,
                                               const ObDASBaseCtDef *attach_ctdef,
                                               ObDASBaseRtDef *attach_rtdef,
                                               const ObDASRelatedTabletID &related_tablet_ids,
                                               transaction::ObTxDesc *trans_desc,
                                               transaction::ObTxReadSnapshot *snapshot,
                                               ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  const ObDASIndexProjLookupCtDef *idx_proj_lookup_ctdef = nullptr;
  ObDASIndexProjLookupRtDef *idx_proj_lookup_rtdef = nullptr;
  const ObDASFuncLookupCtDef *func_lookup_ctdef = nullptr;
  ObDASFuncLookupRtDef *func_lookup_rtdef = nullptr;

  const ObDASBaseCtDef *rowkey_scan_ctdef = nullptr;
  ObDASBaseRtDef *rowkey_scan_rtdef = nullptr;
  ObDASIter *rowkey_scan_iter = nullptr;
  bool lookup_keep_order = false;
  ObTableScanParam *rowkey_scan_param = nullptr;
  void *buf = nullptr;

  // for check {
  const ExprFixedArray *docid_lookup_rowkey_exprs = nullptr;
  const ExprFixedArray *main_lookup_rowkey_exprs =nullptr;
  const ExprFixedArray *rowkey_scan_ouput_exprs = nullptr;
  // for check }

  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(attach_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr to attach def", K(ret), KP(attach_ctdef), KP(attach_rtdef));
  } else if (OB_ISNULL(rowkey_scan_param = OB_NEWx(ObTableScanParam, &alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to new rowkey scan param", K(sizeof(ObTableScanParam)), K(ret));
  } else if (OB_UNLIKELY(attach_ctdef->op_type_ != ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected text retrieval root attach def type", K(ret), KPC(attach_ctdef));
  } else {
    idx_proj_lookup_ctdef = static_cast<const ObDASIndexProjLookupCtDef *>(attach_ctdef);
    idx_proj_lookup_rtdef = static_cast<ObDASIndexProjLookupRtDef *>(attach_rtdef);
    func_lookup_ctdef = static_cast<const ObDASFuncLookupCtDef *>(idx_proj_lookup_ctdef->get_lookup_ctdef());
    func_lookup_rtdef = static_cast<ObDASFuncLookupRtDef *>(idx_proj_lookup_rtdef->get_lookup_rtdef());
    rowkey_scan_ctdef = idx_proj_lookup_ctdef->get_rowkey_scan_ctdef();
    rowkey_scan_rtdef = idx_proj_lookup_rtdef->get_rowkey_scan_rtdef();
    if (OB_ISNULL(func_lookup_ctdef) || OB_ISNULL(func_lookup_rtdef)
        || OB_ISNULL(rowkey_scan_ctdef) || OB_ISNULL(rowkey_scan_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to ctdef", K(ret), KP(func_lookup_ctdef));
    } else if (OB_UNLIKELY(rowkey_scan_ctdef->op_type_ != ObDASOpType::DAS_OP_IR_AUX_LOOKUP
        && rowkey_scan_ctdef->op_type_ != ObDASOpType::DAS_OP_TABLE_SCAN
        && rowkey_scan_ctdef->op_type_ != ObDASOpType::DAS_OP_SORT)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected rowkey scan type", K(ret), KPC(rowkey_scan_ctdef));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ObDASOpType::DAS_OP_IR_AUX_LOOKUP == rowkey_scan_ctdef->op_type_) {
    const ObDASIRScanCtDef *ir_scan_ctdef = nullptr;
    ObDASIRScanRtDef *ir_scan_rtdef = nullptr;
    const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = static_cast<const ObDASIRAuxLookupCtDef *>(rowkey_scan_ctdef);
    rowkey_scan_ouput_exprs = &aux_lookup_ctdef->get_lookup_scan_ctdef()->result_output_;
    ObDASIRAuxLookupRtDef *aux_lookup_rtdef = static_cast<ObDASIRAuxLookupRtDef *>(rowkey_scan_rtdef);
    ObDASLocalLookupIter *doc_id_lookup_iter = nullptr;
    ObDASIter *text_retrieval_result = nullptr;
    const ObDASSortCtDef *sort_ctdef = nullptr;
    ObDASSortRtDef *sort_rtdef = nullptr;
    ObDASIter *sort_result = nullptr;
    bool taat_mode = false;

    ObDASFTSTabletID fts_tablet_ids;
    fts_tablet_ids.inv_idx_tablet_id_ = related_tablet_ids.inv_idx_tablet_id_;
    fts_tablet_ids.fwd_idx_tablet_id_ = related_tablet_ids.fwd_idx_tablet_id_;
    fts_tablet_ids.doc_id_idx_tablet_id_ = related_tablet_ids.doc_id_idx_tablet_id_;
    const bool need_rewind = true;
    const bool need_distinct = false;
    if (OB_FAIL(ObDASUtils::find_target_das_def(
        rowkey_scan_ctdef,
        rowkey_scan_rtdef,
        ObDASOpType::DAS_OP_IR_SCAN,
        ir_scan_ctdef,
        ir_scan_rtdef))) {
      LOG_WARN("fail to find ir scan definition", K(ret));
    } else if (OB_FAIL(create_text_retrieval_sub_tree(
        scan_param.ls_id_,
        alloc,
        ir_scan_ctdef,
        ir_scan_rtdef,
        fts_tablet_ids,
        trans_desc,
        snapshot,
        text_retrieval_result))) {
      LOG_WARN("failed to create text retrieval sub tree", K(ret));
    } else if (FALSE_IT(rowkey_scan_iter = text_retrieval_result)) {
    } else if (aux_lookup_ctdef->get_doc_id_scan_ctdef()->op_type_ != ObDASOpType::DAS_OP_SORT) {
      // do nothing, just skip
    } else if (FALSE_IT(sort_ctdef = static_cast<const ObDASSortCtDef *>(aux_lookup_ctdef->get_doc_id_scan_ctdef()))) {
    } else if (FALSE_IT(sort_rtdef = static_cast<ObDASSortRtDef *>(aux_lookup_rtdef->get_doc_id_scan_rtdef()))) {
    } else if (OB_FAIL(create_sort_sub_tree(
        alloc, sort_ctdef, sort_rtdef, need_rewind, need_distinct, text_retrieval_result, sort_result))) {
      LOG_WARN("failed to create sort sub tree", K(ret));
    } else {
      rowkey_scan_iter = sort_result;
    }
    if (OB_FAIL(ret)) {
    } else {
      ObDASScanIter *docid_rowkey_table_iter = nullptr;
      ObDASScanIterParam docid_rowkey_table_param;
      const ObDASScanCtDef *lookup_ctdef = static_cast<const ObDASScanCtDef*>(aux_lookup_ctdef->get_lookup_scan_ctdef());
      ObDASScanRtDef *lookup_rtdef = static_cast<ObDASScanRtDef*>(aux_lookup_rtdef->get_lookup_scan_rtdef());
      docid_rowkey_table_param.scan_ctdef_ = lookup_ctdef;
      docid_rowkey_table_param.max_size_ = lookup_rtdef->eval_ctx_->is_vectorized() ? lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
      docid_rowkey_table_param.eval_ctx_ = lookup_rtdef->eval_ctx_;
      docid_rowkey_table_param.exec_ctx_ = &lookup_rtdef->eval_ctx_->exec_ctx_;
      docid_rowkey_table_param.output_ = &lookup_ctdef->result_output_;
      if (OB_FAIL(create_das_iter(alloc, docid_rowkey_table_param, docid_rowkey_table_iter))) {
        LOG_WARN("failed to create doc id table iter", K(ret));
      } else {
        ObDASLocalLookupIterParam doc_id_lookup_param;
        doc_id_lookup_param.max_size_ = aux_lookup_rtdef->eval_ctx_->is_vectorized()
           ? aux_lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
        doc_id_lookup_param.eval_ctx_ = aux_lookup_rtdef->eval_ctx_;
        doc_id_lookup_param.exec_ctx_ = &aux_lookup_rtdef->eval_ctx_->exec_ctx_;
        doc_id_lookup_param.output_ = &aux_lookup_ctdef->result_output_;
        doc_id_lookup_param.default_batch_row_count_ = doc_id_lookup_param.max_size_;
        doc_id_lookup_param.index_ctdef_ = aux_lookup_ctdef->get_doc_id_scan_ctdef();
        doc_id_lookup_param.index_rtdef_ = aux_lookup_rtdef->get_doc_id_scan_rtdef();
        doc_id_lookup_param.lookup_ctdef_ = aux_lookup_ctdef->get_lookup_scan_ctdef();
        doc_id_lookup_param.lookup_rtdef_ = aux_lookup_rtdef->get_lookup_scan_rtdef();
        doc_id_lookup_param.index_table_iter_ = rowkey_scan_iter;
        doc_id_lookup_param.data_table_iter_ = docid_rowkey_table_iter;
        doc_id_lookup_param.trans_desc_ = trans_desc;
        doc_id_lookup_param.snapshot_ = snapshot;
        doc_id_lookup_param.rowkey_exprs_ = &aux_lookup_ctdef->get_lookup_scan_ctdef()->rowkey_exprs_;
        ObDASTextRetrievalMergeIter *tr_merge_iter = static_cast<ObDASTextRetrievalMergeIter *>(text_retrieval_result);
        taat_mode = tr_merge_iter->is_taat_mode();
        if (taat_mode || sort_result) {
          doc_id_lookup_param.lookup_rtdef_->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
        }
        if (OB_FAIL(create_das_iter(alloc, doc_id_lookup_param, doc_id_lookup_iter))) {
          LOG_WARN("failed to create doc id lookup iter", K(ret));
        } else if (OB_FAIL(create_iter_children_array(2, alloc, doc_id_lookup_iter))) {
          LOG_WARN("failed to create iter children array", K(ret));
        } else {
          doc_id_lookup_iter->get_children()[0] = rowkey_scan_iter;
          doc_id_lookup_iter->get_children()[1] = docid_rowkey_table_iter;
          docid_rowkey_table_iter->set_scan_param(doc_id_lookup_iter->get_lookup_param());
          doc_id_lookup_iter->set_tablet_id(related_tablet_ids.doc_id_idx_tablet_id_);
          doc_id_lookup_iter->set_ls_id(scan_param.ls_id_);
          rowkey_scan_iter = doc_id_lookup_iter;
        }
      }
    }
  } else if (ObDASOpType::DAS_OP_TABLE_SCAN == rowkey_scan_ctdef->op_type_) {
    ObDASScanIter *scan_iter = nullptr;
    ObDASScanIterParam iter_param;
    // this code is based on the assumption that scan_param will be not released util this iter is released
    const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef*>(rowkey_scan_ctdef);
    ObDASScanRtDef *rtdef = static_cast<ObDASScanRtDef*>(rowkey_scan_rtdef);
    iter_param.scan_ctdef_ = ctdef;
    iter_param.max_size_ = rtdef->eval_ctx_->is_vectorized() ? rtdef->eval_ctx_->max_batch_size_ : 1;
    iter_param.eval_ctx_ = rtdef->eval_ctx_;
    iter_param.exec_ctx_ = &rtdef->eval_ctx_->exec_ctx_;
    iter_param.output_ = &ctdef->result_output_;
    if (OB_FAIL(create_das_iter(alloc, iter_param, scan_iter))) {
      LOG_WARN("failed to create data table lookup scan iter", K(ret));
    } else if (FALSE_IT(scan_iter->set_scan_param(scan_param))) {
      LOG_WARN("failed to init default scan param", K(ret));
    } else {
      rowkey_scan_iter = scan_iter;
      rowkey_scan_ouput_exprs = &static_cast<const ObDASScanCtDef *>(rowkey_scan_ctdef)->pd_expr_spec_.access_exprs_;
    }
  } else if (ObDASOpType::DAS_OP_SORT == rowkey_scan_ctdef->op_type_) {
    const ObDASScanCtDef *scan_ctdef = nullptr;
    ObDASScanRtDef *scan_rtdef = nullptr;
    ObDASScanIterParam iter_param;
    const ObDASSortCtDef *sort_ctdef = nullptr;
    ObDASSortRtDef *sort_rtdef = nullptr;
    ObDASIter *sort_result = nullptr;
    ObDASScanIter *scan_iter = nullptr;
    const bool need_rewind = true;
    const bool need_distinct = false;
    if (OB_FAIL(ObDASUtils::find_target_das_def(
        rowkey_scan_ctdef,
        rowkey_scan_rtdef,
        ObDASOpType::DAS_OP_TABLE_SCAN,
        scan_ctdef,
        scan_rtdef))) {
      LOG_WARN("fail to find scan definition", K(ret));
    } else {
      const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef*>(scan_ctdef);
      ObDASScanRtDef *rtdef = static_cast<ObDASScanRtDef*>(scan_rtdef);
      iter_param.scan_ctdef_ = ctdef;
      iter_param.max_size_ = rtdef->eval_ctx_->is_vectorized() ? rtdef->eval_ctx_->max_batch_size_ : 1;
      iter_param.eval_ctx_ = rtdef->eval_ctx_;
      iter_param.exec_ctx_ = &rtdef->eval_ctx_->exec_ctx_;
      iter_param.output_ = &ctdef->result_output_;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_das_iter(alloc, iter_param, scan_iter))) {
      LOG_WARN("failed to create data table lookup scan iter", K(ret));
    } else if (FALSE_IT(scan_iter->set_scan_param(scan_param))) {
    } else if (FALSE_IT(sort_ctdef = static_cast<const ObDASSortCtDef *>(rowkey_scan_ctdef))) {
    } else if (FALSE_IT(sort_rtdef = static_cast<ObDASSortRtDef *>(rowkey_scan_rtdef))) {
    } else if (OB_FAIL(create_sort_sub_tree(
        alloc, sort_ctdef, sort_rtdef, need_rewind, need_distinct, scan_iter, sort_result))) {
      LOG_WARN("failed to create sort sub tree", K(ret));
    } else {
      rowkey_scan_iter = sort_result;
      rowkey_scan_ouput_exprs = &scan_ctdef->pd_expr_spec_.access_exprs_;
    }
  }

  // check exprs
  docid_lookup_rowkey_exprs =  &static_cast<const ObDASScanCtDef *>(func_lookup_ctdef->get_doc_id_lookup_scan_ctdef())->rowkey_exprs_;
  bool find = false;
  for (int i = 0; OB_SUCC(ret) && i < docid_lookup_rowkey_exprs->count(); i++) {
    for (int j = 0; OB_SUCC(ret) && !find && j < rowkey_scan_ouput_exprs->count(); j++) {
      if (rowkey_scan_ouput_exprs->at(j) == docid_lookup_rowkey_exprs->at(i)) {
        find = true;
      }
    }
    if (OB_UNLIKELY(!find)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, rowkey scan output exprs count not equal to docid lookup rowkey exprs count", K(ret));
    } else {
      find = false;
    }
  }

  if (OB_SUCC(ret) && func_lookup_ctdef->has_main_table_lookup()) {
    find = false;
    main_lookup_rowkey_exprs = &static_cast<const ObDASScanCtDef *>(func_lookup_ctdef->get_main_lookup_scan_ctdef())->rowkey_exprs_;
    for (int i = 0; OB_SUCC(ret) && i < main_lookup_rowkey_exprs->count(); i++) {
      for (int j = 0; OB_SUCC(ret) && !find && j < rowkey_scan_ouput_exprs->count(); j++) {
        if (rowkey_scan_ouput_exprs->at(j) == main_lookup_rowkey_exprs->at(i)) {
          find = true;
        }
      }
      if (OB_UNLIKELY(!find)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, rowkey scan output exprs count not equal to docid lookup rowkey exprs count", K(ret));
      } else {
        find = false;
      }
    }
  }

  ObDASIter *func_lookup_result = nullptr;
  ObDASCacheLookupIter *root_lookup_iter = nullptr;
  if (FAILEDx(create_functional_lookup_sub_tree(
      scan_param,
      scan_param.ls_id_,
      alloc,
      func_lookup_ctdef,
      func_lookup_rtdef,
      related_tablet_ids,
      true,
      trans_desc,
      snapshot,
      func_lookup_result))) {
    LOG_WARN("failed to create domain index lookup iters", K(ret));
  } else {
    ObDASCacheLookupIterParam root_lookup_param;
    root_lookup_param.max_size_ = idx_proj_lookup_rtdef->eval_ctx_->is_vectorized()
        ? idx_proj_lookup_rtdef->get_rowkey_scan_rtdef()->eval_ctx_->max_batch_size_ : 1;
    root_lookup_param.eval_ctx_ = idx_proj_lookup_rtdef->eval_ctx_;
    root_lookup_param.exec_ctx_ = &idx_proj_lookup_rtdef->eval_ctx_->exec_ctx_;
    root_lookup_param.output_ = &idx_proj_lookup_ctdef->result_output_;
    root_lookup_param.default_batch_row_count_ = root_lookup_param.max_size_;
    root_lookup_param.index_ctdef_ = idx_proj_lookup_ctdef->get_rowkey_scan_ctdef();
    root_lookup_param.index_rtdef_ = idx_proj_lookup_rtdef->get_rowkey_scan_rtdef();
    root_lookup_param.lookup_ctdef_ = static_cast<const ObDASScanCtDef *>(func_lookup_ctdef->get_doc_id_lookup_scan_ctdef());
    root_lookup_param.lookup_rtdef_ = static_cast<ObDASScanRtDef *>(func_lookup_rtdef->get_doc_id_lookup_scan_rtdef());
    root_lookup_param.index_table_iter_ = rowkey_scan_iter;
    root_lookup_param.data_table_iter_ = func_lookup_result;
    root_lookup_param.trans_desc_ = trans_desc;
    root_lookup_param.snapshot_ = snapshot;
    root_lookup_param.rowkey_exprs_ = &static_cast<const ObDASScanCtDef *>(func_lookup_ctdef->get_doc_id_lookup_scan_ctdef())->rowkey_exprs_;
    root_lookup_param.lookup_rtdef_->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    if (OB_FAIL(create_das_iter(alloc, root_lookup_param, root_lookup_iter))) {
      LOG_WARN("failed to create das iter", K(ret));
    } else if (OB_FAIL(create_iter_children_array(2, alloc, root_lookup_iter))) {
      LOG_WARN("failed to create iter children array", K(ret));
    } else {
      root_lookup_iter->get_children()[0] = rowkey_scan_iter;
      root_lookup_iter->get_children()[1] = func_lookup_result;
      static_cast<ObDASFuncLookupIter *>(func_lookup_result)->set_index_scan_param(root_lookup_iter->get_lookup_param());
      root_lookup_iter->set_tablet_id(related_tablet_ids.rowkey_doc_tablet_id_);
      root_lookup_iter->set_ls_id(scan_param.ls_id_);
      iter_tree = root_lookup_iter;
    }
  }
  return ret;
}

int ObDASIterUtils::create_functional_lookup_sub_tree(ObTableScanParam &scan_param,
                                                      const ObLSID &ls_id,
                                                      common::ObIAllocator &alloc,
                                                      const ObDASFuncLookupCtDef *func_lookup_ctdef,
                                                      ObDASFuncLookupRtDef *func_lookup_rtdef,
                                                      const ObDASRelatedTabletID &related_tablet_ids,
                                                      const bool &lookup_keep_order,
                                                      transaction::ObTxDesc *trans_desc,
                                                      transaction::ObTxReadSnapshot *snapshot,
                                                      ObDASIter *&fun_lookup_result)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  ObDASIter **data_table_iters = nullptr;
  ObDASScanIter *main_lookup_table_iter = nullptr;

  ObDASFuncDataIter *fts_merge_iter = nullptr;
  ObDASScanIter *rowkey_docid_iter = nullptr;

  ObDASFuncLookupIter *func_lookup_iter = nullptr;

  // ObDASCacheLookupIter *root_local_lookup_iter = nullptr;
  const int64_t func_lookup_cnt = func_lookup_ctdef->func_lookup_cnt_;
  const int64_t total_lookup_cnt = func_lookup_ctdef->has_main_table_lookup() ? func_lookup_cnt + 1 : func_lookup_cnt;

  ObDASFuncDataIterParam fts_merge_iter_param;
  if (OB_UNLIKELY(0 == func_lookup_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, func lookup count is 0", K(ret));
  } else if (OB_ISNULL(buf = alloc.alloc(sizeof(ObDASIter *) * func_lookup_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate enough memory", K(sizeof(ObDASIter *) * func_lookup_cnt), K(ret));
  } else {
    data_table_iters = static_cast<ObDASIter **>(buf);
    for (int64_t i = 0; OB_SUCC(ret) && i < func_lookup_cnt; i++) {
      data_table_iters[i] = nullptr;
      ObDASFTSTabletID fts_tablet_ids;
      fts_tablet_ids.inv_idx_tablet_id_ = related_tablet_ids.fts_tablet_ids_[i].inv_idx_tablet_id_;
      fts_tablet_ids.fwd_idx_tablet_id_ = related_tablet_ids.fts_tablet_ids_[i].fwd_idx_tablet_id_;
      fts_tablet_ids.doc_id_idx_tablet_id_ = related_tablet_ids.fts_tablet_ids_[i].doc_id_idx_tablet_id_;
      if (OB_FAIL(create_functional_text_retrieval_sub_tree(scan_param.ls_id_,
                                                            alloc,
                                                            static_cast<const ObDASIRScanCtDef *>(func_lookup_ctdef->get_func_lookup_scan_ctdef(i)),
                                                            static_cast<ObDASIRScanRtDef *>(func_lookup_rtdef->get_func_lookup_scan_rtdef(i)),
                                                            fts_tablet_ids,
                                                            trans_desc,
                                                            snapshot,
                                                            data_table_iters[i]))) {
        LOG_WARN("failed to create text retrieval sub tree", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      fts_merge_iter_param.tr_merge_iters_ = data_table_iters;
      fts_merge_iter_param.iter_count_ = func_lookup_cnt;
      fts_merge_iter_param.trans_desc_ = trans_desc;
      fts_merge_iter_param.snapshot_ = snapshot;
      if (func_lookup_ctdef->has_main_table_lookup()) {
        ObDASScanIterParam main_table_param;
        const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef *>(func_lookup_ctdef->get_main_lookup_scan_ctdef());
        ObDASScanRtDef *rtdef = static_cast<ObDASScanRtDef *>(func_lookup_rtdef->get_main_lookup_scan_rtdef());
        if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, ctdef or rtdef is nullptr", K(ret), KPC(ctdef), KPC(rtdef));
        } else if (ObDASOpType::DAS_OP_TABLE_SCAN != ctdef->op_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted error, ctdef is not table scan", K(ret), K(ctdef->op_type_), K(ObDASOpType::DAS_OP_TABLE_SCAN));
        } else {
          main_table_param.scan_ctdef_ = ctdef;
          main_table_param.max_size_ = rtdef->eval_ctx_->is_vectorized() ? rtdef->eval_ctx_->max_batch_size_ : 1;
          main_table_param.eval_ctx_ = rtdef->eval_ctx_;
          main_table_param.exec_ctx_ = &rtdef->eval_ctx_->exec_ctx_;
          main_table_param.output_ = &ctdef->result_output_;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(create_das_iter(alloc, main_table_param, main_lookup_table_iter))) {
          LOG_WARN("failed to create data table lookup scan iter", K(ret));
        } else {
          if (lookup_keep_order) {
            rtdef->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
          }
          fts_merge_iter_param.main_lookup_ctdef_ = ctdef;
          fts_merge_iter_param.main_lookup_rtdef_ = rtdef;
          fts_merge_iter_param.main_lookup_iter_ = main_lookup_table_iter;
        }
      }
    }
  }

  // create fts merge iter
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_das_iter(alloc, fts_merge_iter_param, fts_merge_iter))) {
    LOG_WARN("failed to create fts merge iter", K(ret));
  } else if (OB_FAIL(create_iter_children_array(total_lookup_cnt, alloc, fts_merge_iter))) {
    LOG_WARN("failed to create iter children array", K(ret));
  } else {
    for (int64_t i = 0; i < func_lookup_cnt; ++i) {
      fts_merge_iter->get_children()[i] = data_table_iters[i];
    }
    if (func_lookup_ctdef->has_main_table_lookup()) {
      fts_merge_iter->get_children()[func_lookup_cnt] = main_lookup_table_iter;
      main_lookup_table_iter->set_scan_param(fts_merge_iter->get_main_lookup_scan_param());
    }
    fts_merge_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_); // for main_lookup
    fts_merge_iter->set_ls_id(ls_id);
  }

  // create function lookup iter
  if (OB_SUCC(ret)) {
    const ObDASBaseCtDef *rowkey_docid_ctdef = func_lookup_ctdef->get_doc_id_lookup_scan_ctdef();
    ObDASBaseRtDef *rowkey_docid_rtdef = func_lookup_rtdef->get_doc_id_lookup_scan_rtdef();

    ObDASScanIterParam rowkey_docid_param;
    const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef *>(rowkey_docid_ctdef);
    ObDASScanRtDef *rtdef = static_cast<ObDASScanRtDef*>(rowkey_docid_rtdef);
    rowkey_docid_param.scan_ctdef_ = ctdef;
    rowkey_docid_param.max_size_ = rtdef->eval_ctx_->is_vectorized() ? rtdef->eval_ctx_->max_batch_size_ : 1;
    rowkey_docid_param.eval_ctx_ = rtdef->eval_ctx_;
    rowkey_docid_param.exec_ctx_ = &rtdef->eval_ctx_->exec_ctx_;
    rowkey_docid_param.output_ = &ctdef->result_output_;
    if (OB_FAIL(create_das_iter(alloc, rowkey_docid_param, rowkey_docid_iter))) {
      LOG_WARN("failed to create data table lookup scan iter", K(ret));
    } else {
      ObDASFuncLookupIterParam func_lookup_param;
      func_lookup_param.max_size_ = func_lookup_rtdef->eval_ctx_->is_vectorized() ? func_lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
      func_lookup_param.eval_ctx_ = func_lookup_rtdef->eval_ctx_;
      func_lookup_param.exec_ctx_ = &func_lookup_rtdef->eval_ctx_->exec_ctx_;
      func_lookup_param.output_ = &func_lookup_ctdef->result_output_;
      func_lookup_param.default_batch_row_count_ = func_lookup_param.max_size_;
      func_lookup_param.index_ctdef_ = rowkey_docid_ctdef;
      func_lookup_param.index_rtdef_ = rowkey_docid_rtdef;
      func_lookup_param.lookup_ctdef_ = nullptr;
      func_lookup_param.lookup_rtdef_ = nullptr;
      func_lookup_param.index_table_iter_ = rowkey_docid_iter;
      func_lookup_param.data_table_iter_ = fts_merge_iter;
      func_lookup_param.trans_desc_ = trans_desc;
      func_lookup_param.snapshot_ = snapshot;
      func_lookup_param.doc_id_expr_ = func_lookup_ctdef->lookup_doc_id_expr_;
      if (lookup_keep_order) {
        static_cast<ObDASScanRtDef *>(func_lookup_param.index_rtdef_)->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
      }

      if (OB_FAIL(create_das_iter(alloc, func_lookup_param, func_lookup_iter))) {
        LOG_WARN("failed to create doc id lookup iter", K(ret));
      } else if (OB_FAIL(create_iter_children_array(2, alloc, func_lookup_iter))) {
        LOG_WARN("failed to create iter children array", K(ret));
      } else {
        func_lookup_iter->get_children()[0] = rowkey_docid_iter;
        func_lookup_iter->get_children()[1] = fts_merge_iter;
      }
    }
  }

  if (OB_SUCC(ret)) {
    fun_lookup_result = func_lookup_iter;
  }
  return ret;
}

/*                      local_lookup
 *                       |        |
 *                 sort_distinct  main_data_table
 *                     |
 *               aux_local_lookup
 *                 |        |
 *           index_table  docid_rowkey_table
*/
int ObDASIterUtils::create_mvi_lookup_tree(ObTableScanParam &scan_param,
                                           common::ObIAllocator &alloc,
                                           const ObDASBaseCtDef *attach_ctdef,
                                           ObDASBaseRtDef *attach_rtdef,
                                           const ObDASRelatedTabletID &related_tablet_ids,
                                           transaction::ObTxDesc *trans_desc,
                                           transaction::ObTxReadSnapshot *snapshot,
                                           ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;

  const ObDASSortCtDef *sort_ctdef = nullptr;
  ObDASSortRtDef *sort_rtdef = nullptr;
  const ObDASTableLookupCtDef *lookup_ctdef = nullptr;
  ObDASTableLookupRtDef *lookup_rtdef = nullptr;
  const ObDASIRAuxLookupCtDef *mvi_lookup_ctdef = nullptr;
  ObDASIRAuxLookupRtDef *mvi_lookup_rtdef = nullptr;

  ObDASScanIter *index_table_iter = nullptr;
  ObDASScanIter *docid_rowkey_table_iter = nullptr;
  ObDASMVILookupIter *mvi_lookup_iter = nullptr;
  ObDASIter *sort_iter = nullptr;

  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(attach_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table lookup param is nullptr", KP(attach_ctdef), KP(attach_rtdef));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_SORT, sort_ctdef, sort_rtdef))) {
    LOG_WARN("find sort def failed", K(ret));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_IR_AUX_LOOKUP, mvi_lookup_ctdef, mvi_lookup_rtdef))) {
    LOG_WARN("find ir aux lookup def failed", K(ret));
  } else if (mvi_lookup_ctdef->children_cnt_ != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find index def failed", K(ret), K(mvi_lookup_ctdef->children_cnt_));
  } else {
    const ObDASScanCtDef* index_ctdef = static_cast<const ObDASScanCtDef*>(mvi_lookup_ctdef->children_[0]);
    ObDASScanRtDef * index_rtdef = static_cast<ObDASScanRtDef *>(mvi_lookup_rtdef->children_[0]);
    const ObDASScanCtDef* docid_table_ctdef = mvi_lookup_ctdef->get_lookup_scan_ctdef();
    ObDASScanRtDef * docid_table_rtdef = mvi_lookup_rtdef->get_lookup_scan_rtdef();

    ObDASScanIterParam index_table_param;
    init_scan_iter_param(index_table_param, index_ctdef, index_rtdef);
    ObDASScanIterParam docid_rowkey_table_param;
    init_scan_iter_param(docid_rowkey_table_param, docid_table_ctdef, docid_table_rtdef);

    if (OB_FAIL(create_das_iter(alloc, index_table_param, index_table_iter))) {
      LOG_WARN("failed to create index table scan iter", K(ret));
    } else if (OB_FAIL(create_das_iter(alloc, docid_rowkey_table_param, docid_rowkey_table_iter))){
      LOG_WARN("failed to create docid rowkey table scan iter", K(ret));
    } else {
      ObDASLocalLookupIterParam mvi_lookup_param;
      mvi_lookup_param.max_size_ = 1;
      mvi_lookup_param.eval_ctx_ = mvi_lookup_rtdef->eval_ctx_;
      mvi_lookup_param.exec_ctx_ = &mvi_lookup_rtdef->eval_ctx_->exec_ctx_;
      mvi_lookup_param.output_ = &mvi_lookup_ctdef->result_output_;
      mvi_lookup_param.index_ctdef_ = index_ctdef;
      mvi_lookup_param.index_rtdef_ = index_rtdef;
      mvi_lookup_param.lookup_ctdef_ = docid_table_ctdef;
      mvi_lookup_param.lookup_rtdef_ = docid_table_rtdef;
      mvi_lookup_param.index_table_iter_ = index_table_iter;
      mvi_lookup_param.data_table_iter_ = docid_rowkey_table_iter;
      mvi_lookup_param.trans_desc_ = trans_desc;
      mvi_lookup_param.snapshot_ = snapshot;
      mvi_lookup_param.rowkey_exprs_ = &mvi_lookup_ctdef->get_lookup_scan_ctdef()->rowkey_exprs_;
      if (OB_FAIL(create_das_iter(alloc, mvi_lookup_param, mvi_lookup_iter))) {
        LOG_WARN("failed to create mvi lookup iter", K(ret));
      } else if (OB_FAIL(create_iter_children_array(2, alloc, mvi_lookup_iter))) {
        LOG_WARN("failed to create iter children array", K(ret));
      } else {
        mvi_lookup_iter->get_children()[0] = index_table_iter;
        mvi_lookup_iter->get_children()[1] = docid_rowkey_table_iter;
        index_table_iter->set_scan_param(scan_param);
        docid_rowkey_table_iter->set_scan_param(mvi_lookup_iter->get_lookup_param());
        mvi_lookup_iter->set_tablet_id(related_tablet_ids.aux_lookup_tablet_id_);
        mvi_lookup_iter->set_ls_id(scan_param.ls_id_);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_sort_sub_tree(alloc, sort_ctdef, sort_rtdef,false/*need_rewind*/,
                                          true/*need_distinct*/, mvi_lookup_iter, sort_iter))) {
    LOG_WARN("failed to create sort sub tree", K(ret));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_TABLE_LOOKUP, lookup_ctdef, lookup_rtdef))) {
    // multivalue index scan and don't need to index back lookup.
    ret = OB_SUCCESS;
    iter_tree = sort_iter;
  } else {
    ObDASScanIter *data_table_iter = nullptr;
    ObDASLocalLookupIter *local_lookup_iter = nullptr;

    const ObDASScanCtDef *data_table_ctdef = lookup_ctdef->get_lookup_scan_ctdef();
    ObDASScanRtDef *data_table_rtdef = lookup_rtdef->get_lookup_scan_rtdef();
    ObDASScanIterParam data_table_param;
    init_scan_iter_param(data_table_param, data_table_ctdef, data_table_rtdef);

    if (OB_FAIL(create_das_iter(alloc, data_table_param, data_table_iter))) {
      LOG_WARN("failed to create data table scan iter", K(ret));
    } else {
      ObDASIter *tmp_data_table_iter = static_cast<ObDASIter *>(data_table_iter);

      ObDASBaseCtDef *ctdef = lookup_ctdef->children_[1];
      ObDASBaseRtDef *rtdef = lookup_rtdef->children_[1];
      if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpeted error, ctdef or rtdef is nullptr", K(ret), KPC(ctdef), KPC(rtdef));
      } else if (ObDASOpType::DAS_OP_TABLE_SCAN == ctdef->op_type_) {
        // nothing to do
      } else if (OB_UNLIKELY(ObDASOpType::DAS_OP_VID_MERGE == ctdef->op_type_)) {
        if (OB_FAIL(create_vid_scan_sub_tree(scan_param, alloc, static_cast<const ObDASVIdMergeCtDef *>(ctdef),
              static_cast<ObDASVIdMergeRtDef *>(rtdef), related_tablet_ids, trans_desc, snapshot, tmp_data_table_iter))) {
          LOG_WARN("fail to create doc id scan sub tree", K(ret), K(scan_param), KPC(ctdef), KPC(rtdef));
        }
      } else if (OB_UNLIKELY(ObDASOpType::DAS_OP_DOC_ID_MERGE != ctdef->op_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, attach op type isn't doc id merge", K(ret), K(ctdef->op_type_), KPC(ctdef));
      } else if (OB_FAIL(create_doc_id_scan_sub_tree(scan_param, alloc, static_cast<const ObDASDocIdMergeCtDef *>(ctdef),
              static_cast<ObDASDocIdMergeRtDef *>(rtdef), related_tablet_ids, trans_desc, snapshot, tmp_data_table_iter))) {
        LOG_WARN("fail to create doc id scan sub tree", K(ret), K(scan_param), KPC(ctdef), KPC(rtdef));
      }

      if (OB_SUCC(ret)) {
        ObDASLocalLookupIterParam lookup_param;
        lookup_param.max_size_ = lookup_rtdef->eval_ctx_->is_vectorized() ? data_table_rtdef->eval_ctx_->max_batch_size_ : 1;
        lookup_param.eval_ctx_ = lookup_rtdef->eval_ctx_;
        lookup_param.exec_ctx_ = &lookup_rtdef->eval_ctx_->exec_ctx_;
        lookup_param.output_ = &lookup_ctdef->result_output_;
        lookup_param.index_ctdef_ = sort_ctdef;
        lookup_param.index_rtdef_ = sort_rtdef;
        lookup_param.lookup_ctdef_ = data_table_ctdef;
        lookup_param.lookup_rtdef_ = data_table_rtdef;
        lookup_param.index_table_iter_ = sort_iter;
        lookup_param.data_table_iter_ = tmp_data_table_iter;
        lookup_param.trans_desc_ = trans_desc;
        lookup_param.snapshot_ = snapshot;
        lookup_param.rowkey_exprs_ = &lookup_ctdef->get_lookup_scan_ctdef()->rowkey_exprs_;

        if (OB_FAIL(create_das_iter(alloc, lookup_param, local_lookup_iter))) {
          LOG_WARN("failed to create mvi lookup iter", K(ret));
        } else if (OB_FAIL(create_iter_children_array(2, alloc, local_lookup_iter))) {
          LOG_WARN("failed to create iter children array", K(ret));
        } else {
          local_lookup_iter->get_children()[0] = sort_iter;
          local_lookup_iter->get_children()[1] = tmp_data_table_iter;
          data_table_iter->set_scan_param(local_lookup_iter->get_lookup_param());
          local_lookup_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
          local_lookup_iter->set_ls_id(scan_param.ls_id_);
          iter_tree = local_lookup_iter;
        }
      }
    }
  }

  return ret;
}

int ObDASIterUtils::create_gis_lookup_tree(ObTableScanParam &scan_param,
                                           common::ObIAllocator &alloc,
                                           const ObDASBaseCtDef *attach_ctdef,
                                           ObDASBaseRtDef *attach_rtdef,
                                           const ObDASRelatedTabletID &related_tablet_ids,
                                           transaction::ObTxDesc *trans_desc,
                                           transaction::ObTxReadSnapshot *snapshot,
                                           ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;

  const ObDASTableLookupCtDef *lookup_ctdef = nullptr;
  ObDASTableLookupRtDef *lookup_rtdef = nullptr;
  const ObDASSortCtDef *sort_ctdef = nullptr;
  ObDASSortRtDef *sort_rtdef = nullptr;

  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(attach_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table lookup param is nullptr", KP(attach_ctdef), KP(attach_rtdef));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef,
                                                     attach_rtdef,
                                                     DAS_OP_TABLE_LOOKUP,
                                                     lookup_ctdef,
                                                     lookup_rtdef))) {
    LOG_WARN("find data table lookup def failed", K(ret));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef,
                                                    attach_rtdef,
                                                    DAS_OP_SORT,
                                                    sort_ctdef,
                                                    sort_rtdef))) {
    LOG_WARN("find sort def failed", K(ret));
  } else {
    ObDASScanIter *data_table_iter = nullptr;
    ObDASSpatialScanIter *index_table_iter = nullptr;
    ObDASIter *sort_iter = nullptr;  // ObDASSortDistinctIter
    ObDASLocalLookupIter *local_lookup_iter = nullptr;

    const ObDASScanCtDef *data_table_ctdef = lookup_ctdef->get_lookup_scan_ctdef();
    ObDASScanRtDef *data_table_rtdef = lookup_rtdef->get_lookup_scan_rtdef();
    const ObDASScanCtDef* index_ctdef = static_cast<const ObDASScanCtDef*>(sort_ctdef->children_[0]);
    ObDASScanRtDef * index_rtdef = static_cast<ObDASScanRtDef *>(sort_rtdef->children_[0]);

    ObDASSpatialScanIterParam index_table_param;
    init_spatial_scan_iter_param(index_table_param, index_ctdef, index_rtdef);
    ObDASScanIterParam data_table_param;
    init_scan_iter_param(data_table_param, data_table_ctdef, data_table_rtdef);

    if (OB_FAIL(create_das_spatial_scan_iter(alloc, index_table_param, index_table_iter))) {
      LOG_WARN("failed to create index table scan iter", K(ret));
    } else if (OB_FALSE_IT(index_table_iter->set_scan_param(scan_param))) {
    } else if (OB_FAIL(create_das_iter(alloc, data_table_param, data_table_iter))) {
      LOG_WARN("failed to create data table scan iter", K(ret));
    }

    if (OB_SUCC(ret) && OB_FAIL(create_sort_sub_tree(alloc,
                                                     sort_ctdef,
                                                     sort_rtdef,
                                                     false/*need_rewind*/,
                                                     true/*need_distinct*/,
                                                     index_table_iter,
                                                     sort_iter))) {
      LOG_WARN("failed to create sort sub tree", K(ret));
    }

    if (OB_SUCC(ret)) {
      ObDASLocalLookupIterParam lookup_param;
      lookup_param.max_size_ = lookup_rtdef->eval_ctx_->is_vectorized() ? data_table_rtdef->eval_ctx_->max_batch_size_ : 1;
      lookup_param.eval_ctx_ = lookup_rtdef->eval_ctx_;
      lookup_param.exec_ctx_ = &lookup_rtdef->eval_ctx_->exec_ctx_;
      lookup_param.output_ = &lookup_ctdef->result_output_;
      lookup_param.index_ctdef_ = sort_ctdef;
      lookup_param.index_rtdef_ = sort_rtdef;
      lookup_param.lookup_ctdef_ = data_table_ctdef;
      lookup_param.lookup_rtdef_ = data_table_rtdef;
      lookup_param.index_table_iter_ = sort_iter;
      lookup_param.data_table_iter_ = data_table_iter;
      lookup_param.trans_desc_ = trans_desc;
      lookup_param.snapshot_ = snapshot;
      lookup_param.rowkey_exprs_ = &lookup_ctdef->get_lookup_scan_ctdef()->rowkey_exprs_;
      if (OB_FAIL(create_das_iter(alloc, lookup_param, local_lookup_iter))) {
        LOG_WARN("failed to create mvi lookup iter", K(ret));
      } else if (OB_FAIL(create_iter_children_array(2, alloc, local_lookup_iter))) {
        LOG_WARN("failed to create iter children array", K(ret));
      } else {
        local_lookup_iter->get_children()[0] = sort_iter;
        local_lookup_iter->get_children()[1] = data_table_iter;
        data_table_iter->set_scan_param(local_lookup_iter->get_lookup_param());
        local_lookup_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
        local_lookup_iter->set_ls_id(scan_param.ls_id_);
      }
    }

    if (OB_SUCC(ret)) {
      iter_tree = local_lookup_iter;
    }
  }

  return ret;
}

int ObDASIterUtils::create_sort_sub_tree(common::ObIAllocator &alloc,
                                         const ObDASSortCtDef *sort_ctdef,
                                         ObDASSortRtDef *sort_rtdef,
                                         const bool need_rewind,
                                         const bool need_distinct,
                                         ObDASIter *sort_input,
                                         ObDASIter *&sort_result)
{
  int ret = OB_SUCCESS;
  ObDASSortIterParam sort_iter_param;
  ObDASSortIter *sort_iter = nullptr;
  ObEvalCtx *eval_ctx = sort_rtdef->eval_ctx_;
  sort_iter_param.max_size_ = eval_ctx->is_vectorized() ? eval_ctx->max_batch_size_ : 1;
  sort_iter_param.eval_ctx_ = eval_ctx;
  sort_iter_param.exec_ctx_ = &eval_ctx->exec_ctx_;
  sort_iter_param.output_ = &sort_ctdef->result_output_;
  sort_iter_param.sort_ctdef_ = sort_ctdef;
  sort_iter_param.child_ = sort_input;
  sort_iter_param.need_rewind_ = need_rewind;
  sort_iter_param.need_distinct_ = need_distinct;
  if (OB_FAIL(create_das_iter(alloc, sort_iter_param, sort_iter))) {
    LOG_WARN("failed to create sort iter", K(ret));
  } else if (OB_FAIL(create_iter_children_array(1, alloc, sort_iter))) {
    LOG_WARN("failed to create iter children array", K(ret));
  } else {
    sort_iter->get_children()[0] = sort_input;
    sort_result = sort_iter;
  }
  return ret;
}

int ObDASIterUtils::create_table_scan_iter_tree(const ObTableScanCtDef &tsc_ctdef,
                                                ObEvalCtx &eval_ctx,
                                                ObExecContext &exec_ctx,
                                                ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                                const ObTableScanSpec &spec,
                                                ObDASMergeIter *&scan_iter,
                                                ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  ObDASMergeIter *iter = nullptr;
  ObDASMergeIterParam param;
  const ObDASScanCtDef *scan_ctdef = &tsc_ctdef.scan_ctdef_;
  param.max_size_ = eval_ctx.is_vectorized() ? eval_ctx.max_batch_size_ : 1;
  param.eval_ctx_ = &eval_ctx;
  param.exec_ctx_ = &exec_ctx;
  param.output_ = &tsc_ctdef.get_das_output_exprs();
  param.group_id_expr_ = scan_ctdef->group_id_expr_;
  param.eval_infos_ = &eval_infos;
  param.need_update_partition_id_ = true;
  param.pdml_partition_id_ = spec.pdml_partition_id_;
  param.partition_id_calc_type_ = spec.partition_id_calc_type_;
  param.should_scan_index_ = spec.should_scan_index();
  param.ref_table_id_ = spec.ref_table_id_;
  param.is_vectorized_ = spec.is_vectorized();
  param.frame_info_ = &spec.plan_->get_expr_frame_info();
  param.execute_das_directly_ = !spec.use_dist_das_;
  param.enable_rich_format_ = spec.use_rich_format_;
  param.used_for_keep_order_ = false;

  if (OB_FAIL(create_das_iter(exec_ctx.get_allocator(), param, iter))) {
    LOG_WARN("failed to create das merge iter", K(ret));
  } else {
    scan_iter = iter;
    iter_tree = iter;
  }

  return ret;
}


int ObDASIterUtils::create_global_lookup_iter_tree(const ObTableScanCtDef &tsc_ctdef,
                                                   ObTableScanRtDef &tsc_rtdef,
                                                   ObEvalCtx &eval_ctx,
                                                   ObExecContext &exec_ctx,
                                                   ObFixedArray<ObEvalInfo*, ObIAllocator> &eval_infos,
                                                   const ObTableScanSpec &spec,
                                                   bool can_retry,
                                                   ObDASMergeIter *&scan_iter,
                                                   ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  ObDASMergeIter *index_table_iter = nullptr;
  ObDASMergeIter *data_table_iter = nullptr;
  ObDASGlobalLookupIter *lookup_iter = nullptr;

  /********* create index table iter *********/
  const ObDASScanCtDef *scan_ctdef = &tsc_ctdef.scan_ctdef_;
  const ObDASScanCtDef *lookup_ctdef = tsc_ctdef.lookup_ctdef_;
  ObDASMergeIterParam param;
  param.max_size_ = eval_ctx.is_vectorized() ? eval_ctx.max_batch_size_ : 1;
  param.eval_ctx_ = &eval_ctx;
  param.exec_ctx_ = &exec_ctx;
  param.output_ = &scan_ctdef->result_output_;
  param.group_id_expr_ = tsc_ctdef.scan_ctdef_.group_id_expr_;
  param.eval_infos_ = &eval_infos;
  param.need_update_partition_id_ = true;
  param.pdml_partition_id_ = spec.pdml_partition_id_;
  param.partition_id_calc_type_ = spec.partition_id_calc_type_;
  param.should_scan_index_ = spec.should_scan_index();
  param.ref_table_id_ = scan_ctdef->ref_table_id_;
  param.is_vectorized_ = spec.is_vectorized();
  param.frame_info_ = &spec.plan_->get_expr_frame_info();
  param.execute_das_directly_ = !spec.use_dist_das_;
  param.enable_rich_format_ = spec.use_rich_format_;
  param.used_for_keep_order_ = false;

  if (OB_FAIL(create_das_iter(exec_ctx.get_allocator(), param, index_table_iter))) {
    LOG_WARN("failed to create global index table iter", K(ret));
  }

  /********* create data table iter *********/
  if (OB_SUCC(ret)) {
    param.output_ = &lookup_ctdef->result_output_;
    param.ref_table_id_ = lookup_ctdef->ref_table_id_;
    param.need_update_partition_id_ = false;
    param.execute_das_directly_ = false;
    param.enable_rich_format_ = false;
    param.used_for_keep_order_ = tsc_ctdef.is_das_keep_order_;
    if (OB_FAIL(create_das_iter(exec_ctx.get_allocator(), param, data_table_iter))) {
      LOG_WARN("failed to create global data table iter", K(ret));
    } else {
      index_table_iter->set_global_lookup_iter(data_table_iter);
    }
  }

  /********* create global lookup iter *********/
  if (OB_SUCC(ret)) {
    ObDASGlobalLookupIterParam lookup_param;
    lookup_param.assgin(param);
    lookup_param.type_ = DAS_ITER_GLOBAL_LOOKUP;
    lookup_param.index_ctdef_ = scan_ctdef;
    lookup_param.index_rtdef_ = &tsc_rtdef.scan_rtdef_;
    lookup_param.lookup_ctdef_ = lookup_ctdef;
    lookup_param.lookup_rtdef_ = tsc_rtdef.lookup_rtdef_;
    lookup_param.rowkey_exprs_ = !lookup_ctdef->rowkey_exprs_.empty() ? &lookup_ctdef->rowkey_exprs_
                                                                      : &tsc_ctdef.global_index_rowkey_exprs_;
    lookup_param.index_table_iter_ = index_table_iter;
    lookup_param.data_table_iter_ = data_table_iter;
    lookup_param.can_retry_ = can_retry;
    lookup_param.calc_part_id_ = tsc_ctdef.calc_part_id_expr_;
    lookup_param.attach_ctdef_ = tsc_ctdef.attach_spec_.attach_ctdef_;
    lookup_param.attach_rtinfo_ = tsc_rtdef.attach_rtinfo_;

    if (OB_FAIL(create_das_iter(exec_ctx.get_allocator(), lookup_param, lookup_iter))) {
      LOG_WARN("failed to create global lookup iter", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObDASIter **&children = lookup_iter->get_children();
    if (OB_ISNULL(children = OB_NEW_ARRAY(ObDASIter*, &exec_ctx.get_allocator(), 2))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc for das iter children", K(ret));
    } else {
      lookup_iter->set_children_cnt(2);
      children[0] = index_table_iter;
      children[1] = data_table_iter;
    }
  }

  if (OB_SUCC(ret)) {
    scan_iter = index_table_iter;
    iter_tree = lookup_iter;
  }

  return ret;
}

int ObDASIterUtils::create_index_merge_iter_tree(ObTableScanParam &scan_param,
                                                 common::ObIAllocator &alloc,
                                                 const ObDASBaseCtDef *attach_ctdef,
                                                 ObDASBaseRtDef *attach_rtdef,
                                                 const ObDASRelatedTabletID &related_tablet_ids,
                                                 transaction::ObTxDesc *tx_desc,
                                                 transaction::ObTxReadSnapshot *snapshot,
                                                 ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(attach_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    const bool need_lookup = attach_ctdef->op_type_ == DAS_OP_TABLE_LOOKUP;
    const ObDASBaseCtDef *index_merge_ctdef = need_lookup ? attach_ctdef->children_[0] : attach_ctdef;
    ObDASBaseRtDef *index_merge_rtdef = need_lookup ? attach_rtdef->children_[0] : attach_rtdef;
    ObDASIter *index_merge_root = nullptr;
    if (OB_FAIL(create_index_merge_sub_tree(scan_param.ls_id_,
                                            alloc,
                                            index_merge_ctdef,
                                            index_merge_rtdef,
                                            related_tablet_ids,
                                            tx_desc,
                                            snapshot,
                                            index_merge_root))) {
      LOG_WARN("failed to create index merge iter tree", K(ret));
    } else if (need_lookup) {
      ObDASLocalLookupIter *lookup_iter = nullptr;
      ObDASScanIter *data_table_iter = nullptr;
      const ObDASScanCtDef *lookup_ctdef = static_cast<const ObDASScanCtDef*>(attach_ctdef->children_[1]);
      ObDASScanRtDef *lookup_rtdef = static_cast<ObDASScanRtDef*>(attach_rtdef->children_[1]);
      ObDASScanIterParam data_table_param;
      init_scan_iter_param(data_table_param, lookup_ctdef, lookup_rtdef);

      if (OB_FAIL(create_das_iter(alloc, data_table_param, data_table_iter))) {
        LOG_WARN("failed to create data table iter", K(ret));
      } else {
        ObDASLocalLookupIterParam lookup_param;
        lookup_param.max_size_ = lookup_rtdef->eval_ctx_->is_vectorized() ? lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
        lookup_param.eval_ctx_ = lookup_rtdef->eval_ctx_;
        lookup_param.exec_ctx_ = &lookup_rtdef->eval_ctx_->exec_ctx_;
        lookup_param.output_ = &lookup_ctdef->result_output_;
        lookup_param.index_ctdef_ = index_merge_ctdef;
        lookup_param.index_rtdef_ = index_merge_rtdef;
        lookup_param.lookup_ctdef_ = lookup_ctdef;
        lookup_param.lookup_rtdef_ = lookup_rtdef;
        lookup_param.index_table_iter_ = index_merge_root;
        lookup_param.data_table_iter_ = data_table_iter;
        lookup_param.trans_desc_ = tx_desc;
        lookup_param.snapshot_ = snapshot;
        lookup_param.rowkey_exprs_ = &lookup_ctdef->rowkey_exprs_;
        if (OB_FAIL(create_das_iter(alloc, lookup_param, lookup_iter))) {
          LOG_WARN("failed to create local lookup iter", K(ret));
        } else if (OB_FAIL(create_iter_children_array(2, alloc, lookup_iter))) {
          LOG_WARN("failed to create iter children array", K(ret));
        } else {
          lookup_iter->get_children()[0] = index_merge_root;
          lookup_iter->get_children()[1] = data_table_iter;
          data_table_iter->set_scan_param(lookup_iter->get_lookup_param());
          lookup_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
          lookup_iter->set_ls_id(scan_param.ls_id_);
          iter_tree = lookup_iter;
        }
      }
    } else {
      iter_tree = index_merge_root;
    }
  }

  return ret;
}

int ObDASIterUtils::create_index_merge_sub_tree(const ObLSID &ls_id,
                                                common::ObIAllocator &alloc,
                                                const ObDASBaseCtDef *ctdef,
                                                ObDASBaseRtDef *rtdef,
                                                const ObDASRelatedTabletID &related_tablet_ids,
                                                transaction::ObTxDesc *tx_desc,
                                                transaction::ObTxReadSnapshot *snapshot,
                                                ObDASIter *&iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ctdef), K(rtdef));
  } else if (ctdef->op_type_ == DAS_OP_TABLE_SCAN) {
    ObDASScanIterParam scan_param;
    init_scan_iter_param(scan_param, static_cast<const ObDASScanCtDef*>(ctdef), rtdef);

    ObDASScanIter *scan_iter = nullptr;
    if (OB_FAIL(create_das_iter(alloc, scan_param, scan_iter))) {
      LOG_WARN("failed to create das scan iter", K(ret));
    } else {
      iter = scan_iter;
    }
  } else if (ctdef->op_type_ == DAS_OP_SORT) {
    const ObDASSortCtDef *sort_ctdef = static_cast<const ObDASSortCtDef*>(ctdef);
    ObDASSortRtDef *sort_rtdef = static_cast<ObDASSortRtDef*>(rtdef);
    OB_ASSERT(ctdef->children_ != nullptr &&
              ctdef->children_cnt_ == 1 &&
              ctdef->children_[0]->op_type_ == DAS_OP_TABLE_SCAN);
    const ObDASScanCtDef *scan_ctdef = static_cast<ObDASScanCtDef*>(ctdef->children_[0]);
    ObDASScanIterParam child_scan_param;
    init_scan_iter_param(child_scan_param, scan_ctdef, rtdef);

    ObDASScanIter *child_scan_iter = nullptr;
    ObDASIter *sort_iter = nullptr;
    if (OB_FAIL(create_das_iter(alloc, child_scan_param, child_scan_iter))) {
      LOG_WARN("failed to create das scan iter", K(ret));
    } else if (OB_FAIL(create_sort_sub_tree(alloc, sort_ctdef, sort_rtdef, false/*need_rewind*/, false/*need_distinct*/, child_scan_iter, sort_iter))) {
      LOG_WARN("failed to create das sort iter", K(ret));
    } else {
      iter = sort_iter;
    }
  } else if (ctdef->op_type_ == DAS_OP_INDEX_MERGE) {
    const ObDASIndexMergeCtDef *merge_ctdef = static_cast<const ObDASIndexMergeCtDef*>(ctdef);
    ObDASIndexMergeRtDef *merge_rtdef = static_cast<ObDASIndexMergeRtDef*>(rtdef);
    const ObDASBaseCtDef *left_ctdef = merge_ctdef->children_[0];
    const ObDASBaseCtDef *right_ctdef = merge_ctdef->children_[1];
    const ObDASScanCtDef *right_scan_ctdef = nullptr;
    OB_ASSERT(right_ctdef->op_type_ == DAS_OP_TABLE_SCAN || right_ctdef->op_type_ == DAS_OP_SORT);
    if (right_ctdef->op_type_ == DAS_OP_TABLE_SCAN) {
      right_scan_ctdef = static_cast<const ObDASScanCtDef*>(right_ctdef);
    } else {
      OB_ASSERT(right_ctdef->children_ != nullptr &&
                right_ctdef->children_cnt_ == 1 &&
                right_ctdef->children_[0]->op_type_ == DAS_OP_TABLE_SCAN);
      right_scan_ctdef = static_cast<const ObDASScanCtDef*>(right_ctdef->children_[0]);
    }
    OB_ASSERT(right_scan_ctdef != nullptr);
    ObDASIter *left_iter = nullptr;
    ObDASIter *right_iter = nullptr;
    if (OB_FAIL(create_index_merge_sub_tree(ls_id,
                                            alloc,
                                            merge_ctdef->children_[0],
                                            merge_rtdef->children_[0],
                                            related_tablet_ids,
                                            tx_desc,
                                            snapshot,
                                            left_iter))) {
      LOG_WARN("failed to create index merge sub tree", K(ret));
    } else if (OB_FAIL(create_index_merge_sub_tree(ls_id,
                                                   alloc,
                                                   merge_ctdef->children_[1],
                                                   merge_rtdef->children_[1],
                                                   related_tablet_ids,
                                                   tx_desc,
                                                   snapshot,
                                                   right_iter))) {
      LOG_WARN("failed to create index merge sub tree", K(ret));
    } else {
      ObDASIndexMergeIterParam merge_param;
      ObDASIndexMergeIter *merge_iter = nullptr;
      merge_param.max_size_ = merge_rtdef->eval_ctx_->is_vectorized() ?
          merge_rtdef->eval_ctx_->max_batch_size_ : 1;
      merge_param.eval_ctx_ = merge_rtdef->eval_ctx_;
      merge_param.exec_ctx_ = &merge_rtdef->eval_ctx_->exec_ctx_;
      merge_param.output_ = &right_scan_ctdef->rowkey_exprs_;
      merge_param.merge_type_ = merge_ctdef->merge_type_;
      merge_param.rowkey_exprs_ = &right_scan_ctdef->rowkey_exprs_;
      merge_param.left_iter_ = left_iter;
      merge_param.left_output_ = left_iter->get_output();
      merge_param.right_iter_ = right_iter;
      merge_param.right_output_ = right_iter->get_output();
      merge_param.ctdef_ = merge_ctdef;
      merge_param.rtdef_ = merge_rtdef;
      merge_param.tx_desc_ = tx_desc;
      merge_param.snapshot_ = snapshot;
      merge_param.is_reverse_ = merge_ctdef->is_reverse_;
      merge_param.is_left_child_leaf_node_ = (left_iter->get_type() != DAS_ITER_INDEX_MERGE);
      if (OB_FAIL(create_das_iter(alloc, merge_param, merge_iter))) {
        LOG_WARN("failed to create index merge iter", K(merge_param));
      } else if (OB_FAIL(merge_iter->set_ls_tablet_ids(ls_id, related_tablet_ids))) {
        LOG_WARN("failed to set ls tablet ids", K(ret));
      } else if (OB_FAIL(create_iter_children_array(2, alloc, merge_iter))) {
        LOG_WARN("failed to create iter children array", K(ret));
      } else {
        merge_iter->get_children()[0] = left_iter;
        merge_iter->get_children()[1] = right_iter;
        if (left_iter->get_type() != DAS_ITER_INDEX_MERGE) {
          ObDASScanIter *left_scan_iter = (left_iter->get_type() == DAS_ITER_SCAN) ?
              static_cast<ObDASScanIter*>(left_iter) : static_cast<ObDASScanIter*>(left_iter->get_children()[0]);
          OB_ASSERT(left_scan_iter != nullptr);
          left_scan_iter->set_scan_param(merge_iter->get_left_param());
        }
        OB_ASSERT(right_iter->get_type() == DAS_ITER_SCAN || right_iter->get_type() == DAS_ITER_SORT);
        ObDASScanIter *right_scan_iter = (right_iter->get_type() == DAS_ITER_SCAN) ?
            static_cast<ObDASScanIter*>(right_iter) : static_cast<ObDASScanIter*>(right_iter->get_children()[0]);
        OB_ASSERT(right_scan_iter != nullptr);
        right_scan_iter->set_scan_param(merge_iter->get_right_param());
        iter = merge_iter;
      }
    }
  }

  return ret;
}

int ObDASIterUtils::create_iter_children_array(const int64_t children_cnt,
                                               common::ObIAllocator &alloc,
                                               ObDASIter *iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(children_cnt <= 0) || OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid children array args", K(ret), K(children_cnt), KP(iter));
  } else if (OB_NOT_NULL(iter->get_children())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected das iter already has an children array", K(ret), KPC(iter));
  } else if (OB_ISNULL(iter->get_children() = OB_NEW_ARRAY(ObDASIter *, &alloc, children_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc das iter children array", K(ret), K(children_cnt));
  } else {
    iter->set_children_cnt(children_cnt);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
