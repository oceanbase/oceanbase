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
#include "sql/das/iter/ob_das_text_retrieval_eval_node.h"
#include "sql/das/iter/ob_das_hnsw_scan_iter.h"
#include "sql/das/iter/ob_das_ivf_scan_iter.h"

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

int ObDASIterUtils::create_das_ivf_scan_iter(
  ObVectorIndexAlgorithmType type,
  common::ObIAllocator &alloc,
  ObDASIvfScanIterParam &param,
  ObDASIvfScanIter *&result)
{
  int ret = OB_SUCCESS;
  ObDASIvfScanIter *iter = nullptr;
  switch (type) {
    case ObVectorIndexAlgorithmType::VIAT_IVF_FLAT: {
      iter = OB_NEWx(ObDASIvfScanIter, &alloc);
      break;
    }
    case ObVectorIndexAlgorithmType::VIAT_IVF_PQ: {
      iter = OB_NEWx(ObDASIvfPQScanIter, &alloc);
      break;
    }
    case ObVectorIndexAlgorithmType::VIAT_IVF_SQ8: {
      iter = OB_NEWx(ObDASIvfSQ8ScanIter, &alloc);
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported vector index algorithm type", K(type), K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(iter)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to new a das iter", K(ret));
  } else if (OB_FAIL(iter->init(param))) {
    LOG_WARN("failed to init das iter", K(param), K(ret));
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
    case ITER_TREE_VEC_LOOKUP: {
      ret = create_vec_lookup_tree(scan_param, alloc, attach_ctdef, attach_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
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

bool ObDASIterUtils::is_vec_ivf_scan(const ObDASBaseCtDef *attach_ctdef, ObDASBaseRtDef *attach_rtdef)
{
  int ret = OB_SUCCESS;

  int bret = false;
  if (attach_ctdef != nullptr) {
    const ObDASVecAuxScanCtDef *vec_aux_ctdef = nullptr;
    ObDASVecAuxScanRtDef *vec_aux_rtdef = nullptr;

    if (OB_FAIL(ObDASUtils::find_target_das_def(
            attach_ctdef, attach_rtdef, DAS_OP_VEC_SCAN, vec_aux_ctdef, vec_aux_rtdef))) {
      LOG_WARN("find ir scan definition failed", K(ret));
    } else if (vec_aux_ctdef->algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_IVF_FLAT ||
               vec_aux_ctdef->algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_IVF_SQ8 ||
               vec_aux_ctdef->algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_IVF_PQ) {
      bret = true;
    }
  }

  return bret;
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
                                                   ObDASBaseRtDef *attach_rtdef,
                                                  const ObDASRelatedTabletID &related_tablet_ids,
                                                  const ObLSID &ls_id,
                                                  ObDASIter *root_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(attach_rtdef) || OB_ISNULL(root_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(attach_ctdef), KP(attach_rtdef), KP(root_iter));
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
    case ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_LOCAL_LOOKUP)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
      } else {
        ObDASCacheLookupIter *lookup_iter = static_cast<ObDASCacheLookupIter *>(root_iter);
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
        aux_lookup_iter->set_tablet_id(related_tablet_ids.doc_rowkey_tablet_id_);
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
        int64_t fts_idx = static_cast<ObDASIRScanRtDef *>(attach_rtdef)->fts_idx_;
        if (OB_FAIL(tr_merge_iter->set_related_tablet_ids(ls_id, related_tablet_ids.fts_tablet_ids_.at(fts_idx)))) {
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
            attach_rtdef->children_[i],
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
                                                ObDASBaseRtDef *attach_rtdef,
                                                const ObDASRelatedTabletID &related_tablet_ids,
                                                const ObLSID &ls_id,
                                                ObDASIter *root_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(attach_rtdef) || OB_ISNULL(root_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(attach_ctdef), KP(attach_rtdef), KP(root_iter));
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
      case ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP: {
        if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_LOCAL_LOOKUP)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("iter type not match with ctdef", K(ret), K(attach_ctdef->op_type_), K(iter_type));
        } else {
          ObDASCacheLookupIter *lookup_iter = static_cast<ObDASCacheLookupIter *>(root_iter);
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
          aux_lookup_iter->set_tablet_id(related_tablet_ids.doc_rowkey_tablet_id_);
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
          int64_t fts_index_idx = static_cast<ObDASIRScanRtDef*>(attach_rtdef)->fts_idx_;
          if (OB_FAIL(tr_merge_iter->set_related_tablet_ids(ls_id, related_tablet_ids.fts_tablet_ids_.at(fts_index_idx)))) {
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

    if (OB_FAIL(ret)) {
    } else if (!need_set_child) {
    } else if (OB_UNLIKELY(attach_ctdef->children_cnt_ != root_iter->get_children_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter children count not equal to ctdef children count",
          K(attach_ctdef->children_cnt_), K(root_iter->get_children_cnt()), K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < attach_ctdef->children_cnt_; ++i) {
        if (OB_FAIL(set_index_merge_related_ids(attach_ctdef->children_[i],
                                                attach_rtdef->children_[i],
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
                                                     ObDASBaseRtDef *attach_rtdef,
                                                     const ObDASRelatedTabletID &related_tablet_ids,
                                                     const ObLSID &ls_id,
                                                     ObDASIter *root_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(attach_rtdef) || OB_ISNULL(root_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(attach_ctdef), KP(attach_rtdef), KP(root_iter));
  } else {
    const ObDASIterType &iter_type = root_iter->get_type();
    bool need_set_child = false;
    switch (attach_ctdef->op_type_) {
    case ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP: {
      if (OB_UNLIKELY(iter_type != ObDASIterType::DAS_ITER_LOCAL_LOOKUP)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("iter type not match", K(ret), K(iter_type));
      } else {
        ObDASCacheLookupIter *local_lookup_iter = static_cast<ObDASCacheLookupIter *>(root_iter);
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
        ObDASFuncLookupRtDef *func_lookup_rtdef = static_cast<ObDASFuncLookupRtDef *>(attach_rtdef);
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
              func_lookup_rtdef->get_func_lookup_scan_rtdef(i),
              related_tablet_ids,
              ls_id,
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
        aux_lookup_iter->set_tablet_id(related_tablet_ids.doc_rowkey_tablet_id_);
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
        int64_t fts_index_idx = static_cast<ObDASIRScanRtDef*>(attach_rtdef)->fts_idx_;
        if (OB_FAIL(tr_merge_iter->set_related_tablet_ids(ls_id, related_tablet_ids.fts_tablet_ids_.at(fts_index_idx)))) {
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

    if (OB_FAIL(ret) || !need_set_child) {
    } else if (OB_UNLIKELY(attach_ctdef->children_cnt_ != root_iter->get_children_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter children count not equal to ctdef children count",
          K(ret), K(attach_ctdef->children_cnt_), K(root_iter->get_children_cnt()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < attach_ctdef->children_cnt_; ++i) {
        if (OB_FAIL(set_func_lookup_iter_related_ids(
            attach_ctdef->children_[i],
            attach_rtdef->children_[i],
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

int ObDASIterUtils::set_vec_lookup_related_ids(const ObDASBaseCtDef *attach_ctdef,
                                               ObDASBaseRtDef *attach_rtdef,
                                               const ObDASRelatedTabletID &related_tablet_ids,
                                               const ObLSID &ls_id,
                                               ObDASIter *root_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(root_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(attach_ctdef), KP(root_iter));
  } else {
    const ObDASVecAuxScanCtDef *vec_aux_ctdef = nullptr;
    ObDASVecAuxScanRtDef *vec_aux_rtdef = nullptr;
    bool is_shortcut_scan = false;
    if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_VEC_SCAN, vec_aux_ctdef, vec_aux_rtdef))) {
      LOG_WARN("find ir scan definition failed", K(ret));
    } else if (FALSE_IT(is_shortcut_scan = vec_aux_ctdef->access_pk_)) {
    } else if (ObDASIterType::DAS_ITER_SORT == root_iter->get_type()) {
      ObDASIter *iter = root_iter->get_children()[0];
      if (nullptr != iter && ObDASIterType::DAS_ITER_LOCAL_LOOKUP == iter->get_type()) {
        ObDASLocalLookupIter *local_lookup_iter = static_cast<ObDASLocalLookupIter *>(iter);
        local_lookup_iter->set_ls_id(ls_id);
        if (is_shortcut_scan) {
          local_lookup_iter->set_tablet_id(related_tablet_ids.doc_rowkey_tablet_id_);
        } else {
          local_lookup_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
        }
      }
    } else if (is_shortcut_scan) {
      ObDASLocalLookupIter *aux_lookup_iter = static_cast<ObDASLocalLookupIter *>(root_iter);
      aux_lookup_iter->set_ls_id(ls_id);
      aux_lookup_iter->set_tablet_id(related_tablet_ids.doc_rowkey_tablet_id_);

      if (ObDASIterType::DAS_ITER_HNSW_SCAN == aux_lookup_iter->get_children()[0]->get_type()) {
        ObDASHNSWScanIter *hnsw_scan_iter = static_cast<ObDASHNSWScanIter *>(aux_lookup_iter->get_children()[0]);
        hnsw_scan_iter->set_ls_id(ls_id);
        hnsw_scan_iter->set_related_tablet_ids(related_tablet_ids);
        if (ObDASIterType::DAS_ITER_LOCAL_LOOKUP == hnsw_scan_iter->get_inv_idx_scan_iter()->get_type()) {
          ObDASLocalLookupIter *inv_idx_scan_iter = static_cast<ObDASLocalLookupIter *>(hnsw_scan_iter->get_inv_idx_scan_iter());
          inv_idx_scan_iter->set_ls_id(ls_id);
          inv_idx_scan_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
        }
      }
    } else if (ObDASIterType::DAS_ITER_LOCAL_LOOKUP == root_iter->get_type()) {
      ObDASLocalLookupIter *local_lookup_iter = static_cast<ObDASLocalLookupIter *>(root_iter);
      local_lookup_iter->set_ls_id(ls_id);
      local_lookup_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
      if (ObDASIterType::DAS_ITER_LOCAL_LOOKUP == local_lookup_iter->get_children()[0]->get_type()) {
        ObDASLocalLookupIter *aux_lookup_iter = static_cast<ObDASLocalLookupIter *>(local_lookup_iter->get_children()[0]);
        aux_lookup_iter->set_ls_id(ls_id);
        aux_lookup_iter->set_tablet_id(related_tablet_ids.doc_rowkey_tablet_id_);
        if (ObDASIterType::DAS_ITER_HNSW_SCAN == aux_lookup_iter->get_children()[0]->get_type()) {
          ObDASHNSWScanIter *hnsw_scan_iter = static_cast<ObDASHNSWScanIter *>(aux_lookup_iter->get_children()[0]);
          hnsw_scan_iter->set_ls_id(ls_id);
          hnsw_scan_iter->set_related_tablet_ids(related_tablet_ids);
          if (ObDASIterType::DAS_ITER_LOCAL_LOOKUP == hnsw_scan_iter->get_inv_idx_scan_iter()->get_type()) {
            ObDASLocalLookupIter *inv_idx_scan_iter = static_cast<ObDASLocalLookupIter *>(hnsw_scan_iter->get_inv_idx_scan_iter());
            inv_idx_scan_iter->set_ls_id(ls_id);
            inv_idx_scan_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
          }
        }
      } else if (ObDASIterType::DAS_ITER_IVF_SCAN == local_lookup_iter->get_children()[0]->get_type()) {
        ObDASIvfBaseScanIter *ivf_scan_iter = static_cast<ObDASIvfBaseScanIter *>(local_lookup_iter->get_children()[0]);
        ivf_scan_iter->set_ls_id(ls_id);
        ivf_scan_iter->set_related_tablet_ids(related_tablet_ids);
        if (ObDASIterType::DAS_ITER_LOCAL_LOOKUP == ivf_scan_iter->get_inv_idx_scan_iter()->get_type()) {
          ObDASLocalLookupIter *inv_idx_scan_iter = static_cast<ObDASLocalLookupIter *>(ivf_scan_iter->get_inv_idx_scan_iter());
          inv_idx_scan_iter->set_ls_id(ls_id);
          inv_idx_scan_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
        }
      }
    }
  }
  return ret;
}

/***************** PUBLIC END *****************/

int ObDASIterUtils::create_das_scan_iter(common::ObIAllocator &alloc,
                                         const ObDASScanCtDef *scan_ctdef,
                                         ObDASScanRtDef *scan_rtdef,
                                         ObDASScanIter *&iter_tree)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(scan_ctdef) || OB_ISNULL(scan_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(scan_ctdef), KP(scan_rtdef));
  } else {
    ObDASScanIterParam param;
    init_scan_iter_param(param, scan_ctdef, scan_rtdef);
    if (OB_FAIL(create_das_iter(alloc, param, iter_tree))) {
      LOG_WARN("failed to create das scan iter", K(ret));
    }
  }

  return ret;
}

int ObDASIterUtils::create_das_scan_with_merge_iter(storage::ObTableScanParam &scan_param,
                                                    common::ObIAllocator &alloc,
                                                    const ObDASBaseCtDef *input_ctdef,
                                                    ObDASBaseRtDef *input_rtdef,
                                                    const ObDASRelatedTabletID &related_tablet_ids,
                                                    transaction::ObTxDesc *trans_desc,
                                                    transaction::ObTxReadSnapshot *snapshot,
                                                    ObDASScanIter *&data_table_tree,
                                                    ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ObDASOpType::DAS_OP_DOC_ID_MERGE != input_ctdef->op_type_
                  && ObDASOpType::DAS_OP_VID_MERGE != input_ctdef->op_type_
                  && ObDASOpType::DAS_OP_DOMAIN_ID_MERGE != input_ctdef->op_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, input ctdef op type isn't doc id merge or vid merge", K(ret), K(input_ctdef->op_type_), KPC(input_ctdef));
  } else if (ObDASOpType::DAS_OP_DOC_ID_MERGE == input_ctdef->op_type_) {
    const ObDASDocIdMergeCtDef *docid_merge_ctdef = static_cast<const ObDASDocIdMergeCtDef *>(input_ctdef);
    ObDASDocIdMergeRtDef *docid_merge_rtdef = static_cast<ObDASDocIdMergeRtDef *>(input_rtdef);

    if (OB_FAIL(create_doc_id_scan_sub_tree(scan_param, alloc, docid_merge_ctdef, docid_merge_rtdef,
                                            related_tablet_ids, trans_desc, snapshot, data_table_tree, iter_tree))) {
      LOG_WARN("fail to create doc id merge scan sub tree", K(ret), K(scan_param), KPC(input_ctdef), KPC(input_rtdef));
    }
  } else if (DAS_OP_VID_MERGE == input_ctdef->op_type_) {
    const ObDASVIdMergeCtDef *vid_merge_ctdef = static_cast<const ObDASVIdMergeCtDef *>(input_ctdef);
    ObDASVIdMergeRtDef *vid_merge_rtdef = static_cast<ObDASVIdMergeRtDef *>(input_rtdef);
    if (OB_FAIL(create_vid_scan_sub_tree(scan_param, alloc, vid_merge_ctdef, vid_merge_rtdef,
                                         related_tablet_ids, trans_desc, snapshot, data_table_tree, iter_tree))) {
      LOG_WARN("fail to create vec vid scan sub tree", K(ret), K(scan_param), KPC(input_ctdef), KPC(input_rtdef));
    }
  } else if (ObDASOpType::DAS_OP_DOMAIN_ID_MERGE == input_ctdef->op_type_) {
    if (OB_FAIL(create_domain_id_scan_sub_tree(scan_param, alloc, static_cast<const ObDASDomainIdMergeCtDef *>(input_ctdef),
          static_cast<ObDASDomainIdMergeRtDef *>(input_rtdef), related_tablet_ids, trans_desc, snapshot, data_table_tree, iter_tree))) {
      LOG_WARN("fail to create domain id scan sub tree", K(ret), K(scan_param), KPC(input_rtdef), KPC(input_rtdef));
    }
  }

  return ret;
}

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

  ObDASScanIter *data_table_iter = nullptr;
  if (OB_ISNULL(attach_ctdef)) {
    if (OB_FAIL(create_das_scan_iter(alloc, scan_ctdef, scan_rtdef, data_table_iter))) {
      LOG_WARN("fail to create das scan iter", K(ret), K(scan_ctdef), KPC(scan_rtdef));
    } else {
      data_table_iter->set_scan_param(scan_param);
      iter_tree = data_table_iter;
    }
  } else {
    if (OB_FAIL(create_das_scan_with_merge_iter(scan_param, alloc, attach_ctdef, attach_rtdef, related_tablet_ids,
                                                     trans_desc, snapshot, data_table_iter, iter_tree))) {
      LOG_WARN("fail to create das scan iter", K(ret), K(scan_param), KPC(attach_ctdef), KPC(attach_rtdef));
    } else {
      data_table_iter->set_scan_param(scan_param);
    }
  }

  return ret;
}


/* If attach_ctdef is null, this is the simplest case, and we can directly create iter tree
 * based on lookup_ctdef. If attach_ctdef is not null, then there may be three scenarios:
 *
 * 1.              attach_ctdef(ObDASTableLookupCtDef)
 *                     /                        \
 *           index_table_sub_tree      data_table(ObDASScanCtDef)
 *
 * 2.              attach_ctdef(ObDASTableLookupCtDef)
 *                     /                        \
 *           index_table_sub_tree      docid_merge(ObDASDocIdMergeCtDef)
 *                                          /                   \
 *                            data_table(ObDASScanCtDef)   rowkey_docid(ObDASScanCtDef)
 *
 * 3.              attach_ctdef(ObDASTableLookupCtDef)
 *                     /                        \
 *           index_table_sub_tree      vid_merge(ObDASVidMergeCtDef)
 *                                          /                   \
 *                            data_table(ObDASScanCtDef)   rowkey_vid(ObDASScanCtDef)
 */
int ObDASIterUtils::create_local_lookup_sub_tree(ObTableScanParam &scan_param,
                                                 common::ObIAllocator &alloc,
                                                 const ObDASBaseCtDef *index_ctdef,
                                                 ObDASBaseRtDef *index_rtdef,
                                                 const ObDASScanCtDef *lookup_ctdef,
                                                 ObDASScanRtDef *lookup_rtdef,
                                                 const ObDASBaseCtDef *attach_ctdef,
                                                 ObDASBaseRtDef *attach_rtdef,
                                                 const ObDASRelatedTabletID &related_tablet_ids,
                                                 transaction::ObTxDesc *trans_desc,
                                                 transaction::ObTxReadSnapshot *snapshot,
                                                 const ObTabletID &lookup_tablet_id,
                                                 ObDASIter *index_table_sub_tree,
                                                 ObDASIter *&iter_tree,
                                                 int64_t batch_row_count)
{
  int ret = OB_SUCCESS;

  ObDASScanIter *data_table_iter = nullptr;
  ObDASIter *data_table_sub_tree = nullptr;
  const ExprFixedArray *local_lookup_iter_output = nullptr;

  if (OB_ISNULL(attach_ctdef) && OB_ISNULL(attach_rtdef)) {
    if (OB_ISNULL(lookup_ctdef) || OB_ISNULL(lookup_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, data table ctdef or rtdef is null", K(ret), KPC(lookup_ctdef), KPC(lookup_rtdef));
    } else if (OB_FAIL(create_das_scan_iter(alloc, lookup_ctdef, lookup_rtdef, data_table_iter))) {
      LOG_WARN("failed to create data table das scan iter", K(ret), K(scan_param), KPC(lookup_ctdef), KPC(lookup_rtdef));
    } else {
      data_table_sub_tree = data_table_iter;
      local_lookup_iter_output = &lookup_ctdef->result_output_;
    }
  } else {
    if (OB_UNLIKELY((ObDASOpType::DAS_OP_TABLE_LOOKUP != attach_ctdef->op_type_ && ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP != attach_ctdef->op_type_)
                  || attach_ctdef->children_cnt_ < 2)
     || OB_UNLIKELY((ObDASOpType::DAS_OP_TABLE_LOOKUP != attach_rtdef->op_type_ && ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP != attach_rtdef->op_type_)
                  || attach_rtdef->children_cnt_ < 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, table lookup ctdef or rtdef isn't table lookup or children cnt isn't 2", K(ret), KPC(attach_ctdef), KPC(attach_rtdef));
    } else {
      local_lookup_iter_output = &(static_cast<const ObDASTableLookupCtDef *>(attach_ctdef))->result_output_;

      ObDASBaseCtDef *ctdef = attach_ctdef->children_[1];
      ObDASBaseRtDef *rtdef = attach_rtdef->children_[1];

      if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpeted error, ctdef or rtdef is nullptr", K(ret), KPC(ctdef), KPC(rtdef));
      } else if (ObDASOpType::DAS_OP_TABLE_SCAN == ctdef->op_type_) {
        ObDASScanCtDef *data_table_ctdef = static_cast<ObDASScanCtDef *>(ctdef);
        ObDASScanRtDef *data_table_rtdef = static_cast<ObDASScanRtDef *>(rtdef);
        if (OB_FAIL(create_das_scan_iter(alloc, data_table_ctdef, data_table_rtdef, data_table_iter))) {
          LOG_WARN("failed to create data table scan iter", K(ret));
        } else {
          data_table_sub_tree = data_table_iter;
        }
      } else if (OB_FAIL(create_das_scan_with_merge_iter(scan_param, alloc, ctdef, rtdef, related_tablet_ids,
                                                         trans_desc, snapshot, data_table_iter, data_table_sub_tree))) {
        LOG_WARN("failed to create data table scan iter", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObDASLocalLookupIterParam lookup_param;
    ObEvalCtx *eval_ctx = lookup_rtdef->eval_ctx_;
    lookup_param.max_size_ = eval_ctx->is_vectorized() ? eval_ctx->max_batch_size_ : 1;
    lookup_param.eval_ctx_ = eval_ctx;
    lookup_param.exec_ctx_ = &eval_ctx->exec_ctx_;
    lookup_param.output_ = local_lookup_iter_output;
    lookup_param.index_ctdef_ = index_ctdef;
    lookup_param.index_rtdef_ = index_rtdef;
    lookup_param.lookup_ctdef_ = lookup_ctdef;
    lookup_param.lookup_rtdef_ = lookup_rtdef;
    lookup_param.index_table_iter_ = index_table_sub_tree;
    lookup_param.data_table_iter_ = data_table_sub_tree;
    lookup_param.trans_desc_ = trans_desc;
    lookup_param.snapshot_ = snapshot;
    lookup_param.rowkey_exprs_ = &lookup_ctdef->rowkey_exprs_;
    lookup_param.default_batch_row_count_ = batch_row_count;

    ObDASLocalLookupIter *lookup_iter = nullptr;
    if (OB_FAIL(create_das_iter(alloc, lookup_param, lookup_iter))) {
      LOG_WARN("failed to create local lookup iter", K(ret));
    } else if (OB_FAIL(create_iter_children_array(2, alloc, lookup_iter))) {
      LOG_WARN("failed to create iter children array", K(ret));
    } else {
      lookup_iter->get_children()[0] = index_table_sub_tree;
      lookup_iter->get_children()[1] = data_table_sub_tree;
      data_table_iter->set_scan_param(lookup_iter->get_lookup_param());
      lookup_iter->set_tablet_id(lookup_tablet_id);
      lookup_iter->set_ls_id(scan_param.ls_id_);
      iter_tree = lookup_iter;
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
  if (OB_FAIL(create_das_scan_iter(alloc, scan_ctdef, scan_rtdef, index_table_iter))) {
    LOG_WARN("fail to create index table iter", K(ret));
  } else if (OB_FALSE_IT(index_table_iter->set_scan_param(scan_param))) {
  } else if (OB_FAIL(create_local_lookup_sub_tree(scan_param, alloc, scan_ctdef, scan_rtdef, lookup_ctdef, lookup_rtdef, attach_ctdef, attach_rtdef,
                                                  related_tablet_ids, trans_desc, snapshot, related_tablet_ids.lookup_tablet_id_, index_table_iter, iter_tree))) {
    LOG_WARN("fail to create local lookup tree", K(ret));
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
  const bool has_lookup = ObDASOpType::DAS_OP_TABLE_LOOKUP == attach_ctdef->op_type_ ||
                          ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP == attach_ctdef->op_type_;
  int64_t token_cnt = 0;
  bool taat_mode = false;
  if (OB_UNLIKELY(attach_ctdef->op_type_ != ObDASOpType::DAS_OP_IR_SCAN
      && attach_ctdef->op_type_ != ObDASOpType::DAS_OP_TABLE_LOOKUP
      && attach_ctdef->op_type_ != ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP
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
      related_tablet_ids.fts_tablet_ids_.at(ir_scan_rtdef->fts_idx_),
      false,
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

int ObDASIterUtils::create_text_retrieval_sub_tree(const ObLSID &ls_id,
                                                  common::ObIAllocator &alloc,
                                                  const ObDASIRScanCtDef *ir_scan_ctdef,
                                                  ObDASIRScanRtDef *ir_scan_rtdef,
                                                  const ObDASFTSTabletID &related_tablet_ids,
                                                  const bool is_func_lookup,
                                                  transaction::ObTxDesc *trans_desc,
                                                  transaction::ObTxReadSnapshot *snapshot,
                                                  ObDASIter *&retrieval_result)
          {
  int ret = OB_SUCCESS;
  ObExpr *search_text = ir_scan_ctdef->search_text_;
  const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
  ObDASTextRetrievalMergeIterParam merge_iter_param;
  ObDASTextRetrievalMergeIter *tr_merge_iter = nullptr;
  ObDASScanIterParam doc_cnt_agg_param;
  ObDASScanIter *doc_cnt_agg_iter = nullptr;
  bool taat_mode = false;
  bool need_inv_idx_agg_reset = !is_func_lookup;

  merge_iter_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
  merge_iter_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
  merge_iter_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
  merge_iter_param.output_ = &ir_scan_ctdef->result_output_;
  merge_iter_param.ir_ctdef_ = ir_scan_ctdef;
  merge_iter_param.ir_rtdef_ = ir_scan_rtdef;
  merge_iter_param.tx_desc_ = trans_desc;
  merge_iter_param.snapshot_ = snapshot;
  merge_iter_param.force_return_docid_ = is_func_lookup;

  if (0 != merge_iter_param.query_tokens_.count()) {
    merge_iter_param.query_tokens_.reuse();
  }
  ObFtsEvalNode *root_node = nullptr;
  ObMatchAgainstMode mode = ir_scan_ctdef->mode_flag_;

  if (OB_FAIL(ObDASTextRetrievalMergeIter::build_query_tokens(ir_scan_ctdef, ir_scan_rtdef, alloc, merge_iter_param.query_tokens_, root_node))) {
    LOG_WARN("failed to get query tokens for text retrieval", K(ret));
  } else if (OB_LIKELY(!ir_scan_ctdef->need_proj_relevance_score() && is_func_lookup)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("functional lookup without relevance score not supported", K(ret));
  } else if (BOOLEAN_MODE == mode && merge_iter_param.query_tokens_.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("boolean mode with too many tokens not supported", K(ret));
  } else if ((is_func_lookup && merge_iter_param.query_tokens_.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT) ||
              (!is_func_lookup && (merge_iter_param.query_tokens_.count() > OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT || !ir_scan_ctdef->need_proj_relevance_score()))) {
    need_inv_idx_agg_reset = true;
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
    ObDASTRTaatLookupIter *fts_lookup_merge_iter = nullptr;
    if (OB_FAIL(ret)) {
    } else if (!is_func_lookup && OB_FAIL(create_das_iter(alloc, merge_iter_param, fts_merge_iter))) {
      LOG_WARN("failed to create text retrieval merge iter", K(ret));
    } else if (is_func_lookup && OB_FAIL(create_das_iter(alloc, merge_iter_param, fts_lookup_merge_iter))) {
      LOG_WARN("failed to create text retrieval merge iter", K(ret));
    } else {
      tr_merge_iter = is_func_lookup ? fts_lookup_merge_iter : fts_merge_iter;
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
    ObDASTRDaatLookupIter *fts_lookup_merge_iter = nullptr;
    if (OB_FAIL(ret)) {
    } else if (!is_func_lookup && OB_FAIL(create_das_iter(alloc, merge_iter_param, fts_merge_iter))) {
      LOG_WARN("failed to create text retrieval merge iter", K(ret));
    } else if (is_func_lookup && OB_FAIL(create_das_iter(alloc, merge_iter_param, fts_lookup_merge_iter))) {
      LOG_WARN("failed to create text retrieval merge iter", K(ret));
    } else {
      tr_merge_iter = is_func_lookup ? fts_lookup_merge_iter : fts_merge_iter;
      taat_mode = false;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(root_node)) {
      root_node->release();
    }
  } else {
    if (BOOLEAN_MODE == mode) {
      tr_merge_iter->set_boolean_compute_node(root_node);
    }
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
        } else if (!is_func_lookup && OB_FAIL(retrieval_iter->set_query_token(query_tokens.at(i)))) {
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
    ObDASScanIter *&data_table_iter,
    ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_ctdef) || OB_ISNULL(merge_rtdef) || OB_UNLIKELY(2 != merge_ctdef->children_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(merge_ctdef), KPC(merge_rtdef), KPC(iter_tree));
  } else if (related_tablet_ids.domain_tablet_ids_.count() + 1 != merge_ctdef->children_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for domain tablet ids", K(ret), K(related_tablet_ids.domain_tablet_ids_),
             KPC(merge_ctdef), KPC(merge_rtdef), KPC(iter_tree));
  } else {
    ObDASDomainIdMergeIterParam domain_id_merge_param;
    ObDASDomainIdMergeIter *domain_id_merge_iter = nullptr;
    ObDASScanIter *rowkey_doc_iter = nullptr;
    ObDASScanCtDef* data_ctdef = static_cast<ObDASScanCtDef *>(merge_ctdef->children_[0]);
    ObDASScanRtDef* data_rtdef = static_cast<ObDASScanRtDef *>(merge_rtdef->children_[0]);
    ObDASScanCtDef* doc_id_ctdef = static_cast<ObDASScanCtDef *>(merge_ctdef->children_[1]);
    ObDASScanRtDef* doc_id_rtdef = static_cast<ObDASScanRtDef *>(merge_rtdef->children_[1]);
    if (OB_FAIL(create_das_scan_iter(alloc, data_ctdef, data_rtdef, data_table_iter))) {
      LOG_WARN("fail to create data table scan iter", K(ret), KPC(data_ctdef), KPC(data_rtdef));
    } else if (OB_FAIL(create_das_scan_iter(alloc, doc_id_ctdef, doc_id_rtdef, rowkey_doc_iter))) {
      LOG_WARN("fail to create das scan iter", K(ret), KPC(doc_id_ctdef), KPC(doc_id_rtdef));
    } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_table_iters_.push_back(rowkey_doc_iter))) {
      LOG_WARN("fail to push back domain iter", K(ret));
    } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_ctdefs_.push_back(doc_id_ctdef))) {
      LOG_WARN("fail to push back domain ctdef", K(ret));
    } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_rtdefs_.push_back(doc_id_rtdef))) {
      LOG_WARN("fail to push back domain rtdef", K(ret));
    } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_tablet_ids_.push_back(related_tablet_ids.domain_tablet_ids_.at(0)))) {
      LOG_WARN("fail to push back domain tablet id", K(ret));
    } else {
      DomainIdxs domain_idx;
      // build domain info for data ctdef
      if (data_ctdef->domain_types_.count() != data_ctdef->domain_tids_.count() ||
          data_ctdef->domain_types_.count() != data_ctdef->domain_id_idxs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid domain info", K(ret), K(data_ctdef->domain_types_), K(data_ctdef->domain_tids_), K(data_ctdef->domain_id_idxs_));
      } else if (data_ctdef->domain_types_.count() > 0) {
        if (data_ctdef->domain_types_.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid domain info", K(ret), K(data_ctdef->domain_types_), K(data_ctdef->domain_tids_), K(data_ctdef->domain_id_idxs_));
        }
      } else if (OB_FAIL(data_ctdef->domain_tids_.init(1))) {
        LOG_WARN("fail to init domain tid", K(ret));
      } else if (OB_FAIL(data_ctdef->domain_types_.init(1))) {
        LOG_WARN("fail to init domain type", K(ret));
      } else if (OB_FAIL(data_ctdef->domain_id_idxs_.init(1))) {
        LOG_WARN("fail to init domain id idx", K(ret));
      } else if (OB_FAIL(data_ctdef->domain_tids_.push_back(domain_id_merge_param.rowkey_domain_ctdefs_.at(0)->ref_table_id_))) {
        LOG_WARN("fail to push back table id", K(ret));
      } else if (OB_FAIL(data_ctdef->domain_types_.push_back(ObDomainIdUtils::ObDomainIDType::DOC_ID))) {
        LOG_WARN("fail to push back domain type", K(ret));
      } else if (OB_FAIL(domain_idx.push_back(data_ctdef->doc_id_idx_))) {
        LOG_WARN("fail to push back vec vid idx", K(ret));
      } else if (OB_FAIL(data_ctdef->domain_id_idxs_.push_back(domain_idx))) {
        LOG_WARN("fail to push back domain idx", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else {
        domain_id_merge_param.rowkey_domain_ls_id_ = scan_param.ls_id_;
        domain_id_merge_param.data_table_iter_     = data_table_iter;
        domain_id_merge_param.data_table_ctdef_    = data_ctdef;
        domain_id_merge_param.data_table_rtdef_    = data_rtdef;
        domain_id_merge_param.trans_desc_          = trans_desc;
        domain_id_merge_param.snapshot_            = snapshot;
        if (OB_FAIL(create_das_iter(alloc, domain_id_merge_param, domain_id_merge_iter))) {
          LOG_WARN("fail to create doc id merge iter", K(ret), K(domain_id_merge_param));
        } else if (OB_FAIL(create_iter_children_array(merge_ctdef->children_cnt_, alloc, domain_id_merge_iter))) {
          LOG_WARN("fail to create doc id merge iter children array", K(ret));
        } else {
          domain_id_merge_iter->get_children()[0] = data_table_iter;
          for (int64_t i = 1; i < merge_ctdef->children_cnt_; i++) {
            domain_id_merge_iter->get_children()[i] = domain_id_merge_param.rowkey_domain_table_iters_.at(i - 1);
            domain_id_merge_param.rowkey_domain_table_iters_.at(i - 1)->set_scan_param(domain_id_merge_iter->get_rowkey_domain_scan_param(i - 1));
          }
          iter_tree = domain_id_merge_iter;
        }
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
    ObDASScanIter *&data_table_iter,
    ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_ctdef) || OB_ISNULL(merge_rtdef) || OB_UNLIKELY(2 != merge_ctdef->children_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(merge_ctdef), KPC(merge_rtdef), KPC(iter_tree));
  } else if (related_tablet_ids.domain_tablet_ids_.count() + 1 != merge_ctdef->children_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for domain tablet ids", K(ret), K(related_tablet_ids.domain_tablet_ids_),
             KPC(merge_ctdef), KPC(merge_rtdef), KPC(iter_tree));
  } else {
    // create domain iter by vid merge ctdef
    ObDASDomainIdMergeIterParam domain_id_merge_param;
    ObDASDomainIdMergeIter *domain_id_merge_iter = nullptr;
    ObDASScanIter *rowkey_vid_iter = nullptr;
    ObDASScanCtDef* data_ctdef = static_cast<ObDASScanCtDef *>(merge_ctdef->children_[0]);
    ObDASScanRtDef* data_rtdef = static_cast<ObDASScanRtDef *>(merge_rtdef->children_[0]);
    ObDASScanCtDef* vid_ctdef = static_cast<ObDASScanCtDef *>(merge_ctdef->children_[1]);
    ObDASScanRtDef* vid_rtdef = static_cast<ObDASScanRtDef *>(merge_rtdef->children_[1]);
    if (OB_FAIL(create_das_scan_iter(alloc, data_ctdef, data_rtdef, data_table_iter))) {
      LOG_WARN("fail to create data table scan iter", K(ret), KPC(data_ctdef), KPC(data_rtdef));
    } else if (OB_FAIL(create_das_scan_iter(alloc, vid_ctdef, vid_rtdef, rowkey_vid_iter))) {
      LOG_WARN("fail to create das scan iter", K(ret), KPC(vid_ctdef), KPC(vid_rtdef));
    } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_table_iters_.push_back(rowkey_vid_iter))) {
      LOG_WARN("fail to push back domain iter", K(ret));
    } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_ctdefs_.push_back(vid_ctdef))) {
      LOG_WARN("fail to push back domain ctdef", K(ret));
    } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_rtdefs_.push_back(vid_rtdef))) {
      LOG_WARN("fail to push back domain rtdef", K(ret));
    } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_tablet_ids_.push_back(related_tablet_ids.domain_tablet_ids_.at(0)))) {
      LOG_WARN("fail to push back domain tablet id", K(ret));
    } else {

      DomainIdxs domain_idx;
      // build domain info for data ctdef
      if (data_ctdef->domain_types_.count() != data_ctdef->domain_tids_.count() ||
          data_ctdef->domain_types_.count() != data_ctdef->domain_id_idxs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid domain info", K(ret), K(data_ctdef->domain_types_), K(data_ctdef->domain_tids_), K(data_ctdef->domain_id_idxs_));
      } else if (data_ctdef->domain_types_.count() > 0) {
        if (data_ctdef->domain_types_.count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid domain info", K(ret), K(data_ctdef->domain_types_), K(data_ctdef->domain_tids_), K(data_ctdef->domain_id_idxs_));
        }
      } else if (OB_FAIL(data_ctdef->domain_tids_.init(1))) {
        LOG_WARN("fail to init domain tid", K(ret));
      } else if (OB_FAIL(data_ctdef->domain_types_.init(1))) {
        LOG_WARN("fail to init domain type", K(ret));
      } else if (OB_FAIL(data_ctdef->domain_id_idxs_.init(1))) {
        LOG_WARN("fail to init domain id idx", K(ret));
      } else if (OB_FAIL(data_ctdef->domain_tids_.push_back(domain_id_merge_param.rowkey_domain_ctdefs_.at(0)->ref_table_id_))) {
        LOG_WARN("fail to push back table id", K(ret));
      } else if (OB_FAIL(data_ctdef->domain_types_.push_back(ObDomainIdUtils::ObDomainIDType::VID))) {
        LOG_WARN("fail to push back domain type", K(ret));
      } else if (OB_FAIL(domain_idx.push_back(data_ctdef->vec_vid_idx_))) {
        LOG_WARN("fail to push back vec vid idx", K(ret));
      } else if (OB_FAIL(data_ctdef->domain_id_idxs_.push_back(domain_idx))) {
        LOG_WARN("fail to push back domain idx", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else {
        domain_id_merge_param.rowkey_domain_ls_id_ = scan_param.ls_id_;
        domain_id_merge_param.data_table_iter_     = data_table_iter;
        domain_id_merge_param.data_table_ctdef_    = data_ctdef;
        domain_id_merge_param.data_table_rtdef_    = data_rtdef;
        domain_id_merge_param.trans_desc_          = trans_desc;
        domain_id_merge_param.snapshot_            = snapshot;
        if (OB_FAIL(create_das_iter(alloc, domain_id_merge_param, domain_id_merge_iter))) {
          LOG_WARN("fail to create doc id merge iter", K(ret), K(domain_id_merge_param));
        } else if (OB_FAIL(create_iter_children_array(merge_ctdef->children_cnt_, alloc, domain_id_merge_iter))) {
          LOG_WARN("fail to create doc id merge iter children array", K(ret));
        } else {
          domain_id_merge_iter->get_children()[0] = data_table_iter;
          for (int64_t i = 1; i < merge_ctdef->children_cnt_; i++) {
            domain_id_merge_iter->get_children()[i] = domain_id_merge_param.rowkey_domain_table_iters_.at(i - 1);
            domain_id_merge_param.rowkey_domain_table_iters_.at(i - 1)->set_scan_param(domain_id_merge_iter->get_rowkey_domain_scan_param(i - 1));
          }
          iter_tree = domain_id_merge_iter;
        }
      }
    }
  }
  return ret;
}

int ObDASIterUtils::create_domain_id_scan_sub_tree(
    ObTableScanParam &scan_param,
    common::ObIAllocator &alloc,
    const ObDASDomainIdMergeCtDef *merge_ctdef,
    ObDASDomainIdMergeRtDef *merge_rtdef,
    const ObDASRelatedTabletID &related_tablet_ids,
    transaction::ObTxDesc *trans_desc,
    transaction::ObTxReadSnapshot *snapshot,
    ObDASScanIter *&data_table_iter,
    ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_ctdef) || OB_ISNULL(merge_rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(merge_ctdef), KPC(merge_rtdef), KPC(iter_tree));
  } else if (merge_ctdef->domain_types_.count() + 1 != merge_ctdef->children_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for domain types", K(ret), KPC(merge_ctdef), KPC(merge_rtdef), KPC(iter_tree));
  } else if (related_tablet_ids.domain_tablet_ids_.count() + 1 != merge_ctdef->children_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for domain tablet ids", K(ret), K(related_tablet_ids.domain_tablet_ids_),
             KPC(merge_ctdef), KPC(merge_rtdef), KPC(iter_tree));
  } else {
    ObDASDomainIdMergeIterParam domain_id_merge_param;
    ObDASDomainIdMergeIter *domain_id_merge_iter = nullptr;
    ObDASScanCtDef* data_ctdef = static_cast<ObDASScanCtDef *>(merge_ctdef->children_[0]);
    ObDASScanRtDef* data_rtdef = static_cast<ObDASScanRtDef *>(merge_rtdef->children_[0]);
    if (OB_FAIL(create_das_scan_iter(alloc, data_ctdef, data_rtdef, data_table_iter))) {
      LOG_WARN("fail to create data table scan iter", K(ret), KPC(data_ctdef), KPC(data_rtdef));
    }
    for (int64_t i = 1; OB_SUCC(ret) && i < merge_ctdef->children_cnt_; i++) {
      ObDASScanIter *rowkey_domain_iter = nullptr;
      ObDASScanCtDef* domain_ctdef = static_cast<ObDASScanCtDef *>(merge_ctdef->children_[i]);
      ObDASScanRtDef* domain_rtdef = static_cast<ObDASScanRtDef *>(merge_rtdef->children_[i]);
      if (OB_FAIL(create_das_scan_iter(alloc, domain_ctdef, domain_rtdef, rowkey_domain_iter))) {
        LOG_WARN("fail to create das scan iter", K(ret), KPC(domain_ctdef), KPC(domain_rtdef));
      } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_table_iters_.push_back(rowkey_domain_iter))) {
        LOG_WARN("fail to push back domain iter", K(ret));
      } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_ctdefs_.push_back(domain_ctdef))) {
        LOG_WARN("fail to push back domain ctdef", K(ret));
      } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_rtdefs_.push_back(domain_rtdef))) {
        LOG_WARN("fail to push back domain rtdef", K(ret));
      } else if (OB_FAIL(domain_id_merge_param.rowkey_domain_tablet_ids_.push_back(related_tablet_ids.domain_tablet_ids_.at(i - 1)))) {
        LOG_WARN("fail to push back domain tablet id", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      domain_id_merge_param.rowkey_domain_ls_id_ = scan_param.ls_id_;
      domain_id_merge_param.data_table_iter_     = data_table_iter;
      domain_id_merge_param.data_table_ctdef_    = data_ctdef;
      domain_id_merge_param.data_table_rtdef_    = data_rtdef;
      domain_id_merge_param.trans_desc_          = trans_desc;
      domain_id_merge_param.snapshot_            = snapshot;
      if (OB_FAIL(create_das_iter(alloc, domain_id_merge_param, domain_id_merge_iter))) {
        LOG_WARN("fail to create doc id merge iter", K(ret), K(domain_id_merge_param));
      } else if (OB_FAIL(create_iter_children_array(merge_ctdef->children_cnt_, alloc, domain_id_merge_iter))) {
        LOG_WARN("fail to create doc id merge iter children array", K(ret));
      } else {
        domain_id_merge_iter->get_children()[0] = data_table_iter;
        for (int64_t i = 1; i < merge_ctdef->children_cnt_; i++) {
          domain_id_merge_iter->get_children()[i] = domain_id_merge_param.rowkey_domain_table_iters_.at(i - 1);
          domain_id_merge_param.rowkey_domain_table_iters_.at(i - 1)->set_scan_param(domain_id_merge_iter->get_rowkey_domain_scan_param(i - 1));
        }
        iter_tree = domain_id_merge_iter;
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
        doc_id_lookup_iter->set_tablet_id(related_tablet_ids.doc_rowkey_tablet_id_);
        doc_id_lookup_iter->set_ls_id(ls_id);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (main_lookup_keep_order) {
      table_lookup_rtdef->get_lookup_scan_rtdef()->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    }

    int64_t batch_row_count = ObDASLookupIterParam::LOCAL_LOOKUP_ITER_DEFAULT_BATCH_ROW_COUNT;
    if (scan_param.table_param_->is_fts_index()) {
      ObEvalCtx *eval_ctx = table_lookup_rtdef->get_lookup_scan_rtdef()->eval_ctx_;
      batch_row_count = eval_ctx->is_vectorized() ? eval_ctx->max_batch_size_ : 1;
    }

    if (OB_FAIL(create_local_lookup_sub_tree(scan_param, alloc, table_lookup_ctdef->get_rowkey_scan_ctdef(), table_lookup_rtdef->get_rowkey_scan_rtdef(),
                                             table_lookup_ctdef->get_lookup_scan_ctdef(), table_lookup_rtdef->get_lookup_scan_rtdef(), table_lookup_ctdef,
                                             table_lookup_rtdef, related_tablet_ids, trans_desc, snapshot, related_tablet_ids.lookup_tablet_id_, doc_id_lookup_iter,
                                            domain_lookup_result, batch_row_count))) {
      LOG_WARN("failed to create local lookup sub tree", K(ret));
    }
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
        && rowkey_scan_ctdef->op_type_ != ObDASOpType::DAS_OP_SORT
        && rowkey_scan_ctdef->op_type_ != ObDASOpType::DAS_OP_INDEX_MERGE)) {
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
        related_tablet_ids.fts_tablet_ids_.at(ir_scan_rtdef->fts_idx_),
        false,
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
        doc_id_lookup_param.max_size_ = 1; // TODO: zyx439997 "A temporary solution, to be resolved after FTS4 is merged."
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
          doc_id_lookup_iter->set_tablet_id(related_tablet_ids.doc_rowkey_tablet_id_);
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
  } else if (ObDASOpType::DAS_OP_INDEX_MERGE == rowkey_scan_ctdef->op_type_) {
    ObDASIter *index_merge_root = nullptr;
    if (OB_FAIL(create_index_merge_sub_tree(scan_param,
                                            alloc,
                                            rowkey_scan_ctdef,
                                            rowkey_scan_rtdef,
                                            related_tablet_ids,
                                            trans_desc,
                                            snapshot,
                                            index_merge_root))) {
      LOG_WARN("failed to create index merge sub tree", K(ret));
    } else {
      rowkey_scan_iter = index_merge_root;
      rowkey_scan_ouput_exprs = index_merge_root->get_output();
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
    root_lookup_param.max_size_ = 1; // TODO: zyx439997 "A temporary solution, to be resolved after FTS4 is merged."
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
    if (idx_proj_lookup_ctdef->index_scan_proj_exprs_.count() > 0 &&
        OB_FAIL(root_lookup_param.index_scan_proj_exprs_.assign(idx_proj_lookup_ctdef->index_scan_proj_exprs_))) {
      LOG_WARN("failed to assign index scan proj exprs", K(ret));
    } else if (OB_FAIL(create_das_iter(alloc, root_lookup_param, root_lookup_iter))) {
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
      const ObDASIRScanCtDef *ir_ctdef = static_cast<const ObDASIRScanCtDef*>(func_lookup_ctdef->get_func_lookup_scan_ctdef(i));
      ObDASIRScanRtDef *ir_rtdef = static_cast<ObDASIRScanRtDef*>(func_lookup_rtdef->get_func_lookup_scan_rtdef(i));
      if (OB_FAIL(create_text_retrieval_sub_tree(scan_param.ls_id_,
                                                 alloc,
                                                 ir_ctdef,
                                                 ir_rtdef,
                                                 related_tablet_ids.fts_tablet_ids_.at(ir_rtdef->fts_idx_),
                                                 true,
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
    if (lookup_keep_order) {
      rtdef->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    }
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
        mvi_lookup_iter->set_tablet_id(related_tablet_ids.doc_rowkey_tablet_id_);
        mvi_lookup_iter->set_ls_id(scan_param.ls_id_);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_sort_sub_tree(alloc, sort_ctdef, sort_rtdef, false/*need_rewind*/,
                                          true/*need_distinct*/, mvi_lookup_iter, sort_iter))) {
    LOG_WARN("failed to create sort sub tree", K(ret));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_TABLE_LOOKUP, lookup_ctdef, lookup_rtdef))) {
    // multivalue index scan and don't need to index back lookup.
    ret = OB_SUCCESS;
    iter_tree = sort_iter;
  } else if (OB_FAIL(create_local_lookup_sub_tree(scan_param, alloc, lookup_ctdef->get_rowkey_scan_ctdef(), lookup_rtdef->get_rowkey_scan_rtdef(),
                                                  lookup_ctdef->get_lookup_scan_ctdef(), lookup_rtdef->get_lookup_scan_rtdef(), lookup_ctdef,
                                                  lookup_rtdef, related_tablet_ids, trans_desc, snapshot, related_tablet_ids.lookup_tablet_id_, sort_iter, iter_tree))) {
    LOG_WARN("failed to create local lookup sub tree", K(ret));
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
                                           ObDASIter *&iter_tree,
                                           const bool in_vec_pre_filter)
{
  int ret = OB_SUCCESS;

  const ObDASTableLookupCtDef *lookup_ctdef = nullptr;
  ObDASTableLookupRtDef *lookup_rtdef = nullptr;
  const ObDASSortCtDef *sort_ctdef = nullptr;
  ObDASSortRtDef *sort_rtdef = nullptr;

  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(attach_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table lookup param is nullptr", KP(attach_ctdef), KP(attach_rtdef));
  } else if (!in_vec_pre_filter && OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_TABLE_LOOKUP, lookup_ctdef, lookup_rtdef))) {
    LOG_WARN("find data table lookup def failed", K(ret));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_SORT, sort_ctdef, sort_rtdef))) {
    LOG_WARN("find sort def failed", K(ret));
  } else {
    ObDASIter *sort_iter = nullptr;
    ObDASSpatialScanIter *index_table_iter = nullptr;

    const ObDASScanCtDef* index_ctdef = static_cast<const ObDASScanCtDef *>(sort_ctdef->children_[0]);
    ObDASScanRtDef * index_rtdef = static_cast<ObDASScanRtDef *>(sort_rtdef->children_[0]);
    ObDASSpatialScanIterParam index_table_param;
    init_spatial_scan_iter_param(index_table_param, index_ctdef, index_rtdef);

    if (OB_FAIL(create_das_spatial_scan_iter(alloc, index_table_param, index_table_iter))) {
      LOG_WARN("failed to create index table scan iter", K(ret));
    } else if (OB_FALSE_IT(index_table_iter->set_scan_param(scan_param))) {
    } else if (OB_FAIL(create_sort_sub_tree(alloc, sort_ctdef, sort_rtdef, false/*need_rewind*/,
                                            true/*need_distinct*/, index_table_iter, sort_iter))) {
      LOG_WARN("failed to create sort sub tree", K(ret));
    } else if (!in_vec_pre_filter && OB_FAIL(create_local_lookup_sub_tree(scan_param, alloc, index_ctdef, index_rtdef, lookup_ctdef->get_lookup_scan_ctdef(),
                                                    lookup_rtdef->get_lookup_scan_rtdef(), lookup_ctdef, lookup_rtdef, related_tablet_ids,
                                                    trans_desc, snapshot, related_tablet_ids.lookup_tablet_id_, sort_iter, iter_tree))) {
      LOG_WARN("failed to create local lookup sub tree", K(ret));
    } else if (in_vec_pre_filter) {
      iter_tree = sort_iter;
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
    const bool need_lookup = (attach_ctdef->op_type_ == ObDASOpType::DAS_OP_TABLE_LOOKUP) ||
                             (attach_ctdef->op_type_ == ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP);
    const ObDASBaseCtDef *index_merge_ctdef = need_lookup ? attach_ctdef->children_[0] : attach_ctdef;
    ObDASBaseRtDef *index_merge_rtdef = need_lookup ? attach_rtdef->children_[0] : attach_rtdef;
    ObDASIter *index_merge_root = nullptr;
    if (OB_FAIL(create_index_merge_sub_tree(scan_param,
                                            alloc,
                                            index_merge_ctdef,
                                            index_merge_rtdef,
                                            related_tablet_ids,
                                            tx_desc,
                                            snapshot,
                                            index_merge_root))) {
      LOG_WARN("failed to create index merge iter tree", K(ret));
    } else if (OB_ISNULL(index_merge_root)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null index merge root", KPC(attach_ctdef), K(ret));
    } else if (need_lookup) {
      const ObDASScanCtDef *lookup_ctdef = static_cast<const ObDASScanCtDef*>(attach_ctdef->children_[1]);
      ObDASScanRtDef *lookup_rtdef = static_cast<ObDASScanRtDef*>(attach_rtdef->children_[1]);
      if (attach_ctdef->op_type_ == ObDASOpType::DAS_OP_TABLE_LOOKUP) {
        ObDASLocalLookupIter *lookup_iter = nullptr;
        if (OB_FAIL(create_local_lookup_sub_tree(scan_param,
                                                alloc,
                                                index_merge_ctdef,
                                                index_merge_rtdef,
                                                lookup_ctdef,
                                                lookup_rtdef,
                                                tx_desc,
                                                snapshot,
                                                index_merge_root,
                                                related_tablet_ids.lookup_tablet_id_,
                                                lookup_iter))) {
          LOG_WARN("failed to create lookup iter for index merge", K(ret));
        } else {
          iter_tree = lookup_iter;
        }
      } else if (attach_ctdef->op_type_ == ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP) {
        ObDASCacheLookupIter *lookup_iter = nullptr;
        if (OB_FAIL(create_cache_lookup_sub_tree(scan_param,
                                                alloc,
                                                attach_ctdef,
                                                attach_rtdef,
                                                tx_desc,
                                                snapshot,
                                                index_merge_root,
                                                related_tablet_ids.lookup_tablet_id_,
                                                lookup_iter))) {
          LOG_WARN("failed to create lookup iter for index merge", K(ret));
        } else {
          iter_tree = lookup_iter;
        }
      }
    } else {
      iter_tree = index_merge_root;
    }
  }

  return ret;
}

int ObDASIterUtils::create_local_lookup_sub_tree(ObTableScanParam &scan_param,
                                                 common::ObIAllocator &alloc,
                                                 const ObDASBaseCtDef *index_ctdef,
                                                 ObDASBaseRtDef *index_rtdef,
                                                 const ObDASScanCtDef *lookup_ctdef,
                                                 ObDASScanRtDef *lookup_rtdef,
                                                 transaction::ObTxDesc *trans_desc,
                                                 transaction::ObTxReadSnapshot *snapshot,
                                                 ObDASIter *index_iter,
                                                 const ObTabletID &lookup_tablet_id,
                                                 ObDASLocalLookupIter *&lookup_iter,
                                                 const bool lookup_keep_order,
                                                 const int64_t lookup_batch_size)
{
  int ret = OB_SUCCESS;
  ObDASScanIterParam data_table_param;
  ObDASScanIter *data_table_iter = nullptr;
  data_table_param.scan_ctdef_ = lookup_ctdef;
  data_table_param.max_size_ = lookup_rtdef->eval_ctx_->is_vectorized() ? lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
  data_table_param.eval_ctx_ = lookup_rtdef->eval_ctx_;
  data_table_param.exec_ctx_ = &lookup_rtdef->eval_ctx_->exec_ctx_;
  data_table_param.output_ = &lookup_ctdef->result_output_;
  if (OB_FAIL(create_das_iter(alloc, data_table_param, data_table_iter))) {
    LOG_WARN("failed to create data table iter", K(ret));
  } else {
    ObDASLocalLookupIterParam lookup_param;
    lookup_param.max_size_ = lookup_rtdef->eval_ctx_->is_vectorized() ? lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
    lookup_param.eval_ctx_ = lookup_rtdef->eval_ctx_;
    lookup_param.exec_ctx_ = &lookup_rtdef->eval_ctx_->exec_ctx_;
    lookup_param.output_ = &lookup_ctdef->result_output_;
    lookup_param.default_batch_row_count_ = lookup_batch_size;
    lookup_param.index_ctdef_ = index_ctdef;
    lookup_param.index_rtdef_ = index_rtdef;
    lookup_param.lookup_ctdef_ = lookup_ctdef;
    lookup_param.lookup_rtdef_ = lookup_rtdef;
    lookup_param.index_table_iter_ = index_iter;
    lookup_param.data_table_iter_ = data_table_iter;
    lookup_param.trans_desc_ = trans_desc;
    lookup_param.snapshot_ = snapshot;
    lookup_param.rowkey_exprs_ = &lookup_ctdef->rowkey_exprs_;
    if (OB_LIKELY(lookup_keep_order)) {
      lookup_param.lookup_rtdef_->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    }
    if (OB_FAIL(create_das_iter(alloc, lookup_param, lookup_iter))) {
      LOG_WARN("failed to create local lookup iter", K(ret));
    } else if (OB_FAIL(create_iter_children_array(2, alloc, lookup_iter))) {
      LOG_WARN("failed to create iter children array", K(ret));
    } else {
      lookup_iter->get_children()[0] = index_iter;
      lookup_iter->get_children()[1] = data_table_iter;
      data_table_iter->set_scan_param(lookup_iter->get_lookup_param());
      lookup_iter->set_tablet_id(lookup_tablet_id);
      lookup_iter->set_ls_id(scan_param.ls_id_);
    }
  }
  return ret;
}

int ObDASIterUtils::create_cache_lookup_sub_tree(ObTableScanParam &scan_param,
                                                 common::ObIAllocator &alloc,
                                                 const ObDASBaseCtDef *attach_ctdef,
                                                 ObDASBaseRtDef *attach_rtdef,
                                                 transaction::ObTxDesc *trans_desc,
                                                 transaction::ObTxReadSnapshot *snapshot,
                                                 ObDASIter *index_iter,
                                                 const ObTabletID &lookup_tablet_id,
                                                 ObDASCacheLookupIter *&lookup_iter,
                                                 const bool lookup_keep_order,
                                                 const int64_t lookup_batch_size)
{
  int ret = OB_SUCCESS;
  ObDASScanIterParam data_table_param;
  ObDASScanIter *data_table_iter = nullptr;
  const ObDASIndexProjLookupCtDef *idx_proj_lookup_ctdef = nullptr;
  ObDASIndexProjLookupRtDef *idx_proj_lookup_rtdef = nullptr;
  const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
  ObDASIRAuxLookupRtDef *aux_lookup_rtdef = nullptr;
  const ObDASBaseCtDef *index_ctdef = nullptr;
  ObDASBaseRtDef *index_rtdef = nullptr;
  const ObDASScanCtDef *lookup_ctdef = nullptr;
  ObDASScanRtDef *lookup_rtdef = nullptr;

  if (attach_ctdef->op_type_ == DAS_OP_INDEX_PROJ_LOOKUP) {
    idx_proj_lookup_ctdef = static_cast<const ObDASIndexProjLookupCtDef *>(attach_ctdef);
    idx_proj_lookup_rtdef = static_cast<ObDASIndexProjLookupRtDef *>(attach_rtdef);
    if (OB_ISNULL(idx_proj_lookup_ctdef) || OB_ISNULL(idx_proj_lookup_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to attach def", K(ret), KP(attach_ctdef), KP(attach_rtdef));
    } else {
      lookup_ctdef = idx_proj_lookup_ctdef->get_lookup_scan_ctdef();
      lookup_rtdef = idx_proj_lookup_rtdef->get_lookup_scan_rtdef();
      index_ctdef = idx_proj_lookup_ctdef->get_rowkey_scan_ctdef();
      index_rtdef = idx_proj_lookup_rtdef->get_rowkey_scan_rtdef();
    }
  } else if (attach_ctdef->op_type_ == DAS_OP_IR_AUX_LOOKUP) {
    aux_lookup_ctdef = static_cast<const ObDASIRAuxLookupCtDef*>(attach_ctdef);
    aux_lookup_rtdef = static_cast<ObDASIRAuxLookupRtDef*>(attach_rtdef);
    if (OB_ISNULL(aux_lookup_ctdef) || OB_ISNULL(aux_lookup_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to attach def", K(ret), KP(attach_ctdef), KP(attach_rtdef));
    } else {
      lookup_ctdef = aux_lookup_ctdef->get_lookup_scan_ctdef();
      lookup_rtdef = aux_lookup_rtdef->get_lookup_scan_rtdef();
      index_ctdef = aux_lookup_ctdef->get_doc_id_scan_ctdef();
      index_rtdef = aux_lookup_rtdef->get_doc_id_scan_rtdef();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(lookup_ctdef) || OB_ISNULL(lookup_rtdef) ||
      OB_ISNULL(index_ctdef) || OB_ISNULL(index_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(lookup_ctdef), KP(lookup_rtdef), KP(index_ctdef), KP(index_rtdef));
  } else {
    data_table_param.scan_ctdef_ = lookup_ctdef;
    data_table_param.max_size_ = lookup_rtdef->eval_ctx_->is_vectorized() ? lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
    data_table_param.eval_ctx_ = lookup_rtdef->eval_ctx_;
    data_table_param.exec_ctx_ = &lookup_rtdef->eval_ctx_->exec_ctx_;
    data_table_param.output_ = &lookup_ctdef->result_output_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_das_iter(alloc, data_table_param, data_table_iter))) {
    LOG_WARN("failed to create data table iter", K(ret));
  } else {
    ObDASCacheLookupIterParam lookup_param;
    lookup_param.max_size_ = lookup_rtdef->eval_ctx_->is_vectorized() ? lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
    lookup_param.eval_ctx_ = lookup_rtdef->eval_ctx_;
    lookup_param.exec_ctx_ = &lookup_rtdef->eval_ctx_->exec_ctx_;
    lookup_param.default_batch_row_count_ = lookup_batch_size;
    lookup_param.index_ctdef_ = index_ctdef;
    lookup_param.index_rtdef_ = index_rtdef;
    lookup_param.lookup_ctdef_ = lookup_ctdef;
    lookup_param.lookup_rtdef_ = lookup_rtdef;
    lookup_param.index_table_iter_ = index_iter;
    lookup_param.data_table_iter_ = data_table_iter;
    lookup_param.trans_desc_ = trans_desc;
    lookup_param.snapshot_ = snapshot;
    lookup_param.rowkey_exprs_ = &lookup_ctdef->rowkey_exprs_;
    if (OB_LIKELY(lookup_keep_order)) {
      lookup_param.lookup_rtdef_->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    }
    if (idx_proj_lookup_ctdef != nullptr) {
      lookup_param.output_ =  &idx_proj_lookup_ctdef->result_output_;
      if (idx_proj_lookup_ctdef->index_scan_proj_exprs_.count() > 0 &&
          OB_FAIL(lookup_param.index_scan_proj_exprs_.assign(idx_proj_lookup_ctdef->index_scan_proj_exprs_))) {
        LOG_WARN("failed to assign index scan proj exprs", K(ret));
      }
    } else if (aux_lookup_ctdef != nullptr) {
      lookup_param.output_ =  &aux_lookup_ctdef->result_output_;
      if (aux_lookup_ctdef->relevance_proj_col_ != nullptr &&
          OB_FAIL(lookup_param.index_scan_proj_exprs_.push_back(aux_lookup_ctdef->relevance_proj_col_))) {
        LOG_WARN("failed to pushback relevance proj col to index scan proj exprs", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_das_iter(alloc, lookup_param, lookup_iter))) {
      LOG_WARN("failed to create local lookup iter", K(ret));
    } else if (OB_FAIL(create_iter_children_array(2, alloc, lookup_iter))) {
      LOG_WARN("failed to create iter children array", K(ret));
    } else {
      lookup_iter->get_children()[0] = index_iter;
      lookup_iter->get_children()[1] = data_table_iter;
      data_table_iter->set_scan_param(lookup_iter->get_lookup_param());
      lookup_iter->set_tablet_id(lookup_tablet_id);
      lookup_iter->set_ls_id(scan_param.ls_id_);
    }
  }

  return ret;
}

int ObDASIterUtils::create_index_merge_sub_tree(ObTableScanParam &scan_param,
                                                common::ObIAllocator &alloc,
                                                const ObDASBaseCtDef *ctdef,
                                                ObDASBaseRtDef *rtdef,
                                                const ObDASRelatedTabletID &related_tablet_ids,
                                                transaction::ObTxDesc *tx_desc,
                                                transaction::ObTxReadSnapshot *snapshot,
                                                ObDASIter *&iter)
{
  int ret = OB_SUCCESS;
if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef) || ctdef->op_type_ != DAS_OP_INDEX_MERGE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ctdef), K(rtdef));
  } else {
    const ObDASIndexMergeCtDef *merge_ctdef = static_cast<const ObDASIndexMergeCtDef*>(ctdef);
    ObDASIndexMergeRtDef *merge_rtdef = static_cast<ObDASIndexMergeRtDef*>(rtdef);
    ObArray<ObDASIter*> child_iters;
    ObArray<ObDASScanIter*> child_scan_iters;
    ObArray<ObDASScanRtDef*> child_scan_rtdefs;
    int64_t children_cnt = ctdef->children_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt; ++i) {
      ObDASIter *child_iter = nullptr;
      ObDASScanIter *child_scan_iter = nullptr;
      ObDASScanRtDef *child_scan_rtdef = nullptr;
      const ObDASBaseCtDef *child_ctdef = merge_ctdef->children_[i];
      ObDASBaseRtDef *child_rtdef = merge_rtdef->children_[i];
      if (OB_ISNULL(child_ctdef) || OB_ISNULL(child_rtdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null child ctdef or rtdef", K(ret));
      } else if (merge_ctdef->merge_node_types_.at(i) == INDEX_MERGE_UNION
                 || merge_ctdef->merge_node_types_.at(i) == INDEX_MERGE_INTERSECT) {
        if (OB_FAIL(create_index_merge_sub_tree(scan_param,
                                                alloc,
                                                child_ctdef,
                                                child_rtdef,
                                                related_tablet_ids,
                                                tx_desc,
                                                snapshot,
                                                child_iter))) {
          LOG_WARN("failed to create index merge sub tree", K(ret));
        }
      } else if (merge_ctdef->merge_node_types_.at(i) == INDEX_MERGE_SCAN) {
        ObDASScanRtDef *scan_rtdef = nullptr;
        if (child_rtdef->op_type_ == DAS_OP_TABLE_SCAN) {
          scan_rtdef = static_cast<ObDASScanRtDef*>(child_rtdef);
        } else if (child_rtdef->op_type_ == DAS_OP_SORT) {
          if (child_rtdef->children_cnt_ == 1 && OB_NOT_NULL(child_rtdef->children_[0]) &&
                child_rtdef->children_[0]->op_type_ == DAS_OP_TABLE_SCAN) {
            scan_rtdef = static_cast<ObDASScanRtDef*>(child_rtdef->children_[0]);
          }
        }
        if (OB_ISNULL(scan_rtdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null scan rtdef", K(ret));
        } else {
          child_scan_rtdef = scan_rtdef;
          ObDASScanIterParam scan_param;
          ObDASScanIter *scan_iter = nullptr;
          const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(scan_rtdef->ctdef_);
          scan_param.scan_ctdef_ = scan_ctdef;
          scan_param.max_size_ = rtdef->eval_ctx_->is_vectorized() ? rtdef->eval_ctx_->max_batch_size_ : 1;
          scan_param.eval_ctx_ = rtdef->eval_ctx_;
          scan_param.exec_ctx_ = &rtdef->eval_ctx_->exec_ctx_;
          scan_param.output_ = &scan_ctdef->result_output_;
          if (OB_FAIL(create_das_iter(alloc, scan_param, scan_iter))) {
            LOG_WARN("failed to create das scan iter", K(ret));
          } else {
            child_scan_iter = scan_iter;
            child_iter = scan_iter;
          }
        }
      } else if (merge_ctdef->merge_node_types_.at(i) == INDEX_MERGE_FTS_INDEX) {
        ObDASIRScanRtDef *ir_rtdef = nullptr;
        const ObDASIRScanCtDef *ir_ctdef = nullptr;
        ObDASIter *ir_iter = nullptr;
        const ObDASBaseCtDef *ctdef = (child_ctdef->op_type_ == DAS_OP_SORT) ? child_ctdef->children_[0] : child_ctdef;
        ObDASBaseRtDef *rtdef = (child_rtdef->op_type_ == DAS_OP_SORT) ? child_rtdef->children_[0] : child_rtdef;
        if (OB_NOT_NULL(ctdef) && OB_NOT_NULL(rtdef)) {
          ir_ctdef = static_cast<const ObDASIRScanCtDef*>((ctdef->op_type_ == DAS_OP_IR_AUX_LOOKUP) ? ctdef->children_[0] : ctdef);
          ir_rtdef = static_cast<ObDASIRScanRtDef *>((rtdef->op_type_ == DAS_OP_IR_AUX_LOOKUP) ? rtdef->children_[0] : rtdef);
        }
        if (OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null ir ctdef or rtdef", K(ret));
        } else if (OB_FAIL(create_text_retrieval_sub_tree(scan_param.ls_id_,
                                                          alloc,
                                                          ir_ctdef,
                                                          ir_rtdef,
                                                          related_tablet_ids.fts_tablet_ids_.at(ir_rtdef->fts_idx_),
                                                          false,
                                                          tx_desc,
                                                          snapshot,
                                                          ir_iter))) {
          LOG_WARN("failed to create text retrieval sub tree", K(ret));
        } else if (rtdef->op_type_ == DAS_OP_IR_AUX_LOOKUP) {
          ObDASScanRtDef *lookup_rtdef = static_cast<ObDASIRAuxLookupRtDef*>(rtdef)->get_lookup_scan_rtdef();
          if (OB_ISNULL(lookup_rtdef)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null doc id lookup rtdef", K(ret));
          } else if (ir_ctdef->need_proj_relevance_score()) {
            ObDASCacheLookupIter *doc_id_lookup_iter = nullptr;
            if (OB_FAIL(create_cache_lookup_sub_tree(scan_param,
                                                    alloc,
                                                    ctdef,
                                                    rtdef,
                                                    tx_desc,
                                                    snapshot,
                                                    ir_iter,
                                                    related_tablet_ids.doc_rowkey_tablet_id_,
                                                    doc_id_lookup_iter))) {
              LOG_WARN("failed to create local lookup sub tree", K(ret));
            } else {
              child_iter = doc_id_lookup_iter;
            }
          } else {
            ObDASLocalLookupIter *doc_id_lookup_iter = nullptr;
            if (OB_FAIL(create_local_lookup_sub_tree(scan_param,
                                                     alloc,
                                                     ir_rtdef->ctdef_,
                                                     ir_rtdef,
                                                     static_cast<const ObDASScanCtDef*>(lookup_rtdef->ctdef_),
                                                     lookup_rtdef,
                                                     tx_desc,
                                                     snapshot,
                                                     ir_iter,
                                                     related_tablet_ids.doc_rowkey_tablet_id_,
                                                     doc_id_lookup_iter))) {
              LOG_WARN("failed to create local lookup sub tree", K(ret));
            } else {
              child_iter = doc_id_lookup_iter;
            }
          }
        } else {
          child_iter = ir_iter;
        }
      }

      if (OB_SUCC(ret) && OB_NOT_NULL(child_iter)) {
        if ((merge_ctdef->merge_node_types_.at(i) == INDEX_MERGE_SCAN
             || merge_ctdef->merge_node_types_.at(i) == INDEX_MERGE_FTS_INDEX)
            && child_ctdef->op_type_ == DAS_OP_SORT) {
          // insert a sort iter, data rows are deep copied in index merge thus the sort iter doesn't need to hold the memory
          const bool need_rewind = false;
          const bool need_distinct = false;
          ObDASIter *sort_iter = nullptr;
          ObDASSortRtDef *sort_rtdef = static_cast<ObDASSortRtDef*>(child_rtdef);
          const ObDASSortCtDef *sort_ctdef = static_cast<const ObDASSortCtDef*>(child_ctdef);
          if (OB_FAIL(create_sort_sub_tree(alloc, sort_ctdef, sort_rtdef, need_rewind, need_distinct, child_iter, sort_iter))) {
            LOG_WARN("failed to create sort sub tree", K(ret));
          } else {
            child_iter = sort_iter;
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(child_iters.push_back(child_iter))) {
        LOG_WARN("failed to push back child iter", K(ret));
      } else if (OB_FAIL(child_scan_iters.push_back(child_scan_iter))) {
        LOG_WARN("failed to push back child scan iter", K(ret));
      } else if (OB_FAIL(child_scan_rtdefs.push_back(child_scan_rtdef))) {
        LOG_WARN("failed to push back child scan rtdef", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObDASIndexMergeIterParam merge_param;
      ObDASIndexMergeIter *merge_iter = nullptr;
      merge_param.max_size_ = merge_rtdef->eval_ctx_->is_vectorized() ?
          merge_rtdef->eval_ctx_->max_batch_size_ : 1;
      merge_param.eval_ctx_ = merge_rtdef->eval_ctx_;
      merge_param.exec_ctx_ = &merge_rtdef->eval_ctx_->exec_ctx_;
      merge_param.output_ = &merge_ctdef->result_output_;
      merge_param.merge_type_ = merge_ctdef->merge_type_;
      merge_param.ctdef_ = merge_ctdef;
      merge_param.rtdef_ = merge_rtdef;
      merge_param.child_iters_ = &child_iters;
      merge_param.child_scan_rtdefs_ = &child_scan_rtdefs;
      merge_param.tx_desc_ = tx_desc;
      merge_param.snapshot_ = snapshot;
      merge_param.is_reverse_ = merge_ctdef->is_reverse_;
      merge_param.rowkey_exprs_ = &merge_ctdef->rowkey_exprs_;
      if (OB_FAIL(create_das_iter(alloc, merge_param, merge_iter))) {
        LOG_WARN("failed to create das index merge iter", K(ret));
      } else if (OB_FAIL(merge_iter->set_ls_tablet_ids(scan_param.ls_id_, related_tablet_ids))) {
        LOG_WARN("failed to set ls tablet ids", K(ret));
      } else if (OB_FAIL(create_iter_children_array(children_cnt, alloc, merge_iter))) {
        LOG_WARN("failed to create iter children array", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt; ++i) {
          merge_iter->get_children()[i] = child_iters[i];
          ObTableScanParam *child_scan_param = nullptr;
          if (child_scan_rtdefs.at(i) != nullptr) {
            if (OB_ISNULL(child_scan_iters.at(i)) || OB_ISNULL(child_scan_param = merge_iter->get_child_scan_param(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null child scan iter or param", K(ret));
            } else {
              child_scan_iters.at(i)->set_scan_param(*child_scan_param);
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
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

int ObDASIterUtils::create_vec_lookup_tree(ObTableScanParam &scan_param,
                                           common::ObIAllocator &alloc,
                                           const ObDASBaseCtDef *attach_ctdef,
                                           ObDASBaseRtDef *attach_rtdef,
                                           const ObDASRelatedTabletID &related_tablet_ids,
                                           transaction::ObTxDesc *trans_desc,
                                           transaction::ObTxReadSnapshot *snapshot,
                                           ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  bool is_ivf = is_vec_ivf_scan(attach_ctdef, attach_rtdef);
  if (!is_ivf) {
    ret = create_vec_hnsw_lookup_tree(
        scan_param, alloc, attach_ctdef, attach_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
  } else {
    ret = create_vec_ivf_lookup_tree(
        scan_param, alloc, attach_ctdef, attach_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
  }
  return ret;
}

int ObDASIterUtils::create_vec_hnsw_lookup_tree(ObTableScanParam &scan_param,
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
  const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
  ObDASIRAuxLookupRtDef *aux_lookup_rtdef = nullptr;
  const ObDASVecAuxScanCtDef *vec_aux_ctdef = nullptr;
  ObDASVecAuxScanRtDef *vec_aux_rtdef = nullptr;
  const ObDASSortCtDef *sort_ctdef = nullptr;
  ObDASSortRtDef *sort_rtdef = nullptr;

  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(attach_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(attach_ctdef), K(attach_rtdef));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_TABLE_LOOKUP, lookup_ctdef, lookup_rtdef))) {
    LOG_WARN("find data table lookup def failed", K(ret));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_IR_AUX_LOOKUP, aux_lookup_ctdef, aux_lookup_rtdef))) {
    LOG_WARN("find vec aux lookup definition failed");
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_VEC_SCAN, vec_aux_ctdef, vec_aux_rtdef))) {
    LOG_WARN("find ir scan definition failed", K(ret));
  } else {
    sort_ctdef = static_cast<const ObDASSortCtDef *>(aux_lookup_ctdef->children_[0]);
    sort_rtdef = static_cast<ObDASSortRtDef *>(aux_lookup_rtdef->children_[0]);

    const ObDASBaseCtDef *inv_idx_ctdef = vec_aux_ctdef->get_inv_idx_scan_ctdef();
    ObDASBaseRtDef *inv_idx_rtdef = vec_aux_rtdef->get_inv_idx_scan_rtdef();
    const ObDASScanCtDef *data_table_ctdef = lookup_ctdef->get_lookup_scan_ctdef();
    ObDASScanRtDef *data_table_rtdef = lookup_rtdef->get_lookup_scan_rtdef();
    data_table_rtdef->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    const ObDASScanCtDef* vid_rowkey_ctdef = aux_lookup_ctdef->get_lookup_scan_ctdef();
    ObDASScanRtDef *vid_rowkey_rtdef = aux_lookup_rtdef->get_lookup_scan_rtdef();
    vid_rowkey_rtdef->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    const ObDASScanCtDef *rowkey_vid_ctdef = vec_aux_ctdef->get_vec_aux_tbl_ctdef(vec_aux_ctdef->get_rowkey_vid_tbl_idx(), ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN);
    ObDASScanRtDef *rowkey_vid_rtdef = vec_aux_rtdef->get_vec_aux_tbl_rtdef(vec_aux_ctdef->get_rowkey_vid_tbl_idx());
    rowkey_vid_rtdef->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    const ObDASScanCtDef *index_id_tbl_ctdef = vec_aux_ctdef->get_vec_aux_tbl_ctdef(vec_aux_ctdef->get_index_id_tbl_idx(), ObTSCIRScanType::OB_VEC_IDX_ID_SCAN);
    ObDASScanRtDef *index_id_tbl_rtdef = vec_aux_rtdef->get_vec_aux_tbl_rtdef(vec_aux_ctdef->get_index_id_tbl_idx());
    index_id_tbl_rtdef->scan_flag_.scan_order_ = ObQueryFlag::Reverse;
    const ObDASScanCtDef *com_aux_tbl_ctdef = vec_aux_ctdef->get_vec_aux_tbl_ctdef(vec_aux_ctdef->get_com_aux_tbl_idx(), ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
    ObDASScanRtDef *com_aux_tbl_rtdef = vec_aux_rtdef->get_vec_aux_tbl_rtdef(vec_aux_ctdef->get_com_aux_tbl_idx());
    com_aux_tbl_rtdef->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;

    ObDASIter *inv_idx_iter = nullptr;
    ObDASScanIter *delta_buf_table_iter = nullptr;
    ObDASScanIter *index_id_table_iter = nullptr;
    ObDASScanIter *snapshot_table_iter = nullptr;
    ObDASScanIter *vid_rowkey_table_iter = nullptr;
    ObDASScanIter *com_aux_vec_iter = nullptr;
    ObDASScanIter *rowkey_vid_table_iter = nullptr;

    bool is_primary_index = false;
    if (scan_param.table_param_->is_spatial_index()) {
      if (OB_FAIL(create_gis_lookup_tree(scan_param, alloc, inv_idx_ctdef, inv_idx_rtdef, related_tablet_ids, trans_desc, snapshot, inv_idx_iter, true))) {
        LOG_WARN("failed to create gis lookup tree", K(ret));
      }
    } else {
      ObDASScanIter *inv_idx_scan_iter = nullptr;
      const ObDASScanCtDef *inv_idx_scan_ctdef = static_cast<const ObDASScanCtDef*>(inv_idx_ctdef);
      ObDASScanRtDef *inv_idx_scan_rtdef = static_cast<ObDASScanRtDef*>(inv_idx_rtdef);
      if (OB_FAIL(create_das_scan_iter(alloc, inv_idx_scan_ctdef, inv_idx_scan_rtdef, inv_idx_scan_iter))) {
      LOG_WARN("failed to create inv idx scan iter", K(ret));
      } else {
        inv_idx_scan_iter->set_scan_param(scan_param);
        inv_idx_iter = inv_idx_scan_iter;

        is_primary_index = inv_idx_scan_ctdef->ref_table_id_ == data_table_ctdef->ref_table_id_;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_das_scan_iter(alloc, vec_aux_ctdef->get_vec_aux_tbl_ctdef(vec_aux_ctdef->get_delta_tbl_idx(), ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN),
                                           vec_aux_rtdef->get_vec_aux_tbl_rtdef(vec_aux_ctdef->get_delta_tbl_idx()), delta_buf_table_iter))) {
      LOG_WARN("failed to create delta buf table iter", K(ret));
    } else if (OB_FAIL(create_das_scan_iter(alloc, index_id_tbl_ctdef, index_id_tbl_rtdef, index_id_table_iter))) {
      LOG_WARN("failed to create index id table iter", K(ret));
    } else if (OB_FAIL(create_das_scan_iter(alloc, vec_aux_ctdef->get_vec_aux_tbl_ctdef(vec_aux_ctdef->get_snapshot_tbl_idx(), ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN),
                                            vec_aux_rtdef->get_vec_aux_tbl_rtdef(vec_aux_ctdef->get_snapshot_tbl_idx()), snapshot_table_iter))) {
      LOG_WARN("failed to create snapshot table iter", K(ret));
    } else if (OB_FAIL(create_das_scan_iter(alloc, vid_rowkey_ctdef, vid_rowkey_rtdef, vid_rowkey_table_iter))) {
      LOG_WARN("failed to create vid rowkey table iter", K(ret));
    } else if (OB_FAIL(create_das_scan_iter(alloc, com_aux_tbl_ctdef, com_aux_tbl_rtdef, com_aux_vec_iter))) {
      LOG_WARN("failed to create data table iter", K(ret));
    } else if (OB_FAIL(create_das_scan_iter(alloc, rowkey_vid_ctdef, rowkey_vid_rtdef, rowkey_vid_table_iter))) {
      LOG_WARN("failed to create rowkey vid table iter", K(ret));
    }

    bool is_pre_filter = vec_aux_ctdef->is_pre_filter();
    bool is_post_filter = vec_aux_ctdef->is_post_filter();
    bool need_pre_lookup = is_pre_filter
                           && !data_table_ctdef->pd_expr_spec_.pushdown_filters_.empty()
                           && !is_primary_index;

    if (OB_SUCC(ret)) {
      ObDASIter *inv_idx_scan_iter_sub_tree = nullptr;
      if (need_pre_lookup) {
        if (OB_FAIL(create_local_lookup_sub_tree(scan_param, alloc, inv_idx_ctdef, inv_idx_rtdef, data_table_ctdef, data_table_rtdef,
                                                 nullptr, nullptr, related_tablet_ids, trans_desc, snapshot,
                                                 related_tablet_ids.lookup_tablet_id_, inv_idx_iter, inv_idx_scan_iter_sub_tree))) {
          LOG_WARN("failed to create pre lookup iter", K(ret));
        }
      } else {
        inv_idx_scan_iter_sub_tree = inv_idx_iter;
      }

      if (OB_SUCC(ret)) {
        ObDASIter *aux_lookup_iter = nullptr;
        ObDASHNSWScanIter *hnsw_scan_iter = nullptr;

        ObDASHNSWScanIterParam hnsw_scan_param;
        hnsw_scan_param.max_size_ = vec_aux_rtdef->eval_ctx_->is_vectorized() ? vec_aux_rtdef->eval_ctx_->max_batch_size_ : 1;
        hnsw_scan_param.eval_ctx_ = vec_aux_rtdef->eval_ctx_;
        hnsw_scan_param.exec_ctx_ = &vec_aux_rtdef->eval_ctx_->exec_ctx_;
        hnsw_scan_param.output_ = &vec_aux_ctdef->result_output_;
        hnsw_scan_param.inv_idx_scan_iter_ = inv_idx_scan_iter_sub_tree;
        hnsw_scan_param.delta_buf_iter_ = delta_buf_table_iter;
        hnsw_scan_param.index_id_iter_ = index_id_table_iter;
        hnsw_scan_param.snapshot_iter_ = snapshot_table_iter;
        hnsw_scan_param.vid_rowkey_iter_ = vid_rowkey_table_iter;
        hnsw_scan_param.com_aux_vec_iter_ = com_aux_vec_iter;
        hnsw_scan_param.rowkey_vid_iter_ = rowkey_vid_table_iter;
        hnsw_scan_param.vec_aux_ctdef_ = vec_aux_ctdef;
        hnsw_scan_param.vec_aux_rtdef_ = vec_aux_rtdef;
        hnsw_scan_param.vid_rowkey_ctdef_ = vid_rowkey_ctdef;
        hnsw_scan_param.vid_rowkey_rtdef_ = vid_rowkey_rtdef;
        hnsw_scan_param.sort_ctdef_ = sort_ctdef;
        hnsw_scan_param.sort_rtdef_ = sort_rtdef;
        hnsw_scan_param.ls_id_ = scan_param.ls_id_;
        hnsw_scan_param.tx_desc_ = trans_desc;
        hnsw_scan_param.snapshot_ = snapshot;
        uint64_t batch_count = hnsw_scan_param.max_size_;

        if (OB_FAIL(create_das_iter(alloc, hnsw_scan_param, hnsw_scan_iter))) {
          LOG_WARN("failed to create hnsw scan iter", K(ret));
        } else if (FALSE_IT(batch_count = hnsw_scan_iter->adjust_batch_count(vec_aux_rtdef->eval_ctx_->is_vectorized(), hnsw_scan_param.max_size_))) {
        } else if (OB_FALSE_IT(hnsw_scan_iter->set_related_tablet_ids(related_tablet_ids))) {
        } else if (OB_FAIL(create_local_lookup_sub_tree(scan_param, alloc, vec_aux_ctdef, vec_aux_rtdef, vid_rowkey_ctdef, vid_rowkey_rtdef,
                                                 nullptr, nullptr, related_tablet_ids, trans_desc, snapshot,
                                                 related_tablet_ids.doc_rowkey_tablet_id_, hnsw_scan_iter, aux_lookup_iter,
                                                 batch_count))) {
          LOG_WARN("failed to create aux local lookup sub tree", K(ret));
        } else if (hnsw_scan_iter->enable_using_simplified_scan() &&
                   vec_aux_ctdef->access_pk_) {
          iter_tree = aux_lookup_iter;
        } else if (OB_FAIL(create_local_lookup_sub_tree(scan_param, alloc, aux_lookup_ctdef, aux_lookup_rtdef, data_table_ctdef, data_table_rtdef,
                                                 nullptr, nullptr, related_tablet_ids, trans_desc, snapshot,
                                                 related_tablet_ids.lookup_tablet_id_, aux_lookup_iter, iter_tree,
                                                 batch_count))) {
          LOG_WARN("failed to create local lookup iter", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDASIterUtils::create_vec_ivf_lookup_tree(ObTableScanParam &scan_param,
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
  const ObDASVecAuxScanCtDef *vec_aux_ctdef = nullptr;
  ObDASVecAuxScanRtDef *vec_aux_rtdef = nullptr;
  const ObDASSortCtDef *sort_ctdef = nullptr;
  ObDASSortRtDef *sort_rtdef = nullptr;

  if (OB_ISNULL(attach_ctdef) || OB_ISNULL(attach_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(attach_ctdef), K(attach_rtdef));
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(
                 attach_ctdef, attach_rtdef, DAS_OP_TABLE_LOOKUP, lookup_ctdef, lookup_rtdef))) {
    LOG_WARN("find data table lookup def failed", K(ret));
  } else if (OB_FAIL(
                 ObDASUtils::find_target_das_def(attach_ctdef, attach_rtdef, DAS_OP_SORT, sort_ctdef, sort_rtdef))) {
    LOG_WARN("find vec aux lookup definition failed");
  } else if (OB_FAIL(ObDASUtils::find_target_das_def(
                 attach_ctdef, attach_rtdef, DAS_OP_VEC_SCAN, vec_aux_ctdef, vec_aux_rtdef))) {
    LOG_WARN("find ir scan definition failed", K(ret));
  } else {
    const ObDASScanCtDef *data_table_ctdef = lookup_ctdef->get_lookup_scan_ctdef();
    ObDASScanRtDef *data_table_rtdef = lookup_rtdef->get_lookup_scan_rtdef();
    data_table_rtdef->scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
    const ObDASBaseCtDef *inv_idx_ctdef = vec_aux_ctdef->get_inv_idx_scan_ctdef();
    ObDASBaseRtDef *inv_idx_rtdef = vec_aux_rtdef->get_inv_idx_scan_rtdef();

    ObDASIter *inv_idx_iter = nullptr;
    ObDASScanIter *centroid_table_iter = nullptr;
    ObDASScanIter *cid_vec_table_iter = nullptr;
    ObDASScanIter *rowkey_cid_table_iter = nullptr;
    ObDASScanIter *sq_meta_iter = nullptr;
    ObDASScanIter *pq_centroid_iter = nullptr;

    bool is_primary_index = false;
    if (scan_param.table_param_->is_spatial_index()) {
      if (OB_FAIL(create_gis_lookup_tree(scan_param,
                                         alloc,
                                         inv_idx_ctdef,
                                         inv_idx_rtdef,
                                         related_tablet_ids,
                                         trans_desc,
                                         snapshot,
                                         inv_idx_iter,
                                         true))) {
        LOG_WARN("failed to create gis lookup tree", K(ret));
      }
    } else {
      ObDASScanIter *inv_idx_scan_iter = nullptr;
      const ObDASScanCtDef *inv_idx_scan_ctdef = static_cast<const ObDASScanCtDef *>(inv_idx_ctdef);
      ObDASScanRtDef *inv_idx_scan_rtdef = static_cast<ObDASScanRtDef *>(inv_idx_rtdef);
      if (OB_FAIL(create_das_scan_iter(alloc, inv_idx_scan_ctdef, inv_idx_scan_rtdef, inv_idx_scan_iter))) {
        LOG_WARN("failed to create inv idx scan iter", K(ret));
      } else {
        inv_idx_scan_iter->set_scan_param(scan_param);
        inv_idx_iter = inv_idx_scan_iter;

        is_primary_index = inv_idx_scan_ctdef->ref_table_id_ == data_table_ctdef->ref_table_id_;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_das_scan_iter(
                   alloc,
                   vec_aux_ctdef->get_vec_aux_tbl_ctdef(vec_aux_ctdef->get_ivf_centroid_tbl_idx(),
                                                        ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN),
                   vec_aux_rtdef->get_vec_aux_tbl_rtdef(vec_aux_ctdef->get_ivf_centroid_tbl_idx()),
                   centroid_table_iter))) {
      LOG_WARN("failed to create delta buf table iter", K(ret));
    } else if (OB_FAIL(
                   create_das_scan_iter(alloc,
                                        vec_aux_ctdef->get_vec_aux_tbl_ctdef(vec_aux_ctdef->get_ivf_cid_vec_tbl_idx(),
                                                                             ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN),
                                        vec_aux_rtdef->get_vec_aux_tbl_rtdef(vec_aux_ctdef->get_ivf_cid_vec_tbl_idx()),
                                        cid_vec_table_iter))) {
      LOG_WARN("failed to create index id table iter", K(ret));
    } else if (OB_FAIL(create_das_scan_iter(
                   alloc,
                   vec_aux_ctdef->get_vec_aux_tbl_ctdef(vec_aux_ctdef->get_ivf_rowkey_cid_tbl_idx(),
                                                        ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN),
                   vec_aux_rtdef->get_vec_aux_tbl_rtdef(vec_aux_ctdef->get_ivf_rowkey_cid_tbl_idx()),
                   rowkey_cid_table_iter))) {
      LOG_WARN("failed to create snapshot table iter", K(ret));
    } else if (vec_aux_ctdef->algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_IVF_SQ8 &&
               OB_FAIL(create_das_scan_iter(
                   alloc,
                   vec_aux_ctdef->get_vec_aux_tbl_ctdef(vec_aux_ctdef->get_ivf_sq_meta_tbl_idx(),
                                                        ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN),
                   vec_aux_rtdef->get_vec_aux_tbl_rtdef(vec_aux_ctdef->get_ivf_sq_meta_tbl_idx()),
                   sq_meta_iter))) {
      LOG_WARN("failed to create spacial table iter", K(ret));
    } else if (vec_aux_ctdef->algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_IVF_PQ && OB_FAIL(create_das_scan_iter(
                   alloc,
                   vec_aux_ctdef->get_vec_aux_tbl_ctdef(vec_aux_ctdef->get_ivf_pq_id_tbl_idx(),
                                                        ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN),
                   vec_aux_rtdef->get_vec_aux_tbl_rtdef(vec_aux_ctdef->get_ivf_pq_id_tbl_idx()),
                   pq_centroid_iter))) {
      LOG_WARN("failed to create spacial table iter", K(ret));
    }

    bool is_post_filter = vec_aux_ctdef->is_post_filter();
    bool is_pre_filter = vec_aux_ctdef->is_pre_filter();
    bool need_pre_lookup = is_pre_filter
                           && !data_table_ctdef->pd_expr_spec_.pushdown_filters_.empty()
                           && !is_primary_index;

    if (OB_SUCC(ret)) {
      ObDASIter *inv_idx_scan_iter_sub_tree = nullptr;
      if (need_pre_lookup) {
        if (OB_FAIL(create_local_lookup_sub_tree(scan_param,
                                                 alloc,
                                                 inv_idx_ctdef,
                                                 inv_idx_rtdef,
                                                 data_table_ctdef,
                                                 data_table_rtdef,
                                                 nullptr/*attach_ctdef*/,
                                                 nullptr/*attach_rtdef*/,
                                                 related_tablet_ids,
                                                 trans_desc,
                                                 snapshot,
                                                 related_tablet_ids.lookup_tablet_id_,
                                                 inv_idx_iter,
                                                 inv_idx_scan_iter_sub_tree))) {

          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only support ivf post filter yet", K(ret));
        }
      } else {
        inv_idx_scan_iter_sub_tree = inv_idx_iter;
      }

      if (OB_SUCC(ret)) {
        ObDASIvfScanIter *ivf_scan_iter = nullptr;

        ObDASIvfScanIterParam ivf_scan_param(vec_aux_ctdef->algorithm_type_);
        ivf_scan_param.max_size_ =
            vec_aux_rtdef->eval_ctx_->is_vectorized() ? vec_aux_rtdef->eval_ctx_->max_batch_size_ : 1;
        ivf_scan_param.eval_ctx_ = vec_aux_rtdef->eval_ctx_;
        ivf_scan_param.exec_ctx_ = &vec_aux_rtdef->eval_ctx_->exec_ctx_;
        ivf_scan_param.output_ = &vec_aux_ctdef->result_output_;
        ivf_scan_param.inv_idx_scan_iter_ = inv_idx_scan_iter_sub_tree;

        ivf_scan_param.centroid_iter_ = centroid_table_iter;
        ivf_scan_param.cid_vec_iter_ = cid_vec_table_iter;
        ivf_scan_param.rowkey_cid_iter_ = rowkey_cid_table_iter;

        ivf_scan_param.vec_aux_ctdef_ = vec_aux_ctdef;
        ivf_scan_param.vec_aux_rtdef_ = vec_aux_rtdef;
        ivf_scan_param.sort_ctdef_ = sort_ctdef;
        ivf_scan_param.sort_rtdef_ = sort_rtdef;
        ivf_scan_param.ls_id_ = scan_param.ls_id_;
        ivf_scan_param.tx_desc_ = trans_desc;
        ivf_scan_param.snapshot_ = snapshot;
        if (vec_aux_ctdef->algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_IVF_SQ8) {
          ivf_scan_param.sq_meta_iter_ = sq_meta_iter;
        } else if (vec_aux_ctdef->algorithm_type_ == ObVectorIndexAlgorithmType::VIAT_IVF_PQ) {
          ivf_scan_param.pq_centroid_iter_ = pq_centroid_iter;
        }

        if (OB_FAIL(create_das_ivf_scan_iter(vec_aux_ctdef->algorithm_type_, alloc, ivf_scan_param, ivf_scan_iter))) {
          LOG_WARN("failed to create hnsw scan iter", K(ret));
        } else if (OB_FALSE_IT(ivf_scan_iter->set_related_tablet_ids(related_tablet_ids))) {
        } else if (OB_FAIL(create_local_lookup_sub_tree(
                       scan_param,
                       alloc,
                       vec_aux_ctdef,
                       vec_aux_rtdef,
                       data_table_ctdef,
                       data_table_rtdef,
                       nullptr,
                       nullptr,
                       related_tablet_ids,
                       trans_desc,
                       snapshot,
                       related_tablet_ids.lookup_tablet_id_,
                       ivf_scan_iter,
                       iter_tree))) {
          LOG_WARN("failed to create local lookup iter", K(ret));
        }
      }
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
