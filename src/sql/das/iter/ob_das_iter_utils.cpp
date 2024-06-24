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
      ret = create_partition_scan_tree(scan_param, alloc, scan_ctdef, scan_rtdef, iter_tree);
      break;
    }
    case ITER_TREE_LOCAL_LOOKUP: {
      ret = create_local_lookup_tree(scan_param, alloc, scan_ctdef, scan_rtdef, lookup_ctdef, lookup_rtdef, related_tablet_ids, trans_desc, snapshot, iter_tree);
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
    default: {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to create das scan iter tree", K(ret));
  }

  LOG_DEBUG("create das scan iter tree", K(tree_type), K(ret));
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
        if (OB_FAIL(tr_merge_iter->set_related_tablet_ids(ls_id, related_tablet_ids))) {
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
/***************** PUBLIC END *****************/

int ObDASIterUtils::create_partition_scan_tree(storage::ObTableScanParam &scan_param,
                                               common::ObIAllocator &alloc,
                                               const ObDASScanCtDef *scan_ctdef,
                                               ObDASScanRtDef *scan_rtdef,
                                               ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  ObDASScanIterParam param;
  param.scan_ctdef_ = scan_ctdef;
  ObDASScanIter *scan_iter = nullptr;
  if (OB_FAIL(create_das_iter(alloc, param, scan_iter))) {
    LOG_WARN("failed to create das scan iter", K(ret));
  } else {
    scan_iter->set_scan_param(scan_param);
    iter_tree = scan_iter;
  }
  return ret;
}

int ObDASIterUtils::create_local_lookup_tree(ObTableScanParam &scan_param,
                                             common::ObIAllocator &alloc,
                                             const ObDASScanCtDef *scan_ctdef,
                                             ObDASScanRtDef *scan_rtdef,
                                             const ObDASScanCtDef *lookup_ctdef,
                                             ObDASScanRtDef *lookup_rtdef,
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
  index_table_param.scan_ctdef_ = scan_ctdef;
  ObDASScanIterParam data_table_param;
  data_table_param.scan_ctdef_ = lookup_ctdef;
  if (OB_FAIL(create_das_iter(alloc, index_table_param, index_table_iter))) {
    LOG_WARN("failed to create index table iter", K(ret));
  } else if (OB_FAIL(create_das_iter(alloc, data_table_param, data_table_iter))) {
    LOG_WARN("failed to create data table iter", K(ret));
  } else {
    ObDASLocalLookupIterParam lookup_param;
    lookup_param.max_size_ = lookup_rtdef->eval_ctx_->is_vectorized() ? lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
    lookup_param.eval_ctx_ = lookup_rtdef->eval_ctx_;
    lookup_param.exec_ctx_ = &lookup_rtdef->eval_ctx_->exec_ctx_;
    lookup_param.output_ = &lookup_ctdef->result_output_;
    lookup_param.default_batch_row_count_ = 1000; // hard code 1000 for local lookup
    lookup_param.index_ctdef_ = scan_ctdef;
    lookup_param.index_rtdef_ = scan_rtdef;
    lookup_param.lookup_ctdef_ = lookup_ctdef;
    lookup_param.lookup_rtdef_ = lookup_rtdef;
    lookup_param.index_table_iter_ = index_table_iter;
    lookup_param.data_table_iter_ = data_table_iter;
    lookup_param.trans_desc_ = trans_desc;
    lookup_param.snapshot_ = snapshot;
    lookup_param.rowkey_exprs_ = &lookup_ctdef->rowkey_exprs_;
    if (OB_FAIL(create_das_iter(alloc, lookup_param, lookup_iter))) {
      LOG_WARN("failed to create local lookup iter", K(ret));
    } else if (OB_FAIL(create_iter_children_array(2, alloc, lookup_iter))) {
      LOG_WARN("failed to create iter children array", K(ret));
    } else {
      lookup_iter->get_children()[0] = index_table_iter;
      lookup_iter->get_children()[1] = data_table_iter;
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
      related_tablet_ids,
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
    if (!has_sort) {
      // skip
    } else if (OB_FAIL(create_sort_sub_tree(alloc, sort_ctdef, sort_rtdef, text_retrieval_result, sort_result))) {
      LOG_WARN("failed to create sort sub tree", K(ret));
    } else {
      root_iter = sort_result;
    }
  }

  if (OB_SUCC(ret) && has_lookup) {
    ObDASIter *domain_lookup_result = nullptr;
    if (OB_FAIL(create_domain_lookup_sub_tree(
        scan_param.ls_id_,
        alloc,
        table_lookup_ctdef,
        table_lookup_rtdef,
        related_tablet_ids,
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
                                                  const ObDASRelatedTabletID &related_tablet_ids,
                                                  transaction::ObTxDesc *trans_desc,
                                                  transaction::ObTxReadSnapshot *snapshot,
                                                  ObDASIter *&retrieval_result)
          {
  int ret = OB_SUCCESS;
  ObDASTextRetrievalMergeIterParam merge_iter_param;
  ObDASTextRetrievalMergeIter *tr_merge_iter = nullptr;
  ObDASScanIterParam doc_cnt_agg_param;
  ObDASScanIter *doc_cnt_agg_iter = nullptr;

  merge_iter_param.max_size_ = ir_scan_rtdef->eval_ctx_->max_batch_size_;
  merge_iter_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
  merge_iter_param.exec_ctx_ = &ir_scan_rtdef->eval_ctx_->exec_ctx_;
  merge_iter_param.output_ = &ir_scan_ctdef->result_output_;
  merge_iter_param.ir_ctdef_ = ir_scan_ctdef;
  merge_iter_param.ir_rtdef_ = ir_scan_rtdef;
  merge_iter_param.tx_desc_ = trans_desc;
  merge_iter_param.snapshot_ = snapshot;
  if (ir_scan_ctdef->need_do_total_doc_cnt()) {
    doc_cnt_agg_param.scan_ctdef_ = ir_scan_ctdef->get_doc_id_idx_agg_ctdef();
    if (OB_FAIL(create_das_iter(alloc, doc_cnt_agg_param, doc_cnt_agg_iter))) {
      LOG_WARN("failed to create doc cnt agg scan iter", K(ret));
    } else {
      merge_iter_param.doc_cnt_iter_ = doc_cnt_agg_iter;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_das_iter(alloc, merge_iter_param, tr_merge_iter))) {
    LOG_WARN("failed to create text retrieval merge iter", K(ret));
  } else {
    ObSEArray<ObDASIter *, 16> iters;
    const ObIArray<ObString> &query_tokens = tr_merge_iter->get_query_tokens();
    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens.count(); ++i) {
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

      ObDASScanIterParam inv_idx_scan_iter_param;
      ObDASScanIter *inv_idx_scan_iter = nullptr;
      inv_idx_scan_iter_param.scan_ctdef_ = ir_scan_ctdef->get_inv_idx_scan_ctdef();
      ObDASScanIterParam inv_idx_agg_iter_param;
      ObDASScanIter *inv_idx_agg_iter = nullptr;
      inv_idx_agg_iter_param.scan_ctdef_ = ir_scan_ctdef->get_inv_idx_agg_ctdef();
      ObDASScanIterParam fwd_idx_iter_param;
      ObDASScanIter *fwd_idx_iter = nullptr;
      fwd_idx_iter_param.scan_ctdef_ = ir_scan_ctdef->get_fwd_idx_agg_ctdef();
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
        if (OB_FAIL(create_das_iter(alloc, retrieval_param, retrieval_iter))) {
          LOG_WARN("failed to create text retrieval iter", K(ret));
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
      const int64_t tr_merge_children_cnt = ir_scan_ctdef->need_do_total_doc_cnt() ? iters.count() + 1 : iters.count();
      if (0 != tr_merge_children_cnt
          && OB_FAIL(create_iter_children_array(tr_merge_children_cnt, alloc, tr_merge_iter))) {
        LOG_WARN("failed to alloc text retrieval merge iter children", K(ret), K(tr_merge_children_cnt));
      } else {
        for (int64_t i = 0; i < iters.count(); ++i) {
          tr_merge_children[i] = iters.at(i);
        }
        if (ir_scan_ctdef->need_do_total_doc_cnt()) {
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

int ObDASIterUtils::create_domain_lookup_sub_tree(const ObLSID &ls_id,
                                                  common::ObIAllocator &alloc,
                                                  const ObDASTableLookupCtDef *table_lookup_ctdef,
                                                  ObDASTableLookupRtDef *table_lookup_rtdef,
                                                  const ObDASRelatedTabletID &related_tablet_ids,
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

    if (OB_FAIL(create_das_iter(alloc, doc_id_table_param, doc_id_table_iter))) {
      LOG_WARN("failed to create doc id table iter", K(ret));
    } else {
      // TODO: set to batch size after support formal vectorization
      doc_id_lookup_param.max_size_ = 1;
      // doc_id_lookup_param.max_size_ = aux_lookup_rtdef->eval_ctx_->is_vectorized()
      //     ? aux_lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
      doc_id_lookup_param.eval_ctx_ = aux_lookup_rtdef->eval_ctx_;
      doc_id_lookup_param.exec_ctx_ = &aux_lookup_rtdef->eval_ctx_->exec_ctx_;
      doc_id_lookup_param.output_ = &aux_lookup_ctdef->result_output_;
      doc_id_lookup_param.default_batch_row_count_ = 1;
      doc_id_lookup_param.index_ctdef_ = aux_lookup_ctdef->get_doc_id_scan_ctdef();
      doc_id_lookup_param.index_rtdef_ = aux_lookup_rtdef->get_doc_id_scan_rtdef();
      doc_id_lookup_param.lookup_ctdef_ = aux_lookup_ctdef->get_lookup_scan_ctdef();
      doc_id_lookup_param.lookup_rtdef_ = aux_lookup_rtdef->get_lookup_scan_rtdef();
      doc_id_lookup_param.index_table_iter_ = doc_id_iter;
      doc_id_lookup_param.data_table_iter_ = doc_id_table_iter;
      doc_id_lookup_param.trans_desc_ = trans_desc;
      doc_id_lookup_param.snapshot_ = snapshot;
      doc_id_lookup_param.rowkey_exprs_ = &aux_lookup_ctdef->get_lookup_scan_ctdef()->rowkey_exprs_;
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

  if (OB_SUCC(ret)) {
    ObDASScanIter *data_table_iter = nullptr;
    ObDASScanIterParam data_table_param;
    data_table_param.scan_ctdef_ = table_lookup_ctdef->get_lookup_scan_ctdef();
    if (OB_FAIL(create_das_iter(alloc, data_table_param, data_table_iter))) {
      LOG_WARN("failed to create data table lookup scan iter", K(ret));
    } else {
      // TODO: set to batch size after support formal vectorization
      main_lookup_param.max_size_ = 1;
      // main_lookup_param.max_size_ = table_lookup_rtdef->eval_ctx_->is_vectorized()
      //     ? aux_lookup_rtdef->eval_ctx_->max_batch_size_ : 1;
      main_lookup_param.eval_ctx_ = table_lookup_rtdef->eval_ctx_;
      main_lookup_param.exec_ctx_ = &table_lookup_rtdef->eval_ctx_->exec_ctx_;
      main_lookup_param.output_ = &table_lookup_ctdef->result_output_;
      main_lookup_param.default_batch_row_count_ = 1;
      main_lookup_param.index_ctdef_ = table_lookup_ctdef->get_rowkey_scan_ctdef();
      main_lookup_param.index_rtdef_ = table_lookup_rtdef->get_rowkey_scan_rtdef();
      main_lookup_param.lookup_ctdef_ = table_lookup_ctdef->get_lookup_scan_ctdef();
      main_lookup_param.lookup_rtdef_ = table_lookup_rtdef->get_lookup_scan_rtdef();
      main_lookup_param.index_table_iter_ = doc_id_lookup_iter;
      main_lookup_param.data_table_iter_ = data_table_iter;
      main_lookup_param.trans_desc_ = trans_desc;
      main_lookup_param.snapshot_ = snapshot;
      main_lookup_param.rowkey_exprs_ = &table_lookup_ctdef->get_lookup_scan_ctdef()->rowkey_exprs_;
      if (OB_FAIL(create_das_iter(alloc, main_lookup_param, main_lookup_iter))) {
        LOG_WARN("failed to create das iter", K(ret));
      } else if (OB_FAIL(create_iter_children_array(2, alloc, main_lookup_iter))) {
        LOG_WARN("failed to create iter children array", K(ret));
      } else {
        main_lookup_iter->get_children()[0] = doc_id_lookup_iter;
        main_lookup_iter->get_children()[1] = data_table_iter;
        data_table_iter->set_scan_param(main_lookup_iter->get_lookup_param());
        main_lookup_iter->set_tablet_id(related_tablet_ids.lookup_tablet_id_);
        main_lookup_iter->set_ls_id(ls_id);
      }
    }
  }

  if (OB_SUCC(ret)) {
    domain_lookup_result = main_lookup_iter;
  }

  return ret;
}

int ObDASIterUtils::create_sort_sub_tree(common::ObIAllocator &alloc,
                                         const ObDASSortCtDef *sort_ctdef,
                                         ObDASSortRtDef *sort_rtdef,
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
    lookup_param.default_batch_row_count_ = 10000; // hard code 10000 for global lookup
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
