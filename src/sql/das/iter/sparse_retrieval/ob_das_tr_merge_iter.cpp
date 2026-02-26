/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_das_tr_merge_iter.h"
#include "sql/das/ob_das_ir_define.h"

namespace oceanbase
{
using namespace share;
namespace sql
{

ObDASTRMergeIter::ObDASTRMergeIter()
  : ObDASIter(ObDASIterType::DAS_ITER_TEXT_RETRIEVAL_MERGE),
    mem_context_(nullptr),
    myself_allocator_(lib::ObMemAttr(MTL_ID(), "SRMergeIterSelf"), OB_MALLOC_NORMAL_BLOCK_SIZE),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    tx_desc_(nullptr),
    snapshot_(nullptr),
    query_tokens_(),
    dim_weights_(),
    sr_iter_param_(),
    sparse_retrieval_iter_(nullptr),
    dim_iters_(),
    dim_iter_(),
    total_doc_cnt_scan_param_(nullptr),
    inv_scan_params_(),
    inv_agg_params_(),
    fwd_scan_params_(),
    boolean_compute_node_(nullptr),
    block_max_scan_params_(),
    block_max_iter_param_(),
    doc_length_est_param_(),
    doc_length_est_stat_cols_(),
    topk_limit_(0),
    ls_id_(),
    total_doc_cnt_tablet_id_(),
    inv_idx_tablet_id_(),
    fwd_idx_tablet_id_(),
    flags_(0),
    check_rangekey_inited_(false),
    inv_idx_tablet_switched_(false),
    is_inited_(false)
{
}

int ObDASTRMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObDASIterType::DAS_ITER_TEXT_RETRIEVAL_MERGE != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for text retrieval merge iter", K(ret), K(param));
  } else {
    ObDASTRMergeIterParam &merge_param = static_cast<ObDASTRMergeIterParam &>(param);
    ir_ctdef_ = merge_param.ir_ctdef_;
    ir_rtdef_ = merge_param.ir_rtdef_;
    tx_desc_ = merge_param.tx_desc_;
    snapshot_ = merge_param.snapshot_;
    boolean_compute_node_ = merge_param.boolean_compute_node_;
    flags_ = merge_param.flags_;
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_param.query_tokens_.count(); ++i) {
      if (OB_FAIL(query_tokens_.push_back(merge_param.query_tokens_.at(i)))) {
        LOG_WARN("failed to push back query token", K(ret));
      }
    }
    if (merge_param.dim_weights_.count() >= 1) {
      for (int64_t i = 0; OB_SUCC(ret) && i < merge_param.dim_weights_.count(); ++i) {
        if (OB_FAIL(dim_weights_.push_back(merge_param.dim_weights_.at(i)))) {
          LOG_WARN("failed to push back dim weight", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null pointer", K(ret), KP_(ir_ctdef), KP_(ir_rtdef));
    } else if (topk_mode_ && OB_FAIL(init_topk_limit())) {
      LOG_WARN("failed to init topk limit", K(ret));
    } else if (OB_UNLIKELY(!ir_ctdef_->need_inv_idx_agg() && ir_ctdef_->need_fwd_idx_agg())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inv idx agg and fwd idx agg are not both needed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      lib::ContextParam mem_param;
      mem_param.set_mem_attr(MTL_ID(), "TextMergeIter", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, mem_param))) {
        LOG_WARN("failed to create text retrieval iterator memory context", K(ret));
      }
    }
    sr_iter_param_.max_batch_size_ = merge_param.max_batch_size_; // may be greater than ir_rtdef_->eval_ctx_->max_batch_size_
    inv_idx_tablet_switched_ = false;
    is_inited_ = true;
    LOG_DEBUG("tr merge iter", K_(function_lookup_mode), K_(topk_mode), K_(daat_mode), K_(taat_mode));
  }
  return ret;
}

int ObDASTRMergeIter::init_das_iter_scan_params()
{
  int ret = OB_SUCCESS;
  const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
  if (!ir_ctdef_->need_estimate_total_doc_cnt() && 0 != dim_iter_cnt) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = myself_allocator_.alloc(sizeof(ObTableScanParam)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for total doc cnt scan param", K(ret));
    } else {
      total_doc_cnt_scan_param_ = new (buf) ObTableScanParam();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_das_iter_scan_param(
        ls_id_, total_doc_cnt_tablet_id_,
        ir_ctdef_->get_doc_agg_ctdef(),
        ir_rtdef_->get_doc_agg_rtdef(),
        tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
        *total_doc_cnt_scan_param_))) {
      LOG_WARN("failed to init total doc cnt scan param", K(ret));
    } else {
      static_cast<ObDASScanIter*>(children_[children_cnt_ - 1])->set_scan_param(*total_doc_cnt_scan_param_);
    }
  }

  if (OB_SUCC(ret) && 0 != dim_iter_cnt) {
    if (FALSE_IT(inv_scan_params_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(inv_scan_params_.init(dim_iter_cnt))) {
      LOG_WARN("failed to init inv scan params array", K(ret));
    } else if (OB_FAIL(inv_scan_params_.prepare_allocate(dim_iter_cnt))) {
      LOG_WARN("failed to prepare allocate inv scan params array", K(ret));
    }

    if (OB_FAIL(ret) || !ir_ctdef_->need_inv_idx_agg()) {
    } else if (FALSE_IT(inv_agg_params_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(inv_agg_params_.init(dim_iter_cnt))) {
      LOG_WARN("failed to init inv agg params array", K(ret));
    } else if (OB_FAIL(inv_agg_params_.prepare_allocate(dim_iter_cnt))) {
      LOG_WARN("failed to prepare allocate inv agg params array", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (!ir_ctdef_->need_fwd_idx_agg()) {
    } else if (OB_ISNULL(ir_rtdef_->eval_ctx_) || OB_UNLIKELY(ir_rtdef_->eval_ctx_->max_batch_size_ >= 1)) {
      ret = OB_NOT_IMPLEMENT;
      LOG_ERROR("fwd idx agg with max batch size not implemented", K(ret));
    } else if (FALSE_IT(fwd_scan_params_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(fwd_scan_params_.init(dim_iter_cnt))) {
      LOG_WARN("failed to init fwd scan params array", K(ret));
    } else if (OB_FAIL(fwd_scan_params_.prepare_allocate(dim_iter_cnt))) {
      LOG_WARN("failed to prepare allocate fwd scan params array", K(ret));
    }

    if (OB_SUCC(ret)) {
      void *inv_buf = nullptr;
      if (OB_ISNULL(inv_buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * dim_iter_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for inv scan param", K(ret));
      } else {
        ObTableScanParam *scan_params = static_cast<ObTableScanParam*>(inv_buf);
        for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
          inv_scan_params_[i] = new (&scan_params[i]) ObTableScanParam();
          if (OB_FAIL(init_das_iter_scan_param(
              ls_id_, inv_idx_tablet_id_,
              ir_ctdef_->get_inv_idx_scan_ctdef(), ir_rtdef_->get_inv_idx_scan_rtdef(),
              tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
              *inv_scan_params_[i]))) {
            LOG_WARN("failed to init inv scan param", K(ret));
          } else {
            static_cast<ObDASScanIter*>(children_[i])->set_scan_param(*inv_scan_params_[i]);
          }
        }
      }

      if (OB_SUCC(ret) && ir_ctdef_->need_inv_idx_agg()) {
        void *agg_buf = nullptr;
        if (OB_ISNULL(agg_buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * dim_iter_cnt))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for inv agg param", K(ret));
        } else {
          ObTableScanParam *agg_params = static_cast<ObTableScanParam*>(agg_buf);
          for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
            inv_agg_params_[i] = new (&agg_params[i]) ObTableScanParam();
            if (OB_FAIL(init_das_iter_scan_param(
                ls_id_, inv_idx_tablet_id_,
                ir_ctdef_->get_inv_idx_agg_ctdef(),
                ir_rtdef_->get_inv_idx_agg_rtdef(),
                tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
                *inv_agg_params_[i]))) {
              LOG_WARN("failed to init inv agg param", K(ret));
            } else {
              static_cast<ObDASScanIter*>(children_[i + dim_iter_cnt])->set_scan_param(*inv_agg_params_[i]);
            }
            inv_scan_params_[i]->scan_flag_.scan_order_ = ObQueryFlag::Forward;
          }
        }
      }

      if (OB_SUCC(ret) && ir_ctdef_->need_fwd_idx_agg()) {
        void *fwd_buf = nullptr;
        if (OB_ISNULL(fwd_buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * dim_iter_cnt))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for fwd scan param", K(ret));
        } else {
          ObTableScanParam *fwd_params = static_cast<ObTableScanParam*>(fwd_buf);
          for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
            fwd_scan_params_[i] = new (&fwd_params[i]) ObTableScanParam();
            if (OB_FAIL(init_das_iter_scan_param(
                ls_id_, fwd_idx_tablet_id_,
                ir_ctdef_->get_fwd_idx_agg_ctdef(),
                ir_rtdef_->get_fwd_idx_agg_rtdef(),
                tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
                *fwd_scan_params_[i]))) {
              LOG_WARN("failed to init fwd scan param", K(ret));
            } else {
              static_cast<ObDASScanIter*>(children_[i + dim_iter_cnt * 2])->set_scan_param(*fwd_scan_params_[i]);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::init_block_max_iter_param()
{
  int ret = OB_SUCCESS;
  const int64_t token_cnt = query_tokens_.count();
  block_max_scan_params_.set_allocator(&myself_allocator_);
  if (0 == token_cnt) {
    // do nothing
  } else if (OB_FAIL(block_max_iter_param_.init(*ir_ctdef_, myself_allocator_))) {
    LOG_WARN("failed to init block max iter param", K(ret));
  } else if (OB_FAIL(block_max_scan_params_.init(token_cnt))) {
    LOG_WARN("failed to init block max scan params", K(ret));
  } else if (OB_FAIL(block_max_scan_params_.prepare_allocate(token_cnt))) {
    LOG_WARN("failed to prepare allocate block max scan params", K(ret));
  } else {
    ObTableScanParam *scan_params = nullptr;
    if (OB_ISNULL(scan_params = static_cast<ObTableScanParam *>(myself_allocator_.alloc(sizeof(ObTableScanParam) * token_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for block max scan params", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < token_cnt; ++i) {
      block_max_scan_params_[i] = new (&scan_params[i]) ObTableScanParam();
      if (OB_FAIL(init_das_iter_scan_param(
          ls_id_, inv_idx_tablet_id_,
          ir_ctdef_->get_block_max_scan_ctdef(),
          ir_rtdef_->get_block_max_scan_rtdef(),
          tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
          *block_max_scan_params_[i]))) {
        LOG_WARN("failed to init block max scan param", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::init_doc_length_est_param()
{
  int ret = OB_SUCCESS;
  const ObTextAvgDocLenEstSpec &doc_len_est_spec = ir_ctdef_->avg_doc_len_est_spec_;
  doc_length_est_stat_cols_.set_allocator(&myself_allocator_);
  if (!ir_ctdef_->need_avg_doc_len_est() || !doc_len_est_spec.can_est_by_sum_skip_index_) {
    // skip
  } else if (OB_UNLIKELY(!doc_len_est_spec.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid doc length est spec", K(ret), K(doc_len_est_spec));
  } else if (OB_FAIL(doc_length_est_stat_cols_.init(doc_len_est_spec.col_types_.count()))) {
    LOG_WARN("failed to init doc length est stat cols", K(ret));
  } else if (OB_FAIL(doc_length_est_stat_cols_.push_back(
      ObSkipIndexColMeta(doc_len_est_spec.col_store_idxes_.at(0), doc_len_est_spec.col_types_.at(0))))) {
    LOG_WARN("failed to append skip index col meta", K(ret));
  } else if (OB_FAIL(doc_length_est_param_.init(
      doc_length_est_stat_cols_,
      doc_len_est_spec.scan_col_proj_,
      *inv_scan_params_[0], // TODO: use independent param with whole scan range
      true,
      true,
      true))) {
    LOG_WARN("failed to init doc length est param", K(ret));
  }
  return ret;
}

int ObDASTRMergeIter::init_das_iter_scan_param(const ObLSID &ls_id,
                                               const ObTabletID &tablet_id,
                                               const ObDASScanCtDef *ctdef,
                                               ObDASScanRtDef *rtdef,
                                               transaction::ObTxDesc *tx_desc,
                                               transaction::ObTxReadSnapshot *snapshot,
                                               common::ObArenaAllocator &allocator,
                                               ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(ctdef), KPC(rtdef), K(ls_id), K(tablet_id));
  } else {
    uint64_t tenant_id = MTL_ID();
    scan_param.tenant_id_ = tenant_id;
    scan_param.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
    scan_param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = false; // scan
    scan_param.is_for_foreign_check_ = false;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &allocator;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.force_refresh_lc_ = rtdef->force_refresh_lc_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.limit_param_ = rtdef->limit_param_;
    scan_param.need_scn_ = rtdef->need_scn_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.fb_snapshot_ = rtdef->fb_snapshot_;
    scan_param.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_NOT_NULL(tx_desc)) {
      scan_param.tx_id_ = tx_desc->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }

    if (OB_NOT_NULL(snapshot)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null snapshot", K(ret), KP(snapshot));
    }

    if (FAILEDx(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to init column ids", K(ret));
    }
  }
  return ret;
}

int ObDASTRMergeIter::create_dim_iters()
{
  int ret = OB_SUCCESS;
  if (topk_mode_) {
    if (OB_FAIL(init_block_max_iter_param())) {
      LOG_WARN("failed to init block max iter param", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      ObTextRetrievalScanIterParam iter_param;
      ObTextRetrievalBlockMaxIter *dim_iter = nullptr;
      if (OB_FAIL(init_dim_iter_param(iter_param, i))) {
        LOG_WARN("failed to init create tr iter param", K(ret));
      } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalBlockMaxIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for block max iter", K(ret));
      } else if (OB_FAIL(dim_iter->init(iter_param, block_max_iter_param_, *block_max_scan_params_[i]))) {
        LOG_WARN("failed to init text retrieval block max iter", K(ret));
      } else if (OB_FAIL(dim_iters_.push_back(dim_iter))) {
        LOG_WARN("failed to push back dim iter", K(ret));
      }
    }
  } else if (daat_mode_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      ObTextRetrievalScanIterParam iter_param;
      ObTextRetrievalDaaTTokenIter *dim_iter = nullptr;
      if (OB_FAIL(init_dim_iter_param(iter_param, i))) {
        LOG_WARN("failed to init create tr iter param", K(ret));
      } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalDaaTTokenIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for text retrieval daat token iter", K(ret));
      } else if (OB_FAIL(dim_iter->init(iter_param))) {
          LOG_WARN("failed to init text retrieval daat token iter", K(ret));
      } else if (OB_FAIL(dim_iters_.push_back(dim_iter))) {
        LOG_WARN("failed to push back dim iter", K(ret));
      }
    }
  } else if (taat_mode_) {
    ObTextRetrievalScanIterParam iter_param;
    ObTextRetrievalTokenIter *dim_iter = nullptr;
    if (OB_FAIL(init_dim_iter_param(iter_param, 0))) {
      LOG_WARN("failed to init create tr iter param", K(ret));
    } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalTokenIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for text retrieval token iter", K(ret));
    } else if (OB_FAIL(dim_iter->init(iter_param))) {
      LOG_WARN("failed to init text retrieval token iter", K(ret));
    } else {
      dim_iter_ = dim_iter;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mode", K(ret), K_(flags));
  }
  return ret;
}

int ObDASTRMergeIter::init_dim_iter_param(ObTextRetrievalScanIterParam &iter_param, const int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_) || OB_ISNULL(ir_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP_(ir_ctdef), KP_(ir_rtdef));
  } else {
    const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
    iter_param.inv_idx_scan_param_ = inv_scan_params_[idx];
    iter_param.inv_idx_scan_iter_ = static_cast<ObDASScanIter*>(children_[idx]);
    if (ir_ctdef_->need_inv_idx_agg()) {
      iter_param.inv_idx_agg_param_ = inv_agg_params_[idx];
      iter_param.inv_idx_agg_iter_ = static_cast<ObDASScanIter*>(children_[idx + dim_iter_cnt]);
      if (OB_UNLIKELY(1 != ir_ctdef_->get_inv_idx_agg_ctdef()
          ->pd_expr_spec_.pd_storage_aggregate_output_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected inv idx agg expr count", K(ret));
      } else {
        iter_param.inv_idx_agg_expr_ = ir_ctdef_->get_inv_idx_agg_ctdef()
            ->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
      }
    }
    if (OB_SUCC(ret) && ir_ctdef_->need_fwd_idx_agg()) {
      iter_param.fwd_idx_scan_param_ = fwd_scan_params_[idx];
      iter_param.fwd_idx_agg_iter_ = static_cast<ObDASScanIter*>(children_[idx + dim_iter_cnt * 2]);
      if (OB_UNLIKELY(1 != ir_ctdef_->get_fwd_idx_agg_ctdef()
          ->pd_expr_spec_.pd_storage_aggregate_output_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected fwd idx agg expr count", K(ret));
      } else {
        iter_param.fwd_idx_agg_expr_ = ir_ctdef_->get_fwd_idx_agg_ctdef()
            ->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
      }
    }
    iter_param.allocator_ = &myself_allocator_;
    iter_param.mem_context_ = mem_context_;
    iter_param.eval_ctx_ = ir_rtdef_->eval_ctx_;
    iter_param.relevance_expr_ = ir_ctdef_->relevance_expr_;
    iter_param.inv_scan_doc_length_col_ = ir_ctdef_->inv_scan_doc_length_col_;
    iter_param.inv_scan_domain_id_col_ = ir_ctdef_->inv_scan_domain_id_col_;
    iter_param.inv_idx_agg_cache_mode_ = function_lookup_mode_ && !taat_mode_;
  }
  return ret;
}

int ObDASTRMergeIter::create_sparse_retrieval_iter()
{
  int ret = OB_SUCCESS;
  sr_iter_param_.dim_weights_ = dim_weights_.count() >= 1 ? &dim_weights_ : nullptr;
  sr_iter_param_.limit_param_ = &ir_rtdef_->get_inv_idx_scan_rtdef()->limit_param_;
  sr_iter_param_.eval_ctx_ = ir_rtdef_->eval_ctx_;
  sr_iter_param_.id_proj_expr_ = ir_ctdef_->inv_scan_domain_id_col_;
  sr_iter_param_.relevance_expr_ = ir_ctdef_->relevance_expr_;
  sr_iter_param_.relevance_proj_expr_ = ir_ctdef_->relevance_proj_col_;
  sr_iter_param_.filter_expr_ = ir_ctdef_->match_filter_;
  sr_iter_param_.topk_limit_ = topk_limit_;
  if (OB_NOT_NULL(ir_ctdef_->field_boost_expr_)) {
    ObDatum *boost_datum = nullptr;
    if (OB_FAIL(ir_ctdef_->field_boost_expr_->eval(*ir_rtdef_->eval_ctx_, boost_datum))) {
      LOG_WARN("failed to eval field boost expr", K(ret));
    } else if (OB_ISNULL(boost_datum) || OB_UNLIKELY(boost_datum->is_null())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported field boost", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "null field boost is");
    } else {
      sr_iter_param_.field_boost_ = boost_datum->get_double();
      if (sr_iter_param_.field_boost_ <= 0.0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported field boost", K(ret), K(sr_iter_param_.field_boost_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "field boost < 0 is");
      } else if (query_tokens_.count() == 0) {
        // do nothing
      } else if (OB_ISNULL(sr_iter_param_.dim_weights_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null dim weights", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < dim_weights_.count(); ++i) {
          if (sr_iter_param_.dim_weights_->at(i) <= 0.0) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not supported dim weight", K(ret), K(sr_iter_param_.dim_weights_->at(i)));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "token weight < 0 is");
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (topk_mode_) {
    ObTextDaaTParam iter_param;
    ObTextBMWIter *bmw_iter = nullptr;
    if (OB_FAIL(init_daat_iter_param(iter_param))) {
      LOG_WARN("failed to init sr iter param", K(ret));
    } else if (OB_ISNULL(bmw_iter = OB_NEWx(ObTextBMWIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for text retrieval bm25 iter", K(ret));
    } else if (OB_FAIL(bmw_iter->init(iter_param))) {
      LOG_WARN("failed to init text retrieval bm25 iter", K(ret));
    } else {
      sparse_retrieval_iter_ = bmw_iter;
    }
  } else if (daat_mode_) {
    ObTextDaaTParam iter_param;
    ObTextDaaTIter *daat_iter = nullptr;
    if (OB_FAIL(init_daat_iter_param(iter_param))) {
      LOG_WARN("failed to init sr iter param", K(ret));
    } else if (OB_ISNULL(daat_iter = OB_NEWx(ObTextDaaTIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for text retrieval daat iter", K(ret));
    } else if (OB_FAIL(daat_iter->init(iter_param))) {
      LOG_WARN("failed to init text retrieval daat iter", K(ret));
    } else {
      sparse_retrieval_iter_ = daat_iter;
    }
  } else if (taat_mode_) {
    ObTextTaaTParam iter_param;
    ObTextTaaTIter *taat_iter = nullptr;
    if (OB_FAIL(init_taat_iter_param(iter_param))) {
      LOG_WARN("failed to init sr iter param", K(ret));
    } else if (OB_ISNULL(taat_iter = OB_NEWx(ObTextTaaTIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for text retrieval taat iter", K(ret));
    } else if (OB_FAIL(taat_iter->init(iter_param))) {
      LOG_WARN("failed to init text retrieval taat iter", K(ret));
    } else {
      sparse_retrieval_iter_ = taat_iter;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mode", K(ret), K_(flags));
  }
  if (OB_SUCC(ret) && function_lookup_mode_) {
    ObSRLookupIter *lookup_iter = nullptr;
    if (OB_UNLIKELY(topk_mode_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected both topk mode and function lookup mode", K(ret));
    } else if (daat_mode_) {
      if (OB_ISNULL(lookup_iter = OB_NEWx(ObSRSortedLookupIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for sorted lookup iter", K(ret));
      }
    } else if (taat_mode_) {
      if (OB_ISNULL(lookup_iter = OB_NEWx(ObSRHashLookupIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for hash lookup iter", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mode", K(ret), K_(flags));
    }
    if (FAILEDx(lookup_iter->init(sr_iter_param_, *sparse_retrieval_iter_,
                                  myself_allocator_, sr_iter_param_.max_batch_size_))) {
      LOG_WARN("failed to init lookup iter", K(ret));
    } else {
      sparse_retrieval_iter_ = lookup_iter;
    }
  }
  return ret;
}

int ObDASTRMergeIter::init_daat_iter_param(ObTextDaaTParam &iter_param)
{
  int ret = OB_SUCCESS;
  iter_param.dim_iters_ = &dim_iters_;
  iter_param.base_param_ = &sr_iter_param_;
  iter_param.allocator_ = &myself_allocator_;
  iter_param.mode_flag_ = ir_ctdef_->mode_flag_;
  iter_param.function_lookup_mode_ = function_lookup_mode_;
  iter_param.bm25_param_est_ctx_.total_doc_cnt_expr_
      = ir_ctdef_->get_doc_agg_ctdef()->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
  iter_param.bm25_param_est_ctx_.estimated_total_doc_cnt_ = ir_ctdef_->estimated_total_doc_cnt_;
  iter_param.bm25_param_est_ctx_.need_est_avg_doc_token_cnt_ = ir_ctdef_->need_avg_doc_len_est();
  iter_param.bm25_param_est_ctx_.can_est_by_sum_skip_index_ = ir_ctdef_->avg_doc_len_est_spec_.can_est_by_sum_skip_index_;
  iter_param.bm25_param_est_ctx_.avg_doc_token_cnt_expr_ = ir_ctdef_->avg_doc_token_cnt_expr_;
  iter_param.bm25_param_est_ctx_.doc_length_est_param_ = &doc_length_est_param_;
  if (query_tokens_.count() == 0) {
    // do nothing
  } else if (OB_ISNULL(iter_param.bm25_param_est_ctx_.total_doc_cnt_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null total doc cnt expr", K(ret));
  } else if (OB_FAIL(init_doc_length_est_param())) {
    LOG_WARN("failed to init doc length est param", K(ret));
  } else if (!ir_ctdef_->need_estimate_total_doc_cnt()) {
    if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(total_doc_cnt_scan_param_->pd_storage_flag_).is_aggregate_pushdown())) {
      ret = OB_NOT_IMPLEMENT;
      LOG_ERROR("aggregate without pushdown not implemented", K(ret));
    } else {
      iter_param.bm25_param_est_ctx_.total_doc_cnt_iter_ = static_cast<sql::ObDASScanIter*>(children_[children_cnt_ - 1]);
    }
  }
  if (OB_SUCC(ret)) {
    if (BOOLEAN_MODE == ir_ctdef_->mode_flag_) {
      ObSRDaaTBooleanRelevanceCollector *boolean_relevance_collector = nullptr;
      if (OB_ISNULL(boolean_relevance_collector = OB_NEWx(ObSRDaaTBooleanRelevanceCollector, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for boolean relevance collector", K(ret));
      } else if (OB_FAIL(boolean_relevance_collector->init(&myself_allocator_, dim_iters_.count(), boolean_compute_node_))) {
        LOG_WARN("failed to init boolean relevance collector", K(ret));
      } else {
        iter_param.relevance_collector_ = boolean_relevance_collector;
      }
    } else {
      int64_t should_match = ir_rtdef_->minimum_should_match_;
      ObSRDaaTInnerProductRelevanceCollector *inner_product_relevance_collector = nullptr;
      if (OB_ISNULL(inner_product_relevance_collector = OB_NEWx(ObSRDaaTInnerProductRelevanceCollector, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for inner product relevance collector", K(ret));
      } else if (OB_FAIL(inner_product_relevance_collector->init(should_match))) {
        LOG_WARN("failed to init boolean relevance collector", K(ret));
      } else {
        iter_param.relevance_collector_ = inner_product_relevance_collector;
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::init_taat_iter_param(ObTextTaaTParam &iter_param)
{
  int ret = OB_SUCCESS;
  iter_param.query_tokens_ = &query_tokens_;
  iter_param.base_param_ = &sr_iter_param_;
  iter_param.allocator_ = &myself_allocator_;
  iter_param.mode_flag_ = ir_ctdef_->mode_flag_;
  iter_param.function_lookup_mode_ = function_lookup_mode_;
  iter_param.bm25_param_est_ctx_.total_doc_cnt_expr_
      = ir_ctdef_->get_doc_agg_ctdef()->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
  iter_param.bm25_param_est_ctx_.estimated_total_doc_cnt_ = ir_ctdef_->estimated_total_doc_cnt_;
  iter_param.bm25_param_est_ctx_.need_est_avg_doc_token_cnt_ = ir_ctdef_->need_avg_doc_len_est();
  iter_param.bm25_param_est_ctx_.can_est_by_sum_skip_index_ = ir_ctdef_->avg_doc_len_est_spec_.can_est_by_sum_skip_index_;
  iter_param.bm25_param_est_ctx_.avg_doc_token_cnt_expr_ = ir_ctdef_->avg_doc_token_cnt_expr_;
  iter_param.bm25_param_est_ctx_.doc_length_est_param_ = &doc_length_est_param_;
  if (OB_ISNULL(iter_param.dim_iter_ = dim_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null dim iter", K(ret));
  } else if (OB_ISNULL(iter_param.bm25_param_est_ctx_.total_doc_cnt_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null total doc cnt expr", K(ret));
  } else if (OB_FAIL(init_doc_length_est_param())) {
    LOG_WARN("failed to init doc length est param", K(ret));
  } else if (!ir_ctdef_->need_estimate_total_doc_cnt()) {
    if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(total_doc_cnt_scan_param_->pd_storage_flag_).is_aggregate_pushdown())) {
      ret = OB_NOT_IMPLEMENT;
      LOG_ERROR("aggregate without pushdown not implemented", K(ret));
    } else {
      iter_param.bm25_param_est_ctx_.total_doc_cnt_iter_ = static_cast<sql::ObDASScanIter*>(children_[children_cnt_ - 1]);
    }
  }
  return ret;
}

int ObDASTRMergeIter::set_children_iter_rangekey()
{
  int ret = OB_SUCCESS;
  ObNewRange fwd_idx_agg_range;
  check_rangekey_inited_ = true;
  if (OB_UNLIKELY(function_lookup_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected function lookup mode", K(ret), K_(flags));
  } else if (OB_UNLIKELY(0 == query_tokens_.count())) {
    // do nothing
  } else if (OB_UNLIKELY(inv_scan_params_.empty()
      || (ir_ctdef_->need_inv_idx_agg() && inv_agg_params_.empty()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected uninited scan params", K(ret));
  } else if (ir_ctdef_->need_fwd_idx_agg() && OB_FAIL(gen_fwd_idx_scan_feak_range(fwd_idx_agg_range))) {
    LOG_WARN("failed to generate fwd idx scan range", K(ret));
  } else {
    const ExprFixedArray *exprs = &(ir_ctdef_->get_inv_idx_scan_ctdef()->pd_expr_spec_.access_exprs_);
    int64 group_id = 0;
    for (int64_t i = 0; i < exprs->count(); ++i) {
      if (T_PSEUDO_GROUP_ID == exprs->at(i)->type_) {
        group_id = exprs->at(i)->locate_expr_datum(*eval_ctx_).get_int();
      }
    }
    int64_t group_idx = ObNewRange::get_group_idx(group_id);
    ObNewRange inv_idx_scan_range;
    const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
      if (OB_FAIL(gen_inv_idx_scan_default_range(query_tokens_[i], inv_idx_scan_range))) {
        LOG_WARN("failed to generate inverted index scan range", K(ret), K(query_tokens_[i]));
      } else if (ir_ctdef_->need_inv_idx_agg()
          && OB_FAIL(inv_agg_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      } else if (FALSE_IT(inv_idx_scan_range.group_idx_ = group_idx)) {
      } else if (OB_FAIL(inv_scan_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      } else if (ir_ctdef_->need_fwd_idx_agg()
          && OB_FAIL(fwd_scan_params_[i]->key_ranges_.push_back(fwd_idx_agg_range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      } else if (topk_mode_ && ir_ctdef_->need_block_max_scan()
          && OB_FAIL(block_max_scan_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::set_children_iter_rangekey(const common::ObIArray<std::pair<ObDocIdExt, int>> &virtual_rangekeys, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  check_rangekey_inited_ = true;
  if (OB_UNLIKELY(!function_lookup_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-function lookup mode", K(ret), K_(flags));
  } else if (nullptr == sparse_retrieval_iter_) {
    if (OB_FAIL(init_das_iter_scan_params())) {
      LOG_WARN("failed to init das iter scan params", K(ret));
    } else if (OB_FAIL(create_dim_iters())) {
      LOG_WARN("failed to create dim iters", K(ret));
    } else if (OB_FAIL(create_sparse_retrieval_iter())) {
      LOG_WARN("failed to create sparse retrieval iter", K(ret));
    }
  }
  if (OB_FAIL(ret) || 0 == query_tokens_.count()) {
  } else if (OB_UNLIKELY(inv_scan_params_.empty()
      || (ir_ctdef_->need_inv_idx_agg() && inv_agg_params_.empty())
      || ir_ctdef_->need_fwd_idx_agg())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected uninited scan params", K(ret));
  } else {
    const ExprFixedArray *exprs = &(ir_ctdef_->get_inv_idx_scan_ctdef()->pd_expr_spec_.access_exprs_);
    int64 group_id = 0;
    for (int64_t i = 0; i < exprs->count(); ++i) {
      if (T_PSEUDO_GROUP_ID == exprs->at(i)->type_) {
        group_id = exprs->at(i)->locate_expr_datum(*eval_ctx_).get_int();
      }
    }
    int64_t group_idx = ObNewRange::get_group_idx(group_id);
    ObNewRange inv_idx_scan_range;
    const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (OB_FAIL(gen_inv_idx_scan_one_range(query_tokens_[i],
                                               virtual_rangekeys.at(j).first,
                                               inv_idx_scan_range))) {
          LOG_WARN("failed to generate inverted index scan range", K(ret),
                   K(query_tokens_[i]), K(virtual_rangekeys.at(j).first));
        } else if (FALSE_IT(inv_idx_scan_range.group_idx_ = group_idx)) {
        } else if (OB_FAIL(inv_scan_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
          LOG_WARN("failed to push back lookup range", K(ret));
        }
      }

      if (!ir_ctdef_->need_inv_idx_agg()) {
      } else if (OB_FAIL(gen_inv_idx_scan_default_range(query_tokens_[i], inv_idx_scan_range))) {
        LOG_WARN("failed to generate inverted index scan range", K(ret), K(query_tokens_[i]));
      } else if (OB_FAIL(inv_agg_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sparse retrieval iter", K(ret));
  } else if (OB_FAIL(static_cast<ObSRLookupIter *>(sparse_retrieval_iter_)
      ->set_hints(virtual_rangekeys, batch_size))) {
    LOG_WARN("failed to set hints", K(ret), K(batch_size));
  }
  return ret;
}

int ObDASTRMergeIter::adjust_topk_limit(const int64_t limit)
{
  int ret = OB_SUCCESS;
  topk_limit_ = limit;

  if (!topk_mode_) {
    // do nothing
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sparse retrieval iter", K(ret));
  } else {
    storage::ObTextBMWIter *bmw_iter = static_cast<storage::ObTextBMWIter *>(sparse_retrieval_iter_);
    if (OB_FAIL(bmw_iter->adjust_topk_limit(limit))) {
      LOG_WARN("failed to adjust topk limit for bmw iter", K(ret), K(limit));
    }
  }

  return ret;
}

int ObDASTRMergeIter::gen_inv_idx_scan_default_range(const ObString &query_token, ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObObj *obj_ptr = nullptr;
  common::ObArenaAllocator &ctx_alloc = mem_context_->get_arena_allocator();
  constexpr int64_t obj_cnt = INV_IDX_ROWKEY_COL_CNT * 2;
  ObObj tmp_obj;
  tmp_obj.set_string(ObVarcharType, query_token);
  // We need to ensure collation type / level between query text and token column is compatible
  tmp_obj.set_meta_type(ir_ctdef_->search_text_->obj_meta_);

  if (OB_ISNULL(buf = ctx_alloc.alloc(sizeof(ObObj) * obj_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for rowkey obj", K(ret));
  } else if (OB_ISNULL(obj_ptr = new (buf) ObObj[obj_cnt])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(ob_write_obj(ctx_alloc, tmp_obj, obj_ptr[0]))) {
    LOG_WARN("failed to write obj", K(ret));
  } else if (OB_FAIL(ob_write_obj(ctx_alloc, tmp_obj, obj_ptr[2]))) {
    LOG_WARN("failed to write obj", K(ret));
  } else {
    obj_ptr[1].set_min_value();
    obj_ptr[3].set_max_value();
    ObRowkey start_key(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
    ObRowkey end_key(&obj_ptr[2], INV_IDX_ROWKEY_COL_CNT);
    common::ObTableID inv_table_id = ir_ctdef_->get_inv_idx_scan_ctdef()->ref_table_id_;
    scan_range.table_id_ = inv_table_id;
    scan_range.start_key_.assign(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
    scan_range.end_key_.assign(&obj_ptr[2], INV_IDX_ROWKEY_COL_CNT);
    scan_range.border_flag_.set_inclusive_start();
    scan_range.border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObDASTRMergeIter::gen_inv_idx_scan_one_range(const ObString &query_token, const ObDocIdExt &doc_id, ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObObj *obj_ptr = nullptr;
  common::ObArenaAllocator &ctx_alloc = mem_context_->get_arena_allocator();
  constexpr int64_t obj_cnt = INV_IDX_ROWKEY_COL_CNT;
  ObObj tmp_obj;
  tmp_obj.set_string(ObVarcharType, query_token);
  // We need to ensure collation type / level between query text and token column is compatible
  tmp_obj.set_meta_type(ir_ctdef_->search_text_->obj_meta_);

  if (OB_ISNULL(buf = ctx_alloc.alloc(sizeof(ObObj) * obj_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for rowkey obj", K(ret));
  } else if (OB_ISNULL(obj_ptr = new (buf) ObObj[obj_cnt])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(ob_write_obj(ctx_alloc, tmp_obj, obj_ptr[0]))) {
    LOG_WARN("failed to write obj", K(ret));
  } else if (OB_FAIL(doc_id.get_datum().to_obj(obj_ptr[1], ir_ctdef_->inv_scan_domain_id_col_->obj_meta_))) {
    LOG_WARN("failed to set obj", K(ret));
  } else {
    ObRowkey row_key(obj_ptr, obj_cnt);
    common::ObTableID inv_table_id = ir_ctdef_->get_inv_idx_scan_ctdef()->ref_table_id_;
    if (OB_FAIL(scan_range.build_range(inv_table_id, row_key))) {
      LOG_WARN("failed to build lookup range", K(ret), K(inv_table_id), K(row_key));
    }
  }
  return ret;
}

int ObDASTRMergeIter::gen_fwd_idx_scan_feak_range(ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  scan_range.table_id_ = ir_ctdef_->get_fwd_idx_agg_ctdef()->ref_table_id_;
  return ret;
}

int ObDASTRMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    if (function_lookup_mode_) {
      // skip the following, which should have been done
    } else if (OB_FAIL(init_das_iter_scan_params())) {
      LOG_WARN("failed to init das iter scan params", K(ret));
    } else if (OB_FAIL(create_dim_iters())) {
      LOG_WARN("failed to create dim iters", K(ret));
    } else if (OB_FAIL(create_sparse_retrieval_iter())) {
      LOG_WARN("failed to create sparse retrieval iter", K(ret));
    } else if (OB_FAIL(set_children_iter_rangekey())) {
      LOG_WARN("failed to set children iter rangekey", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!check_rangekey_inited_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rangekey is not inited", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      if (OB_FAIL(children_[i]->do_table_scan())) {
        LOG_WARN("failed to do table scan", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::reuse_das_iter_scan_param(const ObLSID &ls_id,
                                                const ObTabletID &tablet_id,
                                                ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  const ObTabletID &old_tablet_id = scan_param.tablet_id_;
  scan_param.need_switch_param_ = scan_param.need_switch_param_
      || (old_tablet_id.is_valid() && old_tablet_id != tablet_id);
  if (!scan_param.key_ranges_.empty()) {
    scan_param.key_ranges_.reuse();
  }
  scan_param.tablet_id_ = tablet_id;
  scan_param.ls_id_ = ls_id;
  return ret;
}

int ObDASTRMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sparse retrieval iter is null", K(ret));
  } else if (query_tokens_.count() > 0) {
    if (OB_NOT_NULL(total_doc_cnt_scan_param_)) {
      if (OB_FAIL(reuse_das_iter_scan_param(ls_id_, total_doc_cnt_tablet_id_, *total_doc_cnt_scan_param_))) {
        LOG_WARN("failed to reuse total doc cnt scan param", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inv_scan_params_.count(); ++i) {
      if (OB_FAIL(reuse_das_iter_scan_param(ls_id_, inv_idx_tablet_id_, *inv_scan_params_[i]))) {
        LOG_WARN("failed to reuse inv scan param", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inv_agg_params_.count(); ++i) {
      if (OB_FAIL(reuse_das_iter_scan_param(ls_id_, inv_idx_tablet_id_, *inv_agg_params_[i]))) {
        LOG_WARN("failed to reuse inv agg param", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < block_max_scan_params_.count(); ++i) {
      if (OB_FAIL(reuse_das_iter_scan_param(ls_id_, inv_idx_tablet_id_, *block_max_scan_params_[i]))) {
        LOG_WARN("failed to reuse block max scan param", K(ret), K(i));
      }
    }
    for (int64_t i = 0; i < OB_SUCC(ret) && fwd_scan_params_.count(); ++i) {
      if (OB_FAIL(reuse_das_iter_scan_param(ls_id_, fwd_idx_tablet_id_, *fwd_scan_params_[i]))) {
        LOG_WARN("failed to reuse fwd scan param", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      if (OB_FAIL(children_[i]->reuse())) {
        LOG_WARN("failed to reuse child", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    sparse_retrieval_iter_->reuse(inv_idx_tablet_switched_);
    inv_idx_tablet_switched_ = false;
  }

  if (OB_NOT_NULL(mem_context_)) {
    mem_context_->reset_remain_one_page();
  }
  check_rangekey_inited_ = false;
  return ret;
}

int ObDASTRMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sparse retrieval iter is null", K(ret));
  } else if (!function_lookup_mode_ && OB_FAIL(set_children_iter_rangekey())) {
    LOG_WARN("failed to set children iter rangekey", K(ret));
  } else if (OB_UNLIKELY(!check_rangekey_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rangekey is not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      // TODO: update scan param
      if (OB_FAIL(children_[i]->rescan())) {
        LOG_WARN("failed to do table scan", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(total_doc_cnt_scan_param_)) {
    total_doc_cnt_scan_param_->destroy_schema_guard();
    total_doc_cnt_scan_param_->snapshot_.reset();
    total_doc_cnt_scan_param_->destroy();
    total_doc_cnt_scan_param_ = nullptr;
  }

  for (int64_t i = 0; i < inv_scan_params_.count(); ++i) {
    if (OB_NOT_NULL(inv_scan_params_[i])) {
      inv_scan_params_[i]->destroy_schema_guard();
      inv_scan_params_[i]->snapshot_.reset();
      inv_scan_params_[i]->destroy();
    }
  }
  inv_scan_params_.reset();

  for (int64_t i = 0; i < inv_agg_params_.count(); ++i) {
    if (OB_NOT_NULL(inv_agg_params_[i])) {
      inv_agg_params_[i]->destroy_schema_guard();
      inv_agg_params_[i]->snapshot_.reset();
      inv_agg_params_[i]->destroy();
    }
  }
  inv_agg_params_.reset();

  for (int64_t i = 0; i < fwd_scan_params_.count(); ++i) {
    if (OB_NOT_NULL(fwd_scan_params_[i])) {
      fwd_scan_params_[i]->destroy_schema_guard();
      fwd_scan_params_[i]->snapshot_.reset();
      fwd_scan_params_[i]->destroy();
    }
  }
  fwd_scan_params_.reset();

  for (int64_t i = 0; i < block_max_scan_params_.count(); ++i) {
    if (OB_NOT_NULL(block_max_scan_params_[i])) {
      block_max_scan_params_[i]->destroy_schema_guard();
      block_max_scan_params_[i]->snapshot_.reset();
      block_max_scan_params_[i]->destroy();
    }
  }
  block_max_scan_params_.reset();

  if (OB_NOT_NULL(boolean_compute_node_)) {
    boolean_compute_node_->release();
    boolean_compute_node_ = nullptr;
  }

  if (OB_NOT_NULL(sparse_retrieval_iter_)) {
    sparse_retrieval_iter_->reset();
    sparse_retrieval_iter_ = nullptr;
  }
  dim_iters_.reset();
  dim_iter_ = nullptr;
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  query_tokens_.reset();
  dim_weights_.reset();
  myself_allocator_.reset();
  ir_ctdef_ = nullptr;
  ir_rtdef_ = nullptr;
  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  topk_limit_ = 0;
  inv_idx_tablet_switched_ = false;
  is_inited_ = false;
  return ret;
}

int ObDASTRMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sparse retrieval iter is null", K(ret));
  } else {
    ret = sparse_retrieval_iter_->get_next_row();
  }
  return ret;
}

int ObDASTRMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sparse retrieval iter is null", K(ret));
  } else if (OB_UNLIKELY(0 == capacity)) {
    count = 0;
  } else {
    ret = sparse_retrieval_iter_->get_next_rows(capacity, count);
  }
  return ret;
}

static int get_query_tokens_by_compacting_repeated_token(ObString &query_str,
                                                         const ObCollationType &cs_type,
                                                         ObArray<ObString> &query_tokens,
                                                         ObArray<double> &boost_values,
                                                         common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  char split_token_tag = ' ';
  char split_boost_tag = '^';
  hash::ObHashMap<ObString, double> token_map;
  const int64_t ft_word_bkt_cnt = MAX(query_str.length() / 10, 2);
  if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
    LOG_WARN("failed to create token map", K(ret));
  }
  while (!query_str.empty() && OB_SUCC(ret)) {
    ObString token_str = query_str.split_on(split_token_tag);
    query_str = query_str.trim();
    if (token_str.empty()) {
      token_str = query_str;
      query_str.reset();
    }
    ObString token_key = token_str.split_on(split_boost_tag);
    double boost_value = 1.0;
    if (token_key.empty()) {
      token_key = token_str;
      token_str.reset();
    } else {
      char *boost_str = static_cast<char *>(alloc.alloc(token_str.length() + 1));
      if (OB_ISNULL(boost_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else {
        memcpy(boost_str, token_str.ptr(), token_str.length());
        boost_str[token_str.length()] = '\0';
        char *end_ptr = nullptr;
        boost_value = strtod(boost_str, &end_ptr);
        if (end_ptr != boost_str + token_str.length() || boost_value <= 0.0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported field boost", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid field boost");
        }
      }
    }
    double cur_boost = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(token_map.get_refactored(token_key, cur_boost))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get relevance", K(ret),K(token_key), K(cur_boost));
      } else if (OB_FAIL(token_map.set_refactored(token_key, boost_value, 1/*overwrite*/))) {
        LOG_WARN("failed to push data", K(ret));
      }
    } else if (OB_FAIL(token_map.set_refactored(token_key, boost_value + cur_boost, 1/*overwrite*/))) {
      LOG_WARN("failed to push data", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(query_tokens.reserve(token_map.size()))) {
    LOG_WARN("failed to reserve query tokens", K(ret));
  } else if (OB_FAIL(boost_values.reserve(token_map.size()))) {
    LOG_WARN("failed to reserve boost values", K(ret));
  }
  for (hash::ObHashMap<ObString, double>::const_iterator iter = token_map.begin();
      OB_SUCC(ret) && iter != token_map.end();
      ++iter) {
    const ObString &token = iter->first;
    ObString token_string;
    if (OB_FAIL(common::ObCharset::charset_convert(alloc, token, ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI, cs_type, token_string, common::ObCharset::CONVERT_FLAG::COPY_STRING_ON_SAME_CHARSET))) {
      LOG_WARN("failed to convert string", K(ret), K(token_string));
    } else if (OB_FAIL(query_tokens.push_back(token_string))) {
      LOG_WARN("failed to append query token", K(ret));
    } else if (OB_FAIL(boost_values.push_back(iter->second))) {
      LOG_WARN("failed to append boost value", K(ret));
    }
  }
  return ret;
}

static int get_query_tokens_directly(ObString &query_str,
                                     const ObCollationType &cs_type,
                                     ObArray<ObString> &query_tokens,
                                     ObArray<double> &boost_values,
                                     common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  char split_token_tag = ' ';
  char split_boost_tag = '^';
  while (!query_str.empty() && OB_SUCC(ret)) {
    ObString token_str = query_str.split_on(split_token_tag);
    query_str = query_str.trim();
    if (token_str.empty()) {
      token_str = query_str;
      query_str.reset();
    }
    ObString token_key = token_str.split_on(split_boost_tag);
    double boost_value = 1.0;
    if (token_key.empty()) {
      token_key = token_str;
      token_str.reset();
    } else {
      char *boost_str = static_cast<char *>(alloc.alloc(token_str.length() + 1));
      if (OB_ISNULL(boost_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else {
        memcpy(boost_str, token_str.ptr(), token_str.length());
        boost_str[token_str.length()] = '\0';
        char *end_ptr = nullptr;
        boost_value = strtod(boost_str, &end_ptr);
        if (end_ptr != boost_str + token_str.length() || boost_value <= 0.0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported field boost", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid field boost");
        }
      }
    }
    double cur_boost = 0;
    if (OB_FAIL(ret)) {
    } else {
      ObString token_string;
      if (OB_FAIL(common::ObCharset::charset_convert(alloc, token_key, ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI, cs_type, token_string, common::ObCharset::CONVERT_FLAG::COPY_STRING_ON_SAME_CHARSET))) {
        LOG_WARN("failed to convert string", K(ret), K(token_string));
      } else if (OB_FAIL(query_tokens.push_back(token_string))) {
        LOG_WARN("failed to push token", K(ret));
      } else if (OB_FAIL(boost_values.push_back(boost_value))) {
        LOG_WARN("failed to push boost", K(ret));
      }
    }
  }
  return ret;
}

int ObDASTRMergeIter::build_query_tokens(const ObDASIRScanCtDef *ir_ctdef,
                                         ObDASIRScanRtDef *ir_rtdef,
                                         common::ObIAllocator &alloc,
                                         ObArray<ObString> &query_tokens,
                                         ObArray<double> &boost_values,
                                         ObFtsEvalNode *&root_node,
                                         bool &has_duplicate_boolean_tokens)
{
  int ret = OB_SUCCESS;
  has_duplicate_boolean_tokens = false;
  ObExpr *search_text = ir_ctdef->search_text_;
  ObEvalCtx *eval_ctx = ir_rtdef->eval_ctx_;
  ObDatum *search_text_datum = nullptr;
  if (OB_ISNULL(search_text) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(search_text), KP(eval_ctx));
  } else if (query_tokens.count() != 0 || boost_values.count() != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query tokens or boost values is not empty", K(ret));
  } else if (OB_FAIL(search_text->eval(*eval_ctx, search_text_datum))) {
    LOG_WARN("expr evaluation failed", K(ret));
  } else if (0 == search_text_datum->len_) {
    // empty query text
  } else if (OB_NOT_NULL(ir_ctdef->field_boost_expr_)) {
    const ObString &search_text_string = search_text_datum->get_string();
    const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
    const ObCollationType dst_type = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;

    ObString str_dest;
    if (cs_type != dst_type) {
      ObString tmp_out;
      if (OB_FAIL(ObCharset::tolower(cs_type, search_text_string, tmp_out, alloc))) {
        LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(search_text_string));
      } else if (OB_FAIL(common::ObCharset::charset_convert(alloc, tmp_out, cs_type, dst_type, str_dest))) {
        LOG_WARN("failed to convert string", K(ret), K(cs_type), K(search_text_string));
      }
    } else if (OB_FAIL(ObCharset::tolower(cs_type, search_text_string, str_dest, alloc))){
      LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(search_text_string));
    }
    if (OB_FAIL(ret)) {
    } else {
      const bool compact_repeated_token = ir_rtdef->minimum_should_match_ <= 1;
      ObString query_str = str_dest.trim();
      if (compact_repeated_token && OB_FAIL(get_query_tokens_by_compacting_repeated_token(query_str, cs_type, query_tokens, boost_values, alloc))) {
        LOG_WARN("failed to get query tokens by compacting repeated token", K(ret));
      } else if (!compact_repeated_token && OB_FAIL(get_query_tokens_directly(query_str, cs_type, query_tokens, boost_values, alloc))) {
        LOG_WARN("failed to get query tokens directly", K(ret));
      }
    }
  } else if (BOOLEAN_MODE == ir_ctdef->mode_flag_) {
    const ObString &search_text_string = search_text_datum->get_string();
    const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
    const ObCollationType dst_type = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;

    ObString str_dest;
    if (cs_type != dst_type) {
      ObString tmp_out;
      if (OB_FAIL(ObCharset::tolower(cs_type, search_text_string, tmp_out, alloc))) {
        LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(search_text_string));
      } else if (OB_FAIL(common::ObCharset::charset_convert(alloc, tmp_out, cs_type, dst_type, str_dest))) {
        LOG_WARN("failed to convert string", K(ret), K(cs_type), K(search_text_string));
      }
    } else if (OB_FAIL(ObCharset::tolower(cs_type, search_text_string, str_dest, alloc))){
      LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(search_text_string));
    }

    void *buf = nullptr;
    FtsParserResult *fts_parser;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = (&alloc)->alloc(sizeof(FtsParserResult)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(sizeof(FtsParserResult)), K(ret));
    } else {
      fts_parser = static_cast<FtsParserResult *>(buf);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = (&alloc)->alloc(str_dest.length() + 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(sizeof(FtsParserResult)), K(ret));
    } else {
      MEMSET(buf, 0, str_dest.length() + 1);
      MEMCPY(buf, str_dest.ptr(), str_dest.length());
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(fts_parse_docment(static_cast<char *>(buf), str_dest.length(), &alloc, fts_parser))) {
    } else if (FTS_OK != fts_parser->ret_) {
      if (FTS_ERROR_MEMORY == fts_parser->ret_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (FTS_ERROR_SYNTAX == fts_parser->ret_) {
        ret = OB_ERR_PARSER_SYNTAX;
      } else if (FTS_ERROR_OTHER == fts_parser->ret_) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("failed to parse query text", K(ret), K(fts_parser->err_info_.str_));
    } else if (OB_ISNULL(fts_parser->root_)) {
      // do nothing
    } else {
      FtsNode *node = fts_parser->root_;
      ObFtsEvalNode *parant_node =nullptr;
      hash::ObHashMap<ObString, int32_t> tokens_map;
      const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
      if (OB_FAIL(tokens_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
        LOG_WARN("failed to create token map", K(ret));
      } else if (OB_FAIL(ObFtsEvalNode::fts_boolean_node_create(parant_node, node, cs_type, alloc, query_tokens, tokens_map, has_duplicate_boolean_tokens))) {
        LOG_WARN("failed to get query tokens", K(ret));
      } else {
        root_node = parant_node;
      }
    }
    LOG_DEBUG("boolean query", K(has_duplicate_boolean_tokens), K(search_text_string), K(query_tokens));
  } else {
    // TODO: FTParseHelper currently does not support deduplicate tokens
    //       We should abstract such universal analyse functors into utility structs
    const ObString &search_text_string = search_text_datum->get_string();
    const ObString &parser_name = ir_ctdef->get_inv_idx_scan_ctdef()->table_param_.get_parser_name();
    const ObString &parser_properties = ir_ctdef->get_inv_idx_scan_ctdef()->table_param_.get_parser_property();

    const ObObjMeta &meta = search_text->obj_meta_;
    int64_t doc_length = 0;
    storage::ObFTParseHelper tokenize_helper;
    common::ObSEArray<ObFTWord, 16> tokens;
    hash::ObHashMap<ObFTWord, int64_t> token_map;
    const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
    if (OB_FAIL(tokenize_helper.init(&alloc, parser_name, parser_properties))) {
      LOG_WARN("failed to init tokenize helper", K(ret));
    } else if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
      LOG_WARN("failed to create token map", K(ret));
    } else if (OB_FAIL(tokenize_helper.segment(
                           meta,
                           search_text_string.ptr(),
                           search_text_string.length(),
                           doc_length,
                           token_map))) {
      LOG_WARN("failed to segment", K(ret), K(search_text_string), K(meta), K(doc_length));
    } else {
      for (hash::ObHashMap<ObFTWord, int64_t>::const_iterator iter = token_map.begin();
          OB_SUCC(ret) && iter != token_map.end();
          ++iter) {
        const ObFTWord &token = iter->first;
        ObString token_string;
        if (OB_FAIL(ob_write_string(alloc, token.get_word().get_string(), token_string))) {
          LOG_WARN("failed to deep copy query token", K(ret));
        } else if (OB_FAIL(query_tokens.push_back(token_string))) {
          LOG_WARN("failed to append query token", K(ret));
        }
      }
    }
    LOG_DEBUG("tokenized text query:", K(ret), KPC(search_text_datum), K(query_tokens));
  }
  return ret;
}

int ObDASTRMergeIter::init_topk_limit()
{
  int ret = OB_SUCCESS;
  topk_limit_ = 0;
  ObExpr *topk_limit_expr = ir_ctdef_->topk_limit_expr_;
  ObExpr *topk_offset_expr = ir_ctdef_->topk_offset_expr_;
  if (OB_UNLIKELY(!topk_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non topk mode", K(ret));
  } else {
    int64_t limit = 0;
    int64_t offset = 0;
    ObDatum *topk_limit_datum = nullptr;
    ObDatum *topk_offset_datum = nullptr;
    if (OB_FAIL(topk_limit_expr->eval(*ir_rtdef_->eval_ctx_, topk_limit_datum))) {
      LOG_WARN("failed to eval topk limit expr", K(ret));
    } else if (nullptr != topk_offset_expr && OB_FAIL(topk_offset_expr->eval(*ir_rtdef_->eval_ctx_, topk_offset_datum))) {
      LOG_WARN("failed to eval topk offset expr", K(ret));
    } else {
      limit = (topk_limit_datum->is_null() || topk_limit_datum->get_int() < 0) ? 0 : topk_limit_datum->get_int();
      if (nullptr != topk_offset_datum) {
        offset = (topk_offset_datum->is_null() || topk_offset_datum->get_int() < 0) ? 0 : topk_offset_datum->get_int();
      }
      topk_limit_ = limit + offset;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
