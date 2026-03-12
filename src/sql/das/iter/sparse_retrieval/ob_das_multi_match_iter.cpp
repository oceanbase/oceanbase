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
#include "ob_das_multi_match_iter.h"
#include "ob_das_tr_merge_iter.h"

namespace oceanbase
{
using namespace share;
namespace sql
{

#define INIT_FIXED_ARRAY(array, count, allocator)                     \
if (OB_SUCC(ret)) {                                                   \
  array.set_allocator(&allocator);                                    \
  if (OB_FAIL(array.init(count))) {                                   \
    LOG_WARN("failed to init "#array, K(ret), K(count));              \
  } else if (OB_FAIL(array.prepare_allocate(count))) {                \
    LOG_WARN("failed to prepare allocate "#array, K(ret), K(count));  \
  }                                                                   \
}

#define ASSIGN_FIXED_ARRAY(array, other, allocator)                   \
if (OB_SUCC(ret)) {                                                   \
  array.set_allocator(&allocator);                                    \
  if (OB_FAIL(array.assign(other))) {                                 \
    LOG_WARN("failed to assign "#array, K(ret));                      \
  }                                                                   \
}

ObDASMultiMatchIter::ObDASMultiMatchIter()
  : ObDASIter(ObDASIterType::DAS_ITER_ES_MATCH),
    mem_context_(nullptr),
    myself_allocator_(lib::ObMemAttr(MTL_ID(), "MMatchIterSelf"), OB_MALLOC_NORMAL_BLOCK_SIZE),
    es_match_ctdef_(nullptr),
    es_match_rtdef_(nullptr),
    ir_scan_ctdefs_(),
    ir_scan_rtdefs_(),
    tx_desc_(nullptr),
    snapshot_(nullptr),
    query_tokens_(),
    token_weights_(),
    field_boosts_(),
    max_batch_size_(0),
    sr_iter_params_(),
    sparse_retrieval_iter_(nullptr),
    dim_iters_(),
    inv_scan_params_(),
    inv_agg_params_(),
    fwd_scan_params_(),
    block_max_scan_params_(),
    total_doc_cnt_scan_params_(),
    bm25_param_est_ctxs_(),
    block_max_iter_param_(),
    topk_limit_(0),
    ls_id_(),
    fts_tablet_ids_(),
    check_rangekey_inited_(false),
    is_inited_(false)
{}

int ObDASMultiMatchIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(ObDASIterType::DAS_ITER_ES_MATCH != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type", K(ret), K_(param.type));
  } else {
    ObDASMultiMatchIterParam &match_param = static_cast<ObDASMultiMatchIterParam &>(param);
    es_match_ctdef_ = match_param.es_match_ctdef_;
    es_match_rtdef_ = match_param.es_match_rtdef_;
    tx_desc_ = match_param.tx_desc_;
    snapshot_ = match_param.snapshot_;
    max_batch_size_ = match_param.max_batch_size_;
    ASSIGN_FIXED_ARRAY(ir_scan_ctdefs_, match_param.ir_scan_ctdefs_, myself_allocator_);
    ASSIGN_FIXED_ARRAY(ir_scan_rtdefs_, match_param.ir_scan_rtdefs_, myself_allocator_);
    ASSIGN_FIXED_ARRAY(query_tokens_, match_param.query_tokens_, myself_allocator_);
    ASSIGN_FIXED_ARRAY(token_weights_, match_param.token_weights_, myself_allocator_);
    INIT_FIXED_ARRAY(field_boosts_, field_cnt(), myself_allocator_);
    for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt(); ++i) {
      ObDatum *field_boost_datum = nullptr;
      if (OB_FAIL(ir_scan_ctdefs_.at(i)->field_boost_expr_
          ->eval(*ir_scan_rtdefs_.at(i)->eval_ctx_, field_boost_datum))) {
        LOG_WARN("failed to eval field boost expr", K(ret));
      } else if (OB_ISNULL(field_boost_datum) || OB_UNLIKELY(field_boost_datum->is_null())
          || OB_UNLIKELY(field_boost_datum->get_double() <= 0.0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid field boost", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH: field boosts must be positive");
      } else {
        field_boosts_.at(i) = field_boost_datum->get_double();
      }
    }
    INIT_FIXED_ARRAY(fts_tablet_ids_, field_cnt(), myself_allocator_);
    if (OB_FAIL(ret)) {
    } else if (is_topk_mode() && OB_FAIL(ObDASTRMergeIter::init_topk_limit(
        ir_scan_ctdefs_.at(0), *es_match_rtdef_->eval_ctx_, topk_limit_))) {
      LOG_WARN("failed to init topk limit", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      lib::ContextParam mem_param;
      mem_param.set_mem_attr(MTL_ID(), "MultiMatchIter", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, mem_param))) {
        LOG_WARN("failed to create multi match iterator memory context", K(ret));
      }
    }
    if (OB_SUCC(ret) && ObMatchOperator::MATCH_OPERATOR_AND == es_match_rtdef_->match_operator_) {
      es_match_rtdef_->minimum_should_match_ = token_cnt();
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObDASMultiMatchIter::init_das_iter_scan_params()
{
  int ret = OB_SUCCESS;
  INIT_FIXED_ARRAY(inv_scan_params_, dim_cnt(), myself_allocator_);
  if (ir_scan_ctdefs_.at(0)->need_inv_idx_agg()) {
    INIT_FIXED_ARRAY(inv_agg_params_, dim_cnt(), myself_allocator_);
  }
  if (ir_scan_ctdefs_.at(0)->need_fwd_idx_agg()) {
    INIT_FIXED_ARRAY(fwd_scan_params_, dim_cnt(), myself_allocator_);
  }
  if (is_topk_mode()) {
    INIT_FIXED_ARRAY(block_max_scan_params_, dim_cnt(), myself_allocator_);
    if (FAILEDx(block_max_iter_param_.init(*ir_scan_ctdefs_.at(0), myself_allocator_))) {
      LOG_WARN("failed to init block max iter param", K(ret));
    }
  }
  if (!ir_scan_ctdefs_.at(0)->need_estimate_total_doc_cnt()) {
    INIT_FIXED_ARRAY(total_doc_cnt_scan_params_, sr_cnt(), myself_allocator_);
    // TODO: total_doc_cnt_iter may be shared among estimators
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt(); ++i) {
    const ObDASIRScanCtDef *ir_scan_ctdef = ir_scan_ctdefs_.at(i);
    ObDASIRScanRtDef *ir_scan_rtdef = ir_scan_rtdefs_.at(i);
    void *buf = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * token_cnt()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for scan params", K(ret));
    } else {
      ObTableScanParam *scan_params = static_cast<ObTableScanParam *>(buf);
      for (int64_t j = 0; OB_SUCC(ret) && j < token_cnt(); ++j) {
        inv_scan_params_[i * token_cnt() + j] = new (scan_params + j) ObTableScanParam();
        if (OB_FAIL(ObDASTRMergeIter::init_das_iter_scan_param(
            ls_id_, fts_tablet_ids_.at(i).inv_idx_tablet_id_,
            ir_scan_ctdef->get_inv_idx_scan_ctdef(), ir_scan_rtdef->get_inv_idx_scan_rtdef(),
            tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
            *inv_scan_params_[i * token_cnt() + j]))) {
          LOG_WARN("failed to init scan param", K(ret));
        } else {
          static_cast<ObDASScanIter *>(children_[i * token_cnt() + j])
              ->set_scan_param(*inv_scan_params_[i * token_cnt() + j]);
        }
      }
    }
    if (OB_FAIL(ret) || !ir_scan_ctdef->need_inv_idx_agg()) {
    } else if (OB_ISNULL(buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * token_cnt()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for scan params", K(ret));
    } else {
      ObTableScanParam *scan_params = static_cast<ObTableScanParam *>(buf);
      for (int64_t j = 0; OB_SUCC(ret) && j < token_cnt(); ++j) {
        inv_agg_params_[i * token_cnt() + j] = new (scan_params + j) ObTableScanParam();
        if (OB_FAIL(ObDASTRMergeIter::init_das_iter_scan_param(
            ls_id_, fts_tablet_ids_.at(i).inv_idx_tablet_id_,
            ir_scan_ctdef->get_inv_idx_agg_ctdef(), ir_scan_rtdef->get_inv_idx_agg_rtdef(),
            tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
            *inv_agg_params_[i * token_cnt() + j]))) {
          LOG_WARN("failed to init scan param", K(ret));
        } else {
          static_cast<ObDASScanIter *>(children_[i * token_cnt() + j + dim_cnt()])
              ->set_scan_param(*inv_agg_params_[i * token_cnt() + j]);
          inv_scan_params_[i * token_cnt() + j]->scan_flag_.scan_order_ = ObQueryFlag::Forward;
        }
      }
    }
    if (OB_FAIL(ret) || !ir_scan_ctdef->need_fwd_idx_agg()) {
    } else if (OB_ISNULL(buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * token_cnt()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for scan params", K(ret));
    } else {
      ObTableScanParam *scan_params = static_cast<ObTableScanParam *>(buf);
      for (int64_t j = 0; OB_SUCC(ret) && j < token_cnt(); ++j) {
        fwd_scan_params_[i * token_cnt() + j] = new (scan_params + j) ObTableScanParam();
        if (OB_FAIL(ObDASTRMergeIter::init_das_iter_scan_param(
            ls_id_, fts_tablet_ids_.at(i).fwd_idx_tablet_id_,
            ir_scan_ctdef->get_fwd_idx_agg_ctdef(), ir_scan_rtdef->get_fwd_idx_agg_rtdef(),
            tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
            *fwd_scan_params_[i * token_cnt() + j]))) {
          LOG_WARN("failed to init scan param", K(ret));
        } else {
          static_cast<ObDASScanIter *>(children_[i * token_cnt() + j + 2 * dim_cnt()])
              ->set_scan_param(*fwd_scan_params_[i * token_cnt() + j]);
        }
      }
    }
    if (OB_FAIL(ret) || !ir_scan_ctdef->has_pushdown_topk()) {
    } else if (OB_ISNULL(buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * token_cnt()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for scan params", K(ret));
    } else {
      ObTableScanParam *scan_params = static_cast<ObTableScanParam *>(buf);
      for (int64_t j = 0; OB_SUCC(ret) && j < token_cnt(); ++j) {
        block_max_scan_params_[i * token_cnt() + j] = new (scan_params + j) ObTableScanParam();
        if (OB_FAIL(ObDASTRMergeIter::init_das_iter_scan_param(
            ls_id_, fts_tablet_ids_.at(i).inv_idx_tablet_id_,
            ir_scan_ctdef->get_block_max_scan_ctdef(), ir_scan_rtdef->get_block_max_scan_rtdef(),
            tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
            *block_max_scan_params_[i * token_cnt() + j]))) {
          LOG_WARN("failed to init scan param", K(ret));
        }
      }
    }
    if (OB_FAIL(ret) || ir_scan_ctdef->need_estimate_total_doc_cnt()) {
    } else if (i > 0 && !is_two_level_merge()) {
    } else if (OB_ISNULL(buf = myself_allocator_.alloc(sizeof(ObTableScanParam)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for scan param", K(ret));
    } else {
      total_doc_cnt_scan_params_[i] = new (buf) ObTableScanParam();
      if (OB_FAIL(ObDASTRMergeIter::init_das_iter_scan_param(
          ls_id_, fts_tablet_ids_.at(i).domain_id_idx_tablet_id_,
          ir_scan_ctdef->get_doc_agg_ctdef(), ir_scan_rtdef->get_doc_agg_rtdef(),
          tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
          *total_doc_cnt_scan_params_[i]))) {
        LOG_WARN("failed to init scan param", K(ret));
      } else {
        static_cast<ObDASScanIter *>(children_[children_cnt_ - sr_cnt() + i])
            ->set_scan_param(*total_doc_cnt_scan_params_[i]);
      }
    }
  }
  return ret;
}

int ObDASMultiMatchIter::create_dim_iters()
{
  int ret = OB_SUCCESS;
  if (!is_two_level_merge()) {
    INIT_FIXED_ARRAY(dim_iters_, 1, myself_allocator_);
    INIT_FIXED_ARRAY(dim_iters_[0], dim_cnt(), myself_allocator_);
  } else {
    INIT_FIXED_ARRAY(dim_iters_, field_cnt(), myself_allocator_);
    for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt(); ++i) {
      INIT_FIXED_ARRAY(dim_iters_[i], token_cnt(), myself_allocator_);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_topk_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_cnt(); ++i) {
      ObTextRetrievalScanIterParam iter_param;
      ObTextRetrievalBlockMaxIter *dim_iter = nullptr;
      if (OB_FAIL(init_dim_iter_param(iter_param, i))) {
        LOG_WARN("failed to init dim iter param", K(ret));
      } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalBlockMaxIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for dim iter", K(ret));
      } else if (OB_FAIL(dim_iter->init(iter_param, block_max_iter_param_,
                                        *block_max_scan_params_[i]))) {
          LOG_WARN("failed to init dim iter", K(ret));
      } else if (!is_two_level_merge()) {
        dim_iters_[0][i] = dim_iter;
      } else {
        dim_iters_[i / token_cnt()][i % token_cnt()] = dim_iter;
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_cnt(); ++i) {
      ObTextRetrievalScanIterParam iter_param;
      ObTextRetrievalDaaTTokenIter *dim_iter = nullptr;
      if (OB_FAIL(init_dim_iter_param(iter_param, i))) {
        LOG_WARN("failed to init dim iter param", K(ret));
      } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalDaaTTokenIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for dim iter", K(ret));
      } else if (OB_FAIL(dim_iter->init(iter_param))) {
          LOG_WARN("failed to init dim iter", K(ret));
      } else if (!is_two_level_merge()) {
        dim_iters_[0][i] = dim_iter;
      } else {
        dim_iters_[i / token_cnt()][i % token_cnt()] = dim_iter;
      }
    }
  }
  return ret;
}

int ObDASMultiMatchIter::init_dim_iter_param(ObTextRetrievalScanIterParam &iter_param,
                                             const int64_t idx)
{
  int ret = OB_SUCCESS;
  const int64_t field_idx = idx / token_cnt();
  const ObDASIRScanCtDef *ir_scan_ctdef = ir_scan_ctdefs_.at(field_idx);
  ObDASIRScanRtDef *ir_scan_rtdef = ir_scan_rtdefs_.at(field_idx);
  iter_param.allocator_ = &myself_allocator_;
  iter_param.inv_idx_scan_param_ = inv_scan_params_[idx];
  iter_param.inv_idx_scan_iter_ = static_cast<ObDASScanIter *>(children_[idx]);
  if (ir_scan_ctdef->need_inv_idx_agg()) {
    iter_param.inv_idx_agg_param_ = inv_agg_params_[idx];
    iter_param.inv_idx_agg_iter_ = static_cast<ObDASScanIter *>(children_[idx + dim_cnt()]);
    if (OB_UNLIKELY(1 != ir_scan_ctdef->get_inv_idx_agg_ctdef()
        ->pd_expr_spec_.pd_storage_aggregate_output_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected agg output count", K(ret));
    } else {
      iter_param.inv_idx_agg_expr_ = ir_scan_ctdef->get_inv_idx_agg_ctdef()
          ->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
    }
  }
  if (OB_SUCC(ret) && ir_scan_ctdef->need_fwd_idx_agg()) {
    iter_param.fwd_idx_scan_param_ = fwd_scan_params_[idx];
    iter_param.fwd_idx_agg_iter_ = static_cast<ObDASScanIter*>(children_[idx + 2 * dim_cnt()]);
    if (OB_UNLIKELY(1 != ir_scan_ctdef->get_fwd_idx_agg_ctdef()
        ->pd_expr_spec_.pd_storage_aggregate_output_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected agg output count", K(ret));
    } else {
      iter_param.fwd_idx_agg_expr_ = ir_scan_ctdef->get_fwd_idx_agg_ctdef()
          ->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
    }
  }
  iter_param.eval_ctx_ = ir_scan_rtdef->eval_ctx_;
  iter_param.relevance_expr_ = ir_scan_ctdef->relevance_expr_;
  iter_param.inv_scan_doc_length_col_ = ir_scan_ctdef->inv_scan_doc_length_col_;
  iter_param.inv_scan_domain_id_col_ = ir_scan_ctdef->inv_scan_domain_id_col_;
  iter_param.reuse_inv_idx_agg_res_ = false; // function_lookup_mode_ && !taat_mode_
  return ret;
}

int ObDASMultiMatchIter::create_sparse_retrieval_iter() {
  int ret = OB_SUCCESS;
  INIT_FIXED_ARRAY(sr_iter_params_, sr_cnt(), myself_allocator_);
  INIT_FIXED_ARRAY(bm25_param_est_ctxs_, sr_cnt(), myself_allocator_);
  ObArray<ObSRBlockMaxTopKIterImpl *> topk_iters;
  if (OB_SUCC(ret) && is_two_level_merge() && OB_FAIL(topk_iters.prepare_allocate(field_cnt()))) {
    LOG_WARN("failed to prepare allocate topk iters", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sr_cnt(); ++i) {
    sr_iter_params_[i].limit_param_ = &ir_scan_rtdefs_.at(i)
        ->get_inv_idx_scan_rtdef()->limit_param_;
    sr_iter_params_[i].eval_ctx_ = ir_scan_rtdefs_.at(i)->eval_ctx_;
    sr_iter_params_[i].id_proj_expr_ = ir_scan_ctdefs_.at(i)->inv_scan_domain_id_col_;
    sr_iter_params_[i].relevance_expr_ = nullptr;
    sr_iter_params_[i].relevance_proj_expr_ = ir_scan_ctdefs_.at(i)->relevance_proj_col_;
    sr_iter_params_[i].filter_expr_ = ir_scan_ctdefs_.at(i)->match_filter_;
    sr_iter_params_[i].topk_limit_ = topk_limit_;
    sr_iter_params_[i].max_batch_size_ = max_batch_size_;
    sr_iter_params_[i].need_set_score_norm_ = !is_two_level_merge()
        && SCORE_NORM_MIN_MAX == es_match_rtdef_->score_norm_function_;
    if (is_topk_mode()) {
      ObTextDaaTParam iter_param;
      ObTextBMWIter *bmw_iter = nullptr;
      if (OB_FAIL(init_daat_iter_param(iter_param, i))) {
        LOG_WARN("failed to init daat iter param", K(ret));
      } else if (OB_ISNULL(bmw_iter = OB_NEWx(ObTextBMWIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for bmw iter", K(ret));
      } else if (OB_FAIL(bmw_iter->init(iter_param))) {
        LOG_WARN("failed to init bmw iter", K(ret));
      } else if (!is_two_level_merge()) {
        sparse_retrieval_iter_ = bmw_iter;
      } else {
        topk_iters[i] = bmw_iter;
      }
    } else {
      ObTextDaaTParam iter_param;
      ObTextDaaTIter *daat_iter = nullptr;
      if (OB_FAIL(init_daat_iter_param(iter_param, i))) {
        LOG_WARN("failed to init daat iter param", K(ret));
      } else if (OB_ISNULL(daat_iter = OB_NEWx(ObTextDaaTIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for daat iter", K(ret));
      } else if (OB_FAIL(daat_iter->init(iter_param))) {
        LOG_WARN("failed to init daat iter", K(ret));
      } else {
        sparse_retrieval_iter_ = daat_iter;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_two_level_merge()) {
    ObSRDisjunctiveMaxIterParam iter_param;
    ObSRDisjunctiveMaxIter *disj_max_iter = nullptr;
    iter_param.es_match_ctdef_ = es_match_ctdef_;
    iter_param.es_match_rtdef_ = es_match_rtdef_;
    iter_param.ir_scan_ctdefs_ = &ir_scan_ctdefs_;
    iter_param.ir_scan_rtdefs_ = &ir_scan_rtdefs_;
    iter_param.topk_limit_ = topk_limit_;
    if (OB_ISNULL(disj_max_iter = OB_NEWx(ObSRDisjunctiveMaxIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for disjunctive max iter", K(ret));
    } else if (OB_FAIL(disj_max_iter->init(iter_param, topk_iters, myself_allocator_))) {
      LOG_WARN("failed to init disjunctive max iter", K(ret));
    } else {
      sparse_retrieval_iter_ = disj_max_iter;
    }
  }
  return ret;
}

int ObDASMultiMatchIter::init_daat_iter_param(ObTextDaaTParam &iter_param, const int64_t idx)
{
  int ret = OB_SUCCESS;
  iter_param.dim_iters_ = &dim_iters_[idx];
  iter_param.base_param_ = &sr_iter_params_[idx];
  iter_param.allocator_ = &myself_allocator_;
  iter_param.bm25_param_est_ctx_ = &bm25_param_est_ctxs_[idx];
  iter_param.mode_flag_ = ir_scan_ctdefs_.at(idx)->mode_flag_;
  iter_param.function_lookup_mode_ = false;
  if (!is_two_level_merge()) {
    if (OB_FAIL(bm25_param_est_ctxs_[idx].init(ir_scan_ctdefs_, inv_scan_params_,
                                               &myself_allocator_))) {
      LOG_WARN("failed to init bm25 param est ctx", K(ret));
    }
  } else {
    ObSEArray<const ObDASIRScanCtDef *, 1> ir_scan_ctdefs;
    if (OB_FAIL(ir_scan_ctdefs.push_back(ir_scan_ctdefs_.at(idx)))) {
      LOG_WARN("failed to push back ir scan ctdef", K(ret));
    } else if (OB_FAIL(bm25_param_est_ctxs_[idx].init(ir_scan_ctdefs, inv_scan_params_,
                                                      &myself_allocator_))) {
      LOG_WARN("failed to init bm25 param est ctx", K(ret));
    }
  }
  if (OB_SUCC(ret) && !ir_scan_ctdefs_.at(idx)->need_estimate_total_doc_cnt()) {
    if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(total_doc_cnt_scan_params_[idx]
        ->pd_storage_flag_).is_aggregate_pushdown())) {
      ret = OB_NOT_IMPLEMENT;
      LOG_ERROR("aggregate without pushdown not implemented", K(ret));
    } else {
      bm25_param_est_ctxs_[idx].total_doc_cnt_iter_ =
          static_cast<sql::ObDASScanIter *>(children_[children_cnt_ - sr_cnt() + idx]);
    }
  }
  if (OB_SUCC(ret)) {
    ObMultiMatchRelevanceCollector *collector = nullptr;
    if (OB_ISNULL(collector = OB_NEWx(ObMultiMatchRelevanceCollector, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for multi match relevance collector", K(ret));
    } else if (!is_two_level_merge()) {
      if (OB_FAIL(collector->init(&myself_allocator_, field_boosts_, token_weights_,
                                  es_match_rtdef_->match_boost_,
                                  es_match_rtdef_->minimum_should_match_,
                                  es_match_rtdef_->match_fields_type_))) {
        LOG_WARN("failed to init multi match relevance collector", K(ret));
      }
    } else {
      ObSEArray<double, 1> field_boosts;
      if (OB_FAIL(field_boosts.push_back(field_boosts_.at(idx)))) {
        LOG_WARN("failed to push back field boost", K(ret));
      } else if (OB_FAIL(collector->init(&myself_allocator_, field_boosts, token_weights_,
                                         es_match_rtdef_->match_boost_,
                                         es_match_rtdef_->minimum_should_match_,
                                         es_match_rtdef_->match_fields_type_))) {
        LOG_WARN("failed to init multi match relevance collector", K(ret));
      }
    }
    iter_param.relevance_collector_ = collector;
  }
  return ret;
}

int ObDASMultiMatchIter::set_children_iter_rangekey()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(check_rangekey_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rangekey already inited", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_cnt(); ++i) {
    const ObDASIRScanCtDef *ir_scan_ctdef = ir_scan_ctdefs_.at(i);
    const ExprFixedArray *exprs = &(ir_scan_ctdefs_.at(i)->get_inv_idx_scan_ctdef()
        ->pd_expr_spec_.access_exprs_);
    int64_t group_id = 0;
    for (int64_t j = 0; j < exprs->count(); ++j) {
      if (T_PSEUDO_GROUP_ID == exprs->at(j)->type_) {
        group_id = exprs->at(j)->locate_expr_datum(*eval_ctx_).get_int();
      }
    }
    int64_t group_idx = ObNewRange::get_group_idx(group_id);
    ObNewRange inv_scan_range;
    ObNewRange fwd_scan_range;
    if (ir_scan_ctdef->need_fwd_idx_agg()
        && OB_FAIL(ObDASTRMergeIter::gen_fwd_idx_scan_feak_range(ir_scan_ctdef, fwd_scan_range))) {
      LOG_WARN("failed to generate fwd idx agg range", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < token_cnt(); ++j) {
      const int64_t idx = i * token_cnt() + j;
      if (OB_FAIL(ObDASTRMergeIter::gen_inv_idx_scan_default_range(
          ir_scan_ctdef, query_tokens_.at(j),
          mem_context_->get_arena_allocator(), inv_scan_range))) {
        LOG_WARN("failed to generate inv idx scan range", K(ret));
      } else if (ir_scan_ctdef->need_inv_idx_agg() && OB_ISNULL(inv_agg_params_[idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null inv agg param", K(ret));
      } else if (ir_scan_ctdef->need_inv_idx_agg()
          && OB_FAIL(inv_agg_params_[idx]->key_ranges_.push_back(inv_scan_range))) {
        LOG_WARN("failed to push scan range into inv agg param", K(ret));
      } else if (FALSE_IT(inv_scan_range.group_idx_ = group_idx)) {
      } else if (OB_ISNULL(inv_scan_params_[idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null inv scan param", K(ret));
      } else if (OB_FAIL(inv_scan_params_[idx]->key_ranges_.push_back(inv_scan_range))) {
        LOG_WARN("failed to push scan range into inv scan param", K(ret));
      } else if (ir_scan_ctdef->need_fwd_idx_agg() && OB_ISNULL(fwd_scan_params_[idx])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null fwd scan param", K(ret));
      } else if (ir_scan_ctdef->need_fwd_idx_agg()
          && OB_FAIL(fwd_scan_params_[idx]->key_ranges_.push_back(fwd_scan_range))) {
        LOG_WARN("failed to push scan range into fwd scan param", K(ret));
      } else if (ir_scan_ctdef->has_pushdown_topk()
          && OB_FAIL(block_max_scan_params_[idx]->key_ranges_.push_back(inv_scan_range))) {
        LOG_WARN("failed to push scan range into block max scan param", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    check_rangekey_inited_ = true;
  }
  return ret;
}

int ObDASMultiMatchIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_trivially_empty()) {
    // skip the following
  } else if (OB_FAIL(init_das_iter_scan_params())) {
    LOG_WARN("failed to init das iter scan params", K(ret));
  } else if (OB_FAIL(create_dim_iters())) {
    LOG_WARN("failed to create dim iters");
  } else if (OB_FAIL(create_sparse_retrieval_iter())) {
    LOG_WARN("failed to create sparse retrieval iter", K(ret));
  } else if (OB_FAIL(set_children_iter_rangekey())) {
    LOG_WARN("failed to set children iter rangekey", K(ret));
  } else if (OB_UNLIKELY(!check_rangekey_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected uninited rangkey", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      if (OB_FAIL(children_[i]->do_table_scan())) {
        LOG_WARN("failed to do table scan", K(ret));
      }
    }
  }
  return ret;
}

int ObDASMultiMatchIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_trivially_empty()) {
    // skip the following
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sparse retrieval iter", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < inv_scan_params_.count(); ++i) {
      if (nullptr != inv_scan_params_[i] && OB_FAIL(ObDASTRMergeIter::reuse_das_iter_scan_param(
          ls_id_, fts_tablet_ids_.at(i / token_cnt()).inv_idx_tablet_id_, *inv_scan_params_[i]))) {
        LOG_WARN("failed to reuse scan param", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inv_agg_params_.count(); ++i) {
      if (nullptr != inv_agg_params_[i] && OB_FAIL(ObDASTRMergeIter::reuse_das_iter_scan_param(
          ls_id_, fts_tablet_ids_.at(i / token_cnt()).inv_idx_tablet_id_, *inv_agg_params_[i]))) {
        LOG_WARN("failed to reuse scan param", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < fwd_scan_params_.count(); ++i) {
      if (nullptr != fwd_scan_params_[i] && OB_FAIL(ObDASTRMergeIter::reuse_das_iter_scan_param(
          ls_id_, fts_tablet_ids_.at(i / token_cnt()).fwd_idx_tablet_id_, *fwd_scan_params_[i]))) {
        LOG_WARN("failed to reuse scan param", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < total_doc_cnt_scan_params_.count(); ++i) {
      if (nullptr != total_doc_cnt_scan_params_[i]
          && OB_FAIL(ObDASTRMergeIter::reuse_das_iter_scan_param(ls_id_,
              fts_tablet_ids_.at(i).domain_id_idx_tablet_id_, *total_doc_cnt_scan_params_[i]))) {
        LOG_WARN("failed to reuse scan param", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      if (OB_FAIL(children_[i]->reuse())) {
        LOG_WARN("failed to reuse child iter", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      check_rangekey_inited_ = false;
      sparse_retrieval_iter_->reuse();
    }
  }
  if (OB_NOT_NULL(mem_context_)) {
    mem_context_->reset_remain_one_page();
  }
  return ret;
}

int ObDASMultiMatchIter::rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_trivially_empty()) {
    // skip the following
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sparse retrieval iter", K(ret));
  } else if (OB_UNLIKELY(!check_rangekey_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected uninited rangkey", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      if (OB_FAIL(children_[i]->rescan())) {
        LOG_WARN("failed to do table scan", K(ret));
      }
    }
  }
  return ret;
}

int ObDASMultiMatchIter::inner_release()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < inv_scan_params_.count(); ++i) {
    if (nullptr != inv_scan_params_[i]) {
      inv_scan_params_[i]->destroy_schema_guard();
      inv_scan_params_[i]->snapshot_.reset();
      inv_scan_params_[i]->destroy();
    }
  }
  inv_scan_params_.reset();
  for (int64_t i = 0; i < inv_agg_params_.count(); ++i) {
    if (nullptr != inv_agg_params_[i]) {
      inv_agg_params_[i]->destroy_schema_guard();
      inv_agg_params_[i]->snapshot_.reset();
      inv_agg_params_[i]->destroy();
    }
  }
  inv_agg_params_.reset();
  for (int64_t i = 0; i < fwd_scan_params_.count(); ++i) {
    if (nullptr != fwd_scan_params_[i]) {
      fwd_scan_params_[i]->destroy_schema_guard();
      fwd_scan_params_[i]->snapshot_.reset();
      fwd_scan_params_[i]->destroy();
    }
  }
  fwd_scan_params_.reset();
  for (int64_t i = 0; i < block_max_scan_params_.count(); ++i) {
    if (nullptr != block_max_scan_params_[i]) {
      block_max_scan_params_[i]->destroy_schema_guard();
      block_max_scan_params_[i]->snapshot_.reset();
      block_max_scan_params_[i]->destroy();
    }
  }
  block_max_scan_params_.reset();
  for (int64_t i = 0; i < total_doc_cnt_scan_params_.count(); ++i) {
    if (nullptr != total_doc_cnt_scan_params_[i]) {
      total_doc_cnt_scan_params_[i]->destroy_schema_guard();
      total_doc_cnt_scan_params_[i]->snapshot_.reset();
      total_doc_cnt_scan_params_[i]->destroy();
    }
  }
  total_doc_cnt_scan_params_.reset();
  bm25_param_est_ctxs_.reset();
  if (OB_NOT_NULL(sparse_retrieval_iter_)) {
    sparse_retrieval_iter_->reset();
    sparse_retrieval_iter_ = nullptr;
  }
  for (int64_t i = 0; i < dim_iters_.count(); ++i) {
    dim_iters_[i].reset();
  }
  dim_iters_.reset();
  if (OB_NOT_NULL(mem_context_)) {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  query_tokens_.reset();
  token_weights_.reset();
  field_boosts_.reset();
  es_match_ctdef_ = nullptr;
  es_match_rtdef_ = nullptr;
  ir_scan_ctdefs_.reset();
  ir_scan_rtdefs_.reset();
  fts_tablet_ids_.reset();
  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  topk_limit_ = 0;
  myself_allocator_.reset();
  is_inited_ = false;
  return ret;
}

int ObDASMultiMatchIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_trivially_empty()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sparse retrieval iter", K(ret));
  } else {
    ret = sparse_retrieval_iter_->get_next_row();
  }
  return ret;
}

int ObDASMultiMatchIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_trivially_empty()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sparse retrieval iter", K(ret));
  } else if (OB_UNLIKELY(0 == capacity)) {
    count = 0;
  } else {
    ret = sparse_retrieval_iter_->get_next_rows(capacity, count);
  }
  return ret;
}

int ObDASMultiMatchIter::parse_match_params(const ObDASIREsMatchCtDef &match_ctdef,
                                            ObDASIREsMatchRtDef &match_rtdef,
                                            ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObEvalCtx *eval_ctx = nullptr;
  ObExpr *param_text = nullptr;
  ObDatum *param_text_datum = nullptr;
  if (OB_ISNULL(eval_ctx = match_rtdef.eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null eval ctx", K(ret));
  } else if (OB_ISNULL(param_text = match_ctdef.es_param_text_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param text expr", K(ret));
  } else if (OB_FAIL(param_text->eval(*eval_ctx, param_text_datum))) {
    LOG_WARN("failed to eval param text", K(ret));
  } else if (OB_ISNULL(param_text_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param text datum", K(ret));
  } else {
    match_rtdef.minimum_should_match_ = 0;
    match_rtdef.match_boost_ = 1.0;
    match_rtdef.match_operator_ = ObMatchOperator::MATCH_OPERATOR_OR;
    match_rtdef.score_norm_function_ = ObMatchScoreNorm::SCORE_NORM_NONE;
    match_rtdef.match_fields_type_ = ObMatchFieldsType::MATCH_BEST_FIELDS;
  }
  if (OB_SUCC(ret) && param_text_datum->len_ > 0) {
    const ObString &original_string = param_text_datum->get_string();
    const ObCollationType &cs_type = param_text->datum_meta_.cs_type_;
    const ObCollationType dst_type = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;
    ObString param_string;
    if (cs_type != dst_type) {
      ObString tmp_string;
      if (OB_FAIL(ObCharset::tolower(cs_type, original_string, tmp_string, alloc))) {
        LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(original_string));
      } else if (OB_FAIL(ObCharset::charset_convert(alloc, tmp_string, cs_type,
                                                    dst_type, param_string))) {
        LOG_WARN("failed to convert string", K(ret), K(cs_type), K(original_string));
      }
    } else if (OB_FAIL(ObCharset::tolower(cs_type, original_string, param_string, alloc))){
      LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(original_string));
    }
    if (OB_SUCC(ret)) {
      char split_tag = ';';
      char equal_tag = '=';
      ObString param_operator("operator");
      ObString param_boost("boost");
      ObString param_minimum_should_match("minimum_should_match");
      ObString param_score_norm("score_norm");
      ObString param_type("type");
      ObConstRawExpr *boost_expr = nullptr;
      int should_match_value = 1;
      ObConstRawExpr *should_match_expr = nullptr;
      while (OB_SUCC(ret) && !param_string.empty()) {
        ObString param = param_string.split_on(split_tag);
        if (param.empty()) {
          param = param_string;
          param_string.reset();
        }
        ObString param_key = param.split_on(equal_tag).trim();
        if (OB_UNLIKELY(param_key.empty() || nullptr != param.find(split_tag))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH: misformatted parameters");
          LOG_WARN("invalid parameter", K(ret), K(param));
        } else if (param_key.compare_equal(param_operator)) {
          if (param.compare_equal("and")) {
            match_rtdef.match_operator_ = ObMatchOperator::MATCH_OPERATOR_AND;
          } else if (param.compare_equal("or")) {
            match_rtdef.match_operator_ = ObMatchOperator::MATCH_OPERATOR_OR;
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH: invalid value for parameter 'operator'");
            LOG_WARN("invalid operator", K(ret), K(param));
          }
        } else if (param_key.compare_equal(param_boost)) {
          char *value_str = static_cast<char *>(alloc.alloc(param.length() + 1));
          if (OB_ISNULL(value_str)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else {
            memcpy(value_str, param.ptr(), param.length());
            value_str[param.length()] = '\0';
            char *end_ptr = nullptr;
            match_rtdef.match_boost_ = strtod(value_str, &end_ptr);
            if (end_ptr != value_str + param.length() || match_rtdef.match_boost_ <= 0.0) {
              ret = OB_INVALID_ARGUMENT;
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH: value for parameter "
                             "'boost' must be a positive number");
              LOG_WARN("invalid boost", K(ret), K(match_rtdef.match_boost_));
            }
          }
        } else if (param_key.compare_equal(param_minimum_should_match)) {
          char *value_str = static_cast<char *>(alloc.alloc(param.length() + 1));
          if (OB_ISNULL(value_str)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else {
            memcpy(value_str, param.ptr(), param.length());
            value_str[param.length()] = '\0';
            char *end_ptr = nullptr;
            match_rtdef.minimum_should_match_ = strtol(value_str, &end_ptr, 10);
            if (end_ptr != value_str + param.length() || match_rtdef.minimum_should_match_ <= 0) {
              ret = OB_INVALID_ARGUMENT;
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH: value for parameter "
                             "'minimum_should_match' must be a positive integer");
              LOG_WARN("invalid minimum_should_match", K(ret),
                       K(match_rtdef.minimum_should_match_));
            }
          }
        } else if (param_key.compare_equal(param_score_norm)) {
          if (param.compare_equal("min-max")) {
            match_rtdef.score_norm_function_ = ObMatchScoreNorm::SCORE_NORM_MIN_MAX;
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH: invalid value for parameter 'score_norm'");
            LOG_WARN("invalid score_norm", K(ret), K(param));
          }
        } else if (param_key.compare_equal(param_type)) {
          if (param.compare_equal("most_fields")) {
            match_rtdef.match_fields_type_ = ObMatchFieldsType::MATCH_MOST_FIELDS;
          } else if (param.compare_equal("best_fields")) {
            match_rtdef.match_fields_type_ = ObMatchFieldsType::MATCH_BEST_FIELDS;
          } else if (param.compare_equal("cross_fields")) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "Value 'cross_fields' for parameter 'type' in MATCH");
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH: invalid value for parameter 'type'");
            LOG_WARN("invalid type", K(ret), K(param));
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH: invalid parameter");
          LOG_WARN("invalid parameter", K(ret), K(param));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ObMatchOperator::MATCH_OPERATOR_AND == match_rtdef.match_operator_
      && match_rtdef.minimum_should_match_ > 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                   "MATCH: parameter 'minimum_should_match' not allowed with operator 'and'");
    LOG_WARN("minumin should match not allowed when operator is and", K(ret));
  } else if (1 == match_rtdef.children_cnt_) {
    match_rtdef.match_fields_type_ = ObMatchFieldsType::MATCH_MOST_FIELDS;
  }
  return ret;
}

int ObDASMultiMatchIter::parse_match_tokens(const ObDASIRScanCtDef &ir_ctdef,
                                            ObDASIRScanRtDef &ir_rtdef,
                                            ObIAllocator &alloc,
                                            const bool compact_duplicate_tokens,
                                            ObArray<ObString> &query_tokens,
                                            ObArray<double> &token_weights)
{
  int ret = OB_SUCCESS;
  ObEvalCtx *eval_ctx = nullptr;
  ObExpr *search_text = nullptr;
  ObDatum *search_text_datum = nullptr;
  if (OB_ISNULL(eval_ctx = ir_rtdef.eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null eval ctx", K(ret));
  } else if (OB_ISNULL(search_text = ir_ctdef.search_text_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null search text expr", K(ret));
  } else if (OB_FAIL(search_text->eval(*eval_ctx, search_text_datum))) {
    LOG_WARN("failed to eval search text", K(ret));
  } else if (OB_ISNULL(search_text_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null search text datum", K(ret));
  } else if (0 == search_text_datum->len_) {
    // empty search text
  } else {
    const ObString &original_string = search_text_datum->get_string();
    const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
    const ObCollationType dst_type = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;
    ObString search_string;
    if (cs_type != dst_type) {
      ObString tmp_string;
      if (OB_FAIL(ObCharset::tolower(cs_type, original_string, tmp_string, alloc))) {
        LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(original_string));
      } else if (OB_FAIL(ObCharset::charset_convert(alloc, tmp_string, cs_type,
                                                    dst_type, search_string))) {
        LOG_WARN("failed to convert string", K(ret), K(cs_type), K(original_string));
      }
    } else if (OB_FAIL(ObCharset::tolower(cs_type, original_string, search_string, alloc))){
      LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(original_string));
    }
    if (OB_SUCC(ret)) {
      search_string = search_string.trim();
      char split_tag = ' ';
      char boost_tag = '^';
      hash::ObHashMap<ObString, double> token_map;
      if (compact_duplicate_tokens) {
        const int64_t map_bucket_cnt = MAX(search_string.length() / 10, 2);
        if (OB_FAIL(token_map.create(map_bucket_cnt, common::ObMemAttr(MTL_ID(), "FTWordMap")))) {
          LOG_WARN("failed tp create token map", K(ret));
        }
      }
      while (OB_SUCC(ret) && !search_string.empty()) {
        ObString token_str = search_string.split_on(split_tag);
        search_string = search_string.trim();
        if (token_str.empty()) {
          token_str = search_string;
          search_string.reset();
        }
        ObString token_key = token_str.split_on(boost_tag);
        double boost = 1.0;
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
            boost = strtod(boost_str, &end_ptr);
            if (end_ptr != boost_str + token_str.length() || boost <= 0.0) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid token weight", K(ret));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MATCH: token weights must be positive");
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (compact_duplicate_tokens) {
          double prev_boost = 0.0;
          if (OB_FAIL(token_map.get_refactored(token_key, prev_boost))) {
            if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
              LOG_WARN("fail to get relevance", K(ret), K(token_key));
            } else if (OB_FAIL(token_map.set_refactored(token_key, boost, 0/*overwrite*/))) {
              LOG_WARN("failed to push data", K(ret));
            }
          } else if (FALSE_IT(boost += prev_boost)) {
          } else if (OB_FAIL(token_map.set_refactored(token_key, boost, 1/*overwrite*/))) {
            LOG_WARN("failed to push data", K(ret));
          }
        } else {
          ObString token;
          if (OB_FAIL(ObCharset::charset_convert(
              alloc, token_key, dst_type, cs_type, token,
              ObCharset::CONVERT_FLAG::COPY_STRING_ON_SAME_CHARSET))) {
            LOG_WARN("failed to convert string", K(ret));
          } else if (OB_FAIL(query_tokens.push_back(token))) {
            LOG_WARN("failed to append query token", K(ret));
          } else if (OB_FAIL(token_weights.push_back(boost))) {
            LOG_WARN("failed to append token weight", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && compact_duplicate_tokens) {
        if (OB_FAIL(query_tokens.reserve(token_map.size()))) {
          LOG_WARN("failed to reserve query tokens", K(ret));
        } else if (OB_FAIL(token_weights.reserve(token_map.size()))) {
          LOG_WARN("failed to reserve token weights", K(ret));
        }
        for (hash::ObHashMap<ObString, double>::const_iterator iter = token_map.begin();
            OB_SUCC(ret) && iter != token_map.end();
            ++iter) {
          ObString token;
          if (OB_FAIL(ObCharset::charset_convert(
              alloc, iter->first, dst_type, cs_type, token,
              ObCharset::CONVERT_FLAG::COPY_STRING_ON_SAME_CHARSET))) {
            LOG_WARN("failed to convert string", K(ret));
          } else if (OB_FAIL(query_tokens.push_back(token))) {
            LOG_WARN("failed to append query token", K(ret));
          } else if (OB_FAIL(token_weights.push_back(iter->second))) {
            LOG_WARN("failed to append token weight", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

#undef INIT_FIXED_ARRAY
#undef ASSIGN_FIXED_ARRAY

} // namespace sql
} // namespace oceanbase
