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

#define USING_LOG_PREFIX STORAGE

#include "ob_sparse_daat_iter.h"
#include "ob_block_max_iter.h"

namespace oceanbase
{
namespace storage
{

ObSRMergeCmp::ObSRMergeCmp()
  : cmp_func_(nullptr),
    iter_ids_(nullptr),
    is_inited_(false)
{
}

int ObSRMergeCmp::init(ObDatumMeta id_meta, const ObFixedArray<const ObDatum *, ObIAllocator> *iter_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_ids)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(iter_ids));
  } else {
    iter_ids_ = iter_ids;
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(id_meta.type_, id_meta.cs_type_);
    cmp_func_ = lib::is_oracle_mode() ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
    if (OB_ISNULL(cmp_func_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to init IRIterLoserTreeCmp", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSRMergeCmp::cmp(
    const ObSRMergeItem &l,
    const ObSRMergeItem &r,
    int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    int tmp_ret = 0;
    if (OB_FAIL(cmp_func_(get_id_datum(l.iter_idx_), get_id_datum(r.iter_idx_), tmp_ret))) {
      LOG_WARN("failed to compare doc id by datum", K(ret));
    } else {
      cmp_ret = tmp_ret;
    }
  }
  return ret;
}

ObSRDaaTIterImpl::ObSRDaaTIterImpl()
  : ObISparseRetrievalMergeIter(),
    iter_allocator_(nullptr),
    iter_param_(nullptr),
    dim_iters_(nullptr),
    merge_cmp_(),
    merge_heap_(nullptr),
    relevance_collector_(nullptr),
    iter_domain_ids_(),
    buffered_domain_ids_(),
    buffered_relevances_(),
    next_round_iter_idxes_(),
    next_round_cnt_(0),
    set_datum_func_(nullptr)
{
}

int ObSRDaaTIterImpl::init(
    ObSparseRetrievalMergeParam &iter_param,
    ObIArray<ObISRDaaTDimIter *> &dim_iters,
    ObIAllocator &iter_allocator,
    ObSRDaaTRelevanceCollector &relevance_collector)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(iter_param.max_batch_size_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected max batch size", K(ret), K(iter_param.max_batch_size_));
  } else {
    iter_allocator_ = &iter_allocator;
    iter_param_ = &iter_param;
    dim_iters_ = &dim_iters;
    relevance_collector_ = &relevance_collector;
    const int64_t max_batch_size = OB_MAX(iter_param_->eval_ctx_->max_batch_size_, iter_param_->max_batch_size_);
    if (iter_param_->id_proj_expr_->datum_meta_.type_ == common::ObUInt64Type) {
      set_datum_func_ = ObISparseRetrievalMergeIter::set_datum_int;
    } else {
      set_datum_func_ = ObISparseRetrievalMergeIter::set_datum_shallow;
    }
    if (OB_UNLIKELY(dim_iters.count() == 0)) {
    } else if (OB_NOT_NULL(iter_param_->dim_weights_) && dim_iters.count() != iter_param_->dim_weights_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dim iters count", K(ret), K(dim_iters_->count()), KP(iter_param_->dim_weights_));
    } else if (FALSE_IT(iter_domain_ids_.set_allocator(iter_allocator_))) {
    } else if (OB_FAIL(iter_domain_ids_.init(dim_iters.count()))) {
      LOG_WARN("failed to init iter domain ids array", K(ret));
    } else if (OB_FAIL(iter_domain_ids_.prepare_allocate(dim_iters.count()))) {
      LOG_WARN("failed to prepare allocate iter domain ids array", K(ret));
    } else if (FALSE_IT(next_round_iter_idxes_.set_allocator(iter_allocator_))) {
    } else if (OB_FAIL(next_round_iter_idxes_.init(dim_iters.count()))) {
      LOG_WARN("failed to init next round iter idxes array", K(ret));
    } else if (OB_FAIL(next_round_iter_idxes_.prepare_allocate(dim_iters.count()))) {
      LOG_WARN("failed to prepare allocate next round iter idxes array", K(ret));
    } else if (FALSE_IT(buffered_domain_ids_.set_allocator(iter_allocator_))) {
    } else if (OB_FAIL(buffered_domain_ids_.init(max_batch_size))) {
      LOG_WARN("failed to init buffered domain ids array", K(ret));
    } else if (OB_FAIL(buffered_domain_ids_.prepare_allocate(max_batch_size))) {
      LOG_WARN("failed to prepare allocate buffered domain ids array", K(ret));
    } else if (FALSE_IT(buffered_relevances_.set_allocator(iter_allocator_))) {
    } else if (OB_FAIL(buffered_relevances_.init(max_batch_size))) {
      LOG_WARN("failed to init buffered relevances array", K(ret));
    } else if (OB_FAIL(buffered_relevances_.prepare_allocate(max_batch_size))) {
      LOG_WARN("failed to prepare allocate buffered relevances array", K(ret));
    } else if (OB_FAIL(merge_cmp_.init(iter_param_->id_proj_expr_->datum_meta_, &iter_domain_ids_))) {
      LOG_WARN("failed to init loser tree comparator", K(ret));
    } else if (OB_ISNULL(merge_heap_ = OB_NEWx(ObSRMergeLoserTree, iter_allocator_, merge_cmp_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate loser tree", K(ret));
    } else if (OB_FAIL(merge_heap_->init(dim_iters_->count(), dim_iters_->count(), *iter_allocator_))) {
      LOG_WARN("failed to init iter loser tree", K(ret));
    } else if (OB_FAIL(merge_heap_->open(dim_iters_->count()))) {
      LOG_WARN("failed to open iter loser tree", K(ret));
    } else {
      for (int64_t i = 0; i < dim_iters.count(); ++i) {
        next_round_iter_idxes_[i] = i;
      }
    }

    if (OB_SUCC(ret)) {
      next_round_cnt_ = dim_iters.count();
      input_row_cnt_ = 0;
      output_row_cnt_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObSRDaaTIterImpl::reset()
{
  iter_domain_ids_.reset();
  if (OB_NOT_NULL(merge_heap_)) {
    merge_heap_->~ObSRMergeLoserTree();
    merge_heap_ = nullptr;
  }
  if (OB_NOT_NULL(relevance_collector_)) {
    relevance_collector_->reset();
    relevance_collector_ = nullptr;
  }
  iter_domain_ids_.reset();
  buffered_domain_ids_.reset();
  buffered_relevances_.reset();
  next_round_iter_idxes_.reset();
  next_round_cnt_ = 0;
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  is_inited_ = false;
}

void ObSRDaaTIterImpl::reuse(const bool switch_tablet)
{
  if (OB_NOT_NULL(dim_iters_)) {
    if (OB_NOT_NULL(merge_heap_)) {
      merge_heap_->reuse();
      merge_heap_->open(dim_iters_->count());
    }
    next_round_cnt_ = dim_iters_->count();
    for (int64_t i = 0; i < next_round_cnt_; ++i) {
      next_round_iter_idxes_[i] = i;
    }
  }
  if (OB_NOT_NULL(relevance_collector_)) {
    relevance_collector_->reuse();
  }
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
}

  int ObSRDaaTIterImpl::get_query_max_score(double &score)
  {
    int ret = OB_SUCCESS;
    score = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
      ObISRDaaTDimIter *dim_iter = dim_iters_->at(i);
      double dim_score = 0.0;
      if (OB_ISNULL(dim_iter) || OB_ISNULL(iter_param_->dim_weights_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null dimension iter", K(ret), K(i));
      } else if (OB_FAIL(dim_iter->get_dim_max_score(dim_score))) {
        LOG_WARN("failed to get dim max score and boost", K(ret));
      } else {
        score += dim_score * iter_param_->field_boost_ * iter_param_->dim_weights_->at(i);
      }
    }

    return ret;
  }

int ObSRDaaTIterImpl::get_next_row()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_FAIL(get_next_rows(1, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    } else if (OB_UNLIKELY(count != 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row count", K(ret), K(count));
    }
  } else if (OB_UNLIKELY(count != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected row count", K(ret), K(count));
  }
  return ret;
}

int ObSRDaaTIterImpl::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(0 == capacity)) {
    count = 0;
  } else if (0 == dim_iters_->count()) {
    ret = OB_ITER_END;
  } else if (iter_param_->limit_param_->is_valid() && output_row_cnt_ >= iter_param_->limit_param_->limit_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(pre_process())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to pre process", K(ret));
    } else {
      count = 0;
    }
  } else {
    count = 0;
    const int64_t real_capacity = MIN(capacity, iter_param_->max_batch_size_);
    while (OB_SUCC(ret) && count < real_capacity) {
      if (OB_FAIL(do_one_merge_round(count))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to do one merge round", K(ret), K(count));
        }
      }
    }

    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (count > 0 && OB_FAIL(project_results(count))) {
        LOG_WARN("failed to project results", K(ret), K(count));
      }
    }

  }
  return ret;
}

int ObSRDaaTIterImpl::pre_process()
{
  return OB_NOT_IMPLEMENT;
}

int ObSRDaaTIterImpl::do_one_merge_round(int64_t &count)
{
  int ret = OB_SUCCESS;
  bool need_project = true;
  double relevance = 0.0;
  const ObDatum *id_datum = nullptr;
  if (OB_FAIL(fill_merge_heap())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to fill merge heap", K(ret));
    }
  } else if (OB_FAIL(collect_dims_by_id(id_datum, relevance, need_project))) {
    LOG_WARN("failed to merge dimensions", K(ret));
  } else if (need_project && OB_FAIL(process_collected_row(*id_datum, relevance))) {
    LOG_WARN("failed to process collected row", K(ret));
  } else if (need_project && OB_FAIL(filter_on_demand(count, relevance, need_project))) {
    LOG_WARN("failed to process filter", K(ret));
  } else if (need_project && OB_FAIL(cache_result(count, *id_datum, relevance))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to cache result", K(ret));
    }
  }
  return ret;
}

int ObSRDaaTIterImpl::fill_merge_heap()
{
  int ret = OB_SUCCESS;
  ObSRMergeItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    ObISRDaaTDimIter *dim_iter = nullptr;
    if (OB_ISNULL(dim_iter = dim_iters_->at(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null dimension iter", K(ret), K(iter_idx), KPC_(iter_param));
    } else if (OB_FAIL(dim_iter->get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to try load next batch dimension data", K(ret), K(iter_idx), K_(next_round_cnt));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(dim_iter->get_curr_score(item.relevance_))) {
      LOG_WARN("fail to get current score", K(ret));
    } else if (OB_NOT_NULL(iter_param_->dim_weights_) && FALSE_IT(item.relevance_ = item.relevance_ * iter_param_->field_boost_ * iter_param_->dim_weights_->at(iter_idx))) {
    } else if (OB_FAIL(dim_iter->get_curr_id(iter_domain_ids_[iter_idx]))) {
      LOG_WARN("fail to get current doc id", K(ret));
    } else if (FALSE_IT(item.iter_idx_ = iter_idx)) {
    } else if (OB_FAIL(merge_heap_->push(item))) {
      LOG_WARN("fail to push item to merge heap", K(ret), K(item));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (merge_heap_->empty()) {
    ret = OB_ITER_END;
  } else if (0 != next_round_cnt_ && OB_FAIL(merge_heap_->rebuild())) {
    LOG_WARN("fail to rebuild merge heap", K(ret));
  } else {
    next_round_cnt_ = 0;
  }
  return ret;
}

int ObSRDaaTIterImpl::collect_dims_by_id(const ObDatum *&id_datum, double &relevance, bool &got_valid_id)
{
  int ret = OB_SUCCESS;

  const ObSRMergeItem *top_item = nullptr;
  bool curr_doc_end = false;
  int64_t iter_idx = 0;
  relevance = 0.0;
  got_valid_id = false;
  relevance_collector_->reuse();

  while (OB_SUCC(ret) && !merge_heap_->empty() && !curr_doc_end) {
    if (merge_heap_->is_unique_champion()) {
      curr_doc_end = true;
    }
    if (OB_FAIL(merge_heap_->top(top_item))) {
      LOG_WARN("failed to get top item from merge heap", K(ret));
    } else if (OB_FAIL(relevance_collector_->collect_one_dim(top_item->iter_idx_, top_item->relevance_))) {
      LOG_WARN("failed to collect one dimension", K(ret));
    } else if (FALSE_IT(iter_idx = top_item->iter_idx_)) {
    } else if (OB_FAIL(merge_heap_->pop())) {
      LOG_WARN("failed to pop top item in heap", K(ret));
    } else {
      next_round_iter_idxes_[next_round_cnt_++] = iter_idx;
    }
  }

  if (OB_SUCC(ret)) {
    id_datum = iter_domain_ids_[iter_idx];
    LOG_DEBUG("collect one dim", KPC(id_datum));
    if (OB_ISNULL(id_datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null id datum", K(ret));
    } else if (OB_FAIL(relevance_collector_->get_result(relevance, got_valid_id))) {
      LOG_WARN("failed to get result", K(ret));
    }
  }

  return ret;
}

int ObSRDaaTIterImpl::process_collected_row(const ObDatum &id_datum, const double relevance)
{
  return OB_SUCCESS;
}

int ObSRDaaTIterImpl::filter_on_demand(const int64_t count, const double relevance, bool &need_project)
{
  int ret = OB_SUCCESS;
  ObExpr *filter_expr = iter_param_->filter_expr_;
  ObExpr *relevance_proj_expr = iter_param_->relevance_proj_expr_;
  ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
  ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);

  if (!iter_param_->need_filter()) {
    need_project = true;
  } else if (eval_ctx->is_vectorized()) {
    if (OB_UNLIKELY(count >= eval_ctx->max_batch_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count", K(ret), K(count), K(eval_ctx->max_batch_size_));
    } else {
      guard.set_batch_idx(count);
      ObDatum &relevance_proj_datum = relevance_proj_expr->locate_datum_for_write(*eval_ctx);
      relevance_proj_datum.set_double(relevance);
      relevance_proj_expr->get_evaluated_flags(*eval_ctx).set(count);
      relevance_proj_expr->set_evaluated_projected(*eval_ctx);
      ObDatum *filter_res = nullptr;
      filter_expr->clear_evaluated_flag(*eval_ctx);
      if (OB_FAIL(filter_expr->eval(*eval_ctx, filter_res))) {
        LOG_WARN("failed to evaluate filter", K(ret));
      } else {
        need_project = !(filter_res->is_null() || 0 == filter_res->get_int());
      }
    }
  } else if (OB_UNLIKELY(0 != count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected number of results to project", K(ret), K(count));
  } else {
    guard.set_batch_idx(0);
    ObDatum &relevance_proj_datum = relevance_proj_expr->locate_datum_for_write(*eval_ctx);
    relevance_proj_datum.set_double(relevance);
    relevance_proj_expr->set_evaluated_projected(*eval_ctx);
  }
  return ret;
}

int ObSRDaaTIterImpl::cache_result(int64_t &count, const ObDatum &id_datum, const double relevance)
{
  int ret = OB_SUCCESS;
  const common::ObLimitParam *limit_param= iter_param_->limit_param_;
  const int64_t limit = limit_param->limit_;
  const int64_t offset = limit_param->offset_;
  ++input_row_cnt_;
  if (limit_param->is_valid() && input_row_cnt_ <= offset) {
    // TODO: Maybe we should not process offset logic here
    // don't need to project
  } else if (OB_UNLIKELY(count >= buffered_domain_ids_.count() || count >= buffered_relevances_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffered idx", K(ret), K(count), K(buffered_domain_ids_.count()), K(buffered_relevances_.count()));
  } else {
    buffered_domain_ids_[count].from_datum(id_datum);
    buffered_relevances_[count] = relevance;
    ++count;
    ++output_row_cnt_;
    if (limit_param->is_valid() && output_row_cnt_ >= limit) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObSRDaaTIterImpl::project_results(const int64_t count)
{
  int ret = OB_SUCCESS;
  ObExpr *relevance_proj_expr = iter_param_->relevance_proj_expr_;
  ObExpr *id_proj_expr = iter_param_->id_proj_expr_;
  ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
  ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);

  if (eval_ctx->is_vectorized()) {
    sql::ObBitVector &id_evaluated_flags = id_proj_expr->get_evaluated_flags(*eval_ctx);
    for (int64_t i = 0; i < count; ++i) {
      guard.set_batch_idx(i);
      ObDatum &id_proj_datum = id_proj_expr->locate_datum_for_write(*eval_ctx);
      set_datum_func_(id_proj_datum, buffered_domain_ids_[i]);
      id_evaluated_flags.set(i);
      id_proj_expr->set_evaluated_projected(*eval_ctx);
    }
    if (iter_param_->need_project_relevance()) {
      sql::ObBitVector &relevance_evaluated_flags = relevance_proj_expr->get_evaluated_flags(*eval_ctx);
      for (int64_t i = 0; i < count; ++i) {
        guard.set_batch_idx(i);
        ObDatum &relevance_proj_datum = relevance_proj_expr->locate_datum_for_write(*eval_ctx);
        relevance_proj_datum.set_double(buffered_relevances_[i]);
        relevance_evaluated_flags.set(i);
        relevance_proj_expr->set_evaluated_projected(*eval_ctx);
      }
    }
  } else if (OB_UNLIKELY(1 != count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected number of results to project", K(ret), K(count));
  } else {
    guard.set_batch_idx(0);
    ObDatum &id_proj_datum = id_proj_expr->locate_datum_for_write(*eval_ctx);
    set_datum_func_(id_proj_datum, buffered_domain_ids_[0]);
    id_proj_expr->set_evaluated_projected(*eval_ctx);
    ObDatum &relevance_proj_datum = relevance_proj_expr->locate_datum_for_write(*eval_ctx);
    relevance_proj_datum.set_double(buffered_relevances_[0]);
    relevance_proj_expr->set_evaluated_projected(*eval_ctx);
  }
  return ret;
}

ObSRBlockMaxTopKIterImpl::ObSRBlockMaxTopKIterImpl()
  : ObSRDaaTIterImpl(),
    allocator_("BMWAlloc"),
    less_score_cmp_(),
    top_k_heap_(less_score_cmp_, &allocator_),
    top_k_count_(0),
    domain_id_cmp_(),
    id_cache_(),
    initial_top_k_threshold_(0.0),
    status_(BMSearchStatus::MAX_STATUS)
{
  allocator_.set_tenant_id(MTL_ID());
}

int ObSRBlockMaxTopKIterImpl::init(
    ObSparseRetrievalMergeParam &iter_param,
    ObIArray<ObISRDaaTDimIter *> &dim_iters,
    ObIAllocator &iter_allocator,
    ObSRDaaTRelevanceCollector &relevance_collector)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSRDaaTIterImpl::init(iter_param, dim_iters, iter_allocator, relevance_collector))) {
    LOG_WARN("failed to init ObSRDaaTIterImpl", K(ret));
  } else if (OB_UNLIKELY(iter_param.topk_limit_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(iter_param));
  } else if (iter_param.topk_limit_ == 0) {
    top_k_count_ = 0;
    status_ = BMSearchStatus::FINISHED;
    is_inited_ = true;
  } else {
    top_k_count_ = iter_param.topk_limit_;
    status_ = BMSearchStatus::MAX_STATUS;
    id_cache_.set_allocator(&allocator_);
    if (OB_FAIL(domain_id_cmp_.init(iter_param.id_proj_expr_->obj_meta_))) {
      LOG_WARN("failed to init domain id cmp", K(ret));
    } else if (OB_FAIL(id_cache_.init(top_k_count_))) {
      LOG_WARN("failed to init id cache", K(ret));
    } else if (OB_FAIL(id_cache_.prepare_allocate(top_k_count_))) {
      LOG_WARN("failed to prepare allocate id cache", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSRBlockMaxTopKIterImpl::adjust_topk_limit(const int64_t new_topk_limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(new_topk_limit < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid topk limit", K(ret), K(new_topk_limit));
  } else if (new_topk_limit > top_k_count_) {
    // Need to expand id_cache_
    // Since ObFixedArray doesn't support resize, we need to destroy and reinit
    id_cache_.destroy();
    if (OB_FAIL(id_cache_.init(new_topk_limit))) {
      LOG_WARN("failed to init id cache with new topk limit", K(ret), K(new_topk_limit));
    } else if (OB_FAIL(id_cache_.prepare_allocate(new_topk_limit))) {
      LOG_WARN("failed to prepare allocate id cache with new topk limit", K(ret), K(new_topk_limit));
    } else {
      top_k_count_ = new_topk_limit;
    }
  } else {
    // new_topk_limit <= top_k_count_
    // For shrink or equal case: no need to reallocate id_cache_
    // Keep the larger id_cache_ capacity to avoid frequent reallocation
    top_k_count_ = new_topk_limit;
  }

  // Update iter_param_ topk_limit_ to keep it in sync
  if (OB_SUCC(ret) && OB_NOT_NULL(iter_param_)) {
    iter_param_->topk_limit_ = new_topk_limit;
  }

  return ret;
}

void ObSRBlockMaxTopKIterImpl::reuse(const bool switch_tablet)
{
  while (!top_k_heap_.empty()) {
    top_k_heap_.pop();
  }
  status_ = BMSearchStatus::MAX_STATUS;
  initial_top_k_threshold_ = 0.0;
  ObSRDaaTIterImpl::reuse(switch_tablet);
}

void ObSRBlockMaxTopKIterImpl::reset()
{
  top_k_heap_.reset();
  top_k_count_ = 0;
  status_ = BMSearchStatus::MAX_STATUS;
  initial_top_k_threshold_ = 0.0;
  domain_id_cmp_.reset();
  id_cache_.reset();
  allocator_.reset();
  ObSRDaaTIterImpl::reset();
}

int ObSRBlockMaxTopKIterImpl::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(capacity));
  } else if (BMSearchStatus::FINISHED == status_) {
    // skip
  } else if (OB_FAIL(top_k_search())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to top k search", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (FAILEDx(project_rows_from_top_k_heap(capacity, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to project rows from top k heap", K(ret));
    }
  }
  return ret;
}

int ObSRBlockMaxTopKIterImpl::preset_top_k_threshold(const double threshold)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(BMSearchStatus::MAX_STATUS != status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected max status", K(ret), K_(status));
  } else if (OB_UNLIKELY(threshold < 0.0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected negative threshold", K(ret), K(threshold));
  } else {
    initial_top_k_threshold_ = threshold;
  }
  return ret;
}

int ObSRBlockMaxTopKIterImpl::process_collected_row(const ObDatum &id_datum, const double relevance)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(top_k_heap_.count() > top_k_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected top k heap count", K(ret), K(top_k_heap_.count()), K_(top_k_count));
  } else if (top_k_heap_.count() < top_k_count_) {
    if (OB_FAIL(id_cache_.at(top_k_heap_.count()).from_datum(id_datum))) {
      LOG_WARN("failed to from datum", K(ret));
    } else if (OB_FAIL(top_k_heap_.push(TopKItem(relevance, top_k_heap_.count())))) {
      LOG_WARN("failed to push top k item", K(ret));
    }
  } else {
    const int64_t cache_idx = top_k_heap_.top().cache_idx_;
    const double top_k_threshold = get_top_k_threshold();
    if (relevance <= top_k_threshold) {
      // not a new top k candidate
    } else if (OB_FAIL(id_cache_.at(cache_idx).from_datum(id_datum))) {
      LOG_WARN("failed to from datum", K(ret));
    } else if (OB_FAIL(top_k_heap_.pop())) {
      LOG_WARN("failed to pop top k heap", K(ret));
    } else if (OB_FAIL(top_k_heap_.push(TopKItem(relevance, cache_idx)))) {
      LOG_WARN("failed to push top k item", K(ret));
    }
  }
  return ret;
}

int ObSRBlockMaxTopKIterImpl::before_top_k_process()
{
  int ret = OB_SUCCESS;
  const bool has_initial_top_k_threshold = initial_top_k_threshold_ > 0;
  if (OB_UNLIKELY(!top_k_heap_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-empty top k heap");
  } else if (has_initial_top_k_threshold) {
    if (OB_FAIL(fill_merge_heap())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to fill merge heap", K(ret));
      }
    }
  } else if (OB_FAIL(build_top_k_heap())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to build top k heap", K(ret));
    }
  }

  if (FAILEDx(init_before_topk_search())) {
    LOG_WARN("failed to init before wand process", K(ret));
  }
  return ret;
}

int ObSRBlockMaxTopKIterImpl::build_top_k_heap()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!top_k_heap_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty top k heap", K(ret));
  } else {
    bool need_project = true;
    double relevance = 0.0;
    const ObDatum *id_datum = nullptr;
    while (OB_SUCC(ret) && top_k_heap_.count() < top_k_count_) {
      if (OB_FAIL(fill_merge_heap())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to fill merge heap", K(ret));
        }
      } else if (OB_FAIL(collect_dims_by_id(id_datum, relevance, need_project))) {
        LOG_WARN("failed to collect dims by id", K(ret));
      } else if (need_project && OB_FAIL(process_collected_row(*id_datum, relevance))) {
        LOG_WARN("failed to process collected row", K(ret));
      }
    }

    if (FAILEDx(fill_merge_heap())) {
      LOG_WARN("failed to fill merge heap after build top k heap", K(ret));
    }
  }
  return ret;
}

// Generate next candidate range from block max index
// This function should be called right after evaluate a false positive pivot range
int ObSRBlockMaxTopKIterImpl::next_pivot_range(const double &score_threshold, int64_t &skip_range_cnt)
{
  int ret = OB_SUCCESS;
  /*
    Try to prune as much domain id ranges as possible.
    We will generate minimum prunable ranges to evaluate based on max score tuple and current ids.
    And then try to prune as much ranges as possible.

    When an unprunable range is found, it implies that there could be candidate pivot ids in range.
  */

  const ObDatum *max_evaluated_id = nullptr;
  const ObDatum *min_unevaluated_id = nullptr;
  bool last_border_inclusive = false;
  // find minimum max domain id
  const ObDatum *minimum_max_domain_id = nullptr;
  int64_t next_round_iter_end_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    const ObMaxScoreTuple *max_score_tuple = nullptr;
    int cmp_ret = 0;
    ObISRDimBlockMaxIter *iter = nullptr;
    if (OB_ISNULL(iter = get_iter(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), KPC(iter), K(iter_idx));
    } else if (iter->iter_end()) {
      ++next_round_iter_end_cnt;
    } else if (OB_UNLIKELY(!iter->in_shallow_status())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-shallow iter", K(ret), KPC(iter), K(iter_idx));
    } else if (OB_FAIL(iter->get_curr_block_max_info(max_score_tuple))) {
      LOG_WARN("failed to get block max info", K(ret), K(iter_idx));
    } else if (OB_ISNULL(max_score_tuple)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null max score tuple", K(ret));
    } else if (nullptr == minimum_max_domain_id) {
      minimum_max_domain_id = max_score_tuple->max_domain_id_;
    } else if (OB_FAIL(domain_id_cmp_.compare(*max_score_tuple->max_domain_id_, *minimum_max_domain_id, cmp_ret))) {
      LOG_WARN("failed to compare domain id", K(ret));
    } else if (0 < cmp_ret) {
      minimum_max_domain_id = max_score_tuple->max_domain_id_;
    }
  }

  // check if minimum max domain id might be included in other non-pivot dimensions
  if (OB_FAIL(ret)) {
  } else if (next_round_cnt_ == next_round_iter_end_cnt && merge_heap_->empty()) {
    ret = OB_ITER_END;
    next_round_cnt_ = 0;
  } else if (!merge_heap_->empty()) {
    const ObSRMergeItem *top_item = nullptr;
    int64_t iter_idx = 0;
    const ObDatum *next_domain_id = nullptr;
    int cmp_ret = 0;
    if (OB_FAIL(merge_heap_->top(top_item))) {
      LOG_WARN("failed to get top item from merge heap", K(ret));
    } else if (FALSE_IT(iter_idx = top_item->iter_idx_)) {
    } else if (OB_FAIL(get_iter(iter_idx)->get_curr_id(next_domain_id))) {
      LOG_WARN("failed to get current id", K(ret), K(iter_idx));
    } else if (nullptr == minimum_max_domain_id) {
      max_evaluated_id = next_domain_id;
      last_border_inclusive = true;
    } else if (OB_FAIL(domain_id_cmp_.compare(*next_domain_id, *minimum_max_domain_id, cmp_ret))) {
      LOG_WARN("failed to compare domain id", K(ret));
    } else if (cmp_ret <= 0) {
      max_evaluated_id = next_domain_id;
      last_border_inclusive = true;
    } else {
      max_evaluated_id = minimum_max_domain_id;
      last_border_inclusive = false;
    }
  } else {
    max_evaluated_id = minimum_max_domain_id;
    last_border_inclusive = false;
  }

  // try to generate next candidate range
  bool is_candidate_range = false;
  while (OB_SUCC(ret) && !is_candidate_range) {
    LOG_DEBUG("[Sparse Retrieval] BMW next pivot range", K(ret), K(is_candidate_range),
        KPC(max_evaluated_id), KPC(min_unevaluated_id));
    if (OB_FAIL(fill_merge_heap_with_shallow_dims(max_evaluated_id, last_border_inclusive))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to fill merge heap with shallow dims", K(ret));
      }
    } else if (OB_FAIL(try_generate_next_range_from_merge_heap(
        score_threshold, is_candidate_range, min_unevaluated_id, max_evaluated_id))) {
      LOG_WARN("failed to try generate next range from merge heap", K(ret));
    } else {
      last_border_inclusive = false;
    }
    ++skip_range_cnt;
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_candidate_range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no candidate range is found", K(ret),
        K(last_border_inclusive), KPC(max_evaluated_id), KPC(min_unevaluated_id));
  } else {
    // found a new top k candidate range, fill heap with non-shallow dim iters for next pivot searching
    if (OB_ISNULL(min_unevaluated_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null advance to domain id", K(ret));
    } else if (OB_FAIL(advance_dim_iters_for_next_round(*min_unevaluated_id, false))) {
      LOG_WARN("failed to advance dim iters for next round", K(ret));
    }
  }
  return ret;
}

int ObSRBlockMaxTopKIterImpl::fill_merge_heap_with_shallow_dims(const ObDatum *last_range_border_id, const bool inclusive)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(last_range_border_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null last evaluated domain id", K(ret));
  }

  ObSRMergeItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    ObISRDimBlockMaxIter *iter = nullptr;
    if (OB_ISNULL(iter = get_iter(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_FAIL(iter->advance_shallow(*last_range_border_id, inclusive))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance shallow", K(ret), K(iter_idx));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(iter->get_curr_id(iter_domain_ids_[iter_idx]))) {
      LOG_WARN("failed to get current id", K(ret), K(iter_idx));
    } else {
      item.iter_idx_ = iter_idx;
      item.relevance_ = 0.0;
      if (OB_FAIL(merge_heap_->push(item))) {
        LOG_WARN("failed to push iter item to merge heap", K(ret), K(item));
      }
    }
  }

  // rebuild merge heap
  if (OB_FAIL(ret)) {
  } else if (merge_heap_->empty()) {
    ret = OB_ITER_END;
  } else if (0 != next_round_cnt_ && OB_FAIL(merge_heap_->rebuild())) {
    LOG_WARN("failed to rebuild merge heap", K(ret));
  } else {
    next_round_cnt_ = 0;
  }
  return ret;
}

int ObSRBlockMaxTopKIterImpl::try_generate_next_range_from_merge_heap(
    const double &score_threshold,
    bool &is_candidate_range,
    const ObDatum *&min_domain_id_with_pivot,
    const ObDatum *&max_domain_id_without_pivot)
{
  int ret = OB_SUCCESS;
  is_candidate_range = false;
  min_domain_id_with_pivot = nullptr;
  max_domain_id_without_pivot = nullptr;

  // validate next candidate range
  const ObDatum *minimum_max_domain_id = nullptr;
  bool curr_range_reach_end = false;
  const double top_k_threshold = score_threshold;;
  int64_t iter_idx = 0;
  double max_score = 0.0;
  while (OB_SUCC(ret) && max_score < top_k_threshold && !curr_range_reach_end && !merge_heap_->empty()) {
    const ObSRMergeItem *top_item = nullptr;
    ObISRDimBlockMaxIter *iter = nullptr;
    const ObDatum *curr_domain_id = nullptr;
    if (OB_FAIL(merge_heap_->top(top_item))) {
      LOG_WARN("failed to get top item from merge heap", K(ret));
    } else if (FALSE_IT(iter_idx = top_item->iter_idx_)) {
    } else if (OB_ISNULL(iter = get_iter(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
    } else if (iter->in_shallow_status()) {
      // already in shallow status, no need to advance shallow
    } else if (OB_FAIL(iter->get_curr_id(curr_domain_id))) {
      LOG_WARN("failed to get current id", K(ret), K(iter_idx));
    } else if (OB_FAIL(iter->advance_shallow(*curr_domain_id, true))) {
      LOG_WARN("failed to advance shallow", K(ret), K(iter_idx));
    }

    const ObMaxScoreTuple *max_score_tuple = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(iter->get_curr_block_max_info(max_score_tuple))) {
      LOG_WARN("failed to get block max info", K(ret), K(iter_idx));
    } else if (OB_ISNULL(max_score_tuple)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null max score tuple", K(ret));
    } else {
      int cmp_ret = 0;
      if (nullptr == minimum_max_domain_id) {
        minimum_max_domain_id = max_score_tuple->max_domain_id_;
      } else if (OB_FAIL(domain_id_cmp_.compare(*max_score_tuple->min_domain_id_, *minimum_max_domain_id, cmp_ret))) {
        LOG_WARN("failed to compare min domain id with minimum max domain id", K(ret));
      } else if (cmp_ret > 0) {
        // no intersected range with this block max iter
        curr_range_reach_end = true;
      } else if (OB_FAIL(domain_id_cmp_.compare(*max_score_tuple->max_domain_id_, *minimum_max_domain_id, cmp_ret))) {
        LOG_WARN("failed to compare max domain id with minimum max domain id", K(ret));
      } else if (0 < cmp_ret) {
        minimum_max_domain_id = max_score_tuple->max_domain_id_;
      }

      if (OB_FAIL(ret) || curr_range_reach_end) {
      } else if (OB_FAIL(merge_heap_->pop())) {
        LOG_WARN("failed to pop top item from merge heap", K(ret));
      } else {
        next_round_iter_idxes_[next_round_cnt_++] = iter_idx;
        max_score += max_score_tuple->max_score_;
        if (max_score > top_k_threshold) {
          // found a new top k candidate range
          is_candidate_range = true;
          min_domain_id_with_pivot = max_score_tuple->min_domain_id_;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !is_candidate_range) {
    max_domain_id_without_pivot = minimum_max_domain_id;
  }
  return ret;
}

int ObSRBlockMaxTopKIterImpl::advance_dim_iters_for_next_round(const ObDatum &target_id, const bool iter_end_available)
{
  int ret = OB_SUCCESS;
  ObSRMergeItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    ObISRDaaTDimIter *iter = nullptr;
    if (OB_ISNULL(iter = dim_iters_->at(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
    } else if (iter->iter_end()) {
      if (!iter_end_available) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected iter end", K(ret), K(i), K(iter_idx));
      }
    } else if (OB_FAIL(iter->advance_to(target_id))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance to target id", K(ret), K(iter_idx));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(iter->get_curr_id(iter_domain_ids_[iter_idx]))) {
      LOG_WARN("failed to get current id", K(ret), K(iter_idx));
    } else if (OB_FAIL(iter->get_curr_score(item.relevance_))) {
      LOG_WARN("failed to get current score", K(ret), K(iter_idx));
    } else if (FALSE_IT(item.iter_idx_ = iter_idx)) {
    } else if (OB_FAIL(merge_heap_->push(item))) {
      LOG_WARN("failed to push item to top k heap", K(ret), K(item));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (merge_heap_->empty()) {
    ret = OB_ITER_END;
  } else if (0 != next_round_cnt_ && OB_FAIL(merge_heap_->rebuild())) {
    LOG_WARN("failed to rebuild merge heap", K(ret));
  } else {
    next_round_cnt_ = 0;
  }
  return ret;
}

int ObSRBlockMaxTopKIterImpl::project_rows_from_top_k_heap(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(BMSearchStatus::FINISHED != status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", K(ret), K_(status));
  } else if (OB_UNLIKELY(top_k_heap_.empty())) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(capacity < 0 || capacity > buffered_domain_ids_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(capacity), K(buffered_domain_ids_.count()));
  } else {
    ObExpr *relevance_proj_expr = iter_param_->relevance_proj_expr_;
    ObExpr *id_proj_expr = iter_param_->id_proj_expr_;
    ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
    sql::ObBitVector &id_evaluated_flags = id_proj_expr->get_evaluated_flags(*eval_ctx);
    sql::ObBitVector *relevance_evaluated_flags = nullptr;
    ObDatum *id_datums = id_proj_expr->locate_datums_for_update(*eval_ctx, capacity);
    ObDatum *relevance_datums = nullptr;
    if (iter_param_->need_project_relevance()) {
      relevance_datums = relevance_proj_expr->locate_datums_for_update(*eval_ctx, capacity);
      relevance_evaluated_flags = &relevance_proj_expr->get_evaluated_flags(*eval_ctx);
    }

    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
    count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < capacity && !top_k_heap_.empty(); ++i) {
      guard.set_batch_idx(i);
      const TopKItem &top_k_item = top_k_heap_.top();
      const ObDocIdExt &id = id_cache_.at(top_k_item.cache_idx_);
      set_datum_func_(id_datums[i], id);
      id_evaluated_flags.set(i);
      id_proj_expr->set_evaluated_projected(*eval_ctx);

      if (iter_param_->need_project_relevance()) {
        relevance_datums[i].set_double(top_k_item.relevance_);
        relevance_evaluated_flags->set(i);
        relevance_proj_expr->set_evaluated_projected(*eval_ctx);
      }
      ++count;
      if (OB_FAIL(top_k_heap_.pop())) {
        LOG_WARN("failed to pop top k heap", K(ret));
      }
    }
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase
