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

void ObSRDaaTIterImpl::reuse()
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
        LOG_WARN("fail to try load next batch dimension data", K(ret));\
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
    } else if (got_valid_id && OB_FAIL(process_collected_row(*id_datum, relevance))) {
      LOG_WARN("failed to process collected row", K(ret));
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

} // namespace storage
} // namespace oceanbase