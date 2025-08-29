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

#define USING_LOG_PREFIX STORAGE

#include "ob_sparse_bmw_iter.h"
#include "ob_block_max_iter.h"

namespace oceanbase
{
namespace storage
{

ObSRBMWIterImpl::ObSRBMWIterImpl()
  : ObSRDaaTIterImpl(),
    allocator_("BMWAlloc"),
    less_score_cmp_(),
    top_k_heap_(less_score_cmp_, &allocator_),
    top_k_count_(0),
    domain_id_cmp_(),
    id_cache_(),
    status_(BMWStatus::MAX_STATUS)
{}

int ObSRBMWIterImpl::init(
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
    status_ = BMWStatus::FINISHED;
    is_inited_ = true;
  } else {
    top_k_count_ = iter_param.topk_limit_;
    status_ = BMWStatus::MAX_STATUS;
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

void ObSRBMWIterImpl::reuse()
{

  while (!top_k_heap_.empty()) {
    top_k_heap_.pop();
  }
  status_ = BMWStatus::MAX_STATUS;
  ObSRDaaTIterImpl::reuse();
}

void ObSRBMWIterImpl::reset()
{
  top_k_heap_.reset();
  top_k_count_ = 0;
  status_ = BMWStatus::MAX_STATUS;
  domain_id_cmp_.reset();
  id_cache_.reset();
  allocator_.reset();
}

int ObSRBMWIterImpl::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(capacity));
  } else if (BMWStatus::FINISHED == status_) {
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

int ObSRBMWIterImpl::top_k_search()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(BMWStatus::MAX_STATUS != status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected max status", K(ret), K_(status));
  } else if (OB_FAIL(build_top_k_heap())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to build top k heap", K(ret));
    } else {
      status_ = BMWStatus::FINISHED;
    }
  } else if (OB_FAIL(init_before_wand_process())) {
    LOG_WARN("failed to init before wand process", K(ret));
  } else {
    status_ = BMWStatus::FIND_NEXT_PIVOT;
  }

  // stats for debug & optimization
  int64_t pivot_cnt = 0;
  int64_t eval_pivot_cnt = 0;
  int64_t eval_pivot_range_cnt = 0;
  int64_t skip_range_cnt = 0;

  int64_t pivot_iter_idx = 0;
  while (OB_SUCC(ret) && BMWStatus::FINISHED != status_) {
    LOG_DEBUG("[Sparse Retrieval] top k search status", K_(status));
    switch (status_) {
    case BMWStatus::FIND_NEXT_PIVOT: {
      if (OB_FAIL(next_pivot(pivot_iter_idx))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to next pivot id", K(ret));
        } else {
          status_ = BMWStatus::FINISHED;
          ret = OB_SUCCESS;
        }
      } else {
        ++pivot_cnt;
        status_ = BMWStatus::EVALUATE_PIVOT_RANGE;
      }
      break;
    }
    case BMWStatus::EVALUATE_PIVOT_RANGE: {
      bool is_candidate = false;
      if (OB_FAIL(evaluate_pivot_range(pivot_iter_idx, is_candidate))) {
        LOG_WARN("failed to evaluate pivot range", K(ret));
      } else if (is_candidate) {
        ++eval_pivot_cnt;
        status_ = BMWStatus::EVALUATE_PIVOT;
      } else {
        status_ = BMWStatus::FIND_NEXT_PIVOT_RANGE;
      }
      break;
    }
    case BMWStatus::EVALUATE_PIVOT: {
      if (OB_FAIL(evaluate_pivot(pivot_iter_idx))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to evaluate pivot id", K(ret));
        } else {
          status_ = BMWStatus::FINISHED;
          ret = OB_SUCCESS;
        }
      } else {
        status_ = BMWStatus::FIND_NEXT_PIVOT;
      }
      break;
    }
    case BMWStatus::FIND_NEXT_PIVOT_RANGE: {
      if (OB_FAIL(next_pivot_range(skip_range_cnt))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to next pivot range", K(ret));
        } else {
          status_ = BMWStatus::FINISHED;
          ret = OB_SUCCESS;
        }
      } else {
        // For pivots found next_pivot_range, maybe we can save one evaluate_pivot_range call
        ++eval_pivot_range_cnt;
        status_ = BMWStatus::FIND_NEXT_PIVOT;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", K(ret), K_(status));
    }
    }
  }

  LOG_DEBUG("[[Sparse Retrieval]] BMW topk search statistics", K(ret), K(pivot_cnt), K(eval_pivot_cnt),
      K(eval_pivot_range_cnt), K(skip_range_cnt));
  return ret;
}

int ObSRBMWIterImpl::build_top_k_heap()
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
      }
    }

    if (FAILEDx(fill_merge_heap())) {
      LOG_WARN("failed to fill merge heap after build top k heap", K(ret));
    }
  }
  return ret;
}


int ObSRBMWIterImpl::process_collected_row(const ObDatum &id_datum, const double relevance)
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

int ObSRBMWIterImpl::next_pivot(int64_t &pivot_iter_idx)
{
  int ret = OB_SUCCESS;

  const double top_k_threshold = get_top_k_threshold();
  int64_t iter_idx = 0;
  double max_score = 0.0;
  bool curr_id_end = false;
  bool found_pivot = false;
  while (OB_SUCC(ret) && !merge_heap_->empty() && !found_pivot) {
    const ObSRMergeItem *top_item = nullptr;
    double iter_score = 0.0;
    // make sure all dimensions containing pivot candidate are included in this iteration round
    curr_id_end = merge_heap_->is_unique_champion();
    if (OB_FAIL(merge_heap_->top(top_item))) {
      LOG_WARN("failed to get top item from merge heap", K(ret));
    } else if (FALSE_IT(iter_idx = top_item->iter_idx_)) {
    } else if (OB_FAIL(get_iter(iter_idx)->get_dim_max_score(iter_score))) {
      LOG_WARN("failed to get current score", K(ret));
    } else if (FALSE_IT(max_score += iter_score)) {
    } else if (OB_FAIL(merge_heap_->pop())) {
      LOG_WARN("failed to pop top item from merge heap", K(ret));
    } else {
      next_round_iter_idxes_[next_round_cnt_++] = iter_idx;
      found_pivot = max_score > top_k_threshold && curr_id_end;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (max_score <= top_k_threshold) {
    ret = OB_ITER_END;
  } else {
    pivot_iter_idx = iter_idx;
  }
  return ret;
}

// this function should be called right after evaluate a false positive pivot range
int ObSRBMWIterImpl::next_pivot_range(int64_t &skip_range_cnt)
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
    if (is_next_round_iter_end(i)) {
      ++next_round_iter_end_cnt;
    } else if (OB_ISNULL(iter = get_iter(iter_idx)) || OB_UNLIKELY(!iter->in_shallow_status())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
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
        is_candidate_range, min_unevaluated_id, max_evaluated_id))) {
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

int ObSRBMWIterImpl::evaluate_pivot(const int64_t pivot_iter_idx)
{
  int ret = OB_SUCCESS;
  ObISRDimBlockMaxIter *iter = get_iter(pivot_iter_idx);
  const ObDatum *pivot_id = nullptr;
  const ObDatum *collected_id = nullptr;
  double pivot_relevance = 0.0;
  bool need_project = false;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null iter", K(ret));
  } else if (OB_FAIL(iter->get_curr_id(pivot_id))) {
    LOG_WARN("failed to get pivot id", K(ret), K(pivot_iter_idx));
  } else if (OB_FAIL(advance_dim_iters_for_next_round(*pivot_id, true))) {
    LOG_WARN("failed to advance dim iters for next round", K(ret));
  } else if (OB_FAIL(collect_dims_by_id(collected_id, pivot_relevance, need_project))) {
    LOG_WARN("failed to collect dims by id", K(ret));
  } else if (OB_FAIL(unify_dim_iters_for_next_round())) {
    LOG_WARN("failed to unify dim iters for next round", K(ret));
  } else if (OB_FAIL(fill_merge_heap())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to fill merge heap after evaluate pivot", K(ret));
    }
  }

  return ret;
}

int ObSRBMWIterImpl::evaluate_pivot_range(const int64_t pivot_iter_idx, bool &is_candidate)
{
  int ret = OB_SUCCESS;
  ObISRDimBlockMaxIter *iter = get_iter(pivot_iter_idx);
  const ObDatum *pivot_id = nullptr;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null iter", K(ret));
  } else if (OB_FAIL(iter->get_curr_id(pivot_id))) {
    LOG_WARN("failed to get pivot id", K(ret), K(pivot_iter_idx));
  }

  double block_max_score = 0.0;
  const ObMaxScoreTuple *max_score_tuple = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    ObISRDimBlockMaxIter *iter = get_iter(iter_idx);
    if (OB_ISNULL(iter) || OB_UNLIKELY(is_next_round_iter_end(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter idx", K(ret), K(i), K(iter_idx));
    } else if (OB_FAIL(iter->advance_shallow(*pivot_id, true))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance shallow", K(ret), K(iter_idx));
      } else {
        set_next_round_iter_end(i);
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(iter->get_curr_block_max_info(max_score_tuple))) {
      LOG_WARN("failed to get block max score", K(ret), K(iter_idx));
    } else {
      block_max_score += max_score_tuple->max_score_;
    }
  }

  if (OB_SUCC(ret)) {
    is_candidate = block_max_score > get_top_k_threshold();
  }

  LOG_DEBUG("[Sparse Retrieval] BMW evaluate pivot range", K(ret),
      K(pivot_iter_idx), KPC(pivot_id), K(block_max_score), K(is_candidate), K(get_top_k_threshold()));

  return ret;
}


int ObSRBMWIterImpl::fill_merge_heap_with_shallow_dims(const ObDatum *last_range_border_id, const bool inclusive)
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
    if (is_next_round_iter_end(i)) {
      // skip
    } else if (OB_ISNULL(iter = get_iter(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
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

int ObSRBMWIterImpl::try_generate_next_range_from_merge_heap(
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
  const double top_k_threshold = get_top_k_threshold();
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

int ObSRBMWIterImpl::project_rows_from_top_k_heap(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(BMWStatus::FINISHED != status_)) {
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

int ObSRBMWIterImpl::unify_dim_iters_for_next_round()
{
  int ret = OB_SUCCESS;

  ObDatum *curr_domain_id = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    ObISRDimBlockMaxIter *iter = get_iter(iter_idx);
    const ObDatum *curr_domain_id = nullptr;
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
    } else if (!iter->in_shallow_status()) {
      // skip
    } else if (OB_FAIL(iter->get_curr_id(curr_domain_id))) {
      LOG_WARN("failed to get current id", K(ret), K(iter_idx));
    } else if (OB_FAIL(iter->advance_to(*curr_domain_id))) {
      LOG_WARN("failed to advance to domain id", K(ret), K(iter_idx));
    }
  }

  return ret;
}

int ObSRBMWIterImpl::advance_dim_iters_for_next_round(
    const ObDatum &target_id,
    const bool iter_end_available)
{
  int ret = OB_SUCCESS;
  ObSRMergeItem item;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    ObISRDimBlockMaxIter *iter = nullptr;
    if (is_next_round_iter_end(i)) {
      if (!iter_end_available) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected iter end", K(ret), K(i), K(iter_idx));
      }
    } else if (OB_ISNULL(iter = get_iter(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
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
    if (iter_end_available) {
      ret = OB_ITER_END;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (0 != next_round_cnt_ && OB_FAIL(merge_heap_->rebuild())) {
    LOG_WARN("failed to rebuild merge heap", K(ret));
  } else {
    next_round_cnt_ = 0;
  }

  return ret;
}



} // namespace storage
} // namespace oceanbase
