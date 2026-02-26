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
  : ObSRBlockMaxTopKIterImpl()
{}

int ObSRBMWIterImpl::top_k_search()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(BMSearchStatus::MAX_STATUS != status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected max status", K(ret), K_(status));
  } else if (OB_FAIL(before_top_k_process())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to before top k process", K(ret));
    } else {
      status_ = BMSearchStatus::FINISHED;
    }
  } else {
    status_ = BMSearchStatus::FIND_NEXT_PIVOT;
  }

  // stats for debug & optimization
  int64_t pivot_cnt = 0;
  int64_t eval_pivot_cnt = 0;
  int64_t eval_pivot_range_cnt = 0;
  int64_t skip_range_cnt = 0;

  int64_t pivot_iter_idx = 0;
  while (OB_SUCC(ret) && BMSearchStatus::FINISHED != status_) {
    LOG_DEBUG("[Sparse Retrieval] top k search status", K_(status));
    switch (status_) {
    case BMSearchStatus::FIND_NEXT_PIVOT: {
      if (OB_FAIL(next_pivot(pivot_iter_idx))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to next pivot id", K(ret));
        } else {
          status_ = BMSearchStatus::FINISHED;
          ret = OB_SUCCESS;
        }
      } else {
        ++pivot_cnt;
        status_ = BMSearchStatus::EVALUATE_PIVOT_RANGE;
      }
      break;
    }
    case BMSearchStatus::EVALUATE_PIVOT_RANGE: {
      bool is_candidate = false;
      if (OB_FAIL(evaluate_pivot_range(pivot_iter_idx, is_candidate))) {
        LOG_WARN("failed to evaluate pivot range", K(ret));
      } else if (is_candidate) {
        ++eval_pivot_cnt;
        status_ = BMSearchStatus::EVALUATE_PIVOT;
      } else {
        status_ = BMSearchStatus::FIND_NEXT_PIVOT_RANGE;
      }
      break;
    }
    case BMSearchStatus::EVALUATE_PIVOT: {
      if (OB_FAIL(evaluate_pivot(pivot_iter_idx))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to evaluate pivot id", K(ret));
        } else {
          status_ = BMSearchStatus::FINISHED;
          ret = OB_SUCCESS;
        }
      } else {
        status_ = BMSearchStatus::FIND_NEXT_PIVOT;
      }
      break;
    }
    case BMSearchStatus::FIND_NEXT_PIVOT_RANGE: {
      if (OB_FAIL(next_pivot_range(get_top_k_threshold(), skip_range_cnt))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to next pivot range", K(ret));
        } else {
          status_ = BMSearchStatus::FINISHED;
          ret = OB_SUCCESS;
        }
      } else {
        // For pivots found next_pivot_range, maybe we can save one evaluate_pivot_range call
        ++eval_pivot_range_cnt;
        status_ = BMSearchStatus::FIND_NEXT_PIVOT;
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
  } else if (OB_FAIL(process_collected_row(*collected_id, pivot_relevance))) {
    LOG_WARN("failed to process collected row", K(ret));
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
    if (OB_ISNULL(iter) || OB_UNLIKELY(iter->iter_end())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter idx", K(ret), K(i), K(iter_idx));
    } else if (OB_FAIL(iter->advance_shallow(*pivot_id, true))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance shallow", K(ret), K(iter_idx));
      } else {
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

} // namespace storage
} // namespace oceanbase
