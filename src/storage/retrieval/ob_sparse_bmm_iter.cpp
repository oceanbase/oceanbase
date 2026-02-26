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

#include "ob_sparse_bmm_iter.h"
#include "ob_block_max_iter.h"

namespace oceanbase
{
namespace storage
{

ObSRBMMIterImpl::ObSRBMMIterImpl()
  : ObSRBlockMaxTopKIterImpl(),
    sorted_iters_(),
    non_essential_dim_count_(0),
    non_essential_dim_max_score_(0.0),
    non_essential_dim_threshold_(0.0)
{}

int ObSRBMMIterImpl::init(
    ObSparseRetrievalMergeParam &iter_param,
    ObIArray<ObISRDaaTDimIter *> &dim_iters,
    ObIAllocator &iter_allocator,
    ObSRDaaTRelevanceCollector &relevance_collector)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSRBlockMaxTopKIterImpl::init(iter_param, dim_iters, iter_allocator, relevance_collector))) {
    LOG_WARN("failed to init ObSRBlockMaxTopKIterImpl", K(ret));
  } else if (FALSE_IT(sorted_iters_.set_allocator(&allocator_))) {
  } else if (OB_FAIL(sorted_iters_.init(dim_iters.count()))) {
    LOG_WARN("failed to init sorted iters", K(ret));
  } else if (OB_FAIL(sorted_iters_.prepare_allocate(dim_iters.count()))) {
    LOG_WARN("failed to prepare allocate sorted iters", K(ret));
  }
  return ret;
}

void ObSRBMMIterImpl::reuse(const bool switch_tablet)
{
  non_essential_dim_count_ = 0;
  non_essential_dim_max_score_ = 0.0;
  non_essential_dim_threshold_ = 0.0;
  ObSRBlockMaxTopKIterImpl::reuse(switch_tablet);
}

void ObSRBMMIterImpl::reset()
{
  sorted_iters_.reset();
  non_essential_dim_count_ = 0;
  non_essential_dim_max_score_ = 0.0;
  non_essential_dim_threshold_ = 0.0;
  ObSRBlockMaxTopKIterImpl::reset();
}

int ObSRBMMIterImpl::top_k_search()
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
  } else if (OB_FAIL(sort_and_classify_dims())) {
    LOG_WARN("failed to sort and classify dims", K(ret));
  } else {
    status_ = BMSearchStatus::FIND_NEXT_PIVOT;
  }

  double non_essential_block_max_score = 0.0;
  ObDocIdExt curr_pivot_id;
  while (OB_SUCC(ret) && BMSearchStatus::FINISHED != status_) {
    LOG_DEBUG("[Sparse Retrieval] bmm top k search", K(ret), K_(status), K(curr_pivot_id));
    switch (status_) {
    case BMSearchStatus::FIND_NEXT_PIVOT: {
      if (OB_FAIL(next_pivot(curr_pivot_id))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to find next bmm pivot", K(ret));
        } else {
          status_ = BMSearchStatus::FINISHED;
          ret = OB_SUCCESS;
        }
      } else {
        status_ = BMSearchStatus::EVALUATE_PIVOT_RANGE;
      }
      break;
    }
    case BMSearchStatus::EVALUATE_PIVOT_RANGE: {
      bool is_candidate = false;
      non_essential_block_max_score = 0.0;
      if (OB_FAIL(evaluate_pivot_range(curr_pivot_id.get_datum(), non_essential_block_max_score, is_candidate))) {
        LOG_WARN("failed to evaluate bmm pivot range", K(ret));
      } else if (is_candidate) {
        status_ = BMSearchStatus::EVALUATE_PIVOT;
      } else {
        status_ = BMSearchStatus::FIND_NEXT_PIVOT_RANGE;
      }
      break;
    }
    case BMSearchStatus::EVALUATE_PIVOT: {
      bool is_candidate = false;
      bool is_valid_pivot = false;
      double essential_score = 0.0;
      if (OB_FAIL(evaluate_essential_pivot(
          curr_pivot_id.get_datum(), non_essential_block_max_score, essential_score, is_candidate, is_valid_pivot))) {
        LOG_WARN("failed to evaluate bmm essential pivot", K(ret));
      } else if (is_candidate) {
        if (OB_FAIL(evaluate_pivot(curr_pivot_id.get_datum(), essential_score))) {
          LOG_WARN("failed to evaluate bmm pivot", K(ret));
        } else if (!need_update_essential_dims()) {
          // skip
        } else if (OB_FAIL(forward_next_round_iters())) {
          LOG_WARN("failed to forward next round iters", K(ret));
        } else if (OB_FAIL(try_update_essential_dims())) {
          LOG_WARN("failed to try update essential dims", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!is_valid_pivot) {
        // Since pivot might comes from block max iter, which is not accurate, need to find next valid pivot
        // But this situation should be rare, and has relatively low impact on performance
        status_ = BMSearchStatus::FIND_NEXT_PIVOT;
      } else if (OB_FAIL(fill_merge_heap())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to fill merge heap", K(ret), K(is_candidate), K_(next_round_cnt));
        } else {
          ret = OB_SUCCESS;
          status_ = BMSearchStatus::FINISHED;
        }
      } else {
        status_ = BMSearchStatus::FIND_NEXT_PIVOT;
      }
      break;
    }
    case BMSearchStatus::FIND_NEXT_PIVOT_RANGE: {
      int64_t skip_range_cnt = 0;
      if (OB_FAIL(next_pivot_range(get_essential_dim_threshold(), skip_range_cnt))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to next pivot range", K(ret));
        } else {
          ret = OB_SUCCESS;
          status_ = BMSearchStatus::FINISHED;
        }
      } else {
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
  return ret;
}

int ObSRBMMIterImpl::sort_and_classify_dims()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < dim_iters_->count(); ++i) {
    sorted_iters_[i] = i;
  }

  DimMaxScoreCmp cmp(*dim_iters_, ret);
  lib::ob_sort(sorted_iters_.begin(), sorted_iters_.end(), cmp);

  non_essential_dim_count_ = 0;
  non_essential_dim_max_score_ = 0.0;
  non_essential_dim_threshold_ = 0.0;
  if (FAILEDx(try_update_essential_dims())) {
    LOG_WARN("failed to try update essential dims", K(ret));
  }
  LOG_DEBUG("[Sparse Retrieval] first classify sorted dims",
      K(non_essential_dim_count_), K(non_essential_dim_max_score_), K(non_essential_dim_threshold_));
  return ret;
}

int ObSRBMMIterImpl::try_update_essential_dims()
{
  int ret = OB_SUCCESS;
  const double top_k_threshold = get_top_k_threshold();

  // Notice that non_essential_dim_max_score could decrease with non-essential dims iter end
  double curr_non_ess_max_score = 0;
  for (int64_t i = 0;
      OB_SUCC(ret) && i < sorted_iters_.count() && non_essential_dim_threshold_ <= top_k_threshold;
      ++i) {
    const int64_t iter_idx = sorted_iters_[i];
    ObISRDimBlockMaxIter *iter = nullptr;
    double score = 0.0;
    if (OB_ISNULL(iter = get_iter(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_FAIL(iter->get_dim_max_score(score))) {
      LOG_WARN("failed to get curr score", K(ret));
    } else {
      const double next_non_ess_max_score = curr_non_ess_max_score + score;
      if (next_non_ess_max_score > top_k_threshold) {
        non_essential_dim_count_ = (i - 1) >= 0 ? i : 0;
        non_essential_dim_max_score_ = curr_non_ess_max_score;
        non_essential_dim_threshold_ = next_non_ess_max_score;
      } else {
        curr_non_ess_max_score = next_non_ess_max_score;
      }
    }
  }

    LOG_DEBUG("[Sparse Retrieval] try update essential dims",
      K(ret), K(non_essential_dim_count_), K(non_essential_dim_max_score_),
      K(non_essential_dim_threshold_), K(top_k_threshold), K(sorted_iters_));

  // rebuild merge heap with essential dims only
  merge_heap_->reuse();
  if (FAILEDx(merge_heap_->open(dim_iters_->count() - non_essential_dim_count_))) {
    LOG_WARN("failed to open merge heap", K(ret));
  }
  ObSRMergeItem item;
  for (int64_t i = non_essential_dim_count_; OB_SUCC(ret) && i < sorted_iters_.count(); ++i) {
    const int64_t iter_idx = sorted_iters_[i];
    ObISRDimBlockMaxIter *iter = get_iter(iter_idx);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
    } else if (iter->iter_end()) {
      // skip
    } else if (iter->in_shallow_status() && FALSE_IT(item.relevance_ = 0.0)) {
    } else if (!iter->in_shallow_status() && OB_FAIL(iter->get_curr_score(item.relevance_))) {
      LOG_WARN("failed to get curr score", K(ret));
    } else if (OB_FAIL(iter->get_curr_id(iter_domain_ids_[iter_idx]))) {
      LOG_WARN("failed to get curr id", K(ret));
    } else if (FALSE_IT(item.iter_idx_ = iter_idx)) {
    } else if (OB_FAIL(merge_heap_->push(item))) {
      LOG_WARN("failed to push item to merge heap", K(ret));
    }
  }
  if (FAILEDx(merge_heap_->rebuild())) {
    LOG_WARN("failed to rebuild merge heap", K(ret));
  } else {
    next_round_cnt_ = 0;
  }

  return ret;
}

// Since we did not ensure "dim iters in this state are not in shallow status" to save expensive advance_to for now
// There's chance that pivot id selected from dims in shallow status does not exist in coresponding inverted list actually
int ObSRBMMIterImpl::next_pivot(ObDocIdExt &pivot_id)
{
  int ret = OB_SUCCESS;

  int64_t iter_idx = 0;
  bool curr_id_end = false;
  bool found_pivot = false;
  if (merge_heap_->empty()) {
    ret = OB_ITER_END;
  }

  while (OB_SUCC(ret) && !merge_heap_->empty() && !found_pivot) {
    const ObSRMergeItem *top_item = nullptr;
    double iter_score = 0.0;
    // make sure all dimensions containing pivot candidate are included in this iteration round
    curr_id_end = merge_heap_->is_unique_champion();
    if (OB_FAIL(merge_heap_->top(top_item))) {
      LOG_WARN("failed to get top item from merge heap", K(ret));
    } else if (FALSE_IT(iter_idx = top_item->iter_idx_)) {
    } else if (OB_FAIL(merge_heap_->pop())) {
      LOG_WARN("failed to pop top item from merge heap", K(ret));
    } else {
      // sine we have maintained the invariant that any essential dim_max_score plus non-essential dim_max_score
      // would be larger than top_k_threshold, here we only need to collect any essential dimensions with pivot id for this merge round
      next_round_iter_idxes_[next_round_cnt_++] = iter_idx;
      found_pivot = curr_id_end;
    }
  }

  const ObDatum *pivot_id_datum = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_iter(iter_idx)->get_curr_id(pivot_id_datum))) {
    LOG_WARN("failed to get pivot id", K(ret), K(iter_idx));
  } else if (OB_FAIL(pivot_id.from_datum(*pivot_id_datum))) {
    LOG_WARN("failed to copy pivot id", K(ret));
  }
  LOG_DEBUG("[Sparse Retrieval] next bmm pivot", K(ret), K(iter_idx), K(pivot_id), K(get_iter(iter_idx)->in_shallow_status()));

  return ret;
}

int ObSRBMMIterImpl::evaluate_pivot_range(
    const ObDatum &pivot_id,
    double &non_essential_block_max_score,
    bool &is_candidate)
{
  int ret = OB_SUCCESS;
  is_candidate = false;
  non_essential_block_max_score = 0.0;

  double block_max_score = 0.0;
  const double essential_threshold = get_essential_dim_threshold();
  const ObMaxScoreTuple *max_score_tuple = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    ObISRDimBlockMaxIter *iter = get_iter(iter_idx);
    if (OB_ISNULL(iter) || OB_UNLIKELY(iter->iter_end())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter idx", K(ret), K(i), K(iter_idx));
    } else if (OB_FAIL(iter->advance_shallow(pivot_id, true))) {
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
    is_candidate = block_max_score > essential_threshold;
  }

  if (OB_SUCC(ret) && is_candidate) {
    is_candidate = false;
    const double top_k_threshold = get_top_k_threshold();
    for (int64_t i = 0; OB_SUCC(ret) && i < non_essential_dim_count_; ++i) {
      const int64_t iter_idx = sorted_iters_[i];
      ObISRDimBlockMaxIter *iter = nullptr;
      if (OB_ISNULL(iter = get_iter(iter_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null iter", K(ret));
      } else if (iter->iter_end()) {
        // skip
      } else if (OB_FAIL(iter->advance_shallow(pivot_id, true))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to advance shallow", K(ret), K(iter_idx));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(iter->get_curr_block_max_info(max_score_tuple))) {
        LOG_WARN("failed to get block max score", K(ret), K(iter_idx));
      } else {
        block_max_score += max_score_tuple->max_score_;
        non_essential_block_max_score += max_score_tuple->max_score_;
      }
    }
    if (OB_SUCC(ret)) {
      is_candidate = block_max_score > top_k_threshold;
    }
  }

  LOG_DEBUG("[Sparse Retrieval] eval bmm pivot", K(ret), K(pivot_id), K(is_candidate), K(block_max_score),
      K(non_essential_block_max_score), K(essential_threshold), K(get_top_k_threshold()), K_(sorted_iters));
  return ret;
}

int ObSRBMMIterImpl::evaluate_essential_pivot(
    const ObDatum &pivot_id,
    const double non_essential_block_max_score,
    double &essential_score,
    bool &is_candidate,
    bool &is_valid_pivot)
{
  int ret = OB_SUCCESS;
  is_candidate = false;
  is_valid_pivot = false;

  const ObDatum *collected_id = nullptr;
  essential_score = 0.0;
  bool need_project = false;

  if (OB_FAIL(advance_dim_iters_for_next_round(pivot_id, true))) {
    LOG_WARN("failed to advance dim iters for next round", K(ret), K(pivot_id));
  } else if (!merge_heap_->empty()) {
    const ObSRMergeItem *top_item = nullptr;
    const ObDatum *id_datum = nullptr;
    int id_cmp_ret = 0;
    if (OB_FAIL(merge_heap_->top(top_item))) {
      LOG_WARN("failed to get top item from merge heap", K(ret));
    } else if (OB_ISNULL(id_datum = iter_domain_ids_[top_item->iter_idx_])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null id datum", K(ret));
    } else if (OB_FAIL(domain_id_cmp_.compare(*id_datum, pivot_id, id_cmp_ret))) {
      LOG_WARN("failed to compare domain id", K(ret));
    } else if (0 == id_cmp_ret) {
      is_valid_pivot = true;
    }
  }

  if (OB_FAIL(ret) || !is_valid_pivot) {
  } else if (OB_FAIL(collect_dims_by_id(collected_id, essential_score, need_project))) {
    LOG_WARN("failed to collect dims by id", K(ret));
  } else {
    is_candidate = non_essential_block_max_score + essential_score > get_top_k_threshold();
  }
  LOG_DEBUG("[Sparse Retrieval] eval essential pivot", K(ret), K(pivot_id), KPC(collected_id),
        K(non_essential_block_max_score), K(essential_score), K(is_candidate), K(is_valid_pivot));
  return ret;
}

int ObSRBMMIterImpl::evaluate_pivot(const ObDatum &pivot_id, const double &essential_score)
{
  int ret = OB_SUCCESS;
  double non_essential_score =0.0;

  for (int64_t i = 0; OB_SUCC(ret) && i < non_essential_dim_count_; ++i) {
    double score = 0.0;
    int id_cmp_ret = 0;
    const int64_t iter_idx = sorted_iters_[i];
    ObISRDimBlockMaxIter *iter = nullptr;
    if (OB_ISNULL(iter = get_iter(iter_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_FAIL(iter->advance_to(pivot_id))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance to pivot id", K(ret), K(iter_idx));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(iter->get_curr_score(score))) {
      LOG_WARN("failed to get current score", K(ret), K(iter_idx));
    } else if (OB_FAIL(iter->get_curr_id(iter_domain_ids_[iter_idx]))) {
      LOG_WARN("failed to get current id", K(ret), K(iter_idx));
    } else if (OB_FAIL(domain_id_cmp_.compare(*iter_domain_ids_[iter_idx], pivot_id, id_cmp_ret))) {
      LOG_WARN("failed to compare domain id", K(ret));
    } else if (0 != id_cmp_ret) {
      // skip
    } else if (OB_FAIL(relevance_collector_->collect_one_dim(iter_idx, score))) {
      LOG_WARN("failed to collect one dim", K(ret), K(iter_idx));
    } else {
      non_essential_score += score;
    }
  }

  double pivot_score = 0.0;
  bool got_valid_id = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(relevance_collector_->get_result(pivot_score, got_valid_id))) {
    LOG_WARN("failed to get result", K(ret));
  } else if (got_valid_id && OB_FAIL(process_collected_row(pivot_id, pivot_score))) {
    LOG_WARN("failed to process collected row", K(ret));
  }
  LOG_DEBUG("[Sparse Retrieval] eval bmm pivot", K(ret), K(pivot_id), K(non_essential_score),
      K(essential_score), K(pivot_score), K(got_valid_id));
  return ret;
}

int ObSRBMMIterImpl::forward_next_round_iters()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    ObISRDimBlockMaxIter *iter = get_iter(iter_idx);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret));
    } else if (OB_FAIL(iter->get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
