/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_sparse_bmm_iter.h"
#include "ob_block_max_iter.h"

ERRSIM_POINT_DEF(EN_BLOCK_MERGE_BUCKET_SIZE, "Override block merge bucket size");
namespace oceanbase
{
namespace storage
{

ObSRBMMIterImpl::ObSRBMMIterImpl()
  : ObSRBlockMaxTopKIterImpl(),
    sorted_iters_(),
    non_essential_dim_count_(0),
    non_essential_dim_max_score_(0.0),
    non_essential_dim_threshold_(0.0),
    all_dims_max_score_sum_(0.0),
    dim_max_scores_(),
    block_merge_bucket_size_(0),
    block_merge_upper_bound_(0),
    block_merge_doc_cnt_(0),
    block_merge_scores_(),
    block_merge_doc_ids_(),
    can_use_block_merge_(false),
    is_max_score_cached_(false)
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
  } else if (FALSE_IT(dim_max_scores_.set_allocator(&allocator_))) {
  } else if (OB_FAIL(dim_max_scores_.init(dim_iters.count()))) {
    LOG_WARN("failed to init iter max scores", K(ret));
  } else if (OB_FAIL(dim_max_scores_.prepare_allocate(dim_iters.count()))) {
    LOG_WARN("failed to prepare allocate iter max scores", K(ret));
  } else if (FALSE_IT(can_use_block_merge_ = can_use_block_merge())) {
  } else if (can_use_block_merge_ && OB_FAIL(init_block_merge_buffers())) {
    LOG_WARN("failed to init block merge buffers", K(ret));
  }
  return ret;
}

void ObSRBMMIterImpl::reuse(const bool switch_tablet)
{
  non_essential_dim_count_ = 0;
  non_essential_dim_max_score_ = 0.0;
  non_essential_dim_threshold_ = 0.0;
  all_dims_max_score_sum_ = 0.0;
  block_merge_upper_bound_ = 0;
  block_merge_doc_cnt_ = 0;
  is_max_score_cached_ = false;
  ObSRBlockMaxTopKIterImpl::reuse(switch_tablet);
}

void ObSRBMMIterImpl::reset()
{
  sorted_iters_.reset();
  non_essential_dim_count_ = 0;
  non_essential_dim_max_score_ = 0.0;
  non_essential_dim_threshold_ = 0.0;
  all_dims_max_score_sum_ = 0.0;
  dim_max_scores_.reset();
  block_merge_bucket_size_ = 0;
  block_merge_upper_bound_ = 0;
  block_merge_doc_cnt_ = 0;
  block_merge_scores_.reset();
  block_merge_doc_ids_.reset();
  can_use_block_merge_ = false;
  is_max_score_cached_ = false;
  ObSRBlockMaxTopKIterImpl::reset();
}

bool ObSRBMMIterImpl::can_use_block_merge() const
{
  return nullptr != iter_param_
         && nullptr != iter_param_->id_proj_expr_
         && common::ObUInt64Type == iter_param_->id_proj_expr_->datum_meta_.type_
         && nullptr != relevance_collector_
         && relevance_collector_->is_pure_additive();
}


int ObSRBMMIterImpl::init_block_merge_buffers()
{
  int ret = OB_SUCCESS;
  const int64_t configured_bucket_size = EN_BLOCK_MERGE_BUCKET_SIZE;
  block_merge_bucket_size_ = configured_bucket_size > 0 ? configured_bucket_size : DEFAULT_BLOCK_MERGE_BUCKET_SIZE;
  block_merge_scores_.set_allocator(&allocator_);
  block_merge_doc_ids_.set_allocator(&allocator_);
  if (OB_FAIL(block_merge_scores_.init(block_merge_bucket_size_))) {
    LOG_WARN("failed to init block merge scores", K(ret), K_(block_merge_bucket_size));
  } else if (OB_FAIL(block_merge_scores_.prepare_allocate(block_merge_bucket_size_))) {
    LOG_WARN("failed to prepare allocate block merge scores", K(ret), K_(block_merge_bucket_size));
  } else if (OB_FAIL(block_merge_doc_ids_.init(block_merge_bucket_size_))) {
    LOG_WARN("failed to init block merge doc ids", K(ret), K_(block_merge_bucket_size));
  } else if (OB_FAIL(block_merge_doc_ids_.prepare_allocate(block_merge_bucket_size_))) {
    LOG_WARN("failed to prepare allocate block merge doc ids", K(ret), K_(block_merge_bucket_size));
  } else {
    reuse_block_merge_buffers();
  }
  return ret;
}

void ObSRBMMIterImpl::reuse_block_merge_buffers()
{
  for (int64_t i = 0; i < block_merge_bucket_size_; ++i) {
    block_merge_scores_[i] = 0.0;
    block_merge_doc_ids_[i] = 0;
  }
  block_merge_doc_cnt_ = 0;
}

int ObSRBMMIterImpl::get_uint_doc_id(const ObDatum &datum, uint64_t &doc_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!can_use_block_merge_ || datum.is_null() || datum.is_ext())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not use block merge or datum is ext", K(ret), K_(can_use_block_merge), K(datum));
  } else {
    doc_id = datum.get_uint();
  }
  return ret;
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

  uint64_t block_merge_pivot_id = 0;
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
      if (can_use_block_merge_ && OB_FAIL(get_uint_doc_id(curr_pivot_id.get_datum(), block_merge_pivot_id))) {
        LOG_WARN("failed to get uint doc id from pivot id", K(ret));
      } else if (can_use_block_merge_ && block_merge_pivot_id < block_merge_upper_bound_) {
        if (OB_FAIL(block_merge_pivot_range(curr_pivot_id.get_datum(), non_essential_block_max_score))) {
          LOG_WARN("failed to block merge pivot range", K(ret), K(curr_pivot_id),
              K_(block_merge_upper_bound), K(non_essential_block_max_score));
        } else if (OB_FAIL(try_update_essential_dims())) {
          LOG_WARN("failed to rebuild essential dims after block merge", K(ret));
        } else {
          status_ = BMSearchStatus::FIND_NEXT_PIVOT;
        }
      } else {
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
  double next_non_ess_max_score = 0;
  relevance_collector_->reuse();
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
    } else if (OB_FAIL(relevance_collector_->collect_one_dim(iter_idx, score))) {
      LOG_WARN("failed to collect one dimension", K(ret));
    } else if (OB_FAIL(relevance_collector_->get_partial_result(next_non_ess_max_score))) {
      LOG_WARN("failed to get result", K(ret));
    } else {
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

int ObSRBMMIterImpl::decide_block_merge_upper_bound(
    const ObMaxScoreTuple &max_score_tuple,
    bool &found_upper_bound)
{
  int ret = OB_SUCCESS;
  uint64_t tuple_max_doc_id = 0;
  if (OB_UNLIKELY(nullptr == max_score_tuple.max_domain_id_ || max_score_tuple.max_domain_id_->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null max domain id in max score tuple", K(ret), K(max_score_tuple.max_score_));
  } else if (max_score_tuple.max_domain_id_->is_max()) {
    LOG_DEBUG("ext max domain id in max score tuple", K(max_score_tuple.max_score_));
  } else if (OB_FAIL(get_uint_doc_id(*max_score_tuple.max_domain_id_, tuple_max_doc_id))) {
    LOG_WARN("failed to get uint doc id from tuple max domain id", K(ret));
  } else if (!found_upper_bound) {
    block_merge_upper_bound_ = tuple_max_doc_id;
    found_upper_bound = true;
  } else {
    block_merge_upper_bound_ = MIN(block_merge_upper_bound_, tuple_max_doc_id);
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("[Sparse Retrieval] update block merge range upper bound",
        K(tuple_max_doc_id), K(found_upper_bound), K_(block_merge_upper_bound),
        K(max_score_tuple.max_score_));
  }
  return ret;
}

int ObSRBMMIterImpl::evaluate_pivot_range(
    const ObDatum &pivot_id,
    double &non_essential_block_max_score,
    bool &is_candidate)
{
  int ret = OB_SUCCESS;
  is_candidate = false;

  relevance_collector_->reuse();
  const double essential_threshold = get_essential_dim_threshold();
  const ObMaxScoreTuple *max_score_tuple = nullptr;
  block_merge_upper_bound_ = 0;
  bool found_upper_bound = false;
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
    } else if (OB_FAIL(relevance_collector_->collect_one_dim(iter_idx, max_score_tuple->max_score_))) {
      LOG_WARN("failed to collect one dimension", K(ret));
    } else if (can_use_block_merge_ && OB_FAIL(decide_block_merge_upper_bound(*max_score_tuple, found_upper_bound))) {
      LOG_WARN("failed to update range upper bound from essential tuple", K(ret), K(iter_idx));
    }
  }

  double essential_block_max_score = 0.0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(relevance_collector_->get_partial_result(essential_block_max_score))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    is_candidate = essential_block_max_score > essential_threshold;
  }

  if (OB_SUCC(ret) && is_candidate) {
    relevance_collector_->reuse();
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
      } else if (OB_FAIL(relevance_collector_->collect_one_dim(iter_idx, max_score_tuple->max_score_))) {
        LOG_WARN("failed to collect one dimension", K(ret));
      } else if (can_use_block_merge_ && OB_FAIL(decide_block_merge_upper_bound(*max_score_tuple, found_upper_bound))) {
        LOG_WARN("failed to update range upper bound from non-essential tuple", K(ret), K(iter_idx));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(relevance_collector_->get_partial_result(non_essential_block_max_score))) {
      LOG_WARN("failed to get result", K(ret));
    } else {
      is_candidate = essential_block_max_score + non_essential_block_max_score > top_k_threshold;
    }
  }

  LOG_DEBUG("[Sparse Retrieval] eval bmm pivot", K(ret), K(pivot_id), K(is_candidate),
      K(essential_block_max_score + non_essential_block_max_score),
      K(non_essential_block_max_score), K(essential_threshold), K(get_top_k_threshold()),
      K_(can_use_block_merge), K_(block_merge_upper_bound), K_(sorted_iters));
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
  bool need_project = true;

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
  } else if (OB_FAIL(collect_dims_by_id(true, collected_id, essential_score, need_project))) {
    LOG_WARN("failed to collect dims by id", K(ret));
  } else {
    is_candidate = non_essential_block_max_score + essential_score > get_top_k_threshold();
  }
  LOG_DEBUG("[Sparse Retrieval] eval essential pivot", K(ret), K(pivot_id), KPC(collected_id),
        K(non_essential_block_max_score), K(essential_score), K(is_candidate), K(is_valid_pivot));
  return ret;
}

int ObSRBMMIterImpl::block_merge_pivot_range(
    const ObDatum &range_min_id_datum,
    const double non_essential_block_max_score)
{
  int ret = OB_SUCCESS;
  uint64_t range_min_id = 0;
  bool top_k_updated = false;
  bool stop_block_merge = false;
  if (OB_FAIL(get_uint_doc_id(range_min_id_datum, range_min_id))) {
    LOG_WARN("failed to get uint doc id from candidate range min id", K(ret), K(range_min_id_datum));
  } else if (OB_UNLIKELY(block_merge_upper_bound_ < range_min_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected reversed candidate range", K(ret),
        K(range_min_id), K_(block_merge_upper_bound));
  }

  ObStorageDatum datum_buf;
  uint64_t block_start_id = range_min_id;
  while (OB_SUCC(ret) && !top_k_updated && block_start_id <= block_merge_upper_bound_) {
    const bool is_first_block = block_start_id == range_min_id;
    const uint64_t remaining_doc_cnt = block_merge_upper_bound_ - block_start_id + 1;
    const int64_t bucket_cnt = remaining_doc_cnt < block_merge_bucket_size_ ? remaining_doc_cnt : block_merge_bucket_size_;
    const uint64_t block_end_id = block_start_id + bucket_cnt - 1;
    bool has_valid_doc_in_block = false;

    reuse_block_merge_buffers();
    datum_buf.set_uint(block_start_id);
    if (OB_FAIL(block_merge_essential_dims(is_first_block, datum_buf, block_end_id, has_valid_doc_in_block))) {
      LOG_WARN("failed to block merge essential dims", K(ret), K(datum_buf), K(block_end_id));
    } else if (has_valid_doc_in_block) {
      const double top_k_threshold = get_top_k_threshold();
      for (int64_t offset = 0; offset < bucket_cnt; ++offset) {
        if (block_merge_scores_[offset] > 0 && block_merge_scores_[offset] + non_essential_block_max_score > top_k_threshold) {
          block_merge_doc_ids_[block_merge_doc_cnt_] = block_start_id + offset;
          block_merge_scores_[block_merge_doc_cnt_] = block_merge_scores_[offset];
          block_merge_doc_cnt_++;
        }
      }
      if (block_merge_doc_cnt_ > 0 && OB_FAIL(block_merge_non_essential_dims(datum_buf))) {
        LOG_WARN("failed to block merge non-essential dims", K(ret), K_(block_merge_doc_cnt));
      }
    }

    for (int64_t valid_doc_idx = 0; OB_SUCC(ret) && valid_doc_idx < block_merge_doc_cnt_; ++valid_doc_idx) {
      const uint64_t id = block_merge_doc_ids_[valid_doc_idx];
      const double relevance = block_merge_scores_[valid_doc_idx];
      bool is_top_k = false;
      datum_buf.set_uint(id);
      if (OB_FAIL(process_collected_row(datum_buf, relevance, is_top_k))) {
        LOG_WARN("failed to process block merge row", K(ret), K(valid_doc_idx), K(id), K(relevance));
      } else if (is_top_k) {
        top_k_updated = true;
        LOG_DEBUG("[Sparse Retrieval] block merge updates topk",
            K(valid_doc_idx), K(id), K(block_start_id), K(relevance),
            K(is_top_k), K(top_k_updated), "top_k_threshold", get_top_k_threshold());
      } else {
        LOG_DEBUG("[Sparse Retrieval] block merge doc evaluated",
            K(valid_doc_idx), K(id), K(block_start_id), K(relevance),
            K(is_top_k), "top_k_threshold", get_top_k_threshold());
      }
    }

    if (OB_FAIL(ret)) {
    } else if (top_k_updated) {
      LOG_DEBUG("[Sparse Retrieval] stop block merge after topk update",
          K(block_start_id), K(block_end_id), K(bucket_cnt),
          K_(block_merge_upper_bound), "top_k_threshold", get_top_k_threshold());
    } else {
      block_start_id = block_end_id + 1;
      LOG_DEBUG("[Sparse Retrieval] advance block merge",
          K(block_end_id), K(block_start_id), K_(block_merge_upper_bound));
    }
  }

  if (OB_FAIL(ret) || !top_k_updated) {
  } else if (OB_FAIL(update_filter_thresholds_after_topk_update())) {
    LOG_WARN("failed to update filter thresholds after block merge topk update", K(ret));
  } else {
    LOG_DEBUG("[Sparse Retrieval] finish block merge range",
        K(range_min_id), K_(block_merge_upper_bound), K(top_k_updated),
        "top_k_threshold", get_top_k_threshold());
  }
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
  bool is_top_k = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(relevance_collector_->get_result(pivot_score, got_valid_id))) {
    LOG_WARN("failed to get result", K(ret));
  } else if (got_valid_id && OB_FAIL(process_collected_row(pivot_id, pivot_score, is_top_k))) {
    LOG_WARN("failed to process collected row", K(ret));
  } else if (is_top_k) {
    if (OB_FAIL(update_filter_thresholds_after_topk_update())) {
      LOG_WARN("failed to update filter thresholds after topk update", K(ret));
    }
  }
  LOG_DEBUG("[Sparse Retrieval] eval bmm pivot", K(ret), K(pivot_id), K(non_essential_score),
      K(essential_score), K(pivot_score), K(got_valid_id));
  return ret;
}


int ObSRBMMIterImpl::calc_other_dims_max_score_sum(const int64_t iter_idx, double &max_score_sum)
{
  int ret = OB_SUCCESS;
  if (!is_max_score_cached_) {
    all_dims_max_score_sum_ = 0.0;
    for (int64_t j = 0; OB_SUCC(ret) && j < dim_iters_->count(); ++j) {
      ObISRDimBlockMaxIter *iter = get_iter(j);
      if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null iter", K(ret), K(j));
      } else {
        double dim_max_score = 0.0;
        if (OB_FAIL(iter->get_dim_max_score(dim_max_score))) {
          LOG_WARN("failed to get iter dim max score", K(ret), K(j));
        } else {
          dim_max_scores_.at(j) = dim_max_score;
          all_dims_max_score_sum_ += dim_max_score;
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_max_score_cached_ = true;
      LOG_DEBUG("[Sparse Retrieval] cached all iters max score sum", K(all_dims_max_score_sum_), K(dim_iters_->count()));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(iter_idx < 0 || iter_idx >= dim_iters_->count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter idx", K(ret), K(iter_idx), K(dim_iters_->count()));
    } else {
      max_score_sum = all_dims_max_score_sum_ - dim_max_scores_.at(iter_idx);
    }
  }
  return ret;
}

int ObSRBMMIterImpl::set_filter_threshold_for_dim(const int64_t iter_idx, ObISRDimBlockMaxIter *iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null iter", K(ret));
  } else {
    double other_iters_max_score_sum = 0.0;
    if (OB_FAIL(calc_other_dims_max_score_sum(iter_idx, other_iters_max_score_sum))) {
      LOG_WARN("failed to calc other iters max score sum", K(ret), K(iter_idx));
    } else {
      const double top_k_threshold = get_top_k_threshold();
      const double filter_threshold = top_k_threshold - other_iters_max_score_sum;
      iter->set_filter_threshold(filter_threshold);
      LOG_DEBUG("[Sparse Retrieval] set filter threshold for iter", K(iter_idx), K(filter_threshold),
          K(top_k_threshold), K(other_iters_max_score_sum));
    }
  }
  return ret;
}

int ObSRBMMIterImpl::update_filter_thresholds_after_topk_update()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
    ObISRDimBlockMaxIter *iter = get_iter(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(i));
    } else if (OB_FAIL(set_filter_threshold_for_dim(i, iter))) {
      LOG_WARN("failed to set filter threshold for iter", K(ret), K(i));
    }
  }
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
    } else if (OB_FAIL(set_filter_threshold_for_dim(iter_idx, iter))) {
      LOG_WARN("failed to set filter threshold for iter", K(ret), K(iter_idx));
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

int ObSRBMMIterImpl::block_merge_essential_dims(
    const bool is_first_block,
    const ObDatum &block_start_id_datum,
    const uint64_t block_end_id,
    bool &has_valid_doc_in_block)
{
  int ret = OB_SUCCESS;
  for (int64_t i = non_essential_dim_count_; OB_SUCC(ret) && i < sorted_iters_.count(); ++i) {
    bool reach_block_end = false;
    const int64_t iter_idx = sorted_iters_[i];
    ObISRDimBlockMaxIter *iter = get_iter(iter_idx);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
    } else if (is_first_block && OB_FAIL(iter->advance_to(block_start_id_datum))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance iter to block merge start id", K(ret), K(iter_idx), K(block_start_id_datum));
      } else {
        ret = OB_SUCCESS;
      }
    }
    while (OB_SUCC(ret) && !reach_block_end && !iter->iter_end()) {
      const ObDatum *curr_id_datum = nullptr;
      uint64_t curr_id = 0;
      double curr_score = 0.0;
      if (OB_FAIL(iter->get_curr_id(curr_id_datum))) {
        LOG_WARN("failed to get curr id from iter", K(ret), K(iter_idx));
      } else if (OB_ISNULL(curr_id_datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null curr id", K(ret), K(iter_idx));
      } else if (OB_FAIL(get_uint_doc_id(*curr_id_datum, curr_id))) {
        LOG_WARN("failed to get uint doc id from iter datum", K(ret), K(iter_idx));
      } else if (curr_id > block_end_id) {
        reach_block_end = true;
      } else if (OB_FAIL(iter->get_curr_score(curr_score))) {
        LOG_WARN("failed to get curr score from iter", K(ret), K(iter_idx), KPC(curr_id_datum));
      } else {
        has_valid_doc_in_block = true;
        const int64_t offset = static_cast<int64_t>(curr_id - block_start_id_datum.get_uint());
        block_merge_scores_[offset] += curr_score;
        LOG_DEBUG("[Sparse Retrieval] collect block merge score",
            K(iter_idx), K(curr_id), K(block_start_id_datum), K(offset), K(curr_score),
            K_(block_merge_doc_cnt), "active_score", block_merge_scores_[offset]);
        if (OB_FAIL(iter->get_next_row())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get next row in block", K(ret), K(iter_idx), KPC(curr_id_datum));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
    }
  }
  return ret;
}

int ObSRBMMIterImpl::block_merge_non_essential_dims(ObDatum &doc_id_datum)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < non_essential_dim_count_; ++i) {
    const int64_t iter_idx = sorted_iters_[i];
    ObISRDimBlockMaxIter *iter = get_iter(iter_idx);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null non-essential iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
    } else {
      int64_t doc_idx = 0;
      while (OB_SUCC(ret) && doc_idx < block_merge_doc_cnt_) {
        const uint64_t doc_id = block_merge_doc_ids_[doc_idx];
        const ObDatum *curr_id_datum = nullptr;
        uint64_t curr_id = 0;
        double curr_score = 0.0;
        doc_id_datum.set_uint(doc_id);
        if (OB_FAIL(iter->advance_to(doc_id_datum))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to advance non-essential iter to candidate doc", K(ret), K(iter_idx), K(doc_id));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(iter->get_curr_id(curr_id_datum))) {
          LOG_WARN("failed to get current id from non-essential iter", K(ret), K(iter_idx), K(doc_id));
        } else if (OB_ISNULL(curr_id_datum)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null current id from non-essential iter", K(ret), K(iter_idx), K(doc_id));
        } else if (OB_FAIL(get_uint_doc_id(*curr_id_datum, curr_id))) {
          LOG_WARN("failed to get uint doc id from non-essential iter curr_id_datum", K(ret), K(iter_idx), K(doc_id));
        } else {
          for (; OB_SUCC(ret) && doc_idx < block_merge_doc_cnt_; ++doc_idx) {
            if (block_merge_doc_ids_[doc_idx] == curr_id) {
              if (OB_FAIL(iter->get_curr_score(curr_score))) {
                LOG_WARN("failed to get current score from non-essential iter", K(ret), K(iter_idx), K(curr_id));
              } else {
                block_merge_scores_[doc_idx] += curr_score;
                LOG_DEBUG("[Sparse Retrieval] refine block merge candidate with non-essential score",
                    K(iter_idx), K(doc_idx), K(curr_id), K(curr_score),
                    "refined_score", block_merge_scores_[doc_idx]);
              }
              doc_idx++;
              break;
            } else if (block_merge_doc_ids_[doc_idx] > curr_id) {
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
