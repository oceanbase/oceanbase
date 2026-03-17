/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_bmm_op.h"
#include "sql/das/search/ob_disjunctive_daat_merger.h"

namespace oceanbase
{
namespace sql
{

int ObDASBMMOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(score_ops_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score ops is null", KR(ret));
  } else if (OB_FAIL(children.assign(*score_ops_))) {
    LOG_WARN("failed to assign score ops", KR(ret));
  }
  return ret;
}

ObDASBMMOp::ObDASBMMOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    iter_allocator_(nullptr),
    merge_iters_(),
    sorted_iter_idxes_(),
    non_essential_dim_count_(0),
    non_essential_dim_max_score_(0.0),
    non_essential_dim_threshold_(0.0),
    row_merger_(nullptr),
    filter_op_(nullptr),
    curr_filter_id_(),
    status_(SearchStatus::MAX_STATUS),
    query_optional_(false),
    iters_sorted_(false),
    filter_end_(false),
    is_inited_(false) {}


int ObDASBMMOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASBMMOpParam &param = static_cast<const ObDASBMMOpParam &>(op_param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (OB_FAIL(init_merge_iters(param, *param.get_iter_allocator()))) {
    LOG_WARN("failed to init merge iters", K(ret), K(param));
  } else if (OB_FAIL(ObIDisjunctiveDAATMerger::alloc_merger(
      merge_iters_,
      search_ctx_.get_rowid_exprs(),
      search_ctx_.get_rowid_meta(),
      search_ctx_.get_rowid_type(),
      false,
      *param.get_iter_allocator(),
      row_merger_))) {
    LOG_WARN("failed to alloc row merger", K(ret), K(param));
  } else {
    status_ = SearchStatus::FIND_NEXT_PIVOT;
    iter_allocator_ = param.get_iter_allocator();
    filter_op_ = param.get_filter_op();
    curr_filter_id_.set_min();
    query_optional_ = param.query_optional();
    iters_sorted_ = false;
    filter_end_ = false;
    min_competitive_score_ = 0.0;
    non_essential_dim_count_ = 0;
    non_essential_dim_max_score_ = 0.0;
    non_essential_dim_threshold_ = 0.0;
    is_inited_ = true;
  }
  return ret;
}

int ObDASBMMOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->open())) {
      LOG_WARN("failed to open child", K(ret));
    }
  }
  if (OB_SUCC(ret) && has_filter()) {
    if (OB_FAIL(filter_op_->open())) {
      LOG_WARN("failed to open filter op", K(ret));
    }
  }
  return ret;
}

int ObDASBMMOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(row_merger_->reuse())) {
    LOG_WARN("failed to reuse row merger", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i]) || OB_ISNULL(merge_iters_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", K(ret), K(i), KP(children_[i]), KP(merge_iters_[i]));
    } else if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("failed to rescan child", K(ret));
    } else {
      merge_iters_[i]->reuse();
      sorted_iter_idxes_[i] = i;
    }
  }

  if (OB_SUCC(ret) && has_filter()) {
    if (OB_FAIL(filter_op_->rescan())) {
      LOG_WARN("failed to rescan filter op", K(ret));
    } else {
      curr_filter_id_.set_min();
    }
  }

  if (OB_SUCC(ret)) {
    status_ = SearchStatus::FIND_NEXT_PIVOT;
    min_competitive_score_ = 0.0;
    iters_sorted_ = false;
    filter_end_ = false;
    non_essential_dim_count_ = 0;
    non_essential_dim_max_score_ = 0.0;
    non_essential_dim_threshold_ = 0.0;
  }
  return ret;
}

int ObDASBMMOp::do_close()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  for (int64_t i = 0; i < merge_iters_.count(); ++i) {
    if (nullptr != merge_iters_[i]) {
      merge_iters_[i]->~ObDAATMergeIter();
      if (nullptr != iter_allocator_) {
        iter_allocator_->free(merge_iters_[i]);
      }
      merge_iters_[i] = nullptr;
    }
  }
  merge_iters_.reset();
  sorted_iter_idxes_.reset();
  if (nullptr != row_merger_) {
    row_merger_->~ObIDisjunctiveDAATMerger();
    if (nullptr != iter_allocator_) {
      iter_allocator_->free(row_merger_);
    }
    row_merger_ = nullptr;
  }
  filter_op_ = nullptr;
  query_optional_ = false;
  filter_end_ = false;
  iter_allocator_ = nullptr;
  return ret;
}

int ObDASBMMOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (0 == min_competitive_score_) {
    if (OB_FAIL(inner_get_next_row_plain(next_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row plain");
    }
  } else if (OB_FAIL(inner_get_next_row_with_pruning(next_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row with pruning");
  }
  return ret;
}

int ObDASBMMOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  return OB_NOT_SUPPORTED;
}

int ObDASBMMOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters_.count(); ++i) {
      if (OB_ISNULL(merge_iters_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null merge iter", K(ret), K(i));
      } else if (merge_iters_[i]->iter_end()) {
        // skip
      } else if (OB_FAIL(merge_iters_[i]->advance_to(target))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to advance to", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_merger_->reuse_with_selected_iters(
      sorted_iter_idxes_, false, non_essential_dim_count_, sorted_iter_idxes_.count() - 1))) {
    LOG_WARN("failed to rebuild with selected iters", K(ret));
  } else if (OB_FAIL(do_next_rowid(curr_id, score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rowid", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObDASBMMOp::do_set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(ObIDASSearchOp::do_set_min_competitive_score(threshold))) {
    LOG_WARN("failed to set min competitive score", K(ret));
  } else if (!iters_sorted_ && OB_FAIL(sort_iters_by_max_score())) {
    LOG_WARN("failed to sort iters by max score", K(ret));
  } else if (OB_FAIL(try_update_essential_dims())) {
    LOG_WARN("failed to try update essential dims", K(ret));
  }
  return ret;
}

int ObDASBMMOp::init_merge_iters(const ObDASBMMOpParam &op_param, ObIAllocator &iter_allocator)
{
  int ret = OB_SUCCESS;
  merge_iters_.set_allocator(&iter_allocator);
  sorted_iter_idxes_.set_allocator(&iter_allocator);
  const int64_t score_op_cnt = op_param.get_score_ops()->count();
  if (OB_FAIL(merge_iters_.init(score_op_cnt))) {
    LOG_WARN("failed to init merge iters", K(ret));
  } else if (OB_FAIL(merge_iters_.prepare_allocate(score_op_cnt))) {
    LOG_WARN("failed to prepare allocate merge iters", K(ret));
  } else if (OB_FAIL(sorted_iter_idxes_.init(score_op_cnt))) {
    LOG_WARN("failed to init sorted iter idxes", K(ret));
  } else if (OB_FAIL(sorted_iter_idxes_.prepare_allocate(score_op_cnt))) {
    LOG_WARN("failed to prepare allocate sorted iter idxes", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < score_op_cnt; ++i) {
      ObDAATMergeIter *merge_iter = OB_NEWx(ObDAATMergeIter, &iter_allocator);
      const int64_t iter_idx = i;
      if (OB_ISNULL(merge_iter)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for merge iter", K(ret));
      } else if (OB_FAIL(merge_iter->init(iter_idx, *op_param.get_score_ops()->at(i)))) {
        LOG_WARN("failed to init merge iter", K(ret));
      } else {
        merge_iters_[i] = merge_iter;
        sorted_iter_idxes_[i] = iter_idx;
      }
    }
  }
  return ret;
}

int ObDASBMMOp::inner_get_next_row_plain(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  int64_t dim_cnt = 0;
  if (has_filter()) {
    if (OB_FAIL(get_next_row_plain_with_filter(next_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next row plain with filter", K(ret));
    }
  } else if (OB_FAIL(row_merger_->rebuild_with_next())) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with next", K(ret));
  } else if (OB_FAIL(row_merger_->merge_one_row(next_id, score, dim_cnt))) {
    LOG_WARN("failed to merge one row", K(ret));
  }
  return ret;
}

int ObDASBMMOp::inner_get_next_row_with_pruning(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(SearchStatus::FINISHED == status_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(SearchStatus::FIND_NEXT_PIVOT != status_
      || !iters_sorted_
      || 0 == min_competitive_score_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status for pruning", K(ret), K_(status), K(iters_sorted_), K(min_competitive_score_));
  }

  bool found_row = false;
  ObDASRowID pivot_id;
  double non_essential_block_max_score = 0.0;
  while (OB_SUCC(ret) && !found_row && SearchStatus::FINISHED != status_) {
    switch (status_) {
    case SearchStatus::FIND_NEXT_PIVOT: {
      double max_score = 0.0;
      if (has_filter()) {
        if (OB_FAIL(find_next_pivot_with_filter(pivot_id))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to find next pivot with filter", K(ret));
        }
      } else if (OB_FAIL(row_merger_->rebuild_with_next())) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with next", K(ret));
      } else if (OB_FAIL(row_merger_->pop_until_found_pivot(0, 1, pivot_id, max_score))) {
        LOG_WARN("failed to pop until found pivot", K(ret));
      }
      if (OB_SUCC(ret)) {
        status_ = SearchStatus::EVALUATE_PIVOT_RANGE;
      }
      break;
    }
    case SearchStatus::EVALUATE_PIVOT_RANGE: {
      bool is_candidate = false;
      non_essential_block_max_score = 0.0;
      if (OB_FAIL(evaluate_pivot_range(pivot_id, non_essential_block_max_score, is_candidate))) {
        LOG_WARN("failed to evaluate pivot range", K(ret));
      } else if (is_candidate) {
        status_ = SearchStatus::EVALUATE_PIVOT;
      } else {
        status_ = SearchStatus::FIND_NEXT_PIVOT_RANGE;
      }
      break;
    }
    case SearchStatus::EVALUATE_PIVOT: {
      if (OB_FAIL(evaluate_bmm_pivot(pivot_id, non_essential_block_max_score, next_id, score, found_row))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to evaluate bmm pivot", K(ret));
      } else {
        status_ = SearchStatus::FIND_NEXT_PIVOT;
      }
      break;
    }
    case SearchStatus::FIND_NEXT_PIVOT_RANGE: {
      if (OB_FAIL(row_merger_->to_next_range_with_shallow_iters(get_essential_dim_threshold()))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to to next range with shallow iters", K(ret));
        }
      } else {
        status_ = SearchStatus::FIND_NEXT_PIVOT;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", K(ret), K_(status));
    }
    }
  }

  if (OB_ITER_END == ret) {
    status_ = SearchStatus::FINISHED;
  }
  return ret;
}


int ObDASBMMOp::sort_iters_by_max_score()
{
  int ret = OB_SUCCESS;

  class DimMaxScoreCmp
  {
  public:
    DimMaxScoreCmp(const ObIArray<ObDAATMergeIter *> &merge_iters, int &ret)
      : merge_iters_(merge_iters), ret_(ret)
    {}
    bool operator()(const int64_t lhs, const int64_t rhs) const
    {
      int ret = OB_SUCCESS;
      double score1 = 0.0;
      double score2 = 0.0;
      if (OB_FAIL(merge_iters_.at(lhs)->get_max_score(score1))) {
        LOG_WARN("failed to get dim max score", K(ret));
      } else if (OB_FAIL(merge_iters_.at(rhs)->get_max_score(score2))) {
        LOG_WARN("failed to get dim max score", K(ret));
      }
      ret_ = ret;
      return score1 < score2;
    }
  private:
    const ObIArray<ObDAATMergeIter *> &merge_iters_;
    int &ret_;
  };

  DimMaxScoreCmp cmp(merge_iters_, ret);
  lib::ob_sort(sorted_iter_idxes_.begin(), sorted_iter_idxes_.end(), cmp);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to sort iters by max score", K(ret));
  } else {
    iters_sorted_ = true;
  }
  return ret;
}

int ObDASBMMOp::try_update_essential_dims()
{
  int ret = OB_SUCCESS;

  // Notice that non_essential_dim_max_score could decrease with non-essential dims iter end
  double curr_non_ess_max_score = 0;
  for (int64_t i = 0;
      OB_SUCC(ret) && i < sorted_iter_idxes_.count() && non_essential_dim_threshold_ <= min_competitive_score_;
      ++i) {
    const int64_t iter_idx = sorted_iter_idxes_[i];
    ObDAATMergeIter *iter = merge_iters_[iter_idx];
    double score = 0.0;
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_FAIL(iter->get_max_score(score))) {
      LOG_WARN("failed to get curr score", K(ret));
    } else {
      const double next_non_ess_max_score = curr_non_ess_max_score + score;
      if (next_non_ess_max_score > min_competitive_score_) {
        non_essential_dim_count_ = (i - 1) >= 0 ? i : 0;
        non_essential_dim_max_score_ = curr_non_ess_max_score;
        non_essential_dim_threshold_ = next_non_ess_max_score;
      } else {
        curr_non_ess_max_score = next_non_ess_max_score;
      }
    }
  }

  if (FAILEDx(row_merger_->reuse_with_selected_iters(
      sorted_iter_idxes_, true, non_essential_dim_count_, sorted_iter_idxes_.count() - 1))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to reuse with selected iters", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  LOG_DEBUG("[Sparse Retrieval] try update essential dims", K(ret), K(non_essential_dim_count_), K(non_essential_dim_max_score_),
      K(non_essential_dim_threshold_), K(min_competitive_score_), K(sorted_iter_idxes_));
  return ret;
}

int ObDASBMMOp::evaluate_pivot_range(
    const ObDASRowID &pivot_id,
    double &non_essential_block_max_score,
    bool &is_candidate)
{
  int ret = OB_SUCCESS;
  is_candidate = false;
  non_essential_block_max_score = 0.0;
  double essential_block_max_score = 0.0;
  if (OB_FAIL(row_merger_->advance_for_block_max_score(pivot_id, essential_block_max_score))) {
    LOG_WARN("failed to advance for block max score", K(ret));
  } else if (essential_block_max_score <= get_essential_dim_threshold()) {
    is_candidate = false;
  } else {
    const MaxScoreTuple *max_score_tuple = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < non_essential_dim_count_; ++i) {
      const int64_t iter_idx = sorted_iter_idxes_[i];
      ObDAATMergeIter *iter = merge_iters_[iter_idx];
      if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
      } else if (iter->iter_end()) {
        // skip
      } else if (OB_FAIL(iter->advance_shallow(pivot_id, true))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to advance shallow", K(ret), K(iter_idx));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(iter->get_curr_block_max_info(max_score_tuple))) {
        LOG_WARN("failed to get curr block max info", K(ret), K(iter_idx));
      } else {
        non_essential_block_max_score += max_score_tuple->get_max_score();
      }
    }
    if (OB_SUCC(ret)) {
      is_candidate = non_essential_block_max_score + essential_block_max_score > min_competitive_score_;
    }
  }
  LOG_DEBUG("[Sparse Retrieval] eval bmm pivot range", K(ret), K(pivot_id), K(is_candidate),
      K(non_essential_block_max_score), K(essential_block_max_score), K(get_essential_dim_threshold()),
      K_(non_essential_dim_count), K_(min_competitive_score));
  return ret;
}

int ObDASBMMOp::evaluate_essential_pivot(
    const ObDASRowID &pivot_id,
    const double non_essential_block_max_score,
    ObDASRowID &collected_id,
    double &essential_score,
    bool &is_candidate)
{
  int ret = OB_SUCCESS;
  is_candidate = false;
  int cmp_ret = 0;
  int64_t dim_cnt = 0;
  if (OB_FAIL(row_merger_->rebuild_with_advance(pivot_id))) {
    LOG_WARN("failed to rebuild with advance", K(ret));
  } else if (OB_FAIL(row_merger_->merge_one_row(collected_id, essential_score, dim_cnt))) {
    LOG_WARN("failed to merge one row", K(ret));
  } else if (OB_FAIL(search_ctx_.compare_rowid(collected_id, pivot_id, cmp_ret))) {
    LOG_WARN("failed to compare rowid", K(ret));
  } else if (0 != cmp_ret) {
    // collected id different from pivot id, need full evaluation
    is_candidate = true;
  } else {
    is_candidate = non_essential_block_max_score + essential_score > min_competitive_score_;
  }
  LOG_DEBUG("[Sparse Retrieval] eval essential pivot", K(ret), K(pivot_id), K(collected_id),
        K(non_essential_block_max_score), K(essential_score), K_(min_competitive_score), K(is_candidate));
  return ret;
}

int ObDASBMMOp::evaluate_pivot(const ObDASRowID &pivot_id, const double &essential_score, double &score)
{
  int ret = OB_SUCCESS;
  double non_essential_score = 0.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < non_essential_dim_count_; ++i) {
    const int64_t iter_idx = sorted_iter_idxes_[i];
    ObDAATMergeIter *iter = merge_iters_[iter_idx];
    ObDASRowID curr_id;
    int cmp_ret = 0;
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null iter", K(ret), K(iter_idx));
    } else if (iter->iter_end()) {
      // skip
    } else if (OB_FAIL(iter->advance_to(pivot_id))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance to pivot id", K(ret), K(iter_idx));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(iter->get_curr_score(score))) {
      LOG_WARN("failed to get curr score", K(ret), K(iter_idx));
    } else if (OB_FAIL(iter->get_curr_id(curr_id))) {
      LOG_WARN("failed to get curr id", K(ret), K(iter_idx));
    } else if (OB_FAIL(search_ctx_.compare_rowid(curr_id, pivot_id, cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret));
    } else if (0 != cmp_ret) {
      // skip
    } else if (OB_FAIL(iter->get_curr_score(score))) {
      LOG_WARN("failed to get curr score", K(ret), K(iter_idx));
    } else {
      non_essential_score += score;
    }
  }

  if (OB_SUCC(ret)) {
    score = non_essential_score + essential_score;
  }

  LOG_DEBUG("[Sparse Retrieval] eval pivot", K(ret), K(pivot_id), K(essential_score),
      K(non_essential_score), K(score));
  return ret;
}

int ObDASBMMOp::next_filter_rowid(ObDASRowID &next_id)
{
  int ret = OB_SUCCESS;
  double filter_score = 0.0;
  if (OB_UNLIKELY(filter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(filter_op_->next_rowid(next_id, filter_score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rowid", K(ret));
    } else {
      filter_end_ = true;
    }
  }
  return ret;
}

int ObDASBMMOp::advance_filter_to(const ObDASRowID &target, ObDASRowID &curr_id)
{
  int ret = OB_SUCCESS;
  double filter_score = 0.0;
  if (OB_UNLIKELY(filter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(filter_op_->advance_to(target, curr_id, filter_score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to target", K(ret));
    } else {
      filter_end_ = true;
    }
  }
  return ret;
}

int ObDASBMMOp::get_next_row_plain_with_filter(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(filter_end_)) {
    ret = OB_ITER_END;
  } else {
    bool found_row = false;
    int64_t dim_cnt = 0;
    bool project_filter_id = false;
    if (curr_filter_id_.is_min() && OB_FAIL(next_filter_rowid(curr_filter_id_))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rowid from filter", K(ret));
    } else if (OB_FAIL(row_merger_->pop_until_target(curr_filter_id_))) {
      LOG_WARN("failed to pop until target", K(ret), K_(curr_filter_id));
    } else if (OB_FAIL(row_merger_->rebuild_with_advance(curr_filter_id_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to rebuild with advance", K(ret), K_(curr_filter_id));
      } else if (query_optional_) {
        ret = OB_SUCCESS;
        project_filter_id = true;
      }
    }

    ObDASRowID top_id;
    while(OB_SUCC(ret) && !found_row && !project_filter_id) {
      int cmp_ret = 0;
      if (OB_FAIL(row_merger_->get_top_id(top_id))) {
        LOG_WARN("failed to get top id", K(ret));
      } else if (OB_FAIL(search_ctx_.compare_rowid(top_id, curr_filter_id_, cmp_ret))) {
        LOG_WARN("failed to compare rowid", K(ret), K(top_id), K_(curr_filter_id));
      } else if (OB_UNLIKELY(cmp_ret < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected top id smaller than curr_filter_id", K(ret), K(top_id), K(curr_filter_id_));
      } else if (cmp_ret == 0) {
        // filter aligned with next row
      } else if (query_optional_) {
        project_filter_id = true;
      } else if (OB_FAIL(advance_filter_to(top_id, curr_filter_id_))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to advance filter to top id", K(ret), K(top_id));
      } else if (OB_FAIL(search_ctx_.compare_rowid(top_id, curr_filter_id_, cmp_ret))) {
        LOG_WARN("failed to compare rowid", K(ret), K(top_id), K_(curr_filter_id));
      }

      if (OB_FAIL(ret)) {
      } else if (project_filter_id) {
        // skip
      } else if (cmp_ret != 0) {
        if (OB_FAIL(row_merger_->pop_until_target(curr_filter_id_))) {
          LOG_WARN("failed to pop until target", K(ret), K_(curr_filter_id));
        } else if (OB_FAIL(row_merger_->rebuild_with_advance(curr_filter_id_))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with advance", K(ret), K_(curr_filter_id));
        }
      } else if (OB_FAIL(row_merger_->merge_one_row(next_id, score, dim_cnt))) {
        LOG_WARN("failed to merge one row", K(ret));
      } else {
        found_row = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (project_filter_id) {
        next_id = curr_filter_id_;
        score = 0.0;
      }

      if (!found_row && !project_filter_id) {
        // only move filter op forward when filter id is evaluated
      } else if (OB_FAIL(next_filter_rowid(curr_filter_id_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next rowid", K(ret));
        } else {
          // return last row
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObDASBMMOp::find_next_pivot_with_filter(ObDASRowID &pivot_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(filter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(curr_filter_id_.is_min()) && OB_FAIL(next_filter_rowid(curr_filter_id_))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to get next rowid from filter", K(ret));
  } else if (OB_FAIL(row_merger_->pop_until_target(curr_filter_id_))) {
    LOG_WARN("failed to pop until target", K(ret), K_(curr_filter_id));
  } else if (OB_FAIL(row_merger_->rebuild_with_advance(curr_filter_id_))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with advance", K(ret), K_(curr_filter_id));
  }
  bool found_matched_pivot = false;
  double max_score = 0.0;
  while (OB_SUCC(ret) && !found_matched_pivot) {
    int cmp_ret = 0;
    if (OB_FAIL(row_merger_->pop_until_found_pivot(0, 1, pivot_id, max_score))) {
      LOG_WARN("failed to pop until found pivot", K(ret), K(max_score));
    } else if (OB_FAIL(search_ctx_.compare_rowid(pivot_id, curr_filter_id_, cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret), K(pivot_id), K_(curr_filter_id));
    } else if (OB_UNLIKELY(cmp_ret < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pivot id smaller than curr_filter_id", K(ret), K(pivot_id), K_(curr_filter_id));
    } else if (0 == cmp_ret) {
      found_matched_pivot = true;
    } else if (OB_FAIL(advance_filter_to(pivot_id, curr_filter_id_))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance filter to pivot id", K(ret), K(pivot_id));
    } else if (OB_FAIL(search_ctx_.compare_rowid(pivot_id, curr_filter_id_, cmp_ret))) {
      LOG_WARN("failed to compare rowid", K(ret), K(pivot_id), K_(curr_filter_id));
    } else if (0 == cmp_ret) {
      found_matched_pivot = true;
    } else if (OB_FAIL(row_merger_->pop_until_target(curr_filter_id_))) {
      LOG_WARN("failed to pop until target", K(ret), K_(curr_filter_id));
    } else if (OB_FAIL(row_merger_->rebuild_with_advance(curr_filter_id_))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to rebuild with advance", K(ret), K_(curr_filter_id));
    }
  }
  return ret;
}

int ObDASBMMOp::evaluate_bmm_pivot(
    const ObDASRowID &pivot_id,
    const double &non_essential_bm_score,
    ObDASRowID &next_id,
    double &score,
    bool &found_row)
{
  int ret = OB_SUCCESS;
  ObDASRowID collected_id;
  double essential_score = 0.0;
  bool is_candidate = false;
  bool need_forward_filter_after_evaluate = false;
  if (OB_FAIL(evaluate_essential_pivot(
      pivot_id, non_essential_bm_score, collected_id, essential_score, is_candidate))) {
    LOG_WARN("failed to evaluate essential pivot", K(ret));
  } else if (is_candidate) {
    bool filter_match = true;
    if (has_filter()) {
      int cmp_ret = 0;
      if (OB_FAIL(search_ctx_.compare_rowid(collected_id, curr_filter_id_, cmp_ret))) {
        LOG_WARN("failed to compare rowid", K(ret));
      } else if (OB_UNLIKELY(cmp_ret < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected collected id smaller than curr_filter_id", K(ret),
            K(pivot_id), K(collected_id), K_(curr_filter_id));
      } else if (0 == cmp_ret) {
        filter_match = true;
      } else {
        // collected filter id not match, advance filter op to collected id
        filter_match = false;
        if (OB_FAIL(advance_filter_to(collected_id, curr_filter_id_))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to advance filter to collected id", K(ret), K(collected_id));
        } else if (OB_FAIL(search_ctx_.compare_rowid(collected_id, curr_filter_id_, cmp_ret))) {
          LOG_WARN("failed to compare rowid", K(ret), K(collected_id), K_(curr_filter_id));
        } else if (0 == cmp_ret) {
          filter_match = true;
        }
      }
    }

    if (OB_FAIL(ret) || !filter_match) {
    } else if (OB_FAIL(evaluate_pivot(collected_id, essential_score, score))) {
      LOG_WARN("failed to evaluate pivot", K(ret));
    } else if (FALSE_IT(need_forward_filter_after_evaluate = has_filter())) {
      LOG_WARN("failed to get next rowid from filter", K(ret));
    } else if (score > min_competitive_score_) {
      found_row = true;
      next_id = collected_id;
    }
  } else {
    need_forward_filter_after_evaluate = has_filter();
  }

  if (OB_FAIL(ret) || !need_forward_filter_after_evaluate) {
  } else if (OB_FAIL(next_filter_rowid(curr_filter_id_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rowid from filter", K(ret));
    } else if (found_row) {
      // return last matched pivot
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
