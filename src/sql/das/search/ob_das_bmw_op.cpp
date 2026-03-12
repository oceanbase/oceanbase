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

#include "sql/das/search/ob_das_bmw_op.h"
#include "sql/das/search/ob_disjunctive_daat_merger.h"

namespace oceanbase
{
namespace sql
{

int ObDASBMWOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children is null", KR(ret));
  } else if (OB_FAIL(children.assign(*children_))) {
    LOG_WARN("failed to assign children", KR(ret));
  }
  return ret;
}

ObDASBMWOp::ObDASBMWOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    iter_allocator_(nullptr),
    merge_iters_(),
    row_merger_(nullptr),
    status_(SearchStatus::MAX_STATUS),
    minimum_should_match_(0),
    is_inited_(false) {}

int ObDASBMWOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASBMWOpParam &param = static_cast<const ObDASBMWOpParam &>(op_param);
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
    minimum_should_match_ = param.get_minimum_should_match();
    min_competitive_score_ = 0.0;
    iter_allocator_ = param.get_iter_allocator();
    is_inited_ = true;
  }
  return ret;
}

int ObDASBMWOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->open())) {
      LOG_WARN("failed to open child", K(ret), K(i));
    }
  }
  return ret;
}

int ObDASBMWOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(row_merger_->reuse())) {
    LOG_WARN("failed to reuse row merger", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr child", K(ret), K(i));
    } else if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("failed to rescan child", K(ret), K(i));
    } else {
      merge_iters_[i]->reuse();
    }
  }
  if (OB_SUCC(ret)) {
    status_ = SearchStatus::FIND_NEXT_PIVOT;
    min_competitive_score_ = 0.0;
  }
  return ret;
}

int ObDASBMWOp::do_close()
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
  if (nullptr != row_merger_) {
    row_merger_->~ObIDisjunctiveDAATMerger();
    if (nullptr != iter_allocator_) {
      iter_allocator_->free(row_merger_);
    }
    row_merger_ = nullptr;
  }
  iter_allocator_ = nullptr;
  return ret;
}

int ObDASBMWOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(row_merger_->pop_all())) {
    LOG_WARN("failed to pop all", K(ret));
  } else if (OB_FAIL(row_merger_->rebuild_with_advance(target))) {
    LOG_WARN("failed to reset row merger", K(ret));
  } else if (OB_FAIL(do_next_rowid(curr_id, score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to next rowid", K(ret));
    }
  }
  return ret;
}

int ObDASBMWOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  // currently advance shallow interface of BMW op would not be called, since there should be
  // only on operator that drives score-based dynamic pruning.
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObDASBMWOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == min_competitive_score_) {
    if (OB_FAIL(inner_get_next_row_plain(next_id, score))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row plain", K(ret));
      }
    }
  } else if (OB_FAIL(inner_get_next_row_with_pruning(next_id, score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row with pruning", K(ret));
    }
  }
  return ret;
}

int ObDASBMWOp::init_merge_iters(const ObDASBMWOpParam &op_param, ObIAllocator &iter_allocator)
{
  int ret = OB_SUCCESS;
  merge_iters_.set_allocator(&iter_allocator);
  if (OB_FAIL(merge_iters_.init(op_param.get_children()->count()))) {
    LOG_WARN("failed to init merge iters", K(ret));
  } else if (OB_FAIL(merge_iters_.prepare_allocate(op_param.get_children()->count()))) {
    LOG_WARN("failed to prepare allocate merge iters", K(ret));
  } else {
    for (int64_t i = 0; i < op_param.get_children()->count(); ++i) {
      ObDAATMergeIter *merge_iter = OB_NEWx(ObDAATMergeIter, &iter_allocator);
      if (OB_ISNULL(merge_iter)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for merge iter", K(ret));
      } else if (OB_FAIL(merge_iter->init(i, *op_param.get_children()->at(i)))) {
        LOG_WARN("failed to init merge iter", K(ret));
      } else {
        merge_iters_[i] = merge_iter;
      }
    }
  }
  return ret;
}

int ObDASBMWOp::inner_get_next_row_plain(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  int64_t dim_cnt = 0;
  bool found_row = false;
  while (OB_SUCC(ret) && !found_row) {
    if (OB_FAIL(row_merger_->rebuild_with_next())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to rebuild with next", K(ret));
      }
    } else if (OB_FAIL(row_merger_->merge_one_row(next_id, score, dim_cnt))) {
      LOG_WARN("failed to merge one row", K(ret));
    } else if (dim_cnt >= minimum_should_match_) {
      found_row = true;
    }
  }
  return ret;
}

int ObDASBMWOp::inner_get_next_row_with_pruning(ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  if (SearchStatus::FINISHED == status_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(SearchStatus::FIND_NEXT_PIVOT != status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status before get next row", K(ret), K_(status));
  }
  bool found_row = false;
  ObDASRowID pivot_id;
  while (OB_SUCC(ret) && !found_row) {
    switch (status_) {
    case SearchStatus::FIND_NEXT_PIVOT: {
      if (OB_FAIL(row_merger_->rebuild_with_next())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to rebuild with next", K(ret));
        }
      } else if (OB_FAIL(find_next_pivot(pivot_id))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to find next pivot", K(ret));
        }
      } else {
        status_ = SearchStatus::EVALUATE_PIVOT_RANGE;
      }
      break;
    }
    case SearchStatus::EVALUATE_PIVOT_RANGE: {
      bool is_candidate_range = false;
      if (OB_FAIL(evaluate_pivot_range(pivot_id, is_candidate_range))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to evaluate pivot range", K(ret));
        }
      } else if (is_candidate_range) {
        status_ = SearchStatus::EVALUATE_PIVOT;
      } else {
        status_ = SearchStatus::FIND_NEXT_PIVOT_RANGE;
      }
      break;
    }
    case SearchStatus::EVALUATE_PIVOT: {
      int64_t dim_cnt = 0;
      if (OB_FAIL(evaluate_pivot(pivot_id, curr_id, score, dim_cnt))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to evaluate pivot", K(ret));
        }
      } else {
        found_row = score > min_competitive_score_ && dim_cnt >= minimum_should_match_;
        status_ = SearchStatus::FIND_NEXT_PIVOT;
      }
      break;
    }
    case SearchStatus::FIND_NEXT_PIVOT_RANGE: {
      if (OB_FAIL(find_next_pivot_range())) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to find next pivot range", K(ret));
        }
        } else {
          status_ = SearchStatus::FIND_NEXT_PIVOT;
        }
        break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", K(ret), K_(status));
      }
    }
    }
  }

  if (OB_ITER_END == ret) {
    status_ = SearchStatus::FINISHED;
  }

  return ret;
}

int ObDASBMWOp::find_next_pivot(ObDASRowID &pivot_id)
{
  int ret = OB_SUCCESS;
  double max_score = 0.0;
  if (OB_FAIL(row_merger_->pop_until_found_pivot(
      min_competitive_score_,
      minimum_should_match_,
      pivot_id,
      max_score))) {
    LOG_WARN("failed to pop until found pivot", K(ret));
  } else if (max_score < min_competitive_score_) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObDASBMWOp::evaluate_pivot_range(const ObDASRowID &pivot_id, bool &is_candidate)
{
  int ret = OB_SUCCESS;
  is_candidate = false;
  double block_max_score = 0.0;
  if (OB_FAIL(row_merger_->advance_for_block_max_score(pivot_id, block_max_score))) {
    LOG_WARN("failed to advance for block max score", K(ret));
  } else {
    is_candidate = block_max_score > min_competitive_score_;
  }
  return ret;
}

int ObDASBMWOp::evaluate_pivot(
    const ObDASRowID &pivot_id,
    ObDASRowID &curr_id,
    double &score,
    int64_t &dim_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_merger_->rebuild_with_advance(pivot_id))) {
    LOG_WARN("failed to rebuild with advance", K(ret));
  } else if (OB_FAIL(row_merger_->merge_one_row(curr_id, score, dim_cnt))) {
    LOG_WARN("failed to merge one row", K(ret));
  }
  return ret;
}

int ObDASBMWOp::find_next_pivot_range()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_merger_->to_next_range_with_shallow_iters(min_competitive_score_))) {
    LOG_WARN("failed to to next range with shallow iters", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
