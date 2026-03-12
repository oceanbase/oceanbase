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
#include "ob_das_disjunction_op.h"
#include "sql/das/ob_das_def_reg.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_disjunctive_daat_merger.h"

namespace oceanbase
{
namespace sql
{

int ObDASDisjunctionOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(optional_ops_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("optional ops is null", KR(ret));
  } else if (OB_FAIL(children.assign(*optional_ops_))) {
    LOG_WARN("failed to assign optional ops", KR(ret));
  }
  return ret;
}

ObDASDisjunctionOp::ObDASDisjunctionOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    row_merger_(nullptr),
    merge_iters_(),
    lead_cost_(),
    minimum_should_match_(0),
    max_score_tuple_(),
    need_score_(false),
    iter_end_(false),
    is_inited_(false)
{}

int ObDASDisjunctionOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASDisjunctionOpParam &disjunction_op_param = static_cast<const ObDASDisjunctionOpParam &>(op_param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_FAIL(init_merge_iters(disjunction_op_param, ctx_allocator()))) {
    LOG_WARN("failde to init merge iters", K(ret));
  } else if (OB_FAIL(ObIDisjunctiveDAATMerger::alloc_merger(
      merge_iters_,
      search_ctx_.get_rowid_exprs(),
      search_ctx_.get_rowid_meta(),
      search_ctx_.get_rowid_type(),
      disjunction_op_param.get_use_max_score(),
      ctx_allocator(),
      row_merger_))) {
    LOG_WARN("failed to alloc row merger", K(ret));
  } else {
    lead_cost_ = disjunction_op_param.get_lead_cost();
    minimum_should_match_ = disjunction_op_param.get_minimum_should_match();
    need_score_ = true; // currently always need score for disjunction op
    iter_end_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObDASDisjunctionOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr child", K(ret), K(i));
      } else if (OB_FAIL(children_[i]->open())) {
        LOG_WARN("failed to open child", K(ret));
      }
    }
  }
  return ret;
}

int ObDASDisjunctionOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(row_merger_->reuse())) {
    LOG_WARN("failed to reuse row merger", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i]) || merge_iters_.at(i) == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), K(i), KP(children_[i]), KP(merge_iters_.at(i)));
    } else if (OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("failed to rescan optional op", K(ret));
    } else {
      merge_iters_.at(i)->reuse();
    }
  }

  if (OB_SUCC(ret)) {
    max_score_tuple_.reset();
    iter_end_ = false;
  }
  return ret;
}

int ObDASDisjunctionOp::do_close()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  for (int64_t i = 0; i < merge_iters_.count(); ++i) {
    if (nullptr != merge_iters_[i]) {
      merge_iters_[i]->~ObDAATMergeIter();
      ctx_allocator().free(merge_iters_[i]);
      merge_iters_[i] = nullptr;
    }
  }
  merge_iters_.reset();
  if (nullptr != row_merger_) {
    row_merger_->~ObIDisjunctiveDAATMerger();
    ctx_allocator().free(row_merger_);
    row_merger_ = nullptr;
  }
  return ret;
}

int ObDASDisjunctionOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
    LOG_WARN("iter end", K(ret));
  } else if (OB_FAIL(row_merger_->pop_all())) {
    LOG_WARN("failed to reuse row merger", K(ret));
  } else if (OB_FAIL(row_merger_->rebuild_with_advance(target))) {
    LOG_WARN("failed to rebuild with advance", K(ret));
  } else if (OB_FAIL(inner_get_next_row(curr_id, score))) {
    LOG_WARN("failed to get next row", K(ret));
  }
  return ret;
}

int ObDASDisjunctionOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
    LOG_WARN("iter end", K(ret));
  } else if (OB_FAIL(row_merger_->pop_all())) {
    LOG_WARN("failed to reuse row merger", K(ret));
  } else {
    ObDASRowID min_id;
    ObDASRowID max_id;
    min_id.set_max();
    max_id.set_max();
    double max_score = 0.0;
    bool all_iter_end = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_iters_.count(); ++i) {
      const MaxScoreTuple *max_score_tuple = nullptr;
      ObDAATMergeIter *merge_iter = merge_iters_[i];
      int min_cmp_ret = 0;
      int max_cmp_ret = 0;
      if (OB_ISNULL(merge_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr merge iter", K(ret), K(i));
      } else if (merge_iter->iter_end()) {
        // skip
      } else if (FALSE_IT(all_iter_end = false)) {
      } else if (OB_FAIL(merge_iter->advance_shallow(target, inclusive))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to advance shallow", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(merge_iter->get_curr_block_max_info(max_score_tuple))) {
        LOG_WARN("failed to get curr block max info", K(ret));
      } else if (OB_ISNULL(max_score_tuple)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr max score tuple", K(ret));
      } else if (OB_FAIL(search_ctx_.compare_rowid(max_score_tuple->get_min_id(), min_id, min_cmp_ret))) {
        LOG_WARN("failed to compare min id", K(ret));
      } else if (OB_FAIL(search_ctx_.compare_rowid(max_score_tuple->get_max_id(), max_id, max_cmp_ret))) {
        LOG_WARN("failed to compare max id", K(ret));
      } else {
        max_score += max_score_tuple->get_max_score();
        if (min_cmp_ret < 0) {
          min_id = max_score_tuple->get_min_id();
        }
        if (max_cmp_ret < 0) {
          max_id = max_score_tuple->get_max_id();
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (all_iter_end) {
      ret = OB_ITER_END;
    } else {
      max_score_tuple_.set(min_id, max_id, max_score);
      max_score_tuple = &max_score_tuple_;
    }
  }
  return ret;
}

int ObDASDisjunctionOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(inner_get_next_row(next_id, score))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObDASDisjunctionOp::do_set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!need_score_) {
    // do nothing
  } else if (children_cnt_ == 1) {
    if (OB_FAIL(children_[0]->set_min_competitive_score(threshold))) {
      LOG_WARN("failed to set min competitive score", K(ret));
    }
  } else if (OB_FAIL(ObIDASSearchOp::do_set_min_competitive_score(threshold))) {
    LOG_WARN("failed to set min competitive score", K(ret));
  }
  return ret;
}

int ObDASDisjunctionOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!need_score_) {
    threshold = 0.0;
  } else if (OB_FAIL(ObIDASSearchOp::do_calc_max_score(threshold))) {
    LOG_WARN("failed to calc max score", K(ret));
  }
  return ret;
}

int ObDASDisjunctionOp::init_merge_iters(const ObDASDisjunctionOpParam &op_param, ObIAllocator &iter_allocator)
{
  int ret = OB_SUCCESS;
  merge_iters_.set_allocator(&iter_allocator);
  if (OB_FAIL(merge_iters_.init(op_param.get_optional_ops()->count()))) {
    LOG_WARN("failed to init merge iters", K(ret));
  } else if (OB_FAIL(merge_iters_.prepare_allocate(op_param.get_optional_ops()->count()))) {
    LOG_WARN("failed to prepare allocate merge iters", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < op_param.get_optional_ops()->count(); ++i) {
      ObDAATMergeIter *merge_iter = OB_NEWx(ObDAATMergeIter, &iter_allocator);
      if (OB_ISNULL(merge_iter)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for merge iter", K(ret));
      } else if (OB_FAIL(merge_iter->init(i, *op_param.get_optional_ops()->at(i)))) {
        LOG_WARN("failed to init merge iter", K(ret));
      } else {
        merge_iters_[i] = merge_iter;
      }
    }
  }
  return ret;
}

int ObDASDisjunctionOp::inner_get_next_row(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  bool found_row = false;
  if (iter_end_) {
    ret = OB_ITER_END;
  }
  while (OB_SUCC(ret) && !found_row) {
    double curr_score = 0;
    int64_t dim_cnt = 0;
    if (OB_FAIL(row_merger_->rebuild_with_next())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to rebuild with next", K(ret));
      } else {
        iter_end_ = true;
      }
    } else if (OB_FAIL(row_merger_->merge_one_row(next_id, curr_score, dim_cnt))) {
      LOG_WARN("failed to merge one row", K(ret));
    } else {
      const bool score_valid = need_score_
          ? (curr_score > min_competitive_score_ || 0 == min_competitive_score_) : true;
      const bool dim_cnt_valid = dim_cnt >= minimum_should_match_;
      if (score_valid && dim_cnt_valid) {
        found_row = true;
        score = curr_score;
      }
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
