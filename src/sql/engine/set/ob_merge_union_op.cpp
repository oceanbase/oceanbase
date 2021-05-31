/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_merge_union_op.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObMergeUnionSpec::ObMergeUnionSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObMergeSetSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObMergeUnionSpec, ObMergeSetSpec));

ObMergeUnionOp::ObMergeUnionOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObMergeSetOp(exec_ctx, spec, input),
      cur_child_op_(nullptr),
      candidate_child_op_(nullptr),
      candidate_output_row_(nullptr),
      next_child_op_idx_(1),
      first_got_row_(true),
      get_next_row_func_(nullptr)
{}

int ObMergeUnionOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMergeSetOp::inner_open())) {
    LOG_WARN("failed to init open", K(ret));
  } else {
    cur_child_op_ = left_;
    next_child_op_idx_ = 1;
    if (MY_SPEC.is_distinct_) {
      if (OB_UNLIKELY(2 != get_child_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected merge union distinct", K(ret), K(get_child_cnt()));
      } else {
        get_next_row_func_ = &ObMergeUnionOp::distinct_get_next_row;
      }
    } else {
      get_next_row_func_ = &ObMergeUnionOp::all_get_next_row;
    }
  }
  return ret;
}

int ObMergeUnionOp::inner_close()
{
  return ObMergeSetOp::inner_close();
}

int ObMergeUnionOp::rescan()
{
  int ret = OB_SUCCESS;
  cur_child_op_ = left_;
  next_child_op_idx_ = 1;
  first_got_row_ = true;
  candidate_output_row_ = nullptr;
  candidate_child_op_ = nullptr;
  if (MY_SPEC.is_distinct_) {
    get_next_row_func_ = &ObMergeUnionOp::distinct_get_next_row;
  } else {
    get_next_row_func_ = &ObMergeUnionOp::all_get_next_row;
  }
  if (OB_FAIL(ObMergeSetOp::rescan())) {}
  return ret;
}

void ObMergeUnionOp::destroy()
{
  ObMergeSetOp::destroy();
}

int ObMergeUnionOp::get_first_row(const ObIArray<ObExpr*>*& output_row)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  const ObIArray<ObExpr*>* right_row = nullptr;
  clear_evaluated_flag();
  if (OB_FAIL(left_->get_next_row())) {
    if (OB_ITER_END == ret) {
      // switch to the right operator
      candidate_output_row_ = NULL;
      candidate_child_op_ = NULL;
      cur_child_op_ = right_;
      clear_evaluated_flag();
      if (OB_FAIL(cur_child_op_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else {
        output_row = &cur_child_op_->get_spec().output_;
      }
    }
  } else if (OB_FAIL(do_strict_distinct(*right_, left_->get_spec().output_, right_row, cmp))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      candidate_child_op_ = NULL;
      candidate_output_row_ = NULL;
      cur_child_op_ = left_;
      output_row = &left_->get_spec().output_;
    } else {
      LOG_WARN("fail to get right operator's row", K(ret));
    }
  } else {
    candidate_child_op_ = cmp < 0 ? right_ : left_;
    candidate_output_row_ = cmp < 0 ? &right_->get_spec().output_ : &left_->get_spec().output_;
    cur_child_op_ = cmp < 0 ? left_ : right_;
    output_row = cmp < 0 ? &left_->get_spec().output_ : &right_->get_spec().output_;
    LOG_DEBUG("trace first row", K(ROWEXPR2STR(eval_ctx_, *output_row)), K(cmp), K(ret));
  }
  return ret;
}

/**
 * copy from ob_merge_union.cpp
 * When UNION DISTINCT, we consider that left and right query already in ordered.
 * cur_child_operator: get current row from this operator in the first place
 * candidate_output_row && candidate_child_operator: the candidate output row from the candidate
 * child operator
 *
 * in distinct_get_nexr_row, we get the next row from the cur_child_operator as input_row at first,
 * input_row is distinct with the last output row
 * if in the end of the cur_child_opertor iterator, we must output the candidate_output_row
 * as the current row and switch the cur_child_operator to candidate_child_operator to get next row,
 *
 * if in the end of the candidate_child_operator iterator, we only need to get next row from the
 * cur_child_operator, and don't need to cmp_( with candidate_child_operator's row
 *
 * if cur_child_operator and candidate_child_operator are present, we need to cmp_( input_row
 * with the  candidate_output_row, if input_row is less than candidate_output_row, return input_row
 * as the result, if input_row equal to candidate_output_row, return input_row as the result and
 * get the distinct candidate_output_row from the candidate_child_operator, otherwise, return
 * candidate_output_row as the result and switch candidate_child_operator with cur_child_operator
 * for the next iteration
 */
int ObMergeUnionOp::distinct_get_next_row()
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  const ObIArray<ObExpr*>* input_row = nullptr;
  clear_evaluated_flag();
  if (!first_got_row_) {
    int cur_child_err = OB_SUCCESS;
    int candidate_child_err = OB_SUCCESS;
    if (OB_ISNULL(cur_child_op_) || OB_ISNULL(last_row_.store_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_child_op is NULL or last row is null", K(ret));
    } else if (OB_UNLIKELY((OB_SUCCESS !=
                            (cur_child_err = do_strict_distinct(*cur_child_op_, last_row_.store_row_, input_row))))) {
      if (OB_ITER_END == cur_child_err) {
        if (OB_LIKELY(NULL != candidate_child_op_)) {
          // current operator in the end of iterator, so switch to the candidate operator
          cur_child_op_ = candidate_child_op_;
          candidate_child_op_ = NULL;
          if (OB_FAIL(convert_row(*candidate_output_row_, MY_SPEC.set_exprs_))) {
            LOG_WARN("failed to convert row", K(ret));
          }
          candidate_output_row_ = NULL;
        } else {
          ret = OB_ITER_END;
        }
      } else {
        ret = cur_child_err;
        LOG_WARN("failed to do_strict_distinct", K(ret));
      }
    } else if (NULL == candidate_child_op_) {
      if (OB_FAIL(convert_row(*input_row, MY_SPEC.set_exprs_))) {
        LOG_WARN("failed to convert row", K(ret));
      }
    } else if (OB_UNLIKELY(NULL == input_row || NULL == candidate_output_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input row is NULL or candidate_output_row_ is NULL", K(input_row), K(candidate_output_row_), K(ret));
    } else if (OB_FAIL(cmp_(*input_row, *candidate_output_row_, eval_ctx_, cmp))) {
      LOG_WARN("compatible cmp_( failed", K(ret));
    } else if (0 == cmp) {
      // left row equal to right row
      if (OB_FAIL(convert_row(*input_row, MY_SPEC.set_exprs_))) {
        LOG_WARN("failed to convert row", K(ret));
      }
      candidate_child_err = do_strict_distinct(*candidate_child_op_, input_row, candidate_output_row_);

      if (OB_SUCCESS != candidate_child_err) {
        if (OB_ITER_END == candidate_child_err) {
          // candidate operator in the end of row iteration, candidate operator not exist
          candidate_child_op_ = NULL;
          candidate_output_row_ = NULL;
        } else {
          ret = candidate_child_err;
          LOG_WARN("candidate child operator get next row failed", K(ret));
        }
      }
    } else if (cmp < 0) {
      // output current row
      if (OB_FAIL(convert_row(*input_row, MY_SPEC.set_exprs_))) {
        LOG_WARN("failed to convert row", K(ret));
      }
    } else if (cmp > 0) {
      // output candidate row and switch candidate operator to current operator for next iteration
      ObOperator* tmp_op = NULL;
      if (OB_FAIL(convert_row(*candidate_output_row_, MY_SPEC.set_exprs_))) {
        LOG_WARN("failed to convert row", K(ret));
      }
      candidate_output_row_ = input_row;
      tmp_op = candidate_child_op_;
      candidate_child_op_ = cur_child_op_;
      cur_child_op_ = tmp_op;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(last_row_.save_store_row(MY_SPEC.set_exprs_, eval_ctx_, 0))) {
        LOG_WARN("storage current row for next cmp_( failed", K(ret));
      }
    }
  } else {
    // the first row, not need to cmp_( the row
    // first, get next row
    const ObIArray<ObExpr*>* child_row = nullptr;
    first_got_row_ = false;
    if (OB_FAIL(get_first_row(child_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get first row failed", K(ret));
      }
    }
    // second, storage current row
    if (OB_SUCC(ret)) {
      if (OB_FAIL(convert_row(*child_row, MY_SPEC.set_exprs_))) {
        LOG_WARN("failed to convert row", K(ret));
      } else if (OB_FAIL(last_row_.save_store_row(*child_row, eval_ctx_, 0))) {
        LOG_WARN("storage current row for next cmp_( failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("trace output row", K(ROWEXPR2STR(eval_ctx_, MY_SPEC.set_exprs_)), K(cmp));
  }
  return ret;
}

int ObMergeUnionOp::all_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_ISNULL(cur_child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator failed", K(MY_SPEC.id_));
  } else if (OB_FAIL(cur_child_op_->get_next_row())) {
    // get next row with the next child operator
    while (OB_ITER_END == ret && next_child_op_idx_ < get_child_cnt()) {
      cur_child_op_ = get_child(next_child_op_idx_);
      ++next_child_op_idx_;
      if (OB_ISNULL(cur_child_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get physical operator failed", K(MY_SPEC.id_));
      } else if (OB_FAIL(cur_child_op_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(convert_row(cur_child_op_->get_spec().output_, MY_SPEC.set_exprs_))) {
    LOG_WARN("failed to convert row", K(ret));
  }
  return ret;
}

int ObMergeUnionOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_next_row_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get_next_row_func is NULL", K(ret));
  } else if (OB_FAIL((this->*get_next_row_func_)())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
