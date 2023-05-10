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

namespace oceanbase
{
using namespace common;
namespace sql
{

ObMergeUnionSpec::ObMergeUnionSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObMergeSetSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObMergeUnionSpec, ObMergeSetSpec));

ObMergeUnionOp::ObMergeUnionOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObMergeSetOp(exec_ctx, spec, input),
  cur_child_op_(nullptr),
  candidate_child_op_(nullptr),
  candidate_output_row_(nullptr),
  next_child_op_idx_(1),
  first_got_row_(true),
  get_next_row_func_(nullptr),
  get_next_batch_func_(nullptr),
  left_info_(),
  right_info_(),
  curr_info_(&left_info_),
  candidate_info_(&right_info_),
  last_valid_idx_(-1),
  need_push_candidate_(false)
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
        get_next_batch_func_ = &ObMergeUnionOp::distinct_get_next_batch;
      }
    } else {
      get_next_row_func_ = &ObMergeUnionOp::all_get_next_row;
      get_next_batch_func_ = &ObMergeUnionOp::all_get_next_batch;
    }
  }
  return ret;
}

int ObMergeUnionOp::inner_close()
{
  return ObMergeSetOp::inner_close();
}

int ObMergeUnionOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  cur_child_op_ = left_;
  next_child_op_idx_ = 1;
  first_got_row_ = true;
  candidate_output_row_ = nullptr;
  candidate_child_op_ = nullptr;
  last_valid_idx_ = -1;
  left_info_.reset();
  right_info_.reset();
  curr_info_ = &left_info_;
  candidate_info_ = &right_info_;
  need_push_candidate_ = false;
  if (MY_SPEC.is_distinct_) {
    get_next_row_func_ = &ObMergeUnionOp::distinct_get_next_row;
    get_next_batch_func_ = &ObMergeUnionOp::distinct_get_next_batch;
  } else {
    get_next_row_func_ = &ObMergeUnionOp::all_get_next_row;
    get_next_batch_func_ = &ObMergeUnionOp::all_get_next_batch;
  }
  if (OB_FAIL(ObMergeSetOp::inner_rescan())) {
  }
  return ret;
}

void ObMergeUnionOp::destroy()
{
  ObMergeSetOp::destroy();
}

int ObMergeUnionOp::get_first_row(const ObIArray<ObExpr*> *&output_row)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  const ObIArray<ObExpr*> *right_row = nullptr;
  clear_evaluated_flag();
  if (OB_FAIL(left_->get_next_row())) {
    if (OB_ITER_END == ret) {
      //switch to the right operator
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
  } else if (OB_FAIL(do_strict_distinct(
      *right_, left_->get_spec().output_, right_row, cmp))) {
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

int ObMergeUnionOp::get_first_row_vectorize(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  const ObBatchRows *left_brs = nullptr;
  bool found_valid_row = false;
  clear_evaluated_flag();
  if (OB_FAIL(left_->get_next_batch(batch_size, left_info_.op_brs_))) {
    LOG_WARN("failed to get next batch", K(ret));
  } else if (left_info_.op_brs_->end_ && 0 == left_info_.op_brs_->size_) {
    //left is empty, switch to right
    candidate_output_row_ = NULL;
    candidate_child_op_ = NULL;
    candidate_info_ = nullptr;
    cur_child_op_ = right_;
    clear_evaluated_flag();
    if (OB_FAIL(cur_child_op_->get_next_batch(batch_size, curr_info_->op_brs_))) {
      LOG_WARN("failed to get next batch", K(ret));
    } else if (curr_info_->op_brs_->end_ && 0 == curr_info_->op_brs_->size_) {
      ret = OB_ITER_END;
    } else {
      //got a batch now, locate curr_op_idx_ to first row without skipped
      //ObOperator::get_next_batch promise we can get 1 row in a valid batch
      curr_info_->op_idx_ = 0;
      while(curr_info_->op_idx_ < curr_info_->op_brs_->size_
            && curr_info_->op_brs_->skip_->at(curr_info_->op_idx_)) {
        ++curr_info_->op_idx_;
      }
    }
  } else {
    while(left_info_.op_idx_ < left_info_.op_brs_->size_
          && left_info_.op_brs_->skip_->at(left_info_.op_idx_)) {
      ++left_info_.op_idx_;
    }
    if (OB_FAIL(do_strict_distinct_vectorize(*right_, nullptr, left_->get_spec().output_,
                                             batch_size, left_info_.op_idx_,
                                             right_info_, cmp, found_valid_row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        candidate_info_ = nullptr;
        curr_info_ = &left_info_;
        cur_child_op_ = left_;
      } else {
        LOG_WARN("failed to do strict disticnt", K(ret));
      }
    } else {
      if (cmp <= 0) {
        cur_child_op_ = left_;
        candidate_child_op_ = right_;
        curr_info_ = &left_info_;
        candidate_info_ = &right_info_;
      } else {
        cur_child_op_ = right_;
        candidate_child_op_ = left_;
        curr_info_ = &right_info_;
        candidate_info_ = &left_info_;
      }
    }
  }
  return ret;
}

/**
 * 注释直接copy过来了
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
  const ObIArray<ObExpr*> *input_row = nullptr;
  clear_evaluated_flag();
  if (!first_got_row_) {
    int cur_child_err = OB_SUCCESS;
    int candidate_child_err = OB_SUCCESS;
    if (OB_ISNULL(cur_child_op_) || OB_ISNULL(last_row_.store_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cur_child_op is NULL or last row is null", K(ret));
    } else if (OB_UNLIKELY((OB_SUCCESS !=
                      (cur_child_err = do_strict_distinct(*cur_child_op_,
                                                          last_row_.store_row_,
                                                          input_row))))) {
      if (OB_ITER_END == cur_child_err) {
        if (OB_LIKELY(NULL != candidate_child_op_)) {
          //current operator in the end of iterator, so switch to the candidate operator
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
      LOG_WARN("input row is NULL or candidate_output_row_ is NULL", K(input_row),
                K(candidate_output_row_), K(ret));
    } else if (OB_FAIL(cmp_(
        *input_row, *candidate_output_row_, eval_ctx_, cmp))) {
      LOG_WARN("compatible cmp_( failed", K(ret));
    } else if (0 == cmp) {
      //left row equal to right row
      if (OB_FAIL(convert_row(*input_row, MY_SPEC.set_exprs_))) {
        LOG_WARN("failed to convert row", K(ret));
      } else {
        candidate_child_err = do_strict_distinct(*candidate_child_op_,
                                                  input_row,
                                                  candidate_output_row_);

        if (OB_SUCCESS != candidate_child_err) {
          if (OB_ITER_END == candidate_child_err) {
            //candidate operator in the end of row iteration, candidate operator not exist
            candidate_child_op_ = NULL;
            candidate_output_row_ = NULL;
          } else {
            ret = candidate_child_err;
            LOG_WARN("candidate child operator get next row failed", K(ret));
          }
        }
      }
    } else if (cmp < 0) {
      //output current row
      if (OB_FAIL(convert_row(*input_row, MY_SPEC.set_exprs_))) {
        LOG_WARN("failed to convert row", K(ret));
      }
    } else if (cmp > 0) {
      //output candidate row and switch candidate operator to current operator for next iteration
      ObOperator *tmp_op = NULL;
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
    const ObIArray<ObExpr*> *child_row = nullptr;
    first_got_row_ = false;
    if (OB_FAIL(get_first_row(child_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get first row failed", K(ret));
      }
    }
    //second, storage current row
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

int ObMergeUnionOp::distinct_get_next_batch(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  int64_t got_cnt = 0;
  clear_evaluated_flag();
  brs_.skip_->reset(batch_size);
  //we use do_strict_distinct to find valid row in curr or candidate
  //but if idx point to batch end, it is a invalid row, indicate we need to get a new batch
  bool found_valid_row = false;
  while (OB_SUCC(ret) && got_cnt < batch_size) {
    if (!first_got_row_) {
      int cur_child_err = OB_SUCCESS;
      int candidate_child_err = OB_SUCCESS;
      if (OB_NOT_NULL(candidate_child_op_) && need_push_candidate_) {
        need_push_candidate_ = false;
        candidate_child_err = do_strict_distinct_vectorize(*candidate_child_op_,
                                                            last_row_.store_row_,
                                                            MY_SPEC.set_exprs_,
                                                            batch_size,
                                                            last_valid_idx_,
                                                            *candidate_info_,
                                                            cmp,
                                                            found_valid_row);
        if (OB_SUCCESS != candidate_child_err) {
          if (OB_ITER_END == candidate_child_err) {
            //candidate operator in the end of row iteration, candidate operator not exist
            candidate_child_op_ = NULL;
            candidate_info_ = nullptr;
          } else {
            ret = candidate_child_err;
            LOG_WARN("candidate child operator get next row failed", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(cur_child_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur_child_op is NULL is null", K(ret));
      } else if (OB_UNLIKELY(OB_SUCCESS !=
                              (cur_child_err = do_strict_distinct_vectorize(*cur_child_op_,
                                                                            last_row_.store_row_,
                                                                            MY_SPEC.set_exprs_,
                                                                            batch_size,
                                                                            last_valid_idx_,
                                                                            *curr_info_,
                                                                            cmp,
                                                                            found_valid_row)))) {
        if (OB_ITER_END == cur_child_err) {
          if (OB_LIKELY(nullptr != candidate_child_op_)) {
            switch_curr_and_candidate();
            candidate_child_op_ = nullptr;
            candidate_info_ = nullptr;
            if (OB_FAIL(convert_row(cur_child_op_->get_spec().output_,
                                           MY_SPEC.set_exprs_, curr_info_->op_idx_, got_cnt))) {
              LOG_WARN("failed to convert row", K(ret));
            } else {
              curr_info_->op_added_ = true;
            }
          } else {
            ret = OB_ITER_END;
          }
        } else {
          ret = cur_child_err;
          LOG_WARN("failed to do strict distinct", K(ret));
        }
      } else if (!found_valid_row) {
        //cur op iter at end of batch && have rows converted to output,
        //need return this batch firstly
        break;
      } else if (nullptr == candidate_child_op_) {
        if (OB_FAIL(convert_row(cur_child_op_->get_spec().output_,
                                MY_SPEC.set_exprs_, curr_info_->op_idx_, got_cnt))) {
          LOG_WARN("failed to convert row", K(ret));
        } else {
          curr_info_->op_added_ = true;
        }
      } else if (OB_FAIL(cmp_(cur_child_op_->get_spec().output_,
                              candidate_child_op_->get_spec().output_,
                              curr_info_->op_idx_,
                              candidate_info_->op_idx_,
                              eval_ctx_,
                              cmp))) {
        LOG_WARN("failed to cmp curr row", K(ret));
      } else if (0 == cmp) {
        //left equal right, output left row
        if (OB_FAIL(convert_row(cur_child_op_->get_spec().output_,
                                MY_SPEC.set_exprs_, curr_info_->op_idx_, got_cnt))) {
          LOG_WARN("failed to convert row", K(ret));
        } else {
          curr_info_->op_added_ = true;
          //in this branch, we must got a valid row, so reset store_row and record last_valid_idx_
          last_valid_idx_ = got_cnt;
          candidate_child_err = do_strict_distinct_vectorize(*candidate_child_op_,
                                                             last_row_.store_row_,
                                                             MY_SPEC.set_exprs_,
                                                             batch_size,
                                                             last_valid_idx_,
                                                             *candidate_info_,
                                                             cmp,
                                                             found_valid_row);
          if (OB_SUCCESS != candidate_child_err) {
            if (OB_ITER_END == candidate_child_err) {
              //candidate operator in the end of row iteration, candidate operator not exist
              candidate_child_op_ = NULL;
              candidate_info_ = nullptr;
            } else {
              ret = candidate_child_err;
              LOG_WARN("candidate child operator get next row failed", K(ret));
            }
          } else if (!found_valid_row) {
            //we got 1 row inside a batch, record its idx and got from expr
            last_valid_idx_ = got_cnt;
            got_cnt++;
            //candidate move to end, need to get_next_batch after return batch
            need_push_candidate_ = true;
            //candidate op iter at end of batch && have rows converted to output,
            //need return this batch firstly
            break;
          }
        }
      } else if (cmp < 0) {
        //output curr row
        if (OB_FAIL(convert_row(cur_child_op_->get_spec().output_, MY_SPEC.set_exprs_,
                                curr_info_->op_idx_, got_cnt))) {
          LOG_WARN("failed to convert row", K(ret));
        } else {
          curr_info_->op_added_ = true;
        }
      } else if (cmp > 0) {
        //output candidate row and switch candidate operator to current operator for next iteration
        if (OB_FAIL(convert_row(candidate_child_op_->get_spec().output_,
                                MY_SPEC.set_exprs_,
                                candidate_info_->op_idx_,
                                got_cnt))) {
          LOG_WARN("failed to convert row", K(ret));
        } else {
          candidate_info_->op_added_ = true;
          switch_curr_and_candidate();
        }
      }
      if (OB_SUCC(ret)) {
        //we got 1 row inside a batch, record its idx and got from expr
        last_valid_idx_ = got_cnt;
        got_cnt++;
      }
    } else {
      // the first row, not need to cmp_ the row
      // first, get next row
      first_got_row_ = false;
      if (OB_FAIL(get_first_row_vectorize(batch_size))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get first row", K(ret));
        } else {
          brs_.end_ = true;
          brs_.size_ = 0;
        }
      } else if (OB_FAIL(convert_row(cur_child_op_->get_spec().output_, MY_SPEC.set_exprs_,
                                     curr_info_->op_idx_, got_cnt))) {
        LOG_WARN("failed to convert row", K(ret));
      } else {
        //we got 1st row, record its idx and set last_row.store_row_ to nullptr
        curr_info_->op_added_ = true;
        last_valid_idx_ = got_cnt;
        got_cnt++;
      }
    }
  }
  if (OB_SUCC(ret) && last_valid_idx_ >= 0) {
    //at end of a batch, we must store last valid row and reset last_valid_idx_ to -1
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_idx(last_valid_idx_);
    if (OB_FAIL(last_row_.save_store_row(MY_SPEC.set_exprs_, eval_ctx_, 0))) {
      LOG_WARN("failed to save last row", K(ret));
    }
    last_valid_idx_ = -1;
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.end_ = true;
    brs_.size_ = got_cnt;
  } else {
    left_info_.op_added_ = false;
    right_info_.op_added_ = false;
    brs_.size_ = got_cnt;
    brs_.end_ = false;
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
    //get next row with the next child operator
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

int ObMergeUnionOp::all_get_next_batch(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  const ObBatchRows *child_brs = nullptr;
  if (OB_ISNULL(cur_child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator failed", K(MY_SPEC.id_));
  } else if (OB_FAIL(cur_child_op_->get_next_batch(batch_size, child_brs))) {
    LOG_WARN("failed to get next batch", K(ret));
  } else if (child_brs->end_ && 0 == child_brs->size_) {
    while(OB_SUCC(ret)
          && child_brs->end_ && 0 == child_brs->size_
          && next_child_op_idx_ < get_child_cnt()) {
      cur_child_op_ = get_child(next_child_op_idx_++);
      if (OB_FAIL(cur_child_op_->get_next_batch(batch_size, child_brs))) {
        LOG_WARN("failed to get next batch", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    brs_.size_ = child_brs->size_;
    if (get_child_cnt() == next_child_op_idx_) {
      brs_.end_ = child_brs->end_;
    }
    brs_.skip_->deep_copy(*child_brs->skip_, brs_.size_);
    if (OB_FAIL(convert_batch(cur_child_op_->get_spec().output_,
                                      MY_SPEC.set_exprs_,
                                      brs_,
                                      true))) {
      LOG_WARN("failed to convert batch", K(ret));
    }
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

int ObMergeUnionOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = std::min(max_row_cnt, MY_SPEC.max_batch_size_);
  if (OB_ISNULL(get_next_batch_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get next batch func is null", K(ret));
  } else if (OB_FAIL((this->*get_next_batch_func_)(batch_size))) {
    LOG_WARN("get next batch failed ", K(ret));
  }
  return ret;
}

void ObMergeUnionOp::switch_curr_and_candidate()
{
  std::swap(curr_info_, candidate_info_);
  std::swap(cur_child_op_, candidate_child_op_);
}

int ObMergeUnionOp::do_strict_distinct_vectorize(ObOperator &child_op,
                                                 const ObChunkDatumStore::StoredRow *compare_row,
                                                 const common::ObIArray<ObExpr*> &compare_expr,
                                                 const int64_t batch_size,
                                                 const int64_t compare_idx,
                                                 UnionOpInfo &op_info,
                                                 int &cmp,
                                                 bool &found_valid_row)
{
  int ret = OB_SUCCESS;
  found_valid_row = false;
  //if compare_row is nullptr, means we do row compare in a batch, get values from expr
  if (OB_NOT_NULL(op_info.op_brs_) && OB_SUCC(ret) && op_info.op_idx_ < op_info.op_brs_->size_) {
    for (int64_t i = op_info.op_idx_; OB_SUCC(ret)
                                      && !found_valid_row
                                      && i < op_info.op_brs_->size_; ++i, ++op_info.op_idx_) {
      if (op_info.op_brs_->skip_->at(i)) {
        continue;
      }
      if (OB_UNLIKELY(compare_idx < 0)
          && OB_FAIL(cmp_(*compare_row, child_op.get_spec().output_,
                           op_info.op_idx_, eval_ctx_, cmp))) {
        LOG_WARN("strict compare with last row failed", K(ret));
      } else if (OB_LIKELY(compare_idx >= 0)
                && OB_FAIL(cmp_(compare_expr, child_op.get_spec().output_,
                                compare_idx, op_info.op_idx_, eval_ctx_, cmp))) {
        LOG_WARN("strict compare with last expr failed", K(ret));
      } else if (0 != cmp) {
        found_valid_row = true;
        break;
      }
    }
  }
  //if curr_op has row convert to output, can not get next batch before return
  if (!op_info.op_added_) {
    //if cannot find a bigger row, iter until end or find it
    while (OB_SUCC(ret) && !found_valid_row) {
      if (OB_FAIL(child_op.get_next_batch(batch_size, op_info.op_brs_))) {
        LOG_WARN("failed to get next batch ", K(ret));
      } else if (op_info.op_brs_->end_ && 0 == op_info.op_brs_->size_) {
        ret = OB_ITER_END;
      } else {
        op_info.op_idx_ = 0;
        for (int64_t i = 0; OB_SUCC(ret) && !found_valid_row
                                         && i < op_info.op_brs_->size_; ++i, ++op_info.op_idx_) {
          if (op_info.op_brs_->skip_->at(i)) {
            continue;
          }
          if (OB_UNLIKELY(compare_idx < 0)
            && OB_FAIL(cmp_(*compare_row, child_op.get_spec().output_,
                            op_info.op_idx_, eval_ctx_, cmp))) {
            LOG_WARN("strict compare with last row failed", K(ret));
          } else if (OB_LIKELY(compare_idx >= 0)
                     && OB_FAIL(cmp_(compare_expr, child_op.get_spec().output_,
                                     compare_idx, op_info.op_idx_, eval_ctx_, cmp))) {
            LOG_WARN("strict compare with last row failed ", K(ret));
          } else if (0 != cmp) {
            found_valid_row = true;
            break;
          }
        }
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
