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

#include "ob_merge_union_vec_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObMergeUnionVecSpec::ObMergeUnionVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObMergeSetVecSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObMergeUnionVecSpec, ObMergeSetVecSpec));

ObMergeUnionVecOp::ObMergeUnionVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObMergeSetVecOp(exec_ctx, spec, input),
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

int ObMergeUnionVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMergeSetVecOp::inner_open())) {
    LOG_WARN("failed to init open", K(ret));
  } else {
    cur_child_op_ = left_;
    next_child_op_idx_ = 1;
    if (MY_SPEC.is_distinct_) {
      if (OB_UNLIKELY(2 != get_child_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected merge union distinct", K(ret), K(get_child_cnt()));
      } else {
        void *mem = ctx_.get_allocator().alloc(ObBitVector::memory_size(MY_SPEC.max_batch_size_));
        if (OB_ISNULL(mem)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          left_info_.result_op_brs_.skip_ = to_bit_vector(mem);
          left_info_.result_op_brs_.skip_->init(0);
          mem = ctx_.get_allocator().alloc(ObBitVector::memory_size(MY_SPEC.max_batch_size_));
          if (OB_ISNULL(mem)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            right_info_.result_op_brs_.skip_ = to_bit_vector(mem);
            right_info_.result_op_brs_.skip_->init(MY_SPEC.max_batch_size_);
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(left_src_idx_selectors_ = static_cast<uint16_t *>
          (ctx_.get_allocator().alloc(sizeof(uint16_t) * MY_SPEC.max_batch_size_)))){
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc left_src_idx_selectors_", K(ret), K(MY_SPEC.max_batch_size_));
        } else if (OB_ISNULL(left_dst_idx_selectors_ = static_cast<uint16_t *>
          (ctx_.get_allocator().alloc(sizeof(uint16_t) * MY_SPEC.max_batch_size_)))){
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc left_dst_idx_selectors_", K(ret), K(MY_SPEC.max_batch_size_));
        } else if (OB_ISNULL(right_src_idx_selectors_ = static_cast<uint16_t *>
          (ctx_.get_allocator().alloc(sizeof(uint16_t) * MY_SPEC.max_batch_size_)))){
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc right_src_idx_selectors_", K(ret), K(MY_SPEC.max_batch_size_));
        } else if (OB_ISNULL(right_dst_idx_selectors_ = static_cast<uint16_t *>
          (ctx_.get_allocator().alloc(sizeof(uint16_t) * MY_SPEC.max_batch_size_)))){
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc right_dst_idx_selectors_", K(ret), K(MY_SPEC.max_batch_size_));
        }
        left_selectors_item_cnt_ = 0;
        right_selectors_item_cnt_ = 0;
        get_next_batch_func_ = &ObMergeUnionVecOp::distinct_get_next_batch;
      }
    } else {
      get_next_batch_func_ = &ObMergeUnionVecOp::all_get_next_batch;
    }
    if (OB_ISNULL(get_next_batch_func_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get next batch func is null", K(ret));
    }
  }
  return ret;
}

int ObMergeUnionVecOp::inner_close()
{
  if (left_info_.result_op_brs_.skip_ != nullptr) {
    alloc_.free(left_info_.result_op_brs_.skip_);
    left_info_.result_op_brs_.skip_ = nullptr;
  }
  if (right_info_.result_op_brs_.skip_ != nullptr) {
    alloc_.free(right_info_.result_op_brs_.skip_);
    right_info_.result_op_brs_.skip_ = nullptr;
  }
  return ObMergeSetVecOp::inner_close();
}

int ObMergeUnionVecOp::inner_rescan()
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
  last_row_belong_to_left_op_ = true;
  if (MY_SPEC.is_distinct_) {
    get_next_batch_func_ = &ObMergeUnionVecOp::distinct_get_next_batch;
  } else {
    get_next_batch_func_ = &ObMergeUnionVecOp::all_get_next_batch;
  }
  if (OB_ISNULL(get_next_batch_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get next batch func is null", K(ret));
  } else if (OB_FAIL(ObMergeSetVecOp::inner_rescan())) {
  }
  return ret;
}

void ObMergeUnionVecOp::destroy()
{
  if (left_info_.result_op_brs_.skip_ != nullptr) {
    alloc_.free(left_info_.result_op_brs_.skip_);
    left_info_.result_op_brs_.skip_ = nullptr;
  }
  if (right_info_.result_op_brs_.skip_ != nullptr) {
    alloc_.free(right_info_.result_op_brs_.skip_);
    right_info_.result_op_brs_.skip_ = nullptr;
  }
  ObMergeSetVecOp::destroy();
}

int ObMergeUnionVecOp::get_first_row_vectorize(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  const ObBatchRows *left_brs = nullptr;
  bool found_valid_row = false;
  bool is_first = true;
  clear_evaluated_flag();
  if (OB_FAIL(left_->get_next_batch(MY_SPEC.max_batch_size_, left_info_.child_op_brs_))) {
    LOG_WARN("failed to get next batch", K(ret));
  } else if (left_info_.child_op_brs_->end_ && 0 == left_info_.child_op_brs_->size_) {
    //left is empty, switch to right
    candidate_output_row_ = NULL;
    candidate_child_op_ = NULL;
    candidate_info_ = nullptr;
    cur_child_op_ = right_;
    curr_info_ = &right_info_;
    clear_evaluated_flag();
    if (OB_FAIL(cur_child_op_->get_next_batch(MY_SPEC.max_batch_size_, curr_info_->child_op_brs_))) {
      LOG_WARN("failed to get next batch", K(ret));
    } else if (curr_info_->child_op_brs_->end_ && 0 == curr_info_->child_op_brs_->size_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(distinct_for_batch(*cur_child_op_, *curr_info_->child_op_brs_, is_first,
                MY_SPEC.set_exprs_, -1, curr_info_->result_op_brs_))) {
      LOG_WARN("distinct for right first batch failed", K(ret));
     } else {
      //got a batch now, locate curr_op_idx_ to first row without skipped
      //ObOperator::get_next_batch promise we can get 1 row in a valid batch
      curr_info_->op_idx_ = 0;
      while(curr_info_->op_idx_ < curr_info_->child_op_brs_->size_
            && curr_info_->child_op_brs_->skip_->at(curr_info_->op_idx_)) {
        ++curr_info_->op_idx_;
      }
    }
  } else if (OB_FAIL(distinct_for_batch(*left_, *left_info_.child_op_brs_, is_first,
                MY_SPEC.set_exprs_, -1, left_info_.result_op_brs_))) {
    LOG_WARN("distinct for left first batch failed", K(ret));
  } else {
    // left have row
    while(left_info_.op_idx_ < left_info_.child_op_brs_->size_
          && left_info_.child_op_brs_->skip_->at(left_info_.op_idx_)) {
      ++left_info_.op_idx_;
    }
    if (OB_FAIL(do_strict_distinct_vectorize(*right_, nullptr, nullptr,
                                             batch_size, left_info_.op_idx_,
                                             right_info_, cmp, found_valid_row))) {
      if (OB_ITER_END == ret) {
        // right are all left first row, all be filtered
        ret = OB_SUCCESS;
        candidate_info_ = nullptr;
        curr_info_ = &left_info_;
        cur_child_op_ = left_;
      } else {
        LOG_WARN("failed to do strict disticnt", K(ret));
      }
    } else {
      // first row will be direct output to set_exprs_ after this func return,
      // need cmp_ left with right here to decide which is cur_child_op_
      while(right_info_.op_idx_ < right_info_.child_op_brs_->size_
          && right_info_.result_op_brs_.skip_->at(right_info_.op_idx_)) {
        ++right_info_.op_idx_;
      }
      if (OB_FAIL(cmp_(left_->get_spec().output_,
                       right_->get_spec().output_,
                       left_info_.op_idx_,
                       right_info_.op_idx_,
                       eval_ctx_,
                       cmp))) {
        LOG_WARN("cmp left and right first row failed", K(ret), K(left_info_.op_idx_),
          K(right_info_.op_idx_));
      } else if (cmp <= 0) {
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

void ObMergeUnionVecOp::add_idx_into_selector(int src_idx, int dst_idx, bool is_left) {
  last_row_belong_to_left_op_ = is_left;
  last_valid_idx_ = src_idx;
  if (is_left) {
    left_src_idx_selectors_[left_selectors_item_cnt_] = src_idx;
    left_dst_idx_selectors_[left_selectors_item_cnt_++] = dst_idx;
  } else {
    right_src_idx_selectors_[right_selectors_item_cnt_] = src_idx;
    right_dst_idx_selectors_[right_selectors_item_cnt_++] = dst_idx;
  }
}

int ObMergeUnionVecOp::distinct_get_next_batch(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  int64_t got_cnt = 0;
  left_selectors_item_cnt_ = 0;
  right_selectors_item_cnt_ = 0;
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
                                                            last_row_.compact_row_,
                                                            last_row_.ref_row_meta_,
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
                                                                            last_row_.compact_row_,
                                                                            last_row_.ref_row_meta_,
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
            add_idx_into_selector(curr_info_->op_idx_, got_cnt, curr_info_ == &left_info_);
            curr_info_->op_idx_++;
            curr_info_->op_added_ = true;
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
        add_idx_into_selector(curr_info_->op_idx_, got_cnt, curr_info_ == &left_info_);
        curr_info_->op_idx_++;
        curr_info_->op_added_ = true;
      } else if (OB_FAIL(cmp_(cur_child_op_->get_spec().output_,
                              candidate_child_op_->get_spec().output_,
                              curr_info_->op_idx_,
                              candidate_info_->op_idx_,
                              eval_ctx_,
                              cmp))) {
        LOG_WARN("failed to cmp curr row", K(ret));
      } else if (0 == cmp) {
        //left equal right, output left row
        add_idx_into_selector(curr_info_->op_idx_, got_cnt, curr_info_ == &left_info_);
        curr_info_->op_idx_++; // left++
        candidate_info_->op_idx_++; // right++
        curr_info_->op_added_ = true;
        candidate_child_err = do_strict_distinct_vectorize(*candidate_child_op_,
                                                            last_row_.compact_row_,
                                                            last_row_.ref_row_meta_,
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
          got_cnt++;
          //candidate move to end, need to get_next_batch after return batch
          need_push_candidate_ = true;
          //candidate op iter at end of batch && have rows converted to output,
          //need return this batch firstly
          break;
        }
      } else if (cmp < 0) {
        //output curr row
        add_idx_into_selector(curr_info_->op_idx_, got_cnt, curr_info_ == &left_info_);
        curr_info_->op_idx_++;
        curr_info_->op_added_ = true;
      } else if (cmp > 0) {
        add_idx_into_selector(candidate_info_->op_idx_, got_cnt, candidate_info_ == &left_info_);
        candidate_info_->op_idx_++;
        candidate_info_->op_added_ = true;
        switch_curr_and_candidate();
      }
      if (OB_SUCC(ret)) {
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
      } else {
        add_idx_into_selector(curr_info_->op_idx_, got_cnt, curr_info_ == &left_info_);
        curr_info_->op_idx_++;
        curr_info_->op_added_ = true;
        got_cnt++;
      }
    }
  }
  if (OB_SUCC(ret) && (left_selectors_item_cnt_ >= 1 ||
                       right_selectors_item_cnt_ >= 1)) {
    //at end of a batch, we must store last valid row and reset last_valid_idx_ to -1
    if (last_valid_idx_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), K(last_valid_idx_), K(left_selectors_item_cnt_),
        K(right_selectors_item_cnt_));
    } else if (OB_FAIL(convert_batch_rows())) {
      LOG_WARN("convert batch rows failed", K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
      guard.set_batch_idx(last_valid_idx_);
      if (OB_FAIL(last_row_.save_store_row(MY_SPEC.set_exprs_, brs_, eval_ctx_))) {
        LOG_WARN("failed to save last row", K(ret));
      }
      last_valid_idx_ = -1;
    }
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

int ObMergeUnionVecOp::all_get_next_batch(const int64_t batch_size)
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

int ObMergeUnionVecOp::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObMergeUnionVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = std::min(max_row_cnt, MY_SPEC.max_batch_size_);
  // init res vec format
  for (int i = 0; OB_SUCC(ret) && i < MY_SPEC.set_exprs_.count(); i++) {
    ObExpr* expr = MY_SPEC.set_exprs_.at(i);
    VectorFormat default_format = !expr->batch_result_ ? VEC_UNIFORM_CONST
      : (expr->datum_meta_.type_ == ObNullType ? VEC_UNIFORM
    : (expr->is_fixed_length_data_ ? VEC_FIXED : VEC_DISCRETE));
    if (OB_FAIL(expr->init_vector(eval_ctx_, default_format, 
        MY_SPEC.max_batch_size_))) {
      LOG_WARN("init vector failed", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL((this->*get_next_batch_func_)(batch_size))) {
    LOG_WARN("get next batch failed ", K(ret));
  }
  return ret;
}

void ObMergeUnionVecOp::switch_curr_and_candidate()
{
  std::swap(curr_info_, candidate_info_);
  std::swap(cur_child_op_, candidate_child_op_);
}

int ObMergeUnionVecOp::do_strict_distinct_vectorize(ObOperator &child_op,
                                                    const ObCompactRow *compare_row,
                                                    const RowMeta *meta,
                                                    const int64_t batch_size,
                                                    const int64_t compare_idx,
                                                    UnionOpInfo &op_info,
                                                    int &cmp,
                                                    bool &found_valid_row)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObExpr*> &compare_expr = last_row_belong_to_left_op_ ?
                              left_->get_spec().output_ : right_->get_spec().output_;
  found_valid_row = false;

  if (OB_SUCC(ret) && OB_NOT_NULL(op_info.child_op_brs_)
                   && op_info.op_idx_ < op_info.child_op_brs_->size_) {
    for (int64_t i = op_info.op_idx_; OB_SUCC(ret)
                                      && !found_valid_row
                                      && i < op_info.child_op_brs_->size_; ++i, ++op_info.op_idx_) {
      if (op_info.result_op_brs_.skip_->at(i)) {
        continue;
      }
      found_valid_row = true;
      break;
    }
  }
  //if curr_op has row convert to output, can not get next batch before return
  // (compare_row == nullptr && compare_idx < 0)说明没有上一个store的row，也还没有输出行
  bool is_first = (compare_row == nullptr && compare_idx < 0);
  if (OB_SUCC(ret) && !op_info.op_added_) {
    //if cannot find a bigger row, iter until end or find it
    while (OB_SUCC(ret) && !found_valid_row) {
      if (OB_FAIL(child_op.get_next_batch(MY_SPEC.max_batch_size_, op_info.child_op_brs_))) {
        LOG_WARN("failed to get next batch ", K(ret));
      } else if (op_info.child_op_brs_->end_ && 0 == op_info.child_op_brs_->size_) {
        ret = OB_ITER_END;
      } else {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(distinct_for_batch(child_op, *op_info.child_op_brs_, is_first,
                compare_expr, compare_idx, op_info.result_op_brs_))) {
          // 在进行batch内去重的时候，就需要根据compare_idx进行判断，看last_row是从compactrow里面拿，还是从compare_expr里面拿
          LOG_WARN("distinct for batch failed", K(ret));
        }
        op_info.op_idx_ = 0;
        for (int64_t i = 0; OB_SUCC(ret) && !found_valid_row
                                         && i < op_info.child_op_brs_->size_; ++i, ++op_info.op_idx_) {
          if (op_info.result_op_brs_.skip_->at(i)) {
            continue;
          }
          found_valid_row = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObMergeUnionVecOp::convert_batch_rows() {
  int ret = OB_SUCCESS;
  ObExpr *src_expr = NULL;
  ObExpr *dst_expr = NULL;
  // update last_valid_idx_ here, as last idx in set_exprs for store last row after this return
  // and check here
  last_valid_idx_ = left_selectors_item_cnt_ + right_selectors_item_cnt_ - 1;
  if ((left_selectors_item_cnt_ <= 0 ||
      last_valid_idx_ != left_dst_idx_selectors_[left_selectors_item_cnt_ - 1]) &&
      (right_selectors_item_cnt_ <= 0 ||
      last_valid_idx_ != right_dst_idx_selectors_[right_selectors_item_cnt_ - 1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check last_valid_idx_ failed", K(ret), K(last_valid_idx_),
      K(left_selectors_item_cnt_), K(right_selectors_item_cnt_));
  }
  for (int i = 0; OB_SUCC(ret) && i < MY_SPEC.set_exprs_.count(); i++) {
    dst_expr = MY_SPEC.set_exprs_.at(i);
    if (left_selectors_item_cnt_ >= 1) {
      src_expr = left_->get_spec().output_.at(i);
      dispatch_convert_batch_rows(src_expr, dst_expr, left_src_idx_selectors_,
        left_dst_idx_selectors_, left_selectors_item_cnt_);
    }
    if (OB_FAIL(ret)) {
    } else if (right_selectors_item_cnt_ >= 1) {
      src_expr = right_->get_spec().output_.at(i);
      dispatch_convert_batch_rows(src_expr, dst_expr, right_src_idx_selectors_,
        right_dst_idx_selectors_, right_selectors_item_cnt_);
    }
  }
  return ret;
}

void ObMergeUnionVecOp::dispatch_convert_batch_rows(ObExpr *src_expr,
                                                   ObExpr *dst_expr,
                                                   uint16_t *src_selector,
                                                   uint16_t *dst_selector,
                                                   uint16_t selector_cnt)
{
  ObIVector *src_vec = src_expr->get_vector(eval_ctx_);
  ObIVector *dst_vec = dst_expr->get_vector(eval_ctx_);
  int src_format = src_vec->get_format();
  int dst_format = dst_vec->get_format();
  common::ObObjType src_type = src_expr->datum_meta_.type_;
  common::ObObjType dst_type = dst_expr->datum_meta_.type_;
  bool src_has_null = src_vec->has_null();

  if (src_format == dst_format && src_type == dst_type) {
    if (src_format == VEC_FIXED && ob_is_integer_type(src_type)) {
      ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> *src_fixed_vec =
        static_cast<ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> *> (src_vec);
      ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> *dst_fixed_vec =
        static_cast<ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> *> (dst_vec);
      convert_batch_column<FixedLengthVectorBigInt, FixedLengthVectorBigInt> 
        (src_fixed_vec, dst_fixed_vec, src_selector, dst_selector, selector_cnt, src_has_null);
    } else if (src_format == VEC_DISCRETE && ob_is_string_tc(src_type)) {
      ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> *src_string_vec = 
        static_cast<ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> *> (src_vec);
      ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> *dst_string_vec = 
        static_cast<ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> *> (dst_vec);
      convert_batch_column<DiscreteVectorString, DiscreteVectorString> 
        (src_string_vec, dst_string_vec, src_selector, dst_selector, selector_cnt, src_has_null);
    } else {
      convert_batch_column<ObIVector, ObIVector> 
        (src_vec, dst_vec, src_selector, dst_selector, selector_cnt, src_has_null);
    }
  } else {
    convert_batch_column<ObIVector, ObIVector> 
      (src_vec, dst_vec, src_selector, dst_selector, selector_cnt, src_has_null);
  }
}

template<typename SrcVec, typename DstVec>
void ObMergeUnionVecOp::convert_batch_column(SrcVec *src_vec, DstVec *dst_vec,
                                    uint16_t *src_selector, uint16_t *dst_selector,
                                    uint16_t selector_cnt, bool hash_null)
{
  uint16_t src_idx;
  uint16_t dst_idx;
  if (hash_null) {
    for (int i = 0; i < selector_cnt; i++) {
      src_idx = src_selector[i];
      dst_idx = dst_selector[i];
      if (src_vec->is_null(src_idx)) {
        dst_vec->set_null(dst_idx);
      } else {
        dst_vec->set_payload_shallow(dst_idx, src_vec->get_payload(src_idx), src_vec->get_length(src_idx));
      }
    }
  } else {
    for (int i = 0; i < selector_cnt; i++) {
      src_idx = src_selector[i];
      dst_idx = dst_selector[i];
      dst_vec->set_payload_shallow(dst_idx, src_vec->get_payload(src_idx), src_vec->get_length(src_idx));
    }
  }
}

} // end namespace sql
} // end namespace oceanbase
