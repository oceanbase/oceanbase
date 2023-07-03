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

#include "sql/engine/subquery/ob_unpivot_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
ObUnpivotSpec::ObUnpivotSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type), max_part_count_(-1), unpivot_info_(false, 0, 0, 0) {}

OB_SERIALIZE_MEMBER((ObUnpivotSpec, ObOpSpec), max_part_count_, unpivot_info_);

ObUnpivotOp::ObUnpivotOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input), curr_part_idx_(-1), curr_cols_idx_(-1),
    child_brs_(NULL), multiplex_(NULL) {}

int ObUnpivotOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (MY_SPEC.is_vectorized()) {
    int64_t size = MY_SPEC.max_batch_size_ * MY_SPEC.get_output_count() * sizeof(ObDatum);
    multiplex_ = static_cast<ObDatum *>(ctx_.get_allocator().alloc(size));
    if (size > 0 && OB_ISNULL(multiplex_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(size), K(multiplex_));
    }
  }
  return ret;
}

int ObUnpivotOp::inner_close()
{
  return ObOperator::inner_close();
}

int ObUnpivotOp::inner_rescan()
{
  reset();
  return ObOperator::inner_rescan();
}

int ObUnpivotOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  clear_evaluated_flag();
  if (curr_part_idx_ < 0 || curr_part_idx_ >= MY_SPEC.max_part_count_) {
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row from child operator failed", K(ret));
      }
    } else {
      curr_part_idx_ = 0;
      got_next_row = true;
    }
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("arrive unpivot", K(ret), K(child_->get_spec().output_), K(MY_SPEC.output_),
              K(curr_part_idx_), K(MY_SPEC.max_part_count_), K(MY_SPEC.unpivot_info_));
  }

  if (OB_SUCC(ret) && !MY_SPEC.unpivot_info_.is_include_null_) {
    bool need_try_next_part = true;
    while (need_try_next_part && OB_SUCC(ret)) {
      if (curr_part_idx_ >= MY_SPEC.max_part_count_) {
        if (got_next_row) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("has already get next row once, maybe filter does not work", K(ret),
                    K(child_->get_spec().output_));
        } else if (OB_FAIL(child_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row from child operator failed", K(ret));
          }
        } else {
          curr_part_idx_ = 0;
          got_next_row = true;
        }
      }

      int64_t null_count = 0;
      const int64_t base_idx = curr_part_idx_ * MY_SPEC.unpivot_info_.get_new_column_count()
                              + MY_SPEC.unpivot_info_.old_column_count_
                              + MY_SPEC.unpivot_info_.for_column_count_;
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.unpivot_info_.unpivot_column_count_; ++i) {
        const int64_t input_idx = i + base_idx;
        ObDatum *datum = NULL;
        ObExpr *expr = child_->get_spec().output_[input_idx];
        if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
          LOG_WARN("expr evaluate failed", K(ret), K(expr));
        } else if (datum->is_null()) {
          ++null_count;
        }
      }

      if (OB_SUCC(ret)) {
        if (null_count == MY_SPEC.unpivot_info_.unpivot_column_count_) {
          LOG_DEBUG("is null, try next row", K(ret), K(curr_part_idx_),
                    K(MY_SPEC.unpivot_info_), K(child_->get_spec().output_));
          ++curr_part_idx_;
        } else {
          need_try_next_part = false;
        }
      }
    } // end of while
  } // end of exclude null

  if (OB_SUCC(ret)) {
    // old_column is the same as child, pass
    int64_t output_idx = MY_SPEC.unpivot_info_.old_column_count_;
    const int64_t base_idx = curr_part_idx_ * MY_SPEC.unpivot_info_.get_new_column_count();
    for (; OB_SUCC(ret) && output_idx < MY_SPEC.get_output_count(); ++output_idx) {
      const int64_t input_idx = output_idx + base_idx;
      ObDatum *datum = NULL;
      ObExpr *expr = child_->get_spec().output_[input_idx];
      if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
        LOG_WARN("expr evaluate failed", K(ret), K(expr));
      } else {
        MY_SPEC.output_[output_idx]->locate_expr_datum(eval_ctx_) = *datum;
        MY_SPEC.output_[output_idx]->set_evaluated_projected(eval_ctx_);
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("output next row", K(curr_part_idx_), K(MY_SPEC.unpivot_info_),
              K(child_->get_spec().output_), K(MY_SPEC.output_));
    ++curr_part_idx_;
  }
  return ret;
}

int ObUnpivotOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = nullptr;
  int64_t batch_size = common::min(max_row_cnt, MY_SPEC.max_batch_size_);
  clear_evaluated_flag();

  if (curr_part_idx_ < 0 || curr_part_idx_ >= MY_SPEC.max_part_count_) {
    const ObBatchRows *child_brs = nullptr;
    if (OB_FAIL(child_->get_next_batch(batch_size, child_brs))) {
      LOG_WARN("get next batch from child operator failed", K(ret));
    } else if (child_brs->end_ && 0 == child_brs->size_) {
      ret = OB_ITER_END;
    } else {
      brs_.size_ = child_brs->size_;
      child_brs_ = child_brs;
      curr_part_idx_ = 0;
      curr_cols_idx_ = 0;
      int64_t pos = 0;
      MEMSET(multiplex_, 0, MY_SPEC.max_batch_size_ * MY_SPEC.get_output_count() * sizeof(ObDatum));
      for (int64_t cols = 0; OB_SUCC(ret) && cols < MY_SPEC.get_output_count(); ++cols) {
        ObExpr *child_expr = child_->get_spec().output_[cols];
        ObDatum *child_datum = child_expr->locate_batch_datums(eval_ctx_);
        if (child_expr->is_batch_result()) {
          MEMCPY(multiplex_ + pos, child_datum, child_brs->size_ * sizeof(ObDatum));
          pos += child_brs->size_;
        } else {
          for (int64_t rows = 0; rows < child_brs->size_; ++rows) {
            MEMCPY(multiplex_ + pos++, child_datum, sizeof(ObDatum));
          }
        }
      }
    }
  } else {
    brs_.size_ = child_brs_->size_;
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("arrive unpivot batch", K(ret), K(child_->get_spec().output_), K(MY_SPEC.output_),
              K(curr_part_idx_), K(MY_SPEC.max_part_count_), K(MY_SPEC.unpivot_info_));
  }

  for (int64_t read_piece = 0; OB_SUCC(ret) && read_piece < brs_.size_; ++read_piece) {
    int64_t read_cur_row = (curr_part_idx_ * brs_.size_ + read_piece) / MY_SPEC.max_part_count_;
    // check row whether is skip in child
    if (OB_SUCC(ret) && child_brs_->skip_->at(read_cur_row)) {
      brs_.skip_->set(read_piece);
      curr_cols_idx_ = (curr_cols_idx_ + 1) % MY_SPEC.max_part_count_;
      continue;
    }
    // check for_column whether is all null
    if (OB_SUCC(ret) && !MY_SPEC.unpivot_info_.is_include_null_) {
      int64_t null_count = 0;
      const int64_t base_idx = curr_cols_idx_ * MY_SPEC.unpivot_info_.get_new_column_count()
                               + MY_SPEC.unpivot_info_.old_column_count_
                               + MY_SPEC.unpivot_info_.for_column_count_;
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.unpivot_info_.unpivot_column_count_; ++i) {
        const int64_t input_idx = i + base_idx;
        if (0 == curr_cols_idx_) {
          if (multiplex_[input_idx * brs_.size_ + read_cur_row].is_null()) {
            ++null_count;
          }
        } else {
          ObExpr *child_expr = child_->get_spec().output_[input_idx];
          ObDatum &child_datum = child_expr->locate_expr_datum(eval_ctx_, read_cur_row);
          if (child_datum.is_null()) {
            ++null_count;
          }
        }
      }
      if (OB_SUCC(ret) && null_count == MY_SPEC.unpivot_info_.unpivot_column_count_) {
        brs_.skip_->set(read_piece);
        curr_cols_idx_ = (curr_cols_idx_ + 1) % MY_SPEC.max_part_count_;
        continue;
      }
    }
    int64_t output_idx = 0;
    // old_column
    for (; OB_SUCC(ret) && output_idx < MY_SPEC.unpivot_info_.old_column_count_; ++output_idx) {
      ObExpr *father_expr = MY_SPEC.output_[output_idx];
      ObDatum *father_datum = father_expr->locate_batch_datums(eval_ctx_);
      int64_t datum_pos = father_expr->is_batch_result() ? read_piece : 0;
      father_datum[datum_pos] = multiplex_[output_idx * brs_.size_ + read_cur_row];
    }
    // new_column
    if (0 == curr_cols_idx_) {
      for (; OB_SUCC(ret) && output_idx < MY_SPEC.get_output_count(); ++output_idx) {
        ObExpr *father_expr = MY_SPEC.output_[output_idx];
        ObDatum *father_datum = father_expr->locate_batch_datums(eval_ctx_);
        int64_t datum_pos = father_expr->is_batch_result() ? read_piece : 0;
        father_datum[datum_pos] = multiplex_[output_idx * brs_.size_ + read_cur_row];
      }
    } else {
      const int64_t base_idx = curr_cols_idx_ * MY_SPEC.unpivot_info_.get_new_column_count();
      for (; OB_SUCC(ret) && output_idx < MY_SPEC.get_output_count(); ++output_idx) {
        const int64_t input_idx = output_idx + base_idx;
        ObExpr *child_expr = child_->get_spec().output_[input_idx];
        ObExpr *father_expr = MY_SPEC.output_[output_idx];
        ObDatum *child_datum = child_expr->locate_batch_datums(eval_ctx_);
        ObDatum *father_datum = father_expr->locate_batch_datums(eval_ctx_);
        int64_t datum_pos = father_expr->is_batch_result() ? read_piece : 0;
        if (child_expr->is_batch_result()) {
          father_datum[datum_pos] = child_datum[read_cur_row];
        } else {
          father_datum[datum_pos] = *child_datum;
        }
      }
    }
    curr_cols_idx_ = (curr_cols_idx_ + 1) % MY_SPEC.max_part_count_;
  }

  if (OB_SUCC(ret)) {
    ++curr_part_idx_;
    if (curr_part_idx_ == MY_SPEC.max_part_count_) {
      for (int64_t output_idx = 0; output_idx < MY_SPEC.get_output_count(); ++output_idx) {
        ObExpr *father_expr = MY_SPEC.output_[output_idx];
        father_expr->set_evaluated_projected(eval_ctx_);
      }
    }
  } else if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.end_ = true;
    brs_.size_ = 0;
  }
  return ret;
}

void ObUnpivotOp::destroy()
{
  ObOperator::destroy();
}

void ObUnpivotOp::reset()
{
  curr_part_idx_ = -1;
  curr_cols_idx_ = -1;
  child_brs_ = NULL;
  memset(multiplex_, 0, MY_SPEC.max_batch_size_ * MY_SPEC.get_output_count() * sizeof(ObDatum));
}

} // end namespace sql
} // end namespace oceanbase
