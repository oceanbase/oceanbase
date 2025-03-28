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
#include "sql/engine/basic/ob_vector_result_holder.h"

namespace oceanbase
{
namespace sql
{
// unpivot v1
OB_SERIALIZE_MEMBER(ObUnpivotInfo,
                    old_column_count_,
                    unpivot_column_count_,
                    for_column_count_,
                    is_include_null_);

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

// unpivot v2
ObUnpivotV2Spec::ObUnpivotV2Spec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), origin_exprs_(alloc),
      label_exprs_(alloc), value_exprs_(alloc), is_include_null_(false) {}

OB_SERIALIZE_MEMBER((ObUnpivotV2Spec, ObOpSpec), origin_exprs_,
                    label_exprs_, value_exprs_, is_include_null_);

ObUnpivotV2Op::ObUnpivotV2Op(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input), cur_part_idx_(-1), offset_(0),
    child_brs_(NULL), buffer_(NULL), using_buffer_(false) {}

int ObUnpivotV2Op::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (!MY_SPEC.use_rich_format_) {
    int64_t size = MY_SPEC.max_batch_size_ * MY_SPEC.origin_exprs_.count() * sizeof(ObDatum);
    buffer_ = static_cast<ObDatum *>(ctx_.get_allocator().alloc(size));
    if (size > 0 && OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(size), K(buffer_));
    }
  } else {
    void *holder_buf = nullptr;
    holder_buf = ctx_.get_allocator().alloc(sizeof(ObVectorsResultHolder));
    if (OB_ISNULL(holder_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else {
      vec_holder_ = new (holder_buf) ObVectorsResultHolder();
      if (OB_FAIL(vec_holder_->init(child_->get_spec().output_, eval_ctx_))) {
        LOG_WARN("init result holder failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (MY_SPEC.use_rich_format_) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.label_exprs_.count(); ++i) {
      ObExpr *label_expr = MY_SPEC.label_exprs_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < label_expr->arg_cnt_; ++j) {
        ObExpr *arg_expr = label_expr->args_[j];
        if (arg_expr->is_batch_result()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param expr of unpivot value expr", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.value_exprs_.count(); ++i) {
      ObExpr *value_expr = MY_SPEC.value_exprs_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < value_expr->arg_cnt_; ++j) {
        ObExpr *arg_expr = value_expr->args_[j];
        if (!arg_expr->is_batch_result()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param expr of unpivot value expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnpivotV2Op::inner_close()
{
  return ObOperator::inner_close();
}

int ObUnpivotV2Op::inner_rescan()
{
  reset();
  return ObOperator::inner_rescan();
}

int ObUnpivotV2Op::inner_get_next_batch(const int64_t max_row_cnt)
{
  return MY_SPEC.use_rich_format_ ? next_vector(max_row_cnt) : next_batch(max_row_cnt);
}

int ObUnpivotV2Op::next_vector(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = common::min(max_row_cnt, MY_SPEC.max_batch_size_);
  int64_t part_count = MY_SPEC.label_exprs_.at(0)->arg_cnt_;
  clear_evaluated_flag();
  if (cur_part_idx_ == -1 || cur_part_idx_ == part_count) {
    if (using_buffer_) {
      if (OB_FAIL(vec_holder_->restore())) {
        LOG_WARN("faild to restore child vector", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(child_->get_next_batch(batch_size, child_brs_))) {
      LOG_WARN("failed to get next batch of child", K(ret));
    } else {
      cur_part_idx_ = 0;
      using_buffer_ = false;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(brs_.copy(child_brs_))) {
    LOG_WARN("failed to copy", K(ret));
  } else if (child_brs_->end_ && child_brs_->size_ == 0) {
    // do nothing
  } else {
    if (offset_ == 0) {
      if (batch_size >= child_brs_->size_) {
        if (OB_FAIL(project_vector())) {
          LOG_WARN("failed to do projection", K(ret));
        }
      } else {
        brs_.size_ = batch_size;
        if (using_buffer_ && OB_FAIL(vec_holder_->restore())) {
          LOG_WARN("failed to restore child vector", K(ret));
        } else if (!using_buffer_ && OB_FAIL(vec_holder_->save(child_brs_->size_))) {
          LOG_WARN("failed to get result buffer", K(ret));
        } else if (FALSE_IT(using_buffer_ = true)) {
        } else if (OB_FAIL(project_vector_with_offset())) {
          LOG_WARN("failed to do batch copy", K(ret));
        }
      }
    } else {
      brs_.size_ = common::min(brs_.size_, child_brs_->size_ - offset_);
      if (OB_FAIL(vec_holder_->restore())) {
        LOG_WARN("failed to restore child vector", K(ret));
      } else {
        brs_.set_all_rows_active(true);
        for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; ++i) {
          if (child_brs_->skip_->at(i + offset_)) {
            brs_.set_skip(i);
            brs_.set_all_rows_active(false);
          } else {
            brs_.reset_skip(i);
          }
        }
      }
      if (OB_FAIL(project_vector_with_offset())) {
        LOG_WARN("failed to do batch copy", K(ret));
      }
    }
    if (!MY_SPEC.is_include_null_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; ++i) {
        if (brs_.skip_->at(i)) {
          // skip, do nothing
        } else {
          bool is_all_null = true;
          for (int64_t j = 0; OB_SUCC(ret) && is_all_null && j < MY_SPEC.value_exprs_.count(); ++j) {
            ObExpr *expr = MY_SPEC.value_exprs_.at(j);
            if (!expr->get_vector(eval_ctx_)->is_null(i)) {
              is_all_null = false;
            }
          }
          if (OB_SUCC(ret) && is_all_null) {
            brs_.skip_->set(i);
            brs_.set_all_rows_active(false);
          }
        }
      }
    }
    brs_.end_ = cur_part_idx_ == part_count && child_brs_->end_;
  }
  return ret;
}

int ObUnpivotV2Op::project_vector()
{
  int ret = OB_SUCCESS;
  if (using_buffer_) {
    if (OB_FAIL(vec_holder_->restore())) {
      LOG_WARN("failed to restore child vector", K(ret));
    } else {
      using_buffer_ = false;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.label_exprs_.count(); ++i) {
    ObExpr *expr = MY_SPEC.label_exprs_.at(i);
    ObExpr *arg_expr = expr->args_[cur_part_idx_];
    if (OB_FAIL(arg_expr->eval_vector(eval_ctx_, *child_brs_))) {
      LOG_WARN("failed to eval arg expr", K(ret));
    } else if (arg_expr->get_format(eval_ctx_) != VEC_UNIFORM_CONST) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg_expr format", K(ret));
    } else {
      ObDatum *arg_datum = static_cast<ObUniformBase *>(arg_expr->get_vector(eval_ctx_))->get_datums();
      OZ(expr->init_vector(eval_ctx_, VEC_UNIFORM_CONST, child_brs_->size_));
      ObUniformBase *vec = static_cast<ObUniformBase *>(expr->get_vector(eval_ctx_));
      *vec->get_datums() = *arg_datum;
    }
    expr->set_evaluated_projected(eval_ctx_);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.value_exprs_.count(); ++i) {
    ObExpr *expr = MY_SPEC.value_exprs_.at(i);
    ObExpr *arg_expr = expr->args_[cur_part_idx_];
    if (OB_FAIL(arg_expr->eval_vector(eval_ctx_, *child_brs_))) {
      LOG_WARN("eval batch failed", K(ret));
    } else {
      VectorHeader &arg_vec_header = arg_expr->get_vector_header(eval_ctx_);
      VectorHeader &vec_header = expr->get_vector_header(eval_ctx_);
      if (arg_vec_header.format_ == VEC_UNIFORM_CONST) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg_expr format", K(ret));
      } else if (arg_vec_header.format_ == VEC_UNIFORM) {
        ObUniformBase *uni_vec = static_cast<ObUniformBase *>(arg_expr->get_vector(eval_ctx_));
        ObDatum *src = uni_vec->get_datums();
        ObDatum *dst = expr->locate_batch_datums(eval_ctx_);
        if (src != dst) {
          MEMCPY(dst, src, child_brs_->size_ * sizeof(ObDatum));
        }
        OZ(expr->init_vector(eval_ctx_, VEC_UNIFORM, child_brs_->size_));
      } else {
        vec_header = arg_vec_header;
        if (arg_expr->is_nested_expr()) {
          OZ(expr->assign_nested_vector(*arg_expr, eval_ctx_));
        }
      }
    }
    expr->set_evaluated_projected(eval_ctx_);
  }
  if (OB_SUCC(ret)) {
    cur_part_idx_++;
  }
  return ret;
}

int ObUnpivotV2Op::project_vector_with_offset()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.origin_exprs_.count(); ++i) {
    ObExpr *expr = MY_SPEC.origin_exprs_.at(i);
    ObIVector *vec = expr->get_vector(eval_ctx_);
    if (0 != offset_) {
      for (int64_t j = 0; OB_SUCC(ret) && j < brs_.size_; ++j) {
        if (child_brs_->skip_->at(j + offset_)) {
          /* do nothing */
        } else if (vec->is_null(j + offset_)) {
          vec->set_null(j);
        } else {
          vec->unset_null(j);
          vec->set_payload_shallow(j, vec->get_payload(j + offset_), vec->get_length(j + offset_));
        }
      }
    }
    expr->set_evaluated_projected(eval_ctx_);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.label_exprs_.count(); ++i) {
    ObExpr *expr = MY_SPEC.label_exprs_.at(i);
    ObExpr *arg_expr = expr->args_[cur_part_idx_];
    if (OB_FAIL(arg_expr->eval_vector(eval_ctx_, *child_brs_))) {
      LOG_WARN("failed to eval arg expr", K(ret));
    } else {
      ObDatum *arg_datum = static_cast<ObUniformBase *>(arg_expr->get_vector(eval_ctx_))->get_datums();
      OZ(expr->init_vector(eval_ctx_, VEC_UNIFORM_CONST, child_brs_->size_));
      ObUniformBase *vec = static_cast<ObUniformBase *>(expr->get_vector(eval_ctx_));
      *vec->get_datums() = *arg_datum;
    }
    expr->set_evaluated_projected(eval_ctx_);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.value_exprs_.count(); ++ i) {
    ObExpr *expr = MY_SPEC.value_exprs_.at(i);
    ObExpr *arg_expr = expr->args_[cur_part_idx_];
    if (OB_FAIL(arg_expr->eval_vector(eval_ctx_, *child_brs_))) {
      LOG_WARN("failed to eval batch for arg expr", K(ret));
    } else if (OB_FAIL(expr->init_vector(eval_ctx_, arg_expr->get_format(eval_ctx_), brs_.size_))) {
      LOG_WARN("failed to init vector", K(ret));
    } else {
      ObIVector *vec = expr->get_vector(eval_ctx_);
      ObIVector *arg_vec = arg_expr->get_vector(eval_ctx_);
      for (int64_t j = 0; OB_SUCC(ret) && j < brs_.size_; ++j) {
        if (child_brs_->skip_->at(j + offset_)) {
          /* do nothing */
        } else if (arg_vec->is_null(j + offset_)) {
          vec->set_null(j);
        } else {
          vec->unset_null(j);
          vec->set_payload_shallow(j, arg_vec->get_payload(j + offset_), arg_vec->get_length(j + offset_));
        }
      }
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  if (OB_SUCC(ret)) {
    offset_ += brs_.size_;
    if (offset_ == child_brs_->size_) {
      offset_ = 0;
      cur_part_idx_++;
    }
  }
  return ret;
}

int ObUnpivotV2Op::next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = common::min(max_row_cnt, MY_SPEC.max_batch_size_);
  int64_t part_count = MY_SPEC.label_exprs_.at(0)->arg_cnt_;
  clear_evaluated_flag();
  if (cur_part_idx_ == -1 || cur_part_idx_ == part_count) {
    if (OB_FAIL(restore_batch_if_need())) {
      LOG_WARN("failed to restore batch", K(ret));
    } else if (OB_FAIL(child_->get_next_batch(batch_size, child_brs_))) {
      LOG_WARN("failed to get next batch of child", K(ret));
    } else {
      cur_part_idx_ = 0;
      using_buffer_ = false;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(brs_.copy(child_brs_))) {
    LOG_WARN("failed to copy", K(ret));
  } else if (child_brs_->end_ && child_brs_->size_ == 0) {
    // do nothing
  } else {
    if (offset_ == 0) {
      if (batch_size >= child_brs_->size_) {
        if (OB_FAIL(batch_project_data())) {
          LOG_WARN("failed to do projection", K(ret));
        }
      } else {
        brs_.size_ = batch_size;
        if (OB_FAIL(get_result_buffer())) {
          LOG_WARN("failed to get result buffer", K(ret));
        } else if (OB_FAIL(batch_copy_data())) {
          LOG_WARN("failed to do batch copy", K(ret));
        }
      }
    } else {
      brs_.size_ = common::min(brs_.size_, child_brs_->size_ - offset_);
      if (OB_FAIL(batch_copy_data())) {
        LOG_WARN("failed to do batch copy", K(ret));
      }
    }
    if (!MY_SPEC.is_include_null_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; ++i) {
        if (brs_.skip_->at(i)) {
          // skip, do nothing
        } else {
          bool is_all_null = true;
          for (int64_t j = 0; OB_SUCC(ret) && is_all_null && j < MY_SPEC.value_exprs_.count(); ++j) {
            ObExpr *expr = MY_SPEC.value_exprs_.at(j);
            if (!expr->locate_batch_datums(eval_ctx_)[i].is_null()) {
              is_all_null = false;
            }
          }
          if (OB_SUCC(ret) && is_all_null) {
            brs_.skip_->set(i);
            brs_.set_all_rows_active(false);
          }
        }
      }
    }
    brs_.end_ = cur_part_idx_ == part_count && child_brs_->end_;
  }
  return ret;
}

// project lab expr and val expr
int ObUnpivotV2Op::batch_project_data()
{
  int ret = OB_SUCCESS;
  if (using_buffer_) {
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.origin_exprs_.count(); ++ i) {
      ObExpr *expr = MY_SPEC.origin_exprs_.at(i);
      ObDatum *buffer_datums = buffer_ + pos;
      if (expr->is_batch_result()) {
        ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
        MEMCPY(datums, buffer_datums, child_brs_->size_ * sizeof(ObDatum));
        pos += child_brs_->size_;
      } else {
        // expr that is not batch result do not need modify datum;
        // do nothing
      }
      expr->set_evaluated_projected(eval_ctx_);
      using_buffer_ = false;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.label_exprs_.count(); ++i) {
    ObExpr *expr = MY_SPEC.label_exprs_.at(i);
    ObExpr *arg_expr = expr->args_[cur_part_idx_];
    ObDatum *datum = NULL;
    if (OB_FAIL(arg_expr->eval(eval_ctx_, datum))) {
      LOG_WARN("failed to eval arg expr", K(ret));
    } else {
      expr->locate_expr_datum(eval_ctx_) = *datum;
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.value_exprs_.count(); ++i) {
    ObExpr *expr = MY_SPEC.value_exprs_.at(i);
    ObExpr *arg_expr = expr->args_[cur_part_idx_];
    if (OB_FAIL(arg_expr->eval_batch(eval_ctx_, *child_brs_->skip_, child_brs_->size_))) {
      LOG_WARN("failed to eval batch for arg expr", K(ret));
    } else {
      ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
      ObDatum *arg_datums = arg_expr->locate_batch_datums(eval_ctx_);
      MEMCPY(datums, arg_datums, brs_.size_ * sizeof(ObDatum));
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  if (OB_SUCC(ret)) {
    cur_part_idx_++;
  }
  return ret;
}

int ObUnpivotV2Op::batch_copy_data()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  brs_.set_all_rows_active(true);
  for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; ++i) {
    if (child_brs_->skip_->at(i + offset_)) {
      brs_.set_skip(i);
      brs_.set_all_rows_active(false);
    } else {
      brs_.reset_skip(i);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.origin_exprs_.count(); ++ i) {
    ObExpr *expr = MY_SPEC.origin_exprs_.at(i);
    ObDatum *buffer_datums = buffer_ + pos;
    if (expr->is_batch_result()) {
      ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
      MEMCPY(datums, buffer_datums + offset_, brs_.size_ * sizeof(ObDatum));
      pos += child_brs_->size_;
    } else {
      // expr that is not batch result do not need modify datum;
      // do nothing
    }
    expr->set_evaluated_projected(eval_ctx_);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.label_exprs_.count(); ++ i) {
    ObExpr *expr = MY_SPEC.label_exprs_.at(i);
    ObExpr *arg_expr = expr->args_[cur_part_idx_];
    ObDatum *datum = NULL;
    if (OB_FAIL(arg_expr->eval(eval_ctx_, datum))) {
      LOG_WARN("failed to eval arg expr", K(ret));
    } else {
      expr->locate_expr_datum(eval_ctx_) = *datum;
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.value_exprs_.count(); ++ i) {
    ObExpr *expr = MY_SPEC.value_exprs_.at(i);
    ObExpr *arg_expr = expr->args_[cur_part_idx_];
    if (OB_FAIL(arg_expr->eval_batch(eval_ctx_, *child_brs_->skip_, child_brs_->size_))) {
      LOG_WARN("failed to eval batch for arg expr", K(ret));
    } else {
      ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
      ObDatum *arg_datums = arg_expr->locate_batch_datums(eval_ctx_);
      MEMCPY(datums, (arg_datums + offset_), brs_.size_ * sizeof(ObDatum));
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  if (OB_SUCC(ret)) {
    offset_ += brs_.size_;
    if (offset_ == child_brs_->size_) {
      offset_ = 0;
      cur_part_idx_++;
    }
  }
  return ret;
}

int ObUnpivotV2Op::get_result_buffer()
{
  int ret = OB_SUCCESS;
  if (!using_buffer_) {
    int64_t pos = 0;
    using_buffer_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.origin_exprs_.count(); ++i) {
      ObExpr *origin_expr = MY_SPEC.origin_exprs_.at(i);
      ObDatum *datums = origin_expr->locate_batch_datums(eval_ctx_);
      if (OB_FAIL(origin_expr->eval_batch(eval_ctx_, *child_brs_->skip_, child_brs_->size_))) {
        LOG_WARN("failed to eval batch", K(ret));
      } else if (origin_expr->is_batch_result()) {
        MEMCPY(buffer_ + pos, datums, child_brs_->size_ * sizeof(ObDatum));
        pos += child_brs_->size_;
      } else {
        // expr that is not batch result do not need buffer;
        // do nothing
      }
    }
  }
  return ret;
}

int ObUnpivotV2Op::restore_batch_if_need()
{
  int ret = OB_SUCCESS;
  if (using_buffer_) {
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.origin_exprs_.count(); ++i) {
      ObExpr *origin_expr = MY_SPEC.origin_exprs_.at(i);
      ObDatum *datums = origin_expr->locate_batch_datums(eval_ctx_);
      if (OB_FAIL(origin_expr->eval_batch(eval_ctx_, *child_brs_->skip_, child_brs_->size_))) {
          LOG_WARN("failed to eval batch", K(ret));
      } else if (origin_expr->is_batch_result()) {
        MEMCPY(datums, buffer_ + pos, child_brs_->size_ * sizeof(ObDatum));
        pos += child_brs_->size_;
      } else {
        // expr that is not batch result do not need buffer;
        // do nothing
      }
    }
    using_buffer_ = false;
  }
  return ret;
}

void ObUnpivotV2Op::destroy()
{
  ObOperator::destroy();
}

void ObUnpivotV2Op::reset()
{
  cur_part_idx_ = -1;
  offset_ = 0;
  child_brs_ = NULL;
  using_buffer_ = false;
}

} // end namespace sql
} // end namespace oceanbase
