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

#include "sql/engine/basic/ob_vector_result_holder.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

void ObVectorsResultHolder::ObColResultHolder::reset(common::ObIAllocator &alloc)
{
#define SAFE_DELETE(ptr)  \
if (nullptr != ptr) {     \
  alloc.free(ptr);        \
  ptr = nullptr;          \
}

  LST_DO_CODE(SAFE_DELETE,
              frame_nulls_,
              frame_datums_,
              frame_data_,
              frame_lens_,
              frame_ptrs_,
              frame_offsets_,
              frame_continuous_data_);
}

int ObVectorsResultHolder::ObColResultHolder::copy_vector_base(const ObVectorBase &vec)
{
  int ret = OB_SUCCESS;
  CK (max_row_cnt_ > 0 && max_row_cnt_ <= UINT16_MAX);
  return ret;
}
int ObVectorsResultHolder::ObColResultHolder::
copy_bitmap_null_base(const ObBitmapNullVectorBase &vec,
                      common::ObIAllocator &alloc,
                      const int64_t batch_size,
                      ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_vector_base(vec))) {
    LOG_WARN("failed to copy vector base", K(ret));
  } else if (nullptr == frame_nulls_) {
    void *buffer = nullptr;
    if (OB_ISNULL(buffer = alloc.alloc(sql::ObBitVector::memory_size(max_row_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      frame_nulls_ = sql::to_bit_vector(buffer);
    }
  }
  if (OB_SUCC(ret)) {
    has_null_ = vec.has_null();
    nulls_ = const_cast<sql::ObBitVector *> (vec.get_nulls());
    if (OB_NOT_NULL(expr_)) {
      frame_nulls_->deep_copy(expr_->get_nulls(eval_ctx), max_row_cnt_);
    }
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::
copy_fixed_base(const ObFixedLengthBase &vec,
                ObIAllocator &alloc,
                const int64_t batch_size,
                ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_bitmap_null_base(vec, alloc, batch_size, eval_ctx))) {
    LOG_WARN("failed to copy bitmap null base", K(ret));
  } else if (nullptr == frame_data_) {
    len_ = vec.get_length();
    if (OB_ISNULL(frame_data_ = static_cast<char *> (alloc.alloc(len_ * max_row_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(len_ * max_row_cnt_));
    }
  }
  if (OB_SUCC(ret)) {
    data_ = const_cast<char *> (vec.get_data());
    if (OB_NOT_NULL(expr_)) {
      MEMCPY(frame_data_, expr_->get_rev_buf(eval_ctx), len_ * max_row_cnt_);
    }
  }
  return ret;
}
int ObVectorsResultHolder::ObColResultHolder::
copy_discrete_base(const ObDiscreteBase &vec,
                   ObIAllocator &alloc,
                   const int64_t batch_size,
                   ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_bitmap_null_base(vec, alloc, batch_size, eval_ctx))) {
    LOG_WARN("failed to copy bitmap null base", K(ret));
  } else if (nullptr == frame_lens_) {
    if (OB_ISNULL(frame_lens_
                  = static_cast<ObLength *> (alloc.alloc(sizeof(ObLength) * max_row_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc lengths", K(ret));
    } else if (OB_ISNULL(frame_ptrs_
                          = static_cast<char **> (alloc.alloc(sizeof(char *) * max_row_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc ptrs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    lens_ = const_cast<ObLength *> (vec.get_lens());
    ptrs_ = const_cast<char **> (vec.get_ptrs());
    if (OB_NOT_NULL(expr_)) {
      MEMCPY(frame_lens_, expr_->get_discrete_vector_lens(eval_ctx), sizeof(ObLength) * max_row_cnt_);
      MEMCPY(frame_ptrs_, expr_->get_discrete_vector_ptrs(eval_ctx), sizeof(char *) * max_row_cnt_);
    }
  }
  return ret;
}
int ObVectorsResultHolder::ObColResultHolder::
copy_continuous_base(const ObContinuousBase &vec,
                     ObIAllocator &alloc,
                     const int64_t batch_size,
                     ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_bitmap_null_base(vec, alloc, batch_size, eval_ctx))) {
    LOG_WARN("failed to copy bitmap null base", K(ret));
  } else if (nullptr == frame_offsets_) {
    if (OB_ISNULL(frame_offsets_
                  = static_cast<uint32_t *> (alloc.alloc(sizeof(uint32_t) * (max_row_cnt_ + 1))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc offsets", K(ret), K(max_row_cnt_));
    }
  }
  if (OB_SUCC(ret)) {
    offsets_ = const_cast<uint32_t *> (vec.get_offsets());
    continuous_data_ = const_cast<char *> (vec.get_data());
    if (OB_NOT_NULL(expr_)) {
      MEMCPY(frame_offsets_, expr_->get_continuous_vector_offsets(eval_ctx), sizeof(uint32_t) * (max_row_cnt_ + 1));
    }
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::copy_uniform_base(
    const ObExpr *expr, const ObUniformBase &vec, bool is_const,
    ObEvalCtx &eval_ctx, ObIAllocator &alloc,
    const int64_t batch_size) {
  int ret = OB_SUCCESS;
  int64_t copy_size = is_const ? 1 : max_row_cnt_;
  bool need_copy_rev_buf = expr->res_buf_len_ > 0 &&
                           (expr->is_fixed_length_data_ ||
                           ObNumberTC == ob_obj_type_class(expr->datum_meta_.get_type()));
  if (OB_FAIL(copy_vector_base(vec))) {
    LOG_WARN("failed to copy vector base", K(ret));
  } else {
    if (nullptr == frame_datums_) {
      if (OB_ISNULL(frame_datums_ = static_cast<ObDatum *>(
                        alloc.alloc(sizeof(ObDatum) * copy_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc datums", K(ret), K(copy_size),
                 K(max_row_cnt_));
      }
    }
    if (OB_SUCC(ret) && need_copy_rev_buf && nullptr == frame_data_) {
      len_ = expr->res_buf_len_;
      if (OB_ISNULL(
              frame_data_ = static_cast<char *>(alloc.alloc(len_ * copy_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(len_ * copy_size));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (need_copy_rev_buf) {
      MEMCPY(frame_data_, expr->get_rev_buf(eval_ctx), len_ * copy_size);
    }
    datums_ = const_cast<ObDatum *> (vec.get_datums());
    MEMCPY(frame_datums_, expr->locate_batch_datums(eval_ctx), sizeof(ObDatum) * copy_size);
  }
  return ret;
}

void ObVectorsResultHolder::ObColResultHolder::restore_vector_base(ObVectorBase &vec) const
{
  UNUSED(vec);
}
void ObVectorsResultHolder::ObColResultHolder::
restore_bitmap_null_base(ObBitmapNullVectorBase &vec, const int64_t batch_size, ObEvalCtx &eval_ctx) const
{
  restore_vector_base(vec);
  vec.set_has_null(has_null_);
  vec.set_nulls(nulls_);
  if (OB_NOT_NULL(expr_)) {
    expr_->get_nulls(eval_ctx).deep_copy(*frame_nulls_, max_row_cnt_);
  }

}
void ObVectorsResultHolder::ObColResultHolder::restore_fixed_base(ObFixedLengthBase &vec,
                                                                  const int64_t batch_size,
                                                                  ObEvalCtx &eval_ctx) const
{
  restore_bitmap_null_base(vec, batch_size, eval_ctx);
  vec.set_data(data_);
  if (OB_NOT_NULL(expr_)) {
    MEMCPY(expr_->get_rev_buf(eval_ctx), frame_data_, len_ * max_row_cnt_);
  }
}


void ObVectorsResultHolder::ObColResultHolder::restore_discrete_base(ObDiscreteBase &vec,
                                                                     const int64_t batch_size,
                                                                     ObEvalCtx &eval_ctx) const
{
  restore_bitmap_null_base(vec, batch_size, eval_ctx);
  vec.set_lens(lens_);
  vec.set_ptrs(ptrs_);
  if (OB_NOT_NULL(expr_)) {
    MEMCPY(expr_->get_discrete_vector_lens(eval_ctx), frame_lens_, sizeof(ObLength) * max_row_cnt_);
    MEMCPY(expr_->get_discrete_vector_ptrs(eval_ctx), frame_ptrs_, sizeof(char *) * max_row_cnt_);
  }
}
void ObVectorsResultHolder::ObColResultHolder::
restore_continuous_base(ObContinuousBase &vec,
                        const int64_t batch_size,
                        ObEvalCtx &eval_ctx) const
{
  restore_bitmap_null_base(vec, batch_size, eval_ctx);
  vec.set_data(continuous_data_);
  vec.set_offsets(offsets_);
  if (OB_NOT_NULL(expr_)) {
    MEMCPY(expr_->get_continuous_vector_offsets(eval_ctx), frame_offsets_, sizeof(uint32_t) * (max_row_cnt_ + 1));
  }
}


void ObVectorsResultHolder::ObColResultHolder::restore_uniform_base(
    const ObExpr *expr, ObUniformBase &vec, bool is_const,
    ObEvalCtx &eval_ctx,
    const int64_t batch_size) const {
  int ret = OB_SUCCESS;
  bool need_copy_rev_buf = expr->res_buf_len_ > 0 &&
                           (expr->is_fixed_length_data_ ||
                           ObNumberTC == ob_obj_type_class(expr->datum_meta_.get_type()));
  int64_t copy_size = is_const ? 1 : max_row_cnt_;
  restore_vector_base(vec);
  vec.set_datums(datums_);
  MEMCPY(expr->locate_batch_datums(eval_ctx), frame_datums_, sizeof(ObDatum) * copy_size);
  if (need_copy_rev_buf) {
    MEMCPY(expr->get_rev_buf(eval_ctx), frame_data_, len_ * copy_size);
  }
}

void ObVectorsResultHolder::ObColResultHolder::restore_base_single_row(ObVectorBase &vec, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const
{
  UNUSED(vec);
  UNUSED(from_idx);
  UNUSED(to_idx);
  UNUSED(eval_ctx);
}

void ObVectorsResultHolder::ObColResultHolder::restore_bitmap_null_base_single_row(ObBitmapNullVectorBase &vec, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const
{
  restore_base_single_row(vec, from_idx, to_idx, eval_ctx);
  vec.set_has_null(has_null_);
  // vec.set_nulls(nulls_);
  if (OB_NOT_NULL(expr_)) {
    ObBitVector &dst_bit_vec = expr_->get_nulls(eval_ctx);
    ObBitVector *src_bit_vec =  (&dst_bit_vec == nulls_) ? to_bit_vector(frame_nulls_) : nulls_;
    if (src_bit_vec->at(from_idx)) {
      dst_bit_vec.set(to_idx);
    } else {
      dst_bit_vec.reset(to_idx);
    }
  }
}

void ObVectorsResultHolder::ObColResultHolder::restore_fixed_base_single_row(ObFixedLengthBase &vec,
                                                                             int64_t from_idx,
                                                                             int64_t to_idx,
                                                                             ObEvalCtx &eval_ctx) const
{
  restore_bitmap_null_base_single_row(vec, from_idx, to_idx, eval_ctx);
  if (!vec.is_null(from_idx)) {
    if (OB_NOT_NULL(expr_)) {
      if (data_ == expr_->get_rev_buf(eval_ctx)) {
        MEMCPY(expr_->get_rev_buf(eval_ctx) + to_idx * len_, frame_data_ + from_idx * len_, len_);
      } else {
        MEMCPY(expr_->get_rev_buf(eval_ctx) + to_idx * len_, data_ + from_idx * len_, len_);
      }
    }
  }
}

void ObVectorsResultHolder::ObColResultHolder::restore_discrete_base_single_row(ObDiscreteBase &vec,
                                                                             int64_t from_idx,
                                                                             int64_t to_idx,
                                                                             ObEvalCtx &eval_ctx) const
{
  restore_bitmap_null_base_single_row(vec, from_idx, to_idx, eval_ctx);
  //vec.set_lens(lens_);
  //vec.set_ptrs(ptrs_);
  if (OB_NOT_NULL(expr_)) {
    if (lens_ == expr_->get_discrete_vector_lens(eval_ctx)) {
      MEMCPY(expr_->get_discrete_vector_lens(eval_ctx) + to_idx, frame_lens_ + from_idx, sizeof(ObLength));
    } else {
      MEMCPY(expr_->get_discrete_vector_lens(eval_ctx) + to_idx, lens_ + from_idx, sizeof(ObLength));
    }
    if (expr_->get_discrete_vector_ptrs(eval_ctx) == ptrs_) {
      MEMCPY(expr_->get_discrete_vector_ptrs(eval_ctx) + to_idx, frame_ptrs_ + from_idx, sizeof(char *));
    } else {
      MEMCPY(expr_->get_discrete_vector_ptrs(eval_ctx) + to_idx, ptrs_ + from_idx, sizeof(char *));
    }
  }
}
// NOTE: continous format can't be restored single row because the value of elems[k] will affect the value of elems[k +1] ... elems[k + N] for var-len type
// So we need convert the continous format to other format and then restore single row
void ObVectorsResultHolder::ObColResultHolder::restore_continuous_base_single_row(ObExpr *expr, int64_t from_idx,
                                                                                  int64_t to_idx, VectorFormat dst_fmt, ObEvalCtx &eval_ctx) const
{
  switch(dst_fmt) {
    case VEC_FIXED:
      convert_continuous_to_fixed(static_cast<ObFixedLengthBase &>(*expr->get_vector(eval_ctx)), from_idx, to_idx, eval_ctx);
      break;
    case VEC_DISCRETE:
      convert_continuous_to_discrete(static_cast<ObDiscreteBase &>(*expr->get_vector(eval_ctx)), from_idx, to_idx, eval_ctx);
      break;
    case VEC_UNIFORM:
      convert_continuous_to_uniform(expr, from_idx, to_idx, eval_ctx);
      break;
    case VEC_UNIFORM_CONST:
      convert_continuous_to_uniform(expr, from_idx, 0, eval_ctx);
      break;
    default:
      LOG_INFO("get wrong vector format", K(dst_fmt));
      break;
  }
}

void ObVectorsResultHolder::ObColResultHolder::convert_continuous_to_fixed(ObFixedLengthBase &vec, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const
{
  uint32_t offset = offsets_[from_idx];
  uint32_t len = offsets_[from_idx + 1] - offsets_[from_idx];
  const char *ptr = continuous_data_ + offset;
  restore_bitmap_null_base_single_row(vec, from_idx, to_idx, eval_ctx);
  if (!vec.is_null(from_idx)) {
    MEMCPY(expr_->get_rev_buf(eval_ctx) + to_idx * len_, ptr, len);
  }
}
void ObVectorsResultHolder::ObColResultHolder::convert_continuous_to_discrete(ObDiscreteBase &vec, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const
{
  uint32_t offset = offsets_[from_idx];
  uint32_t len = offsets_[from_idx + 1] - offsets_[from_idx];
  const char *ptr = continuous_data_ + offset;
  restore_bitmap_null_base_single_row(vec, from_idx, to_idx, eval_ctx);
  MEMCPY(expr_->get_discrete_vector_lens(eval_ctx) + to_idx, &len, sizeof(ObLength));
  MEMCPY(expr_->get_discrete_vector_ptrs(eval_ctx) + to_idx, &ptr, sizeof(char *));
}

void ObVectorsResultHolder::ObColResultHolder::convert_continuous_to_uniform(ObExpr *expr, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const
{
  uint32_t offset = offsets_[from_idx];
  uint32_t len = offsets_[from_idx + 1] - offsets_[from_idx];
  const char *ptr = continuous_data_ + offset;
  ObDatum *datums = expr_->locate_batch_datums(eval_ctx);
  if (nulls_->at(from_idx)) {
    datums[to_idx].set_null();
  } else {
    datums[to_idx].ptr_ = ptr;
    datums[to_idx].pack_ = len;
    datums[to_idx].null_ = false;
  }
}


void ObVectorsResultHolder::ObColResultHolder::restore_uniform_base_single_row(const ObExpr *expr, ObUniformBase &vec,
                              int64_t from_idx, int64_t to_idx,
                              ObEvalCtx &eval_ctx,  bool is_const
                              ) const
{
  bool need_copy_rev_buf = expr->is_fixed_length_data_
                           || ObNumberTC == ob_obj_type_class(expr->datum_meta_.get_type());
  restore_vector_base(vec);
  // vec.set_datums(datums_);
  if (is_const) {
    // MEMCPY(expr->locate_batch_datums(eval_ctx), frame_datums_, sizeof(ObDatum));
    if (datums_ == expr->locate_batch_datums(eval_ctx)) {
      MEMCPY(expr->locate_batch_datums(eval_ctx), frame_datums_, sizeof(ObDatum));
    } else {
      MEMCPY(expr->locate_batch_datums(eval_ctx), datums_, sizeof(ObDatum));
    }
  } else {
    if (datums_ == expr->locate_batch_datums(eval_ctx)) {
      MEMCPY(expr->locate_batch_datums(eval_ctx) + to_idx, frame_datums_ + from_idx, sizeof(ObDatum));
    } else {
      MEMCPY(expr->locate_batch_datums(eval_ctx) + to_idx, datums_ + from_idx, sizeof(ObDatum));
    }
  }
}

void ObVectorsResultHolder::ObColResultHolder::extend_fixed_base_vector(ObFixedLengthBase &vec, int64_t from_idx, int64_t start_dst_idx, int64_t size, ObEvalCtx &eval_ctx) const
{
  for (int64_t k = 0; k < size; k++) {
    int64_t to_idx = start_dst_idx + k;
    restore_fixed_base_single_row(vec, from_idx, to_idx, eval_ctx);
  }
}

void ObVectorsResultHolder::ObColResultHolder::extend_discrete_base_vector(ObDiscreteBase &vec, int64_t from_idx, int64_t start_dst_idx, int64_t size, ObEvalCtx &eval_ctx) const
{
  for (int64_t k = 0; k < size; k++) {
    int64_t to_idx = start_dst_idx + k;
    restore_discrete_base_single_row(vec, from_idx, to_idx, eval_ctx);
  }
}

void ObVectorsResultHolder::ObColResultHolder::extend_uniform_base_vector(const ObExpr *expr, ObUniformBase &vec, int64_t from_idx, int64_t start_dst_idx, int64_t size, ObEvalCtx &eval_ctx,  bool is_const) const
{
  for (int64_t k = 0; k < size; k++) {
    int64_t to_idx = start_dst_idx + k;
    restore_uniform_base_single_row(expr, vec, from_idx, to_idx, eval_ctx, is_const);
  }
}

void ObVectorsResultHolder::ObColResultHolder::extend_continuous_base_vector(ObExpr *expr, int64_t from_idx, int64_t start_dst_idx, int64_t size, VectorFormat dst_fmt, ObEvalCtx &eval_ctx) const
{
  for (int64_t k = 0; k < size; k++) {
    int64_t to_idx = start_dst_idx + k;
    restore_continuous_base_single_row(expr, from_idx, to_idx, dst_fmt, eval_ctx);
  }
}

int ObVectorsResultHolder::init(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    // do nothing
  } else if (exprs.count() == 0) {
    // do nothing
  } else {
    exprs_ = &exprs;
    eval_ctx_ = &eval_ctx;
    int64_t batch_size = eval_ctx.max_batch_size_;
    ObIAllocator &allocator = (tmp_alloc_ != nullptr ? *tmp_alloc_ : eval_ctx_->exec_ctx_.get_allocator());
    if (OB_ISNULL(backup_cols_ = static_cast<ObColResultHolder *>(
                    allocator.alloc(sizeof(ObColResultHolder) * exprs.count())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc ptrs", K(ret));
    } else {
      for (int64_t i = 0; i < exprs.count(); ++i) {
        new (&backup_cols_[i]) ObColResultHolder(eval_ctx.max_batch_size_, exprs.at(i));
        if (exprs.at(i)->is_nested_expr() && !is_uniform_format(exprs_->at(i)->get_format(*eval_ctx_))) {
          backup_cols_[i].expr_attrs_ = exprs.at(i)->attrs_;
          backup_cols_[i].attrs_cnt_ = exprs.at(i)->attrs_cnt_;
          if (OB_ISNULL(backup_cols_[i].attrs_res_ = static_cast<ObColResultHolder *>
                      (eval_ctx.exec_ctx_.get_allocator().alloc(sizeof(ObColResultHolder)
                                                                * backup_cols_[i].attrs_cnt_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc ptrs", K(ret));
          } else {
            for (uint32_t j = 0; j < backup_cols_[i].attrs_cnt_; ++j) {
              new (&backup_cols_[i].attrs_res_[j]) ObColResultHolder(eval_ctx.max_batch_size_,
                                                                     backup_cols_[i].expr_attrs_[j]);
            }
          }
        }
      }
    }
    inited_ = true;
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::save_nested(ObIAllocator &alloc, const int64_t batch_size,
                                                          ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; OB_SUCC(ret) && i < attrs_cnt_; ++i) {
    if (OB_FAIL(attrs_res_[i].header_.assign(expr_attrs_[i]->get_vector_header(*eval_ctx)))) {
      LOG_WARN("failed to assign vector", K(ret));
    } else if (OB_FAIL(attrs_res_[i].save(alloc, batch_size, eval_ctx))) {
      LOG_WARN("failed to backup col", K(ret), K(i));
    }
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::save(ObIAllocator &alloc, const int64_t batch_size,
                                                   ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  VectorFormat format = header_.format_;
  switch (format) {
    case VEC_FIXED:
      OZ(copy_fixed_base(
          static_cast<const ObFixedLengthBase &>(*expr_->get_vector(*eval_ctx)), alloc, batch_size, *eval_ctx));
      break;
    case VEC_DISCRETE:
      OZ(copy_discrete_base(
          static_cast<const ObDiscreteBase &>(*expr_->get_vector(*eval_ctx)), alloc, batch_size, *eval_ctx));
      break;
    case VEC_CONTINUOUS:
      OZ(copy_continuous_base(
          static_cast<const ObContinuousBase &>(*expr_->get_vector(*eval_ctx)), alloc, batch_size, *eval_ctx));
      break;
    case VEC_UNIFORM:
      OZ(copy_uniform_base(expr_, static_cast<const ObUniformBase &>(*expr_->get_vector(*eval_ctx)),
          false, *eval_ctx, alloc, batch_size));
      break;
    case VEC_UNIFORM_CONST:
      OZ(copy_uniform_base(expr_, static_cast<const ObUniformBase &>(*expr_->get_vector(*eval_ctx)),
          true, *eval_ctx, alloc, batch_size));
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get wrong vector format", K(format), K(ret));
      break;
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::restore_nested(const int64_t saved_size, ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; OB_SUCC(ret) && i < attrs_cnt_; ++i) {
    if (OB_FAIL(expr_attrs_[i]->get_vector_header(*eval_ctx).assign(attrs_res_[i].header_))) {
      LOG_WARN("failed to assign vector", K(ret));
    } else if (OB_FAIL(attrs_res_[i].restore(saved_size, eval_ctx))) {
      LOG_WARN("failed to backup col", K(ret), K(i));
    }
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::restore(const int64_t saved_size, ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  VectorFormat format = header_.format_;
  switch (format) {
    case VEC_FIXED:
      restore_fixed_base(static_cast<ObFixedLengthBase &>(*expr_->get_vector(*eval_ctx)), saved_size, *eval_ctx);
      break;
    case VEC_DISCRETE:
      restore_discrete_base(static_cast<ObDiscreteBase &>(*expr_->get_vector(*eval_ctx)), saved_size, *eval_ctx);
      break;
    case VEC_CONTINUOUS:
      restore_continuous_base(static_cast<ObContinuousBase &>(*expr_->get_vector(*eval_ctx)), saved_size, *eval_ctx);
      break;
    case VEC_UNIFORM:
      restore_uniform_base(expr_, static_cast<ObUniformBase &>(*expr_->get_vector(*eval_ctx)), false, *eval_ctx, saved_size);
      break;
    case VEC_UNIFORM_CONST:
      restore_uniform_base(expr_, static_cast<ObUniformBase &>(*expr_->get_vector(*eval_ctx)), true, *eval_ctx, saved_size);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get wrong vector format", K(format), K(ret));
      break;
  }
  return ret;
}

void ObVectorsResultHolder::destroy()
{
  if (tmp_alloc_ != nullptr && exprs_ != nullptr && backup_cols_ != nullptr) {
    for (int64_t i = 0; i < exprs_->count(); ++i) {
      backup_cols_[i].reset(*tmp_alloc_);
    }
    tmp_alloc_->free(backup_cols_);
    backup_cols_ = nullptr;
  }
}

int ObVectorsResultHolder::save(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_ || 0 == batch_size) {
    saved_size_ = 0;
  } else if (OB_ISNULL(backup_cols_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
  } else {
    saved_size_ = batch_size;
    ObIAllocator &alloc = (tmp_alloc_ != nullptr ? *tmp_alloc_ : eval_ctx_->exec_ctx_.get_allocator());
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_->count(); ++i) {
      if (OB_FAIL(backup_cols_[i].header_.assign(exprs_->at(i)->get_vector_header(*eval_ctx_)))) {
        LOG_WARN("failed to assign vector", K(ret));
      } else if (OB_FAIL(backup_cols_[i].save(alloc, batch_size, eval_ctx_))) {
        LOG_WARN("failed to backup col", K(ret), K(i));
      } else if (exprs_->at(i)->is_nested_expr() && !is_uniform_format(exprs_->at(i)->get_format(*eval_ctx_))
                 && OB_FAIL(backup_cols_[i].save_nested(alloc, batch_size, eval_ctx_))) {
        LOG_WARN("failed to backup nested col", K(ret), K(i));
      }
    }
  }
  saved_ = true;
  return ret;
}
int ObVectorsResultHolder::restore() const
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_ || !saved_ || 0 == saved_size_) {
    // empty expr_: do nothing
  } else if (OB_ISNULL(backup_cols_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_->count(); ++i) {
      if (OB_FAIL(exprs_->at(i)->get_vector_header(*eval_ctx_).assign(backup_cols_[i].header_))) {
        LOG_WARN("failed to assign vector", K(ret));
      } else if (OB_FAIL(backup_cols_[i].restore(saved_size_, eval_ctx_))) {
        LOG_WARN("failed to restore col", K(ret), K(saved_size_), K(i));
      } else if (exprs_->at(i)->is_nested_expr() && !is_uniform_format(exprs_->at(i)->get_format(*eval_ctx_))
                 && OB_FAIL(backup_cols_[i].restore_nested(saved_size_, eval_ctx_))) {
        LOG_WARN("failed to backup nested col", K(ret), K(i));
      }
    }
  }
  return ret;
}

#define CALC_FIXED_COL(vec_tc)                                                                     \
  case (vec_tc): {                                                                                 \
    mem_size += sizeof(RTCType<vec_tc>) * batch_size;                                              \
  } break

template<VectorFormat fmt>
int ObVectorsResultHolder::calc_col_backup_size(ObExpr *expr, int32_t batch_size, int32_t &mem_size)
{
  int ret = OB_SUCCESS;
  mem_size = 0;
  if (fmt == VEC_UNIFORM || fmt == VEC_UNIFORM_CONST) {
    int32_t copy_size = (fmt == VEC_UNIFORM_CONST ? 1 : batch_size);
    mem_size += sizeof(ObDatum) * copy_size;
    bool need_copy_rev_buf = (expr->is_fixed_length_data_
                              || ObNumberTC == ob_obj_type_class(expr->datum_meta_.get_type()));
    if (need_copy_rev_buf) {
      mem_size += expr->res_buf_len_ * copy_size;
    }
  } else {
    // bitmap
    mem_size += sql::ObBitVector::memory_size(batch_size);
    if (fmt == VEC_FIXED) {
      switch(expr->get_vec_value_tc()) {
        LST_DO_CODE(CALC_FIXED_COL, FIXED_VEC_LIST);
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vector class", K(ret), K(expr->get_vec_value_tc()));
        }
      }
    } else if (fmt == VEC_DISCRETE) {
      mem_size += (sizeof(ObLength) + sizeof(char *)) * batch_size;
    } else if (fmt == VEC_CONTINUOUS) {
      mem_size += sizeof(uint32_t) * (batch_size + 1);
    }
  }
  return ret;
}

#define CALC_COL_BACKUP_SIZE(fmt)                                                                  \
  do {                                                                                             \
    if (OB_FAIL(calc_col_backup_size<fmt>(exprs.at(i), eval_ctx.max_batch_size_, col_mem_size))) { \
      LOG_WARN("calc col backup size failed", K(ret));                                             \
    } else {                                                                                       \
      max_col_mem_size = std::max(col_mem_size, max_col_mem_size);                                 \
    }                                                                                              \
  } while (false)

int ObVectorsResultHolder::calc_backup_size(const common::ObIArray<ObExpr *> &exprs,
                                            ObEvalCtx &eval_ctx, int32_t &mem_size)
{
  int ret = OB_SUCCESS;
  mem_size = 0;
  if (OB_LIKELY(exprs.count() > 0)) {
    // ObColResultHolder
    mem_size += sizeof(ObColResultHolder) * exprs.count();
    // VectorHeader
    mem_size += sizeof(VectorHeader) * exprs.count();
    for (int i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      int32_t max_col_mem_size = 0, col_mem_size = 0;
      if (!exprs.at(i)->is_batch_result()) {
        CALC_COL_BACKUP_SIZE(VEC_UNIFORM_CONST);
      } else if (is_fixed_length_vec(exprs.at(i)->get_vec_value_tc())) {
        LST_DO_CODE(CALC_COL_BACKUP_SIZE, VEC_FIXED, VEC_UNIFORM);
      } else {
        LST_DO_CODE(CALC_COL_BACKUP_SIZE, VEC_DISCRETE, VEC_CONTINUOUS, VEC_UNIFORM);
      }
      if (OB_SUCC(ret)) {
        mem_size += max_col_mem_size;
      }
    }
  }
  return ret;
}

#undef CALC_FIXED_COL
#undef CALC_COL_BACKUP_SIZE

int ObVectorsResultHolder::drive_row_extended(int64_t from_idx, int64_t start_dst_idx, int64_t size)
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_ || !saved_ || 0 == saved_size_) {
    // empty expr_: do nothing
  } else if (OB_ISNULL(backup_cols_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_->count(); ++i) {
      VectorFormat backup_format = backup_cols_[i].header_.format_;
      LOG_TRACE("drive row extended for NLJ_VEC_2, and backup format is", K(i), K(backup_format));
      VectorFormat extend_format = get_single_row_restore_format(backup_cols_[i].header_.get_format(), exprs_->at(i));
      if (OB_FAIL(exprs_->at(i)->init_vector(*eval_ctx_, extend_format, eval_ctx_->max_batch_size_))) {
        LOG_WARN("failed to init vector for backup expr", K(i), K(backup_cols_[i].header_.get_format()), K(ret));
      } else if (exprs_->at(i)->is_nested_expr() && !is_uniform_format(backup_format)
                 && OB_FAIL(backup_cols_[i].extend_nested_rows(*exprs_->at(i), *eval_ctx_, extend_format, from_idx, start_dst_idx, size))) {
        LOG_WARN("failed to restore nested single row", K(ret), K(i), K(backup_cols_[i].header_.get_format()));
      } else {
        switch(backup_format) {
          case VEC_FIXED:
            backup_cols_[i].extend_fixed_base_vector(static_cast<ObFixedLengthBase &>(*exprs_->at(i)->get_vector(*eval_ctx_)),
                                                     from_idx, start_dst_idx, size, *eval_ctx_);
            break;
          case VEC_DISCRETE:
            backup_cols_[i].extend_discrete_base_vector(static_cast<ObDiscreteBase &>(*exprs_->at(i)->get_vector(*eval_ctx_)),
                                                     from_idx, start_dst_idx, size, *eval_ctx_);
            break;
          case VEC_CONTINUOUS:
            backup_cols_[i].extend_continuous_base_vector(exprs_->at(i), from_idx, start_dst_idx, size, extend_format, *eval_ctx_);
            break;
          case VEC_UNIFORM:
            backup_cols_[i].extend_uniform_base_vector(exprs_->at(i), static_cast<ObUniformBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)), from_idx, start_dst_idx, size,
                                              *eval_ctx_, false);
            break;
          case VEC_UNIFORM_CONST:
            backup_cols_[i].extend_uniform_base_vector(exprs_->at(i), static_cast<ObUniformBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)), from_idx, start_dst_idx, size,
                                              *eval_ctx_, true);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get wrong vector format", K(backup_format), K(ret));
            break;
        }
      }
    }
  }
  return ret;
}

int ObVectorsResultHolder::restore_single_row(int64_t from_idx, int64_t to_idx) const
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_ || !saved_ || 0 == saved_size_) {
    // empty expr_: do nothing
  } else if (OB_ISNULL(backup_cols_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_->count(); ++i) {
      VectorFormat extend_format = get_single_row_restore_format(backup_cols_[i].header_.get_format(), exprs_->at(i));
      if (OB_FAIL(exprs_->at(i)->init_vector(*eval_ctx_, extend_format, eval_ctx_->max_batch_size_))) {
        LOG_WARN("failed to init vector for backup expr", K(i), K(backup_cols_[i].header_.get_format()), K(ret));
      } else if (exprs_->at(i)->is_nested_expr() && !is_uniform_format(backup_cols_[i].header_.format_) &&
                 OB_FAIL(backup_cols_[i].restore_nested_single_row(*exprs_->at(i), *eval_ctx_, extend_format, from_idx, to_idx))) {
        LOG_WARN("failed to restore nested single row", K(ret), K(i), K(backup_cols_[i].header_.get_format()));
      } else {
        VectorFormat format = backup_cols_[i].header_.format_;
        LOG_TRACE("vector format is", K(format));
        switch (format) {
          case VEC_FIXED:
              backup_cols_[i].restore_fixed_base_single_row(static_cast<ObFixedLengthBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)),
                                              from_idx, to_idx, *eval_ctx_);
              break;
            case VEC_DISCRETE:
              backup_cols_[i].restore_discrete_base_single_row(static_cast<ObDiscreteBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)),
                                              from_idx, to_idx, *eval_ctx_);
              break;
            case VEC_CONTINUOUS:
              backup_cols_[i].restore_continuous_base_single_row(exprs_->at(i), from_idx, to_idx, extend_format, *eval_ctx_);
              break;
            case VEC_UNIFORM:
              backup_cols_[i].restore_uniform_base_single_row(exprs_->at(i), static_cast<ObUniformBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)), from_idx, to_idx,
                                              *eval_ctx_, false);
              break;
            case VEC_UNIFORM_CONST:
              backup_cols_[i].restore_uniform_base_single_row(exprs_->at(i), static_cast<ObUniformBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)), from_idx, to_idx,
                                              *eval_ctx_, true);
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get wrong vector format", K(format), K(ret));
              break;
        }
      }
    }
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::extend_nested_rows(const ObExpr &expr, ObEvalCtx &eval_ctx, const VectorFormat extend_format,
                                                                 int64_t from_idx, int64_t start_dst_idx, int64_t size) const
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; OB_SUCC(ret) && i < attrs_cnt_; ++i) {
    VectorFormat format = attrs_res_[i].header_.format_;
    switch (format) {
      case VEC_FIXED:
        attrs_res_[i].extend_fixed_base_vector(static_cast<ObFixedLengthBase &>(*expr_attrs_[i]->get_vector(eval_ctx)), from_idx,
          start_dst_idx, size, eval_ctx);
        break;
      case VEC_DISCRETE:
        attrs_res_[i].extend_discrete_base_vector(static_cast<ObDiscreteBase &>(*expr_attrs_[i]->get_vector(eval_ctx)), from_idx,
          start_dst_idx, size, eval_ctx);
        break;
      case VEC_CONTINUOUS:
        if (extend_format != VEC_DISCRETE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get wrong vector format", K(extend_format), K(ret));
        } else {
          attrs_res_[i].extend_continuous_base_vector(expr_attrs_[i], from_idx, start_dst_idx, size, extend_format, eval_ctx);
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get wrong vector format", K(format), K(ret));
        break;
    }
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::restore_nested_single_row(const ObExpr &expr, ObEvalCtx &eval_ctx, const VectorFormat extend_format,
                                                                        int64_t from_idx, int64_t to_idx) const
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; OB_SUCC(ret) && i < attrs_cnt_; ++i) {
    VectorFormat format = attrs_res_[i].header_.format_;
    switch (format) {
      case VEC_FIXED:
        attrs_res_[i].restore_fixed_base_single_row(static_cast<ObFixedLengthBase &>
                                                    (*expr_attrs_[i]->get_vector(eval_ctx)), from_idx, to_idx, eval_ctx);
        break;
      case VEC_DISCRETE:
        attrs_res_[i].restore_discrete_base_single_row(static_cast<ObDiscreteBase &>(*expr_attrs_[i]->get_vector(eval_ctx)), from_idx,
          to_idx, eval_ctx);
        break;
      case VEC_CONTINUOUS:
        if (extend_format != VEC_DISCRETE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get wrong vector format", K(extend_format), K(ret));
        } else {
          attrs_res_[i].restore_continuous_base_single_row(expr_attrs_[i], from_idx, to_idx, extend_format, eval_ctx);
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get wrong vector format", K(format), K(ret));
        break;
    }
  }
  return ret;
}

int ObVectorsResultHolder::extend_save(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (size > saved_size_) {
    LOG_INFO("needed backup left extended size", K(size), K(saved_size_));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
