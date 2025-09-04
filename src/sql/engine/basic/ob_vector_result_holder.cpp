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

void ObVectorsResultHolder::ObColResultHolder::restore_fixed_base_single_row(ObFixedLengthBase &vec,
                                                                             int64_t from_idx,
                                                                             int64_t to_idx) const
{
  vec.append_rows_multiple_times(
      (vec.get_nulls() == nulls_) ? to_bit_vector(frame_nulls_) : nulls_,
      vec.get_data() == data_ ? frame_data_ : data_, len_, from_idx,
      from_idx + 1, 1, to_idx);
}

void ObVectorsResultHolder::ObColResultHolder::restore_discrete_base_single_row(ObDiscreteBase &vec,
                                                                             int64_t from_idx,
                                                                             int64_t to_idx) const
{
  vec.append_rows_multiple_times(
      (vec.get_nulls() == nulls_) ? to_bit_vector(frame_nulls_) : nulls_,
      vec.get_ptrs() == ptrs_ ? frame_ptrs_ : ptrs_,
      vec.get_lens() == lens_ ? frame_lens_ : lens_, from_idx, from_idx + 1, 1,
      to_idx);
}

void ObVectorsResultHolder::ObColResultHolder::restore_continuous_base_single_row(ObVectorBase &vec, int64_t from_idx,
                                                                                  int64_t to_idx, VectorFormat dst_fmt) const
{
  switch(dst_fmt) {
    case VEC_FIXED:
      convert_continuous_to_fixed(static_cast<ObFixedLengthBase &>(vec), from_idx, to_idx);
      break;
    case VEC_DISCRETE:
      convert_continuous_to_discrete(static_cast<ObDiscreteBase &>(vec), from_idx, to_idx);
      break;
    case VEC_UNIFORM:
      convert_continuous_to_uniform(static_cast<ObUniformBase &>(vec), from_idx, to_idx);
      break;
    case VEC_UNIFORM_CONST:
      convert_continuous_to_uniform(static_cast<ObUniformBase &>(vec), from_idx, 0);
      break;
    default:
      LOG_INFO("get wrong vector format", K(dst_fmt));
      break;
  }
}

void ObVectorsResultHolder::ObColResultHolder::convert_continuous_to_fixed(ObFixedLengthBase &vec,
                                                                           int64_t from_idx,
                                                                           int64_t to_idx) const
{
  uint32_t offset = offsets_[from_idx];
  uint32_t len = offsets_[from_idx + 1] - offsets_[from_idx];
  char *ptr = continuous_data_ + offset;
  uint64_t const_skip = 0;
  ObBitVector *skip = to_bit_vector(&const_skip);
  bool is_null = ((vec.get_nulls() == nulls_) ? frame_nulls_ : nulls_)->at(from_idx);
  if (is_null) {
    skip->set(0);
  }
  vec.append_rows_multiple_times(
      skip, ptr,
      len, 0, 1, 1, to_idx);
}

void ObVectorsResultHolder::ObColResultHolder::convert_continuous_to_discrete(ObDiscreteBase &vec,
                                                                              int64_t from_idx,
                                                                              int64_t to_idx) const
{
  int32_t offset = offsets_[from_idx];
  int32_t len = offsets_[from_idx + 1] - offsets_[from_idx];
  char *ptr = continuous_data_ + offset;
  uint64_t const_skip = 0;
  ObBitVector *skip = to_bit_vector(&const_skip);
  bool is_null = ((vec.get_nulls() == nulls_) ? frame_nulls_ : nulls_)->at(from_idx);
  if (is_null) {
    skip->set(0);
  }
  vec.append_rows_multiple_times(
      skip, &ptr,
      &len, 0, 1, 1, to_idx);
}

void ObVectorsResultHolder::ObColResultHolder::convert_continuous_to_uniform(ObUniformBase &vec,
                                                                             int64_t from_idx,
                                                                             int64_t to_idx) const
{
  uint32_t offset = offsets_[from_idx];
  uint32_t len = offsets_[from_idx + 1] - offsets_[from_idx];
  const char *ptr = continuous_data_ + offset;
  ObDatum datum(ptr, len, frame_nulls_->at(from_idx));
  vec.append_rows_multiple_times(&datum, 0, 1, 1, to_idx);
}

void ObVectorsResultHolder::ObColResultHolder::restore_uniform_base_single_row(ObUniformBase &vec,
                                                                               int64_t from_idx,
                                                                               int64_t to_idx,
                                                                               bool is_const) const
{
  if (is_const) {
    vec.append_rows_multiple_times(
        vec.get_datums() == datums_ ? frame_datums_ : datums_, 0, 1, 1, 0);
  } else {
    vec.append_rows_multiple_times(vec.get_datums() == datums_ ? frame_datums_
                                                               : datums_,
                                   from_idx, from_idx + 1, 1, to_idx);
  }
}

VectorFormat ObVectorsResultHolder::ObColResultHolder::get_extend_vec_format() const
{
  return header_.format_ == VEC_CONTINUOUS ? expr_->get_default_res_format() : header_.format_;
}

void ObVectorsResultHolder::ObColResultHolder::extend_fixed_base_vector(ObFixedLengthBase &vec,
                                                                        const int64_t src_start_idx,
                                                                        const int64_t srt_end_idx,
                                                                        const int64_t size,
                                                                        const int64_t start_dst_idx) const
{
  vec.append_rows_multiple_times(
      (vec.get_nulls() == nulls_) ? frame_nulls_ : nulls_,
      vec.get_data() == data_ ? frame_data_ : data_, len_, src_start_idx,
      srt_end_idx, size, start_dst_idx);
}

void ObVectorsResultHolder::ObColResultHolder::extend_discrete_base_vector(ObDiscreteBase &vec,
                                                                           const int64_t src_start_idx,
                                                                           const int64_t srt_end_idx,
                                                                           const int64_t size,
                                                                           const int64_t start_dst_idx) const
{
  vec.append_rows_multiple_times(
      (vec.get_nulls() == nulls_) ? frame_nulls_ : nulls_,
      vec.get_ptrs() == ptrs_ ? frame_ptrs_ : ptrs_,
      vec.get_lens() == lens_ ? frame_lens_ : lens_, src_start_idx, srt_end_idx,
      size, start_dst_idx);
}

void ObVectorsResultHolder::ObColResultHolder::extend_uniform_base_vector(ObUniformBase &vec,
                                                                          const int64_t src_start_idx,
                                                                          const int64_t srt_end_idx,
                                                                          const int64_t size,
                                                                          const int64_t start_dst_idx,
                                                                          const bool is_const) const
{
  if (is_const) {
    vec.append_rows_multiple_times(
        vec.get_datums() == datums_ ? frame_datums_ : datums_, 0,
        1, 1, 0);
  } else {
    vec.append_rows_multiple_times(
        vec.get_datums() == datums_ ? frame_datums_ : datums_, src_start_idx,
        srt_end_idx, size, start_dst_idx);
  }
}

void ObVectorsResultHolder::ObColResultHolder::extend_continuous_base_vector(ObVectorBase &vec,
                                                                             const int64_t src_start_idx,
                                                                             const int64_t srt_end_idx,
                                                                             const int64_t size,
                                                                             const int64_t start_dst_idx,
                                                                             const VectorFormat dst_fmt) const
{
  const int64_t interval = srt_end_idx - src_start_idx;
  if (interval > 1) {
    for (int64_t i = 0; i < size * interval; ++i) {
      restore_continuous_base_single_row(vec, src_start_idx + i % interval, start_dst_idx + i, dst_fmt);
    }
  } else {
    for (int64_t i = 0; i < size; ++i) {
      restore_continuous_base_single_row(vec, src_start_idx, start_dst_idx + i, dst_fmt);
    }
  }
}

int ObVectorsResultHolder::inner_init(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx, const int64_t max_row_cnt)
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
        new (&backup_cols_[i]) ObColResultHolder(max_row_cnt, exprs.at(i));
      }
    }
    inited_ = true;
  }
  return ret;
}

int ObVectorsResultHolder::init(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx)
{
  return inner_init(exprs, eval_ctx, eval_ctx.max_batch_size_);
}

int ObVectorsResultHolder::init_for_actual_rows(const common::ObIArray<ObExpr *> &exprs,
                                                const int32_t batch_size, ObEvalCtx &eval_ctx)
{
  return inner_init(exprs, eval_ctx, batch_size);
}

int ObVectorsResultHolder::ObColResultHolder::save(ObIAllocator &alloc, const int64_t batch_size,
                                                   ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  VectorFormat format = header_.format_;
  if (OB_UNLIKELY(batch_size > max_row_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid batch size", K(ret), K(max_row_cnt_), K(batch_size));
  } else {
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

int ObVectorsResultHolder::rows_extend(int64_t src_start_idx, int64_t src_end_idx, int64_t start_dst_idx, int64_t size)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs_->count(); ++i) {
    VectorFormat backup_format = backup_cols_[i].header_.format_;
    VectorFormat extend_format = backup_cols_[i].get_extend_vec_format();
    LOG_DEBUG("drive row extended for NLJ_VEC_2, and backup format is", K(i), K(backup_format), K(extend_format));
    switch(backup_format) {
      case VEC_FIXED:
        backup_cols_[i].extend_fixed_base_vector(static_cast<ObFixedLengthBase &>(*exprs_->at(i)->get_vector(*eval_ctx_)),
                                                  src_start_idx, src_end_idx, size, start_dst_idx);
        break;
      case VEC_DISCRETE:
        backup_cols_[i].extend_discrete_base_vector(static_cast<ObDiscreteBase &>(*exprs_->at(i)->get_vector(*eval_ctx_)),
                                                  src_start_idx, src_end_idx, size, start_dst_idx);
        break;
      case VEC_CONTINUOUS:
        backup_cols_[i].extend_continuous_base_vector(static_cast<ObVectorBase &>(*exprs_->at(i)->get_vector(*eval_ctx_)),
                                                  src_start_idx, src_end_idx, size, start_dst_idx,
                                                  extend_format);
        break;
      case VEC_UNIFORM:
        backup_cols_[i].extend_uniform_base_vector(static_cast<ObUniformBase &>
                                          (*exprs_->at(i)->get_vector(*eval_ctx_)), src_start_idx, src_end_idx, size, start_dst_idx, false);
        break;
      case VEC_UNIFORM_CONST:
        backup_cols_[i].extend_uniform_base_vector(static_cast<ObUniformBase &>
                                          (*exprs_->at(i)->get_vector(*eval_ctx_)), src_start_idx, src_end_idx, size, start_dst_idx, true);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get wrong vector format", K(backup_format), K(backup_format), K(ret));
        break;
    }
  }
  return ret;
}

int ObVectorsResultHolder::driver_row_extend(int64_t from_idx, int64_t start_dst_idx, int64_t times)
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_ || !saved_ || 0 == saved_size_) {
    // empty expr_: do nothing
  } else if (OB_ISNULL(backup_cols_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_->count(); ++i) {
      VectorFormat format = backup_cols_[i].get_extend_vec_format();
      LOG_DEBUG("drive row extended for NLJ_VEC_2, and backup format is", K(i), K(format));
      if (OB_FAIL(exprs_->at(i)->init_vector(*eval_ctx_, format, eval_ctx_->max_batch_size_))) {
        LOG_WARN("failed to init vector for backup expr", K(i), K(backup_cols_[i].header_.get_format()), K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(rows_extend(from_idx, from_idx + 1, start_dst_idx, times))) {
      LOG_WARN("failed to extend driver side row", K(ret));
    }
  }
  return ret;
}

int ObVectorsResultHolder::driver_rows_extend(const ObBatchRows *driver_brs,
                                              int64_t &from_idx,
                                              int64_t &extend_rows_cnt,
                                              int64_t times)
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_ || !saved_ || 0 == saved_size_) {
    // empty exprs
    if (driver_brs->all_rows_active_) {
      extend_rows_cnt = std::min(extend_rows_cnt, driver_brs->size_ - from_idx);
      from_idx += extend_rows_cnt;
    } else {
      int64_t i = 0;
      for (; OB_SUCC(ret) && i < extend_rows_cnt && from_idx < driver_brs->size_; from_idx++) {
        if (driver_brs->skip_->exist(from_idx)) {
        } else {
          i++;
        }
      }
      extend_rows_cnt = i; //real extend rows cnt
    }
  } else if (OB_ISNULL(backup_cols_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_->count(); ++i) {
      VectorFormat format = backup_cols_[i].get_extend_vec_format();
      LOG_DEBUG("drive row extended for NLJ_VEC_2, and backup format is", K(i), K(format));
      if (OB_FAIL(exprs_->at(i)->init_vector(*eval_ctx_, format, eval_ctx_->max_batch_size_))) {
        LOG_WARN("failed to init vector for backup expr", K(i), K(backup_cols_[i].header_.get_format()), K(ret));
      }
    }
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < extend_rows_cnt && from_idx < driver_brs->size_; from_idx++) {
      if (!driver_brs->all_rows_active_ && driver_brs->skip_->exist(from_idx)) {
      } else {
        OZ(rows_extend(from_idx, from_idx + 1, i * times, times));
        i++;
      }
    }
    extend_rows_cnt = i; //real extend rows cnt
  }
  return ret;
}

int ObVectorsResultHolder::driven_rows_extend(int64_t from_idx,
                                              int64_t end_idx,
                                              int64_t start_dst_idx,
                                              int64_t times)
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_ || !saved_ || 0 == saved_size_) {
    // empty expr_: do nothing
  } else if (OB_ISNULL(backup_cols_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_->count(); ++i) {
      VectorFormat format = backup_cols_[i].get_extend_vec_format();
      LOG_DEBUG("drive row extended for NLJ_VEC_2, and backup format is", K(i), K(format));
      if (OB_FAIL(exprs_->at(i)->init_vector(*eval_ctx_, format, eval_ctx_->max_batch_size_))) {
        LOG_WARN("failed to init vector for backup expr", K(i), K(format), K(ret));
      }
    }
    OZ(rows_extend(from_idx, end_idx, start_dst_idx, times));
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
      VectorFormat backup_format = backup_cols_[i].header_.format_;
      VectorFormat extend_format = backup_cols_[i].get_extend_vec_format();
      if (OB_FAIL(exprs_->at(i)->init_vector(*eval_ctx_, extend_format, eval_ctx_->max_batch_size_))) {
        LOG_WARN("failed to init vector for backup expr", K(i), K(backup_format), K(extend_format), K(ret));
      } else {
        LOG_DEBUG("vector format is", K(extend_format));
        switch (backup_format) {
          case VEC_FIXED:
              backup_cols_[i].restore_fixed_base_single_row(static_cast<ObFixedLengthBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)),
                                              from_idx, to_idx);
              break;
            case VEC_DISCRETE:
              backup_cols_[i].restore_discrete_base_single_row(static_cast<ObDiscreteBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)),
                                              from_idx, to_idx);
              break;
            case VEC_CONTINUOUS:
              backup_cols_[i].restore_continuous_base_single_row(static_cast<ObVectorBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)), from_idx, to_idx,
                                              extend_format);
              break;
            case VEC_UNIFORM:
              backup_cols_[i].restore_uniform_base_single_row(static_cast<ObUniformBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)), from_idx, to_idx, false);
              break;
            case VEC_UNIFORM_CONST:
              backup_cols_[i].restore_uniform_base_single_row(static_cast<ObUniformBase &>
                                              (*exprs_->at(i)->get_vector(*eval_ctx_)), from_idx, to_idx, true);
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get wrong vector format", K(extend_format), K(backup_format), K(ret));
              break;
        }
      }
    }
  }
  return ret;
}

static int64_t bit_xor(const uint64_t &l, const uint64_t &r) {
  return (l ^ r);
}

int ObVectorsResultHolder::ObColResultHolder::check_bitmap_null_base(const ObBitmapNullVectorBase &vec,
                                                                     const int64_t batch_size,
                                                                     ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == frame_nulls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame_nulls_ is null", K(ret));
  } else if (has_null_ != vec.has_null() || nulls_ != vec.get_nulls()){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has_null or nulls is not match", K(has_null_), K(vec.has_null()), K(ret));
  } else if (!ObBitVector::bit_op_zero(*frame_nulls_, expr_->get_nulls(eval_ctx), batch_size, bit_xor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame_nulls is not match", K(ret));
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::check_uniform_base(const ObUniformBase &vec, const int64_t batch_size,
                                                                 bool is_const, ObEvalCtx &eval_ctx, const ObBitVector &skip)
{
  int ret = OB_SUCCESS;
  int check_size = is_const ? 1 : batch_size;
  const ObDatum *datums = vec.get_datums();
  switch(header_.get_format()) {
    case VEC_FIXED:{
      char *ptr = data_;
      for (int i = 0; OB_SUCC(ret) && i < check_size; i++) {
        if (skip.at(i)) {
        } else if (datums[i].null_) {
          if (!nulls_->at(i)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("datum null is not match", K(i), K(datums[i]), K(nulls_->at(i)), K(ret));
          }
        } else if (nulls_->at(i)
                   || datums[i].ptr_ != ptr
                   || datums[i].len_ != len_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datum is not match", K(i), K(datums[i]), KP(ptr), K(len_), K(nulls_->at(i)), K(ret));
        }
        ptr += len_;
      }
      break;
    }
    case VEC_DISCRETE: {
      for (int i = 0; OB_SUCC(ret) && i < check_size; i++) {
        if (skip.at(i)) {
        } else if (datums[i].null_) {
          if (!nulls_->at(i)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("datum null is not match", K(i), K(datums[i]), K(nulls_->at(i)), K(ret));
          }
        } else if (nulls_->at(i)
            ||  datums[i].ptr_ != ptrs_[i]
            || (datums[i].pack_ != lens_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datum is not match", K(i), KP(expr_), K(datums[i]), KP(ptrs_[i]), K(lens_[i]), K(nulls_->at(i)));
        }
      }
      break;
    }
    case VEC_CONTINUOUS: {
      for (int i = 0; OB_SUCC(ret) && i < check_size; i++) {
        if (skip.at(i)) {
        } else if (datums[i].null_) {
          if (!nulls_->at(i)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("datum null is not match", K(i), K(datums[i]), K(nulls_->at(i)), K(ret));
          }
        } else if (nulls_->at(i)
            || datums[i].ptr_ != continuous_data_ + offsets_[i]
            || datums[i].len_ != offsets_[i + 1] - offsets_[i]) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datum is not match", K(i), K(datums[i]), KP(continuous_data_ + offsets_[i]),
                                          K(offsets_[i + 1] - offsets_[i]), K(nulls_->at(i)), K(ret));
        }
      }
      break;
    }
    case VEC_UNIFORM:
    case VEC_UNIFORM_CONST : {
      if (datums == nullptr) {
      } else if (datums_ != datums) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("datum is not match", K(ret), KP(datums_), KP(datums));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid vector format", K(header_.format_));
    }
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::check_fixed_base(const ObFixedLengthBase &vec, const int64_t batch_size,
                                            ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_bitmap_null_base(static_cast<const ObBitmapNullVectorBase &>(vec), batch_size, eval_ctx))) {
    LOG_WARN("check bitmap null base failed", K(ret));
  } else if (data_ != vec.get_data() || len_ != vec.get_length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data_ptr or len is not match", KP(data_), KP(vec.get_data()), K(len_), K(vec.get_length()), K(ret));
  } else if (0 != MEMCMP(frame_data_, expr_->get_rev_buf(eval_ctx), len_ * batch_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame_data is not match", K(ret));
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::check_discrete_base(const ObDiscreteBase &vec, const int64_t batch_size,
                                               ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_bitmap_null_base(static_cast<const ObBitmapNullVectorBase &>(vec), batch_size, eval_ctx))) {
    LOG_WARN("check bitmap null base failed", K(ret));
  } else if (nullptr == frame_lens_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame_lens_ is null", K(ret));
  } else if (ptrs_ != vec.get_ptrs() || lens_ != vec.get_lens()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptrs_ or lens_ is not match", KP(ptrs_), KP(vec.get_ptrs()), KP(lens_), KP(vec.get_lens()), K(ret));
  } else if (0 != MEMCMP(frame_ptrs_, expr_->get_discrete_vector_ptrs(eval_ctx), sizeof(char *) * batch_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame_ptrs is not match", K(ret));
  } else if (0 != MEMCMP(frame_lens_, expr_->get_discrete_vector_lens(eval_ctx), sizeof(ObLength) * batch_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame_lens is not match", K(ret));
  }
  return ret;
}

int ObVectorsResultHolder::ObColResultHolder::check_continuous_base(const ObContinuousBase &vec, const int64_t batch_size,
                                                 ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_bitmap_null_base(static_cast<const ObBitmapNullVectorBase &>(vec), batch_size, eval_ctx))) {
    LOG_WARN("check bitmap null base failed", K(ret));
  } else if (OB_UNLIKELY(nullptr == frame_offsets_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame_offsets_ is null", K(ret));
  } else if (offsets_ != vec.get_offsets() || continuous_data_ != vec.get_data()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offsets_ or continuous_data_ is not match", KP(offsets_), KP(vec.get_offsets()), KP(continuous_data_),
             KP(vec.get_data()), K(ret));
  } else if (0 != MEMCMP(frame_offsets_,
                         expr_->get_continuous_vector_offsets(eval_ctx),
                         sizeof(uint32_t) * (batch_size + 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("frame_offsets is not match", K(ret));
  }
  return ret;
}

int ObVectorsResultHolder::check_vec_modified(const ObBitVector &skip)
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_ || !saved_ || 0 == saved_size_) {
    // empty expr_: do nothing
  } else if (OB_ISNULL(backup_cols_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_NOT_INIT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_->count(); ++i) {
      VectorFormat org_format = backup_cols_[i].header_.format_;
      VectorFormat cur_format = exprs_->at(i)->get_format(*eval_ctx_);
      if (org_format != cur_format && VEC_UNIFORM != cur_format) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vector format is not match", K(org_format), K(cur_format), K(ret));
      } else {
        switch (cur_format) {
          case VEC_UNIFORM:
            OZ(backup_cols_[i].check_uniform_base(
              static_cast<const ObUniformBase &>(*exprs_->at(i)->get_vector(*eval_ctx_)),
              saved_size_,
              false,
              *eval_ctx_,
              skip
            ));
            break;
          case VEC_UNIFORM_CONST:
            OZ(backup_cols_[i].check_uniform_base(
              static_cast<const ObUniformBase &>(*exprs_->at(i)->get_vector(*eval_ctx_)),
              saved_size_,
              true,
              *eval_ctx_,
              skip
            ));
            break;
          case VEC_FIXED:
            OZ(backup_cols_[i].check_fixed_base(
              static_cast<const ObFixedLengthBase &>(*exprs_->at(i)->get_vector(*eval_ctx_)),
              saved_size_,
              *eval_ctx_
            ));
            break;
          case VEC_DISCRETE:
            OZ(backup_cols_[i].check_discrete_base(
              static_cast<const ObDiscreteBase &>(*exprs_->at(i)->get_vector(*eval_ctx_)),
              saved_size_,
              *eval_ctx_
            ));
            break;
          case VEC_CONTINUOUS:
            OZ(backup_cols_[i].check_continuous_base(
              static_cast<const ObContinuousBase &>(*exprs_->at(i)->get_vector(*eval_ctx_)),
              saved_size_,
              *eval_ctx_
            ));
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get wrong vector format", K(cur_format), K(ret));
            break;
        }
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
