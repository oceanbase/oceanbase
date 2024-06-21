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
#include "lib/container/ob_se_array_iterator.h"
#include "share/vector/ob_uniform_base.h"
#include "share/vector/ob_continuous_base.h"
#include "share/vector/ob_discrete_base.h"
#include "share/vector/ob_fixed_length_base.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

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
  } else if (nullptr == data_) {
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
  bool need_copy_rev_buf = expr->is_fixed_length_data_
                           || ObNumberTC == ob_obj_type_class(expr->datum_meta_.get_type());
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
  bool need_copy_rev_buf = expr->is_fixed_length_data_
                           || ObNumberTC == ob_obj_type_class(expr->datum_meta_.get_type());
  int64_t copy_size = is_const ? 1 : max_row_cnt_;
  restore_vector_base(vec);
  vec.set_datums(datums_);
  MEMCPY(expr->locate_batch_datums(eval_ctx), frame_datums_, sizeof(ObDatum) * copy_size);
  if (need_copy_rev_buf) {
    MEMCPY(expr->get_rev_buf(eval_ctx), frame_data_, len_ * copy_size);
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
      }
    }
    inited_ = true;
  }
  return ret;
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
      } else {
        VectorFormat format = backup_cols_[i].header_.format_;
        switch (format) {
          case VEC_FIXED:
            OZ (backup_cols_[i].copy_fixed_base(static_cast<const ObFixedLengthBase &>
                                            (*exprs_->at(i)->get_vector(*eval_ctx_)),
                                            alloc, batch_size, *eval_ctx_));
            break;
          case VEC_DISCRETE:
            OZ (backup_cols_[i].copy_discrete_base(static_cast<const ObDiscreteBase &>
                                            (*exprs_->at(i)->get_vector(*eval_ctx_)),
                                            alloc, batch_size, *eval_ctx_));
            break;
          case VEC_CONTINUOUS:
            OZ (backup_cols_[i].copy_continuous_base(static_cast<const ObContinuousBase &>
                                            (*exprs_->at(i)->get_vector(*eval_ctx_)),
                                            alloc, batch_size, *eval_ctx_));
            break;
          case VEC_UNIFORM:
            OZ (backup_cols_[i].copy_uniform_base(exprs_->at(i), static_cast<const ObUniformBase &>
                                            (*exprs_->at(i)->get_vector(*eval_ctx_)), false,
                                            *eval_ctx_, alloc, batch_size));
            break;
          case VEC_UNIFORM_CONST:
            OZ (backup_cols_[i].copy_uniform_base(exprs_->at(i), static_cast<const ObUniformBase &>
                                            (*exprs_->at(i)->get_vector(*eval_ctx_)), true,
                                            *eval_ctx_, alloc, batch_size));
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get wrong vector format", K(format), K(ret));
            break;
        }
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
      } else {
        VectorFormat format = backup_cols_[i].header_.format_;
        switch (format) {
          case VEC_FIXED:
            backup_cols_[i].restore_fixed_base(static_cast<ObFixedLengthBase &>
                                            (*exprs_->at(i)->get_vector(*eval_ctx_)),
                                            saved_size_, *eval_ctx_);
            break;
          case VEC_DISCRETE:
            backup_cols_[i].restore_discrete_base(static_cast<ObDiscreteBase &>
                                            (*exprs_->at(i)->get_vector(*eval_ctx_)),
                                            saved_size_, *eval_ctx_);
            break;
          case VEC_CONTINUOUS:
            backup_cols_[i].restore_continuous_base(static_cast<ObContinuousBase &>
                                            (*exprs_->at(i)->get_vector(*eval_ctx_)),
                                            saved_size_, *eval_ctx_);
            break;
          case VEC_UNIFORM:
            backup_cols_[i].restore_uniform_base(exprs_->at(i), static_cast<ObUniformBase &>
                                            (*exprs_->at(i)->get_vector(*eval_ctx_)), false,
                                            *eval_ctx_, saved_size_);
            break;
          case VEC_UNIFORM_CONST:
            backup_cols_[i].restore_uniform_base(exprs_->at(i), static_cast<ObUniformBase &>
                                            (*exprs_->at(i)->get_vector(*eval_ctx_)), true,
                                            *eval_ctx_, saved_size_);
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

} // end namespace sql
} // end namespace oceanbase
