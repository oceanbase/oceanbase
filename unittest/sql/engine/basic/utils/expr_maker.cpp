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

#include "expr_maker.h"

#include <algorithm>
#include <cstring>
#include <type_traits>
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace sql
{

namespace
{
// Evaluates a VEC_UNIFORM_CONST integer expression from frame payload.
// Parameters:
// - expr: expression with res_buf_off_ pointing to int64 payload
// - ctx : eval context that owns the frame memory
// Example:
//   ConstIntExprMaker(7, 4) -> this callback always writes value 7.
auto eval_const_int_value = [](const ObExpr &expr, ObEvalCtx &ctx,
                               const ObBitVector &, const EvalBound &) -> int
{
  const_cast<ObExpr &>(expr).init_vector(ctx, VEC_UNIFORM_CONST, 1);
  const int64_t v = *reinterpret_cast<const int64_t *>(
      ctx.frames_[expr.frame_idx_] + expr.res_buf_off_);
  expr.get_vector(ctx)->set_int(0, v);
  expr.get_eval_info(ctx).set_evaluated(true);
  return OB_SUCCESS;
};

// Shared metadata initializer for generated expressions.
// Parameters:
// - expr      : target expression metadata holder
// - type      : datum object type (e.g. ObIntType, ObVarcharType)
// - value_tc  : vector value type class
// - scale/precision/cs_type/res_buf_len: type-specific attributes
// Example:
//   setup_expr_meta_common(expr, ObIntType, VEC_TC_INTEGER);
void setup_expr_meta_common(ObExpr &expr, common::ObObjType type,
                         common::VecValueTypeClass value_tc, int8_t scale=0,
                         int8_t precision=0, common::ObCollationType cs_type=common::CS_TYPE_BINARY,
                         int64_t res_buf_len = sizeof(int64_t)) {
  expr.datum_meta_.type_ = type;
  expr.datum_meta_.scale_ = scale;
  expr.datum_meta_.precision_ = precision;
  expr.datum_meta_.cs_type_ = cs_type;
  expr.res_buf_len_ = res_buf_len;
  expr.vec_value_tc_ = value_tc;
  expr.basic_funcs_ = common::ObDatumFuncs::get_basic_func(
      expr.datum_meta_.type_, expr.datum_meta_.cs_type_, expr.datum_meta_.scale_,
      lib::is_oracle_mode(), false, expr.datum_meta_.precision_);
  ASSERT_NE(nullptr, expr.basic_funcs_);
}
} // namespace

int64_t ExprMakerImpl::bitmap_count_for_format(common::VectorFormat format)
{
  return (format == common::VEC_UNIFORM || format == common::VEC_UNIFORM_CONST) ? 2 : 3;
}

void ExprMakerImpl::layout_frame(ObExpr &expr, ObEvalCtx &eval_ctx, int64_t frame_idx,
                                 int64_t datum_count, int64_t skip_size,
                                 common::VectorFormat vec_format, int64_t res_buf_size)
{
  int64_t pos = 0;
  expr.frame_idx_ = frame_idx;
  expr.datum_off_ = pos;
  pos += sizeof(ObDatum) * datum_count;
  expr.pvt_skip_off_ = pos;
  pos += ObBitVector::memory_size(skip_size);

  if (vec_format == common::VEC_DISCRETE) {
    expr.len_arr_off_ = pos;
    pos += sizeof(int32_t) * skip_size;
    expr.ptr_arr_off_ = pos;
    pos += sizeof(char *) * skip_size;
    expr.is_fixed_length_data_ = false;
  } else if (vec_format == common::VEC_CONTINUOUS) {
    expr.offset_off_ = pos;
    pos += sizeof(uint32_t) * (skip_size + 1);
    expr.is_fixed_length_data_ = false;
  } else if (vec_format == common::VEC_FIXED) {
    expr.is_fixed_length_data_ = true;
    expr.len_ = static_cast<uint32_t>(res_buf_size / std::max<int64_t>(datum_count, 1));
  }

  expr.vector_header_off_ = pos;
  pos += sizeof(VectorHeader);

  if (vec_format == common::VEC_FIXED || vec_format == common::VEC_DISCRETE
      || vec_format == common::VEC_CONTINUOUS) {
    expr.null_bitmap_off_ = pos;
    pos += ObBitVector::memory_size(skip_size);
  }

  if (vec_format == common::VEC_CONTINUOUS) {
    expr.cont_buf_off_ = pos;
    pos += res_buf_size;
  }

  expr.eval_info_off_ = pos;
  pos += sizeof(ObEvalInfo);
  expr.eval_flags_off_ = pos;
  pos += ObBitVector::memory_size(skip_size);

  if (vec_format != common::VEC_CONTINUOUS) {
    expr.res_buf_off_ = pos;
    pos += res_buf_size;
  }

  memset(eval_ctx.frames_[frame_idx] + expr.eval_info_off_, 0, sizeof(ObEvalInfo));
  memset(eval_ctx.frames_[frame_idx] + expr.eval_flags_off_, 0,
         ObBitVector::memory_size(skip_size));
  memset(eval_ctx.frames_[frame_idx] + expr.pvt_skip_off_, 0,
         ObBitVector::memory_size(skip_size));
  memset(eval_ctx.frames_[frame_idx] + expr.vector_header_off_, 0, sizeof(VectorHeader));
  if (vec_format == common::VEC_FIXED || vec_format == common::VEC_DISCRETE
      || vec_format == common::VEC_CONTINUOUS) {
    memset(eval_ctx.frames_[frame_idx] + expr.null_bitmap_off_, 0,
           ObBitVector::memory_size(skip_size));
  }
}

int64_t ExprMakerImpl::compute_frame_mem_size(int64_t datum_count, int64_t skip_size,
                                              common::VectorFormat vec_format,
                                              int64_t res_buf_size)
{
  int64_t size = sizeof(ObDatum) * datum_count + sizeof(ObEvalInfo)
      + ObBitVector::memory_size(skip_size) * bitmap_count_for_format(vec_format)
      + sizeof(VectorHeader) + res_buf_size;
  if (vec_format == common::VEC_DISCRETE) {
    size += sizeof(int32_t) * skip_size + sizeof(char *) * skip_size;
  } else if (vec_format == common::VEC_CONTINUOUS) {
    size += sizeof(uint32_t) * (skip_size + 1);
  }
  return size;
}

int64_t GenExprMaker::frame_mem_size() const
{
  return ExprMakerImpl::compute_frame_mem_size(
      batch_size_, batch_size_, vec_format_, sizeof(int64_t) * batch_size_);
}

void GenExprMaker::make(ObExpr &expr, ObEvalCtx &eval_ctx)
{
  ASSERT_GE(frame_idx_, 0);
  layout_frame(expr, eval_ctx, frame_idx_, batch_size_, batch_size_, vec_format_,
               sizeof(int64_t) * batch_size_);

  expr.batch_result_ = true;
  expr.batch_idx_mask_ = UINT64_MAX;

  setup_expr_meta(expr);
  ObDatum *datums = expr.locate_batch_datums(eval_ctx);
  for (int64_t i = 0; i < batch_size_; ++i) {
    datums[i].ptr_ = eval_ctx.frames_[frame_idx_] + expr.res_buf_off_ + i * sizeof(int64_t);
  }
}

int64_t SeqIntGenExprMaker::frame_mem_size() const
{
  const int64_t elem_cnt = std::max(batch_size_, max_row_count_);
  return ExprMakerImpl::compute_frame_mem_size(
      elem_cnt, elem_cnt, vec_format_, sizeof(int64_t) * elem_cnt);
}

void SeqIntGenExprMaker::setup_expr_meta(ObExpr &expr)
{
  setup_expr_meta_common(expr, common::ObIntType, common::VEC_TC_INTEGER);
}

void SeqIntGenExprMaker::gen(ObExpr &expr, ObBatchRows &brs, ObEvalCtx &eval_ctx)
{
  if (row_id_ >= max_row_count_) {
    brs.size_ = 0;
    brs.end_ = true;
    return;
  }
  const int64_t batch_size = std::min(brs.size_, max_row_count_ - row_id_);
  brs.size_ = batch_size;
  brs.end_ = false;
  ASSERT_EQ(OB_SUCCESS, expr.init_vector(eval_ctx, vec_format_, batch_size));
  brs.all_rows_active_ = true;
  ObIVector *vec = expr.get_vector(eval_ctx);
  for (int64_t i = 0; i < batch_size; ++i) {
    vec->set_int(i, row_id_ + i);
  }
  // Mark active rows as evaluated for projection stage.
  ObBitVector &evaluated = expr.get_evaluated_flags(eval_ctx);
  evaluated.reset(batch_size_);
  evaluated.set_all(static_cast<int64_t>(0), batch_size);
  expr.get_eval_info(eval_ctx).set_evaluated(true);
  row_id_ += batch_size;
  if (row_id_ >= max_row_count_) {
    brs.end_ = true;
  }
}

template <typename T>
void FixedGenExprMaker<T>::setup_expr_meta(ObExpr &expr)
{
  if constexpr (std::is_same<T, int64_t>::value) {
    setup_expr_meta_common(expr, common::ObIntType, common::VEC_TC_INTEGER);
  } else if constexpr (std::is_same<T, std::string>::value) {
    constexpr int64_t kMaxStrLen = 32;
    setup_expr_meta_common(expr, common::ObVarcharType, common::VEC_TC_STRING,
                           0, 0, common::CS_TYPE_UTF8MB4_BIN, kMaxStrLen);
  } else {
    ASSERT_TRUE(false) << "FixedGenExprMaker only supports int64_t and std::string";
  }
}

template <typename T>
int64_t FixedGenExprMaker<T>::frame_mem_size() const
{
  if constexpr (std::is_same<T, std::string>::value) {
    constexpr int64_t kMaxStrLen = 32;
    EXPECT_TRUE(vec_format_ == common::VEC_CONTINUOUS || vec_format_ == common::VEC_DISCRETE);
    return ExprMakerImpl::compute_frame_mem_size(
        batch_size_, batch_size_, vec_format_, kMaxStrLen * batch_size_);
  } else {
    return GenExprMaker::frame_mem_size();
  }
}

template <typename T>
void FixedGenExprMaker<T>::make(ObExpr &expr, ObEvalCtx &eval_ctx)
{
  ASSERT_GE(frame_idx_, 0);
  if constexpr (std::is_same<T, std::string>::value) {
    constexpr int64_t kMaxStrLen = 32;
    const int64_t res_buf_size = kMaxStrLen * batch_size_;
    EXPECT_TRUE(vec_format_ == common::VEC_CONTINUOUS || vec_format_ == common::VEC_DISCRETE);
    layout_frame(expr, eval_ctx, frame_idx_, batch_size_, batch_size_, vec_format_, res_buf_size);
    expr.batch_result_ = true;
    expr.batch_idx_mask_ = UINT64_MAX;
    setup_expr_meta(expr);
  } else {
    GenExprMaker::make(expr, eval_ctx);
  }
}

template <typename T>
void FixedGenExprMaker<T>::gen(ObExpr &expr, ObBatchRows &brs, ObEvalCtx &eval_ctx)
{
  if (batch_idx_ >= static_cast<int64_t>(values_.size())) {
    brs.size_ = 0;
    brs.end_ = true;
    return;
  }
  std::vector<std::optional<T>> &batch_values = values_[batch_idx_];
  int64_t batch_size = batch_values.size();
  ASSERT_GE(brs.size_, batch_size);
  brs.size_ = batch_size;
  brs.end_ = false;
  brs.all_rows_active_ = true;

  bool has_skip_mask = skip_mask_.size() > 0 && skip_mask_[batch_idx_].size() > 0;
  GenRuntimeCtx rt = {};
  prepare_gen_runtime(expr, brs, eval_ctx, batch_size, rt);

  for (int64_t i = 0; i < batch_size; ++i) {
    if (has_skip_mask && skip_mask_[batch_idx_][i]) {
      set_row_skipped(brs, i);
    } else {
      set_row_value(rt, i, batch_values[i]);
    }
  }
  finish_gen_runtime(expr, brs, eval_ctx, batch_size);
  ++batch_idx_;
}

template <typename T>
void FixedGenExprMaker<T>::prepare_gen_runtime(ObExpr &expr, ObBatchRows &brs,
                                               ObEvalCtx &eval_ctx, int64_t batch_size,
                                               typename FixedGenExprMaker<T>::GenRuntimeCtx &rt) const
{
  const bool use_reserve_buf = std::is_same<T, std::string>::value
      && vec_format_ != common::VEC_CONTINUOUS;
  ASSERT_EQ(OB_SUCCESS, expr.init_vector(eval_ctx, vec_format_, batch_size, use_reserve_buf));
  rt.vec_ = expr.get_vector(eval_ctx);
  rt.datums_ = nullptr;
  rt.res_buf_ = nullptr;
  rt.offsets_ = nullptr;
  rt.cur_ = 0;

  if constexpr (std::is_same<T, int64_t>::value) {
    rt.datums_ = expr.locate_batch_datums(eval_ctx);
    rt.res_buf_ = eval_ctx.frames_[expr.frame_idx_] + expr.res_buf_off_;
    // Initialize all datum slots in this frame to a safe default.
    // This keeps payload metadata valid even when downstream logic reads a
    // batch index outside current brs.size_ but still within max batch size.
    for (int64_t i = 0; i < batch_size_; ++i) {
      rt.datums_[i].ptr_ = rt.res_buf_ + i * sizeof(int64_t);
      rt.datums_[i].set_null();
      *reinterpret_cast<int64_t *>(rt.res_buf_ + i * sizeof(int64_t)) = 0;
    }
  }

  if (vec_format_ == common::VEC_CONTINUOUS) {
    rt.offsets_ = reinterpret_cast<uint32_t *>(
        eval_ctx.frames_[expr.frame_idx_] + expr.offset_off_);
  }
  UNUSED(brs);
}

template <typename T>
void FixedGenExprMaker<T>::set_row_skipped(ObBatchRows &brs, int64_t row_idx) const
{
  brs.skip_->set(row_idx);
  brs.all_rows_active_ = false;
}

template <typename T>
void FixedGenExprMaker<T>::set_row_value(typename FixedGenExprMaker<T>::GenRuntimeCtx &rt,
                                         int64_t row_idx,
                                         const std::optional<T> &opt) const
{
  if (vec_format_ == common::VEC_CONTINUOUS) {
    rt.offsets_[row_idx] = rt.cur_;
  }
  if (!opt.has_value()) {
    rt.vec_->set_null(row_idx);
    if constexpr (std::is_same<T, int64_t>::value) {
      rt.datums_[row_idx].set_null();
    }
  } else {
    if constexpr (std::is_same<T, int64_t>::value) {
      rt.vec_->set_int(row_idx, opt.value());
      *reinterpret_cast<int64_t *>(rt.res_buf_ + row_idx * sizeof(int64_t)) = opt.value();
      rt.datums_[row_idx].ptr_ = rt.res_buf_ + row_idx * sizeof(int64_t);
      rt.datums_[row_idx].pack_ = sizeof(int64_t);
    } else if constexpr (std::is_same<T, std::string>::value) {
      const std::string &val = opt.value();
      rt.vec_->set_string(row_idx, val.data(), static_cast<uint32_t>(val.size()));
      if (vec_format_ == common::VEC_CONTINUOUS) {
        rt.cur_ += static_cast<uint32_t>(val.size());
      }
    }
  }
}

template <typename T>
void FixedGenExprMaker<T>::finish_gen_runtime(ObExpr &expr, const ObBatchRows &brs,
                                              ObEvalCtx &eval_ctx, int64_t batch_size) const
{
  // Mark current active rows evaluated to avoid projection-side re-evaluation
  // that may re-init vectors in another format.
  ObBitVector &evaluated = expr.get_evaluated_flags(eval_ctx);
  evaluated.reset(batch_size_);
  if (brs.all_rows_active_) {
    evaluated.set_all(static_cast<int64_t>(0), batch_size);
  } else {
    for (int64_t i = 0; i < batch_size; ++i) {
      if (!brs.skip_->at(i)) {
        evaluated.set(i);
      }
    }
  }
  expr.get_eval_info(eval_ctx).set_evaluated(true);
}

template <typename T>
void AllAtOnceGenExprMaker<T>::gen(ObExpr &expr, ObBatchRows &brs, ObEvalCtx &eval_ctx)
{
  if (cursor_ >= static_cast<int64_t>(values_.size())) {
    brs.size_ = 0;
    brs.end_ = true;
    return;
  }
  const int64_t remain = static_cast<int64_t>(values_.size()) - cursor_;
  const int64_t batch_size = std::min(brs.size_, remain);
  brs.size_ = batch_size;
  brs.end_ = false;
  brs.all_rows_active_ = true;

  const bool has_skip_mask = !skip_mask_.empty();
  typename FixedGenExprMaker<T>::GenRuntimeCtx rt = {};
  this->prepare_gen_runtime(expr, brs, eval_ctx, batch_size, rt);

  for (int64_t i = 0; i < batch_size; ++i) {
    const int64_t value_idx = cursor_ + i;
    if (has_skip_mask && skip_mask_[value_idx]) {
      this->set_row_skipped(brs, i);
    } else {
      this->set_row_value(rt, i, std::optional<T>(values_[value_idx]));
    }
  }
  this->finish_gen_runtime(expr, brs, eval_ctx, batch_size);
  cursor_ += batch_size;
  if (cursor_ >= static_cast<int64_t>(values_.size())) {
    brs.end_ = true;
  }
}

int64_t ConstIntExprMaker::frame_mem_size() const
{
  return ExprMakerImpl::compute_frame_mem_size(
      1, batch_size_, vec_format_, sizeof(int64_t));
}

void ConstIntExprMaker::make(ObExpr &expr, ObEvalCtx &eval_ctx)
{
  ASSERT_GE(frame_idx_, 0);
  layout_frame(expr, eval_ctx, frame_idx_, 1, batch_size_, vec_format_, sizeof(int64_t));

  expr.batch_result_ = false;
  expr.batch_idx_mask_ = 0;
  setup_expr_meta_common(expr, common::ObIntType, common::VEC_TC_INTEGER);
  expr.eval_vector_func_ = eval_const_int_value;
  expr.eval_batch_func_ = expr_default_eval_batch_func;

  ObDatum *datum = expr.locate_batch_datums(eval_ctx);
  char *val_slot = eval_ctx.frames_[frame_idx_] + expr.res_buf_off_;
  *reinterpret_cast<int64_t *>(val_slot) = value_;
  datum[0].ptr_ = val_slot;
}

template class FixedGenExprMaker<int64_t>;
template class FixedGenExprMaker<std::string>;
template class AllAtOnceGenExprMaker<int64_t>;
template class AllAtOnceGenExprMaker<std::string>;

} // namespace sql
} // namespace oceanbase
