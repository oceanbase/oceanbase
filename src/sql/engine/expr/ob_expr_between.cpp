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

#define USING_LOG_PREFIX SQL_EXE
#include "common/object/ob_obj_compare.h"
#include "sql/engine/expr/ob_expr_between.h"
#include "sql/engine/expr/ob_expr_less_than.h"
#include "sql/engine/expr/ob_expr_less_equal.h"
#include "sql/engine/expr/ob_expr_cmp_func.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/vector/expr_cmp_func.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprBetween::ObExprBetween(ObIAllocator &alloc)
    : ObRelationalExprOperator(alloc, T_OP_BTW, N_BTW, 3)
{
}

int calc_between_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  // left <= val <= right
  int ret = OB_SUCCESS;
  ObDatum *val = NULL;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, val))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (val->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, left))) {
    LOG_WARN("eval arg 1 failed", K(ret));
  } else if (OB_FAIL(expr.args_[2]->eval(ctx, right))) {
    LOG_WARN("eval arg 2 failed", K(ret));
  } else if (left->is_null() && right->is_null()) {
    res_datum.set_null();
  } else {
    bool left_cmp_succ = true;  // is left <= val true or not
    bool right_cmp_succ = true; // is val <= right true or not
    int cmp_ret = 0;
    if (!left->is_null()) {
      if (OB_FAIL((reinterpret_cast<DatumCmpFunc>(expr.inner_functions_[0]))(*left, *val, cmp_ret))) {
        LOG_WARN("compare left failed", K(ret));
      } else {
        left_cmp_succ = cmp_ret <= 0 ? true : false;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (left->is_null() || (left_cmp_succ && !right->is_null())) {
      if (OB_FAIL((reinterpret_cast<DatumCmpFunc>(expr.inner_functions_[1]))(*val, *right, cmp_ret))) {
        LOG_WARN("compare left failed", K(ret));
      } else {
        right_cmp_succ = cmp_ret <= 0 ? true : false;
      }
    }
    if (OB_FAIL(ret)) {
    } else if ((left->is_null() && right_cmp_succ) || (right->is_null() && left_cmp_succ)) {
      res_datum.set_null();
    } else if (left_cmp_succ && right_cmp_succ) {
      res_datum.set_int32(1);
    } else {
      res_datum.set_int32(0);
    }
  }
  return ret;
}

#define BETWEEN_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(                      \
        func_name, stage, l_format, right_vec, res_vec)                  \
switch (l_format) {                                                      \
  case VEC_FIXED: {                                                      \
    ret = func_name<ObFixedLengthBase, right_vec, res_vec, stage>(       \
          expr, ctx, my_skip, bound);                                    \
    break;                                                               \
  }                                                                      \
  case VEC_DISCRETE: {                                                   \
    ret = func_name<ObDiscreteFormat, right_vec, res_vec, stage>(        \
          expr, ctx, my_skip, bound);                                    \
    break;                                                               \
  }                                                                      \
  case VEC_CONTINUOUS: {                                                 \
    ret = func_name<ObContinuousFormat, right_vec, res_vec, stage>(      \
          expr, ctx, my_skip, bound);                                    \
    break;                                                               \
  }                                                                      \
  case VEC_UNIFORM: {                                                    \
    ret = func_name<ObUniformFormat<false>, right_vec, res_vec, stage>(  \
          expr, ctx, my_skip, bound);                                    \
    break;                                                               \
  }                                                                      \
  case VEC_UNIFORM_CONST: {                                              \
    ret = func_name<ObUniformFormat<true>, right_vec, res_vec, stage>(   \
          expr, ctx, my_skip, bound);                                    \
    break;                                                               \
  }                                                                      \
  default: {                                                             \
    ret = func_name<ObVectorBase, right_vec, res_vec, stage>(            \
          expr, ctx, my_skip, bound);                                    \
  }                                                                      \
}

#define BETWEEN_DISPATCH_VECTOR_IN_RIGHT_ARG_FORMAT(                  \
        func_name, stage, l_format, r_format, res_vec)                \
switch (r_format) {                                                   \
  case VEC_FIXED: {                                                   \
    BETWEEN_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(                       \
      func_name, stage, l_format, ObFixedLengthBase, res_vec);        \
    break;                                                            \
  }                                                                   \
  case VEC_DISCRETE: {                                                \
    BETWEEN_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(                       \
      func_name, stage, l_format, ObDiscreteFormat, res_vec);         \
    break;                                                            \
  }                                                                   \
  case VEC_CONTINUOUS: {                                              \
    BETWEEN_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(                       \
      func_name, stage, l_format, ObContinuousFormat, res_vec);       \
    break;                                                            \
  }                                                                   \
  case VEC_UNIFORM: {                                                 \
    BETWEEN_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(                       \
      func_name, stage, l_format, ObUniformFormat<false>, res_vec);   \
    break;                                                            \
  }                                                                   \
  case VEC_UNIFORM_CONST: {                                           \
    BETWEEN_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(                       \
      func_name, stage, l_format, ObUniformFormat<true>, res_vec);    \
    break;                                                            \
  }                                                                   \
  default: {                                                          \
    BETWEEN_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(                       \
      func_name, stage, l_format, ObVectorBase, res_vec);             \
  }                                                                   \
}

#define BETWEEN_DISPATCH_VECTOR_IN_RES_ARG_FORMAT(                    \
        func_name, stage, l_format, r_format)                         \
switch (res_format) {                                                 \
  case VEC_FIXED: {                                                   \
    BETWEEN_DISPATCH_VECTOR_IN_RIGHT_ARG_FORMAT(                      \
      func_name, stage, l_format, r_format, IntegerFixedVec);         \
    break;                                                            \
  }                                                                   \
  case VEC_UNIFORM: {                                                 \
    BETWEEN_DISPATCH_VECTOR_IN_RIGHT_ARG_FORMAT(                      \
      func_name, stage, l_format, r_format, IntegerUniVec);           \
    break;                                                            \
  }                                                                   \
  case VEC_UNIFORM_CONST: {                                           \
    BETWEEN_DISPATCH_VECTOR_IN_RIGHT_ARG_FORMAT(                      \
      func_name, stage, l_format, r_format, IntegerUniCVec);          \
    break;                                                            \
  }                                                                   \
  default: {                                                          \
    BETWEEN_DISPATCH_VECTOR_IN_RIGHT_ARG_FORMAT(                      \
      func_name, stage, l_format, r_format, ObVectorBase);            \
  }                                                                   \
}


int ObExprBetween::eval_between_vector(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const EvalBound &bound)
{
  // left <= val <= right
  int ret = OB_SUCCESS;
  const ObExpr &val_expr = *expr.args_[0];
  const ObExpr &left_expr = *expr.args_[1];
  const ObExpr &right_expr = *expr.args_[2];
  ObBitVector &my_skip = expr.get_pvt_skip(ctx);
  my_skip.deep_copy(skip, bound.start(), bound.end());
  if (OB_FAIL(val_expr.eval_vector(ctx, my_skip, bound))) {
    LOG_WARN("eval left operand failed", K(ret));
  } else if (OB_FAIL(left_expr.eval_vector(ctx, my_skip, bound))) {
    LOG_WARN("eval left operand failed", K(ret));
  } else if (OB_FAIL(right_expr.eval_vector(ctx, my_skip, bound))) {
    LOG_WARN("eval left operand failed", K(ret));
  } else {
    VectorFormat val_format = val_expr.get_format(ctx);
    VectorFormat left_format = left_expr.get_format(ctx);
    VectorFormat right_format = right_expr.get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    BETWEEN_DISPATCH_VECTOR_IN_RES_ARG_FORMAT(
          inner_eval_between_vector, BETWEEN_LEFT,
          left_format, val_format);
    if (OB_FAIL(ret)) {
      LOG_WARN("compare left and val failed", K(ret));
    } else {
      BETWEEN_DISPATCH_VECTOR_IN_RES_ARG_FORMAT(
          inner_eval_between_vector, BETWEEN_RIGHT,
          val_format, right_format);
      if (OB_FAIL(ret)) {
        LOG_WARN("compare val and right failed", K(ret));
      } else {
        ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
        eval_flags.bit_not(skip, bound);
      }
    }
  }
  return ret;
}

int ObExprBetween::cg_expr(ObExprCGCtx &expr_cg_ctx,
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const
{
  // left <= val <= right
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(3 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_) ||
      OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1]) ||
      OB_ISNULL(rt_expr.args_[2]) || OB_ISNULL(expr_cg_ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rt_expr is invalid", K(ret), K(rt_expr.arg_cnt_), KP(rt_expr.args_),
              KP(rt_expr.args_[0]), KP(rt_expr.args_[1]), KP(rt_expr.args_[2]));
  } else {
    DatumCmpFunc cmp_func_1 = NULL;  // left <= val
    DatumCmpFunc cmp_func_2 = NULL;  // val <= right
    const ObDatumMeta &val_meta = rt_expr.args_[0]->datum_meta_;
    const ObDatumMeta &left_meta = rt_expr.args_[1]->datum_meta_;
    const ObDatumMeta &right_meta = rt_expr.args_[2]->datum_meta_;
    const ObCollationType cmp_cs_type =
      raw_expr.get_result_type().get_calc_collation_type();
    const bool has_lob_header1 = rt_expr.args_[0]->obj_meta_.has_lob_header() ||
                                 rt_expr.args_[1]->obj_meta_.has_lob_header();
    const bool has_lob_header2 = rt_expr.args_[0]->obj_meta_.has_lob_header() ||
                                 rt_expr.args_[2]->obj_meta_.has_lob_header();
    if (OB_ISNULL(cmp_func_1 = ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
                                                        left_meta.type_, val_meta.type_,
                                                        left_meta.scale_, val_meta.scale_,
                                                        left_meta.precision_, val_meta.precision_,
                                                        is_oracle_mode(),
                                                        cmp_cs_type,
                                                        has_lob_header1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get_datum_expr_cmp_func failed", K(ret), K(left_meta), K(val_meta),
                K(is_oracle_mode()), K(rt_expr));
    } else if (OB_ISNULL(cmp_func_2 = ObExprCmpFuncsHelper::get_datum_expr_cmp_func(
                                                        val_meta.type_, right_meta.type_,
                                                        val_meta.scale_, right_meta.scale_,
                                                        val_meta.precision_, right_meta.precision_,
                                                        is_oracle_mode(),
                                                        cmp_cs_type,
                                                        has_lob_header2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get_datum_expr_cmp_func failed", K(ret), K(val_meta), K(right_meta),
                K(is_oracle_mode()), K(rt_expr));
    } else {
      rt_expr.eval_func_ = calc_between_expr;
    }
    if (OB_FAIL(ret)) {
    } else if (expr_cg_ctx.session_->use_rich_format()) {
      RowCmpFunc vec_cmp_func_1 = NULL;  // left <= val
      RowCmpFunc vec_cmp_func_2 = NULL;  // val <= right
      if (OB_ISNULL(vec_cmp_func_1 = VectorCmpExprFuncsHelper::get_row_cmp_func(
                                                  left_meta, val_meta))) {
        ret = OB_ERR_UNEXPECTED;
        VecValueTypeClass value_tc = get_vec_value_tc(val_meta.type_, val_meta.scale_, val_meta.precision_);
        VecValueTypeClass left_tc = get_vec_value_tc(left_meta.type_, left_meta.scale_, left_meta.precision_);
        LOG_WARN("The result of get_eval_vector_between_expr_cmp_func(left) is null.",
                  K(ret), K(left_meta), K(val_meta), K(right_meta), K(value_tc), K(left_tc), K(rt_expr));
      } else if (OB_ISNULL(vec_cmp_func_2 = VectorCmpExprFuncsHelper::get_row_cmp_func(
                                                  val_meta, right_meta))) {
        ret = OB_ERR_UNEXPECTED;
        VecValueTypeClass value_tc = get_vec_value_tc(val_meta.type_, val_meta.scale_, val_meta.precision_);
        VecValueTypeClass right_tc = get_vec_value_tc(right_meta.type_, right_meta.scale_, right_meta.precision_);
        LOG_WARN("The result of get_eval_vector_between_expr_cmp_func(right) is null.",
                  K(ret), K(left_meta), K(val_meta), K(right_meta), K(value_tc), K(right_tc), K(rt_expr));
      } else if (OB_ISNULL(rt_expr.inner_functions_ = reinterpret_cast<void**>(
                          expr_cg_ctx.allocator_->alloc(sizeof(DatumCmpFunc) * 2 + sizeof(RowCmpFunc) * 2)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory for inner_functions_ failed", K(ret));
      } else {
        rt_expr.inner_func_cnt_ = 4;
        rt_expr.inner_functions_[0] = reinterpret_cast<void*>(cmp_func_1);
        rt_expr.inner_functions_[1] = reinterpret_cast<void*>(cmp_func_2);
        rt_expr.inner_functions_[2] = reinterpret_cast<void*>(vec_cmp_func_1);
        rt_expr.inner_functions_[3] = reinterpret_cast<void*>(vec_cmp_func_2);
        rt_expr.eval_vector_func_ = eval_between_vector;
      }
    } else {  // not use_rich_format
      if (OB_ISNULL(rt_expr.inner_functions_ = reinterpret_cast<void**>(
                          expr_cg_ctx.allocator_->alloc(sizeof(DatumCmpFunc) * 2)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory for inner_functions_ failed", K(ret));
      } else {
        rt_expr.inner_func_cnt_ = 2;
        rt_expr.inner_functions_[0] = reinterpret_cast<void*>(cmp_func_1);
        rt_expr.inner_functions_[1] = reinterpret_cast<void*>(cmp_func_2);
      }
    }
  }
  return ret;
}

#define DO_VECTOR_BETWEEN_ROW_CMP()                                        \
  if (std::is_same<LVec, ObFixedLengthBase>::value) {                      \
    l_payload = fixed_base_l_payload + l_len * i;                          \
  } else if (!std::is_same<LVec, ObUniformFormat<true>>::value){           \
    l_vector->get_payload(i, l_payload, l_len);                            \
  }                                                                        \
  if (std::is_same<RVec, ObFixedLengthBase>::value) {                      \
    r_payload = fixed_base_r_payload + r_len * i;                          \
  } else if (!std::is_same<RVec, ObUniformFormat<true>>::value){           \
    r_vector->get_payload(i, r_payload, r_len);                            \
  }                                                                        \
  if (Stage == EvalBetweenStage::BETWEEN_LEFT) {                           \
    ret = (reinterpret_cast<RowCmpFunc>(expr.inner_functions_[2]))         \
          (left->obj_meta_, right->obj_meta_,                              \
          (const void *)l_payload, l_len,                                  \
          (const void *)r_payload, r_len, cmp_ret);                        \
  } else {  /*BETWEEN_RIGHT*/                                              \
    ret = (reinterpret_cast<RowCmpFunc>(expr.inner_functions_[3]))         \
          (left->obj_meta_, right->obj_meta_,                              \
          (const void *)l_payload, l_len,                                  \
          (const void *)r_payload, r_len, cmp_ret);                        \
  }

#define DO_VECTOR_BETWEEN_SET_RES()                                                    \
  /*  Result priority: false > null > true  */                                         \
  if (OB_FAIL(ret)) {                                                                  \
  } else if (Stage == EvalBetweenStage::BETWEEN_LEFT) {                                \
    /* If the current calculation is left<=val, any result is directly filled in.      \
    If the result is false, the subsequent calculation results are meaningless,        \
    and skip is set to true. */                                                        \
    res_vec->set_int(i, (cmp_ret <= 0));                                               \
    if (cmp_ret > 0) {                                                                 \
      skip.set(i);                                                                     \
    }                                                                                  \
  } else if (cmp_ret > 0) {    /*BETWEEN_RIGHT*/                                       \
    /* If currently calculating val<=right,                                            \
    only when the result is false will it be filled in. */                             \
    res_vec->set_int(i, 0);                                                            \
  }


template <typename LVec, typename RVec, typename ResVec,
          ObExprBetween::EvalBetweenStage Stage>
int ObExprBetween::inner_eval_between_vector(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            ObBitVector &skip,
                            const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObExpr *left = nullptr;
  ObExpr *right = nullptr;
  if (Stage == EvalBetweenStage::BETWEEN_LEFT) {
    left = expr.args_[1];
    right = expr.args_[0];
  } else {
    left = expr.args_[0];
    right = expr.args_[2];
  }
  LVec *l_vector = static_cast<LVec *>(left->get_vector(ctx));
  RVec *r_vector = static_cast<RVec *>(right->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  const char *l_payload = nullptr, *r_payload = nullptr;
  const char *fixed_base_l_payload = nullptr, *fixed_base_r_payload = nullptr;
  ObLength l_len = 0, r_len = 0;
  int cmp_ret = 0;
  bool l_has_null = l_vector->has_null();
  bool r_has_null = r_vector->has_null();
  // If a constant value exists and that constant value is null,
  // then set the entire res_vec to null.
  if (std::is_same<LVec, ObUniformFormat<true>>::value && l_has_null) {
    // If at this point the computation is val < right, and val is null,
    // then the result must have already been set to null previously,
    // and can be skipped directly.
    if (Stage == EvalBetweenStage::BETWEEN_LEFT) {
      for (int i = bound.start(); i < bound.end(); ++i) {
        if (skip.at(i) || eval_flags.at(i)) { continue; }
        res_vec->set_null(i);
      }
    }
  } else if (std::is_same<RVec, ObUniformFormat<true>>::value && r_has_null) {
    for (int i = bound.start(); i < bound.end(); ++i) {
      if (skip.at(i) || eval_flags.at(i)) { continue; }
      res_vec->set_null(i);
    }
  // For the case where both sides are constants, calculate only once,
  // then fill the values in a loop;
  // there is no need to consider the null situation,
  // as it has already been assessed previously.
  } else if (std::is_same<LVec, ObUniformFormat<true>>::value &&
             std::is_same<RVec, ObUniformFormat<true>>::value) {
    l_vector->get_payload(0, l_payload, l_len);
    r_vector->get_payload(0, r_payload, r_len);
    if (Stage == EvalBetweenStage::BETWEEN_LEFT) {
      ret = (reinterpret_cast<RowCmpFunc>(expr.inner_functions_[2]))
            (left->obj_meta_, right->obj_meta_,
            (const void *)l_payload, l_len,
            (const void *)r_payload, r_len, cmp_ret);
    } else {  /*BETWEEN_RIGHT*/
      ret = (reinterpret_cast<RowCmpFunc>(expr.inner_functions_[3]))
            (left->obj_meta_, right->obj_meta_,
            (const void *)l_payload, l_len,
            (const void *)r_payload, r_len, cmp_ret);
    }
    for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
      if (skip.at(i) || eval_flags.at(i)) { continue; }
      DO_VECTOR_BETWEEN_SET_RES();
    }
  } else {
    if (std::is_same<LVec, ObFixedLengthBase>::value) {
      fixed_base_l_payload = (reinterpret_cast<ObFixedLengthBase *>(l_vector))->get_data();
      l_len = (reinterpret_cast<ObFixedLengthBase *>(l_vector))->get_length();
    } else if (std::is_same<LVec, ObUniformFormat<true>>::value) {
      l_vector->get_payload(0, l_payload, l_len);
    }
    if (std::is_same<RVec, ObFixedLengthBase>::value) {
      fixed_base_r_payload = (reinterpret_cast<ObFixedLengthBase *>(r_vector))->get_data();
      r_len = (reinterpret_cast<ObFixedLengthBase *>(r_vector))->get_length();
    } else if (std::is_same<RVec, ObUniformFormat<true>>::value) {
      r_vector->get_payload(0, r_payload, r_len);
    }
    if (!(l_has_null || r_has_null)) {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
        if (skip.at(i) || eval_flags.at(i)) { continue; }
        DO_VECTOR_BETWEEN_ROW_CMP();
        DO_VECTOR_BETWEEN_SET_RES();
      }
    } else {
      for (int i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
        if (skip.at(i) || eval_flags.at(i)) { continue; }
        if (l_vector->is_null(i) || r_vector->is_null(i)) {
          res_vec->set_null(i);
        } else {
          DO_VECTOR_BETWEEN_ROW_CMP();
          DO_VECTOR_BETWEEN_SET_RES();
        }
      }
    }
  }
  return ret;
}

}
}
