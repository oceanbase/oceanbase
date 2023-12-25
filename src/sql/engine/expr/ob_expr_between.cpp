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

// eval_left_and_right_operand
template <typename ValVec, typename ResVec>
static int eval_lr_operand(const ObExpr &expr, ObEvalCtx &ctx,
                           ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const ObExpr &val_expr = *expr.args_[0];
  const ObExpr &left_expr = *expr.args_[1];
  const ObExpr &right_expr = *expr.args_[2];
  ValVec *val_vec = static_cast<ValVec *>(val_expr.get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  if (val_vec->has_null()) {
    for (int i = bound.start(); i < bound.end(); ++i) {
      if (!skip.at(i) && val_vec->is_null(i)) {
        res_vec->set_null(i);
        skip.set(i);
      }
    }
  }
  // if skip is all true, `eval_vector` still needs to be called, for that expr format may not be inited.
  if (OB_FAIL(left_expr.eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval left operand failed", K(ret));
  } else if (OB_FAIL(right_expr.eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval right operand failed", K(ret));
  }
  return ret;
}

template <typename ValVec>
static int dispatch_eval_between_operands(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  VectorFormat res_format = expr.get_format(ctx);
  switch (res_format) {
    case VEC_FIXED: {
      ret = eval_lr_operand<ValVec, ObBitmapNullVectorBase>(expr, ctx, skip, bound);
      break;
    }
    case VEC_UNIFORM: {
      ret = eval_lr_operand<ValVec, ObUniformFormat<false>>(expr, ctx, skip, bound);
      break;
    }
    case VEC_UNIFORM_CONST: {
      ret = eval_lr_operand<ValVec, ObUniformFormat<true>>(expr, ctx, skip, bound);
      break;
    }
    default: {
      ret = eval_lr_operand<ValVec, ObVectorBase>(expr, ctx, skip, bound);
    }
  }
  return ret;
}

static int eval_between_operands(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const ObExpr &val_expr = *expr.args_[0];
  if (OB_FAIL(val_expr.eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval left operand failed", K(ret));
  } else {
    VectorFormat val_format = val_expr.get_format(ctx);
    switch (val_format) {
      case VEC_DISCRETE:
      case VEC_CONTINUOUS:
      case VEC_FIXED: {
        ret = dispatch_eval_between_operands<ObBitmapNullVectorBase>(expr, ctx, skip, bound);
        break;
      }
      case VEC_UNIFORM: {
        ret = dispatch_eval_between_operands<ObUniformFormat<false>>(expr, ctx, skip, bound);
        break;
      }
      case VEC_UNIFORM_CONST: {
        ret = dispatch_eval_between_operands<ObUniformFormat<true>>(expr, ctx, skip, bound);
        break;
      }
      default: {
        ret = dispatch_eval_between_operands<ObVectorBase>(expr, ctx, skip, bound);
      }
    }
  }
  return ret;
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
  if (OB_FAIL(eval_between_operands(expr, ctx, my_skip, bound))) {
    LOG_WARN("eval between operands failed", K(ret));
  } else if (OB_FAIL((reinterpret_cast<EvalVectorBetweenFunc>(expr.inner_functions_[2]))(
                  expr, left_expr, val_expr, ctx, my_skip, bound))) { // eval left <= val
    LOG_WARN("compare left and val failed", K(ret));
  } else if (OB_FAIL((reinterpret_cast<EvalVectorBetweenFunc>(expr.inner_functions_[3]))(
                  expr, val_expr, right_expr, ctx, my_skip, bound))) { // eval val <= right
    LOG_WARN("compare val and right failed", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    eval_flags.bit_not(skip, bound);
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
    EvalVectorBetweenFunc vec_cmp_func_1 = NULL;  // left <= val
    EvalVectorBetweenFunc vec_cmp_func_2 = NULL;  // val <= right
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
    } else if (OB_ISNULL(vec_cmp_func_1 = VectorCmpExprFuncsHelper::get_eval_vector_between_expr_cmp_func(
                                          left_meta, val_meta, EvalBetweenStage::BETWEEN_LEFT))) {
      ret = OB_ERR_UNEXPECTED;
      VecValueTypeClass value_tc = get_vec_value_tc(val_meta.type_, val_meta.scale_, val_meta.precision_);
      VecValueTypeClass left_tc = get_vec_value_tc(left_meta.type_, left_meta.scale_, left_meta.precision_);
      LOG_WARN("The result of get_eval_vector_between_expr_cmp_func(left) is null.",
                K(ret), K(left_meta), K(val_meta), K(right_meta), K(value_tc), K(left_tc), K(rt_expr));
    } else if (OB_ISNULL(vec_cmp_func_2 = VectorCmpExprFuncsHelper::get_eval_vector_between_expr_cmp_func(
                                          val_meta, right_meta, EvalBetweenStage::BETWEEN_RIGHT))) {
      ret = OB_ERR_UNEXPECTED;
      VecValueTypeClass value_tc = get_vec_value_tc(val_meta.type_, val_meta.scale_, val_meta.precision_);
      VecValueTypeClass right_tc = get_vec_value_tc(right_meta.type_, right_meta.scale_, right_meta.precision_);
      LOG_WARN("The result of get_eval_vector_between_expr_cmp_func(right) is null.",
                K(ret), K(left_meta), K(val_meta), K(right_meta), K(value_tc), K(right_tc), K(rt_expr));
    } else if (OB_ISNULL(rt_expr.inner_functions_ = reinterpret_cast<void**>(
                         expr_cg_ctx.allocator_->alloc(sizeof(DatumCmpFunc) * 2 +
                                                       sizeof(EvalVectorBetweenFunc) * 2)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for inner_functions_ failed", K(ret));
    } else {
      rt_expr.inner_func_cnt_ = 4;
      rt_expr.inner_functions_[0] = reinterpret_cast<void*>(cmp_func_1);
      rt_expr.inner_functions_[1] = reinterpret_cast<void*>(cmp_func_2);
      rt_expr.inner_functions_[2] = reinterpret_cast<void*>(vec_cmp_func_1);
      rt_expr.inner_functions_[3] = reinterpret_cast<void*>(vec_cmp_func_2);
      rt_expr.eval_func_ = calc_between_expr;
      rt_expr.eval_vector_func_ = eval_between_vector;
    }
  }
  return ret;
}

}
}
