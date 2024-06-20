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

#include "objit/common/ob_item_type.h"
#include "lib/oblog/ob_log.h"
#include "lib/number/ob_number_v2.h"
#include "sql/engine/expr/ob_expr_sin.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include <math.h>

namespace oceanbase
{
using namespace common;
using namespace common::number;
namespace sql
{

ObExprSin::ObExprSin(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SIN, N_SIN, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSin::~ObExprSin()
{
}

int ObExprSin::calc_result_type1(ObExprResType &type,
                                 ObExprResType &radian,
                                 ObExprTypeCtx &type_ctx) const
{
  return calc_trig_function_result_type1(type, radian, type_ctx);
}

DEF_CALC_TRIGONOMETRIC_EXPR(sin, false, OB_SUCCESS);

int ObExprSin::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_sin_expr;
  if (ObDoubleType == rt_expr.args_[0]->datum_meta_.type_) {
    rt_expr.eval_vector_func_ = eval_double_sin_vector;
  } else if (ObNumberType == rt_expr.args_[0]->datum_meta_.type_) {
    rt_expr.eval_vector_func_ = eval_number_sin_vector;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg type", K(rt_expr.args_[0]->datum_meta_.type_), K(ret));
  }
  return ret;
}

template <typename ArgVec, typename ResVec, bool IS_DOUBLE>
static int vector_sin(const ObExpr &expr,
                      ObEvalCtx &ctx,
                      const ObBitVector &skip,
                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else if (IS_DOUBLE) {
      const double arg = arg_vec->get_double(idx);
      double res = sin(arg);
      res_vec->set_double(idx, res);
    } else {
      number::ObNumber res_nmb;
      number::ObNumber radian_nmb(arg_vec->get_number(idx));
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      if (OB_FAIL(radian_nmb.sin(res_nmb, alloc_guard.get_allocator()))) {
        LOG_WARN("calc expr failed", K(ret), K(radian_nmb), K(expr));
      } else {
        res_vec->set_number(idx, res_nmb);
      }
    }
    eval_flags.set(idx);
  }

  return ret;
}

int ObExprSin::eval_number_sin_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval sin param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_sin<NumberDiscVec, NumberDiscVec, false>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_sin<NumberUniVec, NumberDiscVec, false>(expr, ctx, skip, bound);
    } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_sin<NumberContVec, NumberDiscVec, false>(expr, ctx, skip, bound);
    //...
    } else {
      ret = vector_sin<ObVectorBase, ObVectorBase, false>(expr, ctx, skip, bound);
    }
  }

  return ret;
}

int ObExprSin::eval_double_sin_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval sin param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
      vector_sin<DoubleFixedVec, DoubleFixedVec, true>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      vector_sin<DoubleUniVec, DoubleFixedVec, true>(expr, ctx, skip, bound);
    //...
    } else {
      vector_sin<ObVectorBase, ObVectorBase, true>(expr, ctx, skip, bound);
    }
  }

  return ret;
}

} /* sql */
} /* oceanbase */
