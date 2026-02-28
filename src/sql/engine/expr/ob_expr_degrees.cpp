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
#include "lib/ob_define.h"
#include "sql/engine/expr/ob_expr_degrees.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{

const double ObExprDegrees::degrees_ratio_ = 180.0/std::acos(-1);

ObExprDegrees::ObExprDegrees(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_DEGREES, N_DEGREES, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprDegrees::~ObExprDegrees()
{
}

int ObExprDegrees::calc_result_type1(ObExprResType &type,
                                    ObExprResType &radian,
                                    ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (NOT_ROW_DIMENSION != row_dimension_ || ObMaxType == radian.get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else {
    type.set_double();
    radian.set_calc_type(ObDoubleType);
    ObExprOperator::calc_result_flag1(type, radian);
  }
  return ret;
}

int ObExprDegrees::check_overflow(const double res, const double val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(isinf(res))) {
    char expr_str[OB_MAX_FUNC_EXPR_LENGTH];
    int64_t pos = 0;
    ret = OB_OPERATE_OVERFLOW;
    databuff_printf(expr_str, OB_MAX_FUNC_EXPR_LENGTH, pos, "degrees(%e)", val);
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DOUBLE", expr_str);
  }
  return ret;
}

int ObExprDegrees::calc_degrees_expr(const ObExpr &expr, ObEvalCtx &ctx,
                      ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum * radian = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, radian))) {
    LOG_WARN("eval radian arg failed", K(ret), K(expr));
  } else if (radian->is_null()) {
    res_datum.set_null();
  } else {
    const double val = radian->get_double();
    const double res = val * degrees_ratio_;
    if (OB_FAIL(check_overflow(res, val))) {
      LOG_WARN("double out of range", K(val), K(res), K(ret));
    } else {
      res_datum.set_double(res);
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprDegrees::vector_degrees(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObBitVector &skip,
                                  const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  bool no_skip_no_nulls = bound.get_all_rows_active() && !arg_vec->has_null();

  if (no_skip_no_nulls
      && std::is_same_v<DoubleFixedVec, ArgVec> && std::is_same_v<DoubleFixedVec, ResVec>) {
    DoubleFixedVec *double_arg_vec = reinterpret_cast<DoubleFixedVec *>(arg_vec);
    DoubleFixedVec *double_res_vec = reinterpret_cast<DoubleFixedVec *>(res_vec);
    const double *__restrict start_arg
        = reinterpret_cast<double *>(double_arg_vec->get_data()) + bound.start();
    double *__restrict start_res
        = reinterpret_cast<double *>(double_res_vec->get_data()) + bound.start();
    uint16_t length = bound.end() - bound.start();

    for (uint16_t i = 0; OB_SUCC(ret) && i < length; i++) {
      start_res[i] = start_arg[i] * degrees_ratio_;
      if (OB_FAIL(check_overflow(start_res[i], start_arg[i]))) {
        LOG_WARN("double out of range", K(start_arg[i]), K(start_res[i]), K(ret));
      }
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else {
        const double arg = arg_vec->get_double(idx);
        const double res = arg * degrees_ratio_;
        if (OB_FAIL(check_overflow(res, arg))) {
          LOG_WARN("double out of range", K(arg), K(res), K(ret));
        } else {
          res_vec->set_double(idx, res);
        }
      }
    }
  }
  return ret;
}

int ObExprDegrees::calc_degrees_vector_expr(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            const ObBitVector &skip,
                                            const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval child vector failed", K(ret), K(expr));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (arg_format == VectorFormat::VEC_FIXED && res_format == VectorFormat::VEC_FIXED) {
      ret = vector_degrees<DoubleFixedVec, DoubleFixedVec>(expr, ctx, skip, bound);
    } else if (arg_format == VectorFormat::VEC_UNIFORM && res_format == VectorFormat::VEC_FIXED) {
      ret = vector_degrees<DoubleUniVec, DoubleFixedVec>(expr, ctx, skip, bound);
    } else {
      ret = vector_degrees<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
    }
  }
  return ret;
}

int ObExprDegrees::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) ||
      (ObDoubleType != rt_expr.args_[0]->datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_ or res type is invalid", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_degrees_expr;
    rt_expr.eval_vector_func_ = calc_degrees_vector_expr;
  }
  return ret;
}

}
}
