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
#include "sql/engine/expr/ob_expr_radians.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

const double ObExprRadians::radians_ratio_ = std::acos(-1) / 180;

ObExprRadians::ObExprRadians(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_RADIANS, N_RADIANS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRadians::~ObExprRadians()
{
}

int ObExprRadians::calc_result_type1(ObExprResType &type,
                                   ObExprResType &type1,
                                   common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (NOT_ROW_DIMENSION != row_dimension_ || ObMaxType == type1.get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else {
    type.set_double();
    type1.set_calc_type(ObDoubleType);
    ObExprOperator::calc_result_flag1(type, type1);
  }
  return ret;
}

int ObExprRadians::calc_radians_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    double val = arg->get_double();
    res_datum.set_double(val * radians_ratio_);
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprRadians::vector_radians(const ObExpr &expr,
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

    for (uint16_t i = 0; i < length; i++) {
      start_res[i] = start_arg[i] * radians_ratio_;
    }
  } else {
    for (int64_t idx = bound.start(); idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else {
        res_vec->set_double(idx, arg_vec->get_double(idx) * radians_ratio_);
      }
    }
  }
  return ret;
}

int ObExprRadians::calc_radians_vector_expr(const ObExpr &expr,
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
      ret = vector_radians<DoubleFixedVec, DoubleFixedVec>(expr, ctx, skip, bound);
    } else if (arg_format == VectorFormat::VEC_UNIFORM && res_format == VectorFormat::VEC_FIXED) {
      ret = vector_radians<DoubleUniVec, DoubleFixedVec>(expr, ctx, skip, bound);
    } else {
      ret = vector_radians<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
    }
  }
  return ret;
}

int ObExprRadians::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) ||
      (ObDoubleType != rt_expr.args_[0]->datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_ or arg res type is invalid", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_radians_expr;
    rt_expr.eval_vector_func_ = calc_radians_vector_expr;
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase
