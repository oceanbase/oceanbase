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

#define USING_LOG_PREFIX  SQL_ENG
#include "ob_expr_date.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprDate::ObExprDate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_DATE, N_DATE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprDate::~ObExprDate()
{
}

int ObExprDate::cg_expr(ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprDate::eval_date;
  rt_expr.eval_vector_func_ = ObExprDate::eval_date_vector;
  return ret;
}

int ObExprDate::eval_date(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("fail to eval conv", K(ret), K(expr));
  } else if (param->is_null()) {
    res_datum.set_null();
  } else {
    res_datum.set_date(param->get_date());
  }

  return ret;
}

template <typename ArgVec, typename ResVec>
int vector_date(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();
  if (OB_LIKELY(no_skip_no_null)) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      res_vec->set_date(idx, arg_vec->get_date(idx));
      eval_flags.set(idx);
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
        continue;
      }
      res_vec->set_date(idx, arg_vec->get_date(idx));
      eval_flags.set(idx);
    }
  }
  return ret;
}
template<>
int vector_date<DateFixedVec, DateFixedVec>(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  DateFixedVec *arg_vec = static_cast<DateFixedVec *>(expr.args_[0]->get_vector(ctx));
  DateFixedVec *res_vec = static_cast<DateFixedVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                         && eval_flags.accumulate_bit_cnt(bound) == 0;
  if (OB_LIKELY(no_skip_no_null)) {
    res_vec->set_data(arg_vec->get_data());
    eval_flags.set_all(bound.start(), bound.end());
  } else {
    for (int64_t idx = bound.start(); idx < bound.end() && OB_SUCC(ret); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
        continue;
      }
      res_vec->set_date(idx, arg_vec->get_date(idx));
      eval_flags.set(idx);
    }
  }
  return ret;
}
int ObExprDate::eval_date_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
      ret = vector_date<DateFixedVec, DateFixedVec>(expr, ctx, skip, bound);
    } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_date<DateFixedVec, DateUniVec>(expr, ctx, skip, bound);
    } else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {
      ret = vector_date<DateFixedVec, DateUniCVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = vector_date<DateUniVec, DateFixedVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_date<DateUniVec, DateUniVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {
      ret = vector_date<DateUniVec, DateUniCVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_date<DateUniCVec, DateUniVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {
      ret = vector_date<DateUniCVec, DateUniCVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {
      ret = vector_date<DateUniCVec, DateFixedVec>(expr, ctx, skip, bound);
    } else {
      ret = vector_date<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
