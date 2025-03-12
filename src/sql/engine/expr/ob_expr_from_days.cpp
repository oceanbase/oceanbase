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
#include "ob_expr_from_days.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprFromDays::ObExprFromDays(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_FROM_DAYS, N_FROM_DAYS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
};

ObExprFromDays::~ObExprFromDays()
{
}

int ObExprFromDays::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fromdays expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of fromdays expr is null", K(ret), K(rt_expr.args_));
  } else {
    CK(ObInt32Type == rt_expr.args_[0]->datum_meta_.type_);
    rt_expr.eval_func_ = ObExprFromDays::calc_fromdays;
    rt_expr.eval_vector_func_ = ObExprFromDays::calc_fromdays_vector;
  }
  return ret;
}

int ObExprFromDays::calc_fromdays(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else {
    // max: 9999-12-31 min:0000-00-00
    int32_t value = param_datum->get_int32();
    if (value >= MIN_DAYS_OF_DATE
        && value <= MAX_DAYS_OF_DATE) {
      expr_datum.set_date(value - DAYS_FROM_ZERO_TO_BASE);
    } else {
      expr_datum.set_date(ObTimeConverter::ZERO_DATE);
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int vector_fromdays(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null =
    bound.get_all_rows_active() && eval_flags.accumulate_bit_cnt(bound) == 0 && !arg_vec->has_null();

  if (no_skip_no_null) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      int32_t value = arg_vec->get_int32(idx);
      if (value >= MIN_DAYS_OF_DATE && value <= MAX_DAYS_OF_DATE) {
        res_vec->set_date(idx, value - DAYS_FROM_ZERO_TO_BASE);
      } else {
        res_vec->set_date(idx, ObTimeConverter::ZERO_DATE);
      }
    }
    eval_flags.set_all(bound.start(), bound.end());
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
        continue;
      }
      int32_t value = arg_vec->get_int32(idx);
      if (value >= MIN_DAYS_OF_DATE && value <= MAX_DAYS_OF_DATE) {
        res_vec->set_date(idx, value - DAYS_FROM_ZERO_TO_BASE);
      } else {
        res_vec->set_date(idx, ObTimeConverter::ZERO_DATE);
      }
      eval_flags.set(idx);
    }
  }
  return ret;
}
int ObExprFromDays::calc_fromdays_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {
      ret = vector_fromdays<IntegerUniCVec, DateUniCVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {
      ret = vector_fromdays<IntegerUniCVec, DateFixedVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_fromdays<IntegerUniCVec, DateUniVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {
      ret = vector_fromdays<IntegerUniVec, DateUniCVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = vector_fromdays<IntegerUniVec, DateFixedVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_fromdays<IntegerUniVec, DateUniVec>(expr, ctx, skip, bound);
    } else if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
      ret = vector_fromdays<IntegerFixedVec, DateFixedVec>(expr, ctx, skip, bound);
    } else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {
      ret = vector_fromdays<IntegerFixedVec, DateUniCVec>(expr, ctx, skip, bound);
    } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_fromdays<IntegerFixedVec, DateUniVec>(expr, ctx, skip, bound);
    } else {
      ret = vector_fromdays<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

#undef CHECK_SKIP_NULL
#undef BATCH_CALC

} //namespace sql
} //namespace oceanbase
