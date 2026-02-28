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
#include "sql/engine/expr/ob_expr_exp.h"

#include <type_traits>

namespace oceanbase
{
using namespace common;
using namespace common::number;

namespace sql
{
ObExprExp::ObExprExp(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_EXP, N_EXP, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprExp::~ObExprExp()
{
}

int ObExprExp::calc_result_type1(ObExprResType &type,
                                  ObExprResType &type1,
                                  common::ObExprTypeCtx &type_ctx) const
{
  return calc_trig_function_result_type1(type, type1, type_ctx);
}

int calc_exp_expr_double(const ObExpr &expr, ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    double value = arg->get_double();
    value = std::exp(value);
    if (isinf(value) || isnan(value)) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      res_datum.set_double(value);
    }
  }
  return ret;
}

int calc_exp_expr_number(const ObExpr &expr, ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    number::ObNumber arg_nmb(arg->get_number());
    number::ObNumber res_nmb;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_FAIL(arg_nmb.e_power(res_nmb, alloc_guard.get_allocator()))) {
      LOG_WARN("e_power failed", K(ret));
    } else {
      res_datum.set_number(res_nmb);
    }
  }
  return ret;
}

int ObExprExp::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_ of expr", K(ret), K(rt_expr));
  } else {
    ObObjType arg_res_type = rt_expr.args_[0]->datum_meta_.type_;
    if (ObDoubleType == arg_res_type) {
      rt_expr.eval_func_ = calc_exp_expr_double;
      rt_expr.eval_vector_func_ = ObExprExp::eval_double_exp_vector;
    } else if (ObNumberType == arg_res_type) {
      rt_expr.eval_func_ = calc_exp_expr_number;
      rt_expr.eval_vector_func_ = ObExprExp::eval_number_exp_vector;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("arg type must be double or number", K(ret), K(arg_res_type));
    }
  }
  return ret;
}

// Specialized fast path for double type
template <typename ArgVec, typename ResVec>
static int vector_exp_double_fast(ArgVec *arg_vec, ResVec *res_vec, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const int64_t start = bound.start();
  const int64_t end = bound.end();

  // 对于 DoubleFixedVec，可以直接获取数组指针进行批量处理
  if constexpr (std::is_same_v<ArgVec, DoubleFixedVec> && std::is_same_v<ResVec, DoubleFixedVec>) {
    const double *__restrict arg_array
        = reinterpret_cast<const double *>(arg_vec->get_data() + start * sizeof(double));
    double *__restrict res_array
        = reinterpret_cast<double *>(res_vec->get_data() + start * sizeof(double));
    const int64_t count = end - start;

    for (int64_t i = 0; i < count; ++i) {
      const double res = std::exp(arg_array[i]);
      if (OB_UNLIKELY(isinf(res) || isnan(res))) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("exp overflow", K(ret), K(arg_array[i]), K(res));
        break;
      }
      res_array[i] = res;
    }
  } else {
    for (int64_t idx = start; idx < end; ++idx) {
      const double arg = arg_vec->get_double(idx);
      const double res = std::exp(arg);
      if (OB_UNLIKELY(isinf(res) || isnan(res))) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("exp overflow", K(ret), K(arg), K(res));
        break;
      }
      res_vec->set_double(idx, res);
    }
  }

  return ret;
}

// Specialized fast path for number type
template <typename ArgVec, typename ResVec>
static int vector_exp_number_fast(ArgVec *arg_vec,
                                  ResVec *res_vec,
                                  ObEvalCtx &ctx,
                                  const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const int64_t start = bound.start();
  const int64_t end = bound.end();
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);

  for (int64_t idx = start; idx < end; ++idx) {
    number::ObNumber arg_nmb(arg_vec->get_number(idx));
    number::ObNumber res_nmb;
    if (OB_FAIL(arg_nmb.e_power(res_nmb, alloc_guard.get_allocator()))) {
      LOG_WARN("calc expr failed", K(ret), K(arg_nmb));
      break;
    }
    res_vec->set_number(idx, res_nmb);
  }

  return ret;
}

template <typename ArgVec, typename ResVec, bool IS_DOUBLE>
static int vector_exp(const ObExpr &expr,
                      ObEvalCtx &ctx,
                      const ObBitVector &skip,
                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  const bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();
  if (no_skip_no_null) {
    // Fast path: no skip, no null, no need to check eval_flags
    if (IS_DOUBLE) {
      ret = vector_exp_double_fast<ArgVec, ResVec>(arg_vec, res_vec, bound);
    } else {
      ret = vector_exp_number_fast<ArgVec, ResVec>(arg_vec, res_vec, ctx, bound);
    }
  } else {
    // Slow path: need to check skip, eval_flags, and nulls
    const int64_t start = bound.start();
    const int64_t end = bound.end();

    if (IS_DOUBLE) {
      for (int64_t idx = start; OB_SUCC(ret) && idx < end; ++idx) {
        if (skip.at(idx) || eval_flags.at(idx)) {
          continue;
        } else if (arg_vec->is_null(idx)) {
          res_vec->set_null(idx);
        } else {
          const double arg = arg_vec->get_double(idx);
          const double res = std::exp(arg);
          if (OB_UNLIKELY(isinf(res) || isnan(res))) {
            ret = OB_OPERATE_OVERFLOW;
            LOG_WARN("exp overflow", K(ret), K(arg), K(res));
          } else {
            res_vec->set_double(idx, res);
          }
        }
      }
    } else {
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      for (int64_t idx = start; OB_SUCC(ret) && idx < end; ++idx) {
        if (skip.at(idx) || eval_flags.at(idx)) {
          continue;
        } else if (arg_vec->is_null(idx)) {
          res_vec->set_null(idx);
        } else {
          number::ObNumber arg_nmb(arg_vec->get_number(idx));
          number::ObNumber res_nmb;
          if (OB_FAIL(arg_nmb.e_power(res_nmb, alloc_guard.get_allocator()))) {
            LOG_WARN("calc expr failed", K(ret), K(arg_nmb));
          } else {
            res_vec->set_number(idx, res_nmb);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprExp::eval_number_exp_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval exp param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_exp<NumberDiscVec, NumberDiscVec, false>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_exp<NumberUniVec, NumberDiscVec, false>(expr, ctx, skip, bound);
    } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_exp<NumberContVec, NumberDiscVec, false>(expr, ctx, skip, bound);
    } else {
      ret = vector_exp<ObVectorBase, ObVectorBase, false>(expr, ctx, skip, bound);
    }
  }

  return ret;
}

int ObExprExp::eval_double_exp_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval exp param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
      ret = vector_exp<DoubleFixedVec, DoubleFixedVec, true>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = vector_exp<DoubleUniVec, DoubleFixedVec, true>(expr, ctx, skip, bound);
    } else {
      ret = vector_exp<ObVectorBase, ObVectorBase, true>(expr, ctx, skip, bound);
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
