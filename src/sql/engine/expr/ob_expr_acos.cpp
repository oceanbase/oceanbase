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
#include "ob_expr_acos.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprAcos::ObExprAcos(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ACOS, N_ACOS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {}

ObExprAcos::~ObExprAcos()
{
}

int ObExprAcos::calc_result_type1(ObExprResType &type,
                                  ObExprResType &type1,
                                  common::ObExprTypeCtx &type_ctx) const
{
  return calc_trig_function_result_type1(type, type1, type_ctx);
}

DEF_CALC_TRIGONOMETRIC_EXPR(acos, arg > 1 || arg < -1, OB_ERR_ARGUMENT_OUT_OF_RANGE);

template <typename ArgVec, typename ResVec, bool IS_DOUBLE>
static int vector_acos(const ObExpr &expr,
                       ObEvalCtx &ctx,
                       const ObBitVector &skip,
                       const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_nulls = bound.get_all_rows_active() && !arg_vec->has_null();
  bool all_valid = false;
  uint16_t length = bound.end() - bound.start();
  double *__restrict start_arg = nullptr;

  if (lib::is_mysql_mode() && IS_DOUBLE && no_skip_no_nulls
      && std::is_same_v<DoubleFixedVec, ArgVec> && std::is_same_v<DoubleFixedVec, ResVec>) {
    DoubleFixedVec *double_arg_vec = reinterpret_cast<DoubleFixedVec *>(arg_vec);
    start_arg
        = reinterpret_cast<double *>(double_arg_vec->get_data()) + bound.start();
    all_valid = true;
    for (uint16_t i = 0; all_valid && i < length; i++) {
      all_valid = all_valid && start_arg[i] <= 1 && start_arg[i] >= -1;
    }
  }

  if (all_valid) {
    DoubleFixedVec *double_res_vec = reinterpret_cast<DoubleFixedVec *>(res_vec);
    double *__restrict start_res
        = reinterpret_cast<double *>(double_res_vec->get_data()) + bound.start();
    for (uint16_t i = 0; i < length; i++) {
      start_res[i] = acos(start_arg[i]);
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else if (IS_DOUBLE) {
        const double arg = arg_vec->get_double(idx);
        if (arg > 1 || arg < -1) {
          if (lib::is_oracle_mode()) {
            ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
          } else {
            res_vec->set_null(idx);
          }
        } else {
          res_vec->set_double(idx, acos(arg));
        }
      } else {
        number::ObNumber res_nmb;
        number::ObNumber radian_nmb(arg_vec->get_number(idx));
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        if (OB_FAIL(radian_nmb.acos(res_nmb, alloc_guard.get_allocator()))) {
          LOG_WARN("calc expr failed", K(ret), K(radian_nmb), K(expr));
        } else {
          res_vec->set_number(idx, res_nmb);
        }
      }
    }
  }

  return ret;
}


int ObExprAcos::eval_number_acos_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval acos param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_acos<NumberDiscVec, NumberDiscVec, false>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_acos<NumberUniVec, NumberDiscVec, false>(expr, ctx, skip, bound);
    } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
      ret = vector_acos<NumberContVec, NumberDiscVec, false>(expr, ctx, skip, bound);
    } else {
      ret = vector_acos<ObVectorBase, ObVectorBase, false>(expr, ctx, skip, bound);
    }
  }

  return ret;
}

int ObExprAcos::eval_double_acos_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval acos param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
      ret = vector_acos<DoubleFixedVec, DoubleFixedVec, true>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = vector_acos<DoubleUniVec, DoubleFixedVec, true>(expr, ctx, skip, bound);
    } else {
      ret = vector_acos<ObVectorBase, ObVectorBase, true>(expr, ctx, skip, bound);
    }
  }

  return ret;
}

int ObExprAcos::cg_expr(ObExprCGCtx &expr_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_acos_expr;
  if (ObDoubleType == rt_expr.args_[0]->datum_meta_.type_) {
    rt_expr.eval_vector_func_ = eval_double_acos_vector;
  } else if (ObNumberType == rt_expr.args_[0]->datum_meta_.type_) {
    rt_expr.eval_vector_func_ = eval_number_acos_vector;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg type", K(rt_expr.args_[0]->datum_meta_.type_), K(ret));
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase
