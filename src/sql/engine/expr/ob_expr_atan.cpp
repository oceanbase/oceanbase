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
#include "ob_expr_atan.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprAtan::ObExprAtan(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ATAN, N_ATAN, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprAtan::~ObExprAtan()
{
}

int ObExprAtan::calc_result_typeN(ObExprResType &type,
                                   ObExprResType *types,
                                   int64_t type_num,
                                   common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && OB_UNLIKELY(NULL == types || type_num != 1)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("Invalid argument.", K(ret), K(types), K(type_num));
  } else if (lib::is_mysql_mode()
             && OB_UNLIKELY(NULL == types || type_num <= 0 || type_num > 2)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("Invalid argument.", K(ret), K(types), K(type_num));
  } else {
    if (1 == type_num) {
      ret = calc_trig_function_result_type1(type, types[0], type_ctx);
    } else {
      ret = calc_trig_function_result_type2(type, types[0], types[1], type_ctx);
    }
  }
  return ret;
}

int calc_atan_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  if (1 == expr.arg_cnt_) {
    ObDatum *radian = NULL;
    if (OB_FAIL(expr.args_[0]->eval(ctx, radian))) {
      LOG_WARN("eval radian arg failed", K(ret), K(expr));
    } else if (radian->is_null()) {
      res_datum.set_null();
    } else if (ObNumberType == expr.args_[0]->datum_meta_.type_) {
      number::ObNumber res_nmb;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      number::ObNumber nmb(radian->get_number());
      if (OB_FAIL(nmb.atan(res_nmb, alloc_guard.get_allocator()))) {
        LOG_WARN("fail to calc atan", K(ret), K(res_nmb));
      } else {
        res_datum.set_number(res_nmb);
      }
    } else if (ObDoubleType == expr.args_[0]->datum_meta_.type_) {
      const double arg = radian->get_double();
      res_datum.set_double(atan(arg));
    }
  } else { // 2 == expr.arg_cnt_
    // only mysql mode
    ObExpr *arg0 = expr.args_[0];
    ObExpr *arg1 = expr.args_[1];
    ObDatum *y = NULL;
    ObDatum *x = NULL;
    if (OB_FAIL(arg0->eval(ctx, y)) || OB_FAIL(arg1->eval(ctx, x))) {
      LOG_WARN("eval arg failed", K(ret), K(expr), KP(y), KP(x));
    } else if (y->is_null() || x->is_null()) {
      /* arg is already be cast to number type, no need to is_null_oracle */
      res_datum.set_null();
    } else if (ObDoubleType == arg0->datum_meta_.type_
              && ObDoubleType == arg1->datum_meta_.type_) {
      res_datum.set_double(atan2(y->get_double(), x->get_double()));
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObExprAtan::eval_number_atan_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  // oracle mode only has one param.
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval one param vector", K(ret));
  } else {
    VectorFormat arg0_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_FIXED == arg0_format && VEC_FIXED == res_format) {
      ret = calc_one_param_vector<DoubleFixedVec, DoubleFixedVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg0_format && VEC_FIXED == res_format) {
      ret = calc_one_param_vector<StrDiscVec, DoubleFixedVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM_CONST == arg0_format && VEC_UNIFORM_CONST == res_format) {
      ret = calc_one_param_vector<DoubleUniVec, DoubleUniVec, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = calc_one_param_vector<ObVectorBase, ObVectorBase, false>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

int ObExprAtan::eval_double_atan_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (1 == expr.arg_cnt_) {
    if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
      LOG_WARN("fail to eval one param vector", K(ret));
    } else {
      VectorFormat arg0_format = expr.args_[0]->get_format(ctx);
      VectorFormat res_format = expr.get_format(ctx);
      if (VEC_FIXED == arg0_format && VEC_FIXED == res_format) {
        ret = calc_one_param_vector<DoubleFixedVec, DoubleFixedVec, true>(
            VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == arg0_format && VEC_FIXED == res_format) {
        ret = calc_one_param_vector<StrDiscVec, DoubleFixedVec, true>(
            VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM_CONST == arg0_format && VEC_UNIFORM_CONST == res_format) {
        ret = calc_one_param_vector<DoubleUniVec, DoubleUniVec, true>(
            VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = calc_one_param_vector<ObVectorBase, ObVectorBase, true>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    }
  } else { // 2 == expr.arg_cnt_
    if (OB_FAIL(expr.eval_vector_param_value_null_short_circuit(ctx, skip, bound))) {
      LOG_WARN("fail to eval two param vector", K(ret));
    } else {
      VectorFormat arg0_format = expr.args_[0]->get_format(ctx);
      VectorFormat arg1_format = expr.args_[1]->get_format(ctx);
      VectorFormat res_format = expr.get_format(ctx);
      if (VEC_FIXED == arg0_format && VEC_FIXED == arg1_format && VEC_FIXED == res_format) {
        ret = calc_two_param_vector<DoubleFixedVec, DoubleFixedVec, DoubleFixedVec>(
            VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == arg0_format && VEC_DISCRETE == arg1_format
                 && VEC_FIXED == res_format) {
        ret = calc_two_param_vector<StrDiscVec, StrDiscVec, DoubleFixedVec>(
            VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_FIXED == arg0_format && VEC_DISCRETE == arg1_format
                 && VEC_FIXED == res_format) {
        ret = calc_two_param_vector<DoubleFixedVec, StrDiscVec, DoubleFixedVec>(
            VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == arg0_format && VEC_FIXED == arg1_format
                 && VEC_FIXED == res_format) {
        ret = calc_two_param_vector<StrDiscVec, DoubleFixedVec, DoubleFixedVec>(
            VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM_CONST == arg0_format && VEC_UNIFORM_CONST == arg1_format
                 && VEC_UNIFORM_CONST == res_format) {
        ret = calc_two_param_vector<DoubleUniVec, DoubleUniVec, DoubleUniVec>(
            VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = calc_two_param_vector<ObVectorBase, ObVectorBase, ObVectorBase>(
            VECTOR_EVAL_FUNC_ARG_LIST);
      }
    }
  }
  return ret;
}

template <typename Arg0Vec, typename ResVec, bool IS_DOUBLE>
int ObExprAtan::calc_one_param_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  const Arg0Vec *arg0_vec = static_cast<const Arg0Vec *>(expr.args_[0]->get_vector(ctx));

  bool no_skip_no_null = bound.get_all_rows_active() && !arg0_vec->has_null();

  if (no_skip_no_null) {
    if (IS_DOUBLE) {
      if (std::is_same_v<DoubleFixedVec, Arg0Vec> && std::is_same_v<DoubleFixedVec, ResVec>) {
        // Fast path: direct pointer arithmetic for DoubleFixedVec
        DoubleFixedVec *double_arg_vec
            = reinterpret_cast<DoubleFixedVec *>(const_cast<Arg0Vec *>(arg0_vec));
        DoubleFixedVec *double_res_vec = reinterpret_cast<DoubleFixedVec *>(res_vec);
        const double *__restrict start_arg
            = reinterpret_cast<const double *>(double_arg_vec->get_data()) + bound.start();
        double *__restrict start_res
            = reinterpret_cast<double *>(double_res_vec->get_data()) + bound.start();
        uint16_t length = bound.end() - bound.start();

        for (uint16_t i = 0; i < length; i++) {
          start_res[i] = atan(start_arg[i]);
        }
      } else {
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
          const double arg = arg0_vec->get_double(idx);
          res_vec->set_double(idx, atan(arg));
        }
      }
    } else {
      // oracle mode
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        number::ObNumber res_nmb;
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        number::ObNumber nmb(arg0_vec->get_number(idx));
        if (OB_FAIL(nmb.atan(res_nmb, alloc_guard.get_allocator()))) {
          LOG_WARN("fail to calc atan", K(ret), K(res_nmb));
        } else {
          res_vec->set_number(idx, res_nmb);
        }
      }
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg0_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else if (IS_DOUBLE) {
        const double arg = arg0_vec->get_double(idx);
        res_vec->set_double(idx, atan(arg));
      } else {
        number::ObNumber res_nmb;
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        number::ObNumber nmb(arg0_vec->get_number(idx));
        if (OB_FAIL(nmb.atan(res_nmb, alloc_guard.get_allocator()))) {
          LOG_WARN("fail to calc atan", K(ret), K(res_nmb));
        } else {
          res_vec->set_number(idx, res_nmb);
        }
      }
    }
  }
  return ret;
}

template <typename Arg0Vec, typename Arg1Vec, typename ResVec>
int ObExprAtan::calc_two_param_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  // Could be executed in mysql mode only.
  int ret = OB_SUCCESS;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  const Arg0Vec *arg0_vec = static_cast<const Arg0Vec *>(expr.args_[0]->get_vector(ctx));
  const Arg1Vec *arg1_vec = static_cast<const Arg1Vec *>(expr.args_[1]->get_vector(ctx));

  bool no_skip_no_null = bound.get_all_rows_active() && !arg0_vec->has_null() && !arg1_vec->has_null();

  if (no_skip_no_null) {
    if (std::is_same_v<DoubleFixedVec, Arg0Vec> && std::is_same_v<DoubleFixedVec, Arg1Vec>
        && std::is_same_v<DoubleFixedVec, ResVec>) {
      // Fast path: direct pointer arithmetic for DoubleFixedVec
      DoubleFixedVec *double_arg0_vec
          = reinterpret_cast<DoubleFixedVec *>(const_cast<Arg0Vec *>(arg0_vec));
      DoubleFixedVec *double_arg1_vec
          = reinterpret_cast<DoubleFixedVec *>(const_cast<Arg1Vec *>(arg1_vec));
      DoubleFixedVec *double_res_vec = reinterpret_cast<DoubleFixedVec *>(res_vec);
      const double *__restrict start_arg0
          = reinterpret_cast<const double *>(double_arg0_vec->get_data()) + bound.start();
      const double *__restrict start_arg1
          = reinterpret_cast<const double *>(double_arg1_vec->get_data()) + bound.start();
      double *__restrict start_res
          = reinterpret_cast<double *>(double_res_vec->get_data()) + bound.start();
      uint16_t length = bound.end() - bound.start();

      for (uint16_t i = 0; i < length; i++) {
        start_res[i] = atan2(start_arg0[i], start_arg1[i]);
      }
    } else {
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        res_vec->set_double(idx, atan2(arg0_vec->get_double(idx), arg1_vec->get_double(idx)));
      }
    }
  } else {
    // General path: use accessor methods
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      }

      if (arg0_vec->is_null(idx) || arg1_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else {
        res_vec->set_double(idx, atan2(arg0_vec->get_double(idx), arg1_vec->get_double(idx)));
      }
    }
  }
  return ret;
}

int ObExprAtan::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (lib::is_oracle_mode() && OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else if (lib::is_mysql_mode()
             && OB_UNLIKELY(1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_atan_expr;
    if (ObNumberType == rt_expr.args_[0]->datum_meta_.type_) {
      rt_expr.eval_vector_func_ = ObExprAtan::eval_number_atan_vector;
    } else if (ObDoubleType == rt_expr.args_[0]->datum_meta_.type_) {
      rt_expr.eval_vector_func_ = ObExprAtan::eval_double_atan_vector;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid arg type", K(ret), K(rt_expr.args_[0]->datum_meta_.type_));
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
