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
#include "ob_expr_to_days.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/vector/ob_vector_define.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprToDays::ObExprToDays(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TO_DAYS, N_TO_DAYS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {};

ObExprToDays::~ObExprToDays()
{
}

int ObExprToDays::calc_result_type1(ObExprResType &type,
                                           ObExprResType &date,
                                           common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_int();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  date.set_calc_type(ObDateType);
  return ret;
}

int calc_todays_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *day_datum = NULL;
  // 这里没有像老框架一样，判断cast的返回值是否是OB_INVLIAD_DATE_VALUE
  // 如果转换失败，结果会被置为ZERO_DATE（cast mode默认是ZERO_ON_WARN）
  // MySQL的行为是，结果是zero date时，返回NULL
  if (OB_FAIL(expr.args_[0]->eval(ctx, day_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (day_datum->is_null()) {
    res_datum.set_null();
  } else {
    int64_t day_int = day_datum->get_date() + DAYS_FROM_ZERO_TO_BASE;
    if (day_int < 0 || ObTimeConverter::ZERO_DATE == day_int) {
      res_datum.set_null();
    } else {
      res_datum.set_int(day_int);
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int vector_to_days_impl(const ObExpr &expr, ObEvalCtx &ctx,
                        const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                         && eval_flags.accumulate_bit_cnt(bound) == 0;
  if (OB_LIKELY(no_skip_no_null)) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      int32_t date = arg_vec->get_date(idx);
      int64_t day_int = date + DAYS_FROM_ZERO_TO_BASE;
      if (day_int < 0 || ObTimeConverter::ZERO_DATE == day_int) {
        res_vec->set_null(idx);
      } else {
        res_vec->set_int(idx, day_int);
      }
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      }
      if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else {
        int32_t date = arg_vec->get_date(idx);
        int64_t day_int = date + DAYS_FROM_ZERO_TO_BASE;
        if (day_int < 0 || ObTimeConverter::ZERO_DATE == day_int) {
          res_vec->set_null(idx);
        } else {
          res_vec->set_int(idx, day_int);
        }
      }
    }
  }
  return ret;
}

int ObExprToDays::calc_to_days_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                       const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval to_days param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
      ret = vector_to_days_impl<DateFixedVec, IntegerFixedVec>(expr, ctx, skip, bound);
    } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_to_days_impl<DateFixedVec, IntegerUniVec>(expr, ctx, skip, bound);
    } else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {
      ret = vector_to_days_impl<DateFixedVec, IntegerUniCVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = vector_to_days_impl<DateUniVec, IntegerFixedVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_to_days_impl<DateUniVec, IntegerUniVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {
      ret = vector_to_days_impl<DateUniVec, IntegerUniCVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {
      ret = vector_to_days_impl<DateUniCVec, IntegerFixedVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {
      ret = vector_to_days_impl<DateUniCVec, IntegerUniVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {
      ret = vector_to_days_impl<DateUniCVec, IntegerUniCVec>(expr, ctx, skip, bound);
    } else {
      ret = vector_to_days_impl<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("vector to_days calculation failed", K(ret), K(arg_format), K(res_format));
    }
  }
  return ret;
}

int ObExprToDays::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(1 != raw_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw_expr should got one child", K(ret), K(raw_expr));
  } else if (ObDateType != rt_expr.args_[0]->datum_meta_.type_) {
    // 类型推导部分有针对enum/set设置的calc type，但是新框架cast目前对enum/set支持不完整
    // 这里先报错，后续补上对enum/set的处理
    // enum/set->varchar->date
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param type should be date", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_todays_expr;
    rt_expr.eval_vector_func_ = ObExprToDays::calc_to_days_vector;
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
