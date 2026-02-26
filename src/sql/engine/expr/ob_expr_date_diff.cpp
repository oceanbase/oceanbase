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

#include "sql/engine/expr/ob_expr_date_diff.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprDateDiff::ObExprDateDiff(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_DATE_DIFF, N_DATE_DIFF, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprDateDiff::~ObExprDateDiff()
{
}

ObExprMonthsBetween::ObExprMonthsBetween(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_MONTHS_BETWEEN, N_MONTHS_BETWEEN, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprMonthsBetween::~ObExprMonthsBetween()
{
}

int ObExprMonthsBetween::calc_result_type2(ObExprResType &type,
                                           ObExprResType &type1,
                                           ObExprResType &type2,
                                           ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_number();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_scale());
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_precision());
  type1.set_calc_type(ObDateTimeType);
  type2.set_calc_type(ObDateTimeType);
  return ret;
}

int ObExprDateDiff::cg_expr(ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprDateDiff::eval_date_diff;
  rt_expr.eval_vector_func_ = eval_date_diff_vector;

  return ret;
}

int ObExprDateDiff::eval_date_diff(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  int32_t date_left = 0;
  int32_t date_right = 0;
  if (OB_FAIL(expr.args_[0]->eval(ctx, left))
      || OB_FAIL(expr.args_[1]->eval(ctx, right))) {
    LOG_WARN("fail to eval conv", K(ret), K(expr));
  } else if (left->is_null() || right->is_null()) {
    res_datum.set_null();
  } else {
    if (ob_is_mysql_date_tc(expr.args_[0]->datum_meta_.type_)) {
      date_left = ObTimeConverter::calc_date(left->get_mysql_date());
      date_right = ObTimeConverter::calc_date(right->get_mysql_date());
    } else {
      date_left = left->get_date();
      date_right = right->get_date();
    }
    if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date_left ||
                      ObTimeConverter::ZERO_DATE == date_right)) {
      res_datum.set_null();
    } else {
      res_datum.set_int(date_left - date_right);
    }
  }

  return ret;
}

int ObExprMonthsBetween::cg_expr(ObExprCGCtx &op_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprMonthsBetween::eval_months_between;

  return ret;
}

int ObExprMonthsBetween::eval_months_between(const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  const int64_t DAYS_PER_MONTH_BASE = 31;
  int64_t date_value1 = 0;
  int64_t date_value2 = 0;
  int64_t rest_utc_diff = 0;
  int64_t months_diff = 0;
  number::ObNumber res_num;
  number::ObNumber res_int;
  number::ObNumber res_frac;
  number::ObNumber res_numerator;
  number::ObNumber res_denominator;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  common::ObArenaAllocator &tmp_allocator = alloc_guard.get_allocator();
  if (OB_FAIL(expr.args_[0]->eval(ctx, left))
      || OB_FAIL(expr.args_[1]->eval(ctx, right))) {
    LOG_WARN("fail to eval conv", K(ret), K(expr));
  } else if (left->is_null() || right->is_null()) {
    res_datum.set_null();
  } else if (FALSE_IT(date_value1 = left->get_datetime())) {
    // do nothing
  } else if (FALSE_IT(date_value2 = right->get_datetime())) {
    // do nothing
  } else if (OB_FAIL(ObTimeConverter::calc_days_and_months_between_dates(date_value1,
                                                                         date_value2,
                                                                         months_diff,
                                                                         rest_utc_diff))) {
    LOG_WARN("fail to calc diff of days", K(ret));
  } else if (OB_FAIL(res_int.from(months_diff, tmp_allocator))) {
    LOG_WARN("fail to from integer", K(ret));
  } else if (OB_FAIL(res_numerator.from(rest_utc_diff, tmp_allocator))) {
    LOG_WARN("fail to from integer", K(ret));
  } else if (OB_FAIL(res_denominator.from(DAYS_PER_MONTH_BASE * USECS_PER_DAY, tmp_allocator))) {
    LOG_WARN("fail to from integer", K(ret));
  } else if (OB_FAIL(res_numerator.div(res_denominator, res_frac, tmp_allocator))) {
    LOG_WARN("fail to div", K(ret));
  } else if (OB_FAIL(res_int.add(res_frac, res_num, tmp_allocator))) {
    LOG_WARN("fail to add", K(ret));
  } else {
    res_datum.set_number(res_num);
  }

  return ret;
}


#define CHECK_SKIP_NULL(idx) {                                              \
  if (skip.at(idx) || eval_flags.at(idx)) {                                 \
    continue;                                                               \
  } else if (left_arg_vec->is_null(idx) || right_arg_vec->is_null(idx)) {   \
    res_vec->set_null(idx);                                                 \
    eval_flags.set(idx);                                                    \
    continue;                                                               \
  }                                                                         \
}
#define BATCH_CALC(BODY) {                                                        \
  if (OB_LIKELY(no_skip_no_null)) {                                               \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      BODY;                                                                       \
    }                                                                             \
  } else {                                                                        \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      CHECK_SKIP_NULL(idx);                                                       \
      BODY;                                                                       \
    }                                                                             \
  }                                                                               \
}

template <typename LeftArgVec, typename RightArgVec, typename ResVec, typename IN_TYPE>
int vector_date_diff(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  LeftArgVec *left_arg_vec = static_cast<LeftArgVec *>(expr.args_[0]->get_vector(ctx));
  RightArgVec *right_arg_vec = static_cast<RightArgVec *>(expr.args_[1]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  int64_t tz_offset = 0;
  DateType left_date = 0;
  DateType right_date = 0;
  UsecType left_usec = 0;
  UsecType right_usec = 0;
  bool no_skip_no_null = bound.get_all_rows_active() && !left_arg_vec->has_null() && !right_arg_vec->has_null()
                         && eval_flags.accumulate_bit_cnt(bound) == 0;

  BATCH_CALC({
    IN_TYPE left_val = *reinterpret_cast<const IN_TYPE*>(left_arg_vec->get_payload(idx));
    IN_TYPE right_val = *reinterpret_cast<const IN_TYPE*>(right_arg_vec->get_payload(idx));
    if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(left_val, tz_offset, lib::is_oracle_mode(), left_date, left_usec))) {
      LOG_WARN("get date and usec from vec failed", K(ret));
    } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(right_val, tz_offset, lib::is_oracle_mode(), right_date, right_usec))) {
      LOG_WARN("get date and usec from vec failed", K(ret));
    } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == left_date
                          || ObTimeConverter::ZERO_DATE == right_date)) {
      res_vec->set_null(idx);
    } else {
      int64_t datediff = left_date - right_date;
      res_vec->set_int(idx, datediff);
    }
    eval_flags.set(idx);
  });
  return ret;
}
int ObExprDateDiff::eval_date_diff_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_diff param_1", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_diff param_2", K(ret));
  } else {
    VectorFormat left_arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat right_arg_format = expr.args_[1]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObObjTypeClass arg_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);

#define DEF_DATE_DIFF_VECTOR(arg_type, res_type)\
  if (VEC_FIXED == left_arg_format && VEC_FIXED == right_arg_format) {\
    ret = vector_date_diff<CONCAT(arg_type, FixedVec), CONCAT(arg_type, FixedVec), res_type, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_FIXED == left_arg_format && VEC_UNIFORM == right_arg_format) {\
    ret = vector_date_diff<CONCAT(arg_type, FixedVec), CONCAT(arg_type, UniVec), res_type, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_FIXED == left_arg_format && VEC_UNIFORM_CONST == right_arg_format) {\
    ret = vector_date_diff<CONCAT(arg_type, FixedVec), CONCAT(arg_type, UniCVec), res_type, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM == left_arg_format && VEC_FIXED == right_arg_format) {\
    ret = vector_date_diff<CONCAT(arg_type, UniVec), CONCAT(arg_type, FixedVec), res_type, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM == left_arg_format && VEC_UNIFORM == right_arg_format) {\
    ret = vector_date_diff<CONCAT(arg_type, UniVec), CONCAT(arg_type, UniVec), res_type, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM == left_arg_format && VEC_UNIFORM_CONST == right_arg_format) {\
    ret = vector_date_diff<CONCAT(arg_type, UniVec), CONCAT(arg_type, UniCVec), res_type, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM_CONST == left_arg_format && VEC_FIXED == right_arg_format) {\
    ret = vector_date_diff<CONCAT(arg_type, UniCVec), CONCAT(arg_type, FixedVec), res_type, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM_CONST == left_arg_format && VEC_UNIFORM == right_arg_format) {\
    ret = vector_date_diff<CONCAT(arg_type, UniCVec), CONCAT(arg_type, UniVec), res_type, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM_CONST == left_arg_format && VEC_UNIFORM_CONST == right_arg_format) {\
    ret = vector_date_diff<CONCAT(arg_type, UniCVec), CONCAT(arg_type, UniCVec), res_type, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  } else {\
    ret = vector_date_diff<ObVectorBase, ObVectorBase, ObVectorBase, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  }

#define DISPATCH_DATE_DIFF_ARG_VECTOR(arg_type)\
  if (VEC_FIXED == res_format) { \
    DEF_DATE_DIFF_VECTOR(arg_type, DecInt64FixedVec)\
  } else if (VEC_UNIFORM == res_format) {\
    DEF_DATE_DIFF_VECTOR(arg_type, DecInt64UniVec)\
  } else if (VEC_UNIFORM_CONST == res_format) {\
    DEF_DATE_DIFF_VECTOR(arg_type, DecInt64UniCVec)\
  } else {\
    ret = vector_date_diff<ObVectorBase, ObVectorBase, ObVectorBase, CONCAT(arg_type, Type)>(expr, ctx, skip, bound);\
  }

    if (ObMySQLDateTC == arg_tc) {
      DISPATCH_DATE_DIFF_ARG_VECTOR(MySQLDate)
    } else if (ObDateTC == arg_tc) {
      DISPATCH_DATE_DIFF_ARG_VECTOR(Date)
    }

#undef DISPATCH_DATE_DIFF_ARG_VECTOR
#undef DEF_DATE_DIFF_VECTOR

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
