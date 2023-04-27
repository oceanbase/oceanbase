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
#include "lib/timezone/ob_time_convert.h"
#include "lib/ob_name_def.h"

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

  return ret;
}

int ObExprDateDiff::eval_date_diff(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *left = NULL;
  ObDatum *right = NULL;
  int64_t date_left = 0;
  int64_t date_right = 0;
  if (OB_FAIL(expr.args_[0]->eval(ctx, left))
      || OB_FAIL(expr.args_[1]->eval(ctx, right))) {
    LOG_WARN("fail to eval conv", K(ret), K(expr));
  } else if (left->is_null() || right->is_null()) {
    res_datum.set_null();
  } else if (FALSE_IT(date_left = left->get_date())) {
    // do nothing
  } else if (FALSE_IT(date_right = right->get_date())) {
    // do nothing
  } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date_left ||
                         ObTimeConverter::ZERO_DATE == date_right)) {
    res_datum.set_null();
  } else {
    res_datum.set_int(date_left - date_right);
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

} //namespace sql
} //namespace oceanbase
