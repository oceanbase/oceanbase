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

#include "sql/engine/expr/ob_expr_period_diff.h"
#include "lib/ob_name_def.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprPeriodDiff::ObExprPeriodDiff(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PERIOD_DIFF, N_PERIOD_DIFF, 2, NOT_ROW_DIMENSION)
{}

ObExprPeriodDiff::~ObExprPeriodDiff()
{}

int ObExprPeriodDiff::calc_result2(ObObj& result, const ObObj& left, const ObObj& right, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left.is_null() || right.is_null())) {
    result.set_null();
  } else {
    TYPE_CHECK(left, ObUInt64Type);
    TYPE_CHECK(right, ObUInt64Type);
    if (OB_SUCC(ret)) {
      ret = calc(result, left, right);
    }
  }
  UNUSED(expr_ctx);
  return ret;
}

template <typename T>
int ObExprPeriodDiff::calc(T& result, const T& left, const T& right)
{
  int ret = OB_SUCCESS;
  uint64_t lvalue = left.get_uint64();
  uint64_t rvalue = right.get_uint64();
  uint64_t lyear = 0;
  uint64_t lmonth = 0;
  uint64_t ryear = 0;
  uint64_t rmonth = 0;
  int64_t diff = 0;
  if (OB_FAIL(get_year_month(lvalue, lyear, lmonth))) {
    LOG_WARN("get_year_month failed", K(ret), K(lvalue), K(lyear), K(lmonth));
  } else if (OB_FAIL(get_year_month(rvalue, ryear, rmonth))) {
    LOG_WARN("get_year_month failed", K(ret), K(rvalue), K(ryear), K(rmonth));
  } else {
    diff = static_cast<int64_t>((lyear - ryear) * MONS_PER_YEAR + lmonth - rmonth);
    result.set_int(diff);
  }
  return ret;
}

int ObExprPeriodDiff::get_year_month(const uint64_t value, uint64_t& year, uint64_t& month)
{
  int ret = OB_SUCCESS;
  const uint64_t YEAR_BASE_YEAR = 1900;
  const uint64_t YEAR_VALUE_FACTOR = 100;
  const uint64_t YEAR_ADJUST_THRESHOLD = 70;
  const uint64_t YEAR_ADJUST_OFFSET = 2000;
  year = value / YEAR_VALUE_FACTOR;
  uint64_t tmp = year;
  /*
   *11(uint) will be treat as 11 + YEAR_ADJUST_OFFSET = 2011(year)
   *68(uint) will be treat as 68 + YEAR_BASE_YEAR = 1968(year)
   *1234(uint) will be treat as 1234(year)
   */
  if (year < YEAR_ADJUST_THRESHOLD)
    year += YEAR_ADJUST_OFFSET;
  else if (tmp < YEAR_VALUE_FACTOR)
    year += YEAR_BASE_YEAR;
  month = value - tmp * YEAR_VALUE_FACTOR;  // % operation ? too expensive !
  return ret;
}

int ObExprPeriodDiff::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("perioddiff expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of perioddiff expr is null", K(ret), K(rt_expr.args_));
  } else {
    CK(ObUInt64Type == rt_expr.args_[0]->datum_meta_.type_);
    CK(ObUInt64Type == rt_expr.args_[1]->datum_meta_.type_);
    rt_expr.eval_func_ = ObExprPeriodDiff::calc_perioddiff;
  }
  return ret;
}

int ObExprPeriodDiff::calc_perioddiff(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* param_datum1 = NULL;
  ObDatum* param_datum2 = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, param_datum1, param_datum2))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (OB_UNLIKELY(param_datum1->is_null() || param_datum2->is_null())) {
    expr_datum.set_null();
  } else {
    ObDatum* res_datum = static_cast<ObDatum*>(&expr_datum);
    ret = calc(*res_datum, *param_datum1, *param_datum2);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
