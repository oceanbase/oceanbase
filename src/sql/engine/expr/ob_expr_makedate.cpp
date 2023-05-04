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
#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_makedate.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprMakedate::ObExprMakedate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_MAKEDATE, N_MAKEDATE, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprMakedate::~ObExprMakedate()
{
}

int ObExprMakedate::calc_result_type2(ObExprResType &type,
                                        ObExprResType &year,
                                        ObExprResType &day,
                                        common::ObExprTypeCtx &type_ctx) const
{
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
  int ret = common::OB_SUCCESS;
  type.set_type(ObDateType);
  year.set_calc_type(ObIntType);
  day.set_calc_type(ObIntType);
  return ret;
}

int ObExprMakedate::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("makedate expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
             || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of makedate expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprMakedate::calc_makedate;
  }
  return ret;
}

int ObExprMakedate::calc_makedate(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *year = NULL;
  ObDatum *day = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, year, day))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (year->is_null() || day->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(calc(expr_datum, year->get_int(), day->get_int()))) {
    LOG_WARN("calc make date failed", K(ret));
  }
  return ret;
}

template <typename T>
int ObExprMakedate::calc(T &res, int64_t year, int64_t day)
{
  int ret = OB_SUCCESS;
  if (year < 0 || year > 9999 || day <= 0 || day > DATE_MAX_VAL) {
    res.set_null();
  } else {
    if (year < 70) {
      year += 2000;
    } else if (year < 100) {
      year += 1900;
    }
    ObTime ot;
    ot.parts_[DT_YEAR] = year;
    ot.parts_[DT_MON] = 1;
    ot.parts_[DT_MDAY] = 1;
    int32_t date_value = ObTimeConverter::ob_time_to_date(ot);
    if (date_value - 1 + day > DATE_MAX_VAL) {
      res.set_null();
    } else {
      res.set_date(date_value - 1 + day);
    }
  }
  return ret;
}

}
}
