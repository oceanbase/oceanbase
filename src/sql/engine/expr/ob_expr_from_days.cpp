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
#include "sql/session/ob_sql_session_info.h"
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

} //namespace sql
} //namespace oceanbase
