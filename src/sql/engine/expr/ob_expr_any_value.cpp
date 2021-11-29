/*
 * Copyright 2014-2021 Alibaba Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_expr_any_value.cpp is for any_value function
 *
 * Date: 2021/7/9
 *
 * Authors:
 *     ailing.lcq<ailing.lcq@alibaba-inc.com>
 *
 */

#include "sql/engine/expr/ob_expr_any_value.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

ObExprAnyValue::ObExprAnyValue(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ANY_VALUE, N_ANY_VAL, 1, NOT_ROW_DIMENSION)
{}

ObExprAnyValue::~ObExprAnyValue()
{}

int ObExprAnyValue::calc_result_type1(ObExprResType &type, ObExprResType &arg, common::ObExprTypeCtx &) const
{
  int ret = OB_SUCCESS;
  type = arg;
  return ret;
}

int ObExprAnyValue::calc_result1(common::ObObj &result, const common::ObObj &arg, common::ObExprCtx &expr_ctx) const
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;
  result = arg;
  return ret;
}

int ObExprAnyValue::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  rt_expr.eval_func_ = ObExprAnyValue::eval_any_value;
  return OB_SUCCESS;
}

int ObExprAnyValue::eval_any_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    SERVER_LOG(WARN, "expr evaluate parameter failed", K(ret));
  } else {
    expr_datum.set_datum(*arg);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
