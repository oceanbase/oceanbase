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

#include "sql/engine/expr/ob_expr_any_value.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprAnyValue::ObExprAnyValue(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_ANY_VALUE, N_ANY_VAL, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprAnyValue::~ObExprAnyValue()
{
}

int ObExprAnyValue::calc_result_type1(ObExprResType &type,
                                           ObExprResType &arg,
                                           common::ObExprTypeCtx &) const
{
  int ret = OB_SUCCESS;
  type = arg;
  return ret;
}

int ObExprAnyValue::cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  rt_expr.eval_func_ = ObExprAnyValue::eval_any_value;
  return OB_SUCCESS;
}

int ObExprAnyValue::eval_any_value(const ObExpr &expr,
                      ObEvalCtx &ctx,
                      ObDatum &expr_datum)
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

} // namespace sql
} // namespace oceanbase
