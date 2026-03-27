/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_cos.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace common::number;
namespace sql
{

ObExprCos::ObExprCos(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_COS, N_COS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprCos::~ObExprCos()
{
}

int ObExprCos::calc_result_type1(ObExprResType &type,
                                 ObExprResType &radian,
                                 ObExprTypeCtx &type_ctx) const
{
  return calc_trig_function_result_type1(type, radian, type_ctx);
}

DEF_CALC_TRIGONOMETRIC_EXPR(cos, false, OB_SUCCESS);

int ObExprCos::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_cos_expr;
  return ret;
}

} /* sql */
} /* oceanbase */
