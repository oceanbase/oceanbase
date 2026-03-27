/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/engine/expr/ob_expr_row_count.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprRowCount::ObExprRowCount(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_ROW_COUNT, N_ROW_COUNT, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRowCount::~ObExprRowCount()
{
}

int ObExprRowCount::calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_int();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  return OB_SUCCESS;
}

int ObExprRowCount::eval_row_count(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "session info is null", K(ret));
  } else {
    expr_datum.set_int(session_info->get_affected_rows());
  }
  return ret;
}

int ObExprRowCount::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprRowCount::eval_row_count;
  return OB_SUCCESS;
}
}
}

