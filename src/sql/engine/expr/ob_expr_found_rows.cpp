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

#include "sql/engine/expr/ob_expr_found_rows.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprFoundRows::ObExprFoundRows(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_FOUND_ROWS, N_FOUND_ROWS, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprFoundRows::~ObExprFoundRows()
{
}

int ObExprFoundRows::calc_result_type0(ObExprResType &type,
                                       common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_int();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  return OB_SUCCESS;
}

int ObExprFoundRows::eval_found_rows(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "session info is null", K(ret));
  } else {
    expr_datum.set_int(session_info->get_found_rows());
  }
  return ret;
}

int ObExprFoundRows::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprFoundRows::eval_found_rows;
  return OB_SUCCESS;
}
}
}
