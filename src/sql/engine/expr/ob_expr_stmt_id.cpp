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

#include "sql/engine/expr/ob_expr_stmt_id.h"
#include "share/ob_i_sql_expression.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
ObExprStmtId::ObExprStmtId(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_STMT_ID, "stmt_id", 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

int ObExprStmtId::calc_result_type0(ObExprResType &type,
                                    ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_int();
  return OB_SUCCESS;
}

int ObExprStmtId::eval_stmt_id(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  ObPhysicalPlanCtx *plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();

  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx is null", K(ret));
  } else {
    res.set_int(plan_ctx->get_cur_stmt_id());
  }

  return ret;
}

int ObExprStmtId::cg_expr(ObExprCGCtx &expr_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_stmt_id;
  return ret;
}

} //end sql
} //end oceanbase
