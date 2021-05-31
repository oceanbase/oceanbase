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

namespace oceanbase {
namespace sql {
ObExprStmtId::ObExprStmtId(common::ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_STMT_ID, "stmt_id", 0, NOT_ROW_DIMENSION)
{}

int ObExprStmtId::calc_result_type0(ObExprResType& type, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  type.set_int();
  return OB_SUCCESS;
}

int ObExprStmtId::calc_result0(ObObj& result, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = expr_ctx.phy_plan_ctx_;
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_ctx is null");
  } else {
    result.set_int(plan_ctx->get_cur_stmt_id());
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
