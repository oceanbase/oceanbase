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
#include "sql/engine/expr/ob_expr_connection_id.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{


ObExprConnectionId::ObExprConnectionId(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_CONNECTION_ID, N_CONNECTION_ID, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprConnectionId::~ObExprConnectionId()
{
}

int ObExprConnectionId::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_uint32();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt32Type].precision_);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  return OB_SUCCESS;
}

int ObExprConnectionId::eval_connection_id(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    uint32_t sid = session_info->is_master_session() ? session_info->get_compatibility_sessid() : session_info->get_master_sessid();
    expr_datum.set_uint32(sid);
  }
  return ret;
}

int ObExprConnectionId::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprConnectionId::eval_connection_id;
  return OB_SUCCESS;
}


}/* ns sql*/
}/* ns oceanbase */
