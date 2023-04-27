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
#include "sql/engine/expr/ob_expr_version.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/system_variable/ob_sys_var_class_type.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{


ObExprVersion::ObExprVersion(ObIAllocator &alloc)
  : ObStringExprOperator(alloc, T_FUN_SYS_VERSION, N_VERSION, 0, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprVersion::~ObExprVersion()
{
}

int ObExprVersion::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_length(static_cast<common::ObLength>(OB_MAX_SYS_VAR_VAL_LENGTH));
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  return OB_SUCCESS;
}

int ObExprVersion::eval_version(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is null", K(ret));
  } else {
    ObString version;
    if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_VERSION, version))) {
      LOG_WARN("fail to get version", K(ret));
    } else {
      expr_datum.set_string(version);
    }
  }

  return ret;
}

int ObExprVersion::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprVersion::eval_version;
  return OB_SUCCESS;
}

}/* ns sql*/
}/* ns oceanbase */
