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

#include "sql/engine/expr/ob_expr_ora_login_user.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_basic_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{
ObExprOraLoginUser::ObExprOraLoginUser(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc,
                       T_FUN_SYS_ORA_LOGIN_USER,
                       N_ORA_LOGIN_USER,
                       0,
                       NOT_VALID_FOR_GENERATED_COL,
                       NOT_ROW_DIMENSION)
{
}

ObExprOraLoginUser::~ObExprOraLoginUser()
{
}


int ObExprOraLoginUser::calc_result_type0(ObExprResType &type,
                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  CK (OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    type.set_varchar();
    type.set_collation_type(session->get_nls_collation());
    type.set_collation_level(CS_LEVEL_SYSCONST);
    type.set_length_semantics(session->get_actual_nls_length_semantics());
    type.set_length(DEFAULT_LENGTH);
  }
  return OB_SUCCESS;
}

int ObExprOraLoginUser::eval_ora_login_user(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(OB_NOT_NULL(session));
  if (OB_SUCC(ret)) {
    const ObString &user_name = session->get_user_name();
    ObString out_user_name;
    ObExprStrResAlloc res_alloc(expr, ctx);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    OZ(ObExprUtil::convert_string_collation(user_name, ObCharset::get_system_collation(),
                                            out_user_name, expr.datum_meta_.cs_type_,
                                            calc_alloc));
    OZ(deep_copy_ob_string(res_alloc, out_user_name, out_user_name));
    OX(res_datum.set_string(out_user_name));
  }
  return ret;
}

int ObExprOraLoginUser::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprOraLoginUser::eval_ora_login_user;
  return OB_SUCCESS;
}

}
}
