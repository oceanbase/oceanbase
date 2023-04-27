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
#include "sql/engine/expr/ob_expr_current_user_priv.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprCurrentUserPriv::ObExprCurrentUserPriv(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_CURRENT_USER_PRIV,
                       N_CURRENT_USER_PRIV, 0,
                       NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {
}

ObExprCurrentUserPriv::~ObExprCurrentUserPriv() {
}

int ObExprCurrentUserPriv::calc_result_type0(ObExprResType &type,
                                             ObExprTypeCtx &type_ctx) const {
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_type(ObIntType);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  return ret;
}

int ObExprCurrentUserPriv::eval_current_user_priv(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum) {
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    const ObPrivSet user_priv_set = session_info->get_user_priv_set();
    expr_datum.set_int(user_priv_set);
  }
  return ret;
}

int ObExprCurrentUserPriv::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprCurrentUserPriv::eval_current_user_priv;
  return OB_SUCCESS;
}

}/* ns sql*/
}/* ns oceanbase */
