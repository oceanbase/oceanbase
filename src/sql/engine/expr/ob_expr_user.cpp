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
#include "sql/engine/expr/ob_expr_user.h"
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


ObExprUser::ObExprUser(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_USER, N_USER, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUser::~ObExprUser()
{
}

int ObExprUser::calc_result_type0(ObExprResType &type,
                                  ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  if (lib::is_oracle_mode()) {
    // FIXME @linshen: This function returns a VARCHAR2 value in oracle mode.
    // https://docs.oracle.com/en/database/oracle/oracle-database/18/sqlrf/USER.html#GUID-AD0B927B-EFD4-4246-89B4-2D55AB3AF531
    // `Oracle Database compares values of this function with blank-padded comparison semantics.`
    // 这个比较行为是 char 的比较行为, 由于 OB 目前比较行为是跟类型关联的, 没有单独的变量控制, 所以这里返回 char.
    type.set_char();
    // DEFAULT for oracle
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  } else {
    type.set_varchar();
    type.set_default_collation_type();
  }
  type.set_collation_level(CS_LEVEL_SYSCONST);
  type.set_length(static_cast<ObLength>((OB_MAX_HOST_NAME_LENGTH
                                           + OB_MAX_USER_NAME_LENGTH + 1)));
  const ObLengthSemantics default_length_semantics = (OB_NOT_NULL(type_ctx.get_session()) ? type_ctx.get_session()->get_actual_nls_length_semantics() : LS_BYTE);
  type.set_length_semantics(default_length_semantics);
  return OB_SUCCESS;
}

int ObExprUser::eval_user(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObBasicSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    if (lib::is_oracle_mode()) {
      expr_datum.set_string(session_info->get_user_name());
    } else {
      expr_datum.set_string(session_info->get_user_at_client_ip());
    }
  }
  return ret;
}

int ObExprUser::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprUser::eval_user;
  return OB_SUCCESS;
}
}/* ns sql*/
}/* ns oceanbase */
