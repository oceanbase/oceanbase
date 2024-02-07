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
#include "sql/engine/expr/ob_expr_current_user.h"
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


ObExprCurrentUser::ObExprCurrentUser(ObIAllocator &alloc)
    :ObStringExprOperator(alloc, T_FUN_SYS_CURRENT_USER, N_CURRENT_USER, 0, NOT_VALID_FOR_GENERATED_COL)
{
}

ObExprCurrentUser::~ObExprCurrentUser()
{
}

int ObExprCurrentUser::calc_result_type0(ObExprResType &type,
                                         ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_default_collation_type();
  type.set_collation_level(CS_LEVEL_SYSCONST);
  type.set_length(static_cast<ObLength>((OB_MAX_HOST_NAME_LENGTH
                                           + OB_MAX_USER_NAME_LENGTH + 1)));
  return ret;
}

int ObExprCurrentUser::eval_current_user(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(session_info = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    uint64_t tenant_id = session_info->get_effective_tenant_id();
    uint64_t priv_user_id = session_info->get_priv_user_id();
    const ObUserInfo *user_info = nullptr;
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL GCTX.schema_service_", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   tenant_id, schema_guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_user_info(tenant_id,
                                                  priv_user_id,
                                                  user_info))) {
      LOG_WARN("failed to get user info", K(ret), K(tenant_id), K(priv_user_id));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL user_info", K(ret), K(tenant_id), K(priv_user_id));
    } else {
      const ObString &user_name = user_info->get_user_name_str();
      const ObString &hostname = user_info->get_host_name_str();
      size_t buf_len = user_name.length() + hostname.length() + 2;
      size_t pos=0;

      char *buf = static_cast<char *>(
                    ctx.get_expr_res_alloc().alloc(buf_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for result buf", K(ret), K(user_info));
      } else if (OB_UNLIKELY(
                     (pos = snprintf(buf, buf_len, "%.*s@%.*s",
                                     user_name.length(), user_name.ptr(),
                                     hostname.length(), hostname.ptr())) < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to snprintf", K(ret), K(user_info), K(buf_len));
      } else {
        expr_datum.set_string(buf, pos);
      }
    }
  }
  return ret;
}

int ObExprCurrentUser::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprCurrentUser::eval_current_user;
  return OB_SUCCESS;
}

}/* ns sql*/
}/* ns oceanbase */
